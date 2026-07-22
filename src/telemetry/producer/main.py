"""Simulator CLI: backfill history or stream live sessions into Redpanda."""
from __future__ import annotations

import argparse
import logging
import random
import time
from datetime import date, datetime, timedelta, timezone

from telemetry.config import get_settings
from telemetry.log import setup_logging
from telemetry.producer.kafka_producer import TelemetryProducer
from telemetry.producer.simulator import (
    TRAINING_DAYS,
    generate_recovery,
    generate_session,
    make_athletes,
    simulate_history,
)

log = logging.getLogger("producer")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="mode", required=True)
    backfill = sub.add_parser("backfill", help="emit N days of history as fast as possible")
    backfill.add_argument("--days", type=int, default=42)
    live = sub.add_parser("live", help="emit one session per athlete in compressed real time")
    live.add_argument("--speed", type=float, default=30.0, help="time compression factor")
    live.add_argument("--week-index", type=int, default=6, help="progression week for weights")
    for sp in (backfill, live):
        sp.add_argument("--athletes", type=int, default=20)
        sp.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def run_backfill(args: argparse.Namespace, producer: TelemetryProducer) -> None:
    rng = random.Random(args.seed)
    athletes = make_athletes(args.athletes, rng)
    start = date.today() - timedelta(days=args.days)  # ends yesterday: no future event_time
    events = simulate_history(athletes, start, args.days, rng)
    log.info("backfill: %d events, %d athletes, %d days", len(events), len(athletes), args.days)
    for event in events:
        producer.send_event(event)


def build_live_timeline(args: argparse.Namespace) -> list:
    """Today's recovery + one session per athlete, all starting within the first 10 minutes."""
    rng = random.Random(args.seed)
    athletes = make_athletes(args.athletes, rng)
    today = date.today()
    base = datetime.now(timezone.utc)
    timeline = []
    for athlete in athletes:
        recovery, readiness = generate_recovery(athlete, today, rng)
        program = TRAINING_DAYS[rng.choice([0, 2, 4])]
        sets = generate_session(athlete, today, args.week_index, readiness, rng, program)
        shift = base + timedelta(seconds=rng.uniform(0, 600)) - sets[0].event_time
        timeline.append(recovery.model_copy(update={"event_time": base}))
        timeline.extend(s.model_copy(update={"event_time": s.event_time + shift}) for s in sets)
    timeline.sort(key=lambda e: e.event_time)
    return timeline


def run_live(args: argparse.Namespace, producer: TelemetryProducer) -> None:
    timeline = build_live_timeline(args)
    t0 = timeline[0].event_time
    wall0 = time.monotonic()
    log.info("live: %d events at %.0fx speed", len(timeline), args.speed)
    for event in timeline:
        due = (event.event_time - t0).total_seconds() / args.speed
        producer.wait(due - (time.monotonic() - wall0))
        # a real device stamps the moment it logs the set
        producer.send_event(event.model_copy(update={"event_time": datetime.now(timezone.utc)}))


def main() -> None:
    args = parse_args()
    settings = get_settings()
    setup_logging(settings.log_level)
    producer = TelemetryProducer(settings)
    try:
        run_backfill(args, producer) if args.mode == "backfill" else run_live(args, producer)
    except KeyboardInterrupt:
        log.warning("interrupted, flushing what is queued")
    finally:
        undelivered = producer.flush()
        log.info("delivered=%d failed=%d undelivered=%d", producer.delivered, producer.failed, undelivered)


if __name__ == "__main__":
    main()
