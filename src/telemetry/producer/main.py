"""Simulator CLI: backfill history or stream live sessions into Redpanda, with optional chaos."""

from __future__ import annotations

import argparse
import logging
import random
import time
from collections import Counter
from datetime import UTC, date, datetime, timedelta

from telemetry.config import Settings, get_settings
from telemetry.log import setup_logging
from telemetry.producer.chaos import ChaosConfig, backdate, corrupt, split_late
from telemetry.producer.kafka_producer import TelemetryProducer
from telemetry.producer.simulator import (
    TRAINING_DAYS,
    generate_recovery,
    generate_session,
    make_athletes,
    simulate_history,
)
from telemetry.schemas.events import WorkoutSetLogged

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
        sp.add_argument("--malformed-rate", type=float, default=0.0)
        sp.add_argument("--duplicate-rate", type=float, default=0.0)
        sp.add_argument("--late-rate", type=float, default=0.0)
    return p.parse_args()


class Emitter:
    """Sends events, applying malformed/duplicate chaos and counting what it did."""

    def __init__(
        self, producer: TelemetryProducer, settings: Settings, cfg: ChaosConfig, seed: int
    ):
        self.producer, self.settings, self.cfg = producer, settings, cfg
        self.rng = random.Random(seed + 1)  # separate stream: chaos never changes the data
        self.stats: Counter[str] = Counter()

    def emit(self, event, late: bool = False) -> None:
        if isinstance(event, WorkoutSetLogged) and self.rng.random() < self.cfg.malformed_rate:
            kind, raw = corrupt(event, self.rng)
            headers = {"event_type": event.event_type, "x-chaos": kind}
            self.producer.send_raw(self.settings.topic_sets, event.athlete_id, raw, headers)
            self.stats[f"malformed:{kind}"] += 1
            return
        self.producer.send_event(event, {"x-chaos": "late"} if late else None)
        self.stats["late" if late else "clean"] += 1
        if self.rng.random() < self.cfg.duplicate_rate:
            self.producer.send_event(event, {"x-chaos": "duplicate"})
            self.stats["duplicate"] += 1


def run_backfill(args: argparse.Namespace, emitter: Emitter) -> None:
    rng = random.Random(args.seed)
    athletes = make_athletes(args.athletes, rng)
    start = date.today() - timedelta(days=args.days)  # ends yesterday: no future event_time
    events = simulate_history(athletes, start, args.days, rng)
    on_time, late = split_late(events, args.late_rate, emitter.rng)
    log.info("backfill: %d events (%d held back as late)", len(events), len(late))
    for event in on_time:
        emitter.emit(event)
    for event in late:
        emitter.emit(event, late=True)


def build_live_timeline(args: argparse.Namespace) -> list:
    """Today's recovery + one session per athlete, all starting within the first 10 minutes."""
    rng = random.Random(args.seed)
    athletes = make_athletes(args.athletes, rng)
    today = date.today()
    base = datetime.now(UTC)
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


def run_live(args: argparse.Namespace, emitter: Emitter) -> None:
    timeline = build_live_timeline(args)
    t0 = timeline[0].event_time
    wall0 = time.monotonic()
    log.info("live: %d events at %.0fx speed", len(timeline), args.speed)
    for event in timeline:
        due = (event.event_time - t0).total_seconds() / args.speed
        emitter.producer.wait(due - (time.monotonic() - wall0))
        stamped = event.model_copy(update={"event_time": datetime.now(UTC)})
        is_late = isinstance(event, WorkoutSetLogged) and emitter.rng.random() < args.late_rate
        emitter.emit(
            backdate(stamped, emitter.cfg, emitter.rng) if is_late else stamped, late=is_late
        )


def main() -> None:
    args = parse_args()
    settings = get_settings()
    setup_logging(settings.log_level)
    cfg = ChaosConfig(
        malformed_rate=args.malformed_rate,
        duplicate_rate=args.duplicate_rate,
        late_rate=args.late_rate,
    )
    producer = TelemetryProducer(settings)
    emitter = Emitter(producer, settings, cfg, args.seed)
    try:
        run_backfill(args, emitter) if args.mode == "backfill" else run_live(args, emitter)
    except KeyboardInterrupt:
        log.warning("interrupted, flushing what is queued")
    finally:
        undelivered = producer.flush()
        log.info("chaos summary: %s", dict(sorted(emitter.stats.items())))
        log.info(
            "delivered=%d failed=%d undelivered=%d",
            producer.delivered,
            producer.failed,
            undelivered,
        )


if __name__ == "__main__":
    main()
