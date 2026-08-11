"""Fatigue processor: event-time session windows over workout sets -> ClickHouse gold."""

from __future__ import annotations

import argparse
import logging
import time
from collections import Counter
from datetime import timedelta

from confluent_kafka import Message, TopicPartition
from pydantic import ValidationError

from telemetry.clients import make_clickhouse_client
from telemetry.config import Settings, get_settings
from telemetry.consumers.common import (
    GracefulShutdown,
    build_consumer,
    fmt_partitions,
    real_messages,
)
from telemetry.dlq.publisher import DeadLetterPublisher
from telemetry.log import setup_logging
from telemetry.processing.fatigue_windows.windows import (
    FATIGUE_COLUMNS,
    PartitionState,
    SessionWindower,
    summarize,
)
from telemetry.schemas.events import WorkoutSetLogged, parse_event

log = logging.getLogger("fatigue_windows")
GROUP_ID = "fatigue-windows"
FLUSH_EVERY_S = 5.0


class FatigueProcessor:
    def __init__(self, settings: Settings, args: argparse.Namespace) -> None:
        self.settings = settings
        self.topic = settings.topic_sets
        self.consumer = build_consumer(settings, GROUP_ID)
        self.ch = make_clickhouse_client(settings)
        self.windower = SessionWindower(
            gap=timedelta(minutes=args.gap_minutes),
            allowed_lateness=timedelta(minutes=args.lateness_minutes),
        )
        self.idle_seconds = args.idle_seconds
        self.states: dict[int, PartitionState] = {}
        self.next_offsets: dict[int, int] = {}
        self.results: list[list] = []
        self.stats: Counter[str] = Counter()
        self.last_flush = time.monotonic()
        self.dlq = DeadLetterPublisher(settings, GROUP_ID)

    def handle(self, msg: Message) -> None:
        p = msg.partition()
        state = self.states.setdefault(p, PartitionState())
        state.last_seen_wall = time.monotonic()
        self.next_offsets[p] = msg.offset() + 1
        try:
            event = parse_event(msg.value() or b"")
        except ValidationError:
            self.stats["invalid_skipped"] += 1  # the silver loader owns invalid events
            return
        if not isinstance(event, WorkoutSetLogged):
            return
        if self.windower.add(state, event, msg.offset()):
            self.stats["accepted"] += 1
        else:
            self.on_late(msg, event, state)
        for window in self.windower.close_ready(state):
            self.results.append(summarize(window, "watermark"))

    def on_late(self, msg: Message, event: WorkoutSetLogged, state: PartitionState) -> None:
        self.stats["late"] += 1
        wm = self.windower.watermark(state)
        error = (
            f"event_time {event.event_time.isoformat()} is older than watermark "
            f"{wm.isoformat()} minus allowed lateness {self.windower.allowed_lateness}"
        )
        self.dlq.publish(msg, reason="late_event", error=error)

    def close_idle(self) -> None:
        now = time.monotonic()
        for state in self.states.values():
            if state.windows and now - state.last_seen_wall >= self.idle_seconds:
                for window in self.windower.close_all(state):
                    self.results.append(summarize(window, "idle"))

    def flush(self) -> None:
        if self.results:
            self.ch.insert("gold_session_fatigue", self.results, column_names=FATIGUE_COLUMNS)
            self.stats["windows_emitted"] += len(self.results)
            log.info("emitted %d windows, totals %s", len(self.results), dict(self.stats))
            self.results = []
        offsets = [
            TopicPartition(self.topic, p, self.windower.safe_commit_offset(self.states[p], nxt))
            for p, nxt in self.next_offsets.items()
        ]
        if offsets:
            self.dlq.flush()
            self.consumer.commit(offsets=offsets, asynchronous=False)
        self.last_flush = time.monotonic()

    def _forget(self, partitions) -> None:
        for tp in partitions:
            self.states.pop(tp.partition, None)
            self.next_offsets.pop(tp.partition, None)

    def _on_revoke(self, consumer, partitions) -> None:
        log.info("revoked %s", fmt_partitions(partitions))
        self.flush()
        self._forget(partitions)  # the new owner rebuilds state by replaying from our commit

    def _on_lost(self, consumer, partitions) -> None:
        log.warning("LOST %s", fmt_partitions(partitions))
        self._forget(partitions)

    def run(self) -> None:
        self.consumer.subscribe(
            [self.topic],
            on_assign=lambda c, ps: log.info("assigned %s", fmt_partitions(ps)),
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )
        shutdown = GracefulShutdown()
        try:
            while not shutdown.requested:
                for msg in real_messages(self.consumer.consume(num_messages=500, timeout=1.0)):
                    self.handle(msg)
                self.close_idle()
                if time.monotonic() - self.last_flush >= FLUSH_EVERY_S:
                    self.flush()
            self.flush()
        finally:
            self.consumer.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--gap-minutes", type=float, default=30)
    p.add_argument("--lateness-minutes", type=float, default=10)
    p.add_argument("--idle-seconds", type=float, default=60)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    setup_logging(settings.log_level)
    FatigueProcessor(settings, args).run()


if __name__ == "__main__":
    main()
