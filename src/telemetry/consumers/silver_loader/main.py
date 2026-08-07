"""Silver loader: validate, batch-insert into ClickHouse, DLQ bad data, then commit."""
from __future__ import annotations

import logging
import time
import traceback
from collections import Counter

from clickhouse_connect.driver.exceptions import OperationalError
from confluent_kafka import Message
from pydantic import ValidationError

from telemetry.clients import make_clickhouse_client
from telemetry.config import Settings, get_settings
from telemetry.consumers.common import (
    GracefulShutdown,
    build_consumer,
    fmt_partitions,
    real_messages,
)
from telemetry.consumers.silver_loader.rows import (
    RECOVERY_COLUMNS,
    SET_COLUMNS,
    recovery_row,
    set_row,
)
from telemetry.dlq.publisher import DeadLetterPublisher, describe_validation_error
from telemetry.log import setup_logging
from telemetry.retry import retry
from telemetry.schemas.events import WorkoutSetLogged, parse_event

log = logging.getLogger("silver_loader")
GROUP_ID = "silver-loader"
MAX_MESSAGES = 2000
MAX_AGE_S = 5.0
TRANSIENT = (OperationalError, ConnectionError, TimeoutError)


class SilverLoader:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.consumer = build_consumer(settings, GROUP_ID)
        self.ch = make_clickhouse_client(settings)
        self.dlq = DeadLetterPublisher(settings, GROUP_ID)
        self.set_rows: list[list] = []
        self.recovery_rows: list[list] = []
        self.pending = 0
        self.batch_started: float | None = None
        self.stats: Counter[str] = Counter()

    def handle(self, msg: Message) -> None:
        if self.batch_started is None:
            self.batch_started = time.monotonic()
        self.pending += 1
        try:
            event = parse_event(msg.value() or b"")
        except ValidationError as exc:
            self.stats["invalid"] += 1
            self.dlq.publish(msg, "validation_error", describe_validation_error(exc))
            return
        try:
            if isinstance(event, WorkoutSetLogged):
                self.set_rows.append(set_row(event, msg.partition(), msg.offset()))
            else:
                self.recovery_rows.append(recovery_row(event, msg.partition(), msg.offset()))
        except Exception:  # no I/O here: any failure is deterministic -> poison message
            self.stats["processing_error"] += 1
            log.exception("processing error at %s[%d]@%d", msg.topic(), msg.partition(), msg.offset())
            self.dlq.publish(msg, "processing_error", traceback.format_exc(limit=3))

    def should_flush(self) -> bool:
        if self.batch_started is None:
            return False
        return self.pending >= MAX_MESSAGES or time.monotonic() - self.batch_started >= MAX_AGE_S

    def _insert(self, table: str, rows: list[list], columns: list[str]) -> None:
        retry(
            lambda: self.ch.insert(table, rows, column_names=columns),
            retry_on=TRANSIENT,
            what=f"insert into {table}",
        )

    def flush(self) -> None:
        if self.pending == 0:
            return
        if self.set_rows:
            self._insert("silver_workout_sets", self.set_rows, SET_COLUMNS)
        if self.recovery_rows:
            self._insert("silver_daily_recovery", self.recovery_rows, RECOVERY_COLUMNS)
        self.dlq.flush()  # bad messages durable in the DLQ before we move on
        self.consumer.commit(asynchronous=False)  # only after both outputs are durable
        self.stats["sets"] += len(self.set_rows)
        self.stats["recovery"] += len(self.recovery_rows)
        log.info("flushed %d messages, totals %s", self.pending, dict(self.stats))
        self.set_rows, self.recovery_rows = [], []
        self.pending, self.batch_started = 0, None

    def _on_revoke(self, consumer, partitions) -> None:
        log.info("revoked %s", fmt_partitions(partitions))
        self.flush()

    def _on_lost(self, consumer, partitions) -> None:
        log.warning("LOST %s, dropping the uncommitted batch", fmt_partitions(partitions))
        self.set_rows, self.recovery_rows = [], []
        self.pending, self.batch_started = 0, None

    def run(self) -> None:
        self.consumer.subscribe(
            [self.settings.topic_sets, self.settings.topic_recovery],
            on_assign=lambda c, ps: log.info("assigned %s", fmt_partitions(ps)),
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )
        shutdown = GracefulShutdown()
        try:
            while not shutdown.requested:
                for msg in real_messages(self.consumer.consume(num_messages=500, timeout=1.0)):
                    self.handle(msg)
                if self.should_flush():
                    self.flush()
            self.flush()
        finally:
            self.consumer.close()


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    SilverLoader(settings).run()


if __name__ == "__main__":
    main()
