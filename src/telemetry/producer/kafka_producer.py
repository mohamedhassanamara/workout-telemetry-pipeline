"""Thin, reliable wrapper around confluent_kafka.Producer."""

from __future__ import annotations

import logging
import time

from confluent_kafka import KafkaError, Message, Producer

from telemetry.config import Settings
from telemetry.schemas.events import DailyRecoveryLogged, WorkoutSetLogged, serialize

log = logging.getLogger(__name__)


def topic_for(event: WorkoutSetLogged | DailyRecoveryLogged, settings: Settings) -> str:
    if isinstance(event, WorkoutSetLogged):
        return settings.topic_sets
    if isinstance(event, DailyRecoveryLogged):
        return settings.topic_recovery
    raise TypeError(f"no topic for {type(event).__name__}")


class TelemetryProducer:
    def __init__(self, settings: Settings, client_id: str = "telemetry-simulator") -> None:
        self._settings = settings
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "client.id": client_id,
                "enable.idempotence": True,  # implies retries + ordering safety
                "acks": "all",
                "compression.type": "zstd",
                "linger.ms": 20,
                "batch.size": 131072,
            }
        )
        self.delivered = 0
        self.failed = 0

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed topic=%s key=%s: %s", msg.topic(), msg.key(), err)
        else:
            self.delivered += 1

    def send_raw(
        self, topic: str, key: str, value: bytes, headers: dict[str, str] | None = None
    ) -> None:
        while True:
            try:
                self._producer.produce(
                    topic,
                    key=key.encode(),
                    value=value,
                    headers=list((headers or {}).items()),
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:  # local queue full: serve callbacks, then retry
                self._producer.poll(0.5)
        self._producer.poll(0)

    def send_event(
        self,
        event: WorkoutSetLogged | DailyRecoveryLogged,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        headers = {
            "event_type": event.event_type,
            "schema_version": str(event.schema_version),
            "content-type": "application/json",
            **(extra_headers or {}),
        }
        self.send_raw(topic_for(event, self._settings), event.athlete_id, serialize(event), headers)

    def wait(self, seconds: float) -> None:
        """Sleep while still serving delivery callbacks."""
        deadline = time.monotonic() + seconds
        while (left := deadline - time.monotonic()) > 0:
            self._producer.poll(min(left, 0.5))

    def flush(self, timeout: float = 30.0) -> int:
        """Block until queued messages are delivered. Returns how many are still undelivered."""
        return self._producer.flush(timeout)
