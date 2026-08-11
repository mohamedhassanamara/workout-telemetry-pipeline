"""Dead-letter publisher: original bytes untouched, failure context in headers."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from confluent_kafka import KafkaError, Message, Producer
from pydantic import ValidationError

from telemetry.config import Settings

log = logging.getLogger(__name__)
MAX_ERROR_BYTES = 1024


def describe_validation_error(exc: ValidationError) -> str:
    return "; ".join(f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()[:5])


class DeadLetterPublisher:
    def __init__(self, settings: Settings, source_group: str) -> None:
        self.topic = settings.topic_dlq
        self.source_group = source_group
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "client.id": f"{source_group}-dlq",
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 20,
            }
        )
        self.published = 0
        self.failed = 0

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.failed += 1
            log.error("DLQ delivery failed: %s", err)
        else:
            self.published += 1

    def publish(self, msg: Message, reason: str, error: str) -> None:
        original = list(msg.headers() or [])
        prior_attempts = next((int(v) for k, v in original if k == "dlq.attempts" and v), 0)
        headers = [(k, v) for k, v in original if not k.startswith("dlq.")] + [
            ("dlq.reason", reason),
            ("dlq.error", error.encode()[:MAX_ERROR_BYTES]),
            ("dlq.source.topic", msg.topic()),
            ("dlq.source.partition", str(msg.partition())),
            ("dlq.source.offset", str(msg.offset())),
            ("dlq.consumer.group", self.source_group),
            ("dlq.failed_at", datetime.now(UTC).isoformat()),
            ("dlq.attempts", str(prior_attempts + 1)),
        ]
        while True:
            try:
                self._producer.produce(
                    self.topic,
                    key=msg.key(),
                    value=msg.value(),
                    headers=headers,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                self._producer.poll(0.5)
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> None:
        """Raise unless every DLQ message so far is durable. Call BEFORE committing offsets."""
        remaining = self._producer.flush(timeout)
        if remaining or self.failed:
            raise RuntimeError(
                f"DLQ publish incomplete: undelivered={remaining} failed={self.failed}"
            )
