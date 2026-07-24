"""Shared consumer plumbing: config, graceful shutdown, message helpers."""
from __future__ import annotations

import logging
import signal

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition

from telemetry.config import Settings

log = logging.getLogger(__name__)


def build_consumer(settings: Settings, group_id: str, **overrides) -> Consumer:
    config = {
        "bootstrap.servers": settings.kafka_bootstrap,
        "group.id": group_id,
        "client.id": group_id,
        "enable.auto.commit": False,  # commit only after the work is durable
        "auto.offset.reset": "earliest",  # a new group starts from the oldest message
        "partition.assignment.strategy": "cooperative-sticky",
        "session.timeout.ms": 45000,
        "max.poll.interval.ms": 300000,
        **overrides,
    }
    return Consumer(config)


class GracefulShutdown:
    """Turns SIGINT/SIGTERM into a flag the poll loop checks."""

    def __init__(self) -> None:
        self.requested = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum, frame) -> None:
        log.info("shutdown requested (signal %s), finishing current batch", signum)
        self.requested = True


def real_messages(messages: list[Message]) -> list[Message]:
    """Drop non-fatal client errors (logged); raise on fatal ones."""
    out = []
    for msg in messages:
        err = msg.error()
        if err is None:
            out.append(msg)
        elif err.fatal():
            raise KafkaException(err)
        else:
            log.warning("consumer error: %s", err)
    return out


def headers_dict(msg: Message) -> dict[str, str]:
    return {
        k: (v.decode(errors="replace") if v is not None else "") for k, v in (msg.headers() or [])
    }


def fmt_partitions(partitions: list[TopicPartition]) -> list[str]:
    return sorted(f"{p.topic}[{p.partition}]" for p in partitions)
