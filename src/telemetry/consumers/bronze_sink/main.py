"""Bronze sink: archive every message, byte for byte, to S3. At-least-once."""
from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import time

from confluent_kafka import TIMESTAMP_NOT_AVAILABLE, Message, TopicPartition

from telemetry.config import Settings, get_settings
from telemetry.consumers.bronze_sink.batcher import TP, MicroBatcher, RawRecord
from telemetry.consumers.bronze_sink.keys import plan_objects
from telemetry.consumers.bronze_sink.s3_writer import BronzeWriter
from telemetry.consumers.common import (
    GracefulShutdown,
    build_consumer,
    fmt_partitions,
    headers_dict,
    real_messages,
)
from telemetry.log import setup_logging

log = logging.getLogger("bronze_sink")
GROUP_ID = "bronze-sink"


def to_record(msg: Message) -> RawRecord:
    ts_type, ts_ms = msg.timestamp()
    if ts_type == TIMESTAMP_NOT_AVAILABLE:
        ts_ms = int(time.time() * 1000)
    value = msg.value() or b""
    try:
        payload = {"payload": value.decode("utf-8")}
    except UnicodeDecodeError:
        payload = {"payload_b64": base64.b64encode(value).decode()}
    envelope = {
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "kafka_timestamp_ms": ts_ms,
        "key": msg.key().decode(errors="replace") if msg.key() else None,
        "headers": headers_dict(msg),
        **payload,
    }
    return RawRecord(msg.offset(), ts_ms, json.dumps(envelope, separators=(",", ":")).encode())


class BronzeSink:
    def __init__(self, settings: Settings, args: argparse.Namespace) -> None:
        self.settings = settings
        self.consumer = build_consumer(settings, GROUP_ID)
        self.batcher = MicroBatcher(args.max_records, args.max_bytes, args.max_age)
        self.writer = BronzeWriter(settings)
        self.crash_after_writes = args.crash_after_writes
        self.objects_written = 0

    def flush(self, only: set[TP] | None = None) -> None:
        drained = self.batcher.drain(only)
        if not drained:
            return
        commits = []
        for (topic, partition), records in drained.items():
            for key, recs in plan_objects(topic, partition, records):
                size = self.writer.write(key, recs)
                self.objects_written += 1
                log.info("wrote %s (%d records, %d bytes)", key, len(recs), size)
                if self.crash_after_writes and self.objects_written >= self.crash_after_writes:
                    log.error("FAULT INJECTION: hard exit after write, before commit")
                    os._exit(1)
            commits.append(TopicPartition(topic, partition, records[-1].offset + 1))
        # only now is the data durable -> safe to move the committed position
        self.consumer.commit(offsets=commits, asynchronous=False)
        log.info("committed %s", fmt_partitions(commits))

    def run(self) -> None:
        topics = [self.settings.topic_sets, self.settings.topic_recovery, self.settings.topic_dlq]
        self.consumer.subscribe(topics)
        shutdown = GracefulShutdown()
        try:
            while not shutdown.requested:
                for msg in real_messages(self.consumer.consume(num_messages=500, timeout=1.0)):
                    self.batcher.add((msg.topic(), msg.partition()), to_record(msg))
                if self.batcher.should_flush():
                    self.flush()
            self.flush()  # drain what is left on a clean shutdown
        finally:
            self.consumer.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--max-records", type=int, default=5000)
    p.add_argument("--max-bytes", type=int, default=5_000_000)
    p.add_argument("--max-age", type=float, default=30.0, help="seconds")
    p.add_argument("--crash-after-writes", type=int, default=0, help="fault injection")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    setup_logging(settings.log_level)
    BronzeSink(settings, args).run()


if __name__ == "__main__":
    main()
