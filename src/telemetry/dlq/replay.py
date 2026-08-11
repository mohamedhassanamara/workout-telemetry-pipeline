"""Re-publish DLQ messages of one reason to their source topic. Dry run unless --execute."""

from __future__ import annotations

import argparse
import logging
import time

from confluent_kafka import Consumer, Producer

from telemetry.config import get_settings
from telemetry.consumers.common import headers_dict, real_messages
from telemetry.log import setup_logging

log = logging.getLogger("dlq_replay")
IDLE_STOP_S = 10.0


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--reason", required=True, help="validation_error | processing_error | late_event"
    )
    p.add_argument(
        "--error-contains", default=None, help="only messages whose dlq.error contains this"
    )
    p.add_argument("--max-attempts", type=int, default=3)
    p.add_argument("--execute", action="store_true", help="actually re-publish and commit")
    args = p.parse_args()
    s = get_settings()
    setup_logging(s.log_level)

    consumer = Consumer(
        {
            "bootstrap.servers": s.kafka_bootstrap,
            "group.id": f"dlq-replayer-{args.reason}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer(
        {"bootstrap.servers": s.kafka_bootstrap, "enable.idempotence": True, "acks": "all"}
    )
    consumer.subscribe([s.topic_dlq])
    replayed = parked = skipped = 0
    last_message = time.monotonic()
    try:
        while time.monotonic() - last_message < IDLE_STOP_S:
            msgs = real_messages(consumer.consume(num_messages=200, timeout=1.0))
            if msgs:
                last_message = time.monotonic()
            for msg in msgs:
                h = headers_dict(msg)
                if h.get("dlq.reason") != args.reason or (
                    args.error_contains and args.error_contains not in h.get("dlq.error", "")
                ):
                    skipped += 1
                    continue
                attempts = int(h.get("dlq.attempts", "1"))
                if attempts >= args.max_attempts:
                    parked += 1
                    log.warning("parked (attempts=%d): %s", attempts, h.get("dlq.error", "")[:120])
                    continue
                target = h["dlq.source.topic"]
                headers = [(k, v) for k, v in (msg.headers() or []) if not k.startswith("dlq.")]
                headers += [
                    ("dlq.attempts", str(attempts)),
                    ("dlq.replayed_from", f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"),
                ]
                replayed += 1
                if args.execute:
                    producer.produce(target, key=msg.key(), value=msg.value(), headers=headers)
                    producer.poll(0)
                else:
                    log.info("DRY RUN would replay to %s: %s", target, h.get("dlq.error", "")[:120])
        if args.execute:
            if producer.flush(30):
                raise RuntimeError("replay produce did not complete; not committing")
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
    mode = "EXECUTED" if args.execute else "DRY RUN"
    log.info("%s: replayed=%d parked=%d skipped_other=%d", mode, replayed, parked, skipped)


if __name__ == "__main__":
    main()
