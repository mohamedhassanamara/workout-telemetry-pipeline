"""Read the whole DLQ (no commits, no side effects) and summarise it."""
from __future__ import annotations

import argparse
from collections import Counter

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, TopicPartition

from telemetry.config import get_settings
from telemetry.consumers.common import headers_dict


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--samples", type=int, default=2, help="examples to print per reason")
    args = p.parse_args()
    s = get_settings()

    consumer = Consumer(
        {
            "bootstrap.servers": s.kafka_bootstrap,
            "group.id": "dlq-inspector",  # required by the client; never committed
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
    )
    partitions = consumer.list_topics(s.topic_dlq, timeout=10).topics[s.topic_dlq].partitions
    consumer.assign([TopicPartition(s.topic_dlq, p, OFFSET_BEGINNING) for p in partitions])

    by_reason: Counter[tuple[str, str]] = Counter()
    by_label: Counter[tuple[str, str]] = Counter()
    by_attempts: Counter[str] = Counter()
    samples: dict[str, list[str]] = {}
    remaining = len(partitions)
    while remaining:
        msg = consumer.poll(5.0)
        if msg is None:
            break
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                remaining -= 1
                continue
            raise KafkaException(msg.error())
        h = headers_dict(msg)
        reason = h.get("dlq.reason", "?")
        by_reason[(h.get("dlq.consumer.group", "?"), reason)] += 1
        by_label[(reason, h.get("x-chaos", "-"))] += 1
        by_attempts[h.get("dlq.attempts", "?")] += 1
        if len(samples.setdefault(reason, [])) < args.samples:
            src = f"{h.get('dlq.source.topic')}[{h.get('dlq.source.partition')}]@{h.get('dlq.source.offset')}"
            samples[reason].append(f"{src}  {h.get('dlq.error', '')[:160]}")
    consumer.close()

    print("== by consumer group and reason")
    for (group, reason), n in sorted(by_reason.items()):
        print(f"{group:18} {reason:18} {n}")
    print("== by reason and chaos label")
    for (reason, label), n in sorted(by_label.items()):
        print(f"{reason:18} {label:18} {n}")
    print("== by attempts:", dict(sorted(by_attempts.items())))
    for reason, lines in samples.items():
        print(f"== samples: {reason}")
        for line in lines:
            print("  ", line)


if __name__ == "__main__":
    main()
