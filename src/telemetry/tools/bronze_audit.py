"""Reconcile bronze with the broker: every retained offset must exist in S3 at least once."""

import gzip
import json
from collections import Counter

from confluent_kafka import Consumer, TopicPartition

from telemetry.clients import make_s3_client
from telemetry.config import get_settings


def main() -> None:
    s = get_settings()
    s3 = make_s3_client(s)
    seen: Counter[tuple[str, int, int]] = Counter()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=s.s3_bucket, Prefix="topic="):
        for obj in page.get("Contents", []):
            body = s3.get_object(Bucket=s.s3_bucket, Key=obj["Key"])["Body"].read()
            for line in gzip.decompress(body).splitlines():
                rec = json.loads(line)
                seen[(rec["topic"], rec["partition"], rec["offset"])] += 1

    probe = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": "bronze-audit"})
    metadata = probe.list_topics(timeout=10)
    total_missing = total_dupes = 0
    for topic in (s.topic_sets, s.topic_recovery, s.topic_dlq):
        for p in sorted(metadata.topics[topic].partitions):
            low, high = probe.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            counts = {o: c for (t, pp, o), c in seen.items() if t == topic and pp == p}
            missing = [o for o in range(low, high) if o not in counts]
            dupes = sum(c - 1 for c in counts.values())
            total_missing += len(missing)
            total_dupes += dupes
            print(f"{topic}[{p}] offsets={high - low} missing={len(missing)} duplicated={dupes}")
    probe.close()
    print(f"TOTAL missing={total_missing} duplicated={total_dupes}")


if __name__ == "__main__":
    main()
