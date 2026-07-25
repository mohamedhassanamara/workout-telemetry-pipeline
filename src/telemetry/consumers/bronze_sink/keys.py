"""Deterministic, Hive-partitioned S3 object keys for the bronze layer."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from telemetry.consumers.bronze_sink.batcher import RawRecord


def object_key(
    topic: str, partition: int, ingest_date: str, hour: str, first_offset: int, last_offset: int
) -> str:
    return (
        f"topic={topic}/ingest_date={ingest_date}/hour={hour}/partition={partition:02d}/"
        f"offsets-{first_offset:012d}-{last_offset:012d}.jsonl.gz"
    )


def plan_objects(
    topic: str, partition: int, records: list[RawRecord]
) -> list[tuple[str, list[RawRecord]]]:
    """Split one partition's batch by ingestion hour -> [(object key, records)]."""
    groups: dict[tuple[str, str], list[RawRecord]] = defaultdict(list)
    for r in records:
        ts = datetime.fromtimestamp(r.timestamp_ms / 1000, tz=timezone.utc)
        groups[(ts.strftime("%Y-%m-%d"), ts.strftime("%H"))].append(r)
    return [
        (object_key(topic, partition, day, hour, recs[0].offset, recs[-1].offset), recs)
        for (day, hour), recs in sorted(groups.items())
    ]
