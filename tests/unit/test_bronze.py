from telemetry.consumers.bronze_sink.batcher import MicroBatcher, RawRecord
from telemetry.consumers.bronze_sink.keys import plan_objects

T0 = 1_760_000_000_000  # 2025-10-09 08:53:20 UTC in ms


def test_flush_on_record_count():
    b = MicroBatcher(max_records=2, max_age_s=999, clock=lambda: 0.0)
    b.add(("t", 0), RawRecord(1, T0, b"a"))
    assert not b.should_flush()
    b.add(("t", 0), RawRecord(2, T0, b"b"))
    assert b.should_flush()


def test_flush_on_age():
    now = [0.0]
    b = MicroBatcher(max_records=999, max_age_s=10, clock=lambda: now[0])
    b.add(("t", 0), RawRecord(1, T0, b"a"))
    now[0] = 10.0
    assert b.should_flush()


def test_drain_only_selected_partitions():
    b = MicroBatcher()
    b.add(("t", 0), RawRecord(1, T0, b"a"))
    b.add(("t", 1), RawRecord(1, T0, b"b"))
    assert list(b.drain(only={("t", 1)})) == [("t", 1)]
    assert b.record_count == 1


def test_keys_split_by_hour_and_carry_offsets():
    recs = [
        RawRecord(100, T0, b"a"),
        RawRecord(101, T0, b"b"),
        RawRecord(102, T0 + 3_600_000, b"c"),
    ]
    plan = plan_objects("workout.sets.v1", 3, recs)
    assert [k for k, _ in plan] == [
        "topic=workout.sets.v1/ingest_date=2025-10-09/hour=08/partition=03/offsets-000000000100-000000000101.jsonl.gz",
        "topic=workout.sets.v1/ingest_date=2025-10-09/hour=09/partition=03/offsets-000000000102-000000000102.jsonl.gz",
    ]


def test_keys_are_deterministic():
    recs = [RawRecord(5, T0, b"a")]
    assert plan_objects("t", 0, recs) == plan_objects("t", 0, recs)
