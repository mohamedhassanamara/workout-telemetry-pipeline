"""End to end: producer -> Redpanda -> silver loader -> ClickHouse + DLQ. Needs `make up`."""

import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime

import pytest
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition

from telemetry.clients import make_clickhouse_client
from telemetry.config import get_settings
from telemetry.consumers.common import headers_dict
from telemetry.producer.kafka_producer import TelemetryProducer
from telemetry.schemas.events import WorkoutSetLogged, serialize

pytestmark = pytest.mark.integration


def wait_for(condition, timeout: float = 90, every: float = 1.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(every)
    raise AssertionError("condition not met before timeout")


@pytest.fixture(scope="module")
def silver_loader():
    proc = subprocess.Popen([sys.executable, "-m", "telemetry.consumers.silver_loader.main"])
    yield proc
    proc.terminate()  # SIGTERM -> graceful shutdown
    proc.wait(timeout=30)


def dlq_contains(run_id: str) -> bool:
    s = get_settings()
    consumer = Consumer(
        {
            "bootstrap.servers": s.kafka_bootstrap,
            "group.id": f"it-{run_id}",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
    )
    partitions = consumer.list_topics(s.topic_dlq, timeout=10).topics[s.topic_dlq].partitions
    consumer.assign([TopicPartition(s.topic_dlq, p, OFFSET_BEGINNING) for p in partitions])
    remaining, found = len(partitions), False
    try:
        while remaining and not found:
            msg = consumer.poll(5.0)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    remaining -= 1
                continue
            h = headers_dict(msg)
            found = h.get("x-test-run") == run_id and h.get("dlq.reason") == "validation_error"
    finally:
        consumer.close()
    return found


def test_valid_duplicate_and_malformed_land_in_the_right_place(silver_loader):
    s = get_settings()
    run_id = uuid.uuid4().hex
    event = WorkoutSetLogged(
        athlete_id="ath_999",
        session_id=uuid.uuid4(),
        exercise="deadlift",
        set_number=1,
        reps=3,
        weight_kg=180,
        rpe=8.5,
        rest_seconds=200,
        event_time=datetime.now(UTC),
    )
    producer = TelemetryProducer(s, client_id="integration-test")
    producer.send_event(event, {"x-test-run": run_id})
    producer.send_event(event, {"x-test-run": run_id})  # duplicate
    bad = serialize(event).replace(b'"reps":3', b'"reps":-3')
    producer.send_raw(s.topic_sets, event.athlete_id, bad, {"x-test-run": run_id})
    assert producer.flush() == 0

    ch = make_clickhouse_client(s)
    params = {"sid": event.session_id}

    def unique_rows():
        return ch.query(
            "SELECT uniqExact(event_id) FROM silver_workout_sets WHERE session_id = {sid:UUID}",
            parameters=params,
        ).result_rows[0][0]

    assert wait_for(unique_rows) == 1
    final_rows = ch.query(
        "SELECT count() FROM silver_workout_sets FINAL WHERE session_id = {sid:UUID}",
        parameters=params,
    ).result_rows[0][0]
    assert final_rows == 1  # the duplicate collapsed
    assert wait_for(lambda: dlq_contains(run_id))  # the malformed copy is in the DLQ
