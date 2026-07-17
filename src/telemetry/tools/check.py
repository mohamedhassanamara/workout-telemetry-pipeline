"""Connectivity check from the host: broker, schema registry, S3."""
import httpx
from confluent_kafka.admin import AdminClient

from telemetry.clients import make_s3_client
from telemetry.config import get_settings


def main() -> None:
    s = get_settings()

    admin = AdminClient({"bootstrap.servers": s.kafka_bootstrap})
    metadata = admin.list_topics(timeout=10)
    topics = sorted(t for t in metadata.topics if not t.startswith("_"))
    print(f"kafka   OK  brokers={len(metadata.brokers)} topics={topics}")

    resp = httpx.get(f"{s.schema_registry_url}/subjects", timeout=5)
    resp.raise_for_status()
    print(f"schema  OK  subjects={resp.json()}")

    make_s3_client(s).head_bucket(Bucket=s.s3_bucket)
    print(f"s3      OK  bucket={s.s3_bucket}")


if __name__ == "__main__":
    main()
