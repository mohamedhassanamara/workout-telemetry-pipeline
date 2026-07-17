"""All runtime configuration, read from TELEMETRY_* environment variables (or .env)."""
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="TELEMETRY_", extra="ignore")

    # Redpanda
    kafka_bootstrap: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:18081"
    topic_sets: str = "workout.sets.v1"
    topic_recovery: str = "recovery.daily.v1"
    topic_dlq: str = "workout.dlq.v1"

    # LocalStack S3
    s3_endpoint: str = "http://localhost:4566"
    s3_bucket: str = "telemetry-bronze"
    aws_region: str = "us-east-1"
    aws_access_key_id: str = "test"
    aws_secret_access_key: str = "test"

    # ClickHouse (used from Step 13)
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "telemetry"
    clickhouse_password: str = "telemetry"
    clickhouse_database: str = "telemetry"

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
