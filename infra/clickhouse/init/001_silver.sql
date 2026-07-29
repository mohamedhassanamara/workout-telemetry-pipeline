CREATE DATABASE IF NOT EXISTS telemetry;

CREATE TABLE IF NOT EXISTS telemetry.silver_workout_sets
(
    event_id        UUID,
    schema_version  UInt8,
    athlete_id      LowCardinality(String),
    session_id      UUID,
    exercise        LowCardinality(String),
    set_number      UInt8,
    reps            UInt8,
    weight_kg       Float32,
    rpe             Float32,
    rest_seconds    UInt16,
    event_time      DateTime64(3, 'UTC'),
    produced_at     DateTime64(3, 'UTC'),
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3),
    kafka_partition UInt16,
    kafka_offset    UInt64
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (athlete_id, event_time, event_id);

CREATE TABLE IF NOT EXISTS telemetry.silver_daily_recovery
(
    event_id        UUID,
    schema_version  UInt8,
    athlete_id      LowCardinality(String),
    recovery_date   Date,
    sleep_hours     Float32,
    sleep_quality   UInt8,
    resting_hr_bpm  UInt8,
    hrv_ms          Nullable(Float32),
    event_time      DateTime64(3, 'UTC'),
    produced_at     DateTime64(3, 'UTC'),
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3),
    kafka_partition UInt16,
    kafka_offset    UInt64
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (athlete_id, recovery_date);
