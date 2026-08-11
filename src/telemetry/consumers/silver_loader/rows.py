"""Pydantic event -> ClickHouse row. Column order must match the lists."""

from telemetry.schemas.events import DailyRecoveryLogged, WorkoutSetLogged

SET_COLUMNS = [
    "event_id",
    "schema_version",
    "athlete_id",
    "session_id",
    "exercise",
    "set_number",
    "reps",
    "weight_kg",
    "rpe",
    "rest_seconds",
    "event_time",
    "produced_at",
    "kafka_partition",
    "kafka_offset",
]
RECOVERY_COLUMNS = [
    "event_id",
    "schema_version",
    "athlete_id",
    "recovery_date",
    "sleep_hours",
    "sleep_quality",
    "resting_hr_bpm",
    "hrv_ms",
    "event_time",
    "produced_at",
    "kafka_partition",
    "kafka_offset",
]


def set_row(e: WorkoutSetLogged, partition: int, offset: int) -> list:
    return [
        e.event_id,
        e.schema_version,
        e.athlete_id,
        e.session_id,
        str(e.exercise),
        e.set_number,
        e.reps,
        e.weight_kg,
        e.rpe,
        e.rest_seconds,
        e.event_time,
        e.produced_at,
        partition,
        offset,
    ]


def recovery_row(e: DailyRecoveryLogged, partition: int, offset: int) -> list:
    return [
        e.event_id,
        e.schema_version,
        e.athlete_id,
        e.recovery_date,
        e.sleep_hours,
        e.sleep_quality,
        e.resting_hr_bpm,
        e.hrv_ms,
        e.event_time,
        e.produced_at,
        partition,
        offset,
    ]
