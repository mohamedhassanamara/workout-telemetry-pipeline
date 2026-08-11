"""Event contracts. Every event on the wire must parse into one of these models."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, TypeAdapter, field_validator

MAX_CLOCK_SKEW = timedelta(minutes=5)


def utc_now() -> datetime:
    return datetime.now(UTC)


class Exercise(StrEnum):
    BACK_SQUAT = "back_squat"
    BENCH_PRESS = "bench_press"
    DEADLIFT = "deadlift"
    OVERHEAD_PRESS = "overhead_press"
    BARBELL_ROW = "barbell_row"
    PULL_UP = "pull_up"


class EventBase(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    event_id: UUID = Field(default_factory=uuid4)
    schema_version: Literal[1] = 1
    athlete_id: str = Field(pattern=r"^ath_[0-9]{3}$")
    event_time: AwareDatetime  # when it happened (event time)
    produced_at: AwareDatetime = Field(default_factory=utc_now)  # when it was sent

    @field_validator("event_time")
    @classmethod
    def not_in_future(cls, v: datetime) -> datetime:
        if v > utc_now() + MAX_CLOCK_SKEW:
            raise ValueError("event_time is in the future")
        return v


class WorkoutSetLogged(EventBase):
    event_type: Literal["workout.set_logged"] = "workout.set_logged"
    session_id: UUID
    exercise: Exercise
    set_number: int = Field(ge=1, le=20)
    reps: int = Field(ge=0, le=50)
    weight_kg: float = Field(ge=0, le=500)
    rpe: float = Field(ge=1, le=10, multiple_of=0.5)  # rate of perceived exertion
    rest_seconds: int = Field(ge=0, le=1800)  # rest taken after this set


class DailyRecoveryLogged(EventBase):
    event_type: Literal["recovery.daily_logged"] = "recovery.daily_logged"
    recovery_date: date
    sleep_hours: float = Field(ge=0, le=16)
    sleep_quality: int = Field(ge=1, le=5)
    resting_hr_bpm: int = Field(ge=30, le=120)
    hrv_ms: float | None = Field(default=None, ge=5, le=250)


TelemetryEvent = Annotated[
    WorkoutSetLogged | DailyRecoveryLogged, Field(discriminator="event_type")
]
_adapter: TypeAdapter[WorkoutSetLogged | DailyRecoveryLogged] = TypeAdapter(TelemetryEvent)


def parse_event(raw: bytes | str) -> WorkoutSetLogged | DailyRecoveryLogged:
    """Raw JSON -> validated event. Raises pydantic.ValidationError on any contract violation."""
    return _adapter.validate_json(raw)


def serialize(event: WorkoutSetLogged | DailyRecoveryLogged) -> bytes:
    return event.model_dump_json().encode()
