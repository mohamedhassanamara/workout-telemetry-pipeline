from datetime import UTC, datetime
from uuid import uuid4

import pytest

from telemetry.schemas.events import WorkoutSetLogged

BASE = datetime(2026, 1, 5, 18, 0, tzinfo=UTC)  # a Monday, safely in the past


@pytest.fixture
def make_set():
    def _make(**overrides) -> WorkoutSetLogged:
        fields = dict(
            athlete_id="ath_001",
            session_id=uuid4(),
            exercise="back_squat",
            set_number=1,
            reps=5,
            weight_kg=100.0,
            rpe=7.5,
            rest_seconds=150,
            event_time=BASE,
        )
        fields.update(overrides)
        return WorkoutSetLogged(**fields)

    return _make
