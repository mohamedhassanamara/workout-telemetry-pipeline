import random
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from telemetry.producer.chaos import corrupt
from telemetry.schemas.events import DailyRecoveryLogged, parse_event, serialize


def test_round_trip(make_set):
    event = make_set()
    assert parse_event(serialize(event)) == event


def test_discriminator_picks_recovery_model():
    rec = DailyRecoveryLogged(
        athlete_id="ath_002",
        event_time=datetime(2026, 1, 5, 7, tzinfo=UTC),
        recovery_date="2026-01-05",
        sleep_hours=7.2,
        sleep_quality=4,
        resting_hr_bpm=55,
    )
    assert isinstance(parse_event(serialize(rec)), DailyRecoveryLogged)


@pytest.mark.parametrize(
    "field,value",
    [("reps", -1), ("rpe", 7.3), ("rpe", 11), ("athlete_id", "athlete-1"), ("weight_kg", 900)],
)
def test_rejects_out_of_contract_values(make_set, field, value):
    with pytest.raises(ValidationError):
        make_set(**{field: value})


def test_rejects_naive_timestamp(make_set):
    with pytest.raises(ValidationError):
        make_set(event_time=datetime(2026, 1, 5, 18, 0))


def test_rejects_future_event_time(make_set):
    with pytest.raises(ValidationError):
        make_set(event_time=datetime.now(UTC) + timedelta(hours=1))


def test_rejects_unknown_field(make_set):
    raw = serialize(make_set()).replace(b"{", b'{"mood":"great",', 1)
    with pytest.raises(ValidationError):
        parse_event(raw)


@pytest.mark.parametrize("seed", range(40))
def test_every_chaos_corruption_is_rejected(make_set, seed):
    _, raw = corrupt(make_set(), random.Random(seed))
    with pytest.raises(ValidationError):
        parse_event(raw)
