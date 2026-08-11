import random
from datetime import date

from telemetry.producer.simulator import generate_session, make_athletes, simulate_history
from telemetry.schemas.events import WorkoutSetLogged

START = date(2026, 1, 5)  # Monday


def run(seed: int, athletes: int = 3, days: int = 14):
    rng = random.Random(seed)
    return simulate_history(make_athletes(athletes, rng), START, days, rng)


def test_same_seed_gives_identical_events():
    assert [e.event_id for e in run(7)] == [e.event_id for e in run(7)]


def test_event_counts_for_two_weeks():
    events = run(42)
    sets = [e for e in events if isinstance(e, WorkoutSetLogged)]
    assert len(events) == 258 and len(sets) == 216


def test_events_are_sorted_by_event_time():
    times = [e.event_time for e in run(3)]
    assert times == sorted(times)


def test_rest_day_has_no_session():
    rng = random.Random(1)
    athlete = make_athletes(1, rng)[0]
    tuesday = date(2026, 1, 6)
    assert generate_session(athlete, tuesday, 0, 1.0, rng) == []


def test_weights_progress_over_weeks():
    events = [e for e in run(5, athletes=1, days=42) if isinstance(e, WorkoutSetLogged)]
    squats = [e for e in events if e.exercise == "back_squat"]
    assert squats[-1].weight_kg > squats[0].weight_kg
