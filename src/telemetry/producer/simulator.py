"""Pure workout simulator: no Kafka, no I/O. Same seed -> same events."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from uuid import UUID

from telemetry.schemas.events import DailyRecoveryLogged, Exercise, WorkoutSetLogged

TelemetryEvent = WorkoutSetLogged | DailyRecoveryLogged

TRAINING_DAYS: dict[int, list[Exercise]] = {
    0: [Exercise.BACK_SQUAT, Exercise.BENCH_PRESS, Exercise.BARBELL_ROW],  # Monday
    2: [Exercise.DEADLIFT, Exercise.OVERHEAD_PRESS, Exercise.PULL_UP],  # Wednesday
    4: [Exercise.BACK_SQUAT, Exercise.BENCH_PRESS, Exercise.BARBELL_ROW],  # Friday
}
SETS_PER_EXERCISE = 4
TARGET_REPS = 5
INTENSITY = 0.75  # working weight = 75% of training max
WEEKLY_PROGRESSION = 0.015  # +1.5% per week
BASE_TRAINING_MAX = {
    Exercise.BACK_SQUAT: 120.0,
    Exercise.BENCH_PRESS: 90.0,
    Exercise.DEADLIFT: 150.0,
    Exercise.OVERHEAD_PRESS: 55.0,
    Exercise.BARBELL_ROW: 80.0,
    Exercise.PULL_UP: 20.0,  # added load on a belt
}


@dataclass(frozen=True)
class Athlete:
    athlete_id: str
    training_max: dict[Exercise, float]
    baseline_rhr: int
    sleep_mean: float
    session_hour: int


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def plate_round(kg: float) -> float:
    return round(kg / 2.5) * 2.5


def new_uuid(rng: random.Random) -> UUID:
    return UUID(int=rng.getrandbits(128), version=4)  # deterministic under a seed


def make_athletes(n: int, rng: random.Random) -> list[Athlete]:
    athletes = []
    for i in range(1, n + 1):
        strength = rng.uniform(0.8, 1.3)
        athletes.append(
            Athlete(
                athlete_id=f"ath_{i:03d}",
                training_max={ex: round(tm * strength, 1) for ex, tm in BASE_TRAINING_MAX.items()},
                baseline_rhr=rng.randint(50, 65),
                sleep_mean=rng.uniform(6.3, 8.0),
                session_hour=rng.choice([8, 12, 18]),
            )
        )
    return athletes


def generate_recovery(
    athlete: Athlete, day: date, rng: random.Random
) -> tuple[DailyRecoveryLogged, float]:
    """Morning recovery log. Returns the event and a 0..1 readiness that shapes today's session."""
    sleep = clamp(rng.gauss(athlete.sleep_mean, 0.8), 3.5, 10.0)
    readiness = clamp((sleep - 5) / 3, 0.0, 1.0)
    quality = int(clamp(round(1 + (sleep - 4) * 0.7 + rng.gauss(0, 0.5)), 1, 5))
    rhr = athlete.baseline_rhr + round((1 - readiness) * 6 + rng.gauss(0, 1.5))
    hrv = round(clamp(rng.gauss(55 + readiness * 25, 8), 10, 200), 1)
    event_time = datetime.combine(day, time(6, 30), tzinfo=UTC) + timedelta(
        minutes=rng.randint(0, 60)
    )
    event = DailyRecoveryLogged(
        event_id=new_uuid(rng),
        athlete_id=athlete.athlete_id,
        event_time=event_time,
        recovery_date=day,
        sleep_hours=round(sleep, 1),
        sleep_quality=quality,
        resting_hr_bpm=int(rhr),
        hrv_ms=hrv,
    )
    return event, readiness


def generate_session(
    athlete: Athlete,
    day: date,
    week_index: int,
    readiness: float,
    rng: random.Random,
    program: list[Exercise] | None = None,
) -> list[WorkoutSetLogged]:
    exercises = program or TRAINING_DAYS.get(day.weekday())
    if not exercises:
        return []  # rest day
    session_id = new_uuid(rng)
    t = datetime.combine(day, time(athlete.session_hour), tzinfo=UTC)
    t += timedelta(minutes=rng.randint(0, 30))
    fatigue = 0.0
    events: list[WorkoutSetLogged] = []
    for exercise in exercises:
        tm = athlete.training_max[exercise] * (1 + WEEKLY_PROGRESSION * week_index)
        weight = plate_round(tm * INTENSITY)
        for set_number in range(1, SETS_PER_EXERCISE + 1):
            capacity = TARGET_REPS + 1.5 * readiness - fatigue + rng.gauss(0, 0.6)
            reps = int(clamp(round(capacity), 1, TARGET_REPS))
            raw_rpe = 6.5 + fatigue * 0.8 + (1 - readiness) + rng.gauss(0, 0.3)
            rpe = clamp(round(raw_rpe * 2) / 2, 5.0, 10.0)
            rest = int(clamp(rng.gauss(120 + (rpe - 6) * 30, 15), 60, 420))
            t += timedelta(seconds=reps * 4)  # time under tension
            events.append(
                WorkoutSetLogged(
                    event_id=new_uuid(rng),
                    athlete_id=athlete.athlete_id,
                    event_time=t,
                    session_id=session_id,
                    exercise=exercise,
                    set_number=set_number,
                    reps=reps,
                    weight_kg=weight,
                    rpe=rpe,
                    rest_seconds=rest,
                )
            )
            t += timedelta(seconds=rest)
            fatigue += 0.35
        fatigue *= 0.6  # partial recovery between exercises
        t += timedelta(minutes=3)
    return events


def simulate_history(
    athletes: list[Athlete], start: date, days: int, rng: random.Random
) -> list[TelemetryEvent]:
    """All events for `days` days from `start`, sorted by event time."""
    events: list[TelemetryEvent] = []
    for offset in range(days):
        day = start + timedelta(days=offset)
        for athlete in athletes:
            recovery, readiness = generate_recovery(athlete, day, rng)
            events.append(recovery)
            events.extend(generate_session(athlete, day, offset // 7, readiness, rng))
    events.sort(key=lambda e: e.event_time)
    return events
