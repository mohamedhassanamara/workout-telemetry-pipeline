"""Deliberate bad data so the pipeline's defences have something to catch."""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import timedelta

from telemetry.schemas.events import WorkoutSetLogged

CORRUPTIONS = (
    "negative_reps",
    "missing_field",
    "naive_timestamp",
    "wrong_type",
    "unknown_field",
    "truncated_json",
)


@dataclass(frozen=True)
class ChaosConfig:
    malformed_rate: float = 0.0
    duplicate_rate: float = 0.0
    late_rate: float = 0.0
    late_min_minutes: int = 30
    late_max_minutes: int = 90


def corrupt(event: WorkoutSetLogged, rng: random.Random) -> tuple[str, bytes]:
    """Return (corruption name, bytes that must FAIL contract validation)."""
    kind = rng.choice(CORRUPTIONS)
    payload = event.model_dump(mode="json")
    if kind == "truncated_json":
        raw = json.dumps(payload).encode()
        return kind, raw[: len(raw) // 2]
    if kind == "negative_reps":
        payload["reps"] = -3
    elif kind == "missing_field":
        payload.pop("weight_kg")
    elif kind == "naive_timestamp":
        payload["event_time"] = payload["event_time"][:19]  # drop the timezone
    elif kind == "wrong_type":
        payload["rpe"] = "hard"
    elif kind == "unknown_field":
        payload["mood"] = "great"
    return kind, json.dumps(payload).encode()


def split_late(events: list, rate: float, rng: random.Random) -> tuple[list, list]:
    """Backfill: pull some set events out so they can be sent after everything else."""
    on_time, late = [], []
    for e in events:
        (late if isinstance(e, WorkoutSetLogged) and rng.random() < rate else on_time).append(e)
    return on_time, late


def backdate(event: WorkoutSetLogged, cfg: ChaosConfig, rng: random.Random) -> WorkoutSetLogged:
    """Live: an offline phone uploading an old set."""
    minutes = rng.uniform(cfg.late_min_minutes, cfg.late_max_minutes)
    return event.model_copy(update={"event_time": event.event_time - timedelta(minutes=minutes)})
