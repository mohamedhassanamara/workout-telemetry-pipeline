"""Event-time session windows with per-partition watermarks. Pure: no Kafka, no I/O."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from statistics import mean
from uuid import UUID

from telemetry.schemas.events import WorkoutSetLogged

FATIGUE_COLUMNS = [
    "athlete_id",
    "window_start",
    "window_end",
    "device_session_ids",
    "set_count",
    "total_reps",
    "volume_load_kg",
    "avg_rpe",
    "rpe_drift",
    "rep_drop_pct",
    "fatigue_index",
    "closed_by",
]


@dataclass
class SessionWindow:
    athlete_id: str
    start: datetime
    end: datetime  # event time of the latest set so far
    first_offset: int  # earliest Kafka offset that fed this window
    sets: dict[UUID, WorkoutSetLogged] = field(default_factory=dict)  # by event_id: dedup

    def add(self, event: WorkoutSetLogged, offset: int) -> None:
        self.sets[event.event_id] = event
        self.start = min(self.start, event.event_time)
        self.end = max(self.end, event.event_time)
        self.first_offset = min(self.first_offset, offset)

    def absorb(self, other: SessionWindow) -> None:
        self.sets.update(other.sets)
        self.start = min(self.start, other.start)
        self.end = max(self.end, other.end)
        self.first_offset = min(self.first_offset, other.first_offset)


@dataclass
class PartitionState:
    max_event_time: datetime | None = None
    last_seen_wall: float = 0.0
    windows: dict[str, list[SessionWindow]] = field(default_factory=dict)  # athlete -> open

    def open_windows(self) -> list[SessionWindow]:
        return [w for ws in self.windows.values() for w in ws]


class SessionWindower:
    def __init__(
        self,
        gap: timedelta = timedelta(minutes=30),
        allowed_lateness: timedelta = timedelta(minutes=10),
        max_out_of_order: timedelta = timedelta(minutes=2),
    ) -> None:
        self.gap = gap
        self.allowed_lateness = allowed_lateness
        self.max_out_of_order = max_out_of_order

    def watermark(self, state: PartitionState) -> datetime | None:
        if state.max_event_time is None:
            return None
        return state.max_event_time - self.max_out_of_order

    def add(self, state: PartitionState, event: WorkoutSetLogged, offset: int) -> bool:
        """True if the event joined a window; False if it is too late for any window."""
        wm = self.watermark(state)
        if wm is not None and event.event_time < wm - self.allowed_lateness:
            return False
        windows = state.windows.setdefault(event.athlete_id, [])
        touching = [
            w for w in windows if w.start - self.gap <= event.event_time <= w.end + self.gap
        ]
        if touching:
            target = touching[0]
            for other in touching[1:]:  # the event bridges two sessions: merge them
                target.absorb(other)
                windows.remove(other)
        else:
            target = SessionWindow(event.athlete_id, event.event_time, event.event_time, offset)
            windows.append(target)
        target.add(event, offset)
        if state.max_event_time is None or event.event_time > state.max_event_time:
            state.max_event_time = event.event_time
        return True

    def close_ready(self, state: PartitionState) -> list[SessionWindow]:
        wm = self.watermark(state)
        if wm is None:
            return []
        ready: list[SessionWindow] = []
        for athlete in list(state.windows):
            keep = []
            for w in state.windows[athlete]:
                closed = wm > w.end + self.gap + self.allowed_lateness
                (ready if closed else keep).append(w)
            if keep:
                state.windows[athlete] = keep
            else:
                del state.windows[athlete]
        return ready

    def close_all(self, state: PartitionState) -> list[SessionWindow]:
        ready = state.open_windows()
        state.windows.clear()
        return ready

    @staticmethod
    def safe_commit_offset(state: PartitionState, next_offset: int) -> int:
        """Earliest offset still needed by an open window (replay rebuilds it from there)."""
        firsts = [w.first_offset for w in state.open_windows()]
        return min(firsts) if firsts else next_offset


def summarize(window: SessionWindow, closed_by: str) -> list:
    """Closed window -> one gold_session_fatigue row (FATIGUE_COLUMNS order)."""
    sets = sorted(window.sets.values(), key=lambda e: e.event_time)
    per_exercise: dict[str, list[WorkoutSetLogged]] = {}
    for s in sets:
        per_exercise.setdefault(str(s.exercise), []).append(s)
    drifts, drops = [], []
    for ex_sets in per_exercise.values():
        if len(ex_sets) < 2:
            continue
        first, last = ex_sets[0], ex_sets[-1]
        drifts.append(last.rpe - first.rpe)
        drops.append(max(0.0, 1 - last.reps / first.reps))
    rpe_drift = mean(drifts) if drifts else 0.0
    rep_drop = mean(drops) if drops else 0.0
    fatigue = 100 * (0.5 * min(max(rpe_drift / 3, 0.0), 1.0) + 0.5 * rep_drop)
    return [
        window.athlete_id,
        window.start,
        window.end,
        sorted({s.session_id for s in sets}),
        len(sets),
        sum(s.reps for s in sets),
        round(sum(s.reps * s.weight_kg for s in sets), 1),
        round(mean(s.rpe for s in sets), 2),
        round(rpe_drift, 2),
        round(100 * rep_drop, 1),
        round(fatigue, 1),
        closed_by,
    ]
