from datetime import timedelta

import pytest

from telemetry.processing.fatigue_windows.windows import (
    PartitionState,
    SessionWindower,
    summarize,
)
from tests.conftest import BASE

MIN = timedelta(minutes=1)


@pytest.fixture
def windower():
    return SessionWindower()  # gap 30, lateness 10, out-of-order 2 (minutes)


def test_window_closes_once_watermark_passes(windower, make_set):
    state = PartitionState()
    windower.add(state, make_set(event_time=BASE), 0)
    windower.add(state, make_set(event_time=BASE + 5 * MIN), 1)
    assert windower.close_ready(state) == []
    # another athlete on the same partition pushes the watermark past end + gap + lateness
    windower.add(state, make_set(athlete_id="ath_002", event_time=BASE + 48 * MIN), 2)
    closed = windower.close_ready(state)
    assert len(closed) == 1 and closed[0].athlete_id == "ath_001"


def test_gap_starts_a_new_session(windower, make_set):
    state = PartitionState()
    windower.add(state, make_set(event_time=BASE), 0)
    windower.add(state, make_set(event_time=BASE + 31 * MIN), 1)
    assert len(state.windows["ath_001"]) == 2


def test_late_event_is_rejected(windower, make_set):
    state = PartitionState()
    windower.add(state, make_set(event_time=BASE + 120 * MIN), 0)
    assert windower.add(state, make_set(event_time=BASE), 1) is False


def test_out_of_order_within_tolerance_is_accepted(windower, make_set):
    state = PartitionState()
    windower.add(state, make_set(event_time=BASE + 10 * MIN), 0)
    assert windower.add(state, make_set(event_time=BASE + 5 * MIN), 1) is True


def test_duplicate_event_counts_once(windower, make_set):
    state = PartitionState()
    event = make_set(event_time=BASE)
    windower.add(state, event, 0)
    windower.add(state, event, 1)
    assert len(state.windows["ath_001"][0].sets) == 1


def test_bridging_event_merges_sessions(make_set):
    windower = SessionWindower(allowed_lateness=timedelta(minutes=60))  # so the bridge isn't late
    state = PartitionState()
    windower.add(state, make_set(event_time=BASE), 0)
    windower.add(state, make_set(event_time=BASE + 50 * MIN), 1)
    windower.add(state, make_set(event_time=BASE + 25 * MIN), 2)
    assert len(state.windows["ath_001"]) == 1


def test_safe_commit_offset_is_earliest_open_window(windower, make_set):
    state = PartitionState()
    windower.add(state, make_set(event_time=BASE), 7)
    windower.add(state, make_set(athlete_id="ath_002", event_time=BASE + MIN), 9)
    assert SessionWindower.safe_commit_offset(state, next_offset=12) == 7


def test_fatigue_index_formula(windower, make_set):
    state = PartitionState()
    windower.add(state, make_set(event_time=BASE, rpe=7.0, reps=5), 0)
    windower.add(state, make_set(event_time=BASE + 3 * MIN, rpe=9.0, reps=3), 1)
    row = summarize(windower.close_all(state)[0], "idle")
    # drift 2 -> 0.5 * 2/3 ; rep drop 0.4 -> 0.5 * 0.4 ; total 0.533 -> 53.3
    assert row[-2] == pytest.approx(53.3, abs=0.1)
