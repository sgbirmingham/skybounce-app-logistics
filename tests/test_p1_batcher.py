# Copyright (c) 2026 Imaginary Root Studio LLC. All rights reserved.
# Proprietary and confidential. See LICENSE.

"""test_p1_batcher.py — P1BatchingTransport unit tests (SB45_SIM_V4).

Covers the per-WINDOW V4 model:
- P2 passes through immediately AND is recorded in the window (p2_present).
- LOG passes through; P1 accumulates.
- A window closes at elapsed_s >= boundary; ONE summary per window aggregates
  all P1 types into code_counts.
- max_score is the window peak (over P1 and P2).
- window_idx is derived from window position (mod 4, 2-bit), gap-robust.
- emit_empty_windows: True emits empty windows (gapless idx); False skips them.
- close() flushes an open partial window iff it saw activity.
- Multiple boundaries crossed in one tick still emit each window in order.
"""

from __future__ import annotations

from typing import List

import pytest

from skybounce_app_logistics.p1_batcher import P1BatchingTransport
from skybounce_app_logistics.transport import Event, SummaryEvent


# -----------------------------------------------------------------------------
# Spy transport: records every emit / emit_summary / tick / close call.
# -----------------------------------------------------------------------------

class SpyTransport:
    def __init__(self) -> None:
        self.per_event: List[Event] = []
        self.summaries: List[SummaryEvent] = []
        self.ticks: List[tuple[float, float]] = []
        self.closed: bool = False

    def emit(self, event: Event) -> None:
        self.per_event.append(event)

    def emit_summary(self, summary: SummaryEvent) -> None:
        self.summaries.append(summary)

    def tick(self, ts_epoch_s: float, elapsed_s: float) -> None:
        self.ticks.append((ts_epoch_s, elapsed_s))

    def close(self) -> None:
        self.closed = True


# -----------------------------------------------------------------------------
# Event factory
# -----------------------------------------------------------------------------

def make_event(
    *,
    event_type: str,
    priority: str,
    elapsed_s: float,
    score: float = 0.5,
    gps_lat: float = 40.44,
    gps_lon: float = -79.99,
) -> Event:
    return Event(
        ts_epoch_s=1_700_000_000.0 + elapsed_s,
        elapsed_s=elapsed_s,
        event_type=event_type,
        score=score,
        priority=priority,
        event_class="ROAD_EVENT" if priority == "P1" else "SAFETY_IMMEDIATE",
        packet_rank=55,
        policy_reason="test",
        detail="test event",
        speed_m_s=20.0,
        speed_mph=44.7,
        decel_m_s2=2.0,
        accel_m_s2=0.0,
        lin_accel_g=0.1,
        jerk_g_s=0.05,
        gps_confidence=0.9,
        gps_lat=gps_lat,
        gps_lon=gps_lon,
        anchor_lat=40.44,
        anchor_lon=-79.99,
        analyzer_state="MOVING",
    )


# Convenience: a batcher that does NOT emit empty windows, so tests that only
# care about activity windows don't have to filter out empties.
def _batcher_no_empties(spy, window_s=300.0):
    return P1BatchingTransport(spy, window_s=window_s, emit_empty_windows=False)


# -----------------------------------------------------------------------------
# Routing
# -----------------------------------------------------------------------------

def test_p2_passes_through_immediately():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy)
    p2 = make_event(event_type="hard_brake", priority="P2", elapsed_s=10.0)
    batcher.emit(p2)
    assert spy.per_event == [p2]      # reached inner untouched
    assert spy.summaries == []        # window hasn't closed


def test_p0_passes_through():
    # P0 (was LOG/NORMAL) passes through the batcher to the inner transport;
    # the IPC wire drops it, but the batcher itself is wire-agnostic.
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy)
    p0_evt = make_event(event_type="state_moving", priority="P0", elapsed_s=10.0)
    batcher.emit(p0_evt)
    assert spy.per_event == [p0_evt]
    assert spy.summaries == []


def test_p1_accumulates_no_immediate_emit():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy)
    p1 = make_event(event_type="moderate_impact", priority="P1", elapsed_s=10.0)
    batcher.emit(p1)
    assert spy.per_event == []        # not passed through
    assert spy.summaries == []        # window hasn't closed
    assert batcher.p1_events_seen == 1


# -----------------------------------------------------------------------------
# Window boundaries
# -----------------------------------------------------------------------------

def test_first_window_end_aligned_to_window_s():
    spy = SpyTransport()
    batcher = _batcher_no_empties(spy, window_s=300.0)
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=10.0,
    ))
    batcher.tick(ts_epoch_s=0, elapsed_s=10.0)
    assert spy.summaries == []                      # no boundary crossed
    batcher.tick(ts_epoch_s=0, elapsed_s=300.0)     # cross 300s boundary
    assert len(spy.summaries) == 1
    assert spy.summaries[0].window_end_elapsed_s == 300.0


def test_one_summary_per_window_aggregates_all_types():
    spy = SpyTransport()
    batcher = _batcher_no_empties(spy, window_s=300.0)
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=10.0,
    ))
    batcher.emit(make_event(
        event_type="hard_accel", priority="P1", elapsed_s=20.0,
    ))
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=30.0,
    ))
    batcher.tick(ts_epoch_s=0, elapsed_s=300.0)
    # ONE summary for the window, all types aggregated into code_counts.
    assert len(spy.summaries) == 1
    assert spy.summaries[0].code_counts == {"moderate_impact": 2, "hard_accel": 1}


def test_max_score_is_window_peak_no_mean():
    spy = SpyTransport()
    batcher = _batcher_no_empties(spy, window_s=300.0)
    for i, s in enumerate([0.30, 0.55, 0.80, 0.40]):
        batcher.emit(make_event(
            event_type="moderate_impact", priority="P1",
            elapsed_s=10.0 + i * 5, score=s,
        ))
    batcher.tick(ts_epoch_s=0, elapsed_s=300.0)
    s = spy.summaries[0]
    assert s.code_counts == {"moderate_impact": 4}
    assert s.max_score == pytest.approx(0.80)
    assert not hasattr(s, "mean_score")  # V4 dropped mean


# -----------------------------------------------------------------------------
# Empty windows: emit (default) vs skip
# -----------------------------------------------------------------------------

def test_empty_window_emits_by_default():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, window_s=300.0)  # emit_empty default True
    batcher.tick(ts_epoch_s=0, elapsed_s=10.0)          # open window [0,300)
    batcher.tick(ts_epoch_s=0, elapsed_s=300.0)         # cross its boundary
    assert len(spy.summaries) == 1
    s = spy.summaries[0]
    assert s.code_counts == {}
    assert s.p2_present is False
    assert s.window_idx == 1            # round(300/300) % 4


def test_empty_window_skipped_when_disabled():
    spy = SpyTransport()
    batcher = _batcher_no_empties(spy, window_s=300.0)
    batcher.tick(ts_epoch_s=0, elapsed_s=300.0)
    assert spy.summaries == []


# -----------------------------------------------------------------------------
# window_idx: derived from position, gap-robust, wraps mod 4 (2-bit)
# -----------------------------------------------------------------------------

def test_window_idx_derived_from_position():
    spy = SpyTransport()
    batcher = _batcher_no_empties(spy, window_s=300.0)
    for w in range(1, 6):
        batcher.emit(make_event(
            event_type="moderate_impact", priority="P1",
            elapsed_s=(w - 1) * 300.0 + 10.0,
        ))
        batcher.tick(ts_epoch_s=0, elapsed_s=w * 300.0)
    # 2-bit window_idx: window number mod 4.
    assert [s.window_idx for s in spy.summaries] == [1, 2, 3, 0, 1]


def test_window_idx_wraps_mod_4():
    spy = SpyTransport()
    batcher = _batcher_no_empties(spy, window_s=300.0)
    # Window ending at 4*300 = 1200 -> idx 0; 5*300 = 1500 -> idx 1.
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=1200.0 - 10.0,
    ))
    batcher.tick(ts_epoch_s=0, elapsed_s=1200.0)
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=1500.0 - 10.0,
    ))
    batcher.tick(ts_epoch_s=0, elapsed_s=1500.0)
    assert [s.window_idx for s in spy.summaries] == [0, 1]


def test_window_idx_gap_robust_when_skipping_empties():
    """Even skipping empty windows, each emitted summary carries the index for
    its actual elapsed position -- so a window_idx gap is real, not an artifact
    of skipping."""
    spy = SpyTransport()
    batcher = _batcher_no_empties(spy, window_s=300.0)
    # Activity in window 1 and window 4; windows 2-3 empty (skipped).
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=10.0,
    ))
    batcher.tick(ts_epoch_s=0, elapsed_s=300.0)
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=910.0,
    ))
    batcher.tick(ts_epoch_s=0, elapsed_s=1200.0)
    # window 1 -> idx 1; window 4 -> idx 0 (mod 4). The gap (1 then 0, skipping
    # 2 and 3) is real, not an artifact of skipping.
    assert [s.window_idx for s in spy.summaries] == [1, 0]


# -----------------------------------------------------------------------------
# P2 recorded in the window summary
# -----------------------------------------------------------------------------

def test_p2_recorded_in_window_and_passes_through():
    spy = SpyTransport()
    batcher = _batcher_no_empties(spy, window_s=300.0)
    p2 = make_event(event_type="severe_impact", priority="P2",
                    elapsed_s=10.0, score=0.95)
    p1 = make_event(event_type="moderate_impact", priority="P1",
                    elapsed_s=20.0, score=0.40)
    batcher.emit(p2)
    batcher.emit(p1)
    batcher.tick(ts_epoch_s=0, elapsed_s=300.0)
    assert spy.per_event == [p2]                 # P2 still real-time
    s = spy.summaries[0]
    assert s.p2_present is True
    assert s.code_counts == {"moderate_impact": 1}   # P2 NOT counted here
    assert s.max_score == pytest.approx(0.95)    # peak includes the P2


def test_p2_only_window_emits_with_p2_present():
    spy = SpyTransport()
    batcher = _batcher_no_empties(spy, window_s=300.0)
    batcher.emit(make_event(event_type="severe_impact", priority="P2",
                            elapsed_s=10.0, score=0.9))
    batcher.tick(ts_epoch_s=0, elapsed_s=300.0)
    assert len(spy.summaries) == 1
    s = spy.summaries[0]
    assert s.p2_present is True
    assert s.code_counts == {}


# -----------------------------------------------------------------------------
# Multi-boundary tick: don't-skip emits each window in order
# -----------------------------------------------------------------------------

def test_tick_crossing_multiple_boundaries_emits_each_window():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, window_s=300.0)  # emit_empty True
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=10.0,
    ))
    # Jump from 10 to 900: crosses 300, 600, 900 -> 3 summaries.
    batcher.tick(ts_epoch_s=0, elapsed_s=900.0)
    assert [s.window_idx for s in spy.summaries] == [1, 2, 3]
    assert spy.summaries[0].code_counts == {"moderate_impact": 1}
    assert spy.summaries[1].code_counts == {}    # empty windows still emitted
    assert spy.summaries[2].code_counts == {}


# -----------------------------------------------------------------------------
# close() flushes open window
# -----------------------------------------------------------------------------

def test_close_flushes_partial_window_with_activity():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, window_s=300.0)
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=10.0,
    ))
    batcher.close()                              # no tick crossing the boundary
    assert len(spy.summaries) == 1
    assert spy.summaries[0].code_counts == {"moderate_impact": 1}
    assert spy.summaries[0].window_end_elapsed_s == 300.0  # planned boundary
    assert spy.closed is True


def test_close_no_activity_emits_no_summary():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, window_s=300.0)  # emit_empty True...
    batcher.close()                                     # ...but no window ran
    assert spy.summaries == []
    assert spy.closed is True


# -----------------------------------------------------------------------------
# tick propagation, invalid construction
# -----------------------------------------------------------------------------

def test_tick_propagates_to_inner():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, window_s=300.0)
    batcher.tick(ts_epoch_s=1.0, elapsed_s=2.0)
    batcher.tick(ts_epoch_s=2.0, elapsed_s=3.0)
    assert spy.ticks == [(1.0, 2.0), (2.0, 3.0)]


def test_zero_window_rejected():
    with pytest.raises(ValueError):
        P1BatchingTransport(SpyTransport(), window_s=0.0)


def test_negative_window_rejected():
    with pytest.raises(ValueError):
        P1BatchingTransport(SpyTransport(), window_s=-10.0)
