# Copyright (c) 2026 Imaginary Root Studio LLC. All rights reserved.
# Proprietary and confidential. See LICENSE.

"""test_p1_batcher.py — P1BatchingTransport unit tests.

Covers:
- P2 events pass straight through; P1 events get accumulated.
- A window closes at elapsed_s >= boundary; one summary per type that fired.
- Empty types emit nothing.
- max / mean / count are computed correctly.
- window_idx advances mod 8 per type.
- close() flushes any partially-accumulated open window.
- Multiple tick boundaries crossed in one big jump still emit a summary
  for each non-empty window in order (gap robustness).
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
    """Minimal Transport-protocol implementation that records every call so
    tests can assert on the sequence of emit/emit_summary/tick/close events
    seen by the wrapped transport."""

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
    """Build a minimal Event with the fields the batcher actually reads.
    Other fields get plausible-but-uninteresting defaults."""
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


# -----------------------------------------------------------------------------
# Routing: P2 passes through, P1 accumulates, LOG passes through
# -----------------------------------------------------------------------------

def test_p2_passes_through_immediately():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy)
    p2 = make_event(event_type="hard_brake", priority="P2", elapsed_s=10.0)
    batcher.emit(p2)
    # P2 reached inner transport untouched.
    assert spy.per_event == [p2]
    assert spy.summaries == []


def test_log_passes_through_by_default():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy)
    log = make_event(event_type="long_stop", priority="LOG", elapsed_s=10.0)
    batcher.emit(log)
    assert spy.per_event == [log]
    assert spy.summaries == []


def test_p1_accumulates_no_immediate_emit():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy)
    p1 = make_event(event_type="moderate_impact", priority="P1", elapsed_s=10.0)
    batcher.emit(p1)
    assert spy.per_event == []        # not passed through
    assert spy.summaries == []        # window hasn't closed
    assert batcher.p1_events_seen == 1


def test_log_batched_when_flag_set():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, batch_log_priority=True)
    log_evt = make_event(event_type="long_stop", priority="LOG", elapsed_s=10.0)
    batcher.emit(log_evt)
    assert spy.per_event == []
    assert batcher.p1_events_seen == 1


# -----------------------------------------------------------------------------
# Window boundaries: 5-min tumbling, wall-clock-aligned in elapsed time
# -----------------------------------------------------------------------------

def test_first_window_end_aligned_to_window_s():
    """Event at elapsed_s=10 lives in the window ending at 300s (the next
    multiple of 300 strictly greater than 10)."""
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, window_s=300.0)
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=10.0,
    ))
    batcher.tick(ts_epoch_s=0, elapsed_s=10.0)
    # No close yet.
    assert spy.summaries == []
    # Cross the 300s boundary -> window closes.
    batcher.tick(ts_epoch_s=0, elapsed_s=300.0)
    assert len(spy.summaries) == 1
    assert spy.summaries[0].window_end_elapsed_s == 300.0


def test_window_boundaries_aligned_when_first_event_late():
    """Event at elapsed_s=150 lives in the window ending at 300, not at 450.
    Tumbling alignment is global to the session, not relative to first event."""
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, window_s=300.0)
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=150.0,
    ))
    batcher.tick(ts_epoch_s=0, elapsed_s=300.0)
    assert len(spy.summaries) == 1
    assert spy.summaries[0].window_end_elapsed_s == 300.0


def test_one_summary_per_type_per_window():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, window_s=300.0)
    # Two types, one event each in the first window.
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=10.0,
    ))
    batcher.emit(make_event(
        event_type="hard_accel", priority="P1", elapsed_s=20.0,
    ))
    batcher.tick(ts_epoch_s=0, elapsed_s=300.0)
    types = sorted(s.event_type for s in spy.summaries)
    assert types == ["hard_accel", "moderate_impact"]
    assert all(s.count == 1 for s in spy.summaries)


def test_empty_type_emits_nothing():
    """A window with no P1 activity at all emits no summaries."""
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, window_s=300.0)
    # No P1 events submitted; just cross a boundary.
    batcher.tick(ts_epoch_s=0, elapsed_s=300.0)
    assert spy.summaries == []


# -----------------------------------------------------------------------------
# Stat computation: count, max, mean
# -----------------------------------------------------------------------------

def test_count_max_mean_correct():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, window_s=300.0)
    scores = [0.30, 0.55, 0.80, 0.40]
    for i, s in enumerate(scores):
        batcher.emit(make_event(
            event_type="moderate_impact", priority="P1",
            elapsed_s=10.0 + i * 5, score=s,
        ))
    batcher.tick(ts_epoch_s=0, elapsed_s=300.0)
    assert len(spy.summaries) == 1
    s = spy.summaries[0]
    assert s.count == 4
    assert s.max_score == pytest.approx(0.80)
    assert s.mean_score == pytest.approx(sum(scores) / len(scores))


# -----------------------------------------------------------------------------
# window_idx rotation
# -----------------------------------------------------------------------------

def test_window_idx_advances_per_type_mod_8():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, window_s=300.0)
    # 10 consecutive windows for the same type -> indices cycle 0..7,0,1
    expected_indices = []
    for w in range(10):
        # one event then close window
        batcher.emit(make_event(
            event_type="moderate_impact", priority="P1",
            elapsed_s=10.0 + w * 300.0,
        ))
        batcher.tick(ts_epoch_s=0, elapsed_s=300.0 * (w + 1))
        expected_indices.append(w % 8)
    indices = [s.window_idx for s in spy.summaries]
    assert indices == expected_indices


def test_window_idx_per_type_independent():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, window_s=300.0)
    # window 1: only type A
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=10.0,
    ))
    batcher.tick(ts_epoch_s=0, elapsed_s=300.0)
    # window 2: only type B (A had no event so should not advance its idx)
    batcher.emit(make_event(
        event_type="hard_accel", priority="P1", elapsed_s=310.0,
    ))
    batcher.tick(ts_epoch_s=0, elapsed_s=600.0)
    # window 3: both
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=610.0,
    ))
    batcher.emit(make_event(
        event_type="hard_accel", priority="P1", elapsed_s=620.0,
    ))
    batcher.tick(ts_epoch_s=0, elapsed_s=900.0)

    per_type = {}
    for s in spy.summaries:
        per_type.setdefault(s.event_type, []).append(s.window_idx)
    # moderate_impact emitted in window 1 (idx 0) and window 3 (idx 1).
    assert per_type["moderate_impact"] == [0, 1]
    # hard_accel emitted in window 2 (idx 0) and window 3 (idx 1).
    assert per_type["hard_accel"] == [0, 1]


# -----------------------------------------------------------------------------
# Multi-boundary tick: gap robustness
# -----------------------------------------------------------------------------

def test_tick_crossing_multiple_boundaries_emits_in_order():
    """If tick jumps from elapsed=10 to elapsed=900 (skipping multiple
    5-min boundaries), the events submitted at elapsed=10 should emit a
    summary for window ending at 300; subsequent empty windows emit
    nothing, and the clock advances past them all so future events land
    in the right window."""
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, window_s=300.0)
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=10.0,
    ))
    # Jump straight to elapsed=900 (3 boundaries crossed: 300, 600, 900).
    batcher.tick(ts_epoch_s=0, elapsed_s=900.0)
    # Only one summary fires (for window ending at 300; the 600 and 900
    # windows had no P1 events).
    assert len(spy.summaries) == 1
    assert spy.summaries[0].window_end_elapsed_s == 300.0
    # Window cursor should now be at 1200, not 300 or 600.
    # We can verify by emitting an event and crossing the next boundary.
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=950.0,
    ))
    batcher.tick(ts_epoch_s=0, elapsed_s=1200.0)
    assert len(spy.summaries) == 2
    assert spy.summaries[1].window_end_elapsed_s == 1200.0


# -----------------------------------------------------------------------------
# close() flushes open window
# -----------------------------------------------------------------------------

def test_close_flushes_partial_window():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, window_s=300.0)
    batcher.emit(make_event(
        event_type="moderate_impact", priority="P1", elapsed_s=10.0,
    ))
    # No tick crossing the boundary. Close should still flush.
    batcher.close()
    assert len(spy.summaries) == 1
    assert spy.summaries[0].count == 1
    # The window-end is the planned boundary, not the actual close time.
    assert spy.summaries[0].window_end_elapsed_s == 300.0
    assert spy.closed is True


def test_close_no_events_emits_no_summaries():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, window_s=300.0)
    batcher.close()
    assert spy.summaries == []
    assert spy.closed is True


# -----------------------------------------------------------------------------
# Tick is propagated to inner transport
# -----------------------------------------------------------------------------

def test_tick_propagates_to_inner():
    spy = SpyTransport()
    batcher = P1BatchingTransport(spy, window_s=300.0)
    batcher.tick(ts_epoch_s=1.0, elapsed_s=2.0)
    batcher.tick(ts_epoch_s=2.0, elapsed_s=3.0)
    assert spy.ticks == [(1.0, 2.0), (2.0, 3.0)]


# -----------------------------------------------------------------------------
# Invalid construction
# -----------------------------------------------------------------------------

def test_zero_window_rejected():
    spy = SpyTransport()
    with pytest.raises(ValueError):
        P1BatchingTransport(spy, window_s=0.0)


def test_negative_window_rejected():
    spy = SpyTransport()
    with pytest.raises(ValueError):
        P1BatchingTransport(spy, window_s=-10.0)
