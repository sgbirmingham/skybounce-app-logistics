# Copyright (c) 2026 Imaginary Root Studio LLC. All rights reserved.
# Proprietary and confidential. See LICENSE.

"""P1BatchingTransport — wraps any Transport with per-window SB45_SIM_V4 batching.

Motivation
----------

The 2026-06-06 4-hour soak confirmed the radio can only absorb ~mu = 15
frames/hr aggregate (5/hr/node). Per-event P1 streams (90-135/hr offered)
swamp that: 15-21% accept, 3h+ backlog-drain delays. P2 (safety-immediate)
frames must stay per-event and real-time, but P1 (medium-priority) traffic is
collapsed to ONE summary frame per Pi per 30-min window. The raw events stay
in the engine's local stream for forensics; nothing is lost on the sensor side.

Design (SB45_SIM_V4)
--------------------

- Window: 30-minute wall-clock-aligned tumbling windows on each Pi. Boundaries
  at elapsed_s multiples of `window_s` (default 1800.0; sized by the 2026-06-07
  capacity sim so 3 nodes stay under the radio's mu). The clock is driven by
  the engine's frame stream via `tick(ts_epoch_s, elapsed_s)`.

- One summary per WINDOW (not per type). A single accumulator gathers, for the
  open window: per-event-type P1 counts, peak severity (over P1 and P2),
  whether any P2 fired (`p2_present`), and the last event's position + anchor.

- End-of-window send: at the first tick crossing a window boundary, the
  accumulator emits ONE SummaryEvent to the wrapped transport's
  `emit_summary()`, then resets. The IPC transport packs it via
  `pack_sb45_summary_v4` (priority_code == 3); the file transport writes it as
  a marked JSON line.

- window_idx: 0..31, DERIVED from the window position
  (`round(window_end_s / window_s) mod 4`), not a free-running counter. This
  makes it gap-robust -- a paused engine that resumes still tags each window
  with the index the basestation expects for that elapsed time. 2-bit width
  wraps every 4 windows = 2h at 30-min windows -- a dedup/ordering/gap
  cross-check, since absolute window time now comes from the Pi-side emission
  timestamp in the IPC envelope (see the IPC repo's
  sb45_v4_window_idx_decision_2026-06-07.md).

- emit_empty_windows (default True): emit a summary for every crossed boundary
  even when no P1/P2 fired ("node alive, here, nothing happened"). Keeps
  window_idx a gapless sequence so the basestation can detect a dropped window,
  and gives logistics a position breadcrumb every window. Set False to suppress
  empty-window frames -- but then a gap in window_idx is ambiguous between
  "empty" and "dropped".

- P2 and P0 events pass through to the inner transport. P2 is also OBSERVED by
  the open window (sets p2_present, contributes to peak severity and position)
  so the summary still records that a safety event occurred even if the
  real-time P2 frame is later dropped by the radio.

- `close()` flushes the open partial window IF it saw any activity, then closes
  the wrapped transport.

Lifecycle
---------

    base = IpcTransport(endpoint_id=...) or FileTransport(path=...)
    batched = P1BatchingTransport(base)  # wrap
    engine = StreamingEngine(transport=batched)
    for frame in source:
        engine.consume(frame)  # consume() also calls transport.tick()
    engine.flush()
    batched.close()            # flushes any open window, then closes base
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from .transport import Event, SummaryEvent, Transport

log = logging.getLogger(__name__)


DEFAULT_WINDOW_S = 1800.0  # 30 minutes (SB45_SIM_V4). Chosen from the
                           # 2026-06-07 capacity sim: 15-min windows put 3
                           # nodes over mu=15/hr; 30-min lands at mu (with P0
                           # dropped at the IPC wire). See the findings doc.
WINDOW_IDX_MODULUS = 4     # 2-bit window_idx; wraps every 4 windows (2h @ 30min)


@dataclass
class _WindowState:
    """Single-window accumulator for the currently-open 30-min window. Gathers
    every P1 event (by type) plus severity / position / P2-presence context
    from both P1 and P2 events. Reset after the window emits its summary."""
    code_counts: dict[str, int] = field(default_factory=dict)  # P1 type -> count
    max_score: float = 0.0          # peak severity over P1 AND P2 events
    p2_present: bool = False
    p1_count: int = 0
    any_activity: bool = False      # any P1 or P2 event observed this window
    last_gps_lat: Optional[float] = None
    last_gps_lon: Optional[float] = None
    last_location_status: str = "NO_LOCATION"
    anchor_lat: Optional[float] = None
    anchor_lon: Optional[float] = None

    def _observe(self, event: Event) -> None:
        """Severity + position context, from any P1 or P2 event."""
        if event.score > self.max_score:
            self.max_score = float(event.score)
        # Position = last event's location, mirroring the per-event encoder's
        # gps-presence -> location_status derivation.
        if event.gps_lat is not None and event.gps_lon is not None:
            self.last_gps_lat = event.gps_lat
            self.last_gps_lon = event.gps_lon
            self.last_location_status = "DIRECT_EVENT_LOCATION"
        else:
            self.last_gps_lat = None
            self.last_gps_lon = None
            self.last_location_status = "NO_LOCATION"
        # Anchor is the session anchor (constant); keep the last non-None seen.
        if event.anchor_lat is not None:
            self.anchor_lat = event.anchor_lat
        if event.anchor_lon is not None:
            self.anchor_lon = event.anchor_lon
        self.any_activity = True

    def add_p1(self, event: Event) -> None:
        self.code_counts[event.event_type] = (
            self.code_counts.get(event.event_type, 0) + 1
        )
        self.p1_count += 1
        self._observe(event)

    def note_p2(self, event: Event) -> None:
        self.p2_present = True
        self._observe(event)


class P1BatchingTransport:
    """Wraps a Transport. P1 events are accumulated into one per-window
    SB45_SIM_V4 summary; P2 and P0 events pass through unchanged (P2 is also
    observed by the open window for the summary's p2_present / severity /
    position context).

    The wrapped transport must implement the standard Transport protocol
    (emit, emit_summary, tick, close). `emit_summary` on the wrapped transport
    is what actually puts a summary on the wire.
    """

    def __init__(
        self,
        inner: Transport,
        window_s: float = DEFAULT_WINDOW_S,
        emit_empty_windows: bool = True,
    ) -> None:
        if window_s <= 0.0:
            raise ValueError(f"window_s must be positive, got {window_s!r}")
        self._inner = inner
        self._window_s = float(window_s)
        self._emit_empty_windows = bool(emit_empty_windows)

        # The currently-open window's accumulator.
        self._window = _WindowState()

        # End-of-current-window in elapsed_s. None until first event/tick.
        self._current_window_end_s: Optional[float] = None

        # Observability counters.
        self._p1_events_seen = 0
        self._p2_events_seen = 0
        self._summaries_emitted = 0

        log.info(
            "P1BatchingTransport: window_s=%.0fs, emit_empty_windows=%s",
            self._window_s, self._emit_empty_windows,
        )

    # -------------------------------------------------------------------------
    # Transport protocol
    # -------------------------------------------------------------------------

    def emit(self, event: Event) -> None:
        """Route an event by priority:
        - P1: accumulate into the open window's per-type bucket.
        - P2: pass through to inner immediately (real-time safety), and also
          observe it in the open window (p2_present + severity + position).
        - P0: pass through unchanged.
        """
        if event.priority == "P1":
            self._roll_to_event(event.elapsed_s)
            self._window.add_p1(event)
            self._p1_events_seen += 1
        elif event.priority == "P2":
            self._roll_to_event(event.elapsed_s)
            self._window.note_p2(event)
            self._p2_events_seen += 1
            self._inner.emit(event)
        else:  # P0
            self._inner.emit(event)

    def emit_summary(self, summary: SummaryEvent) -> None:
        """Pass through to inner. Summaries generated by this batcher go through
        this method too; further-nested batchers would be unusual but the
        abstraction supports it."""
        self._inner.emit_summary(summary)

    def tick(self, ts_epoch_s: float, elapsed_s: float) -> None:
        """Advance the window clock. If `elapsed_s` crossed one or more window
        boundaries since the last tick, close each crossed window in order, so
        sparse ticks with gaps still produce the right sequence of frames. Then
        propagate the tick to the inner transport."""
        self._lazy_init_window(elapsed_s)
        assert self._current_window_end_s is not None

        while elapsed_s >= self._current_window_end_s:
            self._close_window(self._current_window_end_s)
            self._current_window_end_s += self._window_s

        self._inner.tick(ts_epoch_s, elapsed_s)

    def close(self) -> None:
        """Flush the open partial window if it saw any activity, then close
        inner. The summary's window_end_elapsed_s is the planned boundary even
        though only a partial slice of the window ran."""
        if self._current_window_end_s is not None and self._window.any_activity:
            self._close_window(self._current_window_end_s)
        log.info(
            "P1BatchingTransport closing: p1_events_seen=%d, p2_events_seen=%d, "
            "summaries_emitted=%d",
            self._p1_events_seen, self._p2_events_seen, self._summaries_emitted,
        )
        self._inner.close()

    # -------------------------------------------------------------------------
    # Observability
    # -------------------------------------------------------------------------

    @property
    def p1_events_seen(self) -> int:
        return self._p1_events_seen

    @property
    def p2_events_seen(self) -> int:
        return self._p2_events_seen

    @property
    def summaries_emitted(self) -> int:
        return self._summaries_emitted

    # -------------------------------------------------------------------------
    # Internals
    # -------------------------------------------------------------------------

    def _lazy_init_window(self, elapsed_s: float) -> None:
        if self._current_window_end_s is None:
            self._current_window_end_s = self._next_boundary(elapsed_s)
            log.debug(
                "P1BatchingTransport: first window ends at elapsed_s=%.1fs",
                self._current_window_end_s,
            )

    def _roll_to_event(self, elapsed_s: float) -> None:
        """Ensure the open accumulator is the window that CONTAINS elapsed_s,
        closing any earlier windows first. This attributes each event to its
        own window regardless of tick cadence or emit/tick ordering -- without
        it, an event arriving after a boundary that no tick has crossed yet
        would be mis-folded into the previous (already-elapsed) window."""
        self._lazy_init_window(elapsed_s)
        assert self._current_window_end_s is not None
        event_window_end = self._next_boundary(elapsed_s)
        while self._current_window_end_s < event_window_end:
            self._close_window(self._current_window_end_s)
            self._current_window_end_s += self._window_s

    def _next_boundary(self, elapsed_s: float) -> float:
        """Next multiple of window_s strictly greater than elapsed_s."""
        n_completed = int(elapsed_s // self._window_s)
        return (n_completed + 1) * self._window_s

    def _window_idx_for(self, window_end_s: float) -> int:
        """Window index derived from elapsed position: the count of completed
        windows at window_end_s, mod 4. Gap-robust (independent of how many
        windows actually emitted) so it maps cleanly to absolute time."""
        return int(round(window_end_s / self._window_s)) % WINDOW_IDX_MODULUS

    def _close_window(self, window_end_s: float) -> None:
        """Emit one SummaryEvent for the just-closed window (subject to
        emit_empty_windows), then reset the accumulator."""
        w = self._window
        if w.any_activity or self._emit_empty_windows:
            summary = SummaryEvent(
                window_idx=self._window_idx_for(window_end_s),
                code_counts=dict(w.code_counts),
                max_score=w.max_score,
                p2_present=w.p2_present,
                window_end_elapsed_s=window_end_s,
                last_gps_lat=w.last_gps_lat,
                last_gps_lon=w.last_gps_lon,
                last_location_status=w.last_location_status,
                anchor_lat=w.anchor_lat,
                anchor_lon=w.anchor_lon,
            )
            self._inner.emit_summary(summary)
            self._summaries_emitted += 1
        self._window = _WindowState()
