# Copyright (c) 2026 Imaginary Root Studio LLC. All rights reserved.
# Proprietary and confidential. See LICENSE.

"""P1BatchingTransport — wraps any Transport with per-type 5-minute P1 batching.

Motivation
----------

The 2026-06-02 radio team Disposition Contract v1.0 bench run surfaced two
related problems for high-density P1 streams:

- Bursty P1 events overflow the radio's intake queue (DROPPED_BUFFER_FULL).
- Even non-rejected frames can wait 20+ minutes between QUEUED and TRANSMITTED
  under contention.

P2 (safety-immediate) frames must stay per-event and real-time — but for P1
(medium-priority) we'd rather send fewer wire frames carrying aggregate
information. The raw events stay in the engine's local JSONL stream for
forensic analysis; nothing is lost on the sensor side.

Design (SB45_SIM_V3)
--------------------

- Window: 5-minute wall-clock-aligned tumbling windows on each Pi. Boundaries
  at elapsed_s multiples of `window_s` (default 300.0). Per-Pi clocks are
  driven by the engine's frame stream via `tick(ts_epoch_s, elapsed_s)`.

- Per-type accumulation: P1 events arriving via `emit()` are bucketed by
  `event_type`. The bucket records count, max_score, score_sum (for mean),
  and the last event's location + anchor.

- End-of-window send: at the first tick whose elapsed_s crosses a window
  boundary, every bucket with count >= 1 produces one SummaryEvent passed
  to the wrapped transport's `emit_summary()`. Empty buckets emit nothing.
  After flushing, buckets are cleared and the window-end advances by
  `window_s`.

- window_idx: per-event-type rolling 0..7 counter, advanced each time that
  type emits a summary. Lets the basestation detect a dropped summary frame
  for type X (e.g. via gap in the sequence).

- P2 and LOG events pass through unchanged. P2 is safety-immediate and must
  not be delayed. LOG is debatable — for v1 we pass through; future tuning
  can move LOG into a separate batch with a longer window if traffic warrants.

- `close()` flushes any open window with count >= 1, then closes the wrapped
  transport. Sessions that end mid-window still report their partial-window
  P1 activity.

Lifecycle
---------

    base = IpcTransport(endpoint_id=...) or FileTransport(path=...)
    batched = P1BatchingTransport(base)  # wrap
    engine = StreamingEngine(transport=batched)
    for frame in source:
        engine.consume(frame)  # consume() now also calls transport.tick()
    engine.flush()             # interval events
    batched.close()            # flushes any open window, then closes base
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from .transport import Event, SummaryEvent, Transport

log = logging.getLogger(__name__)


DEFAULT_WINDOW_S = 300.0  # 5 minutes


@dataclass
class _BucketState:
    """Per-event-type accumulator for one open 5-min window. Reset after
    the window emits its summary."""
    count: int = 0
    score_sum: float = 0.0
    max_score: float = 0.0
    last_gps_lat: Optional[float] = None
    last_gps_lon: Optional[float] = None
    last_location_status: str = "NO_LOCATION"
    anchor_lat: Optional[float] = None
    anchor_lon: Optional[float] = None

    def add(self, event: Event) -> None:
        self.count += 1
        self.score_sum += float(event.score)
        if event.score > self.max_score:
            self.max_score = float(event.score)
        self.last_gps_lat = event.gps_lat
        self.last_gps_lon = event.gps_lon
        # Engine doesn't tag location_status today; derive from gps presence
        # the same way IpcTransport.emit does for per-event frames.
        if event.gps_lat is not None and event.gps_lon is not None:
            self.last_location_status = "DIRECT_EVENT_LOCATION"
        else:
            self.last_location_status = "NO_LOCATION"
        self.anchor_lat = event.anchor_lat
        self.anchor_lon = event.anchor_lon

    def mean_score(self) -> float:
        if self.count == 0:
            return 0.0
        return self.score_sum / self.count


class P1BatchingTransport:
    """Wraps a Transport. P1 events are accumulated into per-type 5-minute
    summaries; P2 and LOG events pass through unchanged.

    The wrapped transport must implement the standard Transport protocol
    (emit, emit_summary, tick, close). `emit_summary` on the wrapped
    transport is what actually puts a summary on the wire.
    """

    def __init__(
        self,
        inner: Transport,
        window_s: float = DEFAULT_WINDOW_S,
        batch_log_priority: bool = False,
    ) -> None:
        if window_s <= 0.0:
            raise ValueError(f"window_s must be positive, got {window_s!r}")
        self._inner = inner
        self._window_s = float(window_s)
        # Also batch LOG events? Off by default; reserved for a follow-up
        # tuning step once we have data on how much LOG traffic matters.
        self._batch_log_priority = bool(batch_log_priority)

        # Per-event-type accumulators for the currently-open window.
        self._buckets: dict[str, _BucketState] = {}

        # Per-event-type rolling 0..7 window index, used as a dedup hint on
        # the basestation side.
        self._window_idx: dict[str, int] = {}

        # End-of-current-window in elapsed_s. None until first event/tick;
        # set on first call so the first window aligns with that elapsed_s.
        self._current_window_end_s: Optional[float] = None

        # Observability counters.
        self._p1_events_seen = 0
        self._summaries_emitted = 0

        log.info("P1BatchingTransport: window_s=%.0fs, batch_log=%s",
                 self._window_s, self._batch_log_priority)

    # -------------------------------------------------------------------------
    # Transport protocol
    # -------------------------------------------------------------------------

    def emit(self, event: Event) -> None:
        """Route an event by priority:
        - P2: pass through to inner transport immediately (real-time safety).
        - P1: accumulate into the per-type bucket for the current window.
        - LOG: pass through (unless --batch-log enabled, then accumulate too).
        """
        if event.priority == "P1" or (
            self._batch_log_priority and event.priority == "LOG"
        ):
            self._lazy_init_window(event.elapsed_s)
            bucket = self._buckets.setdefault(event.event_type, _BucketState())
            bucket.add(event)
            self._p1_events_seen += 1
        else:
            self._inner.emit(event)

    def emit_summary(self, summary: SummaryEvent) -> None:
        """Pass through to inner. Summaries generated by this batcher go
        through this method too; passing further-nested batchers would be
        unusual but the abstraction supports it."""
        self._inner.emit_summary(summary)

    def tick(self, ts_epoch_s: float, elapsed_s: float) -> None:
        """Advance the window clock. If `elapsed_s` has crossed one or more
        window boundaries since last tick, flush each crossed window in
        order (so multiple sparse ticks with gaps still produce the right
        sequence of summary frames). Then propagate the tick to the inner
        transport (no-op on FileTransport / IpcTransport)."""
        self._lazy_init_window(elapsed_s)
        # Defensive: typing-wise, _lazy_init_window guarantees this isn't None.
        assert self._current_window_end_s is not None

        while elapsed_s >= self._current_window_end_s:
            self._close_window(self._current_window_end_s)
            self._current_window_end_s += self._window_s

        self._inner.tick(ts_epoch_s, elapsed_s)

    def close(self) -> None:
        """Flush any open window (count >= 1 per type) and close inner.

        Sessions ending mid-window still get their partial-window stats
        reported. The summary's window_end_elapsed_s is the planned
        boundary, even though only a partial slice of the window ran;
        count tells the basestation how many events actually fired."""
        if self._current_window_end_s is not None:
            self._close_window(self._current_window_end_s)
        log.info(
            "P1BatchingTransport closing: p1_events_seen=%d, summaries_emitted=%d",
            self._p1_events_seen, self._summaries_emitted,
        )
        self._inner.close()

    # -------------------------------------------------------------------------
    # Observability
    # -------------------------------------------------------------------------

    @property
    def p1_events_seen(self) -> int:
        return self._p1_events_seen

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

    def _next_boundary(self, elapsed_s: float) -> float:
        """Next multiple of window_s strictly greater than elapsed_s. Wall-
        clock-aligned within the session's elapsed time."""
        n_completed = int(elapsed_s // self._window_s)
        return (n_completed + 1) * self._window_s

    def _close_window(self, window_end_s: float) -> None:
        """Emit one SummaryEvent per event_type with count >= 1, then clear
        the buckets. window_idx is advanced per type."""
        if not self._buckets:
            return
        for event_type in sorted(self._buckets.keys()):
            bucket = self._buckets[event_type]
            if bucket.count == 0:
                continue
            idx = self._window_idx.get(event_type, 0)
            summary = SummaryEvent(
                event_type=event_type,
                window_end_elapsed_s=window_end_s,
                window_idx=idx,
                count=bucket.count,
                max_score=bucket.max_score,
                mean_score=bucket.mean_score(),
                last_gps_lat=bucket.last_gps_lat,
                last_gps_lon=bucket.last_gps_lon,
                last_location_status=bucket.last_location_status,
                anchor_lat=bucket.anchor_lat,
                anchor_lon=bucket.anchor_lon,
            )
            self._inner.emit_summary(summary)
            self._summaries_emitted += 1
            self._window_idx[event_type] = (idx + 1) % 8
        self._buckets.clear()
