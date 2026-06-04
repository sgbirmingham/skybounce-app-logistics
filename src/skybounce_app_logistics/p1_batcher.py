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

- Summary jitter: when N P1 types fire in the same window, emitting all N
  summary frames simultaneously at the boundary creates exactly the burst
  the radio's intake queue can't absorb (this is the failure mode we saw
  on 2026-06-04 -- the run had to be aborted). To avoid it, summaries are
  not emitted directly; they go into a pending-emit queue with target
  ts_epoch_s offsets `0, jitter/N, 2*jitter/N, ...` and are drained on
  subsequent `tick()` calls as ts_epoch_s advances past each target. The
  engine never blocks on time.sleep -- the per-frame tick cadence drives
  the staggered emission naturally. Default jitter = 60 sec; 0 disables.
  Must be < `window_s` so a window's emissions finish before the next
  window closes.

- `close()` flushes any open window with count >= 1, then flushes any
  still-pending deferred emits regardless of their scheduled time
  (sessions ending mid-jitter still get their summaries on the wire),
  then closes the wrapped transport. Sessions that end mid-window also
  report their partial-window P1 activity.

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
DEFAULT_SUMMARY_JITTER_S = 60.0  # spread N summaries over 60 s at window close


@dataclass
class _PendingEmit:
    """One summary scheduled for deferred emission. The batcher's tick()
    drains entries whose not_before_ts_epoch_s has been reached."""
    summary: "SummaryEvent"
    not_before_ts_epoch_s: float


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
        summary_jitter_s: float = DEFAULT_SUMMARY_JITTER_S,
        batch_log_priority: bool = False,
    ) -> None:
        if window_s <= 0.0:
            raise ValueError(f"window_s must be positive, got {window_s!r}")
        if summary_jitter_s < 0.0:
            raise ValueError(
                f"summary_jitter_s must be >= 0, got {summary_jitter_s!r}"
            )
        if summary_jitter_s >= window_s:
            raise ValueError(
                f"summary_jitter_s ({summary_jitter_s}) must be < window_s "
                f"({window_s}); otherwise the next window starts before the "
                f"previous window's summaries finish emitting."
            )
        self._inner = inner
        self._window_s = float(window_s)
        self._summary_jitter_s = float(summary_jitter_s)
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

        # Deferred-emit queue: summaries pending release as ts_epoch_s
        # advances past each entry's not_before time. _close_window()
        # appends; tick() and close() drain.
        self._pending_emits: list[_PendingEmit] = []

        # Last tick's ts_epoch_s -- the clock against which not_before is
        # compared. None until the first tick.
        self._last_tick_ts_epoch_s: Optional[float] = None

        # Observability counters.
        self._p1_events_seen = 0
        self._summaries_emitted = 0

        log.info("P1BatchingTransport: window_s=%.0fs, jitter=%.0fs, batch_log=%s",
                 self._window_s, self._summary_jitter_s,
                 self._batch_log_priority)

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
        """Advance the window clock. Three things happen, in order:

        1. If `elapsed_s` has crossed one or more window boundaries since
           last tick, queue each crossed window's summaries with staggered
           not_before timestamps (so they don't burst into the radio's
           intake queue simultaneously).
        2. Drain any pending emits whose not_before has been reached --
           including the just-queued i=0 entry, which has not_before equal
           to this tick's ts_epoch_s and so fires immediately.
        3. Propagate the tick to the inner transport (no-op on
           FileTransport / IpcTransport).
        """
        self._last_tick_ts_epoch_s = ts_epoch_s
        self._lazy_init_window(elapsed_s)
        # Defensive: typing-wise, _lazy_init_window guarantees this isn't None.
        assert self._current_window_end_s is not None

        while elapsed_s >= self._current_window_end_s:
            self._close_window(self._current_window_end_s)
            self._current_window_end_s += self._window_s

        self._drain_pending(ts_epoch_s)
        self._inner.tick(ts_epoch_s, elapsed_s)

    def close(self) -> None:
        """Flush any open window and any still-pending deferred emits, then
        close the wrapped transport.

        Sessions ending mid-window still get their partial-window stats
        reported (the summary's window_end_elapsed_s is the planned
        boundary; count tells the basestation how many events actually
        fired in the partial slice).

        Sessions ending mid-jitter still get their staggered emits on the
        wire (we ignore the not_before times here so nothing is lost --
        the staggering only matters while the session is live and trying
        to avoid intake-queue bursts)."""
        if self._current_window_end_s is not None:
            self._close_window(self._current_window_end_s)
        # Drain everything left, ignoring not_before timestamps.
        for entry in self._pending_emits:
            self._inner.emit_summary(entry.summary)
            self._summaries_emitted += 1
        self._pending_emits.clear()
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
        """Queue one SummaryEvent per event_type with count >= 1 for deferred
        emission, with not_before timestamps spaced 0, jitter/N, 2*jitter/N,
        ... so the N entries don't burst into the inner transport at the
        same wall-clock instant. window_idx is advanced per type at queue
        time (the basestation sees indexes in the order summaries are
        conceptually emitted, even though wire-time is staggered)."""
        # Buckets snapshot -> empty before anything else, so the next window
        # starts clean even if pending-emit queueing throws.
        bucket_items = sorted(self._buckets.items())
        self._buckets.clear()

        pending_summaries: list[SummaryEvent] = []
        for event_type, bucket in bucket_items:
            if bucket.count == 0:
                continue
            idx = self._window_idx.get(event_type, 0)
            pending_summaries.append(SummaryEvent(
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
            ))
            self._window_idx[event_type] = (idx + 1) % 8

        n = len(pending_summaries)
        if n == 0:
            return

        # Base ts for the staggered offsets: the latest tick we've seen.
        # In normal operation _close_window is called from tick(), which
        # set _last_tick_ts_epoch_s on entry, so this is non-None. If a
        # caller closes a window outside tick() (e.g. close()), we fall
        # through to a 0-base which makes every entry immediately drainable.
        base_ts = self._last_tick_ts_epoch_s if self._last_tick_ts_epoch_s is not None else 0.0
        spacing = (self._summary_jitter_s / n) if self._summary_jitter_s > 0 else 0.0
        for i, summary in enumerate(pending_summaries):
            self._pending_emits.append(_PendingEmit(
                summary=summary,
                not_before_ts_epoch_s=base_ts + i * spacing,
            ))

    def _drain_pending(self, ts_epoch_s: float) -> None:
        """Emit any pending summaries whose not_before time has been reached.
        Order is preserved; entries are removed in place. Called from tick()
        after _close_window() so the i=0 entry queued this tick (offset 0)
        fires immediately."""
        if not self._pending_emits:
            return
        still_pending: list[_PendingEmit] = []
        for entry in self._pending_emits:
            if entry.not_before_ts_epoch_s <= ts_epoch_s:
                self._inner.emit_summary(entry.summary)
                self._summaries_emitted += 1
            else:
                still_pending.append(entry)
        self._pending_emits = still_pending
