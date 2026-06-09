"""
transport.py

Transports for streaming events. Two implementations:

- FileTransport: writes one JSON object per event to a file, one event per
  line. Used for offline replay validation and during development. No IPC
  dependency.

- IpcTransport: encodes events using SB45_SIM_V2 and submits them via the
  SkyBounce IPC client. Used on the Pi for real radio operation. Imports
  from skybounce-ipc-python at construction time, so this file is safe to
  import even when the IPC stack isn't installed.

Both implement the same Transport protocol: emit(event) and close().
"""

from __future__ import annotations

import collections
import json
import logging
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional, Protocol


log = logging.getLogger("skybounce.app.logistics.transport")


# -----------------------------------------------------------------------------
# Event record
# -----------------------------------------------------------------------------

@dataclass
class Event:
    """One detected event. Carries everything the transport layer needs to
    build a packet, plus debug context for the file transport.
    """
    ts_epoch_s: float
    elapsed_s: float
    event_type: str
    score: float
    priority: str               # "P2" | "P1" | "P0"
    event_class: str
    packet_rank: int
    policy_reason: str
    detail: str
    speed_m_s: float
    speed_mph: float
    decel_m_s2: float
    accel_m_s2: float
    lin_accel_g: float
    jerk_g_s: float
    gps_confidence: float
    gps_lat: Optional[float]
    gps_lon: Optional[float]
    anchor_lat: Optional[float]
    anchor_lon: Optional[float]
    analyzer_state: str


# -----------------------------------------------------------------------------
# Batch-summary event (SB45_SIM_V3)
# -----------------------------------------------------------------------------

@dataclass
class SummaryEvent:
    """One per-window (15-min) P1 batch summary, ready for SB45_SIM_V4 packing.

    Produced by the P1BatchingTransport at window close; consumed by the
    underlying Transport's emit_summary(). The IPC transport packs this via
    pack_sb45_summary_v4; the file transport writes it as a marked JSON line.

    One frame aggregates ALL P1 event types that fired in the window (V4
    replaces V3's one-frame-per-type model).

    Fields:
        window_idx: 0..31, derived from the window position (gap-robust); the
            basestation combines it with its clock to place the window in
            absolute time.
        code_counts: {event_type: count} for P1 events in the window. The IPC
            transport maps these to fixed 2-bit count buckets (scheme B); only
            P1-capable types have a slot.
        max_score: peak severity over the window (P1 and P2), 0..1.
        p2_present: True iff >=1 P2 event fired this window (P2s are sent
            per-event, so this is the only summary-side record of one).
        window_end_elapsed_s: elapsed-time of the window boundary that closed
            (for logging / the file transport; not carried on the V4 wire).
        last_gps_lat / last_gps_lon / last_location_status: last event's
            position + fix status in the window.
        anchor_lat / anchor_lon: session anchor (first valid fix).
    """
    window_idx: int
    code_counts: dict[str, int]
    max_score: float
    p2_present: bool
    window_end_elapsed_s: float
    last_gps_lat: Optional[float]
    last_gps_lon: Optional[float]
    last_location_status: str
    anchor_lat: Optional[float]
    anchor_lon: Optional[float]


# -----------------------------------------------------------------------------
# Transport interface
# -----------------------------------------------------------------------------

class Transport(Protocol):
    def emit(self, event: Event) -> None: ...
    def emit_summary(self, summary: SummaryEvent) -> None: ...
    def tick(self, ts_epoch_s: float, elapsed_s: float) -> None: ...
    def close(self, drain_timeout_s: Optional[float] = None) -> None: ...


# -----------------------------------------------------------------------------
# File transport
# -----------------------------------------------------------------------------

class FileTransport:
    """Append events as JSON lines to a file. Each line is a self-contained
    record, so the file is consumable by `jq`, `pandas.read_json(lines=True)`,
    or our regression check script.

    Pass mode="w" to overwrite, "a" (default) to append.
    """

    def __init__(self, path: Path, mode: str = "w") -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._f = open(path, mode, encoding="utf-8")
        self._path = path
        self._count = 0
        log.info("file transport writing to %s", path)

    def emit(self, event: Event) -> None:
        record = asdict(event)
        self._f.write(json.dumps(record, sort_keys=True) + "\n")
        self._f.flush()  # one event = one line on disk, every time
        self._count += 1

    def emit_summary(self, summary: SummaryEvent) -> None:
        """Write a P1 batch-summary record as a JSON line. Marked with
        `record_type: "summary"` so downstream readers can distinguish summary
        lines from per-event lines (which have no `record_type` field today)."""
        record = asdict(summary)
        record["record_type"] = "summary"
        record["sb45_layout_version"] = "SB45_SIM_V4"
        self._f.write(json.dumps(record, sort_keys=True) + "\n")
        self._f.flush()
        self._count += 1

    def tick(self, ts_epoch_s: float, elapsed_s: float) -> None:
        """No-op for the file transport. The P1BatchingTransport calls this
        on every wrapped transport to keep the interface uniform, but the
        file transport has no time-based bookkeeping of its own."""
        pass

    def close(self, drain_timeout_s: Optional[float] = None) -> None:
        # drain_timeout_s is accepted for interface parity (the batcher forwards
        # it); the file transport writes synchronously, so there's nothing to drain.
        log.info("file transport closing, %d records written to %s",
                 self._count, self._path)
        self._f.close()

    @property
    def event_count(self) -> int:
        return self._count


# -----------------------------------------------------------------------------
# IPC transport
# -----------------------------------------------------------------------------

class IpcTransport:
    """Encode events using SB45_SIM_V2 and submit to the SkyBounce IPC client.

    Imports from skybounce_ipc and sb_telemetry_payload happen at construction
    time so this module can still be imported (and FileTransport used) when
    the IPC stack isn't installed -- which matters for replay-mode validation
    on a workstation.

    Registers handlers for CMD, TELEMETRY_ACK, and STATUS at construction so
    the app layer has visibility into the IPC counter-flow. The CMD handler is
    a v0.1 placeholder that logs and returns OK; replace when real CMD
    vocabulary is defined. The TELEMETRY_ACK and STATUS handlers are
    log + counter only.
    """

    # Map analyzer-style priority strings to IPC Priority enum values.
    # Centralized here so the mapping is one place to audit.
    # App priority (P0/P1/P2) -> IPC Telemetry.priority enum (the radio's
    # contract field). P2 events submit as CRITICAL so the bridge's preemption
    # path (higher priority evicts lower) can act on them.
    _PRIORITY_MAP = {
        "P2": "CRITICAL",
        "P1": "ELEVATED",
        "P0": "NORMAL",
    }

    # Default close() drain budget. Long enough to absorb a burst-submit
    # finishing the writer queue plus the daemon's QUEUED -> DELIVERED hop;
    # short enough that a stuck daemon doesn't hang process shutdown.
    _DEFAULT_CLOSE_DRAIN_TIMEOUT_S = 5.0

    # Poll cadence while waiting for terminal dispositions. 20 ms keeps
    # the wait responsive without busy-looping; ACKs only flow during
    # daemon round-trips so finer granularity buys nothing.
    _DRAIN_POLL_INTERVAL_S = 0.02

    def __init__(
        self,
        endpoint_id: int,
        socket_path: Optional[str] = None,
        max_submit_rate_per_min: float = 0.0,
    ) -> None:
        # Defer imports until construction so module imports cleanly without IPC.
        try:
            from skybounce_client import SkyBounceClient
            from skybounce_ipc import (
                CmdResult, Disposition, LinkState, Priority,
            )
            from sb_telemetry_payload import (
                SB45Event, SB45SummaryV4, SB45PrecisePosition,
                pack_sb45, pack_sb45_summary_v4, pack_sb45_precise,
                SUMMARY_SLOT_OF_TYPE,
            )
        except ImportError as e:
            raise ImportError(
                "IPC transport requires skybounce-ipc-python on PYTHONPATH. "
                f"Original error: {e}"
            ) from e

        self._SB45Event = SB45Event
        self._SB45SummaryV4 = SB45SummaryV4
        self._SB45PrecisePosition = SB45PrecisePosition
        self._pack_sb45 = pack_sb45
        self._pack_sb45_summary_v4 = pack_sb45_summary_v4
        self._pack_sb45_precise = pack_sb45_precise
        self._summary_slot_of_type = SUMMARY_SLOT_OF_TYPE
        self._Priority = Priority
        self._CmdResult = CmdResult
        self._Disposition = Disposition
        self._LinkState = LinkState

        # Observability counters; surfaced in close() summary and via the
        # ack_counts / cmds_received properties (for tests, health checks,
        # or a future operator endpoint).
        self._count = 0
        self._cmds_received = 0
        self._p0_frames_skipped = 0
        self._precise_frames_sent = 0
        self._preempted_count = 0
        self._ack_counts: dict[int, int] = {}
        self._last_link_state: Optional[int] = None

        # DROPPED_PREEMPTED: the radio team's preemption path sheds a
        # lower-priority frame (in practice, one of our P1 summaries) to admit a
        # higher-priority P2 into a full queue. That's expected under contention
        # -- distinct from DROPPED_BUFFER_FULL (a real capacity stall). Resolved
        # from the IPC Disposition enum so it auto-activates when the contract
        # ships the code; None (and inert) until then. See the 2026-06-07
        # preemption thread.
        self._DROPPED_PREEMPTED = getattr(self._Disposition, "DROPPED_PREEMPTED", None)

        # Retry-on-BUFFER_FULL was removed 2026-06-07: the radio team's finding
        # is that re-submitting on 0x80 adds churn without goodput at
        # saturation -- the retries are part of what overflows the intake
        # queue. The correct response is to not overflow in the first place
        # (proactive pacing/backpressure), which SB45_SIM_V4 per-window
        # batching already achieves (steady-state offered load ~12/hr < the
        # radio's ~15/hr service rate). A frame that still hits 0x80 is logged
        # and counted as a terminal failure, not re-sent.

        # Sliding-60s submission rate limit. Applies to every wire submission
        # (emit, emit_summary). 0 = unlimited (existing behavior).
        # When the cap is reached, _rate_limited_submit_telemetry sleeps until
        # the oldest timestamp ages out. recent_submit_ts is only touched
        # from the engine's tick/emit thread, never from the ACK handler,
        # so no lock needed.
        self._max_submit_rate_per_min = float(max_submit_rate_per_min)
        self._recent_submit_ts: collections.deque = collections.deque()
        self._rate_limit_sleeps = 0

        # P2 resubmit: when a CRITICAL-priority frame (P2 event or precise-
        # position) gets DROPPED_BUFFER_FULL, re-submit it until delivered.
        # The resubmit thread wakes on _resubmit_ready, pops one entry,
        # waits an exponential backoff (1s, 2s, 4s, ... capped at 30s),
        # and re-submits.  No attempt limit — P2 crash frames keep trying
        # until they get through or the transport closes.
        self._P2_RESUBMIT_INITIAL_BACKOFF_S = 1.0
        self._P2_RESUBMIT_MAX_BACKOFF_S = 30.0
        self._p2_pending: dict[int, tuple[bytes, Any, int]] = {}
        self._p2_pending_lock = threading.Lock()
        self._resubmit_queue: list[tuple[bytes, Any, int]] = []
        self._resubmit_lock = threading.Lock()
        self._resubmit_ready = threading.Event()
        self._resubmit_count = 0
        self._resubmit_inflight = 0
        self._resubmit_inflight_lock = threading.Lock()
        self._resubmit_stop = threading.Event()

        # Guard against double-close (drain timeout makes calling close()
        # twice expensive otherwise, and SkyBounceClient.stop() is not
        # documented as idempotent).
        self._closed = False

        self._client = SkyBounceClient(endpoint_id=endpoint_id, socket_path=socket_path)

        # Register handlers BEFORE start() so the reader thread sees them
        # when the first non-handshake frame arrives.
        self._client.set_cmd_handler(self._on_cmd)
        self._client.set_telemetry_ack_handler(self._on_telemetry_ack)
        self._client.set_status_handler(self._on_status)

        self._client.start()

        self._resubmit_thread = threading.Thread(
            target=self._resubmit_loop, daemon=True, name="p2-resubmit")
        self._resubmit_thread.start()

        log.info(
            "IPC transport started, endpoint_id=0x%08X, rate_limit=%s/min "
            "(P2 resubmit: unlimited, backoff %.1fs-%.1fs)",
            endpoint_id,
            f"{self._max_submit_rate_per_min:.0f}"
            if self._max_submit_rate_per_min > 0 else "unlimited",
            self._P2_RESUBMIT_INITIAL_BACKOFF_S,
            self._P2_RESUBMIT_MAX_BACKOFF_S,
        )

    # -------------------------------------------------------------------------
    # Handlers
    # -------------------------------------------------------------------------

    def _on_cmd(self, cmd_code: int, data: bytes) -> tuple:
        """v0.1 placeholder: log every CMD and return OK with no data.

        Logistics v0.1 has no CMD vocabulary. Returning OK (rather than
        ERR_UNKNOWN_CODE as the library default would) keeps a future bench
        session against sensor_app_stub.py compatible -- the stub OKs all
        CMDs too. When real CMD types are defined, replace this with a
        dispatch table that returns the appropriate CmdResult per cmd_code.
        """
        self._cmds_received += 1
        log.info(
            "CMD rx (#%d) cmd_code=0x%02X data_len=%d -> OK (no v0.1 handler)",
            self._cmds_received, cmd_code, len(data),
        )
        return self._CmdResult.OK, b""

    def _on_telemetry_ack(self, ack) -> None:
        """Log disposition and bump per-disposition counter.

        Terminal failures (high bit set per spec Section 7.4) log at WARNING
        with a running failure total; non-terminal dispositions log at INFO.

        DROPPED_BUFFER_FULL (0x80) on a CRITICAL frame (P2/precise) is
        intercepted for resubmit -- the payload is queued for retry until
        delivered. Non-CRITICAL 0x80s remain terminal failures.
        """
        disp = int(ack.disposition)
        self._ack_counts[disp] = self._ack_counts.get(disp, 0) + 1
        name = self._disposition_name(disp)
        tid = ack.telemetry_id

        if self._DROPPED_PREEMPTED is not None and disp == self._DROPPED_PREEMPTED:
            self._preempted_count += 1
            with self._p2_pending_lock:
                self._p2_pending.pop(tid, None)
            log.info(
                "TELEMETRY_ACK tid=%d disposition=%s reason=0x%04X "
                "(shed for a higher-priority frame; expected under contention)",
                tid, name, ack.reason_code,
            )
            return

        if disp & 0x80:
            with self._p2_pending_lock:
                entry = self._p2_pending.pop(tid, None)
            if entry is not None and disp == int(self._Disposition.DROPPED_BUFFER_FULL):
                payload, priority, attempt = entry
                log.info(
                    "TELEMETRY_ACK tid=%d BUFFER_FULL on CRITICAL frame "
                    "(attempt %d) -- queuing for resubmit",
                    tid, attempt,
                )
                with self._resubmit_lock:
                    self._resubmit_queue.append((payload, priority, attempt + 1))
                self._resubmit_ready.set()
                self._count -= 1
                return
            else:
                failed_total = sum(c for d, c in self._ack_counts.items() if d & 0x80)
                log.warning(
                    "TELEMETRY_ACK tid=%d disposition=%s reason=0x%04X "
                    "(failures so far: %d)",
                    tid, name, ack.reason_code, failed_total,
                )
        else:
            if disp == int(self._Disposition.DELIVERED):
                with self._p2_pending_lock:
                    self._p2_pending.pop(tid, None)
            log.info(
                "TELEMETRY_ACK tid=%d disposition=%s reason=0x%04X",
                tid, name, ack.reason_code,
            )

    def _on_status(self, status) -> None:
        """Log link-state transitions; steady-state STATUS is DEBUG-only.

        Per the v1.0 known limitations, DEGRADED is never emitted in this
        spec version -- only DOWN / ACQUIRING / UP. A DEGRADED would still
        log here cleanly if a future spec version introduces it.
        """
        ls = int(status.link_state)
        if self._last_link_state == ls:
            log.debug(
                "STATUS link_state=%s queue_depth=%d",
                self._link_state_name(ls), status.queue_depth,
            )
            return
        prev_name = (
            self._link_state_name(self._last_link_state)
            if self._last_link_state is not None
            else "UNKNOWN"
        )
        log.info(
            "link state %s -> %s, queue_depth=%d",
            prev_name, self._link_state_name(ls), status.queue_depth,
        )
        self._last_link_state = ls

    def _disposition_name(self, disp: int) -> str:
        try:
            return self._Disposition(disp).name
        except ValueError:
            return f"0x{disp:02X}"

    def _link_state_name(self, ls: int) -> str:
        try:
            return self._LinkState(ls).name
        except ValueError:
            return f"0x{ls:02X}"

    # -------------------------------------------------------------------------
    # P2 resubmit loop (background thread)
    # -------------------------------------------------------------------------

    def _resubmit_loop(self) -> None:
        """Pop rejected CRITICAL frames from _resubmit_queue and re-submit
        them after an exponential backoff. Runs on a daemon thread; exits
        when _resubmit_stop is set and queue + inflight are both empty."""
        while True:
            self._resubmit_ready.wait(timeout=0.5)
            if self._resubmit_stop.is_set():
                with self._resubmit_lock:
                    if not self._resubmit_queue:
                        return
            with self._resubmit_lock:
                if not self._resubmit_queue:
                    self._resubmit_ready.clear()
                    if self._resubmit_stop.is_set():
                        return
                    continue
                payload, priority, attempt = self._resubmit_queue.pop(0)
            with self._resubmit_inflight_lock:
                self._resubmit_inflight += 1
            backoff = min(
                self._P2_RESUBMIT_INITIAL_BACKOFF_S * (2 ** (attempt - 1)) if attempt > 0 else self._P2_RESUBMIT_INITIAL_BACKOFF_S,
                self._P2_RESUBMIT_MAX_BACKOFF_S,
            )
            time.sleep(backoff)
            tid = self._rate_limited_submit_telemetry(
                payload, priority, _resubmit_attempt=attempt)
            self._count += 1
            self._resubmit_count += 1
            with self._resubmit_inflight_lock:
                self._resubmit_inflight -= 1
            log.info(
                "P2 resubmit: tid=%d attempt=%d backoff=%.1fs",
                tid, attempt, backoff,
            )

    # -------------------------------------------------------------------------
    # Submit helpers (rate-limited)
    # -------------------------------------------------------------------------

    def _rate_limited_submit_telemetry(self, bytes_payload: bytes,
                                       priority: Any,
                                       _resubmit_attempt: int = 0) -> int:
        """Wrap client.submit_telemetry() with a sliding-60s rate cap.

        CRITICAL-priority frames are registered in _p2_pending so the ACK
        handler can intercept BUFFER_FULL and queue them for bounded resubmit.
        """
        cap = self._max_submit_rate_per_min
        if cap > 0:
            now = time.monotonic()
            cutoff = now - 60.0
            while self._recent_submit_ts and self._recent_submit_ts[0] < cutoff:
                self._recent_submit_ts.popleft()
            if len(self._recent_submit_ts) >= cap:
                sleep_until = self._recent_submit_ts[0] + 60.0
                sleep_s = sleep_until - now
                if sleep_s > 0:
                    log.info(
                        "rate-limit: pausing %.1fs (cap=%.0f/min, "
                        "%d submits in last 60s)",
                        sleep_s, cap, len(self._recent_submit_ts),
                    )
                    time.sleep(sleep_s)
                    self._rate_limit_sleeps += 1
                cutoff = time.monotonic() - 60.0
                while self._recent_submit_ts and self._recent_submit_ts[0] < cutoff:
                    self._recent_submit_ts.popleft()
        tid = self._client.submit_telemetry(bytes_payload, priority=priority)
        self._recent_submit_ts.append(time.monotonic())
        if priority == getattr(self._Priority, "CRITICAL"):
            with self._p2_pending_lock:
                self._p2_pending[tid] = (bytes_payload, priority, _resubmit_attempt)
        return tid

    def _pack_and_submit_event(self, event: Event) -> int:
        """Pack an Event as SB45 and submit it (rate-limited)."""
        location_status = (
            "DIRECT_EVENT_LOCATION"
            if event.gps_lat is not None and event.gps_lon is not None
            else "NO_LOCATION"
        )
        sb45_event = self._SB45Event(
            event_type=event.event_type,
            priority=event.priority,
            location_status=location_status,
            score=event.score,
            speed_mph=event.speed_mph,
            peak_decel_m_s2=event.decel_m_s2,
            gps_confidence=event.gps_confidence,
            elapsed_s=event.elapsed_s,
            lat=event.gps_lat,
            lon=event.gps_lon,
            anchor_lat=event.anchor_lat,
            anchor_lon=event.anchor_lon,
        )
        packed = self._pack_sb45(sb45_event)
        ipc_prio_name = self._PRIORITY_MAP.get(event.priority, "NORMAL")
        ipc_prio = getattr(self._Priority, ipc_prio_name)
        return self._rate_limited_submit_telemetry(packed.bytes_payload, ipc_prio)

    def _pack_and_submit_summary(self, summary: SummaryEvent) -> int:
        """Pack a SummaryEvent as an SB45_SIM_V4 per-window summary and submit
        it (rate-limited).

        code_counts is filtered to types that have a V4 summary slot; a
        non-slottable (non-P1-capable) type would make pack_sb45_summary_v4
        raise, which must not crash the wire path mid-drive, so we drop it with
        a warning. In practice every P1 type the engine emits has a slot."""
        slottable = {
            t: c for t, c in summary.code_counts.items()
            if t in self._summary_slot_of_type
        }
        dropped = set(summary.code_counts) - set(slottable)
        if dropped:
            log.warning(
                "V4 summary: dropping non-slottable P1 type(s) %s from code_counts",
                sorted(dropped),
            )
        sb45_summary = self._SB45SummaryV4(
            window_idx=summary.window_idx,
            code_counts=slottable,
            max_score=summary.max_score,
            location_status=summary.last_location_status,
            p2_present=summary.p2_present,
            lat=summary.last_gps_lat,
            lon=summary.last_gps_lon,
            anchor_lat=summary.anchor_lat,
            anchor_lon=summary.anchor_lon,
        )
        packed = self._pack_sb45_summary_v4(sb45_summary)
        ipc_prio = getattr(self._Priority, "ELEVATED")
        return self._rate_limited_submit_telemetry(packed.bytes_payload, ipc_prio)

    # -------------------------------------------------------------------------
    # Transport protocol
    # -------------------------------------------------------------------------

    def emit(self, event: Event) -> None:
        # P0 ("local-only context", formerly LOG/NORMAL) is NOT radioed -- it
        # stays in the local stream only. Sending it consumed ~half the
        # offered-load budget in the 2026-06-07 capacity sim, and node liveness
        # is already covered by the IPC contract heartbeat, so P0 carries no
        # signal worth the air-time. Only P1/P2 go OTA; the file transport still
        # records P0 for forensics.
        if event.priority == "P0":
            self._p0_frames_skipped += 1
            return
        self._pack_and_submit_event(event)
        self._count += 1
        # P2 (severe_impact) gets a companion precise-position frame for
        # real-time crash dispatch (region-absolute ~62 m). Parked/session-start
        # precise position is forensic (on-Pi log), not radioed -- so the on-wire
        # precise trigger is P2 only. See the V4 decision doc, Addendum 3.
        if event.priority == "P2" and event.gps_lat is not None \
                and event.gps_lon is not None:
            packed = self._pack_sb45_precise(
                self._SB45PrecisePosition(lat=event.gps_lat, lon=event.gps_lon)
            )
            # CRITICAL like the P2 it locates, so preemption protects it too.
            self._rate_limited_submit_telemetry(
                packed.bytes_payload, getattr(self._Priority, "CRITICAL"))
            self._precise_frames_sent += 1
            self._count += 1

    def emit_summary(self, summary: SummaryEvent) -> None:
        """Pack a P1 per-window batch summary via SB45_SIM_V4
        (pack_sb45_summary_v4) and submit it as a TELEMETRY frame.
        pack_sb45_summary_v4 hard-codes the SB45 priority field to
        BATCH_SUMMARY (=3); the IPC frame's own priority is ELEVATED (same as
        a P1 per-event) because a summary REPRESENTS P1 events and we don't
        want the radio's TX scheduler to de-prioritize it.
        """
        tid = self._pack_and_submit_summary(summary)
        self._count += 1
        log.info(
            "emit_summary: tid=%d window_idx=%d window_end=%.0fs "
            "p1_types=%d p1_total=%d max=%.2f p2_present=%s",
            tid, summary.window_idx, summary.window_end_elapsed_s,
            len(summary.code_counts), sum(summary.code_counts.values()),
            summary.max_score, summary.p2_present,
        )

    def tick(self, ts_epoch_s: float, elapsed_s: float) -> None:
        """Per-frame pulse from the engine. No-op for the IPC transport: with
        retry-on-0x80 removed there is no per-tick work to do here. Kept to
        satisfy the Transport protocol (the P1 batcher calls tick() on its
        wrapped transport)."""
        pass

    def close(self, drain_timeout_s: Optional[float] = None) -> None:
        """Drain pending submissions (best-effort), log summary, stop client.

        Before tearing down the IPC socket, wait up to drain_timeout_s for
        every telemetry submitted via emit() to receive a terminal disposition
        -- DELIVERED, or any failure code per spec Section 7.4 (high bit set).
        SkyBounceClient.stop() closes the socket immediately and does not drain
        its writer queue, so without this wait the replay-mode pattern of
        "submit a burst, then immediately close" drops in-flight frames.

        If the deadline passes with submissions still outstanding, logs a
        WARNING with the outstanding count and proceeds to stop -- the daemon
        will see the disconnect and stop ACKing, but at least the operator
        knows how much was lost.

        Idempotent: a second call is a no-op.
        """
        if self._closed:
            return
        self._closed = True

        if drain_timeout_s is None:
            drain_timeout_s = self._DEFAULT_CLOSE_DRAIN_TIMEOUT_S
        # drain_timeout_s <= 0 means drain UNTIL every frame is terminal, with no
        # deadline -- so a backed-up radio queue (drains at ~mu) isn't abandoned
        # at socket close. Interruptible with Ctrl-C / SIGINT.
        unlimited = drain_timeout_s <= 0

        if self._count > 0:
            delivered = int(self._Disposition.DELIVERED)

            def _terminal_count() -> int:
                return sum(
                    c for d, c in self._ack_counts.items()
                    if d == delivered or (d & 0x80)
                )

            def _resubmit_active() -> bool:
                with self._resubmit_lock:
                    queued = bool(self._resubmit_queue)
                with self._resubmit_inflight_lock:
                    inflight = self._resubmit_inflight > 0
                return queued or inflight

            deadline = None if unlimited else time.monotonic() + drain_timeout_s
            next_progress = time.monotonic() + 30.0
            while (_terminal_count() < self._count or _resubmit_active()) and (
                deadline is None or time.monotonic() < deadline
            ):
                time.sleep(self._DRAIN_POLL_INTERVAL_S)
                if unlimited and time.monotonic() >= next_progress:
                    log.info(
                        "IPC transport draining: %d/%d frames terminal -- waiting "
                        "for the radio to deliver its backlog (Ctrl-C to stop)",
                        _terminal_count(), self._count,
                    )
                    next_progress = time.monotonic() + 30.0

            outstanding = self._count - _terminal_count()
            if outstanding > 0:
                log.warning(
                    "IPC transport close: drain deadline (%.1fs) reached with "
                    "%d/%d telemetries terminal; %d in-flight frames will be "
                    "dropped at socket close",
                    drain_timeout_s, _terminal_count(), self._count, outstanding,
                )

        self._resubmit_stop.set()
        self._resubmit_ready.set()
        self._resubmit_thread.join(timeout=5.0)

        log.info(
            "IPC transport closing, %d frames submitted, %d P0 frames "
            "skipped (not radioed), %d preempted (shed for higher priority)",
            self._count, self._p0_frames_skipped, self._preempted_count)
        if self._resubmit_count:
            log.info("P2 resubmit: %d resubmissions", self._resubmit_count)
        if self._ack_counts:
            summary = ", ".join(
                f"{self._disposition_name(d)}={c}"
                for d, c in sorted(self._ack_counts.items())
            )
            log.info("TELEMETRY_ACK summary: %s", summary)
        if self._cmds_received:
            log.info("CMDs received during session: %d", self._cmds_received)
        self._client.stop()

    # -------------------------------------------------------------------------
    # Observability accessors
    # -------------------------------------------------------------------------

    @property
    def event_count(self) -> int:
        return self._count

    @property
    def ack_counts(self) -> dict[int, int]:
        """Per-disposition ACK counts. Read-only snapshot."""
        return dict(self._ack_counts)

    @property
    def cmds_received(self) -> int:
        return self._cmds_received

    @property
    def preempted_count(self) -> int:
        """Frames terminated by DROPPED_PREEMPTED (shed to admit a
        higher-priority frame). 0 until the contract defines the code."""
        return self._preempted_count
