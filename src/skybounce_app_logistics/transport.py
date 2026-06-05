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
    priority: str               # "P2" | "P1" | "LOG"
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
    """One 5-minute per-type P1 window summary, ready for SB45_SIM_V3 packing.

    Produced by the P1BatchingTransport at window close; consumed by the
    underlying Transport's emit_summary(). The IPC transport packs this via
    pack_sb45_summary; the file transport writes it as a marked JSON line.

    `window_end_elapsed_s` is the elapsed-time of the window boundary that
    just closed; the basestation reads this as the SB45 `elapsed_min_bin`.

    `window_idx` is a per-event-type rolling 0..7 counter, advanced once each
    time a summary is emitted for that type. Lets the basestation detect a
    dropped window for type X.
    """
    event_type: str
    window_end_elapsed_s: float
    window_idx: int
    count: int
    max_score: float
    mean_score: float
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
    def close(self) -> None: ...


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
        record["sb45_layout_version"] = "SB45_SIM_V3"
        self._f.write(json.dumps(record, sort_keys=True) + "\n")
        self._f.flush()
        self._count += 1

    def tick(self, ts_epoch_s: float, elapsed_s: float) -> None:
        """No-op for the file transport. The P1BatchingTransport calls this
        on every wrapped transport to keep the interface uniform, but the
        file transport has no time-based bookkeeping of its own."""
        pass

    def close(self) -> None:
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
    _PRIORITY_MAP = {
        "P2": "CRITICAL",
        "P1": "ELEVATED",
        "LOG": "NORMAL",
    }

    # Default close() drain budget. Long enough to absorb a burst-submit
    # finishing the writer queue plus the daemon's QUEUED -> DELIVERED hop;
    # short enough that a stuck daemon doesn't hang process shutdown.
    _DEFAULT_CLOSE_DRAIN_TIMEOUT_S = 5.0

    # Poll cadence while waiting for terminal dispositions. 20 ms keeps
    # the wait responsive without busy-looping; ACKs only flow during
    # daemon round-trips so finer granularity buys nothing.
    _DRAIN_POLL_INTERVAL_S = 0.02

    # Retry-on-BUFFER_FULL backoff schedule. Per the 2026-06-04 radio team
    # memo, 0x80 is backpressure rather than a hard failure: the radio is
    # telling us the intake queue is momentarily full. Re-submitting after
    # a backoff usually recovers the frame. The original tid stays in its
    # contract-correct terminal failed state; the retry is a new tid.
    _RETRY_INITIAL_BACKOFF_S = 10.0
    _RETRY_BACKOFF_FACTOR = 2.0
    _RETRY_BACKOFF_MAX_S = 120.0

    def __init__(
        self,
        endpoint_id: int,
        socket_path: Optional[str] = None,
        max_retries: int = 3,
        max_submit_rate_per_min: float = 0.0,
    ) -> None:
        # Defer imports until construction so module imports cleanly without IPC.
        try:
            from skybounce_client import SkyBounceClient
            from skybounce_ipc import (
                CmdResult, Disposition, LinkState, Priority,
            )
            from sb_telemetry_payload import (
                SB45Event, SB45Summary, pack_sb45, pack_sb45_summary,
            )
        except ImportError as e:
            raise ImportError(
                "IPC transport requires skybounce-ipc-python on PYTHONPATH. "
                f"Original error: {e}"
            ) from e

        self._SB45Event = SB45Event
        self._SB45Summary = SB45Summary
        self._pack_sb45 = pack_sb45
        self._pack_sb45_summary = pack_sb45_summary
        self._Priority = Priority
        self._CmdResult = CmdResult
        self._Disposition = Disposition
        self._LinkState = LinkState

        # Observability counters; surfaced in close() summary and via the
        # ack_counts / cmds_received properties (for tests, health checks,
        # or a future operator endpoint).
        self._count = 0
        self._cmds_received = 0
        self._ack_counts: dict[int, int] = {}
        self._last_link_state: Optional[int] = None

        # Retry-on-BUFFER_FULL state. Per-tid payload mapping lets the ACK
        # handler reconstruct the Event/SummaryEvent to re-submit when 0x80
        # fires. retry_queue holds entries that haven't yet aged through
        # their exponential backoff; tick() drains it on each call. The
        # lock protects retry_queue from concurrent access between the
        # SkyBounceClient reader thread (which enqueues from _on_telemetry_ack)
        # and the engine's tick() thread (which dequeues + re-submits).
        self._max_retries = int(max_retries)
        self._tid_to_payload: dict[int, tuple] = {}
        self._tid_to_retry_count: dict[int, int] = {}
        self._retry_queue: list = []
        self._retry_queue_lock = threading.Lock()
        self._retries_submitted = 0

        # Sliding-60s submission rate limit. Applies to every wire submission
        # (emit, emit_summary, retry). 0 = unlimited (existing behavior).
        # When the cap is reached, _rate_limited_submit_telemetry sleeps until
        # the oldest timestamp ages out. recent_submit_ts is only touched
        # from the engine's tick/emit thread, never from the ACK handler,
        # so no lock needed.
        self._max_submit_rate_per_min = float(max_submit_rate_per_min)
        self._recent_submit_ts: collections.deque = collections.deque()
        self._rate_limit_sleeps = 0

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
        log.info(
            "IPC transport started, endpoint_id=0x%08X, max_retries=%d, "
            "rate_limit=%s/min",
            endpoint_id, self._max_retries,
            f"{self._max_submit_rate_per_min:.0f}"
            if self._max_submit_rate_per_min > 0 else "unlimited",
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
        Per the v1.0 known limitations, DELIVERED is a hand-off confirmation,
        not an L1 ACK from the base station -- callers who care about that
        distinction should consult sensor_ipc_KNOWN_LIMITATIONS.md.

        On DROPPED_BUFFER_FULL (0x80) with retries remaining, the originating
        Event or SummaryEvent is queued for re-submission. The original tid
        stays in its terminal failed state per the v1.0 contract; the retry
        is a new submission (new tid). Bounded at max_retries per origin.
        """
        disp = int(ack.disposition)
        self._ack_counts[disp] = self._ack_counts.get(disp, 0) + 1
        name = self._disposition_name(disp)

        # Queue a retry on BUFFER_FULL if we have attempts left for this tid.
        # The ACK handler runs on the SkyBounceClient reader thread; the
        # actual re-submission happens later in tick(), so this handler never
        # blocks on rate-limit sleeps or socket I/O.
        if (self._max_retries > 0
                and disp == 0x80   # DROPPED_BUFFER_FULL
                and ack.telemetry_id in self._tid_to_payload):
            attempt = self._tid_to_retry_count.get(ack.telemetry_id, 0)
            if attempt < self._max_retries:
                kind, payload = self._tid_to_payload[ack.telemetry_id]
                backoff = min(
                    self._RETRY_INITIAL_BACKOFF_S
                    * (self._RETRY_BACKOFF_FACTOR ** attempt),
                    self._RETRY_BACKOFF_MAX_S,
                )
                with self._retry_queue_lock:
                    self._retry_queue.append((
                        kind, payload, time.monotonic() + backoff, attempt + 1,
                    ))
                log.info(
                    "retry queued for tid=%d (BUFFER_FULL); attempt #%d, "
                    "backoff=%.0fs",
                    ack.telemetry_id, attempt + 1, backoff,
                )
            else:
                log.warning(
                    "retry exhausted for tid=%d after %d attempts; giving up",
                    ack.telemetry_id, attempt,
                )

        if disp & 0x80:
            failed_total = sum(c for d, c in self._ack_counts.items() if d & 0x80)
            log.warning(
                "TELEMETRY_ACK tid=%d disposition=%s reason=0x%04X "
                "(failures so far: %d)",
                ack.telemetry_id, name, ack.reason_code, failed_total,
            )
        else:
            log.info(
                "TELEMETRY_ACK tid=%d disposition=%s reason=0x%04X",
                ack.telemetry_id, name, ack.reason_code,
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
    # Submit helpers (rate-limited, retry-aware)
    # -------------------------------------------------------------------------

    def _rate_limited_submit_telemetry(self, bytes_payload: bytes,
                                       priority: Any) -> int:
        """Wrap client.submit_telemetry() with a sliding-60s rate cap.

        Sleeps until the oldest tracked submit ages out if the cap is at the
        limit. Only invoked from the engine's tick/emit thread (and from
        _drain_retry_queue which also runs there) -- never from the ACK
        handler -- so no lock is needed on recent_submit_ts.
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
        return tid

    def _pack_and_submit_event(self, event: Event) -> int:
        """Pack an Event as SB45 and submit it. Records the original Event
        in tid_to_payload so the ACK handler can re-queue on BUFFER_FULL."""
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
        """Pack a SummaryEvent as SB45_SIM_V3 summary and submit it. Records
        the original SummaryEvent in tid_to_payload so the ACK handler can
        re-queue on BUFFER_FULL."""
        sb45_summary = self._SB45Summary(
            event_type=summary.event_type,
            priority="BATCH_SUMMARY",
            location_status=summary.last_location_status,
            window_idx=summary.window_idx,
            count=summary.count,
            max_score=summary.max_score,
            mean_score=summary.mean_score,
            window_end_elapsed_s=summary.window_end_elapsed_s,
            lat=summary.last_gps_lat,
            lon=summary.last_gps_lon,
            anchor_lat=summary.anchor_lat,
            anchor_lon=summary.anchor_lon,
        )
        packed = self._pack_sb45_summary(sb45_summary)
        ipc_prio = getattr(self._Priority, "ELEVATED")
        return self._rate_limited_submit_telemetry(packed.bytes_payload, ipc_prio)

    def _drain_retry_queue(self) -> int:
        """Re-submit any retry entries whose backoff has expired. Each retry
        gets a NEW tid; the original tid stays in its terminal failed state.
        Called from tick() so retry I/O happens on the engine's main thread
        (never the ACK reader thread).

        Returns the number of retries actually re-submitted on this call
        (0 if nothing was due yet).
        """
        if self._max_retries <= 0:
            return 0
        now = time.monotonic()
        # Snapshot under lock so the ACK handler can't append while we iterate.
        with self._retry_queue_lock:
            if not self._retry_queue:
                return 0
            due, not_due = [], []
            for entry in self._retry_queue:
                (due if entry[2] <= now else not_due).append(entry)
            self._retry_queue[:] = not_due
        n_submitted = 0
        for kind, payload, _not_before, attempt in due:
            if kind == "event":
                new_tid = self._pack_and_submit_event(payload)
            else:  # "summary"
                new_tid = self._pack_and_submit_summary(payload)
            self._tid_to_payload[new_tid] = (kind, payload)
            self._tid_to_retry_count[new_tid] = attempt
            self._retries_submitted += 1
            self._count += 1
            n_submitted += 1
            log.info(
                "retry submitted: kind=%s attempt=%d new_tid=%d",
                kind, attempt, new_tid,
            )
        return n_submitted

    # -------------------------------------------------------------------------
    # Transport protocol
    # -------------------------------------------------------------------------

    def emit(self, event: Event) -> None:
        tid = self._pack_and_submit_event(event)
        if self._max_retries > 0:
            self._tid_to_payload[tid] = ("event", event)
            self._tid_to_retry_count[tid] = 0
        self._count += 1

    def emit_summary(self, summary: SummaryEvent) -> None:
        """Pack a P1 batch summary via SB45_SIM_V3 (pack_sb45_summary) and
        submit it as a TELEMETRY frame. pack_sb45_summary hard-codes the
        SB45 priority field to BATCH_SUMMARY (=3) regardless of what we pass.
        The IPC frame's own priority is ELEVATED (same as a P1 per-event)
        because a summary REPRESENTS P1 events; we don't want the radio's
        TX scheduler to de-prioritize it.
        """
        tid = self._pack_and_submit_summary(summary)
        if self._max_retries > 0:
            self._tid_to_payload[tid] = ("summary", summary)
            self._tid_to_retry_count[tid] = 0
        self._count += 1
        log.info(
            "emit_summary: tid=%d type=%s window_end=%.0fs count=%d max=%.2f mean=%.2f",
            tid, summary.event_type, summary.window_end_elapsed_s,
            summary.count, summary.max_score, summary.mean_score,
        )

    def tick(self, ts_epoch_s: float, elapsed_s: float) -> None:
        """Per-frame pulse from the engine. Drains the retry queue (re-submits
        any entries whose backoff has expired). When retries are disabled
        (max_retries=0) this is a no-op, preserving the historical contract."""
        if self._max_retries > 0:
            self._drain_retry_queue()

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

        if self._count > 0:
            delivered = int(self._Disposition.DELIVERED)

            def _terminal_count() -> int:
                # DELIVERED is the only success disposition; failures all set
                # the 0x80 bit per spec Section 7.4. Intermediate states like
                # QUEUED and TRANSMITTED don't count -- they aren't terminal.
                return sum(
                    c for d, c in self._ack_counts.items()
                    if d == delivered or (d & 0x80)
                )

            deadline = time.monotonic() + drain_timeout_s
            while _terminal_count() < self._count and time.monotonic() < deadline:
                time.sleep(self._DRAIN_POLL_INTERVAL_S)

            outstanding = self._count - _terminal_count()
            if outstanding > 0:
                log.warning(
                    "IPC transport close: drain deadline (%.1fs) reached with "
                    "%d/%d telemetries terminal; %d in-flight frames will be "
                    "dropped at socket close",
                    drain_timeout_s, _terminal_count(), self._count, outstanding,
                )

        log.info("IPC transport closing, %d events submitted", self._count)
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
