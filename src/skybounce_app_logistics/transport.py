"""
transport.py

Transports for streaming events. Two implementations:

- FileTransport: writes one JSON object per event to a file, one event per
  line. Used for offline replay validation and during development. No IPC
  dependency.

- IpcTransport: encodes events using SB45_SIM_V0 and submits them via the
  SkyBounce IPC client. Used on the Pi for real radio operation. Imports
  from skybounce_IPC_python at construction time, so this file is safe to
  import even when the IPC stack isn't installed.

Both implement the same Transport protocol: emit(event) and close().
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional, Protocol


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
# Transport interface
# -----------------------------------------------------------------------------

class Transport(Protocol):
    def emit(self, event: Event) -> None: ...
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

    def close(self) -> None:
        log.info("file transport closing, %d events written to %s",
                 self._count, self._path)
        self._f.close()

    @property
    def event_count(self) -> int:
        return self._count


# -----------------------------------------------------------------------------
# IPC transport
# -----------------------------------------------------------------------------

class IpcTransport:
    """Encode events using SB45_SIM_V0 and submit to the SkyBounce IPC client.

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

    def __init__(self, endpoint_id: int, socket_path: Optional[str] = None) -> None:
        # Defer imports until construction so module imports cleanly without IPC.
        try:
            from skybounce_client import SkyBounceClient
            from skybounce_ipc import (
                CmdResult, Disposition, LinkState, Priority,
            )
            from sb_telemetry_payload import SB45Event, pack_sb45
        except ImportError as e:
            raise ImportError(
                "IPC transport requires skybounce_IPC_python on PYTHONPATH. "
                f"Original error: {e}"
            ) from e

        self._SB45Event = SB45Event
        self._pack_sb45 = pack_sb45
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

        self._client = SkyBounceClient(endpoint_id=endpoint_id, socket_path=socket_path)

        # Register handlers BEFORE start() so the reader thread sees them
        # when the first non-handshake frame arrives.
        self._client.set_cmd_handler(self._on_cmd)
        self._client.set_telemetry_ack_handler(self._on_telemetry_ack)
        self._client.set_status_handler(self._on_status)

        self._client.start()
        log.info("IPC transport started, endpoint_id=0x%08X", endpoint_id)

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
        """
        disp = int(ack.disposition)
        self._ack_counts[disp] = self._ack_counts.get(disp, 0) + 1
        name = self._disposition_name(disp)
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
    # Transport protocol
    # -------------------------------------------------------------------------

    def emit(self, event: Event) -> None:
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

        self._client.submit_telemetry(packed.bytes_payload, priority=ipc_prio)
        self._count += 1

    def close(self) -> None:
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
