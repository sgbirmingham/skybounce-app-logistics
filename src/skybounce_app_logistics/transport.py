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
            from skybounce_ipc import Priority
            from sb_telemetry_payload import SB45Event, pack_sb45
        except ImportError as e:
            raise ImportError(
                "IPC transport requires skybounce_IPC_python on PYTHONPATH. "
                f"Original error: {e}"
            ) from e

        self._SB45Event = SB45Event
        self._pack_sb45 = pack_sb45
        self._Priority = Priority

        self._client = SkyBounceClient(endpoint_id=endpoint_id, socket_path=socket_path)
        self._client.start()
        self._count = 0
        log.info("IPC transport started, endpoint_id=0x%08X", endpoint_id)

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
        self._client.stop()

    @property
    def event_count(self) -> int:
        return self._count
