"""
csv_source.py

CSV ingest for the streaming engine.

Two modes:
- batch:   read a closed CSV from disk and yield every row once. Used for
           replay validation against the PC analyzer.
- tail:    follow an active CSV that the logger is currently writing. Used
           on the Pi during live operation.

Both modes yield SensorFrame objects, which mirror the columns the analyzer
relies on. Type coercion and NaN handling match the logger's safe_float
semantics exactly so behavior is identical in both directions.
"""

from __future__ import annotations

import csv
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional


# Columns the streaming engine actually consumes. Anything else in the CSV
# is ignored. This list is the contract between the logger and the engine.
REQUIRED_COLUMNS = (
    "ts_epoch_s",
    "elapsed_s",
    "accel_x_g",
    "accel_y_g",
    "accel_z_g",
    "gyro_x_dps",
    "gyro_y_dps",
    "gyro_z_dps",
    "gps_valid",
    "gps_mode",
    "gps_sats",
    "gps_lat",
    "gps_lon",
    "gps_speed_m_s",
    "gps_time",
)


def _safe_float(x) -> Optional[float]:
    """Match logger semantics: None / empty / NaN / inf -> None."""
    if x is None or x == "":
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if v != v or v == float("inf") or v == float("-inf"):  # NaN / inf check
        return None
    return v


def _safe_int(x) -> Optional[int]:
    v = _safe_float(x)
    return None if v is None else int(v)


@dataclass
class SensorFrame:
    """One row of sensor data. Matches the analyzer's per-row contract.

    Numeric fields use None to signal missing data (sensor outage, no GPS fix,
    etc.). The engine treats None defensively at every consumption point.
    """
    ts_epoch_s: float
    elapsed_s: float

    # IMU
    accel_x_g: Optional[float]
    accel_y_g: Optional[float]
    accel_z_g: Optional[float]
    gyro_x_dps: Optional[float]
    gyro_y_dps: Optional[float]
    gyro_z_dps: Optional[float]

    # GPS
    gps_valid: int             # 0 or 1
    gps_mode: int              # 0, 2, 3
    gps_sats: int
    gps_lat: Optional[float]
    gps_lon: Optional[float]
    gps_speed_m_s: Optional[float]
    gps_time: str              # ISO string from gpsd, used for repeat detection
    gps_track_deg: Optional[float] = None   # heading; sharp-turn lateral accel

    @classmethod
    def from_row(cls, row: dict) -> "SensorFrame":
        ts = _safe_float(row.get("ts_epoch_s"))
        if ts is None:
            raise ValueError(f"row missing ts_epoch_s: {row!r}")
        return cls(
            ts_epoch_s=ts,
            elapsed_s=_safe_float(row.get("elapsed_s")) or 0.0,
            accel_x_g=_safe_float(row.get("accel_x_g")),
            accel_y_g=_safe_float(row.get("accel_y_g")),
            accel_z_g=_safe_float(row.get("accel_z_g")),
            gyro_x_dps=_safe_float(row.get("gyro_x_dps")),
            gyro_y_dps=_safe_float(row.get("gyro_y_dps")),
            gyro_z_dps=_safe_float(row.get("gyro_z_dps")),
            gps_valid=_safe_int(row.get("gps_valid")) or 0,
            gps_mode=_safe_int(row.get("gps_mode")) or 0,
            gps_sats=_safe_int(row.get("gps_sats")) or 0,
            gps_lat=_safe_float(row.get("gps_lat")),
            gps_lon=_safe_float(row.get("gps_lon")),
            gps_speed_m_s=_safe_float(row.get("gps_speed_m_s")),
            gps_track_deg=_safe_float(row.get("gps_track_deg")),
            gps_time=str(row.get("gps_time", "") or ""),
        )


# -----------------------------------------------------------------------------
# Batch mode (replay / regression validation)
# -----------------------------------------------------------------------------

def batch_frames(csv_path: Path) -> Iterator[SensorFrame]:
    """Yield every row of a closed CSV in order. For replay against stored logs.

    Skips rows missing a usable ts_epoch_s instead of crashing -- the engine
    is meant to tolerate logger startup oddities.
    """
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                yield SensorFrame.from_row(row)
            except ValueError:
                continue


# -----------------------------------------------------------------------------
# Tail mode (live Pi operation)
# -----------------------------------------------------------------------------

def tail_frames(
    csv_path: Path,
    poll_interval_s: float = 0.25,
    stop_on_eof: bool = False,
) -> Iterator[SensorFrame]:
    """Follow an actively-written CSV. Yields each new row as the logger
    flushes it.

    poll_interval_s: how often to recheck the file when at EOF.
    stop_on_eof: if True, exit when no new rows appear within ~5 polls.
                 If False (default), wait forever -- this is what the live
                 streaming engine wants.

    Assumes the file is line-buffered or flushed-per-row. The current logger
    flushes every row, so this works without changes.
    """
    # Wait for the file to exist (logger may not have created it yet).
    while not csv_path.exists():
        time.sleep(poll_interval_s)

    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        # Read header
        header_line = f.readline()
        if not header_line:
            # Empty file -- wait for header to be written.
            while not header_line:
                time.sleep(poll_interval_s)
                header_line = f.readline()
        fieldnames = [c.strip() for c in header_line.rstrip("\r\n").split(",")]

        # Now stream rows. csv.reader handles partial lines correctly when
        # we use a generator that returns one line at a time.
        idle_polls = 0
        leftover = ""
        while True:
            chunk = f.readline()
            if not chunk:
                idle_polls += 1
                if stop_on_eof and idle_polls > 20:
                    return
                time.sleep(poll_interval_s)
                continue

            idle_polls = 0
            line = leftover + chunk
            if not line.endswith("\n"):
                # Partial line -- save and wait for completion.
                leftover = line
                continue
            leftover = ""

            line = line.rstrip("\r\n")
            if not line:
                continue
            values = next(csv.reader([line]))
            row = dict(zip(fieldnames, values))
            try:
                yield SensorFrame.from_row(row)
            except ValueError:
                continue
