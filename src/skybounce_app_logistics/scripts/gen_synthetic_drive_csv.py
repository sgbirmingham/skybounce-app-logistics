#!/usr/bin/env python3
"""Generate a synthetic vehicle_behavior_simple_logger_v0_1 CSV with engineered
events for bench / simnet testing.

The generated CSV has the same schema as the real Pi logger output, but with
values designed to deterministically trigger specific events (hard_brake,
hard_accel, severe_impact, ...) at known timestamps. Useful for:

* Multi-sensor simnet tests where you want to compare what events each Pi
  should produce against what actually came through.
* Reproducible regression of the engine against known-good event timelines.
* Quick smoke tests that don't require a 40-minute real drive recording.

Default layout (300 s, three transmittable events):
    0 -   9 s   GPS acquiring (gps_valid=0, sats=0..4)
   10 -  29 s   GPS acquired, confidence ramps to >= 0.80
   30 - 179 s   Cruising 15 m/s, no events (startup_suppress active until 180 s)
  180 - 182 s   HARD BRAKE: 15 -> 8 -> 5 m/s   (decel ~ 5 m/s^2, hits severe)
  185 - 229 s   Cruising 8 m/s   (brake cooldown 60 s elapsed by t=245)
  230 - 232 s   SEVERE IMPACT: accel_z spike to -2.0 g for one row
                (lin_accel ~ 1.0 g, jerk ~ 1.0 g/s -- both thresholds met)
  235 - 284 s   Cruising 8 m/s   (impact cooldown 45 s elapsed by t=280)
  285 - 287 s   HARD ACCEL: 8 -> 14 -> 18 m/s   (accel ~ 5 m/s^2, hits severe)
  288 - 300 s   Cruising 18 m/s

Per-sensor variation: pass --seed N for IMU noise variation and a small
random phase offset on each event timestamp (jitter <= +/- 2 s). With the
same --seed, two runs produce identical CSVs.

Usage:
    python -m skybounce_app_logistics.scripts.gen_synthetic_drive_csv \\
        --out /tmp/synthetic_simnet_sensor1.csv \\
        --seed 1

    python -m skybounce_app_logistics.scripts.gen_synthetic_drive_csv \\
        --out /tmp/synthetic_simnet_sensor2.csv \\
        --seed 2 \\
        --events brake:200,impact:250,accel:290
"""

from __future__ import annotations

import argparse
import csv
import math
import random
import sys
from datetime import datetime, timezone
from typing import Iterator, Optional

# Same schema as vehicle_behavior_simple_logger_v0_1
HEADER = [
    "session_id",
    "row_id",
    "ts_epoch_s",
    "ts_local_iso",
    "elapsed_s",
    "time_synced",
    "clock_jump_detected",
    "imu_ok",
    "gps_ok",
    "bme_ok",
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
    "gps_track_deg",
    "gps_time",
    "temp_C",
    "humidity_RH",
    "pressure_hPa",
    "notes",
]

# Default location: somewhere in western PA (matches the user's home test region)
DEFAULT_LAT = 40.7031235
DEFAULT_LON = -80.1112526
DEFAULT_ALT = 329.3  # m

# Default cruise speeds (m/s)
CRUISE_INITIAL = 15.0  # ~34 mph
CRUISE_AFTER_BRAKE = 8.0  # ~18 mph
CRUISE_AFTER_ACCEL = 18.0  # ~40 mph


def parse_event_spec(spec: str) -> list[tuple[str, float]]:
    """Parse '--events brake:180,impact:230,accel:285' into [(type, t_s), ...]."""
    out: list[tuple[str, float]] = []
    for chunk in spec.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if ":" not in chunk:
            raise ValueError(f"event spec must be 'type:seconds', got: {chunk!r}")
        kind, t_s = chunk.split(":", 1)
        out.append((kind.strip(), float(t_s)))
    return out


def session_id_from_seed(seed: int) -> str:
    """8-character hex session id, deterministic from seed."""
    rng = random.Random(seed)
    return "".join(rng.choice("0123456789abcdef") for _ in range(8))


def gen_rows(
    duration_s: float,
    events: list[tuple[str, float]],
    seed: int,
    base_epoch_s: float,
) -> Iterator[dict]:
    """Yield one CSV row per second from 0 to duration_s-1."""
    rng = random.Random(seed)
    sess = session_id_from_seed(seed)

    # Apply optional small jitter per event (deterministic from seed)
    jittered_events: list[tuple[str, float]] = []
    for kind, t in events:
        # jitter is +/- 2 s, deterministic for this seed
        jitter = rng.uniform(-2.0, 2.0) if seed != 0 else 0.0
        jittered_events.append((kind, t + jitter))

    n_rows = int(duration_s)

    # Pre-compute speed trajectory across the whole CSV.
    # We start with cruise_initial, then each brake/accel rewrites the
    # speed series around its timestamp.
    speeds = [0.0] * n_rows
    cruise = CRUISE_INITIAL
    for i in range(n_rows):
        speeds[i] = cruise if i >= 30 else 0.0  # parked until GPS acquired

    # Apply brake events: speed drops over 2 rows by ~5 m/s/s
    for kind, t in jittered_events:
        idx = int(round(t))
        if kind == "brake":
            # decel from current cruise to (cruise - 10) over 2 rows
            if 0 <= idx < n_rows - 2:
                pre = speeds[idx]
                speeds[idx] = pre
                speeds[idx + 1] = max(2.5, pre - 5.5)  # decel ~ 5.5 m/s/s in 1 s
                speeds[idx + 2] = max(2.0, pre - 10.0)  # decel another 4.5 -> total 5 m/s^2 floor for hits
                # Hold the new cruise speed
                new_cruise = max(2.0, pre - 10.0)
                for j in range(idx + 3, n_rows):
                    speeds[j] = new_cruise
                cruise = new_cruise
        elif kind == "accel":
            # accel from current cruise to (cruise + 10) over 2 rows
            if 0 <= idx < n_rows - 2:
                pre = speeds[idx]
                speeds[idx] = pre
                speeds[idx + 1] = pre + 6.0  # accel ~ 6 m/s/s
                speeds[idx + 2] = pre + 10.0  # accel ~ 4 -> meets 5 m/s/s threshold
                new_cruise = pre + 10.0
                for j in range(idx + 3, n_rows):
                    speeds[j] = new_cruise
                cruise = new_cruise

    # Build impact-spike row index set
    impact_rows: dict[int, str] = {}
    for kind, t in jittered_events:
        idx = int(round(t))
        if kind == "impact":
            impact_rows[idx] = "severe"
        elif kind == "moderate_impact":
            impact_rows[idx] = "moderate"

    # GPS state schedule
    # rows 0-9:   gps_valid=0, sats=0..4 (acquiring)
    # rows 10-29: gps_valid=1, sats=5..12 (acquired, confidence ramping)
    # rows 30+:   gps_valid=1, sats=12 (stable)

    for i in range(n_rows):
        ts_epoch = base_epoch_s + i
        ts_iso = datetime.fromtimestamp(ts_epoch, tz=timezone.utc).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "")
        # ts_local_iso emulates Pi's behavior (local time, no tz suffix)
        local_iso = datetime.fromtimestamp(ts_epoch).isoformat(timespec="microseconds")
        elapsed = float(i)

        # GPS state
        if i < 10:
            gps_valid = 0
            gps_mode = 0
            gps_sats = min(i // 2, 4)
            gps_lat = ""
            gps_lon = ""
            gps_speed = ""
            gps_track = ""
            gps_time = ""
        elif i < 30:
            gps_valid = 1
            gps_mode = 3
            gps_sats = 5 + (i - 10) // 2  # 5..14
            gps_lat = DEFAULT_LAT
            gps_lon = DEFAULT_LON
            gps_speed = speeds[i]
            gps_track = 90.0
            gps_time = ts_iso + "Z"
        else:
            gps_valid = 1
            gps_mode = 3
            gps_sats = 12 + rng.randint(-2, 3)  # 10..15
            gps_lat = DEFAULT_LAT + (speeds[i] * i * 1e-7)  # tiny eastward drift
            gps_lon = DEFAULT_LON
            gps_speed = speeds[i]
            gps_track = 90.0
            gps_time = ts_iso + "Z"

        # IMU: baseline accel_z = -1.0 (gravity), small noise on x/y
        accel_x = rng.gauss(0.0, 0.02)
        accel_y = rng.gauss(0.0, 0.02)
        accel_z = -1.0 + rng.gauss(0.0, 0.01)
        gyro_x = rng.gauss(0.0, 0.5)
        gyro_y = rng.gauss(0.0, 0.5)
        gyro_z = rng.gauss(0.0, 0.5)

        # Impact spike: dial up accel magnitude for one row
        if i in impact_rows:
            kind = impact_rows[i]
            if kind == "severe":
                # lin_accel = 1.0 g, jerk = 1.0 g/s (need both for severe)
                accel_z = -2.0
                accel_x = 0.05
                accel_y = 0.05
            elif kind == "moderate":
                # lin_accel = 0.35 g (above 0.30 threshold)
                accel_z = -1.35
                accel_x = 0.05
                accel_y = 0.05

        # BME280 environmental: indoor stable values
        temp_c = 27.0 + rng.gauss(0.0, 0.05)
        humid = 41.0 + rng.gauss(0.0, 0.5)
        pressure = 977.5 + rng.gauss(0.0, 0.1)

        yield {
            "session_id": sess,
            "row_id": i,
            "ts_epoch_s": f"{ts_epoch:.7f}",
            "ts_local_iso": local_iso,
            "elapsed_s": f"{elapsed:.9f}",
            "time_synced": 1,
            "clock_jump_detected": 0,
            "imu_ok": 1,
            "gps_ok": 1,
            "bme_ok": 1,
            "accel_x_g": f"{accel_x:.6f}",
            "accel_y_g": f"{accel_y:.6f}",
            "accel_z_g": f"{accel_z:.6f}",
            "gyro_x_dps": f"{gyro_x:.6f}",
            "gyro_y_dps": f"{gyro_y:.6f}",
            "gyro_z_dps": f"{gyro_z:.6f}",
            "gps_valid": gps_valid,
            "gps_mode": gps_mode,
            "gps_sats": gps_sats,
            "gps_lat": gps_lat,
            "gps_lon": gps_lon,
            "gps_speed_m_s": gps_speed,
            "gps_track_deg": gps_track,
            "gps_time": gps_time,
            "temp_C": f"{temp_c:.6f}",
            "humidity_RH": f"{humid:.6f}",
            "pressure_hPa": f"{pressure:.6f}",
            "notes": "",
        }


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description=(
            "Generate a synthetic vehicle_behavior_simple_logger_v0_1 CSV "
            "with engineered events for bench / simnet testing."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Default event timeline (with --duration-s 300):\n"
            "  brake at  t=180s\n"
            "  impact at t=230s\n"
            "  accel at  t=285s\n"
            "\n"
            "All events fall after startup_suppress_s (180 s); see the\n"
            "skybounce-event-rules AnalyzerConfig for threshold values."
        ),
    )
    p.add_argument(
        "--out",
        required=True,
        metavar="PATH",
        help="Output CSV path.",
    )
    p.add_argument(
        "--duration-s",
        type=float,
        default=300.0,
        metavar="N",
        help="Total CSV duration in seconds at 1 Hz row rate. (default: 300)",
    )
    p.add_argument(
        "--events",
        type=str,
        default="brake:180,impact:230,accel:285",
        metavar="LIST",
        help=(
            "Comma-separated event:timestamp pairs. "
            "Supported types: brake, accel, impact, moderate_impact. "
            "(default: brake:180,impact:230,accel:285)"
        ),
    )
    p.add_argument(
        "--seed",
        type=int,
        default=1,
        metavar="N",
        help=(
            "Random seed for IMU noise + per-event jitter. Same seed gives "
            "identical CSV; different seeds give per-sensor variation. "
            "Use seed=0 to disable jitter entirely. (default: 1)"
        ),
    )
    p.add_argument(
        "--base-epoch-s",
        type=float,
        default=None,
        metavar="EPOCH",
        help=(
            "ts_epoch_s of row 0. Defaults to current wall time. Set for "
            "reproducible timestamps across runs."
        ),
    )
    args = p.parse_args(argv)

    try:
        events = parse_event_spec(args.events)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    base_epoch = (
        args.base_epoch_s if args.base_epoch_s is not None else datetime.now().timestamp()
    )

    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        for row in gen_rows(args.duration_s, events, args.seed, base_epoch):
            writer.writerow(row)

    print(f"wrote {int(args.duration_s)} rows to {args.out}")
    print(f"session_id: {session_id_from_seed(args.seed)}")
    print(f"events scheduled (with jitter for seed={args.seed}):")
    rng = random.Random(args.seed)
    for kind, t in events:
        jitter = rng.uniform(-2.0, 2.0) if args.seed != 0 else 0.0
        print(f"  {kind:>16s} at t = {t + jitter:6.2f} s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
