"""
state.py

Streaming state for the SkyBounce logistics engine.

This is where the streaming engine differs structurally from the PC
analyzer. The analyzer has the whole CSV in memory and can compute things
like GPS fix age by looking at the global series; the streaming engine
must track all of that incrementally, one row at a time.

The math itself is identical. We delegate every per-row decision to
skybounce_event_rules. This module is the bookkeeping: previous values,
running EWMA, GPS repeat counters, debounced state, interval tracking,
cooldown timestamps.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from skybounce_event_rules import (
    AnalyzerConfig,
    accel_magnitude_g,
    candidate_state,
    dwell_time_for_state,
    ewma_update,
    gps_confidence_raw,
    gps_quality_state,
    jerk_g_per_s,
    linear_accel_g,
    longitudinal_accel_m_s2,
    movement_state_raw,
)

from .csv_source import SensorFrame


@dataclass
class ComputedFeatures:
    """Per-row derived quantities. Equivalent to one row of the PC analyzer's
    features DataFrame for the columns this engine cares about.
    """
    ts_epoch_s: float
    elapsed_s: float
    dt_s: float

    # Acceleration math
    accel_mag_g: float
    lin_accel_g: float
    jerk_g_s: float
    accel_m_s2: float           # signed longitudinal accel
    decel_m_s2: float           # positive deceleration only

    # GPS quality
    gps_fix_age_s: float
    gps_repeat_counter: int
    gps_data_stale_flag: bool
    gps_fix_stale: bool
    gps_confidence_raw: float
    gps_confidence: float       # EWMA-smoothed
    gps_quality_state: str

    # Movement
    movement_state_raw: str
    speed_m_s: float            # 0.0 when GPS speed missing

    # State machine
    analyzer_state_candidate: str
    analyzer_state: str         # committed state after dwell


@dataclass
class StreamingState:
    """Everything the engine needs to remember between frames.

    Initialized once per session. Reset only on full restart.
    """
    cfg: AnalyzerConfig

    # Frame-to-frame deltas
    prev_ts_epoch_s: Optional[float] = None
    prev_accel_mag_g: Optional[float] = None
    prev_speed_m_s: Optional[float] = None
    prev_gps_lat: Optional[float] = None
    prev_gps_lon: Optional[float] = None
    prev_gps_time: str = ""

    # GPS fix age tracking
    last_good_fix_ts: Optional[float] = None

    # GPS repeat-report tracking
    gps_repeat_counter: int = 0

    # EWMA accumulator
    gps_confidence_smoothed: Optional[float] = None

    # Session anchor (first valid GPS fix; used by packet encoder for lat/lon)
    session_start_ts: Optional[float] = None
    anchor_lat: Optional[float] = None
    anchor_lon: Optional[float] = None

    # State machine
    committed_state: str = "UNKNOWN"
    pending_state: Optional[str] = None
    pending_start_ts: Optional[float] = None

    # Cooldown timestamps per event type
    last_event_ts: dict = field(default_factory=dict)


def _is_good_fix(frame: SensorFrame, cfg: AnalyzerConfig) -> bool:
    """Mirror analyzer's gps_good_fix criterion."""
    return (
        frame.gps_valid == 1
        and frame.gps_mode >= cfg.gps_min_mode
        and frame.gps_sats >= cfg.gps_min_sats
        and frame.gps_lat is not None
        and frame.gps_lon is not None
    )


def _update_gps_repeat_counter(state: StreamingState, frame: SensorFrame) -> int:
    """Track consecutive identical GPS reports. Mirrors analyzer's
    same_report detection."""
    if frame.gps_lat is None or frame.gps_lon is None:
        state.gps_repeat_counter = 0
        return 0

    lat_same = (
        state.prev_gps_lat is not None
        and abs(frame.gps_lat - state.prev_gps_lat) < 1e-7
    )
    lon_same = (
        state.prev_gps_lon is not None
        and abs(frame.gps_lon - state.prev_gps_lon) < 1e-7
    )
    time_same = frame.gps_time == state.prev_gps_time

    if lat_same and lon_same and time_same:
        state.gps_repeat_counter += 1
    else:
        state.gps_repeat_counter = 0

    return state.gps_repeat_counter


def _update_state_machine(
    state: StreamingState,
    candidate: str,
    ts: float,
    cfg: AnalyzerConfig,
) -> str:
    """Apply dwell-time debounce. Mirrors add_state_machine_columns().

    Returns the currently committed state.
    """
    if state.committed_state == "UNKNOWN":
        state.committed_state = candidate
        state.pending_state = None
        state.pending_start_ts = None
        return state.committed_state

    if candidate == state.committed_state:
        state.pending_state = None
        state.pending_start_ts = None
        return state.committed_state

    # Candidate differs from committed. Track or extend the pending window.
    if state.pending_state != candidate:
        state.pending_state = candidate
        state.pending_start_ts = ts
        pending_age = 0.0
    else:
        pending_age = max(0.0, ts - (state.pending_start_ts or ts))

    required = dwell_time_for_state(candidate, cfg)
    if pending_age >= required:
        state.committed_state = candidate
        state.pending_state = None
        state.pending_start_ts = None

    return state.committed_state


def process_frame(
    state: StreamingState,
    frame: SensorFrame,
) -> ComputedFeatures:
    """Advance the streaming state by one sensor frame and return derived
    features.

    The order here mirrors the PC analyzer's compute_features() + state
    machine update. Any deviation is a bug.
    """
    cfg = state.cfg

    # Session start: first frame seen.
    if state.session_start_ts is None:
        state.session_start_ts = frame.ts_epoch_s

    # dt clipped to [0, 10] exactly like the analyzer.
    if state.prev_ts_epoch_s is None:
        dt_s = 0.0
    else:
        dt_s = max(0.0, min(10.0, frame.ts_epoch_s - state.prev_ts_epoch_s))

    # -- IMU features ---------------------------------------------------------
    accel_mag = accel_magnitude_g(frame.accel_x_g, frame.accel_y_g, frame.accel_z_g)
    lin_accel = linear_accel_g(accel_mag)
    jerk = jerk_g_per_s(accel_mag, state.prev_accel_mag_g, dt_s)

    # -- GPS-derived accel/decel ---------------------------------------------
    # Match the analyzer's pandas behavior: pd.to_numeric(...).fillna(0) means
    # missing speed values are treated as 0.0 in the diff calculation. This
    # is faithful to the oracle, including the spurious-glitch events the
    # analyzer also fires. Filtering out unphysical glitches is a deliberate
    # rule-set change that should bump RULES_VERSION; not done in v0.1.
    speed_now = frame.gps_speed_m_s if frame.gps_speed_m_s is not None else 0.0
    prev_speed = state.prev_speed_m_s if state.prev_speed_m_s is not None else 0.0
    long_a = longitudinal_accel_m_s2(speed_now, prev_speed, dt_s)
    decel = max(0.0, -long_a) if long_a < 0 else 0.0
    accel = max(0.0, long_a) if long_a > 0 else 0.0

    # -- GPS fix age ---------------------------------------------------------
    if _is_good_fix(frame, cfg):
        state.last_good_fix_ts = frame.ts_epoch_s
        gps_fix_age_s = 0.0
    else:
        if state.last_good_fix_ts is None:
            gps_fix_age_s = float("inf")
        else:
            gps_fix_age_s = frame.ts_epoch_s - state.last_good_fix_ts

    gps_fix_stale = gps_fix_age_s > cfg.gps_stale_s

    # -- GPS repeat report tracking ------------------------------------------
    repeat_n = _update_gps_repeat_counter(state, frame)
    gps_data_stale_flag = repeat_n >= cfg.gps_repeat_degraded_n

    # -- GPS confidence (raw + EWMA-smoothed) --------------------------------
    raw = gps_confidence_raw(
        gps_valid=frame.gps_valid,
        gps_mode=frame.gps_mode,
        gps_sats=frame.gps_sats,
        gps_fix_age_s=gps_fix_age_s,
        gps_repeat_counter=repeat_n,
        cfg=cfg,
    )
    smoothed = ewma_update(raw, state.gps_confidence_smoothed, cfg.gps_confidence_alpha)
    state.gps_confidence_smoothed = smoothed
    quality = gps_quality_state(smoothed)

    # -- Movement state ------------------------------------------------------
    # Use the fillna(0)-treated speed, matching the analyzer.
    mvmt = movement_state_raw(
        gps_confidence=smoothed,
        gps_data_stale_flag=gps_data_stale_flag,
        gps_fix_stale=gps_fix_stale,
        gps_speed_m_s=speed_now,
        cfg=cfg,
    )

    # -- Candidate and committed state --------------------------------------
    elapsed_since_session_start = frame.ts_epoch_s - state.session_start_ts
    cand = candidate_state(
        movement_state=mvmt,
        gps_confidence=smoothed,
        elapsed_s=elapsed_since_session_start,
        cfg=cfg,
    )
    committed = _update_state_machine(state, cand, frame.ts_epoch_s, cfg)

    # -- Anchor (first valid GPS fix) ----------------------------------------
    if state.anchor_lat is None and frame.gps_lat is not None and frame.gps_lon is not None:
        state.anchor_lat = frame.gps_lat
        state.anchor_lon = frame.gps_lon

    # -- Update frame-to-frame deltas ---------------------------------------
    state.prev_ts_epoch_s = frame.ts_epoch_s
    state.prev_accel_mag_g = accel_mag
    # Store the post-fillna value so next frame's diff matches the analyzer.
    state.prev_speed_m_s = speed_now
    state.prev_gps_lat = frame.gps_lat
    state.prev_gps_lon = frame.gps_lon
    state.prev_gps_time = frame.gps_time

    return ComputedFeatures(
        ts_epoch_s=frame.ts_epoch_s,
        elapsed_s=elapsed_since_session_start,
        dt_s=dt_s,
        accel_mag_g=accel_mag,
        lin_accel_g=lin_accel,
        jerk_g_s=jerk,
        accel_m_s2=accel,
        decel_m_s2=decel,
        gps_fix_age_s=gps_fix_age_s,
        gps_repeat_counter=repeat_n,
        gps_data_stale_flag=gps_data_stale_flag,
        gps_fix_stale=gps_fix_stale,
        gps_confidence_raw=raw,
        gps_confidence=smoothed,
        gps_quality_state=quality,
        movement_state_raw=mvmt,
        speed_m_s=speed_now,
        analyzer_state_candidate=cand,
        analyzer_state=committed,
    )
