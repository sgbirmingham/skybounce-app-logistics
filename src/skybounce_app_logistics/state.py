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
    lateral_accel_m_s2,
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

    # Cornering (v0_4_0): GPS heading-rate lateral accel, m/s^2. Trailing default
    # so interval-event snapshots that omit it still construct.
    lat_accel_m_s2: float = 0.0


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
    prev_gps_track_deg: Optional[float] = None
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

    # State-interval window. Tracks the currently-occupied committed state so
    # we can stamp interval events (trip_start, state_highway, long_stop, etc.)
    # on the LAST in-state row, matching the PC analyzer's add_interval_event
    # convention (seg.iloc[-1]; duration = ts(last in-state) - ts(first in-state)).
    state_window_start_ts: Optional[float] = None
    state_window_last_ts: Optional[float] = None
    state_window_speed_mph_sum: float = 0.0
    state_window_sample_count: int = 0

    # Cooldown timestamps per event type
    last_event_ts: dict = field(default_factory=dict)

    # A severe-impact CANDIDATE (g/jerk gate passed) awaiting post-impact
    # speed-collapse confirmation. dict: {ts, speed, score, detail, feats}.
    # Confirmed -> severe_impact (P2); window expires without collapse ->
    # moderate_impact (it was a hard bump, not a crash).
    pending_severe: Optional[dict] = None


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


def update_state_window(
    state: StreamingState,
    ts_epoch_s: float,
    speed_m_s: float,
    transitioned: bool,
) -> Optional[tuple[float, float, float, int]]:
    """Maintain the rolling window of the currently-occupied committed state.

    Called once per frame by the engine. Tracks (start_ts, last_in_state_ts,
    speed_mph_sum, sample_count) so interval events can be stamped on the LAST
    in-state row and carry an avg_speed_mph that matches the analyzer's
    seg["speed_mph"].mean().

    Args:
        transitioned: True if the committed state just changed from the
            previous frame. The caller is responsible for that comparison.

    Returns:
        On a transition, a snapshot of the JUST-CLOSED window as
        (start_ts, last_in_state_ts, speed_mph_sum, sample_count). The window
        is then reset to start on the current frame (the first in-state row
        of the new state).

        Returns None on non-transition frames (window simply extended).

    Notes:
        Speeds are accumulated in mph to match add_interval_event's detail
        string format. Conversion factor 2.23694 matches engine._emit_event.
    """
    speed_mph = speed_m_s * 2.23694

    if transitioned:
        snapshot = (
            state.state_window_start_ts
            if state.state_window_start_ts is not None
            else ts_epoch_s,
            state.state_window_last_ts
            if state.state_window_last_ts is not None
            else ts_epoch_s,
            state.state_window_speed_mph_sum,
            state.state_window_sample_count,
        )
        # Reset window to start on this (first in-state) row.
        state.state_window_start_ts = ts_epoch_s
        state.state_window_last_ts = ts_epoch_s
        state.state_window_speed_mph_sum = speed_mph
        state.state_window_sample_count = 1
        return snapshot

    # Same state as previous frame: extend the window.
    if state.state_window_start_ts is None:
        state.state_window_start_ts = ts_epoch_s
    state.state_window_last_ts = ts_epoch_s
    state.state_window_speed_mph_sum += speed_mph
    state.state_window_sample_count += 1
    return None


# =============================================================================
# Persistent-condition tracker (v0.2)
# =============================================================================

@dataclass
class PersistentConditionTracker:
    """Tracks one continuous "condition true" interval and decides when to emit.

    Streaming analog of the analyzer's detect_persistent_condition() loop.
    The condition is evaluated per frame by the caller (engine), via the rules
    library's is_gps_bad() / is_gps_loss_while_moving() scalar helpers; this
    class manages the duration tracking, gps_confidence + speed_mph
    accumulation, and the flip-to-false emit decision.

    Mirrors add_interval_event's contract:
      - event stamped on the LAST in-condition row
      - duration = ts(last in-condition) - ts(first in-condition)
      - avg_speed_mph = mean(speed_mph) over the in-condition window
      - score = 1.0 - mean(gps_confidence) over the in-condition window

    Lifecycle:
        tracker.step(condition=False, ...)   # idle, returns None
        tracker.step(condition=True, ts=100, ...)   # enters, returns None
        tracker.step(condition=True, ts=101, ...)   # accumulates, returns None
        tracker.step(condition=True, ts=102, ...)   # accumulates, returns None
        tracker.step(condition=False, ts=103, ...)  # flip-to-false: returns
                                                    # emit-spec if duration ok
        tracker.step(condition=False, ...)   # idle again, returns None
    """
    event_type: str            # "gps_degraded_persistent" or "gps_loss_while_moving"
    min_duration_s: float      # cfg.gps_degraded_min_duration_s or gps_loss_while_moving_min_s
    score: float = 0.65        # placeholder; replaced by tracker's avg-confidence score on emit

    active: bool = False
    start_ts: Optional[float] = None
    last_in_state_ts: Optional[float] = None
    gps_confidence_sum: float = 0.0
    speed_mph_sum: float = 0.0
    sample_count: int = 0

    def step(
        self,
        condition: bool,
        ts: float,
        gps_confidence: float,
        speed_m_s: float,
    ) -> Optional[dict]:
        """Advance one frame.

        Returns an emit-spec dict on flip-to-false past threshold, else None.

        Emit-spec keys:
            event_type:        the configured event type string
            last_in_state_ts:  ts to stamp the event with
            duration_s:        ts(last) - ts(first)
            score:             1.0 - (gps_confidence_sum / sample_count)
            avg_speed_mph:     speed_mph_sum / sample_count
            avg_gps_confidence: gps_confidence_sum / sample_count (for caller use)
        """
        if condition:
            if not self.active:
                # Enter the in-condition window. First frame: pin start.
                self.active = True
                self.start_ts = ts
                self.last_in_state_ts = ts
                self.gps_confidence_sum = gps_confidence
                self.speed_mph_sum = speed_m_s * 2.23694
                self.sample_count = 1
            else:
                # Already in the window; extend.
                self.last_in_state_ts = ts
                self.gps_confidence_sum += gps_confidence
                self.speed_mph_sum += speed_m_s * 2.23694
                self.sample_count += 1
            return None

        # condition is False
        if not self.active:
            # Idle. Nothing to do.
            return None

        # Flip-to-false: window just closed. Decide whether to emit.
        start_ts = self.start_ts if self.start_ts is not None else ts
        last_in_state_ts = self.last_in_state_ts if self.last_in_state_ts is not None else ts
        duration_s = last_in_state_ts - start_ts

        emit_spec: Optional[dict] = None
        if duration_s >= self.min_duration_s and self.sample_count > 0:
            avg_gps_conf = self.gps_confidence_sum / self.sample_count
            avg_speed_mph = self.speed_mph_sum / self.sample_count
            emit_spec = {
                "event_type": self.event_type,
                "last_in_state_ts": last_in_state_ts,
                "duration_s": duration_s,
                "score": 1.0 - avg_gps_conf,
                "avg_speed_mph": avg_speed_mph,
                "avg_gps_confidence": avg_gps_conf,
            }

        # Reset for the next episode.
        self.active = False
        self.start_ts = None
        self.last_in_state_ts = None
        self.gps_confidence_sum = 0.0
        self.speed_mph_sum = 0.0
        self.sample_count = 0
        return emit_spec

    def flush(self) -> Optional[dict]:
        """End-of-session: if currently active and past threshold, emit.

        Mirrors detect_persistent_condition's tail block at line 588-594 of
        edge_analyzer: if condition is still active when the stream ends,
        treat the last in-state row as the closing edge.
        """
        if not self.active or self.sample_count == 0:
            return None
        start_ts = self.start_ts if self.start_ts is not None else 0.0
        last_in_state_ts = self.last_in_state_ts if self.last_in_state_ts is not None else start_ts
        duration_s = last_in_state_ts - start_ts
        if duration_s < self.min_duration_s:
            return None
        avg_gps_conf = self.gps_confidence_sum / self.sample_count
        avg_speed_mph = self.speed_mph_sum / self.sample_count
        return {
            "event_type": self.event_type,
            "last_in_state_ts": last_in_state_ts,
            "duration_s": duration_s,
            "score": 1.0 - avg_gps_conf,
            "avg_speed_mph": avg_speed_mph,
            "avg_gps_confidence": avg_gps_conf,
        }


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
    # missing speed values are treated as 0.0 in the diff calculation. The raw
    # decel/accel still reflect GPS speed deltas (including glitch spikes); as of
    # event_rules_v0_4_0 the detectors reject the unphysical ones via frame-GPS
    # trust, location, IMU corroboration, and a physical cap (see is_hard_brake).
    speed_now = frame.gps_speed_m_s if frame.gps_speed_m_s is not None else 0.0
    prev_speed = state.prev_speed_m_s if state.prev_speed_m_s is not None else 0.0
    long_a = longitudinal_accel_m_s2(speed_now, prev_speed, dt_s)
    decel = max(0.0, -long_a) if long_a < 0 else 0.0
    accel = max(0.0, long_a) if long_a > 0 else 0.0

    # -- GPS heading-rate lateral accel (sharp-turn signal, v0_4_0) ----------
    # Mirrors edge_analyzer's lat_accel_m_s2 = |speed * d(track)/dt| with the
    # heading delta wrapped to [-180, 180]. speed_now is the fillna(0) value,
    # matching the analyzer.
    lat_accel = lateral_accel_m_s2(
        speed_now, frame.gps_track_deg, state.prev_gps_track_deg, dt_s,
    )

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
    state.prev_gps_track_deg = frame.gps_track_deg
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
        lat_accel_m_s2=lat_accel,
    )
