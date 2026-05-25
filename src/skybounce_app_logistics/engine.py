"""
engine.py

The streaming event engine.

Pulls SensorFrames from a source iterator, advances the StreamingState,
calls the shared detectors, applies cooldown gating, emits events through
the chosen Transport.

This is the streaming equivalent of edge_analyzer's detect_events() +
state-interval logic, minus the look-backward features (clustering,
context windows, packet budget) which are deferred to v0.2.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Iterable, Optional

from skybounce_event_rules import (
    AnalyzerConfig,
    RULES_VERSION,
    classify_event_policy,
    cooldown_ok,
    is_hard_accel,
    is_hard_brake,
    is_moderate_impact,
    is_severe_impact,
)

from .csv_source import SensorFrame
from .state import ComputedFeatures, StreamingState, process_frame
from .transport import Event, Transport


log = logging.getLogger("skybounce.app.logistics.engine")


# -----------------------------------------------------------------------------
# Interval tracking (trips, long stops, GPS episodes)
# -----------------------------------------------------------------------------

@dataclass
class IntervalTracker:
    """Tracks one state's continuous occupancy for emitting interval events.

    The streaming analog of detect_state_intervals(). When the committed state
    leaves the tracked state, if the duration meets a threshold, emit an event.
    """
    state_name: str
    start_ts: Optional[float] = None
    accumulated_duration_s: float = 0.0
    has_emitted_this_episode: bool = False

    def enter(self, ts: float) -> None:
        self.start_ts = ts
        self.accumulated_duration_s = 0.0
        self.has_emitted_this_episode = False

    def update_duration(self, now_ts: float) -> float:
        if self.start_ts is None:
            return 0.0
        self.accumulated_duration_s = now_ts - self.start_ts
        return self.accumulated_duration_s

    def exit(self) -> None:
        self.start_ts = None
        self.accumulated_duration_s = 0.0
        self.has_emitted_this_episode = False


@dataclass
class TripContext:
    """Per-trip flags so we don't flood with repeated state summaries."""
    trip_active: bool = False
    emitted_states: set = field(default_factory=set)  # which state summaries emitted this trip
    in_gps_degraded_episode: bool = False
    in_gps_untrusted_episode: bool = False


# -----------------------------------------------------------------------------
# Engine
# -----------------------------------------------------------------------------

class StreamingEngine:
    """Owns one streaming session: state, interval trackers, transport.

    Lifecycle:
        engine = StreamingEngine(transport=t)
        for frame in source:
            engine.consume(frame)
        engine.flush()      # emit any pending interval events on shutdown
    """

    def __init__(
        self,
        transport: Transport,
        cfg: Optional[AnalyzerConfig] = None,
    ) -> None:
        self.cfg = cfg or AnalyzerConfig()
        self.state = StreamingState(cfg=self.cfg)
        self.transport = transport

        # Interval trackers, one per state we summarize.
        self._trackers: dict[str, IntervalTracker] = {}
        self._trip = TripContext()
        self._current_state: Optional[str] = None
        self._current_state_start_ts: Optional[float] = None

        self._startup_acquired_emitted = False
        self._startup_acquiring_emitted = False

        log.info("streaming engine started, rules=%s", RULES_VERSION)

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def consume(self, frame: SensorFrame) -> None:
        """Process one sensor frame end to end."""
        feats = process_frame(self.state, frame)
        self._detect_startup_events(feats)
        self._detect_point_events(feats, frame)
        self._update_state_tracking(feats)

    def flush(self) -> None:
        """Emit any pending interval events. Call at session end."""
        if self._current_state is None or self._current_state_start_ts is None:
            return
        # Use the last seen ts as the synthetic "exit" timestamp.
        last_ts = self.state.prev_ts_epoch_s
        if last_ts is None:
            return
        self._maybe_emit_state_interval(
            state_name=self._current_state,
            start_ts=self._current_state_start_ts,
            end_ts=last_ts,
            reason="session_end",
        )

    # -------------------------------------------------------------------------
    # Startup events
    # -------------------------------------------------------------------------

    def _detect_startup_events(self, feats: ComputedFeatures) -> None:
        """Emit one-shot startup events while in the suppression window."""
        if feats.elapsed_s >= self.cfg.startup_suppress_s:
            return

        # Emit "acquiring" once at the start if confidence is low.
        if not self._startup_acquiring_emitted and feats.gps_confidence < 0.40:
            self._emit_event(
                feats=feats,
                event_type="startup_gps_acquiring",
                score=0.25,
                detail="startup GPS not trusted",
            )
            self._startup_acquiring_emitted = True

        # Emit "acquired" the first time confidence crosses 0.80 during startup.
        if not self._startup_acquired_emitted and feats.gps_confidence >= 0.80:
            acquisition_s = feats.elapsed_s
            score = min(acquisition_s / max(self.cfg.startup_suppress_s, 1.0), 1.0)
            self._emit_event(
                feats=feats,
                event_type="startup_gps_acquired",
                score=score,
                detail=f"GPS acquired in {acquisition_s:.0f}s",
            )
            self._startup_acquired_emitted = True

    # -------------------------------------------------------------------------
    # Point events
    # -------------------------------------------------------------------------

    def _detect_point_events(self, feats: ComputedFeatures, frame: SensorFrame) -> None:
        """Run the four point detectors with cooldown gating.

        Suppress during the startup window, exactly like the analyzer.
        """
        if feats.elapsed_s < self.cfg.startup_suppress_s:
            return

        t = feats.ts_epoch_s
        cfg = self.cfg

        # Severe impact
        fired, score, detail = is_severe_impact(
            lin_accel_g=feats.lin_accel_g,
            jerk_g_s=feats.jerk_g_s,
            gps_confidence=feats.gps_confidence,
            gps_speed_m_s=feats.speed_m_s,
            cfg=cfg,
        )
        if fired and cooldown_ok(self.state.last_event_ts, "severe_impact", t, cfg.impact_cooldown_s):
            self._emit_event(feats, "severe_impact", score, detail)
            return  # mirror analyzer's `continue`

        # Moderate impact
        fired, score, detail = is_moderate_impact(
            lin_accel_g=feats.lin_accel_g,
            jerk_g_s=feats.jerk_g_s,
            gps_confidence=feats.gps_confidence,
            gps_speed_m_s=feats.speed_m_s,
            cfg=cfg,
        )
        if fired and cooldown_ok(self.state.last_event_ts, "moderate_impact", t, cfg.impact_cooldown_s):
            self._emit_event(feats, "moderate_impact", score, detail)

        # Hard brake
        fired, score, detail = is_hard_brake(
            decel=feats.decel_m_s2,
            gps_confidence=feats.gps_confidence,
            cfg=cfg,
        )
        if fired and cooldown_ok(self.state.last_event_ts, "hard_brake", t, cfg.accel_event_cooldown_s):
            self._emit_event(feats, "hard_brake", score, detail)

        # Hard accel
        fired, score, detail = is_hard_accel(
            accel=feats.accel_m_s2,
            gps_confidence=feats.gps_confidence,
            cfg=cfg,
        )
        if fired and cooldown_ok(self.state.last_event_ts, "hard_accel", t, cfg.accel_event_cooldown_s):
            self._emit_event(feats, "hard_accel", score, detail)

    # -------------------------------------------------------------------------
    # State / trip / stop / GPS episode tracking
    # -------------------------------------------------------------------------

    def _update_state_tracking(self, feats: ComputedFeatures) -> None:
        """Watch committed state transitions and emit interval events.

        Mirrors the analyzer's detect_trip_events + detect_movement_state_summaries
        logic, but in forward-only streaming form.
        """
        if feats.elapsed_s < self.cfg.startup_suppress_s:
            return

        new_state = feats.analyzer_state
        if new_state in ("STARTUP_GPS_ACQUIRING", "STARTUP_GPS_READY"):
            return

        # First post-startup state we see.
        if self._current_state is None:
            self._current_state = new_state
            self._current_state_start_ts = feats.ts_epoch_s
            return

        if new_state == self._current_state:
            return

        # State transition. Emit interval event for the state we just left.
        self._maybe_emit_state_interval(
            state_name=self._current_state,
            start_ts=self._current_state_start_ts or feats.ts_epoch_s,
            end_ts=feats.ts_epoch_s,
            reason="state_transition",
        )

        self._current_state = new_state
        self._current_state_start_ts = feats.ts_epoch_s

    def _maybe_emit_state_interval(
        self,
        state_name: str,
        start_ts: float,
        end_ts: float,
        reason: str,
    ) -> None:
        """Decide whether the just-ended interval is worth emitting as an event.

        Mirrors detect_trip_events() and detect_movement_state_summaries()
        for the cases that fit forward-only streaming.
        """
        cfg = self.cfg
        duration = end_ts - start_ts
        if duration < cfg.min_state_duration_s:
            return

        # Build a synthetic "features" snapshot at interval end. We pull from
        # the engine's current state, which represents the last frame seen.
        # NB: for a real interval-end frame we want the values *at end_ts*, not
        # "now". The state machine has already moved on, so we approximate
        # using the last computed values held by self.state.
        synthetic = self._synthetic_features_at(end_ts, state_name)

        # Trip start: first sustained MOVING/HIGHWAY/LOW_SPEED interval.
        if state_name in ("MOVING", "HIGHWAY", "LOW_SPEED"):
            if (not self._trip.trip_active) and duration >= cfg.trip_start_min_duration_s:
                self._emit_event(
                    feats=synthetic,
                    event_type="trip_start",
                    score=0.70,
                    detail=f"duration={duration:.0f}s",
                )
                self._trip.trip_active = True

            # Once per trip: state_highway / state_moving summary
            if state_name == "HIGHWAY" and "HIGHWAY" not in self._trip.emitted_states:
                self._emit_event(synthetic, "state_highway", 0.40, f"duration={duration:.0f}s")
                self._trip.emitted_states.add("HIGHWAY")
            elif state_name == "MOVING" and "MOVING" not in self._trip.emitted_states:
                self._emit_event(synthetic, "state_moving", 0.35, f"duration={duration:.0f}s")
                self._trip.emitted_states.add("MOVING")

        elif state_name == "STOPPED":
            # state_stopped only for real stops (not stoplights).
            if duration >= cfg.state_stopped_min_duration_s:
                self._emit_event(synthetic, "state_stopped", 0.20, f"duration={duration:.0f}s")

            # long_stop or trip_pause_or_parked.
            if self._trip.trip_active and duration >= cfg.trip_end_min_stopped_s:
                self._emit_event(synthetic, "trip_pause_or_parked", 0.65,
                                 f"duration={duration:.0f}s, very long stopped interval")
                # Reset per-trip flags so the next drive emits fresh state summaries.
                self._trip.emitted_states.clear()
                # trip_active stays True -- same session.

            elif self._trip.trip_active and duration >= cfg.long_stop_min_duration_s:
                self._emit_event(synthetic, "long_stop", 0.45,
                                 f"duration={duration:.0f}s, stopped within active session")

        elif state_name == "GPS_DEGRADED":
            if not self._trip.in_gps_degraded_episode:
                self._emit_event(synthetic, "state_gps_degraded", 0.50, f"duration={duration:.0f}s")
                self._trip.in_gps_degraded_episode = True

        elif state_name == "GPS_UNTRUSTED":
            if not self._trip.in_gps_untrusted_episode:
                self._emit_event(synthetic, "state_gps_untrusted", 0.65, f"duration={duration:.0f}s")
                self._trip.in_gps_untrusted_episode = True

        # When we leave a GPS-degraded/untrusted state, reset the episode flag.
        if state_name not in ("GPS_DEGRADED", "GPS_UNTRUSTED"):
            self._trip.in_gps_degraded_episode = False
            self._trip.in_gps_untrusted_episode = False

    def _synthetic_features_at(self, ts: float, state_name: str) -> ComputedFeatures:
        """Build a feature snapshot for interval-end events.

        We don't have the exact frame at end_ts (the engine has already moved
        past it). For interval summaries this is acceptable: the values that
        matter for packet encoding (lat/lon at event time, speed, confidence)
        come from the most recently seen frame, which is what the analyzer
        also does -- it uses the seg.iloc[-1] row.
        """
        st = self.state
        return ComputedFeatures(
            ts_epoch_s=ts,
            elapsed_s=ts - (st.session_start_ts or ts),
            dt_s=0.0,
            accel_mag_g=st.prev_accel_mag_g or 1.0,
            lin_accel_g=0.0,
            jerk_g_s=0.0,
            accel_m_s2=0.0,
            decel_m_s2=0.0,
            gps_fix_age_s=0.0 if st.last_good_fix_ts is None else ts - st.last_good_fix_ts,
            gps_repeat_counter=st.gps_repeat_counter,
            gps_data_stale_flag=False,
            gps_fix_stale=False,
            gps_confidence_raw=0.0,
            gps_confidence=st.gps_confidence_smoothed or 0.0,
            gps_quality_state="UNKNOWN",
            movement_state_raw=state_name,
            speed_m_s=st.prev_speed_m_s if st.prev_speed_m_s is not None else 0.0,
            analyzer_state_candidate=state_name,
            analyzer_state=state_name,
        )

    # -------------------------------------------------------------------------
    # Emission
    # -------------------------------------------------------------------------

    def _emit_event(
        self,
        feats: ComputedFeatures,
        event_type: str,
        score: float,
        detail: str,
    ) -> None:
        """Classify, build an Event, hand it to the transport."""
        st = self.state
        has_loc = st.prev_gps_lat is not None and st.prev_gps_lon is not None

        priority, event_class, packet_rank, reason = classify_event_policy(
            event_type=event_type,
            score=score,
            decel=feats.decel_m_s2,
            accel=feats.accel_m_s2,
            gps_confidence=feats.gps_confidence,
            has_location=has_loc,
        )

        ev = Event(
            ts_epoch_s=feats.ts_epoch_s,
            elapsed_s=feats.elapsed_s,
            event_type=event_type,
            score=score,
            priority=priority,
            event_class=event_class,
            packet_rank=packet_rank,
            policy_reason=reason,
            detail=detail,
            speed_m_s=feats.speed_m_s,
            speed_mph=feats.speed_m_s * 2.23694,
            decel_m_s2=feats.decel_m_s2,
            accel_m_s2=feats.accel_m_s2,
            lin_accel_g=feats.lin_accel_g,
            jerk_g_s=feats.jerk_g_s,
            gps_confidence=feats.gps_confidence,
            gps_lat=st.prev_gps_lat,
            gps_lon=st.prev_gps_lon,
            anchor_lat=st.anchor_lat,
            anchor_lon=st.anchor_lon,
            analyzer_state=feats.analyzer_state,
        )
        self.transport.emit(ev)


def run_engine(
    frames: Iterable[SensorFrame],
    transport: Transport,
    cfg: Optional[AnalyzerConfig] = None,
) -> StreamingEngine:
    """Convenience: run an engine to completion over an iterable of frames.

    Returns the engine so the caller can inspect final state.
    """
    engine = StreamingEngine(transport=transport, cfg=cfg)
    for frame in frames:
        engine.consume(frame)
    engine.flush()
    return engine
