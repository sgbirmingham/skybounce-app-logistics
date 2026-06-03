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
    is_gps_bad,
    is_gps_loss_while_moving,
    is_hard_accel,
    is_hard_brake,
    is_moderate_impact,
    is_severe_impact,
)

from .csv_source import SensorFrame
from .state import (
    ComputedFeatures,
    PersistentConditionTracker,
    StreamingState,
    process_frame,
    update_state_window,
)
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
        # NB: _current_state_start_ts removed in the state-window refactor.
        # Window timing (start_ts, last_ts, speed_sum, sample_count) now lives
        # in StreamingState and is managed by update_state_window().

        self._startup_acquired_emitted = False
        self._startup_acquiring_emitted = False

        # Persistent-condition trackers (v0.2).
        # One per event type. Each receives a per-frame condition decision
        # (via the rules library's is_gps_bad / is_gps_loss_while_moving
        # helpers) and emits on flip-to-false past its min_duration_s threshold.
        self._gps_degraded_tracker = PersistentConditionTracker(
            event_type="gps_degraded_persistent",
            min_duration_s=self.cfg.gps_degraded_min_duration_s,
        )
        self._gps_loss_moving_tracker = PersistentConditionTracker(
            event_type="gps_loss_while_moving",
            min_duration_s=self.cfg.gps_loss_while_moving_min_s,
        )

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
        self._update_persistent_conditions(feats)
        # Forward the frame clock to the transport. Lets a wrapping transport
        # (P1BatchingTransport) close 5-min windows on time even if no events
        # fire in a window. No-op on the per-event-only FileTransport and
        # IpcTransport. hasattr guard keeps backward compat with any older
        # Transport implementation that predates the tick() method.
        if hasattr(self.transport, "tick"):
            self.transport.tick(frame.ts_epoch_s, frame.elapsed_s)

    def flush(self) -> None:
        """Emit any pending interval events. Call at session end.

        The state-window helper has been accumulating start_ts/last_ts/speed_sum
        for the still-active state. We treat session-end as a transition with
        no successor: emit the current window as a closed interval.

        Matches the analyzer's detect_state_intervals fallback at lines 622-625:
        if the final state runs to end-of-stream, it is emitted as an interval
        ending on the last row.

        Also drains the persistent-condition trackers if their conditions are
        still active at end-of-stream (mirrors detect_persistent_condition's
        tail block at edge_analyzer lines 588-594).
        """
        # Drain persistent-condition trackers first. Their stamps live on
        # ts_epoch_s independently of state-window timing, so ordering with
        # state events doesn't matter for correctness; we emit them first
        # only for consistency with how the analyzer's detect_events()
        # interleaves these (state events / persistent events / point events).
        for tracker in (self._gps_degraded_tracker, self._gps_loss_moving_tracker):
            spec = tracker.flush()
            if spec is not None:
                self._emit_persistent_condition_event(spec)

        if self._current_state is None:
            return

        st = self.state
        if st.state_window_start_ts is None or st.state_window_last_ts is None:
            return

        self._maybe_emit_state_interval(
            state_name=self._current_state,
            start_ts=st.state_window_start_ts,
            last_in_state_ts=st.state_window_last_ts,
            speed_sum_mph=st.state_window_speed_mph_sum,
            sample_count=st.state_window_sample_count,
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

        The interval is stamped on the LAST in-state row (matching the
        analyzer's add_interval_event seg.iloc[-1] convention), and the
        avg_speed_mph in the detail string is the mean of speed_mph across
        the in-state samples (matching seg["speed_mph"].mean()).
        """
        if feats.elapsed_s < self.cfg.startup_suppress_s:
            return

        new_state = feats.analyzer_state
        if new_state in ("STARTUP_GPS_ACQUIRING", "STARTUP_GPS_READY"):
            return

        # First post-startup state we see. Initialize the window; nothing to emit.
        if self._current_state is None:
            self._current_state = new_state
            update_state_window(
                self.state, feats.ts_epoch_s, feats.speed_m_s, transitioned=True,
            )
            return

        transitioned = new_state != self._current_state
        snapshot = update_state_window(
            self.state, feats.ts_epoch_s, feats.speed_m_s, transitioned=transitioned,
        )

        if not transitioned:
            return

        # Transition: snapshot is the just-closed state's window.
        start_ts, last_in_state_ts, speed_sum, sample_count = snapshot
        self._maybe_emit_state_interval(
            state_name=self._current_state,
            start_ts=start_ts,
            last_in_state_ts=last_in_state_ts,
            speed_sum_mph=speed_sum,
            sample_count=sample_count,
        )

        self._current_state = new_state

    def _maybe_emit_state_interval(
        self,
        state_name: str,
        start_ts: float,
        last_in_state_ts: float,
        speed_sum_mph: float,
        sample_count: int,
    ) -> None:
        """Decide whether the just-ended interval is worth emitting as an event.

        Mirrors detect_trip_events() and detect_movement_state_summaries()
        for the cases that fit forward-only streaming.

        Duration is computed as ts(last in-state) - ts(first in-state), and
        the detail string format is `f"duration={d:.0f}s, avg_speed={s:.1f} mph"`
        plus any per-event suffix, exactly matching add_interval_event.
        """
        cfg = self.cfg
        duration = last_in_state_ts - start_ts
        if duration < cfg.min_state_duration_s:
            return

        avg_speed_mph = speed_sum_mph / sample_count if sample_count > 0 else 0.0
        base_detail = f"duration={duration:.0f}s, avg_speed={avg_speed_mph:.1f} mph"

        # Snapshot features at the last in-state row. The engine has already
        # advanced past it (we're being called on the transition frame), but
        # st.prev_* still holds last-in-state values because the per-frame
        # delta update in process_frame happens AFTER the state machine call
        # path -- the prev_* fields hold the values from the row that triggered
        # the transition, which is the first OUT-of-state row. NB: this means
        # lat/lon/confidence in the emitted event reflect the first-out-of-state
        # row, not the last-in-state row. The analyzer uses seg.iloc[-1] which
        # is the last-in-state row. This residual delta is one sample of
        # lat/lon drift; for stationary STOPPED intervals it is zero, and for
        # moving intervals it is ~speed * 1s. Acceptable for v0.1; revisit
        # if a future RULES_VERSION bump touches interval emission.
        snapshot = self._snapshot_features_at(last_in_state_ts, state_name)

        # Trip start: first sustained MOVING/HIGHWAY/LOW_SPEED interval.
        if state_name in ("MOVING", "HIGHWAY", "LOW_SPEED"):
            if (not self._trip.trip_active) and duration >= cfg.trip_start_min_duration_s:
                self._emit_event(
                    feats=snapshot,
                    event_type="trip_start",
                    score=0.70,
                    detail=base_detail,
                )
                self._trip.trip_active = True

            # Once per trip: state_highway / state_moving summary
            if state_name == "HIGHWAY" and "HIGHWAY" not in self._trip.emitted_states:
                self._emit_event(snapshot, "state_highway", 0.40, base_detail)
                self._trip.emitted_states.add("HIGHWAY")
            elif state_name == "MOVING" and "MOVING" not in self._trip.emitted_states:
                self._emit_event(snapshot, "state_moving", 0.35, base_detail)
                self._trip.emitted_states.add("MOVING")

        elif state_name == "STOPPED":
            # state_stopped only for real stops (not stoplights).
            if duration >= cfg.state_stopped_min_duration_s:
                self._emit_event(snapshot, "state_stopped", 0.20, base_detail)

            # long_stop or trip_pause_or_parked.
            if self._trip.trip_active and duration >= cfg.trip_end_min_stopped_s:
                self._emit_event(
                    snapshot, "trip_pause_or_parked", 0.65,
                    f"{base_detail}, very long stopped interval",
                )
                # Reset per-trip flags so the next drive emits fresh state summaries.
                self._trip.emitted_states.clear()
                # trip_active stays True -- same session.

            elif self._trip.trip_active and duration >= cfg.long_stop_min_duration_s:
                self._emit_event(
                    snapshot, "long_stop", 0.45,
                    f"{base_detail}, stopped within active session",
                )

        elif state_name == "GPS_DEGRADED":
            if not self._trip.in_gps_degraded_episode:
                self._emit_event(snapshot, "state_gps_degraded", 0.50, base_detail)
                self._trip.in_gps_degraded_episode = True

        elif state_name == "GPS_UNTRUSTED":
            if not self._trip.in_gps_untrusted_episode:
                self._emit_event(snapshot, "state_gps_untrusted", 0.65, base_detail)
                self._trip.in_gps_untrusted_episode = True

        # When we leave a GPS-degraded/untrusted state, reset the episode flag.
        if state_name not in ("GPS_DEGRADED", "GPS_UNTRUSTED"):
            self._trip.in_gps_degraded_episode = False
            self._trip.in_gps_untrusted_episode = False

    def _snapshot_features_at(self, ts: float, state_name: str) -> ComputedFeatures:
        """Build a feature snapshot for interval-end events.

        Stamped on the supplied ts (which the caller sets to the last in-state
        row's timestamp, matching the analyzer's seg.iloc[-1] convention).

        The lat/lon/speed/confidence values come from self.state.prev_*, which
        currently hold the FIRST out-of-state row's values (one sample after
        last_in_state). See the note in _maybe_emit_state_interval. This is
        accepted v0.1 residual; the timestamp and detail string are correct
        to the sample, which is what matters for analyzer parity on
        elapsed_s, duration, and avg_speed.
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
    # Persistent-condition events (gps_degraded_persistent, gps_loss_while_moving)
    # -------------------------------------------------------------------------

    def _update_persistent_conditions(self, feats: ComputedFeatures) -> None:
        """Evaluate the two persistent-condition detectors for this frame.

        Mirrors edge_analyzer's detect_persistent_condition() calls at lines
        770-791. The per-row condition checks are delegated to the rules
        library (is_gps_bad / is_gps_loss_while_moving); the trackers manage
        duration accumulation and the emit decision.

        Suppress during the startup window, matching the analyzer's
        operational = df[~df["startup_suppressed"]] split.
        """
        if feats.elapsed_s < self.cfg.startup_suppress_s:
            return

        # gps_degraded_persistent: GPS bad for >= cfg.gps_degraded_min_duration_s.
        gps_bad = is_gps_bad(
            gps_confidence=feats.gps_confidence,
            gps_data_stale_flag=feats.gps_data_stale_flag,
            gps_fix_stale=feats.gps_fix_stale,
            cfg=self.cfg,
        )
        spec = self._gps_degraded_tracker.step(
            condition=gps_bad,
            ts=feats.ts_epoch_s,
            gps_confidence=feats.gps_confidence,
            speed_m_s=feats.speed_m_s,
        )
        if spec is not None:
            self._emit_persistent_condition_event(spec)

        # gps_loss_while_moving: gps_bad AND speed >= cfg.moving_speed_m_s.
        gps_loss_moving = is_gps_loss_while_moving(
            gps_confidence=feats.gps_confidence,
            gps_data_stale_flag=feats.gps_data_stale_flag,
            gps_fix_stale=feats.gps_fix_stale,
            gps_speed_m_s=feats.speed_m_s,
            cfg=self.cfg,
        )
        spec = self._gps_loss_moving_tracker.step(
            condition=gps_loss_moving,
            ts=feats.ts_epoch_s,
            gps_confidence=feats.gps_confidence,
            speed_m_s=feats.speed_m_s,
        )
        if spec is not None:
            self._emit_persistent_condition_event(spec)

    def _emit_persistent_condition_event(self, spec: dict) -> None:
        """Emit a persistent-condition event from a tracker's emit-spec.

        spec contains: event_type, last_in_state_ts, duration_s, score,
                       avg_speed_mph, avg_gps_confidence.

        Detail string matches analyzer's add_interval_event:
            f"duration={d:.0f}s, avg_speed={s:.1f} mph"
        """
        detail = (
            f"duration={spec['duration_s']:.0f}s, "
            f"avg_speed={spec['avg_speed_mph']:.1f} mph"
        )
        snapshot = self._snapshot_features_at(
            spec["last_in_state_ts"],
            state_name=self.state.committed_state,
        )
        self._emit_event(
            feats=snapshot,
            event_type=spec["event_type"],
            score=spec["score"],
            detail=detail,
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
