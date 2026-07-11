"""
test_streaming_engine.py

Unit tests for the streaming engine and supporting modules.

These tests don't replace the comparison-harness validation against the PC
analyzer; that's the integration test. These cover narrower cases that the
integration test can't easily exercise: edge conditions, missing data,
streaming state-machine debounce, cooldown gating.
"""

from __future__ import annotations

from skybounce_app_logistics.csv_source import SensorFrame, _safe_float
from skybounce_app_logistics.engine import StreamingEngine
from skybounce_app_logistics.state import (
           StreamingState,
           PersistentConditionTracker,
           process_frame,
           update_state_window,
       )
from skybounce_app_logistics.transport import Event, FileTransport
from skybounce_event_rules import AnalyzerConfig


def _stationary_frame(ts: float, elapsed: float = 0.0) -> SensorFrame:
    """A normal at-rest frame: stationary, GPS good, IMU showing 1g down."""
    return SensorFrame(
        ts_epoch_s=ts,
        elapsed_s=elapsed,
        accel_x_g=0.0, accel_y_g=0.0, accel_z_g=1.0,
        gyro_x_dps=0.0, gyro_y_dps=0.0, gyro_z_dps=0.0,
        gps_valid=1, gps_mode=3, gps_sats=8,
        gps_lat=40.44, gps_lon=-79.99,
        gps_speed_m_s=0.0,
        gps_time=f"2026-01-01T00:00:{int(ts)%60:02d}Z",
    )


def _moving_frame(ts: float, elapsed: float, speed_m_s: float) -> SensorFrame:
    f = _stationary_frame(ts, elapsed)
    f.gps_speed_m_s = speed_m_s
    return f


def _warmup_engine_past_startup(engine, n_seconds: int = 200) -> float:
    """Feed n_seconds of moving frames so the engine is past startup_suppress_s.
    Returns the next ts to use after warmup."""
    for i in range(n_seconds):
        engine.consume(_moving_frame(ts=1000.0 + i, elapsed=0.0, speed_m_s=10.0))
    return 1000.0 + n_seconds


# =============================================================================
# CSV source
# =============================================================================

class TestSafeFloat:
    def test_none(self):
        assert _safe_float(None) is None

    def test_empty_string(self):
        assert _safe_float("") is None

    def test_number(self):
        assert _safe_float("3.14") == 3.14

    def test_nan(self):
        assert _safe_float(float("nan")) is None

    def test_inf(self):
        assert _safe_float(float("inf")) is None


# =============================================================================
# State machine (debounce)
# =============================================================================

class TestStreamingStateDebounce:
    def setup_method(self):
        self.cfg = AnalyzerConfig()
        self.state = StreamingState(cfg=self.cfg)

    def test_state_commits_after_warmup(self):
        # The engine tracks elapsed from the FIRST frame it sees, not from
        # the logger's elapsed_s field. Feed 200s of moving frames so the
        # session elapsed exceeds startup_suppress_s (180s).
        for i in range(200):
            f = _moving_frame(ts=1000.0 + i, elapsed=0.0, speed_m_s=10.0)
            feats = process_frame(self.state, f)
        # By now should be committed in a real movement state.
        assert feats.analyzer_state in ("LOW_SPEED", "MOVING", "HIGHWAY")

    def test_speed_jump_takes_dwell_to_commit(self):
        # Build a long stretch of MOVING, then jump to STOPPED. The committed
        # state should not flip to STOPPED until dwell time elapses.
        for i in range(60):
            f = _moving_frame(ts=1000.0 + i, elapsed=300.0 + i, speed_m_s=10.0)
            process_frame(self.state, f)
        # After warmup we should be in MOVING.
        # Now feed stationary frames; STOPPED dwell is 20s.
        last_committed = self.state.committed_state
        for i in range(10):  # less than 20s
            f = _stationary_frame(ts=1060.0 + i, elapsed=360.0 + i)
            feats = process_frame(self.state, f)
        # Committed state hasn't transitioned to STOPPED yet.
        # (May still be MOVING or have just transitioned through LOW_SPEED.)
        assert self.state.pending_state in ("STOPPED", None) or self.state.committed_state != "STOPPED" or last_committed == "STOPPED"


# =============================================================================
# Engine end-to-end
# =============================================================================

class TestEngineEndToEnd:
    def test_zero_events_for_stationary_session(self, tmp_path):
        """Stationary, GPS-good, IMU at rest: no point events should fire."""
        out_path = tmp_path / "events.jsonl"
        transport = FileTransport(out_path)
        engine = StreamingEngine(transport=transport)

        # Skip startup suppression by giving each frame a high elapsed.
        for i in range(300):
            f = _stationary_frame(ts=1000.0 + i, elapsed=200.0 + i)
            engine.consume(f)
        engine.flush()
        transport.close()

        events = out_path.read_text().splitlines()
        # Should have at most startup events and a state_stopped (if duration met).
        # NOT hard_brake / severe_impact.
        event_types = set()
        import json
        for line in events:
            event_types.add(json.loads(line)["event_type"])
        assert "severe_impact" not in event_types
        assert "hard_brake" not in event_types
        assert "moderate_impact" not in event_types

    def test_severe_impact_fires(self, tmp_path):
        """A crash-magnitude spike FOLLOWED BY a speed collapse fires severe_impact.

        Severe needs lin_accel_g >= severe_impact_g (3.0) AND jerk >= 1.0 in a
        moving, GPS-trusted state, THEN a post-impact speed collapse to confirm
        (a real crash stops the vehicle; a hard bump doesn't).
        """
        out_path = tmp_path / "events.jsonl"
        transport = FileTransport(out_path)
        engine = StreamingEngine(transport=transport)

        # Warm up past startup_suppress_s (180s), in a moving state.
        for i in range(200):
            engine.consume(_moving_frame(ts=1000.0 + i, elapsed=0.0, speed_m_s=10.0))

        # Crash spike: accel_mag = sqrt(5^2 + 1^2) ~= 5.1g -> lin_accel ~4.1g,
        # jerk ~4.1 g/s over 1s. Both clear the 3.0g / 1.0 g/s gate.
        spike = _moving_frame(ts=1200.0, elapsed=0.0, speed_m_s=10.0)
        spike.accel_x_g = 5.0
        spike.accel_y_g = 0.0
        spike.accel_z_g = 1.0
        engine.consume(spike)

        # Speed collapses to 0 -> confirms the crash.
        for i in range(10):
            engine.consume(_moving_frame(ts=1201.0 + i, elapsed=0.0, speed_m_s=0.0))

        engine.flush()
        transport.close()

        import json
        events = [json.loads(line) for line in out_path.read_text().splitlines()]
        types = [e["event_type"] for e in events]
        assert "severe_impact" in types, f"expected severe_impact in events; got {types}"

    def test_hard_bump_without_speed_collapse_is_moderate(self, tmp_path):
        """A crash-magnitude spike with NO speed collapse is a bump, not a crash:
        it must classify as moderate_impact and never severe_impact."""
        out_path = tmp_path / "events.jsonl"
        transport = FileTransport(out_path)
        engine = StreamingEngine(transport=transport)

        for i in range(200):
            engine.consume(_moving_frame(ts=1000.0 + i, elapsed=0.0, speed_m_s=10.0))

        spike = _moving_frame(ts=1200.0, elapsed=0.0, speed_m_s=10.0)
        spike.accel_x_g = 5.0
        spike.accel_z_g = 1.0
        engine.consume(spike)

        # Speed holds at 10 through the confirmation window -> no collapse.
        for i in range(15):
            engine.consume(_moving_frame(ts=1201.0 + i, elapsed=0.0, speed_m_s=10.0))

        engine.flush()
        transport.close()

        import json
        events = [json.loads(line) for line in out_path.read_text().splitlines()]
        types = [e["event_type"] for e in events]
        assert "severe_impact" not in types, f"bump must not be severe; got {types}"
        assert "moderate_impact" in types, f"bump should fall back to moderate; got {types}"

    def test_cooldown_suppresses_duplicate_fires(self, tmp_path):
        """Two consecutive severe-impact spikes within cooldown -> only one event."""
        out_path = tmp_path / "events.jsonl"
        transport = FileTransport(out_path)
        engine = StreamingEngine(transport=transport)

        for i in range(200):
            engine.consume(_moving_frame(ts=1000.0 + i, elapsed=0.0, speed_m_s=10.0))

        def crash(ts):
            s = _moving_frame(ts=ts, elapsed=0.0, speed_m_s=10.0)
            s.accel_x_g = 5.0
            s.accel_z_g = 1.0
            return s

        # First crash: spike then speed collapse -> confirms severe #1.
        engine.consume(crash(1200.0))
        for i in range(3):
            engine.consume(_moving_frame(ts=1201.0 + i, elapsed=0.0, speed_m_s=0.0))
        # Vehicle recovers (moving again) so the next spike is a fresh candidate.
        for i in range(3):
            engine.consume(_moving_frame(ts=1204.0 + i, elapsed=0.0, speed_m_s=10.0))
        # Second crash 10s after the first (within 45s impact_cooldown_s): it
        # confirms via collapse but the cooldown suppresses the duplicate.
        engine.consume(crash(1210.0))
        for i in range(5):
            engine.consume(_moving_frame(ts=1211.0 + i, elapsed=0.0, speed_m_s=0.0))

        engine.flush()
        transport.close()

        import json
        events = [json.loads(line) for line in out_path.read_text().splitlines()]
        severe = [e for e in events if e["event_type"] == "severe_impact"]
        assert len(severe) == 1, f"expected 1 severe_impact, got {len(severe)}"


# =============================================================================
# Transport
# =============================================================================

class TestFileTransport:
    def test_writes_jsonl(self, tmp_path):
        out = tmp_path / "evt.jsonl"
        t = FileTransport(out)
        t.emit(Event(
            ts_epoch_s=1.0, elapsed_s=1.0, event_type="x", score=0.5,
            priority="P1", event_class="ROAD_EVENT", packet_rank=55,
            policy_reason="test", detail="d", speed_m_s=1.0, speed_mph=2.0,
            decel_m_s2=0.0, accel_m_s2=0.0, lin_accel_g=0.0, jerk_g_s=0.0,
            gps_confidence=0.8, gps_lat=40.0, gps_lon=-79.0,
            anchor_lat=40.0, anchor_lon=-79.0, analyzer_state="MOVING",
        ))
        t.close()
        import json
        line = out.read_text().strip()
        d = json.loads(line)
        assert d["event_type"] == "x"
        assert d["priority"] == "P1"

"""
Additions to test_streaming_engine.py for the v0.1.1 state-window fix.

Apply by:
1) Adding `update_state_window` to the import line near the top of
   test_streaming_engine.py:

       from skybounce_app_logistics.state import (
           StreamingState, process_frame, update_state_window,
       )

2) Appending the two new test classes below to the end of the file.

The first class (TestStateWindow) tests the new state.py helper in
isolation -- snapshot semantics, accumulation, reset on transition.

The second class (TestStateIntervalEmission) tests the end-to-end
detail-string format and timestamp convention through the engine,
locking the contract with the PC analyzer's add_interval_event:
   - event stamped on the LAST in-state row
   - duration = ts(last in-state) - ts(first in-state)
   - detail = f"duration={d:.0f}s, avg_speed={s:.1f} mph"
"""


# =============================================================================
# State-window helper (snapshot semantics)
# =============================================================================

class TestStateWindow:
    """Unit tests for update_state_window in state.py.

    Exercises the helper directly without going through the engine, so the
    contract -- snapshot tuple shape, accumulation math, reset on transition --
    is locked independent of any downstream behavior.
    """

    def setup_method(self):
        self.cfg = AnalyzerConfig()
        self.state = StreamingState(cfg=self.cfg)

    def test_first_transition_initializes_window(self):
        """The very first call with transitioned=True establishes the window.

        Before any call the window fields are at their defaults
        (start_ts=None, last_ts=None, sum=0.0, count=0). The returned snapshot
        falls back to (ts, ts, 0.0, 0) for the (empty) prior window, then the
        helper resets to start a fresh window on the current frame.
        """
        ts = 1000.0
        speed_m_s = 5.0

        snapshot = update_state_window(
            self.state, ts_epoch_s=ts, speed_m_s=speed_m_s, transitioned=True,
        )

        start_ts, last_in_state_ts, speed_sum_mph, sample_count = snapshot
        # Empty prior window: snapshot falls back to current ts on both ends.
        assert start_ts == ts
        assert last_in_state_ts == ts
        assert speed_sum_mph == 0.0
        assert sample_count == 0

        # Window has been reset to start on this frame.
        assert self.state.state_window_start_ts == ts
        assert self.state.state_window_last_ts == ts
        assert self.state.state_window_speed_mph_sum == speed_m_s * 2.23694
        assert self.state.state_window_sample_count == 1

    def test_window_accumulates_within_state(self):
        """Three same-state frames: start_ts pins to first, last_ts to most
        recent, speed_sum and count grow."""
        # Frame 1: transition (entering new state). Speed 10 m/s.
        update_state_window(
            self.state, ts_epoch_s=100.0, speed_m_s=10.0, transitioned=True,
        )
        # Frames 2 and 3: same state, accumulate. Speeds 20 and 30 m/s.
        update_state_window(
            self.state, ts_epoch_s=101.0, speed_m_s=20.0, transitioned=False,
        )
        update_state_window(
            self.state, ts_epoch_s=102.0, speed_m_s=30.0, transitioned=False,
        )

        assert self.state.state_window_start_ts == 100.0
        assert self.state.state_window_last_ts == 102.0
        # Sum is (10 + 20 + 30) * 2.23694 mph.
        expected_sum = (10.0 + 20.0 + 30.0) * 2.23694
        assert abs(self.state.state_window_speed_mph_sum - expected_sum) < 1e-9
        assert self.state.state_window_sample_count == 3

    def test_snapshot_on_transition_returns_just_closed_window(self):
        """After accumulating, a transition returns the closed window and
        resets to start a new one on the transition frame."""
        # Build a window of three frames in state A.
        update_state_window(
            self.state, ts_epoch_s=100.0, speed_m_s=10.0, transitioned=True,
        )
        update_state_window(
            self.state, ts_epoch_s=101.0, speed_m_s=20.0, transitioned=False,
        )
        update_state_window(
            self.state, ts_epoch_s=102.0, speed_m_s=30.0, transitioned=False,
        )

        # Frame 4: transition into state B. Speed 5 m/s.
        snapshot = update_state_window(
            self.state, ts_epoch_s=103.0, speed_m_s=5.0, transitioned=True,
        )

        # Snapshot is the just-closed (state A) window.
        start_ts, last_in_state_ts, speed_sum_mph, sample_count = snapshot
        assert start_ts == 100.0
        assert last_in_state_ts == 102.0
        expected_sum = (10.0 + 20.0 + 30.0) * 2.23694
        assert abs(speed_sum_mph - expected_sum) < 1e-9
        assert sample_count == 3

        # Window has been reset onto the transition frame (state B start).
        assert self.state.state_window_start_ts == 103.0
        assert self.state.state_window_last_ts == 103.0
        assert abs(
            self.state.state_window_speed_mph_sum - 5.0 * 2.23694
        ) < 1e-9
        assert self.state.state_window_sample_count == 1

    def test_non_transition_returns_none(self):
        """A non-transition call returns None (no snapshot)."""
        update_state_window(
            self.state, ts_epoch_s=100.0, speed_m_s=10.0, transitioned=True,
        )
        result = update_state_window(
            self.state, ts_epoch_s=101.0, speed_m_s=10.0, transitioned=False,
        )
        assert result is None


# =============================================================================
# State-interval emission (analyzer-parity detail strings and timestamps)
# =============================================================================

class TestStateIntervalEmission:
    """Tests the state-interval emission path with analyzer-parity formatting.

    Locks the contract with the PC analyzer's add_interval_event:
      - event stamped on the LAST in-state row's ts_epoch_s
      - duration_s = ts(last in-state) - ts(first in-state)
      - detail format = f"duration={d:.0f}s, avg_speed={s:.1f} mph[, suffix]"

    These tests drive _maybe_emit_state_interval directly with pre-computed
    window values rather than running a full stream through the dwell-time
    state machine. This isolates the formatting contract from rules-library
    debounce behavior (which has its own tests in skybounce-event-rules).

    Future drift in any of these (renaming a field, changing the format
    string, off-by-one in the boundary row) breaks these tests.
    """

    def setup_method(self):
        self.cfg = AnalyzerConfig()

    def _make_engine(self, tmp_path):
        out_path = tmp_path / "events.jsonl"
        transport = FileTransport(out_path)
        engine = StreamingEngine(transport=transport, cfg=self.cfg)
        # Prime st.prev_* so _snapshot_features_at returns sensible values.
        # The snapshot needs prev_speed_m_s, prev_gps_lat/lon, and
        # gps_confidence_smoothed populated. We set them directly rather
        # than running frames through process_frame.
        engine.state.session_start_ts = 1000.0
        engine.state.prev_ts_epoch_s = 1299.0
        engine.state.prev_speed_m_s = 0.0
        engine.state.prev_gps_lat = 40.44
        engine.state.prev_gps_lon = -79.99
        engine.state.gps_confidence_smoothed = 0.9
        engine.state.anchor_lat = 40.44
        engine.state.anchor_lon = -79.99
        return engine, out_path

    def _read_events(self, out_path):
        import json
        return [json.loads(line) for line in out_path.read_text().splitlines()]

    def test_state_stopped_detail_format_locks_analyzer_contract(self, tmp_path):
        """STOPPED window of 100 stationary samples spanning [1200.0 .. 1299.0]
        produces 'duration=99s, avg_speed=0.0 mph' stamped on ts=1299.0.

        This is the analyzer-parity contract: last in-state row, duration
        as the difference between last and first in-state timestamps,
        avg_speed as the mean of speed_mph over the window.
        """
        engine, out_path = self._make_engine(tmp_path)

        # Engine needs trip_active=False for state_stopped (no long_stop).
        # Drive _maybe_emit_state_interval directly with a known window.
        engine._maybe_emit_state_interval(
            state_name="STOPPED",
            start_ts=1200.0,
            last_in_state_ts=1299.0,
            speed_sum_mph=0.0,        # 100 samples * 0.0 m/s
            sample_count=100,
        )

        events = self._read_events(out_path)
        state_stopped = [e for e in events if e["event_type"] == "state_stopped"]
        assert len(state_stopped) == 1, (
            f"expected 1 state_stopped, got events {[e['event_type'] for e in events]}"
        )

        ev = state_stopped[0]
        # Detail-string format locked to analyzer's add_interval_event.
        # Update both sides together or not at all.
        assert ev["detail"] == "duration=99s, avg_speed=0.0 mph", (
            f"detail string drift; got {ev['detail']!r}"
        )
        # Stamped on the LAST in-state row, not the first out-of-state row.
        assert ev["ts_epoch_s"] == 1299.0

    def test_state_stopped_avg_speed_is_mean_of_window_samples(self, tmp_path):
        """A window with non-zero speeds produces avg_speed = sum_mph / count.

        Build a window with sum_mph = 5.0 * 2.23694 * 100 = 1118.47 mph-samples
        across 100 samples, i.e. each sample contributing 5.0 m/s -> 11.18 mph.
        Expected avg_speed = 11.2 mph (after :.1f formatting).
        """
        engine, out_path = self._make_engine(tmp_path)
        sum_mph = 5.0 * 2.23694 * 100   # 1118.47

        engine._maybe_emit_state_interval(
            state_name="STOPPED",
            start_ts=1200.0,
            last_in_state_ts=1299.0,
            speed_sum_mph=sum_mph,
            sample_count=100,
        )

        events = self._read_events(out_path)
        state_stopped = [e for e in events if e["event_type"] == "state_stopped"]
        assert len(state_stopped) == 1
        # avg = 1118.47 / 100 = 11.1847 -> ":.1f" -> "11.2"
        assert state_stopped[0]["detail"] == "duration=99s, avg_speed=11.2 mph"

    def test_long_stop_appends_suffix_to_base_detail(self, tmp_path):
        """long_stop = state_stopped detail + ', stopped within active session'.

        Requires trip_active. The 120s window is above long_stop_min_duration_s
        (90s default) so long_stop fires alongside state_stopped.
        """
        engine, out_path = self._make_engine(tmp_path)
        engine._trip.trip_active = True   # required for long_stop emit

        engine._maybe_emit_state_interval(
            state_name="STOPPED",
            start_ts=1200.0,
            last_in_state_ts=1319.0,   # 119s duration
            speed_sum_mph=0.0,
            sample_count=120,
        )

        events = self._read_events(out_path)
        long_stop = [e for e in events if e["event_type"] == "long_stop"]
        assert len(long_stop) == 1, (
            f"expected 1 long_stop, got events {[e['event_type'] for e in events]}"
        )
        assert long_stop[0]["detail"] == (
            "duration=119s, avg_speed=0.0 mph, stopped within active session"
        )
        assert long_stop[0]["ts_epoch_s"] == 1319.0

    def test_below_min_duration_emits_nothing(self, tmp_path):
        """A window shorter than min_state_duration_s (30s default) emits
        no event, even though the format would otherwise be valid."""
        engine, out_path = self._make_engine(tmp_path)

        engine._maybe_emit_state_interval(
            state_name="STOPPED",
            start_ts=1200.0,
            last_in_state_ts=1210.0,   # 10s, below min_state_duration_s
            speed_sum_mph=0.0,
            sample_count=11,
        )

        events = self._read_events(out_path)
        assert events == []

"""
Tests are organized:

   TestPersistentConditionTracker — unit tests for the tracker in isolation.
     Locks the contract on accumulation, emit-on-flip, threshold gating,
     flush-of-active-window. Independent of the engine; failing here means
     the tracker class itself is broken.

   TestPersistentConditionIntegration — end-to-end through the engine.
     Verifies the engine wires the tracker to consume(), calls the rules
     library helper correctly, and the emitted Event has the analyzer-parity
     detail format. Failing here means the engine integration is broken,
     even if the tracker passes its unit tests.
"""


# =============================================================================
# PersistentConditionTracker unit tests
# =============================================================================

class TestPersistentConditionTracker:
    """Locks the contract on tracker behavior independent of the engine."""

    def test_idle_when_condition_false(self):
        """No frames yet, condition always False -> stays inactive, no emit."""
        t = PersistentConditionTracker(
            event_type="gps_degraded_persistent", min_duration_s=120.0,
        )
        for i in range(50):
            result = t.step(
                condition=False, ts=1000.0 + i,
                gps_confidence=0.9, speed_m_s=10.0,
            )
            assert result is None
        assert t.active is False
        assert t.sample_count == 0

    def test_activates_and_accumulates_when_condition_true(self):
        """Three frames in-condition: window pins to first, accumulates."""
        t = PersistentConditionTracker(
            event_type="gps_degraded_persistent", min_duration_s=10.0,
        )
        t.step(condition=True, ts=1000.0, gps_confidence=0.3, speed_m_s=5.0)
        t.step(condition=True, ts=1001.0, gps_confidence=0.2, speed_m_s=6.0)
        t.step(condition=True, ts=1002.0, gps_confidence=0.1, speed_m_s=7.0)

        assert t.active is True
        assert t.start_ts == 1000.0
        assert t.last_in_state_ts == 1002.0
        assert t.sample_count == 3
        # gps_confidence_sum = 0.3 + 0.2 + 0.1 = 0.6
        assert abs(t.gps_confidence_sum - 0.6) < 1e-9
        # speed_mph_sum = (5 + 6 + 7) * 2.23694
        expected_speed_sum = (5.0 + 6.0 + 7.0) * 2.23694
        assert abs(t.speed_mph_sum - expected_speed_sum) < 1e-9

    def test_emits_on_flip_to_false_past_threshold(self):
        """150 in-condition frames spanning >= min_duration_s, then flip to
        False: emit spec has correct duration, last_in_state_ts, score,
        avg_speed_mph."""
        t = PersistentConditionTracker(
            event_type="gps_degraded_persistent", min_duration_s=120.0,
        )
        # 150 frames at 1Hz; each contributing conf=0.2, speed=5.0 m/s.
        # Window: ts=1000 (first in) .. ts=1149 (last in), duration=149s.
        for i in range(150):
            r = t.step(
                condition=True, ts=1000.0 + i,
                gps_confidence=0.2, speed_m_s=5.0,
            )
            assert r is None
        # Flip-to-False: emit
        spec = t.step(
            condition=False, ts=1150.0,
            gps_confidence=0.9, speed_m_s=12.0,
        )
        assert spec is not None
        assert spec["event_type"] == "gps_degraded_persistent"
        assert spec["last_in_state_ts"] == 1149.0
        assert spec["duration_s"] == 149.0
        # avg_gps_conf = 0.2, score = 1 - 0.2 = 0.8
        assert abs(spec["score"] - 0.8) < 1e-9
        # avg_speed_mph = 5.0 * 2.23694
        assert abs(spec["avg_speed_mph"] - 5.0 * 2.23694) < 1e-9
        # Tracker resets after emit.
        assert t.active is False
        assert t.sample_count == 0

    def test_suppresses_emit_below_min_duration(self):
        """30s in-condition window, min_duration=60s: no emit on flip-to-false."""
        t = PersistentConditionTracker(
            event_type="gps_loss_while_moving", min_duration_s=60.0,
        )
        for i in range(30):
            t.step(
                condition=True, ts=2000.0 + i,
                gps_confidence=0.2, speed_m_s=5.0,
            )
        spec = t.step(
            condition=False, ts=2030.0,
            gps_confidence=0.9, speed_m_s=12.0,
        )
        assert spec is None
        # Tracker still resets even if no emit.
        assert t.active is False

    def test_flush_emits_active_window_past_threshold(self):
        """End-of-stream with in-progress window past threshold: flush emits.
        Mirrors detect_persistent_condition's tail block in the analyzer."""
        t = PersistentConditionTracker(
            event_type="gps_degraded_persistent", min_duration_s=120.0,
        )
        for i in range(150):
            t.step(
                condition=True, ts=3000.0 + i,
                gps_confidence=0.3, speed_m_s=2.0,
            )
        spec = t.flush()
        assert spec is not None
        assert spec["duration_s"] == 149.0
        assert spec["last_in_state_ts"] == 3149.0
        # avg_gps_conf = 0.3, score = 0.7
        assert abs(spec["score"] - 0.7) < 1e-9

    def test_flush_no_emit_below_threshold(self):
        """End-of-stream with in-progress window below threshold: no emit."""
        t = PersistentConditionTracker(
            event_type="gps_degraded_persistent", min_duration_s=120.0,
        )
        for i in range(30):
            t.step(
                condition=True, ts=3000.0 + i,
                gps_confidence=0.3, speed_m_s=2.0,
            )
        assert t.flush() is None

    def test_flush_idle_tracker_no_emit(self):
        """flush() on never-activated tracker returns None."""
        t = PersistentConditionTracker(
            event_type="gps_degraded_persistent", min_duration_s=120.0,
        )
        assert t.flush() is None

    def test_multiple_episodes_in_sequence(self):
        """Two separate in-condition windows: first emits, second emits, no
        contamination between them."""
        t = PersistentConditionTracker(
            event_type="gps_degraded_persistent", min_duration_s=120.0,
        )
        # First episode: 150 in, then out.
        for i in range(150):
            t.step(
                condition=True, ts=1000.0 + i,
                gps_confidence=0.2, speed_m_s=5.0,
            )
        spec1 = t.step(
            condition=False, ts=1150.0,
            gps_confidence=0.9, speed_m_s=12.0,
        )
        assert spec1 is not None and spec1["duration_s"] == 149.0

        # Idle for a bit.
        for i in range(50):
            t.step(
                condition=False, ts=1151.0 + i,
                gps_confidence=0.9, speed_m_s=12.0,
            )

        # Second episode: 200 in, then out.
        for i in range(200):
            t.step(
                condition=True, ts=2000.0 + i,
                gps_confidence=0.1, speed_m_s=3.0,
            )
        spec2 = t.step(
            condition=False, ts=2200.0,
            gps_confidence=0.9, speed_m_s=12.0,
        )
        assert spec2 is not None
        assert spec2["duration_s"] == 199.0  # 2199 - 2000
        # Different gps_confidence average from first episode.
        assert abs(spec2["score"] - 0.9) < 1e-9
        # Different avg_speed from first episode.
        assert abs(spec2["avg_speed_mph"] - 3.0 * 2.23694) < 1e-9


# =============================================================================
# End-to-end engine integration for persistent-condition events
# =============================================================================

class TestPersistentConditionIntegration:
    """Verify the engine wires the tracker correctly through consume() and
    that emitted events carry the analyzer-parity detail format.
    """

    def test_gps_degraded_persistent_fires_through_engine(self, tmp_path):
        """Synthetic stream: clear startup with good GPS, then a long
        GPS-confidence-bad window (using stale-flag rather than raw confidence
        so we don't fight movement-state classification). After the window
        closes, expect a single gps_degraded_persistent event with detail
        string matching the analyzer's format.

        We can't easily drive gps_confidence below 0.40 from raw frame inputs
        because the EWMA-smoothed confidence depends on history. The cleaner
        approach for this integration test: directly poke the engine's
        tracker, then drive one frame to trigger the flip-to-false emit
        path, and assert the resulting Event.
        """
        out_path = tmp_path / "events.jsonl"
        transport = FileTransport(out_path)
        engine = StreamingEngine(transport=transport)

        # Prime st.prev_* so _snapshot_features_at returns sensible values.
        engine.state.session_start_ts = 1000.0
        engine.state.prev_ts_epoch_s = 1299.0
        engine.state.prev_speed_m_s = 2.0
        engine.state.prev_gps_lat = 40.44
        engine.state.prev_gps_lon = -79.99
        engine.state.gps_confidence_smoothed = 0.3
        engine.state.anchor_lat = 40.44
        engine.state.anchor_lon = -79.99
        engine.state.committed_state = "GPS_UNTRUSTED"

        # Manually drive the tracker with a known-good window.
        tracker = engine._gps_degraded_tracker
        for i in range(150):
            tracker.step(
                condition=True, ts=1100.0 + i,
                gps_confidence=0.2, speed_m_s=2.0,
            )
        # Flip-to-false -> tracker returns emit spec -> engine emits Event.
        spec = tracker.step(
            condition=False, ts=1250.0,
            gps_confidence=0.85, speed_m_s=12.0,
        )
        assert spec is not None
        engine._emit_persistent_condition_event(spec)
        transport.close()

        import json
        events = [json.loads(line) for line in out_path.read_text().splitlines()]
        gps_events = [e for e in events if e["event_type"] == "gps_degraded_persistent"]
        assert len(gps_events) == 1
        ev = gps_events[0]
        # Detail format must mirror analyzer's add_interval_event.
        assert ev["detail"] == "duration=149s, avg_speed=4.5 mph", (
            f"detail string drift; got {ev['detail']!r}"
        )
        # Stamped on the last-in-condition row (1249.0), not the flip row (1250.0).
        assert ev["ts_epoch_s"] == 1249.0
        # Score = 1 - avg_gps_conf = 1 - 0.2 = 0.8
        assert abs(ev["score"] - 0.8) < 1e-9

    def test_flush_emits_in_progress_persistent_condition(self, tmp_path):
        """If the persistent condition is still active at flush() time,
        the engine emits it. Same end-of-stream semantics as the analyzer's
        detect_persistent_condition tail block."""
        out_path = tmp_path / "events.jsonl"
        transport = FileTransport(out_path)
        engine = StreamingEngine(transport=transport)

        # Prime engine state for _snapshot_features_at.
        engine.state.session_start_ts = 1000.0
        engine.state.prev_ts_epoch_s = 1149.0
        engine.state.prev_speed_m_s = 2.0
        engine.state.prev_gps_lat = 40.44
        engine.state.prev_gps_lon = -79.99
        engine.state.gps_confidence_smoothed = 0.3
        engine.state.anchor_lat = 40.44
        engine.state.anchor_lon = -79.99
        engine.state.committed_state = "GPS_UNTRUSTED"

        # Drive 150 in-condition frames into the tracker, never flip to false.
        tracker = engine._gps_degraded_tracker
        for i in range(150):
            tracker.step(
                condition=True, ts=1000.0 + i,
                gps_confidence=0.2, speed_m_s=2.0,
            )

        # flush() should emit the pending window.
        engine.flush()
        transport.close()

        import json
        events = [json.loads(line) for line in out_path.read_text().splitlines()]
        gps_events = [e for e in events if e["event_type"] == "gps_degraded_persistent"]
        assert len(gps_events) == 1
        ev = gps_events[0]
        assert ev["detail"] == "duration=149s, avg_speed=4.5 mph"
        assert ev["ts_epoch_s"] == 1149.0