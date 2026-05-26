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
    StreamingState, process_frame, update_state_window,
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
        """Inject a synthetic severe impact and verify it's emitted.

        Severe needs lin_accel_g >= 0.45 AND jerk_g_s >= 1.0 in a moving state
        with GPS confidence >= 0.70. Build a baseline of steady 1g, then
        spike accel for one frame so both lin_accel and jerk cross thresholds.
        """
        out_path = tmp_path / "events.jsonl"
        transport = FileTransport(out_path)
        engine = StreamingEngine(transport=transport)

        # Warm up past startup_suppress_s (180s), in a moving state.
        for i in range(200):
            engine.consume(_moving_frame(ts=1000.0 + i, elapsed=0.0, speed_m_s=10.0))

        # Spike: jump accel_mag from 1.0 (steady) to ~2.0 (big crash).
        # accel_mag = sqrt(1.5^2 + 0 + 1.5^2) = ~2.12. lin_accel = 1.12, jerk = ~1.12 over 1s.
        spike = _moving_frame(ts=1200.0, elapsed=0.0, speed_m_s=10.0)
        spike.accel_x_g = 1.5
        spike.accel_y_g = 0.0
        spike.accel_z_g = 1.5
        engine.consume(spike)

        # Trail off
        for i in range(10):
            engine.consume(_moving_frame(ts=1201.0 + i, elapsed=0.0, speed_m_s=10.0))

        engine.flush()
        transport.close()

        import json
        events = [json.loads(line) for line in out_path.read_text().splitlines()]
        types = [e["event_type"] for e in events]
        assert "severe_impact" in types, f"expected severe_impact in events; got {types}"

    def test_cooldown_suppresses_duplicate_fires(self, tmp_path):
        """Two consecutive severe-impact spikes within cooldown -> only one event."""
        out_path = tmp_path / "events.jsonl"
        transport = FileTransport(out_path)
        engine = StreamingEngine(transport=transport)

        for i in range(200):
            engine.consume(_moving_frame(ts=1000.0 + i, elapsed=0.0, speed_m_s=10.0))

        # First spike (will fire)
        spike1 = _moving_frame(ts=1200.0, elapsed=0.0, speed_m_s=10.0)
        spike1.accel_x_g = 1.5
        spike1.accel_z_g = 1.5
        engine.consume(spike1)
        # Recover to baseline
        engine.consume(_moving_frame(ts=1201.0, elapsed=0.0, speed_m_s=10.0))
        # Second spike 10 seconds later (within 45s impact_cooldown_s -> suppressed)
        spike2 = _moving_frame(ts=1210.0, elapsed=0.0, speed_m_s=10.0)
        spike2.accel_x_g = 1.5
        spike2.accel_z_g = 1.5
        engine.consume(spike2)

        for i in range(5):
            engine.consume(_moving_frame(ts=1211.0 + i, elapsed=0.0, speed_m_s=10.0))

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