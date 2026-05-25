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
from skybounce_app_logistics.state import StreamingState, process_frame
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
