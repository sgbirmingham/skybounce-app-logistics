"""
test_replay_pacing.py

Tests for the `--rate` pacing logic in `replay.py` (the `_pace_frames`
generator). Covers the timing-correct path and the input-validation guard.
"""

from __future__ import annotations

import time

import pytest

from skybounce_app_logistics.csv_source import SensorFrame
from skybounce_app_logistics.scripts.replay import _pace_frames


def _frame(ts_epoch_s: float) -> SensorFrame:
    """Minimal SensorFrame at the given timestamp. Other fields don't
    matter for pacing tests — only ts_epoch_s is consulted."""
    return SensorFrame(
        ts_epoch_s=ts_epoch_s,
        elapsed_s=0.0,
        accel_x_g=0.0, accel_y_g=0.0, accel_z_g=1.0,
        gyro_x_dps=0.0, gyro_y_dps=0.0, gyro_z_dps=0.0,
        gps_valid=0, gps_mode=0, gps_sats=0,
        gps_lat=None, gps_lon=None, gps_speed_m_s=None,
        gps_time="",
    )


class TestPaceFrames:
    def test_introduces_proportional_delay(self):
        """0.5s of CSV time at rate=10 should take ~50 ms of wall time.

        Tolerance is generous (20-300 ms) to absorb OS scheduling jitter.
        The first frame yields immediately; the second waits 0.5/10 = 50 ms.
        """
        frames = [_frame(0.0), _frame(0.5)]
        start = time.monotonic()
        out = list(_pace_frames(iter(frames), rate=10.0))
        elapsed = time.monotonic() - start

        assert len(out) == 2
        assert 0.02 < elapsed < 0.30, (
            f"expected ~50 ms wall for 0.5s CSV @ rate=10, got {elapsed*1000:.1f} ms"
        )

    def test_first_frame_yields_immediately(self):
        """The first frame must not block — only subsequent frames pace."""
        # rate=0.01 means 1s of CSV = 100s of wall — extreme slow-down.
        # But with only one frame, the function should still return instantly.
        start = time.monotonic()
        out = list(_pace_frames(iter([_frame(1000.0)]), rate=0.01))
        elapsed = time.monotonic() - start

        assert len(out) == 1
        assert elapsed < 0.05, (
            f"single-frame path should not sleep; took {elapsed*1000:.1f} ms"
        )

    def test_non_advancing_timestamps_do_not_sleep(self):
        """Duplicate or backward timestamps yield without blocking."""
        # Three frames all at the same ts_epoch_s; with rate=0.001 (extremely
        # slow), naive elapsed_csv/rate would be 0 for all — should be a
        # no-op, total wall time near zero.
        frames = [_frame(100.0), _frame(100.0), _frame(99.0)]
        start = time.monotonic()
        out = list(_pace_frames(iter(frames), rate=0.001))
        elapsed = time.monotonic() - start

        assert len(out) == 3
        assert elapsed < 0.05, (
            f"flat/backward timestamps should not sleep; took {elapsed*1000:.1f} ms"
        )

    def test_rejects_zero_rate(self):
        with pytest.raises(ValueError, match="rate must be positive"):
            list(_pace_frames(iter([_frame(0.0)]), rate=0.0))

    def test_rejects_negative_rate(self):
        with pytest.raises(ValueError, match="rate must be positive"):
            list(_pace_frames(iter([_frame(0.0)]), rate=-1.0))
