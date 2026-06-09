"""
replay.py

Replay a stored logger CSV through the streaming engine.

This is the validation tool: run it against a CSV that the PC analyzer
has already processed, then compare the streaming engine's event list
to the analyzer's events_v3_0.csv. They should match event-for-event
on event_type, priority, and score (modulo the known v0.1 omissions:
no clustering, no context windows, no packet budget).

Two transport modes:

    --transport file (default)
        Emit events as JSON lines to --out. The validation-friendly
        path; no IPC dependency.

    --transport ipc
        Encode each event as SB45 and submit via IpcTransport. Requires
        a running SkyBounce daemon on the configured Unix socket and
        skybounce-ipc-python on PYTHONPATH. --out is ignored.

Usage:
    sb-logistics-replay --input drive.csv --out events.jsonl
    sb-logistics-replay --input drive.csv --transport ipc

Or as a module:
    python -m skybounce_app_logistics.scripts.replay \
        --input drive.csv --out events.jsonl
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from pathlib import Path
from typing import Iterator

from skybounce_event_rules import AnalyzerConfig, RULES_VERSION

from ..csv_source import SensorFrame, batch_frames
from ..engine import run_engine
from ..transport import FileTransport, IpcTransport


def _pace_frames(frames: Iterator[SensorFrame], rate: float) -> Iterator[SensorFrame]:
    """Yield frames at (rate × real-time) of their `ts_epoch_s` cadence.

    rate=1.0 means real-time (1 s of CSV time = 1 s of wall time).
    rate=10.0 means 10× faster (1 s of CSV = 100 ms of wall).
    The first frame is yielded immediately to anchor both clocks; subsequent
    frames wait until ``(ts_epoch_s - first.ts_epoch_s) / rate`` seconds have
    elapsed on the wall clock since that first yield.

    Frames whose ts_epoch_s does not advance (logger pause, clock jitter,
    duplicate rows) yield immediately rather than blocking for negative time.

    Raises ValueError if rate is non-positive.
    """
    if rate <= 0:
        raise ValueError(f"rate must be positive, got {rate}")
    start_csv: float | None = None
    start_wall = time.monotonic()
    for f in frames:
        if start_csv is None:
            start_csv = f.ts_epoch_s
        else:
            elapsed_csv = f.ts_epoch_s - start_csv
            target_wall = start_wall + elapsed_csv / rate
            delay = target_wall - time.monotonic()
            if delay > 0:
                time.sleep(delay)
        yield f


# v0.1 placeholder endpoint_id. "SAPP" in ASCII; matches the IPC repo's
# sensor_app_stub default so bench validation sees a consistent peer id.
# Real deployments should set a meaningful per-device value.
DEFAULT_ENDPOINT_ID = 0x53415050


def _default_endpoint_id() -> int:
    """Per-device endpoint id: $SKYBOUNCE_ENDPOINT_ID (decimal / 0x / 0o) if set,
    else the shared placeholder. Each Pi in a fleet MUST set a unique value
    (e.g. in its shell profile) so the basestation can route/dispatch per
    vehicle -- a shared id misroutes ACKs and starves nodes under load."""
    v = os.environ.get("SKYBOUNCE_ENDPOINT_ID")
    return int(v, 0) if v else DEFAULT_ENDPOINT_ID


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay a logger CSV through the streaming engine.")
    parser.add_argument("--input", required=True, type=Path,
                        help="Path to a vehicle_behavior_simple_logger CSV.")
    parser.add_argument("--out", type=Path,
                        help="Where to write the JSON-lines event log. "
                             "Required for --transport file; ignored for "
                             "--transport ipc.")
    parser.add_argument("--transport", choices=("file", "ipc"), default="file",
                        help="Where to emit events. 'file' writes JSONL; "
                             "'ipc' submits SB45 via the SkyBounce IPC daemon.")
    parser.add_argument("--socket", type=str, default=None,
                        help="Unix socket path for --transport ipc. Overrides "
                             "$SKYBOUNCE_SENSOR_SOCK; library default if unset.")
    parser.add_argument("--endpoint-id", type=lambda x: int(x, 0),
                        default=_default_endpoint_id(),
                        help="32-bit endpoint identifier sent in HELLO "
                             "(--transport ipc only). Defaults to "
                             "$SKYBOUNCE_ENDPOINT_ID if set, else the shared "
                             "placeholder. Decimal, 0x hex, or 0o octal.")
    parser.add_argument("--rate", type=float, default=None, metavar="N",
                        help="Pace yields at N x real-time of the row timestamps "
                             "(1.0 = real-time, 10.0 = 10x faster). Omit for "
                             "burst behavior (read the CSV as fast as DictReader, "
                             "the default). Useful for sustained-load bench tests "
                             "and ACK-timing observation under realistic cadence.")
    parser.add_argument("--batch-p1", action="store_true",
                        help="Wrap the transport in a P1BatchingTransport: P1 "
                             "events are accumulated over 30-min wall-clock-"
                             "aligned windows and emitted as a single "
                             "SB45_SIM_V4 per-window summary frame (per-type "
                             "count buckets + coarse position). P2 events pass "
                             "through unchanged (and are recorded in the "
                             "summary's p2_present). Eases pressure on the "
                             "radio's intake queue under high-density P1 "
                             "traffic. Default off.")
    parser.add_argument("--batch-window-s", type=float, default=1800.0,
                        metavar="SEC",
                        help="Window size in elapsed seconds for --batch-p1. "
                             "Default 1800 (30 min, SB45_SIM_V4 -- sized by the "
                             "2026-06-07 capacity sim). Ignored without "
                             "--batch-p1.")
    # Retry-on-0x80 was removed 2026-06-07 (radio team: retry adds churn
    # without goodput at saturation). The proactive controls are V4 per-window
    # batching (--batch-p1) and the submission rate cap below.
    parser.add_argument("--impact-cooldown-s", type=float, default=None,
                        metavar="SEC",
                        help="Override AnalyzerConfig.impact_cooldown_s (default "
                             "45). Lower it (e.g. 2) to let a tight burst of "
                             "severe_impacts fire on one node for stress tests. "
                             "Test-only knob.")
    parser.add_argument("--severe-impact-g", type=float, default=None,
                        metavar="G",
                        help="Override AnalyzerConfig.severe_impact_g (default "
                             "0.45). TEST-ONLY: lower it (e.g. 0.45) to let a real "
                             "drive's road bumps register as P2 for exercising the "
                             "safety path -- do NOT lower it in production (real "
                             "driving tops out ~1g; a crash is 10g+).")
    parser.add_argument("--severe-jerk-g-s", type=float, default=None,
                        metavar="GS",
                        help="Override AnalyzerConfig.severe_jerk_g_s (default 1.0). "
                             "Test-only companion to --severe-impact-g.")
    parser.add_argument("--drain-timeout-s", type=float, default=0.0,
                        metavar="SEC",
                        help="At close, wait this long for every submitted frame "
                             "to reach a terminal disposition before tearing down "
                             "the socket. 0 (default) = UNLIMITED -- drain until "
                             "the radio has delivered the whole backlog (Ctrl-C to "
                             "stop). Set >0 to bound it (the old 5s behaviour: 5).")
    parser.add_argument("--max-submit-rate-per-min", type=float, default=0.0,
                        metavar="N",
                        help="--transport ipc only. Cap submissions to N per "
                             "minute (sliding 60s window). Helps prevent "
                             "BUFFER_FULL in the first place. 0 = unlimited. "
                             "Suggested starting point: 30 (one submit per "
                             "2 sec). Honors radio team's app-side "
                             "rate-limiting request. Default: 0.")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    if args.rate is not None and args.rate <= 0:
        parser.error(f"--rate must be positive (got {args.rate})")

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    log = logging.getLogger("skybounce.app.logistics.replay")

    # Select transport.
    if args.transport == "file":
        if args.out is None:
            parser.error("--out is required with --transport file")
        transport = FileTransport(args.out, mode="w")
        out_descr = str(args.out)
    else:
        if args.out is not None:
            log.warning("--out %s ignored when --transport ipc", args.out)
        try:
            transport = IpcTransport(
                endpoint_id=args.endpoint_id,
                socket_path=args.socket,
                max_submit_rate_per_min=args.max_submit_rate_per_min,
            )
        except ImportError as e:
            parser.error(f"--transport ipc requires skybounce-ipc-python: {e}")
            return  # unreachable; parser.error exits
        out_descr = f"IPC endpoint_id=0x{args.endpoint_id:08X}"

    # Optionally wrap with the P1 batcher. P2 events still flow through to the
    # underlying transport; P1 events get accumulated into one SB45_SIM_V4
    # per-window (30-min) summary frame. (LOG events are dropped at the IPC
    # wire, so over IPC they never go OTA regardless of batching.)
    if args.batch_p1:
        from skybounce_app_logistics.p1_batcher import P1BatchingTransport
        transport = P1BatchingTransport(transport, window_s=args.batch_window_s)

    print(f"Rules library: {RULES_VERSION}")
    print(f"Input:     {args.input}")
    print(f"Transport: {args.transport}")
    print(f"Output:    {out_descr}")
    if args.batch_p1:
        print(f"P1 batch:  on ({args.batch_window_s:.0f}s windows)")
    else:
        print(f"P1 batch:  off (per-event)")
    if args.rate is not None:
        print(f"Rate:      {args.rate} x real-time (paced)")
    else:
        print(f"Rate:      burst (as-fast-as-possible)")

    _cfg_overrides = {k: v for k, v in {
        "impact_cooldown_s": args.impact_cooldown_s,
        "severe_impact_g": args.severe_impact_g,
        "severe_jerk_g_s": args.severe_jerk_g_s,
    }.items() if v is not None}
    cfg = AnalyzerConfig(**_cfg_overrides)
    frames = batch_frames(args.input)
    if args.rate is not None:
        frames = _pace_frames(frames, args.rate)
    try:
        run_engine(frames, transport=transport, cfg=cfg)
    finally:
        transport.close(drain_timeout_s=args.drain_timeout_s)

    # event_count is exposed by FileTransport but not by every Transport (IPC,
    # batched wrappers). hasattr keeps this summary line useful when present
    # and silently skips it when not.
    if hasattr(transport, "event_count"):
        print(f"Records emitted: {transport.event_count}")
    if hasattr(transport, "summaries_emitted"):
        print(f"  summary frames: {transport.summaries_emitted} "
              f"(absorbing {transport.p1_events_seen} P1 events)")


if __name__ == "__main__":
    main()
