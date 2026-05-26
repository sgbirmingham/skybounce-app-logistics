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
        skybounce_IPC_python on PYTHONPATH. --out is ignored.

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
from pathlib import Path

from skybounce_event_rules import AnalyzerConfig, RULES_VERSION

from ..csv_source import batch_frames
from ..engine import run_engine
from ..transport import FileTransport, IpcTransport


# v0.1 placeholder endpoint_id. "SAPP" in ASCII; matches the IPC repo's
# sensor_app_stub default so bench validation sees a consistent peer id.
# Real deployments should set a meaningful per-device value.
DEFAULT_ENDPOINT_ID = 0x53415050


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
                        default=DEFAULT_ENDPOINT_ID,
                        help="32-bit endpoint identifier sent in HELLO "
                             "(--transport ipc only). Decimal, 0x hex, or 0o octal.")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

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
            )
        except ImportError as e:
            parser.error(f"--transport ipc requires skybounce_IPC_python: {e}")
            return  # unreachable; parser.error exits
        out_descr = f"IPC endpoint_id=0x{args.endpoint_id:08X}"

    print(f"Rules library: {RULES_VERSION}")
    print(f"Input:     {args.input}")
    print(f"Transport: {args.transport}")
    print(f"Output:    {out_descr}")

    cfg = AnalyzerConfig()
    try:
        run_engine(batch_frames(args.input), transport=transport, cfg=cfg)
    finally:
        transport.close()

    print(f"Events emitted: {transport.event_count}")


if __name__ == "__main__":
    main()
