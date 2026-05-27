"""
live.py

Live streaming-engine driver for the Pi.

Tails an actively-written logger CSV and emits events as they're detected,
in real time. The companion to replay.py:

    replay.py: closed CSV in -> complete JSONL out (offline validation)
    live.py:   active CSV in -> JSONL appended as events fire (Pi operation)

Two ways to point at the logger CSV:

    --input <path>     Explicit path. Use this if you know the filename.
    --watch <dir>      Auto-discover the latest matching CSV in <dir>.
                       Useful because the logger generates timestamped names
                       you don't know in advance.

Examples:

    # Logger already running, you know the filename:
    python -m skybounce_app_logistics.scripts.live \
        --input ~/coldchain_poc/data/simple_logger/raw/vehicle_behavior_simple_logger_v0_1_2026-05-25_14-22-10.csv \
        --out ~/coldchain_poc/data/events/live.jsonl

    # Auto-discover the newest logger CSV:
    python -m skybounce_app_logistics.scripts.live \
        --watch ~/coldchain_poc/data/simple_logger/raw \
        --out ~/coldchain_poc/data/events/live.jsonl

The script runs until interrupted (Ctrl+C) or the logger stops appending
rows and --stop-on-idle is set.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from pathlib import Path

from skybounce_event_rules import AnalyzerConfig, RULES_VERSION

from ..csv_source import tail_frames
from ..engine import StreamingEngine
from ..transport import FileTransport, IpcTransport


log = logging.getLogger("skybounce.app.logistics.live")


# v0.1 placeholder endpoint_id. "SAPP" in ASCII; matches the IPC repo's
# sensor_app_stub default so bench validation sees a consistent peer id.
# Real deployments should set a meaningful per-device value.
DEFAULT_ENDPOINT_ID = 0x53415050


# -----------------------------------------------------------------------------
# CSV discovery
# -----------------------------------------------------------------------------

LOGGER_FILENAME_GLOB = "vehicle_behavior_simple_logger_v*_*.csv"


def find_latest_logger_csv(watch_dir: Path, max_wait_s: float = 60.0) -> Path:
    """Return the most recently modified logger CSV in watch_dir.

    Waits up to max_wait_s for a matching CSV to appear if the directory is
    empty, then raises FileNotFoundError. This handles the case where the
    streaming engine starts slightly before the logger.
    """
    start = time.monotonic()
    while True:
        candidates = sorted(
            watch_dir.glob(LOGGER_FILENAME_GLOB),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            latest = candidates[0]
            log.info("found logger CSV: %s", latest)
            return latest
        if time.monotonic() - start > max_wait_s:
            raise FileNotFoundError(
                f"no logger CSV matching {LOGGER_FILENAME_GLOB} in {watch_dir} "
                f"within {max_wait_s:.0f}s"
            )
        log.info("waiting for logger CSV in %s ...", watch_dir)
        time.sleep(2.0)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Tail an active logger CSV and stream events in real time.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--input", type=Path,
                       help="Explicit path to the logger CSV to tail.")
    group.add_argument("--watch", type=Path,
                       help="Directory containing logger CSVs. The newest is used.")

    parser.add_argument("--out", type=Path,
                       help="Where to append the JSON-lines event log. "
                            "Required for --transport file; ignored for "
                            "--transport ipc.")
    parser.add_argument("--transport", choices=("file", "ipc"), default="file",
                       help="Where to emit events. 'file' appends JSONL to --out; "
                            "'ipc' submits SB45 via the SkyBounce IPC daemon.")
    parser.add_argument("--socket", type=str, default=None,
                       help="Unix socket path for --transport ipc. Overrides "
                            "$SKYBOUNCE_SENSOR_SOCK; library default if unset.")
    parser.add_argument("--endpoint-id", type=lambda x: int(x, 0),
                       default=DEFAULT_ENDPOINT_ID,
                       help="32-bit endpoint identifier sent in HELLO "
                            "(--transport ipc only). Decimal, 0x hex, or 0o octal.")
    parser.add_argument("--stop-on-idle", action="store_true",
                       help="Exit if no new rows appear for ~5 seconds. "
                            "Useful for ad-hoc tests; do NOT use during driving.")
    parser.add_argument("--log-level", default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    # Resolve input path.
    if args.input is not None:
        csv_path = args.input
        log.info("tailing explicit path: %s", csv_path)
    else:
        watch_dir = args.watch
        if not watch_dir.is_dir():
            log.error("watch directory does not exist: %s", watch_dir)
            sys.exit(2)
        csv_path = find_latest_logger_csv(watch_dir)

    # Select transport.
    if args.transport == "file":
        if args.out is None:
            parser.error("--out is required with --transport file")
        transport = FileTransport(args.out, mode="a")  # append: multiple
                                                       # invocations preserve history
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
            parser.error(f"--transport ipc requires skybounce-ipc-python: {e}")
            return  # unreachable; parser.error exits
        out_descr = f"IPC endpoint_id=0x{args.endpoint_id:08X}"

    print(f"Rules library: {RULES_VERSION}")
    print(f"Tailing:       {csv_path}")
    print(f"Transport:     {args.transport}")
    print(f"Output:        {out_descr}")
    if args.stop_on_idle:
        print("Will exit when no new rows appear for ~5 seconds.")
    else:
        print("Running until interrupted (Ctrl+C).")

    # Set up clean Ctrl+C handling so we flush the engine on shutdown.
    stop_requested = {"value": False}

    def _on_sigint(signum, frame):
        log.info("interrupt received; finishing up...")
        stop_requested["value"] = True
        # Re-raise on second Ctrl+C to allow hard-exit if cleanup hangs.
        signal.signal(signal.SIGINT, signal.SIG_DFL)

    signal.signal(signal.SIGINT, _on_sigint)

    cfg = AnalyzerConfig()
    engine = StreamingEngine(transport=transport, cfg=cfg)

    last_status_print = time.monotonic()
    rows_since_status = 0

    try:
        for frame in tail_frames(csv_path, stop_on_eof=args.stop_on_idle):
            if stop_requested["value"]:
                break
            engine.consume(frame)
            rows_since_status += 1

            # Light heartbeat: every 60s of wall time, print row count and
            # event total. Quiet enough to ignore, loud enough to know it's
            # alive during a long drive.
            now = time.monotonic()
            if now - last_status_print >= 60.0:
                log.info(
                    "live: processed %d rows in last %.0fs, total events emitted: %d",
                    rows_since_status, now - last_status_print, transport.event_count,
                )
                rows_since_status = 0
                last_status_print = now
    finally:
        engine.flush()
        transport.close()
        print(f"Events emitted: {transport.event_count}")


if __name__ == "__main__":
    main()
