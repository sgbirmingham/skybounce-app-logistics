"""
replay.py

Replay a stored logger CSV through the streaming engine.

This is the validation tool: run it against a CSV that the PC analyzer
has already processed, then compare the streaming engine's event list
to the analyzer's events_v3_0.csv. They should match event-for-event
on event_type, priority, and score (modulo the known v0.1 omissions:
no clustering, no context windows, no packet budget).

Usage:
    sb-logistics-replay --input drive.csv --out events.jsonl

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
from ..transport import FileTransport


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay a logger CSV through the streaming engine.")
    parser.add_argument("--input", required=True, type=Path,
                        help="Path to a vehicle_behavior_simple_logger CSV.")
    parser.add_argument("--out", required=True, type=Path,
                        help="Where to write the JSON-lines event log.")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    print(f"Rules library: {RULES_VERSION}")
    print(f"Input:  {args.input}")
    print(f"Output: {args.out}")

    cfg = AnalyzerConfig()
    transport = FileTransport(args.out, mode="w")
    try:
        engine = run_engine(batch_frames(args.input), transport=transport, cfg=cfg)
    finally:
        transport.close()

    print(f"Events emitted: {transport.event_count}")


if __name__ == "__main__":
    main()
