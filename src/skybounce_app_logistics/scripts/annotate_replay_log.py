"""
annotate_replay_log.py

Prepend a human-readable event timeline to a raw engine log so the
archived bench-session file reads as a summary + transcript.

A `--transport ipc` replay captures only the IPC-level transcript
(handshake, TELEMETRY_ACK dispositions, PING/PONG, etc.) — not the
per-event detail (event_type, priority, score, elapsed_s) that the
engine emits. The detail is in the engine's `Event` objects, which the
sister `--transport file` replay of the same CSV writes as JSONL.

This script merges the two: it reads the JSONL, builds a sorted
timeline table, and prepends it as a header on top of the verbatim log.
The raw log body is preserved unchanged.

The annotation is idempotent: re-running on an already-annotated log
detects the marker line and replaces the previous header, so the script
can be re-run safely if the JSONL is updated.

Usage:
    python -m skybounce_app_logistics.scripts.annotate_replay_log \\
        EVENTS.jsonl LOG_FILE

Example:
    python -m skybounce_app_logistics.scripts.annotate_replay_log \\
        ~/sensor_data/vehicle_behavior/events/replay_may26.jsonl \\
        /tmp/replay_session_2026-05-28_realtime.log

The LOG_FILE is rewritten in place. Make a copy first if the original
matters.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


# Marker line written into the prepended header. Used to detect a prior
# annotation when re-running so the script is idempotent.
RAW_LOG_MARKER = "RAW ENGINE LOG (stdout + stderr, captured verbatim during the run)"


def _build_timeline_header(jsonl_path: Path) -> str:
    """Read the JSONL events file, sort by elapsed_s, return the
    prepend-able header text including the table and the marker line."""
    events = []
    with jsonl_path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            events.append(json.loads(line))
    events.sort(key=lambda e: e["elapsed_s"])

    sep = "=" * 78
    lines = [
        sep,
        "EVENT TIMELINE (annotation)",
        sep,
        f"Source: {jsonl_path.name} — {len(events)} events emitted by the streaming engine.",
        "Times below are relative to the engine's session start (elapsed_s).",
        "Priority: LOG = local context / heartbeat; P1 = standard; P2 = safety-immediate.",
        "",
        f"{'t+':>9s}  {'prio':<4s}  {'event_type':<28s}  {'class':<18s}  detail",
        f"{'-'*9}  {'-'*4}  {'-'*28}  {'-'*18}  {'-'*40}",
    ]
    for e in events:
        s = int(e["elapsed_s"])
        mins, secs = divmod(s, 60)
        t = f"{mins:>3d}m{secs:02d}s"
        cls = e.get("event_class", "")
        lines.append(
            f"{t:>9s}  "
            f"{e.get('priority', '?'):<4s}  "
            f"{e.get('event_type', '?'):<28s}  "
            f"{cls:<18s}  "
            f"{e.get('detail', '')}"
        )
    lines.append("")
    lines.append(sep)
    lines.append(RAW_LOG_MARKER)
    lines.append(sep)
    lines.append("")
    return "\n".join(lines) + "\n"


def _strip_prior_annotation(body: str) -> str:
    """If the log has a prior annotation, strip everything through the
    end of the marker block; return the raw-log body alone. If no
    prior annotation, return the body unchanged."""
    if RAW_LOG_MARKER not in body:
        return body
    idx = body.find(RAW_LOG_MARKER)
    # Skip past the marker line + the trailing '=' separator + one blank line.
    # Structure expected:
    #   <marker line>\n
    #   <'='*78>\n
    #   \n           (blank)
    #   <raw log starts here>
    parts = body[idx:].split("\n", 4)
    if len(parts) >= 4:
        return "\n".join(parts[3:]).lstrip("\n")
    return body  # malformed; leave as-is


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    jsonl_path = Path(sys.argv[1])
    log_path = Path(sys.argv[2])

    if not jsonl_path.exists():
        print(f"jsonl input not found: {jsonl_path}", file=sys.stderr)
        return 1
    if not log_path.exists():
        print(f"log file not found: {log_path}", file=sys.stderr)
        return 1

    header = _build_timeline_header(jsonl_path)
    body = log_path.read_text(encoding="utf-8")
    body = _strip_prior_annotation(body)

    log_path.write_text(header + body, encoding="utf-8")
    print(f"annotated {log_path}")
    print(f"  prepended header from {jsonl_path}")
    print(f"  total file size: {log_path.stat().st_size} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
