"""
compare_to_analyzer.py

Compare streaming engine output (JSON lines) against PC analyzer events CSV.

Reports:
- Matched events (same type at same timestamp +/- 2s)
- Missed events (in analyzer but not in stream)
- Extra events (in stream but not in analyzer)
- Diffs (same timestamp, different event type -- usually ordering)

Known v0.1 deltas (not failures):
- gps_degraded_persistent: deferred to v0.2 (sustained-condition tracking)
- moderate_impact events: streaming keeps all; analyzer drops single-event
  clusters via cluster_moderate_impacts. Real moderate impacts that occur in
  bursts WILL match.

Usage:
    python compare_to_analyzer.py STREAM.jsonl ANALYZER_EVENTS.csv
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd


KNOWN_DEFERRED_TYPES = {"gps_degraded_persistent", "gps_loss_while_moving"}


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} STREAM.jsonl ANALYZER_EVENTS.csv")
        sys.exit(2)

    stream_path = Path(sys.argv[1])
    analyzer_path = Path(sys.argv[2])

    with open(stream_path) as f:
        stream = [json.loads(line) for line in f if line.strip()]
    analyzer = pd.read_csv(analyzer_path)

    ba = sorted(analyzer.to_dict(orient="records"), key=lambda r: r["ts_epoch_s"])
    sa = sorted(stream, key=lambda e: e["ts_epoch_s"])

    matches = []
    misses = []
    extras = list(sa)
    same_ts_diff_type = []

    # For each analyzer event, look for a streaming event of the same type
    # within TS_TOLERANCE_S. If found, mark as matched; else mark as miss.
    TS_TOLERANCE_S = 2.0
    for a in ba:
        best_j = None
        best_dt = float("inf")
        for j, s in enumerate(extras):
            if s["event_type"] != a["event_type"]:
                continue
            dt = abs(s["ts_epoch_s"] - a["ts_epoch_s"])
            if dt <= TS_TOLERANCE_S and dt < best_dt:
                best_dt = dt
                best_j = j
        if best_j is not None:
            matches.append((a, extras.pop(best_j)))
        else:
            # Type mismatch within tolerance? Note it but still call it a miss.
            close_other = [s for s in extras if abs(s["ts_epoch_s"] - a["ts_epoch_s"]) <= TS_TOLERANCE_S]
            if close_other:
                same_ts_diff_type.append((a, close_other[0]))
            misses.append(a)

    misses_expected = [e for e in misses if e["event_type"] in KNOWN_DEFERRED_TYPES]
    misses_unexpected = [e for e in misses if e["event_type"] not in KNOWN_DEFERRED_TYPES]

    extras_known = [e for e in extras if e["event_type"] == "moderate_impact"]
    extras_unexpected = [e for e in extras if e["event_type"] != "moderate_impact"]

    print(f"Analyzer events:  {len(ba)}")
    print(f"Streaming events: {len(sa)}")
    print(f"Matched:          {len(matches)}")
    print()
    print(f"Same-timestamp ordering diffs:  {len(same_ts_diff_type)}")
    print(f"Expected misses (deferred):     {len(misses_expected)}  {[e['event_type'] for e in misses_expected]}")
    print(f"Unexpected misses:              {len(misses_unexpected)}")
    if misses_unexpected:
        for m in misses_unexpected:
            print(f"    MISS: {m['event_type']:<24} elapsed={m['elapsed_s']:.0f}s")
    print(f"Known extras (uncluster'd moderate_impact): {len(extras_known)}")
    print(f"Unexpected extras: {len(extras_unexpected)}")
    if extras_unexpected:
        for e in extras_unexpected:
            print(f"    EXTRA: {e['event_type']:<24} elapsed={e['elapsed_s']:.0f}s")

    print()
    if not misses_unexpected and not extras_unexpected:
        print("OK: all differences are within known v0.1 deltas.")
        sys.exit(0)
    else:
        print("FAIL: unexpected differences detected.")
        sys.exit(1)


if __name__ == "__main__":
    main()
