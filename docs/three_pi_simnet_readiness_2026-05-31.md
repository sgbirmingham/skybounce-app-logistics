# Three-Pi Simnet — Readiness State (end of 2026-05-31)

Companion to [three_pi_simnet_setup_prompt.md](three_pi_simnet_setup_prompt.md).
That document is the bring-up prompt for the next session; this document is the
state-of-the-world snapshot when that session begins.

## Sensor lineup

| Role | Machine | IP | User | Arch | Deploy path |
|---|---|---|---|---|---|
| **Sensor 1** | Pi 5 | 192.168.40.178 | `sgbirm` | aarch64 | `/home/sgbirm/skybounce-app/` |
| **Sensor 2** | Pi 0 2WH | 192.168.40.20 | `sgbir` | armv7l | `/home/sgbir/skybounce/` |
| **Sensor 3** | Pi 500 | 192.168.40.145 | `sgbir` | aarch64 | `/home/sgbir/skybounce-app/` |
| Basestation | (separate machine) | TBD | n/a | n/a | radio team's setup |

Note the path / user inconsistencies — they reflect historical setup:
- Sensor 1 was newly provisioned 2026-05-29 with the `sgbirm` user
- Sensor 2 was deployed 2026-05-27 under the original `sgbir/skybounce/` layout
- Sensor 3 (Pi 500) was set up 2026-05-31 alongside an existing radio-team repo
  at `/home/sgbir/skybounce/`, so our stack landed at `/home/sgbir/skybounce-app/`

## Per-Pi capability matrix

| Capability | Sensor 1 (Pi 5) | Sensor 2 (Pi 0 2WH) | Sensor 3 (Pi 500) |
|---|---|---|---|
| `replay.py --rate` flag | ✅ | ✅ | ✅ |
| `replay.py --auto-events` flag | only if PR #7 merged + redeployed | only if PR #7 merged + redeployed | only if PR #7 merged + redeployed |
| `gen_synthetic_drive_csv.py` (on box) | ⚠️ stale copy from earlier deploy | ❌ not present | ❌ not present |
| `skybounce-ipc-python` (SB45_SIM_V2) | ✅ | ✅ | ✅ |
| `skybounce-event-rules` (0.2.0) | ✅ | ✅ | ✅ |
| Python venv | ✅ | ✅ | ✅ |
| All 6 synthetic CSVs | ✅ | ✅ | ✅ |
| `socat` for simnet approach A | ❌ | ❌ | ❌ |
| Engine smoke test passed on 2026-05-31 | ✅ (1.22 s for 12h CSV) | ✅ (0.58 s for 5-min) | ✅ (0.08 s for 5-min) |

**Note on `--auto-events`**: this flag is only in `feat/synthetic-drive-csv-gen`
(PR #7) and lives in `gen_synthetic_drive_csv.py`, not in `replay.py`. The Pis
don't need it for replay — they only need it if you want to regenerate synthetic
CSVs locally on a Pi. CSV regeneration is currently done on Windows.

## Synthetic CSV catalog (identical on all 3 Pis)

Path: `~/sensor_data/vehicle_behavior/drive_csvs/` (Pi 0 2WH and Pi 500 use
`sgbir/`, Pi 5 uses `sgbirm/`).

| File | Duration | Events | Purpose |
|---|---|---|---|
| `synthetic_short_sensor1.csv` | 300 s | 3 (brake/impact/accel) | Setup-works smoke test |
| `synthetic_short_sensor2.csv` | 300 s | 3 (per-sensor seed jitter) | Setup-works smoke test |
| `synthetic_short_sensor3.csv` | 300 s | 3 (per-sensor seed jitter) | Setup-works smoke test |
| `synthetic_long_sensor1.csv` | 12 h | 48 starting at t=180 s, every 15 min | Endurance / sustained-load |
| `synthetic_long_sensor2.csv` | 12 h | 48 starting at t=480 s, every 15 min | Endurance / sustained-load |
| `synthetic_long_sensor3.csv` | 12 h | 48 starting at t=780 s, every 15 min | Endurance / sustained-load |

**Phase offset design**: across the 3-Pi fleet, the long CSVs are timed so that
one transmittable event fires every ~5 min from the basestation's view (each
Pi contributes one event every 15 min, staggered 300 s apart). This avoids
collisions at the basestation while still exercising sustained operation.

Verified event counts (engine `--transport file` against `synthetic_long_sensor1`,
2026-05-31 on Pi 5):

```
52 events total
  16 hard_brake          (SAFETY_IMMEDIATE)
  16 hard_accel          (SAFETY_IMMEDIATE)
   8 severe_impact       (SAFETY_IMMEDIATE)  ← jerk just above 1.0 g/s
   8 moderate_impact     (ROAD_EVENT)        ← jerk just below 1.0 g/s
   2 startup_gps_*       (LOCAL_CONTEXT)
   2 trip/state          (TRIP_SUMMARY / LOCAL_CONTEXT)
```

The 8/8 severe/moderate split happens because the synthetic spike is right at
the rules library's jerk threshold (1.0 g/s) and IMU noise pushes some impacts
above, some below. Useful for exercising both classifications.

## Launch reference

### Sensor 1 — Pi 5 (`sgbirm@192.168.40.178`)

```bash
ssh sgbirm@192.168.40.178 "cd /home/sgbirm/skybounce-app/skybounce-app-logistics && \
    PYTHONPATH=/home/sgbirm/skybounce-app/skybounce-ipc-python \
    /home/sgbirm/skybounce-app/venv/bin/python \
    -m skybounce_app_logistics.scripts.replay \
    --input /home/sgbirm/sensor_data/vehicle_behavior/drive_csvs/synthetic_long_sensor1.csv \
    --transport ipc --socket /tmp/skybounce-sensor.sock --rate 1.0"
```

### Sensor 2 — Pi 0 2WH (`sgbir@192.168.40.20`)

```bash
ssh sgbir@192.168.40.20 "cd /home/sgbir/skybounce/skybounce-app-logistics && \
    PYTHONPATH=/home/sgbir/skybounce/skybounce-ipc-python \
    /home/sgbir/skybounce/venv/bin/python \
    -m skybounce_app_logistics.scripts.replay \
    --input /home/sgbir/sensor_data/vehicle_behavior/drive_csvs/synthetic_long_sensor2.csv \
    --transport ipc --socket /tmp/skybounce-sensor.sock --rate 1.0"
```

### Sensor 3 — Pi 500 (`sgbir@192.168.40.145`)

```bash
ssh sgbir@192.168.40.145 "cd /home/sgbir/skybounce-app/skybounce-app-logistics && \
    PYTHONPATH=/home/sgbir/skybounce-app/skybounce-ipc-python \
    /home/sgbir/skybounce-app/venv/bin/python \
    -m skybounce_app_logistics.scripts.replay \
    --input /home/sgbir/sensor_data/vehicle_behavior/drive_csvs/synthetic_long_sensor3.csv \
    --transport ipc --socket /tmp/skybounce-sensor.sock --rate 1.0"
```

### Common knobs

| Swap | What changes |
|---|---|
| `--rate 1.0` → `--rate 60.0` | 60× compressed (12 h → 12 min playback) |
| `--rate 1.0` → omit `--rate` | Burst all 48 events instantly (stress test) |
| `synthetic_long_sensorN.csv` → `synthetic_short_sensorN.csv` | 5-min smoke test |
| `--transport ipc --socket .../sensor.sock` → `--transport file --out events.jsonl` | Write JSONL instead of submitting via IPC |

## What the radio team owns

The user confirmed 2026-05-31: the radio team's receive side is ready. From our
side we just need each Pi to emit packets in whatever shape they expect. We did
NOT have to design architecture A/B/C (socat / TCP backend / app-level forwarder)
— that decision is settled on the receive side.

What we don't yet know (questions for the next session):
- Where exactly does each Pi's IPC stream go? `s.py` running locally on each Pi,
  or a custom forwarder, or something else?
- Are there per-Pi endpoint_id requirements? (Default is `0x53415050` = "SAPP".)

Both questions are answerable in <1 min when the radio team is available.

## Open follow-up items

1. **Commit `--rate` to main** (tracked in TODO list as task #69). The flag exists
   on all 3 Pis but is NOT on `main` on GitHub. The Windows local working copy
   is missing it. Risk: a fresh clone gives a `replay.py` without `--rate`.
   Discovered today when the Pi 500 deploy from the Windows local copy produced
   a `replay.py` without `--rate` — fix was to rsync from the work Pi instead.
2. **Install `socat`** if/when the radio team ever wants Approach A. One-line apt
   install on each Pi. Currently not needed.
3. **Pi 500's `skybounce-app/` dir has unrelated user folders** alongside our
   3 repos (coldchain_analysis, drone_collision_analysis, geophone_analysis,
   pi_sensor_stack, vehicle_behavior_analysis). They're the user's existing
   analysis work; not in our way, but worth a heads-up if reorganizing.

## Companion PRs at end of session

- **PR #6** `docs(simnet): three-Pi simulated-sensor network bring-up prompt`
- **PR #7** `feat(scripts): synthetic drive CSV generator for simnet testing`
  (now with `--auto-events` for sustained-load schedules)
