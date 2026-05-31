# Three-Pi Simulated Sensor Network — Bring-up Prompt

**Authored**: 2026-05-29, end-of-day after the Pi 5 + HackRF bench session.

This document is a self-contained prompt for a fresh Claude session that will help
set up a SkyBounce sensor network on the home LAN — three Pis playing back recorded
drive CSVs as simulated sensors, no HackRFs, with a fourth machine acting as the
basestation aggregator over WiFi instead of HF radio.

## Why this exists

By the end of 2026-05-29 we had:
- Pi 5 deployed with the full sensor stack (logger, engine, IPC, rules)
- HackRF OTA TX confirmed working with the radio team's daemon (`s.py`)
- First-ever `disposition=DELIVERED` observed end-to-end
- 18-event burst replays demonstrated through the IPC → s.py path

The natural next step is a **multi-sensor test** without depending on more HackRFs.
Three Pis running paced CSV replays simultaneously will exercise:
- Multi-connection IPC behavior on whatever serves as the network aggregator
- Concurrent telemetry merging on the basestation side
- Disposition reporting across multiple endpoint_ids
- Slot/timing assumptions when 3 sensors talk to 1 basestation

## Conventions you should know before reading the prompt

- **Pi access** — see `pi_access.md`. WSL Ubuntu beats PowerShell for driving Pis.
- **Quoting layers** — bash heredocs through wsl→ssh sometimes drop `$VAR` and `$(...)`.
  When that bites, scp a script file and execute it, or build literal scripts with
  `printf "%s\n" ...`.
- **CSV format** — use `vehicle_behavior_simple_logger_v0_1` schema CSVs. The
  engine is calibrated for that shape. The older `vehicle_behavior_logger_v1`
  format produces fewer events.
- **Drive CSV idle prefix** — the 06-38-45 reference drive has ~7 min of GPS
  acquiring and parked time before motion. Trim to start-of-motion (we have
  `drive_trimmed_from_motion.csv` on Pi 5 already) or pace high (`--rate 30+`).

## The prompt itself

Paste everything in the fenced block below into a fresh Claude session.

```
I want to set up a SkyBounce sensor network on my home LAN for bench testing — no real
radios, no HackRFs. Three Raspberry Pis act as simulated sensors (each replays a recorded
drive CSV through the SkyBounce engine), and a fourth machine acts as the basestation,
collecting telemetry from all three sensors over WiFi instead of HF radio.

Help me design and stand up this network from scratch.

# Hardware / network
- Pi 5 at sgbirm@192.168.40.178   (already deployed today: logger, engine, skybounce-radio repo)
- Pi 0 2W at sgbir@192.168.40.20  (full SkyBounce deploy lives at /home/sgbir/skybounce/)
- One more Pi (TBD model/IP — I'll provide once chosen)
- Basestation: another computer on the same WiFi (Linux preferred, IP TBD)
- No HackRFs in this setup. The RF link between sensor s.py and basestation b.py
  is REPLACED by TCP-over-WiFi.

# Software stack already on hand
- skybounce-ipc-python   IPC core, flat module layout, PYTHONPATH-based
- skybounce-event-rules  event_rules_v0_2_0 rules library (pip -e)
- skybounce-app-logistics  engine + replay.py (with --rate flag for CSV pacing,
                          and --transport {file,ipc})
- skybounce-radio        radio team's daemon. Two main scripts:
                         scripts/s.py = sensor-side daemon (talks IPC → HackRF)
                         scripts/b.py = basestation daemon (talks HackRF → backend)
- pi-sensor-stack        vehicle_behavior_simple_logger_v0_1 (the 1Hz CSV writer)
- Recorded drive CSVs at /home/sgbir/sensor_data/vehicle_behavior/raw/
  on the .40.20 Pi (vehicle_behavior_simple_logger_v0_1_2026-05-26_06-38-45.csv
  is the one we've been bench-testing with)

# Memory files you should read first
- ~/.claude/projects/C--Users-sgbir/memory/MEMORY.md
- ~/.claude/projects/C--Users-sgbir/memory/pi_access.md     (Pi IPs, SSH layout, WSL > PowerShell)
- ~/.claude/projects/C--Users-sgbir/memory/skybounce_architecture.md
- ~/.claude/projects/C--Users-sgbir/memory/project_layout.md
- ~/.claude/projects/C--Users-sgbir/memory/data_layout_decision.md

# Architecture to figure out (this is the design question I want your help on)

Today, on a single Pi, the flow is:
  drive CSV → replay.py (engine) → SkyBounce IPC (Unix socket) → s.py → HackRF → over the air → b.py → backend

I want this network-replacement flow:
  drive CSV → replay.py (engine) → IPC frame → TCP/WiFi → basestation → backend logic

Three approaches to consider — pick the simplest that lets us drive realistic protocol
behavior from the basestation perspective:

  A. socat bridge per Pi. Each Pi runs `socat UNIX-LISTEN:/tmp/skybounce-sensor.sock
     TCP:basestation:PORT`. s.py unmodified, no HackRF init. Each sensor's IPC stream
     becomes one TCP connection to basestation. Basestation does multi-conn accept loop.
     Pros: zero code change. Cons: no L2/L3 protocol — just raw IPC frames. Basestation
     has to implement the sensor-side coordination, JOIN handshake, ACK0 beacons, etc.

  B. Replace HackRF backend with a TCP "fake radio". The radio team already has
     skybounce/hackrf_backend_rx.py and possibly a hackrf_fake.py (saw it on .40.20:
     /home/sgbir/HackRF/hackrf_fake.py). If their codebase has a clean backend
     interface, write a TcpBackend that pretends to be a HackRF but ships bits via
     TCP. Keeps L1/L2 protocol intact; only the physical layer is replaced.
     Pros: protocol-accurate simulation. Cons: need to write/wire a new backend.

  C. Skip s.py/b.py entirely. Each Pi runs replay → engine → --transport file (JSONL).
     A small TCP forwarder ships new JSONL lines to basestation. Basestation aggregates.
     No L2/L3 sim at all — purely application-level event collection.
     Pros: trivial. Cons: doesn't exercise the radio team's daemon at all.

Recommend an approach (A is probably best for fast bench validation), get my agreement,
then execute step by step.

# Steps I expect, once we agree on architecture
1. Confirm WiFi connectivity between all four machines (ping matrix).
2. Identify/provision the third Pi (model? Pi 4? another Pi 0 2W? — ask me).
3. Decide an IP/port scheme for the basestation listener.
4. On each Pi, deploy logger + engine + selected replay CSV (rsync from .40.20 via
   WSL transit, same pattern used 2026-05-29). Use 1Hz CSV (the 06-38-45 file or
   a similar one).
5. On the basestation, deploy whatever process aggregates incoming streams.
   - If approach A: a small Python TCP server that reads SkyBounce IPC frames per
     connection, logs/ACKs them, possibly using the goldens at
     skybounce-ipc-python/docs/sensor_ipc_goldens.json to validate framing.
   - If approach B: s.py with a new --hackrf-backend tcp option pointing at... well,
     this requires more upstream coordination with the radio team.
6. Bring up the basestation first, then each Pi sensor one at a time, watching the
   handshake (HELLO / HELLO_ACK / STEADY) for each.
7. Trigger replay simultaneously on all three Pis (paced realistically, --rate 1.0
   or 5.0). Watch the basestation accept all three IPC streams concurrently.
8. Verify dispositions: each Pi's engine should see QUEUED at minimum, ideally
   DELIVERED if the basestation acks each telemetry.
9. Write a small log report at the end summarizing TELEMETRY_ACK disposition counts
   per sensor and total throughput.

# Conventions / gotchas
- I use WSL Ubuntu (not PowerShell) to drive Pis — saves a lot of pain. PowerShell
  mangles quoting through ssh. See pi_access.md.
- bash heredocs through wsl→ssh layers sometimes eat $VAR — when in doubt, scp a
  script file and execute, or use `printf "%s\n" ...` to build literal scripts.
- The radio team's s.py needs ham-mode + callsign when transmitting on real RF.
  In this test, no RF, so --ham-mode is moot.
- replay.py with --rate 1.0 is real-time; the 06-38-45 drive CSV starts with
  ~7 minutes of GPS-acquiring and parked time before any motion events fire.
  Use the trimmed version we created today (`drive_trimmed_from_motion.csv` on Pi 5
  at /home/sgbirm/sensor_data/vehicle_behavior/drive_csvs/), or trim a fresh one.

Begin by reading the memory files and proposing architecture A/B/C with your reasoning.
```

## Companion artifacts that already exist on Pi 5

When the new session spins up, these are useful preexisting pieces it can build on
rather than recreate:

```
Pi 5 (sgbirm@192.168.40.178)
  /home/sgbirm/skybounce-app/skybounce-app-logistics/       engine + replay.py (with --rate)
  /home/sgbirm/skybounce-app/skybounce-ipc-python/          IPC core + goldens
  /home/sgbirm/skybounce-app/skybounce-event-rules/         rules library
  /home/sgbirm/skybounce-radio/                             radio team's daemon (s.py, b.py)
  /home/sgbirm/skybounce-app/venv/                          venv (Python 3.11) with all deps + ntplib
  /home/sgbirm/sensor_data/vehicle_behavior/
    drive_csvs/
      drive_trimmed_from_motion.csv                          ← starts ~60s before motion
      vehicle_behavior_simple_logger_v0_1_2026-05-26_06-38-45.csv  (the full source)
    events/
      engine_burst_2026-05-29_165636.log                     ← 18-event burst run log
```

## Open questions to flag for the new session

1. **s.py vs b.py confusion** — `s.py` is the *sensor* daemon, `b.py` is the
   *basestation*. The user originally said "another computer will run s.py and
   function as the basestation". That's almost certainly b.py. Confirm.
2. **Third Pi** — model, IP, OS. User needs to provide.
3. **Approach A vs B** — A is fast to stand up but skips L2/L3 protocol. B is
   protocol-accurate but needs a new backend. C is application-only. The
   approach choice gates the whole rest of the design.
4. **CSV distribution** — do all 3 Pis replay the SAME drive CSV (showing what
   3 identical sensors look like), or 3 DIFFERENT drives (showing real fleet diversity)?
5. **Pacing rate** — `--rate 1.0` (real-time, 42 min) or higher for quicker
   iteration cycles?
