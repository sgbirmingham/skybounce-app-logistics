# How the engine + encoder are started on the Pi

> Short answer: one Python command line. It's a module invocation, not
> a daemon — currently launched by hand (or by a future systemd unit).
> It runs as a single long-lived process that owns three jobs at once:
> tail the logger CSV, run the rules engine, encode and submit
> telemetry to the SkyBounce daemon.

**Companion document:** [pipeline_overview.md](./pipeline_overview.md)
explains why these three jobs live in one process and where this
process sits in the full sensor stack.

## The TL;DR command

This is the production-style invocation — tail the logger CSV
continuously, encode events as SB45, ship them over the IPC socket:

```bash
PYTHONPATH=/home/sgbir/skybounce/skybounce-ipc-python \
    /home/sgbir/skybounce/venv/bin/python \
    -m skybounce_app_logistics.scripts.live \
        --watch /home/sgbir/coldchain_poc/data/simple_logger/raw \
        --transport ipc \
        --socket /tmp/skybounce-sensor.sock \
        --endpoint-id 0x53415050
```

That single process is the **engine + encoder**. There's no separate
encoder process — `pack_sb45` is just a function call inside
`IpcTransport.emit()`, which is called by the engine when a rule
fires.

## What each piece of that command does

| Piece | Why it's there |
| --- | --- |
| `PYTHONPATH=/home/sgbir/skybounce/skybounce-ipc-python` | The IPC repo uses flat modules (no `pyproject.toml`); it isn't pip-installable. The engine imports `skybounce_client`, `skybounce_ipc`, `sb_telemetry_payload` from it, so it needs to be on `PYTHONPATH`. |
| `/home/sgbir/skybounce/venv/bin/python` | A venv with `skybounce-app-logistics` and `skybounce-event-rules` installed editable (`pip install -e`). Also has `pytest` and `numpy` (numpy is a dep of app-logistics). |
| `-m skybounce_app_logistics.scripts.live` | Runs `src/skybounce_app_logistics/scripts/live.py` as `__main__`. This is the live-tail script. The companion `replay` script is for offline. |
| `--watch <dir>` | Directory containing logger CSVs. The script auto-discovers the most recently modified one matching `vehicle_behavior_simple_logger_v*_*.csv`. If the dir is empty when the engine starts, it waits up to 60 s for a file to appear (handles the engine-starts-first race). Alternatively, `--input <path>` gives an explicit CSV. |
| `--transport ipc` | Routes events to the SkyBounce daemon via Unix socket instead of writing JSONL to a file. `--transport file` is the dev / no-radio path; `--transport ipc` is production. |
| `--socket /tmp/skybounce-sensor.sock` | The agreed IPC socket address (matches the radio team's listener). Same env-var override (`SKYBOUNCE_SENSOR_SOCK`) and same default as everywhere else. |
| `--endpoint-id 0x53415050` | 32-bit ID sent in HELLO so the daemon can identify which sensor app it's talking to. Current default is `"SAPP"` in ASCII; real deployments should set a per-device value. |

## Two flavors of launch

### `live` — production, continuous

What's above. Tails an actively-written CSV. Runs until Ctrl+C.
Heartbeat log every 60 s ("processed N rows in last 60s, total
events emitted: M") so an operator can tell it's alive during a long
drive.

### `replay` — offline, one-shot

```bash
PYTHONPATH=/home/sgbir/skybounce/skybounce-ipc-python \
    /home/sgbir/skybounce/venv/bin/python \
    -m skybounce_app_logistics.scripts.replay \
        --input /path/to/captured-drive.csv \
        --out /tmp/events.jsonl
```

Reads a **closed** CSV start-to-finish, emits events to `--out`
(JSONL) or via `--transport ipc`, exits. Used for validation: capture
a drive, replay it through the engine, compare to expected output.
The three real-vehicle drives in `replay_check/` were processed this
way.

## What needs to be running first

For the `live` + `--transport ipc` invocation above:

1. **The logger** must be running (or about to start) and producing
   rows in `--watch`. That's `vehicle_behavior_simple_logger_v0_1.py`
   from `pi_sensor_stack` — a separate process started independently.
2. **The SkyBounce daemon** must be bound to the IPC socket. If the
   engine connects and the socket isn't there or no one is `listen()`
   ing, it retries in a reconnect loop (handled by
   `SkyBounceClient`). Engine will sit and wait until the daemon
   comes up.

For dev / no-radio testing, neither of those needs to be production:

- Replace the logger with a prerecorded CSV: use `--input` instead of
  `--watch`.
- Replace the daemon with `examples/mock_skybounce_listener.py` from
  `skybounce-ipc-python`. Same socket contract, no radio.

## Stopping it

Ctrl+C once: clean shutdown. The signal handler:

1. Stops consuming new rows from the CSV.
2. Calls `engine.flush()` — emits any pending interval events (e.g.,
   a `state_moving` summary if a movement segment is open).
3. Calls `transport.close()` — for `IpcTransport`, this waits up to
   5 s for pending TELEMETRY frames to reach a terminal disposition
   (DELIVERED or a failure code) before tearing down the socket. This
   is the drain behavior from the 2026-05-27 fix. Without it, the
   last 1–4 events of a session would race the socket teardown and
   never make the wire.

Ctrl+C twice: hard exit (raises). Use if cleanup hangs (it shouldn't).

## Currently a manual launch — no systemd yet

Right now, on the Pi, someone (or some setup script) opens an SSH
session and runs that command. Anything that wants the engine to come
up automatically at boot, or restart on crash, would need a
**systemd unit** that wraps the same command. That's explicitly noted
as out of scope for v0.1 in the Pi deploy cheatsheet
([pi_deploy_cheatsheet.md](./pi_deploy_cheatsheet.md), Section 7).
When the radio team is ready for unattended operation, that's the
missing piece — and it's a 30-line `.service` file, not a code
change.

## If you remember one thing

**The "engine" and the "encoder" are the same process.** It's one
`python -m …` invocation that imports `pack_sb45` as a function and
calls it inline whenever a rule fires. The encoder doesn't need to be
started separately, configured separately, or even noticed — it's
just code that runs when the engine has an event to send. The thing
you start is the engine; the encoder comes along for the ride.
