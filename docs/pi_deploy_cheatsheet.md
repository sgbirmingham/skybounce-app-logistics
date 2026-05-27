# Pi Deploy Cheat Sheet

Tomorrow-you running this on a Pi (Raspberry Pi 4/5 or similar) to get
the streaming engine working. Validated on Ubuntu/Linux Python 3.14 in
this session; should work identically on Pi OS / Raspberry Pi OS with
Python 3.10+.

Convention: `/home/sgbir/skybounce/` as the deploy root. Adjust paths
if you use something else.

## 0. Prereqs on the Pi (one-time)

```bash
# Python + venv module (Pi OS usually ships these, but newer Pythons may need this)
sudo apt update
sudo apt install -y python3-venv python3-pip git rsync

# Optional sanity check
python3 --version            # need 3.10+
python3 -m venv --help >/dev/null && echo "venv OK"
```

If `python3 -m venv` complains about `ensurepip`, install the matching
version-specific package:

```bash
sudo apt install -y python3.X-venv     # replace X with your minor version
```

## 1. Get the three repos onto the Pi

Pick one method.

### Option A — git clone (simplest if Pi has internet + your GitHub access)

```bash
mkdir -p /home/sgbir/skybounce
cd /home/sgbir/skybounce
git clone https://github.com/sgbirmingham/skybounce-ipc-python.git
git clone https://github.com/sgbirmingham/skybounce-event-rules.git
git clone https://github.com/sgbirmingham/skybounce-app-logistics.git
```

### Option B — rsync from this Windows machine (via WSL)

Run from WSL on the Windows box, replacing `pi-hostname` with whatever
your Pi answers to (mDNS hostname or IP):

```bash
# From WSL on the Windows machine
PI=pi-hostname     # or 192.168.x.x

ssh sgbir@$PI 'mkdir -p /home/sgbir/skybounce'

for repo in skybounce-ipc-python skybounce-event-rules skybounce-app-logistics; do
    rsync -avz --exclude='.venv' --exclude='__pycache__' --exclude='.pytest_cache' \
        /mnt/c/Users/sgbir/Projects/$repo/ \
        sgbir@$PI:/home/sgbir/skybounce/$repo/
done
```

Rsync wins if you have uncommitted local changes you want on the Pi.
Otherwise git clone is fine and simpler.

## 2. Set up the venv on the Pi

```bash
cd /home/sgbir/skybounce
python3 -m venv venv
./venv/bin/python -m pip install --upgrade pip
./venv/bin/python -m pip install pytest

# skybounce-event-rules and skybounce-app-logistics are pip-installable editable.
./venv/bin/python -m pip install -e ./skybounce-event-rules
./venv/bin/python -m pip install -e ./skybounce-app-logistics

# skybounce-ipc-python has flat modules (no pyproject.toml) — use PYTHONPATH instead.
# Below commands all set PYTHONPATH inline. If you want it permanent:
echo 'export PYTHONPATH=/home/sgbir/skybounce/skybounce-ipc-python' >> ~/.bashrc
# then: source ~/.bashrc
```

## 3. Loopback validation (confirms IpcTransport works on ARM)

```bash
cd /home/sgbir/skybounce/skybounce-app-logistics
PYTHONPATH=/home/sgbir/skybounce/skybounce-ipc-python \
    ~/skybounce/venv/bin/python -m pytest tests/test_ipc_transport_loopback.py -v
```

Expected: **3 passed in < 1s**. Validates handshake, SB45 telemetry
round-trip with DELIVERED ACK, CMD round-trip with OK, STATUS link-state
surfacing — all against an in-process mock SkyBounce. No real radio
required for this test.

Full test suite (29 streaming-engine tests + the 3 loopback):

```bash
PYTHONPATH=/home/sgbir/skybounce/skybounce-ipc-python \
    ~/skybounce/venv/bin/python -m pytest -v
```

Expected: **32 passed**.

## 4. Run the streaming engine, file transport (dev / no radio)

The logger runs as a systemd service (`vehicle-logger.service`), so you
do NOT start it manually. The service writes CSVs to
`/home/sgbir/sensor_data/vehicle_behavior/raw/` continuously.

```bash
# Terminal 1: verify the logger service is running and producing fresh CSVs
systemctl is-active vehicle-logger.service
ls -lt /home/sgbir/sensor_data/vehicle_behavior/raw/ | head -3

# Terminal 2: the streaming engine, file transport (no IPC daemon needed)
cd ~/skybounce/skybounce-app-logistics
PYTHONPATH=/home/sgbir/skybounce/skybounce-ipc-python \
    ~/skybounce/venv/bin/python -m skybounce_app_logistics.scripts.live \
        --watch /home/sgbir/sensor_data/vehicle_behavior/raw \
        --out   /home/sgbir/sensor_data/vehicle_behavior/events/live.jsonl
```

Events flow into `live.jsonl`. Tail it with `tail -f` to watch them
appear. Ctrl+C the engine for clean shutdown (it flushes any pending
interval events first).

Note: when the Pi is stationary (e.g., sitting on a desk indoors with no
GPS fix), the engine correctly stays in `STARTUP_GPS_ACQUIRING` state
and emits very few events. For a demo-grade event stream, use replay
against a validated drive CSV — see section 6.

## 5. Run the streaming engine, IPC transport (real)

Requires the SkyBounce daemon (or the mock listener) running and
listening on the configured socket.

```bash
# Optional: mock listener for end-to-end test without real radio
PYTHONPATH=/home/sgbir/skybounce/skybounce-ipc-python \
    ~/skybounce/venv/bin/python /home/sgbir/skybounce/skybounce-ipc-python/examples/mock_skybounce_listener.py &

# The streaming engine, IPC transport
cd ~/skybounce/skybounce-app-logistics
PYTHONPATH=/home/sgbir/skybounce/skybounce-ipc-python \
    ~/skybounce/venv/bin/python -m skybounce_app_logistics.scripts.live \
        --watch /home/sgbir/sensor_data/vehicle_behavior/raw \
        --transport ipc \
        --socket /tmp/skybounce-sensor.sock \
        --endpoint-id 0x53415050
```

The `--out` arg is ignored in `--transport ipc` mode (a warning logs
that fact). Events are encoded as 6-byte SB45 payloads and submitted
via the IPC client. Watch the mock listener's output to see what
arrived.

## 6. Replay against a validated drive CSV (demo path)

This is the most demonstrable use of the streaming engine: a real
validated drive CSV in, event JSONL out, in under a second. Three drives
are validated and known to produce byte-exact analyzer parity:

```bash
cd ~/skybounce/skybounce-app-logistics
PYTHONPATH=/home/sgbir/skybounce/skybounce-ipc-python \
    ~/skybounce/venv/bin/python -m skybounce_app_logistics.scripts.replay \
        --input /home/sgbir/sensor_data/vehicle_behavior/raw/vehicle_behavior_simple_logger_v0_1_2026-05-26_06-38-45.csv \
        --out   /home/sgbir/sensor_data/vehicle_behavior/events/replay_may26.jsonl
```

Expected: ~22 events emitted (14 analyzer-parity events + state-interval
heartbeats + 1 startup event).

To inspect:

```bash
wc -l /home/sgbir/sensor_data/vehicle_behavior/events/replay_may26.jsonl
grep -v "startup_gps_acquiring" /home/sgbir/sensor_data/vehicle_behavior/events/replay_may26.jsonl \
    | python3 -c "import sys,json; [print(json.loads(l).get('event_type','?'), '/', json.loads(l).get('priority','?')) for l in sys.stdin]"
```

Validated drives:

- `vehicle_behavior_simple_logger_v0_1_2026-05-18_16-15-23.csv` (47 min, 12 hard events)
- `vehicle_behavior_simple_logger_v0_1_2026-05-19_12-52-09.csv` (70 min, 21 hard events)
- `vehicle_behavior_simple_logger_v0_1_2026-05-26_06-38-45.csv` (48 min, 14 hard events)

## 7. Troubleshooting

**`ModuleNotFoundError: No module named 'skybounce_client'`**
PYTHONPATH not set. The IPC repo has flat modules and isn't
pip-installable; either export `PYTHONPATH=.../skybounce-ipc-python`
or prefix every command with it as shown above.

**`--transport ipc requires skybounce-ipc-python: ...`**
Same as above — PYTHONPATH issue, surfaced as a clean argparse error.

**Loopback test skipped on the Pi**
The test skips if `AF_UNIX` isn't available. On Linux/Pi OS this should
never happen; if it does, your Python build is unusual — check
`python3 -c "import socket; print(hasattr(socket, 'AF_UNIX'))"`.

**`SKYBOUNCE_SENSOR_SOCK` env var**
The `--socket` flag overrides this env var; the env var overrides the
library default (`/tmp/skybounce-sensor.sock`). Useful when running
multiple instances or testing against a non-default daemon location.

**Outbound queue full**
Library default is 64 frames. If the SkyBounce daemon is slow to ACK
and the engine is producing fast, telemetry submissions will raise
`queue.Full`. Look in `transport.py`'s TELEMETRY_ACK summary at
shutdown to see what dispositions came back.

**No events appearing in live mode**
The engine is conservative on startup. When the Pi is stationary and
GPS hasn't acquired a fix, the engine sits in `STARTUP_GPS_ACQUIRING`
and emits only the initial context event. Shake hard or wait for GPS;
better, use the replay path in section 6.

**Pytest run from `~` finds unrelated tests**
Don't run pytest from your home directory — it walks the whole tree
and tries to import every `test_*.py` and `*_test.py` file, including
old HackRF, ads1256, and coldchain_poc code that has unrelated
dependencies. Always `cd` into the specific repo first.

## 8. What's NOT covered here

- **systemd unit for the streaming engine.** The logger has a service
  (`vehicle-logger.service`) but the streaming engine still runs in the
  foreground. Ask for a follow-up if needed.
- **Real SkyBounce daemon** install / config. This sheet assumes either
  the mock listener or that a SkyBounce daemon is already running.
- **Sensor wiring** (BME280, DS18B20, GPS, MPU6050). That's
  `pi-sensor-stack` territory; existing scripts handle it.

## 9. Quick reference: paths to remember

```
/home/sgbir/skybounce/                          deploy root
    venv/                                       Python venv
    skybounce-ipc-python/                       IPC core (flat modules; PYTHONPATH)
    skybounce-event-rules/                      shared rules (pip -e)
    skybounce-app-logistics/                    streaming engine (pip -e)

/home/sgbir/repos/pi-sensor-stack/              vehicle-behavior logger (canonical)
    runtime/vehicle_behavior/logger/            logger script (runs as service)

/home/sgbir/sensor_data/vehicle_behavior/       sensor data, neutral location
    raw/                                        logger CSV output
        archive_v1.1/                           older v1.1 logger CSVs (April)
        archive_v7_4/                           older v7_4 logger CSVs (April)
    events/                                     streaming engine JSONL output

/etc/systemd/system/vehicle-logger.service      logger systemd unit

/tmp/skybounce-sensor.sock                      default IPC socket
```