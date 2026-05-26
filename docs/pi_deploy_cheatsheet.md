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

for repo in skybounce_IPC_python skybounce-event-rules skybounce-app-logistics; do
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

# skybounce_IPC_python has flat modules (no pyproject.toml) — use PYTHONPATH instead.
# Below commands all set PYTHONPATH inline. If you want it permanent:
echo 'export PYTHONPATH=/home/sgbir/skybounce/skybounce_IPC_python' >> ~/.bashrc
# then: source ~/.bashrc
```

## 3. Loopback validation (confirms IpcTransport works on ARM)

```bash
cd /home/sgbir/skybounce/skybounce-app-logistics
PYTHONPATH=/home/sgbir/skybounce/skybounce_IPC_python \
    ~/skybounce/venv/bin/python -m pytest tests/test_ipc_transport_loopback.py -v
```

Expected: **3 passed in < 1s**. Validates handshake, SB45 telemetry
round-trip with DELIVERED ACK, CMD round-trip with OK, STATUS link-state
surfacing — all against an in-process mock SkyBounce. No real radio
required for this test.

Full test suite (29 streaming-engine tests + the 3 loopback):

```bash
PYTHONPATH=/home/sgbir/skybounce/skybounce_IPC_python \
    ~/skybounce/venv/bin/python -m pytest -v
```

Expected: **32 passed**.

## 4. Run the streaming engine, file transport (dev / no radio)

If you've also got `pi_sensor_stack` deployed and the logger running,
this tails its CSV and writes events to JSONL.

```bash
# Terminal 1: the logger (from pi_sensor_stack)
cd ~/pi_sensor_stack/runtime/vehicle_behavior/logger
python3 vehicle_behavior_simple_logger_v0_1.py \
    --interval-s 1.0 \
    --out-dir /home/sgbir/coldchain_poc/data/simple_logger/raw &

# Terminal 2: the streaming engine, file transport (no IPC daemon needed)
PYTHONPATH=/home/sgbir/skybounce/skybounce_IPC_python \
    ~/skybounce/venv/bin/python -m skybounce_app_logistics.scripts.live \
        --watch /home/sgbir/coldchain_poc/data/simple_logger/raw \
        --out   /home/sgbir/coldchain_poc/data/events/live.jsonl
```

Events flow into `live.jsonl`. Tail it with `tail -f` to watch them
appear. Ctrl+C the engine for clean shutdown (it flushes any pending
interval events first).

## 5. Run the streaming engine, IPC transport (real)

Requires the SkyBounce daemon (or the mock listener) running and
listening on the configured socket.

```bash
# Optional: mock listener for end-to-end test without real radio
PYTHONPATH=/home/sgbir/skybounce/skybounce_IPC_python \
    ~/skybounce/venv/bin/python /home/sgbir/skybounce/skybounce_IPC_python/examples/mock_skybounce_listener.py &

# The streaming engine, IPC transport
PYTHONPATH=/home/sgbir/skybounce/skybounce_IPC_python \
    ~/skybounce/venv/bin/python -m skybounce_app_logistics.scripts.live \
        --watch /home/sgbir/coldchain_poc/data/simple_logger/raw \
        --transport ipc \
        --socket /tmp/skybounce-sensor.sock \
        --endpoint-id 0x53415050
```

The `--out` arg is ignored in `--transport ipc` mode (a warning logs
that fact). Events are encoded as 6-byte SB45 payloads and submitted
via the IPC client. Watch the mock listener's output to see what
arrived.

For replay against a stored CSV (offline validation, no logger needed):

```bash
PYTHONPATH=/home/sgbir/skybounce/skybounce_IPC_python \
    ~/skybounce/venv/bin/python -m skybounce_app_logistics.scripts.replay \
        --input /path/to/drive.csv \
        --out   /tmp/events.jsonl
```

## 6. Troubleshooting

**`ModuleNotFoundError: No module named 'skybounce_client'`**
PYTHONPATH not set. The IPC repo has flat modules and isn't
pip-installable; either export `PYTHONPATH=.../skybounce_IPC_python`
or prefix every command with it as shown above.

**`--transport ipc requires skybounce_IPC_python: ...`**
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

## 7. What's NOT covered here

- **systemd units** for keeping the logger / engine alive across reboots.
  Out of scope for v0.1; ask for a follow-up if needed.
- **Real SkyBounce daemon** install / config. This sheet assumes either
  the mock listener or that a SkyBounce daemon is already running.
- **Sensor wiring** (BME280, DS18B20, GPS, MPU6050). That's
  `pi_sensor_stack` territory; existing scripts handle it.
- **Logger deployment**. The vehicle_behavior logger lives in
  `pi_sensor_stack` and follows its own deploy script
  (`runtime/vehicle_behavior/deploy_to_runtime.sh` — not read by me;
  may or may not be current).

## 8. Quick reference: paths to remember

```
/home/sgbir/skybounce/                          deploy root
    venv/                                       Python venv
    skybounce_IPC_python/                       IPC core (flat modules; PYTHONPATH)
    skybounce-event-rules/                      shared rules (pip -e)
    skybounce-app-logistics/                    streaming engine (pip -e)

/home/sgbir/coldchain_poc/data/                 sensor data, by convention
    simple_logger/raw/                          logger CSV output
    events/                                     streaming engine JSONL output

/tmp/skybounce-sensor.sock                      default IPC socket
```
