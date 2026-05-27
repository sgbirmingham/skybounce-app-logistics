# Sensor stack pipeline — how a sensor sample becomes a radio packet

> The Pi doesn't run "one program that does everything." It runs a
> **chain of small processes**, each owning one stage of a pipeline,
> talking to the next stage through a clean boundary (a CSV file, a
> Python function call, or a Unix socket). The repos on GitHub mirror
> that split — they are not alternatives to each other or libraries of
> one main app. They are **the four stages**.

**Companion document:** [running_the_engine.md](./running_the_engine.md)
covers how the engine + encoder process is started on the Pi.

## The pipeline

```
   ┌──────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌──────────────┐
   │  HARDWARE    │    │   STREAMING      │    │   SB45 ENCODER   │    │   IPC OVER      │    │  SKYBOUNCE   │
   │  SENSORS     │───▶│   ENGINE         │───▶│                  │───▶│   UNIX SOCKET   │───▶│  DAEMON      │
   │              │    │   (rules)        │    │   (payload)      │    │   (framing)     │    │  (radio)     │
   └──────────────┘    └──────────────────┘    └──────────────────┘    └─────────────────┘    └──────────────┘
   IMU, GPS, BME280,    Reads each CSV row,     Squashes the 20+      Wraps the 6 bytes      Other team's code.
   DS18B20 — driven     runs the state          fields of an Event    in a 14-byte IPC       Takes the SB45 bytes,
   by pi_sensor_stack.  machine and the rules   into a packed         frame, sends over      schedules the RF
   Writes one CSV       library, emits Event    6-byte SB45_SIM_V2    AF_UNIX socket at      transmission, ACKs back
   row per second.      objects when a rule     payload.              /tmp/skybounce-        through the same socket.
                        fires.                                        sensor.sock.

   ──── pi_sensor_stack ────────── skybounce-app-logistics ────────── skybounce-ipc-python ─────────── (their repo) ────
```

Two file-on-disk artifacts cross stage boundaries:

- **The logger CSV** (one row per second of sensor data) — between
  sensors and engine. Persists, lets us replay drives offline.
- **The Unix socket** at `/tmp/skybounce-sensor.sock` — between IPC
  client and SkyBounce daemon. Doesn't persist; it's just the
  rendezvous point.

Everything else is in-process function calls.

## Concrete trace of one event — a hard brake

Driver brakes hard at 14 minutes into a trip.

1. **MPU6050 IMU** measures a sustained ~0.5 g deceleration.
2. **`pi_sensor_stack`'s logger** (running on the Pi, in its own
   process) reads the IMU once a second and appends a CSV row:
   `ts_epoch_s, elapsed_s, accel_x_g, accel_y_g, accel_z_g, gyro_*,
   gps_*, …`
3. **The streaming engine** (a separate Python process —
   `python -m skybounce_app_logistics.scripts.live` — tailing the
   same CSV) reads that row. It computes derived features (linear
   accel, jerk, speed change), updates its state machine (MOVING /
   STOPPED / GPS_DEGRADED / etc.), and runs the rules from the
   `skybounce-event-rules` library against the current frame plus
   history. The brake threshold fires.
4. The engine constructs an **`Event`** object — a normal Python
   dataclass — with `event_type="hard_brake"`, `priority="P2"`,
   `score=0.72`, `speed_mph=58`, `decel_m_s2=4`, lat/lon, etc. ~20
   fields total.
5. The engine calls **`IpcTransport.emit(event)`**. That handler is in
   `skybounce-app-logistics/src/skybounce_app_logistics/transport.py`.
   The transport object is a thin wrapper around `SkyBounceClient`.
6. Inside `emit()`, the Event is squashed into an **SB45 payload** —
   six bytes — by `pack_sb45()` (in
   `skybounce-ipc-python/sb_telemetry_payload.py`). Twenty Python
   fields get bit-packed into 45 bits: event code (4 bits), priority
   (2), location status (2), severity 0–7 (3), speed in mph (7),
   deceleration (5), GPS confidence (3), minutes since session start
   mod 256 (8), lat/lon offsets from anchor (5+6). For the hard_brake
   we're tracing, the bytes work out to `1755D130742X` (last hex
   differs by lat/lon).
7. The IPC client wraps those 6 bytes in an IPC **TELEMETRY frame**:
   a 14-byte header (message type, sequence number, microsecond
   timestamp, payload length) followed by the SB45 bytes. Total 26
   bytes on the wire.
8. The client writes the 26 bytes to
   **`/tmp/skybounce-sensor.sock`** — a Unix domain socket the
   SkyBounce daemon is listening on. This is the boundary with the
   other team's code.
9. The **SkyBounce daemon** (the radio team's process, not in our
   repos) reads the frame, queues it for RF transmission, and sends an
   immediate **`TELEMETRY_ACK` (QUEUED)** back through the same
   socket so our side knows it's been accepted.
10. When the daemon actually transmits over the radio (or hands off
    to the next L2 layer), it sends another ACK —
    **`TELEMETRY_ACK` (DELIVERED)** — so our engine logs confirmation
    of dispatch.
11. The base station downstream of the radio receives the 6 SB45
    bytes, decodes them with `unpack_sb45()` (which is in the same
    `skybounce-ipc-python` module so both sides stay consistent), and
    gets back the event_code, severity, speed, etc.

The end-to-end latency on the Pi side is sub-millisecond; the radio
transmission is the slow part.

## Why it's split this way

Each boundary exists because the two sides have **different concerns
and update at different cadences**:

| Boundary | Why it exists |
| --- | --- |
| Sensors → Logger CSV | Logger isolates the hardware lifecycle (sensor failures, GPS lock-loss, kernel drivers) from anything analytical. CSV also lets us replay drives offline — three sample drives have already been replayed end-to-end this way. |
| Logger CSV → Engine | Engine has no hardware dependencies and runs identically on a workstation, on the Pi, or against a captured CSV. Rules can be tested without a sensor in the loop. |
| Engine Event → SB45 bytes | Rules logic lives in software; bit-packing lives in a known-spec module. SB45 has its own versioning (`SB45_SIM_V0/V1/V2`) so encoder changes don't drag along rules changes. |
| SB45 bytes → IPC frame | The IPC layer is **payload-agnostic** — a partner shipping a non-SB45 telemetry format keeps the IPC unchanged and just submits different bytes. The radio team only cares about the IPC contract, not the SB45 format. |
| IPC socket → Daemon | This is the org boundary. Our team owns everything up to the socket; the radio team owns everything from the socket onward. We can each iterate without breaking the other as long as the spec doesn't change. |

If they were one program, **every change would touch every concern**.
Splitting it lets the hardware team, the analytics team, the protocol
team, and the radio team work without stepping on each other.

## Where each stage lives (repo map)

| Stage | Repo / module | Process on the Pi |
| --- | --- | --- |
| Hardware drivers + 1 Hz logger | `pi_sensor_stack` (separate repo) | `vehicle_behavior_simple_logger_v0_1.py` (one process) |
| CSV ingest + state machine + rules engine + Event emit | `skybounce-app-logistics` (main orchestrator) | `python -m skybounce_app_logistics.scripts.live` (one process) |
| SB45 payload encoder | `skybounce-ipc-python/sb_telemetry_payload.py` | Loaded as a Python module by the engine — no separate process |
| IPC client (frame format, socket, handshake, ACK lifecycle) | `skybounce-ipc-python/skybounce_client.py` | Same — module loaded by the engine |
| IPC daemon (binds the socket, schedules radio TX) | **other team's repo** | Separate process |
| Spec defining frame format, message types, state machine | `skybounce-ipc-python/docs/SkyBounce_Sensor_IPC_Protocol_v1.0.pdf` | n/a |

So on the Pi, when everything's running for real, there are
**three processes**:

```
$ ps -ef | grep skybounce
sgbir   …   vehicle_behavior_simple_logger_v0_1.py            (1) logger, in pi_sensor_stack
sgbir   …   python -m skybounce_app_logistics.scripts.live    (2) engine + encoder + IPC client
sgbir   …   skybounce_daemon_or_whatever_they_call_it         (3) the radio team's daemon
```

They communicate through one CSV (logger → engine) and one Unix
socket (engine → daemon). That's the whole architecture.

## If you remember one thing

**Sensors don't trigger packets directly.** A sensor sample is just a
row in a CSV. The streaming engine is what watches the CSV, evaluates
rules over a sliding window of rows, and **decides** when a
packet-worthy event has occurred. Then a separate encoder turns that
decision into 6 bytes, and a separate IPC client puts those 6 bytes on
a socket for the radio. The chain is
`samples → rules → event → packet`, and each arrow is a real boundary
you can poke at independently.
