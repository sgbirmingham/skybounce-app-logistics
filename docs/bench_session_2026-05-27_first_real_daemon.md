# Bench session — 2026-05-27 — first contact with the real daemon

**Status:** Connectivity ✅, handshake ✅, **frame ACK ❌** — every TELEMETRY
returned `DROPPED_INVALID`. Pick this up tomorrow.

## What was run

```bash
# The streaming engine, IPC transport
cd ~/skybounce/skybounce-app-logistics
PYTHONPATH=/home/sgbir/skybounce/skybounce-ipc-python \
    ~/skybounce/venv/bin/python -m skybounce_app_logistics.scripts.live \
        --watch /home/sgbir/sensor_data/vehicle_behavior/raw \
        --transport ipc \
        --socket /tmp/skybounce-sensor.sock \
        --endpoint-id 0x53415050
```

Stationary at desk, no GPS fix; engine sees the active logger CSV
(`vehicle_behavior_simple_logger_v0_1_2026-05-27_15-44-53.csv`) and runs
for ~3 minutes before Ctrl+C.

## Verbatim output

```
2026-05-27 15:52:19,967 skybounce.app.logistics.live INFO found logger CSV: /home/sgbir/sensor_data/vehicle_behavior/raw/vehicle_behavior_simple_logger_v0_1_2026-05-27_15-44-53.csv
2026-05-27 15:52:20,073 skybounce.client INFO starting SkyBounce client, socket=/tmp/skybounce-sensor.sock, endpoint_id=0x53415050
2026-05-27 15:52:20,075 skybounce.client INFO state DISCONNECTED -> CONNECTING
2026-05-27 15:52:20,076 skybounce.client INFO connected to /tmp/skybounce-sensor.sock
2026-05-27 15:52:20,077 skybounce.client INFO state CONNECTING -> AWAITING_HELLO
2026-05-27 15:52:20,078 skybounce.app.logistics.transport INFO IPC transport started, endpoint_id=0x53415050
Rules library: event_rules_v0_2_0
2026-05-27 15:52:20,079 skybounce.client INFO peer HELLO: role=0x02, endpoint_id=0x53433242, caps=0x0000
Tailing:       /home/sgbir/sensor_data/vehicle_behavior/raw/vehicle_behavior_simple_logger_v0_1_2026-05-27_15-44-53.csv
2026-05-27 15:52:20,080 skybounce.client INFO state AWAITING_HELLO -> AWAITING_HELLO_ACK
Transport:     ipc
2026-05-27 15:52:20,081 skybounce.client INFO state AWAITING_HELLO_ACK -> STEADY
Output:        IPC endpoint_id=0x53415050
2026-05-27 15:52:20,082 skybounce.client INFO handshake complete; STEADY
Running until interrupted (Ctrl+C).
2026-05-27 15:52:20,083 skybounce.app.logistics.engine INFO streaming engine started, rules=event_rules_v0_2_0
2026-05-27 15:52:20,094 skybounce.app.logistics.transport WARNING TELEMETRY_ACK tid=1 disposition=DROPPED_INVALID reason=0x0000 (failures so far: 1)
2026-05-27 15:52:20,435 skybounce.app.logistics.transport INFO link state UNKNOWN -> ACQUIRING, queue_depth=0
2026-05-27 15:53:20,787 skybounce.app.logistics.live INFO live: processed 508 rows in last 61s, total events emitted: 1
2026-05-27 15:54:20,871 skybounce.app.logistics.live INFO live: processed 60 rows in last 60s, total events emitted: 1
^C2026-05-27 15:55:15,854 skybounce.app.logistics.live INFO interrupt received; finishing up...
2026-05-27 15:55:15,948 skybounce.app.logistics.transport WARNING TELEMETRY_ACK tid=2 disposition=DROPPED_INVALID reason=0x0000 (failures so far: 2)
2026-05-27 15:55:15,948 skybounce.app.logistics.transport WARNING TELEMETRY_ACK tid=3 disposition=DROPPED_INVALID reason=0x0000 (failures so far: 3)
2026-05-27 15:55:15,966 skybounce.app.logistics.transport INFO IPC transport closing, 3 events submitted
2026-05-27 15:55:15,967 skybounce.app.logistics.transport INFO TELEMETRY_ACK summary: DROPPED_INVALID=3
2026-05-27 15:55:15,967 skybounce.client INFO stopping SkyBounce client
2026-05-27 15:55:15,967 skybounce.client INFO closing socket: client stop
2026-05-27 15:55:15,968 skybounce.client INFO state STEADY -> DISCONNECTED
2026-05-27 15:55:15,969 skybounce.client INFO peer closed connection
Events emitted: 3
```

## What worked

- **Socket binding & connection** — the radio team's daemon is bound to
  `/tmp/skybounce-sensor.sock` and accepts our connect.
- **HELLO/HELLO_ACK handshake** — completes cleanly, both sides reach
  STEADY in ~10 ms. Their `endpoint_id = 0x53433242` (ASCII `"SC2B"`).
  Their `role = 0x02` (SKYBOUNCE), correct.
- **TELEMETRY_ACK delivery** — they're sending ACKs back through the
  same socket, so the bidirectional IPC framing is healthy.
- **Drain fix in close()** — all 3 submitted events received terminal
  dispositions before socket teardown. No lost-on-shutdown frames.
- **Engine processing** — 508 + 60 = 568 logger rows consumed across
  ~2 minutes, no errors on our side.

## What didn't work

- **Every single TELEMETRY came back as `DROPPED_INVALID`** (disposition
  `0x81`, high bit set per spec §7.4 = terminal failure;
  `reason_code = 0x0000`, no specific reason given). 3 events submitted,
  3 rejected.
- **`link_state` only ever reached `ACQUIRING`** (`0x01`), never `UP`
  (`0x02`). The daemon is telling us the radio side isn't ready.

These two are likely related but not necessarily the same issue.

## Hypotheses for tomorrow (ordered by likelihood)

1. **Daemon rejects telemetry while `link_state ≠ UP`.** Plausible
   policy: "I'm in ACQUIRING; I have nowhere to put your bytes; drop
   them." If so, `DROPPED_INVALID` is the wrong disposition (the spec
   has `DROPPED_LINK_DOWN = 0x82` for exactly this case); we'd want to
   confirm with them whether they intend to use one disposition or the
   other.

2. **SB45 payload format mismatch.** We're sending V2 (the post-A2
   wrap encoder). Their decoder may have been written against an older
   draft or assume a different field layout. Worth handing them
   `docs/sb45_payload_companion_doc.md` if they haven't seen it, and
   the goldens at `skybounce-ipc-python/docs/sensor_ipc_goldens.json`.

   - Note: the 3 events emitted in this session are all early-startup
     types — `startup_gps_acquired` (code 1), `gps_degraded_persistent`
     (code 8), `state_gps_untrusted` (code 7). All of these codes are
     **unchanged** between SB45_SIM_V0, V1, and V2. So if their decoder
     handles V0 it should handle these. The vocabulary changes
     (codes 3, 13, 15) wouldn't have been exercised here.

3. **Frame-level header mismatch.** The 14-byte IPC header (msg_type,
   flags, payload_len, seq, ts_unix_us) is upstream of the SB45
   payload. If their parser disagrees about any of those fields,
   `DROPPED_INVALID` is a reasonable failure mode. The HELLO and
   HELLO_ACK exchanges succeeded — implying the header format itself
   is broadly compatible — but TELEMETRY-specific framing may differ.
   The goldens cover both header and payload entries; cross-checking
   their parser against `sensor_ipc_goldens.json` is exactly what
   those entries are for.

4. **Daemon is in a known broken state for testing**, and they're
   reporting `DROPPED_INVALID` to all incoming frames intentionally.
   Worth asking before doing any deep diagnosis.

## What to do first thing tomorrow

1. **Ask the radio team what `DROPPED_INVALID reason=0x0000` means in
   their daemon's current behavior.** Two-minute conversation that
   could rule out hypothesis 1 or 4 immediately.

2. **Capture the bytes we're sending.** Run the IPC demo against the
   mock listener (which we know accepts our SB45 fine) and dump the
   exact byte sequence for one of the engine's GPS-degraded events.
   Then run the same against their daemon. If the bytes are identical
   and theirs rejects, the problem is in their decoder, not ours.
   ```bash
   # Mock listener side — we already verified this path works:
   /tmp/start_mock_listener.sh
   # Then run the engine with --transport ipc against it for ~10s,
   # capture the TELEMETRY hex dumps from the listener log.
   ```

3. **Compare against the goldens.** Pick one matching event type
   (a real telemetry payload, e.g. `telemetry_payload_basic`) and
   confirm the bytes we put on the wire are what
   `tests/test_goldens.py` expects. If not, we have an encoder bug we
   need to chase. If yes, the goldens themselves should be the
   conformance contract we hand them.

4. **Optionally test with `sensor_app_stub.py`** instead of the live
   engine — it sends raw test bytes via `submit_telemetry()` directly,
   bypassing the SB45 encoder. If those are also `DROPPED_INVALID`,
   the issue is upstream of SB45 (the IPC frame layer). If those are
   accepted, the issue is in SB45 or the streaming engine.

   ```bash
   PYTHONPATH=/home/sgbir/skybounce/skybounce-ipc-python \
       /home/sgbir/skybounce/venv/bin/python \
       /home/sgbir/skybounce/skybounce-ipc-python/scripts/sensor_app_stub.py \
       --period 0.5 --max 4 --bytes 5 --log-file /tmp/stub_vs_real_daemon.log
   ```

## State at end of session

| Thing | Where |
|---|---|
| Logger | `vehicle-logger.service` was `active` and writing CSVs. Stopped before this engine run? Confirm tomorrow. |
| Engine process | Stopped (Ctrl+C, clean shutdown via drain) |
| `live.jsonl` events file | `/home/sgbir/sensor_data/vehicle_behavior/events/live.jsonl` — contains the 3 events emitted before the daemon dropped them |
| Their daemon | Still running on the Pi as of 15:55 (we didn't stop it; they presumably manage it) |
| Stale files | `/tmp/engine.log`, `/tmp/engine.pid` — from the earlier file-transport run; harmless |
| Reference docs | [pipeline_overview.md](./pipeline_overview.md), [running_the_engine.md](./running_the_engine.md), [pi_deploy_cheatsheet.md](./pi_deploy_cheatsheet.md) |

## Other detail worth keeping

- **Their endpoint_id is `0x53433242`** (ASCII `"SC2B"` — guessing
  SkyBounce Comms? doesn't matter, just an identifier). Ours we sent
  was `0x53415050` (`"SAPP"`). Both 32-bit, both arbitrary per the
  spec, just useful so each side knows who it's talking to in logs.
- **The link_state transition order matters.** We saw
  `UNKNOWN → ACQUIRING` and that's it. The full sequence per spec is
  `DOWN ↔ ACQUIRING → UP` (and `DEGRADED` is reserved but not emitted
  in v1.0). If they never report `UP`, hypothesis 1 above gains
  weight.
