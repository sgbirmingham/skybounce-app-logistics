# IPC v1.0 Conformance Audit — skybounce-app-logistics

Audit target: `skybounce-app-logistics` (this repo).
Reference: SkyBounce Sensor IPC Protocol v1.0 + companion artifacts in
`../skybounce_IPC_python/{docs,scripts}/`.
Read-only audit; no code modified. Audit date: 2026-05-26.

## Scope note: the ownership boundary

This repo does not pack any IPC bytes directly. Its IPC footprint is one
class, `IpcTransport` (`src/skybounce_app_logistics/transport.py`, lines
111–184), which delegates **all** wire concerns to the IPC reference
library (`skybounce_client.SkyBounceClient`, `skybounce_ipc.*`,
`sb_telemetry_payload.*`). The audit-target's only direct contribution
to the wire is the **6-byte SB45 payload** sitting inside TELEMETRY
frames. Everything else (frame headers, HELLO/HELLO_ACK, PING/PONG,
TELEMETRY framing, dispatch) is the library's responsibility.

Conformance of the audit-target therefore reduces to three questions:
(a) does it invoke the library correctly, (b) does it register the
callbacks the library exposes, and (c) is the SB45 payload it produces
well-formed.

**Two material findings before the section-by-section results:**

- **`IpcTransport` is never instantiated anywhere in this repo.** Not in
  `scripts/replay.py`, not in `scripts/live.py`, not in any test. Both
  CLIs hardcode `FileTransport` (`replay.py:53`, `live.py:147`). No
  `--transport` flag exists despite the README claiming `--transport file`
  is the default. `IpcTransport` is dead code until a caller is added.
- **Reference-stub ambiguity (flag, do not resolve):** `sensor_app_stub.py`
  imports `skybounce.sensor_ipc.endpoint`, `.transport`, `.wire`. Those
  module paths do not exist in `../skybounce_IPC_python/` on disk; the
  repo has flat `skybounce_ipc.py` / `skybounce_client.py`. The stub
  appears to target a future package layout. The state-machine comparison
  in Section 2 is therefore based on the stub's documented intent and
  callback contract, not on executing it.

---

## Section 1 — Wire-format inventory (15 goldens)

Status legend:
- **COVERED** — audit-target's IpcTransport, when constructed, invokes a
  code path that produces these bytes (via the library). The library is
  the goldens reference; bytes match by construction.
- **PARTIAL** — code path exists but a handler/callback is missing, or
  only one direction is wired.
- **ABSENT** — no code path invokes this; or it is unreachable because
  IpcTransport is never instantiated (see scope note above).

If `IpcTransport` were instantiated and wired into a CLI, the per-entry
status would be as shown in the "Status (wired)" column. Until then
every send-side row is also "ABSENT at runtime" because no caller exists.

| # | Golden                                          | Direction (sensor side) | Status (wired) | Owner / pointer                                                                  |
|---|-------------------------------------------------|-------------------------|----------------|---------------------------------------------------------------------------------|
| 1 | `header_telemetry_basic`                        | TX                      | COVERED        | Library: `skybounce_ipc.make_frame` via `SkyBounceClient._writer_loop` (writer pulls from `_outbound`, builds header). Audit-target produces nothing here. |
| 2 | `header_ping_with_full_ts`                      | TX                      | COVERED        | Library: `SkyBounceClient._keepalive_loop` enqueues `PING` with library-assigned token. Pure library; audit-target uninvolved. |
| 3 | `hello_sensor_app`                              | TX                      | COVERED        | Library: `SkyBounceClient._do_handshake` (`skybounce_client.py:341`). Triggered by `IpcTransport.__init__` calling `self._client.start()` (`transport.py:145`). |
| 4 | `hello_skybounce`                               | RX                      | N/A            | Peer-side payload; sensor only consumes. `SkyBounceClient._do_handshake` reads and validates it. Audit-target has no visibility. |
| 5 | `telemetry_payload_basic`                       | TX                      | PARTIAL        | TELEMETRY framing (telemetry_id, priority, data_len, header) by library `Telemetry.pack` (`skybounce_ipc.py:173`). **Opaque `data` field is the audit-target's contribution**: `IpcTransport.emit` calls `pack_sb45(sb45_event)` (`transport.py:170`) then `submit_telemetry(packed.bytes_payload, …)` (`transport.py:175`). Golden's `data=DE AD BE EF` (4 bytes) would never come from this repo — SB45 is always 6 bytes. The mechanics are conformant; the example value is not what this repo would emit. |
| 6 | `telemetry_ack_queued`                          | RX                      | PARTIAL        | Library parses and DEBUG-logs at `skybounce_client.py:467`. Audit-target **never registers** a `telemetry_ack_handler` (no call to `set_telemetry_ack_handler` anywhere). The ACK is consumed silently by the library. App layer has no visibility into whether telemetry was queued. |
| 7 | `telemetry_ack_dropped_invalid_with_reason`     | RX                      | PARTIAL        | Same as #6. Drops with `reason_code` would be DEBUG-logged inside the library and lost. **No reaction at the app layer.** |
| 8 | `cmd_payload_with_data`                         | RX                      | ABSENT         | Library dispatches to `_cmd_handler` at `skybounce_client.py:533`. Audit-target **never calls** `set_cmd_handler`, so the library's `_default_cmd_handler` returns `ERR_UNKNOWN_CODE` for every CMD (`skybounce_client.py:123`). |
| 9 | `cmd_payload_empty_data`                        | RX                      | ABSENT         | Same as #8. |
| 10 | `cmd_ack_ok`                                   | TX                      | ABSENT (unreachable) | Library `CmdAck.pack` (`skybounce_ipc.py:268`) would emit this — but it is reached only via the registered cmd handler. With no handler registered, `OK` is never returned; the library always sends `ERR_UNKNOWN_CODE` ACKs instead. |
| 11 | `cmd_ack_ok_with_data`                         | TX                      | ABSENT (unreachable) | Same as #10, plus interacts with v1.0 known-limitation #3 (return data dropped on SkyBounce side). |
| 12 | `status_link_up_queue_3`                       | RX                      | PARTIAL        | Library DEBUG-logs at `skybounce_client.py:494`. Audit-target **never registers** a `status_handler`. App layer is blind to link state transitions. |
| 13 | `status_link_down_queue_0`                     | RX                      | PARTIAL        | Same as #12. Link going DOWN is not surfaced to the engine, the transport, or any caller. |
| 14 | `ping_pong_token_0x1234`                       | TX + RX                 | COVERED        | Entirely library: send-side in `_keepalive_loop`, RX-pong handling at `skybounce_client.py:508`, RX-ping → enqueue PONG at `skybounce_client.py:506`. Audit-target uninvolved by design. |
| 15 | `full_frame_telemetry_basic`                   | TX                      | PARTIAL        | Composition is library work. The 6-byte SB45 sits inside; see notes on #5. |

**Summary:** of the 15 goldens, **0 are produced or consumed by audit-target
code directly**; the relevant question is which paths the audit-target
*invokes*. Of the send-side paths, all are COVERED-via-library *once
`IpcTransport` is wired into a CLI*. Of the receive-side paths,
**4 (TELEMETRY_ACK ×2, STATUS ×2) are PARTIAL** — frames are received
and parsed by the library but never surface to the app, because no
handler is registered. **2 (CMD ×2)** are ABSENT — no handler means
every CMD is rejected with `ERR_UNKNOWN_CODE`.

---

## Section 2 — State-machine comparison vs `sensor_app_stub.py`

The stub (`../skybounce_IPC_python/scripts/sensor_app_stub.py`) is the
minimal sensor-side reference. Both it and this repo are sensor-side
peers — they connect to a SkyBounce process and run identical state
machines. The comparison below maps each stub behavior to the
audit-target's equivalent.

**Caveat:** the stub imports from a `skybounce.sensor_ipc.*` package
that does not exist on disk in `../skybounce_IPC_python/`. The
comparison treats the stub as a behavioral spec, not as code I have
verified runs against the present IPC reference.

| Stub behavior                          | Stub mechanism                                            | Audit-target equivalent                                                                 | Status   |
|----------------------------------------|----------------------------------------------------------|----------------------------------------------------------------------------------------|----------|
| Connect to Unix socket                 | `Endpoint(...).connect(sock_path)` (`stub:156`)           | `SkyBounceClient(..., socket_path).start()` via `IpcTransport.__init__` (`transport.py:144–145`); library handles `socket.connect` in `_connect` (`skybounce_client.py:323`) | ✓ COVERED |
| Send HELLO + read peer HELLO + HELLO_ACK | Inside `Endpoint.connect` (intent, not visible)         | `SkyBounceClient._do_handshake` (`skybounce_client.py:341–391`) — full sequence per spec §10 | ✓ COVERED |
| Reconnect loop with backoff            | Outer `while not stop.is_set()` (`stub:153–174`); 1s sleep on failure | `SkyBounceClient._reader_loop` (`skybounce_client.py:424–463`) — exponential backoff with jitter, max 30s | ✓ COVERED, better than stub |
| Emit TELEMETRY on cadence              | `endpoint.send_telemetry(...)` in `run_session` (`stub:99`) | `IpcTransport.emit(event)` → `submit_telemetry(packed.bytes_payload, priority=ipc_prio)` (`transport.py:175`). Cadence is driven by `StreamingEngine.consume()`, not a timer. | ✓ COVERED |
| Handle CMD → CMD_ACK                   | `on_cmd` callback acks every CMD with `CmdResult.OK` (`stub:65–72`) | **No `set_cmd_handler` call.** Library default handler returns `ERR_UNKNOWN_CODE` for every CMD. | ✗ ABSENT |
| Observe TELEMETRY_ACK                  | `on_telemetry_ack` callback logs disposition (`stub:74–77`) | **No `set_telemetry_ack_handler` call.** App layer cannot count QUEUED/DELIVERED/DROPPED or react to drops. | ✗ ABSENT |
| Observe STATUS                         | `on_status` callback logs link-state transitions (`stub:79–83`) | **No `set_status_handler` call.** App layer cannot tell when the radio is DOWN. | ✗ ABSENT |
| PING/PONG keepalive                    | Inside `EndpointWorker` (intent)                          | `SkyBounceClient._keepalive_loop` (`skybounce_client.py:589–625`) — full 3-strike timeout per spec §7.8 | ✓ COVERED, library |
| Detect disconnect                      | `on_disconnect` callback (`stub:90`)                      | Library closes socket on framing/socket error (`skybounce_client.py:463`); audit-target gets no callback. `IpcTransport` has no way to react to disconnect mid-session. | ✗ ABSENT (no hook) |
| Clean shutdown                         | `ep.close()` in `finally` (`stub:171`)                    | `IpcTransport.close()` → `self._client.stop()` (`transport.py:180`). **But:** since no CLI constructs `IpcTransport`, this path is never reached in practice. | ✓ COVERED in code |

**Bottom line:** the audit-target's IpcTransport implements the
foundational lifecycle (connect, handshake, reconnect, send, shutdown)
correctly via the library, but **none of the three receive-side
callbacks (CMD, TELEMETRY_ACK, STATUS) are registered**, leaving the
app completely deaf to the SkyBounce side except for whatever frames
it sends.

---

## Section 3 — SB45 payload mapping

`IpcTransport.emit` constructs an `SB45Event` from an `Event`
(`transport.py:156–169`) and calls `pack_sb45`. The encoder declares
ten 45-bit fields (`sb_telemetry_payload.py:67–96, 220–232`). Each row
below: SB45 field (code name) → source on `Event` →
transformation → gap.

**Documentation discrepancy to flag:** the SB45 companion doc
(`sb45_payload_companion_doc.md` field table) labels one field
`routing_tag` with observed value `DIRECT`. The encoder calls this
field `location_code` and its values are `NO_LOCATION` /
`ESTIMATED_FROM_NEAREST_FIX` / `DIRECT_EVENT_LOCATION`. These appear to
be the **same** field with two different names in two different
documents. I am not resolving this — flagging for the doc owner.

| SB45 field (encoder)            | Event source (`transport.py:35–59`)         | Transformation                                                                                                            | Gap / risk                                                                                                                   |
|---------------------------------|---------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `event_code_4b`                 | `Event.event_type` (str)                    | `EVENT_TYPE_CODES.get(event_type, 15)`                                                                                    | **Real gap.** Engine emits three event types **not in** `EVENT_TYPE_CODES` and so silently encodes them as 15 (reserved): `trip_pause_or_parked` (`engine.py:394`), `long_stop` (`engine.py:403`), `state_gps_degraded` (`engine.py:409`). |
| `priority_code_2b`              | `Event.priority` (str)                      | `PRIORITY_CODES.get(priority, 0)` — keys are `LOG`/`P1`/`P2`                                                              | OK. `classify_event_policy` returns matching strings.                                                                        |
| `location_code_2b`              | derived in `transport.py:150–154`           | `"DIRECT_EVENT_LOCATION"` if both `gps_lat` and `gps_lon` present, else `"NO_LOCATION"`                                  | Never uses `ESTIMATED_FROM_NEAREST_FIX` (encoder value 1). Acceptable simplification; flag if the analyzer-oracle uses it.   |
| `severity_bin_3b`               | `Event.score` (float 0..1)                  | `clip_int(score * 7, 0, 7)`                                                                                                | OK assuming `score` is in [0, 1]; engine produces values in that range via `classify_event_policy`.                          |
| `speed_mph_bin_7b`              | `Event.speed_mph` (computed `speed_m_s * 2.23694`, `engine.py:568`) | `clip_int(speed_mph, 0, 127)`                                                                  | Silent clip above 127 mph. Implausible for logistics but not impossible (downhill, faulty GPS).                              |
| `peak_decel_bin_5b`             | `Event.decel_m_s2`                          | `clip_int(decel_m_s2, 0, 31)`                                                                                              | Only deceleration is encoded; `Event.accel_m_s2` has **no SB45 destination** (carried in FileTransport JSON only).            |
| `gps_conf_bin_3b`               | `Event.gps_confidence`                      | `clip_int(gps_confidence * 7, 0, 7)`                                                                                       | OK; matches `gps_confidence_smoothed` range.                                                                                  |
| `elapsed_min_bin_8b`            | `Event.elapsed_s`                           | `clip_int(elapsed_s / 60, 0, 255)`                                                                                         | **Real gap for long trips.** 255 minutes ≈ 4h15m; subsequent events wrap silently to 255. Long-haul logistics > 4h is plausible. |
| `lat_offset_bin_5b`             | `Event.gps_lat`, `Event.anchor_lat`         | `encode_latlon_offsets(lat, lon, anchor_lat, anchor_lon)` — ±0.020° box around anchor, 31 levels                          | Out-of-box silently encoded as 0 with `loc_note="LOCATION_OFFSET_CLIPPED"` — but the note is discarded by `IpcTransport.emit` (lives only on `SB45Packed`, never logged or surfaced). ±0.020° ≈ ±2.2 km box; any trip leaving that box loses position resolution silently. |
| `lon_offset_bin_6b`             | `Event.gps_lon`, `Event.anchor_lon`         | as above, then `lon_q * 2` to fit in 6 bits                                                                                | Same gap as latitude.                                                                                                         |

**Event fields with no SB45 destination** (carried only in FileTransport
JSON, dropped on IPC): `event_class`, `packet_rank`, `policy_reason`,
`detail`, `accel_m_s2`, `lin_accel_g`, `jerk_g_s`, `analyzer_state`,
`ts_epoch_s` (only relative `elapsed_s` survives).

---

## Section 4 — Known-limitations risks

The four v1.0 limitations from `sensor_ipc_README.md`:

| # | Limitation                                                                                          | Audit-target risk                                                                                                                                                                              |
|---|-----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 | `DELIVERED` is a hand-off confirmation, not an observed L1 ACK from the base station.              | **No violation, but blind.** Audit-target has no `telemetry_ack_handler` registered, so it cannot misread `DELIVERED` as L1-confirmed — it cannot read it at all. Acceptable for v0.1 if the architecture truly does not care about delivery semantics; surface as a finding if it should. |
| 2 | `STATUS link_state` never reports `DEGRADED` in v1.0.                                              | **No violation.** Audit-target has no `status_handler`. It cannot key on `DEGRADED` because it never sees STATUS at all. (Note: the `GPS_DEGRADED` string in `engine.py:407,418` is the streaming engine's *movement state*, unrelated to `LinkState.DEGRADED`.) |
| 3 | `CMD_ACK` return data on `OK_WITH_DATA` is dropped on the SkyBounce side.                          | **No violation by construction.** Audit-target has no `cmd_handler`; it never returns `OK_WITH_DATA`; the library's default handler returns `ERR_UNKNOWN_CODE` with empty data. The dropped-return-data path is unreachable. |
| 4 | No multi-frame fragmentation; one IPC TELEMETRY = one RF transmission.                              | **No violation.** SB45 is fixed at 6 bytes (`sb_telemetry_payload.py:60–62`), well under the 240-byte opaque-data limit (`skybounce_ipc.py:143`). Every TELEMETRY frame this repo would emit fits in one IPC frame and therefore one RF slot. |

No code path in this repo currently violates a v1.0 limitation. The
"risks" are gaps — limitation #1 in particular means the app cannot
know whether telemetry survived hand-off, which is operationally
significant for a logistics vertical that may want to react to drops.

---

## Section 5 — Concrete gap list (ordered by dependency)

Each item is an actionable change. Items are ordered so that earlier
items are prerequisites for later ones.

1. **Wire `IpcTransport` into the CLIs.** Add a `--transport {file,ipc}`
   flag (default `file`) to `scripts/replay.py` and `scripts/live.py`.
   On `--transport ipc`, accept `--socket` and `--endpoint-id` and
   construct `IpcTransport` instead of `FileTransport`. Until this lands,
   every other item below is dead code.
   - Files: `src/skybounce_app_logistics/scripts/replay.py`,
     `src/skybounce_app_logistics/scripts/live.py`.

2. **Register a CMD handler in `IpcTransport`.** Decide v0.1 CMD vocabulary
   (likely empty — sensor accepts no commands), and at minimum register a
   handler that returns `CmdResult.OK` for whatever the SkyBounce side
   may send (e.g. liveness pings, config no-ops). Without this, every
   CMD is rejected with `ERR_UNKNOWN_CODE`, which a future
   bench-validation run will surface as a divergence from
   `sensor_app_stub.py` behavior.
   - File: `src/skybounce_app_logistics/transport.py` (extend `IpcTransport`
     after line 147; call `self._client.set_cmd_handler(...)`).

3. **Register a TELEMETRY_ACK handler.** At minimum, log disposition and
   reason_code; ideally count by disposition so an operator can tell
   how many events were queued, delivered, or dropped, and why.
   Required to satisfy limitation-#1 visibility.
   - File: `src/skybounce_app_logistics/transport.py`.

4. **Register a STATUS handler.** Log link-state transitions
   (`DOWN` → `ACQUIRING` → `UP`) and the current queue_depth. Surface
   link DOWN to operational logging so a stuck radio is visible.
   - File: `src/skybounce_app_logistics/transport.py`.

5. **Fix the SB45 `event_code` gap.** Three event types emitted by
   `engine.py` are absent from `EVENT_TYPE_CODES` in `sb_telemetry_payload.py`
   and silently encode as 15:
   - `trip_pause_or_parked` (`engine.py:394`)
   - `long_stop` (`engine.py:403`)
   - `state_gps_degraded` (`engine.py:409`)

   Two options: (a) coordinate adding these codes to the IPC repo's
   `EVENT_TYPE_CODES` table (with version bump); (b) map them in
   `IpcTransport.emit` to the nearest existing code (e.g.
   `trip_pause_or_parked` → `trip_end_or_long_stop`,
   `long_stop` → `trip_end_or_long_stop`,
   `state_gps_degraded` → `gps_degraded_persistent`).

6. **Surface `LOCATION_OFFSET_CLIPPED` notes.** The encoder reports a
   `location_encoding_note` on every pack (`sb_telemetry_payload.py:260`)
   that is currently discarded by `IpcTransport.emit`. At minimum log it;
   ideally count clip events so a deployment outside the ±2.2 km anchor
   box is visible to operators.
   - File: `src/skybounce_app_logistics/transport.py`.

7. **Decide on `elapsed_min_bin` overflow policy.** Long-haul trips
   exceed 255 minutes. Either restart `session_start_ts` periodically
   (changes the meaning of `elapsed_s`), or accept the wrap and document
   it. Either way: make the choice explicit.

8. **Loopback test with `IpcTransport`.** Once items 1–6 are in,
   exercise `IpcTransport` against the IPC repo's loopback test
   (`../skybounce_IPC_python/tests/test_loopback.py`) or an equivalent
   in-process mock listener. Confirm: HELLO/HELLO_ACK completes, at
   least one TELEMETRY round-trips with a non-failure ACK disposition,
   one CMD round-trips with `OK`, one PING/PONG exchange completes.

9. **Goldens cross-check (advisory).** Optionally add a unit test that
   constructs the audit-target's `Event` for known-shape inputs, runs it
   through `pack_sb45`, and asserts the resulting bytes match the SB45
   demo payloads documented in `sb45_payload_companion_doc.md`
   (`04C878380C20` / `1755D1307422` / `1D5D5230C422`). This pins the
   field-mapping contract so a future change to `Event` or the encoder
   surfaces as a test failure rather than as a wire divergence at the
   bench. Note: there is a discrepancy between the doc's "canonical
   self-test vector" (`1755D1307420`) and the inputs in
   `sb_telemetry_payload.py:_self_test` (which appear to use the
   hard_brake demo values producing `1755D1307422`). Flag to the IPC
   repo owner; do not resolve here.

10. **Reference-stub layout ambiguity.** Confirm with the IPC repo owner
    whether `scripts/sensor_app_stub.py` is expected to run against the
    present flat-module layout (`skybounce_ipc.py` / `skybounce_client.py`)
    or whether a future `skybounce.sensor_ipc.*` package is pending.
    Until clarified, "passes the goldens and runs against the stub" (per
    `sensor_ipc_README.md` validation procedure) cannot be executed
    end-to-end.

---

## Appendix: artifacts read

- `../skybounce_IPC_python/docs/sensor_ipc_README.md`
- `../skybounce_IPC_python/docs/sensor_ipc_goldens.json` (15 entries)
- `../skybounce_IPC_python/docs/sb45_payload_companion_doc.md`
- `../skybounce_IPC_python/docs/sensor_ipc_HANDOFF.md`
- `../skybounce_IPC_python/scripts/sensor_app_stub.py`
- `../skybounce_IPC_python/skybounce_ipc.py`
- `../skybounce_IPC_python/skybounce_client.py`
- `../skybounce_IPC_python/sb_telemetry_payload.py`
- `src/skybounce_app_logistics/transport.py`
- `src/skybounce_app_logistics/engine.py`
- `src/skybounce_app_logistics/state.py`
- `src/skybounce_app_logistics/__init__.py`
- `src/skybounce_app_logistics/scripts/replay.py`
- `src/skybounce_app_logistics/scripts/live.py`
- `README.md` (audit-target)
- `pyproject.toml` (audit-target)

The PDF specification (`SkyBounce_Sensor_IPC_Protocol_v1.0.pdf`) was not
opened for this audit; the conformance points it carries are sufficiently
captured in `sensor_ipc_README.md`, `sensor_ipc_goldens.json`, and the
inline spec citations in `skybounce_client.py` / `skybounce_ipc.py`. If
a finding above depends on a spec clause not surfaced in those sources,
the PDF should be consulted before action.
