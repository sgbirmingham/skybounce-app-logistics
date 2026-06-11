"""
test_ipc_transport_loopback.py

End-to-end loopback test for IpcTransport against an in-process mock
SkyBounce listener.

Verifies the things sensor_app_stub.py verifies in the IPC reference
repo, plus the handler-registration layer that IpcTransport adds on
top of SkyBounceClient:

    1. HELLO / HELLO_ACK handshake completes (the client reaches STEADY).
    2. TELEMETRY frames sent by IpcTransport.emit carry a 6-byte SB45
       payload on the wire (mock side observes).
    3. TELEMETRY_ACK dispositions reach IpcTransport._on_telemetry_ack
       (ack_counts surfaces them at the app layer).
    4. CMD frames reach IpcTransport._on_cmd; CMD_ACK with OK comes back
       to the listener.
    5. STATUS frames reach IpcTransport._on_status (the transport's
       last-link-state shadow updates on transition).

PING/PONG keepalive is not exercised: the IPC library handles it
transparently and there is no app-layer hook. The IPC repo's own
tests/test_loopback.py covers it at the protocol level.

# -----------------------------------------------------------------------------
# Skip conditions
# -----------------------------------------------------------------------------

This test is integration-only and is skipped when:
  - The platform lacks AF_UNIX (some Windows Python builds, embedded
    distributions). The SkyBounce IPC stack is AF_UNIX-only, so a
    workstation without it cannot run the test in any form.
  - The skybounce-ipc-python package is not installed in the active
    venv. To enable, run:
        pip install -e ../skybounce-ipc-python

On a Pi, Linux, or Mac with the IPC repo installed editable, this runs
as a normal pytest test (a few hundred milliseconds end to end).
"""

from __future__ import annotations

import logging
import os
import socket
import sys
import tempfile
import threading
import time

import pytest


# -----------------------------------------------------------------------------
# Module-level skip conditions. Both run before any imports that would
# otherwise fail noisily on a workstation without AF_UNIX or without the
# IPC library installed.
# -----------------------------------------------------------------------------

if not hasattr(socket, "AF_UNIX"):
    pytest.skip(
        "AF_UNIX not available on this platform "
        "(the SkyBounce IPC stack is AF_UNIX-only)",
        allow_module_level=True,
    )

pytest.importorskip("skybounce_ipc")
pytest.importorskip("skybounce_client")
pytest.importorskip("sb_telemetry_payload")


from skybounce_ipc import (  # noqa: E402  (imports follow the importorskip gate)
    Cmd,
    CmdAck,
    Disposition,
    Hello,
    LinkState,
    MsgType,
    PingPong,
    Role,
    Status,
    Telemetry,
    TelemetryAck,
    make_frame,
    read_frame,
)

from skybounce_app_logistics.transport import Event, IpcTransport  # noqa: E402


log = logging.getLogger("test.ipc_loopback")


# -----------------------------------------------------------------------------
# Mock SkyBounce listener
# -----------------------------------------------------------------------------

class MockSkyBounce:
    """Minimal in-process SkyBounce-side peer for one client connection.

    Trimmed from the canonical MockSkyBounce in
    ../skybounce-ipc-python/tests/test_loopback.py; adds inject_cmd /
    inject_status APIs for driving the receive-side handlers under test.
    """

    def __init__(self, sock_path: str, endpoint_id: int = 0xCAFE0001) -> None:
        self.sock_path = sock_path
        self.endpoint_id = endpoint_id
        self.received_telemetry: list[Telemetry] = []
        self.received_cmd_acks: list[CmdAck] = []
        self.received_pongs: list[int] = []
        self.handshake_done = threading.Event()
        self._tx_seq = 0
        self._sock: socket.socket | None = None
        self._conn: socket.socket | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.buffer_full_count = 0
        self._buffer_full_remaining = 0
        self._silent = False

    # ---- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        if os.path.exists(self.sock_path):
            os.unlink(self.sock_path)
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.bind(self.sock_path)
        self._sock.listen(1)
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        conn = self._conn
        self._conn = None
        if conn is not None:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                conn.close()
            except OSError:
                pass
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        if os.path.exists(self.sock_path):
            try:
                os.unlink(self.sock_path)
            except OSError:
                pass

    # ---- send helpers --------------------------------------------------------

    def _next_seq(self) -> int:
        s = self._tx_seq
        self._tx_seq = (self._tx_seq + 1) & 0xFFFF
        return s

    def _send(self, msg_type: int, payload: bytes) -> None:
        conn = self._conn
        if conn is None:
            return
        frame = make_frame(msg_type, self._next_seq(), int(time.time() * 1e6), payload)
        try:
            conn.sendall(frame)
        except OSError as e:
            log.debug("send failed: %s", e)

    # ---- public injection API (driven from tests) ----------------------------

    def inject_status(self, link_state: LinkState, queue_depth: int = 0) -> None:
        self._send(
            MsgType.STATUS,
            Status(link_state=int(link_state), queue_depth=queue_depth).pack(),
        )

    def inject_cmd(self, cmd_id: int, cmd_code: int, data: bytes = b"") -> None:
        self._send(
            MsgType.CMD,
            Cmd(cmd_id=cmd_id, cmd_code=cmd_code, data=data).pack(),
        )

    def inject_ping(self, token: int) -> None:
        self._send(MsgType.PING, PingPong(token=token).pack())

    def set_buffer_full(self, count: int) -> None:
        """Make the next `count` TELEMETRY submissions respond with
        DROPPED_BUFFER_FULL instead of QUEUED+DELIVERED."""
        self._buffer_full_remaining = count

    def set_silent(self, on: bool = True) -> None:
        """Receive TELEMETRY but send NO ACK -- frames stay in-flight forever.
        Used to exercise the close()-drain exit on peer disconnect."""
        self._silent = on

    # ---- session loop --------------------------------------------------------

    def _serve(self) -> None:
        assert self._sock is not None
        self._sock.settimeout(5.0)
        try:
            conn, _ = self._sock.accept()
        except socket.timeout:
            log.warning("accept timed out; no client connected")
            return
        self._conn = conn

        # Handshake: receive HELLO, send HELLO, receive HELLO_ACK, send HELLO_ACK.
        try:
            hdr, payload = read_frame(conn)
            assert hdr.msg_type == MsgType.HELLO, (
                f"expected HELLO, got 0x{hdr.msg_type:02X}"
            )
            peer = Hello.unpack(payload)
            assert peer.role == Role.SENSOR_APP

            my_hello = Hello(
                role=Role.SKYBOUNCE,
                capabilities=0,
                endpoint_id=self.endpoint_id,
            ).pack()
            self._send(MsgType.HELLO, my_hello)

            hdr, payload = read_frame(conn)
            assert hdr.msg_type == MsgType.HELLO_ACK
            self._send(MsgType.HELLO_ACK, my_hello)
            self.handshake_done.set()
        except (AssertionError, ConnectionError, OSError) as e:
            log.error("handshake failed: %s", e)
            return

        # Initial STATUS UP so the client sees a link-state transition.
        self.inject_status(LinkState.UP, queue_depth=0)

        # Steady-state read loop.
        conn.settimeout(0.5)
        while not self._stop.is_set():
            try:
                hdr, payload = read_frame(conn)
            except socket.timeout:
                continue
            except (ConnectionError, OSError):
                return
            self._dispatch(hdr.msg_type, payload)

    def _dispatch(self, msg_type: int, payload: bytes) -> None:
        if msg_type == MsgType.TELEMETRY:
            tel = Telemetry.unpack(payload)
            self.received_telemetry.append(tel)
            if self._silent:
                return  # receive but never ACK -> frame stays in-flight
            if self._buffer_full_remaining > 0:
                self._buffer_full_remaining -= 1
                self.buffer_full_count += 1
                self._send(
                    MsgType.TELEMETRY_ACK,
                    TelemetryAck(
                        telemetry_id=tel.telemetry_id,
                        disposition=int(Disposition.DROPPED_BUFFER_FULL),
                        reason_code=0x0002,
                    ).pack(),
                )
                return
            for disp in (Disposition.QUEUED, Disposition.DELIVERED):
                self._send(
                    MsgType.TELEMETRY_ACK,
                    TelemetryAck(
                        telemetry_id=tel.telemetry_id,
                        disposition=int(disp),
                        reason_code=0,
                    ).pack(),
                )
                time.sleep(0.02)
        elif msg_type == MsgType.CMD_ACK:
            self.received_cmd_acks.append(CmdAck.unpack(payload))
        elif msg_type == MsgType.PONG:
            self.received_pongs.append(PingPong.unpack(payload).token)
        elif msg_type == MsgType.PING:
            # Client keepalive PING; reply with matching-token PONG.
            p = PingPong.unpack(payload)
            self._send(MsgType.PONG, PingPong(token=p.token).pack())


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _make_event(event_type: str = "hard_brake") -> Event:
    """Realistic Event modeled on the SB45 self-test inputs in
    sb_telemetry_payload.py (so the encoded bytes match a known-good
    canonical pattern)."""
    return Event(
        ts_epoch_s=1716700000.0,
        elapsed_s=842.0,
        event_type=event_type,
        score=0.72,
        priority="P2",
        event_class="impact",
        packet_rank=1,
        policy_reason="audit-loopback",
        detail="loopback test event",
        speed_m_s=25.9,
        speed_mph=58.0,
        decel_m_s2=4.1,
        accel_m_s2=0.0,
        lin_accel_g=0.5,
        jerk_g_s=1.4,
        gps_confidence=0.88,
        gps_lat=40.4406,
        gps_lon=-79.9959,
        anchor_lat=40.4400,
        anchor_lon=-79.9970,
        analyzer_state="MOVING",
    )


def _wait_until(predicate, timeout_s: float = 2.0, interval_s: float = 0.02) -> bool:
    """Poll `predicate` until True or timeout. Returns the final value."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return False


# -----------------------------------------------------------------------------
# Fixture
# -----------------------------------------------------------------------------

@pytest.fixture
def loopback():
    """Stand up MockSkyBounce + IpcTransport pointed at it. Teardown closes
    both and removes the socket file. Yields (transport, mock)."""
    sock_path = tempfile.mktemp(suffix="-skybounce-test.sock")
    mock = MockSkyBounce(sock_path)
    mock.start()
    transport = IpcTransport(endpoint_id=0x53415050, socket_path=sock_path)
    try:
        assert mock.handshake_done.wait(timeout=3.0), (
            "mock side did not complete handshake within 3s"
        )
        assert _wait_until(
            lambda: transport._client.is_steady, timeout_s=3.0
        ), "client did not reach STEADY within 3s"
        yield transport, mock
    finally:
        transport.close()
        mock.stop()


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------

def test_telemetry_round_trips_as_six_byte_sb45(loopback):
    """Emit one Event and confirm it arrives on the mock side as a 6-byte
    SB45 payload, and that the DELIVERED disposition is observed by the
    transport's TELEMETRY_ACK handler."""
    transport, mock = loopback
    transport.emit(_make_event())

    assert _wait_until(
        lambda: len(mock.received_telemetry) >= 1, timeout_s=2.0
    ), "mock did not receive a TELEMETRY frame"
    tel = mock.received_telemetry[0]
    assert len(tel.data) == 6, (
        f"SB45 payload must be 6 bytes; got {len(tel.data)} ({tel.data.hex()})"
    )

    assert _wait_until(
        lambda: int(Disposition.DELIVERED) in transport.ack_counts,
        timeout_s=2.0,
    ), (
        "_on_telemetry_ack did not observe DELIVERED; "
        f"ack_counts={transport.ack_counts}"
    )


def test_cmd_round_trips_with_ok(loopback):
    """Inject a CMD from the mock side; confirm IpcTransport's _on_cmd is
    invoked and an OK CMD_ACK comes back."""
    transport, mock = loopback
    initial = transport.cmds_received
    mock.inject_cmd(cmd_id=1, cmd_code=0x42, data=b"")

    assert _wait_until(
        lambda: transport.cmds_received > initial, timeout_s=2.0
    ), f"_on_cmd was not invoked (cmds_received={transport.cmds_received})"

    assert _wait_until(
        lambda: len(mock.received_cmd_acks) >= 1, timeout_s=2.0
    ), "mock did not receive a CMD_ACK"
    ack = mock.received_cmd_acks[-1]
    assert ack.cmd_id == 1
    assert ack.result == 0x00, (
        f"expected OK (0x00) per v0.1 placeholder handler; got 0x{ack.result:02X}"
    )


def test_status_link_state_transitions_surface(loopback):
    """Drive UP -> DOWN -> UP on the mock side; confirm the transport's
    _on_status handler tracks the transitions (via the private
    _last_link_state shadow)."""
    transport, mock = loopback

    # Fixture's initial inject was UP. Drive a DOWN, then back to UP.
    mock.inject_status(LinkState.DOWN)
    assert _wait_until(
        lambda: transport._last_link_state == int(LinkState.DOWN), timeout_s=2.0
    ), f"DOWN not observed; _last_link_state={transport._last_link_state}"

    mock.inject_status(LinkState.UP)
    assert _wait_until(
        lambda: transport._last_link_state == int(LinkState.UP), timeout_s=2.0
    ), f"UP not observed; _last_link_state={transport._last_link_state}"


def test_close_drains_pending_telemetry(loopback):
    """Burst-submit N events and immediately close the transport. Without
    the drain in close(), only a fraction of frames reach the wire because
    SkyBounceClient.stop() closes the socket before the writer queue is
    flushed. This is exactly the pattern replay scripts hit.

    Regression test for the close-race observed during 2026-05-27 Pi
    validation, where a 4-event burst landed only 1 frame on the mock
    side. With the drain, all N must arrive before close() returns.

    Each P2 event emits exactly one telemetry frame. The precise-position
    companion was removed 2026-06-10 (mu=1 frame/4min capacity), so N events
    produce N_FRAMES = N on the wire.
    """
    transport, mock = loopback
    N = 5
    # P2 event → one frame (no precise-position companion since 2026-06-10)
    N_FRAMES = N

    for _ in range(N):
        transport.emit(_make_event())

    # Close should block until every submission has a terminal disposition
    # (or the drain timeout fires). The fixture mock ACKs with DELIVERED on
    # a ~20ms cadence, so total drain wait should be well under a second.
    transport.close()

    assert len(mock.received_telemetry) == N_FRAMES, (
        f"expected {N_FRAMES} TELEMETRY frames on mock side after close; got "
        f"{len(mock.received_telemetry)} "
        f"(ids={[t.telemetry_id for t in mock.received_telemetry]})"
    )
    delivered = int(Disposition.DELIVERED)
    assert transport.ack_counts.get(delivered, 0) == N_FRAMES, (
        f"expected {N_FRAMES} DELIVERED ACKs accounted for in transport; "
        f"got ack_counts={transport.ack_counts}"
    )


def test_p2_resubmit_on_buffer_full(loopback):
    """A CRITICAL (P2) frame that gets DROPPED_BUFFER_FULL is automatically
    resubmitted by the transport's P2 resubmit thread. The mock rejects the
    first submission with BUFFER_FULL, then accepts the resubmit with
    QUEUED+DELIVERED. The event data and priority make it CRITICAL (P2), so
    the resubmit path fires.

    Validates: unbounded P2 resubmit (transport._resubmit_count > 0,
    the frame ultimately DELIVERED).
    """
    transport, mock = loopback
    # Use a fast resubmit backoff so the test doesn't wait the deployed
    # 15s first step. The resubmit loop reads this attribute each iteration,
    # so overriding it post-construction (before the resubmit fires) is safe.
    transport._P2_RESUBMIT_BACKOFF_SCHEDULE = (0.05,)
    # First P2 event submission → BUFFER_FULL; next submission (the
    # resubmit) → normal QUEUED+DELIVERED. Since 2026-06-10 a P2 event is a
    # single frame (no precise-position companion), so reject just one.
    mock.set_buffer_full(1)
    transport.emit(_make_event())  # P2 event → one frame

    # Wait for the resubmit thread to re-submit and the mock to DELIVER.
    assert _wait_until(
        lambda: transport._resubmit_count >= 1, timeout_s=5.0
    ), (
        f"expected >=1 resubmission; got {transport._resubmit_count} "
        f"(ack_counts={transport.ack_counts})"
    )
    # The original attempt and the resubmit each arrive on the mock side.
    # Original: 1 frame (rejected). Resubmit: 1 frame (accepted).
    assert len(mock.received_telemetry) >= 2, (
        f"expected >=2 TELEMETRY frames (1 rejected + 1 resubmitted); "
        f"got {len(mock.received_telemetry)}"
    )
    delivered = int(Disposition.DELIVERED)
    assert _wait_until(
        lambda: transport.ack_counts.get(delivered, 0) >= 1, timeout_s=3.0
    ), (
        f"resubmitted frame should reach DELIVERED; "
        f"ack_counts={transport.ack_counts}"
    )


def test_close_exits_on_peer_disconnect_without_hanging(loopback):
    """close()'s unlimited drain must NOT hang when the peer disconnects while a
    frame is still in-flight. The client drops frames composed while not STEADY,
    so a stranded frame can never reach a terminal ACK; close() must detect the
    disconnect, count it dropped, and return. Regression for the 2026-06-11
    mid-drain bridge teardown that wedged the sender until SIGKILL."""
    transport, mock = loopback
    mock.set_silent(True)              # receive but never ACK -> in-flight forever
    transport.emit(_make_event())      # P2 submitted, no terminal disposition
    assert _wait_until(
        lambda: len(mock.received_telemetry) >= 1, timeout_s=2.0
    ), "mock never received the frame"

    mock.stop()                        # peer disconnects mid-flight

    t0 = time.monotonic()
    transport.close()                  # unlimited drain (default); must return
    elapsed = time.monotonic() - t0
    assert elapsed < 8.0, f"close() hung {elapsed:.1f}s after peer disconnect"
    # the stranded frame was never DELIVERED -- it's counted dropped at close
    assert transport.ack_counts.get(int(Disposition.DELIVERED), 0) == 0
