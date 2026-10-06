# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""Regression tests for the shared E2E listener-port lease protocol."""

from __future__ import annotations

import errno
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import pytest
from conftest import (
    AntflyServer,
    PublicAntflyServer,
    StandaloneAntflyServer,
    StatefulAntflyServer,
    _standalone_stateful_command,
)
from port_reservations import LoopbackPortReservations, find_free_port
from test_auth import StandaloneAuthServer
from test_standalone import EmbeddedInferenceStandaloneServer
from test_standby import HAStandaloneNode


def test_loopback_port_reservations_hold_ports_until_handoff():
    with LoopbackPortReservations() as reservations:
        explicit_port = find_free_port()
        assert reservations.reserve(explicit_port) == explicit_port
        ports = [explicit_port, *reservations.reserve_many(16)]

        assert len(set(ports)) == len(ports)
        for port in ports:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as contender:
                with pytest.raises(OSError):
                    contender.bind(("127.0.0.1", port))

        assert all(find_free_port() not in ports for _ in range(64))

        released_port = ports.pop()
        reservations.release_if_reserved(released_port)
        reservations.release_if_reserved(released_port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as contender:
            contender.bind(("127.0.0.1", released_port))


def test_reserve_requested_only_falls_back_for_address_in_use():
    class StubReservations(LoopbackPortReservations):
        def __init__(self, error: OSError) -> None:
            self.error = error
            self.calls: list[int] = []

        def reserve(self, port: int = 0) -> int:
            self.calls.append(port)
            if port:
                raise self.error
            return 41000

    address_in_use = StubReservations(OSError(errno.EADDRINUSE, "already in use"))
    assert address_in_use.reserve_requested(40000) == 41000
    assert address_in_use.calls == [40000, 0]

    permission_denied = StubReservations(OSError(errno.EACCES, "permission denied"))
    with pytest.raises(OSError) as exc_info:
        permission_denied.reserve_requested(40000)
    assert exc_info.value.errno == errno.EACCES
    assert permission_denied.calls == [40000]


def test_reserve_excluding_releases_collisions_and_is_bounded():
    class StubReservations(LoopbackPortReservations):
        def __init__(self, ports: tuple[int, ...]) -> None:
            self.ports = iter(ports)
            self.released: list[int] = []

        def reserve(self, port: int = 0) -> int:
            assert port == 0
            return next(self.ports)

        def _discard_unused(self, port: int) -> None:
            self.released.append(port)

    reservations = StubReservations((40001, 40002, 41000))
    assert reservations.reserve_excluding({40001, 40002}, attempts=3) == 41000
    assert reservations.released == [40001, 40002]

    exhausted = StubReservations((40001, 40002))
    with pytest.raises(RuntimeError, match="outside the excluded set"):
        exhausted.reserve_excluding({40001, 40002}, attempts=2)
    assert exhausted.released == [40001, 40002]


@pytest.mark.parametrize("exclude_candidate", [False, True])
def test_random_allocation_preserves_handed_off_port_lease(
    monkeypatch, exclude_candidate
):
    import port_reservations

    lower, _ = port_reservations._listener_range()
    with LoopbackPortReservations() as candidates:
        advertised, rejected, available = candidates.reserve_many(3)
    with LoopbackPortReservations() as reservations:
        reservations.reserve(advertised)
        reservations.handoff_to((advertised,), lambda: None)
        choices = iter(
            [advertised, rejected, available]
            if exclude_candidate
            else [advertised, available]
        )
        monkeypatch.setattr(
            port_reservations.secrets, "randbelow", lambda _limit: next(choices) - lower
        )
        if exclude_candidate:
            allocated = reservations.reserve_excluding({advertised, rejected})
        else:
            allocated = reservations.reserve()
        assert allocated == available
        with LoopbackPortReservations() as other:
            with pytest.raises(OSError) as exc_info:
                other.reserve(advertised)
            assert exc_info.value.errno == errno.EADDRINUSE
            if exclude_candidate:
                assert other.reserve(rejected) == rejected
        # Explicit fixed-port reacquisition remains available for restart.
        reservations.ensure_reserved(advertised)


def test_random_allocation_exhaustion_preserves_handed_off_port_lease(monkeypatch):
    import port_reservations

    lower, _ = port_reservations._listener_range()
    with LoopbackPortReservations() as reservations:
        advertised = reservations.reserve()
        reservations.handoff_to((advertised,), lambda: None)
        monkeypatch.setattr(
            port_reservations.secrets, "randbelow", lambda _limit: advertised - lower
        )
        with pytest.raises(OSError) as exc_info:
            reservations.reserve()
        assert exc_info.value.errno == errno.EADDRINUSE
        with LoopbackPortReservations() as other:
            with pytest.raises(OSError) as exc_info:
                other.reserve(advertised)
            assert exc_info.value.errno == errno.EADDRINUSE


def test_ensure_reserved_rolls_back_partial_reacquisition():
    with LoopbackPortReservations() as reservations:
        first_port, second_port = reservations.reserve_many(2)
        reservations.release(first_port, second_port)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as owner:
            owner.bind(("127.0.0.1", second_port))
            with pytest.raises(OSError) as exc_info:
                reservations.ensure_reserved(first_port, second_port)
            assert exc_info.value.errno == errno.EADDRINUSE

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as contender:
                contender.bind(("127.0.0.1", first_port))

        reservations.ensure_reserved(first_port, second_port)
        for port in (first_port, second_port):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as contender:
                with pytest.raises(OSError):
                    contender.bind(("127.0.0.1", port))


def test_handoff_restores_leases_when_process_spawn_fails():
    with LoopbackPortReservations() as reservations:
        port = reservations.reserve()

        def fail_to_spawn() -> None:
            raise OSError(errno.ENOEXEC, "cannot execute")

        with pytest.raises(OSError) as exc_info:
            reservations.handoff_to((port,), fail_to_spawn)
        assert exc_info.value.errno == errno.ENOEXEC

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as contender:
            with pytest.raises(OSError):
                contender.bind(("127.0.0.1", port))


@pytest.mark.parametrize(
    "server_type",
    (
        AntflyServer,
        PublicAntflyServer,
        StandaloneAntflyServer,
        StandaloneAuthServer,
    ),
)
def test_single_process_servers_release_requested_port_when_spawn_fails(
    monkeypatch: pytest.MonkeyPatch,
    server_type: type,
):
    requested_port = find_free_port()

    def fail_to_spawn(*args: Any, **kwargs: Any) -> subprocess.Popen[str]:
        raise OSError(errno.ENOEXEC, "cannot execute")

    monkeypatch.setattr(subprocess, "Popen", fail_to_spawn)
    with pytest.raises(OSError) as exc_info:
        server_type("/missing-antfly", "127.0.0.1", requested_port)
    assert exc_info.value.errno == errno.ENOEXEC

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as contender:
        contender.bind(("127.0.0.1", requested_port))


@pytest.mark.parametrize(
    ("server_type", "pgwire"),
    (
        (PublicAntflyServer, False),
        (StandaloneAntflyServer, False),
        (StandaloneAntflyServer, True),
    ),
)
def test_single_process_pause_retains_listener_port(server_type: type, pgwire: bool):
    server = object.__new__(server_type)
    server.proc = None
    server.port_reservations = LoopbackPortReservations()
    server.port = server.port_reservations.reserve()
    if server_type is StandaloneAntflyServer:
        server.pgwire_port = server.port_reservations.reserve() if pgwire else None
    ports = (server.port, server.pgwire_port) if pgwire else (server.port,)
    for port in ports:
        server.port_reservations.release(port)

    try:
        server.pause()
        for port in ports:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as contender:
                with pytest.raises(OSError):
                    contender.bind(("127.0.0.1", port))
    finally:
        server.port_reservations.close()


def test_stateful_server_releases_requested_port_when_spawn_fails(
    monkeypatch: pytest.MonkeyPatch,
):
    requested_port = find_free_port()

    def fail_to_spawn(*args: Any, **kwargs: Any) -> subprocess.Popen[str]:
        raise OSError(errno.ENOEXEC, "cannot execute")

    monkeypatch.setattr(subprocess, "Popen", fail_to_spawn)
    with pytest.raises(OSError) as exc_info:
        StatefulAntflyServer("/missing-antfly", "127.0.0.1", requested_port)
    assert exc_info.value.errno == errno.ENOEXEC

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as contender:
        contender.bind(("127.0.0.1", requested_port))


def test_embedded_inference_server_releases_listener_ports_when_spawn_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    listener_ports: tuple[int, ...] = ()
    reserve_many = LoopbackPortReservations.reserve_many

    def record_listener_ports(
        reservations: LoopbackPortReservations,
        count: int,
    ) -> tuple[int, ...]:
        nonlocal listener_ports
        listener_ports = reserve_many(reservations, count)
        return listener_ports

    def fail_to_spawn(*args: Any, **kwargs: Any) -> subprocess.Popen[str]:
        raise OSError(errno.ENOEXEC, "cannot execute")

    monkeypatch.setattr(LoopbackPortReservations, "reserve_many", record_listener_ports)
    monkeypatch.setattr(subprocess, "Popen", fail_to_spawn)
    with pytest.raises(OSError) as exc_info:
        EmbeddedInferenceStandaloneServer("/missing-antfly", tmp_path, "test-model")
    assert exc_info.value.errno == errno.ENOEXEC
    assert len(listener_ports) == 2

    for port in listener_ports:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as contender:
            contender.bind(("127.0.0.1", port))


def test_standalone_fixture_command_disables_unused_health_listener(tmp_path: Path):
    command = _standalone_stateful_command(
        "/missing-antfly",
        host="127.0.0.1",
        port=40000,
        root=tmp_path,
    )

    health_flag = command.index("--health")
    assert command[health_flag + 1] == "false"


def test_ha_standalone_node_reenables_its_reserved_health_listener(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    command: list[str] = []

    class ExitedProcess:
        def poll(self) -> int:
            return 0

    def capture_command(args: list[str], **kwargs: Any) -> ExitedProcess:
        command.extend(args)
        return ExitedProcess()

    monkeypatch.setattr(subprocess, "Popen", capture_command)
    monkeypatch.setattr("test_standby.wait_for_server", lambda *args, **kwargs: True)
    node = HAStandaloneNode(
        binary="/missing-antfly",
        root=tmp_path,
        role="primary",
        node_id="primary-a",
        cluster_id=100,
        timeline_id=1,
        epoch=1,
    )
    try:
        node.start()

        health_flags = [index for index, arg in enumerate(command) if arg == "--health"]
        assert command[health_flags[-1] + 1] == "true"
        health_port_flag = command.index("--health-port")
        assert command[health_port_flag + 1] == str(node.health_port)
    finally:
        node.close()


@pytest.mark.parametrize(
    "server_type",
    (
        AntflyServer,
        PublicAntflyServer,
        StandaloneAntflyServer,
        StandaloneAuthServer,
        StatefulAntflyServer,
    ),
)
def test_single_process_servers_release_port_when_setup_fails(
    monkeypatch: pytest.MonkeyPatch,
    server_type: type,
):
    listener_ports: list[int] = []
    reserve_requested = LoopbackPortReservations.reserve_requested

    def record_listener_port(reservations: LoopbackPortReservations, port: int) -> int:
        reserved_port = reserve_requested(reservations, port)
        listener_ports.append(reserved_port)
        return reserved_port

    def fail_tempdir(*args: Any, **kwargs: Any):
        raise OSError(errno.ENOSPC, "cannot create temporary directory")

    monkeypatch.setattr(
        LoopbackPortReservations, "reserve_requested", record_listener_port
    )
    monkeypatch.setattr(tempfile, "TemporaryDirectory", fail_tempdir)
    with pytest.raises(OSError) as exc_info:
        server_type("/missing-antfly", "127.0.0.1", 0)
    assert exc_info.value.errno == errno.ENOSPC
    assert len(listener_ports) == 1

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as contender:
        contender.bind(("127.0.0.1", listener_ports[0]))


def test_embedded_inference_server_releases_ports_when_setup_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    listener_ports: tuple[int, ...] = ()
    reserve_many = LoopbackPortReservations.reserve_many

    def record_listener_ports(
        reservations: LoopbackPortReservations,
        count: int,
    ) -> tuple[int, ...]:
        nonlocal listener_ports
        listener_ports = reserve_many(reservations, count)
        return listener_ports

    def fail_tempdir(*args: Any, **kwargs: Any):
        raise OSError(errno.ENOSPC, "cannot create temporary directory")

    monkeypatch.setattr(LoopbackPortReservations, "reserve_many", record_listener_ports)
    monkeypatch.setattr(tempfile, "TemporaryDirectory", fail_tempdir)
    with pytest.raises(OSError) as exc_info:
        EmbeddedInferenceStandaloneServer("/missing-antfly", tmp_path, "test-model")
    assert exc_info.value.errno == errno.ENOSPC
    assert len(listener_ports) == 2

    for port in listener_ports:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as contender:
            contender.bind(("127.0.0.1", port))


def test_port_lease_survives_unbound_child_handoff_and_process_boundary():
    with LoopbackPortReservations() as reservations:
        port = reservations.reserve()
        child = reservations.handoff_to(
            (port,),
            lambda: subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(30)"]
            ),
        )
        try:
            # The child has not bound yet. A kernel-only reservation would let
            # another fixture steal this listener during its startup work.
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as unbound:
                unbound.bind(("127.0.0.1", port))
            code = """
import errno, sys
from port_reservations import LoopbackPortReservations
with LoopbackPortReservations() as other:
    try:
        other.reserve(int(sys.argv[1]))
    except OSError as error:
        assert error.errno == errno.EADDRINUSE
    else:
        raise AssertionError("stole another fixture's handed-off port")
"""
            result = subprocess.run(
                [sys.executable, "-c", code, str(port)],
                cwd=Path(__file__).parent,
                capture_output=True,
                text=True,
                timeout=5,
            )
            assert result.returncode == 0, result.stdout + result.stderr
        finally:
            child.terminate()
            child.wait(timeout=5)
    with LoopbackPortReservations() as other:
        assert other.reserve(port) == port


def test_listener_ports_do_not_use_linux_client_ephemeral_range():
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux port range is supplied by procfs")
    lower, upper = map(
        int, Path("/proc/sys/net/ipv4/ip_local_port_range").read_text().split()
    )
    with LoopbackPortReservations() as reservations:
        assert all(not lower <= port <= upper for port in reservations.reserve_many(32))


@pytest.mark.parametrize(
    "client_range, expected",
    [("32768 60999", (10000, 30000)), ("10000 20000", (20001, 65536))],
)
def test_listener_range_excludes_configured_client_ports(
    monkeypatch, client_range, expected
):
    import port_reservations

    monkeypatch.setattr(port_reservations.sys, "platform", "linux")
    monkeypatch.setattr(port_reservations.Path, "read_text", lambda _self: client_range)
    assert port_reservations._listener_range.__wrapped__() == expected


def test_socket_allocation_failure_does_not_leak_port_lease(monkeypatch):
    import port_reservations

    with LoopbackPortReservations() as initial:
        port = initial.reserve()
    original_socket = port_reservations.socket.socket

    def fail(*args, **kwargs):
        raise OSError(errno.EMFILE, "descriptor limit")

    with LoopbackPortReservations() as failed:
        with monkeypatch.context() as patch:
            patch.setattr(port_reservations.socket, "socket", fail)
            with pytest.raises(OSError, match="descriptor limit"):
                failed.reserve(port)
        assert port_reservations.socket.socket is original_socket
        with LoopbackPortReservations() as next_owner:
            assert next_owner.reserve(port) == port
