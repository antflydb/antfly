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
from pathlib import Path
from typing import Any

import pytest

from conftest import (
    AntflyServer,
    PublicAntflyServer,
    StandaloneAntflyServer,
    StatefulAntflyServer,
)
from port_reservations import LoopbackPortReservations, find_free_port
from test_auth import StandaloneAuthServer
from test_standalone import EmbeddedInferenceStandaloneServer


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

        def release(self, *ports: int) -> None:
            self.released.extend(ports)

    reservations = StubReservations((40001, 40002, 41000))
    assert reservations.reserve_excluding({40001, 40002}, attempts=3) == 41000
    assert reservations.released == [40001, 40002]

    exhausted = StubReservations((40001, 40002))
    with pytest.raises(RuntimeError, match="outside the excluded set"):
        exhausted.reserve_excluding({40001, 40002}, attempts=2)
    assert exhausted.released == [40001, 40002]


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
    (AntflyServer, PublicAntflyServer, StandaloneAntflyServer, StandaloneAuthServer),
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


@pytest.mark.parametrize("server_type", (PublicAntflyServer, StandaloneAntflyServer))
def test_single_process_pause_retains_listener_port(server_type: type):
    server = object.__new__(server_type)
    server.proc = None
    server.port_reservations = LoopbackPortReservations()
    server.port = server.port_reservations.reserve()
    server.port_reservations.release(server.port)

    try:
        server.pause()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as contender:
            with pytest.raises(OSError):
                contender.bind(("127.0.0.1", server.port))
    finally:
        server.port_reservations.close()


def test_stateful_server_releases_requested_port_when_spawn_fails(monkeypatch: pytest.MonkeyPatch):
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
    with LoopbackPortReservations() as reservations:
        public_port, health_port = reservations.reserve_many(2)

    def reserve_listener_ports(
        reservations: LoopbackPortReservations,
        count: int,
    ) -> tuple[int, ...]:
        assert count == 2
        return (
            reservations.reserve(public_port),
            reservations.reserve(health_port),
        )

    def fail_to_spawn(*args: Any, **kwargs: Any) -> subprocess.Popen[str]:
        raise OSError(errno.ENOEXEC, "cannot execute")

    monkeypatch.setattr(LoopbackPortReservations, "reserve_many", reserve_listener_ports)
    monkeypatch.setattr(subprocess, "Popen", fail_to_spawn)
    with pytest.raises(OSError) as exc_info:
        EmbeddedInferenceStandaloneServer("/missing-antfly", tmp_path, "test-model")
    assert exc_info.value.errno == errno.ENOEXEC

    for port in (public_port, health_port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as contender:
            contender.bind(("127.0.0.1", port))
