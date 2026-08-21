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

"""Kernel-backed listener-port leases for multi-process E2E fixtures."""

from __future__ import annotations

import errno
import socket
from collections.abc import Callable, Collection, Iterable
from typing import TypeVar


T = TypeVar("T")


def find_free_port(host: str = "127.0.0.1") -> int:
    """Return a free port for a caller that will bind it immediately."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


class LoopbackPortReservations:
    """Own loopback ports until a fixture hands them to a child process.

    A pool belongs to one server or cluster fixture. Keeping the reservation
    sockets open prevents both other tests and port-zero listeners in the same
    cluster from claiming ports that have been advertised but not bound yet.
    """

    def __init__(self, host: str = "127.0.0.1") -> None:
        self.host = host
        self._sockets: dict[int, socket.socket] = {}

    def __enter__(self) -> LoopbackPortReservations:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    def reserve(self, port: int = 0) -> int:
        return self._reserve(port, reuse_address=False)

    def _reserve(self, port: int, *, reuse_address: bool) -> int:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            if reuse_address:
                # Match the child listeners' restart semantics so a fixed-port
                # lease can be reacquired while prior accepted connections
                # remain in TIME_WAIT.
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, port))
            reserved_port = int(sock.getsockname()[1])
            if reserved_port in self._sockets:
                raise RuntimeError(f"kernel returned duplicate reserved port {reserved_port}")
            self._sockets[reserved_port] = sock
            return reserved_port
        except BaseException:
            sock.close()
            raise

    def reserve_many(self, count: int) -> tuple[int, ...]:
        if count < 0:
            raise ValueError("port reservation count must be non-negative")
        ports: list[int] = []
        try:
            for _ in range(count):
                ports.append(self.reserve())
        except BaseException:
            self.release_if_reserved(*ports)
            raise
        return tuple(ports)

    def reserve_requested(self, port: int) -> int:
        """Reserve a requested port, falling back only when it is already owned."""
        try:
            return self.reserve(port)
        except OSError as exc:
            if exc.errno != errno.EADDRINUSE:
                raise
            return self.reserve()

    def reserve_excluding(self, excluded: Collection[int], *, attempts: int = 64) -> int:
        """Reserve a kernel-selected port outside a fixture's advertised set."""
        if attempts <= 0:
            raise ValueError("port reservation attempts must be positive")
        for _ in range(attempts):
            port = self.reserve()
            if port not in excluded:
                return port
            self.release(port)
        raise RuntimeError("could not reserve a port outside the excluded set")

    def ensure_reserved(self, *ports: int) -> None:
        """Idempotently reacquire fixed ports, rolling back a partial acquisition."""
        acquired: list[int] = []
        try:
            for port in ports:
                if port in self._sockets:
                    continue
                self._reserve(port, reuse_address=True)
                acquired.append(port)
        except BaseException:
            self.release_if_reserved(*acquired)
            raise

    def handoff_to(self, ports: Iterable[int], start: Callable[[], T]) -> T:
        """Release fixed ports immediately before starting their child process."""
        handed_off = tuple(ports)
        self.ensure_reserved(*handed_off)
        self.release(*handed_off)
        try:
            return start()
        except BaseException:
            # Preserve the process-spawn exception if another listener happens
            # to claim a port before the parent can restore its lease.
            try:
                self.ensure_reserved(*handed_off)
            except OSError:
                pass
            raise

    def release(self, *ports: int) -> None:
        for port in ports:
            sock = self._sockets.pop(port, None)
            if sock is None:
                raise RuntimeError(f"port {port} is not reserved")
            sock.close()

    def release_if_reserved(self, *ports: int) -> None:
        for port in ports:
            sock = self._sockets.pop(port, None)
            if sock is not None:
                sock.close()

    def close(self) -> None:
        sockets = list(self._sockets.values())
        self._sockets.clear()
        for sock in sockets:
            sock.close()
