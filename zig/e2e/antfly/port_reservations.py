# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
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

"""Kernel reservations and cross-process listener leases for E2E fixtures."""

from __future__ import annotations

import errno
import fcntl
import os
import secrets
import socket
import subprocess
import sys
from collections.abc import Callable, Collection, Iterable
from functools import lru_cache
from pathlib import Path
from typing import Self, TypeVar

T = TypeVar("T")


@lru_cache(maxsize=1)
def _listener_range() -> tuple[int, int]:
    if sys.platform.startswith("linux"):
        first, last = map(
            int, Path("/proc/sys/net/ipv4/ip_local_port_range").read_text().split()
        )
    elif sys.platform == "darwin":
        result = subprocess.run(
            [
                "sysctl",
                "-n",
                "net.inet.ip.portrange.first",
                "net.inet.ip.portrange.last",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        first, last = map(int, result.stdout.split())
    else:
        raise RuntimeError(f"unsupported E2E port allocation platform: {sys.platform}")
    # Prefer a bounded low listener pool. Hosts configured with a low client
    # range can instead use ports above that range, still excluding privileged
    # ports and every client ephemeral port.
    lower, upper = 10000, min(30000, first)
    if upper <= lower:
        lower, upper = max(10000, last + 1), 65536
    if upper <= lower:
        raise RuntimeError("no E2E listener range outside ephemeral ports")
    return lower, upper


def find_free_port(host: str = "127.0.0.1") -> int:
    """Return a free port for a caller that will bind it immediately."""
    with LoopbackPortReservations(host) as reservations:
        return reservations.reserve()


class LoopbackPortReservations:
    """Own loopback ports throughout a fixture's child-process lifecycle.

    Reservation sockets protect pre-start listeners. Advisory leases exclude
    other fixtures across child handoff, pauses, and restarts; allocation outside
    the client ephemeral range also excludes outgoing connection allocation.
    """

    def __init__(self, host: str = "127.0.0.1") -> None:
        self.host = host
        self._sockets: dict[int, socket.socket] = {}
        # Socket reservations cannot span exec without listener activation.
        # Keep a cross-process lease through the handoff gap and the child
        # lifetime, including paused/restarted nodes. Never unlink lock files:
        # doing so permits two processes to lock different inodes for one port.
        self._leases: dict[int, int] = {}

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    def reserve(self, port: int = 0) -> int:
        return self._reserve(port, reuse_address=False)

    def _reserve(self, port: int, *, reuse_address: bool) -> int:
        if port == 0:
            # Avoid the kernel's client ephemeral range: outbound HTTP sockets
            # must not consume a released listener port while its child starts.
            lower, upper = _listener_range()
            for _ in range(128):
                candidate = lower + secrets.randbelow(upper - lower)
                # A handed-off listener may be temporarily unbound. Its lease
                # still belongs to that child; only explicit restart requests
                # may reacquire it within this pool.
                if candidate in self._leases:
                    continue
                try:
                    return self._reserve(candidate, reuse_address=reuse_address)
                except OSError as exc:
                    if exc.errno != errno.EADDRINUSE:
                        raise
            raise OSError(errno.EADDRINUSE, "E2E listener port pool exhausted")
        lease = None
        if port not in self._leases:
            directory = Path("/tmp") / f"antfly-e2e-port-leases-{os.getuid()}"
            directory.mkdir(mode=0o700, exist_ok=True)
            lease = os.open(
                directory / str(port),
                os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW,
                0o600,
            )
            try:
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BaseException as exc:
                os.close(lease)
                if isinstance(exc, BlockingIOError):
                    raise OSError(
                        errno.EADDRINUSE, "E2E listener port is leased"
                    ) from exc
                raise
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if reuse_address:
                # Match the child listeners' restart semantics so a fixed-port
                # lease can be reacquired while prior accepted connections
                # remain in TIME_WAIT.
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, port))
            reserved_port = int(sock.getsockname()[1])
            if reserved_port in self._sockets:
                raise RuntimeError(
                    f"kernel returned duplicate reserved port {reserved_port}"
                )
            self._sockets[reserved_port] = sock
            if lease is not None:
                self._leases[reserved_port] = lease
            return reserved_port
        except BaseException:
            if sock is not None:
                sock.close()
            if lease is not None:
                os.close(lease)
            raise

    def reserve_many(self, count: int) -> tuple[int, ...]:
        if count < 0:
            raise ValueError("port reservation count must be non-negative")
        ports: list[int] = []
        try:
            for _ in range(count):
                ports.append(self.reserve())
        except BaseException:
            for port in ports:
                self._discard_unused(port)
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

    def reserve_excluding(
        self, excluded: Collection[int], *, attempts: int = 64
    ) -> int:
        """Reserve a kernel-selected port outside a fixture's advertised set."""
        if attempts <= 0:
            raise ValueError("port reservation attempts must be positive")
        for _ in range(attempts):
            port = self.reserve()
            if port not in excluded:
                return port
            self._discard_unused(port)
        raise RuntimeError("could not reserve a port outside the excluded set")

    def _discard_unused(self, port: int) -> None:
        """Release a candidate that was never handed to or advertised by a child."""
        self.release_if_reserved(port)
        lease = self._leases.pop(port, None)
        if lease is not None:
            os.close(lease)

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
        leases = list(self._leases.values())
        self._leases.clear()
        for lease in leases:
            os.close(lease)
