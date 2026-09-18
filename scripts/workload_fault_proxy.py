"""Bounded loopback TCP fault relay for owned fixture endpoints.

Partition resets existing connections and rejects new ones. Response discard
forwards requests but discards worker-to-coordinator bytes without acknowledging
application success. Delay is per received byte chunk, not an invented RPC clock.
"""

from __future__ import annotations

import collections
import errno
import select
import socket
import struct
import threading
import time

import workload_proxy_evidence as evidence


class FaultProxy:
    def __init__(
        self,
        listener,
        target_port,
        record,
        name,
        *,
        max_connections=64,
        buffer_bytes=65536,
        capture_response_bytes=0,
        redact_values=(),
    ):
        if (
            type(capture_response_bytes) is not int
            or not 0 <= capture_response_bytes <= 16384
        ):
            raise ValueError("response capture must be0..16384bytes")
        self.capture_response_bytes = capture_response_bytes
        self.redact_values = tuple(value for value in redact_values if value)
        self.listener = listener
        self.listener.listen(128)
        self.listener.setblocking(False)
        self.target = ("127.0.0.1", target_port)
        self.record, self.name = record, name
        self.max_connections, self.buffer_bytes = max_connections, buffer_bytes
        self.lock = threading.Lock()
        self.applied = threading.Condition(self.lock)
        self.generation = self.applied_generation = 0
        self.policy = {"partition": False, "delay_ms": 0, "drop_response": False}
        self.counters = collections.Counter()
        self.paths = collections.Counter()
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self._run, name=f"fault-proxy-{name}")
        self.error = None
        self.thread.start()

    def set_policy(self, *, partition=False, delay_ms=0, drop_response=False):
        if not 0 <= delay_ms <= 10000:
            raise ValueError("proxy delay must be0..10000ms")
        with self.applied:
            self.policy = {
                "partition": partition,
                "delay_ms": delay_ms,
                "drop_response": drop_response,
            }
            self.generation += 1
            generation = self.generation
            if (
                not self.applied.wait_for(
                    lambda: self.applied_generation >= generation
                    or self.error is not None,
                    timeout=2,
                )
                or self.error
            ):
                raise RuntimeError("proxy did not apply fault policy")
        self.record(
            {
                "event": "proxy_policy",
                "proxy": self.name,
                "generation": generation,
                **self.policy,
            }
        )

    def snapshot(self):
        with self.lock:
            return {
                **self.counters,
                "paths": dict(self.paths),
                "policy": self.policy.copy(),
                "error": self.error,
            }

    def close(self):
        self.stopping.set()
        self.thread.join(timeout=2)
        if self.thread.is_alive():
            raise RuntimeError("proxy did not stop within2s")
        if self.error:
            raise RuntimeError(self.error)

    def _run(self):
        pairs = {}
        sequence = 0

        def count(name, value=1):
            with self.lock:
                self.counters[name] += value

        def retire(pair, reason):
            if self.capture_response_bytes:
                captured = bytes(pair["response_prefix"])
                head, separator, body = captured.partition(b"\r\n\r\n")
                lines = head.decode(errors="replace").split("\r\n")
                allowed = {
                    "content-type",
                    "content-length",
                    "x-antfly-workload-evidence",
                }
                headers = []
                if separator:
                    for line in lines[1:]:
                        key, colon, value = line.partition(":")
                        if colon and key.lower() in allowed:
                            headers.append([key, value.strip()])

                def redact(value):
                    for secret in self.redact_values:
                        value = value.replace(secret, "[REDACTED]")
                    return value

                self.record(
                    {
                        "event": "proxy_response_capture",
                        "request_message": evidence.capture_message(
                            bytes(pair["request_prefix"]),
                            pair["request_bytes"],
                            self.redact_values,
                        ),
                        "response_message": evidence.capture_message(
                            captured, pair["response_bytes"], self.redact_values
                        ),
                        "proxy": self.name,
                        "connection": pair["id"],
                        "status_line": redact(lines[0]) if separator else None,
                        "headers": [[key, redact(value)] for key, value in headers],
                        "body_prefix": (
                            redact(body.decode(errors="replace")) if separator else None
                        ),
                        "captured_bytes": len(captured),
                        "received_bytes": pair["response_bytes"],
                        "truncated": pair["response_bytes"] > len(captured),
                        "headers_complete": bool(separator),
                        "scope": "first connection response prefix; not parsed pipelined evidence",
                    }
                )
            for peer in (pair["client"], pair["server"]):
                pairs.pop(peer, None)
                if reason == "partition":
                    peer.setsockopt(
                        socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
                    )
                peer.close()
            count("closed_connections")
            self.record(
                {
                    "event": "proxy_close",
                    "proxy": self.name,
                    "connection": pair["id"],
                    "reason": reason,
                }
            )

        try:
            while not self.stopping.is_set():
                with self.lock:
                    policy = self.policy.copy()
                    generation = self.generation
                unique = {id(pair): pair for pair in pairs.values()}.values()
                if policy["partition"]:
                    for pair in list(unique):
                        retire(pair, "partition")
                if policy["drop_response"]:
                    for pair in list(
                        {id(pair): pair for pair in pairs.values()}.values()
                    ):
                        client = pair["client"]
                        if pair["sizes"][client]:
                            count("dropped_response_bytes", pair["sizes"][client])
                            pair["pending"][client].clear()
                            pair["sizes"][client] = 0
                with self.applied:
                    self.applied_generation = generation
                    self.applied.notify_all()
                for pair in list({id(pair): pair for pair in pairs.values()}.values()):
                    for source, destination in (
                        (pair["client"], pair["server"]),
                        (pair["server"], pair["client"]),
                    ):
                        if (
                            source in pair["eof"]
                            and not pair["pending"][destination]
                            and destination not in pair["shutdown"]
                            and not (
                                destination is pair["server"] and pair["connecting"]
                            )
                        ):
                            try:
                                destination.shutdown(socket.SHUT_WR)
                            except OSError:
                                pass
                            pair["shutdown"].add(destination)
                    if len(pair["eof"]) == 2 and not any(pair["pending"].values()):
                        retire(pair, "drained_eof")
                readable, writable = [self.listener], []
                now = time.monotonic()
                for peer, pair in list(pairs.items()):
                    if peer is pair["server"] and pair["connecting"]:
                        writable.append(peer)
                        continue
                    destination = (
                        pair["server"] if peer is pair["client"] else pair["client"]
                    )
                    queue = pair["pending"][destination]
                    if (
                        peer not in pair["eof"]
                        and pair["sizes"][destination] < self.buffer_bytes
                    ):
                        readable.append(peer)
                    if queue and queue[0][0] <= now:
                        writable.append(destination)
                ready_read, ready_write, _ = select.select(
                    readable, list(set(writable)), [], 0.005
                )
                if self.listener in ready_read:
                    ready_read.remove(self.listener)
                    client, address = self.listener.accept()
                    client.setblocking(False)
                    if policy["partition"] or len(pairs) // 2 >= self.max_connections:
                        if policy["partition"]:
                            client.setsockopt(
                                socket.SOL_SOCKET,
                                socket.SO_LINGER,
                                struct.pack("ii", 1, 0),
                            )
                        client.close()
                        count(
                            "partition_rejections"
                            if policy["partition"]
                            else "capacity_rejections"
                        )
                    else:
                        server = socket.socket()
                        server.setblocking(False)
                        status = server.connect_ex(self.target)
                        if status not in (
                            0,
                            errno.EINPROGRESS,
                            errno.EWOULDBLOCK,
                            errno.EALREADY,
                        ):
                            client.close()
                            server.close()
                            count("connect_failures")
                        else:
                            sequence += 1
                            pair = {
                                "id": sequence,
                                "client": client,
                                "server": server,
                                "connecting": status != 0,
                                "pending": {
                                    client: collections.deque(),
                                    server: collections.deque(),
                                },
                                "sizes": {client: 0, server: 0},
                                "prefix": bytearray(),
                                "path_seen": False,
                                "request_prefix": bytearray(),
                                "request_bytes": 0,
                                "response_prefix": bytearray(),
                                "response_bytes": 0,
                                "eof": set(),
                                "shutdown": set(),
                            }
                            pairs[client] = pairs[server] = pair
                            count("accepted_connections")
                            self.record(
                                {
                                    "event": "proxy_accept",
                                    "proxy": self.name,
                                    "connection": sequence,
                                    "source": address,
                                    "target": self.target,
                                }
                            )
                for peer in ready_read:
                    pair = pairs.get(peer)
                    if pair is None:
                        continue
                    destination = (
                        pair["server"] if peer is pair["client"] else pair["client"]
                    )
                    try:
                        chunk = peer.recv(
                            min(16384, self.buffer_bytes - pair["sizes"][destination])
                        )
                    except BlockingIOError:
                        continue
                    except OSError:
                        retire(pair, "read_error")
                        continue
                    if not chunk:
                        pair["eof"].add(peer)
                        continue
                    from_client = peer is pair["client"]
                    count(
                        (
                            "received_upstream_bytes"
                            if from_client
                            else "received_downstream_bytes"
                        ),
                        len(chunk),
                    )
                    if from_client and self.capture_response_bytes:
                        pair["request_bytes"] += len(chunk)
                        pair["request_prefix"].extend(
                            chunk[
                                : self.capture_response_bytes
                                - len(pair["request_prefix"])
                            ]
                        )
                    if from_client and not pair["path_seen"]:
                        pair["prefix"].extend(chunk[: 4096 - len(pair["prefix"])])
                        if b"\r\n" in pair["prefix"] or len(pair["prefix"]) >= 4096:
                            line = (
                                bytes(pair["prefix"])
                                .split(b"\r\n", 1)[0]
                                .decode(errors="replace")
                            )
                            pair["path_seen"] = True
                            parts = line.split(" ")
                            path = (
                                parts[1]
                                if len(parts) == 3 and parts[2].startswith("HTTP/")
                                else "non-http"
                            )
                            with self.lock:
                                self.paths[
                                    (
                                        path
                                        if path in self.paths or len(self.paths) < 256
                                        else "other_paths"
                                    )
                                ] += 1
                            self.record(
                                {
                                    "event": "proxy_request_line",
                                    "proxy": self.name,
                                    "connection": pair["id"],
                                    "path": path,
                                }
                            )
                    if not from_client and self.capture_response_bytes:
                        pair["response_bytes"] += len(chunk)
                        pair["response_prefix"].extend(
                            chunk[
                                : self.capture_response_bytes
                                - len(pair["response_prefix"])
                            ]
                        )
                    if not from_client and policy["drop_response"]:
                        count("dropped_response_bytes", len(chunk))
                        continue
                    pair["pending"][destination].append(
                        [time.monotonic() + policy["delay_ms"] / 1000, chunk]
                    )
                    pair["sizes"][destination] += len(chunk)
                for peer in ready_write:
                    pair = pairs.get(peer)
                    if pair is None:
                        continue
                    if peer is pair["server"] and pair["connecting"]:
                        if peer.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
                            retire(pair, "connect_error")
                            continue
                        pair["connecting"] = False
                    queue = pair["pending"][peer]
                    if not queue or queue[0][0] > time.monotonic():
                        continue
                    _, chunk = queue[0]
                    try:
                        sent = peer.send(chunk)
                    except BlockingIOError:
                        continue
                    except OSError:
                        retire(pair, "write_error")
                        continue
                    if not sent:
                        retire(pair, "write_zero")
                        continue
                    count(
                        (
                            "forwarded_upstream_bytes"
                            if peer is pair["server"]
                            else "forwarded_downstream_bytes"
                        ),
                        sent,
                    )
                    pair["sizes"][peer] -= sent
                    if sent == len(chunk):
                        queue.popleft()
                    else:
                        queue[0][1] = chunk[sent:]
        except BaseException as error:
            with self.lock:
                self.error = f"{type(error).__name__}: {error}"
            self.record(
                {"event": "proxy_error", "proxy": self.name, "error": self.error}
            )
        finally:
            for pair in list({id(pair): pair for pair in pairs.values()}.values()):
                retire(pair, "proxy_shutdown")
            self.listener.close()
