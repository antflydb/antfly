#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Loopback-only, versioned GCS JSON API fixture with bounded network shaping.

This implements the subset used by Antfly qualification, not a production GCS
service. Object bytes live on disk; generation identities and conditional writes
are real fixture invariants. No request is forwarded and auth headers are never
recorded. Server timings describe observed overlap, not a causal execution DAG.
"""

from __future__ import annotations

import base64
from collections import Counter
from dataclasses import dataclass
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
from pathlib import Path
import re
import socket
import threading
import time
from urllib.parse import parse_qs, unquote, urlsplit


@dataclass(frozen=True)
class Network:
    latency_ms: float = 0
    jitter_ms: float = 0
    bandwidth_mib: float = 0  # shared across every response; zero is unlimited
    concurrency: int = 16
    seed: int = 0
    fault_every: int = 0
    fault: str = "none"  # none, throttle, reset, truncate

    def validate(self):
        if any(
            not math.isfinite(v) or v < 0
            for v in (self.latency_ms, self.jitter_ms, self.bandwidth_mib)
        ):
            raise ValueError("Network values must be finite and nonnegative")
        if (
            not 1 <= self.concurrency <= 256
            or self.fault_every < 0
            or self.fault not in {"none", "throttle", "reset", "truncate"}
        ):
            raise ValueError("Invalid concurrency or fault policy")


class Store:
    def __init__(self, root: Path, bucket="lake-benchmark", max_bytes=8 * 1024**3):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=False)
        self.bucket, self.max_bytes = bucket, max_bytes
        self.lock = threading.RLock()
        self.versions = {}
        self.heads = {}
        self.kinds = {}
        self.generation = 0
        self.bytes = 0

    def put(self, key, body, expected=None):
        with self.lock:
            current = self.heads.get(key)
            if expected is not None and expected != str(current or 0):
                return None
            if len(body) > self.max_bytes - self.bytes:
                raise ValueError("Fixture storage budget exceeded")
            self.generation += 1
            generation = str(self.generation)
            checksum = hashlib.sha256(body).hexdigest()
            path = self.root / generation
            path.write_bytes(body)
            item = {
                "bucket": self.bucket,
                "name": key,
                "generation": generation,
                "size": str(len(body)),
                "etag": '"' + checksum + '"',
                "md5Hash": base64.b64encode(hashlib.md5(body).digest()).decode(),
                "contentType": "application/octet-stream",
            }
            kind = "binary:" + body[:8].hex()
            if len(body) < 2 * 1024**2 and body[:1] in (b"{", b"["):
                try:
                    value = json.loads(body)
                    if isinstance(value, dict):
                        keys = set(value)
                        if {"depth", "leaf", "records", "children"} <= keys:
                            kind = "generation_map"
                        elif {"metadata", "blocks", "bytes"} <= keys:
                            kind = "seekable_directory"
                        elif {"source", "snapshot", "public", "private"} <= keys:
                            kind = "source_navigation"
                        else:
                            kind = "json:" + ",".join(sorted(keys)[:6])
                    else:
                        kind = "json_array"
                except (ValueError, UnicodeDecodeError):
                    pass
            self.kinds[key, generation] = kind
            self.versions[key, generation] = item
            self.heads[key] = generation
            self.bytes += len(body)
            return dict(item)

    def get(self, key, generation=None):
        with self.lock:
            generation = generation or self.heads.get(key)
            item = self.versions.get((key, generation))
            return dict(item) if item else None

    def delete(self, key, generation=None, expected=None):
        with self.lock:
            generation = generation or self.heads.get(key)
            item = self.versions.get((key, generation))
            if expected is not None and expected != str(generation or 0):
                return 412
            if item is None:
                return 404
            del self.versions[key, generation]
            self.kinds.pop((key, generation), None)
            (self.root / generation).unlink()
            self.bytes -= int(item["size"])
            if self.heads.get(key) == generation:
                del self.heads[key]
            return 204

    def listing(self, query):
        with self.lock:
            prefix = query.get("prefix", [""])[0]
            delimiter = query.get("delimiter", [None])[0]
            versions = query.get("versions", ["false"])[0] == "true"
            items = sorted(
                (
                    dict(item)
                    for (key, generation), item in self.versions.items()
                    if key.startswith(prefix)
                    and (versions or self.heads.get(key) == generation)
                ),
                key=lambda item: (item["name"], int(item["generation"])),
            )
            prefixes = set()
            if delimiter:
                selected = []
                for item in items:
                    suffix = item["name"][len(prefix) :]
                    if delimiter in suffix:
                        prefixes.add(prefix + suffix.split(delimiter)[0] + delimiter)
                    else:
                        selected.append(item)
                items = selected
            offset = int(query.get("pageToken", ["0"])[0])
            limit = max(1, min(1000, int(query.get("maxResults", ["1000"])[0])))
            result = {
                "items": items[offset : offset + limit],
                "prefixes": sorted(prefixes),
            }
            if offset + limit < len(items):
                result["nextPageToken"] = str(offset + limit)
            return result


class NetworkStore:
    def __init__(
        self, root, network=Network(), bucket="lake-benchmark", max_bytes=8 * 1024**3
    ):
        network.validate()
        self.store = Store(root, bucket, max_bytes)
        self.network = network
        self.lock = threading.Lock()
        self.records = []
        self.record_limit = 100_000
        self.record_overflow = 0
        self.sequence = 0
        self.phase = "setup"
        self.active = self.peak = self.waiting = 0
        self.next_byte_time = 0.0
        self.stop = threading.Event()
        self.slots = threading.BoundedSemaphore(network.concurrency)
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def setup(self):
                super().setup()
                self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.connection.settimeout(5)

            def log_message(self, *_):
                pass

            def do_HEAD(self):
                self.handle_request()

            def do_GET(self):
                self.handle_request()

            def do_POST(self):
                self.handle_request()

            def do_PUT(self):
                self.handle_request()

            def do_DELETE(self):
                self.handle_request()

            def handle_request(self):
                arrived = time.monotonic_ns()
                with owner.lock:
                    owner.waiting += 1
                    slots = owner.slots
                while not slots.acquire(timeout=0.1):
                    if owner.stop.is_set():
                        with owner.lock:
                            owner.waiting -= 1
                        return
                with owner.lock:
                    owner.waiting -= 1
                    owner.sequence += 1
                    sequence = owner.sequence
                    phase = owner.phase
                    owner.active += 1
                    owner.peak = max(owner.peak, owner.active)
                started = time.monotonic_ns()
                record = {
                    "sequence": sequence,
                    "phase": phase,
                    "method": self.command,
                    "arrived_ns": arrived,
                    "started_ns": started,
                    "status": None,
                    "bytes": 0,
                    "fault": None,
                    "object": None,
                    "range": self.headers.get("Range"),
                }
                source = None
                try:
                    # Each request pays one synthetic RTT. Jitter is reproducible
                    # by request ordinal; bandwidth is paced separately below.
                    digest = hashlib.sha256(
                        f"{owner.network.seed}:{sequence}".encode()
                    ).digest()
                    jitter = (
                        int.from_bytes(digest[:4], "big") / (2**32 - 1) * 2 - 1
                    ) * owner.network.jitter_ms
                    if owner.stop.wait(
                        max(0, owner.network.latency_ms + jitter) / 1000
                    ):
                        return
                    parsed = urlsplit(self.path)
                    query = parse_qs(parsed.query, keep_blank_values=True)
                    match = re.fullmatch(
                        r"/(?:upload/)?storage/v1/b/([^/]+)(?:/o(?:/(.*))?)?",
                        parsed.path,
                    )
                    if not match or unquote(match[1]) != owner.store.bucket:
                        return self.reply(record, 404, {"error": {"code": 404}})
                    key = unquote(match[2]) if match[2] is not None else None
                    record["object"] = hashlib.sha256((key or "").encode()).hexdigest()
                    media = (
                        self.command == "GET"
                        and key is not None
                        and query.get("alt") == ["media"]
                    )
                    record["operation"] = (
                        "delete"
                        if self.command == "DELETE"
                        else "media_range"
                        if media and record["range"]
                        else "media_full"
                        if media
                        else "metadata"
                        if key is not None
                        else "upload"
                        if self.command in {"POST", "PUT"}
                        else "list"
                        if parsed.path.endswith("/o")
                        else "bucket"
                    )
                    # Faults affect media reads only; metadata/CAS authority is
                    # never silently weakened by the failure model.
                    fault = (
                        owner.network.fault
                        if media
                        and owner.network.fault_every
                        and sequence % owner.network.fault_every == 0
                        else "none"
                    )
                    if fault == "throttle":
                        record["fault"] = fault
                        return self.reply(
                            record, 429, {"error": {"code": 429}}, {"Retry-After": "0"}
                        )
                    if fault == "reset":
                        record["fault"] = fault
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        return
                    if key is None and not parsed.path.endswith("/o"):
                        return self.reply(
                            record,
                            200,
                            {"id": owner.store.bucket, "name": owner.store.bucket},
                        )
                    if key is None and self.command == "GET":
                        return self.reply(record, 200, owner.store.listing(query))
                    if key is None and self.command in {"POST", "PUT"}:
                        key = query.get("name", [None])[0]
                        if key is None or query.get("uploadType") != ["media"]:
                            return self.reply(
                                record,
                                400,
                                {
                                    "error": {
                                        "message": "Only bounded media uploads are supported"
                                    }
                                },
                            )
                        length = int(self.headers.get("Content-Length", "0"))
                        if not 0 <= length <= 64 * 1024**2:
                            return self.reply(record, 413, {})
                        body = self.rfile.read(length)
                        if len(body) != length:
                            return self.reply(record, 400, {})
                        item = owner.store.put(
                            key, body, query.get("ifGenerationMatch", [None])[0]
                        )
                        return self.reply(record, 200 if item else 412, item or {})
                    generation = query.get("generation", [None])[0]
                    if self.command == "DELETE":
                        return self.reply(
                            record,
                            owner.store.delete(
                                key,
                                generation,
                                query.get("ifGenerationMatch", [None])[0],
                            ),
                            b"",
                        )
                    # Open under the version lock so concurrent retirement
                    # cannot unlink between generation admission and open. The
                    # open descriptor stays valid through response completion.
                    with owner.store.lock:
                        item = owner.store.get(key, generation)
                        if item is not None:
                            record["kind"] = owner.store.kinds.get(
                                (key, item["generation"]), "unknown"
                            )
                            if media:
                                source = (owner.store.root / item["generation"]).open(
                                    "rb"
                                )
                    if item is None:
                        return self.reply(record, 404, {"error": {"code": 404}})
                    if self.headers.get("If-Match") not in (None, item["etag"]):
                        return self.reply(record, 412, {})
                    if self.command not in {"GET", "HEAD"}:
                        return self.reply(record, 405, {})
                    if not media:
                        return self.reply(record, 200, item)
                    start, end = 0, int(item["size"])
                    status = 200
                    headers = {
                        "ETag": item["etag"],
                        "x-goog-generation": item["generation"],
                        "Accept-Ranges": "bytes",
                    }
                    if record["range"]:
                        selected = re.fullmatch(r"bytes=(\d+)-(\d*)", record["range"])
                        if not selected:
                            return self.reply(record, 416, {})
                        start = int(selected[1])
                        end = min(end, int(selected[2]) + 1) if selected[2] else end
                        if start >= end:
                            return self.reply(record, 416, {})
                        headers["Content-Range"] = (
                            f"bytes {start}-{end - 1}/{item['size']}"
                        )
                        status = 206
                    source.seek(start)
                    self.reply(
                        record,
                        status,
                        source,
                        headers,
                        length=end - start,
                        truncate=fault == "truncate",
                    )
                except (BrokenPipeError, ConnectionResetError, TimeoutError):
                    record["client_disconnected"] = True
                except (ValueError, OSError) as error:
                    record["fixture_error"] = type(error).__name__
                    self.close_connection = True
                finally:
                    if source is not None:
                        source.close()
                    record["ended_ns"] = time.monotonic_ns()
                    with owner.lock:
                        owner.active -= 1
                        if len(owner.records) < owner.record_limit:
                            owner.records.append(record)
                        else:
                            owner.record_overflow += 1
                    slots.release()

            def reply(
                self, record, status, body, headers=None, *, length=None, truncate=False
            ):
                if isinstance(body, dict):
                    body = json.dumps(body, separators=(",", ":")).encode()
                if length is None:
                    length = len(body)
                record["status"] = status
                self.send_response(status)
                self.send_header("Content-Length", str(length))
                self.send_header(
                    "Content-Type",
                    "application/json"
                    if isinstance(body, bytes)
                    else "application/octet-stream",
                )
                for key, value in (headers or {}).items():
                    self.send_header(key, value)
                self.end_headers()
                if self.command == "HEAD":
                    return
                if truncate:
                    record["fault"] = "truncate"
                    self.close_connection = True
                remaining = length // 2 if truncate else length
                position = 0
                while remaining:
                    count = min(64 * 1024, remaining)
                    data = (
                        body[position : position + count]
                        if isinstance(body, bytes)
                        else body.read(count)
                    )
                    if not data:
                        raise ValueError("Short fixture body")
                    if owner.network.bandwidth_mib:
                        with owner.lock:
                            now = time.monotonic()
                            owner.next_byte_time = max(now, owner.next_byte_time) + len(
                                data
                            ) / (owner.network.bandwidth_mib * 1024**2)
                            delay = owner.next_byte_time - now
                        if owner.stop.wait(delay):
                            return
                    self.wfile.write(data)
                    self.wfile.flush()
                    record["bytes"] += len(data)
                    remaining -= len(data)
                    position += len(data)

        class Server(ThreadingHTTPServer):
            def handle_error(self, request, client_address):
                import sys

                if isinstance(
                    sys.exc_info()[1],
                    (BrokenPipeError, ConnectionResetError, TimeoutError),
                ):
                    return
                super().handle_error(request, client_address)

        self.server = Server(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def configure(self, network, phase, *, reset_trace=False):
        network.validate()
        with self.lock:
            if (network != self.network or reset_trace) and (
                self.active or self.waiting
            ):
                raise RuntimeError(
                    "Cannot change network policy with requests in flight"
                )
            if network.concurrency != self.network.concurrency:
                self.slots = threading.BoundedSemaphore(network.concurrency)
            if network != self.network or reset_trace:
                self.next_byte_time = 0
            self.network = network
            self.phase = phase
            if reset_trace:
                self.records.clear()
                self.record_overflow = 0

    def wait_idle(self, timeout=30):
        deadline = time.monotonic() + timeout
        while True:
            with self.lock:
                if not self.active and not self.waiting:
                    return
            if time.monotonic() >= deadline:
                raise TimeoutError("Object store did not become idle")
            time.sleep(0.001)

    def summary(self, phase):
        with self.lock:
            selected = [dict(row) for row in self.records if row["phase"] == phase]
            overflow = self.record_overflow
        counts = Counter((row["method"], row["status"]) for row in selected)
        events = sorted(
            [(row["started_ns"], 1) for row in selected]
            + [(row["ended_ns"], -1) for row in selected]
        )
        active = peak = waves = 0
        for _, change in events:
            if change == 1 and active == 0:
                waves += 1
            active += change
            peak = max(peak, active)
        operation_counts = Counter(row.get("operation", "unknown") for row in selected)
        kind_counts = Counter(
            row.get("kind", "control") for row in selected if row["method"] == "GET"
        )
        object_reads = Counter(
            row["object"]
            for row in selected
            if row["method"] == "GET" and row["object"]
        )
        return {
            "requests": len(selected),
            "operation_counts": dict(operation_counts),
            "get_kind_counts": dict(kind_counts),
            "unique_get_objects": len(object_reads),
            "max_reads_per_object": max(object_reads.values(), default=0),
            "bytes": sum(row["bytes"] for row in selected),
            "counts": {
                f"{method}:{status}": count
                for (method, status), count in counts.items()
            },
            "peak_inflight": peak,
            "observed_request_waves": waves,
            "causal_dependency_depth": None,
            "trace_overflow": overflow,
            "fixture_errors": sum("fixture_error" in row for row in selected),
            "queue_ns": sum(row["started_ns"] - row["arrived_ns"] for row in selected),
            "faults": sum(row["fault"] is not None for row in selected),
        }

    def close(self):
        self.stop.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
