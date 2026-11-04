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

"""Bounded, opt-in writable HN lake qualification on filesystem or existing GCS.

Use a fresh state directory and GCS prefix for every attempt. Raw evidence is
private: configuration contains only credential references, never bearer values.
Catalog-only churn isolates history costs; indexed churn measures visibility and
search while vacuum runs against both exclusively owned qualification tables.
"""

import argparse
import errno
import hashlib
import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import socket
import statistics
import struct
import subprocess
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request

from managed_api import open_request
from poc import RequestFailure, gcs_bearer, parse_lake_metrics, publication_profile

TABLES = ("hn_catalog_history", "hn_search_churn")
KINDS = {
    "hn_id": "integer",
    "title": "string",
    "body": "string",
    "author": "string",
    "points": "integer",
    "created_at": "integer",
    "item_type": "string",
    "qual_version": "integer",
}


def require_available_ports(ports):
    # Refuse an existing listener before sending any API requests. The bind
    # check also catches a reserved port; REUSEADDR permits our own TIME_WAIT
    # sockets after a clean restart. Process publication stays serialized.
    for port in ports:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                probe.settimeout(0.2)
                if probe.connect_ex(("127.0.0.1", port)) != errno.ECONNREFUSED:
                    raise OSError("port is already listening or unavailable")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reservation:
                reservation.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                reservation.bind(("127.0.0.1", port))
        except OSError:
            raise RuntimeError("qualification listen ports are unavailable") from None


def distribution(values):
    if not values:
        return None
    ordered = sorted(values)
    return {
        "count": len(values),
        "median": statistics.median(values),
        "p95": ordered[math.ceil(len(values) * 0.95) - 1],
        "max": ordered[-1],
    }


def manifest_entries(payload):
    from pyiceberg.io import InputFile
    from pyiceberg.manifest import read_manifest_list

    class MemoryInput(InputFile):
        def __len__(self):
            return len(payload)

        def exists(self):
            return True

        def open(self, seekable=True):
            return io.BytesIO(payload)

    return list(read_manifest_list(MemoryInput("qualification-memory")))


def sha256_file(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def atomic_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def table_definition(name, warehouse, indexed):
    properties = {key: {"type": kind} for key, kind in KINDS.items()}
    for column in ("hn_id", "points", "created_at"):
        properties[column]["x-antfly-field"] = {"type": "numeric", "sortable": True}
    for column in ("author", "item_type"):
        properties[column]["x-antfly-field"] = {"type": "keyword", "sortable": True}
    return {
        "storage": {"engine": "object"},
        "schema": {
            "storage_mode": "relational",
            "default_type": "row",
            "enforce_types": True,
            "relational_indexes": [
                {"name": name + "_" + key, "keys": [{"column": key}]}
                for key in ("hn_id", "created_at", "points", "author", "item_type")
            ]
            if indexed
            else [],
            "document_schemas": {
                "row": {
                    "schema": {
                        "type": "object",
                        "properties": properties,
                        "required": ["hn_id"],
                        "additionalProperties": False,
                    }
                }
            },
            "base_source": {
                "kind": "external",
                "format": "iceberg",
                "object_mutability": "immutable",
                "table_id": name,
                "uri": warehouse,
                "credentials": {"ref": "source"},
                "schema_fingerprint": "auto",
                "write_policy": "iceberg_writer",
                "catalog": {"type": "managed"},
            },
        },
        "indexes": {"body_text": {"type": "full_text", "store_source": True}}
        if indexed
        else {},
    }


def catalog_definition(name):
    return {
        "commit_id": "initialize-" + name,
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {
                    "id": i,
                    "name": key,
                    "required": key == "hn_id",
                    "type": "long" if kind == "integer" else "string",
                }
                for i, (key, kind) in enumerate(KINDS.items(), 1)
            ],
        },
        "properties": {
            "format-version": "2",
            "write.metadata.previous-versions-max": "100",
        },
    }


def configuration(args):
    state = args.state.resolve()
    if args.provider == "local":
        source = {"protocol": "filesystem", "root": str(state / "source-store")}
        artifacts = {"protocol": "filesystem", "root": str(state / "artifact-store")}
        bucket, artifact_prefix = "hnqual", "native"
        warehouses = {name: "file:///warehouse/" + name for name in TABLES}
    else:
        credentials = {
            "source": "bearer_token",
            "bearer_token": "${secret:HN_GCS_BEARER}",
        }
        common = {
            "protocol": "gcs",
            "buckets": [args.bucket],
            "bucket_provisioning": "require_existing",
            "credentials": credentials,
        }
        source = dict(common, prefix=args.prefix + "/warehouse")
        artifacts = dict(common, prefix=args.prefix + "/artifacts")
        bucket, artifact_prefix = args.bucket, args.prefix + "/artifacts"
        warehouses = {
            name: f"gs://{args.bucket}/{args.prefix}/warehouse/{name}"
            for name in TABLES
        }
    config = {
        "storage": {
            "engine": "local",
            "local": {"base_dir": str(state / "data")},
            "artifacts": {
                "connection": "artifacts",
                "bucket": bucket,
                "prefix": artifact_prefix,
            },
        },
        "connections": {
            "source": {
                "kind": "external_io",
                "capabilities": ["lake.read", "lake.write"],
                "external_io": source,
            },
            "artifacts": {
                "kind": "external_io",
                "capabilities": ["storage.primary"],
                "external_io": artifacts,
            },
        },
        "lake_cache": {"root": str(state / "cache"), "max_disk_bytes": 1024**3},
        "lake_indexes": {"query_cursors": {"retention_ms": 300000}},
    }
    if args.provider == "local":
        # Explicit storage.artifacts connections admit S3/GCS only. Standalone
        # supplies its native filesystem artifact owner from the local data dir.
        del config["storage"]["artifacts"]
        del config["connections"]["artifacts"]
    return config, warehouses


def fixture(path, count):
    import pyarrow.parquet as pq

    records = []
    with pq.ParquetFile(path) as source:
        available = set(source.schema_arrow.names)
        columns = [
            name for name in KINDS if name != "qual_version" and name in available
        ]
        for batch in source.iter_batches(batch_size=count, columns=columns):
            records.extend(batch.to_pylist()[: count - len(records)])
            if len(records) >= count:
                break
    rows = []
    for source in records:
        row = {
            key: source.get(key)
            if source.get(key) is not None
            else (0 if kind == "integer" else "")
            for key, kind in KINDS.items()
            if key != "qual_version"
        }
        row["hn_id"] = int(row["hn_id"])
        row["qual_version"] = 0
        row["body"] = (
            "database " + str(row.get("body") or row.get("title") or "")[:1024]
        )
        rows.append(row)
    if len(rows) < 2 or len({r["hn_id"] for r in rows}) != len(rows):
        raise ValueError("fixture needs distinct HN IDs")
    rows[0]["body"] = "database hnqualificationanchor " + rows[0]["body"]
    return rows


class Run:
    def __init__(self, args):
        self.args = args
        self.state = args.state.resolve()
        self.started = time.monotonic()
        self.deadline = self.started + args.max_seconds
        self.stop = threading.Event()
        self.lock = threading.RLock()
        self.process = None
        self.process_lock = threading.Lock()
        self.server_log = None
        self.token = None
        self.samples = []
        self.failures = []
        self.metadata = []
        self.transactions = {name: [] for name in TABLES}
        self.expected = {}
        self.known = {}
        self.catalog_seen = set()
        self.publication_seen = set()
        self.generations = set()
        self.observer_requests = 0
        self.request_sequence = 0
        self.vacuum_jobs = {}
        self.anchor_id = None
        self.namespace_checked = False
        self.last_inventory = 0
        self.pending_vacuum = {}
        self.snapshot_lists = {}
        self.continuous_seconds = 0
        self.deletions_during_continuous = 0
        self.events = (self.state / "events.jsonl").open("a", buffering=1)
        self.config, self.warehouses = configuration(args)
        if args.provider == "local":
            (self.state / "artifact-store" / "native").mkdir(parents=True)
            for name in TABLES:
                (self.state / "source-store" / "warehouse" / name).mkdir(parents=True)
        atomic_json(self.state / "config.json", self.config)
        self.base = f"http://127.0.0.1:{args.port}/db/v1"
        self.threads = []

    def event(self, kind, **values):
        with self.lock:
            if self.events.closed:
                return
            self.events.write(
                json.dumps(
                    {
                        "seconds": round(time.monotonic() - self.started, 3),
                        "kind": kind,
                        **values,
                    }
                )
                + "\n"
            )

    def checkpoint(self):
        if time.monotonic() >= self.deadline:
            raise TimeoutError("qualification wall-clock limit reached")

    def call(self, method, route, body=None, timeout=60):
        self.checkpoint()
        started = time.monotonic()
        with self.lock:
            self.request_sequence += 1
            request_id = self.request_sequence
        self.event("request_start", request_id=request_id, method=method, route=route)
        request = Request(
            self.base + route,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type": "application/json"},
            method=method,
        )
        try:
            with open_request(
                request, timeout=max(0.1, min(timeout, self.deadline - started))
            ) as response:
                result = json.load(response) if response.status != 204 else None
        except HTTPError as error:
            failure = RequestFailure.http(
                method,
                route,
                error.code,
                error.read(65536),
                (time.monotonic() - started) * 1000,
            )
            self.event("request_failure", request_id=request_id, **failure.record)
            raise failure from None
        except (URLError, TimeoutError, ConnectionError, OSError) as error:
            failure = RequestFailure.transport(
                method, route, error, (time.monotonic() - started) * 1000
            )
            self.event("request_failure", request_id=request_id, **failure.record)
            raise failure from None
        self.checkpoint()
        self.event(
            "request",
            request_id=request_id,
            method=method,
            route=route,
            elapsed_ms=round((time.monotonic() - started) * 1000, 3),
        )
        return result

    def inventory(self):
        items = []
        token = None
        while True:
            self.checkpoint()
            query = (
                "prefix="
                + quote(self.args.prefix.rstrip("/") + "/", safe="")
                + "&maxResults=1000&fields=nextPageToken,items(name,size)"
            )
            if token:
                query += "&pageToken=" + quote(token, safe="")
            request = Request(
                "https://storage.googleapis.com/storage/v1/b/"
                + quote(self.args.bucket, safe="")
                + "/o?"
                + query,
                headers={"Authorization": "Bearer " + self.token},
            )
            with open_request(
                request, timeout=max(0.1, min(30, self.deadline - time.monotonic()))
            ) as response:
                result = json.load(response)
            self.observer_requests += 1
            items.extend(result.get("items", []))
            total_bytes = sum(int(item["size"]) for item in items)
            if (
                len(items) > self.args.max_objects
                or total_bytes > self.args.max_storage_bytes
            ):
                raise AssertionError("qualification object storage envelope exceeded")
            token = result.get("nextPageToken")
            if not token:
                return {"objects": len(items), "bytes": total_bytes}

    def start(self):
        self.checkpoint()
        env = dict(os.environ)
        if self.args.provider == "gcs":
            self.token = gcs_bearer(self.args.project, 3300)
            env["HN_GCS_BEARER"] = self.token
            if not self.namespace_checked:
                existing = self.inventory()
                if existing["objects"]:
                    raise ValueError("qualification GCS prefix must be empty")
                self.namespace_checked = True
                self.event("fresh_namespace", **existing)
        with self.process_lock:
            self.checkpoint()
            require_available_ports((self.args.port, self.args.port + 1))
            self.checkpoint()
            self.server_log = (self.state / "server.log").open("a")
            self.process = subprocess.Popen(
                [
                    str(self.args.binary.resolve()),
                    "standalone",
                    "--config",
                    str(self.state / "config.json"),
                    "--data-dir",
                    str(self.state / "data"),
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(self.args.port),
                    "--health",
                    "true",
                    "--health-port",
                    str(self.args.port + 1),
                    "--auth",
                    "false",
                    "--models-dir",
                    str(self.state / "empty-models"),
                    "--process-memory-budget-mb",
                    "4096",
                ],
                env=env,
                stdout=self.server_log,
                stderr=subprocess.STDOUT,
            )
        deadline = min(self.deadline, time.monotonic() + 90)
        while True:
            self.checkpoint()
            if self.process.poll() is not None:
                raise RuntimeError("engine exited; retained server log has diagnostics")
            try:
                self.call("GET", "/tables", timeout=5)
                return
            except RequestFailure:
                if time.monotonic() >= deadline:
                    raise TimeoutError("engine startup failed") from None
                time.sleep(0.2)

    def stop_server(self):
        with self.process_lock:
            if self.process and self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(30)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()
            if self.server_log:
                self.server_log.close()

    def deadline_watchdog(self):
        # This owner can terminate the engine even if foreground observation is
        # stuck in a resolver outside the socket timeout. Serialize with process
        # publication so an expiring run cannot race a restart into a new daemon.
        def expire():
            self.stop.set()
            self.stop_server()

        timer = threading.Timer(max(0, self.deadline - time.monotonic()), expire)
        timer.daemon = True
        timer.start()
        return timer

    def metrics(self):
        with open_request(
            f"http://127.0.0.1:{self.args.port + 1}/metrics", timeout=15
        ) as response:
            return parse_lake_metrics(response.read().decode())

    def catalog(self, name):
        return self.call("GET", f"/tables/{name}/lake/catalog")

    def transaction(self, name, changes):
        with self.lock:
            previous = self.transactions[name][-1] if self.transactions[name] else None
            number = len(self.transactions[name]) + 1
            body = {
                "batch_id": f"qualification-{number}",
                "source": "hn-qualification",
                "epoch": "fixed-1",
                "checkpoint": str(number),
                "expected_checkpoint": str(number - 1) if previous else None,
                "key_fields": ["hn_id"],
                "changes": changes,
            }
            record = {
                "number": number,
                "submitted": time.monotonic(),
                "changes": len(changes),
            }
            for change in changes:
                if name == TABLES[1] and change["op"] == "upsert":
                    row = change["row"]
                    self.known[(row["hn_id"], row["qual_version"])] = dict(row)
            atomic_json(self.state / (name + "-pending.json"), body)
        while True:
            try:
                receipt = self.call("POST", f"/tables/{name}/lake/changes", body)
                break
            except RequestFailure as failure:
                if failure.record.get("http_status", 503) not in (
                    429,
                    500,
                    502,
                    503,
                    504,
                ):
                    raise
                self.checkpoint()
                if self.stop.is_set():
                    raise TimeoutError(
                        "pending transaction retained after cancellation"
                    ) from None
                time.sleep(1)
        if receipt.get("state") != "accepted" or not isinstance(
            receipt.get("wal_lsn"), int
        ):
            raise AssertionError("change request did not return a durable receipt")
        with self.lock:
            record.update(accepted=time.monotonic(), receipt=receipt)
            self.transactions[name].append(record)
            if name == TABLES[1]:
                for change in changes:
                    key = change["row"]["hn_id"]
                    if change["op"] == "delete":
                        self.expected.pop(key, None)
                    else:
                        self.expected[key] = dict(change["row"])
        self.event("accepted", table=name, number=number, wal_lsn=receipt["wal_lsn"])
        (self.state / (name + "-pending.json")).unlink()
        return record

    def wait_coverage(self, name, lsn, limit=300):
        deadline = min(self.deadline, time.monotonic() + limit)
        while True:
            result = self.catalog(name)
            coverage = int(
                result["metadata"].get("properties", {}).get("antfly.wal.coverage", 0)
            )
            if coverage >= lsn:
                now = time.monotonic()
                with self.lock:
                    for transaction in self.transactions[name]:
                        if transaction["receipt"]["wal_lsn"] <= coverage:
                            transaction.setdefault("catalog_observed", now)
                return result
            if time.monotonic() >= deadline:
                raise TimeoutError("catalog coverage failed to catch up")
            time.sleep(0.25 if self.args.provider == "local" else 1)

    def object_bytes(self, name, uri):
        self.checkpoint()
        parsed = urlsplit(uri)
        if self.args.provider == "local":
            if (
                parsed.scheme != "object"
                or parsed.netloc != "antfly"
                or ".." in Path(parsed.path).parts
            ):
                raise ValueError("unexpected local catalog object URI")
            root = (
                self.state
                / "source-store"
                / "warehouse"
                / name
                / "buckets"
                / "antfly"
                / "objects"
            )
            with (root / parsed.path.lstrip("/")).open("rb") as source:
                header = source.read(84)
                if len(header) != 84 or header[:8] != b"AFOBJ001":
                    raise ValueError("invalid filesystem object envelope")
                size, content_type_size = struct.unpack("<QI", header[8:20])
                if size > 16 * 1024 * 1024 or content_type_size > 65536:
                    raise ValueError("observer object size limit exceeded")
                source.seek(content_type_size, 1)
                payload = source.read(size + 1)
            if (
                len(payload) != size
                or hashlib.sha256(payload).hexdigest().encode() != header[20:]
            ):
                raise ValueError("filesystem object length or digest mismatch")
            return payload
        if (
            parsed.scheme != "gs"
            or parsed.netloc != self.args.bucket
            or not parsed.path.lstrip("/").startswith(
                self.args.prefix + "/warehouse/" + name + "/"
            )
        ):
            raise ValueError("catalog observer escaped qualification prefix")
        request = Request(
            "https://storage.googleapis.com/storage/v1/b/"
            + quote(self.args.bucket, safe="")
            + "/o/"
            + quote(parsed.path.lstrip("/"), safe="")
            + "?alt=media",
            headers={"Authorization": "Bearer " + self.token},
        )
        self.observer_requests += 1
        with open_request(
            request, timeout=max(0.1, min(30, self.deadline - time.monotonic()))
        ) as response:
            value = response.read(16 * 1024 * 1024 + 1)
        if len(value) > 16 * 1024 * 1024:
            raise ValueError("observer manifest-list size limit exceeded")
        return value

    def observe(self, name, inspect_manifest=False):
        loaded = self.catalog(name)
        metadata = loaded["metadata"]
        now = time.monotonic()
        snapshots = metadata.get("snapshots", [])
        coverage = int(metadata.get("properties", {}).get("antfly.wal.coverage", 0))
        if len(metadata.get("metadata-log", [])) > 100:
            raise AssertionError("catalog metadata-log exceeded configured retention")
        record = {
            "table": name,
            "seconds": now - self.started,
            "coverage": coverage,
            "snapshots": len(snapshots),
            "metadata_log": len(metadata.get("metadata-log", [])),
            "metadata_bytes": len(json.dumps(metadata).encode()),
            "current_snapshot": metadata.get("current-snapshot-id"),
        }
        with self.lock:
            self.catalog_seen.update((name, item["snapshot-id"]) for item in snapshots)
            self.snapshot_lists.update(
                {
                    (name, item["snapshot-id"]): item["manifest-list"]
                    for item in snapshots
                }
            )
            for transaction in self.transactions[name]:
                if transaction["receipt"]["wal_lsn"] <= coverage:
                    transaction.setdefault("catalog_observed", now)
        if inspect_manifest and snapshots:
            current = next(
                s
                for s in snapshots
                if s["snapshot-id"] == metadata["current-snapshot-id"]
            )
            try:
                entries = manifest_entries(
                    self.object_bytes(name, current["manifest-list"])
                )
            except (FileNotFoundError, HTTPError) as error:
                if isinstance(error, HTTPError) and error.code != 404:
                    raise
                # This direct diagnostic reader holds no engine snapshot lease.
                # Concurrent vacuum may retire its snapshot after the catalog GET.
                record["unleased_observer_race"] = True
            else:
                record["manifest_count"] = len(entries)
                record["manifest_bytes"] = sum(e.manifest_length for e in entries)
                for kind, content in (("data", 0), ("delete", 1)):
                    group = [entry for entry in entries if entry.content == content]
                    record[kind + "_manifests"] = len(group)
                    counts = [
                        (entry.added_files_count, entry.existing_files_count)
                        for entry in group
                    ]
                    record["reported_live_" + kind + "_files"] = (
                        sum(added + existing for added, existing in counts)
                        if all(
                            added is not None and existing is not None
                            for added, existing in counts
                        )
                        else None
                    )
        with self.lock:
            self.metadata.append(record)
        self.event("catalog", **record)
        return record

    def index_status(self):
        result = self.call("GET", f"/tables/{TABLES[1]}/indexes/body_text")["status"]
        published = result.get("published_revision")
        if published is not None:
            self.generations.add(published)
        self.event("index_status", status=result)
        return result

    def query(self, visibility):
        started = time.monotonic()
        body = {
            "full_text_search": {"match": "database", "field": "body"},
            "full_text_index": "body_text",
            "fields": list(KINDS),
            "highlight": {"fields": ["body"]},
            "limit": self.args.seed_rows + 1,
            "order_by": [{"field": "_score", "desc": True}],
            "read_requirements": {"visibility": visibility},
        }
        result = self.call("POST", f"/tables/{TABLES[1]}/query", body)["responses"][0]
        now = time.monotonic()
        hits = result["hits"]["hits"]
        if len({int(hit["_source"]["hn_id"]) for hit in hits}) != len(hits):
            raise AssertionError("query returned duplicate HN keys")
        with self.lock:
            for hit in hits:
                row = hit["_source"]
                key = (int(row["hn_id"]), int(row["qual_version"]))
                known = self.known.get(key)
                if known is None or any(
                    row.get(field) != known.get(field) for field in KINDS
                ):
                    raise AssertionError("query returned an unknown row image")
                if visibility == "published":
                    self.publication_seen.add(key)
                    if key[1] > 0 and key[1] <= len(self.transactions[TABLES[1]]):
                        self.transactions[TABLES[1]][key[1] - 1].setdefault(
                            "publication_observed", now
                        )
        if self.anchor_id not in {int(hit["_source"]["hn_id"]) for hit in hits}:
            raise AssertionError("query lost the stable seed anchor")
        if hits and not all(h.get("_highlights", {}).get("body") for h in hits):
            raise AssertionError("query omitted requested highlights")
        return {
            "row_versions": {
                str(hit["_source"]["hn_id"]): int(hit["_source"]["qual_version"])
                for hit in hits
            },
            "visibility": visibility,
            "elapsed_ms": (now - started) * 1000,
            "hits": len(hits),
        }

    def record_vacuum(self, name, request, result):
        # Managed vacuum reports work done by this turn, not a job-wide total.
        # Lost responses can censor deletions; independent object checks remain
        # the proof of physical removal rather than treating this as billing.
        with self.lock:
            previous = self.vacuum_jobs.get((name, request["operation_id"]), {})
            self.vacuum_jobs[(name, request["operation_id"])] = {
                "table": name,
                "operation_id": request["operation_id"],
                "result": result,
                "acknowledged_turns": previous.get("acknowledged_turns", 0) + 1,
                "deleted_objects_observed": previous.get("deleted_objects_observed", 0)
                + result.get("deleted_objects", 0),
            }

    def worker(self, function):
        try:
            function()
        except Exception as error:
            with self.lock:
                self.failures.append(
                    {
                        "type": type(error).__name__,
                        "code": str(error)
                        if isinstance(error, (AssertionError, TimeoutError))
                        else type(error).__name__,
                    }
                )
            self.stop.set()
            self.stop_server()

    def continuous(self, rows):
        started = time.monotonic()
        end = min(self.deadline - 120, time.monotonic() + self.args.duration_seconds)

        def produce():
            number = 1
            while (
                not self.stop.is_set()
                and time.monotonic() < end
                and number <= self.args.max_updates
            ):
                row = dict(rows[1 + (number - 1) % (len(rows) - 1)])
                row["qual_version"] = len(self.transactions[TABLES[1]]) + 1
                row["body"] = (
                    "database hnrevision" + str(row["qual_version"]) + " " + row["body"]
                )
                changes = (
                    [{"op": "delete", "row": {"hn_id": row["hn_id"]}}]
                    if number % 5 == 0
                    else [{"op": "upsert", "row": row}]
                )
                self.transaction(TABLES[1], changes)
                number += 1
                self.stop.wait(self.args.update_interval)

        def churn():
            for index in range(self.args.max_updates):
                if self.stop.is_set() or time.monotonic() >= end:
                    return
                row = dict(rows[0], qual_version=index + self.args.history_commits + 1)
                receipt = self.transaction(TABLES[0], [{"op": "upsert", "row": row}])
                self.wait_coverage(TABLES[0], receipt["receipt"]["wal_lsn"])
                self.stop.wait(max(3, self.args.update_interval))

        def search():
            while not self.stop.is_set() and time.monotonic() < end:
                for visibility in ("published", "accepted"):
                    try:
                        sample = self.query(visibility)
                    except RequestFailure as error:
                        sample = {"visibility": visibility, "failure": error.record}
                        if error.record.get("http_status", 503) in (400, 401, 403, 422):
                            raise
                    with self.lock:
                        self.samples.append(sample)
                self.stop.wait(self.args.query_interval)

        def maintain():
            sequence = 0
            pending = self.pending_vacuum
            while not self.stop.is_set() and time.monotonic() < end:
                for name in TABLES:
                    request = pending.setdefault(
                        name,
                        {
                            "action": "vacuum",
                            "operation_id": f"continuous-vacuum-{sequence}-{name}",
                            "dry_run": False,
                            "exclusive_ownership": True,
                            "retain_ms": 600000,
                            "keep_latest": 2,
                            "max_deleted": 32,
                        },
                    )
                    atomic_json(self.state / "pending-vacuum.json", pending)
                    try:
                        result = self.call(
                            "POST", f"/tables/{name}/lake/maintenance", request
                        )
                    except RequestFailure as error:
                        if error.record.get("http_status", 503) in (
                            409,
                            429,
                            500,
                            502,
                            503,
                            504,
                        ):
                            self.event("vacuum_retry", table=name, failure=error.record)
                            continue
                        raise
                    self.record_vacuum(name, request, result)
                    self.event("vacuum", table=name, request=request, result=result)
                    if result.get("complete"):
                        del pending[name]
                        sequence += 1
                    atomic_json(self.state / "pending-vacuum.json", pending)
                self.stop.wait(self.args.maintenance_interval)

        def observe():
            while not self.stop.is_set() and time.monotonic() < end:
                for name in TABLES:
                    self.observe(name, True)
                self.index_status()
                self.event("metrics", metrics=self.metrics())
                self.event("host", load=os.getloadavg())
                if (
                    self.args.provider == "gcs"
                    and time.monotonic() - self.last_inventory >= 60
                ):
                    self.event("inventory", **self.inventory())
                    self.last_inventory = time.monotonic()
                self.stop.wait(15)

        self.threads = [
            threading.Thread(target=self.worker, args=(fn,), daemon=True)
            for fn in (
                produce,
                churn,
                *([search] * self.args.query_workers),
                maintain,
                observe,
            )
        ]
        for thread in self.threads:
            thread.start()
        for thread in self.threads:
            thread.join(timeout=max(0, self.deadline - time.monotonic()))
        if any(thread.is_alive() for thread in self.threads):
            self.stop.set()
            self.stop_server()
            raise TimeoutError("qualification workers exceeded wall-clock limit")
        self.continuous_seconds = time.monotonic() - started
        self.deletions_during_continuous = sum(
            job["deleted_objects_observed"] for job in self.vacuum_jobs.values()
        )

    def run(self):
        report = {
            "complete": False,
            "provider": self.args.provider,
            "arguments": {
                key: str(value) if isinstance(value, Path) else value
                for key, value in vars(self.args).items()
            },
            "engine_commit": self.args.engine_commit,
            "binary_sha256": sha256_file(self.args.binary),
            "host_timing_label": "contended developer host; not a production SLO or same-region qualification",
        }
        watchdog = self.deadline_watchdog()
        try:
            rows = fixture(self.args.fixture, self.args.seed_rows)
            self.anchor_id = rows[0]["hn_id"]
            report["fixture_sha256"] = sha256_file(self.args.fixture)
            report["seed_sha256"] = hashlib.sha256(
                json.dumps(rows, sort_keys=True).encode()
            ).hexdigest()
            self.start()
            before = self.metrics()
            for name in TABLES:
                self.call(
                    "POST",
                    "/tables/" + name,
                    table_definition(name, self.warehouses[name], name == TABLES[1]),
                )
                self.call(
                    "POST", f"/tables/{name}/lake/catalog", catalog_definition(name)
                )
                seed = rows if name == TABLES[1] else rows[:1]
                record = self.transaction(
                    name, [{"op": "upsert", "row": row} for row in seed]
                )
                self.wait_coverage(name, record["receipt"]["wal_lsn"])
            for index in range(self.args.history_commits):
                row = dict(rows[0], qual_version=index + 1)
                record = self.transaction(TABLES[0], [{"op": "upsert", "row": row}])
                self.wait_coverage(TABLES[0], record["receipt"]["wal_lsn"])
                if index % 10 == 0 or 96 <= index <= 104:
                    self.observe(TABLES[0], True)
            before_burst = self.observe(TABLES[0], True)
            for index in range(self.args.burst_transactions):
                row = dict(rows[0], qual_version=self.args.history_commits + index + 1)
                last = self.transaction(TABLES[0], [{"op": "upsert", "row": row}])
            if self.args.burst_transactions:
                self.wait_coverage(TABLES[0], last["receipt"]["wal_lsn"])
            after_burst = self.observe(TABLES[0], True)
            report["burst"] = {
                "transactions": self.args.burst_transactions,
                "snapshots_created": after_burst["snapshots"]
                - before_burst["snapshots"],
            }
            deadline = min(self.deadline, time.monotonic() + 600)
            while not self.index_status().get("readiness", {}).get("queryable"):
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        "initial indexed publication did not become ready"
                    )
                time.sleep(1)
            report["initial_query"] = self.query("published")
            self.continuous(rows)
            if self.failures:
                raise AssertionError("background qualification worker failed")
            for name in TABLES:
                last = self.transactions[name][-1]["receipt"]["wal_lsn"]
                self.wait_coverage(name, last, 300)
                self.observe(name, True)
            final = self.call(
                "POST",
                "/sql",
                {
                    "statement": f"SELECT hn_id, qual_version FROM {TABLES[1]} ORDER BY hn_id",
                    "read_requirements": {"visibility": "accepted"},
                },
            )
            actual = {int(row[0]): int(row[1]) for row in final["rows"]}
            expected = {key: row["qual_version"] for key, row in self.expected.items()}
            if actual != expected:
                raise AssertionError(
                    "final accepted rows disagree with acknowledged changes"
                )
            report["final_rows_verified"] = len(actual)
            publication_started = time.monotonic()
            publication_deadline = min(self.deadline, publication_started + 300)
            while True:
                try:
                    published = self.query("published")
                    if published["row_versions"] == {
                        str(key): version for key, version in expected.items()
                    }:
                        break
                except RequestFailure as error:
                    if error.record.get("http_status", 503) not in (
                        409,
                        429,
                        500,
                        502,
                        503,
                        504,
                    ):
                        raise
                if time.monotonic() >= publication_deadline:
                    raise TimeoutError(
                        "automatic searchable publication did not catch up"
                    )
                time.sleep(1)
            report["final_publication_catchup_seconds"] = (
                time.monotonic() - publication_started
            )
            time.sleep(6.1)
            report["serving_api_metrics"] = self.metrics()
            report["object_io"] = publication_profile(
                before,
                report["serving_api_metrics"],
                time.monotonic() - self.started,
                True,
            )
            report["object_io"]["scope"] = (
                "API-runtime logical operations from before registration through continuous writes, queries, catalog observations and maintenance; ends before restart. Includes background/nested calls, excludes direct source-observer reads and separately owned runtimes. Not publication-only or billing."
            )
            self.stop_server()
            self.start()
            report["restart_query"] = self.query("accepted")
            if report["restart_query"]["row_versions"] != {
                str(key): version for key, version in expected.items()
            }:
                raise AssertionError("restart changed accepted row images")
            maintenance_deadline = min(self.deadline, time.monotonic() + 300)
            report["resumed_vacuum_operations"] = [
                request["operation_id"] for request in self.pending_vacuum.values()
            ]
            while self.pending_vacuum:
                for name, request in list(self.pending_vacuum.items()):
                    result = self.call(
                        "POST", f"/tables/{name}/lake/maintenance", request
                    )
                    self.record_vacuum(name, request, result)
                    self.event(
                        "resumed_vacuum", table=name, request=request, result=result
                    )
                    if result.get("complete"):
                        del self.pending_vacuum[name]
                    atomic_json(self.state / "pending-vacuum.json", self.pending_vacuum)
                if time.monotonic() >= maintenance_deadline:
                    raise TimeoutError("vacuum did not finish bounded restart recovery")
                if self.pending_vacuum:
                    time.sleep(1)
            after_vacuum = self.query("accepted")
            if after_vacuum["row_versions"] != {
                str(key): version for key, version in expected.items()
            }:
                raise AssertionError("vacuum changed accepted row images")
            deletion_checks = []
            for name in TABLES:
                live = self.catalog(name)["metadata"].get("snapshots", [])
                live_ids = {item["snapshot-id"] for item in live}
                candidates = [
                    (sid, uri)
                    for (table, sid), uri in self.snapshot_lists.items()
                    if table == name and sid not in live_ids
                ][:8]
                for snapshot_id, uri in candidates:
                    absent = False
                    try:
                        self.object_bytes(name, uri)
                    except FileNotFoundError:
                        absent = True
                    except HTTPError as error:
                        if error.code != 404:
                            raise
                        absent = True
                    deletion_checks.append(
                        {"table": name, "snapshot_id": snapshot_id, "absent": absent}
                    )
            report["independent_deletion_checks"] = deletion_checks
            time.sleep(6.1)
            report["restart_api_metrics"] = self.metrics()
            if self.args.provider == "gcs":
                report["final_inventory"] = self.inventory()
            report["complete"] = True
        except Exception as error:
            report["error"] = {
                "type": type(error).__name__,
                "code": str(error)
                if isinstance(error, (AssertionError, TimeoutError))
                else type(error).__name__,
            }
            raise
        finally:
            watchdog.cancel()
            self.stop.set()
            self.stop_server()
            # A resolver stalled outside Python's socket timeout must not keep
            # the engine producing objects past the workload deadline. Worker
            # threads retain their own Python state but cannot outlive process
            # teardown or admit another engine call after deadline expiry.
            drain_deadline = time.monotonic() + 5
            for thread in self.threads:
                thread.join(timeout=max(0, drain_deadline - time.monotonic()))
            report.update(
                elapsed_seconds=time.monotonic() - self.started,
                failures=self.failures,
                samples=self.samples,
                metadata=self.metadata,
                observed_snapshots=len(self.catalog_seen),
                observed_published_revisions=len(self.generations),
                observer_requests=self.observer_requests,
                continuous_seconds=self.continuous_seconds,
                deletions_during_continuous=self.deletions_during_continuous,
            )
            report["query_distributions_ms"] = {
                visibility: distribution(
                    [
                        s["elapsed_ms"]
                        for s in self.samples
                        if s.get("visibility") == visibility and "elapsed_ms" in s
                    ]
                )
                for visibility in ("published", "accepted")
            }
            report["transactions"] = {
                name: [
                    {k: v for k, v in item.items() if k not in ("changes",)}
                    for item in values
                ]
                for name, values in self.transactions.items()
            }
            report["vacuum_jobs"] = list(self.vacuum_jobs.values())
            report["deleted_objects_observed"] = sum(
                job["deleted_objects_observed"] for job in self.vacuum_jobs.values()
            )
            report["query_failures"] = sum(
                "failure" in sample for sample in self.samples
            )
            report["lag_scope"] = (
                "First observed catalog/publication visibility after acceptance ACK; sampling gives upper bounds. Superseded updates and deletes without an observed image are censored, not zero lag."
            )
            report["lag_seconds"] = {
                name: {
                    phase: distribution(
                        [
                            item[phase] - item["accepted"]
                            for item in values
                            if phase in item
                        ]
                    )
                    for phase in ("catalog_observed", "publication_observed")
                }
                for name, values in self.transactions.items()
            }
            history = [
                item["manifest_count"]
                for item in self.metadata
                if item["table"] == TABLES[0] and "manifest_count" in item
            ]
            report["checks"] = {
                "final_rows": bool(report.get("complete")),
                "queries_succeeded": report["query_failures"] == 0
                and all(
                    report["query_distributions_ms"][visibility] is not None
                    for visibility in ("published", "accepted")
                ),
                "retention_window_exercised": self.continuous_seconds >= 660
                if self.args.duration_seconds >= 660
                else None,
                "manifest_merge_observed": any(
                    right < left for left, right in zip(history, history[1:])
                )
                if self.args.history_commits >= 100
                else None,
                "group_commit_observed": report.get("burst", {}).get(
                    "snapshots_created", self.args.burst_transactions
                )
                < self.args.burst_transactions
                if self.args.burst_transactions >= 2
                else None,
                "vacuum_progress_during_continuous": self.deletions_during_continuous
                > 0
                if self.args.duration_seconds >= 660
                else None,
                "vacuum_deleted_objects": report["deleted_objects_observed"] > 0
                if self.args.duration_seconds >= 660
                else None,
                "independent_deletion_observed": any(
                    item["absent"]
                    for item in report.get("independent_deletion_checks", [])
                )
                if self.args.duration_seconds >= 660
                else None,
            }
            report["full_profile"] = (
                self.args.history_commits >= 100
                and self.args.burst_transactions >= 2
                and self.args.duration_seconds >= 660
            )
            report["qualified"] = report["full_profile"] and all(
                value is True for value in report["checks"].values()
            )
            atomic_json(self.state / "report.json", report)
            self.events.close()
        return report


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--engine-commit", required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--provider", choices=("local", "gcs"), default="local")
    parser.add_argument("--project", default="antfly-dev-01")
    parser.add_argument("--bucket")
    parser.add_argument("--prefix")
    parser.add_argument("--port", type=int, default=19197)
    parser.add_argument("--seed-rows", type=int, default=128)
    parser.add_argument("--history-commits", type=int, default=120)
    parser.add_argument("--burst-transactions", type=int, default=32)
    parser.add_argument("--duration-seconds", type=int, default=780)
    parser.add_argument("--max-seconds", type=int, default=2700)
    parser.add_argument("--max-updates", type=int, default=256)
    parser.add_argument("--max-objects", type=int, default=50000)
    parser.add_argument("--max-storage-bytes", type=int, default=1024**3)
    parser.add_argument("--update-interval", type=float, default=3)
    parser.add_argument("--query-interval", type=float, default=2)
    parser.add_argument("--query-workers", type=int, default=1)
    parser.add_argument("--maintenance-interval", type=float, default=15)
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()
    if args.provider == "gcs" and (
        not args.bucket
        or not args.prefix
        or not re.fullmatch(r"hn-qualification/[A-Za-z0-9_/-]+", args.prefix)
        or str(PurePosixPath(args.prefix)) != args.prefix
    ):
        parser.error(
            "GCS requires an existing bucket and a fresh hn-qualification prefix"
        )
    if not re.fullmatch(r"[0-9a-f]{40}", args.engine_commit):
        parser.error("engine-commit must be the full source revision")
    if args.run and (not args.binary.is_file() or not args.fixture.is_file()):
        parser.error("run requires an existing engine binary and Parquet fixture")
    if (
        not 1024 <= args.port <= 65534
        or not 1 <= args.query_workers <= 8
        or args.seed_rows < 2
        or args.seed_rows > 128
        or not 1 <= args.history_commits <= 256
        or not 0 <= args.burst_transactions <= 64
        or args.max_updates < 1
        or args.max_updates > 512
        or args.max_seconds < 120
        or args.max_seconds > 2700
        or not 1 <= args.duration_seconds <= args.max_seconds - 120
        or not 1 <= args.max_objects <= 50000
        or not 1 <= args.max_storage_bytes <= 1024**3
        or min(args.update_interval, args.query_interval, args.maintenance_interval)
        <= 0
    ):
        parser.error("invalid qualification bounds")
    return args


def main():
    args = arguments()
    if not args.run:
        config, warehouses = configuration(args)
        print(
            json.dumps(
                {
                    "config": config,
                    "warehouses": warehouses,
                    "arguments": {
                        key: str(value) if isinstance(value, Path) else value
                        for key, value in vars(args).items()
                    },
                },
                indent=2,
            )
        )
        return
    args.state.mkdir(parents=True, exist_ok=False)
    report = Run(args).run()
    if not report["checks"]["queries_succeeded"]:
        raise SystemExit("search failures observed; partial evidence retained")
    if report["full_profile"] and not report["qualified"]:
        raise SystemExit("qualification criteria not met; partial evidence retained")


if __name__ == "__main__":
    main()
