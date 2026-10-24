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

"""Qualify lake queries and publication with real Parquet and explicit cache scope.

Run with the e2e/antfly Python environment. Generated fixtures are deterministic,
bounded during construction, and committed through immutable manifest chunks.
A --plan can instead supply an existing source definition and exact query cases.
No user source is rewritten; refresh runs apply only to generated fixtures.
"""

from __future__ import annotations

import argparse
from contextlib import closing, contextmanager
import decimal
import hashlib
import json
import os
import platform
import re
import shutil
import statistics
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TABLE = "archive_qualification"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def file_digest(path):
    hasher = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


class VerifiedInput:
    """Bind one run to immutable executable/config/collector contents.

    Cheap identity checks bracket launches and measurements. A final content
    check also rejects in-place changes on filesystems with coarse timestamps.
    Nothing is relocated, so native executable-relative resources still resolve.
    """

    def __init__(self, path):
        self.path = Path(path).resolve(strict=True)
        self.identity = self.signature()
        self.sha256 = file_digest(self.path)
        self.verify()

    def signature(self):
        stat = self.path.stat()
        return (
            stat.st_dev,
            stat.st_ino,
            stat.st_size,
            stat.st_mtime_ns,
            stat.st_ctime_ns,
        )

    def verify(self, *, content=False):
        try:
            changed = self.signature() != self.identity
            if content:
                changed = changed or file_digest(self.path) != self.sha256
            changed = changed or self.signature() != self.identity
        except OSError as error:
            raise RuntimeError(
                f"Qualification input unavailable: {self.path}"
            ) from error
        if changed:
            raise RuntimeError(
                f"Qualification input changed during the run: {self.path}"
            )


def verify_inputs(inputs, *, content=False):
    for value in inputs:
        value.verify(content=content)


@contextmanager
def managed_server(factory, binary, config, output):
    server = factory(str(binary), "127.0.0.1", 0, metrics=True, config_path=config)
    try:
        yield server
    finally:
        try:
            server.stop(cleanup_root=False)
        finally:
            try:
                if server.log_path.exists():
                    shutil.copyfile(server.log_path, output / "server.log")
            finally:
                server.tempdir.cleanup()


def effective_indexes(plan):
    """Resolve one declaration set without submitting duplicate index DDL."""
    embedded = plan["definition"].get("indexes", {})
    additional = plan.get("indexes", {})
    if not isinstance(embedded, dict) or not isinstance(additional, dict):
        raise ValueError("Index declarations must be objects keyed by index name")
    for name in embedded.keys() & additional.keys():
        if embedded[name] != additional[name]:
            raise ValueError(
                f"Conflicting embedded/additional index declaration: {name}"
            )
    return {**embedded, **additional}


def readiness_indexes(plan):
    # Relational indexes are schema declarations, rather than index DDL, but
    # queries can require their published revision just as they require text.
    names = dict.fromkeys(effective_indexes(plan))
    for index in plan["definition"].get("schema", {}).get("relational_indexes", []):
        names[index["name"]] = None
    return tuple(names)


def put_object(objects, name, source):
    """Stream an FsStore envelope; publish the complete object atomically."""
    source = Path(source)
    checksum = file_digest(source)
    target = objects / name
    temporary = target.with_name(target.name + ".pending")
    copied = hashlib.sha256()
    try:
        with temporary.open("wb") as out, source.open("rb") as incoming:
            out.write(
                b"AFOBJ001"
                + struct.pack("<QI", source.stat().st_size, 0)
                + checksum.encode()
            )
            for block in iter(lambda: incoming.read(1024 * 1024), b""):
                out.write(block)
                copied.update(block)
        if copied.hexdigest() != checksum:
            raise ValueError("Input changed while copying the frozen Parquet fixture")
        temporary.replace(target)
        return checksum
    finally:
        temporary.unlink(missing_ok=True)


def put_json(objects, name, value):
    temporary = objects / (name + ".json-input")
    try:
        temporary.write_text(canonical(value))
        return put_object(objects, name, temporary)
    finally:
        temporary.unlink(missing_ok=True)


def generate_fixture(
    root, rows, files, row_group, payload_bytes, refresh_every, generation=0
):
    import pyarrow as pa
    import pyarrow.parquet as pq

    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True, exist_ok=True)
    chunk = []
    for part in range(files):
        begin, end = rows * part // files, rows * (part + 1) // files
        path = root / f"writer-{part}.parquet"
        writer = None
        try:
            for start in range(begin, end, row_group):
                ids = list(range(start, min(end, start + row_group)))
                changed = [int(generation > 0 and i % refresh_every == 0) for i in ids]
                table = pa.table(
                    {
                        "id": pa.array(ids, type=pa.int64()),
                        "uniform_key": pa.array([i % 64 for i in ids], type=pa.int64()),
                        "skew_key": pa.array(
                            [0 if i % 10 else i % 64 for i in ids], type=pa.int64()
                        ),
                        "category": ["story" if i % 4 == 0 else "comment" for i in ids],
                        "body": [
                            f"quasar token{i % 257}" + (" updated" if v else "")
                            for i, v in zip(ids, changed)
                        ],
                        "payload": [f"{i:020d}" + "p" * payload_bytes for i in ids],
                        "revision": pa.array(changed, type=pa.int64()),
                    }
                )
                if writer is None:
                    writer = pq.ParquetWriter(
                        path,
                        table.schema,
                        compression="snappy",
                        write_page_index=True,
                        data_page_version="2.0",
                    )
                writer.write_table(table, row_group_size=row_group)
        finally:
            if writer is not None:
                writer.close()
        name = f"part-{generation}-{part}.parquet"
        checksum = put_object(objects, name, path)
        chunk.append(
            {
                "file_id": name,
                "object_uri": f"object://antfly/{name}",
                "etag": checksum,
                "row_count": end - begin,
                "byte_len": path.stat().st_size,
            }
        )
        path.unlink()
    return publish_manifest(root, chunk)


def publish_manifest(root, chunk, source_id="archive-qualification"):
    objects = root / "buckets" / "antfly" / "objects"
    chunk_name = digest(chunk) + ".json"
    encoded = canonical(chunk).encode()
    checksum = put_json(objects, chunk_name, chunk)
    put_json(
        objects,
        "snapshot.parquet-manifest.json",
        {
            "version": 1,
            "source_id": source_id,
            "file_count": len(chunk),
            "chunks": [
                {
                    "uri": f"object://antfly/{chunk_name}",
                    "sha256": checksum,
                    "byte_len": len(encoded),
                    "file_count": len(chunk),
                }
            ],
        },
    )
    return {
        "manifest_uri": (root / "snapshot.parquet-manifest.json").as_uri(),
        "files": chunk,
        "sha256": digest(chunk),
        "parquet_bytes": sum(f["byte_len"] for f in chunk),
    }


def import_fixture(root, paths, source_id):
    import pyarrow.parquet as pq

    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True, exist_ok=True)
    files = []
    for i, path in enumerate(paths):
        name = f"import-{i}.parquet"
        rows = pq.ParquetFile(path).metadata.num_rows
        checksum = put_object(objects, name, path)
        files.append(
            {
                "file_id": name,
                "object_uri": f"object://antfly/{name}",
                "etag": checksum,
                "row_count": rows,
                "byte_len": path.stat().st_size,
            }
        )
    return publish_manifest(root, files, source_id)


def generated_plan(fixture, rows, build_rows):
    definition = {
        "schema": {
            "storage_mode": "relational",
            "base_source": {
                "kind": "external",
                "table_id": "archive-qualification",
                "format": "parquet",
                "object_mutability": "immutable",
                "uri": fixture["manifest_uri"],
                "row_identity": {"key_columns": ["id"]},
            },
        }
    }
    properties = {
        name: {"type": "integer"}
        for name in ("id", "uniform_key", "skew_key", "revision")
    }
    properties.update(
        {name: {"type": "string"} for name in ("category", "body", "payload")}
    )
    properties["id"]["x-antfly-field"] = {"type": "number", "sortable": True}
    properties["body"]["x-antfly-field"] = {"type": "text"}
    properties["payload"]["x-antfly-field"] = {"type": "text", "index": False}
    definition["schema"].update(
        default_type="row",
        document_schemas={
            "row": {"schema": {"type": "object", "properties": properties}}
        },
        relational_indexes=[
            {
                "name": "archive_id",
                "keys": [{"column": "id"}],
                "include_columns": ["category", "revision"],
            }
        ],
    )
    cases = []

    def sql(name, statement, expected):
        cases.append(
            {
                "name": name,
                "statement": statement,
                "expected_rows": [[str(v) for v in expected]],
            }
        )

    sql(
        "scan_aggregate",
        f"SELECT count(*), sum(id) FROM {TABLE}",
        [rows, rows * (rows - 1) // 2],
    )
    sql("id_lookup", f"SELECT id FROM {TABLE} WHERE id = {rows // 2}", [rows // 2])
    sql(
        "broad_filter",
        f"SELECT count(*) FROM {TABLE} WHERE category = 'comment'",
        [rows - (rows + 3) // 4],
    )
    for column in ("uniform_key", "skew_key"):
        # A bounded build relation keeps the result oracle deterministic.
        # Build cardinality independently controls duplicate fan-out.
        counts = [0] * 64
        for i in range(build_rows):
            key = i % 64 if column == "uniform_key" else (0 if i % 10 else i % 64)
            counts[key] += 1
        expected = sum(
            counts[i % 64 if column == "uniform_key" else (0 if i % 10 else i % 64)]
            for i in range(rows)
        )
        sql(
            column + "_join",
            f"SELECT count(*) FROM {TABLE} a JOIN (SELECT {column} FROM {TABLE} WHERE id < {build_rows}) b ON a.{column} = b.{column}",
            [expected],
        )
    cases.append(
        {
            "name": "ordered_text",
            "identity_field": "id",
            "request": {
                "full_text_search": {"match": "quasar", "field": "body"},
                "fields": ["id"],
                "order_by": [{"field": "id"}],
                "limit": min(rows, 20),
                "profile": True,
            },
            "expected_ids": list(range(min(rows, 20))),
        }
    )
    return {
        "version": 1,
        "definition": definition,
        "indexes": {"text": {"type": "full_text", "store_source": True}},
        "cases": cases,
    }


def validate_plan(plan):
    if plan.get("version") != 1 or not isinstance(plan.get("definition"), dict):
        raise ValueError("Plan requires version 1 and a table definition")
    source = plan["definition"].get("schema", {}).get("base_source", {})
    if (
        source.get("kind") != "external"
        or source.get("format") not in ("parquet", "iceberg")
        or not source.get("table_id")
        or not source.get("uri")
    ):
        raise ValueError("Plan requires an external Parquet/Iceberg base_source")
    effective_indexes(plan)
    names = set()
    if not plan.get("cases"):
        raise ValueError("Plan requires at least one case")
    for case in plan["cases"]:
        if (
            not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", case.get("name", ""))
            or case["name"] in names
        ):
            raise ValueError(
                "Case names must be unique lowercase identifier slugs (at most 64 characters)"
            )
        names.add(case["name"])
        if "identity_field" in case and source.get("row_identity", {}).get(
            "key_columns"
        ) != [case["identity_field"]]:
            raise ValueError(
                "A logical identity field must be the declared single-column row identity"
            )
        if ("statement" in case) == ("request" in case):
            raise ValueError(
                "Each case requires exactly one statement or search request"
            )
        if "statement" in case and not case["statement"].lstrip().upper().startswith(
            ("SELECT ", "WITH ")
        ):
            raise ValueError(
                "Benchmark statements must be read-only SELECT/WITH statements"
            )


def result_value(response, identity_field=None):
    # Preserve all semantic response fields, including SQL types, totals,
    # highlights and ordering. Only known timing/profile metadata is excluded.
    result = {
        key: value for key, value in response.items() if key not in ("profile", "took")
    }
    # Retained-cut/continuation tokens contain per-request expiry and authority.
    # Check their presence, not opaque token equality; cursor replay has its own
    # E2Es and is not qualified by a first-page timing comparison.
    for key in ("remote_snapshot", "next_source_cursor", "next_cursor"):
        if key in result:
            result[key] = {"present": bool(result[key])}
    if "hits" in response:
        hits = []
        for hit in response["hits"]["hits"]:
            value = dict(hit)
            if identity_field is not None:
                source = hit.get("_source")
                if not isinstance(source, dict) or identity_field not in source:
                    raise ValueError(
                        "Search results must project the declared logical identity field"
                    )
                value["_id"] = source[identity_field]
            elif "_id" not in hit:
                raise ValueError(
                    "Search results require public IDs or an explicit declared logical identity"
                )
            hits.append(value)
        result["hits"] = {**response["hits"], "hits": hits}
    return result


def verify_case(case, response):
    if "expected_rows" in case and response.get("rows") != case["expected_rows"]:
        raise ValueError(
            f"Incorrect SQL result for {case['name']}: {response.get('rows')}"
        )
    if "expected_ids" in case:
        field = case.get("identity_field", "id")
        actual = [h["_source"][field] for h in response["hits"]["hits"]]
        if actual != case["expected_ids"]:
            raise ValueError(f"Incorrect logical IDs for {case['name']}: {actual}")
    return digest(result_value(response, case.get("identity_field")))


def parse_metrics(text):
    result = {}
    for line in text.splitlines():
        if line.startswith(
            ("antfly_lake_", "antfly_sql_", "antfly_object_", "antfly_artifact_")
        ):
            key, value = line.rsplit(None, 1)
            value = decimal.Decimal(value)
            if value.is_finite():
                result[key] = (
                    int(value) if value == value.to_integral_value() else float(value)
                )
    return result


def metric_deltas(before, after):
    # Gauges are snapshots. Missing series and resets are unknown, never zero.
    return {
        k: after[k] - v
        for k, v in before.items()
        if k.split("{")[0].endswith("_total") and k in after and after[k] >= v
    }


def artifact_inventory(root):
    result = {}
    for path in (root / "artifacts").rglob("*"):
        try:
            if path.is_file():
                result[str(path.relative_to(root))] = path.stat().st_size
        except FileNotFoundError:
            pass  # A concurrent collector can retire an immutable object.
    return result


def artifact_growth(before, after):
    new = {k: v for k, v in after.items() if k not in before}
    return {
        "new_retained_files": len(new),
        "new_retained_bytes": sum(new.values()),
        "retained_bytes_before": sum(before.values()),
        "retained_bytes_after": sum(after.values()),
        "coverage": "immutable files still present at observation; excludes reclaimed files and temporary spill writes",
    }


def process_rss(pid):
    output = subprocess.check_output(
        ["ps", "-axo", "pid=,ppid=,rss="], text=True, timeout=5
    )
    rows = [
        tuple(map(int, line.split()))
        for line in output.splitlines()
        if len(line.split()) == 3
    ]
    owned = {pid}
    while True:
        more = {p for p, parent, _ in rows if parent in owned}
        if more <= owned:
            break
        owned |= more
    values = [rss * 1024 for p, _, rss in rows if p in owned]
    return sum(values) if values else None


class MemorySample:
    def __init__(self, pid):
        self.pid, self.values, self.errors = pid, [], []
        self.stop = threading.Event()

    def sample(self):
        try:
            value = process_rss(self.pid())
            if value is not None:
                self.values.append(value)
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            self.errors.append(type(error).__name__)

    def __enter__(self):
        self.sample()
        self.thread = threading.Thread(target=self.loop, daemon=True)
        self.thread.start()
        return self

    def loop(self):
        while not self.stop.wait(0.05):
            self.sample()

    def __exit__(self, *unused):
        self.stop.set()
        self.thread.join()
        self.sample()

    def report(self):
        return {
            "baseline_bytes": self.values[0] if self.values else None,
            "peak_sampled_bytes": max(self.values) if self.values else None,
            "samples": len(self.values),
            "interval_ms": 50,
            "scope": "server and descendants; sampled RSS, not exact request allocation peak",
            "sampling_errors": sorted(set(self.errors)),
        }


class Client:
    def __init__(self, server, timeout):
        import requests

        self.session = requests.Session()
        self.server, self.timeout = server, timeout

    def call(self, method, path, body=None):
        deadline = time.monotonic() + min(self.timeout, 30)
        while True:
            response = self.session.request(
                method, self.server.api_url + path, json=body, timeout=self.timeout
            )
            value = response.json() if response.content else None
            if (
                response.status_code == 503
                and isinstance(value, dict)
                and value.get("code") == "53300"
                and value.get("retryable") is True
                and value.get("transaction_status") == "idle"
                and method == "POST"
                and path.startswith("/tables/")
            ):
                if time.monotonic() < deadline:
                    time.sleep(0.1)
                    continue
            if not response.ok:
                import requests

                raise requests.HTTPError(
                    f"{method} {path}: {response.status_code} {response.text[:2048]}",
                    response=response,
                )
            return (
                value.get("responses", [value])[0] if isinstance(value, dict) else value
            )

    def metrics(self):
        response = self.session.get(self.server.health_url + "/metrics", timeout=10)
        response.raise_for_status()
        return parse_metrics(response.text)

    def query(self, case):
        return (
            self.call("POST", "/sql", {"statement": case["statement"]})
            if "statement" in case
            else self.call("POST", f"/tables/{TABLE}/query", case["request"])
        )

    def ready(self, indexes, after=None, expected_updated=None):
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                self.call(
                    "POST", "/sql", {"statement": f"SELECT count(*) FROM {TABLE}"}
                )
                states = [
                    self.call("GET", f"/tables/{TABLE}/indexes/{name}")["status"]
                    for name in indexes
                ]
                if any(s.get("readiness", {}).get("state") == "failed" for s in states):
                    raise RuntimeError(f"Index publication failed: {states}")
                revisions = {
                    name: state.get("published_revision")
                    for name, state in zip(indexes, states)
                }
                queryable = all(
                    s.get("readiness", {}).get("queryable")
                    and revisions[name] is not None
                    for name, s in zip(indexes, states)
                )
                advanced = after is None or all(
                    revisions[name] != after[name] for name in indexes
                )
                if queryable and advanced:
                    if expected_updated is not None:
                        value = self.call(
                            "POST",
                            f"/tables/{TABLE}/query",
                            {
                                "full_text_search": {
                                    "match": "updated",
                                    "field": "body",
                                },
                                "limit": 0,
                            },
                        )
                        total = value["hits"]["total"]
                        if isinstance(total, dict):
                            total = total["value"]
                        if total == expected_updated:
                            return revisions
                    else:
                        return revisions
            except self.session_error_types() as error:
                response = getattr(error, "response", None)
                if response is not None and response.status_code not in (
                    409,
                    425,
                    429,
                    503,
                    504,
                ):
                    raise
            if time.monotonic() > deadline:
                raise TimeoutError("Source/index publication did not become queryable")
            time.sleep(0.1)

    @staticmethod
    def session_error_types():
        import requests

        return (requests.HTTPError, requests.ConnectionError)


def measured(client, operation):
    before = client.metrics()
    with MemorySample(
        lambda: client.server.proc.pid if client.server.proc else -1
    ) as memory:
        start = time.perf_counter_ns()
        response = operation()
        elapsed = time.perf_counter_ns() - start
    after = client.metrics()
    return response, {
        "elapsed_ns": elapsed,
        "memory": memory.report(),
        "metrics_delta": metric_deltas(before, after),
        "metrics_after": after,
        "metric_scope": "isolated process interval; can include asynchronous background work",
    }


def run_queries(client, plan, samples, output, inputs=()):
    rows = []
    for case in plan["cases"]:
        expected = None
        for sample in range(samples):
            verify_inputs(inputs)
            boot = time.perf_counter_ns()
            old_pid = client.server.proc.pid if client.server.proc else None
            with MemorySample(
                lambda: (
                    client.server.proc.pid
                    if client.server.proc and client.server.proc.pid != old_pid
                    else -1
                )
            ) as startup_memory:
                client.server.restart()
                verify_inputs(inputs)
                # Health is a distinct listener and may lag public readiness.
                deadline = time.monotonic() + 15
                while True:
                    try:
                        startup = client.metrics()
                        break
                    except client.session_error_types():
                        if time.monotonic() > deadline:
                            raise
                        time.sleep(0.1)
            restart_ns = time.perf_counter_ns() - boot
            for mode in ("process_restarted", "warm"):
                verify_inputs(inputs)
                response, measurement = measured(client, lambda: client.query(case))
                verify_inputs(inputs)
                checksum = verify_case(case, response)
                answer = result_value(response, case.get("identity_field"))
                if expected is None:
                    (output / (case["name"] + "-answer.json")).write_text(
                        json.dumps(answer, indent=2)
                    )
                if expected is not None and checksum != expected:
                    (output / (case["name"] + "-mismatch.json")).write_text(
                        json.dumps(answer, indent=2)
                    )
                    raise ValueError(
                        f"Unequal answers across cache modes for {case['name']}"
                    )
                expected = checksum
                measurement.update(
                    case=case["name"],
                    sample=sample,
                    cache_mode=mode,
                    result_sha256=checksum,
                    profile=response.get("profile"),
                )
                if mode == "process_restarted":
                    measurement.update(
                        restart_ns=restart_ns,
                        startup_metrics_since_boot=startup,
                        startup_memory=startup_memory.report(),
                    )
                rows.append(measurement)
                (output / "samples.json").write_text(json.dumps(rows, indent=2))
        if "statement" in case:
            # A separate diagnostic execution does not change a timed sample's
            # cache preparation or imply provider/parallel profile coverage.
            verify_inputs(inputs)
            response = client.call(
                "POST",
                "/sql",
                {"statement": "EXPLAIN (ANALYZE, FORMAT JSON) " + case["statement"]},
            )
            verify_inputs(inputs)
            (output / (case["name"] + "-explain.json")).write_text(
                json.dumps(response, indent=2)
            )
    return rows


def summaries(samples):
    grouped = {}
    for row in samples:
        grouped.setdefault((row["case"], row["cache_mode"]), []).append(row)

    def available_median(values):
        return statistics.median(values) if all(v is not None for v in values) else None

    return [
        {
            "case": case,
            "cache_mode": mode,
            "samples": len(rows),
            "median_ns": statistics.median(r["elapsed_ns"] for r in rows),
            "cache_provider_reads_median": available_median(
                [
                    r.get("metrics_delta", {}).get(
                        "antfly_lake_cache_provider_reads_total"
                    )
                    for r in rows
                ]
            ),
            "cache_provider_bytes_median": available_median(
                [
                    r.get("metrics_delta", {}).get(
                        "antfly_lake_cache_provider_bytes_total"
                    )
                    for r in rows
                ]
            ),
            "result_sha256": rows[0]["result_sha256"],
        }
        for (case, mode), rows in grouped.items()
    ]


def compare_reports(baseline, candidate):
    for report in (baseline, candidate):
        if report.get("version") != 1 or report.get("complete") is not True:
            raise ValueError("Only complete version-1 reports can be compared")
        if not report.get("source_identity") or not report.get("workload_sha256"):
            raise ValueError(
                "Comparison requires a frozen source fingerprint and workload identity"
            )
    for key in (
        "source_identity",
        "source_identity_assurance",
        "workload_sha256",
        "cache_contract",
        "tool_sha256",
        "platform",
        "server_config_sha256",
    ):
        if baseline.get(key) != candidate.get(key):
            raise ValueError(f"Incompatible benchmark reports: {key}")

    def indexed(report):
        seen, answers = set(), {}
        for row in report["samples"]:
            identity = row["case"], row["cache_mode"], row["sample"]
            if identity in seen or row["elapsed_ns"] <= 0:
                raise ValueError("Duplicate or invalid timing samples")
            seen.add(identity)
            prior = answers.setdefault(row["case"], row["result_sha256"])
            if prior != row["result_sha256"]:
                raise ValueError("Unequal answers across samples/cache modes")
        if not seen:
            raise ValueError("No timing samples")
        return {(r["case"], r["cache_mode"]): r for r in summaries(report["samples"])}

    left, right = indexed(baseline), indexed(candidate)
    if left.keys() != right.keys():
        raise ValueError("Case/cache-mode coverage differs")
    results = []
    for key, before in left.items():
        after = right[key]
        if before["result_sha256"] != after["result_sha256"]:
            raise ValueError(f"Unequal answers for {key}")
        results.append(
            {
                "case": key[0],
                "cache_mode": key[1],
                "baseline_median_ns": before["median_ns"],
                "candidate_median_ns": after["median_ns"],
                "candidate_over_baseline": after["median_ns"] / before["median_ns"],
                "baseline_samples": before["samples"],
                "candidate_samples": after["samples"],
                "minimum_sample_count_met": min(before["samples"], after["samples"])
                >= 3,
            }
        )
    return {
        "version": 1,
        "timing_qualified": all(r["minimum_sample_count_met"] for r in results),
        "environment_scope": "same declared platform and collector; sequential runs are observational, not randomized paired trials",
        "comparisons": results,
    }


def compare_main(argv):
    parser = argparse.ArgumentParser(
        description="Compare compatible, equal-answer lake qualification reports"
    )
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    result = compare_reports(
        json.loads(args.baseline.read_text()), json.loads(args.candidate.read_text())
    )
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
    print(json.dumps(result, indent=2))
    return 0


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "compare":
        return compare_main(argv[1:])
    if argv and argv[0] == "run":
        argv = argv[1:]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument(
        "--server-config",
        type=Path,
        help="Native server configuration for existing provider connections/secret references; contents are not copied into reports",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--case",
        action="append",
        default=[],
        help="Run only the named query case; repeat to select multiple",
    )
    parser.add_argument(
        "--plan",
        type=Path,
        help="Version-1 source definition and read-only case plan; no fixture rewriting",
    )
    parser.add_argument(
        "--parquet",
        type=Path,
        action="append",
        default=[],
        help="Copy an existing raw Parquet file into an isolated fixture; requires --plan; repeat for multiple files",
    )
    parser.add_argument("--rows", type=int, default=65536)
    parser.add_argument("--files", type=int, default=4)
    parser.add_argument("--row-group", type=int, default=4096)
    parser.add_argument("--payload-bytes", type=int, default=2048)
    parser.add_argument("--join-build-rows", type=int, default=16)
    parser.add_argument("--refresh-every", type=int, default=10)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=900)
    args = parser.parse_args(argv)
    if (
        min(
            args.rows,
            args.files,
            args.row_group,
            args.join_build_rows,
            args.refresh_every,
            args.samples,
        )
        < 1
        or args.files > args.rows
        or args.payload_bytes < 0
        or args.timeout <= 0
    ):
        parser.error(
            "Sizes, samples and timeout must be positive; files cannot exceed rows"
        )
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error("Build the Antfly binary first")
    binary_input = VerifiedInput(binary)
    collector_input = VerifiedInput(__file__)
    config_input = VerifiedInput(args.server_config) if args.server_config else None
    inputs = tuple(
        v for v in (binary_input, collector_input, config_input) if v is not None
    )
    config = config_input.path if config_input else None
    args.output_dir.mkdir(parents=True, exist_ok=False)
    output = args.output_dir.resolve()
    fixture = None
    if args.parquet and not args.plan:
        parser.error("--parquet requires --plan with archive-specific query oracles")
    if args.plan:
        plan = json.loads(args.plan.read_text())
        validate_plan(plan)
        if args.parquet:
            fixture = import_fixture(
                output / "fixture",
                args.parquet,
                plan["definition"]["schema"]["base_source"]["table_id"],
            )
            plan["definition"]["schema"]["base_source"]["uri"] = fixture["manifest_uri"]
    else:
        fixture = generate_fixture(
            output / "fixture",
            args.rows,
            args.files,
            args.row_group,
            args.payload_bytes,
            args.refresh_every,
        )
        plan = generated_plan(fixture, args.rows, min(args.rows, args.join_build_rows))
    validate_plan(plan)
    if args.case:
        unknown = set(args.case) - {c["name"] for c in plan["cases"]}
        if unknown:
            parser.error(f"Unknown cases: {sorted(unknown)}")
        plan["cases"] = [c for c in plan["cases"] if c["name"] in args.case]
    (output / "plan.json").write_text(json.dumps(plan, indent=2))
    sys.path.insert(0, str(ROOT / "e2e" / "antfly"))
    from conftest import StandaloneAntflyServer

    report = {
        "version": 1,
        "binary_sha256": binary_input.sha256,
        "tool_sha256": collector_input.sha256,
        "server_config_sha256": config_input.sha256 if config_input else None,
        "input_identity_contract": "identity checked around launches/measurements and content verified before completion",
        "git_head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "platform": {
            "system": platform.platform(),
            "machine": platform.machine(),
            "cpu_count": os.cpu_count(),
            "python": platform.python_version(),
        },
        "cache_contract": {
            "process_restarted": "restart with retained data, persistent cache and OS page cache; startup navigation may warm before readiness",
            "warm": "same query immediately after its process-restarted sample",
            "empty_cache_qualified": False,
        },
        "complete": False,
        "answer_contract": {
            "opaque_continuations": "presence only; token bytes include authority and expiry",
            "cursor_replay_qualified": False,
        },
        "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "transport": {
            "fixture_backend": "filesystem_object_store"
            if fixture
            else "user_supplied_source",
            "remote_transport_qualified": False,
            "cache_counter_scope": "lake serving-cache provider counters; not a complete transport trace or all SQL/provider I/O",
            "sql_profile_coverage": "retain EXPLAIN ANALYZE coverage fields; unavailable values remain null",
        },
        "source_identity": fixture["sha256"]
        if fixture
        else plan.get("source_fingerprint"),
        "source_identity_assurance": "verified_local_file_bytes"
        if fixture
        else "user_declared_snapshot_fingerprint",
        "oracle_coverage": {
            case["name"]: "exact_expected_rows_or_ids"
            if "expected_rows" in case or "expected_ids" in case
            else "cross_sample_repeatability_only"
            for case in plan["cases"]
        },
        "input": fixture,
        "plan_sha256": digest(plan),
        "workload_sha256": digest(
            {
                "cases": plan["cases"],
                "indexes": plan.get("indexes", {}),
                "source": fixture["sha256"]
                if fixture
                else plan.get("source_fingerprint"),
                "definition": {
                    **plan["definition"],
                    "schema": {
                        **plan["definition"]["schema"],
                        "base_source": {
                            k: v
                            for k, v in plan["definition"]["schema"][
                                "base_source"
                            ].items()
                            if k != "uri" or fixture is None
                        },
                    },
                },
            }
        ),
        "profile_scope": "EXPLAIN ANALYZE is a separate execution; its own coverage fields are authoritative",
    }
    try:
        verify_inputs(inputs)
        with managed_server(StandaloneAntflyServer, binary, config, output) as server:
            verify_inputs(inputs)
            client = Client(server, args.timeout)
            with closing(client.session):
                start = time.perf_counter_ns()
                client.call("POST", f"/tables/{TABLE}", plan["definition"])
                indexes = effective_indexes(plan)
                embedded = plan["definition"].get("indexes", {})
                for name, definition in indexes.items():
                    if name not in embedded:
                        client.call(
                            "POST", f"/tables/{TABLE}/indexes/{name}", definition
                        )
                required_indexes = readiness_indexes(plan)
                revisions = client.ready(required_indexes)
                report["initial_publication_ns"] = time.perf_counter_ns() - start
                report["initial_index_revisions"] = revisions
                report["samples"] = run_queries(
                    client, plan, args.samples, output, inputs
                )
                report["summary"] = summaries(report["samples"])
                if fixture is not None and not args.plan:
                    before = artifact_inventory(server.root)

                    def refresh():
                        fixture_start = time.perf_counter_ns()
                        updated = generate_fixture(
                            output / "fixture",
                            args.rows,
                            args.files,
                            args.row_group,
                            args.payload_bytes,
                            args.refresh_every,
                            1,
                        )
                        commit_ns = time.perf_counter_ns() - fixture_start
                        publication_start = time.perf_counter_ns()
                        changed = (
                            args.rows + args.refresh_every - 1
                        ) // args.refresh_every
                        current = client.ready(
                            required_indexes, after=revisions, expected_updated=changed
                        )
                        answer = client.call(
                            "POST",
                            "/sql",
                            {"statement": f"SELECT sum(revision) FROM {TABLE}"},
                        )
                        if answer["rows"] != [[str(changed)]]:
                            raise ValueError(
                                "Refresh result differs from the generated row oracle"
                            )
                        return {
                            "input": updated,
                            "changed_rows": changed,
                            "revisions": current,
                            "fixture_and_commit_ns": commit_ns,
                            "publication_and_verify_ns": time.perf_counter_ns()
                            - publication_start,
                        }

                    refreshed, timing = measured(client, refresh)
                    timing.update(refreshed)
                    timing["artifacts"] = artifact_growth(
                        before, artifact_inventory(server.root)
                    )
                    timing["retained_artifact_over_rewritten_parquet"] = (
                        timing["artifacts"]["new_retained_bytes"]
                        / refreshed["input"]["parquet_bytes"]
                    )
                    timing["includes_fixture_encoding_and_commit"] = True
                    report["refresh"] = timing
        verify_inputs(inputs, content=True)
        report["complete"] = True
    except BaseException as error:
        report["complete"] = False
        report["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        (output / "report.json").write_text(json.dumps(report, indent=2))
    print(
        json.dumps(
            {
                "report": str(output / "report.json"),
                "complete": report.get("complete", False),
                "summary": report.get("summary", []),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
