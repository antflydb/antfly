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

"""Run the real lake query engine against a shaped loopback object store.

All output belongs in an ignored benchmark directory. Synthetic request latency
and shared bandwidth are explicit inputs. This is not real-GCS qualification.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import struct
import sys
import time

import benchmark_lake_archive as archive
from lake_network_store import Network, NetworkStore


@contextmanager
def environment(values):
    prior = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in prior.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def unwrap(path):
    data = path.read_bytes()
    if data[:8] != b"AFOBJ001" or len(data) < 84:
        raise ValueError("Invalid source fixture envelope")
    size, flags = struct.unpack("<QI", data[8:20])
    if (
        flags != 0
        or len(data) != size + 84
        or hashlib.sha256(data[84:]).hexdigest().encode() != data[20:84]
    ):
        raise ValueError("Source fixture integrity mismatch")
    return data[84:]


def promote(service, fixture, root):
    files = []
    objects = root / "buckets/antfly/objects"
    for original in fixture["files"]:
        key = "fixture/" + original["file_id"]
        item = service.store.put(key, unwrap(objects / original["file_id"]))
        files.append(
            {
                **original,
                "object_uri": f"gs://{service.store.bucket}/{key}",
                "version_id": item["generation"],
                "etag": item["etag"],
            }
        )
    chunk = archive.canonical(files).encode()
    service.store.put("fixture/files.json", chunk)
    head = {
        "version": 1,
        "source_id": "archive-qualification",
        "file_count": len(files),
        "chunks": [
            {
                "uri": f"gs://{service.store.bucket}/fixture/files.json",
                "sha256": hashlib.sha256(chunk).hexdigest(),
                "byte_len": len(chunk),
                "file_count": len(files),
            }
        ],
    }
    service.store.put(
        "fixture/snapshot.parquet-manifest.json", archive.canonical(head).encode()
    )
    return {
        **fixture,
        "manifest_uri": f"gs://{service.store.bucket}/fixture/snapshot.parquet-manifest.json",
        "files": files,
        "sha256": archive.digest(files),
    }


def cases(rows, limit=7):
    base = {
        "full_text_search": {"match": "quasar", "field": "body"},
        "full_text_index": "text",
        "order_by": [{"field": "_score", "desc": True}, {"field": "id"}],
        "fields": ["id"],
        "limit": min(rows, limit),
    }
    common_ids = list(range(min(rows, limit)))
    result = [
        {"name": "common", "request": base, "expected_ids": common_ids},
        {
            "name": "rare",
            "request": {
                **base,
                "full_text_search": {"match": "token17", "field": "body"},
            },
            "expected_ids": list(range(17, rows, 257))[:limit],
        },
        {
            "name": "phrase",
            "request": {
                **base,
                "full_text_search": {"match_phrase": "quasar token17", "field": "body"},
            },
            "expected_ids": list(range(17, rows, 257))[:limit],
        },
        {
            "name": "conjunction",
            "request": {
                **base,
                "full_text_search": {
                    "conjuncts": [
                        {"match": "quasar", "field": "body"},
                        {"match": "token17", "field": "body"},
                    ]
                },
            },
            "expected_ids": list(range(17, rows, 257))[:limit],
        },
        {
            "name": "absent",
            "request": {
                **base,
                "full_text_search": {
                    "match": "absentqualificationtoken",
                    "field": "body",
                },
            },
            "expected_ids": [],
        },
        {
            "name": "highlights",
            "request": {
                **base,
                "fields": ["id", "body"],
                "highlight": {"fields": ["body"]},
            },
            "expected_ids": common_ids,
        },
        {
            "name": "wide_projection",
            "request": {**base, "fields": ["id", "payload"]},
            "expected_ids": common_ids,
        },
        {
            "name": "filtered",
            "request": {
                **base,
                "filter_query": {
                    "numeric_range": {
                        "field": "id",
                        "min": 0,
                        "max": min(rows - 1, 99),
                        "inclusive_min": True,
                        "inclusive_max": True,
                    }
                },
            },
            "expected_ids": common_ids,
        },
    ]
    for case in result:
        case["identity_field"] = "id"
    return result


def await_indexes(client, table, names):
    deadline = time.monotonic() + client.timeout
    while True:
        if client.server.proc.poll() is not None:
            raise RuntimeError(
                "Benchmark server exited: " + client.server.debug_logs()[-4000:]
            )
        states = [
            client.call("GET", f"/tables/{table}/indexes/{name}")["status"]
            for name in names
        ]
        if any(state.get("readiness", {}).get("state") == "failed" for state in states):
            raise RuntimeError(f"Index publication failed: {states}")
        if all(state.get("readiness", {}).get("queryable") for state in states):
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("Benchmark indexes did not become ready")
        time.sleep(0.1)


def qualify_pagination(client, server, case, pages):
    """Compare retained pages before/after process restart, including sort values."""
    request = dict(case["request"])
    first = client.query({"request": request})
    hits = first["hits"]["hits"]
    if not hits or len(hits) < request["limit"]:
        return {"case": case["name"], "continuations": 0, "restart_replayed": False}
    snapshot = first.get("remote_snapshot")
    if snapshot is None or "_sort" not in hits[-1]:
        raise AssertionError("Missing retained snapshot or sort keys")
    initial_after = hits[-1]["_sort"]
    after = initial_after
    expected = []
    ids = [hit["_source"]["id"] for hit in hits]
    for _ in range(pages - 1):
        value = client.query(
            {"request": {**request, "remote_snapshot": snapshot, "search_after": after}}
        )
        page = value["hits"]["hits"]
        expected.append(archive.digest(archive.result_value(value, "id")))
        ids.extend(hit["_source"]["id"] for hit in page)
        if len(page) < request["limit"]:
            break
        after = page[-1]["_sort"]
    if len(ids) != len(set(ids)):
        raise AssertionError("Duplicate logical IDs across retained pages")
    client.session.close()
    server.pause()
    server.resume()
    after = initial_after
    for checksum in expected:
        value = client.query(
            {"request": {**request, "remote_snapshot": snapshot, "search_after": after}}
        )
        if archive.digest(archive.result_value(value, "id")) != checksum:
            raise AssertionError("Retained page changed after restart")
        page = value["hits"]["hits"]
        if page:
            after = page[-1]["_sort"]
    return {
        "case": case["name"],
        "continuations": len(expected),
        "restart_replayed": True,
        "logical_ids": len(ids),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--files", type=int, default=1)
    parser.add_argument("--row-group", type=int, default=1000)
    parser.add_argument("--payload-bytes", type=int, default=512)
    parser.add_argument("--cache-mib", type=int, default=64)
    parser.add_argument(
        "--store-source", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--authoritative-cuts", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument("--qualify-sql", action="store_true")
    parser.add_argument("--latency-ms", type=float, action="append")
    parser.add_argument("--jitter-ms", type=float, default=0)
    parser.add_argument("--bandwidth-mib", type=float, default=0)
    parser.add_argument("--io-concurrency", type=int, default=16)
    parser.add_argument("--clients", type=int, default=1)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--cursor-pages", type=int, default=3)
    parser.add_argument("--case", action="append")
    parser.add_argument(
        "--fault", choices=["none", "throttle", "reset", "truncate"], default="none"
    )
    parser.add_argument("--fault-every", type=int, default=0)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args(argv)
    if (
        min(
            args.rows,
            args.files,
            args.row_group,
            args.samples,
            args.clients,
            args.cache_mib,
        )
        < 1
        or args.files > args.rows
        or args.clients > 64
        or args.cache_mib > 256
        or not 1 <= args.cursor_pages <= 100
        or args.payload_bytes < 0
    ):
        parser.error("Invalid fixture, cache or client size")
    policies = [
        Network(
            latency,
            args.jitter_ms,
            args.bandwidth_mib,
            args.io_concurrency,
            args.seed,
            args.fault_every,
            args.fault,
        )
        for latency in (args.latency_ms or [0, 30, 100])
    ]
    for policy in policies:
        policy.validate()
    binary = archive.VerifiedInput(args.binary)
    inputs = [
        binary,
        archive.VerifiedInput(__file__),
        archive.VerifiedInput(Path(__file__).with_name("lake_network_store.py")),
        archive.VerifiedInput(archive.__file__),
    ]
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    cache = output / "cache"
    fixture_root = output / "fixture"
    fixture = archive.generate_fixture(
        fixture_root, args.rows, args.files, args.row_group, args.payload_bytes, 10
    )
    selected = cases(args.rows)
    if args.case:
        unknown = set(args.case) - {case["name"] for case in selected}
        if unknown:
            parser.error(f"Unknown cases: {sorted(unknown)}")
        selected = [case for case in selected if case["name"] in args.case]
    report = {
        "version": 1,
        "complete": False,
        "binary_sha256": binary.sha256,
        "tool_sha256": {item.path.name: item.sha256 for item in inputs[1:]},
        "fixture": {
            "rows": args.rows,
            "files": args.files,
            "row_group": args.row_group,
            "payload_bytes": args.payload_bytes,
        },
        "cache_mib": args.cache_mib,
        "store_source": args.store_source,
        "authoritative_cuts": args.authoritative_cuts,
        "sql_count_qualified": False,
        "clients": args.clients,
        "samples": [],
        "policies": [asdict(policy) for policy in policies],
        "transport": "local GCS JSON API simulation; no real cloud requests",
        "cache_contract": "empty Antfly cache at launch; startup I/O is recorded separately; OS cache is uncontrolled",
        "io_scope": "all object-store requests admitted during each window, including any background work; observed waves are not causal dependency depth",
    }

    def save():
        (output / "report.json").write_text(json.dumps(report, indent=2))

    sys.path.insert(0, str(archive.ROOT / "e2e/antfly"))
    import conftest as harness

    save()
    try:
        with (
            NetworkStore(output / "objects") as service,
            environment(
                {
                    "GCS_BEARER_TOKEN": "local-benchmark-only",
                    "GCS_JSON_API_ENDPOINT": service.url + "/storage/v1",
                    "GCS_JSON_API_UPLOAD_ENDPOINT": service.url + "/upload/storage/v1",
                }
            ),
        ):
            fixture = promote(service, fixture, fixture_root)
            report["source_sha256"] = fixture["sha256"]
            plan = archive.generated_plan(fixture, args.rows, 16)
            plan["definition"]["storage"] = {"engine": "object"}
            plan["definition"]["schema"]["base_source"]["credentials"] = {
                "ref": "source"
            }
            plan["indexes"]["text"]["store_source"] = args.store_source
            config = {
                "deployment_mode": "standalone",
                "storage": {
                    "engine": "local",
                    "local": {"base_dir": str(output / "native")},
                    "artifacts": {
                        "connection": "artifacts",
                        "bucket": service.store.bucket,
                        "prefix": "artifacts",
                    },
                },
                "lake_cache": {
                    "root": str(cache),
                    "max_memory_bytes": args.cache_mib * 1024**2,
                    "max_disk_bytes": 1024**3,
                },
                "lake_indexes": {
                    "serving_cuts": {"enabled": args.authoritative_cuts},
                    "navigation_warming": {"enabled": False},
                    "artifact_gc": {"enabled": False},
                },
                "connections": {
                    name: {
                        "kind": "external_io",
                        "capabilities": capabilities,
                        "external_io": {
                            "protocol": "gcs",
                            "buckets": [service.store.bucket],
                            "prefix": prefix,
                        },
                    }
                    for name, capabilities, prefix in (
                        ("source", ["lake.read"], "fixture"),
                        ("artifacts", ["storage.primary"], "artifacts"),
                    )
                },
            }
            config_path = output / "config.json"
            config_path.write_text(json.dumps(config))
            config_path.chmod(0o600)
            config_input = archive.VerifiedInput(config_path)
            inputs.extend(
                [
                    config_input,
                    archive.VerifiedEnvironment(),
                    archive.VerifiedInput(harness.__file__),
                ]
            )
            server = harness.StandaloneAntflyServer(
                str(binary.path), "127.0.0.1", 0, metrics=True, config_path=config_path
            )
            client = archive.Client(server, args.timeout)
            failed = True
            try:
                client.call("POST", f"/tables/{archive.TABLE}", plan["definition"])
                for name, definition in plan["indexes"].items():
                    client.call(
                        "POST", f"/tables/{archive.TABLE}/indexes/{name}", definition
                    )
                await_indexes(client, archive.TABLE, archive.readiness_indexes(plan))
                if args.qualify_sql:
                    result = client.call(
                        "POST",
                        "/sql",
                        {"statement": f"SELECT COUNT(*) FROM {archive.TABLE}"},
                    )
                    if result.get("rows") != [[str(args.rows)]]:
                        raise AssertionError(f"Incorrect SQL count: {result}")
                    report["sql_count_result"] = result
                    report["sql_count_qualified"] = True
                references = {}
                for case in selected:
                    value = client.query(case)
                    references[case["name"]] = archive.verify_case(case, value)
                report["pagination"] = (
                    [
                        qualify_pagination(client, server, case, args.cursor_pages)
                        for case in selected
                    ]
                    if args.cursor_pages > 1
                    else []
                )
                for policy_index, policy in enumerate(policies):
                    for case in selected:
                        for sample in range(args.samples):
                            archive.verify_inputs(inputs)
                            client.session.close()
                            server.pause()
                            if cache.exists():
                                shutil.rmtree(cache)
                            service.wait_idle()
                            prefix = f"p{policy_index}-{case['name']}-{sample}"
                            service.configure(
                                policy, prefix + "-startup", reset_trace=True
                            )
                            started = time.perf_counter_ns()
                            server.resume()
                            startup_ns = time.perf_counter_ns() - started
                            service.wait_idle()
                            for mode in ("empty_at_launch", "warm"):
                                phase = prefix + "-" + mode
                                service.configure(policy, phase)
                                with archive.MemorySample(
                                    lambda: server.proc.pid
                                ) as memory:
                                    started = time.perf_counter_ns()

                                    def query(_):
                                        # Separate HTTP clients avoid sharing a mutable
                                        # requests.Session between benchmark threads.
                                        lane = archive.Client(server, args.timeout)
                                        try:
                                            sent = time.perf_counter_ns()
                                            value = lane.query(case)
                                            return value, time.perf_counter_ns() - sent
                                        finally:
                                            lane.session.close()

                                    if args.clients == 1:
                                        values = [query(0)]
                                    else:
                                        with ThreadPoolExecutor(
                                            max_workers=args.clients
                                        ) as pool:
                                            values = list(
                                                pool.map(query, range(args.clients))
                                            )
                                    elapsed_ns = time.perf_counter_ns() - started
                                for value, _ in values:
                                    if (
                                        archive.verify_case(case, value)
                                        != references[case["name"]]
                                    ):
                                        raise AssertionError(
                                            "Query answer changed under the network/cache policy"
                                        )
                                service.wait_idle()
                                row = {
                                    "case": case["name"],
                                    "policy": policy_index,
                                    "sample": sample,
                                    "cache_mode": mode,
                                    "batch_elapsed_ns": elapsed_ns,
                                    "queries": args.clients,
                                    "memory": memory.report(),
                                    "query_elapsed_ns": [
                                        duration for _, duration in values
                                    ],
                                    "answer_sha256": references[case["name"]],
                                    "io": service.summary(phase),
                                }
                                if row["io"]["fixture_errors"]:
                                    raise RuntimeError(
                                        "Simulator failure invalidates the measurement"
                                    )
                                if row["io"]["trace_overflow"]:
                                    raise RuntimeError(
                                        "Transport trace budget exceeded; sample cannot be accepted"
                                    )
                                if mode == "empty_at_launch":
                                    row.update(
                                        startup_ns=startup_ns,
                                        startup_io=service.summary(prefix + "-startup"),
                                    )
                                report["samples"].append(row)
                                save()
                groups = {}
                for row in report["samples"]:
                    key = (row["policy"], row["case"], row["cache_mode"])
                    groups.setdefault(key, []).extend(row["query_elapsed_ns"])
                report["summary"] = [
                    {
                        "policy": key[0],
                        "case": key[1],
                        "cache_mode": key[2],
                        "n": len(values),
                        "median_ns": statistics.median(values),
                        "empirical_p95_ns": sorted(values)[
                            max(0, (95 * len(values) + 99) // 100 - 1)
                        ],
                        "tail_sample_count_under_20": len(values) < 20,
                    }
                    for key, values in groups.items()
                ]
                archive.verify_inputs(inputs, content=True)
                failed = False
                report["complete"] = True
            finally:
                client.session.close()
                if failed:
                    (output / "server-failure.log").write_text(server.debug_logs())
                    with service.lock:
                        (output / "transport-failure.json").write_text(
                            json.dumps(service.records[-2000:], indent=2)
                        )
                server.stop(test_failed=failed)
            report["stored_bytes"] = service.store.bytes
            report["stored_versions"] = len(service.store.versions)
    except BaseException as error:
        report["error"] = type(error).__name__ + ": " + str(error)[:2048]
        raise
    finally:
        save()
    print(
        json.dumps(
            {
                "complete": report["complete"],
                "samples": len(report["samples"]),
                "report": str(output / "report.json"),
            }
        )
    )


if __name__ == "__main__":
    main()
