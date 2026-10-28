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

"""Compare mixed projection policies, chunked source, and selective phrase plans."""

import hashlib
import importlib.util
import json
from pathlib import Path
import statistics
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq

root = Path.cwd()
sys.path.insert(0, str(root / "zig/e2e/antfly"))
import conftest as harness  # noqa: E402 -- repository E2E harness path.

spec = importlib.util.spec_from_file_location(
    "policy_prefetch_bench", root / "zig/tools/benchmark_lake_archive.py"
)
bench = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = bench
spec.loader.exec_module(bench)
case, label, executable, source_commit = sys.argv[1:]
assert case in ("cover", "source", "phrase")
binary = Path(executable).resolve()
base = root / ".benchmark-results/lake/policy-prefetch"
shared = base / f"input-{case}"
shared.mkdir(parents=True, exist_ok=True)
output = base / label
output.mkdir(parents=True, exist_ok=True)
count = 8192 if case == "phrase" else 256
values = {
    "id": list(range(count)),
    "bucket": [i // 64 for i in range(count)],
    "body": [
        "control quasar"
        if i < 64
        else "control pulsar"
        if i >= count - 64
        else "control"
        for i in range(count)
    ],
}
if case != "phrase":
    values["note"] = [f"note {i} " + "n" * 4096 for i in range(count)]
    for column in range(8):
        values[f"v{column}"] = [9007199254740993 + i * 8 + column for i in range(count)]
archive = shared / "archive.parquet"
if not archive.exists():
    pq.write_table(pa.table(values), archive, row_group_size=128, use_dictionary=False)
fixture = bench.import_fixture(shared / "fixture", [archive], "policy-prefetch")
properties = {
    name: {"type": "integer", "x-antfly-field": {"type": "number", "sortable": True}}
    for name in ("id", "bucket")
}
properties["body"] = {"type": "string", "x-antfly-field": {"type": "text"}}
if case != "phrase":
    properties.update({f"v{i}": {"type": "integer"} for i in range(8)})
    properties["note"] = {
        "type": "string",
        "x-antfly-field": {"type": "text", "index": False},
    }
definition = {
    "schema": {
        "storage_mode": "relational",
        "default_type": "row",
        "base_source": {
            "kind": "external",
            "format": "parquet",
            "table_id": "policy-prefetch",
            "object_mutability": "immutable",
            "uri": fixture["manifest_uri"],
            "row_identity": {"key_columns": ["id"]},
        },
        "document_schemas": {
            "row": {"schema": {"type": "object", "properties": properties}}
        },
        "index_sort": [{"field": n, "order": "asc"} for n in ("id", "_id")],
    },
    "indexes": {"text": {"type": "full_text", "store_source": case != "cover"}},
}
if case in ("cover", "phrase"):
    definition["schema"]["relational_indexes"] = [
        {
            "name": "ordered",
            "keys": [{"column": "bucket", "nulls": "first"}, {"column": "id"}]
            if case == "phrase"
            else [{"column": "id"}],
            "include_columns": ["bucket"] + [f"v{i}" for i in range(8)]
            if case == "cover"
            else [],
        }
    ]
queries = []
if case == "phrase":
    for term, descending in (
        ("pulsar", False),
        ("quasar", True),
        ("quasar", False),
        ("pulsar", True),
    ):
        queries.append(
            (
                f"{term}_{'desc' if descending else 'asc'}",
                {
                    "full_text_search": {
                        "match_phrase": f"control {term}",
                        "field": "body",
                    },
                    "filter_query": {"range": {"path": "/bucket", "gte": 0}},
                    "fields": ["id", "bucket"],
                    "order_by": [{"field": "bucket", "desc": descending}],
                    "limit": 20,
                    "profile": True,
                },
                list(range(count - 64, count - 44))
                if term == "pulsar"
                else list(range(20)),
            )
        )
else:
    narrow = ["id", "v0"]
    broad = (
        ["id", "bucket"]
        + [f"v{i}" for i in range(8)]
        + (["note"] if case == "source" else [])
    )
    for name, fields in (
        ("narrow", narrow),
        ("broad", broad),
        ("narrow_repeat", narrow),
        ("broad_repeat", broad),
    ):
        queries.append(
            (
                name,
                {
                    "full_text_search": {"match": "control", "field": "body"},
                    "fields": fields,
                    "order_by": [{"field": "id"}],
                    "limit": count,
                    "profile": True,
                },
                list(range(count)),
            )
        )
config = harness._write_remote_content_e2e_config(shared)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


binary_hash = sha(binary)
samples = []
with bench.managed_server(
    harness.StandaloneAntflyServer,
    binary,
    config,
    output,
    api_root=harness.ANTFLY_PUBLIC_API_ROOT,
) as server:
    client = bench.Client(server, 120)
    try:
        client.call("POST", f"/tables/{bench.TABLE}", definition)
        client.ready(bench.readiness_indexes({"definition": definition}))
        for sample in range(3):
            server.restart()
            for name, query, expected in queries:
                start = time.perf_counter_ns()
                response = client.query({"request": query})
                elapsed = time.perf_counter_ns() - start
                hits = response["hits"]["hits"]
                total = response["hits"]["total"]
                matches = 64 if case == "phrase" else count
                assert (
                    total["value"] == matches
                    if total["relation"] == "exact"
                    else len(hits) <= total["value"] <= matches
                )
                assert [h["_source"]["id"] for h in hits] == expected
                for hit, row in zip(hits, expected):
                    assert hit["_source"] == {
                        f: values[f][row] for f in query["fields"]
                    }
                samples.append(
                    {
                        "sample": sample,
                        "query_name": name,
                        "elapsed_ns": elapsed,
                        "hits": hits,
                        "total": response["hits"]["total"],
                        "profile": response.get("profile"),
                    }
                )
    finally:
        client.session.close()
assert sha(binary) == binary_hash
report = {
    "source_commit": source_commit,
    "binary_sha256": binary_hash,
    "runner_sha256": sha(__file__),
    "parquet_sha256": sha(archive),
    "config_sha256": sha(config),
    "case": case,
    "rows": count,
    "definition": definition,
    "queries": [{"name": n, "request": q} for n, q, _ in queries],
    "cache_contract": "Three restarts retaining OS/persistent caches and readiness warming; query order is recorded, no empty-cache claim.",
    "samples": samples,
    "summary": {
        n: statistics.median(s["elapsed_ns"] for s in samples if s["query_name"] == n)
        for n, _, _ in queries
    },
}
(output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report["summary"], indent=2), flush=True)
