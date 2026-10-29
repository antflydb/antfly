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

"""Real Parquet large-string and mixed-source delivery; run paired binary orders."""

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
import conftest as harness  # noqa: E402

spec = importlib.util.spec_from_file_location(
    "typed_delivery_bench", root / "zig/tools/benchmark_lake_archive.py"
)
bench = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = bench
spec.loader.exec_module(bench)
label, executable, source = sys.argv[1:]
binary = Path(executable).resolve()
base = root / ".benchmark-results/lake/typed-navigation-v1"
shared = base / "input"
shared.mkdir(parents=True, exist_ok=True)
out = base / label
out.mkdir(exist_ok=True)
count = 12
body = ("needle " + "x" * 65520 + '🙂\n"\\') * 4
columns = {"id": list(range(count)), "body": [body + str(i) for i in range(count)]}
for ordinal in range(64):
    columns[f"value_{ordinal:02}"] = [
        None if ordinal == 63 and i % 3 == 0 else 9007199254740993 + ordinal + i
        for i in range(count)
    ]
archive = shared / "archive.parquet"
if not archive.exists():
    pq.write_table(pa.table(columns), archive, row_group_size=4, use_dictionary=False)
assert pq.read_table(archive).to_pydict() == columns
fixture = bench.import_fixture(shared / "fixture", [archive], "typed-navigation-v1")
properties = {
    name: {"type": "string", "x-antfly-field": {"type": "text"}}
    if name == "body"
    else {"type": "integer", "nullable": name == "value_63"}
    for name in columns
}
properties["id"]["x-antfly-field"] = {"type": "number", "sortable": True}
definition = {
    "schema": {
        "storage_mode": "relational",
        "default_type": "row",
        "base_source": {
            "kind": "external",
            "format": "parquet",
            "table_id": "typed-navigation-v1",
            "object_mutability": "immutable",
            "uri": fixture["manifest_uri"],
            "row_identity": {"key_columns": ["id"]},
        },
        "document_schemas": {
            "row": {"schema": {"type": "object", "properties": properties}}
        },
        "index_sort": [{"field": field} for field in ("id", "_id")],
    },
    "indexes": {"text": {"type": "full_text", "store_source": True}},
}
queries = {
    name: {
        "full_text_search": {"match": "needle", "field": "body"},
        "fields": fields,
        "order_by": [{"field": "id"}],
        "limit": count,
    }
    for name, fields in (
        ("native_wide", list(columns)),
        ("mixed_identity_wide", ["_id"] + list(columns)),
        ("native_large_strings", ["body"]),
        ("mixed_identity_large_strings", ["_id", "body"]),
    )
}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


binary_hash = sha(binary)
samples = []
oracles = {}
with bench.managed_server(
    harness.StandaloneAntflyServer,
    binary,
    harness._write_remote_content_e2e_config(out),
    out,
    api_root=harness.ANTFLY_PUBLIC_API_ROOT,
) as server:
    client = bench.Client(server, 120)
    try:
        started = time.perf_counter_ns()
        client.call("POST", f"/tables/{bench.TABLE}", definition)
        client.ready(bench.readiness_indexes({"definition": definition}))
        publication_ns = time.perf_counter_ns() - started
        for request in queries.values():
            client.query({"request": request})
        for sample in range(7):
            names = list(queries)
            if sample % 2:
                names.reverse()
            for name in names:
                started = time.perf_counter_ns()
                response = client.query({"request": queries[name]})
                elapsed = time.perf_counter_ns() - started
                hits = response["hits"]["hits"]
                assert len(hits) == count
                for i, hit in enumerate(hits):
                    expected = {
                        field: hit["_id"] if field == "_id" else columns[field][i]
                        for field in queries[name]["fields"]
                    }
                    assert hit["_source"] == expected, (name, i)
                complete = {"hits": hits, "total": response["hits"]["total"]}
                encoded = json.dumps(complete, sort_keys=True, separators=(",", ":"))
                digest = hashlib.sha256(encoded.encode()).hexdigest()
                assert oracles.setdefault(name, digest) == digest
                samples.append(
                    {
                        "sample": sample,
                        "case": name,
                        "elapsed_ns": elapsed,
                        "complete_sha256": digest,
                        "total": response["hits"]["total"],
                    }
                )
                (out / "samples.json").write_text(json.dumps(samples, indent=2))
    finally:
        client.session.close()
assert sha(binary) == binary_hash
report = {
    "source_commit": source,
    "binary_sha256": binary_hash,
    "runner_sha256": sha(__file__),
    "parquet_sha256": sha(archive),
    "rows": count,
    "body_utf8_bytes": [len(value.encode()) for value in columns["body"]],
    "definition": definition,
    "queries": queries,
    "publication_ns": publication_ns,
    "samples": samples,
    "oracles": oracles,
    "summary": {
        name: statistics.median(
            row["elapsed_ns"] for row in samples if row["case"] == name
        )
        for name in queries
    },
    "qualification": "Warm shared-host local Debug; seven samples per case; strict source assertions verify UTF-8, escapes, nulls and every integer above 2^53. Complete hit and total hashes must agree across binaries and both orders. No task-owned build/E2E/cleanup overlap; no Release, cloud, empty-cache, archive-scale or full-query memory claim.",
}
(out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report["summary"], indent=2), flush=True)
