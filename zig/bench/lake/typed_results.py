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


"""Compare native typed result delivery using independently written wide Parquet.

Usage: typed_results.py LABEL IMMUTABLE_BINARY SOURCE_COMMIT
Run old/new and new/old without overlapping builds/tests/cleanup.
"""

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
    "typed_results_bench", root / "zig/tools/benchmark_lake_archive.py"
)
bench = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = bench
spec.loader.exec_module(bench)
label, binary_arg, source = sys.argv[1:]
binary = Path(binary_arg).resolve()
base = root / ".benchmark-results/lake/typed-results"
shared = base / "input"
shared.mkdir(parents=True, exist_ok=True)
out = base / label
out.mkdir(exist_ok=True)
count = 256
names = [f"value{i}" for i in range(64)]
archive = shared / "archive.parquet"
# Exact values above the f64 integer boundary, plus nulls, expose coercion bugs.
data = {"id": list(range(count)), "body": ["needle"] * count}
for col, name in enumerate(names):
    data[name] = [
        None if (row + col) % 11 == 0 else 9007199254740993 + row + col
        for row in range(count)
    ]
if not archive.exists():
    pq.write_table(pa.table(data), archive, use_dictionary=False, row_group_size=128)
fixture = bench.import_fixture(shared / "fixture", [archive], "typed-results")
properties = {name: {"type": "integer"} for name in ["id"] + names}
properties["id"]["x-antfly-field"] = {"type": "number", "sortable": True}
properties["body"] = {"type": "string", "x-antfly-field": {"type": "text"}}
definition = {
    "schema": {
        "storage_mode": "relational",
        "default_type": "row",
        "base_source": {
            "kind": "external",
            "format": "parquet",
            "table_id": "typed-results",
            "object_mutability": "immutable",
            "uri": fixture["manifest_uri"],
            "row_identity": {"key_columns": ["id"]},
        },
        "document_schemas": {
            "row": {"schema": {"type": "object", "properties": properties}}
        },
        "index_sort": [{"field": name, "order": "asc"} for name in ("id", "_id")],
    },
    "indexes": {"text": {"type": "full_text", "store_source": True}},
}
queries = {
    name: {
        "full_text_search": {"match": "needle", "field": "body"},
        "fields": ["id"] + fields,
        "order_by": [{"field": "id"}],
        "limit": count,
    }
    for name, fields in (
        ("narrow_1", names[:1]),
        ("wide_8", names[:8]),
        ("wide_64", names),
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
        start = time.perf_counter_ns()
        client.call("POST", f"/tables/{bench.TABLE}", definition)
        client.ready(bench.readiness_indexes({"definition": definition}))
        publication_ns = time.perf_counter_ns() - start
        for query in queries.values():
            client.query({"request": query})
        for sample in range(7):
            order = list(queries)
            if sample % 2:
                order.reverse()
            for name in order:
                started = time.perf_counter_ns()
                response = client.query({"request": queries[name]})
                elapsed = time.perf_counter_ns() - started
                hits = response["hits"]["hits"]
                assert len(hits) == count
                for row, hit in enumerate(hits):
                    assert hit["_source"] == {
                        field: data[field][row] for field in queries[name]["fields"]
                    }
                digest = hashlib.sha256(
                    json.dumps(hits, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest()
                assert oracles.setdefault(name, digest) == digest
                samples.append({"sample": sample, "case": name, "elapsed_ns": elapsed})
    finally:
        client.session.close()
assert sha(binary) == binary_hash
report = {
    "source_commit": source,
    "binary_sha256": binary_hash,
    "runner_sha256": sha(__file__),
    "parquet_sha256": sha(archive),
    "rows": count,
    "publication_ns": publication_ns,
    "queries": queries,
    "samples": samples,
    "hit_oracles": oracles,
    "summary": {
        name: statistics.median(
            row["elapsed_ns"] for row in samples if row["case"] == name
        )
        for name in queries
    },
    "qualification": "Warm local Debug; seven samples per shape; independently written Parquet, 256 rows, nullable exact i64 values, 1/8/64 projected scalar columns. Compare both binary orders and complete hit digests. Publication includes table admission and readiness polling and is one observational sample per run. No overlapping task-owned builds, E2E or cleanup. No Release, cloud, empty-cache or archive-scale qualification.",
}
(out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report["summary"], indent=2))
