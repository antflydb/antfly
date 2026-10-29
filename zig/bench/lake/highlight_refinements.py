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


"""Compare regular-layout pruning and broad distinct-term highlights on real Parquet.

Usage: highlight_refinements.py LABEL IMMUTABLE_BINARY SOURCE_COMMIT
Run old/new and new/old with no overlapping task-owned builds/tests/cleanup.
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
    "offset_bench", root / "zig/tools/benchmark_lake_archive.py"
)
bench = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = bench
spec.loader.exec_module(bench)
label, binary_arg, source = sys.argv[1:]
binary = Path(binary_arg).resolve()
base = root / ".benchmark-results/lake/highlight-refinements-v3"
shared = base / "input"
shared.mkdir(parents=True, exist_ok=True)
out = base / label
out.mkdir(exist_ok=True)
archive = shared / "archive.parquet"
dense = "running " * 262144
diverse = " ".join(f"broad{i:05d}" for i in range(20000)) + " "
sparse = "needle " + "." * 32768
clustered = "needle " + "needle quark " * 16384
bodies = [dense, diverse, sparse, clustered]
if not archive.exists():
    pq.write_table(
        pa.table({"id": [0, 1, 2, 3], "body": bodies}),
        archive,
        use_dictionary=False,
        row_group_size=1,
    )
observed = pq.read_table(archive)
assert observed.column("id").to_pylist() == list(range(len(bodies)))
assert observed.column("body").to_pylist() == bodies
fixture = bench.import_fixture(
    shared / "fixture", [archive], "highlight-refinements-v3"
)
definition = {
    "schema": {
        "storage_mode": "relational",
        "default_type": "row",
        "base_source": {
            "kind": "external",
            "format": "parquet",
            "table_id": "highlight-refinements-v3",
            "object_mutability": "immutable",
            "uri": fixture["manifest_uri"],
            "row_identity": {"key_columns": ["id"]},
        },
        "document_schemas": {
            "row": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "body": {"type": "string", "x-antfly-field": {"type": "text"}},
                    },
                }
            }
        },
    },
    "indexes": {"text": {"type": "full_text", "store_source": True}},
}
queries = {
    name: {
        "full_text_search": pattern,
        "fields": ["id"],
        "highlight": {"fields": ["body"], "fragment_size": 100, "max_fragments": 3},
        "limit": 1,
    }
    for name, pattern in (
        ("regular_stride_eight_2mib", {"match": "running", "field": "body"}),
        ("broad_20000_terms", {"prefix": "broad", "field": "body"}),
        ("sparse_32kib_control", {"match": "needle", "field": "body"}),
        (
            "early_cluster_mixed_terms",
            {
                "disjuncts": [
                    {"match": "needle", "field": "body"},
                    {"match": "quark", "field": "body"},
                ]
            },
        ),
    )
}
case_ids = {name: i for i, name in enumerate(queries)}
for name, request in queries.items():
    request["filter_query"] = {"term": {"path": "/id", "value": case_ids[name]}}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


binary_hash = sha(binary)
samples = []
oracles = {}
results = {}
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
        artifact_objects = [
            p
            for p in server.root.rglob("*")
            if p.is_file() and "artifacts" in p.relative_to(server.root).parts
        ]
        assert artifact_objects, "native artifact store was not found"
        artifact_bytes = sum(p.stat().st_size for p in artifact_objects)
        for query in queries.values():
            client.query({"request": query})
        for sample in range(7):
            names = list(queries)
            if sample % 2:
                names.reverse()
            for name in names:
                started = time.perf_counter_ns()
                value = client.query({"request": queries[name]})
                elapsed = time.perf_counter_ns() - started
                hits = value["hits"]["hits"]
                complete = {"hits": hits, "total": value["hits"]["total"]}
                assert results.setdefault(name, complete) == complete
                assert len(hits) == 1, (name, value)
                assert hits[0]["_source"] == {"id": case_ids[name]}
                fragments = hits[0]["_highlights"]["body"]
                original = bodies[case_ids[name]].encode("utf-8")
                assert fragments
                for fragment in fragments:
                    data = fragment["text"].encode("utf-8")
                    assert (
                        original[fragment["offset"] : fragment["offset"] + len(data)]
                        == data
                    )
                    assert fragment["spans"]
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
    "body_bytes": [len(body.encode()) for body in bodies],
    "artifact_bytes": artifact_bytes,
    "publication_ns": publication_ns,
    "queries": queries,
    "samples": samples,
    "hit_oracles": oracles,
    "results": results,
    "summary": {
        name: statistics.median(
            row["elapsed_ns"] for row in samples if row["case"] == name
        )
        for name in queries
    },
    "qualification": "Warm local Debug; seven samples per shape; synthetic real Parquet with regular stride-eight 2-MiB text, 20,000 distinct prefix matches, a sparse control and an early dense cluster followed by alternating terms. Every case is filtered to its input row and records complete hits and totals. Artifact bytes count every native artifact-store object after readiness, including any superseded objects; they do not isolate live text-index bytes. Compare both binary orders and complete hit digests. Publication includes table admission and readiness polling and is a single observational sample per run. No task-owned build, E2E or cleanup overlap. No Release, cloud, empty-cache or archive-scale qualification.",
}
(out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report["summary"], indent=2))
