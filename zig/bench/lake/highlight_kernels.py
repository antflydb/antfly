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

"""Measure sparse/dense highlight kernels and projected/hidden deadline controls.

Run from the repository root with the E2E environment's Python:
  highlight_kernels.py LABEL IMMUTABLE_BINARY SOURCE_COMMIT
Run both binary orders; compare complete hit digests for each query shape.
"""

import hashlib
import importlib.util
import json
import os
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
    "hidden_highlight_bench", root / "zig/tools/benchmark_lake_archive.py"
)
bench = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = bench
spec.loader.exec_module(bench)

label, binary_arg, source = sys.argv[1:]
binary = Path(binary_arg).resolve()
base = root / ".benchmark-results/lake/highlight-kernels"
shared = base / "input"
shared.mkdir(parents=True, exist_ok=True)
out = base / label
out.mkdir(exist_ok=True)
cases = []
for size in (2048, 8192, 32768, 65536, 131072):
    for mode in ("sparse", "dense"):
        index = len(cases)
        prefix = f"case{index} needle "
        word = "needle " if mode == "dense" else "alpha "
        value = (
            prefix
            + (word * ((size + len(word) - 1) // len(word)))[: size - len(prefix)]
        )
        cases.append({"id": index, "bytes": size, "mode": mode, "text": value})
# A second cohort recreates the review's 32-row, 32-KiB deadline probe.
count = len(cases) + 32
names = ["body"]
archive = shared / "archive.parquet"
if not archive.exists():
    pq.write_table(
        pa.table(
            {
                "id": list(range(count)),
                "body": [case["text"] for case in cases]
                + ["deadline needle " + "needle " * 4680] * 32,
            }
        ),
        archive,
        row_group_size=128,
        use_dictionary=False,
    )
fixture = bench.import_fixture(shared / "fixture", [archive], "hidden-highlights")
properties = {
    "id": {"type": "integer", "x-antfly-field": {"type": "number", "sortable": True}}
}
properties.update(
    {name: {"type": "string", "x-antfly-field": {"type": "text"}} for name in names}
)
definition = {
    "schema": {
        "storage_mode": "relational",
        "default_type": "row",
        "base_source": {
            "kind": "external",
            "format": "parquet",
            "table_id": "hidden-highlights",
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
config = harness._write_remote_content_e2e_config(out)
os.environ["ANTFLY_BENCH_QUERY_PROFILE_EVERY"] = "1"


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


binary_hash = sha(binary)
rows, deadlines = [], []
with bench.managed_server(
    harness.StandaloneAntflyServer,
    binary,
    config,
    out,
    api_root=harness.ANTFLY_PUBLIC_API_ROOT,
) as server:
    client = bench.Client(server, 120)
    try:
        client.call("POST", f"/tables/{bench.TABLE}", definition)
        client.ready(bench.readiness_indexes({"definition": definition}))
        for case in cases:
            query = {
                "full_text_search": {
                    "conjuncts": [
                        {"match": "needle", "field": "body"},
                        {"match": f"case{case['id']}", "field": "body"},
                    ]
                },
                "fields": ["id"],
                "highlight": {
                    "fields": ["body"],
                    "max_fragments": 3,
                    "fragment_size": 100,
                },
                "order_by": [{"field": "id"}],
                "limit": 1,
                "profile": True,
            }
            first = client.query({"request": query})
            assert first["hits"]["hits"][0]["_source"] == {"id": case["id"]}
            samples = []
            for _ in range(3):
                started = time.perf_counter_ns()
                response = client.query({"request": query})
                samples.append(time.perf_counter_ns() - started)
                assert response["hits"] == first["hits"]
            row = {k: v for k, v in case.items() if k != "text"}
            row.update(
                samples_ns=samples,
                median_ms=statistics.median(samples) / 1e6,
                hits_sha256=hashlib.sha256(
                    json.dumps(first["hits"], sort_keys=True).encode()
                ).hexdigest(),
            )
            rows.append(row)
            print(json.dumps(row), flush=True)
        query = {
            "full_text_search": {"match": "deadline", "field": "body"},
            "fields": ["id", "body"],
            "highlight": {"fields": ["body"], "max_fragments": 3, "fragment_size": 100},
            "order_by": [{"field": "id"}],
            "limit": 32,
        }
        # Include dense matches in the deadline cohort's highlight work.
        query["full_text_search"] = {
            "conjuncts": [
                query["full_text_search"],
                {"match": "needle", "field": "body"},
            ]
        }
        warm = client.query({"request": query})
        assert [hit["_source"]["id"] for hit in warm["hits"]["hits"]] == list(
            range(10, count)
        )
        assert all(
            hit["_source"]["body"] == "deadline needle " + "needle " * 4680
            for hit in warm["hits"]["hits"]
        )
        for sample in range(3):
            for mode in (
                ("hidden", "projected") if sample % 2 == 0 else ("projected", "hidden")
            ):
                fields = ["id"] if mode == "hidden" else ["id", "body"]
                for timeout_ms in (250, 1):
                    started = time.perf_counter_ns()
                    response = client.session.post(
                        server.api_url + f"/tables/{bench.TABLE}/query",
                        json=dict(query, fields=fields, timeout_ms=timeout_ms),
                        timeout=60,
                    )
                    elapsed = time.perf_counter_ns() - started
                    if response.ok:
                        hits = response.json()["hits"]["hits"]
                        assert [
                            {k: v for k, v in h.items() if k != "_source"} for h in hits
                        ] == [
                            {k: v for k, v in h.items() if k != "_source"}
                            for h in warm["hits"]["hits"]
                        ]
                        assert [hit["_source"] for hit in hits] == [
                            {field: hit["_source"][field] for field in fields}
                            for hit in warm["hits"]["hits"]
                        ]
                    record = {
                        "sample": sample,
                        "mode": mode,
                        "timeout_ms": timeout_ms,
                        "elapsed_ms": elapsed / 1e6,
                        "status": response.status_code,
                    }
                    if not response.ok:
                        record["response_text"] = response.text[:1000]
                    deadlines.append(record)
                    print(json.dumps(record), flush=True)
    finally:
        client.session.close()
assert sha(binary) == binary_hash
report = {
    "source_commit": source,
    "binary_sha256": binary_hash,
    "runner_sha256": sha(__file__),
    "parquet_sha256": sha(archive),
    "rows": rows,
    "deadline_warm_hits_sha256": hashlib.sha256(
        json.dumps(warm["hits"], sort_keys=True).encode()
    ).hexdigest(),
    "python": sys.version,
    "pyarrow": pa.__version__,
    "deadlines": deadlines,
    "qualification": "Warm local Debug, three samples per point, real synthetic Parquet. One hit and at most three snippets for each density point; 32 hits for deadline controls. No task-owned builds, E2E or cleanup overlap; shared host. Compare immutable binaries in both orders. No Release, cloud, empty-cache or archive-scale claim.",
}
(out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
