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

"""Measure hidden source streaming with narrow/projected controls on real Parquet.

Run from the repository root with the E2E environment's Python:
  hidden_highlights.py LABEL IMMUTABLE_BINARY SOURCE_COMMIT
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
base = root / ".benchmark-results/lake/hidden-highlights"
shared = base / "input"
shared.mkdir(parents=True, exist_ok=True)
out = base / label
out.mkdir(exist_ok=True)
count = 256
names = [f"body{i}" for i in range(8)]
archive = shared / "archive.parquet"
if not archive.exists():
    data = {"id": list(range(count))}
    for name in names:
        data[name] = [f"needle {name} {i} " + "alpha " * 170 for i in range(count)]
    pq.write_table(pa.table(data), archive, row_group_size=128, use_dictionary=False)
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
queries = {}
for mode, fields, highlighted in (
    ("hidden_wide", ["id"], names),
    ("projected_wide", ["id"] + names, names),
    ("hidden_narrow", ["id"], names[:1]),
):
    queries[mode] = {
        "full_text_search": {
            "disjuncts": [{"match": "needle", "field": name} for name in highlighted]
        },
        "fields": fields,
        "highlight": {"fields": highlighted},
        "order_by": [{"field": "id"}],
        "limit": count,
        "profile": True,
    }


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


binary_hash = sha(binary)
samples = []
oracles = {}
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
        for query in queries.values():
            client.query({"request": query})
        time.sleep(5.2)
        for sample in range(3):
            order = list(queries)
            if sample % 2:
                order.reverse()
            for mode in order:
                query = queries[mode]
                before = client.metrics()
                start = time.perf_counter_ns()
                response = client.query({"request": query})
                elapsed = time.perf_counter_ns() - start
                deadline = time.monotonic() + 20
                while True:
                    after = client.metrics()
                    if (
                        after["antfly_lake_query_highlight_calls_total"]
                        > before["antfly_lake_query_highlight_calls_total"]
                    ):
                        break
                    if time.monotonic() > deadline:
                        raise TimeoutError("highlight metrics did not refresh")
                    time.sleep(0.1)
                hits = response["hits"]["hits"]
                assert [hit["_source"]["id"] for hit in hits] == list(range(count))
                for hit in hits:
                    assert set(hit["_source"]) == set(query["fields"])
                    assert set(hit["_highlights"]) == set(query["highlight"]["fields"])
                encoded = json.dumps(hits, sort_keys=True, separators=(",", ":"))
                digest = hashlib.sha256(encoded.encode()).hexdigest()
                if mode in oracles:
                    assert oracles[mode] == digest
                else:
                    oracles[mode] = digest
                    (out / f"{mode}-hits.json").write_text(encoded + "\n")
                samples.append(
                    {
                        "sample": sample,
                        "mode": mode,
                        "elapsed_ns": elapsed,
                        "highlight_ns": after[
                            "antfly_lake_query_highlight_nanoseconds_total"
                        ]
                        - before["antfly_lake_query_highlight_nanoseconds_total"],
                        "hydration_ns": after[
                            "antfly_lake_query_hydration_nanoseconds_total"
                        ]
                        - before["antfly_lake_query_hydration_nanoseconds_total"],
                        "hits_sha256": digest,
                    }
                )
    finally:
        client.session.close()
assert sha(binary) == binary_hash
report = {
    "source_commit": source,
    "binary_sha256": binary_hash,
    "runner_sha256": sha(__file__),
    "parquet_sha256": sha(archive),
    "rows": count,
    "wide_fields": len(names),
    "queries": queries,
    "samples": samples,
    "hit_oracles": oracles,
    "summary": {
        mode: {
            key: statistics.median(
                sample[key] for sample in samples if sample["mode"] == mode
            )
            for key in ("elapsed_ns", "highlight_ns", "hydration_ns")
        }
        for mode in queries
    },
    "qualification": "Warm local Debug, three samples per shape, alternating query order. Compare identical query shapes across immutable binaries; projected-wide and hidden-narrow are separate controls. No build, E2E or cleanup overlaps these measurements. This does not qualify cloud latency or archive scale.",
}
(out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report["summary"], indent=2))
