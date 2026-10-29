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

"""Paired real Parquet covering-query controls; run from the worktree root."""

import hashlib
import importlib.util
import json
import os
import pathlib
import statistics
import sys
import time

root = pathlib.Path.cwd()
sys.path.insert(0, str(root / "zig/e2e/antfly"))
import conftest as h  # noqa: E402
from test_lake_archive_qualification import _write_remote_content_e2e_config  # noqa: E402

spec = importlib.util.spec_from_file_location(
    "demand_bench", root / "zig/tools/benchmark_lake_archive.py"
)
b = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = b
spec.loader.exec_module(b)
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

label, binary_arg, source = sys.argv[1:]
out = root / ".benchmark-results/lake/excluded-columns-v3" / label
out.mkdir(parents=True, exist_ok=True)
shared = root / ".benchmark-results/lake/excluded-columns-v3/input"
shared.mkdir(parents=True, exist_ok=True)
archive = shared / "archive.parquet"
count = 256
if not archive.exists():
    pq.write_table(
        pa.table(
            {
                "id": list(range(count)),
                "bucket": [i // 128 for i in range(count)],
                "wide": ["w" * 131000 + str(i).zfill(4) for i in range(count)],
                "body": ["control quasar" for i in range(count)],
            }
        ),
        archive,
        row_group_size=128,
        use_dictionary=False,
        data_page_size=256 * 1024,
        write_batch_size=1,
    )
fixture = b.import_fixture(shared / "fixture", [archive], "ordered-demand-paired")
properties = {
    n: {"type": "integer", "x-antfly-field": {"type": "number", "sortable": True}}
    for n in ["id", "bucket"]
}
properties["wide"] = {
    "type": "string",
    "x-antfly-field": {"type": "text", "index": False},
}
properties["body"] = {"type": "string", "x-antfly-field": {"type": "text"}}
definition = {
    "schema": {
        "storage_mode": "relational",
        "default_type": "row",
        "base_source": {
            "kind": "external",
            "format": "parquet",
            "table_id": "ordered-demand-paired",
            "object_mutability": "immutable",
            "uri": fixture["manifest_uri"],
            "row_identity": {"key_columns": ["id"]},
        },
        "document_schemas": {
            "row": {"schema": {"type": "object", "properties": properties}}
        },
        "index_sort": [{"field": n, "order": "asc"} for n in ["id", "_id"]],
        "relational_indexes": [
            {
                "name": n,
                "keys": [{"column": n, "nulls": "first"}]
                + ([{"column": "wide"}, {"column": "id"}] if n != "id" else []),
            }
            for n in ["bucket", "id"]
        ],
    },
    "indexes": {"text": {"type": "full_text", "store_source": False}},
}
config = _write_remote_content_e2e_config(shared)
cfg = json.loads(config.read_text())
cfg.setdefault("lake_indexes", {}).setdefault("text_cache", {}).update(
    admission_window_segments=64, admission_window_bytes=32 * 1024 * 1024
)
config.write_text(json.dumps(cfg, sort_keys=True))
os.environ.pop("ANTFLY_E2E_FULL_LAKE", None)
os.environ["ANTFLY_BENCH_QUERY_PROFILE_EVERY"] = "1"
binary = pathlib.Path(binary_arg).resolve()
binary_hash = hashlib.sha256(binary.read_bytes()).hexdigest()
queries = {
    name: {
        "full_text_search": {"match": "quasar", "field": "body"},
        "fields": fields,
        "order_by": [{"field": "id", "desc": False}],
        "profile": True,
        "limit": 128,
    }
    for name, fields in (
        ("explicit_narrow_control", ["id", "bucket"]),
        ("wildcard_exclusions", ["*", "-wide", "-body"]),
        ("exclusions_only", ["-wide", "-body"]),
    )
}
samples = []
with b.managed_server(
    h.StandaloneAntflyServer, binary, config, out, api_root=h.ANTFLY_PUBLIC_API_ROOT
) as server:
    c = b.Client(server, 120)
    try:
        c.call("POST", f"/tables/{b.TABLE}", definition)
        c.ready(b.readiness_indexes({"definition": definition}))
        for name, query in queries.items():
            expected = list(range(128))
            for sample in range(3):
                server.restart()
                for mode in ("process_restarted", "warm"):
                    start = time.perf_counter_ns()
                    response = c.query({"request": query})
                    elapsed = time.perf_counter_ns() - start
                    ids = [hit["_source"]["id"] for hit in response["hits"]["hits"]]
                    assert ids == expected, (name, ids, expected)
                    assert all(
                        set(hit["_source"]) == {"id", "bucket"}
                        for hit in response["hits"]["hits"]
                    )
                    samples.append(
                        {
                            "case": name,
                            "sample": sample,
                            "cache_mode": mode,
                            "elapsed_ns": elapsed,
                            "ids": ids,
                            "selected_hits": response["hits"]["hits"],
                            "total": response["hits"]["total"],
                            "profile": response.get("profile"),
                        }
                    )
                    (out / "samples.json").write_text(json.dumps(samples, indent=2))
    finally:
        c.session.close()
assert hashlib.sha256(binary.read_bytes()).hexdigest() == binary_hash
report = {
    "source_commit": source,
    "binary_sha256": binary_hash,
    "parquet_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
    "config_sha256": hashlib.sha256(config.read_bytes()).hexdigest(),
    "runner_sha256": hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
    "rows": count,
    "definition": definition,
    "queries": queries,
    "cache_contract": "Process restart retains OS and persistent caches and readiness warming; no empty-cache claim.",
    "samples": samples,
    "summary": [
        {
            "case": name,
            "cache_mode": mode,
            "median_ns": statistics.median(
                r["elapsed_ns"]
                for r in samples
                if r["case"] == name and r["cache_mode"] == mode
            ),
        }
        for name in queries
        for mode in ("process_restarted", "warm")
    ],
}
(out / "report.json").write_text(json.dumps(report, indent=2))
print(json.dumps(report["summary"], indent=2), flush=True)
