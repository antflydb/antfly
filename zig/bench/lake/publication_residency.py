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

"""Actual coordinator publication timing, with complete SQL row/value oracles.

Usage: publication_residency.py LABEL IMMUTABLE_BINARY SOURCE_COMMIT [ROWS]
Run alternating before/after invocations after builds, E2E and cleanup finish.
Server startup and result validation are outside publication timing. Fresh server
storage still shares the OS file cache; this is not an empty-cache/cloud result.
"""

import hashlib
import importlib.util
import json
import os
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

root = Path.cwd()
sys.path.insert(0, str(root / "zig/e2e/antfly"))
import conftest as harness
from test_lake_archive_qualification import _write_remote_content_e2e_config

spec = importlib.util.spec_from_file_location(
    "publication_bench", root / "zig/tools/benchmark_lake_archive.py"
)
bench = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = bench
spec.loader.exec_module(bench)
label, executable, source = sys.argv[1:4]
count = int(sys.argv[4]) if len(sys.argv) == 5 else 8192
assert count >= 64 and count % 64 == 0
binary = Path(executable).resolve()
base = root / ".benchmark-results/lake/publication-residency-v1"
shared = base / f"input-{count}"
shared.mkdir(parents=True, exist_ok=True)
out = base / label
out.mkdir(exist_ok=True)
names = [f"value_{i}" for i in range(8)]
# Neighboring 64-row physical bodies scatter across the complete ordered drain.
ranks = [
    lane * (count // 64) + batch for batch in range(count // 64) for lane in range(64)
]


def value(row, column):
    tag = f"row-{row:08d}-column-{column}:"
    return tag + "v" * (128 - len(tag))


data = {"id": list(range(count)), "sort_key": ranks}
data.update(
    {
        name: [value(row, column) for row in range(count)]
        for column, name in enumerate(names)
    }
)
archive = shared / "archive.parquet"
if not archive.exists():
    pq.write_table(pa.table(data), archive, row_group_size=count, use_dictionary=False)
assert pq.read_table(archive).to_pydict() == data
fixture = bench.import_fixture(
    shared / "fixture", [archive], "publication-residency-v1"
)
properties = {
    name: {
        "type": "integer" if name in ("id", "sort_key") else "string",
        "x-antfly-field": {"type": "number", "sortable": True}
        if name in ("id", "sort_key")
        else {"type": "text", "index": False},
    }
    for name in data
}
definition = {
    "schema": {
        "storage_mode": "relational",
        "default_type": "row",
        "base_source": {
            "kind": "external",
            "format": "parquet",
            "table_id": "publication-residency-v1",
            "object_mutability": "immutable",
            "uri": fixture["manifest_uri"],
            "row_identity": {"key_columns": ["id"]},
        },
        "document_schemas": {
            "row": {"schema": {"type": "object", "properties": properties}}
        },
        "relational_indexes": [
            {
                "name": "ordered_cover",
                "keys": [{"column": "sort_key", "nulls": "first"}],
                "include_columns": ["id", *names],
            }
        ],
    },
    "indexes": {},
}
config = _write_remote_content_e2e_config(shared)
os.environ.pop("ANTFLY_E2E_FULL_LAKE", None)
binary_hash = bench.file_digest(binary)
with bench.managed_server(
    harness.StandaloneAntflyServer,
    binary,
    config,
    out,
    api_root=harness.ANTFLY_PUBLIC_API_ROOT,
) as server:
    client = bench.Client(server, 240)
    try:
        started = time.perf_counter_ns()
        client.call("POST", f"/tables/{bench.TABLE}", definition)
        deadline = time.monotonic() + 240
        polls = 0
        while True:
            status = client.call("GET", f"/tables/{bench.TABLE}/indexes/ordered_cover")[
                "status"
            ]
            polls += 1
            readiness = status.get("readiness", {})
            if readiness.get("state") == "failed":
                raise RuntimeError(status)
            if (
                readiness.get("queryable")
                and readiness.get("complete")
                and status.get("published_revision") is not None
                and status.get("published_revision") == status.get("target_revision")
            ):
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(status)
            time.sleep(0.025)
        elapsed = time.perf_counter_ns() - started
        columns = ["id", "sort_key", *names]
        # Bounded query pages verify the complete archive after the timed build.
        digest = hashlib.sha256()
        for first in range(0, count, 64):
            rows = client.call(
                "POST",
                "/sql",
                {
                    "statement": f"SELECT {', '.join(columns)} FROM {bench.TABLE} WHERE sort_key >= {first} AND sort_key < {first + 64} ORDER BY sort_key LIMIT 64"
                },
            )["rows"]
            expected = []
            for rank in range(first, min(first + 64, count)):
                row = (rank % (count // 64)) * 64 + rank // (count // 64)
                expected.append(
                    [str(row), str(rank), *[value(row, column) for column in range(8)]]
                )
            assert rows == expected, (first, rows[:1], expected[:1])
            digest.update(json.dumps(rows, separators=(",", ":")).encode())
        report = {
            "source_commit": source,
            "binary_sha256": binary_hash,
            "parquet_sha256": bench.file_digest(archive),
            "runner_sha256": bench.file_digest(Path(__file__)),
            "config_sha256": bench.file_digest(config),
            "rows": count,
            "columns": 10,
            "publication_elapsed_ns": elapsed,
            "polls": polls,
            "verified_result_sha256": digest.hexdigest(),
            "definition": definition,
            "status": status,
            "limitations": "Wall time from table creation through first queryable published revision; 25-ms readiness polling; server startup and complete SQL result validation excluded. Real local Parquet/FsStore, fresh server storage, retained OS caches. No cloud or peak RSS claim.",
        }
    finally:
        client.session.close()
assert bench.file_digest(binary) == binary_hash
(out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
print(
    json.dumps(
        {
            "label": label,
            "rows": count,
            "publication_ms": elapsed / 1e6,
            "result_sha256": report["verified_result_sha256"],
        }
    ),
    flush=True,
)
