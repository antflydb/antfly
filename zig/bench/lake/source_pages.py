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

"""Compare sparse 8-MiB highlights at both ends of independently written Parquet.

Usage: source_pages.py LABEL IMMUTABLE_BINARY SOURCE_COMMIT
Run both binary orders without overlapping task-owned builds/tests/cleanup.
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
    "source_page_bench", root / "zig/tools/benchmark_lake_archive.py"
)
bench = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = bench
spec.loader.exec_module(bench)
label, binary_arg, source = sys.argv[1:]
binary = Path(binary_arg).resolve()
base = root / ".benchmark-results/lake/source-pages"
shared = base / "input"
shared.mkdir(parents=True, exist_ok=True)
out = base / label
out.mkdir(exist_ok=True)
archive = shared / "archive.parquet"
names = ("sparsehead", "sparsetail")
if not archive.exists():
    punctuation = "." * (8 * 1024 * 1024)
    pq.write_table(
        pa.table(
            {
                "id": [0, 1],
                "body": [
                    "sparsehead needle " + punctuation,
                    punctuation + " sparsetail needle",
                ],
            }
        ),
        archive,
        use_dictionary=False,
    )
fixture = bench.import_fixture(shared / "fixture", [archive], "source-pages")
definition = {
    "schema": {
        "storage_mode": "relational",
        "default_type": "row",
        "base_source": {
            "kind": "external",
            "format": "parquet",
            "table_id": "source-pages",
            "object_mutability": "immutable",
            "uri": fixture["manifest_uri"],
            "row_identity": {"key_columns": ["id"]},
        },
        "document_schemas": {
            "row": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": {
                            "type": "integer",
                            "x-antfly-field": {"type": "number", "sortable": True},
                        },
                        "body": {
                            "type": "string",
                            "x-antfly-field": {"type": "text"},
                        },
                    },
                }
            }
        },
        "index_sort": [{"field": "id", "order": "asc"}],
    },
    "indexes": {"text": {"type": "full_text", "store_source": True}},
}
config = harness._write_remote_content_e2e_config(out)
os.environ["ANTFLY_BENCH_QUERY_PROFILE_EVERY"] = "1"


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


binary_hash = sha(binary)
rows = []
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
        for case_id, name in enumerate(names):
            query = {
                "full_text_search": {
                    "conjuncts": [
                        {"match": name, "field": "body"},
                        {"match": "needle", "field": "body"},
                    ]
                },
                "fields": ["id"],
                "highlight": {"fields": ["body"], "fragment_size": 100},
                "limit": 1,
            }
            expected = client.query({"request": query})["hits"]
            assert expected["hits"][0]["_source"] == {"id": case_id}
            assert "needle" in expected["hits"][0]["_highlights"]["body"][0]["text"]
            samples = []
            for sample in range(3):
                for timeout_ms in (None, 25, 50):
                    request = dict(query)
                    if timeout_ms is not None:
                        request["timeout_ms"] = timeout_ms
                    started = time.perf_counter_ns()
                    response = client.session.post(
                        server.api_url + f"/tables/{bench.TABLE}/query",
                        json=request,
                        timeout=60,
                    )
                    elapsed_ms = (time.perf_counter_ns() - started) / 1e6
                    record = {
                        "sample": sample,
                        "timeout_ms": timeout_ms,
                        "elapsed_ms": elapsed_ms,
                        "status": response.status_code,
                    }
                    if response.ok:
                        value = response.json()
                        assert value.get("responses", [value])[0]["hits"] == expected
                    else:
                        record["body"] = response.text[:1000]
                    samples.append(record)
            row = {
                "case": name,
                "field_bytes": 8 * 1024 * 1024 + len("sparsehead needle "),
                "samples": samples,
                "median_ms": statistics.median(
                    item["elapsed_ms"] for item in samples if item["timeout_ms"] is None
                ),
                "hits_sha256": hashlib.sha256(
                    json.dumps(expected, sort_keys=True).encode()
                ).hexdigest(),
            }
            rows.append(row)
            print(json.dumps(row), flush=True)
    finally:
        client.session.close()
assert sha(binary) == binary_hash
(out / "report.json").write_text(
    json.dumps(
        {
            "source_commit": source,
            "binary_sha256": binary_hash,
            "runner_sha256": sha(__file__),
            "parquet_sha256": sha(archive),
            "rows": rows,
            "qualification": "Warm local Debug; three samples per point; synthetic real Parquet. Two sparse 8-MiB fields, matches at opposite ends, one returned hit per request. No task-owned build, E2E or cleanup overlap; shared host. No Release, cloud or archive-scale claim.",
        },
        indent=2,
    )
    + "\n"
)
