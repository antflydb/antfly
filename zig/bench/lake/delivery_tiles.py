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

"""Qualify broad/native delivery and narrow controls using immutable executables."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import statistics
import sys
import time

root = Path.cwd()
sys.path.insert(0, str(root / "zig/e2e/antfly"))
import conftest as h

spec = importlib.util.spec_from_file_location(
    "delivery_bench", root / "zig/tools/benchmark_lake_archive.py"
)
b = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = b
spec.loader.exec_module(b)
import pyarrow as pa
import pyarrow.parquet as pq

label, binary_arg, source = sys.argv[1:]
case = os.environ.get("DELIVERY_CASE", "source")
assert case in ("source", "cover")
narrow = os.environ.get("DELIVERY_NARROW") == "1"
base = root / ".benchmark-results/lake/range-field-delivery"
shared = base / "input"
shared.mkdir(parents=True, exist_ok=True)
out = base / label
out.mkdir(exist_ok=True)
archive = shared / "archive.parquet"
count = 256
if not archive.exists():
    data = {
        "id": list(range(count)),
        "bucket": [i // 64 for i in range(count)],
        "body": ["control quasar"] * count,
        "unused": ["u" * 512] * count,
    }
    for column in range(8):
        data[f"v{column}"] = [9007199254740993 + i * 8 + column for i in range(count)]
    pq.write_table(pa.table(data), archive, row_group_size=128, use_dictionary=False)
fixture = b.import_fixture(shared / "fixture", [archive], "range-field-delivery")
properties = {
    n: {"type": "integer", "x-antfly-field": {"type": "number", "sortable": True}}
    for n in ("id", "bucket")
}
properties.update({f"v{i}": {"type": "integer"} for i in range(8)})
properties["body"] = {"type": "string", "x-antfly-field": {"type": "text"}}
properties["unused"] = {
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
            "table_id": "range-field-delivery",
            "object_mutability": "immutable",
            "uri": fixture["manifest_uri"],
            "row_identity": {"key_columns": ["id"]},
        },
        "document_schemas": {
            "row": {"schema": {"type": "object", "properties": properties}}
        },
        "index_sort": [{"field": n, "order": "asc"} for n in ("id", "_id")],
    },
    "indexes": {"text": {"type": "full_text", "store_source": case == "source"}},
}
if case == "cover":
    definition["schema"]["relational_indexes"] = [
        {
            "name": "covered",
            "keys": [{"column": "id"}],
            "include_columns": ["bucket"] + [f"v{i}" for i in range(8)],
        }
    ]
fields = ["id", "bucket"] + ([] if narrow else [f"v{i}" for i in range(8)])
query = {
    "full_text_search": {"match": "quasar", "field": "body"},
    "fields": fields,
    "order_by": [{"field": "id", "desc": False}],
    "limit": count,
    "profile": True,
}
config = h._write_remote_content_e2e_config(shared)
os.environ.pop("ANTFLY_E2E_FULL_LAKE", None)
os.environ["ANTFLY_BENCH_QUERY_PROFILE_EVERY"] = "1"
binary = Path(binary_arg).resolve()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


binary_hash = sha(binary)
samples = []
with b.managed_server(
    h.StandaloneAntflyServer, binary, config, out, api_root=h.ANTFLY_PUBLIC_API_ROOT
) as server:
    client = b.Client(server, 120)
    try:
        client.call("POST", f"/tables/{b.TABLE}", definition)
        client.ready(b.readiness_indexes({"definition": definition}))
        for sample in range(3):
            server.restart()
            for mode in ("process_restarted", "warm"):
                start = time.perf_counter_ns()
                response = client.query({"request": query})
                elapsed = time.perf_counter_ns() - start
                hits = response["hits"]["hits"]
                assert [hit["_source"]["id"] for hit in hits] == list(range(count))
                for i, hit in enumerate(hits):
                    assert set(hit["_source"]) == set(fields)
                    assert hit["_source"]["bucket"] == i // 64
                    for column in range(0 if narrow else 8):
                        assert (
                            hit["_source"][f"v{column}"]
                            == 9007199254740993 + i * 8 + column
                        )
                samples.append(
                    {
                        "sample": sample,
                        "cache_mode": mode,
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
    "source_commit": source,
    "binary_sha256": binary_hash,
    "runner_sha256": sha(__file__),
    "parquet_sha256": sha(archive),
    "config_sha256": sha(config),
    "case": case,
    "narrow": narrow,
    "rows": count,
    "definition": definition,
    "query": query,
    "cache_contract": "Restarts retain OS and persistent caches and readiness warming; no empty-cache claim.",
    "samples": samples,
    "summary": {
        mode: statistics.median(
            s["elapsed_ns"] for s in samples if s["cache_mode"] == mode
        )
        for mode in ("process_restarted", "warm")
    },
}
(out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report["summary"], indent=2), flush=True)
