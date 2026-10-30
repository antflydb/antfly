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

"""Real Parquet wide composition, chunked strings and bounded global statistics.

Usage: live_delivery.py LABEL IMMUTABLE_BINARY SOURCE_COMMIT MODE
Run before/after and after/before without overlapping builds, E2E or cleanup.
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
    "typed_delivery_bench", root / "zig/tools/benchmark_lake_archive.py"
)
bench = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = bench
spec.loader.exec_module(bench)
label, executable, source, mode = sys.argv[1:]
assert mode in ("wide", "ascii", "escaped", "statistics")
binary = Path(executable).resolve()
base = root / ".benchmark-results/lake/live-admission-v1-qualified" / mode
shared = base / "input"
shared.mkdir(parents=True, exist_ok=True)
out = base / label
out.mkdir(exist_ok=True)
count = 128 if mode == "wide" else 128 if mode == "statistics" else 12
body = {
    "wide": "needle small UTF-8 🙂",
    "ascii": ("needle " + "x" * 65520) * 4,
    "escaped": ("needle " + "x" * 65520 + '🙂\n"\\') * 4,
    "statistics": "unused",
}[mode]
columns = {
    "id": list(range(count)),
    "body": [
        f"quasar{i:03d}" if mode == "statistics" else body + str(i)
        for i in range(count)
    ],
}
for ordinal in range(64 if mode == "wide" else 4 if mode != "statistics" else 0):
    columns[f"value_{ordinal:02}"] = [
        None if ordinal == 3 and i % 3 == 0 else 9007199254740993 + ordinal + i
        for i in range(count)
    ]
archives = []
for shard in range(2 if mode == "statistics" else 1):
    archive = shared / f"archive-{shard}.parquet"
    begin, end = (shard * 64, shard * 64 + 64) if mode == "statistics" else (0, count)
    part = {name: values[begin:end] for name, values in columns.items()}
    if not archive.exists():
        pq.write_table(
            pa.table(part),
            archive,
            row_group_size=4 if mode != "wide" else 128,
            use_dictionary=False,
            data_page_size=256 * 1024,
            write_batch_size=1,
        )
    assert pq.read_table(archive).to_pydict() == part
    archives.append(archive)
fixture = bench.import_fixture(
    shared / "fixture", archives, f"live-admission-v1-{mode}"
)
properties = {
    name: {"type": "string", "x-antfly-field": {"type": "text"}}
    if name == "body"
    else {"type": "integer", "nullable": name == "value_03"}
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
            "table_id": f"live-admission-v1-{mode}",
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
        ("native", list(columns) if mode == "wide" else ["body"]),
        (
            "composed_identity",
            ["_id"] + (list(columns) if mode == "wide" else ["body"]),
        ),
    )
}
if mode == "statistics":
    queries = {
        "batched_exact_terms": {
            "full_text_search": {
                "disjuncts": [
                    {"match": f"quasar{i:03d}", "field": "body"} for i in range(count)
                ]
            },
            "fields": ["id"],
            "limit": count,
            "profile": True,
        },
        "bounded_pattern": {
            "full_text_search": {"prefix": "quasar", "field": "body"},
            "fields": ["id"],
            "limit": count,
            "profile": True,
        },
    }
if mode == "wide":
    queries["physical_sql_scan"] = {
        "statement": f"SELECT {','.join(columns)} FROM {bench.TABLE} ORDER BY id"
    }
config = harness._write_remote_content_e2e_config(out)
cfg = json.loads(config.read_text())
cfg.setdefault("lake_indexes", {}).setdefault("text_cache", {}).update(
    admission_window_segments=1
)
if mode == "statistics":
    cfg["lake_indexes"]["text_cache"]["admission_window_bytes"] = 1
config.write_text(json.dumps(cfg, sort_keys=True))


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


binary_hash = sha(binary)
samples = []
oracles = {}
physical_oracles = {}
with bench.managed_server(
    harness.StandaloneAntflyServer,
    binary,
    config,
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
            warm = client.query(
                request if "statement" in request else {"request": request}
            )
            if mode == "statistics":
                assert (
                    warm["profile"]["sort"]["selection_reason"]
                    == "bounded_global_text_frontier"
                ), warm.get("profile")
        for sample in range(7):
            names = list(queries)
            if sample % 2:
                names.reverse()
            for name in names:
                started = time.perf_counter_ns()
                request = queries[name]
                response = client.query(
                    request if "statement" in request else {"request": request}
                )
                elapsed = time.perf_counter_ns() - started
                if "statement" in request:
                    expected = [
                        [
                            str(values[i]) if isinstance(values[i], int) else values[i]
                            for values in columns.values()
                        ]
                        for i in range(count)
                    ]
                    assert response["rows"] == expected
                    assert [column["name"] for column in response["columns"]] == list(
                        columns
                    )
                    complete = {
                        key: response[key]
                        for key in ("columns", "rows", "command_tag", "rows_affected")
                    }
                    if "sql_nulls" in response:
                        complete["sql_nulls"] = response["sql_nulls"]
                    digest = hashlib.sha256(
                        json.dumps(
                            complete, sort_keys=True, separators=(",", ":")
                        ).encode()
                    ).hexdigest()
                    assert oracles.setdefault(name, digest) == digest
                    samples.append(
                        {
                            "sample": sample,
                            "case": name,
                            "elapsed_ns": elapsed,
                            "complete_sha256": digest,
                            "rows": len(response["rows"]),
                        }
                    )
                    (out / "samples.json").write_text(json.dumps(samples, indent=2))
                    continue
                hits = response["hits"]["hits"]
                assert len(hits) == count
                ids = []
                for i, hit in enumerate(hits):
                    row = hit["_source"]["id"] if mode == "statistics" else i
                    ids.append(row)
                    expected = {
                        field: hit["_id"] if field == "_id" else columns[field][row]
                        for field in queries[name]["fields"]
                    }
                    assert hit["_source"] == expected, (name, i, sorted(hit["_source"]))
                assert sorted(ids) == list(range(count))
                complete = {"hits": hits, "total": response["hits"]["total"]}
                encoded = json.dumps(complete, sort_keys=True, separators=(",", ":"))
                digest = hashlib.sha256(encoded.encode()).hexdigest()
                assert oracles.setdefault(name, digest) == digest
                physical = {
                    "hits": [
                        {
                            **hit,
                            "_source": {
                                key: value
                                for key, value in hit["_source"].items()
                                if key != "_id"
                            },
                        }
                        for hit in hits
                    ],
                    "total": response["hits"]["total"],
                }
                physical_digest = hashlib.sha256(
                    json.dumps(physical, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest()
                assert (
                    physical_oracles.setdefault(name, physical_digest)
                    == physical_digest
                )
                samples.append(
                    {
                        "sample": sample,
                        "case": name,
                        "elapsed_ns": elapsed,
                        "complete_sha256": digest,
                        "total": response["hits"]["total"],
                        "profile": response.get("profile"),
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
    "parquet_sha256": {path.name: sha(path) for path in archives},
    "mode": mode,
    "config": cfg,
    "rows": count,
    "body_utf8_bytes": [len(value.encode()) for value in columns["body"]],
    "definition": definition,
    "queries": queries,
    "publication_ns": publication_ns,
    "samples": samples,
    "oracles": oracles,
    "physical_oracles": physical_oracles,
    "summary": {
        name: statistics.median(
            row["elapsed_ns"] for row in samples if row["case"] == name
        )
        for name in queries
    },
    "qualification": "Warm local Debug; seven samples per case; strict complete source, hit and total equivalence across same-main binaries in both orders. The 128-term statistics fixture sets both admission_window_segments and admission_window_bytes to 1 over two immutable files, and asserts the bounded_global_text_frontier plan. Profiles are retained; the tree-visit unit probe separately isolates term-frequency lookup work. No task-owned build/E2E/cleanup overlap; no Release, cloud, empty-cache, archive-scale or full-query memory claim.",
}
(out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report["summary"], indent=2), flush=True)
