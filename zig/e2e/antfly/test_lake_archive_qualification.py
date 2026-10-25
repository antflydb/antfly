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

"""Exercise the archive qualification CLI against independently written Parquet."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from conftest import DEFAULT_ANTFLY_BIN, _write_remote_content_e2e_config

pytestmark = [
    pytest.mark.fresh_antfly_process,
    pytest.mark.e2e_resource("antfly_process"),
]


@pytest.mark.parametrize("explicit_config", [False, True])
def test_real_parquet_archive_qualification_preserves_cache_and_refresh_contracts(
    tmp_path,
    explicit_config,
):
    pytest.importorskip("pyarrow")
    binary = Path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))).resolve()
    if not binary.is_file():
        pytest.skip("Build the Antfly binary first")
    root = Path(__file__).resolve().parents[2]
    output = tmp_path / "qualification"
    log = tmp_path / "qualification.log"
    config = _write_remote_content_e2e_config(tmp_path)
    command = [
        sys.executable,
        str(root / "tools" / "benchmark_lake_archive.py"),
        "run",
        "--binary",
        str(binary),
        "--server-config",
        str(config),
        "--output-dir",
        str(output),
        "--rows",
        "128",
        "--files",
        "2",
        "--row-group",
        "32",
        "--payload-bytes",
        "32",
        "--samples",
        "1",
        "--case",
        "id_lookup",
        "--case",
        "ordered_text",
        "--timeout",
        "120",
    ]
    if not explicit_config:
        position = command.index("--server-config")
        del command[position : position + 2]
    with log.open("w") as stream:
        completed = subprocess.run(
            command, stdout=stream, stderr=subprocess.STDOUT, timeout=240
        )
    assert completed.returncode == 0, log.read_text()
    report = json.loads((output / "report.json").read_text())
    assert report["version"] == 3
    assert report["refresh_workload"]["refresh_every"] == 10
    assert report["refresh"]["input"]["sha256"] != report["source_identity"]
    assert report["complete"] is True
    assert report["server_config_sha256"]
    assert report["harness_sha256"]
    assert report["runtime_environment_sha256"]
    assert report["server_config_origin"] == (
        "supplied" if explicit_config else "generated"
    )
    assert report["cache_contract"]["empty_cache_qualified"] is False
    assert report["source_identity_assurance"] == "verified_local_file_bytes"
    assert len(report["samples"]) == 4
    for case in ("id_lookup", "ordered_text"):
        samples = [s for s in report["samples"] if s["case"] == case]
        assert {s["cache_mode"] for s in samples} == {"process_restarted", "warm"}
        assert len({s["result_sha256"] for s in samples}) == 1
        assert samples[0]["restart_ns"] > 0
        assert (
            "antfly_lake_cache_provider_reads_total"
            in samples[0]["startup_metrics_since_boot"]
        )
        assert samples[0]["memory"]["scope"].startswith("server and descendants")
    explain = json.loads((output / "id_lookup-explain.json").read_text())
    runtime = json.loads(explain["rows"][0][0])["runtime"]
    assert runtime["coverage"]
    assert "spill_written_bytes" in runtime
    assert report["refresh"]["changed_rows"] == 13
    assert report["refresh"]["includes_fixture_encoding_and_commit"] is True
    assert report["refresh"]["artifacts"]["new_retained_bytes"] > 0
    assert (output / "server.log").is_file()


@pytest.mark.parametrize("bounded_windows", [False, True])
@pytest.mark.parametrize("sort_suffix", [False, True])
@pytest.mark.parametrize("additional_declarations", [False, True])
@pytest.mark.parametrize("physical_order", ["ascending", "shuffled"])
def test_imported_parquet_awaits_embedded_indexes_and_uses_declared_identity(
    tmp_path, additional_declarations, physical_order, sort_suffix, bounded_windows
):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = Path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))).resolve()
    if not binary.is_file():
        pytest.skip("Build the Antfly binary first")
    root = Path(__file__).resolve().parents[2]
    archive = tmp_path / "archive.parquet"
    row_count = 1024 if bounded_windows else 128
    ids = (
        list(range(row_count))
        if physical_order == "ascending"
        else list(range(0, row_count, 2)) + list(range(1, row_count, 2))
    )
    pq.write_table(
        pa.table(
            {
                "hn_id": ids,
                "revision": [i % 4 for i in ids],
                "body": [f"quasar row{i}" for i in ids],
            }
        ),
        archive,
        row_group_size=32,
    )
    text_index = {"type": "full_text", "store_source": True}
    plan = {
        "version": 1,
        "definition": {
            "schema": {
                "storage_mode": "relational",
                "default_type": "row",
                "index_sort": [
                    {"field": "hn_id", "order": "asc"},
                    {"field": "_id", "order": "asc"},
                ],
                "base_source": {
                    "kind": "external",
                    "format": "parquet",
                    "table_id": "hn",
                    "object_mutability": "immutable",
                    "uri": "file:///replaced",
                    "row_identity": {"key_columns": ["hn_id"]},
                },
                "document_schemas": {
                    "row": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "hn_id": {
                                    "type": "integer",
                                    "x-antfly-field": {
                                        "type": "number",
                                        "sortable": True,
                                    },
                                },
                                "body": {
                                    "type": "string",
                                    "x-antfly-field": {"type": "text"},
                                },
                            },
                        }
                    }
                },
                "relational_indexes": [
                    {"name": "hn_key", "keys": [{"column": "hn_id"}]}
                ],
            },
            "indexes": {"text": text_index},
        },
        # Identical declarations must not issue duplicate DDL.
        "indexes": {"text": text_index},
        "cases": [
            {
                "name": "custom_identity",
                "identity_field": "hn_id",
                "expected_ids": list(range(20)),
                "request": {
                    "full_text_search": {"match": "quasar", "field": "body"},
                    "fields": ["hn_id", "body"],
                    "profile": True,
                    "order_by": [{"field": "hn_id"}],
                    "limit": 20,
                },
            }
        ],
    }
    if sort_suffix:
        schema = plan["definition"]["schema"]
        schema["index_sort"].insert(1, {"field": "revision", "order": "asc"})
        schema["document_schemas"]["row"]["schema"]["properties"]["revision"] = {
            "type": "integer",
            "x-antfly-field": {"type": "number", "sortable": True},
        }
    filtered = dict(plan["cases"][0]["request"])
    filtered["filter_query"] = {"range": {"path": "/hn_id", "gte": 64}}
    plan["cases"].append(
        {
            "name": "filtered_identity",
            "identity_field": "hn_id",
            "expected_ids": list(range(64, 84)),
            "request": filtered,
        }
    )
    if bounded_windows:
        del plan["definition"]["schema"]["relational_indexes"]
        reverse = dict(
            plan["cases"][0]["request"], order_by=[{"field": "hn_id", "desc": True}]
        )
        plan["cases"].append(
            {
                "name": "reverse_identity",
                "identity_field": "hn_id",
                "expected_ids": list(range(row_count - 1, row_count - 21, -1)),
                "request": reverse,
            }
        )
    if not additional_declarations:
        del plan["indexes"]
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    config = _write_remote_content_e2e_config(tmp_path)
    if bounded_windows:
        configuration = json.loads(config.read_text())
        configuration.setdefault("lake_indexes", {}).setdefault(
            "text_cache", {}
        ).update(admission_window_segments=1, admission_window_bytes=1)
        config.write_text(json.dumps(configuration))
    output = tmp_path / "qualification"
    log = tmp_path / "qualification.log"
    with log.open("w") as stream:
        result = subprocess.run(
            [
                sys.executable,
                str(root / "tools" / "benchmark_lake_archive.py"),
                "run",
                "--binary",
                str(binary),
                "--server-config",
                str(config),
                "--output-dir",
                str(output),
                "--plan",
                str(plan_path),
                "--parquet",
                str(archive),
                "--samples",
                "1",
                "--timeout",
                "120",
            ],
            stdout=stream,
            stderr=subprocess.STDOUT,
            timeout=180,
        )
    assert result.returncode == 0, log.read_text()
    report = json.loads((output / "report.json").read_text())
    assert report["complete"] is True
    assert set(report["initial_index_revisions"]) == (
        {"text"} if bounded_windows else {"text", "hn_key"}
    )
    assert all(report["initial_index_revisions"].values())
    assert len(report["samples"]) == (6 if bounded_windows else 4)
    for case in ("custom_identity", "filtered_identity"):
        assert (
            len({s["result_sha256"] for s in report["samples"] if s["case"] == case})
            == 1
        )
    filtered_answer = json.loads((output / "filtered_identity-answer.json").read_text())
    assert [
        (h["_source"]["hn_id"], h["_source"]["body"])
        for h in filtered_answer["hits"]["hits"]
    ] == [(i, f"quasar row{i}") for i in range(64, 84)]
    answer = json.loads((output / "custom_identity-answer.json").read_text())
    assert [h["_source"]["hn_id"] for h in answer["hits"]["hits"]] == list(range(20))

    assert [h["_source"]["body"] for h in answer["hits"]["hits"]] == [
        f"quasar row{i}" for i in range(20)
    ]
    profiles = [s["profile"]["sort"] for s in report["samples"]]
    assert all(p["index_sort_coverage"] == "covered_with_bounds" for p in profiles), (
        profiles
    )
    assert all(0 < p["ordered_scanned_count"] < row_count for p in profiles), profiles
    assert all(p["candidate_count"] < row_count for p in profiles), profiles
    if bounded_windows:
        assert all(
            p["selection_reason"] == "bounded_best_first_sort" for p in profiles
        ), profiles


@pytest.mark.parametrize("kind", ["integer", "datetime"])
def test_nullable_native_ordering_and_compound_membership(tmp_path, kind):
    """Independent NULL/date oracles cover bounded Boolean top-K and exact counts."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = Path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))).resolve()
    if not binary.is_file():
        pytest.skip("Build the Antfly binary first")
    root = Path(__file__).resolve().parents[2]
    count = 4096
    ids = list(range(0, count, 2)) + list(range(1, count, 2))
    ranks = [
        None if i % 97 == 0 else (i - count // 2) * 1_000_000_000 + i % 11
        for i in range(count)
    ]
    values = [ranks[i] for i in ids]
    archive = tmp_path / "nullable.parquet"
    pq.write_table(
        pa.table(
            {
                "hn_id": ids,
                "rank": pa.array(
                    values,
                    type=pa.timestamp("ns", tz="UTC")
                    if kind == "datetime"
                    else pa.int64(),
                ),
                "body": ["quasar pulsar" if i % 3 == 0 else "quasar" for i in ids],
            }
        ),
        archive,
        row_group_size=128,
    )
    text = {"type": "full_text", "store_source": True}
    properties = {
        "hn_id": {
            "type": "integer",
            "x-antfly-field": {"type": "number", "sortable": True},
        },
        "rank": {
            "type": kind,
            "nullable": True,
            "x-antfly-field": {
                "type": "datetime" if kind == "datetime" else "number",
                "sortable": True,
            },
        },
        "body": {"type": "string", "x-antfly-field": {"type": "text"}},
    }
    terms = [{"match": "quasar", "field": "body"}, {"match": "pulsar", "field": "body"}]
    plan = {
        "version": 1,
        "definition": {
            "schema": {
                "storage_mode": "relational",
                "default_type": "row",
                "index_sort": [
                    {"field": "rank", "order": "asc"},
                    {"field": "hn_id", "order": "asc"},
                    {"field": "_id", "order": "asc"},
                ],
                "base_source": {
                    "kind": "external",
                    "format": "parquet",
                    "table_id": "nullable-archive",
                    "object_mutability": "immutable",
                    "uri": "file:///replaced",
                    "row_identity": {"key_columns": ["hn_id"]},
                },
                "document_schemas": {
                    "row": {"schema": {"type": "object", "properties": properties}}
                },
            },
            "indexes": {"text": text},
        },
        "cases": [],
    }
    for conjunction in [False, True]:
        matching = [i for i in range(count) if not conjunction or i % 3 == 0]
        for descending in [False, True]:

            def key(i):
                rank = ranks[i]
                if descending:
                    return rank is None, -rank if rank is not None else 0, -i
                return rank is not None, rank if rank is not None else 0, i

            plan["cases"].append(
                {
                    "name": f"{'and' if conjunction else 'or'}_{'desc' if descending else 'asc'}",
                    "identity_field": "hn_id",
                    "expected_ids": sorted(matching, key=key)[:20],
                    "request": {
                        "full_text_search": {
                            "conjuncts" if conjunction else "disjuncts": terms
                        },
                        "full_text_index": "text",
                        "fields": ["hn_id", "rank"],
                        "profile": True,
                        "limit": 20,
                        "order_by": [
                            {"field": "rank", "desc": descending},
                            {"field": "hn_id", "desc": descending},
                        ],
                    },
                }
            )
    plan["cases"].append(
        {
            "name": "exact_count",
            "identity_field": "hn_id",
            "expected_ids": [],
            "request": {
                "full_text_search": {"disjuncts": terms},
                "full_text_index": "text",
                "fields": ["hn_id"],
                "order_by": [{"field": "rank"}],
                "limit": 0,
            },
        }
    )
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    config = _write_remote_content_e2e_config(tmp_path)
    configuration = json.loads(config.read_text())
    configuration.setdefault("lake_indexes", {}).setdefault("text_cache", {}).update(
        admission_window_segments=1, admission_window_bytes=1
    )
    config.write_text(json.dumps(configuration))
    output = tmp_path / "qualification"
    log = tmp_path / "qualification.log"
    with log.open("w") as stream:
        result = subprocess.run(
            [
                sys.executable,
                str(root / "tools" / "benchmark_lake_archive.py"),
                "run",
                "--binary",
                str(binary),
                "--server-config",
                str(config),
                "--output-dir",
                str(output),
                "--plan",
                str(plan_path),
                "--parquet",
                str(archive),
                "--samples",
                "1",
                "--timeout",
                "180",
            ],
            stdout=stream,
            stderr=subprocess.STDOUT,
            timeout=300,
        )
    assert result.returncode == 0, log.read_text()
    report = json.loads((output / "report.json").read_text())
    assert report["complete"] and len(report["samples"]) == 10
    for sample in report["samples"]:
        if sample["case"] == "exact_count":
            continue
        profile = sample["profile"]["sort"]
        assert profile["index_sort_coverage"] == "covered_with_bounds", profile
        assert profile["candidate_count"] < 128, profile
    answer = json.loads((output / "exact_count-answer.json").read_text())
    assert answer["hits"]["total"] == {"value": count, "relation": "eq"}, answer
