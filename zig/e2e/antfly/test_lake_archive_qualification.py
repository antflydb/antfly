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
    else:
        for limit in (1, 12, 64):
            dense = dict(plan["cases"][0]["request"], limit=limit)
            dense["filter_query"] = {"range": {"path": "/hn_id", "gte": 0}}
            plan["cases"].append(
                {
                    "name": f"dense_metadata_{limit}",
                    "identity_field": "hn_id",
                    "expected_ids": list(range(limit)),
                    "request": dense,
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
            env=dict(os.environ, ANTFLY_BENCH_QUERY_PROFILE_EVERY="1"),
        )
    assert result.returncode == 0, log.read_text()
    report = json.loads((output / "report.json").read_text())
    assert report["complete"] is True
    assert set(report["initial_index_revisions"]) == (
        {"text"} if bounded_windows else {"text", "hn_key"}
    )
    assert all(report["initial_index_revisions"].values())
    assert len(report["samples"]) == (6 if bounded_windows else 10)
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
    if not bounded_windows:
        for limit in (1, 12, 64):
            dense_profiles = [
                s["profile"]["sort"]
                for s in report["samples"]
                if s["case"] == f"dense_metadata_{limit}"
            ]
            assert len(dense_profiles) == 2
            assert all(
                p["ordered_scanned_count"] <= limit + 2 for p in dense_profiles
            ), dense_profiles
            assert all(
                p["candidate_count"] <= limit + 1 for p in dense_profiles
            ), dense_profiles
        membership = [
            line for line in (output / "server.log").read_text().splitlines()
            if "antfly_bench_metadata_membership " in line
        ]
        metadata_samples = [
            sample for sample in report["samples"]
            if sample["case"] == "filtered_identity"
            or sample["case"].startswith("dense_metadata_")
        ]
        assert len(membership) == len(metadata_samples), membership
        for sample, line in zip(metadata_samples, membership, strict=True):
            # The selective range may exhaust point admission while walking
            # its unmatched prefix. Exact fallback remains valid for that case.
            if sample["case"].startswith("dense_metadata_"):
                assert "materialized=false" in line, (sample["case"], line)
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
                "missing_null_policy": "nulls_ordered",
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
    plan["definition"]["schema"]["relational_indexes"] = [
        {"name": "hn_metadata", "keys": [{"column": "hn_id"}]}
    ]
    first = plan["cases"][0]
    plan["cases"].append(
        dict(
            first,
            name="broad_metadata_ordered",
            request=dict(
                first["request"], filter_query={"range": {"path": "/hn_id", "gte": 0}}
            ),
        )
    )
    # Fixed NULL boundaries verify both public cursor gates and native replay.
    # The empty ID is below every public ID at the unique hn_id boundary.
    for descending in [False, True]:
        ordered = sorted(
            range(count),
            key=lambda i: (
                (ranks[i] is None, -ranks[i] if ranks[i] is not None else 0, -i)
                if descending
                else (ranks[i] is not None, ranks[i] if ranks[i] is not None else 0, i)
            ),
        )
        null_ids = [i for i in ordered if ranks[i] is None]
        boundary = null_ids[len(null_ids) // 2]
        position = ordered.index(boundary)
        for before in [False, True]:
            expected = (
                ordered[max(0, position - 20) : position]
                if before
                else ordered[position : position + 20]
            )
            plan["cases"].append(
                {
                    "name": f"null_cursor_{'desc' if descending else 'asc'}_{'before' if before else 'after'}",
                    "identity_field": "hn_id",
                    "expected_ids": expected,
                    "request": {
                        "full_text_search": {"disjuncts": terms},
                        "full_text_index": "text",
                        "fields": ["hn_id", "rank"],
                        "profile": True,
                        "limit": 20,
                        "order_by": [
                            {"field": "rank", "desc": descending},
                            {"field": "hn_id", "desc": descending},
                        ],
                        "search_before" if before else "search_after": [
                            None,
                            boundary,
                            "",
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
    # Remote cursors must carry the publication-bound token returned by a
    # matching query. Bind each static boundary, and also replay real tuples.
    import importlib.util

    import conftest as harness

    spec = importlib.util.spec_from_file_location(
        "nullable_archive_bench", root / "tools" / "benchmark_lake_archive.py"
    )
    bench = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = bench
    spec.loader.exec_module(bench)
    output = tmp_path / "qualification"
    output.mkdir()
    fixture = bench.import_fixture(output / "fixture", [archive], "nullable-archive")
    plan["definition"]["schema"]["base_source"]["uri"] = fixture["manifest_uri"]
    samples = []

    def verify(case, response, restarted):
        bench.verify_case(case, response)
        if case["name"] == "exact_count":
            assert response["hits"]["total"] == {"value": count, "relation": "exact"}
        else:
            profile = response["profile"]["sort"]
            assert profile["index_sort_coverage"] == "covered_with_bounds", profile
            assert profile["candidate_count"] < 128, profile
        samples.append(
            {
                "case": case["name"],
                "restarted": restarted,
                "profile": response.get("profile"),
                "expected_ids": case["expected_ids"],
            }
        )

    with bench.managed_server(
        harness.StandaloneAntflyServer,
        binary,
        config,
        output,
        api_root=harness.ANTFLY_PUBLIC_API_ROOT,
    ) as server:
        client = bench.Client(server, 180)
        try:
            client.call("POST", f"/tables/{bench.TABLE}", plan["definition"])
            client.ready(bench.readiness_indexes(plan))
            for restarted in [False, True]:
                if restarted:
                    server.restart()
                    client.ready(bench.readiness_indexes(plan))
                for case in plan["cases"]:
                    request = dict(case["request"])
                    if "search_after" in request or "search_before" in request:
                        bootstrap = {
                            key: value
                            for key, value in request.items()
                            if key not in {"search_after", "search_before"}
                        }
                        bound = client.query({"request": bootstrap})
                        request["remote_snapshot"] = bound["remote_snapshot"]
                    response = client.query({"request": request})
                    verify(case, response, restarted)
                    if case["name"] != "or_asc":
                        continue
                    all_ids = sorted(
                        range(count),
                        key=lambda i: (
                            ranks[i] is not None,
                            ranks[i] if ranks[i] is not None else 0,
                            i,
                        ),
                    )
                    cursor = response["hits"]["hits"][-1]["_sort"]
                    assert cursor[0] is None
                    after = client.query(
                        {
                            "request": dict(
                                request,
                                search_after=cursor,
                                remote_snapshot=response["remote_snapshot"],
                            )
                        }
                    )
                    verify(
                        dict(
                            case, name="literal_null_after", expected_ids=all_ids[20:40]
                        ),
                        after,
                        restarted,
                    )
                    before = client.query(
                        {
                            "request": dict(
                                request,
                                search_before=after["hits"]["hits"][0]["_sort"],
                                remote_snapshot=after["remote_snapshot"],
                            )
                        }
                    )
                    verify(
                        dict(
                            case, name="literal_null_before", expected_ids=all_ids[:20]
                        ),
                        before,
                        restarted,
                    )
        finally:
            client.session.close()
    assert len(samples) == 24
    (output / "report.json").write_text(
        json.dumps({"samples": samples, "complete": True}, indent=2)
    )


def test_small_archive_uses_metadata_order_and_native_range_seeks(tmp_path, monkeypatch):
    """A single admission window must retain bounded seeks for a small page."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = Path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))).resolve()
    if not binary.is_file():
        pytest.skip("Build the Antfly binary first")
    root = Path(__file__).resolve().parents[2]
    count = 8192
    # Independent shuffled input; physical ordering is produced by Antfly.
    ids = list(range(0, count, 2)) + list(range(1, count, 2))
    archive = tmp_path / "archive.parquet"
    pq.write_table(
        pa.table({"id": ids, "revision": [0] * count, "body": ["quasar"] * count}),
        archive,
        row_group_size=128,
    )
    properties = {
        name: {
            "type": "integer",
            "x-antfly-field": {"type": "number", "sortable": True},
        }
        for name in ("id", "revision")
    }
    properties["body"] = {"type": "string", "x-antfly-field": {"type": "text"}}
    plan = {
        "version": 1,
        "definition": {
            "schema": {
                "storage_mode": "relational",
                "default_type": "row",
                "base_source": {
                    "kind": "external",
                    "format": "parquet",
                    "table_id": "range-seeks",
                    "object_mutability": "immutable",
                    "uri": "file:///replaced",
                    "row_identity": {"key_columns": ["id"]},
                },
                "document_schemas": {
                    "row": {"schema": {"type": "object", "properties": properties}}
                },
                "index_sort": [
                    {"field": name, "order": "asc"}
                    for name in ("revision", "id", "_id")
                ],
                "relational_indexes": [
                    {"name": "metadata", "keys": [{"column": "id"}]}
                ],
            },
            "indexes": {"text": {"type": "full_text", "store_source": True}},
        },
        "cases": [],
    }
    for native in (False, True):
        for selection in ("dense", "tail", "reverse"):
            descending = selection == "reverse"
            first = (
                count // 2 - 1 if descending
                else count // 2 if selection == "tail"
                else 0
            )
            expected = list(
                range(first, first - 20, -1) if descending
                else range(first, first + 20)
            )
            fields = ("revision", "id") if native else ("id",)
            plan["cases"].append({
                "name": f"{'native' if native else 'metadata'}_{selection}",
                "identity_field": "id",
                "expected_ids": expected,
                "request": {
                    "full_text_search": {"match": "quasar", "field": "body"},
                    "full_text_index": "text",
                    "filter_query": {
                        "range": {"path": "/id", "lte" if descending else "gte": first}
                    },
                    "fields": ["id"],
                    "limit": 20,
                    "profile": True,
                    "order_by": [
                        {"field": name, "desc": descending} for name in fields
                    ],
                },
            })
    plan["cases"].append({
        "name": "exact_count",
        "identity_field": "id",
        "expected_ids": [],
        "request": {
            "full_text_search": {"match": "quasar", "field": "body"},
            "full_text_index": "text",
            "filter_query": {"range": {"path": "/id", "gte": count // 2}},
            "order_by": [{"field": "id"}],
            "fields": ["id"],
            "limit": 0,
            "profile": True,
        },
    })
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    output = tmp_path / "qualification"
    log = tmp_path / "qualification.log"
    with log.open("w") as stream:
        result = subprocess.run(
            [
                sys.executable,
                str(root / "tools" / "benchmark_lake_archive.py"),
                "run",
                "--binary", str(binary),
                "--output-dir", str(output),
                "--plan", str(plan_path),
                "--parquet", str(archive),
                "--samples", "1",
                "--timeout", "120",
            ],
            stdout=stream,
            stderr=subprocess.STDOUT,
            timeout=240,
            env=dict(os.environ, ANTFLY_BENCH_QUERY_PROFILE_EVERY="1"),
        )
    assert result.returncode == 0, log.read_text()
    report = json.loads((output / "report.json").read_text())
    assert report["complete"] is True
    assert len(report["samples"]) == 14
    count_answer = json.loads((output / "exact_count-answer.json").read_text())
    assert count_answer["hits"]["total"] == {"value": count // 2, "relation": "exact"}
    # Only the two explicit exact-count samples may convert complete metadata
    # membership. Ordered pages must reuse their cursor's full filter proof.
    membership = [
        line for line in (output / "server.log").read_text().splitlines()
        if "antfly_bench_metadata_membership " in line
    ]
    assert sum("materialized=true" in line for line in membership) <= 2, membership
    for sample in report["samples"]:
        if sample["case"] == "exact_count":
            continue
        profile = sample["profile"]["sort"]
        assert 0 < profile["ordered_scanned_count"] <= 64, (sample["case"], profile)
        assert profile["candidate_count"] <= 64, (sample["case"], profile)
        assert profile["candidate_source"] == (
            "sorted_segment_membership"
            if sample["case"].startswith("native_")
            else "ordered_lake_index"
        ), (sample["case"], profile)

    # Replay actual public cursor tuples under their retained publication,
    # including a reversed primary scan whose public-ID direction stays forward.
    import importlib.util
    import conftest as harness

    spec = importlib.util.spec_from_file_location(
        "ordered_archive_bench", root / "tools" / "benchmark_lake_archive.py"
    )
    bench = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = bench
    spec.loader.exec_module(bench)
    cursor_output = tmp_path / "cursors"
    cursor_output.mkdir()
    fixture = bench.import_fixture(cursor_output / "fixture", [archive], "range-seeks")
    plan["definition"]["schema"]["base_source"]["uri"] = fixture["manifest_uri"]
    config = _write_remote_content_e2e_config(cursor_output)
    monkeypatch.setenv("ANTFLY_BENCH_QUERY_PROFILE_EVERY", "1")
    with bench.managed_server(
        harness.StandaloneAntflyServer, binary, config, cursor_output,
        api_root=harness.ANTFLY_PUBLIC_API_ROOT,
    ) as server:
        client = bench.Client(server, 120)
        try:
            client.call("POST", f"/tables/{bench.TABLE}", plan["definition"])
            client.ready(bench.readiness_indexes(plan))
            for descending in (False, True):
                request = dict(plan["cases"][0]["request"],
                               order_by=[{"field": "id", "desc": descending}])
                ordered = list(range(count - 1, -1, -1) if descending else range(count))
                first = client.query({"request": request})
                second = client.query({"request": dict(
                    request, search_after=first["hits"]["hits"][-1]["_sort"],
                    remote_snapshot=first["remote_snapshot"],
                )})
                before = client.query({"request": dict(
                    request, search_before=second["hits"]["hits"][0]["_sort"],
                    remote_snapshot=second["remote_snapshot"],
                )})
                for response, expected in ((first, ordered[:20]),
                                           (second, ordered[20:40]),
                                           (before, ordered[:20])):
                    bench.verify_case({"name": "cursor", "identity_field": "id",
                                       "expected_ids": expected}, response)
                    profile = response["profile"]["sort"]
                    assert profile["candidate_source"] == "ordered_lake_index", profile
                    # Keep the inclusive cursor row and one lookahead until
                    # the public-ID collector resolves the boundary tie.
                    assert profile["ordered_scanned_count"] <= 22, profile
            assert "materialized=true" not in server.log_path.read_text()
            # This row predicate is not enforced by the id index. Removing it
            # with the proved id range would incorrectly return twenty rows.
            request = dict(plan["cases"][0]["request"], filter_query={"conjuncts": [
                {"range": {"id": {"gte": 0, "lt": 20}}},
                {"range": {"path": "/revision", "gte": 1}},
            ]})
            residual = client.query({"request": request})
            bench.verify_case({"name": "residual", "expected_ids": []}, residual)
        finally:
            client.session.close()
