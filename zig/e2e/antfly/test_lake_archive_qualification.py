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
            command, check=False, stdout=stream, stderr=subprocess.STDOUT, timeout=240
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
            check=False,
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
    assert all(
        p["index_sort_coverage"] == "covered_with_bounds"
        or (
            p["candidate_source"] == "ordered_lake_index"
            and p["selection_reason"] == "bounded_ordered_lake_index"
        )
        for p in profiles
    ), profiles
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
            assert all(p["candidate_count"] <= limit + 1 for p in dense_profiles), (
                dense_profiles
            )
        membership = [
            line
            for line in (output / "server.log").read_text().splitlines()
            if "antfly_bench_metadata_membership " in line
        ]
        metadata_samples = [
            sample
            for sample in report["samples"]
            if (
                sample["case"] == "filtered_identity"
                or sample["case"].startswith("dense_metadata_")
            )
            # A publication-bound whole-filter witness avoids re-evaluating
            # metadata membership in the global ordered coordinator.
            and sample["profile"]["sort"]["selection_reason"]
            != "bounded_ordered_lake_index"
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

            def key(i, descending=descending):
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


def test_small_archive_uses_metadata_order_and_native_range_seeks(
    tmp_path, monkeypatch
):
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
        pa.table(
            {
                "id": ids,
                "revision": [0] * count,
                "bucket": [i // 64 for i in ids],
                "body": ["quasar"] * count,
            }
        ),
        archive,
        row_group_size=128,
    )
    properties = {
        name: {
            "type": "integer",
            "x-antfly-field": {"type": "number", "sortable": True},
        }
        for name in ("id", "revision", "bucket")
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
                    {"name": "metadata", "keys": [{"column": "id"}]},
                    {
                        "name": "bucket_key",
                        "keys": [{"column": "bucket"}, {"column": "id"}],
                    },
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
                count // 2 - 1
                if descending
                else count // 2
                if selection == "tail"
                else 0
            )
            expected = list(
                range(first, first - 20, -1) if descending else range(first, first + 20)
            )
            fields = ("revision", "id") if native else ("id",)
            plan["cases"].append(
                {
                    "name": f"{'native' if native else 'metadata'}_{selection}",
                    "identity_field": "id",
                    "expected_ids": expected,
                    "request": {
                        "full_text_search": {"match": "quasar", "field": "body"},
                        "full_text_index": "text",
                        "filter_query": {
                            "range": {
                                "path": "/id",
                                "lte" if descending else "gte": first,
                            }
                        },
                        "fields": ["id"],
                        "limit": 20,
                        "profile": True,
                        "order_by": [
                            {"field": name, "desc": descending} for name in fields
                        ],
                    },
                }
            )
    plan["cases"].append(
        {
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
        }
    )
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
                "--binary",
                str(binary),
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
            check=False,
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
        line
        for line in (output / "server.log").read_text().splitlines()
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
    configuration = json.loads(config.read_text())
    configuration.setdefault("lake_indexes", {}).setdefault("text_cache", {}).update(
        admission_window_segments=1, admission_window_bytes=1
    )
    config.write_text(json.dumps(configuration))
    monkeypatch.setenv("ANTFLY_BENCH_QUERY_PROFILE_EVERY", "1")
    with bench.managed_server(
        harness.StandaloneAntflyServer,
        binary,
        config,
        cursor_output,
        api_root=harness.ANTFLY_PUBLIC_API_ROOT,
    ) as server:
        client = bench.Client(server, 120)
        try:
            client.call("POST", f"/tables/{bench.TABLE}", plan["definition"])
            client.ready(bench.readiness_indexes(plan))
            for descending in (False, True):
                request = dict(
                    plan["cases"][0]["request"],
                    order_by=[{"field": "id", "desc": descending}],
                )
                ordered = list(range(count - 1, -1, -1) if descending else range(count))
                first = client.query({"request": request})
                second = client.query(
                    {
                        "request": dict(
                            request,
                            search_after=first["hits"]["hits"][-1]["_sort"],
                            remote_snapshot=first["remote_snapshot"],
                        )
                    }
                )
                before = client.query(
                    {
                        "request": dict(
                            request,
                            search_before=second["hits"]["hits"][0]["_sort"],
                            remote_snapshot=second["remote_snapshot"],
                        )
                    }
                )
                for response, expected in (
                    (first, ordered[:20]),
                    (second, ordered[20:40]),
                    (before, ordered[:20]),
                ):
                    bench.verify_case(
                        {
                            "name": "cursor",
                            "identity_field": "id",
                            "expected_ids": expected,
                        },
                        response,
                    )
                    profile = response["profile"]["sort"]
                    assert profile["candidate_source"] == "ordered_lake_index", profile
                    # Keep the inclusive cursor row and one lookahead until
                    # the public-ID collector resolves the boundary tie.
                    assert profile["ordered_scanned_count"] <= 22, profile
            # The bucket index has an extra id key and cannot attest public-ID
            # tie order. The global coordinator must consume all 64 rows in the
            # winning primary group before retaining its best twenty IDs.
            for descending in (False, True):
                reference = client.query(
                    {
                        "request": dict(
                            plan["cases"][0]["request"],
                            limit=128,
                            order_by=[{"field": "id", "desc": descending}],
                        )
                    }
                )
                first_group = sorted(
                    reference["hits"]["hits"][:64], key=lambda hit: hit["_id"]
                )
                bucket_request = dict(
                    plan["cases"][0]["request"],
                    order_by=[{"field": "bucket", "desc": descending}],
                    filter_query={"range": {"path": "/bucket", "gte": 0}},
                )
                bucket = client.query({"request": bucket_request})
                bucket_after = client.query(
                    {
                        "request": dict(
                            bucket_request,
                            search_after=bucket["hits"]["hits"][-1]["_sort"],
                            remote_snapshot=bucket["remote_snapshot"],
                        )
                    }
                )
                bucket_before = client.query(
                    {
                        "request": dict(
                            bucket_request,
                            search_before=bucket_after["hits"]["hits"][0]["_sort"],
                            remote_snapshot=bucket_after["remote_snapshot"],
                        )
                    }
                )
                for response, expected in (
                    (bucket, first_group[:20]),
                    (bucket_after, first_group[20:40]),
                    (bucket_before, first_group[:20]),
                ):
                    assert [
                        hit["_source"]["id"] for hit in response["hits"]["hits"]
                    ] == [hit["_source"]["id"] for hit in expected]
                    profile = response["profile"]["sort"]
                    assert (
                        profile["selection_reason"] == "bounded_ordered_lake_index"
                    ), profile
                    assert profile["ordered_scanned_count"] <= 128, profile
            assert "materialized=true" not in server.log_path.read_text()
            # This row predicate is not enforced by the id index. Removing it
            # with the proved id range would incorrectly return twenty rows.
            request = dict(
                plan["cases"][0]["request"],
                filter_query={
                    "conjuncts": [
                        {"range": {"id": {"gte": 0, "lt": 20}}},
                        {"range": {"path": "/revision", "gte": 1}},
                    ]
                },
            )
            residual = client.query({"request": request})
            bench.verify_case({"name": "residual", "expected_ids": []}, residual)
        finally:
            client.session.close()


def test_ordered_metadata_boundary_handles_correlated_text_and_reports_fallback(
    tmp_path, monkeypatch
):
    """Index primary boundaries survive text/residual misses and declined plans."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = Path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))).resolve()
    if not binary.is_file():
        pytest.skip("Build the Antfly binary first")
    import importlib.util

    import conftest as harness

    root = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location(
        "ordered_boundary_bench", root / "tools" / "benchmark_lake_archive.py"
    )
    bench = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = bench
    spec.loader.exec_module(bench)
    count = 8192
    archive = tmp_path / "archive.parquet"
    pq.write_table(
        pa.table(
            {
                "id": list(range(count)),
                "bucket": [i // 64 for i in range(count)],
                "constant": [0] * count,
                "keep": [i < 64 for i in range(count)],
                "keep_tail": [i >= count - 64 for i in range(count)],
                "body": [
                    "control quasar"
                    if i < 64
                    else "control pulsar"
                    if i >= count - 64
                    else "control"
                    for i in range(count)
                ],
            }
        ),
        archive,
        row_group_size=128,
    )
    fixture = bench.import_fixture(tmp_path / "fixture", [archive], "ordered-boundary")
    properties = {
        name: {
            "type": "integer",
            "x-antfly-field": {"type": "number", "sortable": True},
        }
        for name in ("id", "bucket", "constant")
    }
    properties.update(
        keep={"type": "boolean"},
        keep_tail={"type": "boolean"},
        body={"type": "string", "x-antfly-field": {"type": "text"}},
    )
    definition = {
        "schema": {
            "storage_mode": "relational",
            "default_type": "row",
            "base_source": {
                "kind": "external",
                "format": "parquet",
                "table_id": "ordered-boundary",
                "object_mutability": "immutable",
                "uri": fixture["manifest_uri"],
                "row_identity": {"key_columns": ["id"]},
            },
            "document_schemas": {
                "row": {"schema": {"type": "object", "properties": properties}}
            },
            "index_sort": [{"field": name, "order": "asc"} for name in ("id", "_id")],
            "relational_indexes": [
                {"name": name, "keys": [{"column": name}, {"column": "id"}]}
                for name in ("bucket", "constant")
            ],
        },
        "indexes": {"text": {"type": "full_text", "store_source": True}},
    }
    config = _write_remote_content_e2e_config(tmp_path)
    cfg = json.loads(config.read_text())
    cfg.setdefault("lake_indexes", {}).setdefault("text_cache", {}).update(
        admission_window_segments=1, admission_window_bytes=1
    )
    config.write_text(json.dumps(cfg))
    monkeypatch.setenv("ANTFLY_BENCH_QUERY_PROFILE_EVERY", "1")
    with bench.managed_server(
        harness.StandaloneAntflyServer,
        binary,
        config,
        tmp_path,
        api_root=harness.ANTFLY_PUBLIC_API_ROOT,
    ) as server:
        client = bench.Client(server, 120)
        try:
            client.call("POST", f"/tables/{bench.TABLE}", definition)
            client.ready(bench.readiness_indexes({"definition": definition}))
            for restarted in (False, True):
                if restarted:
                    server.restart()
                for term, descending in (("quasar", False), ("pulsar", True)):
                    request = {
                        # Keep text deliberately broad: sparse Boolean and
                        # phrase candidates correctly choose postings now. A
                        # residual with no matches beyond the first primary
                        # group still requires the authenticated boundary proof.
                        "full_text_search": {"match": "control", "field": "body"},
                        "fields": ["id", "bucket"],
                        "limit": 20,
                        "profile": True,
                        "filter_query": {
                            "conjuncts": [
                                {"range": {"path": "/bucket", "gte": 0}},
                                {
                                    "term": {
                                        "path": "/keep"
                                        if term == "quasar"
                                        else "/keep_tail",
                                        "value": True,
                                    }
                                },
                            ]
                        },
                    }
                    oracle = client.query(
                        {
                            "request": dict(
                                request, order_by=[{"field": "_id"}], limit=64
                            )
                        }
                    )
                    expected = oracle["hits"]["hits"]
                    assert len(expected) == 64
                    request["order_by"] = [{"field": "bucket", "desc": descending}]
                    first = client.query({"request": request})
                    after = client.query(
                        {
                            "request": dict(
                                request,
                                search_after=first["hits"]["hits"][-1]["_sort"],
                                remote_snapshot=first["remote_snapshot"],
                            )
                        }
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
                    for response, wanted in (
                        (first, expected[:20]),
                        (after, expected[20:40]),
                        (before, expected[:20]),
                    ):
                        assert [h["_id"] for h in response["hits"]["hits"]] == [
                            h["_id"] for h in wanted
                        ]
                        for actual, reference in zip(response["hits"]["hits"], wanted):
                            assert actual["_source"] == reference["_source"]
                            assert actual["_sort"] == [
                                reference["_source"]["bucket"],
                                reference["_id"],
                            ]
                            assert actual["_score"] == pytest.approx(
                                reference["_score"], rel=1e-6, abs=1e-7
                            )
                        profile = response["profile"]["sort"]
                        assert (
                            profile["selection_reason"] == "bounded_ordered_lake_index"
                        ), profile
                        assert profile["ordered_scanned_count"] <= 88, profile
                        assert 0 < profile["candidate_count"] <= 88, profile
                        assert profile["total_us"] > 0, profile
                    if term == "quasar":
                        residual_request = dict(
                            request,
                            full_text_search={"match": "control", "field": "body"},
                            filter_query={
                                "conjuncts": [
                                    request["filter_query"],
                                    {"term": {"path": "/keep", "value": True}},
                                ]
                            },
                        )
                        residual = client.query({"request": residual_request})
                        assert [h["_id"] for h in residual["hits"]["hits"]] == [
                            h["_id"] for h in expected[:20]
                        ]
                        assert (
                            residual["profile"]["sort"]["selection_reason"]
                            == "bounded_ordered_lake_index"
                        )
                        assert (
                            residual["profile"]["sort"]["ordered_scanned_count"] <= 88
                        )
                        counted_request = dict(residual_request, limit=0)
                        counted_request.pop("order_by")
                        counted = client.query({"request": counted_request})
                        assert counted["hits"]["total"] == {
                            "value": 64,
                            "relation": "exact",
                        }
                # Every row shares one primary value. Quota exhaustion must
                # preserve the exact fallback and all abandoned ordered work.
                request = {
                    "full_text_search": {"match": "control", "field": "body"},
                    "fields": ["id"],
                    "limit": 20,
                    "profile": True,
                }
                oracle = client.query(
                    {"request": dict(request, order_by=[{"field": "_id"}])}
                )
                fallback = client.query(
                    {
                        "request": dict(
                            request,
                            order_by=[{"field": "constant"}],
                            filter_query={"range": {"path": "/constant", "gte": 0}},
                        )
                    }
                )
                assert [h["_id"] for h in fallback["hits"]["hits"]] == [
                    h["_id"] for h in oracle["hits"]["hits"]
                ]
                profile = fallback["profile"]["sort"]
                assert (
                    profile["candidate_source"]
                    == "ordered_lake_index_then_text_windows"
                ), profile
                assert profile["ordered_scanned_count"] >= 1024, profile
                assert profile["candidate_count"] >= 1024, profile
        finally:
            client.session.close()


@pytest.mark.parametrize(
    "case",
    [
        "native_selective",
        "metadata_selective",
        "boolean_selective",
        "phrase_selective",
        "wide_boundary",
    ],
)
def test_ordered_demand_preserves_selective_native_path_and_bounds_key_storage(
    tmp_path, monkeypatch, case
):
    """Regression gates for access-path choice and wide unused index suffixes."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    import importlib.util

    import conftest as harness

    binary = Path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))).resolve()
    if not binary.is_file():
        pytest.skip("Build the Antfly binary first")
    root = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location(
        "ordered_demand_regression", root / "tools" / "benchmark_lake_archive.py"
    )
    bench = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = bench
    spec.loader.exec_module(bench)
    wide = case == "wide_boundary"
    count = 256 if wide else 8192
    data = {
        "id": list(range(count)),
        "bucket": [i // (128 if wide else 64) for i in range(count)],
        "body": [
            "control"
            if wide
            else "control quasar"
            if i < 64
            else "control pulsar"
            if i >= count - 64
            else "control"
            for i in range(count)
        ],
    }
    if wide:
        data["wide"] = ["w" * 131000 + str(i).zfill(4) for i in range(count)]
    archive = tmp_path / "archive.parquet"
    pq.write_table(
        pa.table(data),
        archive,
        row_group_size=128,
        use_dictionary=False,
        data_page_size=256 * 1024,
        write_batch_size=1,
    )
    fixture = bench.import_fixture(
        tmp_path / "fixture", [archive], "ordered-demand-regression"
    )
    properties = {
        name: {
            "type": "integer",
            "x-antfly-field": {"type": "number", "sortable": True},
        }
        for name in ("id", "bucket")
    }
    properties["body"] = {"type": "string", "x-antfly-field": {"type": "text"}}
    if wide:
        properties["wide"] = {
            "type": "string",
            "x-antfly-field": {"type": "text", "index": False},
        }
    keys = (
        [{"column": "bucket", "nulls": "first"}, {"column": "wide"}, {"column": "id"}]
        if wide
        else [{"column": "bucket", "nulls": "first"}, {"column": "id"}]
        if case in ("metadata_selective", "boolean_selective", "phrase_selective")
        else [{"column": "id", "nulls": "first"}]
    )
    definition = {
        "schema": {
            "storage_mode": "relational",
            "default_type": "row",
            "base_source": {
                "kind": "external",
                "format": "parquet",
                "table_id": "ordered-demand-regression",
                "object_mutability": "immutable",
                "uri": fixture["manifest_uri"],
                "row_identity": {"key_columns": ["id"]},
            },
            "document_schemas": {
                "row": {"schema": {"type": "object", "properties": properties}}
            },
            "index_sort": [{"field": name, "order": "asc"} for name in ("id", "_id")],
            "relational_indexes": [{"name": "ordered", "keys": keys}],
        },
        "indexes": {"text": {"type": "full_text", "store_source": True}},
    }
    config = _write_remote_content_e2e_config(tmp_path)
    monkeypatch.setenv("ANTFLY_BENCH_QUERY_PROFILE_EVERY", "1")
    with bench.managed_server(
        harness.StandaloneAntflyServer,
        binary,
        config,
        tmp_path,
        api_root=harness.ANTFLY_PUBLIC_API_ROOT,
    ) as server:
        client = bench.Client(server, 120)
        try:
            client.call("POST", f"/tables/{bench.TABLE}", definition)
            client.ready(bench.readiness_indexes({"definition": definition}))
            for restarted in (False, True):
                if restarted:
                    server.restart()
                if wide:
                    request = {
                        "full_text_search": {"match": "control", "field": "body"},
                        "fields": ["id", "bucket"],
                        "limit": 128,
                        "profile": True,
                    }
                    oracle = client.query(
                        {
                            "request": dict(
                                request,
                                order_by=[{"field": "_id"}],
                                filter_query={"range": {"path": "/bucket", "lte": 0}},
                            )
                        }
                    )
                    actual = client.query(
                        {"request": dict(request, order_by=[{"field": "bucket"}])}
                    )
                    assert [h["_id"] for h in actual["hits"]["hits"]] == [
                        h["_id"] for h in oracle["hits"]["hits"]
                    ]
                    assert (
                        actual["profile"]["sort"]["selection_reason"]
                        == "bounded_ordered_lake_index"
                    )
                    assert actual["profile"]["sort"]["ordered_scanned_count"] <= 129
                    # Long predicate bounds exercise compact range counts and
                    # reverse membership probes, in addition to a wide unused
                    # sort suffix. Both primary directions preserve public ties.
                    filtered = dict(
                        request,
                        filter_query={
                            "conjuncts": [
                                {"range": {"path": "/bucket", "gte": 0, "lte": 0}},
                                {
                                    "range": {
                                        "path": "/wide",
                                        "gte": data["wide"][32],
                                        "lt": data["wide"][64],
                                    }
                                },
                            ]
                        },
                    )
                    reference = client.query(
                        {"request": dict(filtered, order_by=[{"field": "_id"}])}
                    )
                    assert len(reference["hits"]["hits"]) == 32
                    for descending in (False, True):
                        selected = client.query(
                            {
                                "request": dict(
                                    filtered,
                                    order_by=[{"field": "bucket", "desc": descending}],
                                )
                            }
                        )
                        assert selected["hits"]["total"] == reference["hits"]["total"]
                        assert [
                            {key: value for key, value in hit.items() if key != "_sort"}
                            for hit in selected["hits"]["hits"]
                        ] == [
                            {key: value for key, value in hit.items() if key != "_sort"}
                            for hit in reference["hits"]["hits"]
                        ]
                else:
                    for term, descending, wanted in (
                        ("pulsar", False, list(range(8128, 8148))),
                        ("quasar", True, list(range(63, 43, -1))),
                    ):
                        if (
                            case
                            in (
                                "metadata_selective",
                                "boolean_selective",
                                "phrase_selective",
                            )
                            and descending
                        ):
                            wanted = list(range(20))
                        request = {
                            "full_text_search": {"match": term, "field": "body"},
                            "fields": ["id"],
                            "limit": 20,
                            "profile": True,
                            "order_by": [
                                {
                                    "field": "bucket"
                                    if case
                                    in (
                                        "metadata_selective",
                                        "boolean_selective",
                                        "phrase_selective",
                                    )
                                    else "id",
                                    "desc": descending,
                                }
                            ],
                        }
                        if case == "phrase_selective":
                            request["full_text_search"] = {
                                "terms": ["control", term],
                                "field": "body",
                            }
                        if case == "boolean_selective":
                            request["full_text_search"] = {
                                "conjuncts": [
                                    request["full_text_search"],
                                    {"match": "control", "field": "body"},
                                ]
                            }
                        if case in (
                            "metadata_selective",
                            "boolean_selective",
                            "phrase_selective",
                        ):
                            request["filter_query"] = {
                                "range": {"path": "/bucket", "gte": 0}
                            }
                        actual = client.query({"request": request})
                        assert [
                            h["_source"]["id"] for h in actual["hits"]["hits"]
                        ] == wanted
                        if case == "phrase_selective":
                            analyzed = client.query(
                                {
                                    "request": dict(
                                        request,
                                        full_text_search={
                                            "match_phrase": f"control {term}",
                                            "field": "body",
                                        },
                                    )
                                }
                            )
                            assert [
                                h["_source"]["id"] for h in analyzed["hits"]["hits"]
                            ] == wanted
                        profile = actual["profile"]["sort"]
                        assert profile["ordered_scanned_count"] <= 64, profile
                        assert profile["candidate_count"] <= 64, profile
                        assert "then_text_windows" not in profile["candidate_source"], (
                            profile
                        )
        finally:
            client.session.close()


def test_ordered_native_delivery_spans_packs_without_parquet_payloads(
    tmp_path, monkeypatch
):
    """Stored winners cross real serving packs without reopening Parquet data."""
    import base64
    import importlib.util
    import random
    import time

    import conftest as harness

    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = Path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))).resolve()
    if not binary.is_file():
        pytest.skip("Build the Antfly binary first")
    root = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location(
        "multi_pack_delivery", root / "tools" / "benchmark_lake_archive.py"
    )
    bench = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = bench
    spec.loader.exec_module(bench)
    # Incompressible stored values exceed the 16 MiB serving-pack target while
    # the final projection remains small. Each Parquet page stays bounded.
    rng = random.Random(17)
    count = 32
    data = {
        "id": list(range(count)),
        "bucket": [0] * count,
        "body": ["common needle"] * count,
        "payload": [
            base64.b64encode(rng.randbytes(768 * 1024)).decode() for _ in range(count)
        ],
    }
    archive = tmp_path / "archive.parquet"
    pq.write_table(
        pa.table(data),
        archive,
        row_group_size=4,
        use_dictionary=False,
        data_page_size=2 * 1024 * 1024,
        write_batch_size=1,
    )
    fixture_root = tmp_path / "fixture"
    fixture = bench.import_fixture(fixture_root, [archive], "multi-pack-delivery")
    properties = {
        name: {
            "type": "integer",
            "x-antfly-field": {"type": "number", "sortable": True},
        }
        for name in ("id", "bucket")
    }
    properties.update(
        body={"type": "string", "x-antfly-field": {"type": "text"}},
        payload={"type": "string", "x-antfly-field": {"type": "text", "index": False}},
    )
    definition = {
        "schema": {
            "storage_mode": "relational",
            "default_type": "row",
            "base_source": {
                "kind": "external",
                "format": "parquet",
                "table_id": "multi-pack-delivery",
                "object_mutability": "immutable",
                "uri": fixture["manifest_uri"],
                "row_identity": {"key_columns": ["id"]},
            },
            "document_schemas": {
                "row": {"schema": {"type": "object", "properties": properties}}
            },
            "index_sort": [{"field": name, "order": "asc"} for name in ("id", "_id")],
            "relational_indexes": [
                {
                    "name": "bucket",
                    "keys": [{"column": "bucket", "nulls": "first"}, {"column": "id"}],
                }
            ],
        },
        "indexes": {"text": {"type": "full_text", "store_source": True}},
    }
    config = _write_remote_content_e2e_config(tmp_path)
    cfg = json.loads(config.read_text())
    cfg.setdefault("lake_indexes", {}).setdefault("text_cache", {}).update(
        admission_window_segments=1, admission_window_bytes=1
    )
    config.write_text(json.dumps(cfg))
    monkeypatch.setenv("ANTFLY_BENCH_QUERY_PROFILE_EVERY", "1")
    with bench.managed_server(
        harness.StandaloneAntflyServer,
        binary,
        config,
        tmp_path,
        api_root=harness.ANTFLY_PUBLIC_API_ROOT,
    ) as server:
        client = bench.Client(server, 180)
        try:
            client.call("POST", f"/tables/{bench.TABLE}", definition)
            client.ready(bench.readiness_indexes({"definition": definition}))
            parquet_object = (
                fixture_root / "buckets" / "antfly" / "objects" / "import-0.parquet"
            )
            unavailable = parquet_object.with_suffix(".unavailable")
            parquet_object.rename(unavailable)
            try:
                for restarted in (False, True):
                    if restarted:
                        server.restart()
                    for ordered in (True, False):
                        for highlights in (False, True):
                            request = {
                                "full_text_search": {
                                    "match": "needle",
                                    "field": "body",
                                },
                                "fields": ["id"] if highlights else ["id", "body"],
                                "order_by": [{"field": "bucket"}],
                                "limit": 24,
                                "profile": True,
                            }
                            if not ordered:
                                request.pop("order_by")
                            if highlights:
                                request["highlight"] = {"fields": ["body"]}
                            before = client.metrics()
                            result = client.query({"request": request})
                            # The health endpoint refreshes its cached metrics
                            # asynchronously every five seconds. Wait for this
                            # isolated query's hydration before taking a delta.
                            deadline = time.monotonic() + 20
                            while True:
                                after = client.metrics()
                                if (
                                    after["antfly_lake_query_hydration_calls_total"]
                                    > before["antfly_lake_query_hydration_calls_total"]
                                ):
                                    break
                                assert time.monotonic() < deadline, (
                                    "hydration metrics did not refresh"
                                )
                                time.sleep(0.1)
                            hydration = {
                                k: after[k] - before[k]
                                for k in after
                                if "hydration" in k
                            }
                            hits = result["hits"]["hits"]
                            assert [hit["_source"]["id"] for hit in hits] == list(
                                range(24)
                            )
                            if ordered:
                                assert (
                                    result["profile"]["sort"]["selection_reason"]
                                    == "bounded_ordered_lake_index"
                                )
                            assert (
                                after["antfly_lake_query_hydration_native_calls_total"]
                                - before[
                                    "antfly_lake_query_hydration_native_calls_total"
                                ]
                                >= 2
                            ), {
                                "ordered": ordered,
                                "highlights": highlights,
                                "hydration": hydration,
                                "sort": result["profile"]["sort"],
                            }
                            assert (
                                after["antfly_lake_query_hydration_source_calls_total"]
                                == before[
                                    "antfly_lake_query_hydration_source_calls_total"
                                ]
                            )
                            for hit in hits:
                                if highlights:
                                    assert set(hit["_source"]) == {"id"}
                                    assert hit["_highlights"]["body"]
                                else:
                                    assert hit["_source"]["body"] == "common needle"
            finally:
                unavailable.rename(parquet_object)
        finally:
            client.session.close()


def test_native_source_column_batches_preserve_wide_projections_and_hidden_highlights(
    tmp_path,
):
    """Cross batch/chunk boundaries without physical source or hidden-field leaks."""
    import hashlib
    import importlib.util

    import conftest as harness

    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = Path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))).resolve()
    if not binary.is_file():
        pytest.skip("Build the Antfly binary first")
    spec = importlib.util.spec_from_file_location(
        "column_batch_e2e",
        Path(__file__).resolve().parents[2] / "tools" / "benchmark_lake_archive.py",
    )
    bench = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = bench
    spec.loader.exec_module(bench)
    count = 256
    data = {
        "id": list(range(count)),
        "bucket": [i // 16 for i in range(count)],
        "body": ["common needle"] * count,
        # Incompressible projected source spans real native transport blocks,
        # so chunk lookahead reaches the query-owned I/O queue.
        "note": [
            f"unrelated {i} " + hashlib.shake_256(str(i).encode()).hexdigest(2048)
            for i in range(count)
        ],
        "counter": [None if i % 7 == 0 else 9007199254740993 + i for i in range(count)],
        "payload": ["p" * 8192] * count,
    }
    for i in range(4):
        data[f"extra_{i}"] = [9007199254740993 + row + i for row in range(count)]
    highlight_fields = ["body"] + [f"highlight_{i}" for i in range(7)]
    for name in highlight_fields[1:]:
        data[name] = [f"needle {name} {i} " + "alpha " * 170 for i in range(count)]
    archive = tmp_path / "archive.parquet"
    pq.write_table(pa.table(data), archive, row_group_size=16, use_dictionary=False)
    fixture_root = tmp_path / "fixture"
    fixture = bench.import_fixture(fixture_root, [archive], "column-batch-e2e")
    properties = {
        name: {
            "type": "integer",
            "x-antfly-field": {"type": "number", "sortable": True},
        }
        for name in ("id", "bucket", "counter")
    }
    for i in range(4):
        properties[f"extra_{i}"] = {"type": "integer"}
    properties["counter"]["nullable"] = True
    properties.update(
        body={"type": "string", "x-antfly-field": {"type": "text"}},
        note={"type": "string", "x-antfly-field": {"type": "text"}},
        payload={"type": "string", "x-antfly-field": {"type": "text", "index": False}},
    )
    for name in highlight_fields[1:]:
        properties[name] = {"type": "string", "x-antfly-field": {"type": "text"}}
    definition = {
        "schema": {
            "storage_mode": "relational",
            "default_type": "row",
            "base_source": {
                "kind": "external",
                "format": "parquet",
                "table_id": "column-batch-e2e",
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
    config = _write_remote_content_e2e_config(tmp_path)
    with bench.managed_server(
        harness.StandaloneAntflyServer,
        binary,
        config,
        tmp_path,
        api_root=harness.ANTFLY_PUBLIC_API_ROOT,
    ) as server:
        client = bench.Client(server, 120)
        try:
            client.call("POST", f"/tables/{bench.TABLE}", definition)
            client.ready(bench.readiness_indexes({"definition": definition}))
            original = (
                fixture_root / "buckets" / "antfly" / "objects" / "import-0.parquet"
            )
            unavailable = original.with_suffix(".unavailable")
            original.rename(unavailable)
            try:
                for restarted in (False, True):
                    if restarted:
                        server.restart()
                    for descending in (False, True):
                        ids = (
                            list(reversed(range(count)))
                            if descending
                            else list(range(count))
                        )
                        request = {
                            "full_text_search": {"match": "needle", "field": "body"},
                            "fields": ["id", "bucket", "body", "note", "counter"]
                            + [f"extra_{i}" for i in range(4)],
                            "order_by": [{"field": "id", "desc": descending}],
                            "limit": 192,
                        }
                        first = client.query({"request": request})
                        second = client.query(
                            {
                                "request": dict(
                                    request,
                                    search_after=first["hits"]["hits"][-1]["_sort"],
                                    remote_snapshot=first["remote_snapshot"],
                                )
                            }
                        )
                        hits = first["hits"]["hits"] + second["hits"]["hits"]
                        assert [hit["_source"]["id"] for hit in hits] == ids
                        for hit in hits:
                            row = hit["_source"]
                            assert set(row) == set(request["fields"])
                            for name in request["fields"]:
                                assert row[name] == data[name][row["id"]]
                        highlight_query = {
                            "disjuncts": [
                                {"match": "needle", "field": name}
                                for name in highlight_fields
                            ]
                        }
                        visible = client.query(
                            {
                                "request": dict(
                                    request,
                                    full_text_search=highlight_query,
                                    fields=["id"] + highlight_fields,
                                    highlight={
                                        "fields": list(reversed(highlight_fields))
                                    },
                                )
                            }
                        )["hits"]["hits"]
                        hidden = client.query(
                            {
                                "request": dict(
                                    request,
                                    fields=["id"],
                                    full_text_search=highlight_query,
                                    highlight={
                                        "fields": list(reversed(highlight_fields))
                                    },
                                )
                            }
                        )["hits"]["hits"]
                        assert len(hidden) == len(visible) == 192
                        for hit, projected in zip(hidden, visible, strict=True):
                            assert set(hit["_source"]) == {"id"}
                            assert set(hit["_highlights"]) == set(highlight_fields)
                            assert list(hit["_highlights"]) == list(
                                reversed(highlight_fields)
                            )
                            assert {k: v for k, v in hit.items() if k != "_source"} == {
                                k: v for k, v in projected.items() if k != "_source"
                            }
            finally:
                unavailable.rename(original)
        finally:
            client.session.close()


def test_wide_covering_projection_uses_compact_identity_and_selected_columns(
    tmp_path, monkeypatch
):
    """A narrow covered result needs neither wide tuples nor original Parquet."""
    import importlib.util

    import conftest as harness

    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = Path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))).resolve()
    if not binary.is_file():
        pytest.skip("Build the Antfly binary first")
    root = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location(
        "covering_projection_regression", root / "tools" / "benchmark_lake_archive.py"
    )
    bench = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = bench
    spec.loader.exec_module(bench)
    count = 64
    archive = tmp_path / "wide.parquet"
    pq.write_table(
        pa.table(
            {
                "id": list(range(count)),
                "bucket": [i // 32 for i in range(count)],
                "wide": ["w" * 131000 + str(i).zfill(4) for i in range(count)],
                "body": ["control quasar"] * count,
            }
        ),
        archive,
        row_group_size=32,
        use_dictionary=False,
        data_page_size=256 * 1024,
        write_batch_size=1,
    )
    fixture = bench.import_fixture(tmp_path / "fixture", [archive], "covering-wide")
    properties = {
        name: {
            "type": "integer",
            "x-antfly-field": {"type": "number", "sortable": True},
        }
        for name in ("id", "bucket")
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
                "table_id": "covering-wide",
                "object_mutability": "immutable",
                "uri": fixture["manifest_uri"],
                "row_identity": {"key_columns": ["id"]},
            },
            "document_schemas": {
                "row": {"schema": {"type": "object", "properties": properties}}
            },
            "index_sort": [{"field": field} for field in ("id", "_id")],
            "relational_indexes": [
                {
                    "name": "covered",
                    "keys": [{"column": field} for field in ("bucket", "wide", "id")],
                }
            ],
        },
        "indexes": {"text": {"type": "full_text", "store_source": False}},
    }
    config = _write_remote_content_e2e_config(tmp_path)
    monkeypatch.setenv("ANTFLY_BENCH_QUERY_PROFILE_EVERY", "1")
    output = tmp_path / "server"
    output.mkdir()
    with bench.managed_server(
        harness.StandaloneAntflyServer,
        binary,
        config,
        output,
        api_root=harness.ANTFLY_PUBLIC_API_ROOT,
    ) as server:
        client = bench.Client(server, 120)
        try:
            client.call("POST", f"/tables/{bench.TABLE}", definition)
            client.ready(bench.readiness_indexes({"definition": definition}))
            original = tmp_path / "fixture/buckets/antfly/objects/import-0.parquet"
            unavailable = original.with_suffix(".unavailable")
            for restarted in (False, True):
                if restarted:
                    server.restart()
                client.ready(bench.readiness_indexes({"definition": definition}))
                original.rename(unavailable)
                try:
                    for descending in (False, True):
                        response = client.query(
                            {
                                "request": {
                                    "full_text_search": {
                                        "match": "quasar",
                                        "field": "body",
                                    },
                                    "fields": ["id", "bucket"],
                                    "order_by": [{"field": "id", "desc": descending}],
                                    "limit": 32,
                                }
                            }
                        )
                        expected = (
                            list(range(63, 31, -1)) if descending else list(range(32))
                        )
                        assert [
                            hit["_source"]["id"] for hit in response["hits"]["hits"]
                        ] == expected
                        assert all(
                            set(hit["_source"]) == {"id", "bucket"}
                            for hit in response["hits"]["hits"]
                        )
                    sql = client.call(
                        "POST",
                        "/sql",
                        {
                            "statement": f"SELECT id, bucket FROM {bench.TABLE} WHERE bucket = 0 ORDER BY wide, id LIMIT 32"
                        },
                    )
                    assert sql["rows"] == [[str(i), "0"] for i in range(32)]
                finally:
                    unavailable.rename(original)
        finally:
            client.session.close()


@pytest.mark.parametrize("source_format", ["parquet", "iceberg"])
def test_dense_highlights_preserve_results_and_structured_deadlines(
    tmp_path, source_format
):
    """Both source paths share exact snippets and cancellable CPU kernels."""
    import importlib.util

    import conftest as harness

    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = Path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))).resolve()
    if not binary.is_file():
        pytest.skip("Build the Antfly binary first")
    spec = importlib.util.spec_from_file_location(
        "highlight_cpu_e2e",
        Path(__file__).resolve().parents[2] / "tools" / "benchmark_lake_archive.py",
    )
    bench = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = bench
    spec.loader.exec_module(bench)
    count = 32
    body = "needle " * 9362
    sparse_body = "sparsecase needle " + "." * (8 * 1024 * 1024)
    vocabulary = " ".join(f"word{i:06d}" for i in range(512)) + " "
    selective_body = (
        vocabulary * ((2 * 1024 * 1024) // len(vocabulary))
    ) + "uniqueneedle"
    unicode_block = "🙂。" * 128 + " "
    unicode_payload = unicode_block * (
        (8 * 1024 * 1024) // len(unicode_block.encode("utf-8"))
    )
    unicode_payload += "." * (8 * 1024 * 1024 - len(unicode_payload.encode("utf-8")))
    unicode_body = "unicodecase " + unicode_payload + " needle"
    regular_body = "running " * 32768
    diverse_body = " ".join(f"broad{i:05d}" for i in range(8192)) + " "
    rows = pa.table(
        {
            "id": list(range(count + 5)),
            "body": [body] * count
            + [sparse_body, selective_body, unicode_body, regular_body, diverse_body],
        }
    )
    root = tmp_path / "fixture"
    if source_format == "parquet":
        archive = tmp_path / "archive.parquet"
        pq.write_table(rows, archive, row_group_size=1, use_dictionary=False)
        uri = bench.import_fixture(root, [archive], "highlight-cpu")["manifest_uri"]
    else:
        pytest.importorskip("pyiceberg")
        pytest.importorskip("fastavro")
        from pyiceberg.catalog.sql import SqlCatalog
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField, StringType
        from test_iceberg_sql import _export_table

        catalog = SqlCatalog(
            "highlight_cpu",
            uri=f"sqlite:///{tmp_path}/catalog.db",
            warehouse=(tmp_path / "warehouse").as_uri(),
        )
        catalog.create_namespace("highlight_cpu")
        table = catalog.create_table(
            "highlight_cpu.items",
            schema=Schema(
                NestedField(1, "id", LongType(), required=False),
                NestedField(2, "body", StringType(), required=False),
            ),
        )
        # Each large scalar stays below the reader's 16-MiB page limit.
        table.append(rows.slice(0, count + 2))
        table.append(rows.slice(count + 2))
        _export_table(table, root)
        uri = root.as_uri()
    definition = {
        "schema": {
            "storage_mode": "relational",
            "default_type": "row",
            "base_source": {
                "kind": "external",
                "format": source_format,
                "table_id": "highlight-cpu",
                "object_mutability": "immutable",
                "uri": uri,
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
            "index_sort": [{"field": name, "order": "asc"} for name in ("id", "_id")],
        },
        "indexes": {"text": {"type": "full_text", "store_source": True}},
    }
    config = _write_remote_content_e2e_config(tmp_path)
    with bench.managed_server(
        harness.StandaloneAntflyServer,
        binary,
        config,
        tmp_path,
        api_root=harness.ANTFLY_PUBLIC_API_ROOT,
    ) as server:
        client = bench.Client(server, 120)
        try:
            client.call("POST", f"/tables/{bench.TABLE}", definition)
            client.ready(bench.readiness_indexes({"definition": definition}))
            unavailable = []
            for path in (root / "buckets" / "antfly" / "objects").rglob("*.parquet"):
                renamed = path.with_suffix(".unavailable")
                path.rename(renamed)
                unavailable.append((path, renamed))
            assert unavailable
            try:
                for restarted in (False, True):
                    if restarted:
                        server.restart()
                    # The token vector and one source page suffice for a tiny
                    # snippet from an 8-MiB field, even after restart with the
                    # original Parquet objects unavailable.
                    sparse = client.query(
                        {
                            "request": {
                                "full_text_search": {
                                    "conjuncts": [
                                        {"match": "sparsecase", "field": "body"},
                                        {"match": "needle", "field": "body"},
                                    ]
                                },
                                "fields": ["id"],
                                "highlight": {"fields": ["body"], "fragment_size": 100},
                                "limit": 1,
                                "timeout_ms": 50,
                            }
                        }
                    )["hits"]["hits"]
                    assert len(sparse) == 1
                    assert sparse[0]["_source"] == {"id": count}
                    assert "needle" in sparse[0]["_highlights"]["body"][0]["text"]
                    multilingual = client.query(
                        {
                            "request": {
                                "full_text_search": {
                                    "conjuncts": [
                                        {"match": "unicodecase", "field": "body"},
                                        {"match": "needle", "field": "body"},
                                    ]
                                },
                                "fields": ["id"],
                                "highlight": {"fields": ["body"], "fragment_size": 100},
                                "limit": 1,
                                "timeout_ms": 50,
                            }
                        }
                    )["hits"]["hits"]
                    assert len(multilingual) == 1
                    assert multilingual[0]["_source"] == {"id": count + 2}
                    fragment = next(
                        value
                        for value in multilingual[0]["_highlights"]["body"]
                        if "needle" in value["text"]
                    )
                    encoded = fragment["text"].encode("utf-8")
                    assert (
                        unicode_body.encode("utf-8")[
                            fragment["offset"] : fragment["offset"] + len(encoded)
                        ]
                        == encoded
                    )
                    assert any(
                        encoded[span["start"] : span["end"]] == b"needle"
                        for span in fragment["spans"]
                    )
                    # A rare term in a large real vocabulary uses the paged
                    # dictionary and matching occurrence blocks. All patterns
                    # retain exact source spans after native reader restart.
                    for pattern in (
                        {"match": "uniqueneedle", "field": "body"},
                        {"prefix": "uniqueneed", "field": "body"},
                        {"wildcard": "uniqueneed?", "field": "body"},
                        {"fuzzy": "uniqueneedlf", "field": "body", "fuzziness": 1},
                    ):
                        selected = client.query(
                            {
                                "request": {
                                    "full_text_search": pattern,
                                    "fields": ["id"],
                                    "highlight": {
                                        "fields": ["body"],
                                        "fragment_size": 100,
                                    },
                                    "limit": 1,
                                }
                            }
                        )["hits"]["hits"]
                        assert len(selected) == 1, pattern
                        assert selected[0]["_source"] == {"id": count + 1}
                        fragment = selected[0]["_highlights"]["body"][0]
                        assert "uniqueneedle" in fragment["text"]
                        assert any(
                            fragment["text"][span["start"] : span["end"]]
                            == "uniqueneedle"
                            for span in fragment["spans"]
                        )
                    # Both new kernels work from native artifacts after the
                    # originals disappear, including after replica restart.
                    for row_id, pattern, original, term in (
                        (
                            count + 3,
                            {"match": "running", "field": "body"},
                            regular_body,
                            "running",
                        ),
                        (
                            count + 4,
                            {"prefix": "broad", "field": "body"},
                            diverse_body,
                            "broad",
                        ),
                    ):
                        request = {
                            "full_text_search": pattern,
                            "highlight": {
                                "fields": ["body"],
                                "max_fragments": 3,
                                "fragment_size": 100,
                            },
                            "limit": 1,
                        }
                        hidden_result = client.query(
                            {"request": dict(request, fields=["id"])}
                        )["hits"]["hits"]
                        projected_result = client.query(
                            {"request": dict(request, fields=["id", "body"])}
                        )["hits"]["hits"]
                        assert len(hidden_result) == len(projected_result) == 1
                        assert hidden_result[0]["_source"] == {"id": row_id}
                        assert projected_result[0]["_source"]["body"] == original
                        assert (
                            hidden_result[0]["_highlights"]
                            == projected_result[0]["_highlights"]
                        )
                        fragments = hidden_result[0]["_highlights"]["body"]
                        assert len(fragments) == 3
                        for fragment in fragments:
                            assert (
                                original[
                                    fragment["offset"] : fragment["offset"]
                                    + len(fragment["text"])
                                ]
                                == fragment["text"]
                            )
                            assert all(
                                fragment["text"][
                                    span["start"] : span["end"]
                                ].startswith(term)
                                for span in fragment["spans"]
                            )
                    base = {
                        "full_text_search": {"match": "needle", "field": "body"},
                        "highlight": {
                            "fields": ["body"],
                            "max_fragments": 3,
                            "fragment_size": 100,
                        },
                        "order_by": [{"field": "id"}],
                        "limit": count,
                    }
                    projected = client.query(
                        {"request": dict(base, fields=["id", "body"])}
                    )["hits"]["hits"]
                    hidden = client.query({"request": dict(base, fields=["id"])})[
                        "hits"
                    ]["hits"]
                    assert [hit["_source"]["id"] for hit in hidden] == list(
                        range(count)
                    )
                    for visible, other in zip(projected, hidden, strict=True):
                        assert visible["_source"]["body"] == body
                        assert set(other["_source"]) == {"id"}
                        assert {k: v for k, v in visible.items() if k != "_source"} == {
                            k: v for k, v in other.items() if k != "_source"
                        }
                        fragments = other["_highlights"]["body"]
                        assert 1 <= len(fragments) <= 3
                    for fields in (["id"], ["id", "body"]):
                        response = client.session.post(
                            server.api_url + f"/tables/{bench.TABLE}/query",
                            json=dict(base, fields=fields, timeout_ms=1),
                            timeout=30,
                        )
                        assert response.status_code == 504, response.text
                        value = response.json()
                        assert value["code"] == "query_timeout"
                        assert value["error"] == "query_timeout"
                        assert value["retryable"] is True
                        assert response.headers["Retry-After"] == "1"
                    # A timed-out request cannot poison the next reader's state.
                    assert (
                        client.query({"request": dict(base, fields=["id"])})["hits"][
                            "hits"
                        ]
                        == hidden
                    )
            finally:
                for path, renamed in unavailable:
                    renamed.rename(path)
        finally:
            client.session.close()
