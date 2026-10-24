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


def test_real_parquet_archive_qualification_preserves_cache_and_refresh_contracts(
    tmp_path,
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
    with log.open("w") as stream:
        completed = subprocess.run(
            command, stdout=stream, stderr=subprocess.STDOUT, timeout=240
        )
    assert completed.returncode == 0, log.read_text()
    report = json.loads((output / "report.json").read_text())
    assert report["complete"] is True
    assert report["server_config_sha256"]
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


@pytest.mark.parametrize("additional_declarations", [False, True])
def test_imported_parquet_awaits_embedded_indexes_and_uses_declared_identity(
    tmp_path, additional_declarations
):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = Path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))).resolve()
    if not binary.is_file():
        pytest.skip("Build the Antfly binary first")
    root = Path(__file__).resolve().parents[2]
    archive = tmp_path / "archive.parquet"
    pq.write_table(
        pa.table({"hn_id": list(range(128)), "body": ["quasar"] * 128}),
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
                    "fields": ["hn_id"],
                    "order_by": [{"field": "hn_id"}],
                    "limit": 20,
                },
            }
        ],
    }
    if not additional_declarations:
        del plan["indexes"]
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    config = _write_remote_content_e2e_config(tmp_path)
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
    assert set(report["initial_index_revisions"]) == {"text", "hn_key"}
    assert all(report["initial_index_revisions"].values())
    assert len(report["samples"]) == 2
    assert len({s["result_sha256"] for s in report["samples"]}) == 1
    answer = json.loads((output / "custom_identity-answer.json").read_text())
    assert [h["_source"]["hn_id"] for h in answer["hits"]["hits"]] == list(range(20))
