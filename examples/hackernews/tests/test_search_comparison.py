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

"""Exercise persisted index provenance and process isolation against DuckDB."""

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "search_comparison.py"


def test_prebuilt_bm25_restarts_and_rejects_changed_source(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect()
    if not connection.execute(
        "SELECT installed FROM duckdb_extensions() WHERE extension_name='fts'"
    ).fetchone()[0]:
        pytest.skip("Install fts explicitly before running the comparison fixture")
    source = tmp_path / "source'quoted.parquet"
    connection.execute(
        "CREATE TABLE hn (hn_id BIGINT, body VARCHAR, title VARCHAR, item_type VARCHAR, author VARCHAR, points BIGINT, created_at BIGINT)"
    )
    connection.execute(
        "INSERT INTO hn VALUES (1, 'database search', 'one', 'story', 'a', 1, 1), (2, 'database engine', 'two', 'comment', 'b', 2, 2), (3, 'other', 'three', 'story', 'c', 3, 3)"
    )
    connection.execute("COPY hn TO ? (FORMAT PARQUET)", [str(source)])
    connection.close()
    database, output = tmp_path / "fts.duckdb", tmp_path / "result.json"
    command = [
        sys.executable,
        str(SCRIPT),
        "--source",
        str(source),
        "--database",
        str(database),
        "--output",
        str(output),
        "--expected-rows",
        "3",
        "--terms",
        "database",
        "--repeats",
        "2",
    ]
    subprocess.run([*command, "--build"], check=True, capture_output=True, timeout=60)
    result = json.loads(output.read_text())
    assert len(result["cycles"]) == 2
    assert all(c["rows"] == 3 for c in result["cycles"])
    assert all(
        set(c["workloads"]["database"]["top_hn_ids"]) == {1, 2}
        for c in result["cycles"]
    )
    assert (
        result["cycles"][0]["workloads"]["database"]["scores"]
        == result["cycles"][1]["workloads"]["database"]["scores"]
    )
    subprocess.run(command, check=True, capture_output=True, timeout=60)
    changed = tmp_path / "changed.parquet"
    changed.write_bytes(source.read_bytes() + b"changed")
    changed_command = command.copy()
    changed_command[changed_command.index("--source") + 1] = str(changed)
    failed = subprocess.run(changed_command, capture_output=True, text=True, timeout=30)
    assert failed.returncode != 0
    assert "another source inventory" in failed.stderr


def test_worker_failure_does_not_echo_private_source_or_credential(
    tmp_path, monkeypatch, capsys
):
    spec = importlib.util.spec_from_file_location("search_comparison", SCRIPT)
    comparison = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(comparison)
    source = tmp_path / "source.parquet"
    source.write_bytes(b"fixture")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT),
            "--mode",
            "scan",
            "--source",
            str(source),
            "--database",
            str(tmp_path / "unused"),
            "--output",
            str(tmp_path / "out.json"),
        ],
    )
    secret = "sensitive-token"
    private = "https://private.example/source"
    monkeypatch.setattr(
        comparison.subprocess,
        "run",
        lambda *a, **k: subprocess.CompletedProcess([], 1, "", secret + private),
    )
    with pytest.raises(RuntimeError, match="exit code 1") as error:
        comparison.main()
    assert secret not in str(error.value) and private not in str(error.value)
    captured = capsys.readouterr()
    assert secret not in captured.out + captured.err
    assert not (tmp_path / "out.json").exists()
