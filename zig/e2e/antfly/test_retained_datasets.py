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

"""Real administrative process restarts over independent file-backed stores."""

import hashlib
import json
import os
import subprocess
import time
from pathlib import Path

import pytest
from conftest import DEFAULT_ANTFLY_BIN, resolve_binary_path

pytestmark = pytest.mark.e2e_resource("antfly_process")


@pytest.mark.parametrize("kind", ["records", "summary"])
@pytest.mark.parametrize("access", ["queryable", "restore_required"])
def test_retained_dataset_cold_lifecycle_survives_process_restarts(
    tmp_path, kind, access
):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    dataset = [1] * 32
    segment_id = [4] * 32
    partition = [5] * 32
    catalog = (tmp_path / "catalog").as_uri()
    hot = {"id": [6] * 32, "uri": (tmp_path / "hot").as_uri(), "access": "local"}
    cold = {"id": [7] * 32, "uri": (tmp_path / "cold").as_uri(), "access": access}
    schema = '{"type":"object"}'
    digest = list(hashlib.sha256(schema.encode()).digest())
    end = time.time_ns() // 60_000_000_000 * 60_000_000_000
    coverage = {"start_ns": str(end - 60_000_000_000), "end_ns": str(end)}
    definition = {
        "id": dataset,
        "generation": 1,
        "kind": kind,
        "schema": digest,
        "schema_json": schema,
        "authorization": [3] * 32,
        "retention_ns": str(365 * 86400 * 1_000_000_000),
        "local_residence_ns": "0",
    }
    if kind == "summary":
        definition["summary"] = {
            "source": [9] * 32,
            "timestamp": "observed_at",
            "dimensions": ["device"],
            "interval_ns": "60000000000",
            "origin_ns": "0",
            "mutable_for_ns": "0",
            "measures": [{"name": "temperature", "law": "avg", "field": "temperature"}],
        }
    payload = b"40,2" if kind == "summary" else b'{"device":"a","value":20}\n'
    source_file = tmp_path / "source.bin"
    source_file.write_bytes(payload)
    segment = {
        "id": segment_id,
        "dataset": dataset,
        "definition_generation": 1,
        "schema": digest,
        "state_encoding": 1,
        "coverage": coverage,
        "source": {"partition": partition, "incarnation": 1, "sequence": "20"},
        "finalized": True,
        "checksum": list(hashlib.sha256(payload).digest()),
        "byte_len": len(payload),
        "location": hot["id"],
        "access": "local",
    }
    count = 0

    def run(action, ok=True, **fields):
        nonlocal count
        count += 1
        request = tmp_path / f"request-{count}.json"
        request.write_text(
            json.dumps(
                {
                    "format": 1,
                    "action": action,
                    "catalog_uri": catalog,
                    "dataset": dataset,
                    **fields,
                }
            )
        )
        result = subprocess.run(
            [str(binary), "storage", "dataset", "--request", str(request)],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if not ok:
            assert result.returncode != 0, result.stdout
            return result
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)

    run("create", definition=definition)
    run("publish", segment=segment, source=hot, input=str(source_file))
    output = tmp_path / "read.bin"
    run("read", segment_id=segment_id, source=hot, output=str(output))
    assert output.read_bytes() == payload
    run(
        "migrate",
        segment_id=segment_id,
        expected_revision=1,
        source=hot,
        destination=cold,
    )
    status = run("status", segment_id=segment_id)
    assert status["placement_revision"] == 2
    assert status["location"] == cold["id"]
    assert run("reclaim_local", segment_id=segment_id, source=hot) == {
        "complete": False
    }
    assert run("reclaim_local", segment_id=segment_id, source=hot) == {"complete": True}
    if access == "restore_required":
        failure = run(
            "read", ok=False, segment_id=segment_id, source=cold, output=str(output)
        )
        assert "DatasetRestoreRequired" in failure.stderr
        run(
            "restore",
            segment_id=segment_id,
            expected_revision=2,
            source=cold,
            destination=hot,
        )
        active = hot
    else:
        active = cold
    run("read", segment_id=segment_id, source=active, output=str(output))
    assert output.read_bytes() == payload
    plan = run("plan", generation=1, partition=partition, incarnation=1, range=coverage)
    assert len(plan["segments"]) == 1
    assert plan["segments"][0]["id"] == segment_id
    page = run("list", limit=2)
    assert len(page["results"][0]) == 1
    failure = run(
        "retire",
        ok=False,
        segment_id=segment_id,
        expected_revision=3 if access == "restore_required" else 2,
    )
    assert "DatasetRetentionPending" in failure.stderr
