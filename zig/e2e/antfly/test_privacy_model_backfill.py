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

"""Model-free control for protected cross-document migration throughput."""

import json
import os
import time
from pathlib import Path
from uuid import uuid4

import pytest
from helpers import wait_until
from test_auth import AUTH_BOOTSTRAP_PASSWORD, _basic_auth
from test_privacy_indexed_activation import policy

pytest_plugins = ("test_auth", "test_privacy_indexed_activation")


@pytest.mark.parametrize("document_count", [64, 1024])
def test_protected_model_backfill_batches(
    auth_api, privacy_provider, document_count, tmp_path
):
    api = auth_api
    endpoint, calls, _, _ = privacy_provider
    arrivals = []

    class TimedInputs(list):
        def extend(self, values):
            arrivals.append((time.monotonic(), len(values)))
            super().extend(values)

    calls["embed"] = TimedInputs()
    api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
    table = "protected_batch_" + uuid4().hex[:8]
    api.create_table(table)
    status_seconds = 0.0
    status_calls = 0

    def cpu_seconds():
        stat = Path(f"/proc/{api._server.proc.pid}/stat")
        if not stat.exists():
            return None
        values = stat.read_text().rsplit(")", 1)[1].split()
        return (int(values[11]) + int(values[12])) / os.sysconf("SC_CLK_TCK")

    def add_index(name, model):
        api.post(
            f"/tables/{table}/indexes/{name}",
            {
                "type": "embeddings",
                "dimension": 2,
                "field": "body",
                "publication_policy": "atomic",
                "embedder": {"provider": "antfly", "model": model, "api_url": endpoint},
            },
        )

    def complete(name):
        nonlocal status_seconds, status_calls
        began = time.monotonic()
        value = api.get(f"/tables/{table}/indexes/{name}")["status"]["coverage"][
            "complete"
        ]
        status_seconds += time.monotonic() - began
        status_calls += 1
        return value

    add_index("a", "fixture")
    api.put(f"/tables/{table}/privacy", policy(endpoint))
    api.batch_write(
        table,
        {
            "inserts": {
                f"d{i:04}": {
                    "body": f"Orchard schedule {i}. Contact person@example.invalid."
                }
                for i in range(document_count)
            },
            "sync_level": "write",
        },
    )
    assert wait_until(lambda: complete("a"), timeout_s=600), api._server.debug_logs()
    print(f"PROTECTED-BACKFILL A ready documents={document_count}", flush=True)
    before_items = len(calls["embed"])
    before_calls = len(calls["embed_models"])
    before_extractions = len(calls["extract"])
    before_arrivals = len(arrivals)
    status_seconds = 0.0
    status_calls = 0
    api._server.log_file.flush()
    log_start = api._server.log_path.stat().st_size
    cpu_before = cpu_seconds()
    began = time.monotonic()
    add_index("b", "fixture-v2")
    admitted = time.monotonic()
    assert wait_until(lambda: complete("b"), timeout_s=600), api._server.debug_logs()
    cpu_after = cpu_seconds()
    report = {
        "documents": document_count,
        "backfill_seconds": time.monotonic() - began,
        "index_create_seconds": admitted - began,
        "provider_calls_including_admission": len(calls["embed_models"]) - before_calls,
        "embedded_items_including_admission": len(calls["embed"]) - before_items,
        "additional_extractions": len(calls["extract"]) - before_extractions,
        "status_poll_calls": status_calls,
        "status_poll_seconds": status_seconds,
        "server_cpu_seconds": None if cpu_before is None else cpu_after - cpu_before,
        "provider_arrivals": [
            {"seconds": at - began, "items": count}
            for at, count in arrivals[before_arrivals:]
        ],
    }
    print("PROTECTED-BACKFILL " + json.dumps(report), flush=True)
    (tmp_path / "diagnostic.json").write_text(json.dumps(report, indent=2) + "\n")
    api._server.log_file.flush()
    with api._server.log_path.open() as source:
        source.seek(log_start)
        (tmp_path / "server.log").write_text(source.read())
    assert all("@" not in text for text in calls["embed"][before_items:])
    assert report["additional_extractions"] == 0
    assert (
        report["provider_calls_including_admission"] <= (document_count + 31) // 32 + 1
    )
