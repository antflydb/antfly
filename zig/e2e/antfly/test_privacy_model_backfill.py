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

"""Protected model migration: declared batching, publication, and durable output."""

import json
import os
import time
from collections import Counter
from pathlib import Path
from uuid import uuid4

import pytest
from helpers import wait_until
from test_auth import AUTH_BOOTSTRAP_PASSWORD, _basic_auth
from test_privacy_indexed_activation import policy

pytest_plugins = ("test_auth", "test_privacy_indexed_activation")
BATCH_ITEMS = 32


@pytest.mark.parametrize("document_count", [64, 1024])
@pytest.mark.parametrize("privacy_provider", ["backfill"], indirect=True)
def test_protected_model_backfill_batches(
    auth_api, privacy_provider, document_count, tmp_path
):
    api = auth_api
    endpoint, calls, behavior, _ = privacy_provider
    api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
    table = "protected_batch_" + uuid4().hex[:8]
    docs = {
        f"d{i:04}": {
            "body": f"Orchard schedule. Document {i}. Contact person@example.invalid."
        }
        for i in range(document_count)
    }
    protected = {
        row["body"].replace("person@example.invalid", "[EMAIL]")
        for row in docs.values()
    }
    report = {"documents": document_count, "batch_items": BATCH_ITEMS, "phase": "setup"}
    log_start = 0
    started = time.monotonic()
    status_seconds = 0.0
    status_calls = 0

    def cpu_seconds():
        try:
            values = (
                Path(f"/proc/{api._server.proc.pid}/stat")
                .read_text()
                .rsplit(")", 1)[1]
                .split()
            )
            return (int(values[11]) + int(values[12])) / os.sysconf("SC_CLK_TCK")
        except (OSError, ValueError):
            return None

    def add_index(name, model):
        api.post(
            f"/tables/{table}/indexes/{name}",
            {
                "type": "embeddings",
                "dimension": 2,
                "field": "body",
                "publication_policy": "atomic",
                "execution": {"embedding": {"batch_items": BATCH_ITEMS}},
                "embedder": {"provider": "antfly", "model": model, "api_url": endpoint},
            },
        )

    def complete(name):
        nonlocal status_seconds, status_calls
        began = time.monotonic()
        value = api.get(f"/tables/{table}/indexes/{name}")["status"]
        status_seconds += time.monotonic() - began
        status_calls += 1
        coverage = value["coverage"]
        return (
            value["readiness"]["complete"]
            and coverage["observation_complete"]
            and coverage["source_total"] == coverage["produced"] == document_count
        )

    def verify_outputs(names):
        before = calls["snapshot"]()
        for name, expected in names:
            assert complete(name)
            for identity in docs:
                manifest = api.get(
                    f"/tables/{table}/documents/{identity}/artifacts/{name}?detail=raw"
                )
                stored = json.loads(manifest["manifest_json"])
                assert stored["current"], (identity, name, stored)
                assert len(stored["vectors"]) == 1, (identity, name, stored)
                vector = stored["vectors"][0]
                assert vector["values"] == expected, (identity, name, vector)
                assert vector["source_text"] == docs[identity]["body"].replace(
                    "person@example.invalid", "[EMAIL]"
                )
        assert len(calls["snapshot"]()) == len(before), (
            "inspection must not execute inference"
        )

    def tenant_query(index, vector):
        return api.request_raw(
            "POST",
            "/query",
            json={
                "table": table,
                "indexes": [index],
                "embeddings": {index: vector},
                "fields": ["body"],
                "limit": 10,
            },
            headers={"Authorization": _basic_auth("reader", "reader")},
            timeout=15,
        )

    try:
        api.create_table(table)
        add_index("a", "fixture")
        api.put(f"/tables/{table}/privacy", policy(endpoint))
        api.batch_write(table, {"inserts": docs, "sync_level": "write"})
        report["phase"] = "initial-publication"
        assert wait_until(lambda: complete("a"), timeout_s=180, interval_s=0.02)
        verify_outputs((("a", [1.0, 0.0]),))
        api.post(
            "/auth/v1/users/reader",
            {
                "password": "reader",
                "initial_policies": [
                    {"resource": table, "resource_type": "table", "type": "read"}
                ],
            },
        )
        before_requests = len(calls["snapshot"]())
        before_extractions = len(calls["extract"])
        status_seconds = status_calls = 0
        api._server.log_file.flush()
        log_start = api._server.log_path.stat().st_size
        cpu_before = cpu_seconds()
        began = time.monotonic()
        report["phase"] = "backfill"
        behavior["hold_model"] = "fixture-v2"
        behavior["model_gate"].clear()
        add_index("b", "fixture-v2")
        report["index_create_seconds"] = time.monotonic() - began
        assert behavior["model_held"].wait(timeout=20), (
            "B document work was never attempted"
        )
        held_since = time.monotonic()
        assert not complete("b")
        a = tenant_query("a", [1.0, 0.0])
        assert a.status_code == 200, a.text
        a = a.json()["responses"][0]
        assert a["status"] == 200 and a["hits"]["hits"], a
        assert all("@" not in hit["body"] for hit in a["hits"]["hits"])
        b = tenant_query("b", [0.0, 1.0])
        assert b.status_code == 503 or (
            b.status_code == 200 and b.json()["responses"][0]["status"] == 503
        ), b.text
        report["controlled_hold_seconds"] = time.monotonic() - held_since
        released_at = time.monotonic()
        behavior["model_gate"].set()
        assert wait_until(lambda: complete("b"), timeout_s=180, interval_s=0.02)
        report["post_release_seconds"] = time.monotonic() - released_at
        report["backfill_seconds"] = time.monotonic() - began
        report["backfill_includes_controlled_hold"] = True
        cpu_after = cpu_seconds()
        report["server_cpu_seconds"] = (
            None if cpu_before is None or cpu_after is None else cpu_after - cpu_before
        )
        report["status_poll_calls"] = status_calls
        report["status_poll_seconds"] = status_seconds
        report["phase"] = "output-verification"
        verify_outputs((("a", [1.0, 0.0]), ("b", [0.0, 1.0])))
        records = calls["snapshot"]()[before_requests:]
        work = [
            r
            for r in records
            if r["model"] == "fixture-v2" and any(x in protected for x in r["inputs"])
        ]
        assert work and all(
            r["status"] == 200 and r["completed"] is not None for r in work
        ), work
        actual = Counter(text for r in work for text in r["inputs"])
        assert actual == Counter(protected), actual
        assert all("@" not in text for r in work for text in r["inputs"])
        assert len(calls["extract"]) == before_extractions
        report["document_embedding_calls"] = len(work)
        report["embedded_documents"] = sum(actual.values())
        report["phase"] = "restart-verification"
        before_restart = len(calls["snapshot"]())
        api._server.restart()
        assert wait_until(
            lambda: complete("a") and complete("b"), timeout_s=60, interval_s=0.02
        )
        verify_outputs((("a", [1.0, 0.0]), ("b", [0.0, 1.0])))
        restart_requests = calls["snapshot"]()[before_restart:]
        assert not any(
            text in protected for row in restart_requests for text in row["inputs"]
        ), ("restart must reuse published document vectors", restart_requests)
        report["restart_admission_probe_calls"] = len(restart_requests)
        assert len(work) <= (document_count + BATCH_ITEMS - 1) // BATCH_ITEMS, report
        report["phase"] = "completed"
    except BaseException as exc:
        report["failure"] = {"type": type(exc).__name__, "message": str(exc)[:4000]}
        raise
    finally:
        behavior["model_gate"].set()
        report["elapsed_seconds"] = time.monotonic() - started
        report["requests"] = calls["snapshot"]()
        try:
            api._server.log_file.flush()
            with api._server.log_path.open("rb") as source:
                end = source.seek(0, os.SEEK_END)
                start = max(log_start, end - 1024 * 1024)
                source.seek(start)
                (tmp_path / "server.log").write_bytes(source.read(1024 * 1024))
                report["log"] = {
                    "offset": start,
                    "total_bytes": end - log_start,
                    "truncated": start > log_start,
                }
        except OSError as exc:
            report["log_error"] = str(exc)
        (tmp_path / "diagnostic.json").write_text(json.dumps(report, indent=2) + "\n")
