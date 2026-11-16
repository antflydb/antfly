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


@pytest.fixture
def migration_api(monkeypatch, request):
    # Keep the physical wave and planning quantum explicit for the crash gate.
    monkeypatch.setenv("ANTFLY_ENRICHMENT_EMBED_PARALLEL_BATCHES", "2")
    monkeypatch.setenv("ANTFLY_ENRICHMENT_PREPARATION_WINDOW_ITEMS", "64")
    monkeypatch.setenv("ANTFLY_ENRICHMENT_WINDOW_ITEMS", "64")
    return request.getfixturevalue("auth_api")


@pytest.mark.parametrize(
    "document_count,crash_during_backfill", [(64, False), (1024, False), (1024, True)]
)
@pytest.mark.parametrize("privacy_provider", ["backfill"], indirect=True)
def test_protected_model_backfill_batches(
    migration_api, privacy_provider, document_count, crash_during_backfill, tmp_path
):
    api = migration_api
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
    reader_auth = ""

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
        report.setdefault("last_status", {})[name] = value
        status_seconds += time.monotonic() - began
        status_calls += 1
        coverage = value["coverage"]
        return (
            value["readiness"]["complete"]
            and coverage["observation_complete"]
            and coverage["source_total"] == coverage["produced"] == document_count
        )

    def expected_vector(name, identity):
        return [1.0 if name == "a" else -1.0, (int(identity[1:]) + 1) / 2048]

    def document_request(row):
        return any("Orchard schedule." in text for text in row["inputs"])

    def verify_outputs(names):
        before = calls["snapshot"]()
        for name in names:
            # The caller waits for readiness. Inspect each exact current output;
            # unrelated internal commits can invalidate a status census between reads.
            for identity in docs:
                manifest = None

                def inspect_current():
                    nonlocal manifest
                    response = api.request_raw(
                        "GET",
                        f"/tables/{table}/documents/{identity}/artifacts/{name}?detail=raw",
                        timeout=15,
                    )
                    if (
                        response.status_code == 503
                        and response.json().get("code")
                        == "storage_read_temporarily_unavailable"
                    ):
                        return False
                    response.raise_for_status()
                    manifest = response.json()
                    return True

                assert wait_until(inspect_current, timeout_s=15, interval_s=0.05)
                stored = json.loads(manifest["manifest_json"])
                assert stored["current"], (identity, name, stored)
                assert len(stored["vectors"]) == 1, (identity, name, stored)
                vector = stored["vectors"][0]
                assert vector["values"] == expected_vector(name, identity), (
                    identity,
                    name,
                    vector,
                )
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
            headers={"Authorization": reader_auth},
            timeout=15,
        )

    def verify_searches():
        for name in ("a", "b"):
            for identity in (
                "d0000",
                f"d{document_count // 2:04}",
                f"d{document_count - 1:04}",
            ):
                response = tenant_query(name, expected_vector(name, identity))
                assert response.status_code == 200, response.text
                result = response.json()["responses"][0]
                assert result["status"] == 200, result
                hits = result["hits"]["hits"]
                assert hits and hits[0]["_id"] == identity, result
                assert all("@" not in hit["_source"]["body"] for hit in hits), hits

    try:
        # Thousands of oracle reads should authenticate with a durable key,
        # rather than repeatedly running the password-verification workload.
        admin_key = api.post(
            "/auth/v1/users/admin/api-keys", {"name": "migration-oracle"}
        )
        api.s.headers["Authorization"] = f"Bearer {admin_key['encoded']}"
        api.create_table(table)
        add_index("a", "fixture")
        api.put(f"/tables/{table}/privacy", policy(endpoint))
        api.batch_write(table, {"inserts": docs, "sync_level": "write"})
        report["phase"] = "initial-publication"
        assert wait_until(lambda: complete("a"), timeout_s=180, interval_s=0.1)
        verify_outputs(("a",))
        api.post(
            "/auth/v1/users/reader",
            {
                "password": "reader",
                "initial_policies": [
                    {"resource": table, "resource_type": "table", "type": "read"}
                ],
            },
        )
        reader_key = api.post(
            "/auth/v1/users/reader/api-keys", {"name": "migration-query"}
        )
        reader_auth = f"Bearer {reader_key['encoded']}"
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
        behavior["hold_after_document_calls"] = 2 if crash_during_backfill else 0
        pre_crash_work = []
        persisted_before_crash = set()
        add_index("b", "fixture-v2")
        report["index_create_seconds"] = time.monotonic() - began
        assert behavior["model_held"].wait(timeout=20), (
            "B document work was never attempted"
        )
        if crash_during_backfill:

            def partially_published():
                nonlocal persisted_before_crash
                status = api.get(f"/tables/{table}/indexes/b")["status"]
                report["last_partial_status"] = status
                completed = {
                    text
                    for row in calls["snapshot"]()[before_requests:]
                    if row["model"] == "fixture-v2" and row["status"] == 200
                    for text in row["inputs"]
                    if "Orchard schedule." in text
                }
                # The privacy barrier deliberately hides partial B output from
                # searches/inspection. Its published physical count is separate
                # from whole-index coverage. With one vector per unique input,
                # equality accounts for every completed provider result.
                count = status["published_doc_count"]
                if not 0 < count == len(completed) < document_count:
                    return False
                persisted_before_crash = completed
                report["durably_produced_before_crash"] = count
                report["verified_inputs_before_crash"] = sorted(completed)
                return True

            assert wait_until(partially_published, timeout_s=30, interval_s=0.1), report
            pre_crash_work = [
                r for r in calls["snapshot"]()[before_requests:] if document_request(r)
            ]
            assert any(r["status"] == 200 for r in pre_crash_work), pre_crash_work
            assert not complete("b")
            cpu_at_crash = cpu_seconds()
            report["pre_crash_server_cpu_seconds"] = (
                None
                if cpu_before is None or cpu_at_crash is None
                else cpu_at_crash - cpu_before
            )
            api._server.proc.kill()
            api._server.proc.wait(timeout=10)
            behavior["model_gate"].set()
            assert wait_until(
                lambda: all(r["completed"] is not None for r in calls["snapshot"]()),
                timeout_s=10,
            )
            behavior["hold_after_document_calls"] = 0
            behavior["model_gate"].clear()
            behavior["model_held"].clear()
            api._server.restart()
            cpu_before = cpu_seconds()
            report["server_cpu_scope"] = "post-restart"
            assert behavior["model_held"].wait(timeout=20)

            def recovered_publication():
                status = api.get(f"/tables/{table}/indexes/b")["status"]
                report["recovered_partial_status"] = status
                # The provider withholds every new document response. A
                # recovered prefix therefore comes from durable stored work.
                return status["published_doc_count"] >= len(persisted_before_crash)

            assert wait_until(recovered_publication, timeout_s=30, interval_s=0.1), (
                report
            )
            assert not any(
                row["status"] == 200 and document_request(row)
                for row in calls["snapshot"]()
                if row["model"] == "fixture-v2"
                and row["started"] > pre_crash_work[-1]["started"]
            ), "recovery must precede newly completed inference"
            report["crashed_during_backfill"] = True
        held_since = time.monotonic()
        assert not complete("b")
        a = tenant_query("a", expected_vector("a", "d0000"))
        assert a.status_code == 200, a.text
        a = a.json()["responses"][0]
        assert a["status"] == 200 and a["hits"]["hits"], a
        assert all("@" not in hit["_source"]["body"] for hit in a["hits"]["hits"])
        b = tenant_query("b", expected_vector("b", "d0000"))
        assert b.status_code == 503 or (
            b.status_code == 200 and b.json()["responses"][0]["status"] == 503
        ), b.text
        report["controlled_hold_seconds"] = time.monotonic() - held_since
        released_at = time.monotonic()
        behavior["model_gate"].set()
        assert wait_until(lambda: complete("b"), timeout_s=180, interval_s=0.1)
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
        verify_outputs(("a", "b"))
        verify_searches()
        records = calls["snapshot"]()[before_requests:]
        work = [
            r for r in records if r["model"] == "fixture-v2" and document_request(r)
        ]
        document_records = [r for r in calls["snapshot"]() if document_request(r)]
        assert all("@" not in text for r in document_records for text in r["inputs"]), (
            document_records
        )
        assert work and all(r["completed"] is not None for r in work), work
        successful = [r for r in work if r["status"] == 200]
        abandoned = [r for r in work if r["status"] != 200]
        # Foreground reads may cancel a background invocation while the fake
        # deliberately withholds its response. Keep those attempts visible;
        # they must be disconnected held calls, never failures after release.
        assert all(
            r["status"] == 502
            and r.get("response_error") in ("BrokenPipeError", "ConnectionResetError")
            and (
                r["started"] in {attempt["started"] for attempt in pre_crash_work}
                or r["started"] < released_at <= r["completed"]
            )
            for r in abandoned
        ), abandoned
        report["abandoned_held_embedding_calls"] = len(abandoned)
        report["successful_document_embedding_calls"] = len(successful)
        actual = Counter(text for r in successful for text in r["inputs"])
        if crash_during_backfill:
            assert set(actual) == protected and all(
                actual[x] >= 1 for x in protected
            ), actual
            attempted_before_crash = {
                x
                for r in pre_crash_work
                for x in r["inputs"]
                if x not in persisted_before_crash
            }
            assert all(
                count == 1 or text in attempted_before_crash
                for text, count in actual.items()
            ), actual
        else:
            assert actual == Counter(protected), actual
        assert len(calls["extract"]) == before_extractions
        report["document_embedding_calls"] = len(work)
        report["completed_embedding_inputs"] = sum(actual.values())
        report["distinct_embedded_documents"] = len(actual)
        report["phase"] = "restart-verification"
        before_restart = len(calls["snapshot"]())
        api._server.restart()
        assert wait_until(
            lambda: complete("a") and complete("b"), timeout_s=60, interval_s=0.1
        )
        verify_outputs(("a", "b"))
        verify_searches()
        restart_requests = calls["snapshot"]()[before_restart:]
        assert not any(document_request(row) for row in restart_requests), (
            "restart must reuse published document vectors",
            restart_requests,
        )
        report["restart_admission_probe_calls"] = len(restart_requests)
        assert len(successful) <= (
            document_count + BATCH_ITEMS - 1
        ) // BATCH_ITEMS + len(pre_crash_work), report
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
