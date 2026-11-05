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

"""Privacy discovery uses the real producer, catalog CAS and durable backfill."""

from __future__ import annotations

import json
import time
from threading import Event

import pytest
from urllib.parse import quote
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

from helpers import start_http_server, wait_until


@pytest.mark.parametrize("mode", ["monitor", "redact_for_search_and_ai"])
@pytest.mark.parametrize("num_shards", [1, 2])
def test_privacy_preview_policy_cas_backfill_and_findings(
    stateful_api, mode, num_shards
):
    calls = []
    embedding_calls = []
    detector_batch_sizes = []
    gate = Event()
    gate.set()
    behavior = {"malformed": False}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if "input" in request:
                inputs = request["input"]
                if isinstance(inputs, str):
                    inputs = [inputs]
                embedding_calls.extend(inputs)
                payload = json.dumps(
                    {
                        "object": "list",
                        "data": [
                            {
                                "object": "embedding",
                                "index": i,
                                "embedding": [1.0, 0.0, 0.0],
                            }
                            for i in range(len(inputs))
                        ],
                        "model": "text-embedding-3-small",
                        "usage": {
                            "prompt_tokens": len(inputs),
                            "total_tokens": len(inputs),
                        },
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            assert gate.wait(timeout=20), "Inspection fixture was never released"
            prompt = json.loads(request["messages"][1]["content"])
            detector_batch_sizes.append(len(prompt["items"]))
            items = []
            for item in prompt["items"]:
                text = item["input"]
                calls.append(text)
                raw = text.encode()
                start = raw.find(b"a@b.co")
                findings = (
                    [
                        {
                            "category": "email",
                            "start": start,
                            "end": 999 if behavior["malformed"] else start + 6,
                        }
                    ]
                    if start >= 0
                    else []
                )
                items.append({"item": item["id"], "value": {"findings": findings}})
            payload = json.dumps(
                {
                    "choices": [
                        {
                            "message": {
                                "tool_calls": [
                                    {
                                        "id": "privacy-fixture",
                                        "type": "function",
                                        "function": {
                                            "name": "antfly_output",
                                            "arguments": json.dumps({"items": items}),
                                        },
                                    }
                                ]
                            }
                        }
                    ]
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = start_http_server(server)
    table = f"privacy_{uuid4().hex[:8]}"
    try:
        stateful_api.post(
            f"/tables/{table}",
            {
                "num_shards": num_shards,
                "schema": {
                    "default_type": "doc",
                    "document_schemas": {
                        "doc": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "body": {"type": "string", "nullable": True}
                                },
                                "additionalProperties": True,
                            }
                        }
                    },
                },
            },
        )
        stateful_api.create_index(
            table, "body_search", {"type": "full_text", "field": "body"}
        )
        vectors = True
        if vectors:
            stateful_api.create_index(
                table,
                "body_vectors",
                {
                    "type": "embeddings",
                    "field": "body",
                    "dimension": 3,
                    "embedder": {
                        "provider": "openai",
                        "model": "text-embedding-3-small",
                        "url": f"http://127.0.0.1:{server.server_port}",
                        "dimensions": 3,
                    },
                },
            )
        stateful_api.batch_write(
            table,
            inserts={"doc": {"body": "Éva: a@b.co"}, "é-doc": {"body": "a@b.co"}},
            sync_level="full_index",
        )
        embedding_calls.clear()
        policy = {
            "mode": mode,
            "fields": ["body"],
            "category_groups": [],
            "categories": [{"name": "email"}],
            "producer": {
                "type": "generator",
                "revision": "privacy-fixture-v1",
                "config": {
                    "generator": {
                        "provider": "openai",
                        "model": "gpt-4.1",
                        "api_key": "",
                        "url": f"http://127.0.0.1:{server.server_port}/v1",
                    },
                    "output": "auto",
                    "output_schema": {
                        "type": "object",
                        "properties": {
                            "findings": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "category": {"type": "string"},
                                        "start": {"type": "integer"},
                                        "end": {"type": "integer"},
                                    },
                                    "required": ["category", "start", "end"],
                                    "additionalProperties": False,
                                },
                            }
                        },
                        "required": ["findings"],
                        "additionalProperties": False,
                    },
                },
            },
        }
        path = f"/tables/{table}/privacy"
        preview = stateful_api.post(
            f"{path}/preview", {"policy": policy, "sample": {"keys": ["doc"]}}
        )
        field = preview["documents"][0]["fields"][0]
        assert field["original"] == "Éva: a@b.co"
        assert field["text"] == "Éva: [EMAIL]"
        assert field["findings"][0]["start"] == 6
        assert not preview["complete_table_scan"]
        assert preview["external_processing"]
        assert not stateful_api.get(path)["configured"]
        if mode == "monitor" and num_shards == 1:
            # The sample fits, but repeating its identifier in each finding
            # would produce a response above the preview output budget.
            large_id = "p" * (2 * 1024 * 1024)
            fields = ["body", "extra1", "extra2", "extra3", "extra4"]
            stateful_api.batch_write(
                table, inserts={large_id: {name: "a@b.co" for name in fields}}
            )
            oversized = stateful_api.s.post(
                f"{stateful_api.url}{path}/preview",
                json={
                    "policy": dict(policy, fields=fields),
                    "sample": {"keys": [large_id]},
                },
                timeout=60,
            )
            assert oversized.status_code == 422, oversized.text[:500]
            assert oversized.json()["code"] == "privacy_sample_too_large"
            stateful_api.batch_write(table, deletes=[large_id])
        behavior["malformed"] = True
        invalid = stateful_api.s.post(
            f"{stateful_api.url}{path}/preview",
            json={"policy": policy, "sample": {"keys": ["doc"]}},
            timeout=30,
        )
        assert invalid.status_code == 422, invalid.text
        assert not stateful_api.get(path)["configured"]
        behavior["malformed"] = False

        batched_preview = stateful_api.post(
            f"{path}/preview",
            {"policy": policy, "sample": {"keys": ["doc", "é-doc"]}},
        )
        assert len(batched_preview["documents"]) == 2
        assert 2 in detector_batch_sizes, detector_batch_sizes
        detector_batch_sizes.clear()
        if mode == "redact_for_search_and_ai":
            gate.clear()
        response = stateful_api.s.put(
            f"{stateful_api.url}{path}", json=policy, timeout=30
        )
        if mode == "redact_for_search_and_ai" and response.status_code == 202:
            pending = stateful_api.s.get(
                f"{stateful_api.url}/tables/{table}/documents/doc", timeout=15
            )
            assert pending.status_code in (200, 404, 409, 503), pending.text
            assert "a@b.co" not in pending.text
        # The detector gate prevents protected embeddings from starting before
        # acceptance. Exclude calls made while the table was still unprotected.
        embedding_calls.clear()
        gate.set()
        assert response.status_code == 202, response.text
        etag = response.headers["ETag"]
        configured = response.json()
        assert configured["policy_version"] == preview["policy_version"]
        assert (
            stateful_api.s.put(
                f"{stateful_api.url}{path}", json=policy, timeout=30
            ).status_code
            == 428
        )
        assert (
            stateful_api.s.put(
                f"{stateful_api.url}{path}",
                json=policy,
                headers={"If-Match": '"stale"'},
                timeout=30,
            ).status_code
            == 412
        )

        # A semantically identical save races the active worker's cursor and
        # coverage updates. Acknowledge the policy version, preserve the job,
        # and keep executable producer configuration stable across key order.
        equivalent = {
            **policy,
            "producer": {
                **policy["producer"],
                "config": dict(reversed(list(policy["producer"]["config"].items()))),
            },
        }
        saved = stateful_api.s.put(
            f"{stateful_api.url}{path}",
            json=equivalent,
            headers={"If-Match": etag},
            timeout=30,
        )
        assert saved.status_code == 202, json.dumps(
            {
                "response": saved.text,
                "privacy": stateful_api.get(path),
                "logs": stateful_api.debug_logs(),
            }
        )
        assert saved.json()["policy_version"] == configured["policy_version"]
        assert saved.json()["generation"] == configured["generation"]
        assert saved.json()["scan_id"] == configured["scan_id"]
        etag = saved.headers["ETag"]

        def finished():
            scan = stateful_api.get(f"{path}/scans/{configured['scan_id']}")
            assert scan["phase"] != "failed", json.dumps(
                {"scan": scan, "logs": stateful_api.debug_logs()}
            )
            return scan if scan["phase"] == "succeeded" else None

        assert wait_until(finished, timeout_s=60, interval_s=0.2), json.dumps(
            {
                "status": stateful_api.get(path),
                "scan": stateful_api.get(f"{path}/scans/{configured['scan_id']}"),
                "calls": len(calls),
                "embedding_calls": embedding_calls,
                "table_artifacts": stateful_api.get(f"/tables/{table}/artifacts"),
                "vector_index": stateful_api.get_index(table, "body_vectors")
                if vectors
                else None,
                "artifacts": stateful_api.get(
                    f"/tables/{table}/documents/doc/artifacts"
                ),
                "findings": stateful_api.get(f"{path}/findings?document=doc"),
                "logs": stateful_api.debug_logs(),
            }
        )
        findings = wait_until(
            lambda: stateful_api.get(f"{path}/findings?document=doc").get("findings"),
            timeout_s=30,
            interval_s=0.2,
        )
        assert len(findings) == 1
        assert findings[0]["start"] == 6
        assert findings[0]["category"] == "email"
        assert "text" not in findings[0]
        assert "confidence" not in findings[0]
        assert stateful_api.get(f"/tables/{table}/documents/doc")["body"] == (
            "Éva: [EMAIL]" if mode == "redact_for_search_and_ai" else "Éva: a@b.co"
        )
        first = stateful_api.get(f"{path}/findings?limit=1")
        assert len(first["findings"]) == 1
        cursor = quote(first["next_cursor"], safe="")
        second = stateful_api.get(f"{path}/findings?limit=1&cursor={cursor}")
        assert [
            first["findings"][0]["document"],
            second["findings"][0]["document"],
        ] == ["doc", "é-doc"]
        mismatch = stateful_api.s.get(
            f"{stateful_api.url}{path}/findings?cursor={cursor}&field=other", timeout=30
        )
        assert mismatch.status_code == 422
        original = stateful_api.s.put(
            f"{stateful_api.url}{path}",
            json={**policy, "mode": "redact_before_storage"},
            headers={"If-Match": etag},
            timeout=30,
        )
        assert original.status_code == 422
        assert stateful_api.get(path)["policy"]["mode"] == mode
        assert stateful_api.get(path)["etag"] == etag
        tuned = {**policy, "execution": {"batch_items": 1}}
        response = stateful_api.s.put(
            f"{stateful_api.url}{path}",
            json=tuned,
            headers={"If-Match": etag},
            timeout=30,
        )
        assert response.status_code == 202, response.text
        assert response.json()["policy_version"] == configured["policy_version"]
        assert response.json()["scan_id"] == configured["scan_id"]

        def current_coverage():
            value = stateful_api.get(path).get("coverage", {})
            return (
                value if value.get("available") and value.get("current") == 2 else None
            )

        coverage = wait_until(current_coverage, timeout_s=30, interval_s=0.2)
        assert coverage is not None, json.dumps(stateful_api.get(path))
        assert coverage["total"] == 2
        assert coverage["with_findings"] == 2
        assert coverage["categories"] == {"email": 2}
        assert coverage["fields"] == {"body": 2}
        if mode == "redact_for_search_and_ai":

            def protected_results():
                response = stateful_api.s.post(
                    f"{stateful_api.url}/tables/{table}/query",
                    json={"full_text_search": {"match_all": {}}, "limit": 10},
                    timeout=15,
                )
                if response.status_code in (409, 503):
                    return None
                assert response.status_code == 200, response.text
                result = response.json()
                rows = result["responses"][0]
                if rows.get("error"):
                    return None
                hits = rows["hits"]["hits"]
                return hits if len(hits) == 2 else None

            hits = wait_until(protected_results, timeout_s=60, interval_s=0.25)
            assert hits is not None, json.dumps(stateful_api.get(path))
            assert {hit["_id"] for hit in hits} == {"doc", "é-doc"}
            named_text = stateful_api.query_table(
                table,
                {
                    "full_text_search": {"match_all": {}},
                    "indexes": ["body_search"],
                    "limit": 10,
                },
            )
            assert {
                hit["_id"] for hit in named_text["responses"][0]["hits"]["hits"]
            } == {"doc", "é-doc"}, named_text
            assert "a@b.co" not in json.dumps(named_text)
            if vectors:
                nearest = stateful_api.query_table(
                    table,
                    {
                        "embeddings": {"body_vectors": [1.0, 0.0, 0.0]},
                        "indexes": ["body_vectors"],
                        "limit": 10,
                    },
                )
                assert "a@b.co" not in json.dumps(nearest), nearest
                assert len(nearest["responses"][0]["hits"]["hits"]) == 2, json.dumps(
                    {
                        "query": nearest,
                        "embeddings": embedding_calls,
                        "artifacts": stateful_api.get(
                            f"/tables/{table}/documents/doc/artifacts"
                        ),
                        "index": stateful_api.get_index(table, "body_vectors"),
                        "logs": stateful_api.debug_logs(),
                    }
                )
                assert embedding_calls and all(
                    "a@b.co" not in text for text in embedding_calls
                ), embedding_calls
            assert "a@b.co" not in json.dumps(hits)
            assert all("_privacy" not in hit.get("_source", {}) for hit in hits)
            original_terms = stateful_api.query_table(
                table,
                {"full_text_search": {"term": "co", "field": "body"}, "limit": 10},
            )
            assert original_terms["responses"][0]["hits"]["hits"] == []
            assert wait_until(
                lambda: stateful_api.get(path)["coverage"].get("protected_ready") == 2,
                timeout_s=30,
                interval_s=0.2,
            ), stateful_api.get(path)
            bypass = stateful_api.get(
                f"/tables/{table}/documents/doc?_privacy_original=true"
            )
            assert bypass["body"] == "Éva: [EMAIL]"
            forged_scan = stateful_api.s.post(
                f"{stateful_api.url}/tables/{table}/documents",
                json={"limit": 10, "_privacy_original": True, "_privacy_status": True},
                timeout=15,
            )
            assert forged_scan.status_code == 200, forged_scan.text
            assert "a@b.co" not in forged_scan.text

            def read_sql(statement):
                def fetch():
                    response = stateful_api.s.post(
                        f"{stateful_api.url}/sql",
                        json={"statement": statement},
                        timeout=15,
                    )
                    if response.status_code in (409, 503) and response.json().get(
                        "retryable"
                    ):
                        return None
                    assert response.status_code == 200, json.dumps(
                        {"response": response.text, "logs": stateful_api.debug_logs()}
                    )
                    return response

                response = wait_until(fetch, timeout_s=60, interval_s=0.2)
                assert response is not None, json.dumps(stateful_api.get(path))
                return response

            sql = read_sql(f'SELECT body FROM "{table}"')
            assert "a@b.co" not in sql.text
            assert "[EMAIL]" in sql.text, sql.text
            # Predicates and aggregates must consume the sanitized snapshot.
            for value, expected in (("Éva: a@b.co", 0), ("Éva: [EMAIL]", 1)):
                counted = read_sql(
                    f"SELECT COUNT(*) AS n FROM \"{table}\" WHERE body = '{value}'"
                )
                assert int(counted.json()["rows"][0][0]) == expected, counted.text
            # A sanitized read image cannot become an original-row mutation.
            mutation = stateful_api.s.post(
                f"{stateful_api.url}/sql",
                json={"statement": f"UPDATE \"{table}\" SET body = 'overwritten'"},
                timeout=15,
            )
            assert mutation.status_code == 501, mutation.text
            assert mutation.json()["code"] == "0A000"
            assert (
                stateful_api.get(f"/tables/{table}/documents/doc")["body"]
                == "Éva: [EMAIL]"
            )
        # New primary writes must enter the same privacy graph automatically.
        # A held detector cannot expose the replacement source while pending.
        if mode == "redact_for_search_and_ai":
            gate.clear()
        stateful_api.batch_write(table, inserts={"doc": {"body": "changed: a@b.co"}})
        if mode == "redact_for_search_and_ai":
            pending = stateful_api.s.get(
                f"{stateful_api.url}/tables/{table}/documents/doc", timeout=15
            )
            assert pending.status_code in (200, 404, 409, 503), pending.text
            assert "a@b.co" not in pending.text
            pending_sql = stateful_api.s.post(
                f"{stateful_api.url}/sql",
                json={"statement": f'SELECT body FROM "{table}"'},
                timeout=15,
            )
            assert pending_sql.status_code in (409, 503), json.dumps(
                {"response": pending_sql.text, "logs": stateful_api.debug_logs()}
            )
            assert "a@b.co" not in pending_sql.text
        gate.set()
        expected_body = (
            "changed: [EMAIL]"
            if mode == "redact_for_search_and_ai"
            else "changed: a@b.co"
        )

        def changed_document():
            response = stateful_api.s.get(
                f"{stateful_api.url}/tables/{table}/documents/doc", timeout=15
            )
            if response.status_code in (404, 409, 503):
                return None
            assert response.status_code == 200, response.text
            value = response.json()
            return value if value.get("body") == expected_body else None

        # Coverage is an asynchronous observation, not a source-version fence.
        assert wait_until(changed_document, timeout_s=60, interval_s=0.2), (
            stateful_api.get(path)
        )

        def changed_findings():
            findings = stateful_api.get(f"{path}/findings?document=doc")["findings"]
            return findings if findings and findings[0]["start"] == 9 else None

        assert wait_until(changed_findings, timeout_s=60, interval_s=0.2), (
            stateful_api.get(path)
        )

        # Invalid evidence is a visible source-bound failure, never a clean
        # receipt. Retrying a scan after recovery must not reuse that failure.
        behavior["malformed"] = True
        stateful_api.batch_write(table, inserts={"doc": {"body": "retry: a@b.co"}})

        def failed_source():
            value = stateful_api.get(path).get("coverage", {})
            return (
                value
                if value.get("available") and value.get("failed_known") == 1
                else None
            )

        assert wait_until(failed_source, timeout_s=60, interval_s=0.2), json.dumps(
            stateful_api.get(path)
        )
        assert stateful_api.get(f"{path}/findings?document=doc")["findings"] == []
        if mode == "redact_for_search_and_ai":
            failed = stateful_api.s.get(
                f"{stateful_api.url}/tables/{table}/documents/doc", timeout=15
            )
            assert failed.status_code in (404, 409, 503), failed.text
            assert "a@b.co" not in failed.text
        # A fresh attempt must also report its own terminal failure if the
        # provider is still broken, rather than hiding every failure forever.
        before_failed_retry = len(calls)
        failed_retry = stateful_api.post(f"{path}/scans", {"limit": 1})

        def failed_retry_finished():
            scan = stateful_api.get(f"{path}/scans/{failed_retry['scan_id']}")
            return scan if scan["phase"] == "failed" else None

        assert wait_until(failed_retry_finished, timeout_s=60, interval_s=0.2), (
            json.dumps(stateful_api.get(path))
        )
        assert len(calls) > before_failed_retry
        behavior["malformed"] = False
        retried = stateful_api.post(f"{path}/scans", {"limit": 1})

        def retried_finished():
            scan = stateful_api.get(f"{path}/scans/{retried['scan_id']}")
            assert scan["phase"] != "failed", json.dumps(
                {"scan": scan, "logs": stateful_api.debug_logs()}
            )
            return scan if scan["phase"] == "succeeded" else None

        assert wait_until(retried_finished, timeout_s=60, interval_s=0.2), json.dumps(
            stateful_api.get(path)
        )
        assert stateful_api.get(f"/tables/{table}/documents/doc")["body"] == (
            "retry: [EMAIL]" if mode == "redact_for_search_and_ai" else "retry: a@b.co"
        )

        # A semantic change supersedes cursors and rebuilds the projection.
        current_etag = stateful_api.get(path)["etag"]
        replacement = {**tuned, "replacement": "mask"}
        response = stateful_api.s.put(
            f"{stateful_api.url}{path}",
            json=replacement,
            headers={"If-Match": current_etag},
            timeout=30,
        )
        assert response.status_code == 202, response.text
        assert response.json()["policy_version"] != configured["policy_version"]
        old_findings = stateful_api.s.get(
            f"{stateful_api.url}{path}/findings?policy_version={configured['policy_version']}",
            timeout=15,
        )
        assert old_findings.status_code == 410, old_findings.text
        replacement_scan = response.json()["scan_id"]

        def replacement_finished():
            scan = stateful_api.get(f"{path}/scans/{replacement_scan}")
            assert scan["phase"] != "failed", json.dumps(
                {"scan": scan, "logs": stateful_api.debug_logs()}
            )
            return scan if scan["phase"] == "succeeded" else None

        assert wait_until(replacement_finished, timeout_s=60, interval_s=0.2), (
            json.dumps(
                {
                    "status": stateful_api.get(path),
                    "scan": stateful_api.get(f"{path}/scans/{replacement_scan}"),
                    "vectors": stateful_api.get_index(table, "body_vectors"),
                    "text": stateful_api.get_index(table, "body_search"),
                    "indexes": stateful_api.list_indexes(table),
                    "logs": stateful_api.debug_logs(),
                }
            )
        )
        assert stateful_api.get(f"/tables/{table}/documents/doc")["body"] == (
            "retry: [REDACTED]"
            if mode == "redact_for_search_and_ai"
            else "retry: a@b.co"
        )
        if stateful_api.supports_restart:
            stateful_api.restart_server()
            assert (
                stateful_api.get(path)["policy_version"]
                == response.json()["policy_version"]
            )
            assert wait_until(current_coverage, timeout_s=60, interval_s=0.2), (
                json.dumps(stateful_api.get(path))
            )
            assert stateful_api.get(f"/tables/{table}/documents/doc")["body"] == (
                "retry: [REDACTED]"
                if mode == "redact_for_search_and_ai"
                else "retry: a@b.co"
            )

        stateful_api.batch_write(
            table, inserts={"doc3": {"body": None}}, deletes=["é-doc"]
        )

        def current_membership():
            value = stateful_api.get(path).get("coverage", {})
            return (
                value
                if value.get("total") == 2
                and value.get("current") == 2
                and value.get("with_findings") == 1
                else None
            )

        assert wait_until(current_membership, timeout_s=60, interval_s=0.2), json.dumps(
            stateful_api.get(path)
        )
        assert stateful_api.get(f"/tables/{table}/documents/doc3")["body"] is None
        assert stateful_api.get(f"{path}/findings?document=é-doc")["findings"] == []
        assert calls
        # Background workers may consume a singleton before its neighbor is
        # queued, and this test later explicitly tunes batching down to one.
        # Preview above deterministically exercises the shared batching path.
        assert detector_batch_sizes and max(detector_batch_sizes) <= 8

        def vector_members(expected):
            response = stateful_api.s.post(
                f"{stateful_api.url}/tables/{table}/query",
                json={
                    "embeddings": {"body_vectors": [1.0, 0.0, 0.0]},
                    "indexes": ["body_vectors"],
                    "limit": 10,
                },
                timeout=15,
            )
            if response.status_code in (409, 503):
                return None
            assert response.status_code == 200, response.text
            row = response.json()["responses"][0]
            if row.get("error"):
                return None
            hits = row["hits"]["hits"]
            return hits if {hit["_id"] for hit in hits} == expected else None

        assert wait_until(
            lambda: vector_members({"doc"}), timeout_s=60, interval_s=0.2
        ), json.dumps(
            {
                "index": stateful_api.get_index(table, "body_vectors"),
                "logs": stateful_api.debug_logs(),
            }
        )
        if mode == "redact_for_search_and_ai":
            # Returning to monitor must rebuild the original vector consumer
            # without deleting source artifacts through an unordered cleanup.
            current = stateful_api.get(path)
            monitoring = dict(current["policy"], mode="monitor")
            switched = stateful_api.s.put(
                f"{stateful_api.url}{path}",
                json=monitoring,
                headers={"If-Match": current["etag"]},
                timeout=30,
            )
            assert switched.status_code == 202, switched.text

            def monitoring_finished():
                value = stateful_api.get(f"{path}/scans/{switched.json()['scan_id']}")
                assert value["phase"] != "failed", value
                return value if value["phase"] == "succeeded" else None

            assert wait_until(monitoring_finished, timeout_s=60, interval_s=0.2)
            assert wait_until(
                lambda: vector_members({"doc"}), timeout_s=60, interval_s=0.2
            ), json.dumps(
                {
                    "index": stateful_api.get_index(table, "body_vectors"),
                    "logs": stateful_api.debug_logs(),
                }
            )
        assert (
            stateful_api.get(f"/tables/{table}/documents/doc")["body"]
            == "retry: a@b.co"
        )
        stateful_api.batch_write(table, inserts={"doc3": {"body": "second: a@b.co"}})
        assert wait_until(
            lambda: vector_members({"doc", "doc3"}), timeout_s=60, interval_s=0.2
        ), json.dumps(
            {
                "index": stateful_api.get_index(table, "body_vectors"),
                "logs": stateful_api.debug_logs(),
            }
        )
        if stateful_api.supports_restart:
            stateful_api.restart_server()
            assert wait_until(
                lambda: vector_members({"doc", "doc3"}), timeout_s=60, interval_s=0.2
            ), json.dumps(stateful_api.get_index(table, "body_vectors"))
    finally:
        gate.set()
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_privacy_default_native_long_document(stateful_api):
    """Opt-in real checkpoint gate; CI without model weights stays offline."""
    import os

    if os.environ.get("ANTFLY_E2E_PRIVACY_NATIVE") != "1":
        pytest.skip("Set ANTFLY_E2E_PRIVACY_NATIVE=1 with the pinned PII checkpoint")
    table = f"privacy_native_{uuid4().hex[:8]}"
    stateful_api.create_table(table)
    text = (
        "The weather today is pleasant. " * 180
        + "Contact Maria Jensen at maria.jensen@example.dk."
    )
    stateful_api.batch_write(
        table,
        inserts={"long": {"body": text, "subject": "Éva: maria.jensen@example.dk."}},
    )
    path = f"/tables/{table}/privacy"
    policy = {
        "mode": "redact_for_search_and_ai",
        "fields": ["subject", "body"],
        "category_groups": [],
        "categories": [{"name": "email"}, {"name": "person"}],
    }
    preview = stateful_api.post(
        f"{path}/preview", {"policy": policy, "sample": {"keys": ["long"]}}
    )
    assert not preview["external_processing"]
    assert len(preview["documents"][0]["fields"]) == 2
    for field in preview["documents"][0]["fields"]:
        assert "maria.jensen@example.dk" not in field["text"]
        assert any(f["category"] == "email" for f in field["findings"])
    saved = stateful_api.put(path, policy)
    assert (
        saved["resolved_producer"]["revision"]
        == "1cb4166094dc58fa8d836429f060d6c95f62b495"
    )

    def complete():
        scan = stateful_api.get(f"{path}/scans/{saved['scan_id']}")
        assert scan["phase"] != "failed", json.dumps(
            {"scan": scan, "logs": stateful_api.debug_logs()}
        )
        return scan if scan["phase"] == "succeeded" else None

    assert wait_until(complete, timeout_s=120, interval_s=0.25), json.dumps(
        stateful_api.get(path)
    )
    document = stateful_api.get(f"/tables/{table}/documents/long")
    assert "maria.jensen@example.dk" not in json.dumps(document)
    assert document["body"].endswith("Contact [PERSON] at [EMAIL].")
    findings = stateful_api.get(f"{path}/findings?document=long")["findings"]
    assert any(
        f["field"] == "body" and f["category"] == "email" and f["start"] > 5000
        for f in findings
    )
    assert wait_until(
        lambda: stateful_api.get(path)["coverage"].get("protected_ready") == 1,
        timeout_s=30,
        interval_s=0.2,
    ), stateful_api.get(path)


@pytest.mark.parametrize("mode", ["monitor", "redact_for_search_and_ai"])
def test_privacy_empty_table_and_null_source(stateful_api, mode):
    table = f"privacy_empty_{uuid4().hex[:8]}"
    stateful_api.create_table(table)
    path = f"/tables/{table}/privacy"
    saved = stateful_api.put(path, {"mode": mode, "fields": ["body"]})

    def completed():
        scan = stateful_api.get(f"{path}/scans/{saved['scan_id']}")
        assert scan["phase"] != "failed", json.dumps(
            {"scan": scan, "logs": stateful_api.debug_logs()}
        )
        return scan if scan["phase"] == "succeeded" else None

    assert wait_until(completed, timeout_s=30, interval_s=0.2), stateful_api.get(path)
    coverage = wait_until(
        lambda: (
            value
            if (value := stateful_api.get(path)["coverage"]).get("available")
            else None
        ),
        timeout_s=30,
        interval_s=0.2,
    )
    assert coverage is not None, stateful_api.get(path)
    assert coverage["total"] == coverage["current"] == coverage["failed"] == 0

    def empty_query():
        result = stateful_api.s.post(
            f"{stateful_api.url}/tables/{table}/query",
            json={"full_text_search": {"match_all": {}}, "limit": 10},
            timeout=15,
        )
        if result.status_code == 503:
            return None
        assert result.status_code == 200, result.text
        return result.json()

    result = wait_until(empty_query, timeout_s=30, interval_s=0.2)
    assert result is not None, json.dumps(
        {
            "status": stateful_api.get(path),
            "indexes": stateful_api.get(f"/tables/{table}/indexes"),
            "logs": stateful_api.debug_logs(),
        }
    )
    assert result["responses"][0]["hits"]["hits"] == [], result
    stateful_api.batch_write(table, inserts={"null": {"body": None}})

    def current():
        coverage = stateful_api.get(path)["coverage"]
        return coverage if coverage.get("current") == 1 else None

    assert wait_until(current, timeout_s=30, interval_s=0.2), stateful_api.get(path)
    assert stateful_api.get(f"{path}/findings")["findings"] == []
    assert stateful_api.get(f"/tables/{table}/documents/null")["body"] is None


def test_privacy_resolved_category_limit_matches_preview_and_installation(stateful_api):
    table = f"privacy_categories_{uuid4().hex[:8]}"
    stateful_api.create_table(table)
    path = f"/tables/{table}/privacy"
    policy = {
        "mode": "monitor",
        "fields": ["body"],
        "category_groups": ["credentials"],
        "categories": [{"name": f"custom_{i}"} for i in range(128)],
    }
    for method, suffix, body in (
        ("POST", "/preview", {"policy": policy}),
        ("PUT", "", policy),
    ):
        result = stateful_api._request(method, path + suffix, body)
        assert result.status_code == 422, result.text
        assert result.json()["code"] == "InvalidPrivacyCategories", result.text
    assert stateful_api.get(path)["configured"] is False
    # The credentials group contributes five distinct categories. One
    # explicit override counts once, leaving 123 custom categories available.
    policy["categories"] = policy["categories"][:123] + [
        {"name": "password", "threshold": 0.8}
    ]
    preview = stateful_api.post(path + "/preview", {"policy": policy})
    assert preview["sampled"] == 0
    saved = stateful_api.put(path, policy)
    assert saved["configured"] is True
    assert saved["policy_version"] == preview["policy_version"]


@pytest.mark.parametrize("mode", ["monitor", "redact_for_search_and_ai"])
def test_privacy_coverage_resumes_background_pages_and_tracks_membership(
    stateful_api, mode
):
    """Status is a cheap metadata read; census pages complete independently."""
    table = f"privacy_census_{uuid4().hex[:8]}"
    stateful_api.create_table(table, num_shards=2)
    # Missing selected text is a valid empty inspection and needs no weights.
    stateful_api.batch_write(
        table, inserts={f"doc-{i:04}": {"ordinal": i} for i in range(237)}
    )
    path = f"/tables/{table}/privacy"
    configured = stateful_api.put(path, {"mode": mode, "fields": ["body"]})

    observations = []

    def complete(total):
        coverage = stateful_api.get(path)["coverage"]
        observation = {
            key: coverage.get(key)
            for key in ("total", "current", "pending", "completed_at_millis")
        }
        if not observations or observation != observations[-1]:
            observations.append(observation)
        if (
            coverage.get("available")
            and coverage.get("total") == total
            and coverage.get("current") == total
        ):
            assert coverage["pending"] == coverage["failed"] == 0
            assert coverage["with_findings"] == 0
            assert coverage["started_at_millis"] <= coverage["completed_at_millis"]
            assert "next_key" not in coverage
            return coverage
        return None

    def converged(total):
        # Publication is deliberately bounded and backpressured. Require
        # observable progress while allowing slow machines to drain the queue;
        # this is a convergence contract, not a hardware throughput benchmark.
        best = (-1, -1)
        last_progress = time.monotonic()

        def poll():
            nonlocal best, last_progress
            result = complete(total)
            latest = observations[-1]
            score = (int(latest["total"] == total), latest["current"] or 0)
            if score > best:
                best = score
                last_progress = time.monotonic()
            assert time.monotonic() - last_progress < 90, json.dumps(
                {
                    "reason": "privacy backfill stopped progressing",
                    "observations": observations,
                    "scan": stateful_api.get(f"{path}/scans/{configured['scan_id']}"),
                    "logs": stateful_api.debug_logs(),
                }
            )
            return result

        return wait_until(poll, timeout_s=600, interval_s=0.25)

    first = converged(237)
    assert first, json.dumps(
        {
            "observations": observations,
            "status": stateful_api.get(path),
            "scan": stateful_api.get(f"{path}/scans/{configured['scan_id']}"),
            "logs": stateful_api.debug_logs(),
        }
    )

    def scan_finished():
        scan = stateful_api.get(f"{path}/scans/{configured['scan_id']}")
        assert scan["phase"] != "failed", json.dumps(scan)
        return scan if scan["phase"] == "succeeded" else None

    assert wait_until(scan_finished, timeout_s=180, interval_s=0.25), json.dumps(
        {
            "scan": stateful_api.get(f"{path}/scans/{configured['scan_id']}"),
            "logs": stateful_api.debug_logs(),
        }
    )
    stateful_api.batch_write(
        table,
        inserts={f"new-{i:04}": {"ordinal": i} for i in range(13)},
        deletes=[f"doc-{i:04}" for i in range(7)],
    )
    second = converged(243)
    assert second, json.dumps(
        {
            "observations": observations,
            "status": stateful_api.get(path),
            "logs": stateful_api.debug_logs(),
        }
    )
    assert second["completed_at_millis"] > first["completed_at_millis"]


@pytest.mark.parametrize("key_repeats", [1000, 12000])
def test_privacy_findings_continue_with_large_escaped_document_ids(
    stateful_api, key_repeats
):
    table = f"privacy_cursor_{uuid4().hex[:8]}"
    stateful_api.create_table(table)
    stateful_api.batch_write(
        table,
        inserts={('k\\"' * key_repeats) + f"{i:03}": {"ordinal": i} for i in range(51)},
    )
    path = f"/tables/{table}/privacy"
    stateful_api.put(path, {"mode": "monitor", "fields": ["body"]})
    first = stateful_api.get(path + "/findings")
    cursor = first["next_cursor"]
    assert len(cursor) > 4096
    response = stateful_api.s.post(
        stateful_api.url + path + "/findings", json={"cursor": cursor}, timeout=30
    )
    if key_repeats == 1000:
        get_page = stateful_api.s.get(
            stateful_api.url + path + "/findings", params={"cursor": cursor}, timeout=30
        )
        assert get_page.status_code == 200, get_page.text
        assert get_page.json()["examined_documents"] == 1
        assert get_page.json()["next_cursor"] is None
    assert response.status_code == 200, response.text
    assert response.json()["examined_documents"] == 1
    assert response.json()["next_cursor"] is None
    # Cursors cannot be reused with a different document filter.
    mismatch = stateful_api.s.post(
        stateful_api.url + path + "/findings",
        json={"cursor": cursor, "field": "body"},
        timeout=30,
    )
    assert mismatch.status_code == 422, mismatch.text


@pytest.mark.parametrize("mode", ["monitor", "redact_for_search_and_ai"])
def test_privacy_rejects_relational_storage_before_mutation(stateful_api, mode):
    table = f"privacy_relational_{uuid4().hex[:8]}"
    schema = {
        "storage_mode": "relational",
        "default_type": "row",
        "document_schemas": {
            "row": {
                "schema": {
                    "type": "object",
                    "properties": {"id": {"type": "integer"}},
                    "required": ["id"],
                    "additionalProperties": False,
                }
            }
        },
    }
    stateful_api.post(f"/tables/{table}", {"schema": schema})
    path = f"/tables/{table}/privacy"
    policy = {"mode": mode, "fields": ["body"]}
    for method, suffix, payload in [
        ("put", "", policy),
        ("post", "/preview", {"policy": policy, "sample": {"limit": 1}}),
        ("post", "/scans", {}),
    ]:
        response = stateful_api.s.request(
            method, stateful_api.url + path + suffix, json=payload, timeout=30
        )
        assert response.status_code == 422, response.text
        assert response.json()["code"] == "PrivacyRelationalStorageUnsupported", (
            response.text
        )
    assert stateful_api.get(path)["configured"] is False


@pytest.mark.parametrize("key_bytes", [100, 1_100_000, 5_000_000])
def test_privacy_large_id_coverage_and_internal_reads(stateful_api, key_bytes):
    import base64
    from conftest import internal_service_headers
    from urllib.parse import quote

    table = f"privacy_large_id_{uuid4().hex[:8]}"
    key = "x" * key_bytes
    documents = {key: {"ordinal": 1}}
    if key_bytes >= 5_000_000:
        # Put a large ID at the hundred-row boundary so both backfill and
        # coverage persist/resume a large cursor instead of only finishing a
        # one-row page. The second large key must be observed on the next page.
        documents.update({f"a{i:03}": {"ordinal": i} for i in range(99)})
        documents[key + "z"] = {"ordinal": 2}
    stateful_api.create_table(table)
    stateful_api.batch_write(table, inserts=documents)
    path = f"/tables/{table}/privacy"
    installed = stateful_api.put(path, {"mode": "monitor", "fields": ["body"]})

    if key_bytes >= 5_000_000:
        # A 1,000-row rescan must still split these two 5 MB identifiers at
        # the discovery byte ceiling, even though every source body is tiny.
        installed = stateful_api.post(path + "/scans", {"limit": 1000})

    def progress_diagnostic():
        return json.dumps(
            {
                "coverage": {
                    k: v
                    for k, v in status["coverage"].items()
                    if k not in ("failures", "fields", "categories")
                },
                "scan": {
                    k: scan.get(k)
                    for k in ("phase", "scanned", "reprocessed", "failed", "last_error")
                },
                "logs": stateful_api.debug_logs(),
            }
        )

    # This exercises a bounded/backpressured publication pipeline, as do the
    # bulk coverage tests above. Require advancing work rather than a fixed
    # machine-throughput deadline for 101 roots after a multi-megabyte Raft entry.
    deadline = time.monotonic() + 600
    last_progress = time.monotonic()
    best = (-1, -1, -1, -1, -1)
    while time.monotonic() < deadline:
        status = stateful_api.get(path)
        scan = stateful_api.get(path + "/scans/" + installed["scan_id"])
        # Checkpoint IDs remain durable server state, not polling payload.
        assert len(json.dumps(scan)) < 1024 * 1024
        assert scan["phase"] not in ("failed", "superseded"), progress_diagnostic()
        coverage = status["coverage"]
        score = (
            scan.get("scanned", 0),
            scan.get("reprocessed", 0),
            int(coverage.get("total") == len(documents)),
            coverage.get("current") or 0,
            int(scan["phase"] == "succeeded"),
        )
        if score > best:
            best = score
            last_progress = time.monotonic()
        assert time.monotonic() - last_progress < 90, progress_diagnostic()
        if coverage["available"] and scan["phase"] == "succeeded":
            break
        time.sleep(0.25)
    diagnostic = progress_diagnostic()
    assert status["coverage"]["available"], diagnostic
    assert status["coverage"]["total"] == len(documents), diagnostic
    assert status["coverage"]["current"] == len(documents), diagnostic
    assert scan["phase"] == "succeeded", diagnostic
    assert scan["scanned"] == len(documents), diagnostic

    # Force real HTTP dispatch even when this fixture's shard is resident.
    ranges = stateful_api.get("/cluster")["data"]["ranges"]
    assert len(ranges) == 1, ranges
    owner = ranges[0]
    root = stateful_api.url.split("/db/v1")[0]
    uri = (
        root
        + f"/internal/v1/groups/{owner['group_id']}/tables/{quote(owner['table_name'], safe='')}/document-read"
    )
    headers = internal_service_headers()
    for operation in ("lookup", "manifest", "manifests"):
        response = stateful_api.s.post(
            uri,
            json={
                "operation": operation,
                "key_base64": base64.urlsafe_b64encode(key.encode())
                .decode()
                .rstrip("="),
                "query": "read_consistency=read_index&_privacy_original=true",
                "artifact_name": "_privacy_document",
            },
            headers=headers,
            timeout=30,
        )
        assert response.status_code == 200, (
            f"{operation}: {response.status_code}: {response.text[:1000]}"
        )
        if operation == "lookup":
            assert response.json()["ordinal"] == 1
    # A body request cannot grant the raw-lookup capability without a service identity.
    denied = stateful_api.s.post(
        uri,
        json={
            "operation": "lookup",
            "key_base64": base64.urlsafe_b64encode(key.encode()).decode().rstrip("="),
            "query": "_privacy_original=true",
        },
        timeout=30,
    )
    assert denied.status_code in (401, 403), denied.text[:1000]


def test_privacy_large_scan_boundary_is_private_durable_and_keeps_policy_mutable(
    stateful_api,
):
    table = f"privacy_scan_boundary_{uuid4().hex[:8]}"
    stateful_api.create_table(table)
    path = f"/tables/{table}/privacy"
    policy = {"mode": "monitor", "fields": ["body"]}
    stateful_api.put(path, policy)
    # The old catalog CAS serialized this boundary twice and could acknowledge
    # the job, then permanently reject every progress and policy update.
    response = stateful_api.s.post(
        stateful_api.url + path + "/scans",
        json={"from_key": "x" * (33 * 1024 * 1024)},
        timeout=120,
    )
    assert response.status_code == 202, (
        response.text[:1000],
        stateful_api.debug_logs(),
    )
    assert len(response.content) < 4096
    scan_id = response.json()["scan_id"]
    del response
    if stateful_api.supports_restart:
        stateful_api.restart_server()
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        scan = stateful_api.get(path + "/scans/" + scan_id)
        assert len(json.dumps(scan)) < 4096
        assert all(
            key not in scan
            for key in (
                "from_key",
                "to_key",
                "next_key",
                "shard_cursors",
                "verification_cursor",
            )
        )
        assert scan["phase"] not in ("failed", "superseded"), scan
        if scan["phase"] == "succeeded":
            break
        time.sleep(0.25)
    assert scan["phase"] == "succeeded", (scan, stateful_api.debug_logs())
    current = stateful_api.get(path)
    response = stateful_api.s.put(
        stateful_api.url + path,
        json=policy,
        headers={"If-Match": current["etag"]},
        timeout=30,
    )
    assert response.status_code == 202, (
        response.text[:1000],
        stateful_api.debug_logs(),
    )


def test_internal_read_admits_legal_large_key(stateful_api):
    import base64
    from conftest import internal_service_headers

    table = "privacy_large_read_" + uuid4().hex[:8]
    stateful_api.create_table(table)
    owner = stateful_api.get("/cluster")["data"]["ranges"][0]
    root = stateful_api.url.split("/db/v1")[0]
    uri = (
        root + f"/internal/v1/groups/{owner['group_id']}"
        f"/tables/{quote(owner['table_name'], safe='')}/document-read"
    )
    body = {
        "operation": "manifest",
        "key_base64": base64.urlsafe_b64encode(b"x" * (50 * 1024 * 1024))
        .decode()
        .rstrip("="),
        "artifact_name": "_privacy_document",
    }
    response = stateful_api.s.post(
        uri, json=body, headers=internal_service_headers(), timeout=90
    )
    assert response.status_code == 404, response.text[:500]


def test_preview_identifier_and_streaming_sample_budgets(stateful_api):
    table = "privacy_preview_budget_" + uuid4().hex[:8]
    stateful_api.create_table(table)
    key = "x" * (9 * 1024 * 1024)
    # Finish initial publication before testing sample admission. In Raft,
    # catch-up for this large identifier can otherwise occupy the read fence.
    stateful_api.batch_write(
        table, inserts={key: {"ordinal": 1}}, sync_level="full_index"
    )
    policy = {
        "mode": "monitor",
        "fields": ["body"],
        "category_groups": ["contact_details"],
    }
    for sample in ({"keys": [key]}, {"limit": 1}):
        response = stateful_api.s.post(
            stateful_api.url + f"/tables/{table}/privacy/preview",
            json={"policy": policy, "sample": sample},
            timeout=120,
        )
        assert response.status_code == 422, (
            "explicit" if "keys" in sample else "automatic",
            response.text[:500],
            stateful_api.debug_logs()[-12000:],
        )
        assert response.json()["code"] == "privacy_sample_too_large"
    stateful_api.batch_write(
        table, deletes=[key], inserts={"small": {"ordinal": 1}, "tail": {"ordinal": 2}}
    )
    for limit in (1, 10):
        response = stateful_api.post(
            f"/tables/{table}/privacy/preview",
            {"policy": policy, "sample": {"limit": limit}},
        )
        assert response["sampled"] == min(limit, 2)


def test_privacy_multifield_batches_release_detector_scratch(stateful_api):
    padding = "x" * (32 * 1024)
    dense_findings = False
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            prompt = json.loads(request["messages"][1]["content"])
            calls.append(len(prompt["items"]))
            items = [
                {
                    "item": item["id"],
                    "value": {
                        "findings": [
                            {"category": "email", "start": 2 * i, "end": 2 * i + 1}
                            for i in range(2000)
                        ]
                        if dense_findings and item["input"] == "a " * 2000
                        else [],
                        "padding": padding,
                    },
                }
                for item in prompt["items"]
            ]
            payload = json.dumps(
                {
                    "choices": [
                        {
                            "message": {
                                "tool_calls": [
                                    {
                                        "id": "fixture",
                                        "type": "function",
                                        "function": {
                                            "name": "antfly_output",
                                            "arguments": json.dumps({"items": items}),
                                        },
                                    }
                                ]
                            }
                        }
                    ]
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = start_http_server(server)
    api = stateful_api
    table = "privacy_batch_memory_" + uuid4().hex[:8]
    api.create_table(table)
    fields = ["f" + str(i) for i in range(64)]
    api.batch_write(
        table,
        inserts={"doc" + str(i): {name: "hello" for name in fields} for i in range(50)},
    )
    try:
        for count, docs in ((1, 4), (4, 1), (4, 4), (32, 4), (64, 4), (64, 50)):
            policy = {
                "mode": "monitor",
                "fields": fields[:count],
                "category_groups": [],
                "categories": [{"name": "email"}],
                "producer": {
                    "type": "generator",
                    "revision": "memory-fixture-v1",
                    "config": {
                        "generator": {
                            "provider": "openai",
                            "model": "gpt-4.1",
                            "api_key": "",
                            "url": f"http://127.0.0.1:{server.server_port}/v1",
                        },
                        "output": "auto",
                        "output_schema": {
                            "type": "object",
                            "properties": {
                                "findings": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "category": {"type": "string"},
                                            "start": {"type": "integer"},
                                            "end": {"type": "integer"},
                                        },
                                        "required": ["category", "start", "end"],
                                        "additionalProperties": False,
                                    },
                                },
                                "padding": {"type": "string"},
                            },
                            "required": ["findings", "padding"],
                            "additionalProperties": False,
                        },
                    },
                },
            }
            response = api.s.post(
                api.url + f"/tables/{table}/privacy/preview",
                json={
                    "policy": policy,
                    "sample": {"keys": ["doc" + str(i) for i in range(docs)]},
                },
                timeout=180,
            )
            assert response.status_code == 200, (
                count,
                docs,
                calls,
                response.text[:1000],
                api.debug_logs()[-12000:],
            )
            result = response.json()
            assert result["sampled"] == docs
            assert len(result["documents"]) == docs
            assert all(len(row["fields"]) == count for row in result["documents"])
            assert "padding" not in response.text
            assert sum(calls) == count * docs
            assert max(calls) <= 8
            if docs > 1:
                assert max(calls) == min(8, count * docs)
            calls.clear()

        # Managed output contains the source document, independently of the
        # detector's small findings envelope. Exercise both the serial and
        # batched paths with selected and unrelated source-sized values.
        large = "x" * (2 * 1024 * 1024)
        api.batch_write(
            table,
            inserts={
                "large-unselected": {fields[0]: "hello", "payload": large},
                "large-selected": {fields[0]: large},
            },
            sync_level="full_index",
        )
        policy["fields"] = fields[:1]
        policy["producer"]["config"]["batch"] = {"max_input_bytes": 4 * 1024 * 1024}
        for keys in (
            ["large-unselected"],
            ["large-selected"],
            ["doc0", "large-unselected", "large-selected"],
        ):
            response = api.s.post(
                api.url + f"/tables/{table}/privacy/preview",
                json={"policy": policy, "sample": {"keys": keys}},
                timeout=180,
            )
            assert response.status_code == 200, (keys, response.text[:1000])
            result = response.json()
            assert result["sampled"] == len(keys)
            for row in result["documents"]:
                field = row["fields"][0]
                expected = large if row["document"] == "large-selected" else "hello"
                assert field["original"] == expected
                assert field["text"] == expected
                assert field["findings"] == []
            assert sum(calls) == len(keys)
            if len(keys) > 1:
                assert max(calls) == len(keys)
            calls.clear()

        # A protective save must converge on the same documents and serve
        # them normally, rather than accepting a policy that cannot backfill.
        policy["mode"] = "redact_for_search_and_ai"
        installed = api.put(f"/tables/{table}/privacy", policy)
        progress = None
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            progress = api.get(f"/tables/{table}/privacy/scans/{installed['scan_id']}")
            assert progress["phase"] not in ("failed", "superseded"), (
                progress,
                api.debug_logs()[-12000:],
            )
            if progress["phase"] == "succeeded":
                break
            time.sleep(0.2)
        assert progress["phase"] == "succeeded", (progress, api.debug_logs()[-12000:])
        for key in ("large-selected", "large-unselected"):
            document = api.get(f"/tables/{table}/documents/{key}")
            assert document[fields[0]] == (
                large if key == "large-selected" else "hello"
            )
        # A dense response reuses one policy identity across all findings,
        # including when category descriptions make canonicalization expensive.
        api.batch_write(
            table,
            inserts={"dense-findings": {fields[0]: "a " * 2000}},
            sync_level="full_index",
        )
        dense_findings = True
        dense_policy = {
            **policy,
            "categories": [{"name": "email", "description": "x" * 16384}],
        }
        dense = api.post(
            f"/tables/{table}/privacy/preview",
            {"policy": dense_policy, "sample": {"keys": ["dense-findings"]}},
        )
        findings = dense["documents"][0]["fields"][0]["findings"]
        assert len(findings) == 2000
        assert all(row["policy_version"] == dense["policy_version"] for row in findings)
        # Raising the host projection allowance must not raise provider limits.
        padding = "x" * (1024 * 1024 + 1)
        response = api.s.post(
            api.url + f"/tables/{table}/privacy/preview",
            json={"policy": policy, "sample": {"keys": ["doc0"]}},
            timeout=180,
        )
        assert response.status_code == 422, response.text[:1000]
        assert response.json()["code"] == "InferenceResultTooLarge", response.text[
            :1000
        ]
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@pytest.mark.parametrize("mode", ["monitor", "redact_for_search_and_ai"])
def test_privacy_discovery_resumes_after_empty_identifier(stateful_api, mode):
    api = stateful_api
    table = "privacy_empty_id_" + uuid4().hex[:8]
    api.create_table(table, num_shards=2)
    ids = ["", "\x00", "é-after"]
    api.batch_write(
        table,
        inserts={key: {"ordinal": i} for i, key in enumerate(ids)},
        sync_level="full_index",
    )
    path = f"/tables/{table}/privacy"
    api.put(path, {"mode": mode, "fields": ["body"]})
    scan = api.post(path + "/scans", {"limit": 1})

    def finished():
        progress = api.get(path + "/scans/" + scan["scan_id"])
        assert progress["phase"] not in ("failed", "superseded"), progress
        # A repeated empty cursor would keep queuing the first row forever.
        assert progress["scanned"] <= len(ids), progress
        return progress if progress["phase"] == "succeeded" else None

    completed = wait_until(finished, timeout_s=120, interval_s=0.2)
    assert completed, (api.get(path), api.debug_logs()[-12000:])
    assert completed["scanned"] == len(ids)
    assert api.get(path)["coverage"]["current"] == len(ids)


def test_privacy_execution_override_retries_without_changing_identity(stateful_api):
    calls = []
    behavior = {"recover_after": None}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            prompt = json.loads(request["messages"][1]["content"])
            calls.append(len(prompt["items"]))
            recovered = (
                behavior["recover_after"] is not None
                and len(calls) > behavior["recover_after"]
            )
            findings = (
                [] if recovered else [{"category": "email", "start": 999, "end": 1000}]
            )
            items = [
                {"item": item["id"], "value": {"findings": findings}}
                for item in prompt["items"]
            ]
            payload = json.dumps(
                {
                    "choices": [
                        {
                            "message": {
                                "tool_calls": [
                                    {
                                        "id": "fixture",
                                        "type": "function",
                                        "function": {
                                            "name": "antfly_output",
                                            "arguments": json.dumps({"items": items}),
                                        },
                                    }
                                ]
                            }
                        }
                    ]
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = start_http_server(server)
    api = stateful_api
    table = "privacy_controls_" + uuid4().hex[:8]
    api.create_table(table)
    documents = ["doc", "doc-1", "doc-2", "doc-3"]
    api.batch_write(
        table,
        inserts={key: {"body": "hello"} for key in documents},
        sync_level="full_index",
    )
    policy = {
        "mode": "monitor",
        "fields": ["body"],
        "category_groups": [],
        "categories": [{"name": "email"}],
        "execution": {"batch_items": 1, "max_attempts": 1},
        "producer": {
            "type": "generator",
            "revision": "controls-fixture-v1",
            "config": {
                "generator": {
                    "provider": "openai",
                    "model": "gpt-4.1",
                    "api_key": "",
                    "url": f"http://127.0.0.1:{server.server_port}/v1",
                },
                "output_schema": {
                    "type": "object",
                    "properties": {
                        "findings": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "category": {"type": "string"},
                                    "start": {"type": "integer"},
                                    "end": {"type": "integer"},
                                },
                                "required": ["category", "start", "end"],
                                "additionalProperties": False,
                            },
                        }
                    },
                    "required": ["findings"],
                    "additionalProperties": False,
                },
            },
        },
    }
    path = f"/tables/{table}/privacy"

    def finished(scan_id):
        scan = api.get(f"{path}/scans/{scan_id}")
        return scan if scan["phase"] in ("succeeded", "failed") else None

    try:
        preview = api.s.post(
            api.url + path + "/preview",
            json={"policy": policy, "sample": {"keys": documents}},
            timeout=90,
        )
        assert preview.status_code == 422 and calls == [1] * len(documents), (
            preview.text[:1000],
            calls,
        )
        installed = api.put(path, policy)
        failed = wait_until(
            lambda: finished(installed["scan_id"]), timeout_s=90, interval_s=0.2
        )
        assert failed and failed["phase"] == "failed", failed

        # Tuning is stored separately from immutable producer identity. It must
        # survive restart, keep current receipts, and govern the next attempt.
        policy["execution"]["max_attempts"] = 5
        tuned = api.s.put(
            api.url + path,
            json=policy,
            headers={"If-Match": api.get(path)["etag"]},
            timeout=90,
        )
        assert tuned.status_code == 202, tuned.text[:1000]
        for field in ("policy_version", "generation", "scan_id"):
            assert tuned.json()[field] == installed[field]
        if api.supports_restart:
            api.restart_server()
        assert api.get(path)["policy"]["execution"]["max_attempts"] == 5
        calls.clear()
        behavior["recover_after"] = 4
        retry = api.post(path + "/scans", {"limit": 1})
        recovered = wait_until(
            lambda: finished(retry["scan_id"]), timeout_s=90, interval_s=0.2
        )
        assert recovered and recovered["phase"] == "succeeded", (
            recovered,
            calls,
            api.debug_logs()[-12000:],
        )
        assert len(calls) >= 5 and set(calls) == {1}, calls
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@pytest.mark.parametrize("mode", ["monitor", "redact_for_search_and_ai"])
@pytest.mark.parametrize("backup_format", ["portable", "native"])
def test_privacy_restore_rebuilds_destination_checkpoints(
    backup_api, tmp_path, mode, backup_format
):
    api = backup_api
    table = "privacy_restore_" + uuid4().hex[:8]
    api.create_table(table, num_shards=1)
    api.batch_write(table, inserts={"doc": {"ordinal": 1}}, sync_level="full_text")
    path = f"/tables/{table}/privacy"
    installed = api.put(
        path,
        {
            "mode": mode,
            "fields": ["absent"],
            "execution": {"batch_items": 1, "max_attempts": 5},
        },
    )
    assert wait_until(
        lambda: (
            api.get(path + "/scans/" + installed["scan_id"])["phase"] == "succeeded"
        ),
        timeout_s=60,
        interval_s=0.2,
    ), api.debug_logs()[-6000:]
    # This bound is externalized into the metadata blob store under the source
    # table's identity. A destination must never try to dereference that blob.
    bounded = api.post(path + "/scans", {"from_key": "k" * 5000, "limit": 1})
    assert wait_until(
        lambda: api.get(path + "/scans/" + bounded["scan_id"])["phase"] == "succeeded",
        timeout_s=60,
        interval_s=0.2,
    ), api.debug_logs()[-6000:]
    before = api.get(path)
    location = tmp_path.as_uri()
    assert (
        api.backup_table(
            table,
            backup_id="privacy",
            location=location,
            backup_format=backup_format,
        )["backup"]
        == "successful"
    )
    api.delete_table(table)
    api.restore_table(table, backup_id="privacy", location=location)
    after = api.get(path)
    for field in (
        "policy",
        "policy_version",
        "resource_version",
        "generation",
        "resolved_producer",
    ):
        assert after[field] == before[field], field
    assert after["scan_id"] not in (installed["scan_id"], bounded["scan_id"])
    assert wait_until(
        lambda: api.get(path + "/scans/" + after["scan_id"])["phase"] == "succeeded",
        timeout_s=60,
        interval_s=0.2,
    ), (
        api.get(path + "/scans/" + after["scan_id"]),
        api.get(path),
        api.debug_logs()[-6000:],
    )
    assert wait_until(
        lambda: api.get(path)["coverage"].get("current") == 1,
        timeout_s=60,
        interval_s=0.2,
    ), api.get(path)
    assert api.get(path)["coverage"]["failed"] == 0
    if mode == "redact_for_search_and_ai":
        assert api.get(path)["coverage"]["protected_ready"] == 1
    rows = api.scan_keys(table, {"limit": 10, "include_documents": True})
    assert len(rows) == 1


@pytest.mark.parametrize("count", [1, 2])
def test_privacy_large_policy_empty_fields_preview_and_backfill(stateful_api, count):
    api = stateful_api
    table = "privacy_large_policy_" + uuid4().hex[:8]
    api.create_table(table)
    keys = [f"doc-{i}" for i in range(count)]
    api.batch_write(
        table,
        inserts={key: {"ordinal": i} for i, key in enumerate(keys)},
        sync_level="full_index",
    )
    policy = {
        "mode": "redact_for_search_and_ai",
        "fields": ["absent"],
        "category_groups": [],
        "categories": [{"name": "email", "description": "x" * (128 * 1024)}],
        "execution": {"batch_items": 1, "max_attempts": 3},
    }
    path = f"/tables/{table}/privacy"
    preview = api.post(path + "/preview", {"policy": policy, "sample": {"keys": keys}})
    assert preview["sampled"] == count
    assert len(preview["documents"]) == count
    for document in preview["documents"]:
        field = document["fields"][0]
        assert field["field"] == "absent" and field["text"] == ""
        assert field["findings"] == []
        assert field["receipt"]["state"] == "inspected"
        assert field["receipt"]["policy_version"] == preview["policy_version"]
    installed = api.put(path, policy)
    assert installed["policy_version"] == preview["policy_version"]
    assert wait_until(
        lambda: (
            api.get(path + "/scans/" + installed["scan_id"])["phase"] == "succeeded"
        ),
        timeout_s=60,
        interval_s=0.2,
    ), api.debug_logs()[-6000:]
    coverage = api.get(path)["coverage"]
    assert coverage["current"] == count and coverage["failed"] == 0, coverage
    assert coverage["protected_ready"] == count, coverage
    assert len(api.scan_keys(table, {"limit": 10, "include_documents": True})) == count


@pytest.mark.parametrize("sample", [{}, {"keys": ["missing"]}])
def test_privacy_rejects_pre_storage_preview_before_sampling(stateful_api, sample):
    api = stateful_api
    table = "privacy_sync_preview_" + uuid4().hex[:8]
    api.create_table(table)
    path = f"/tables/{table}/privacy"
    response = api.s.post(
        api.url + path + "/preview",
        json={
            "policy": {"mode": "redact_before_storage", "fields": ["body"]},
            "sample": sample,
        },
        timeout=30,
    )
    assert response.status_code == 422, response.text
    assert response.json()["code"] == "privacy_synchronous_ingestion_required"
    assert not api.get(path)["configured"]


@pytest.mark.parametrize("shape", ["object", "array"])
def test_privacy_wide_json_preview_backfill_and_new_writes(stateful_api, shape):
    api = stateful_api
    table = "privacy_wide_" + uuid4().hex[:8]
    api.create_table(table)
    wide = (
        {f"k{i}": i for i in range(10000)} if shape == "object" else list(range(20000))
    )
    exact = 9007199254740993
    source = {"wide": wide, "exact": exact}
    api.batch_write(table, inserts={"doc": source}, sync_level="full_text")
    path = f"/tables/{table}/privacy"
    policy = {"mode": "redact_for_search_and_ai", "fields": ["absent"]}
    preview = api.post(
        path + "/preview", {"policy": policy, "sample": {"keys": ["doc"]}}
    )
    field = preview["documents"][0]["fields"][0]
    assert field["text"] == "" and field["findings"] == []
    assert field["receipt"]["state"] == "inspected"
    saved = api.put(path, policy)
    assert saved["policy_version"] == preview["policy_version"]
    assert wait_until(
        lambda: api.get(path + "/scans/" + saved["scan_id"])["phase"] == "succeeded",
        timeout_s=90,
        interval_s=0.2,
    ), (api.get(path), api.debug_logs()[-6000:])
    assert api.get(path)["coverage"]["current"] == 1
    assert api.get(path)["coverage"]["failed"] == 0
    assert api.get(f"/tables/{table}/documents/doc") == source
    source["exact"] += 1
    api.batch_write(table, inserts={"doc": source}, sync_level="write")

    def updated():
        result = api.s.get(api.url + f"/tables/{table}/documents/doc", timeout=30)
        if result.status_code in (404, 409, 503):
            return None
        assert result.status_code == 200, result.text
        return result.json() if result.json().get("exact") == source["exact"] else None

    assert wait_until(updated, timeout_s=90, interval_s=0.2) == source, (
        api.debug_logs()[-6000:]
    )
