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

"""An unfinished migration must not fence a qualified protected consumer."""

import json
from uuid import uuid4

from helpers import wait_until
from test_auth import AUTH_BOOTSTRAP_PASSWORD, _basic_auth
from test_auth import auth_api as auth_api
from test_privacy_indexed_activation import (
    policy,
    redacted,
)


from test_privacy_indexed_activation import privacy_provider as privacy_provider


def test_protected_tenant_serves_a_during_held_b_and_rolls_back(
    auth_api,
    privacy_provider,  # noqa: F811 -- imported pytest fixtures
):
    api = auth_api
    endpoint, calls, behavior, _ = privacy_provider
    admin = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
    api.s.headers["Authorization"] = admin
    table = "consumer_readiness_" + uuid4().hex[:8]
    api.create_table(table)

    def add_index(name, model):
        api.post(
            f"/tables/{table}/indexes/{name}",
            {
                "type": "embeddings",
                "dimension": 2,
                "field": "body",
                "distance_metric": "cosine",
                "publication_policy": "atomic",
                "embedder": {"provider": "antfly", "model": model, "api_url": endpoint},
            },
        )

    def complete(name):
        return api.get(f"/tables/{table}/indexes/{name}")["status"]["readiness"][
            "complete"
        ]

    add_index("a0", "fixture")
    docs = {"d0": {"body": "Orchard schedule. Contact person@example.invalid."}}
    api.batch_write(table, {"inserts": docs, "sync_level": "full_index"})
    api.put(f"/tables/{table}/privacy", policy(endpoint))
    assert wait_until(lambda: redacted(api, table, docs), timeout_s=60)
    assert wait_until(lambda: complete("a0"), timeout_s=60)
    alias = f"/tables/{table}/index-aliases/search"
    api.put(alias, {"index": "a0", "idempotency_key": "initial", "require": "complete"})
    api.post(
        "/auth/v1/users/reader",
        {
            "password": "reader",
            "initial_policies": [
                {"resource": table, "resource_type": "table", "type": "read"}
            ],
        },
    )
    tenant = _basic_auth("reader", "reader")
    behavior["hold_model"] = "fixture-v2"
    behavior["model_gate"].clear()
    try:
        add_index("b", "fixture-v2")
        assert behavior["model_held"].wait(timeout=20), api._server.debug_logs()
        assert not complete("b")
        status = api.get(f"/tables/{table}/indexes/a0")["status"]
        assert status["readiness"]["queryable"], status
        assert not status["rebuilding"], json.dumps(status, sort_keys=True)
        api.s.headers["Authorization"] = tenant
        for name in ("a0", "search"):
            assert redacted(api, table, docs, name)
        response = api.s.post(
            api.url + "/query",
            json={
                "table": table,
                "indexes": ["b"],
                "embeddings": {"b": [0.0, 1.0]},
                "fields": ["body"],
            },
            timeout=15,
        )
        if response.status_code == 200:
            row = response.json()["responses"][0]
            assert row["status"] == 503, row
        else:
            assert response.status_code == 503, response.text
    finally:
        behavior["model_gate"].set()
        api.s.headers["Authorization"] = admin
    assert wait_until(lambda: complete("b"), timeout_s=60)
    before = len(calls["embed"])
    for name, expected in (("a0", [1.0, 0.0]), ("b", [0.0, 1.0])):
        manifest = api.get(f"/tables/{table}/documents/d0/artifacts/{name}?detail=raw")
        snapshot = json.loads(manifest["manifest_json"])
        assert snapshot["current"]
        assert len(snapshot["vectors"]) == 1
        assert snapshot["vectors"][0]["values"] == expected
        assert "@" not in snapshot["vectors"][0]["source_text"]
    assert len(calls["embed"]) == before
    for target, previous, vector in (("b", "a0", [0.0, 1.0]), ("a0", "b", [1.0, 0.0])):
        api.put(
            alias,
            {
                "index": target,
                "expected_index": previous,
                "idempotency_key": target,
                "require": "complete",
            },
        )
        api.s.headers["Authorization"] = tenant
        try:
            hits = redacted(api, table, docs, "search", vector)
            assert hits and all(hit["_score"] > 0.99 for hit in hits)
        finally:
            api.s.headers["Authorization"] = admin
