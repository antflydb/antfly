# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Memory transport tests cover identity, scope and durable operation keys."""

import httpx
import pytest

from antfly import AntflyClient


def test_memory_preserves_authenticated_transport_and_catalog_scope() -> None:
    client = AntflyClient("http://memory.test", token="scoped-token")
    seen = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(201 if request.url.path == "/memory/v1/spaces" else 200, json={"phase": "succeeded"})

    generated = client._client
    with httpx.Client(
        base_url=client.base_url, headers=generated.get_httpx_client().headers, transport=httpx.MockTransport(respond)
    ) as http:
        generated.set_httpx_client(http)
        client.memory.create("team", idempotency_key="create", database="projects", namespace="agents")
        client.memory.execute(
            "team", "observe", body={"evidence": []}, idempotency_key="event", database="projects", namespace="agents"
        )
        client.memory.execute("team", "advance", idempotency_key="maintenance")
    assert seen[0].headers["authorization"] == "Bearer scoped-token"
    assert seen[0].headers["idempotency-key"] == "create"
    assert seen[1].headers["x-antfly-database"] == "projects"
    assert seen[1].headers["x-antfly-namespace"] == "agents"
    assert seen[1].url.path == "/memory/v1/spaces/team/observations"
    assert seen[2].url.path == "/memory/v1/spaces/team/maintenance"


def test_memory_mutations_require_keys_and_unknown_actions_fail_locally() -> None:
    client = AntflyClient("http://memory.test")
    with pytest.raises(ValueError, match="idempotency_key"):
        client.memory.create("team", idempotency_key="")
    with pytest.raises(ValueError, match="idempotency_key"):
        client.memory.execute("team", "forget")
    with pytest.raises(ValueError, match="unknown"):
        client.memory.execute("team", "invent")
