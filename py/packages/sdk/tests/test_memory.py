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


def test_memory_explanation_is_read_only_post_with_resource_scope() -> None:
    client = AntflyClient("http://memory.test", token="reader")
    seen = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"nodes": [], "complete": True})

    with httpx.Client(base_url=client.base_url, transport=httpx.MockTransport(respond)) as http:
        client._client.set_httpx_client(http)
        client.memory.execute("team", "explain", id="claim/a", body={"revision": 7})
        client.memory.execute("team", "explain", body={"resource": {"kind": "record"}}, database="business")
    assert seen[0].method == "POST"
    assert seen[0].url.raw_path == b"/memory/v1/spaces/team/memories/claim%2Fa/explain"
    assert "idempotency-key" not in seen[0].headers
    assert seen[1].url.path == "/memory/v1/spaces/team/explain"
    assert seen[1].headers["x-antfly-database"] == "business"


def test_native_views_episodes_and_external_verdict_routes() -> None:
    client = AntflyClient("http://memory.test", token="reviewer")
    seen = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={})

    with httpx.Client(base_url=client.base_url, transport=httpx.MockTransport(respond)) as http:
        client._client.set_httpx_client(http)
        client.memory.execute("team", "entity_view", id="concept/a", body={"known_at": 0})
        client.memory.execute("team", "entity_candidates", id="concept/a", body={"kind": "relationship"})
        client.memory.execute("team", "episode", id="thread/a", body={"limit": 1, "known_at": 0})
        client.memory.execute(
            "team",
            "review_entity",
            id="proposal/a",
            body={"expected_revision": 2**64 - 1, "accept": None},
            idempotency_key="defer",
        )
        client.memory.execute(
            "team",
            "review_relationship",
            id="proposal/b",
            body={"expected_revision": 7, "accept": True},
            idempotency_key="accept",
        )
    assert seen[0].url.raw_path == b"/memory/v1/spaces/team/entities/concept%2Fa/view"
    assert "idempotency-key" not in seen[0].headers
    assert "idempotency-key" not in seen[1].headers
    assert seen[2].url.raw_path.startswith(b"/memory/v1/spaces/team/episodes/thread%2Fa?")
    assert seen[2].url.params["known_at"] == "0"
    assert seen[3].url.raw_path == b"/memory/v1/spaces/team/entity-proposals/proposal%2Fa/verdict"
    assert b"18446744073709551615" in seen[3].content
    assert seen[4].url.raw_path == b"/memory/v1/spaces/team/relationship-proposals/proposal%2Fb/verdict"


def test_generated_memory_reads_decode_native_service_fixtures() -> None:
    """Exercise generated HTTP parsers against responses pinned by native tests."""
    from pathlib import Path

    from antfly.client_generated.api.memory import (
        get_memory_entity_candidates,
        get_memory_entity_proposal,
        list_memory_entity_proposals,
    )
    from antfly.client_generated.client import Client
    from antfly.client_generated.models.memory_entity_candidate_page import MemoryEntityCandidatePage
    from antfly.client_generated.models.memory_entity_candidate_request import MemoryEntityCandidateRequest
    from antfly.client_generated.models.memory_entity_proposal_detail import MemoryEntityProposalDetail
    from antfly.client_generated.models.memory_entity_proposal_page import MemoryEntityProposalPage

    fixtures = Path(__file__).resolve().parents[4] / "zig/pkg/antfly-embedded/src/memory/fixtures/wire"
    bodies = {name: (fixtures / f"{name}.json").read_bytes() for name in ("candidates", "proposal", "proposals")}

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/candidates"):
            name = "candidates"
        elif request.url.path.endswith("/entity-proposals"):
            name = "proposals"
        else:
            name = "proposal"
        return httpx.Response(200, content=bodies[name], headers={"Content-Type": "application/json"})

    client = Client(base_url="http://memory.test")
    with httpx.Client(base_url="http://memory.test", transport=httpx.MockTransport(respond)) as http:
        client.set_httpx_client(http)
        candidates = get_memory_entity_candidates.sync("wire", "a", client=client, body=MemoryEntityCandidateRequest())
        assert isinstance(candidates, MemoryEntityCandidatePage)
        assert candidates.seed.entity.id == "a"
        assert candidates.items[0].entity.entity.id == "b"
        assert candidates.items[0].view.canonical_id == "b"
        assert candidates.revision == 3
        assert candidates.next_cursor is None
        proposals = list_memory_entity_proposals.sync("wire", client=client)
        assert isinstance(proposals, MemoryEntityProposalPage)
        proposal_id = proposals.items[0].id
        details = get_memory_entity_proposal.sync("wire", proposal_id, client=client)
        assert isinstance(details, MemoryEntityProposalDetail)
        assert details.proposal.id == proposal_id
        assert details.inputs.actor == "codex/consolidation"
        assert details.inputs.entities[0].revision == 3
        assert details.reason is None
        assert details.stale_configuration is False
