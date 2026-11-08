# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Memory transport tests cover identity, scope and durable operation keys."""

import json

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


def test_memory_mutations_generate_keys_and_unknown_actions_fail_locally() -> None:
    client = AntflyClient("http://memory.test")
    seen = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={})

    with httpx.Client(base_url=client.base_url, transport=httpx.MockTransport(respond)) as http:
        client._client.set_httpx_client(http)
        client.memory.create("team", idempotency_key="")
        client.memory.execute("team", "forget")
        client.memory.execute("team", "forget")
    keys = [request.headers["idempotency-key"] for request in seen]
    assert all(len(key) == 32 for key in keys)
    assert len(set(keys)) == 3
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


def test_memory_branch_feed_supersession_and_evaluation_share_authenticated_transport() -> None:
    client = AntflyClient("http://memory.test", token="reviewer")
    seen = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={})

    with httpx.Client(base_url=client.base_url, transport=httpx.MockTransport(respond)) as http:
        client._client.set_httpx_client(http)
        client.memory.execute("team", "changes", body={"cursor": "resume"}, branch="fork")
        client.memory.execute("team", "supersession_candidates", id="claim/a", branch="fork")
        client.memory.execute("team", "evaluation_tasks", id="run/a")
        client.memory.execute(
            "team",
            "review_supersession",
            id="pair/a",
            branch="fork",
            body={"expected_revision": 7, "verdict": None},
            idempotency_key="defer",
        )
    assert all(request.method == "POST" for request in seen)
    assert all("idempotency-key" not in request.headers for request in seen[:3])
    assert seen[0].headers["x-antfly-memory-branch"] == "fork"
    assert seen[1].url.raw_path == b"/memory/v1/spaces/team/memories/claim%2Fa/supersession-candidates"
    assert seen[2].url.raw_path == b"/memory/v1/spaces/team/evaluations/run%2Fa/tasks"
    assert seen[3].url.raw_path == b"/memory/v1/spaces/team/supersession-proposals/pair%2Fa/verdict"
    assert seen[3].headers["idempotency-key"] == "defer"


def _recording_client() -> tuple[AntflyClient, list[httpx.Request], httpx.Client]:
    client = AntflyClient("http://memory.test", token="agent")
    seen: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={})

    http = httpx.Client(
        base_url=client.base_url,
        headers=client._client.get_httpx_client().headers,
        transport=httpx.MockTransport(respond),
    )
    client._client.set_httpx_client(http)
    return client, seen, http


def test_brief_and_consolidate_are_read_only_posts() -> None:
    client, seen, http = _recording_client()
    with http:
        client.memory.execute("team", "brief", body={"limit": 4}, idempotency_key="ignored")
        client.memory.execute("team", "consolidate")
        client.memory.brief("team")
        client.memory.brief("team", limit=8, token_budget=512, scope="repo", include_provisional=False, branch="fork")
        client.memory.consolidate("team", limit=32, stale_after_ms=2592000000, database="projects")
    assert [request.method for request in seen] == ["POST"] * 5
    assert [request.url.path for request in seen] == [
        "/memory/v1/spaces/team/brief",
        "/memory/v1/spaces/team/consolidation",
        "/memory/v1/spaces/team/brief",
        "/memory/v1/spaces/team/brief",
        "/memory/v1/spaces/team/consolidation",
    ]
    assert all("idempotency-key" not in request.headers for request in seen)
    assert json.loads(seen[2].content) == {}
    assert json.loads(seen[3].content) == {
        "scope": "repo",
        "include_provisional": False,
        "limit": 8,
        "token_budget": 512,
    }
    assert seen[3].headers["x-antfly-memory-branch"] == "fork"
    assert json.loads(seen[4].content) == {"limit": 32, "stale_after_ms": 2592000000}
    assert seen[4].headers["x-antfly-database"] == "projects"
    assert seen[4].headers["authorization"] == "Bearer agent"


def test_recall_passes_known_fields_and_rejects_unknown() -> None:
    client, seen, http = _recording_client()
    with http:
        client.memory.recall("team", query="deploy", subject="production", utility_weight=0.5, limit=None)
        client.memory.recall("team", namespace="agents")
        with pytest.raises(TypeError, match="utilty"):
            client.memory.recall("team", query="x", utilty=0.5)
    assert seen[0].url.path == "/memory/v1/spaces/team/recall"
    assert "idempotency-key" not in seen[0].headers
    assert json.loads(seen[0].content) == {"query": "deploy", "subject": "production", "utility_weight": 0.5}
    assert json.loads(seen[1].content) == {"query": ""}
    assert seen[1].headers["x-antfly-namespace"] == "agents"
    assert len(seen) == 2


def test_simple_path_mutations_generate_or_pass_through_keys() -> None:
    client, seen, http = _recording_client()
    with http:
        client.memory.remember("team", "Production deploys with Terraform.")
        client.memory.remember(
            "team",
            "Runbook v2",
            evidence_id="runbook-2",
            source_id="runbook",
            thread_id="t1",
            visibility="team",
            assertions=[{"id": "a"}],
            idempotency_key="observe-1",
            database="projects",
            namespace="agents",
            branch="fork",
        )
        client.memory.outcome("team", id="o1", task_id="task", success=True, used_memory_ids=["a"])
        client.memory.outcome(
            "team", id="o2", task_id="task", success=False, used_memory_ids=[], detail="failed", idempotency_key="o2"
        )
        client.memory.pin("team", "claim/a", reason="always needed")
        client.memory.unpin("team", "claim/a", idempotency_key="unpin-a")
        client.memory.correct("team", "a", expected_revision=3, lifecycle="retracted", reason="wrong")
        client.memory.forget("team", source_ids=["runbook"])
    assert [request.url.path.rsplit("/", 1)[1] for request in seen] == [
        "observations",
        "observations",
        "outcomes",
        "outcomes",
        "pins",
        "pins",
        "corrections",
        "forget",
    ]
    generated = [seen[i].headers["idempotency-key"] for i in (0, 2, 4, 6, 7)]
    assert all(len(key) == 32 for key in generated)
    assert len(set(generated)) == len(generated)
    assert seen[1].headers["idempotency-key"] == "observe-1"
    assert seen[3].headers["idempotency-key"] == "o2"
    assert seen[5].headers["idempotency-key"] == "unpin-a"

    first = json.loads(seen[0].content)
    assert first["evidence"][0]["text"] == "Production deploys with Terraform."
    assert first["evidence"][0]["source_id"] == "agent"
    assert len(first["evidence"][0]["id"]) == 32
    assert set(first) == {"evidence"} and set(first["evidence"][0]) == {"id", "source_id", "text"}
    assert json.loads(seen[1].content) == {
        "evidence": [
            {"id": "runbook-2", "source_id": "runbook", "text": "Runbook v2", "thread_id": "t1", "visibility": "team"}
        ],
        "assertions": [{"id": "a", "evidence": [{"id": "runbook-2"}]}],
    }
    assert seen[1].headers["x-antfly-database"] == "projects"
    assert seen[1].headers["x-antfly-namespace"] == "agents"
    assert seen[1].headers["x-antfly-memory-branch"] == "fork"
    assert json.loads(seen[2].content) == {"id": "o1", "task_id": "task", "success": True, "used_memory_ids": ["a"]}
    assert json.loads(seen[3].content)["detail"] == "failed"
    assert json.loads(seen[4].content) == {"id": "claim/a", "pinned": True, "reason": "always needed"}
    assert json.loads(seen[5].content) == {"id": "claim/a", "pinned": False}
    assert json.loads(seen[6].content) == {
        "id": "a",
        "expected_revision": 3,
        "lifecycle": "retracted",
        "reason": "wrong",
    }
    assert json.loads(seen[7].content) == {"source_ids": ["runbook"]}


def test_generated_client_exposes_brief_consolidation_and_pins() -> None:
    from antfly.client_generated.api.memory import (
        brief_memory,
        consolidate_memory,
        get_memory_brief,
        pin_memory,
        put_memory_space,
    )
    from antfly.client_generated.models.memory_brief_request import MemoryBriefRequest
    from antfly.client_generated.models.memory_pin import MemoryPin

    for module in (brief_memory, consolidate_memory, get_memory_brief, pin_memory, put_memory_space):
        assert callable(module.sync_detailed)
    assert MemoryPin(id="a").to_dict()["id"] == "a"
    assert MemoryBriefRequest(limit=4).to_dict()["limit"] == 4



def test_remember_explicit_key_retries_send_identical_evidence_and_fill_citations() -> None:
    client, seen, http = _recording_client()
    with http:
        for _ in range(2):
            client.memory.remember("team", "same text", idempotency_key="retry", assertions=[{"id": "a"}])
        client.memory.remember("team", "same text", idempotency_key="other")
    assert seen[0].content == seen[1].content
    assert seen[0].headers["idempotency-key"] == seen[1].headers["idempotency-key"] == "retry"
    body = json.loads(seen[0].content)
    assert body["assertions"][0]["evidence"] == [{"id": body["evidence"][0]["id"]}]
    assert body["evidence"][0]["id"] != json.loads(seen[2].content)["evidence"][0]["id"]
