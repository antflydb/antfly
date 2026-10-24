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

"""Resolver curation admission, owner progress, and namespace routes over HTTP."""

from urllib.parse import quote
from uuid import uuid4

import pytest


# Server ownership is dynamic/direct, outside static fixture inference.
pytestmark = pytest.mark.e2e_resource("antfly_process")


def test_entity_curation_namespace_wait_and_resolver_routes(stateful_api):
    api = stateful_api
    suffix = uuid4().hex[:8]
    destination = f"curation_entities_{suffix}"
    sources = [f"curation_notes_{suffix}", f"curation_memory_{suffix}"]
    api.create_table(destination)
    graph = {
        "type": "graph",
        "source": {
            "artifact": "kg",
            "format": "extraction_graph",
            "mention_edge_type": "mentions",
        },
        "artifact": {
            "name": "kg",
            "kind": "asset",
            "source": {"type": "field", "value": "kg"},
            "content_type": "application/json",
        },
        "edge_types": [{"name": "mentions"}],
        "resolvers": [
            {
                "name": "concepts",
                "table": destination,
                "promote": False,
                "source_artifact": "kg",
                "resolution_artifact": "resolved",
                "key_template": "concept/{{uslug _entity.text}}",
            }
        ],
    }
    for source in sources:
        api.create_table(source, indexes={"knowledge": graph})
    # Generated query adapters must reach the shared pagination/filter parser
    # through both table aliases, without changing resolver policy revisions.
    proposals = f"/tables/{destination}/curation/proposals"
    proposed = {
        api.post(proposals, {"action": "suppress", "key": key})["id"]
        for key in ("concept/proposed-one", "concept/proposed-two")
    }
    for path in (proposals, f"/databases/default/namespaces/public{proposals}"):
        query = "?state=open&action=suppress&limit=1"
        first = api.get(path + query)
        assert len(first["proposals"]) == 1, first
        assert first.get("next_cursor"), first
        second = api.get(path + query + "&cursor=" + first["next_cursor"])
        assert len(second["proposals"]) == 1, second
        assert not second.get("next_cursor"), second
        assert {first["proposals"][0]["id"], second["proposals"][0]["id"]} == proposed
        assert api.get(path + "?state=rejected&limit=1")["proposals"] == []
        assert api.get(path + "?state=open&action=merge&limit=1")["proposals"] == []
    # Main also checks a single open proposal later in this flow. Close these
    # pagination fixtures so both checks own their expected proposal set.
    for proposal_id in proposed:
        api.post(f"{proposals}/{proposal_id}:reject", {})
    key = quote("concept/alpha", safe="")
    entity = f"/tables/{destination}/entities/{key}"
    wait = {"timeout_ms": 5000}
    merged = api.post(entity + ":merge", {"into": "concept/beta", "wait": wait})
    assert merged["complete"] and merged["applied"], merged
    # Destination-owned policy is metadata, never an index or public config.
    assert "_resolver_namespaces" not in str(api.get_table(destination))
    assert "_resolver_namespaces" not in str(api.get(f"/tables/{destination}/indexes"))
    assert len(merged["results"]) == 2
    assert all(row["accepted"] and row["revision"] == 1 for row in merged["results"])
    mention = {"text": "Alpha", "label": "concept"}
    for source in sources:
        rendered = api.post(f"/tables/{source}/resolvers/concepts:render", mention)
        assert rendered["key"] == "concept/beta"
    undone = api.post(entity + ":unmerge", {"pin_distinct": True, "wait": wait})
    assert undone["complete"] and undone["applied"], undone
    suppressed = api.post(entity + ":suppress", {"wait": wait})
    assert suppressed["complete"] and suppressed["applied"], suppressed
    for source in sources:
        assert api.post(f"/tables/{source}/resolvers/concepts:render", mention)[
            "suppressed"
        ]
    assert suppressed["atomicity"] == "namespace"
    # A consumer created after the correction inherits it before its first row.
    late = f"curation_late_{suffix}"
    api.create_table(late, indexes={"knowledge": graph})
    sources.append(late)
    late_state = api.post(f"/tables/{late}/resolvers/concepts:render", mention)
    assert late_state["suppressed"], late_state
    redirects = api.get(entity + "/redirects")
    assert redirects["atomicity"] == "namespace"
    assert len(redirects["results"]) == 1
    restored = api.post(entity + ":unsuppress", {"wait": wait})
    assert restored["complete"] and restored["applied"], restored
    for source in sources:
        resolver = f"/tables/{source}/resolvers/concepts"
        state = api.get(resolver)
        assert state["revision"] == 4 and state["progress"]["applied"], state
        scoped = api.get(f"/databases/default/namespaces/public{resolver}")
        assert scoped["revision"] == 4 and scoped["progress"]["applied"], scoped
        assert api.get(resolver + "/reviews?limit=1")["reviews"] == []
        preview = api.post(
            resolver + ":resolve",
            {
                "extraction": {
                    "entities": [{"id": "a", "text": "Alpha", "label": "concept"}]
                }
            },
        )
        assert preview["resolution"]["entities"][0]["doc_ref"]["key"] == "concept/alpha"
    resolver = f"/tables/{sources[0]}/resolvers/concepts"
    # Once adopted, source resources are views; corrections use the destination.
    rejected = api._request(
        "POST", resolver + "/redirects", {"from": "concept/zeta", "to": "concept/eta"}
    )
    assert rejected.status_code == 409, rejected.text
    zeta = f"/tables/{destination}/entities/" + quote("concept/zeta", safe="")
    api.post(zeta + ":merge", {"into": "concept/eta", "wait": wait})
    decision = api.post(zeta + ":unmerge", {"pin_distinct": True, "wait": wait})
    assert decision["changed"], decision
    assert api.get(resolver + "/redirects")["redirects"] == []

    proposals = f"/tables/{destination}/curation/proposals"
    proposed = api.post(proposals, {"action": "suppress", "key": "concept/alpha"})
    for prefix in ("", "/databases/default/namespaces/public"):
        route = prefix + proposals
        page = api.get(route + "?state=open&action=suppress&limit=1")
        assert [item["id"] for item in page["proposals"]] == [proposed["id"]]
        assert api.get(route + "?state=accepted&limit=1")["proposals"] == []
        invalid = api._request("GET", route + "?limit=0")
        assert invalid.status_code == 400, invalid.text


@pytest.mark.parametrize("raft", [False, True], ids=["standalone", "raft"])
@pytest.mark.parametrize("promote", [False, True], ids=["resolution", "promotion"])
def test_curator_review_decisions_survive_restart(request, monkeypatch, raft, promote):
    import json

    from helpers import wait_until

    if raft:
        monkeypatch.setenv("ANTFLY_E2E_STATEFUL_RAFT", "1")
    api = request.getfixturevalue("stateful_api")
    suffix = uuid4().hex[:8]
    destination, source = f"review_entities_{suffix}", f"review_sources_{suffix}"
    api.create_table(destination)
    api.batch_write(
        destination,
        inserts={
            "concept/alpha": {
                "canonical_name": "Alpha",
                "label": "concept",
            }
        },
        sync_level="full_index",
    )
    api.create_table(
        source,
        indexes={
            "knowledge": {
                "type": "graph",
                "source": {
                    "artifact": "kg",
                    "format": "extraction_graph",
                    "mention_edge_type": "mentions",
                },
                "edge_types": [{"name": "mentions"}],
                "artifact": {
                    "name": "kg",
                    "kind": "asset",
                    "source": {"type": "field", "value": "kg"},
                    "content_type": "application/json",
                },
                "resolvers": [
                    {
                        "name": "concepts",
                        "table": destination,
                        "promote": promote,
                        "config_generation": 1,
                        "source_artifact": "kg",
                        "resolution_artifact": "resolved",
                        "key_template": "concept/{{uslug _entity.text}}",
                        "candidate_search": "exact_key",
                        "scorer": {
                            "comparisons": [],
                            "combine": {"bias": 1.0},
                            "decision": {"match": 0.95, "review": 0.6},
                        },
                    }
                ],
            }
        },
    )
    resolver = f"/tables/{source}/resolvers/concepts"
    assert wait_until(
        lambda: all(
            row["available"] and row["fresh"] and row["config_generation"] >= 1
            for row in api.get(resolver)["progress"]["shards"]
        ),
        timeout_s=30,
        interval_s=0.1,
    ), api.get(resolver)

    def all_reviews():
        items = []
        path = resolver + "/reviews"
        while True:
            page = api.get(path)
            items.extend(page["reviews"])
            cursor = page.get("next_cursor")
            if cursor is None:
                return items
            # Pages bound physical scan work and may contain no reviews while
            # retaining a cursor, particularly after prior reviews settle.
            path = resolver + "/reviews?cursor=" + quote(cursor, safe="")

    last_sequence = 0
    # Reuse the same owner after each publication: initial startup can resolve
    # before ordered producer authority is active and conceal output-read bugs.
    for index in range(3):
        api.batch_write(
            source,
            inserts={
                f"doc-{index}": {
                    "kg": json.dumps(
                        {
                            "entities": [
                                {"id": "e0", "label": "concept", "text": "Alpha"}
                            ]
                        }
                    )
                }
            },
            sync_level="full_index",
        )
        reviews = []

        def pending_review(reviews=reviews):
            reviews[:] = all_reviews()
            return bool(reviews)

        assert wait_until(pending_review, timeout_s=30, interval_s=0.1), (
            api.debug_logs()[-16000:]
        )
        review_id = quote(reviews[0]["id"], safe="")
        accepted = api.post(
            resolver + "/reviews/" + review_id,
            {"decision": "suppressed", "wait": {"timeout_ms": 5000}},
        )
        assert accepted["replay_sequence_kind"] == "source_journal", accepted
        assert accepted["replay_sequence"] > last_sequence, accepted
        last_sequence = accepted["replay_sequence"]

        # The operation is durable even when its bounded HTTP observation
        # returns 202. Await the exact journal cut on detached workers rather
        # than assuming Raft, replay and catalog reads finish in five seconds.
        def decision_applied(accepted=accepted):
            progress = api.get(resolver)["progress"]
            return bool(progress["shards"]) and all(
                row["available"]
                and row["fresh"]
                and row["resolution"]["applied_sequence"] >= accepted["replay_sequence"]
                and (
                    not promote
                    or row["promotion"]["applied_sequence"]
                    >= accepted["replay_sequence"]
                )
                for row in progress["shards"]
            )

        assert accepted["applied"] or wait_until(
            decision_applied, timeout_s=30, interval_s=0.1
        ), (
            json.dumps({"accepted": accepted, "resolver": api.get(resolver)})
            + api.debug_logs()[-16000:]
        )
        assert all_reviews() == []
    api.restart_server()
    assert all_reviews() == []


@pytest.mark.parametrize("raft", [False, True], ids=["standalone", "raft"])
def test_namespace_rejects_incompatible_consumer_without_blocking_metadata(
    request, monkeypatch, raft
):
    import requests

    if raft:
        monkeypatch.setenv("ANTFLY_E2E_STATEFUL_RAFT", "1")
    api = request.getfixturevalue("stateful_api")
    suffix = uuid4().hex[:8]
    destination, source = f"alias_entities_{suffix}", f"alias_source_{suffix}"
    api.create_table(destination)
    graph = {
        "type": "graph",
        "source": {
            "artifact": "kg",
            "format": "extraction_graph",
            "mention_edge_type": "mentions",
        },
        "artifact": {
            "name": "kg",
            "kind": "asset",
            "source": {"type": "field", "value": "kg"},
            "content_type": "application/json",
        },
        "edge_types": [{"name": "mentions"}],
        "resolvers": [
            {
                "name": "concepts",
                "table": destination,
                "promote": False,
                "source_artifact": "kg",
                "resolution_artifact": "resolved",
                "key_template": "concept/{{uslug _entity.text}}",
            }
        ],
    }
    api.create_table(source, indexes={"knowledge": graph})
    entity = f"/tables/{destination}/entities/{quote('concept/a', safe='')}"
    merged = api.post(
        entity + ":merge", {"into": "concept/b", "wait": {"timeout_ms": 5000}}
    )
    assert merged["applied"], merged
    # The projected create must also publish its logical catalog binding and
    # certify the inherited definition in both metadata implementations.
    late = f"alias_late_{suffix}"
    api.create_table(late, indexes={"knowledge": graph})
    rendered = api.post(
        f"/tables/{late}/resolvers/concepts:render", {"text": "A", "label": "concept"}
    )
    assert rendered["key"] == "concept/b", rendered
    graph["resolvers"][0]["aliases"] = [{"key": "concept/b", "target": "concept/a"}]
    with pytest.raises(requests.HTTPError) as rejected:
        api.create_table(f"alias_rejected_{suffix}", indexes={"knowledge": graph})
    rejected_response = rejected.value.response
    assert rejected_response.status_code == 409, (
        rejected_response.status_code,
        rejected_response.text,
        api.debug_logs(),
    )
    assert "outcome is unknown" not in rejected_response.text, (
        rejected_response.text,
        api.debug_logs(),
    )
    api.create_table(f"alias_healthy_{suffix}")
    assert api.get(entity + "/redirects")["results"][0]["canonical_key"] == "concept/b"
    if api.supports_restart:
        api.restart_server()
        api.create_table(f"alias_after_restart_{suffix}")
        assert (
            api.get(entity + "/redirects")["results"][0]["canonical_key"] == "concept/b"
        )


@pytest.mark.parametrize("raft", [False, True], ids=["standalone", "raft"])
@pytest.mark.parametrize("policy", ["legacy", "inline", "adopted-inline"])
def test_entity_merge_adopts_matching_local_policy(request, monkeypatch, raft, policy):
    import copy

    if raft:
        monkeypatch.setenv("ANTFLY_E2E_STATEFUL_RAFT", "1")
    api = request.getfixturevalue("stateful_api")
    suffix = uuid4().hex[:8]
    destination, first, second = (
        f"adopt_entities_{suffix}",
        f"adopt_first_{suffix}",
        f"adopt_second_{suffix}",
    )
    api.create_table(destination)
    graph = {
        "type": "graph",
        "source": {
            "artifact": "kg",
            "format": "extraction_graph",
            "mention_edge_type": "mentions",
        },
        "artifact": {
            "name": "kg",
            "kind": "asset",
            "source": {"type": "field", "value": "kg"},
            "content_type": "application/json",
        },
        "edge_types": [{"name": "mentions"}],
        "resolvers": [
            {
                "name": "concepts",
                "table": destination,
                "promote": False,
                "source_artifact": "kg",
                "resolution_artifact": "resolved",
                "key_template": "concept/{{uslug _entity.text}}",
            }
        ],
    }
    first_graph = copy.deepcopy(graph)
    if policy != "legacy":
        first_graph["resolvers"][0]["aliases"] = [
            {"key": "concept/a", "target": "concept/b"}
        ]
    api.create_table(first, indexes={"knowledge": first_graph})
    api.create_table(second, indexes={"knowledge": graph})
    wait = {"timeout_ms": 5000}
    if policy == "legacy":
        local = api.post(
            f"/tables/{first}/resolvers/concepts/redirects",
            {"from": "concept/a", "to": "concept/b", "wait": wait},
        )
        assert local["applied"], local
    elif policy == "adopted-inline":
        adopted = api.post(
            f"/tables/{destination}/entities/{quote('concept/noise', safe='')}:suppress",
            {"wait": wait},
        )
        assert adopted["applied"], adopted
    entity = f"/tables/{destination}/entities/{quote('concept/a', safe='')}:merge"
    merged = api.post(entity, {"into": "concept/b", "wait": wait})
    assert merged["changed"] and merged["applied"], merged
    assert all(row["accepted"] for row in merged["results"]), merged
    revision = merged["results"][0]["revision"]
    assert revision == (1 if policy == "inline" else 2), merged
    for source_name in (first, second):
        rendered = api.post(
            f"/tables/{source_name}/resolvers/concepts:render",
            {"text": "A", "label": "concept"},
        )
        assert rendered["key"] == "concept/b", rendered
    resolved = api.post(
        f"/tables/{second}/resolvers/concepts:resolve",
        {"extraction": {"entities": []}},
    )
    assert resolved["resolution"]["entities"] == [], resolved
    # Once authority is published, retries do not create another revision.
    retried = api.post(entity, {"into": "concept/b", "wait": wait})
    assert not retried["changed"] and retried["applied"], retried
    assert retried["metadata_commit"] is None, retried
    assert all(row["revision"] == revision for row in retried["results"]), retried
    if api.supports_restart:
        api.restart_server()
        for source_name in (first, second):
            rendered = api.post(
                f"/tables/{source_name}/resolvers/concepts:render",
                {"text": "A", "label": "concept"},
            )
            assert rendered["key"] == "concept/b", rendered


@pytest.mark.parametrize("action", ["merge", "suppress"])
@pytest.mark.parametrize("raft", [False, True], ids=["standalone", "raft"])
def test_runtime_entity_curation_preserves_future_promotion(
    request, monkeypatch, action, raft
):
    """Curation before any destination row exists must retain a usable sink."""
    import json

    from helpers import wait_until

    if raft:
        monkeypatch.setenv("ANTFLY_E2E_STATEFUL_RAFT", "1")
    api = request.getfixturevalue("stateful_api")
    suffix = uuid4().hex[:8]
    destination, source = f"live_entities_{suffix}", f"live_notes_{suffix}"
    api.create_table(destination)
    api.create_table(
        source,
        indexes={
            "knowledge": {
                "type": "graph",
                "source": {
                    "artifact": "kg",
                    "format": "extraction_graph",
                    "mention_edge_type": "mentions",
                },
                "artifact": {
                    "name": "kg",
                    "kind": "asset",
                    "source": {"type": "field", "value": "kg"},
                    "content_type": "application/json",
                },
                "edge_types": [{"name": "mentions"}],
                "resolvers": [
                    {
                        "name": "concepts",
                        "table": destination,
                        "source_artifact": "kg",
                        "resolution_artifact": "resolved",
                        "key_template": "concept/{{uslug _entity.text}}",
                    }
                ],
            }
        },
    )
    path = f"/tables/{destination}/entities/{quote('concept/alpha', safe='')}:{action}"
    decision = api.post(
        path,
        {
            "wait": {"timeout_ms": 5000},
            **({"into": "concept/canonical"} if action == "merge" else {}),
        },
    )
    assert decision["complete"] and decision["applied"], decision
    for i in range(2):
        api.batch_write(
            source,
            inserts={
                f"note-{i}": {
                    "kg": {
                        "entities": [
                            {"id": "a", "text": "Alpha", "label": "concept"},
                            {"id": "b", "text": f"Healthy {i}", "label": "concept"},
                        ]
                    }
                }
            },
            sync_level="write",
        )
        expected = {f"concept/healthy_{n}" for n in range(i + 1)}
        if action == "merge":
            expected.add("concept/canonical")

        def promoted(expected=expected):
            response = api.query_table(
                destination, {"query": {"match_all": {}}, "limit": 20}
            )
            hits = response["responses"][0]["hits"]["hits"]
            ids = {hit["_id"] for hit in hits}
            return expected <= ids and "concept/alpha" not in ids

        assert wait_until(promoted, timeout_s=60), (
            json.dumps(api.get(f"/tables/{source}/resolvers/concepts"), sort_keys=True)
            + "\n"
            + api.debug_logs()[-16000:]
        )
    assert (
        "promotion catch-up failed: EntityCurationUnavailable"
        not in api._server.debug_logs()
    )

    # Table qualification is accepted by server and embedded traversal. A bare
    # incoming key on a resolver graph must not silently select the source table.
    def traversal(start, deduplicate=True):
        return api._request(
            "POST",
            f"/tables/{source}/query",
            {
                "graph_queries": {
                    "incoming": {
                        "index": "knowledge",
                        "traverse": {
                            "start": start,
                            "direction": "in",
                            "edge_types": ["mentions"],
                            "max_depth": 1,
                            "include_paths": True,
                            "deduplicate_nodes": deduplicate,
                        },
                    }
                }
            },
        )

    seed = "concept/canonical" if action == "merge" else "concept/healthy_0"
    for start in ({"keys": [seed]}, {"identities": [{"key": seed}]}):
        bare = traversal(start)
        assert bare.status_code == 422, bare.text
        assert bare.json()["reason"] == "qualified_start_identity_required", bare.text
    qualified = {"identities": [{"table": destination, "key": seed}]}
    # A valid incoming query must not be blamed for a later query's rejection
    # when the error crosses an independently compiled archive.
    for invalid_params, reason in (
        ({"start": {"keys": [seed]}}, "qualified_start_identity_required"),
        (
            {"start": qualified, "deduplicate_nodes": False},
            "deduplicate_nodes_must_be_true",
        ),
    ):
        mixed = api._request(
            "POST",
            f"/tables/{source}/query",
            {
                "graph_queries": {
                    "valid": {
                        "index": "knowledge",
                        "traverse": {
                            "start": qualified,
                            "direction": "in",
                            "max_depth": 1,
                        },
                    },
                    "invalid": {
                        "index": "knowledge",
                        "traverse": {
                            "direction": "in",
                            "max_depth": 1,
                            **invalid_params,
                        },
                    },
                }
            },
        )
        if mixed.ok and reason == "deduplicate_nodes_must_be_true":
            # An unauthenticated complete single-group snapshot can retain
            # per-seed paths locally. Only coordinator admission rejects it.
            results = mixed.json()["responses"][0]["graph_results"]
            assert "valid" in results and "invalid" in results, mixed.text
            continue
        assert mixed.status_code == 422, mixed.text
        assert mixed.json()["operation"] == "invalid", mixed.text
        assert mixed.json()["reason"] == reason, mixed.text

    expected_nodes = {"note-0", "note-1"} if action == "merge" else {"note-0"}

    def graph_published():
        # Entity promotion and graph publication are independent consumers.
        # A write-level acknowledgement plus promoted rows does not fence graph
        # visibility, particularly with independently applied Raft publications.
        reached = traversal(qualified)
        assert reached.ok, reached.text
        nodes = reached.json()["responses"][0]["graph_results"]["incoming"]["nodes"]
        return nodes if {node["key"] for node in nodes} == expected_nodes else None

    assert wait_until(graph_published, timeout_s=30), api.debug_logs()[-16000:]
    multiple_seeds = {"identities": list(qualified["identities"])}
    expected_paths = [(seed, "note-0")]
    if action == "merge":
        multiple_seeds["identities"].append(
            {"table": destination, "key": "concept/healthy_0"}
        )
        expected_paths += [(seed, "note-1"), ("concept/healthy_0", "note-0")]
    no_dedup = traversal(multiple_seeds, False)
    if no_dedup.status_code == 422:
        # A coordinator cannot retain per-seed paths through its deduplicated
        # frontier. Report that constraint, never reject the incoming direction.
        assert no_dedup.json()["reason"] == "deduplicate_nodes_must_be_true", (
            no_dedup.text
        )
    else:
        # Complete local execution supports this shape. Verify both paths to
        # the shared document survive rather than silently deduplicating them.
        assert no_dedup.ok, no_dedup.text
        nodes = no_dedup.json()["responses"][0]["graph_results"]["incoming"]["nodes"]
        assert sorted(
            (node["path"][0]["key"], node["key"]) for node in nodes
        ) == sorted(expected_paths), nodes

    # Re-resolution must refresh accepted graph projections without a new
    # foreground write or another provider invocation.
    edited = api.post(
        f"/tables/{destination}/entities/{quote('concept/healthy_0', safe='')}:merge",
        {"into": "concept/healthy_1", "wait": {"timeout_ms": 5000}},
    )
    assert edited["complete"], edited
    assert wait_until(
        lambda: api.get(f"/tables/{source}/resolvers/concepts")["progress"]["applied"],
        timeout_s=60,
    ), api.get(f"/tables/{source}/resolvers/concepts")

    def refreshed():
        response = traversal(
            {"identities": [{"table": destination, "key": "concept/healthy_1"}]}
        )
        assert response.ok, response.text
        nodes = response.json()["responses"][0]["graph_results"]["incoming"]["nodes"]
        return {node["key"] for node in nodes} == {"note-0", "note-1"}

    assert wait_until(refreshed, timeout_s=30), api.debug_logs()[-16000:]

    def graph_complete():
        status = api.get(f"/tables/{source}/indexes/knowledge")["status"]
        return status["readiness"]["complete"]

    assert wait_until(graph_complete, timeout_s=30), api.get(
        f"/tables/{source}/indexes/knowledge"
    )
