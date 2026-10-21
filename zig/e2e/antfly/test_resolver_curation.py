# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""Resolver curation admission, owner progress, and namespace routes over HTTP."""

from urllib.parse import quote
from uuid import uuid4

import pytest


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

        def pending_review():
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
        assert accepted["applied"], accepted
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
