"""AutoSchemaKG-style label-routed autograph e2e (see zig/AUTOSCHEMA.md).

One extraction_graph artifact carries entity mentions and event mentions
(label "event"). Two resolvers partition it by label: an `event`-labeled
resolver promotes events into the `events` table, and a catch-all resolver
promotes everything else into `entities`. The catch-all must skip the labels
claimed by its labeled sibling, so extraction labels stay open-vocabulary.

Reuses the multi-node cluster fixture and API helper from test_resolution.
"""

from __future__ import annotations

import json

import pytest
import requests

from test_resolution import (  # noqa: F401  (fixture re-export)
    _Api,
    _Deadline,
    _new_e2e_deadline,
    _transient_poll_error,
    resolution_cluster,
)

# The event key template is the production compositional shape:
# `event/{{ hash _entity.event_identity }}`. The exact key is discovered from
# the live cluster (traversal / table queries) instead of predicted here, so
# the hash path, the extractor-asserted predicate, and the participant
# composition all execute on the cluster. The entity template's `x_` prefix
# makes the canonical key segment differ from the raw mention slug, which
# forces the sibling re-drive to RE-KEY every event onto its canonical
# compositional identity — and the promoter to tombstone the provisional
# document with a merged_into redirect.
EVENT_TEXT = "Ada Lovelace writes the first program"
EVENT_TEXT_REWORDED = "The first program is written by Ada Lovelace"

AUTOSCHEMA_INDEXES = {
    "knowledge_graph": {
        "type": "graph",
        "source": {
            "artifact": "kg_v1",
            "format": "extraction_graph",
            "mention_edge_type": "mentions",
        },
        "artifact": {
            "name": "kg_v1",
            "kind": "asset",
            "source": {"type": "field", "value": "kg"},
            "content_type": "application/json",
        },
        "metrics": {"ppr": {"kind": "pagerank", "enabled": True, "damping": 0.85}},
        "edge_types": [
            {"name": "mentions"},
            {"name": "works_at"},
            {"name": "participates_in"},
            {"name": "because"},
        ],
        "resolvers": [
            {
                # Listed FIRST so its initial pass runs before the sibling
                # entities resolution exists: the provisional raw-slug
                # identity is minted deterministically, then the committed
                # entities resolution re-drives this resolver onto the
                # canonical x_-prefixed segments.
                "name": "events",
                "table": "events",
                "source_artifact": "kg_v1",
                "resolution_artifact": "events_resolution_v1",
                "key_template": "event/{{ hash _entity.event_identity }}",
                "labels": ["event"],
                "config_generation": 1,
            },
            {
                # Catch-all: no `labels`, so it consumes every mention label
                # except those claimed by the labeled sibling above. Entity
                # labels from an LLM extractor are open-vocabulary, so the
                # catch-all must not require enumerating them.
                "name": "kg",
                "table": "entities",
                "source_artifact": "kg_v1",
                "resolution_artifact": "entities_resolution_v1",
                "key_template": "entity/x_{{ slug _entity.text }}",
                "config_generation": 1,
            },
        ],
    },
}

def _kg_document(event_text: str) -> dict:
    return {
        "kg": {
            "entities": [
                {"id": "e0", "label": "person", "text": "Ada Lovelace"},
                {"id": "e1", "label": "org", "text": "Antfly"},
                {
                    "id": "v0",
                    "label": "event",
                    "text": event_text,
                    "predicate": "write",
                },
            ],
            "relations": [
                {"type": "works_at", "source": "e0", "target": "e1"},
                {"type": "participates_in", "source": "e0", "target": "v0"},
            ],
        }
    }


def _wait_for_docs(
    api: _Api, table: str, expected_text: dict[str, str], *, deadline: _Deadline
) -> None:
    """Poll table lookups until every key exists and carries its text."""
    pending = set(expected_text)
    last_error: str | None = None
    while not deadline.expired() and pending:
        for key in list(pending):
            if deadline.remaining() < 0.1:
                break
            try:
                timeout = deadline.request_timeout()
            except AssertionError:
                break
            try:
                doc = api.lookup(table, key, timeout=timeout)
            except requests.RequestException as exc:
                if not _transient_poll_error(exc):
                    raise
                last_error = repr(exc)
                continue
            if doc is not None and expected_text[key] in json.dumps(doc):
                pending.discard(key)
        if pending:
            deadline.sleep(0.25)
    assert not pending, (
        f"documents never promoted into {table}: {sorted(pending)} "
        f"last_error={last_error}\n{api.server.debug_logs()}"
    )


def _lookup_absent(api: _Api, table: str, key: str, *, deadline: _Deadline) -> bool:
    try:
        doc = api.lookup(table, key, timeout=deadline.request_timeout())
    except requests.RequestException as exc:
        response = getattr(exc, "response", None)
        if response is not None and response.status_code == 404:
            return True
        raise
    return doc is None


def test_label_routed_autograph_promotes_events_and_entities(resolution_cluster):
    cluster = resolution_cluster
    api = _Api(cluster.data_api_urls[0], cluster)

    api.create_table("entities", num_shards=1, deadline=_new_e2e_deadline())
    api.create_table("events", num_shards=1, deadline=_new_e2e_deadline())
    # One shard: the seeded fresh graph metric below fails closed on
    # cross-shard tables (personalized scores are not globally comparable);
    # cross-shard promotion itself is covered by test_resolution's
    # multi-node autograph suite.
    api.create_table(
        "documents",
        num_shards=1,
        indexes=json.loads(json.dumps(AUTOSCHEMA_INDEXES)),
        deadline=_new_e2e_deadline(),
    )

    # Two documents describe the SAME event with different wordings: the
    # sentence-hash design would mint two nodes, the compositional identity
    # (sorted participant segments + extractor-asserted predicate through
    # the `hash` helper) must converge them onto one.
    api.insert("documents", "doc:a", _kg_document(EVENT_TEXT), deadline=_new_e2e_deadline())
    api.insert("documents", "doc:b", _kg_document(EVENT_TEXT_REWORDED), deadline=_new_e2e_deadline())

    # Label routing: entity mentions promote into `entities` under the
    # x_-prefixed canonical keys.
    _wait_for_docs(
        api,
        "entities",
        {"entity/x_ada_lovelace": "Ada Lovelace", "entity/x_antfly": "Antfly"},
        deadline=_new_e2e_deadline(),
    )

    # One combined convergence poll (the cluster settles asynchronously and
    # the walk can transiently observe the pre-re-drive provisional key):
    # - a SINGLE depth-2 traversal from each document walks
    #   doc -> entity -> event THROUGH the cross-table entity node (the API
    #   read source proves the single-group snapshot complete; no
    #   re-seeding);
    # - both documents' differently-worded events reach the SAME hash-minted
    #   compositional key;
    # - the events table holds exactly one LIVE event — the provisional
    #   raw-slug-identity document the promoter upserted before the sibling
    #   re-drive is now a merged_into redirect, never an orphan.
    event_key = _wait_for_converged_event(api, deadline=_new_e2e_deadline())
    assert event_key.startswith("event/")
    assert "ada_lovelace" not in event_key, event_key  # hash, not slug

    # Partition, not duplication: the catch-all skipped the event mention.
    absent_deadline = _new_e2e_deadline()
    assert _lookup_absent(api, "entities", event_key, deadline=absent_deadline)

    # Entity-sourced topology: seeding the person pushes teleport mass onto
    # its relation targets — the HippoRAG-style proof that the graph carries
    # entity->entity/event topology instead of doc-mediated edges.
    _wait_for_seeded_mass(
        api,
        seed="entity/x_ada_lovelace",
        expect_positive={"entity/x_antfly", event_key},
        deadline=_new_e2e_deadline(),
    )


def _one_query_walk(api: _Api, start_key: str, timeout: float) -> str | None:
    """One depth-2 traversal reaching entity AND event, no re-seeding.

    Returns the discovered event key (table provenance verified on the
    cross-table nodes so identity stays table-qualified through the local
    executor's complete-snapshot expansion), or None while unsettled.
    """
    payload = {
        "graph_queries": {
            "walk": {
                "index": "knowledge_graph",
                "traverse": {
                    "start": {"keys": [start_key]},
                    "direction": "out",
                    "max_depth": 2,
                    "limit": 20,
                },
            }
        },
        "limit": 1,
    }
    try:
        response = api.query_table("documents", payload, timeout=timeout)
    except requests.RequestException as exc:
        if not _transient_poll_error(exc):
            raise
        return None
    result = (
        (response.get("responses") or [{}])[0]
        .get("graph_results", {})
        .get("walk", {})
    )
    nodes = {node.get("key"): node.get("table") for node in result.get("nodes", [])}
    event_keys = [
        key
        for key, table in nodes.items()
        if key and key.startswith("event/") and table == "events"
    ]
    if (
        nodes.get("entity/x_ada_lovelace") == "entities"
        and nodes.get("entity/x_antfly") == "entities"
        and len(event_keys) == 1
    ):
        return event_keys[0]
    return None


def _live_events(api: _Api, timeout: float) -> tuple[list[str], dict[str, str]] | None:
    """(live keys, tombstone -> merged_into) from the events table."""
    payload = {"query": {"match_all": {}}, "limit": 20}
    try:
        response = api.query_table("events", payload, timeout=timeout)
    except requests.RequestException as exc:
        if not _transient_poll_error(exc):
            raise
        return None
    hits = (response.get("responses") or [{}])[0].get("hits", {}).get("hits", [])
    live: list[str] = []
    redirects: dict[str, str] = {}
    for hit in hits:
        source = hit.get("_source") or {}
        if source.get("merged_into"):
            redirects[hit.get("_id")] = source["merged_into"]
        else:
            live.append(hit.get("_id"))
    return live, redirects


def _wait_for_converged_event(api: _Api, *, deadline: _Deadline) -> str:
    last: tuple | None = None
    while not deadline.expired():
        timeout = deadline.request_timeout()
        walk_a = _one_query_walk(api, "doc:a", timeout)
        walk_b = _one_query_walk(api, "doc:b", timeout) if walk_a else None
        events = _live_events(api, timeout)
        last = (walk_a, walk_b, events)
        if walk_a and walk_a == walk_b and events is not None:
            live, redirects = events
            if live == [walk_a] and all(
                target == walk_a for target in redirects.values()
            ):
                return walk_a
        deadline.sleep(0.5)
    raise AssertionError(
        f"compositional event convergence never settled: {last}\n"
        f"{api.server.debug_logs()}"
    )


def _wait_for_seeded_mass(
    api: _Api,
    *,
    seed: str,
    expect_positive: set[str],
    deadline: _Deadline,
) -> None:
    payload = {
        "query": {"match_all": {}},
        "graph_metric": {
            "index": "knowledge_graph",
            "metric": "ppr",
            "seed_nodes": [seed],
            "damping": 0.9,
            "metric_freshness": "fresh",
            "top_k": 20,
        },
        "limit": 1,
    }
    last: dict | None = None
    while not deadline.expired():
        try:
            response = api.query_table(
                "documents", payload, timeout=deadline.request_timeout()
            )
        except requests.RequestException as exc:
            if not _transient_poll_error(exc):
                raise
            deadline.sleep(0.5)
            continue
        last = response
        scores = (
            (response.get("responses") or [{}])[0]
            .get("graph_metric_results", {})
            .get("ppr", {})
            .get("scores", [])
        )
        positive = {s["node"] for s in scores if s.get("score", 0) > 0}
        if expect_positive <= positive and seed in positive:
            return
        deadline.sleep(0.5)
    raise AssertionError(
        f"seeded PageRank mass never reached relation targets of {seed}: "
        f"expected positive {sorted(expect_positive)}, last {json.dumps(last)[:800]}"
    )


def test_overlapping_labeled_resolvers_rejected_at_admission(resolution_cluster):
    cluster = resolution_cluster
    api = _Api(cluster.data_api_urls[0], cluster)

    indexes = json.loads(json.dumps(AUTOSCHEMA_INDEXES))
    graph = indexes["knowledge_graph"]
    # Second labeled resolver claiming the same label must be rejected: the
    # partition would be ambiguous.
    graph["resolvers"][1]["labels"] = ["event"]

    api.create_table("entities", num_shards=1, deadline=_new_e2e_deadline())
    api.create_table("events", num_shards=1, deadline=_new_e2e_deadline())
    with pytest.raises(requests.HTTPError):
        api.create_table(
            "documents",
            num_shards=1,
            indexes=indexes,
            deadline=_new_e2e_deadline(),
        )
