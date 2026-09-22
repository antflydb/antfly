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

# The event key template uses `slug` (not `hash`) so the expected canonical
# key is computable here without reimplementing xxhash64; the `hash` helper
# has its own unit coverage in lib/resolver.
EVENT_TEXT = "Ada Lovelace writes the first program"
EVENT_KEY = "event/ada_lovelace_writes_the_first_program"

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
                "name": "events",
                "table": "events",
                "source_artifact": "kg_v1",
                "resolution_artifact": "events_resolution_v1",
                "key_template": "event/{{ slug _entity.text }}",
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
                "key_template": "entity/{{ slug _entity.text }}",
                "config_generation": 1,
            },
        ],
    },
}

KG_DOCUMENT = {
    "kg": {
        "entities": [
            {"id": "e0", "label": "person", "text": "Ada Lovelace"},
            {"id": "e1", "label": "org", "text": "Antfly"},
            {"id": "v0", "label": "event", "text": EVENT_TEXT},
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

    api.insert("documents", "doc:a", KG_DOCUMENT, deadline=_new_e2e_deadline())

    # Label routing: entity mentions promote into `entities`, the event
    # mention promotes into `events` under its slug-minted canonical key.
    _wait_for_docs(
        api,
        "entities",
        {"entity/ada_lovelace": "Ada Lovelace", "entity/antfly": "Antfly"},
        deadline=_new_e2e_deadline(),
    )
    _wait_for_docs(
        api,
        "events",
        {EVENT_KEY: EVENT_TEXT},
        deadline=_new_e2e_deadline(),
    )

    # Partition, not duplication: the catch-all skipped the event mention, and
    # the event resolver skipped the entity mentions. (The catch-all would have
    # minted the event under `event/...` in `entities`; the event resolver
    # would have minted `event/ada_lovelace` from the person mention.)
    absent_deadline = _new_e2e_deadline()
    assert _lookup_absent(api, "entities", EVENT_KEY, deadline=absent_deadline)
    assert _lookup_absent(api, "events", "event/ada_lovelace", deadline=absent_deadline)

    # Traversal walk across the resolved topology: hop 1 from the document
    # reaches the canonical entity as a cross-table terminal; re-seeding the
    # traversal at that entity key (a seed always expands) walks the
    # entity-sourced relation edges to the org and the event. This is the
    # doc -> entity -> entity/event chain the reviewer asked to see walked,
    # done the way the distributed executor does it: one bounded hop per
    # frontier, re-seeded at each cross-table node.
    _wait_for_entity_walk(api, deadline=_new_e2e_deadline())

    # Entity-sourced topology: once resolution lands, the works_at and
    # participates_in relations re-render with the resolver-minted canonical
    # keys as their topological source. Query-seeded personalized PageRank
    # reads the raw edge snapshot, so seeding the person must push teleport
    # mass onto its relation targets — the HippoRAG-style proof that the
    # graph carries entity->entity/event topology instead of doc-mediated
    # edges. (Traversal EXPANSION from a cross-table entity node remains the
    # GRAPH.md-deferred entity node model; PageRank does not wait for it.)
    _wait_for_seeded_mass(
        api,
        seed="entity/ada_lovelace",
        expect_positive={"entity/antfly", EVENT_KEY},
        deadline=_new_e2e_deadline(),
    )


def _wait_for_entity_walk(api: _Api, *, deadline: _Deadline) -> None:
    def traverse_nodes(start_key: str) -> set[str] | None:
        payload = {
            "graph_queries": {
                "walk": {
                    "index": "knowledge_graph",
                    "traverse": {
                        "start": {"keys": [start_key]},
                        "direction": "both",
                        "max_depth": 1,
                        "limit": 20,
                    },
                }
            },
            "limit": 1,
        }
        try:
            response = api.query_table(
                "documents", payload, timeout=deadline.request_timeout()
            )
        except requests.RequestException as exc:
            if not _transient_poll_error(exc):
                raise
            return None
        result = (
            (response.get("responses") or [{}])[0]
            .get("graph_results", {})
            .get("walk", {})
        )
        return {node.get("key") for node in result.get("nodes", [])}

    last: tuple[set[str] | None, set[str] | None] | None = None
    while not deadline.expired():
        from_doc = traverse_nodes("doc:a")
        from_entity = (
            traverse_nodes("entity/ada_lovelace")
            if from_doc and "entity/ada_lovelace" in from_doc
            else None
        )
        last = (from_doc, from_entity)
        if from_entity and {"entity/antfly", EVENT_KEY} <= from_entity:
            return
        deadline.sleep(0.5)
    raise AssertionError(
        f"traversal walk doc -> entity -> relation targets never completed: {last}\n"
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
