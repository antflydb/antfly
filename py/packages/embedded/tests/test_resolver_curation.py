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

"""Public bindings exercise curation against the C ABI, including reopen."""

import pytest

import antfly_embedded

pytestmark = pytest.mark.usefixtures("require_native")


def _add_curation_graph(db):
    db.add_index(
        {
            "name": "knowledge",
            "type": "graph",
            "source": {"artifact": "kg", "format": "extraction_graph", "mention_edge_type": "mentions"},
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
                    "table": "default",
                    "promote": False,
                    "source_artifact": "kg",
                    "resolution_artifact": "resolved",
                    "key_template": "concept/{{uslug _entity.text}}",
                }
            ],
        }
    )


def test_resolver_curation_round_trip(aflite_path):
    with antfly_embedded.create(aflite_path) as db:
        _add_curation_graph(db)
        db.merge_resolver_keys("concepts", "concept/a", "concept/b")
        assert db.resolver_curation("concepts")["curation_revision"] == 1
        assert db.render_resolver_key("concepts", {"text": "A", "label": "concept"})["key"] == "concept/b"
        db.unmerge_resolver_key("concepts", {"from": "concept/a", "pin_distinct": True})
        db.suppress_resolver_keys("concepts", {"add": {"keys": ["concept/noise"], "texts": ["private source text"]}})
        scoped = db.curate_entity("default", "concept/noise", {"action": "redirects"})["results"][0]
        assert scoped["canonical_key"] == "concept/noise"
        assert scoped["curation"]["suppress"]["keys"] == ["concept/noise"]
        assert scoped["curation"]["suppress"]["texts"] == []
        assert scoped["curation"]["distinct"] == []
        assert "private source text" not in str(scoped)
        assert db.render_resolver_key("concepts", {"text": "Noise", "label": "concept"})["suppressed"]
        db.unsuppress_resolver_key("concepts", {"key": "concept/noise"})
        assert db.resolver_reviews({"operation": "reviews", "resolver": "concepts"})["reviews"] == []
        assert db.resolver_curation("concepts")["curation_revision"] == 4
        assert db.pending_work_stats()["resolver_curation"]["resolvers"][0]["curation_revision"] == 4
    with antfly_embedded.open(aflite_path) as db:
        assert db.resolver_curation("concepts")["curation_revision"] == 4
        assert not db.render_resolver_key("concepts", {"text": "Noise", "label": "concept"})["suppressed"]


def test_namespace_curation_retains_one_authority_across_reopen(aflite_path):
    with antfly_embedded.create(aflite_path) as db:
        _add_curation_graph(db)
        accepted = db.curate_entity("default", "concept/a", {"action": "merge", "into": "concept/b"})
        assert accepted["results"][0]["revision"] == 1
        assert db.render_resolver_key("concepts", {"text": "A", "label": "concept"})["key"] == "concept/b"
        # Namespace adoption gives every consumer one durable authority. A
        # resolver-scoped edit must not create a second, divergent policy.
        for edit in (
            lambda: db.unmerge_resolver_key("concepts", {"from": "concept/a"}),
            lambda: db.suppress_resolver_keys("concepts", {"add": {"keys": ["concept/noise"]}}),
            lambda: db.unsuppress_resolver_key("concepts", {"key": "concept/noise"}),
        ):
            with pytest.raises(antfly_embedded.InvalidArgumentError, match="ResolverNamespaceCurationRequired"):
                edit()
        for revision, key, action in (
            (2, "concept/a", {"action": "unmerge", "pin_distinct": True}),
            (3, "concept/noise", {"action": "suppress"}),
            (4, "concept/noise", {"action": "unsuppress"}),
        ):
            result = db.curate_entity("default", key, action)
            assert result["results"][0]["revision"] == revision
        db.run_until_idle()
        assert db.resolver_curation("concepts")["curation_revision"] == 4
    with antfly_embedded.open(aflite_path) as db:
        assert db.resolver_curation("concepts")["curation_revision"] == 4
        assert db.render_resolver_key("concepts", {"text": "A", "label": "concept"})["key"] == "concept/a"
        assert not db.render_resolver_key("concepts", {"text": "Noise", "label": "concept"})["suppressed"]


def test_suppression_replays_mention_and_relation_edges(aflite_path):
    options = antfly_embedded.OpenOptions(generated_enrichment_replay=True)
    with antfly_embedded.create_with_options(aflite_path, options) as db:
        db.add_index(
            {
                "name": "knowledge",
                "type": "graph",
                "source": {"artifact": "kg", "format": "extraction_graph", "mention_edge_type": "mentions"},
                "artifact": {
                    "name": "kg",
                    "kind": "asset",
                    "source": {"type": "field", "value": "kg"},
                    "content_type": "application/json",
                },
                "edge_types": [{"name": "mentions"}, {"name": "uses"}],
                "resolvers": [
                    {
                        "name": "concepts",
                        "table": "default",
                        "promote": False,
                        "source_artifact": "kg",
                        "resolution_artifact": "resolved",
                        "key_template": "concept/{{uslug _entity.text}}",
                    }
                ],
            }
        )
        db.batch_json(
            {
                "inserts": {
                    "source": {
                        "kg": {
                            "entities": [
                                {"id": "a", "label": "concept", "text": "Alpha"},
                                {"id": "b", "label": "concept", "text": "Beta"},
                            ],
                            "relations": [{"type": "uses", "source": "a", "target": "b"}],
                        }
                    }
                }
            }
        )
        db.run_until_idle()
        assert len(db.edges("knowledge", "source", "mentions", 0)) == 2
        assert len(db.edges("knowledge", "concept/alpha", "uses", 0)) == 1
        db.suppress_resolver_keys("concepts", {"add": {"keys": ["concept/alpha"]}})
        db.run_until_idle()
        assert len(db.edges("knowledge", "source", "mentions", 0)) == 1
        assert db.edges("knowledge", "concept/alpha", "uses", 0) == []
        db.unsuppress_resolver_key("concepts", {"key": "concept/alpha"})
        db.run_until_idle()
        assert len(db.edges("knowledge", "source", "mentions", 0)) == 2
        assert len(db.edges("knowledge", "concept/alpha", "uses", 0)) == 1


def test_reviewed_endpoints_follow_merges_and_unmerges(aflite_path):
    import base64

    options = antfly_embedded.OpenOptions(generated_enrichment_replay=True)
    with antfly_embedded.create_with_options(aflite_path, options) as db:
        db.add_index(
            {
                "name": "knowledge",
                "type": "graph",
                "source": {"artifact": "kg", "format": "extraction_graph", "mention_edge_type": "mentions"},
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
                        "table": "default",
                        "promote": False,
                        "source_artifact": "kg",
                        "resolution_artifact": "resolved",
                        "key_template": "concept/{{uslug _entity.text}}",
                        "candidate_search": "prefix",
                        "scorer": {
                            "comparisons": [],
                            "combine": {"bias": 1},
                            "decision": {"match": 0.95, "review": 0.6},
                        },
                    }
                ],
            }
        )
        db.batch_json(
            {
                "inserts": {
                    "concept/a": {"entity_type": "concept", "canonical_name": "A"},
                    "concept/b": {"entity_type": "concept", "canonical_name": "B"},
                    "source": {"kg": {"entities": [{"id": "c", "text": "C", "label": "concept"}]}},
                }
            }
        )
        db.run_until_idle()
        review = db.resolver_reviews({"operation": "reviews", "resolver": "concepts"})["reviews"][0]
        db.resolver_reviews(
            {"operation": "review", "resolver": "concepts", "id": review["id"], "decision": "match", "key": "concept/a"}
        )
        db.run_until_idle()

        def target():
            edges = db.edges("knowledge", "source", "mentions", 0)
            assert len(edges) == 1
            return base64.b64decode(edges[0]["target_b64"]).decode()

        assert target() == "concept/a"
        db.merge_resolver_keys("concepts", "concept/a", "concept/b")
        db.run_until_idle()
        assert target() == "concept/b"
        # Keep the curator's original identity durable rather than replacing
        # its receipt with B; undoing the merge must restore its original link.
        db.unmerge_resolver_key("concepts", {"from": "concept/a"})
        db.run_until_idle()
        assert target() == "concept/a"
