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


def test_resolver_curation_round_trip(aflite_path):
    with antfly_embedded.create(aflite_path) as db:
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
        accepted = db.curate_entity("default", "concept/a", {"action": "merge", "into": "concept/b"})
        assert accepted["results"][0]["revision"] == 1
        assert db.render_resolver_key("concepts", {"text": "A", "label": "concept"})["key"] == "concept/b"
        db.unmerge_resolver_key("concepts", {"from": "concept/a", "pin_distinct": True})
        db.suppress_resolver_keys("concepts", {"add": {"keys": ["concept/noise"]}})
        assert db.render_resolver_key("concepts", {"text": "Noise", "label": "concept"})["suppressed"]
        db.unsuppress_resolver_key("concepts", {"key": "concept/noise"})
        assert db.resolver_reviews({"operation": "reviews", "resolver": "concepts"})["reviews"] == []
        assert db.resolver_curation("concepts")["curation_revision"] == 4
        assert db.pending_work_stats()["resolver_curation"]["resolvers"][0]["curation_revision"] == 4
    with antfly_embedded.open(aflite_path) as db:
        assert db.resolver_curation("concepts")["curation_revision"] == 4
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
