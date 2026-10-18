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


def test_entity_curation_fanout_wait_and_resolver_routes(stateful_api):
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
    key = quote("concept/alpha", safe="")
    entity = f"/tables/{destination}/entities/{key}"
    wait = {"timeout_ms": 5000}
    merged = api.post(entity + ":merge", {"into": "concept/beta", "wait": wait})
    assert merged["complete"] and merged["applied"], merged
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
    api.post(resolver + "/redirects", {"from": "concept/zeta", "to": "concept/eta"})
    response = api._request(
        "DELETE",
        resolver
        + "/redirects/"
        + quote("concept/zeta", safe="")
        + "?pin_distinct=true",
    )
    decision = api._check(response)
    assert decision["changed"], decision
    assert api.get(resolver + "/redirects")["redirects"] == []
