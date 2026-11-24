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

"""Context sources with authentication and row policies.

Context reads run in the background as the principal that declared the
enrichment, so the declarer's row filter on each source table must narrow
what generated artifacts can contain. Dependency and impact reads expose
lineage, so they honor the caller's table permissions and row policies.
"""

from __future__ import annotations

import pytest
from test_auth import (  # noqa: F401 - pytest fixtures
    AUTH_BOOTSTRAP_PASSWORD,
    AuthApi,
    _basic_auth,
    _wait_until,
    stateful_auth_api,
)
from test_context_sources import (  # noqa: F401 - pytest fixtures
    _enrichment,
    _statement,
    echo_generator,
)

NUM_SHARDS = 3


def _as(api: AuthApi, user: str, password: str) -> None:
    api.s.headers["Authorization"] = _basic_auth(user, password)


def _create_user(api: AuthApi, name: str, policies: list[tuple[str, ...]]) -> None:
    api.post(
        f"/auth/v1/users/{name}",
        {
            "password": name,
            "initial_policies": [
                {
                    "resource": policy[0],
                    "resource_type": policy[2] if len(policy) > 2 else "table",
                    "type": policy[1],
                }
                for policy in policies
            ],
        },
    )


@pytest.mark.e2e_resource("antfly_process")
def test_context_sources_honor_auth_and_row_policies(
    stateful_auth_api: AuthApi, echo_generator
):
    api = stateful_auth_api
    _as(api, "admin", AUTH_BOOTSTRAP_PASSWORD)
    api.create_table("orgs", {"num_shards": NUM_SHARDS})
    api.create_table("people", {"num_shards": NUM_SHARDS})
    api.batch_write(
        "orgs",
        {
            "inserts": {
                "org:gold": {"org": "acme", "tier": "gold", "title": "Acme Gold"},
                "org:silver": {"org": "acme", "tier": "silver", "title": "Acme Silver"},
            },
            "sync_level": "full_index",
        },
    )

    # The declarer may administer `people` and read `orgs`, but only the
    # gold tier of it.
    _create_user(
        api, "writer", [("people", "admin"), ("orgs", "read"), ("org_graph", "read", "graph")]
    )
    api.put("/auth/v1/users/writer/row-filters/orgs", {"term": {"tier": "gold"}})
    # A reader that sees every org but no person.
    _create_user(api, "auditor", [("people", "read"), ("orgs", "read")])
    api.put("/auth/v1/users/auditor/row-filters/people", {"term": {"org": "nobody"}})

    peers = {
        "peers": {
            "query": {
                "table": "orgs",
                "filter_query": {"field": "org", "term": {"$source": "/org"}},
            },
            "fields": ["title"],
            "max_documents": 10,
        }
    }

    # Declaring as the row-filtered user is admitted (it used to answer 403
    # whenever auth was on) and reports the live source.
    _as(api, "writer", "writer")
    registered = api.put(
        "/tables/people/artifacts/brief/enrichment",
        _enrichment(echo_generator, peers),
    )
    assert registered["context_sources"]["peers"] == {
        "kind": "query",
        "tracking": "live",
    }, registered

    # A document source reads one row by key and cannot carry the filter:
    # refused while the declarer has a row filter on that table.
    refused = api.request_raw(
        "PUT",
        "/tables/people/artifacts/org_card/enrichment",
        json=_enrichment(
            echo_generator,
            {"org": {"document": {"table": "orgs", "key_from": "/org_key"}, "fields": ["title"]}},
        ),
    )
    assert refused.status_code == 403, refused.text

    # Background rebuilds read as the declarer: the silver org never reaches
    # the artifact, though the system itself could read it.
    _as(api, "admin", AUTH_BOOTSTRAP_PASSWORD)
    api.batch_write(
        "people",
        {
            "inserts": {"person:1": {"org": "acme", "body": "person one"}},
            "sync_level": "enrichments",
        },
    )

    def gold_brief():
        statement = _statement(api, "people", "person:1", "brief")
        return statement if statement and "Acme Gold" in statement else None

    brief = _wait_until(gold_brief, timeout=60)
    assert brief is not None, _statement(api, "people", "person:1", "brief")
    assert "Acme Silver" not in brief, brief
    assert not echo_generator.errors, echo_generator.errors
    assert not any("Acme Silver" in value for value in echo_generator.inputs)

    # A Cypher source runs as its declarer too: graph execution applies the
    # writer's row policy, so the silver org is never a member.
    api.post(
        "/graphs/org_graph",
        {"labels": [{"name": "Org", "table": "orgs"}], "relationships": []},
    )
    _as(api, "writer", "writer")
    registered = api.put(
        "/tables/people/artifacts/org_card/enrichment",
        _enrichment(
            echo_generator,
            {
                "orgs": {
                    "query": {
                        "table": "orgs",
                        "cypher": {
                            "graph": "org_graph",
                            "query": "MATCH (o:Org) WHERE o.org = $org RETURN o",
                            "parameters": {"org": {"$source": "/org"}},
                        },
                    },
                    "fields": ["title"],
                }
            },
        ),
    )
    assert registered["context_sources"]["orgs"] == {
        "kind": "query",
        "tracking": "snapshot",
        "reason": "cypher",
    }, registered
    _as(api, "admin", AUTH_BOOTSTRAP_PASSWORD)
    api.batch_write(
        "people",
        {
            "inserts": {"person:2": {"org": "acme", "body": "person two"}},
            "sync_level": "enrichments",
        },
    )

    def gold_card():
        statement = _statement(api, "people", "person:2", "org_card")
        return statement if statement and "Acme Gold" in statement else None

    card = _wait_until(gold_card, timeout=60)
    assert card is not None, _statement(api, "people", "person:2", "org_card")
    assert "Acme Silver" not in card, card
    assert not echo_generator.errors, echo_generator.errors

    # So does a SQL source: the writer's row policy on `orgs` applies to
    # the statement, so the silver org's `_id` is never returned.
    _as(api, "writer", "writer")
    registered = api.put(
        "/tables/people/artifacts/org_rows/enrichment",
        _enrichment(
            echo_generator,
            {
                "orgs": {
                    "query": {
                        "table": "orgs",
                        "sql": {"statement": "SELECT _id FROM orgs ORDER BY _id"},
                    },
                    "fields": ["title"],
                }
            },
        ),
    )
    assert registered["context_sources"]["orgs"]["reason"] == "sql", registered
    _as(api, "admin", AUTH_BOOTSTRAP_PASSWORD)
    api.batch_write(
        "people",
        {
            "inserts": {"person:3": {"org": "acme", "body": "person three"}},
            "sync_level": "enrichments",
        },
    )

    def gold_rows():
        statement = _statement(api, "people", "person:3", "org_rows")
        return statement if statement and "Acme Gold" in statement else None

    rows = _wait_until(gold_rows, timeout=60)
    assert rows is not None, _statement(api, "people", "person:3", "org_rows")
    assert "Acme Silver" not in rows, rows
    assert not echo_generator.errors, echo_generator.errors

    # Dependency state and impact are readable under auth.
    _as(api, "writer", "writer")
    def current_deps():
        response = api.request_raw(
            "GET",
            "/tables/people/documents/person%3A1/artifacts/brief/dependencies"
            "?freshness=require_current&wait_timeout_ms=5000",
        )
        assert response.status_code in (200, 409), response.text
        return response.json() if response.status_code == 200 else None

    deps = _wait_until(current_deps, timeout=60)
    assert deps is not None and deps["current"] is True, deps
    impact = api.post("/tables/orgs/dependencies/impact", {"key": "org:gold", "limit": 20})
    assert any(
        row["table"] == "people" and row["key"] == "person:1" for row in impact["dependents"]
    ), impact
    # The writer cannot see the silver org, so its lineage is not found.
    hidden_input = api.request_raw(
        "POST", "/tables/orgs/dependencies/impact", json={"key": "org:silver", "limit": 20}
    )
    assert hidden_input.status_code == 404, hidden_input.text

    # A caller whose row policies hide every person gets no dependent rows,
    # and the answer says it is incomplete.
    _as(api, "auditor", "auditor")
    filtered = api.post("/tables/orgs/dependencies/impact", {"key": "org:gold", "limit": 20})
    assert filtered["dependents"] == [], filtered
    assert filtered["complete"] is False, filtered
    hidden_dependent = api.request_raw(
        "GET", "/tables/people/documents/person%3A1/artifacts/brief/dependencies"
    )
    assert hidden_dependent.status_code == 404, hidden_dependent.text

    # Without credentials nothing is readable.
    api.s.headers.pop("Authorization", None)
    anonymous = api.request_raw(
        "GET", "/tables/people/documents/person%3A1/artifacts/brief/dependencies"
    )
    assert anonymous.status_code == 401, anonymous.text
