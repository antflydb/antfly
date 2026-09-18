# Copyright 2026 Antfly, Inc.
# Licensed under the Elastic License 2.0 (ELv2).
# https://www.antfly.io/licensing/ELv2-license

"""Exact typed values and least-privilege integrity across public boundaries."""

import json

import pytest

import test_auth as auth
from helpers import wait_until
from test_relational_sessions import _schema

auth_api = auth.auth_api


def _ready(api, table):
    assert wait_until(
        lambda: (
            api.get(f"/tables/{table}/constraints/status").get("state") == "enforced"
        ),
        timeout_s=30,
    )


@pytest.mark.parametrize("coordinated", [False, True])
@pytest.mark.parametrize(
    ("spelling", "expected"),
    [
        ("9007199254740993.0", 9007199254740993),
        ("9223372036854775807e0", 9223372036854775807),
        ("-9223372036854775808.0", -9223372036854775808),
    ],
)
def test_relational_exact_numbers_across_mutation_and_session(
    auth_api, coordinated, spelling, expected
):
    api = auth_api
    api.s.headers["Authorization"] = auth._basic_auth("admin", "admin")
    schema = _schema()
    if not coordinated:
        schema.pop("unique_constraints")
    api.post("/tables/rows", {"schema": schema})
    if coordinated:
        _ready(api, "rows")
    response = api.request_raw(
        "POST",
        "/tables/rows/rows/mutate",
        data='{"schema_version":0,"mutations":[{"key":"row",'
        '"expected_version":"0","row":{"id":' + spelling + "}}]}",
        timeout=30,
    )
    assert response.status_code in (200, 201), response.text
    assert api.lookup_key("rows", "row") == {"id": expected}
    projection = api.request_raw(
        "POST", "/tables/rows/rows/query", json={"fields": ["id"]}, timeout=30
    )
    assert projection.status_code == 200, projection.text
    assert json.loads(projection.text)["row"] == {"id": expected}

    tx = api.post("/transactions/begin", {"sync_level": "write"})["transaction_id"]
    response = api.request_raw(
        "POST",
        f"/transactions/{tx}/write",
        data='{"table":"rows","key":"row","document":{"id":' + spelling + "}}",
        timeout=30,
    )
    assert response.status_code == 200, response.text
    response = api.request_raw(
        "POST", f"/transactions/{tx}/commit", json={}, timeout=30
    )
    assert response.status_code == 200, response.text
    assert api.lookup_key("rows", "row") == {"id": expected}


@pytest.mark.parametrize("child_permission", [None, "read", "write"])
@pytest.mark.parametrize("action", ["no_action", "cascade"])
def test_relational_session_authorizes_effects_not_private_witnesses(
    auth_api, child_permission, action
):
    api = auth_api
    api.s.headers["Authorization"] = auth._basic_auth("admin", "admin")
    child = _schema("parents")
    child["foreign_keys"][0].update(on_delete=action, on_update=action)
    for table, schema in [("parents", _schema()), ("children", child)]:
        api.post("/tables/" + table, {"schema": schema})
        _ready(api, table)
        api.post("/tables/" + table + "/batch", {"inserts": {"row": {"id": 1}}})
    policies = [
        {"resource": "parents", "resource_type": "table", "type": "read"},
        {"resource": "parents", "resource_type": "table", "type": "write"},
    ]
    if child_permission:
        policies.append(
            {"resource": "children", "resource_type": "table", "type": child_permission}
        )
    api.post(
        "/auth/v1/users/writer", {"password": "writer", "initial_policies": policies}
    )
    api.s.headers["Authorization"] = auth._basic_auth("writer", "writer")
    tx = api.post("/transactions/begin", {"sync_level": "write"})["transaction_id"]
    response = api.request_raw(
        "POST",
        f"/transactions/{tx}/stage",
        json={
            "read_set": [],
            "tables": {
                "parents": {"deletes": ["row"], "inserts": {"replacement": {"id": 1}}}
            },
        },
        timeout=30,
    )
    if action == "cascade" and child_permission != "write":
        assert response.status_code == 403, response.text
        api.s.headers["Authorization"] = auth._basic_auth("admin", "admin")
        assert api.lookup_key("parents", "row") == {"id": 1}
        assert api.lookup_key("children", "row") == {"id": 1}
        return
    assert response.status_code == 200, response.text
    details = api.request_raw("GET", f"/transactions/{tx}", timeout=30)
    assert details.status_code == 200, details.text
    response = api.request_raw(
        "POST", f"/transactions/{tx}/commit", json={}, timeout=30
    )
    assert response.status_code == 200, response.text
    api.s.headers["Authorization"] = auth._basic_auth("admin", "admin")
    assert api.lookup_key("parents", "replacement") == {"id": 1}
    if action == "no_action":
        assert api.lookup_key("children", "row") == {"id": 1}
