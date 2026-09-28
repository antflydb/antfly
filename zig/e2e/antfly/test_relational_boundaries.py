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

# https://www.antfly.io/licensing/ELv2-license

"""Exact typed values and least-privilege integrity across public boundaries."""

import json
from decimal import Decimal

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


@pytest.mark.parametrize("with_fk", [False, True])
def test_relational_fk_binding_preserves_exact_schema_defaults(auth_api, with_fk):
    api = auth_api
    api.s.headers["Authorization"] = auth._basic_auth(
        "admin", auth.AUTH_BOOTSTRAP_PASSWORD
    )
    api.post("/tables/parents", {"schema": _schema()})
    _ready(api, "parents")
    for case, spelling in enumerate(
        ["9007199254740993.0", "9223372036854775807e0", "-9223372036854775808.0"]
    ):
        schema = _schema("parents") if with_fk else _schema()
        schema["document_schemas"]["row"]["schema"]["properties"]["extra"] = {
            "type": "integer",
            "default": "EXACT_NUMBER",
        }
        body = json.dumps({"schema": schema}).replace('"EXACT_NUMBER"', spelling)
        response = api.request_raw(
            "POST", f"/tables/children_{case}", data=body, timeout=30
        )
        assert response.status_code in (200, 201), response.text
        _ready(api, f"children_{case}")
        response = api.request_raw("GET", f"/tables/children_{case}", timeout=30)
        assert response.status_code == 200, response.text
        stored = json.loads(response.text, parse_float=Decimal)
        default = stored["schema"]["document_schemas"]["row"]["schema"]["properties"][
            "extra"
        ]["default"]
        assert Decimal(str(default)) == Decimal(spelling), response.text


@pytest.mark.parametrize("coordinated", [False, True])
def test_relational_exact_numbers_across_mutation_and_session(auth_api, coordinated):
    api = auth_api
    api.s.headers["Authorization"] = auth._basic_auth(
        "admin", auth.AUTH_BOOTSTRAP_PASSWORD
    )
    for case, (spelling, expected) in enumerate(
        [
            ("9007199254740993.0", 9007199254740993),
            ("9223372036854775807e0", 9223372036854775807),
            ("-9223372036854775808.0", -9223372036854775808),
        ]
    ):
        table = f"rows_{case}"
        schema = _schema()
        if not coordinated:
            schema.pop("unique_constraints")
        api.post(f"/tables/{table}", {"schema": schema})
        if coordinated:
            _ready(api, table)
        response = api.request_raw(
            "POST",
            f"/tables/{table}/rows/mutate",
            data='{"schema_version":0,"mutations":[{"key":"row","expected_version":"0","row":{"id":'
            + spelling
            + "}}]}",
            timeout=30,
        )
        assert response.status_code in (200, 201), response.text
        assert api.lookup_key(table, "row") == {"id": expected}
        projection = api.request_raw(
            "POST", f"/tables/{table}/rows/query", json={"fields": ["id"]}, timeout=30
        )
        assert projection.status_code == 200, projection.text
        assert json.loads(projection.text)["row"] == {"id": expected}
        tx = api.post("/transactions/begin", {"sync_level": "write"})["transaction_id"]
        response = api.request_raw(
            "POST",
            f"/transactions/{tx}/write",
            data=f'{{"table":"{table}","key":"row","document":{{"id":'
            + spelling
            + "}}",
            timeout=30,
        )
        assert response.status_code == 200, response.text
        response = api.request_raw(
            "POST", f"/transactions/{tx}/commit", json={}, timeout=30
        )
        assert response.status_code == 200, response.text
        assert api.lookup_key(table, "row") == {"id": expected}


@pytest.mark.parametrize("child_permission", [None, "read", "write"])
@pytest.mark.parametrize("action", ["no_action", "cascade"])
def test_relational_session_authorizes_effects_not_private_witnesses(
    auth_api, child_permission, action
):
    api = auth_api
    api.s.headers["Authorization"] = auth._basic_auth(
        "admin", auth.AUTH_BOOTSTRAP_PASSWORD
    )
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
        api.s.headers["Authorization"] = auth._basic_auth(
            "admin", auth.AUTH_BOOTSTRAP_PASSWORD
        )
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
    api.s.headers["Authorization"] = auth._basic_auth(
        "admin", auth.AUTH_BOOTSTRAP_PASSWORD
    )
    assert api.lookup_key("parents", "replacement") == {"id": 1}
    if action == "no_action":
        assert api.lookup_key("children", "row") == {"id": 1}
