# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0

"""Public lifecycle, exact schema values and catalog-bound cascade regressions."""

import json

import pytest
import test_auth as auth
from helpers import wait_until
from test_auth import _basic_auth
from test_relational_sessions import _schema

auth_api = auth.auth_api


def _constraint_status(api, table):
    path = f"/tables/{table}/constraints/status"
    # AuthApi exposes raw HTTP through request_raw; the stateful fixture's
    # existing helper is named _request.
    response = (
        api.request_raw("GET", path, timeout=30)
        if hasattr(api, "request_raw")
        else api._request("GET", path)
    )
    if response.status_code == 409 and (
        "constraint schema or ownership changed; refresh and retry" in response.text
    ):
        return None
    response.raise_for_status()
    return response.json()


def _enforced(api, table):
    assert wait_until(
        lambda: (_constraint_status(api, table) or {}).get("state") == "enforced",
        timeout_s=30,
    )


@pytest.mark.parametrize("permissions", ["admin", "both", "parent_only"])
def test_cascade_session_keeps_logical_authorization(auth_api, permissions):
    api = auth_api
    api.s.headers["Authorization"] = _basic_auth("admin", "admin")
    for table, schema in [("parent", _schema()), ("child", _schema("parent"))]:
        api.post(f"/tables/{table}", {"schema": schema})
        _enforced(api, table)
        api.post(f"/tables/{table}/batch", {"inserts": {"row": {"id": 1}}})
    api.post(
        "/auth/v1/users/writer",
        {
            "password": "writer",
            "initial_policies": [
                {"resource": table, "resource_type": "table", "type": mode}
                for table in (
                    ["parent"] if permissions == "parent_only" else ["parent", "child"]
                )
                for mode in ["read", "write"]
            ],
        },
    )
    if permissions != "admin":
        api.s.headers["Authorization"] = _basic_auth("writer", "writer")
    tx = api.post("/transactions/begin", {"sync_level": "write"})["transaction_id"]
    resource = f"/transactions/{tx}"
    savepoint = api.post(resource + "/savepoints", {})["savepoint_id"]

    def stage():
        return api.request_raw(
            "POST",
            resource + "/write",
            json={"table": "parent", "key": "row", "document": {"id": 2}},
            timeout=30,
        )

    response = stage()
    if permissions == "parent_only":
        assert response.status_code == 403, response.text
        # Rejected expansion must not poison the session or grant child access.
        api.get(resource)
        api.post(resource + "/abort", {})
        return
    assert response.status_code == 200, response.text
    api.get(resource)
    api.post(resource + f"/savepoints/{savepoint}/rollback", {})
    response = stage()
    assert response.status_code == 200, response.text
    api.get(resource)
    response = api.request_raw("POST", resource + "/commit", json={}, timeout=30)
    assert response.status_code == 200, response.text
    assert api.lookup_key("parent", "row") == {"id": 2}
    assert api.lookup_key("child", "row") == {"id": 2}


@pytest.mark.parametrize("drop", [False, True])
def test_constraint_retirement_completes_and_survives_restart(stateful_api, drop):
    api = stateful_api
    api.post("/tables/retiring", {"schema": _schema()})
    _enforced(api, "retiring")
    api.post("/tables/retiring/batch", {"inserts": {"row": {"id": 1}}})
    target = _schema()
    target.pop("unique_constraints")
    body = {
        "schema_version": 0,
        **({"drop": True} if drop else {"target_schema": target}),
    }
    response = api._request("POST", "/tables/retiring/constraints/retire", body)
    assert response.status_code == 202, response.text
    if drop:
        # Retirement prepares a durable drop proof; deletion still requires
        # the explicit DELETE authorized by the public lifecycle contract.
        def ready_to_drop():
            status = _constraint_status(api, "retiring") or {}
            return (status.get("retirement") or {}).get("phase") == "ready_to_drop"

        assert wait_until(ready_to_drop, timeout_s=60), api.debug_logs()[-5000:]
        api.restart_server()
        assert ready_to_drop()
        api.delete("/tables/retiring")

    def published():
        response = api._request("GET", "/tables/retiring")
        if drop:
            return response.status_code == 404
        assert response.status_code == 200, response.text
        return response.json()["schema"]["version"] == 1

    assert wait_until(published, timeout_s=60), api.debug_logs()[-5000:]
    api.restart_server()
    assert published()
    if not drop:
        # Claims were drained and policy published, not merely hidden in status.
        api.post("/tables/retiring/batch", {"inserts": {"duplicate": {"id": 1}}})
        assert api.get("/tables/retiring/documents/row") == {"id": 1}
        assert api.get("/tables/retiring/documents/duplicate") == {"id": 1}


@pytest.mark.parametrize(
    "original,replacement,accepted",
    [
        ("1.0", "1.0", True),
        ("1.0", "1", True),
        ("9007199254740993.0", "90071992547409930e-1", True),
        ("9007199254740993.0", "9007199254740992.0", False),
    ],
)
def test_retirement_preserves_exact_schema_numbers(
    stateful_api, original, replacement, accepted
):
    api = stateful_api
    schema = _schema()
    schema["document_schemas"]["row"]["schema"]["properties"]["extra"] = {
        "type": "number",
        "default": "EXACT_DEFAULT",
    }

    def exact(value, token):
        return json.dumps(value).replace('"EXACT_DEFAULT"', token)

    response = api.s.post(
        api.url + "/tables/exact", data=exact({"schema": schema}, original), timeout=30
    )
    assert response.status_code in (200, 201), response.text
    _enforced(api, "exact")
    schema.pop("unique_constraints")
    response = api.s.post(
        api.url + "/tables/exact/constraints/retire",
        data=exact({"schema_version": 0, "target_schema": schema}, replacement),
        timeout=30,
    )
    assert response.status_code == (202 if accepted else 409), response.text
    if not accepted:
        assert api.get("/tables/exact")["schema"]["version"] == 0


def test_check_only_activation_repair_and_retry(stateful_api):
    api = stateful_api
    schema = _schema()
    schema.pop("unique_constraints")
    api.post("/tables/checks", {"schema": schema})
    api.post("/tables/checks/batch", {"inserts": {"old": {"id": -1}}})
    schema["checks"] = [{"name": "positive", "column": "id", "op": "gt", "value": 0}]
    api.put("/tables/checks/schema", schema)
    assert wait_until(
        lambda: (_constraint_status(api, "checks") or {}).get("state") == "invalid",
        timeout_s=30,
    )
    # Repair and retry retain the caller's exact schema fence.
    stale = api._request(
        "POST", "/tables/checks/constraints/retry", {"schema_version": 0}
    )
    assert stale.status_code == 409, stale.text
    rows = api._request("POST", "/tables/checks/rows/query", {"fields": ["id"]})
    assert rows.status_code == 200, rows.text
    old = json.loads(rows.text.strip())
    assert old["row"] == {"id": -1}
    repaired = api._request(
        "POST",
        "/tables/checks/constraints/repair",
        {
            "schema_version": 1,
            "mutations": [
                {"key": "old", "expected_version": old["version"], "row": {"id": 1}}
            ],
            "sync_level": "write",
        },
    )
    assert repaired.status_code in (200, 201), repaired.text
    response = api._request(
        "POST", "/tables/checks/constraints/retry", {"schema_version": 1}
    )
    assert response.status_code == 202, response.text
    _enforced(api, "checks")
    invalid = api._request(
        "POST", "/tables/checks/batch", {"inserts": {"bad": {"id": -1}}}
    )
    assert invalid.status_code in (400, 409), invalid.text
    assert api.get("/tables/checks/documents/old") == {"id": 1}
