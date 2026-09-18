# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0

"""Current catalog authorization and typed cascade assignment regressions."""

import pytest
import test_auth as auth
from helpers import wait_until
from test_relational_sessions import _schema

auth_api = auth.auth_api


@pytest.mark.parametrize("mixed_spelling", [False, True])
def test_equivalent_cascades(auth_api, mixed_spelling):
    api = auth_api
    api.s.headers["Authorization"] = auth._basic_auth("admin", "admin")
    for table in ["p1", "p2", "child"]:
        schema = _schema()
        schema["document_schemas"]["row"]["schema"]["properties"]["id"] = {
            "type": "number"
        }
        if table == "child":
            schema.pop("unique_constraints")
            schema["foreign_keys"] = [
                {
                    "name": "fk_" + parent,
                    "child_columns": ["id"],
                    "parent_table": parent,
                    "parent_columns": ["id"],
                    "on_update": "cascade",
                }
                for parent in ["p1", "p2"]
            ]
        api.post("/tables/" + table, {"schema": schema})
        assert wait_until(
            lambda table=table: (
                api.get("/tables/" + table + "/constraints/status").get("state")
                == "enforced"
            ),
            timeout_s=30,
        )
        api.post("/tables/" + table + "/batch", {"inserts": {"row": {"id": 0}}})
    tx = api.post("/transactions/begin", {"sync_level": "write"})["transaction_id"]
    result = api.request_raw(
        "POST",
        "/transactions/" + tx + "/stage",
        json={
            "read_set": [],
            "tables": {
                "p1": {"inserts": {"row": {"id": 1}}},
                "p2": {"inserts": {"row": {"id": 1.0 if mixed_spelling else 1}}},
            },
        },
        timeout=30,
    )
    assert result.status_code == 200, result.text
    result = api.request_raw(
        "POST", "/transactions/" + tx + "/commit", json={}, timeout=30
    )
    assert result.status_code == 200, result.text
    assert api.lookup_key("child", "row")["id"] == 1


@pytest.mark.parametrize("authorized_after_rename", [False, True])
def test_session_rechecks_renamed_resource_authority(auth_api, authorized_after_rename):
    api = auth_api
    admin = auth._basic_auth("admin", "admin")
    writer = auth._basic_auth("writer", "writer")
    api.s.headers["Authorization"] = admin
    api.create_table("public_rows")
    api.post(
        "/auth/v1/users/writer",
        {
            "password": "writer",
            "initial_policies": [
                {"resource": resource, "resource_type": "table", "type": mode}
                for resource in (
                    ["public_rows", "private_rows"]
                    if authorized_after_rename
                    else ["public_rows"]
                )
                for mode in ["read", "write"]
            ],
        },
    )
    api.s.headers["Authorization"] = writer
    tx = api.post("/transactions/begin", {"sync_level": "write"})["transaction_id"]
    api.post(
        "/transactions/" + tx + "/write",
        {"table": "public_rows", "key": "secret", "document": {"written": "by_writer"}},
    )
    api.s.headers["Authorization"] = admin
    api.post(
        "/databases/default/namespaces/public/tables/public_rows/rename",
        {"name": "private_rows"},
    )
    api.create_table("public_rows")
    api.s.headers["Authorization"] = writer
    denied = api.request_raw(
        "POST",
        "/tables/private_rows/batch",
        json={"inserts": {"control": {"x": 1}}},
        timeout=30,
    )
    assert denied.status_code in ((200, 201) if authorized_after_rename else (403,)), (
        denied.text
    )
    committed = api.request_raw(
        "POST", "/transactions/" + tx + "/commit", json={}, timeout=30
    )
    api.s.headers["Authorization"] = admin
    found = api.request_raw("GET", "/tables/private_rows/documents/secret", timeout=30)
    assert committed.status_code in (
        (200,) if authorized_after_rename else (403, 404, 409)
    ), (
        committed.status_code,
        committed.text,
        found.status_code,
        found.text,
    )
    assert found.status_code == (200 if authorized_after_rename else 404), found.text
    replacement = api.request_raw(
        "GET", "/tables/public_rows/documents/secret", timeout=30
    )
    assert replacement.status_code == 404, replacement.text


def test_session_snapshot_not_disclosed_after_rename(auth_api):
    api = auth_api
    admin = auth._basic_auth("admin", "admin")
    writer = auth._basic_auth("reader", "reader")
    api.s.headers["Authorization"] = admin
    api.create_table("public_rows")
    seeded = api.request_raw(
        "POST",
        "/tables/public_rows/batch",
        json={"inserts": {"secret": {"value": "confidential"}}},
        timeout=30,
    )
    assert seeded.status_code in (200, 201), seeded.text
    lookup = api.request_raw("GET", "/tables/public_rows/documents/secret", timeout=30)
    assert lookup.status_code == 200, lookup.text
    version = lookup.headers.get("X-Antfly-Version")
    assert version is not None
    api.post(
        "/auth/v1/users/reader",
        {
            "password": "reader",
            "initial_policies": [
                {"resource": "public_rows", "resource_type": "table", "type": "read"}
            ],
        },
    )
    api.s.headers["Authorization"] = writer
    tx = api.post("/transactions/begin", {"sync_level": "write"})["transaction_id"]
    staged = api.request_raw(
        "POST",
        "/transactions/" + tx + "/read",
        json={"table": "public_rows", "key": "secret", "version": version},
        timeout=30,
    )
    assert staged.status_code == 200, staged.text
    api.s.headers["Authorization"] = admin
    renamed = api.request_raw(
        "POST",
        "/databases/default/namespaces/public/tables/public_rows/rename",
        json={"name": "private_rows"},
        timeout=30,
    )
    assert renamed.status_code in (200, 201, 204), renamed.text
    api.s.headers["Authorization"] = writer
    details = api.request_raw("GET", "/transactions/" + tx, timeout=30)
    assert details.status_code in (404, 403), (details.status_code, details.text)


@pytest.mark.parametrize("mixed_spelling", [False, True])
def test_equivalent_datetime_cascades(auth_api, mixed_spelling):
    api = auth_api
    api.s.headers["Authorization"] = auth._basic_auth("admin", "admin")
    for table in ["p1", "p2", "child"]:
        schema = _schema()
        schema["document_schemas"]["row"]["schema"]["properties"]["id"] = {
            "type": "datetime",
        }
        if table == "child":
            schema.pop("unique_constraints")
            schema["foreign_keys"] = [
                {
                    "name": "fk_" + parent,
                    "child_columns": ["id"],
                    "parent_table": parent,
                    "parent_columns": ["id"],
                    "on_update": "cascade",
                }
                for parent in ["p1", "p2"]
            ]
        api.post("/tables/" + table, {"schema": schema})
        assert wait_until(
            lambda table=table: (
                api.get("/tables/" + table + "/constraints/status").get("state")
                == "enforced"
            ),
            timeout_s=30,
        )
        api.post(
            "/tables/" + table + "/batch",
            {"inserts": {"row": {"id": "2026-01-01T00:00:00Z"}}},
        )
    tx = api.post("/transactions/begin", {"sync_level": "write"})["transaction_id"]
    result = api.request_raw(
        "POST",
        "/transactions/" + tx + "/stage",
        json={
            "read_set": [],
            "tables": {
                "p1": {"inserts": {"row": {"id": "2026-01-02T00:00:00Z"}}},
                "p2": {
                    "inserts": {
                        "row": {
                            "id": "2026-01-02T00:00:00.000Z"
                            if mixed_spelling
                            else "2026-01-02T00:00:00Z"
                        }
                    }
                },
            },
        },
        timeout=30,
    )
    assert result.status_code == 200, result.text

    result = api.request_raw(
        "POST", "/transactions/" + tx + "/commit", json={}, timeout=30
    )
    assert result.status_code == 200, result.text


@pytest.mark.parametrize("explicit_child", [False, True])
def test_conflicting_cascade_has_client_conflict_status(auth_api, explicit_child):
    api = auth_api
    api.s.headers["Authorization"] = auth._basic_auth("admin", "admin")
    for table in ["p1", "p2", "child"]:
        schema = _schema()
        schema["document_schemas"]["row"]["schema"]["properties"]["id"] = {
            "type": "number"
        }
        if table == "child":
            schema.pop("unique_constraints")
            schema["foreign_keys"] = [
                {
                    "name": "fk_" + parent,
                    "child_columns": ["id"],
                    "parent_table": parent,
                    "parent_columns": ["id"],
                    "on_update": "cascade",
                }
                for parent in ["p1", "p2"]
            ]
        api.post("/tables/" + table, {"schema": schema})
        assert wait_until(
            lambda table=table: (
                api.get("/tables/" + table + "/constraints/status").get("state")
                == "enforced"
            ),
            timeout_s=30,
        )
        api.post("/tables/" + table + "/batch", {"inserts": {"row": {"id": 0}}})
    tx = api.post("/transactions/begin", {"sync_level": "write"})["transaction_id"]
    result = api.request_raw(
        "POST",
        "/transactions/" + tx + "/stage",
        json={
            "read_set": [],
            "tables": {
                "p1": {"inserts": {"row": {"id": 1}}},
                "p2": {"inserts": {"row": {"id": 2}}},
                **(
                    {"child": {"inserts": {"row": {"id": 0}}}} if explicit_child else {}
                ),
            },
        },
        timeout=30,
    )
    assert result.status_code == 409, (result.status_code, result.text)
    for table in ["p1", "p2", "child"]:
        assert api.lookup_key(table, "row")["id"] == 0


def _enforced(api, table):
    assert wait_until(
        lambda: (
            api.get(f"/tables/{table}/constraints/status").get("state") == "enforced"
        ),
        timeout_s=30,
    )


def _cascade_pair(api):
    child = _schema("parents")
    child["document_schemas"]["row"]["schema"]["properties"]["label"] = {
        "type": "string"
    }
    for table, schema in [("parents", _schema()), ("children", child)]:
        api.post(f"/tables/{table}", {"schema": schema})
        _enforced(api, table)
        row = {"id": 1, **({"label": "old"} if table == "children" else {})}
        api.post(f"/tables/{table}/batch", {"inserts": {"row": row}})


def test_self_referencing_cascade_preserves_unchanged_fk(auth_api):
    api = auth_api
    api.s.headers["Authorization"] = auth._basic_auth("admin", "admin")
    schema = _schema()
    schema["document_schemas"]["row"]["schema"]["properties"]["parent_id"] = {
        "type": "integer",
        "nullable": True,
    }
    schema["foreign_keys"] = [
        {
            "name": "self_fk",
            "child_columns": ["parent_id"],
            "parent_table": "nodes",
            "parent_columns": ["id"],
            "on_update": "cascade",
        }
    ]
    api.post("/tables/nodes", {"schema": schema})
    _enforced(api, "nodes")
    api.post("/tables/nodes/batch", {"inserts": {"row": {"id": 1, "parent_id": 1}}})
    api.post("/tables/nodes/batch", {"inserts": {"row": {"id": 2, "parent_id": 1}}})
    assert api.lookup_key("nodes", "row") == {"id": 2, "parent_id": 2}


def test_cascade_and_unrelated_child_update_same_statement(auth_api):
    api = auth_api
    api.s.headers["Authorization"] = auth._basic_auth("admin", "admin")
    _cascade_pair(api)
    tx = api.post("/transactions/begin", {"sync_level": "write"})["transaction_id"]
    resource = f"/transactions/{tx}"
    api.post(
        resource + "/stage",
        {
            "read_set": [],
            "tables": {
                "parents": {"inserts": {"row": {"id": 2}}},
                "children": {"inserts": {"row": {"id": 1, "label": "edited"}}},
            },
        },
    )
    api.post(resource + "/commit", {})
    assert api.lookup_key("children", "row") == {"id": 2, "label": "edited"}


@pytest.mark.parametrize("restart", [False, True])
def test_staged_cascade_rejects_concurrent_child_edit(stateful_api, restart):
    api = stateful_api
    _cascade_pair(api)
    tx = api.post("/transactions/begin", {"sync_level": "write"})["transaction_id"]
    resource = f"/transactions/{tx}"
    api.post(
        resource + "/write",
        {
            "table": "parents",
            "key": "row",
            "document": {"id": 2},
        },
    )
    savepoint = api.post(resource + "/savepoints", {})["savepoint_id"]
    api.post(
        resource + "/write",
        {
            "table": "parents",
            "key": "row",
            "document": {"id": 3},
        },
    )
    if restart:
        api.restart_server()
    api.post(resource + f"/savepoints/{savepoint}/rollback", {})
    api.post(
        "/tables/children/batch",
        {
            "inserts": {"row": {"id": 1, "label": "concurrent"}},
        },
    )
    committed = api._request("POST", resource + "/commit", {})
    assert committed.status_code == 409, committed.text
    assert api.get("/tables/parents/documents/row") == {"id": 1}
    assert api.get("/tables/children/documents/row") == {"id": 1, "label": "concurrent"}


def test_session_delete_then_reinsert_keeps_live_observation(stateful_api):
    api = stateful_api
    api.post("/tables/rows", {"schema": _schema()})
    _enforced(api, "rows")
    api.post("/tables/rows/batch", {"inserts": {"row": {"id": 1}}})
    tx = api.post("/transactions/begin", {"sync_level": "write"})["transaction_id"]
    resource = f"/transactions/{tx}"
    api.post(
        resource + "/stage",
        {
            "read_set": [],
            "tables": {"rows": {"deletes": ["row"]}},
        },
    )
    api.post(
        resource + "/write",
        {
            "table": "rows",
            "key": "row",
            "document": {"id": 2},
        },
    )
    api.post(resource + "/commit", {})
    assert api.get("/tables/rows/documents/row") == {"id": 2}
