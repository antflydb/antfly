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


def test_conflicting_cascade_has_client_conflict_status(auth_api):
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
            },
        },
        timeout=30,
    )
    assert result.status_code == 409, (result.status_code, result.text)
    for table in ["p1", "p2", "child"]:
        assert api.lookup_key(table, "row")["id"] == 0
