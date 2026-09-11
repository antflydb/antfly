# Copyright 2026 Antfly, Inc.
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

"""System catalog identity, placement, and qualified restore regressions."""

import json
import tempfile
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote

import pytest
import requests
from helpers import wait_until


def test_catalog_rename_restart_and_placement(stateful_api):
    api = stateful_api
    suffix = uuid.uuid4().hex[:12]
    database = "catalog_" + suffix
    renamed = database + "_renamed"
    tablespace = "policy_" + suffix
    api.post(f"/databases/{database}", {})
    api.post(f"/databases/{database}/namespaces/serving", {})
    api.post(
        f"/tablespaces/{tablespace}",
        {
            "placement_policy_json": json.dumps(
                {"min_ranges": 2, "desired_replica_count": 1}
            ),
        },
    )
    api.put(f"/databases/{database}/tablespace", {"tablespace_name": tablespace})
    path = f"/databases/{database}/namespaces/serving/tables/events"
    created = api.post(path, {})
    assert len(created["shards"]) == 2
    assert isinstance(created["table_id"], str)
    api.post(
        path + "/batch",
        {"inserts": {"doc1": {"title": "hello"}}, "sync_level": "full_index"},
    )
    api.post(path + "/rename", {"name": "logs"})
    api.post(f"/databases/{database}/namespaces/serving/rename", {"name": "reports"})
    api.post(f"/databases/{database}/rename", {"name": renamed})
    target = f"{renamed}.reports.logs"
    target_path = f"/databases/{renamed}/namespaces/reports/tables/logs"
    assert api.get(f"{target_path}")["table_id"] == created["table_id"]
    assert api.get(f"{target_path}/documents/doc1") == {"title": "hello"}
    result = api.post(
        f"{target_path}/query", {"full_text_search": {"match_all": {}}, "limit": 10}
    )
    assert result["responses"][0]["table"] == target
    mcp_url = api.url.removesuffix("/db/v1") + "/mcp/v1"
    initialized = api.s.post(
        mcp_url,
        json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
        timeout=10,
    )
    initialized.raise_for_status()
    described = api.s.post(
        mcp_url,
        headers={"Mcp-Session-Id": initialized.headers["Mcp-Session-Id"]},
        json={
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "describe_table",
                "arguments": {
                    "database": renamed,
                    "namespace": "reports",
                    "tableName": "logs",
                },
            },
        },
        timeout=10,
    )
    described.raise_for_status()
    content = described.json()["result"]
    assert not content.get("isError", False), content
    assert json.loads(content["content"][0]["text"])["name"] == target
    assert api._request("GET", path).status_code == 404
    assert api._request("DELETE", f"/databases/{renamed}").status_code == 409
    assert api._request("DELETE", f"/tablespaces/{tablespace}").status_code == 409
    assert all(item["table_id"] != created["table_id"] for item in api.get("/tables"))
    api.restart_server()
    assert api.get(f"{target_path}")["table_id"] == created["table_id"]
    assert api.get(f"{target_path}/documents/doc1") == {"title": "hello"}
    api.delete(f"{target_path}")
    api.delete(f"/databases/{renamed}")
    api.delete(f"/tablespaces/{tablespace}")


@pytest.mark.parametrize("long_names", [False, True])
def test_catalog_restore_to_qualified_destination(backup_api, long_names):
    api = backup_api
    database = "restore_" + uuid.uuid4().hex[:12]
    if long_names:
        database = database.ljust(128, "a")
    destination = "destination".ljust(255, "a") if long_names else "destination"
    api.post(f"/databases/{database}", {})
    path = f"/databases/{database}/namespaces/public/tables"
    api.post(path + "/source", {"num_shards": 1})
    api.post(
        path + "/source/batch",
        {"inserts": {"doc1": {"title": "restored"}}, "sync_level": "full_index"},
    )
    with tempfile.TemporaryDirectory(prefix="antfly-catalog-backup-") as directory:
        body = {
            "backup_id": "catalog",
            "location": Path(directory).resolve().as_uri(),
            "connection": "e2e-backups",
        }
        api.post(path + "/source/backup", body)
        response = api.s.post(
            api.url + path + f"/{destination}/restore",
            json=body,
            headers={"Idempotency-Key": "catalog-restore"},
            timeout=30,
        )
        assert response.status_code == 202, response.text
        accepted = response.json()
        assert accepted["table_name"] == f"{database}.public.{destination}"

        def terminal():
            job = api.get("/restore/jobs/" + accepted["job_id"])
            return job if job["phase"] in {"succeeded", "failed", "cancelled"} else None

        completed = wait_until(terminal, timeout_s=60, interval_s=0.1)
        assert completed["phase"] == "succeeded", (
            json.dumps(completed) + api.debug_logs()
        )
        assert api.get(path + f"/{destination}/documents/doc1") == {"title": "restored"}
        assert api.get(path + "/source/documents/doc1") == {"title": "restored"}
        assert (
            api.get(path + "/source")["table_id"]
            != api.get(path + f"/{destination}")["table_id"]
        )
        replay = api.s.post(
            api.url + path + f"/{destination}/restore",
            json=body,
            headers={"Idempotency-Key": "catalog-restore"},
            timeout=30,
        )
        assert replay.status_code == 202, replay.text
        assert replay.json()["job_id"] == accepted["job_id"]
        api.post(
            path + f"/{destination}/batch",
            {"inserts": {"doc2": {"title": "new"}}, "sync_level": "full_index"},
        )
        result = api.post(
            path + f"/{destination}/query",
            {"full_text_search": {"match_all": {}}, "limit": 10},
        )
        assert result["responses"][0]["hits"]["total"]["value"] == 2
    api.delete(path + "/source")
    api.delete(path + f"/{destination}")
    api.delete(f"/databases/{database}")


def test_catalog_scope_indexes_and_placement_overrides(stateful_api):
    api = stateful_api
    database = "scopes_" + uuid.uuid4().hex[:12]
    root = f"/databases/{database}"
    assert (
        api._request(
            "GET", "/tables/table:00000000000000000000000000000000"
        ).status_code
        == 404
    )
    api.post(root, {})
    for namespace in ("left", "right"):
        api.post(root + f"/namespaces/{namespace}", {})
    policies = {database + "_db": 2, database + "_ns": 3, database + "_table": 1}
    for name, ranges in policies.items():
        api.post(
            f"/tablespaces/{name}",
            {
                "placement_policy_json": json.dumps(
                    {"min_ranges": ranges, "desired_replica_count": 1}
                )
            },
        )
    api.put(root + "/tablespace", {"tablespace_name": database + "_db"})
    left = root + "/namespaces/left"
    right = root + "/namespaces/right"
    api.put(left + "/tablespace", {"tablespace_name": database + "_ns"})
    left_table = left + "/tables/events"
    right_table = right + "/tables/events"
    left_created = api.post(left_table, {})
    right_created = api.post(right_table, {})
    assert len(left_created["shards"]) == 3
    assert len(right_created["shards"]) == 2
    assert left_created["table_id"] != right_created["table_id"]
    override = api.post(
        left + "/tables/override", {"tablespace_name": database + "_table"}
    )
    assert len(override["shards"]) == 1
    for path, scope in ((left_table, "left"), (right_table, "right")):
        api.post(
            path + "/batch",
            {"inserts": {"same_key": {"scope": scope}}, "sync_level": "full_index"},
        )
        assert api.get(path + "/documents/same_key") == {"scope": scope}
    for path, expected in ((left, {"events", "override"}), (right, {"events"})):
        assert {
            table["name"].split(".")[-1] for table in api.get(path + "/tables")
        } == expected
    api.post(
        left_table + "/indexes/vectors",
        {
            "type": "embeddings",
            "external": True,
            "dimension": 8,
            "distance_metric": "cosine",
        },
    )
    assert api.get(left_table + "/indexes/vectors")["config"]["name"] == "vectors"
    assert api._request("GET", right_table + "/indexes/vectors").status_code == 404
    api.delete(left_table + "/indexes/vectors")
    assert api._request("GET", left_table + "/indexes/vectors").status_code == 404
    renamed_policy = database + "_renamed"
    api.post("/tablespaces/" + database + "_ns/rename", {"name": renamed_policy})
    assert (
        next(ns for ns in api.get(root + "/namespaces") if ns["name"] == "left")[
            "tablespace_name"
        ]
        == renamed_policy
    )
    assert api._request("DELETE", "/tablespaces/" + renamed_policy).status_code == 409
    api.delete(left + "/tablespace")
    inherited = api.post(left + "/tables/inherited", {})
    assert len(inherited["shards"]) == 2
    # Parent changes are defaults for future creation, not implicit resharding.
    assert len(api.get(left_table)["shards"]) == 3
    api.put(left_table + "/tablespace", {"tablespace_name": database + "_table"})
    api.delete(left_table + "/tablespace")
    for path in (
        left_table,
        right_table,
        left + "/tables/override",
        left + "/tables/inherited",
    ):
        api.delete(path)
    api.delete(root)
    for name in (database + "_db", renamed_policy, database + "_table"):
        api.delete("/tablespaces/" + name)


@pytest.mark.parametrize(
    "name",
    [
        "tenant.public.events",
        "sales/archive",
        "sales archive",
        "*",
        "table:legacy",
        "9events",
        "x" * 255,
    ],
)
def test_catalog_literal_table_names(stateful_api, name):
    api = stateful_api
    path = "/tables/" + quote(name, safe="")
    created = api.post(path, {"num_shards": 1})
    try:
        api.post(
            path + "/batch",
            {"inserts": {"row": {"name": name}}, "sync_level": "full_index"},
        )
        assert api.get(path + "/documents/row") == {"name": name}
        result = api.post(
            path + "/query", {"full_text_search": {"match_all": {}}, "limit": 10}
        )
        assert result["responses"][0]["table"] == name
        assert result["responses"][0]["hits"]["hits"][0]["_id"] == "row"
        assert any(
            item["name"] == name and item["table_id"] == created["table_id"]
            for item in api.get("/tables")
        )
    finally:
        api.delete(path)


def test_catalog_scoped_join_keeps_literal_lookalikes_separate(stateful_api):
    api = stateful_api
    database = "joins_" + uuid.uuid4().hex[:12]
    root = f"/databases/{database}"
    api.post(root, {})
    scoped = root + "/namespaces/public/tables/customers"
    literal = f"{database}.public.customers"
    literal_path = "/tables/" + literal
    docs = root + "/namespaces/public/tables/docs"
    for path in (scoped, literal_path, docs):
        api.post(path, {"num_shards": 1})
    try:
        for path, name in ((scoped, "scoped"), (literal_path, "literal")):
            api.post(
                path + "/batch",
                {"inserts": {"customer": {"name": name}}, "sync_level": "full_index"},
            )
        api.post(
            docs + "/batch",
            {
                "inserts": {"doc": {"customer_id": "customer"}},
                "sync_level": "full_index",
            },
        )
        for target, expected in (
            ({"right_table": literal}, "literal"),
            ({"right_target": {"database": database, "table": "customers"}}, "scoped"),
        ):
            body = {
                "full_text_search": {"match_all": {}},
                "limit": 10,
                "join": {
                    **target,
                    "on": {"left_field": "customer_id", "right_field": "_id"},
                    "right_fields": ["name"],
                },
            }
            response = api.post(docs + "/query", body)["responses"][0]
            assert response["table"] == f"{database}.public.docs"
            hits = response["hits"]["hits"]
            assert len(hits) == 1, response
            assert hits[0]["_source"][literal + ".name"] == expected
            assert "table:" not in json.dumps(response)
        both = {
            "full_text_search": {"match_all": {}},
            "join": {
                "right_table": literal,
                "right_target": {"database": database, "table": "customers"},
                "on": {"left_field": "customer_id", "right_field": "_id"},
            },
        }
        assert (
            api.s.post(api.url + docs + "/query", json=both, timeout=30).status_code
            == 400
        )
    finally:
        for path in (scoped, literal_path, docs):
            api.delete(path)
        api.delete(root)


def test_catalog_cluster_backup_retains_scope_and_literal_names(backup_api):
    api = backup_api
    database = "archive_" + uuid.uuid4().hex[:12]
    api.post(f"/databases/{database}", {})
    literal = "sales/archive.v1"
    paths = [
        "/tables/" + quote(literal, safe=""),
        f"/databases/{database}/namespaces/public/tables/" + quote(literal, safe=""),
    ]
    for index, path in enumerate(paths):
        api.post(path, {})
        api.post(
            path + "/batch",
            {"inserts": {"doc": {"scope": index}}, "sync_level": "full_index"},
        )
    with tempfile.TemporaryDirectory(prefix="antfly-catalog-cluster-") as directory:
        location = Path(directory).as_uri()
        backup = api.cluster_backup(backup_id="catalog-cluster", location=location)
        assert {table["name"] for table in backup["tables"]} == {
            literal,
            f"{database}.public.{literal}",
        }
        # The API assertions below exercise the durable manifest after names
        # have disappeared from the live catalog; they cannot pass through a
        # cache of existing bindings.
        for path in paths:
            api.delete(path)
        restored = api.cluster_restore(
            backup_id="catalog-cluster",
            location=location,
            restore_mode="fail_if_exists",
        )
        assert restored["committed_table_count"] == 2
        for index, path in enumerate(paths):
            assert api.get(path + "/documents/doc") == {"scope": index}
    for path in paths:
        api.delete(path)
    api.delete(f"/databases/{database}")


def test_catalog_ddl_burst_recovers_exact_resource_identities(stateful_api):
    """Tenant provisioning, rename, and offboarding survive one restart."""
    api = stateful_api
    prefix = "catalog_burst_" + uuid.uuid4().hex[:10]
    survivors = {}
    retired = []
    for i in range(12):
        name = f"{prefix}_{i}"
        created = api.post(f"/databases/{name}", {})
        renamed = name + "_renamed"
        api.post(f"/databases/{name}/rename", {"name": renamed})
        assert api._request("GET", f"/databases/{name}").status_code == 404
        if i % 3 == 0:
            api.delete(f"/databases/{renamed}")
            retired.append(renamed)
        else:
            survivors[renamed] = created["database_id"]
            api.post(f"/databases/{renamed}/namespaces/temporary", {})
            api.delete(f"/databases/{renamed}/namespaces/temporary")
    api.restart_server()
    for name in retired:
        assert api._request("GET", f"/databases/{name}").status_code == 404
    for name, identity in survivors.items():
        assert api.get(f"/databases/{name}")["database_id"] == identity
        namespaces = api.get(f"/databases/{name}/namespaces")
        assert {row["name"] for row in namespaces} == {"public"}
        api.delete(f"/databases/{name}")


def test_concurrent_catalog_drop_never_reclassifies_private_tables(stateful_api):
    api = stateful_api
    database = "private_" + uuid.uuid4().hex[:12]
    api.post(f"/databases/{database}", {})
    marker = "private listing " + database
    stopped = threading.Event()
    ready = threading.Barrier(5, timeout=30)

    def list_default():
        with requests.Session() as client:
            client.headers.update(api.s.headers)
            ready.wait()
            while not stopped.is_set():
                response = client.get(api.url + "/tables", timeout=30)
                response.raise_for_status()
                assert all(row.get("description") != marker for row in response.json())

    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            readers = [pool.submit(list_default) for _ in range(4)]
            try:
                ready.wait()
                for i in range(40):
                    path = f"/databases/{database}/namespaces/public/tables/t{i}"
                    api.post(path, {"description": marker})
                    api.delete(path)
            finally:
                stopped.set()
                for reader in readers:
                    reader.result()
    finally:
        api.delete(f"/databases/{database}")


def test_catalog_pagination_preserves_scope_order_and_detects_ddl(stateful_api):
    api = stateful_api
    database = "pages_" + uuid.uuid4().hex[:12]
    api.post(f"/databases/{database}", {})
    path = f"/databases/{database}/namespaces/public/tables"
    names = [
        "item_" + name for name in ("z", "a/b", "a*", "a.b", "one", "two", "three")
    ]
    try:
        for name in names:
            api.post(path + "/" + quote(name, safe=""), {"num_shards": 1})
        seen = []
        cursor = None
        first_cursor = None
        for _ in range(len(names) + 1):
            params = {"limit": 2, "prefix": "item_"}
            if cursor:
                params["cursor"] = cursor
            response = api.s.get(api.url + path, params=params, timeout=30)
            assert response.status_code == 200, response.text
            rows = response.json()
            assert len(rows) <= 2
            seen.extend(row["name"] for row in rows)
            cursor = response.headers.get("X-Antfly-Next-Cursor")
            first_cursor = first_cursor or cursor
            if not cursor:
                break
        else:
            pytest.fail("pagination failed to terminate")
        assert seen == sorted(names)
        assert len({row["table_id"] for row in api.get(path)}) == len(names)
        assert first_cursor
        for params in (
            {"limit": 0},
            {"limit": 1001},
            {"limit": "bad"},
            {"cursor": "!!!"},
            {"cursor": first_cursor, "prefix": "other"},
        ):
            response = api.s.get(api.url + path, params=params, timeout=30)
            assert response.status_code == 400, response.text
        api.post(path + "/new", {"num_shards": 1})
        response = api.s.get(
            api.url + path,
            params={"cursor": first_cursor, "prefix": "item_"},
            timeout=30,
        )
        assert response.status_code == 409, response.text
        assert len(api.get(path)) == len(names) + 1
    finally:
        for row in api.get(path):
            api.delete(path + "/" + quote(row["name"], safe=""))
        api.delete(f"/databases/{database}")
