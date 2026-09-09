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

"""Native catalog identity, placement, and qualified restore regressions."""

import json
import tempfile
import uuid
from pathlib import Path

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
    assert api.get(f"/tables/{target}")["table_id"] == created["table_id"]
    assert api.get(f"/tables/{target}/documents/doc1") == {"title": "hello"}
    result = api.post(
        f"/tables/{target}/query", {"full_text_search": {"match_all": {}}, "limit": 10}
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
            "params": {"name": "describe_table", "arguments": {"tableName": target}},
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
    assert api.get(f"/tables/{target}")["table_id"] == created["table_id"]
    assert api.get(f"/tables/{target}/documents/doc1") == {"title": "hello"}
    api.delete(f"/tables/{target}")
    api.delete(f"/databases/{renamed}")
    api.delete(f"/tablespaces/{tablespace}")


def test_catalog_restore_to_qualified_destination(backup_api):
    api = backup_api
    database = "restore_" + uuid.uuid4().hex[:12]
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
            api.url + path + "/destination/restore",
            json=body,
            headers={"Idempotency-Key": "catalog-restore"},
            timeout=30,
        )
        assert response.status_code == 202, response.text
        accepted = response.json()
        assert accepted["table_name"] == f"{database}.public.destination"

        def terminal():
            job = api.get("/restore/jobs/" + accepted["job_id"])
            return job if job["phase"] in {"succeeded", "failed", "cancelled"} else None

        completed = wait_until(terminal, timeout_s=60, interval_s=0.1)
        assert completed["phase"] == "succeeded", (
            json.dumps(completed) + api.debug_logs()
        )
        assert api.get(path + "/destination/documents/doc1") == {"title": "restored"}
        assert api.get(path + "/source/documents/doc1") == {"title": "restored"}
        assert (
            api.get(path + "/source")["table_id"]
            != api.get(path + "/destination")["table_id"]
        )
        replay = api.s.post(
            api.url + path + "/destination/restore",
            json=body,
            headers={"Idempotency-Key": "catalog-restore"},
            timeout=30,
        )
        assert replay.status_code == 202, replay.text
        assert replay.json()["job_id"] == accepted["job_id"]
        api.post(
            path + "/destination/batch",
            {"inserts": {"doc2": {"title": "new"}}, "sync_level": "full_index"},
        )
        result = api.post(
            path + "/destination/query",
            {"full_text_search": {"match_all": {}}, "limit": 10},
        )
        assert result["responses"][0]["hits"]["total"]["value"] == 2
    api.delete(path + "/source")
    api.delete(path + "/destination")
    api.delete(f"/databases/{database}")
