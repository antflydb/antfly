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

"""Public authenticated Cypher through real metadata/data Raft owners."""

import os
import base64
import json
import time
import uuid
import urllib.error
import urllib.request
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit
from concurrent.futures import ThreadPoolExecutor
import threading
import pytest
import hashlib
import hmac
import struct

from conftest import (
    AUTH_BOOTSTRAP_PASSWORD,
    ready_index_status,
    internal_service_headers,
)
from test_backup_restore import three_by_three_backup_cluster  # noqa: F401


@pytest.fixture(autouse=True)
def cypher_principal_authority(monkeypatch):
    # Metadata signs row-policy grants and data owners verify them. Provision
    # the same isolated authority before either set of processes starts.
    monkeypatch.setenv(
        "ANTFLY_TRUSTED_PRINCIPAL_SECRET",
        "cypher-cluster-principal-authority-secret-v1",
    )
    monkeypatch.setenv("ANTFLY_TRUSTED_PRINCIPAL_ISSUER", "cypher-cluster")


def test_authenticated_cypher_postimage_policy_on_replicated_owners(
    three_by_three_backup_cluster,
):
    cluster = three_by_three_backup_cluster
    leader = urlsplit(cluster.metadata_leader_public_url())
    endpoint = f"{leader.scheme}://{leader.hostname}:{leader.port}"
    script = (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "verify_cypher_postimage_policy.py"
    )
    result = subprocess.run(
        [sys.executable, str(script), "--url", endpoint],
        env={
            **os.environ,
            "ANTFLY_CYPHER_TEST_AUTH": f"admin:{AUTH_BOOTSTRAP_PASSWORD}",
        },
        capture_output=True,
        text=True,
        timeout=220,
    )
    assert result.returncode == 0, (
        result.stdout + result.stderr + "\n" + cluster.debug_logs()
    )
    assert "4 authenticated Cypher postimage-policy checks passed" in result.stdout


def test_authenticated_cypher_across_replicated_owner_nodes(
    three_by_three_backup_cluster,
):
    cluster = three_by_three_backup_cluster
    leader = urlsplit(cluster.metadata_leader_public_url())
    endpoint = f"{leader.scheme}://{leader.hostname}:{leader.port}"
    script = Path(__file__).resolve().parents[2] / "scripts" / "verify_cypher_http.py"
    receipt_file = cluster.root / "cypher-write-receipt.json"
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--url",
            endpoint,
            "--shards",
            "2",
            "--writes",
            "--receipt-artifact",
            str(receipt_file),
        ],
        env={
            **os.environ,
            "ANTFLY_CYPHER_TEST_AUTH": f"admin:{AUTH_BOOTSTRAP_PASSWORD}",
        },
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert result.returncode == 0, (
        result.stdout + result.stderr + "\n" + cluster.debug_logs()
    )
    assert "checks passed" in result.stdout, result.stdout

    catalog_campaign = subprocess.run(
        [
            sys.executable,
            str(script.with_name("verify_cypher_catalog.py")),
            "--url",
            endpoint,
        ],
        env={
            **os.environ,
            "ANTFLY_CYPHER_TEST_AUTH": f"admin:{AUTH_BOOTSTRAP_PASSWORD}",
        },
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert catalog_campaign.returncode == 0, (
        catalog_campaign.stdout + catalog_campaign.stderr + "\n" + cluster.debug_logs()
    )
    assert (
        "6 authenticated graph catalog revision checks passed"
        in catalog_campaign.stdout
    )

    for filename in [
        "verify_cypher_expressions.py",
        "verify_cypher_postimage_policy.py",
    ]:
        qualification = subprocess.run(
            [sys.executable, str(script.with_name(filename)), "--url", endpoint],
            env={
                **os.environ,
                "ANTFLY_CYPHER_TEST_AUTH": f"admin:{AUTH_BOOTSTRAP_PASSWORD}",
            },
            capture_output=True,
            text=True,
            timeout=180,
        )
        policy_diagnostics = []
        if (
            qualification.returncode != 0
            and filename == "verify_cypher_postimage_policy.py"
        ):
            try:
                snapshot = cluster.metadata_snapshot(
                    cluster.metadata_admin_ports.index(leader.port), request_timeout_s=5
                )
                for table in snapshot.get("tables", []):
                    request = urllib.request.Request(
                        endpoint + "/internal/v1/system-catalog",
                        json.dumps(
                            {"policy_publication_status": int(table["table_id"])}
                        ).encode(),
                        {
                            **internal_service_headers(),
                            "Content-Type": "application/json",
                            "X-Antfly-Raft-Mutation-Remaining-Ms": "5000",
                            "X-Antfly-Raft-Mutation-Forwards-Remaining": "0",
                            "X-Antfly-Raft-Mutation-Campaign-Allowed": "false",
                        },
                    )
                    with urllib.request.urlopen(request, timeout=6) as response:
                        policy_diagnostics.append(json.load(response))
                body = json.dumps({"policy_publication_work": 0}).encode()
                expires = int(time.time()) + 60
                key = hmac.new(
                    os.environ["ANTFLY_SETTING_AUTHORITY_SECRET"].encode(),
                    b"antfly/sql-setting-authority/key/v1",
                    hashlib.sha256,
                ).digest()
                mac = hmac.new(
                    key,
                    b"sql-setting-catalog-v1"
                    + os.environ["ANTFLY_SETTING_AUTHORITY_ISSUER"].encode()
                    + b"\0read"
                    + struct.pack("<q", expires)
                    + hashlib.sha256(body).digest(),
                    hashlib.sha256,
                ).hexdigest()
                request = urllib.request.Request(
                    endpoint + "/internal/v1/system-catalog",
                    body,
                    {
                        **internal_service_headers(),
                        "Content-Type": "application/json",
                        "x-antfly-setting-grant": f"v1:{expires}:{mac}",
                        "X-Antfly-Raft-Mutation-Remaining-Ms": "5000",
                        "X-Antfly-Raft-Mutation-Forwards-Remaining": "0",
                        "X-Antfly-Raft-Mutation-Campaign-Allowed": "false",
                    },
                )
                with urllib.request.urlopen(request, timeout=6) as response:
                    work = json.load(response)
                publication = work.get("publication")
                if publication:
                    policy_diagnostics.append(
                        {
                            "phase": publication["phase"],
                            "owners": len(publication["required_owners"]),
                            "installed": len(publication["acknowledged_owners"]),
                            "serving": len(publication["serving_acknowledged_owners"]),
                        }
                    )
            except Exception as diagnostic_error:
                policy_diagnostics.append({"diagnostic_error": str(diagnostic_error)})
        assert qualification.returncode == 0, (
            filename
            + "\n"
            + qualification.stdout
            + qualification.stderr
            + "\npolicy publication stamps: "
            + json.dumps(policy_diagnostics)
            + "\n"
            + cluster.debug_logs()
        )
        assert "checks passed" in qualification.stdout

    transaction_id = json.loads(receipt_file.read_text())["transaction_id"]
    authorization = (
        "Basic "
        + base64.b64encode(f"admin:{AUTH_BOOTSTRAP_PASSWORD}".encode()).decode()
    )

    def receipt(base):
        deadline = time.monotonic() + 30
        while True:
            request = urllib.request.Request(
                base + "/db/v1/transactions/" + transaction_id + "/status",
                headers={"Authorization": authorization},
            )
            try:
                with urllib.request.urlopen(request, timeout=5) as response:
                    result = json.loads(response.read())
                assert result["disposition"] == "committed", result
                assert result["durable"] is True, result
                return result
            except urllib.error.HTTPError as error:
                assert error.code == 503 and time.monotonic() < deadline, (
                    base,
                    error.code,
                    error.read().decode(),
                    cluster.debug_logs(),
                )
            except (TimeoutError, ConnectionError):
                assert time.monotonic() < deadline, (base, cluster.debug_logs())
            time.sleep(0.1)

    # Reads must work on every metadata replica, without routing to an owner.
    for base in cluster.metadata_admin_urls:
        receipt(base)

    owner_index = cluster.metadata_admin_ports.index(leader.port)
    owner = cluster.metadata_procs[owner_index]
    owner.kill()
    owner.wait(timeout=15)
    for index, base in enumerate(cluster.metadata_admin_urls):
        if index != owner_index:
            receipt(base)

    # A committed request ID is globally reserved after leader failover, even
    # when submitted against a different graph and the original table is gone.
    new_leader = urlsplit(cluster.metadata_leader_public_url())
    base = f"{new_leader.scheme}://{new_leader.hostname}:{new_leader.port}"
    table = "cypher_failover_" + uuid.uuid4().hex
    graph = table + "_graph"
    created_table = created_graph = False

    def call(path, body=None, method="POST", accepted=(200, 201, 202, 204)):
        request = urllib.request.Request(
            base + "/db/v1" + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "Authorization": authorization,
                "Content-Type": "application/json",
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                code, content = response.status, response.read()
        except urllib.error.HTTPError as error:
            code, content = error.code, error.read()
        assert code in accepted, (path, code, content.decode(), cluster.debug_logs())
        return json.loads(content) if content else None

    try:
        call(f"/tables/{table}", {"num_shards": 2})
        created_table = True
        call(
            f"/graphs/{graph}",
            {"labels": [{"name": "Document", "table": table}], "relationships": []},
        )
        created_graph = True
        deadline = time.monotonic() + 30
        while True:
            result = call(
                f"/graphs/{graph}/query",
                {"query": "MATCH (n:Document) RETURN n"},
                accepted=(200, 503),
            )
            if result.get("rows") == []:
                break
            assert time.monotonic() < deadline, result
            time.sleep(0.1)
        denied = call(
            f"/graphs/{graph}/query",
            {
                "query": "CREATE (n:Document {id:'duplicate'}) RETURN n",
                "write_request_id": transaction_id,
            },
            accepted=(409,),
        )
        assert denied["code"] == "CypherWriteRequestAlreadyUsed", denied
        assert (
            call(f"/graphs/{graph}/query", {"query": "MATCH (n:Document) RETURN n"})[
                "rows"
            ]
            == []
        )
    finally:
        if created_graph:
            call(f"/graphs/{graph}", method="DELETE")
        if created_table:
            call(f"/tables/{table}", method="DELETE")
    cluster.restart_crashed_node(metadata=True, index=owner_index)
    for base in cluster.metadata_admin_urls:
        receipt(base)


def test_cypher_receipt_recovers_owner_loss_during_index_visibility(
    three_by_three_backup_cluster, progressive_openai_embedder
):
    cluster = three_by_three_backup_cluster
    embedder = progressive_openai_embedder
    entered, released = threading.Event(), threading.Event()
    original_vector = embedder._vector_for_text
    marker = "beta cypher-failover-barrier"

    def vector(text):
        if marker in text:
            entered.set()
            assert released.wait(90), "embedding visibility barrier was not released"
        return original_vector(text)

    embedder._vector_for_text = vector
    authorization = (
        "Basic "
        + base64.b64encode(f"admin:{AUTH_BOOTSTRAP_PASSWORD}".encode()).decode()
    )
    leader = urlsplit(cluster.metadata_leader_public_url())
    base = f"{leader.scheme}://{leader.hostname}:{leader.port}"
    owner_index = cluster.metadata_admin_ports.index(leader.port)
    table = "cypher_commit_fault_" + uuid.uuid4().hex
    graph = table + "_graph"
    transaction_id = uuid.uuid4().hex
    created_table = created_graph = crashed = False

    def call(path, body=None, method="POST", accepted=(200, 201, 202, 204), timeout=30):
        request = urllib.request.Request(
            base + "/db/v1" + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "Authorization": authorization,
                "Content-Type": "application/json",
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                code, content = response.status, response.read()
        except urllib.error.HTTPError as error:
            code, content = error.code, error.read()
        assert code in accepted, (path, code, content.decode(), cluster.debug_logs())
        return json.loads(content) if content else None

    try:
        call(f"/tables/{table}", {"num_shards": 2})
        created_table = True
        call(
            f"/tables/{table}/indexes/semantic_idx",
            {
                "name": "semantic_idx",
                "type": "embeddings",
                "field": "body",
                "dimension": 3,
                "embedder": {
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "url": embedder.url,
                },
            },
        )
        # Index publication on the seeded shard does not establish readiness
        # on the other shard, where the randomly generated CREATE key may land.
        # Establish the complete index generation before injecting owner loss.
        ready_deadline = time.monotonic() + 60
        while True:
            index = call(f"/tables/{table}/indexes/semantic_idx", method="GET")
            if ready_index_status(index, until="complete") is not None:
                break
            assert time.monotonic() < ready_deadline, (index, cluster.debug_logs())
            time.sleep(0.2)
        call(
            f"/tables/{table}/batch",
            {
                "sync_level": "full_index",
                "inserts": {"a": {"id": "a", "body": "alpha concept"}},
            },
        )
        call(
            f"/graphs/{graph}",
            {"labels": [{"name": "Document", "table": table}], "relationships": []},
        )
        created_graph = True
        readiness_deadline = time.monotonic() + 30
        while True:
            readiness = call(
                f"/graphs/{graph}/query",
                {"query": "MATCH (n) RETURN n LIMIT 0"},
                accepted=(200, 409, 503),
            )
            if readiness.get("complete"):
                break
            assert time.monotonic() < readiness_deadline, (
                readiness,
                cluster.debug_logs(),
            )
            time.sleep(0.2)
        with ThreadPoolExecutor(max_workers=1) as executor:
            # Submit once. Owner loss must reconcile this ID, never replay the
            # public query or generate a second ID for the same write.
            future = executor.submit(
                call,
                f"/graphs/{graph}/query",
                {
                    "query": "CREATE (n:Document {id:'c',body:$body}) RETURN n.id",
                    "parameters": {"body": marker},
                    "write_request_id": transaction_id,
                },
                timeout=90,
            )
            injection_deadline = time.monotonic() + 30
            while not entered.wait(0.2):
                if future.done():
                    # Surface admission/prepare failures instead of hiding them
                    # behind an unrelated embedding-checkpoint timeout.
                    delivered = future.result()
                    raise AssertionError(
                        (
                            "write ended before fault injection",
                            delivered,
                            cluster.debug_logs(),
                        )
                    )
                assert time.monotonic() < injection_deadline, cluster.debug_logs()
            owner = cluster.metadata_procs[owner_index]
            owner.kill()
            owner.wait(timeout=15)
            crashed = True
            released.set()
            try:
                delivered = future.result(timeout=30)
                assert delivered["commit"]["transaction_id"] == transaction_id, (
                    delivered
                )
            except (OSError, ConnectionError, TimeoutError):
                pass
        new_leader = urlsplit(cluster.metadata_leader_public_url())
        base = f"{new_leader.scheme}://{new_leader.hostname}:{new_leader.port}"
        deadline = time.monotonic() + 90
        while True:
            result = call(
                f"/transactions/{transaction_id}/status",
                method="GET",
                accepted=(200, 503),
            )
            if result.get("disposition") == "committed":
                assert result["durable"] is True, result
                break
            assert result.get("disposition") != "aborted", result
            assert time.monotonic() < deadline, (result, cluster.debug_logs())
            time.sleep(0.2)
        nodes = call(
            f"/graphs/{graph}/query",
            {"query": "MATCH (n:Document {id:'c'}) RETURN n.id"},
        )
        assert nodes["rows"] == [[{"type": "string", "value": "c"}]], nodes
        ranked = call(
            f"/graphs/{graph}/query",
            {
                "query": f"CALL antfly.search('{table}', {{query:'beta',mode:'semantic',indexes:['semantic_idx'],limit:10}}) YIELD node RETURN node.id"
            },
        )
        assert {row[0]["value"] for row in ranked["rows"]} == {"a", "c"}, ranked
    finally:
        released.set()
        if crashed:
            cluster.restart_crashed_node(metadata=True, index=owner_index)
        if created_graph:
            call(f"/graphs/{graph}", method="DELETE")
        if created_table:
            call(f"/tables/{table}", method="DELETE")
