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

"""Real online-merge outages, without production failpoints or host firewalls.

Advertised owner HTTP addresses pass through local proxies. Exact protocol
operations can be withheld on every route, including routes after elections.
Raft and unrelated HTTP traffic remain live in owner-link tests. A separate
case interrupts all advertised owner Raft links while leaving processes alive.
"""

import json
import threading
import time
from collections import deque
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests
import test_backup_restore as backups
from helpers import wait_until

three_by_three_backup_cluster = backups.three_by_three_backup_cluster


class OwnerLinkFault:
    def __init__(self, window):
        self.window = window
        self.lock = threading.Lock()
        self.healed = False
        self.hits = set()
        self.raft_cut = threading.Event()
        self.raft_seen = threading.Event()
        self.drop_next_reply = False
        self.reply_dropped = threading.Event()
        self.require_durable_decision = False
        self.lose_first_committed_reply = False
        self.transaction_coordinators = {}
        self.committed_decisions = set()
        self.transaction_observations = []
        # Diagnostic only: retain bounded failures after fault healing too.
        # Never change matching, forwarding, or decision tracking below.
        self.failed_owner_responses = deque(maxlen=128)
        self.owner_progress = deque(maxlen=32)

    @staticmethod
    def transaction_identity(body):
        path = body.get("_fault_path", "")
        parts = path.split("/")
        transaction = body.get("_transaction", body)
        txn_id = transaction.get("txn_id")
        if txn_id is None or "groups" not in parts:
            return None
        return json.dumps(txn_id, sort_keys=True), parts[parts.index("groups") + 1]

    def observe_request(self, body):
        with self.lock:
            if self.healed or not self.require_durable_decision:
                return
            identity = self.transaction_identity(body)
            if identity and body.get("_fault_path", "").endswith("/txn-begin"):
                self.transaction_coordinators.setdefault(*identity)

    def observe_response(self, index, body, response):
        with self.lock:
            path = body.get("_fault_path", "")
            if response.status_code == 200 and path.endswith("/restore-owner"):
                try:
                    value = response.json()
                    rewrite = value.get("rewrite") or {}
                    self.owner_progress.append(
                        (
                            index,
                            body.get("action"),
                            path,
                            {
                                "phase": value.get("phase"),
                                "rows": value.get("rows"),
                                "source_next_offset": value.get("source_next_offset"),
                                "snapshot_complete": rewrite.get("snapshot_complete"),
                                "sequence": rewrite.get("sequence"),
                                "final_cut": rewrite.get("final_cut") is not None,
                            },
                        )
                    )
                except ValueError:
                    pass
            if response.status_code >= 400 and path.endswith(
                ("/restore-owner", "/txn-prepare")
            ):
                self.failed_owner_responses.append(
                    (index, path, response.status_code, response.text[:256])
                )
            if self.healed or not self.require_durable_decision:
                return
            identity = self.transaction_identity(body)
            if identity is None:
                return
            txn_id, group = identity
            path = body.get("_fault_path", "")
            if len(self.transaction_observations) < 32:
                self.transaction_observations.append(
                    (index, path, response.status_code, response.text[:256])
                )
            if (
                path.endswith("/txn-resolve")
                and body.get("status") == "committed"
                and self.transaction_coordinators.get(txn_id) == group
                and 200 <= response.status_code < 300
            ):
                self.committed_decisions.add(txn_id)

    def matches(self, body):
        if self.window.startswith("transaction_"):
            transaction = body.get("_transaction", {})
            phase = self.window.removeprefix("transaction_")
            if (
                phase == "resolve"
                and transaction.get("status", body.get("status")) != "committed"
            ):
                return False
            if phase == "resolve" and self.require_durable_decision:
                identity = self.transaction_identity(body)
                if identity is None:
                    return False
                txn_id, group = identity
                if (
                    txn_id not in self.committed_decisions
                    or self.transaction_coordinators.get(txn_id) == group
                ):
                    return False
            return transaction.get("phase") == phase or body.get(
                "_fault_path", ""
            ).endswith(f"/txn-{phase}")
        operation = body.get("operation", {})
        if self.window in ("publication", "snapshot"):
            return self.window in operation
        if self.window == "finalize":
            return operation.get("checkpoint", {}).get("kind") == "finalize"
        return "release" in body.get("_online_source", {})

    def block(self, index, body):
        with self.lock:
            if self.lose_first_committed_reply:
                return False
            if self.healed or not self.matches(body):
                return False
            self.hits.add(index)
            return True

    def observed(self):
        with self.lock:
            return set(self.hits)

    def failed_responses(self):
        with self.lock:
            return list(self.failed_owner_responses)

    def clear_observed(self):
        with self.lock:
            self.hits.clear()

    def heal(self):
        with self.lock:
            self.healed = True

    def drop_reply(self, index, body):
        with self.lock:
            if not self.drop_next_reply or not self.matches(body):
                return False
            self.drop_next_reply = False
            self.hits.add(index)
            self.reply_dropped.set()
            return True

    def heal_with_lost_reply(self):
        with self.lock:
            self.drop_next_reply = True
            self.healed = True


def test_owner_link_failed_response_diagnostics_survive_heal_and_stay_bounded():
    class Response:
        status_code = 400
        text = "x" * 300

    fault = OwnerLinkFault("publication")
    path = "/internal/v1/groups/7/tables/rows/txn-prepare"
    fault.observe_response(0, {"_fault_path": path}, Response())
    fault.heal()
    for index in range(130):
        fault.observe_response(index, {"_fault_path": path}, Response())
    observations = fault.failed_responses()
    assert len(observations) == 128
    assert observations[0][0] == 2
    assert observations[-1] == (129, path, 400, "x" * 256)
    fault.observe_response(0, {"_fault_path": "/unrelated"}, Response())
    Response.status_code = 200
    fault.observe_response(0, {"_fault_path": path}, Response())
    assert fault.failed_responses() == observations
    assert not fault.observed()
    assert not fault.transaction_observations


@pytest.fixture
def owner_link_fault(request, monkeypatch):
    fault = OwnerLinkFault(request.param)
    proxy_raft = request.node.callspec.params["crash"] == "raft_quorum"
    upstreams = [None] * 6

    def handler(index):
        class Proxy(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def setup(self):
                super().setup()
                self.upstream_session = requests.Session()

            def finish(self):
                self.upstream_session.close()
                super().finish()

            def forward(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                # Only inspect owner control traffic; Raft payloads pass
                # through unchanged unless the quorum outage is active.
                control = {}
                if index < 3 and self.command == "POST" and "/internal/" in self.path:
                    try:
                        control = json.loads(body)
                    except (ValueError, UnicodeDecodeError):
                        pass
                    if isinstance(control, dict):
                        # Test-only metadata: the original wire bytes below
                        # are forwarded unchanged to the actual owner.
                        control["_fault_path"] = self.path
                raft_blocked = index >= 3 and fault.raft_cut.is_set()
                if index < 3:
                    fault.observe_request(control)
                if raft_blocked:
                    fault.raft_seen.set()
                if raft_blocked or (index < 3 and fault.block(index, control)):
                    self.send_response(503)
                    self.send_header("Content-Length", "0")
                    self.send_header("Connection", "close")
                    self.end_headers()
                    return
                try:
                    with self.upstream_session.request(
                        self.command,
                        upstreams[index] + self.path,
                        data=body,
                        headers={
                            key: value
                            for key, value in self.headers.items()
                            if key.lower()
                            not in {"host", "connection", "content-length"}
                        },
                        timeout=10,
                    ) as response:
                        if index < 3:
                            fault.observe_response(index, control, response)
                        if (
                            index < 3
                            and 200 <= response.status_code < 300
                            and fault.drop_reply(index, control)
                        ):
                            # The owner accepted the effect. Close without its
                            # successful reply so recovery sees an ambiguous
                            # outcome, not a rejected request.
                            self.close_connection = True
                            return
                        self.send_response(response.status_code)
                        for key, value in response.headers.items():
                            if key.lower() not in {
                                "connection",
                                "content-length",
                                "content-encoding",
                                "transfer-encoding",
                            }:
                                self.send_header(key, value)
                        self.send_header("Content-Length", str(len(response.content)))
                        if index < 3:
                            self.send_header("Connection", "close")
                        self.end_headers()
                        self.wfile.write(response.content)
                except (
                    requests.RequestException,
                    BrokenPipeError,
                    ConnectionResetError,
                ):
                    # A killed owner or timed-out caller must experience a real
                    # ambiguous transport failure, not a fabricated success.
                    self.close_connection = True

            do_GET = forward
            do_POST = forward
            do_PUT = forward
            do_DELETE = forward

            def log_message(self, *args):
                pass

        return Proxy

    with ExitStack() as cleanup:
        urls = []
        for index in range(6 if proxy_raft else 3):
            server = ThreadingHTTPServer(("127.0.0.1", 0), handler(index))
            server.daemon_threads = False
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            cleanup.callback(thread.join)
            cleanup.callback(server.server_close)
            cleanup.callback(server.shutdown)
            urls.append(f"http://127.0.0.1:{server.server_port}")
        original = backups.ThreeByThreeBackupCluster._data_command

        def command(cluster, index):
            upstreams[index] = cluster.data_urls[index]
            upstreams[index + 3] = (
                f"http://{cluster.host}:{cluster.data_raft_ports[index]}"
            )
            result = [
                *original(cluster, index),
                "--api-advertise-url",
                urls[index],
            ]
            if proxy_raft:
                result.extend(["--raft-advertise-url", urls[index + 3]])
                # Use a realistic heartbeat interval through the Python test
                # proxy; 5ms stress ticks swamp scheduling and port budgets.
                result[result.index("--raft-tick-ms") + 1] = "50"
                result[result.index("--control-tick-ms") + 1] = "50"
            return result

        monkeypatch.setattr(backups.ThreeByThreeBackupCluster, "_data_command", command)
        try:
            yield fault
        finally:
            print(f"restore owner progress: {list(fault.owner_progress)}")


@pytest.fixture
def faulted_merge_cluster(owner_link_fault, three_by_three_backup_cluster):
    return three_by_three_backup_cluster


@pytest.mark.parametrize(
    "owner_link_fault,crash",
    [
        ("publication", "coordinator"),
        ("snapshot", "owner"),
        ("finalize", "owner"),
        ("release", "coordinator"),
        ("snapshot", "raft_quorum"),
        ("release", "reply_loss"),
    ],
    indirect=["owner_link_fault"],
)
def test_online_merge_recovers_after_owner_link_outage_and_crash(
    faulted_merge_cluster, owner_link_fault, crash
):
    fault = owner_link_fault

    def interrupt(cluster, table_id, donor, receiver, table_name, documents):
        assert wait_until(fault.observed, timeout_s=90, interval_s=0.1), (
            f"never reached {fault.window}\n{cluster.debug_logs()}"
        )
        leader = cluster.metadata_stable_leader_id(timeout_s=30)
        assert leader is not None, cluster.debug_logs()
        snapshot = cluster.metadata_snapshot(leader - 1, request_timeout_s=3)
        transition = next(
            value
            for value in snapshot["merge_transitions"]
            if int(value["donor_group_id"]) == donor
            and int(value["receiver_group_id"]) == receiver
        )
        online = transition["online"]
        assert online is not None and online["phase"] not in ("cancelled", "complete")
        live = {
            int(value["group_id"])
            for value in snapshot["ranges"]
            if int(value["table_id"]) == table_id
        }
        # A lost source-release link happens AFTER atomic routing publication;
        # all earlier interruptions must leave donor routing intact.
        assert (donor not in live) == (fault.window == "release"), transition
        assert receiver in live
        if fault.window == "snapshot":
            # The immutable source already exists, and copy requests are held.
            # Exercise real retained effects, including replacement of a row
            # in the source image, before failover or loss of owner quorum.
            tail = {
                "0:small": {"title": "updated while copying"},
                "0:tail": {"title": "inserted after the immutable cut"},
            }
            with requests.Session() as session:
                backups._seed_cluster_docs_when_writable(
                    cluster, session, table_name, tail
                )
            documents.update(tail)
        if crash == "reply_loss":
            fault.heal_with_lost_reply()
            assert fault.reply_dropped.wait(30), (
                "release never returned a successful reply"
            )
            return online
        if crash == "raft_quorum":
            fault.raft_cut.set()
            try:
                assert fault.raft_seen.wait(10), "no Raft traffic crossed fault proxy"
                # Release the owner gate while *every* advertised owner Raft
                # link is unavailable. Controllers remain alive and can issue
                # RPCs, but cannot commit receiver progress or routing cutover.
                fault.heal()
                time.sleep(3)
                cluster.assert_processes_alive()
                current = cluster.metadata_stable_leader_id(timeout_s=30)
                assert current is not None
                blocked = cluster.metadata_snapshot(current - 1, request_timeout_s=3)
                live = {
                    int(value["group_id"])
                    for value in blocked["ranges"]
                    if int(value["table_id"]) == table_id
                }
                assert donor in live and receiver in live and len(live) == 3
            finally:
                fault.raft_cut.clear()
            return online
        metadata = crash == "coordinator"
        if metadata:
            index = leader - 1
        else:
            current = cluster.metadata_stable_leader_id(timeout_s=30)
            assert current is not None
            snapshot = cluster.metadata_snapshot(current - 1, request_timeout_s=3)
            group = donor if fault.window == "snapshot" else receiver
            status = next(
                value
                for value in snapshot["merged_group_statuses"]
                if int(value["group_id"]) == group
            )
            assert status["leader_known"], status
            index = int(status["leader_store_id"]) - 4
            assert 0 <= index < len(cluster.data_procs), status
        procs = cluster.metadata_procs if metadata else cluster.data_procs
        procs[index].kill()
        procs[index].wait(timeout=10)
        fault.clear_observed()
        try:
            if metadata:
                successor = cluster.metadata_stable_leader_id(timeout_s=30)
                assert successor is not None and successor != leader, (
                    cluster.debug_logs()
                )
            else:
                assert wait_until(
                    lambda: fault.observed() - {index}, timeout_s=45, interval_s=0.1
                ), f"owner never failed over\n{cluster.debug_logs()}"
            # Keep all matching owner links unavailable across the election.
            # The operation must remain durably online, never fall back to the
            # ordinary merge or publish unverified data on a transport error.
            current = cluster.metadata_stable_leader_id(timeout_s=30)
            assert current is not None
            blocked = cluster.metadata_snapshot(current - 1, request_timeout_s=3)
            recovered = next(
                value
                for value in blocked["merge_transitions"]
                if int(value["donor_group_id"]) == donor
                and int(value["receiver_group_id"]) == receiver
            )
            assert recovered["online"]["scope"] == online["scope"]
            assert recovered["online"]["phase"] not in ("cancelled", "complete")
        finally:
            cluster.restart_crashed_node(metadata=metadata, index=index)
            fault.heal()
        return online

    backups._exercise_online_document_merge(
        faulted_merge_cluster, after_accept=interrupt
    )


@pytest.mark.parametrize(
    "owner_link_fault,crash",
    [("snapshot", "owner"), ("release", "reply_loss"), ("snapshot", "child_owner")],
    indirect=["owner_link_fault"],
)
def test_online_fk_merge_preserves_shadow_claims_and_retained_references(
    faulted_merge_cluster, owner_link_fault, crash
):
    """Actual UNIQUE owners move FK references; neither side is a fake catalog."""
    cluster, fault = faulted_merge_cluster, owner_link_fault
    merge_child = crash == "child_owner"
    parent = f"online_fk_parent_{time.time_ns()}"
    child = f"online_fk_child_{time.time_ns()}"
    schema = {
        "storage_mode": "relational",
        "default_type": "row",
        "unique_constraints": [{"name": "pk", "columns": ["id"]}],
        "document_schemas": {
            "row": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "title": {"type": "string"},
                        "payload": {"type": "string"},
                    },
                    "required": ["id"],
                    "additionalProperties": False,
                }
            }
        },
    }
    documents = {
        "0:large": {"id": 1, "title": "pinned", "payload": "x" * 16384},
        "0:small": {"id": 2, "title": "before tail"},
        "8:base": {"id": 3, "title": "receiver base"},
        "z:untouched": {"id": 4, "title": "other range"},
        **{f"0:parent:{value}": {"id": value} for value in range(5, 37)},
    }
    children = {}
    deleted_parent = "0:parent:5"
    deleted_child = deleted_parent if merge_child else "z:child:5"
    child_schema = dict(schema)
    child_schema.pop("unique_constraints")
    child_schema["foreign_keys"] = [
        {
            "name": "parent_fk",
            "child_columns": ["id"],
            "parent_table": parent,
            "parent_columns": ["id"],
            "on_delete": "cascade",
        }
    ]

    def setup_children(owner, session, table, rows):
        backups._create_cluster_table_when_admitted(
            owner, session, child, {"num_shards": 3, "schema": child_schema}
        )
        assert wait_until(lambda: owner.fully_replicated_topology(child), timeout_s=90)

        def ready():
            response = session.get(
                f"{owner.data_api_urls[0]}/tables/{child}/constraints/status", timeout=5
            )
            return (
                response.status_code == 200
                and response.json().get("state") == "enforced"
            )

        assert wait_until(ready, timeout_s=90), owner.debug_logs()
        children.update(
            {f"z:child:{row['id']}": {"id": row["id"]} for row in rows.values()}
        )
        backups._seed_cluster_docs_when_writable(owner, session, child, children)

    def interrupt(owner, table_id, donor, receiver, table, rows):
        assert wait_until(fault.observed, timeout_s=90, interval_s=0.1), (
            owner.debug_logs()
        )
        leader = owner.metadata_stable_leader_id(timeout_s=30)
        assert leader is not None, owner.debug_logs()
        state = owner.metadata_snapshot(leader - 1)
        transition = next(
            value
            for value in state["merge_transitions"]
            if int(value["donor_group_id"]) == donor
            and int(value["receiver_group_id"]) == receiver
        )
        online = transition["online"]
        assert online is not None and online["phase"] not in ("complete", "cancelled")
        if fault.window == "release":
            fault.heal_with_lost_reply()
            assert fault.reply_dropped.wait(30), owner.debug_logs()
            return online
        with requests.Session() as session:
            session.headers["Connection"] = "close"
            tail = {"0:small": {"id": 2, "title": "updated after certified cut"}}
            tail.update({f"0:tail:{value}": {"id": value} for value in range(100, 112)})
            if merge_child:
                backups._seed_cluster_docs_when_writable(owner, session, parent, tail)
            backups._seed_cluster_docs_when_writable(owner, session, table, tail)
            rows.update(tail)
            references = (
                tail
                if merge_child
                else {
                    f"8:tail-child:{value}": {"id": value} for value in range(100, 112)
                }
            )
            if not merge_child:
                backups._seed_cluster_docs_when_writable(
                    owner, session, child, references
                )
            children.update(references)
            removed = session.post(
                f"{owner.data_api_urls[0]}/tables/{table}/batch",
                json={"deletes": [deleted_parent], "sync_level": "write"},
                timeout=30,
            )
            assert removed.status_code in (200, 201, 202), removed.text
            rows.pop(deleted_parent)
            children.pop(deleted_child)
        # Kill the current donor leader only after the source has retained
        # inserts, replacement, cascading deletes and companion reference effects.
        latest = owner.metadata_snapshot(
            owner.metadata_stable_leader_id(timeout_s=30) - 1
        )
        status = next(
            value
            for value in latest["merged_group_statuses"]
            if int(value["group_id"]) == donor
        )
        assert status["leader_known"], status
        index = int(status["leader_store_id"]) - 4
        assert 0 <= index < len(owner.data_procs)
        owner.data_procs[index].kill()
        owner.data_procs[index].wait(timeout=10)
        fault.clear_observed()
        try:
            assert wait_until(
                lambda: fault.observed() - {index}, timeout_s=45, interval_s=0.1
            ), owner.debug_logs()
        finally:
            owner.restart_crashed_node(metadata=False, index=index)
            fault.heal()
        return online

    if merge_child:
        with requests.Session() as session:
            session.headers["Connection"] = "close"
            backups._create_cluster_table_when_admitted(
                cluster, session, parent, {"num_shards": 3, "schema": schema}
            )
            assert wait_until(
                lambda: cluster.fully_replicated_topology(parent), timeout_s=90
            )

            def ready_parent():
                response = session.get(
                    f"{cluster.data_api_urls[0]}/tables/{parent}/constraints/status",
                    timeout=5,
                )
                return (
                    response.status_code == 200
                    and response.json().get("state") == "enforced"
                )

            assert wait_until(ready_parent, timeout_s=90), cluster.debug_logs()
            backups._seed_cluster_docs_when_writable(
                cluster, session, parent, documents
            )
        children.update(documents)
    backups._exercise_online_document_merge(
        cluster,
        table_name=child if merge_child else parent,
        schema=child_schema if merge_child else schema,
        documents=documents,
        before_merge=None if merge_child else setup_children,
        after_accept=interrupt,
    )
    with requests.Session() as session:
        session.headers["Connection"] = "close"
        for key, row in children.items():
            actual = wait_until(
                lambda key=key: backups._lookup_doc_from_url(
                    session, cluster.data_api_urls[0], child, key
                ),
                timeout_s=30,
            )
            assert actual is not None and actual["id"] == row["id"], (key, actual)
        if fault.window == "snapshot":
            absent = [(child, deleted_child)]
            if not merge_child:
                absent.append((parent, deleted_parent))
            for table, key in absent:
                response = session.get(
                    f"{cluster.data_api_urls[0]}/tables/{table}/documents/{key}",
                    timeout=10,
                )
                assert response.status_code == 404, response.text
        # Probe every live tuple: routing hashes spread claims over all source
        # ranges, so row-only preservation cannot accidentally satisfy this.
        for row in documents.values():
            duplicate = session.post(
                f"{cluster.data_api_urls[0]}/tables/{parent}/batch",
                json={"inserts": {"8:duplicate": {"id": row["id"]}}},
                timeout=20,
            )
            assert duplicate.status_code == 409, duplicate.text
        orphan = session.post(
            f"{cluster.data_api_urls[0]}/tables/{child}/batch",
            json={"inserts": {"8:orphan": {"id": 999999}}},
            timeout=20,
        )
        assert orphan.status_code == 409, orphan.text
        # Imported references must also drive a new post-cutover action job.
        cascade = session.post(
            f"{cluster.data_api_urls[0]}/tables/{parent}/batch",
            json={"deletes": ["0:small"], "sync_level": "write"},
            timeout=30,
        )
        assert cascade.status_code in (200, 201, 202), cascade.text
        cascade_child = "0:small" if merge_child else "z:child:2"
        assert wait_until(
            lambda: (
                session.get(
                    f"{cluster.data_api_urls[0]}/tables/{child}/documents/{cascade_child}",
                    timeout=5,
                ).status_code
                == 404
            ),
            timeout_s=60,
        ), cluster.debug_logs()
