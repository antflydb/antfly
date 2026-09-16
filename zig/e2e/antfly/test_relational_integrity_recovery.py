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

"""Real routed UNIQUE/FK recovery, using owner-link faults and durable Raft."""

import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests

import test_backup_restore as backups
import test_online_merge_recovery as merge_faults
from helpers import wait_until

three_by_three_backup_cluster = backups.three_by_three_backup_cluster
owner_link_fault = merge_faults.owner_link_fault


@pytest.mark.parametrize(
    "status,body,retry",
    [
        (503, "write unavailable", True),
        (409, "batch transaction conflicted", True),
        (409, "unique constraint violation", False),
        (503, "transaction outcome unknown", False),
        (500, "internal error", False),
    ],
)
def test_integrity_seed_only_retries_explicit_precommit_abort(status, body, retry):
    class Cluster:
        data_api_urls = ("http://unused.invalid",)

        @staticmethod
        def assert_processes_alive():
            pass

        @staticmethod
        def debug_logs():
            return ""

    class Session:
        calls = 0

        def post(self, *_args, **_kwargs):
            self.calls += 1
            response = requests.Response()
            response.status_code = status if self.calls == 1 else 201
            response._content = body.encode() if self.calls == 1 else b'{"inserted":1}'
            response.url = "http://unused.invalid"
            response.request = requests.Request("POST", response.url).prepare()
            return response

    session = Session()
    if retry:
        assert backups._seed_cluster_docs_when_writable(
            Cluster(), session, "rows", {"key": {"id": 1}}, timeout_s=1
        ) == {"inserted": 1}
        assert session.calls == 2
    else:
        with pytest.raises(AssertionError):
            backups._seed_cluster_docs_when_writable(
                Cluster(), session, "rows", {"key": {"id": 1}}, timeout_s=1
            )
        assert session.calls == 1


def test_transaction_fault_requires_observed_durable_coordinator_decision():
    fault = merge_faults.OwnerLinkFault("transaction_resolve")
    fault.require_durable_decision = True

    def control(group, phase, txn="target"):
        return {
            "txn_id": txn,
            "status": "committed",
            "_fault_path": f"/internal/v1/groups/{group}/tables/parent/txn-{phase}",
        }

    fault.observe_request(control(11, "begin"))
    fault.observe_request(control(12, "begin"))
    assert not fault.block(0, control(11, "resolve"))
    assert not fault.block(1, control(12, "resolve"))
    rejected = requests.Response()
    rejected.status_code = 503
    fault.observe_response(0, control(11, "resolve"), rejected)
    assert not fault.block(1, control(12, "resolve"))
    accepted = requests.Response()
    accepted.status_code = 200
    fault.observe_response(0, control(11, "resolve"), accepted)
    assert not fault.block(0, control(11, "resolve"))
    assert not fault.block(1, control(12, "resolve", "unrelated"))
    assert fault.block(1, control(12, "resolve"))
    fault.clear_observed()
    fault.lose_first_committed_reply = True
    fault.drop_next_reply = True
    assert not fault.block(1, control(12, "resolve"))
    assert not fault.drop_reply(0, control(11, "resolve"))
    assert fault.drop_reply(1, control(12, "resolve"))
    assert fault.reply_dropped.is_set()
    assert fault.observed() == {1}
    assert not fault.drop_reply(1, control(12, "resolve"))


@pytest.fixture
def integrity_cluster(owner_link_fault, three_by_three_backup_cluster):
    return three_by_three_backup_cluster


def _schema(parent=None):
    result = {
        "storage_mode": "relational",
        "default_type": "row",
        "document_schemas": {
            "row": {
                "schema": {
                    "type": "object",
                    "properties": {"id": {"type": "integer"}},
                    "required": ["id"],
                    "additionalProperties": False,
                }
            }
        },
    }
    if parent is None:
        result["unique_constraints"] = [{"name": "pk", "columns": ["id"]}]
    else:
        result["foreign_keys"] = [
            {
                "name": "parent_fk",
                "child_columns": ["id"],
                "parent_table": parent,
                "parent_columns": ["id"],
                "on_delete": "cascade",
            }
        ]
    return result


def _assert_constraint_rejected(cluster, session, table, inserts):
    observations = []

    def rejected():
        try:
            response = session.post(
                f"{cluster.data_api_urls[0]}/tables/{table}/batch",
                json={"inserts": inserts},
                timeout=20,
            )
        except requests.RequestException as exc:
            observations.append(str(exc))
            return False
        observations.append(f"{response.status_code}: {response.text[:1024]}")
        if response.status_code == 409:
            return True
        # A recovered cluster can still be electing individual group leaders.
        # A success is always a correctness failure, never retried or hidden.
        assert response.status_code in (503, 504), response.text
        return False

    assert wait_until(rejected, timeout_s=90, interval_s=0.5), (
        f"constraint never rejected the invalid write: {observations[-8:]}\n{cluster.debug_logs()}"
    )


@pytest.mark.parametrize(
    "owner_link_fault,crash",
    [("transaction_resolve", "owner"), ("transaction_resolve", "reply_loss")],
    indirect=["owner_link_fault"],
)
def test_fk_cascade_recovers_claims_references_and_rows(
    integrity_cluster, owner_link_fault, crash
):
    cluster, fault = integrity_cluster, owner_link_fault
    fault.heal()  # Only the test mutation, not activation/seed, is faulted.
    parent, child = (f"integrity_{kind}_{time.time_ns()}" for kind in ("p", "c"))
    with requests.Session() as session:
        session.headers.update({"Connection": "close"})
        for table, schema in ((parent, _schema()), (child, _schema(parent))):
            backups._create_cluster_table_when_admitted(
                cluster, session, table, {"num_shards": 3, "schema": schema}
            )
            assert wait_until(
                lambda table=table: cluster.table_is_fully_replicated(table),
                timeout_s=90,
            ), cluster.debug_logs()

            def enforced(table=table):
                response = session.get(
                    f"{cluster.data_api_urls[0]}/tables/{table}/constraints/status",
                    timeout=5,
                )
                if response.status_code == 200:
                    return response.json().get("state") == "enforced"
                assert response.status_code in (409, 503), response.text
                return False

            assert wait_until(enforced, timeout_s=90), cluster.debug_logs()
        backups._seed_cluster_docs_when_writable(
            cluster, session, parent, {"0:parent": {"id": 17}}
        )
        children = {f"{prefix}:child": {"id": 17} for prefix in ("0", "8", "z")}
        backups._seed_cluster_docs_when_writable(cluster, session, child, children)

        with fault.lock:
            fault.healed = False
            fault.hits.clear()
            fault.require_durable_decision = True
            if crash == "reply_loss":
                # Deliver the first post-decision participant effect and lose
                # its successful reply directly, not a prior rejected attempt.
                fault.lose_first_committed_reply = True
                fault.drop_next_reply = True
        # Coordinate from metadata, which owns no data group. Every participant
        # resolution must cross the proxy even if all groups elect one owner.
        mutation_api = cluster.metadata_leader_public_url(timeout_s=30)

        def delete_parent():
            # Only explicit pre-commit unavailability permits a new attempt.
            # Neither transport loss nor post-decision 503 may be replayed.
            deadline = time.monotonic() + 60
            while True:
                result = requests.post(
                    f"{mutation_api}/tables/{parent}/batch",
                    json={"deletes": ["0:parent"], "sync_level": "write"},
                    timeout=max(0.001, deadline - time.monotonic()),
                    headers={"Connection": "close"},
                )
                if (
                    result.status_code != 503
                    or result.text.strip() != "write unavailable"
                    or time.monotonic() >= deadline
                ):
                    return result
                time.sleep(0.1)

        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(delete_parent)
            wait_until(
                lambda: fault.observed() or pending.done(),
                timeout_s=60,
                interval_s=0.05,
            )
            observed = fault.observed()
            if not observed:
                result = pending.result(timeout=65)
                pytest.fail(
                    f"never reached post-decision resolution: response={result.status_code} {result.text}\n"
                    f"transaction transport={fault.transaction_observations}\n"
                    f"{cluster.debug_logs()}"
                )
            if crash == "reply_loss":
                assert fault.reply_dropped.is_set(), (
                    "no accepted resolution reply dropped"
                )
                fault.heal()
            else:
                # Kill an owner reached by the transaction protocol, then
                # reopen its existing logs. Do not synthesize storage effects.
                index = next(iter(fault.observed()))
                cluster.data_procs[index].kill()
                cluster.data_procs[index].wait(timeout=10)
                try:
                    fault.heal()
                finally:
                    cluster.restart_crashed_node(metadata=False, index=index)
            try:
                result = pending.result(timeout=65)
                assert result.status_code in (200, 201, 202, 503), result.text
            except requests.RequestException:
                # Killing the frontend can lose the response after a durable
                # commit. The externally observed database below is the proof.
                pass

        parent_keys = ["0:parent"]

        def all_absent():
            for base in cluster.data_api_urls:
                for table, keys in ((parent, parent_keys), (child, children)):
                    for key in keys:
                        try:
                            response = session.get(
                                f"{base}/tables/{table}/documents/{key}", timeout=5
                            )
                        except requests.RequestException:
                            # Read-only observation can race process restart
                            # and ReadIndex recovery; the write is never retried.
                            return False
                        assert response.status_code in (200, 404, 503, 504), (
                            f"{response.status_code} {response.text}"
                        )
                        if response.status_code != 404:
                            return False
            return True

        assert wait_until(all_absent, timeout_s=90), cluster.debug_logs()
        # Reusing the exact logical tuple detects leaked claim/reference
        # ownership. Bad row counts alone would miss those invisible records.
        backups._seed_cluster_docs_when_writable(
            cluster, session, parent, {"z:replacement": {"id": 17}}
        )
        backups._seed_cluster_docs_when_writable(cluster, session, child, children)
        _assert_constraint_rejected(
            cluster, session, parent, {"8:duplicate": {"id": 17}}
        )
        _assert_constraint_rejected(cluster, session, child, {"8:orphan": {"id": 999}})
        final = session.post(
            f"{cluster.data_api_urls[0]}/tables/{parent}/batch",
            json={"deletes": ["z:replacement"], "sync_level": "write"},
            timeout=30,
        )
        assert final.status_code in (200, 201, 202), final.text
        parent_keys.append("z:replacement")
        assert wait_until(all_absent, timeout_s=60), cluster.debug_logs()


@pytest.mark.parametrize(
    "owner_link_fault,crash",
    [("publication", "coordinator"), ("publication", "reply_loss")],
    indirect=["owner_link_fault"],
)
def test_schema_rewrite_recovers_dependency_cohort(
    integrity_cluster, owner_link_fault, crash
):
    """Recover an admitted online rewrite, not a synthetic staging-only restore."""
    cluster, fault = integrity_cluster, owner_link_fault
    fault.heal()
    parent, child = (f"rewrite_{kind}_{time.time_ns()}" for kind in ("p", "c"))
    source = _schema()
    shape = source["document_schemas"]["row"]["schema"]
    shape["properties"].update({"x": {"type": "integer"}, "g": {"type": "integer"}})
    shape["required"].extend(["x", "g"])

    def generated(op, value):
        return [
            {
                "column": "g",
                "expression": {
                    "op": op,
                    "args": [
                        {"op": "column", "column": "x"},
                        {"op": "literal", "type": "integer", "value": value},
                    ],
                },
            }
        ]

    source["generated_columns"] = generated("add", 1)
    with requests.Session() as session:
        session.headers.update({"Connection": "close"})
        for table, schema in ((parent, source), (child, _schema(parent))):
            backups._create_cluster_table_when_admitted(
                cluster, session, table, {"num_shards": 3, "schema": schema}
            )
            assert wait_until(
                lambda table=table: cluster.table_is_fully_replicated(table),
                timeout_s=90,
            ), cluster.debug_logs()

            def enforced(table=table):
                response = session.get(
                    f"{cluster.data_api_urls[0]}/tables/{table}/constraints/status",
                    timeout=5,
                )
                return (
                    response.status_code == 200
                    and response.json().get("state") == "enforced"
                )

            assert wait_until(enforced, timeout_s=90), cluster.debug_logs()
        rows = {
            f"{prefix}:parent": {"id": i, "x": i + 10}
            for i, prefix in enumerate(("0", "8", "z"), 1)
        }
        children = {
            key.replace("parent", "child"): {"id": value["id"]}
            for key, value in rows.items()
        }
        backups._seed_cluster_docs_when_writable(cluster, session, parent, rows)
        backups._seed_cluster_docs_when_writable(cluster, session, child, children)
        with fault.lock:
            fault.healed = False
            fault.hits.clear()
        leader_id = cluster.metadata_stable_leader_id(timeout_s=30)
        assert leader_id is not None
        admitted = session.patch(
            f"{cluster.metadata_public_urls[leader_id - 1]}/tables/{parent}/schema",
            params={"rewrite": "true"},
            json={"generated_columns": generated("multiply", 3)},
            headers={"Idempotency-Key": f"rewrite-{parent}"},
            timeout=30,
        )
        assert admitted.status_code == 202, f"{admitted.text}\n{cluster.debug_logs()}"
        job_id = admitted.json()["job_id"]
        assert wait_until(fault.observed, timeout_s=90), cluster.debug_logs()
        # Writes acknowledged while the immutable cut is held must survive the
        # generation switch, and must be recomputed with the target expression.
        rows["0:parent"]["x"] = 41
        backups._seed_cluster_docs_when_writable(
            cluster, session, parent, {"0:parent": rows["0:parent"]}
        )
        for key, row in rows.items():
            response = session.get(
                f"{cluster.data_api_urls[0]}/tables/{parent}/documents/{key}", timeout=5
            )
            assert response.status_code == 200, response.text
            assert response.json()["g"] == row["x"] + 1, response.text

        if crash == "reply_loss":
            fault.heal_with_lost_reply()
            assert fault.reply_dropped.wait(30), "no accepted publication reply dropped"
        else:
            cluster.metadata_procs[leader_id - 1].kill()
            cluster.metadata_procs[leader_id - 1].wait(timeout=10)
            try:
                successor = cluster.metadata_stable_leader_id(timeout_s=30)
                assert successor is not None and successor != leader_id, (
                    cluster.debug_logs()
                )
                recovered = session.get(
                    f"{cluster.metadata_public_urls[successor - 1]}/restore/jobs/{job_id}",
                    timeout=10,
                )
                assert recovered.status_code == 200, recovered.text
                assert recovered.json()["phase"] not in (
                    "failed",
                    "cancelled",
                    "succeeded",
                )
            finally:
                cluster.restart_crashed_node(metadata=True, index=leader_id - 1)
                fault.heal()

        observations = {}

        def terminal():
            cluster.assert_processes_alive()
            for base in cluster.metadata_public_urls:
                try:
                    response = session.get(f"{base}/restore/jobs/{job_id}", timeout=2)
                except requests.RequestException:
                    continue
                observations[base] = response.text[:4096]
                if response.status_code == 200:
                    job = response.json()
                    if job.get("phase") in ("succeeded", "failed", "cancelled"):
                        return job
            return None

        completed = wait_until(terminal, timeout_s=180)
        assert completed and completed["phase"] == "succeeded", (
            f"job={completed}, observations={observations}, "
            f"owner_failures={fault.failed_responses()}\n{cluster.debug_logs()}"
        )
        assert completed["result"]["committed_table_count"] == 2, completed
        for base in cluster.data_api_urls:
            for key, row in rows.items():
                response = session.get(
                    f"{base}/tables/{parent}/documents/{key}", timeout=5
                )
                assert response.status_code == 200, response.text
                assert response.json()["g"] == row["x"] * 3, response.text
        orphan = session.post(
            f"{cluster.data_api_urls[0]}/tables/{child}/batch",
            json={"inserts": {"8:orphan": {"id": 999}}},
            timeout=20,
        )
        assert orphan.status_code == 409, orphan.text
        duplicate = session.post(
            f"{cluster.data_api_urls[0]}/tables/{parent}/batch",
            json={"inserts": {"8:duplicate": {"id": 1, "x": 2}}},
            timeout=20,
        )
        assert duplicate.status_code == 409, duplicate.text
        deleted = session.post(
            f"{cluster.data_api_urls[0]}/tables/{parent}/batch",
            json={"deletes": ["0:parent"], "sync_level": "write"},
            timeout=30,
        )
        assert deleted.status_code in (200, 201, 202), deleted.text
        for base in cluster.data_api_urls:
            missing = session.get(f"{base}/tables/{child}/documents/0:child", timeout=5)
            assert missing.status_code == 404, missing.text
