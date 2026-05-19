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

"""Stateful public API transaction tests."""

from __future__ import annotations

import threading
import time

import pytest
import requests

from helpers import wait_until


NUM_SHARDS = 4


def _transform(key: str, *operations: dict[str, object], upsert: bool = False) -> dict[str, object]:
    payload: dict[str, object] = {
        "key": key,
        "operations": list(operations),
    }
    if upsert:
        payload["upsert"] = True
    return payload


def _op(op: str, path: str, value: object | None = None) -> dict[str, object]:
    payload: dict[str, object] = {"op": op, "path": path}
    if value is not None:
        payload["value"] = value
    return payload


class StatelessTransaction:
    def __init__(self, stateful_api):
        self.stateful_api = stateful_api
        self.read_set: list[dict[str, object]] = []

    def read(self, table_name: str, key: str) -> dict:
        read_result = wait_until(
            lambda: _lookup_with_version(self.stateful_api, table_name, key),
            timeout_s=10.0,
            interval_s=0.1,
        )
        assert read_result is not None, f"lookup did not converge for {table_name}/{key}"
        doc, version = read_result
        assert version is not None, "lookup must return X-Antfly-Version for OCC"
        self.read_set.append(
            {
                "table": table_name,
                "key": key,
                "version": version,
            }
        )
        return doc

    def commit(self, tables: dict[str, dict], *, sync_level: str | None = None) -> tuple[int, dict]:
        return self.stateful_api.commit_transaction(
            read_set=self.read_set,
            tables=tables,
            sync_level=sync_level,
        )


def test_multi_batch_commit(stateful_api):
    table_a, table_b = _create_tables(stateful_api, "multi_batch_commit")

    result = stateful_api.multi_batch(
        {
            table_a: {
                "inserts": {
                    "user:1": {"name": "Alice", "email": "alice@example.com"},
                    "user:2": {"name": "Bob", "email": "bob@example.com"},
                },
                "sync_level": "write",
            },
            table_b: {
                "inserts": {
                    "order:1": {"user": "user:1", "item": "widget", "qty": 5},
                    "order:2": {"user": "user:2", "item": "gadget", "qty": 3},
                },
                "sync_level": "write",
            },
        }
    )

    assert result["tables"][table_a]["inserted"] == 2
    assert result["tables"][table_b]["inserted"] == 2

    assert stateful_api.lookup_key(table_a, "user:1")["name"] == "Alice"
    assert stateful_api.lookup_key(table_a, "user:2")["name"] == "Bob"
    assert stateful_api.lookup_key(table_b, "order:1")["item"] == "widget"
    assert stateful_api.lookup_key(table_b, "order:2")["item"] == "gadget"


def test_multi_shard_batch_commit(stateful_api):
    table_name = _create_table(stateful_api, "multi_shard_batch")

    initial_docs = {
        "0_account_a": {"name": "Alice", "balance": 1000},
        "4_account_b": {"name": "Bob", "balance": 500},
        "8_account_c": {"name": "Charlie", "balance": 750},
        "c_account_d": {"name": "Diana", "balance": 250},
    }
    seeded = stateful_api.batch_write(table_name, inserts=initial_docs, sync_level="write")
    assert seeded["inserted"] == 4

    visible_initial = wait_until(
        lambda: _lookup_many(stateful_api, table_name, list(initial_docs.keys())),
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert visible_initial == initial_docs

    updated_docs = {
        "0_account_a": {"name": "Alice", "balance": 1100},
        "4_account_b": {"name": "Bob", "balance": 600},
        "8_account_c": {"name": "Charlie", "balance": 650},
        "c_account_d": {"name": "Diana", "balance": 150},
    }
    result = stateful_api.batch_write(table_name, inserts=updated_docs, sync_level="write")
    assert result["inserted"] == 4

    visible_updated = wait_until(
        lambda: _lookup_many(stateful_api, table_name, list(updated_docs.keys())),
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert visible_updated == updated_docs


def test_atomic_multi_key_update_preserves_balance_sum(stateful_api):
    table_name = _create_table(stateful_api, "atomic_transfer")

    initial_docs = {
        "0_alice": {"name": "Alice", "balance": 1000},
        "8_bob": {"name": "Bob", "balance": 0},
    }
    seeded = stateful_api.batch_write(table_name, inserts=initial_docs, sync_level="write")
    assert seeded["inserted"] == 2

    alice = stateful_api.lookup_key(table_name, "0_alice")
    bob = stateful_api.lookup_key(table_name, "8_bob")
    initial_sum = alice["balance"] + bob["balance"]
    assert initial_sum == 1000

    transferred_docs = {
        "0_alice": {"name": "Alice", "balance": 500},
        "8_bob": {"name": "Bob", "balance": 500},
    }
    result = stateful_api.batch_write(table_name, inserts=transferred_docs, sync_level="write")
    assert result["inserted"] == 2

    updated_docs = wait_until(
        lambda: _lookup_many(stateful_api, table_name, ["0_alice", "8_bob"]),
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert updated_docs is not None
    updated_alice = updated_docs["0_alice"]
    updated_bob = updated_docs["8_bob"]
    assert updated_alice == transferred_docs["0_alice"]
    assert updated_bob == transferred_docs["8_bob"]

    final_sum = updated_alice["balance"] + updated_bob["balance"]
    assert final_sum == initial_sum


def test_multi_shard_transaction_recovery_health(stateful_api):
    table_name = _create_table(stateful_api, "multi_shard_recovery")

    docs = {
        "0_recovery_a": {"name": "Alpha", "value": 100},
        "8_recovery_b": {"name": "Beta", "value": 200},
    }
    first_result = stateful_api.batch_write(table_name, inserts=docs, sync_level="write")
    assert first_result["inserted"] == 2

    visible_first = wait_until(
        lambda: _lookup_many(stateful_api, table_name, list(docs.keys())),
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert visible_first == docs

    time.sleep(2.0)

    visible_after_pause = wait_until(
        lambda: _lookup_many(stateful_api, table_name, list(docs.keys())),
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert visible_after_pause == docs

    more_docs = {
        "4_recovery_c": {"name": "Gamma", "value": 300},
        "c_recovery_d": {"name": "Delta", "value": 400},
    }
    second_result = stateful_api.batch_write(table_name, inserts=more_docs, sync_level="write")
    assert second_result["inserted"] == 2

    all_docs = {}
    all_docs.update(docs)
    all_docs.update(more_docs)
    visible_all = wait_until(
        lambda: _lookup_many(stateful_api, table_name, list(all_docs.keys())),
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert visible_all == all_docs


def test_multi_shard_batch_timeout_preserves_atomicity(stateful_api):
    table_name = _create_table(stateful_api, "multi_shard_timeout")

    initial_docs = {
        "0_account_a": {"name": "Alice", "balance": 1000},
        "4_account_b": {"name": "Bob", "balance": 500},
        "8_account_c": {"name": "Charlie", "balance": 750},
        "c_account_d": {"name": "Diana", "balance": 250},
    }
    seeded = stateful_api.batch_write(table_name, inserts=initial_docs, sync_level="write")
    assert seeded["inserted"] == 4

    updated_docs = {
        "0_account_a": {"name": "Alice_UPDATED", "balance": 9999},
        "4_account_b": {"name": "Bob_UPDATED", "balance": 9999},
        "8_account_c": {"name": "Charlie_UPDATED", "balance": 9999},
        "c_account_d": {"name": "Diana_UPDATED", "balance": 9999},
    }

    timed_out = False
    try:
        stateful_api.batch_write_with_timeout(
            table_name,
            inserts=updated_docs,
            sync_level="write",
            timeout_s=0.001,
        )
    except requests.Timeout:
        timed_out = True

    current_docs = wait_until(
        lambda: _lookup_many(stateful_api, table_name, list(initial_docs.keys())),
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert current_docs is not None
    all_original = current_docs == initial_docs
    all_updated = current_docs == updated_docs

    if timed_out:
        if not (all_original or all_updated):
            converged = wait_until(
                lambda: _lookup_converged_many(stateful_api, table_name, initial_docs, updated_docs),
                timeout_s=10.0,
                interval_s=0.1,
            )
            assert converged is not None, "timed-out multi-shard batch left partial visible state"
    else:
        assert all_updated


def test_multi_batch_mixed_ops(stateful_api):
    table_a, table_b = _create_tables(stateful_api, "multi_batch_mixed")

    seeded = stateful_api.batch_write(
        table_b,
        inserts={"order:old": {"user": "user:0", "item": "legacy", "qty": 1}},
        sync_level="write",
    )
    assert seeded["inserted"] == 1

    result = stateful_api.multi_batch(
        {
            table_a: {
                "inserts": {
                    "user:3": {"name": "Charlie"},
                },
                "sync_level": "write",
            },
            table_b: {
                "deletes": ["order:old"],
                "sync_level": "write",
            },
        }
    )

    assert result["tables"][table_a]["inserted"] == 1
    assert result["tables"][table_b]["deleted"] == 1

    assert stateful_api.lookup_key(table_a, "user:3")["name"] == "Charlie"
    with pytest.raises(requests.HTTPError):
        stateful_api.lookup_key(table_b, "order:old")


def test_multi_batch_abort_on_invalid_table(stateful_api):
    table_a, _ = _create_tables(stateful_api, "multi_batch_abort")

    with pytest.raises(requests.HTTPError):
        stateful_api.multi_batch(
            {
                table_a: {
                    "inserts": {
                        "user:phantom": {"name": "Phantom"},
                    },
                    "sync_level": "write",
                },
                "nonexistent_table": {
                    "inserts": {
                        "key:1": {"data": "should fail"},
                    },
                    "sync_level": "write",
                },
            }
        )

    with pytest.raises(requests.HTTPError):
        stateful_api.lookup_key(table_a, "user:phantom")


def test_multi_batch_transforms(stateful_api):
    table_a, table_b = _create_tables(stateful_api, "multi_batch_transforms")

    stateful_api.batch_write(
        table_a,
        inserts={"user:1": {"name": "Alice", "visits": 1}},
        sync_level="write",
    )
    stateful_api.batch_write(
        table_b,
        inserts={"order:1": {"item": "widget", "version": 1}},
        sync_level="write",
    )

    result = stateful_api.multi_batch(
        {
            table_a: {
                "transforms": [
                    _transform("user:1", _op("$inc", "visits", 2), _op("$set", "status", "active"))
                ],
                "sync_level": "write",
            },
            table_b: {
                "transforms": [
                    _transform("order:1", _op("$max", "version", 3), _op("$set", "status", "rebuilt"))
                ],
                "sync_level": "write",
            },
        }
    )

    assert result["tables"][table_a]["transformed"] == 1
    assert result["tables"][table_b]["transformed"] == 1
    assert stateful_api.lookup_key(table_a, "user:1")["visits"] == 3
    assert stateful_api.lookup_key(table_a, "user:1")["status"] == "active"
    assert stateful_api.lookup_key(table_b, "order:1")["version"] == 3
    assert stateful_api.lookup_key(table_b, "order:1")["status"] == "rebuilt"


def test_occ_basic_read_modify_write(stateful_api):
    table_name = _create_table(stateful_api, "occ_basic")

    seeded = stateful_api.batch_write(
        table_name,
        inserts={"account:alice": {"name": "Alice", "balance": 1000}},
        sync_level="write",
    )
    assert seeded["inserted"] == 1

    tx = StatelessTransaction(stateful_api)
    doc = tx.read(table_name, "account:alice")
    assert doc["name"] == "Alice"

    status, result = tx.commit(
        {
            table_name: {
                "inserts": {
                    "account:alice": {"name": "Alice", "balance": doc["balance"] + 500},
                },
                "sync_level": "write",
            }
        }
    )
    assert status == 200
    assert result["status"] == "committed"

    updated = stateful_api.lookup_key(table_name, "account:alice")
    assert updated["balance"] == 1500


def test_occ_conflict_detection(stateful_api):
    table_name = _create_table(stateful_api, "occ_conflict")

    seeded = stateful_api.batch_write(
        table_name,
        inserts={"account:shared": {"name": "Shared", "balance": 1000}},
        sync_level="write",
    )
    assert seeded["inserted"] == 1

    tx1 = StatelessTransaction(stateful_api)
    tx2 = StatelessTransaction(stateful_api)

    doc1 = tx1.read(table_name, "account:shared")
    doc2 = tx2.read(table_name, "account:shared")
    assert doc1["balance"] == 1000
    assert doc2["balance"] == 1000

    commit_status, commit_result = tx1.commit(
        {
            table_name: {
                "inserts": {
                    "account:shared": {"name": "Shared", "balance": doc1["balance"] + 100},
                },
                "sync_level": "write",
            }
        }
    )
    assert commit_status == 200
    assert commit_result["status"] == "committed"

    abort_status, abort_result = tx2.commit(
        {
            table_name: {
                "inserts": {
                    "account:shared": {"name": "Shared", "balance": doc2["balance"] + 200},
                },
                "sync_level": "write",
            }
        }
    )
    assert abort_status == 409
    assert abort_result["status"] == "aborted"
    assert abort_result["conflict"]["table"] == table_name
    assert abort_result["conflict"]["key"] == "account:shared"

    final_doc = stateful_api.lookup_key(table_name, "account:shared")
    assert final_doc["balance"] == 1100


def test_occ_cross_table_read_modify_write(stateful_api):
    table_a, table_b = _create_tables(stateful_api, "occ_cross_table")

    seeded = stateful_api.batch_write(
        table_a,
        inserts={"user:1": {"name": "Alice", "order_count": 0}},
        sync_level="write",
    )
    assert seeded["inserted"] == 1

    tx = StatelessTransaction(stateful_api)
    user = tx.read(table_a, "user:1")
    assert user["order_count"] == 0

    status, result = tx.commit(
        {
            table_a: {
                "inserts": {
                    "user:1": {"name": "Alice", "order_count": user["order_count"] + 1},
                },
                "sync_level": "write",
            },
            table_b: {
                "inserts": {
                    "order:100": {"user": "user:1", "item": "widget", "qty": 3},
                },
                "sync_level": "write",
            },
        }
    )
    assert status == 200
    assert result["status"] == "committed"
    assert result["tables"][table_a]["inserted"] == 1
    assert result["tables"][table_b]["inserted"] == 1

    updated_user = stateful_api.lookup_key(table_a, "user:1")
    assert updated_user["order_count"] == 1

    order = stateful_api.lookup_key(table_b, "order:100")
    assert order["item"] == "widget"


def test_occ_transform_commit(stateful_api):
    table_name = _create_table(stateful_api, "occ_transform")

    stateful_api.batch_write(
        table_name,
        inserts={"account:alice": {"name": "Alice", "balance": 1000, "version": 1}},
        sync_level="write",
    )

    tx = StatelessTransaction(stateful_api)
    doc = tx.read(table_name, "account:alice")
    assert doc["balance"] == 1000

    status, result = tx.commit(
        {
            table_name: {
                "transforms": [
                    _transform(
                        "account:alice",
                        _op("$inc", "balance", 250),
                        _op("$max", "version", 2),
                    )
                ],
                "sync_level": "write",
            }
        }
    )
    assert status == 200
    assert result["status"] == "committed"
    assert result["tables"][table_name]["transformed"] == 1

    updated = stateful_api.lookup_key(table_name, "account:alice")
    assert updated["balance"] == doc["balance"] + 250
    assert updated["version"] == 2


def test_occ_lost_update_protection(stateful_api):
    table_name = _create_table(stateful_api, "occ_lost_update")

    stateful_api.batch_write(
        table_name,
        inserts={"counter:bug": {"value": 0}},
        sync_level="write",
    )

    read_barrier = threading.Barrier(3)
    commit_barrier = threading.Barrier(3)
    results: list[str | None] = [None, None]
    read_values: list[int | None] = [None, None]
    errors: list[Exception] = []

    def worker(worker_id: int) -> None:
        try:
            tx = StatelessTransaction(stateful_api)
            doc = tx.read(table_name, "counter:bug")
            read_values[worker_id] = int(doc["value"])
            read_barrier.wait(timeout=5)
            commit_barrier.wait(timeout=5)
            status, result = tx.commit(
                {
                    table_name: {
                        "inserts": {
                            "counter:bug": {"value": int(doc["value"]) + 1},
                        },
                        "sync_level": "write",
                    }
                }
            )
            if status == 200:
                results[worker_id] = result["status"]
            else:
                results[worker_id] = result["status"]
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(worker_id,)) for worker_id in range(2)]
    for thread in threads:
        thread.start()

    read_barrier.wait(timeout=5)
    assert read_values == [0, 0]
    commit_barrier.wait(timeout=5)

    for thread in threads:
        thread.join()

    assert not errors
    assert results.count("committed") == 1
    assert results.count("aborted") == 1

    doc = stateful_api.lookup_key(table_name, "counter:bug")
    assert doc["value"] == 1


def test_occ_concurrent_rmw_only_one_commits(stateful_api):
    table_name = _create_table(stateful_api, "occ_concurrent_rmw")

    stateful_api.batch_write(
        table_name,
        inserts={"counter:1": {"value": 0}},
        sync_level="write",
    )

    worker_count = 5
    read_barrier = threading.Barrier(worker_count + 1)
    commit_barrier = threading.Barrier(worker_count + 1)
    results: list[str | None] = [None] * worker_count
    errors: list[Exception] = []

    def worker(worker_id: int) -> None:
        try:
            tx = StatelessTransaction(stateful_api)
            doc = tx.read(table_name, "counter:1")
            assert int(doc["value"]) == 0
            read_barrier.wait(timeout=5)
            commit_barrier.wait(timeout=5)
            status, result = tx.commit(
                {
                    table_name: {
                        "inserts": {
                            "counter:1": {"value": int(doc["value"]) + 1},
                        },
                        "sync_level": "write",
                    }
                }
            )
            if status == 200:
                results[worker_id] = result["status"]
            else:
                results[worker_id] = result["status"]
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(worker_id,)) for worker_id in range(worker_count)]
    for thread in threads:
        thread.start()

    read_barrier.wait(timeout=5)
    commit_barrier.wait(timeout=5)

    for thread in threads:
        thread.join()

    assert not errors
    assert results.count("committed") == 1
    assert results.count("aborted") == worker_count - 1

    doc = stateful_api.lookup_key(table_name, "counter:1")
    assert doc["value"] == 1


def test_session_stage_transform_commit(stateful_api):
    table_name = _create_table(stateful_api, "session_transform")

    stateful_api.batch_write(
        table_name,
        inserts={"account:alice": {"name": "Alice", "balance": 1000, "status": "pending"}},
        sync_level="write",
    )

    session = stateful_api.begin_transaction_session(sync_level="write")
    txn_id = session["transaction_id"]

    staged = stateful_api.stage_transaction_session(
        txn_id,
        tables={
            table_name: {
                "transforms": [
                    _transform(
                        "account:alice",
                        _op("$inc", "balance", 250),
                        _op("$set", "status", "active"),
                    )
                ]
            }
        },
    )
    assert staged["status"] == "staged"
    assert staged["transaction_id"] == txn_id

    status, result = stateful_api.commit_transaction_session(txn_id)
    assert status == 200
    assert result["status"] == "committed"
    assert result["transaction_id"] == txn_id
    assert result["tables"][table_name]["transformed"] == 1

    updated = stateful_api.lookup_key(table_name, "account:alice")
    assert updated["balance"] == 1250
    assert updated["status"] == "active"


def test_session_transform_savepoint_rollback(stateful_api):
    table_name = _create_table(stateful_api, "session_savepoint_transform")

    stateful_api.batch_write(
        table_name,
        inserts={"account:alice": {"name": "Alice", "balance": 1000}},
        sync_level="write",
    )

    session = stateful_api.begin_transaction_session(sync_level="write")
    txn_id = session["transaction_id"]

    staged_first = stateful_api.stage_transaction_session(
        txn_id,
        tables={
            table_name: {
                "transforms": [
                    _transform("account:alice", _op("$inc", "balance", 100))
                ]
            }
        },
    )
    assert staged_first["status"] == "staged"

    savepoint = stateful_api.create_transaction_savepoint(txn_id)
    assert savepoint["status"] == "savepoint_created"
    assert savepoint["transaction_id"] == txn_id
    savepoint_id = savepoint["savepoint_id"]

    staged_second = stateful_api.stage_transaction_session(
        txn_id,
        tables={
            table_name: {
                "transforms": [
                    _transform(
                        "account:alice",
                        _op("$inc", "balance", 50),
                        _op("$set", "rolled_back", True),
                    )
                ]
            }
        },
    )
    assert staged_second["status"] == "staged"

    rolled_back = stateful_api.rollback_transaction_savepoint(txn_id, savepoint_id)
    assert rolled_back["status"] == "rolled_back"
    assert rolled_back["transaction_id"] == txn_id
    assert rolled_back["savepoint_id"] == savepoint_id

    status, result = stateful_api.commit_transaction_session(txn_id)
    assert status == 200
    assert result["status"] == "committed"
    assert result["tables"][table_name]["transformed"] == 1

    updated = stateful_api.lookup_key(table_name, "account:alice")
    assert updated["balance"] == 1100
    assert "rolled_back" not in updated


def test_session_explicit_read_write_commit(stateful_api):
    table_name = _create_table(stateful_api, "session_explicit_rw")

    stateful_api.batch_write(
        table_name,
        inserts={"doc:1": {"title": "alpha", "version": 1}},
        sync_level="write",
    )

    _, version = stateful_api.lookup_key_with_version(table_name, "doc:1")
    assert version is not None

    session = stateful_api.begin_transaction_session(sync_level="write")
    txn_id = session["transaction_id"]

    read_status, read_result = stateful_api.stage_transaction_read(
        txn_id,
        table_name=table_name,
        key="doc:1",
        version=version,
    )
    assert read_status == 200
    assert read_result["status"] == "staged"
    assert read_result["snapshot"]["table"] == table_name
    assert read_result["snapshot"]["key"] == "doc:1"
    assert read_result["snapshot"]["version"] == version
    assert read_result["snapshot"]["document"]["title"] == "alpha"

    staged_write = stateful_api.stage_transaction_write(
        txn_id,
        table_name=table_name,
        key="doc:2",
        document={"title": "beta", "copied_from": "doc:1"},
    )
    assert staged_write["status"] == "staged"
    assert staged_write["transaction_id"] == txn_id

    status, result = stateful_api.commit_transaction_session(txn_id)
    assert status == 200
    assert result["status"] == "committed"
    assert result["tables"][table_name]["inserted"] == 1

    inserted = stateful_api.lookup_key(table_name, "doc:2")
    assert inserted["title"] == "beta"
    assert inserted["copied_from"] == "doc:1"


def test_session_stage_read_version_conflict(stateful_api):
    table_name = _create_table(stateful_api, "session_read_conflict")

    stateful_api.batch_write(
        table_name,
        inserts={"doc:1": {"title": "alpha", "version": 1}},
        sync_level="write",
    )

    _, original_version = stateful_api.lookup_key_with_version(table_name, "doc:1")
    assert original_version is not None

    session = stateful_api.begin_transaction_session(sync_level="write")
    txn_id = session["transaction_id"]

    stateful_api.batch_write(
        table_name,
        inserts={"doc:1": {"title": "beta", "version": 2}},
        sync_level="write",
    )

    conflict_status, conflict = stateful_api.stage_transaction_read(
        txn_id,
        table_name=table_name,
        key="doc:1",
        version=original_version,
    )
    assert conflict_status == 409
    assert conflict["status"] == "conflict"
    assert conflict["transaction_id"] == txn_id
    assert conflict["conflict"]["table"] == table_name
    assert conflict["conflict"]["key"] == "doc:1"
    assert conflict["conflict"]["kind"] == "version_conflict"
    assert conflict["conflict"]["message"] == "version conflict"
    assert str(conflict["conflict"]["expected_version"]) == original_version
    assert str(conflict["conflict"]["current_version"]) != original_version


def test_session_stage_delete_abort(stateful_api):
    table_name = _create_table(stateful_api, "session_delete_abort")

    stateful_api.batch_write(
        table_name,
        inserts={"doc:1": {"title": "alpha"}, "doc:2": {"title": "beta"}},
        sync_level="write",
    )

    session = stateful_api.begin_transaction_session(sync_level="write")
    txn_id = session["transaction_id"]

    staged_delete = stateful_api.stage_transaction_delete(
        txn_id,
        table_name=table_name,
        key="doc:1",
    )
    assert staged_delete["status"] == "staged"
    assert staged_delete["transaction_id"] == txn_id

    aborted = stateful_api.abort_transaction_session(txn_id)
    assert aborted["status"] == "aborted"
    assert aborted["transaction_id"] == txn_id

    preserved = stateful_api.lookup_key(table_name, "doc:1")
    assert preserved["title"] == "alpha"
    assert stateful_api.lookup_key(table_name, "doc:2")["title"] == "beta"


def test_session_stage_delete_commit(stateful_api):
    table_name = _create_table(stateful_api, "session_delete_commit")

    stateful_api.batch_write(
        table_name,
        inserts={"doc:1": {"title": "alpha"}, "doc:2": {"title": "beta"}},
        sync_level="write",
    )

    session = stateful_api.begin_transaction_session(sync_level="write")
    txn_id = session["transaction_id"]

    staged_delete = stateful_api.stage_transaction_delete(
        txn_id,
        table_name=table_name,
        key="doc:1",
    )
    assert staged_delete["status"] == "staged"
    assert staged_delete["transaction_id"] == txn_id

    status, result = stateful_api.commit_transaction_session(txn_id)
    assert status == 200
    assert result["status"] == "committed"
    assert result["tables"][table_name]["deleted"] == 1

    with pytest.raises(requests.HTTPError):
        stateful_api.lookup_key(table_name, "doc:1")
    assert stateful_api.lookup_key(table_name, "doc:2")["title"] == "beta"


def test_session_mixed_read_write_delete_commit(stateful_api):
    table_name = _create_table(stateful_api, "session_mixed")

    stateful_api.batch_write(
        table_name,
        inserts={
            "doc:1": {"title": "alpha", "version": 1},
            "doc:2": {"title": "beta"},
        },
        sync_level="write",
    )

    _, version = stateful_api.lookup_key_with_version(table_name, "doc:1")
    assert version is not None

    session = stateful_api.begin_transaction_session(sync_level="write")
    txn_id = session["transaction_id"]

    read_status, read_result = stateful_api.stage_transaction_read(
        txn_id,
        table_name=table_name,
        key="doc:1",
        version=version,
    )
    assert read_status == 200
    assert read_result["snapshot"]["document"]["title"] == "alpha"

    staged_write = stateful_api.stage_transaction_write(
        txn_id,
        table_name=table_name,
        key="doc:3",
        document={"title": "gamma", "source": "doc:1"},
    )
    assert staged_write["status"] == "staged"

    staged_delete = stateful_api.stage_transaction_delete(
        txn_id,
        table_name=table_name,
        key="doc:2",
    )
    assert staged_delete["status"] == "staged"

    status, result = stateful_api.commit_transaction_session(txn_id)
    assert status == 200
    assert result["status"] == "committed"
    assert result["tables"][table_name]["inserted"] == 1
    assert result["tables"][table_name]["deleted"] == 1

    preserved = stateful_api.lookup_key(table_name, "doc:1")
    assert preserved["title"] == "alpha"
    inserted = stateful_api.lookup_key(table_name, "doc:3")
    assert inserted["title"] == "gamma"
    assert inserted["source"] == "doc:1"
    with pytest.raises(requests.HTTPError):
        stateful_api.lookup_key(table_name, "doc:2")


def _create_table(stateful_api, prefix: str) -> str:
    table_name = f"{prefix}_{time.time_ns()}"
    created = stateful_api.create_table(table_name, num_shards=NUM_SHARDS)
    assert created["name"] == table_name
    return table_name


def _create_tables(stateful_api, prefix: str) -> tuple[str, str]:
    suffix = time.time_ns()
    table_a = f"{prefix}_{suffix}_a"
    table_b = f"{prefix}_{suffix}_b"

    created_a = stateful_api.create_table(table_a, num_shards=NUM_SHARDS)
    assert created_a["name"] == table_a

    created_b = stateful_api.create_table(table_b, num_shards=NUM_SHARDS)
    assert created_b["name"] == table_b

    return table_a, table_b


def _lookup_with_version(stateful_api, table_name: str, key: str) -> tuple[dict, str | None] | None:
    try:
        return stateful_api.lookup_key_with_version(table_name, key)
    except requests.HTTPError:
        return None


def _lookup_many(stateful_api, table_name: str, keys: list[str]) -> dict[str, dict] | None:
    docs: dict[str, dict] = {}
    for key in keys:
        try:
            docs[key] = stateful_api.lookup_key(table_name, key)
        except requests.HTTPError:
            return None
    return docs


def _lookup_converged_many(
    stateful_api,
    table_name: str,
    expected_original: dict[str, dict],
    expected_updated: dict[str, dict],
) -> dict[str, dict] | None:
    docs = _lookup_many(stateful_api, table_name, list(expected_original.keys()))
    if docs is None:
        return None
    if docs == expected_original or docs == expected_updated:
        return docs
    return None
