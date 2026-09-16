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

"""Constrained sessions across real three-metadata/three-data-node owners."""

import time

import requests

import test_backup_restore as backups
from helpers import wait_until

three_by_three_backup_cluster = backups.three_by_three_backup_cluster


def _schema(parent=None, *, deferred=False):
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
                "deferrable": True,
                "timing": "deferred" if deferred else "immediate",
                "on_delete": "no_action" if deferred else "cascade",
                "on_update": "no_action" if deferred else "cascade",
            }
        ]
    return result


def _ready(cluster, session, table):
    def enforced():
        response = session.get(
            f"{cluster.data_api_urls[0]}/tables/{table}/constraints/status", timeout=10
        )
        if response.status_code == 200:
            return response.json().get("state") == "enforced"
        assert response.status_code in (409, 503, 504), response.text
        return False

    assert wait_until(lambda: cluster.table_is_fully_replicated(table), timeout_s=90), (
        cluster.debug_logs()
    )
    assert wait_until(enforced, timeout_s=90), cluster.debug_logs()


def _post(session, url, body=None, *, status=200):
    response = session.post(url, json={} if body is None else body, timeout=45)
    assert response.status_code == status, (
        f"POST {url}: {response.status_code} {response.text}"
    )
    return response.json()


def _begin(session, base):
    result = _post(
        session, f"{base}/transactions/begin", {"sync_level": "write"}, status=201
    )
    return f"{base}/transactions/{result['transaction_id']}"


def _write(session, transaction, table, key, value, *, status=200):
    return _post(
        session,
        f"{transaction}/write",
        {"table": table, "key": key, "document": {"id": value}},
        status=status,
    )


def _details(session, transaction):
    response = session.get(transaction, timeout=15)
    assert response.status_code == 200, response.text
    return response.json()


def _commit(cluster, session, transaction):
    observations = []

    def committed():
        # Only this exact sealed session ID is retried. A 202/transport loss
        # must never cause another transaction to replay the mutation.
        try:
            response = session.post(f"{transaction}/commit", json={}, timeout=45)
        except requests.RequestException as error:
            observations.append(str(error))
            return False
        observations.append(f"{response.status_code}: {response.text[:1024]}")
        assert response.status_code in (200, 202, 503, 504), observations[-1]
        if response.status_code in (503, 504):
            return False
        state = response.json()["status"]
        assert state in (
            "committed",
            "committed_visibility_pending",
            "committed_recovery_pending",
        ), observations[-1]
        return state == "committed"

    assert wait_until(committed, timeout_s=120, interval_s=0.5), (
        f"session did not finish: {observations[-8:]}\n{cluster.debug_logs()}"
    )


def _row(cluster, session, table, key, expected):
    def visible():
        for base in cluster.data_api_urls:
            response = session.get(f"{base}/tables/{table}/documents/{key}", timeout=10)
            if response.status_code in (503, 504):
                return False
            if expected is None:
                assert response.status_code in (200, 404), response.text
                if response.status_code != 404:
                    return False
            else:
                assert response.status_code in (200, 404), response.text
                if response.status_code != 200 or response.json().get("id") != expected:
                    return False
        return True

    assert wait_until(visible, timeout_s=60, interval_s=0.25), (
        f"unexpected row {table}/{key}, expected id={expected}\n{cluster.debug_logs()}"
    )


def test_constrained_sessions_defer_reject_and_rollback_across_owners(
    three_by_three_backup_cluster,
):
    cluster = three_by_three_backup_cluster
    suffix = time.time_ns()
    parent, deferred, immediate = (
        f"session_{kind}_{suffix}" for kind in ("parent", "deferred", "immediate")
    )
    base = cluster.data_api_urls[0]
    with requests.Session() as session:
        session.headers.update({"Connection": "close"})
        for table, schema in (
            (parent, _schema()),
            (deferred, _schema(parent, deferred=True)),
            (immediate, _schema(parent)),
        ):
            backups._create_cluster_table_when_admitted(
                cluster, session, table, {"num_shards": 3, "schema": schema}
            )
            _ready(cluster, session, table)

        # The first statement is temporarily orphaned, but neither statement
        # is publicly visible until the same durable session commits.
        transaction = _begin(session, base)
        _write(session, transaction, deferred, "z:deferred-child", 7)
        _row(cluster, session, deferred, "z:deferred-child", None)
        _write(session, transaction, parent, "0:deferred-parent", 7)
        _row(cluster, session, parent, "0:deferred-parent", None)
        _commit(cluster, session, transaction)
        _row(cluster, session, parent, "0:deferred-parent", 7)
        _row(cluster, session, deferred, "z:deferred-child", 7)
        _commit(cluster, session, transaction)  # Stable terminal retry.

        # Immediate rejection must not contaminate staged rows or seal the
        # session. A later parent statement makes the child statement legal.
        transaction = _begin(session, base)
        before = _details(session, transaction)
        rejected = _write(
            session, transaction, immediate, "z:immediate-child", 17, status=409
        )
        assert rejected["error"] == "ForeignKeyParentMissing", rejected
        after = _details(session, transaction)
        for field in (
            "staged_table_count",
            "staged_write_count",
            "staged_delete_count",
        ):
            assert after[field] == before[field] == 0, after
        _write(session, transaction, parent, "0:immediate-parent", 17)
        _write(session, transaction, immediate, "z:immediate-child", 17)
        _commit(cluster, session, transaction)
        _row(cluster, session, immediate, "z:immediate-child", 17)

        # Cascades are staged primary writes too: rollback must discard both
        # the explicit parent update and generated child update together.
        transaction = _begin(session, base)
        _write(session, transaction, parent, "8:kept-parent", 29)
        savepoint = _post(session, f"{transaction}/savepoints")["savepoint_id"]
        _write(session, transaction, parent, "0:immediate-parent", 18)
        staged = _details(session, transaction)
        assert staged["staged_write_count"] == 3, staged
        assert staged["staged_table_count"] == 2, staged
        _row(cluster, session, parent, "0:immediate-parent", 17)
        _row(cluster, session, immediate, "z:immediate-child", 17)
        _post(session, f"{transaction}/savepoints/{savepoint}/rollback")
        restored = _details(session, transaction)
        assert restored["staged_write_count"] == 1, restored
        assert restored["staged_table_count"] == 1, restored
        _commit(cluster, session, transaction)
        _row(cluster, session, parent, "8:kept-parent", 29)
        _row(cluster, session, parent, "0:immediate-parent", 17)
        _row(cluster, session, immediate, "z:immediate-child", 17)

        # Finally commit consecutive staged cascades. The second statement
        # must observe 18, not recompute its transition from the live value 17.
        transaction = _begin(session, base)
        _write(session, transaction, parent, "0:immediate-parent", 18)
        _write(session, transaction, parent, "0:immediate-parent", 19)
        _commit(cluster, session, transaction)
        _row(cluster, session, parent, "0:immediate-parent", 19)
        _row(cluster, session, immediate, "z:immediate-child", 19)
