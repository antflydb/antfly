# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""One real-process qualification of the opaque metadata/API ownership boundary."""

import json
import os
import time
from pathlib import Path

import pytest
import requests
from conftest import DEFAULT_ANTFLY_BIN, StatefulAntflyServer, resolve_binary_path


def test_distributed_population_and_lock_ownership_survive_restart():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).is_file():
        pytest.skip(f"Antfly binary not found: {binary}")
    server = StatefulAntflyServer(binary, "127.0.0.1", 0)
    failed = True
    try:

        def sql(statement, connection=None):
            body = {"statement": statement}
            if connection is not None:
                body["connection_id"] = connection
            deadline = time.monotonic() + 30
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    pytest.fail("SQL request budget expired before admission")
                # Safe preparation retries share one outer budget. Leave room
                # for durable completion and response delivery on every attempt.
                response = requests.post(
                    server.api_url + "/sql",
                    json=body,
                    headers={"x-antfly-timeout-ms": str(max(1, int(remaining * 500)))},
                    timeout=remaining,
                )
                result = response.json()
                # Listening precedes initial catalog publication. Only these
                # explicit pre-admission witnesses permit resubmission; a lost
                # reply, active transaction or any receipt never does.
                witness = result.get("message", "")
                not_admitted = (
                    connection is None
                    and response.status_code == 503
                    and result.get("code") == "53300"
                    and result.get("retryable") is True
                    and result.get("transaction_status") == "idle"
                    and all(
                        result.get(key) is None
                        for key in (
                            "transaction_id",
                            "mutation_outcome",
                            "admission_outcome",
                            "status",
                            "ddl_receipt",
                            "catalog_receipt",
                        )
                    )
                    and (
                        witness.startswith(
                            "The relation catalog publication is not ready to authorize "
                            "this DDL. No schema change was admitted."
                        )
                        or (
                            statement.startswith("SELECT ")
                            and witness.startswith(
                                "A consistent SQL statement read is temporarily unavailable."
                            )
                        )
                    )
                )
                if not not_admitted or time.monotonic() >= deadline:
                    break
                time.sleep(0.05)
            assert response.status_code in (200, 202), (
                statement,
                response.status_code,
                result,
                server.debug_logs(),
            )
            receipt = result.get("ddl_receipt") or {}
            if job := receipt.get("restore_job_id"):
                deadline = time.monotonic() + 45
                while True:
                    status = requests.get(
                        server.metadata_admin_url + "/db/v1/restore/jobs/" + job,
                        timeout=5,
                    )
                    assert status.status_code == 200, (status.text, server.debug_logs())
                    state = status.json()
                    assert state["job_id"] == job
                    assert state["idempotency_key"] == receipt["idempotency_key"]
                    if state["phase"] == "succeeded":
                        break
                    assert state["phase"] not in ("failed", "cancelled"), (
                        state,
                        server.debug_logs(),
                    )
                    assert time.monotonic() < deadline, (state, server.debug_logs())
                    time.sleep(0.05)
            return result

        def rows(table, status):
            result = sql(f"SELECT id, status FROM {table} ORDER BY id")
            assert len(result["rows"]) == 2, result
            assert [int(row[0]) for row in result["rows"]] == [
                9007199254740993,
                9007199254740994,
            ], result
            assert [row[1] for row in result["rows"]] == [status, status], result

        sql("CREATE TABLE boundary_source(id BIGINT PRIMARY KEY, status TEXT)")
        assert sql("SELECT id FROM boundary_source")["rows"] == []
        sql(
            "INSERT INTO boundary_source VALUES"
            "(9007199254740993,'original'),(9007199254740994,'original')"
        )
        sql("CREATE TABLE boundary_copy AS SELECT id, status FROM boundary_source")
        sql(
            "CREATE MATERIALIZED VIEW boundary_view AS SELECT id, status FROM boundary_source"
        )
        rows("boundary_copy", "original")
        sql("UPDATE boundary_source SET status='refreshed'")
        sql("REFRESH MATERIALIZED VIEW boundary_view")
        rows("boundary_view", "refreshed")
        connection_response = requests.post(
            server.api_url + "/sql/connections", json={}, timeout=10
        )
        assert connection_response.status_code == 200, connection_response.text
        connection = connection_response.json()["connection_id"]
        sql("BEGIN", connection)
        locked = sql("SELECT id FROM boundary_source FOR UPDATE", connection)
        assert len(locked["rows"]) == 2, locked
        sql("COMMIT", connection)
        other_response = requests.post(
            server.api_url + "/sql/connections", json={}, timeout=10
        )
        assert other_response.status_code == 200, other_response.text
        other = other_response.json()["connection_id"]
        inventory_path = (
            Path(__file__).resolve().parents[2]
            / "pkg/antfly-embedded/src/sql/fixtures/sql_parity_inventory.json"
        )
        originals = {
            entry["id"]: entry["sql"]
            for entry in json.loads(inventory_path.read_text())["entries"]
        }

        def unavailable(statement, owner):
            response = requests.post(
                server.api_url + "/sql",
                json={"statement": statement, "connection_id": owner},
                timeout=10,
            )
            assert response.status_code >= 400, response.text
            assert response.json()["code"] == "55P03", response.text

        sql("CREATE TABLE usage_records(id TEXT PRIMARY KEY, status TEXT)")
        sql("CREATE TABLE archived_records(id TEXT PRIMARY KEY, status TEXT)")
        assert sql("SELECT id FROM usage_records")["rows"] == []
        sql("INSERT INTO usage_records VALUES('a','queued'),('b','queued')")
        # All ten valid original lock statements cross the real production API
        # and storage archives, rather than builtin.is_test's direct bridge.
        for identifier in ("sql-0080", "sql-0081"):
            sql("BEGIN", connection)
            sql(originals[identifier], connection)
            sql("BEGIN", other)
            unavailable(
                "LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE NOWAIT", other
            )
            sql("ROLLBACK", other)
            sql("ROLLBACK", connection)
        sql("BEGIN", connection)
        sql("SELECT id FROM usage_records WHERE id='a' FOR UPDATE", connection)
        sql("BEGIN", other)
        assert sql(originals["sql-0173"], other)["rows"] == [["b"]]
        sql("ROLLBACK", other)
        sql("ROLLBACK", connection)
        for identifier in ("sql-0433", "sql-0434", "sql-0435", "sql-0436"):
            sql("BEGIN", connection)
            assert len(sql(originals[identifier], connection)["rows"]) == 2
            sql("BEGIN", other)
            unavailable(originals["sql-0434"], other)
            sql("ROLLBACK", other)
            sql("ROLLBACK", connection)
        sql("BEGIN", connection)
        sql("LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE", connection)
        for statement, explicit in (
            ("SELECT id FROM usage_records", False),
            ("SELECT id FROM usage_records FOR UPDATE", False),
            ("SELECT id FROM usage_records", True),
            ("SELECT id FROM usage_records FOR UPDATE", True),
        ):
            for attempt in range(3):
                if explicit:
                    sql("BEGIN", other)
                blocked = requests.post(
                    server.api_url + "/sql",
                    json={"statement": statement, "connection_id": other},
                    headers={"x-antfly-timeout-ms": "5000"},
                    timeout=15,
                )
                diagnostic = blocked.json()
                # Retry these side-effect-free lock probes only after known
                # cleanup. Explicit transactions keep their failed status until
                # ROLLBACK. An uncertain decision must fail this test.
                assert diagnostic.get("transaction_status") == (
                    "failed" if explicit else "idle"
                ), blocked.text
                for key in (
                    "transaction_id",
                    "mutation_outcome",
                    "admission_outcome",
                    "status",
                    "ddl_receipt",
                    "catalog_receipt",
                ):
                    assert diagnostic.get(key) is None, blocked.text
                if explicit:
                    sql("ROLLBACK", other)
                if diagnostic.get("code") == "53300" and attempt < 2:
                    assert blocked.status_code == 503, blocked.text
                    assert diagnostic.get("retryable") is True, blocked.text
                    if not explicit:
                        assert diagnostic.get("message", "").startswith(
                            "A consistent SQL statement read is temporarily unavailable."
                        ), blocked.text
                    continue
                assert diagnostic.get("code") == "57014", blocked.text
                break
        sql("ROLLBACK", connection)
        assert sql("SELECT id FROM usage_records ORDER BY id", other)["rows"] == [
            ["a"],
            ["b"],
        ]
        sql(originals["sql-0089"], connection)
        sql(originals["sql-0090"], connection)
        assert sql(originals["sql-0091"], connection)["rows"] == [[True]]
        assert sql("SELECT pg_advisory_unlock(-42, 7)", connection)["rows"] == [[True]]
        prepared_response = requests.post(
            server.api_url + "/sql/connections", json={}, timeout=10
        )
        assert prepared_response.status_code == 200, prepared_response.text
        prepared_connection = prepared_response.json()["connection_id"]
        sql("BEGIN", prepared_connection)
        sql("SELECT id FROM usage_records WHERE id='a' FOR UPDATE", prepared_connection)
        sql(
            "UPDATE usage_records SET status='prepared' WHERE id='a'",
            prepared_connection,
        )
        sql("LOCK TABLE archived_records IN SHARE MODE", prepared_connection)
        sql("PREPARE TRANSACTION 'process_lock_custody'", prepared_connection)
        closed_source = requests.delete(
            server.api_url + "/sql/connections/" + prepared_connection, timeout=10
        )
        assert closed_source.status_code in (200, 204), closed_source.text
        sql("SELECT pg_advisory_lock(432109)", connection)
        assert sql("SELECT pg_try_advisory_lock(432109)", other)["rows"] == [[False]]
        server.restart()
        rows("boundary_copy", "original")
        rows("boundary_view", "refreshed")
        # Frozen native row/table custody survives both source connection
        # retirement and a complete process restart; resolution must not need
        # that connection or its live lock-registration lease.
        sql("BEGIN", other)
        unavailable(
            "SELECT id FROM usage_records WHERE id='a' FOR UPDATE NOWAIT", other
        )
        sql("ROLLBACK", other)
        sql("BEGIN", other)
        unavailable(
            "LOCK TABLE archived_records IN ACCESS EXCLUSIVE MODE NOWAIT", other
        )
        sql("ROLLBACK", other)
        sql("COMMIT PREPARED 'process_lock_custody'", other)
        assert sql("SELECT status FROM usage_records WHERE id='a'", other)["rows"] == [
            ["prepared"]
        ]
        sql("BEGIN", other)
        sql("LOCK TABLE archived_records IN ACCESS EXCLUSIVE MODE NOWAIT", other)
        sql("COMMIT", other)
        # The same public connection IDs and native session lock survive an
        # actual metadata/data process restart, without fixture-injected stores.
        assert sql("SELECT pg_try_advisory_lock(432109)", other)["rows"] == [[False]]
        assert sql("SELECT pg_advisory_unlock(432109)", connection)["rows"] == [[True]]
        assert sql("SELECT pg_try_advisory_lock(432109)", other)["rows"] == [[True]]
        sql("SELECT pg_advisory_unlock_all()", other)
        for identifier in (connection, other):
            closed = requests.delete(
                server.api_url + "/sql/connections/" + identifier, timeout=10
            )
            assert closed.status_code in (200, 204), closed.text
        failed = False
    finally:
        server.stop(test_failed=failed)
