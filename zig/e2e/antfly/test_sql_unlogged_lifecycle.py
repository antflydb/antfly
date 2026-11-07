# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Original PostgreSQL UNLOGGED cases through real clean and killed processes."""

import json
import os
import signal
import time
from pathlib import Path

import pytest
import requests
from sql_observation_retry import definite_observation_abort
from conftest import (
    DEFAULT_ANTFLY_BIN,
    REPO_ROOT,
    StandaloneAntflyServer,
    resolve_binary_path,
)


REFERENCE = json.loads(
    (
        REPO_ROOT
        / "pkg/antfly-embedded/src/sql/fixtures/sql_persistence_reference.json"
    ).read_text()
)
CASES = [
    entry
    for entry in REFERENCE["entries"]
    if entry["id"] in {"sql-0362", "sql-0363", "sql-0368", "sql-0569"}
]
UUID = "00000000-0000-0000-0000-000000000009"


TEMP_CASES = [
    entry
    for entry in REFERENCE["entries"]
    if entry["id"] in {"sql-0360", "sql-0361", "sql-0367", "sql-0568"}
]


def _close_temp_owner(server, owner):
    response = requests.delete(server.api_url + "/sql/connections/" + owner, timeout=30)
    assert response.status_code in (200, 204), (response.text, server.debug_logs())


def _temp_absent(server, connection, table):
    response = requests.post(
        server.api_url + "/sql",
        json={
            "statement": f"SELECT id, status FROM {table}",
            "connection_id": connection,
        },
        timeout=30,
    )
    assert response.status_code >= 400 and response.json().get("code") == "42P01", (
        response.status_code,
        response.text,
        server.debug_logs(),
    )


def _temp_rows(server, sql, case, owner, expected):
    table = (
        "usage_session_archive" if "archive" in case["sql"] else "usage_session_records"
    )
    observed = sql(f"SELECT id, status FROM {table} ORDER BY id", owner, read=True)
    assert observed["rows"] == expected["rows"], (
        case["id"],
        observed,
        expected,
        server.debug_logs(),
    )
    assert [(column["name"], column["type"]) for column in observed["columns"]] == [
        (name, {2950: "uuid", 25: "string"}[oid]) for name, oid in expected["columns"]
    ], observed
    return table


def _create_temp_original(server, sql, connection, case):
    owner = connection()
    result = sql(case["sql"], owner)
    expected_tag = (
        "SELECT" if case["command_tag"].startswith("SELECT ") else case["command_tag"]
    )
    assert result["command_tag"] in (expected_tag, "DDL PENDING"), (case["id"], result)
    table = _temp_rows(server, sql, case, owner, case["populated"])
    sql(f"INSERT INTO {table} VALUES ('{UUID}', 'retained')", owner)
    _temp_rows(server, sql, case, owner, case["before_disconnect"])
    if case["duplicate_sqlstate"] is not None:
        duplicate = requests.post(
            server.api_url + "/sql",
            json={
                "statement": f"INSERT INTO {table} VALUES ('{UUID}', 'duplicate')",
                "connection_id": owner,
            },
            timeout=30,
        )
        assert (
            duplicate.status_code >= 400
            and duplicate.json().get("code") == case["duplicate_sqlstate"]
        ), (case["id"], duplicate.status_code, duplicate.text, server.debug_logs())
        _temp_rows(server, sql, case, owner, case["before_disconnect"])
    if "IF NOT EXISTS" in case["sql"]:
        # Retry the actual IF NOT EXISTS statement only as a new deliberate
        # operation. It must retain the prior populated private relation.
        sql(case["sql"], owner)
        _temp_rows(server, sql, case, owner, case["before_disconnect"])
    other = connection()
    try:
        _temp_absent(server, other, table)
    finally:
        _close_temp_owner(server, other)
    return owner, table


def _prepare_temp_disconnect_and_crash(server, sql, connection):
    # Run all four unchanged TEMP originals inside the existing sql-0362
    # process. Its one real SIGKILL below covers all four retained owners.
    sql("CREATE TABLE usage_records(id UUID PRIMARY KEY, status TEXT)")
    sql(
        "INSERT INTO usage_records VALUES ('00000000-0000-0000-0000-000000000001','closed'), ('00000000-0000-0000-0000-000000000002','open'), ('00000000-0000-0000-0000-000000000003','closed')"
    )
    retained = []
    for case in TEMP_CASES:
        owner, table = _create_temp_original(server, sql, connection, case)
        _close_temp_owner(server, owner)
        closed = requests.post(
            server.api_url + "/sql",
            json={
                "statement": f"SELECT id, status FROM {table}",
                "connection_id": owner,
            },
            timeout=30,
        )
        assert closed.status_code >= 400 and closed.json().get("code") == "08003", (
            case["id"],
            closed.status_code,
            closed.text,
            server.debug_logs(),
        )
        fresh = connection()
        try:
            _temp_absent(server, fresh, table)
        finally:
            _close_temp_owner(server, fresh)
        # A new owner gets a fresh original relation; keep it alive until the
        # actual process kill, rather than simulating loss via ordinary close.
        owner, table = _create_temp_original(server, sql, connection, case)
        retained.append((case, owner, table))
    assert len(retained) == 4
    for case, owner, _ in retained:
        _temp_rows(server, sql, case, owner, case["before_disconnect"])
    return retained


def _check_temp_crash(server, sql, connection, retained):
    fresh = connection()
    try:
        for _, _, table in retained:
            _temp_absent(server, fresh, table)
    finally:
        _close_temp_owner(server, fresh)
    assert sql("SELECT id, status FROM usage_records ORDER BY id", read=True)[
        "rows"
    ] == [
        ["00000000-0000-0000-0000-000000000001", "closed"],
        ["00000000-0000-0000-0000-000000000002", "open"],
        ["00000000-0000-0000-0000-000000000003", "closed"],
    ]
    for case, _, _ in retained:
        # Fresh work after actual loss uses the normal owner/typed-PK path.
        owner, _ = _create_temp_original(server, sql, connection, case)
        _close_temp_owner(server, owner)
    print(
        "TEMP original disconnect/crash evidence: sql-0360/0361/0367/0568; four owners; one real SIGKILL",
        flush=True,
    )


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["id"])
def test_unlogged_originals_preserve_clean_rows_and_reset_real_process_loss(case):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    assert Path(binary).is_file(), f"Build the production Debug server first: {binary}"
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0)
    failed = True
    try:
        completed_constraints = set()
        completed_jobs = set()

        def sql(statement, connection=None, *, read=False):
            deadline = time.monotonic() + 60
            while True:
                body = {"statement": statement}
                if connection is not None:
                    body["connection_id"] = connection
                response = requests.post(server.api_url + "/sql", json=body, timeout=30)
                result = response.json()
                # A narrowly witnessed preparation rejection permits retry;
                # receipts, decisions and unknown replies never do.
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
                    and result.get("message", "").startswith(
                        "The relation catalog publication is not ready to authorize this DDL. No schema change was admitted."
                    )
                )
                if (
                    not_admitted
                    or definite_observation_abort(
                        response.status_code, result, pure_observation=read
                    )
                    or (
                        read
                        and response.status_code == 503
                        and result.get("code") in {"40001", "53300", "55000"}
                    )
                ) and time.monotonic() < deadline:
                    time.sleep(0.05)
                    continue
                assert response.status_code in (200, 202), (
                    statement,
                    response.status_code,
                    result,
                    server.debug_logs(),
                )
                receipt = result.get("ddl_receipt") or {}
                if job := receipt.get("restore_job_id"):
                    while True:
                        status = requests.get(
                            server.api_url + "/restore/jobs/" + job, timeout=5
                        )
                        assert status.status_code == 200, (
                            status.text,
                            server.debug_logs(),
                        )
                        state = status.json()
                        assert (
                            state["job_id"] == job
                            and state["idempotency_key"] == receipt["idempotency_key"]
                        ), state
                        if state["phase"] == "succeeded":
                            completed_jobs.add(job)
                            break
                        assert state["phase"] not in ("failed", "cancelled"), (
                            state,
                            server.debug_logs(),
                        )
                        assert time.monotonic() < deadline, (state, server.debug_logs())
                        time.sleep(0.05)
                if result.get("command_tag") == "DDL PENDING" and not job:
                    # Native TEMP initialization owns the captured connection
                    # grant and already waits its admitted deadline. A public
                    # HTTP status request cannot impersonate that owner. Keep
                    # an incomplete receipt as a real failure; do not replay
                    # CREATE or invent authority to finish it from this client.
                    assert not (
                        connection is not None
                        and receipt.get("namespace", "").startswith("pg_temp_")
                    ), (
                        "TEMP owner initialization did not finish",
                        result,
                        server.debug_logs(),
                    )
                    # The declaration is already committed. Observe its exact
                    # identity and actual native enforcement; never replay it.
                    assert receipt.get("state") == "pending", result
                    assert result.get("mutation_outcome") == "committed_pending", result
                    if catalog := result.get("catalog_receipt"):
                        assert catalog["state"] == "committed", result
                    name = receipt["table"]
                    identity = str(receipt["table_id"])
                    version = receipt["schema_version"]
                    while True:
                        table_state = requests.get(
                            server.api_url + "/tables/" + name, timeout=5
                        )
                        if table_state.status_code == 200:
                            assert str(table_state.json()["table_id"]) == identity, (
                                receipt,
                                table_state.json(),
                            )
                            status = requests.get(
                                server.api_url
                                + "/tables/"
                                + name
                                + "/constraints/status",
                                timeout=5,
                            )
                            if status.status_code == 200:
                                actual = status.json()
                                assert actual["schema_version"] == version, (
                                    receipt,
                                    actual,
                                )
                                assert actual["state"] != "invalid", (
                                    receipt,
                                    actual,
                                    server.debug_logs(),
                                )
                                if actual["state"] == "enforced":
                                    completed_constraints.add((identity, version))
                                    break
                            else:
                                assert status.status_code in (404, 409, 503, 504), (
                                    status.text,
                                    server.debug_logs(),
                                )
                        else:
                            assert table_state.status_code in (404, 409, 503), (
                                table_state.text,
                                server.debug_logs(),
                            )
                        assert time.monotonic() < deadline, (
                            receipt,
                            server.debug_logs(),
                        )
                        time.sleep(0.05)
                return result

        def connection():
            response = requests.post(
                server.api_url + "/sql/connections", json={}, timeout=10
            )
            assert response.status_code in (200, 201), (
                response.text,
                server.debug_logs(),
            )
            return response.json()["connection_id"]

        def check(expected, conn=None):
            result = sql(
                f"SELECT {projection} FROM {table} ORDER BY id", conn, read=True
            )
            assert result["rows"] == expected["rows"], (
                case["id"],
                result,
                expected,
                server.debug_logs(),
            )
            assert [(col["name"], col["type"]) for col in result["columns"]] == [
                (name, {2950: "uuid", 25: "string", 3802: "json"}[oid])
                for name, oid in expected["columns"]
            ], result

        def image_ids():
            images = set()
            # Inspect canonical group roots, excluding staging/retained images
            # whose ordinary GC is independent of clean adoption identity.
            for path in server.replica_root.glob(
                "group-*/table-db/root_identity.checkpoint"
            ):
                raw = path.read_bytes()
                assert len(raw) == 32 and raw[:8] == b"AFROOTI1", path
                images.add(raw[12:28])
            return images

        def restart(*, killed):
            # Do not use the fixture's timed fallback as evidence of clean close.
            process = server.proc
            assert process is not None and process.poll() is None
            process.send_signal(signal.SIGKILL if killed else signal.SIGTERM)
            code = process.wait(timeout=30)
            assert code == (-signal.SIGKILL if killed else 0), (
                code,
                server.debug_logs(),
            )
            server.pause()  # Process is already gone; reserve its exact listener.
            server.resume()

        table = (
            "usage_ingest_archive"
            if case["id"] in {"sql-0368", "sql-0569"}
            else "usage_ingest_records"
        )
        projection = "id, status" if table.endswith("archive") else "id, payload"
        sql("CREATE TABLE lifecycle_permanent(id UUID PRIMARY KEY, status TEXT)")
        sql(f"INSERT INTO lifecycle_permanent VALUES ('{UUID}', 'durable')")
        # No constraint worker can stand in for initialized-image publication.
        # Observe an admitted pending receipt, then issue the first write once.
        simple = sql("CREATE UNLOGGED TABLE lifecycle_no_pk(id INT, payload TEXT)")
        assert simple["command_tag"] in ("CREATE TABLE", "DDL PENDING"), simple
        inserted = sql("INSERT INTO lifecycle_no_pk VALUES (1, 'once')")
        assert inserted["rows_affected"] == 1, inserted
        assert sql("SELECT id, payload FROM lifecycle_no_pk", read=True)["rows"] == [
            ["1", "once"]
        ]

        if table.endswith("archive"):
            sql("CREATE TABLE usage_records(id UUID PRIMARY KEY, status TEXT)")
            sql(
                "INSERT INTO usage_records VALUES ('00000000-0000-0000-0000-000000000001','closed'), ('00000000-0000-0000-0000-000000000002','open'), ('00000000-0000-0000-0000-000000000003','closed')"
            )
        first = sql(case["sql"])
        expected_tag = (
            "SELECT"
            if case["command_tag"].startswith("SELECT ")
            else case["command_tag"]
        )
        if first["command_tag"] == "DDL PENDING":
            receipt = first["ddl_receipt"]
            assert (
                receipt.get("restore_job_id") in completed_jobs
                or (str(receipt["table_id"]), receipt["schema_version"])
                in completed_constraints
            ), first
        else:
            assert first["command_tag"] == expected_tag, first
        check(case["populated"])
        if table.endswith("archive"):
            sql(f"INSERT INTO {table} VALUES ('{UUID}', 'retained')")
        else:
            sql(
                f"INSERT INTO {table} VALUES ('{UUID}', '{{\"kind\":\"ingest\"}}'::JSONB)"
            )
            duplicate = requests.post(
                server.api_url + "/sql",
                json={
                    "statement": f"INSERT INTO {table} VALUES ('{UUID}', '{{}}'::JSONB)"
                },
                timeout=30,
            )
            assert (
                duplicate.status_code >= 400
                and duplicate.json()["code"] == case["duplicate_sqlstate"]
            ), duplicate.text
        check(case["before_disconnect"])
        if "IF NOT EXISTS" in case["sql"]:
            before = image_ids()
            repeated = sql(case["sql"])
            assert repeated["command_tag"] == case["repeat_command_tag"], repeated
            assert image_ids() == before, (
                "IF NOT EXISTS replaced a populated physical image"
            )
            check(case["before_disconnect"])
        other = connection()
        check(case["other_session"], other)
        assert sql("SELECT id, payload FROM lifecycle_no_pk", other, read=True)[
            "rows"
        ] == [["1", "once"]]

        closed = requests.delete(
            server.api_url + "/sql/connections/" + other, timeout=10
        )
        assert closed.status_code in (200, 204), closed.text
        if case["id"] == "sql-0362":
            # Genuine named PREPARED native effects must survive an actual clean
            # process shutdown, matching PostgreSQL's independent decision life.
            sql("CREATE UNLOGGED TABLE lifecycle_prepared(id BIGINT PRIMARY KEY)")
            prepared = connection()
            sql("BEGIN", prepared)
            sql("INSERT INTO lifecycle_prepared VALUES (18)", prepared)
            sql("PREPARE TRANSACTION 'unlogged_clean_restart'", prepared)
            closed = requests.delete(
                server.api_url + "/sql/connections/" + prepared, timeout=10
            )
            assert closed.status_code in (200, 204), closed.text
        before_clean = image_ids()
        assert before_clean, "No actual native physical root was initialized"
        restart(killed=False)
        assert sql("SELECT id, payload FROM lifecycle_no_pk", read=True)["rows"] == [
            ["1", "once"]
        ]

        check(case["clean_restart"])
        assert image_ids() == before_clean, (
            "Clean adoption changed physical image identities"
        )
        if "IF NOT EXISTS" in case["sql"]:
            sql(case["sql"])
            check(case["clean_restart"])
            assert image_ids() == before_clean, (
                "IF NOT EXISTS recreated a cleanly adopted generation"
            )
        if case["id"] == "sql-0362":
            sql("COMMIT PREPARED 'unlogged_clean_restart'")
            assert sql("SELECT id FROM lifecycle_prepared", read=True)["rows"] == [
                ["18"]
            ]
            mixed = connection()
            sql("BEGIN", mixed)
            sql(
                "INSERT INTO lifecycle_permanent VALUES ('00000000-0000-0000-0000-000000000010', 'committed-after-loss')",
                mixed,
            )
            sql("INSERT INTO lifecycle_prepared VALUES (19)", mixed)
            sql("PREPARE TRANSACTION 'mixed_native_crash'", mixed)
            native_only = connection()
            sql("BEGIN", native_only)
            sql("INSERT INTO lifecycle_prepared VALUES (20)", native_only)
            sql("PREPARE TRANSACTION 'unlogged_native_crash'", native_only)
            for prepared_connection in (mixed, native_only):
                closed = requests.delete(
                    server.api_url + "/sql/connections/" + prepared_connection,
                    timeout=10,
                )
                assert closed.status_code in (200, 204), closed.text
        temp_crash = (
            _prepare_temp_disconnect_and_crash(server, sql, connection)
            if case["id"] == "sql-0362"
            else []
        )
        before_crash = image_ids()
        restart(killed=True)
        assert sql("SELECT id, payload FROM lifecycle_no_pk", read=True)["rows"] == []

        check(case["crash_restart"])
        assert image_ids() - before_crash, (
            "Crash reset did not initialize a genuine successor image"
        )
        if "IF NOT EXISTS" in case["sql"]:
            after_reset = image_ids()
            sql(case["sql"])
            check(case["crash_restart"])
            assert image_ids() == after_reset, (
                "IF NOT EXISTS replaced the reset successor image"
            )
        if temp_crash:
            # Observe native successor identity above before fresh TEMP owners
            # create any additional images that could contaminate that proof.
            _check_temp_crash(server, sql, connection, temp_crash)
        permanent = sql(
            "SELECT id, status FROM lifecycle_permanent ORDER BY id", read=True
        )
        assert permanent["rows"] == [[UUID, "durable"]], permanent
        if case["id"] == "sql-0362":
            # An undecided durable GID survives without resurrecting its lost
            # native image. Only the user's actual COMMIT decides it; permanent
            # participants still apply while known-lost native effects retire.
            assert sql("SELECT id FROM lifecycle_prepared", read=True)["rows"] == []
            sql("COMMIT PREPARED 'mixed_native_crash'")
            assert sql(
                "SELECT id, status FROM lifecycle_permanent ORDER BY id", read=True
            )["rows"] == [
                [UUID, "durable"],
                ["00000000-0000-0000-0000-000000000010", "committed-after-loss"],
            ]
            assert sql("SELECT id FROM lifecycle_prepared", read=True)["rows"] == []
            sql("COMMIT PREPARED 'unlogged_native_crash'")
            assert sql("SELECT id FROM lifecycle_prepared", read=True)["rows"] == []
            sql("INSERT INTO lifecycle_prepared VALUES (18), (19), (20)")
            assert sql("SELECT id FROM lifecycle_prepared ORDER BY id", read=True)[
                "rows"
            ] == [["18"], ["19"], ["20"]]
        # The reset relation remains usable through the normal native PK path.
        if table.endswith("archive"):
            sql(f"INSERT INTO {table} VALUES ('{UUID}', 'after-reset')")
        else:
            sql(
                f"INSERT INTO {table} VALUES ('{UUID}', '{{\"kind\":\"after-reset\"}}'::JSONB)"
            )
        assert len(sql(f"SELECT id FROM {table}", read=True)["rows"]) == 1
        failed = False
    finally:
        server.stop(test_failed=failed)
