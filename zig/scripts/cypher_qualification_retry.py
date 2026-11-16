# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Retry admission failures without replaying admitted graph mutations."""

import json
import time


def rolled_back_sql_fixture(
    call, statements, *, timeout_s=30, clock=time.monotonic, sleep=time.sleep
):
    """Retry an entirely rolled-back fixture, never an admitted durable write.

    Each item is (statement, expected_rows), with None for mutation results.
    A failed coherent snapshot invalidates the whole transaction. Replaying
    only its failed statement would not qualify the same transactional view.
    """
    for statement, _ in statements:
        assert statement.lstrip().split(None, 1)[0].upper() in (
            "SELECT",
            "UPDATE",
            "DELETE",
            "INSERT",
        ), "fixture statements must not control or commit the transaction"
    deadline = clock() + timeout_s
    while True:
        remaining = deadline - clock()
        assert remaining > 0, "rolled-back SQL fixture deadline exceeded"
        session = call(
            "/sql",
            {"statement": "BEGIN ISOLATION LEVEL READ COMMITTED"},
            timeout_s=remaining,
        )["session_id"]
        try:
            for statement, expected in statements:
                remaining = deadline - clock()
                assert remaining > 0, "rolled-back SQL fixture deadline exceeded"
                result = call(
                    "/sql",
                    {"statement": statement, "session_id": session},
                    timeout_s=remaining,
                )
                if expected is not None:
                    assert result["rows"] == expected, result
            return
        except AssertionError as error:
            detail = error.args[0] if error.args else None
            if not isinstance(detail, tuple) or len(detail) != 3:
                raise
            path, status, content = detail
            try:
                failure = json.loads(content)
            except (ValueError, TypeError):
                raise error
            if (
                path != "/sql"
                or not isinstance(failure, dict)
                or (status, failure.get("code")) not in ((503, "53300"), (409, "40001"))
                or failure.get("retryable") is not True
                or failure.get("transaction_status") != "failed"
                or clock() >= deadline
            ):
                raise
        finally:
            # Cleanup gets its own bounded allowance even if the fixture's
            # deadline expired. Do not retry if rollback itself fails.
            call("/sql", {"statement": "ROLLBACK", "session_id": session}, timeout_s=5)
        sleep(min(0.1, max(0, deadline - clock())))


def await_durable_graph_commit(
    result, read_receipt, *, timeout_s=90, clock=time.monotonic, sleep=time.sleep
):
    """Resolve one admitted write by receipt; never submit the write again."""
    commit = result["commit"]
    if commit["status"] == "committed":
        assert result["complete"] is True, result
        return
    assert commit["status"] in ("unknown", "committed_pending"), result
    assert result["read_only"] is False and commit["transaction_id"], result
    deadline = clock() + timeout_s
    while True:
        status = read_receipt(commit["transaction_id"])
        disposition = status.get("disposition")
        assert disposition != "aborted", status
        if disposition == "committed" and status.get("durable"):
            return
        assert clock() < deadline, status
        sleep(0.2)


def retry_allowed(status, content, *, path, body=None, read_only=False):
    if status not in (409, 503):
        return False
    try:
        failure = json.loads(content)
    except (ValueError, TypeError):
        return False
    if not isinstance(failure, dict):
        return False
    if status == 503 and failure.get("write_outcome") == "not_started":
        return True
    sql_path = path == "/sql" or (
        path.startswith("/sql/prepared/") and path.endswith("/execute")
    )
    return (
        read_only
        and sql_path
        and isinstance(body, dict)
        and "session_id" not in body
        and "transaction_id" not in body
        and (status, failure.get("code")) in ((503, "53300"), (409, "40001"))
        and failure.get("retryable") is True
        and failure.get("transaction_status") == "idle"
    )
