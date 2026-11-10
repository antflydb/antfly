# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Retry admission failures without replaying admitted graph mutations."""

import json
import time


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
