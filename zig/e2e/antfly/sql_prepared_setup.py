# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Bounded test setup retry after a proven rejected named PREPARE.

The mounted lifetime test proves this exact response follows cancellation of
its immutable admission, terminal ABORT, and absence of the named GID. This is
not a production retry policy. Unknown replies and individual writes never
repeat; a qualifying rejection starts a completely new BEGIN and setup.
"""

import time


CATALOG_ABORT_MESSAGE = (
    "The table definition changed before the statement could complete. "
    "Prepare the statement again against the current schema."
)


def definite_named_prepare_abort(status, result):
    return (
        status == 409
        and result.get("code") == "40001"
        and result.get("message") == CATALOG_ABORT_MESSAGE
        and result.get("retryable") is True
        and result.get("transaction_status") == "idle"
        and all(
            result.get(key) is None
            for key in (
                "transaction_id",
                "reconciliation_id",
                "mutation_outcome",
                "admission_outcome",
                "status",
                "ddl_receipt",
                "catalog_receipt",
                "rows",
                "columns",
                "rows_affected",
                "command_tag",
            )
        )
    )


def prepare_named_setup(execute, statements, prepare, *, pause=time.sleep):
    """execute returns (HTTP status, complete JSON); transport errors propagate.

    Only these caller-owned fixed setup statements may be repeated. Neither
    PREPARE alone nor any decision command is retried. At most three attempts may start; retries must begin within one
    minute, and every request retains its own transport timeout.
    """
    assert prepare.startswith("PREPARE TRANSACTION ")
    deadline = time.monotonic() + 60
    for attempt in range(3):
        assert attempt == 0 or time.monotonic() < deadline, (
            "prepared setup retry deadline"
        )
        for statement in ("BEGIN", *statements):
            status, result = execute(statement)
            assert status in (200, 202), (statement, status, result)
        status, result = execute(prepare)
        if status in (200, 202):
            return result
        if (
            attempt == 2
            or time.monotonic() >= deadline
            or not definite_named_prepare_abort(status, result)
        ):
            raise AssertionError((prepare, status, result))
        pause(0.05)
    raise AssertionError("unreachable prepared setup exhaustion")
