# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Retry proof for explicitly pure row observations in the lifetime campaign.

This is test-client policy, not SQL replay middleware. Callers must opt in only
for plain row SELECTs without functions or effects. The enclosing caller retains
its existing absolute deadline and delay. Unknown outcomes never qualify.
"""


def definite_observation_abort(status, result, *, pure_observation):
    return (
        pure_observation is True
        and status == 409
        and result.get("code") == "40001"
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
                "rows",
                "columns",
                "rows_affected",
                "command_tag",
            )
        )
    )
