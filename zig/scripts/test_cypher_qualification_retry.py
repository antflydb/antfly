# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

import json
import unittest

from cypher_qualification_retry import retry_allowed


class QualificationRetryTests(unittest.TestCase):
    def test_mutations_require_explicit_nonadmission(self):
        for outcome in ("aborted", "committed", "unknown", None):
            with self.subTest(outcome=outcome):
                self.assertFalse(
                    retry_allowed(
                        503,
                        json.dumps({"write_outcome": outcome}),
                        path="/graphs/g/query",
                    )
                )
        self.assertTrue(
            retry_allowed(
                503, '{"write_outcome":"not_started"}', path="/graphs/g/query"
            )
        )
        self.assertFalse(
            retry_allowed(
                409, '{"write_outcome":"not_started"}', path="/graphs/g/query"
            )
        )

    def test_only_opted_in_idle_sql_reads_restart(self):
        for status, code in ((503, "53300"), (409, "40001")):
            payload = {"code": code, "retryable": True, "transaction_status": "idle"}
            for path in ("/sql", "/sql/prepared/query/execute"):
                request = (
                    {"statement": "SELECT 1"} if path == "/sql" else {"parameters": []}
                )
                with self.subTest(status=status, path=path):
                    self.assertTrue(
                        retry_allowed(
                            status,
                            json.dumps(payload),
                            path=path,
                            body=request,
                            read_only=True,
                        )
                    )
                    self.assertFalse(
                        retry_allowed(
                            status, json.dumps(payload), path=path, body=request
                        )
                    )
                    for key in ("session_id", "transaction_id"):
                        self.assertFalse(
                            retry_allowed(
                                status,
                                json.dumps(payload),
                                path=path,
                                body={**request, key: "active"},
                                read_only=True,
                            )
                        )
            for transaction_status in ("in_transaction", "failed", None):
                self.assertFalse(
                    retry_allowed(
                        status,
                        json.dumps(
                            {**payload, "transaction_status": transaction_status}
                        ),
                        path="/sql",
                        body={},
                        read_only=True,
                    )
                )

    def test_unknown_outcomes_and_invalid_envelopes_never_restart(self):
        for status, payload in (
            (409, '{"code":"40003","retryable":true,"transaction_status":"idle"}'),
            (503, '{"code":"53300","retryable":false,"transaction_status":"idle"}'),
            (403, '{"write_outcome":"not_started"}'),
            (500, '{"code":"53300","retryable":true,"transaction_status":"idle"}'),
            (503, '{"code":[],"retryable":true,"transaction_status":"idle"}'),
            (503, "[]"),
            (503, "null"),
            (503, "not JSON"),
        ):
            with self.subTest(status=status, payload=payload):
                self.assertFalse(
                    retry_allowed(status, payload, path="/sql", body={}, read_only=True)
                )


if __name__ == "__main__":
    unittest.main()
