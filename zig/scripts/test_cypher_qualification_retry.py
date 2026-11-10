# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

import json
import unittest

from cypher_qualification_retry import await_durable_graph_commit, retry_allowed


class QualificationReceiptTests(unittest.TestCase):
    def result(self, status="unknown", complete=False):
        return {
            "complete": complete,
            "read_only": False,
            "commit": {"status": status, "transaction_id": "original"},
        }

    def test_pending_writes_poll_only_the_original_receipt_until_durable(self):
        for outcome in ("unknown", "committed_pending"):
            calls = []
            receipts = iter(
                [
                    {"code": "temporarily_unavailable"},
                    {"disposition": "committed", "durable": False},
                    {"disposition": "committed", "durable": True},
                ]
            )

            def read(transaction):
                calls.append(transaction)
                return next(receipts)

            await_durable_graph_commit(
                self.result(outcome), read, clock=lambda: 0, sleep=lambda _: None
            )
            self.assertEqual(calls, ["original"] * 3)

    def test_definite_commit_requires_complete_results_and_no_poll(self):
        def unexpected(_):
            self.fail("committed write must not poll")

        await_durable_graph_commit(self.result("committed", True), unexpected)
        with self.assertRaises(AssertionError):
            await_durable_graph_commit(self.result("committed", False), unexpected)

    def test_abort_and_unresolved_deadline_remain_failures(self):
        with self.assertRaises(AssertionError):
            await_durable_graph_commit(
                self.result(), lambda _: {"disposition": "aborted", "durable": True}
            )
        ticks = iter([0, 1])
        with self.assertRaises(AssertionError):
            await_durable_graph_commit(
                self.result(),
                lambda _: {"disposition": "outcome_unknown"},
                timeout_s=1,
                clock=lambda: next(ticks),
                sleep=lambda _: self.fail("deadline must bound polling"),
            )


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
