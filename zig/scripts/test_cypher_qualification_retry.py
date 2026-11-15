# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

import json
import unittest

from cypher_qualification_retry import (
    await_durable_graph_commit,
    retry_allowed,
    rolled_back_sql_fixture,
)


class RolledBackSqlFixtureTests(unittest.TestCase):
    def fixture(self, failure=None, rollback_error=False):
        calls = []
        sessions = 0

        def call(path, body, *, timeout_s):
            nonlocal sessions
            self.assertEqual(path, "/sql")
            self.assertGreater(timeout_s, 0)
            statement = body["statement"]
            if statement.startswith("BEGIN"):
                sessions += 1
                calls.append((sessions, "BEGIN"))
                return {"session_id": sessions}
            self.assertEqual(body["session_id"], sessions)
            calls.append((sessions, statement))
            if statement == "ROLLBACK" and rollback_error:
                raise RuntimeError("rollback failed")
            if statement == "SELECT count(*)" and sessions == 1 and failure:
                raise AssertionError(failure)
            return {"rows": [["2"]]}

        return call, calls

    def test_restarts_whole_fixture_only_after_rollback(self):
        for status, code in ((503, "53300"), (409, "40001")):
            call, calls = self.fixture(
                (
                    "/sql",
                    status,
                    json.dumps(
                        {
                            "code": code,
                            "retryable": True,
                            "transaction_status": "failed",
                        }
                    ),
                )
            )
            rolled_back_sql_fixture(
                call,
                [("UPDATE docs SET x=1", None), ("SELECT count(*)", [["2"]])],
                clock=lambda: 0,
                sleep=lambda _: None,
            )
            self.assertEqual(
                calls,
                [
                    (1, "BEGIN"),
                    (1, "UPDATE docs SET x=1"),
                    (1, "SELECT count(*)"),
                    (1, "ROLLBACK"),
                    (2, "BEGIN"),
                    (2, "UPDATE docs SET x=1"),
                    (2, "SELECT count(*)"),
                    (2, "ROLLBACK"),
                ],
            )

    def test_nonretryable_failures_rollback_without_replaying(self):
        for status, payload in (
            (503, {"code": "40003", "retryable": True, "transaction_status": "failed"}),
            (500, {"code": "53300", "retryable": True, "transaction_status": "failed"}),
            (
                503,
                {"code": "53300", "retryable": False, "transaction_status": "failed"},
            ),
            (503, {"code": "53300", "retryable": True, "transaction_status": "idle"}),
            (503, []),
        ):
            call, calls = self.fixture(("/sql", status, json.dumps(payload)))
            with self.assertRaises(AssertionError):
                rolled_back_sql_fixture(call, [("SELECT count(*)", [["2"]])])
            self.assertEqual(
                calls, [(1, "BEGIN"), (1, "SELECT count(*)"), (1, "ROLLBACK")]
            )

    def test_result_mismatch_and_failed_rollback_do_not_restart(self):
        call, calls = self.fixture()
        with self.assertRaises(AssertionError):
            rolled_back_sql_fixture(call, [("SELECT count(*)", [["1"]])])
        self.assertEqual(calls[-1], (1, "ROLLBACK"))
        call, calls = self.fixture(rollback_error=True)
        with self.assertRaisesRegex(RuntimeError, "rollback failed"):
            rolled_back_sql_fixture(call, [("SELECT count(*)", [["2"]])])
        self.assertEqual(len(calls), 3)

    def test_fixture_must_not_commit(self):
        for statement in ("COMMIT", "BEGIN", "ROLLBACK", "PREPARE TRANSACTION 'x'"):
            with self.assertRaises(AssertionError):
                rolled_back_sql_fixture(None, [(statement, None)])

    def test_deadline_expiry_rolls_back_without_restarting(self):
        now = 0
        calls = []

        def call(path, body, *, timeout_s):
            nonlocal now
            statement = body["statement"]
            calls.append((statement, timeout_s))
            if statement.startswith("BEGIN"):
                now = 2
                return {"session_id": "one"}
            if statement == "ROLLBACK":
                return {}
            self.fail("expired fixture submitted another statement")

        with self.assertRaisesRegex(AssertionError, "deadline exceeded"):
            rolled_back_sql_fixture(
                call, [("SELECT count(*)", [["2"]])], timeout_s=1, clock=lambda: now
            )
        self.assertEqual(
            calls, [("BEGIN ISOLATION LEVEL READ COMMITTED", 1), ("ROLLBACK", 5)]
        )


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
