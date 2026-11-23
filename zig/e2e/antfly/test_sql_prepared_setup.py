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

import unittest

from sql_prepared_setup import (
    CATALOG_ABORT_MESSAGE,
    definite_named_prepare_abort,
    prepare_named_setup,
)


class PreparedSetupTest(unittest.TestCase):
    def setUp(self):
        self.abort = dict(
            code="40001",
            message=CATALOG_ABORT_MESSAGE,
            retryable=True,
            transaction_status="idle",
        )

    def test_closed_known_abort_shape(self):
        self.assertTrue(definite_named_prepare_abort(409, self.abort))
        for status in (200, 202, 400, 503):
            self.assertFalse(definite_named_prepare_abort(status, self.abort))
        for key, value in (
            ("code", "40003"),
            ("message", "unknown"),
            ("retryable", 1),
            ("transaction_status", "failed"),
            ("transaction_status", "in_transaction"),
        ):
            self.assertFalse(
                definite_named_prepare_abort(409, dict(self.abort, **{key: value}))
            )
        for key in ("code", "message", "retryable", "transaction_status"):
            body = dict(self.abort)
            body.pop(key)
            self.assertFalse(definite_named_prepare_abort(409, body))
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
        ):
            self.assertFalse(
                definite_named_prepare_abort(409, dict(self.abort, **{key: "present"}))
            )

    def test_restarts_whole_transaction_after_rejected_prepare(self):
        calls = []

        def execute(sql):
            calls.append(sql)
            if sql.startswith("PREPARE") and calls.count(sql) == 1:
                return 409, self.abort
            return 200, {}

        prepare_named_setup(
            execute,
            ("INSERT fixed-a", "INSERT fixed-b"),
            "PREPARE TRANSACTION 'fixed'",
            pause=lambda _: None,
        )
        self.assertEqual(
            calls,
            ["BEGIN", "INSERT fixed-a", "INSERT fixed-b", "PREPARE TRANSACTION 'fixed'"]
            * 2,
        )

    def test_unknown_write_and_lost_reply_never_replay(self):
        for fail_at, failure in (
            ("INSERT fixed", (409, self.abort)),
            (
                "PREPARE TRANSACTION 'fixed'",
                (409, dict(self.abort, transaction_id="a" * 32)),
            ),
        ):
            calls = []

            def execute(sql):
                calls.append(sql)
                return failure if sql == fail_at else (200, {})

            with self.assertRaises(AssertionError):
                prepare_named_setup(
                    execute, ("INSERT fixed",), "PREPARE TRANSACTION 'fixed'"
                )
            self.assertEqual(calls.count("BEGIN"), 1)
        calls = []

        def lost(sql):
            calls.append(sql)
            if sql.startswith("PREPARE"):
                raise ConnectionError("lost reply")
            return 200, {}

        with self.assertRaises(ConnectionError):
            prepare_named_setup(lost, ("INSERT fixed",), "PREPARE TRANSACTION 'fixed'")
        self.assertEqual(calls.count("BEGIN"), 1)

    def test_retry_is_finite(self):
        calls = []

        def execute(sql):
            calls.append(sql)
            return (409, self.abort) if sql.startswith("PREPARE") else (200, {})

        with self.assertRaises(AssertionError):
            prepare_named_setup(
                execute,
                ("INSERT fixed",),
                "PREPARE TRANSACTION 'fixed'",
                pause=lambda _: None,
            )
        self.assertEqual(calls.count("BEGIN"), 3)
        self.assertEqual(calls.count("INSERT fixed"), 3)


if __name__ == "__main__":
    unittest.main()
