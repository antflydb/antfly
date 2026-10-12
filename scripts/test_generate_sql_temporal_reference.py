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

"""Independent temporal/clock semantics. Requires the declared PostgreSQL oracle."""

import unittest

import psycopg

from generate_sql_postgres_reference import postgres
from generate_sql_temporal_reference import contract, reference


class TemporalReferenceTest(unittest.TestCase):
    def test_postgres_nondecimal_integer_prefix_and_separator_contract(self):
        for expression, expected in (
            ("0x_6", 6),
            ("0o_6", 6),
            ("0b_110", 6),
            ("0x6e0", 1760),
            ("0XFF_FF", 65535),
            ("-0x_8000_0000_0000_0000", -9223372036854775808),
        ):
            with (
                self.subTest(expression=expression),
                self.db.transaction(force_rollback=True),
            ):
                self.assertEqual(
                    expected, self.db.execute("SELECT " + expression).fetchone()[0]
                )
        for expression in ("0x", "0x_", "0x__6", "0x6_", "0b2", "0o8", "0x6.0"):
            with (
                self.subTest(expression=expression),
                self.assertRaises(psycopg.Error) as raised,
            ):
                with self.db.transaction(force_rollback=True):
                    self.db.execute("SELECT " + expression)
            self.assertEqual("42601", raised.exception.sqlstate)

    @classmethod
    def setUpClass(cls):
        cls.server = postgres()
        cls.db = cls.server.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.server.__exit__(None, None, None)

    def test_all_domains_have_independent_binary_and_calendar_evidence(self):
        result = reference(self.db)
        self.assertEqual(174, len(result["entries"]))
        self.assertEqual(21, len(result["comparisons"]))
        self.assertEqual(
            {"date", "time", "time_tz", "timestamp", "timestamp_tz", "interval"},
            {entry["kind"] for entry in result["entries"]},
        )
        self.assertEqual({-1, 0, 1}, {entry["infinity"] for entry in result["entries"]})
        self.assertEqual(
            set(range(7)),
            {entry["precision"] for entry in result["entries"] if "precision" in entry},
        )

    def test_upper_boundary_calendar_fields_retain_last_microsecond(self):
        for kind, expression in (
            ("timestamp", "TIMESTAMP '294276-12-31 23:59:59.999999'"),
            ("timestamp_tz", "TIMESTAMPTZ '294276-12-31 23:59:59.999999+00'"),
        ):
            with self.subTest(kind=kind):
                observed = contract(self.db, kind, expression)
                binary = int.from_bytes(
                    bytes.fromhex(observed["binary"]), "big", signed=True
                )
                self.assertEqual(9223371331199999999, binary)
                self.assertEqual([binary], observed["components"])

    def test_transaction_and_statement_clocks_are_stable_at_their_own_scope(self):
        with self.db.transaction(force_rollback=True):
            first = self.db.execute(
                "SELECT now(), transaction_timestamp(), CURRENT_TIMESTAMP(6), statement_timestamp(), statement_timestamp()"
            ).fetchone()
            self.assertEqual(first[0], first[1])
            self.assertEqual(first[0], first[2])
            self.assertEqual(first[3], first[4])
            self.db.execute("SAVEPOINT clock_scope")
            second = self.db.execute(
                "SELECT now(), statement_timestamp(), statement_timestamp()"
            ).fetchone()
            self.assertEqual(first[0], second[0])
            self.assertEqual(second[1], second[2])
            self.db.execute("ROLLBACK TO clock_scope")
            self.assertEqual(first[0], self.db.execute("SELECT now()").fetchone()[0])
            self.db.execute("RELEASE clock_scope")

    def test_current_timestamp_precision_uses_the_transaction_instant(self):
        with self.db.transaction(force_rollback=True):
            for precision in range(7):
                with self.subTest(precision=precision):
                    result = self.db.execute(
                        f"SELECT CURRENT_TIMESTAMP({precision}) = transaction_timestamp()::timestamptz({precision}), pg_typeof(CURRENT_TIMESTAMP({precision}))::oid"
                    ).fetchone()
                    self.assertEqual((True, 1184), result)

    def test_cursor_fetch_rebinds_statement_time_but_retains_transaction_time(self):
        with self.db.transaction(force_rollback=True):
            self.db.execute(
                "DECLARE clock_cursor CURSOR FOR SELECT now(), statement_timestamp() FROM generate_series(1, 2)"
            )
            first = self.db.execute("FETCH 1 FROM clock_cursor").fetchone()
            # Wait server-side only to make distinct command instants certain;
            # this is not a monotonicity guarantee for the realtime clock.
            self.db.execute("SELECT pg_sleep(0.002)")
            second = self.db.execute("FETCH 1 FROM clock_cursor").fetchone()
            self.assertEqual(first[0], second[0])
            self.assertNotEqual(first[1], second[1])

    def test_precision_syntax_and_warning_contract_are_not_silent_clamping(self):
        notices = []
        handler = lambda notice: notices.append(
            (notice.severity, notice.sqlstate, notice.message_primary)
        )
        self.db.add_notice_handler(handler)
        try:
            result = self.db.execute(
                "SELECT CURRENT_TIMESTAMP(7), CURRENT_TIMESTAMP(0x6)"
            )
            self.assertEqual(
                [1184, 1184], [column.type_code for column in result.description]
            )
            self.assertTrue(
                any(
                    severity == "WARNING" and code == "22023" and "6" in message
                    for severity, code, message in notices
                )
            )
        finally:
            self.db.remove_notice_handler(handler)
        with self.assertRaises(psycopg.errors.SyntaxError) as caught:
            self.db.execute("SELECT CURRENT_TIMESTAMP(-1)")
        self.assertEqual("42601", caught.exception.sqlstate)
        with self.assertRaises(psycopg.errors.UndefinedFunction) as caught:
            self.db.execute("SELECT now(1)")
        self.assertEqual("42883", caught.exception.sqlstate)

    def test_wall_clock_remains_a_finite_timestamp_without_monotonicity_claim(self):
        result = self.db.execute(
            "SELECT isfinite(clock_timestamp()), pg_typeof(clock_timestamp())::oid"
        ).fetchone()
        self.assertEqual((True, 1184), result)

    def test_interval_precision_overflow_is_not_wrapped_or_infinite(self):
        for micros in (-9223372036854775808, 9223372036854775807):
            with self.subTest(micros=micros):
                with self.assertRaises(psycopg.errors.DatetimeFieldOverflow) as caught:
                    self.db.execute(
                        "SELECT %s::interval(0)", (f"{micros} microseconds",)
                    )
                self.assertEqual("22008", caught.exception.sqlstate)


if __name__ == "__main__":
    unittest.main()
