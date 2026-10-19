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

"""Join golden construction guards; actual results use the separate PG --check gate."""

from contextlib import nullcontext, redirect_stderr
from io import StringIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import generate_sql_postgres_reference as reference


class JoinReferenceTest(unittest.TestCase):
    def cases(self):
        return {
            case["id"]: case
            for case in json.loads(
                (reference.FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
        }

    def test_peer_witness_preserves_distinct_from_and_cte_bodies(self):
        cases = self.cases()
        for case_id in ("sql-1341", "sql-1326", "sql-1320"):
            case = cases[case_id]
            witness = reference.join_order_observer(case)
            body = case["sql"].rsplit(" ORDER BY ", 1)[0]
            self.assertIn("FROM (" + body + ") AS join_witness", witness)
            self.assertNotIn(" LIMIT ", witness)
        self.assertIn(
            "IS DISTINCT FROM 'internal'",
            reference.join_order_observer(cases["sql-1341"]),
        )
        self.assertTrue(
            reference.join_order_observer(cases["sql-1320"]).endswith(" OFFSET 4")
        )
        self.assertNotIn(
            " OFFSET ", reference.join_order_observer(cases["sql-1320"], False)
        )
        with self.assertRaises(ValueError):
            reference.join_order_observer({"family": "read", "sql": "SELECT 1"})

    def test_profile_has_duplicate_physical_rows_and_nullable_unmatched_keys(self):
        profile = reference.join_read_profile()
        rows = {row["key"]: row["value"] for row in profile["rows"]}
        self.assertEqual(rows["duplicate-order-a"], rows["duplicate-order-b"])
        self.assertNotIn("customer_id", rows["null-key"])
        self.assertNotIn("amount", rows["null-amount"])
        self.assertNotIn("enabled", rows["c1"])
        self.assertNotEqual(
            rows["duplicate-customer-a"]["name"], rows["duplicate-customer-b"]["name"]
        )
        self.assertEqual(
            rows["duplicate-customer-a"]["id"], rows["duplicate-customer-b"]["id"]
        )
        self.assertEqual("customer_records", profile["additional_tables"][0]["name"])

    def test_output_rejects_partial_discovery_without_overwriting_a_golden(self):
        with TemporaryDirectory(prefix="antfly-sql-join-output-") as directory:
            output = Path(directory) / "reference.json"
            output.write_text("preserve-existing-reference")
            failure = {
                "format": 2,
                "entries": [],
                "excluded": [{"id": "sql-1341", "reason": "witness failed"}],
            }
            db = SimpleNamespace(info=SimpleNamespace(server_version=180000))
            diagnostic = StringIO()
            with (
                patch("sys.argv", ["oracle", "join_read", "--output", str(output)]),
                patch.object(reference, "postgres", return_value=nullcontext(db)),
                patch.object(reference, "read_reference", return_value=failure),
                redirect_stderr(diagnostic),
            ):
                with self.assertRaises(SystemExit) as rejected:
                    reference.main()
            self.assertEqual(2, rejected.exception.code)
            self.assertIn("reference not written", diagnostic.getvalue())
            self.assertEqual("preserve-existing-reference", output.read_text())


if __name__ == "__main__":
    unittest.main()
