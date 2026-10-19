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

"""Exact original name-resolution contracts against an owned PostgreSQL 18+ server."""

import json
import unittest

import psycopg
from generate_sql_postgres_reference import (
    FIXTURES,
    create_table,
    parameters,
    postgres,
    properties,
)


class NameResolutionTest(unittest.TestCase):
    def test_original_alias_references_and_valid_input_output_scopes(self):
        campaign = json.loads(
            (FIXTURES / "sql_name_resolution_campaign.json").read_text()
        )
        profile = json.loads((FIXTURES / "sql_read_campaign_profile.json").read_text())
        inventory = {
            case["id"]: case
            for case in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
        }
        self.assertEqual(1, campaign["format"])
        self.assertEqual(8, len(campaign["entries"]))
        self.assertEqual(8, len({entry["id"] for entry in campaign["entries"]}))
        self.assertEqual(2, len(campaign["controls"]))
        with postgres() as db:
            create_table(
                db, "usage_records", properties(profile["schema"]), profile["rows"]
            )
            create_table(
                db,
                "docs",
                properties(campaign["document_schema"]),
                campaign["document_rows"],
            )
            for entry in campaign["entries"]:
                case = inventory[entry["id"]]
                self.assertNotIn(entry["missing_column"], properties(profile["schema"]))
                self.assertNotIn(
                    entry["missing_column"], properties(campaign["document_schema"])
                )
                with self.subTest(id=entry["id"]):
                    with self.assertRaises(psycopg.errors.UndefinedColumn) as rejected:
                        with db.transaction(force_rollback=True):
                            db.execute("SET TRANSACTION READ ONLY")
                            with psycopg.RawCursor(db) as cursor:
                                cursor.execute(case["sql"], parameters(case))
                    self.assertEqual(entry["sqlstate"], rejected.exception.sqlstate)
                    self.assertIn(
                        '"' + entry["missing_column"] + '"',
                        rejected.exception.diag.message_primary,
                    )
            for control in campaign["controls"]:
                with (
                    self.subTest(sql=control["sql"]),
                    db.transaction(force_rollback=True),
                ):
                    db.execute("SET TRANSACTION READ ONLY")
                    result = db.execute(control["sql"])
                    self.assertEqual(
                        ["amount"], [column.name for column in result.description]
                    )
                    self.assertEqual(
                        [20], [column.type_code for column in result.description]
                    )
                    self.assertEqual(
                        [(int(control["first_integer"]),)], result.fetchall()
                    )
            self.assertEqual(
                len(profile["rows"]),
                db.execute("SELECT count(*) FROM usage_records").fetchone()[0],
            )
            self.assertEqual(
                len(campaign["document_rows"]),
                db.execute("SELECT count(*) FROM docs").fetchone()[0],
            )


if __name__ == "__main__":
    unittest.main()
