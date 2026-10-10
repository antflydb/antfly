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

import json
from pathlib import Path
import unittest

from generate_sql_document_reference import reference, SEEDS


class DocumentReferenceTest(unittest.TestCase):
    def case(self, sql):
        return {"id": "sql-0947", "sql": sql, "params": []}

    def schema(self, **extra):
        return {
            "version": 1,
            "storage_mode": "document",
            "default_type": "doc",
            "document_schemas": {
                "doc": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "title": {"type": "text"},
                            "status": {"type": "keyword", **extra},
                        },
                        "additionalProperties": True,
                    }
                }
            },
        }

    def test_complete_untouched_and_undeclared_values_survive_updates(self):
        result = reference(
            [
                self.case(
                    "UPDATE docs SET title='Changed' WHERE _id='doc:a' RETURNING _id,title"
                )
            ],
            {"sql-0947": self.schema()},
        )
        entry = result["entries"][0]
        self.assertEqual(1, entry["affected"])
        self.assertEqual([["doc:a", "Changed"]], entry["rows"])
        self.assertEqual(SEEDS[1:], entry["final"][1:])
        self.assertEqual(
            SEEDS[0]["value"]["metadata"], entry["final"][0]["value"]["metadata"]
        )

    def test_schema_hint_cannot_change_the_relational_result(self):
        case = self.case("UPDATE docs SET title='Changed' WHERE status='draft'")
        ready = reference(
            [case], {case["id"]: self.schema(**{"x-antfly-index-lifecycle": "ready"})}
        )
        stale = reference(
            [case],
            {case["id"]: self.schema(**{"x-antfly-index-lifecycle": "building"})},
        )
        self.assertEqual(ready["entries"][0]["final"], stale["entries"][0]["final"])

    def test_create_only_identity_is_not_legacy_upsert(self):
        result = reference(
            [self.case("INSERT INTO docs(_id,title) VALUES('doc:a','New')")],
            {"sql-0947": self.schema()},
        )
        self.assertEqual([], result["entries"])
        self.assertIn("UNIQUE", result["excluded"][0]["reason"])

    def test_ddl_and_empty_mutations_do_not_receive_evidence(self):
        for sql in ["DROP TABLE docs", "DELETE FROM docs WHERE false"]:
            self.assertEqual(
                [], reference([self.case(sql)], {"sql-0947": self.schema()})["entries"]
            )

    def test_campaign_has_exact_source_owned_case_ids(self):
        root = (
            Path(__file__).resolve().parents[1]
            / "zig/pkg/antfly-embedded/src/sql/fixtures"
        )
        campaign = json.loads((root / "sql_document_campaign.json").read_text())[
            "entries"
        ]
        inventory = {
            case["id"]: case
            for case in json.loads((root / "sql_parity_inventory.json").read_text())[
                "entries"
            ]
        }
        self.assertEqual(211, len(campaign))
        self.assertEqual(211, len({case["id"] for case in campaign}))
        for case in campaign:
            self.assertEqual(inventory[case["id"]]["family"], case["family"])
            self.assertNotIn("sql", case)
            self.assertNotIn("params", case)


if __name__ == "__main__":
    unittest.main()
