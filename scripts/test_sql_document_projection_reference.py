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

"""The expanded document oracle must not turn discovery into parity credit."""

from copy import deepcopy
import json
import hashlib
import subprocess
import unittest

from generate_sql_postgres_reference import (
    FIXTURES,
    SEEDS,
    document_reference,
    postgres,
)


class DocumentProjectionReferenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cases = {
            entry["id"]: entry
            for entry in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
        }
        cls.schemas = {
            entry["id"]: entry["schema"]
            for entry in json.loads(
                (FIXTURES / "sql_document_campaign.json").read_text()
            )["entries"]
        }

    def test_exact_success_and_failure_preserve_complete_independent_images(self):
        ids = ["sql-0956", "sql-0960", "sql-1069"]
        with postgres() as db:
            result = document_reference(
                db, [self.cases[key] for key in ids], self.schemas, annotation_only=True
            )
        self.assertEqual([], result["excluded"])
        changed, slug, failed = result["entries"]
        self.assertEqual(1, changed["affected"])
        self.assertEqual("Joined", changed["final"][0]["value"]["title"])
        self.assertEqual(1, slug["affected"])
        self.assertEqual("Draft Launch", slug["seeds"][0]["value"]["status"])
        self.assertEqual("Joined", slug["final"][0]["value"]["title"])
        self.assertEqual("42703", failed["sqlstate"])
        self.assertEqual(SEEDS, failed["final"])

    def test_zero_effect_is_not_a_blanket_mutation_waiver(self):
        case = deepcopy(self.cases["sql-0956"])
        case["sql"] = "UPDATE docs SET title='must-not-run' WHERE false"
        with postgres() as db:
            result = document_reference(db, [case], self.schemas, annotation_only=True)
        self.assertEqual([], result["entries"])
        self.assertIn("non-exercising", result["excluded"][0]["reason"])

    def test_annotations_cannot_qualify_generated_assignment_or_alias_contracts(self):
        ids = ["sql-0956", "sql-1079", "sql-1082"]
        with postgres() as db:
            default = document_reference(db, [self.cases[ids[0]]], self.schemas)
            expansion = document_reference(
                db,
                [self.cases[key] for key in ids[1:]],
                self.schemas,
                annotation_only=True,
            )
        self.assertEqual([], default["entries"])
        self.assertEqual([], expansion["entries"])
        self.assertEqual(ids[1:], [entry["id"] for entry in expansion["excluded"]])

    def test_supplemental_json_path_schemas_are_exact_source_owned_profiles(self):
        manifest = json.loads(
            (FIXTURES / "sql_document_projection_campaign.json").read_text()
        )
        inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
        source_bytes = subprocess.check_output(
            ["git", "show", inventory["source_commit"] + ":" + inventory["source_path"]]
        )
        self.assertEqual(
            inventory["source_sha256"], hashlib.sha256(source_bytes).hexdigest()
        )
        source = json.loads(source_bytes)["entries"]
        self.assertEqual(12, len(manifest["entries"]))
        for profile in manifest["entries"]:
            original = source[int(profile["id"][4:]) - 1]
            self.assertEqual(original["family"], profile["family"])
            self.assertEqual(
                json.loads(original["source_schema_json"]), profile["schema"]
            )


if __name__ == "__main__":
    unittest.main()
