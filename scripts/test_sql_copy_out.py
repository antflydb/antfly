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

"""Independently verify native COPY payload goldens against PostgreSQL 18+."""

import json
from pathlib import Path
import unittest

from generate_sql_postgres_reference import FIXTURES, postgres


class CopyOutTest(unittest.TestCase):
    def test_copy_output_bytes(self):
        root = Path(__file__).resolve().parents[1]
        reference = json.loads((root / "zig/pkg/antfly/src/pgwire/fixtures/copy_out_reference.json").read_text())
        with postgres() as db:
            db.execute("CREATE TABLE usage_records(id TEXT,status TEXT,generated TEXT GENERATED ALWAYS AS (id || status) STORED)")
            with db.cursor() as cursor:
                cursor.executemany("INSERT INTO usage_records(id,status) VALUES(%s,%s)", reference["rows"])
            for profile in reference["profiles"]:
                with self.subTest(sql=profile["sql"]):
                    with db.cursor().copy(profile["sql"]) as stream:
                        actual = b"".join(bytes(block) for block in stream)
                    self.assertEqual(bytes.fromhex(profile["hex"]), actual)
            # Implicit table COPY excludes generated columns. SELECT * does
            # not: explicit query projections are a separate read contract.
            with db.cursor().copy("COPY usage_records TO STDOUT (FORMAT binary)") as stream:
                actual = b"".join(bytes(block) for block in stream)
            self.assertEqual(bytes.fromhex(reference["profiles"][-1]["hex"]), actual)
            jsonb = json.loads((root / "zig/pkg/antfly/src/pgwire/fixtures/copy_out_jsonb_reference.json").read_text())
            for profile in jsonb["profiles"]:
                with self.subTest(jsonb=profile["sql"]):
                    with db.cursor().copy(profile["sql"]) as stream:
                        actual = b"".join(bytes(block) for block in stream)
                    self.assertEqual(bytes.fromhex(profile["hex"]), actual)
            with db.cursor().copy("COPY (SELECT 'a' AS x,'b' AS x) TO STDOUT (FORMAT csv, FORCE_QUOTE(x))") as stream:
                self.assertEqual(b'"a",b\n', b"".join(bytes(block) for block in stream))
            originals = {entry["id"]: entry for entry in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())["entries"]}
            for case_id, profile_index in (("sql-0373", 2), ("sql-0374", 0), ("sql-0383", 5), ("sql-0378", 0)):
                with self.subTest(original=case_id):
                    original = originals[case_id]
                    self.assertEqual([], original["params"])
                    with db.cursor().copy(original["sql"]) as stream:
                        actual = b"".join(bytes(block) for block in stream)
                    self.assertEqual(bytes.fromhex(reference["profiles"][profile_index]["hex"]), actual)


if __name__ == "__main__":
    unittest.main()
