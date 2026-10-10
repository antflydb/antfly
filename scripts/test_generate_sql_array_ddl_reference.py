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

import re
import unittest

from generate_sql_postgres_reference import FIXTURES, postgres


class ArrayDdlReferenceTest(unittest.TestCase):
    def test_native_declared_array_identities_match_postgres_catalog(self):
        source = (FIXTURES.parent / "compiler.zig").read_text()
        section = source.split(
            'test "compiler array DDL retains PostgreSQL builtin element identity without dimension constraints" {',
            1,
        )[1].split('\ntest "', 1)[0]
        cases = re.findall(
            r'\.declaration = "([^"]+)", \.element = Element\.\w+, \.oid = @as\(u32, (\d+)\)',
            section,
        )
        self.assertEqual(16, len(cases))
        with postgres() as db:
            for declaration, oid in cases:
                with self.subTest(declaration=declaration):
                    with db.transaction(force_rollback=True):
                        # The declarations come only from the native test source,
                        # never a caller-controlled identifier or SQL fragment.
                        db.execute(
                            f"CREATE TEMP TABLE arrays (payload {declaration} NOT NULL)"
                        )
                        db.execute(
                            f"ALTER TABLE arrays ADD COLUMN added {declaration} NOT NULL"
                        )
                        columns = db.execute(
                            "SELECT attname, atttypid::integer, attnotnull "
                            "FROM pg_attribute WHERE attrelid='arrays'::regclass "
                            "AND attnum>0 ORDER BY attnum"
                        ).fetchall()
                        self.assertEqual(
                            [("payload", int(oid), True), ("added", int(oid), True)],
                            columns,
                        )
                        # Declared dimensions do not constrain actual values,
                        # including the rank-zero empty array.
                        db.execute("INSERT INTO arrays VALUES ('{}','{}')")
                        self.assertEqual(
                            (0, 0),
                            db.execute(
                                "SELECT cardinality(payload), cardinality(added) FROM arrays"
                            ).fetchone(),
                        )


if __name__ == "__main__":
    unittest.main()
