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

"""Independent evidence for exact owned-JSON numeric persistence fixtures.

Owned JSON floating values already have binary64 numeric identity internally;
this is deliberately not a PostgreSQL float8-to-JSONB cast test. PostgreSQL
checks the resulting decimal JSONB values, and Python independently checks the
exact binary64-to-decimal interpretation used by the native fixture.
"""

from decimal import Decimal
import re
import unittest

from generate_sql_postgres_reference import FIXTURES, postgres


class ArrayStorageReferenceTest(unittest.TestCase):
    def test_owned_json_numeric_fixture_preserves_exact_values(self):
        source = (FIXTURES.parent / "array_storage.zig").read_text()
        section = source.split(
            'test "SQL flat JSONB stored arrays canonicalize in-memory and parsed numeric values identically" {',
            1,
        )[1].split('\ntest "', 1)[0]
        pairs = re.findall(
            r'\.value = \.\{ \.(float|integer) = ([^}]+) \}, \.token = "([^"]+)"',
            section,
        )
        self.assertEqual(5, len(pairs))
        with postgres() as db:
            for kind, value, token in pairs:
                with self.subTest(kind=kind, value=value):
                    exact = (
                        Decimal.from_float(float(value))
                        if kind == "float"
                        else Decimal(int(value))
                    )
                    self.assertEqual(exact, Decimal(token))
                    self.assertEqual(
                        (True,),
                        db.execute(
                            "SELECT %s::jsonb = %s::jsonb", (str(exact), token)
                        ).fetchone(),
                    )


if __name__ == "__main__":
    unittest.main()
