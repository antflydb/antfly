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

"""Independent PostgreSQL evidence for the native precise-schema fixture."""

import json
import struct
import unittest

import psycopg

from generate_sql_postgres_reference import FIXTURES, postgres


class ScalarSchemaReferenceTest(unittest.TestCase):
    def test_native_schema_domains_match_postgres_storage(self):
        source = (FIXTURES.parent.parent / "schema" / "mod.zig").read_text()
        section = source.split("const precise_sql_fixture =", 1)[1].split(";", 1)[0]
        schema = json.loads(
            "".join(
                line.strip()[2:]
                for line in section.splitlines()
                if line.strip().startswith("\\\\")
            )
        )
        properties = schema["document_schemas"]["row"]["schema"]["properties"]
        declarations = {
            "int16": ("smallint", 21),
            "float32": ("real", 700),
            "uuid": ("uuid", 2950),
            "text": ("text", 25),
        }
        self.assertEqual(
            set(declarations), {p["x-antfly-sql-type"] for p in properties.values()}
        )
        ddl = ", ".join(
            f'"{name}" {declarations[prop["x-antfly-sql-type"]][0]}'
            for name, prop in properties.items()
        )
        with postgres() as db:
            db.execute(f"CREATE TEMP TABLE precise ({ddl})")
            columns = db.execute(
                "SELECT attname, atttypid::integer FROM pg_attribute "
                "WHERE attrelid='precise'::regclass AND attnum>0 ORDER BY attnum"
            ).fetchall()
            self.assertEqual(
                [
                    (name, declarations[prop["x-antfly-sql-type"]][1])
                    for name, prop in properties.items()
                ],
                columns,
            )
            db.execute(
                "INSERT INTO precise VALUES (32767, 0.1, "
                "'{A0EEBC999C0B4EF8BB6D6BB9BD380A11}', 'valid')"
            )
            self.assertEqual(
                (
                    32767,
                    struct.unpack("=f", struct.pack("=f", 0.1))[0],
                    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                    "valid",
                ),
                db.execute(
                    'SELECT n, f::float8, id::text, "text" FROM precise'
                ).fetchone(),
            )
            for sql in (
                "INSERT INTO precise(n) VALUES (32768)",
                "INSERT INTO precise(n) VALUES (-32769)",
                "INSERT INTO precise(f) VALUES (1e40)",
                "INSERT INTO precise(f) VALUES (1e-50)",
            ):
                with self.subTest(sql=sql):
                    with db.transaction(force_rollback=True):
                        with self.assertRaises(psycopg.errors.NumericValueOutOfRange):
                            db.execute(sql)
            with db.transaction(force_rollback=True):
                with self.assertRaises(psycopg.errors.ProgramLimitExceeded):
                    db.execute(
                        "INSERT INTO precise(\"text\") VALUES ('invalid' || chr(0))"
                    )


if __name__ == "__main__":
    unittest.main()
