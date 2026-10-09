# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Independent live PostgreSQL checks for complete text-array codec fixtures."""

import json
import unittest

import psycopg

from generate_sql_postgres_reference import FIXTURES, postgres


class ArrayTextReferenceTest(unittest.TestCase):
    def test_complete_binary_values_and_error_states(self):
        fixture = json.loads((FIXTURES / "sql_array_text_reference.json").read_text())
        self.assertEqual(19, len(fixture["entries"]))
        self.assertEqual(19, len(fixture["errors"]))
        # Identifiers are fixture-owned, but never interpolate arbitrary SQL.
        types = {
            "text": "text",
            "int2": "int16",
            "int4": "int32",
            "int8": "int64",
            "real": "float32",
            "float8": "float64",
            "bool": "boolean",
            "jsonb": "jsonb",
            "uuid": "uuid",
        }
        with postgres() as db:
            for entry in fixture["entries"]:
                with self.subTest(input=entry["input"], kind=entry["sql_type"]):
                    kind = entry["sql_type"]
                    self.assertEqual(types[kind], entry["element_type"])
                    (binary,) = db.execute(
                        "SELECT pg_catalog.encode(pg_catalog.array_send(%s::"
                        + kind
                        + "[]), 'hex')",
                        (entry["input"],),
                    ).fetchone()
                    self.assertEqual(entry["binary"], binary)
            for entry in fixture["errors"]:
                with self.subTest(input=entry["input"], kind=entry["sql_type"]):
                    kind = entry["sql_type"]
                    self.assertEqual(types[kind], entry["element_type"])
                    with self.assertRaises(psycopg.Error) as caught:
                        db.execute("SELECT %s::" + kind + "[]", (entry["input"],))
                    self.assertEqual(entry["code"], caught.exception.sqlstate)
                    db.rollback()


if __name__ == "__main__":
    unittest.main()
