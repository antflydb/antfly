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

"""Independent JSONB input-domain contracts, including non-rendered parameters."""

import json
import struct
import unittest

import psycopg
from psycopg.adapt import Dumper
from psycopg.pq import Format

from generate_sql_postgres_reference import FIXTURES, postgres


class JsonbDomainReferenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = postgres()
        cls.db = cls.server.__enter__()
        cls.addClassCleanup(cls.server.__exit__, None, None, None)
        cls.fixture = json.loads(
            (FIXTURES / "sql_jsonb_domain_reference.json").read_text()
        )

    def register(self, oid):
        class Payload:
            def __init__(self, data):
                self.data = data

        dumper = type(
            "JsonbDomainDumper",
            (Dumper,),
            {"oid": oid, "format": Format.BINARY, "dump": lambda self, obj: obj.data},
        )
        self.db.adapters.register_dumper(Payload, dumper)
        return Payload

    def test_text_and_binary_scalar_and_non_rendered_array_inputs(self):
        self.assertEqual(22, len(self.fixture["entries"]))
        for entry in self.fixture["entries"]:
            raw = entry["input"]
            with self.subTest(input=raw, mode="text"):
                if "error" in entry:
                    with self.assertRaises(psycopg.Error) as caught:
                        self.db.execute("SELECT jsonb_typeof(%s::jsonb)", (raw,))
                    self.assertEqual(entry["error"], caught.exception.sqlstate)
                    self.db.rollback()
                else:
                    kind, length, digest, text = self.db.execute(
                        "SELECT jsonb_typeof(v),length(v::text),md5(v::text),"
                        "CASE WHEN length(v::text)<512 THEN v::text END "
                        "FROM (SELECT %s::jsonb v) s",
                        (raw,),
                    ).fetchone()
                    self.assertEqual(
                        (entry["kind"], entry["length"], entry["md5"], entry["text"]),
                        (kind, length, digest, text),
                    )
            for oid in (3802, 3807):
                with self.subTest(input=raw, oid=oid):
                    Payload = self.register(oid)
                    wire = b"\x01" + raw.encode()
                    if oid == 3807:
                        wire = (
                            struct.pack("!iiIiii", 1, 0, 3802, 1, 1, len(wire)) + wire
                        )
                    sql = (
                        "SELECT jsonb_typeof($1)"
                        if oid == 3802
                        else "SELECT cardinality($1)"
                    )
                    if "error" in entry:
                        with self.assertRaises(psycopg.Error) as caught:
                            with psycopg.RawCursor(self.db) as cursor:
                                cursor.execute(sql, [Payload(wire)])
                                cursor.fetchone()
                        self.assertEqual(entry["error"], caught.exception.sqlstate)
                        self.db.rollback()
                    else:
                        with psycopg.RawCursor(self.db) as cursor:
                            cursor.execute(sql, [Payload(wire)])
                            self.assertEqual(
                                (entry["kind"] if oid == 3802 else 1,),
                                cursor.fetchone(),
                            )

    def test_binary_receiver_diagnostics(self):
        self.assertEqual(5, len(self.fixture["binary_errors"]))
        Payload = self.register(3802)
        for entry in self.fixture["binary_errors"]:
            with self.subTest(binary=entry["binary"]):
                with self.assertRaises(psycopg.Error) as caught:
                    with psycopg.RawCursor(self.db) as cursor:
                        cursor.execute(
                            "SELECT jsonb_typeof($1)",
                            [Payload(bytes.fromhex(entry["binary"]))],
                        )
                        cursor.fetchone()
                self.assertEqual(entry["error"], caught.exception.sqlstate)
                self.db.rollback()


if __name__ == "__main__":
    unittest.main()
