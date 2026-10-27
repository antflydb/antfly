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

"""Independently verify logical and observable JSONB equality against PostgreSQL."""

import json
from pathlib import Path
import unittest

from generate_sql_postgres_reference import postgres


class JsonbDisplayReferenceTest(unittest.TestCase):
    def test_logical_and_display_identity(self):
        path = (
            Path(__file__).resolve().parents[1]
            / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_jsonb_display_reference.json"
        )
        fixture = json.loads(path.read_text())
        self.assertEqual(19, len(fixture["entries"]))
        with postgres() as db:
            for entry in fixture["entries"]:
                with self.subTest(left=entry["left"], right=entry["right"]):
                    actual = db.execute(
                        "SELECT a=b,a::text=b::text FROM "
                        "(SELECT %s::jsonb a,%s::jsonb b) s",
                        (entry["left"], entry["right"]),
                    ).fetchone()
                    self.assertEqual(
                        (entry["logical_equal"], entry["display_equal"]), actual
                    )

    def test_numeric_scale_and_float_signed_zero(self):
        with postgres() as db:
            for kind, left, right, display in (
                ("numeric", "1.20", "1.2", False),
                ("numeric", "1.20", "120e-2", True),
                ("float8", "-0", "0", False),
                ("float8", "0", "0.0", True),
            ):
                with self.subTest(kind=kind, left=left, right=right):
                    actual = db.execute(
                        "SELECT a=b,a::text=b::text FROM "
                        f"(SELECT %s::{kind} a,%s::{kind} b) s",
                        (left, right),
                    ).fetchone()
                    self.assertEqual((True, display), actual)


if __name__ == "__main__":
    unittest.main()
