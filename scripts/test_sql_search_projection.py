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

"""PostgreSQL oracle for cells projected from captured native search hits.

This verifies SQL projection semantics, not Antfly ranking or search functions.
Those require native index/capture evidence and receive no original-case credit
from this oracle.
"""

import unittest

from generate_sql_postgres_reference import postgres


class SearchProjectionTest(unittest.TestCase):
    def test_jsonb_projection_preserves_sql_null_json_null_and_numeric_scale(self):
        with postgres() as db:
            cursor = db.execute(
                "SELECT payload IS NULL,payload->>'amount',payload->>'fraction' "
                "FROM (VALUES (1,NULL::jsonb),(2,'null'::jsonb),"
                '(3,\'{"amount":9007199254740993.0,"fraction":0.123456789012345678901}\'::jsonb)) '
                "v(id,payload) ORDER BY id"
            )
            self.assertEqual([16, 25, 25], [c.type_code for c in cursor.description])
            self.assertEqual(
                [
                    (True, None, None),
                    (False, None, None),
                    (False, "9007199254740993.0", "0.123456789012345678901"),
                ],
                cursor.fetchmany(4),
            )
