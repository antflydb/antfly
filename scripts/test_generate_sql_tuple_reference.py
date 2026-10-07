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

from generate_sql_postgres_reference import postgres
from generate_sql_tuple_reference import reference


class TupleReferenceTest(unittest.TestCase):
    def test_exact_postgres_truth_tables_and_three_valued_negation(self):
        fixture = Path(__file__).resolve().parents[1] / (
            "zig/pkg/antfly-embedded/src/sql/fixtures/sql_tuple_reference.json"
        )
        with postgres() as db:
            result = reference(db)
        self.assertEqual(json.loads(fixture.read_text()), result)
        self.assertEqual(110, len(result["cases"]))
        self.assertEqual(1740, sum(len(case["truth"]) for case in result["cases"]))


if __name__ == "__main__":
    unittest.main()
