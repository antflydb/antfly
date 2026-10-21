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

"""Guard generation, reduction and comparison semantics against false positives."""

import base64
import json
import os
import sys
from pathlib import Path
import unittest
from differential import ddmin, generated, normalized_output, reduce_case, execute


class DifferentialToolsTests(unittest.TestCase):
    def test_seed_is_reproducible_and_varies(self):
        self.assertEqual(list(generated(7, 50)), list(generated(7, 50)))
        self.assertNotEqual(list(generated(7, 50)), list(generated(8, 50)))

    def test_reducer_preserves_failure_and_becomes_one_minimal(self):
        budget = [100]
        self.assertEqual(
            ddmin("xyzAqB123", lambda s: "A" in s and "B" in s, budget), "AB"
        )

    def test_reducer_bounds_work(self):
        budget = [2]
        ddmin("abcd", lambda s: False, budget)
        self.assertEqual(budget, [0])

    def test_unicode_reduction_keeps_valid_input(self):
        case = {
            "args": ["-e", "xxa"],
            "input": base64.b64encode("αβγ".encode()).decode(),
        }

        def mismatch(c):
            return "a" in c["args"][-1] and "β" in base64.b64decode(c["input"]).decode()

        reduced = reduce_case(case, mismatch)
        self.assertEqual(reduced["args"][-1], "a")
        self.assertEqual(base64.b64decode(reduced["input"]).decode(), "β")

    def test_reducer_removes_irrelevant_flags_in_argument_units(self):
        case = {
            "args": ["-P", "-i", "-r", "replacement", "--json", "-e", "a"],
            "input": "",
        }
        reduced = reduce_case(
            case, lambda c: "--json" in c["args"] and c["args"][-1] == "a"
        )
        self.assertEqual(reduced["args"], ["--json", "-e", "a"])

    def test_invalid_json_is_a_reducible_failure(self):
        result = execute(
            Path(sys.executable),
            {"args": ["-c", "print('broken')", "--json"], "input": ""},
            5,
            dict(os.environ),
        )
        self.assertEqual(result["code"], "invalid-json")

    def test_json_normalization_retains_spans_and_omits_timing(self):
        data = {
            "lines": {"text": "é"},
            "line_number": 2,
            "absolute_offset": 3,
            "submatches": [{"start": 0, "end": 2, "match": {"text": "é"}}],
        }
        raw = b"\n".join(
            json.dumps(v).encode()
            for v in [
                {"type": "match", "data": data, "source": {}},
                {"type": "summary", "data": {"elapsed": 42}},
            ]
        )
        self.assertEqual(json.loads(normalized_output(["--json"], raw)), [data])


if __name__ == "__main__":
    unittest.main()
