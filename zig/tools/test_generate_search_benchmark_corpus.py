#!/usr/bin/env python3
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

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("generate_search_benchmark_corpus.py")
SPEC = importlib.util.spec_from_file_location("corpus_generator", SCRIPT)
assert SPEC and SPEC.loader
generator = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = generator
SPEC.loader.exec_module(generator)


class CorpusGeneratorTest(unittest.TestCase):
    def test_generation_is_deterministic_and_exercises_query_terms(self):
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first.jsonl"
            second = Path(directory) / "second.jsonl"
            first_manifest = generator.generate(32, first)
            second_manifest = generator.generate(32, second)
            self.assertEqual(first_manifest["sha256"], second_manifest["sha256"])
            self.assertEqual(first.read_bytes(), second.read_bytes())
            records = [json.loads(line) for line in first.read_text().splitlines()]
            self.assertEqual(32, len(records))
            self.assertTrue(any("alpha beta" in record["text"] for record in records))
            self.assertTrue(any("gamma gamma" in record["text"] for record in records))


if __name__ == "__main__":
    unittest.main()
