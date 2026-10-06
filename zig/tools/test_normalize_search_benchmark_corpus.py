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


SCRIPT = Path(__file__).with_name("normalize_search_benchmark_corpus.py")
SPEC = importlib.util.spec_from_file_location("normalize_search_corpus", SCRIPT)
assert SPEC and SPEC.loader
normalizer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = normalizer
SPEC.loader.exec_module(normalizer)


class NormalizeSearchCorpusTest(unittest.TestCase):
    def test_extracts_declared_field_into_deterministic_canonical_jsonl(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            output = root / "canonical.jsonl"
            manifest_path = root / "manifest.json"
            source.write_text(
                '{"title":"ignored","body":"Hello\\nworld"}\n\n{"body":"CAFÉ 😀"}\n',
                encoding="utf-8",
            )
            manifest = normalizer.normalize(source, output, manifest_path, "body")

            self.assertEqual(
                output.read_text(encoding="utf-8"),
                '{"text":"Hello\\nworld"}\n{"text":"CAFÉ 😀"}\n',
            )
            self.assertEqual(manifest["output"]["documents"], 2)
            self.assertEqual(manifest["output"]["rejected_documents"], 0)
            self.assertEqual(json.loads(manifest_path.read_text()), manifest)

    def test_fails_closed_when_declared_field_is_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            source.write_text('{"text":"wrong field"}\n', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "field 'body'"):
                normalizer.normalize(
                    source, root / "out.jsonl", root / "manifest.json", "body"
                )


if __name__ == "__main__":
    unittest.main()
