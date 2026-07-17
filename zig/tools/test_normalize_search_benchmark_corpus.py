#!/usr/bin/env python3

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
                '{"title":"ignored","body":"Hello\\nworld"}\n\n'
                '{"body":"CAFÉ 😀"}\n',
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
                normalizer.normalize(source, root / "out.jsonl", root / "manifest.json", "body")


if __name__ == "__main__":
    unittest.main()
