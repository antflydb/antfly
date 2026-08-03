#!/usr/bin/env python3

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
