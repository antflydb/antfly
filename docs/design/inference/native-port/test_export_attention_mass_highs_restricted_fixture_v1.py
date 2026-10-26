#!/usr/bin/env python3
import ast
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


HERE = Path(__file__).resolve().parent
SOURCE = HERE / "export_attention_mass_highs_restricted_fixture_v1.py"
spec = importlib.util.spec_from_file_location("fixture_export", SOURCE)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)


class FixtureExportTest(unittest.TestCase):
    def test_stdlib_only_and_exact_sources(self):
        tree = ast.parse(SOURCE.read_text())
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        self.assertNotIn("numpy", imported)
        self.assertNotIn("scipy", imported)
        witness, variants = module.validate_sources()
        self.assertFalse(witness["accepted_payload"])
        self.assertFalse(variants["held_tensors_loaded"])

    def test_fresh_export_is_raw_and_self_describing(self):
        with tempfile.TemporaryDirectory(prefix="mass-highs-fixture-test-") as parent:
            output = Path(parent) / "fixture"
            manifest = module.export(output)
            loaded = json.loads((output / "manifest.json").read_text())
            self.assertEqual(manifest, loaded)
            self.assertEqual(6, len(loaded["raw_arrays"]))
            self.assertEqual([1128, 288], loaded["raw_arrays"][0]["shape"])
            self.assertEqual("exact raw_weights/alpha/beta payloads", loaded["oracles"]["original_1e_9"]["vector_evidence"])
            self.assertEqual("unavailable; metrics-only oracle", loaded["oracles"]["retry_1e_10"]["vector_evidence"])
            for edge in loaded["raw_arrays"]:
                self.assertEqual(edge["bytes"], (output / edge["path"]).stat().st_size)
            with self.assertRaises(ValueError):
                module.export(output)


if __name__ == "__main__":
    unittest.main()
