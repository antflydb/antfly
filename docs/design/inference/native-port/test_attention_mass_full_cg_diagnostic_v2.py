#!/usr/bin/env python3
"""Controlled entrypoint and static checks; no model, fit, held data, or solver."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import numpy as np

ROOT = Path(__file__).resolve().parent
EXPORTER = ROOT / "export_attention_mass_full_cg_fixture_v2.py"
ORACLE = ROOT / "record_attention_mass_python_full_cg_oracle_v2.py"
DRIVER = ROOT / "attention_mass_highs_v180_full_cg_driver_v2.zig"
README = ROOT / "attention-mass-highs-v180-full-cg-driver-v2.md"
HEAVY_NAME = "run_attention_matching_full36_serial_final_build_heavy_v15_confirmed_parent_lifetime_main3"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class ControlledHeavy:
    def __init__(self, root: Path, mutate_source=False, bad_exact=False):
        self.heavy_path = root / "heavy.py"
        self.mass_path = root / "mass.py"
        self.kernel_path = root / "kernel.py"
        self.source_path = root / "source.json"
        for path, data in ((self.heavy_path, b"heavy\n"),
                           (self.mass_path, b"mass\n"),
                           (self.kernel_path, b"kernel\n"),
                           (self.source_path, b"source\n")):
            path.write_bytes(data)
        self.__file__ = str(self.heavy_path)
        self.checked_calls = 0
        self.origins_calls = 0
        self.mutate_source = mutate_source
        self.bad_exact = bad_exact
        self.fit_calls = 0
        self.kernel = SimpleNamespace(__file__=str(self.kernel_path))
        self.kernel._mass_context = self.mass_context
        self.kernel._mass_matrix = self.mass_matrix
        self.mass_recipe = SimpleNamespace(__file__=str(self.mass_path), mass=self.kernel)
        base = SimpleNamespace(mass288=self.mass_recipe,
                               seed_checked=self.seed_checked,
                               training_checked=self.training_checked)
        self.serial = SimpleNamespace(base=base)

    def checked(self, path, expected, execution):
        self.checked_calls += 1
        if digest(Path(path)) != expected or execution is not True:
            raise ValueError("bad plan")
        return json.loads(Path(path).read_text())

    def origins(self, plan):
        self.origins_calls += 1
        return plan["expected_loaded_origins"]

    def load(self, plan):
        return {}, {}, {"owner_layer": 14, "original_seed": {}, "training": {}}

    def seed_checked(self, owner, seed):
        keys = np.array([[0.5, -0.25], [0.0, 0.75]], dtype=np.float16)
        return 2, keys, keys.copy(), np.zeros(2, np.float32), keys.copy(), np.ones(2)

    def training_checked(self, training):
        q = np.array([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]], dtype=np.float64)
        count = 3 if self.bad_exact else 2
        exact = np.arange(count * 2, dtype=np.float16).reshape(count, 2) / 4
        return q, exact, exact.copy(), 0.5, [{"kv_head": 0}]

    @staticmethod
    def mass_context(q, exact, scale):
        logits = q @ exact.astype(np.float64).T * scale
        maxima = logits.max(axis=1)
        shifted = np.exp(logits - maxima[:, None])
        return maxima, shifted.sum(axis=1), shifted

    def mass_matrix(self, q, compact, scale, maxima, exact_mass):
        if self.mutate_source:
            self.source_path.write_bytes(b"mutated\n")
        return np.exp(q @ compact.T * scale - maxima[:, None]) / exact_mass[:, None]


class FullCgDiagnosticV2(unittest.TestCase):
    def load_exporter(self, fake):
        old = sys.modules.get(HEAVY_NAME)
        sys.modules[HEAVY_NAME] = fake
        try:
            name = "controlled_full_cg_exporter_v2_" + next(tempfile._get_candidate_names())
            spec = importlib.util.spec_from_file_location(name, EXPORTER)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        finally:
            if old is None:
                del sys.modules[HEAVY_NAME]
            else:
                sys.modules[HEAVY_NAME] = old
        module.QUERY_ROWS = 3
        module.EXACT_ROWS = 2
        module.DIM = 2
        module.COLUMNS = 2
        module.HEAVY = fake.heavy_path
        module.HEAVY_SHA = digest(fake.heavy_path)
        module.MASS = fake.mass_path
        module.MASS_SHA = digest(fake.mass_path)
        return module

    def make_plan(self, root, fake):
        origins = [
            {"module": "fake_heavy", "path": str(fake.heavy_path),
             "sha256": digest(fake.heavy_path)},
            {"module": "fake_mass", "path": str(fake.mass_path),
             "sha256": digest(fake.mass_path)},
            {"module": "fake_kernel", "path": str(fake.kernel_path),
             "sha256": digest(fake.kernel_path)},
        ]
        plan = {
            "cell": {"owner_layer": 14, "block_start": 2048,
                     "retained_rows": 2},
            "dependencies": [{"path": row["path"], "sha256": row["sha256"]}
                             for row in origins],
            "source": {"capture": {"path": str(fake.source_path),
                                     "sha256": digest(fake.source_path)}},
            "expected_loaded_origins": origins,
            "expected_final_build": {"runtime_sha256": "a" * 64},
        }
        path = root / "plan.json"
        path.write_text(json.dumps(plan, sort_keys=True))
        return path

    def invoke(self, module, plan, output):
        argv = [str(EXPORTER), "--plan", str(plan), "--plan-sha256", digest(plan),
                "--output-dir", str(output)]
        with mock.patch.object(sys, "argv", argv):
            return module.main()

    def test_actual_entrypoint_separates_query_and_exact_geometry(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as td:
            root = Path(td); fake = ControlledHeavy(root)
            module = self.load_exporter(fake); plan = self.make_plan(root, fake)
            output = root / "output"
            self.assertEqual(self.invoke(module, plan, output), 0)
            manifest = json.loads((output / "manifest.json").read_text())
            self.assertEqual(manifest["input"]["query_rows"], 3)
            self.assertEqual(manifest["input"]["exact_rows"], 2)
            self.assertEqual(manifest["matrix"]["shape"], [3, 2])
            self.assertEqual(fake.checked_calls, 3)
            self.assertEqual(fake.origins_calls, 2)
            self.assertEqual(fake.fit_calls, 0)
            self.assertFalse(manifest["held_tensors_loaded"])
            self.assertEqual(manifest["origins"]["mass_context"],
                             "mass288.mass._mass_context")

    def test_actual_entrypoint_rejects_old_equal_row_geometry(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as td:
            root = Path(td); fake = ControlledHeavy(root, bad_exact=True)
            module = self.load_exporter(fake); plan = self.make_plan(root, fake)
            with self.assertRaisesRegex(ValueError, "source geometry changed"):
                self.invoke(module, plan, root / "output")

    def test_terminal_source_mutation_prevents_manifest(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as td:
            root = Path(td); fake = ControlledHeavy(root, mutate_source=True)
            module = self.load_exporter(fake); plan = self.make_plan(root, fake)
            output = root / "output"
            with self.assertRaisesRegex(ValueError, "input edge changed"):
                self.invoke(module, plan, output)
            self.assertFalse((output / "manifest.json").exists())

    def test_loaded_module_identity_is_not_path_hash_only(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as td:
            root = Path(td); fake = ControlledHeavy(root)
            module = self.load_exporter(fake); plan = self.make_plan(root, fake)
            fake.__file__ = str(root / "different-heavy.py")
            Path(fake.__file__).write_bytes(fake.heavy_path.read_bytes())
            with self.assertRaisesRegex(ValueError, "loaded heavy identity changed"):
                self.invoke(module, plan, root / "output")

    def test_frozen_mass_context_is_three_tuple_and_kernel_is_plan_bound(self):
        path = Path("/private/tmp/attention_matching_direct_key_continuation_v6.py")
        tree = __import__("ast").parse(path.read_text())
        fn = next(node for node in tree.body
                  if isinstance(node, __import__("ast").FunctionDef) and
                  node.name == "_mass_context")
        returns = [node for node in __import__("ast").walk(fn)
                   if isinstance(node, __import__("ast").Return)]
        self.assertEqual(len(returns), 1)
        self.assertIsInstance(returns[0].value, __import__("ast").Tuple)
        self.assertEqual(len(returns[0].value.elts), 3)
        exporter = EXPORTER.read_text()
        self.assertIn("mass_recipe.mass", exporter)
        self.assertIn("mass kernel is not bound by plan", exporter)

    def test_actual_entrypoint_guards_plan_digest_and_fresh_output(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as td:
            root = Path(td); fake = ControlledHeavy(root)
            module = self.load_exporter(fake); plan = self.make_plan(root, fake)
            output = root / "output"
            argv = [str(EXPORTER), "--plan", str(plan), "--plan-sha256", "0" * 64,
                    "--output-dir", str(output)]
            with mock.patch.object(sys, "argv", argv):
                with self.assertRaisesRegex(ValueError, "bad plan"):
                    module.main()
            output.mkdir()
            with self.assertRaisesRegex(ValueError, "output must be fresh"):
                self.invoke(module, plan, output)

    def test_oracle_driver_and_library_path_are_v2_consistent(self):
        oracle = ORACLE.read_text(); driver = DRIVER.read_text(); readme = README.read_text()
        self.assertIn('FIXTURE_SCHEMA = "antfly.attention-matching.mass-full-cg-fixture.v2"', oracle)
        self.assertIn('SCHEMA = "antfly.attention-matching.mass-python-full-cg-oracle.v2"', oracle)
        self.assertIn('fixture_schema = "antfly.attention-matching.mass-full-cg-fixture.v2"', driver)
        self.assertIn('oracle_schema = "antfly.attention-matching.mass-python-full-cg-oracle.v2"', driver)
        self.assertIn("zig/zig-out/native-highs-v180/libhighs.a", readme)
        self.assertNotIn("native-highs-v180/lib/libhighs.a", readme)


if __name__ == "__main__":
    unittest.main()
