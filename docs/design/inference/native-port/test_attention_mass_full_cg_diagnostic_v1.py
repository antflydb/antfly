#!/usr/bin/env python3
"""Static and synthetic contract checks; never imports NumPy or runs a solver."""
from __future__ import annotations

import ast
import hashlib
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent
EXPORTER = ROOT / "export_attention_mass_full_cg_fixture_v1.py"
ORACLE = ROOT / "record_attention_mass_python_full_cg_oracle_v1.py"
DRIVER = ROOT / "attention_mass_highs_v180_full_cg_driver_v1.zig"


class FullCgDiagnosticContract(unittest.TestCase):
    def test_exporter_is_exact_initial_matrix_only(self):
        text = EXPORTER.read_text()
        tree = ast.parse(text)
        calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                try:
                    calls.append(ast.unparse(node.func))
                except Exception:
                    pass
        self.assertIn("heavy.load", calls)
        self.assertIn("heavy.serial.base.mass288._mass_context", calls)
        self.assertIn("heavy.serial.base.mass288._mass_matrix", calls)
        self.assertFalse(any(name.endswith(".fit") or name == "fit" for name in calls))
        self.assertIn('"warm_rows": None', text)
        self.assertIn('"held_tensors_loaded": False', text)

    def test_oracle_wraps_frozen_decisions_and_restores_them(self):
        text = ORACLE.read_text()
        ast.parse(text)
        self.assertIn("additions, worst = original_violations", text)
        self.assertLess(text.index("additions, worst = original_violations"),
                        text.index("fitted = a @ raw"))
        self.assertIn("worker._ORIGINAL_PROTO_SOLVE = recorded_proto", text)
        self.assertIn("worker._ORIGINAL_PROTO_SOLVE = original_proto", text)
        self.assertIn("initial_indices=None", text)
        self.assertIn("minimum_threshold_margin", text)
        self.assertIn("addition_cutoff_margin", text)

    def test_native_path_uses_unmodified_controller_and_adapter(self):
        text = DRIVER.read_text()
        self.assertIn("cg.solvePrimaryConstraintGeneration", text)
        self.assertIn("highs.solveRestricted", text)
        self.assertIn(".solve_restricted_fn = tracedSolve", text)
        self.assertIn("restricted_matrix_sha256", text)
        self.assertIn("rounds_equal", text)
        self.assertIn("selected_rows_equal", text)
        self.assertIn("bias_bytes_equal", text)
        self.assertIn("error.FullCgParityMismatch", text)
        self.assertNotIn("1e-8", text)

    def test_restricted_digest_detects_trajectory_change(self):
        row0 = (1.0).hex().encode() + (2.0).hex().encode()
        row1 = (3.0).hex().encode() + (4.0).hex().encode()
        first = hashlib.sha256(row0 + row1).hexdigest()
        reordered = hashlib.sha256(row1 + row0).hexdigest()
        changed = hashlib.sha256(row0 + (row1 + b"x")).hexdigest()
        self.assertNotEqual(first, reordered)
        self.assertNotEqual(first, changed)


if __name__ == "__main__":
    unittest.main()
