#!/usr/bin/env python3
"""Lightweight source-contract checks; this never compiles or links HiGHS."""
from __future__ import annotations

import re
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
SOURCE = HERE / "attention_mass_highs_v180_dynamic_cg_cli_v1.zig"


class DynamicCliSourceContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = SOURCE.read_text()

    def test_no_machine_or_fixed_cell_paths(self):
        for forbidden in (
            "/private/tmp",
            "/Users/",
            "owner14",
            "block2048",
            "o14-b2048",
        ):
            self.assertNotIn(forbidden, self.text)

    def test_profiles_and_named_modules_are_explicit(self):
        for module in (
            '@import("attention_mass_lp")',
            '@import("attention_mass_cg")',
            '@import("attention_mass_highs")',
        ):
            self.assertIn(module, self.text)
        self.assertRegex(self.text, r"rows == 912 and \(columns == 288 or columns == 352\)")
        self.assertIn("rows == 3000 and columns == 288", self.text)
        self.assertIn("pub fn solveDynamic", self.text)

    def test_retry_policy_and_scientific_logic_stay_in_pinned_modules(self):
        # The CLI may report tolerances supplied by CG, but must not choose or
        # alter them, clip weights, or implement a competing pricing loop.
        self.assertNotIn("1e-9", self.text)
        self.assertNotIn("1e-10", self.text)
        self.assertNotRegex(self.text, r"clip|basis[_ -]?reuse")
        self.assertIn("cg.solvePrimaryConstraintGeneration", self.text)
        self.assertIn("highs.solveRestricted", self.text)

    def test_terminal_recheck_binds_running_binary_and_complete_closure(self):
        self.assertIn("try terminalRecheck(a, io, args[0]", self.text)
        body = re.search(r"fn verifyExecutableClosure.*?\n}\n", self.text, re.S)
        self.assertIsNotNone(body)
        assert body
        for field in (
            "zig_executable",
            "sources.driver",
            "sources.lp",
            "sources.controller",
            "sources.adapter",
            "library_receipt",
            "libhighs",
            "binary",
        ):
            self.assertIn(field, body.group(0))

    def test_outputs_include_scientific_and_lifecycle_evidence(self):
        for token in (
            '"raw-weights.f64le"',
            '"bias.f32le"',
            '"alpha.f64le"',
            '"beta.f64le"',
            '"selected-rows.u64le"',
            ".attempts =",
            ".lifecycle = lifecycle",
            ".rollback_required = true",
            ".rollback_required = false",
        ):
            self.assertIn(token, self.text)


if __name__ == "__main__":
    unittest.main()
