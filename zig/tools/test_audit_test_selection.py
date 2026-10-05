# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import unittest

import importlib.util
import subprocess
import sys
import tempfile
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "audit_test_selection", Path(__file__).with_name("audit_test_selection.py")
)
assert _SPEC and _SPEC.loader
_module = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_module)
audit = _module.audit


class SelectionAuditTests(unittest.TestCase):
    def run_cli(self, *runtime_args):
        with tempfile.TemporaryDirectory() as directory:
            inventory = Path(directory) / "inventory.txt"
            inventory.write_text(
                "TEST\tapi.test.route retry\nTEST\tstorage.test.lease drain\n"
            )
            return subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).with_name("audit_test_selection.py")),
                    "--inventory",
                    str(inventory),
                    "--",
                    *runtime_args,
                ],
                capture_output=True,
                text=True,
            )

    def test_runtime_filters_accept_interleaved_controls_and_patterns(self):
        result = self.run_cli(
            "route", "--seed=42", "--test-filter", "lease", "--timeout-ms=1000"
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_runtime_exclusions_cannot_silently_empty_selection(self):
        result = self.run_cli("route", "--skip-test-filter", "route")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("test selection matched no runnable tests", result.stdout)
        result = self.run_cli(
            "route", "--skip-test-filter", "route", "--allow-empty-test-filter"
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_zig017_suite_filters_validate_the_runtime_selection(self):
        result = self.run_cli("--suite-filter", "lease")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        result = self.run_cli("--suite-filter", "missing")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("test filter matched no declared tests", result.stdout)

    def test_runtime_unknown_controls_are_rejected(self):
        result = self.run_cli("--test-filte", "route")
        self.assertNotEqual(result.returncode, 0)

    def test_selection_can_span_owners(self):
        self.assertEqual(
            audit(
                ["TEST\tapi.test.route retry\n", "TEST\tstorage.test.lease drain\n"],
                ["route", "lease"],
            ),
            [],
        )

    def test_empty_partition_is_allowed_but_empty_union_is_not(self):
        self.assertEqual(audit(["", "TEST\tapi.test.route retry\n"], ["route"]), [])
        self.assertTrue(audit(["", "TEST\troot.test_0\n"], []))

    def test_missing_filter_is_rejected(self):
        self.assertIn(
            "test filter matched no declared tests: missing",
            audit(["TEST\tapi.test.route retry\n"], ["missing"]),
        )

    def test_explicit_empty_override_and_exclusions(self):
        self.assertEqual(audit([""], ["missing"], allow_empty=True), [])
        inventory = ["TEST\tapi.test.route retry\n"]
        self.assertIn(
            "test selection matched no runnable tests",
            audit(inventory, ["route"], ["route"]),
        )
        self.assertEqual(audit(inventory, ["route"], ["route"], allow_empty=True), [])

    def test_duplicate_owner_is_rejected(self):
        inventory = "TEST\tapi.test.route retry\n"
        self.assertTrue(audit([inventory, inventory], []))

    def test_plain_filters_do_not_match_module_names(self):
        self.assertTrue(audit(["TEST\tstorage.test.route retry\n"], ["storage"]))
        self.assertEqual(audit(["TEST\tstorage.test.route retry\n"], ["storage."]), [])


if __name__ == "__main__":
    unittest.main()
