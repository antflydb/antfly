#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).with_name("compare_test_inventories.py")
SPEC = importlib.util.spec_from_file_location("compare_test_inventories", SCRIPT)
assert SPEC and SPEC.loader
inventories = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = inventories
SPEC.loader.exec_module(inventories)


class CompareTestInventoriesTest(unittest.TestCase):
    def test_parses_inventory_lines_among_diagnostics(self):
        self.assertEqual(
            frozenset({"storage.db.test.one", "storage.db.test.two"}),
            inventories.parse_inventory(
                "diagnostic\n"
                "TEST\tstorage.db.test.one\n"
                "TEST\tstorage.db.test.two\n"
            ),
        )

    def test_rejects_empty_inventory(self):
        with self.assertRaisesRegex(ValueError, "no inventory entries"):
            inventories.parse_inventory("diagnostic only\n")

    def test_rejects_duplicate_inventory_names(self):
        with self.assertRaisesRegex(ValueError, "duplicate inventory entries"):
            inventories.parse_inventory("TEST\tone\nTEST\tone\n")

    def test_excludes_unnamed_import_aggregation_tests_by_default(self):
        output = "TEST\troot.test_0\nTEST\tstorage.db.test.named\n"
        self.assertEqual(
            frozenset({"storage.db.test.named"}),
            inventories.parse_inventory(output),
        )
        self.assertEqual(
            frozenset({"root.test_0", "storage.db.test.named"}),
            inventories.parse_inventory(output, include_unnamed=True),
        )

    def test_combines_disjoint_executable_inventories(self):
        with mock.patch.object(
            inventories,
            "executable_inventory",
            side_effect=[frozenset({"one"}), frozenset({"two"})],
        ):
            self.assertEqual(
                frozenset({"one", "two"}),
                inventories.combined_executable_inventory([Path("a"), Path("b")]),
            )

    def test_rejects_overlap_between_executables(self):
        with mock.patch.object(
            inventories,
            "executable_inventory",
            side_effect=[frozenset({"shared"}), frozenset({"shared"})],
        ):
            with self.assertRaisesRegex(ValueError, "duplicate inventory entries"):
                inventories.combined_executable_inventory([Path("a"), Path("b")])

    def test_reports_missing_and_added_names(self):
        self.assertEqual(
            (["baseline-only"], ["candidate-only"]),
            inventories.inventory_diff(
                frozenset({"shared", "baseline-only"}),
                frozenset({"shared", "candidate-only"}),
            ),
        )


if __name__ == "__main__":
    unittest.main()
