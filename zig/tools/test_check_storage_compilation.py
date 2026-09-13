# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Test measurement accounting without invoking a compiler."""

import importlib.util
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "check_storage_compilation",
    Path(__file__).with_name("check_storage_compilation.py"),
)
assert SPEC and SPEC.loader
measurement = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(measurement)


class BuildMemoryAccounting(unittest.TestCase):
    def test_concurrent_descendants_exclude_unrelated_builds(self):
        snapshot = """
        100 1 20
        101 100 100
        102 100 200
        103 102 50
        200 1 9000
        201 200 8000
        """
        self.assertEqual(measurement.tree_rss(snapshot, 100), (370 * 1024, 200 * 1024))

    def test_finished_and_missing_processes(self):
        self.assertEqual(measurement.tree_rss("200 1 9000", 100), (0, 0))
        self.assertEqual(measurement.tree_rss("100 1 20", 100), (20 * 1024, 20 * 1024))


if __name__ == "__main__":
    unittest.main()
