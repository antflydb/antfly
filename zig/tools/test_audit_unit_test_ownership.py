# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

import importlib.util
import json
import struct
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location(
    "audit_unit_test_ownership",
    Path(__file__).with_name("audit_unit_test_ownership.py"),
)
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)
audit, inventory, protocol_inventory = (
    module.audit,
    module.inventory,
    module.protocol_inventory,
)


class OwnershipTests(unittest.TestCase):
    def test_partitioned_inventory_counts_each_named_execution(self):
        names = inventory(
            "[db-core-category] TEST\tstorage.db.test.a\nTEST\troot.test_0\n[db-core-complement] TEST\tstorage.db.test.b\n"
        )
        self.assertEqual(names, ["storage.db.test.a", "storage.db.test.b"])

    def test_repeated_execution_within_one_owner_is_not_hidden(self):
        result = audit({"first": ["a", "a"], "second": ["b", "a"]})
        self.assertEqual(result["executions"], 4)
        self.assertEqual(result["unique_tests"], 2)
        self.assertEqual(result["duplicates"], {"a": ["first", "first", "second"]})

    def test_protocol_inventory_reads_metadata_without_executing_tests(self):
        names = b"first\0second\0"
        payload = struct.pack("=IIIIII", len(names), 2, 0, 6, 0, 0) + names
        data = struct.pack("=II", 3, len(payload)) + payload
        self.assertEqual(protocol_inventory(data), ["first", "second"])
        with self.assertRaises(ValueError):
            protocol_inventory(data[:-1])
        with self.assertRaises(ValueError):
            protocol_inventory(b"")

    def test_disjoint_selection_cannot_silently_drop_baseline_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            (base / "before").write_text("TEST\ta\nTEST\tb\n")
            (base / "after").write_text("TEST\ta\n")
            result = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).with_name("audit_unit_test_ownership.py")),
                    "--inventory",
                    "selected",
                    str(base / "after"),
                    "--baseline-inventory",
                    "original",
                    str(base / "before"),
                    "--report",
                    str(base / "report.json"),
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("lost coverage: b", result.stderr)
            self.assertEqual(
                json.loads((base / "report.json").read_text())["duplicates"], {}
            )


if __name__ == "__main__":
    unittest.main()
