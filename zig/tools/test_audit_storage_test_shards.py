#!/usr/bin/env python3

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("audit_storage_test_shards.py")
SPEC = importlib.util.spec_from_file_location("audit_storage_test_shards", SCRIPT)
assert SPEC and SPEC.loader
audit = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = audit
SPEC.loader.exec_module(audit)


class StorageTestShardAuditTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def write(self, relative: str, contents: str) -> None:
        destination = self.root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(contents, encoding="utf-8")

    def test_allows_test_modules_owned_by_a_shard(self):
        self.write("db/query/scan.zig", 'test "scan" {}\n')
        self.write("db/query/helper.zig", "pub fn helper() void {}\n")
        self.assertEqual(
            [],
            audit.uncovered_modules(self.root, ["storage.db.query."]),
        )

    def test_reports_new_unowned_test_module(self):
        self.write("new_backend.zig", 'test "round trip" {}\n')
        self.assertEqual(
            ["new_backend.zig (storage.new_backend.)"],
            audit.uncovered_modules(self.root, ["storage.db."]),
        )

    def test_mod_file_accepts_directory_namespace(self):
        self.write("lsm/mod.zig", 'test "manifest" {}\n')
        self.assertEqual(
            [],
            audit.uncovered_modules(self.root, ["storage.lsm."]),
        )


if __name__ == "__main__":
    unittest.main()
