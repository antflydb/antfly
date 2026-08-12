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
        self.manifest = self.root / "test_manifest.zig"

    def tearDown(self):
        self.temporary.cleanup()

    def write(self, relative: str, contents: str) -> None:
        destination = self.root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(contents, encoding="utf-8")

    def audit(self, filters, dedicated=()):
        return audit.audit_manifest(self.root, self.manifest, filters, dedicated)

    def test_accepts_authoritative_exactly_once_ownership(self):
        self.write("db/query/scan.zig", 'test "scan" {}\n')
        self.write("test_manifest.zig", '_ = @import("db/query/scan.zig");\n')
        self.assertEqual([], self.audit(["storage.db.query."]))

    def test_rejects_unimported_test_under_an_owned_directory(self):
        self.write("db/query/scan.zig", 'test "scan" {}\n')
        self.write("test_manifest.zig", "")
        self.assertEqual(
            ["test source missing from manifest: db/query/scan.zig"],
            self.audit(["storage.db.query."]),
        )

    def test_rejects_duplicate_manifest_import(self):
        self.write("db/query/scan.zig", 'test "scan" {}\n')
        self.write(
            "test_manifest.zig",
            '_ = @import("db/query/scan.zig");\n_ = @import("db/query/scan.zig");\n',
        )
        self.assertEqual(
            ["manifest imports db/query/scan.zig 2 times"],
            self.audit(["storage.db.query."]),
        )

    def test_rejects_multiple_shard_owners(self):
        self.write("db/query/scan.zig", 'test "scan" {}\n')
        self.write("test_manifest.zig", '_ = @import("db/query/scan.zig");\n')
        self.assertEqual(
            [
                "test source has multiple shard owners: db/query/scan.zig "
                "(storage.db., storage.db.query.)"
            ],
            self.audit(["storage.db.", "storage.db.query."]),
        )

    def test_rejects_stale_manifest_entry(self):
        self.write("db/query/helper.zig", "pub fn helper() void {}\n")
        self.write("test_manifest.zig", '_ = @import("db/query/helper.zig");\n')
        self.assertEqual(
            ["manifest entry has no test declarations: db/query/helper.zig"],
            self.audit(["storage.db.query."]),
        )

    def test_accepts_test_owned_by_separately_linked_suite(self):
        self.write("kernel_owner_test.zig", 'test "owner boundary" {}\n')
        self.write("test_manifest.zig", "")
        self.assertEqual(
            [],
            self.audit([], [Path("kernel_owner_test.zig")]),
        )

    def test_rejects_dedicated_test_also_imported_by_manifest(self):
        self.write("kernel_owner_test.zig", 'test "owner boundary" {}\n')
        self.write(
            "test_manifest.zig",
            '_ = @import("kernel_owner_test.zig");\n',
        )
        self.assertEqual(
            [
                "dedicated test source also imported by manifest: "
                "kernel_owner_test.zig"
            ],
            self.audit(
                ["storage.kernel_owner_test."],
                [Path("kernel_owner_test.zig")],
            ),
        )


if __name__ == "__main__":
    unittest.main()
