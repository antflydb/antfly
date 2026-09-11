# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.sync_generated import reconcile


class GeneratedSourcesTest(unittest.TestCase):
    def test_check_reports_drift_without_writes_and_sync_repairs_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, destination = root / "cache", root / "sources"
            source.mkdir()
            destination.mkdir()
            (source / "types.zig").write_text("new types")
            (source / "root.zig").write_text("root")
            (destination / "types.zig").write_text("old types")
            (destination / "obsolete.zig").write_text("obsolete")
            before = {
                p.name: (p.read_bytes(), p.stat().st_mtime_ns)
                for p in destination.iterdir()
            }
            drift = reconcile(source, destination, check=True)
            self.assertEqual(
                {line.split(":")[0] for line in drift}, {"missing", "changed", "extra"}
            )
            self.assertEqual(
                before,
                {
                    p.name: (p.read_bytes(), p.stat().st_mtime_ns)
                    for p in destination.iterdir()
                },
            )
            reconcile(source, destination, check=False)
            self.assertEqual(reconcile(source, destination, check=True), [])
            mtimes = {p.name: p.stat().st_mtime_ns for p in destination.iterdir()}
            reconcile(source, destination, check=False)
            self.assertEqual(
                mtimes, {p.name: p.stat().st_mtime_ns for p in destination.iterdir()}
            )
            (destination / "root.zig").unlink()
            reconcile(source, destination, check=False)
            self.assertEqual((destination / "root.zig").read_text(), "root")

    def test_missing_destination_and_nested_obsolete_files(self):
        with tempfile.TemporaryDirectory() as directory:
            source, destination = Path(directory) / "cache", Path(directory) / "sources"
            (source / "nested").mkdir(parents=True)
            (source / "nested/root.zig").write_text("generated")
            self.assertTrue(reconcile(source, destination, check=True))
            self.assertFalse(destination.exists())
            reconcile(source, destination, check=False)
            (source / "nested/root.zig").unlink()
            reconcile(source, destination, check=False)
            self.assertFalse((destination / "nested").exists())

    def test_rejects_missing_source_overlap_and_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(ValueError):
                reconcile(root / "missing", root / "output", check=False)
            with self.assertRaises(ValueError):
                reconcile(root, root / "output", check=False)
            source, destination = root / "source", root / "destination"
            source.mkdir()
            destination.mkdir()
            (source / "root.zig").write_text("generated")
            (destination / "root.zig").symlink_to(source / "root.zig")
            with self.assertRaises(ValueError):
                reconcile(source, destination, check=False)


if __name__ == "__main__":
    unittest.main()
