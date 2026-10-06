# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

import tempfile
import unittest
from pathlib import Path

from make_zig_lib_overlay import create_overlay


class OverlaySafetyTest(unittest.TestCase):
    def test_overlapping_paths_preserve_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "lib"
            source.mkdir()
            marker = source / "marker"
            marker.write_text("original")
            alias = root / "alias"
            alias.symlink_to(source, target_is_directory=True)
            for output in (source, source / "overlay", root, alias):
                with self.subTest(output=output):
                    with self.assertRaisesRegex(ValueError, "must not overlap"):
                        create_overlay(source, output)
                    self.assertEqual(marker.read_text(), "original")

    def test_incompatible_library_preserves_existing_overlay(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "lib"
            (source / "std").mkdir(parents=True)
            (source / "std" / "c.zig").write_text("unrecognized Zig version")
            output = root / "overlay"
            output.mkdir()
            marker = output / "marker"
            marker.write_text("previous working overlay")
            with self.assertRaisesRegex(SystemExit, "anchor not found"):
                create_overlay(source, output)
            self.assertEqual(marker.read_text(), "previous working overlay")
            self.assertFalse(list(root.glob(".antfly-zig-overlay-*")))


if __name__ == "__main__":
    unittest.main()
