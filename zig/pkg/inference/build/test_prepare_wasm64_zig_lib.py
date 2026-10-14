#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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

import errno
import json
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

from pkg.inference.build.prepare_wasm64_zig_lib import (
    OLD,
    NEW,
    clone_file,
    patch_compare_source,
    prepare,
)


class Wasm64ZigLibTest(unittest.TestCase):
    def test_read_only_toolchain_preserves_contents_and_permissions(self):
        for cross_volume in (False, True):
            with (
                self.subTest(cross_volume=cross_volume),
                tempfile.TemporaryDirectory() as root,
            ):
                source = Path(root) / "installed"
                runtime = source / "compiler_rt"
                runtime.mkdir(parents=True)
                comparison = runtime / "comparef.zig"
                comparison.write_text(OLD)
                neighbor = source / "std.zig"
                neighbor.write_text("unchanged")
                comparison.chmod(0o444)
                neighbor.chmod(0o444)
                runtime.chmod(0o555)
                source.chmod(0o555)
                output = Path(root) / "cache/overlay"
                try:
                    with patch(
                        "subprocess.check_output",
                        side_effect=[
                            "0.17.0\n",
                            f".lib_dir = {json.dumps(str(source))},\n",
                        ],
                    ):
                        if cross_volume:
                            with patch(
                                "os.link",
                                side_effect=OSError(errno.EXDEV, "cross volume"),
                            ):
                                prepare("zig", output, comparison)
                        else:
                            prepare("zig", output, comparison)
                    self.assertEqual(comparison.read_text(), OLD)
                    self.assertEqual(neighbor.read_text(), "unchanged")
                    self.assertEqual(
                        (output / "compiler_rt/comparef.zig").read_text(), NEW
                    )
                    self.assertEqual((output / "std.zig").read_text(), "unchanged")
                    for path, mode in (
                        (source, 0o555),
                        (runtime, 0o555),
                        (comparison, 0o444),
                        (neighbor, 0o444),
                    ):
                        self.assertEqual(stat.S_IMODE(path.stat().st_mode), mode)
                    self.assertNotEqual(
                        comparison.stat().st_ino,
                        (output / "compiler_rt/comparef.zig").stat().st_ino,
                    )
                    self.assertEqual(list(output.parent.glob("wasm64-lib-*")), [])
                finally:
                    source.chmod(0o755)
                    runtime.chmod(0o755)

    def test_unknown_runtime_is_rejected(self):
        for source in ("different compiler runtime", OLD + OLD):
            with self.assertRaisesRegex(RuntimeError, "unrecognized"):
                patch_compare_source(source)

    def test_overlay_preserves_installed_library(self):
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "installed"
            (source / "compiler_rt").mkdir(parents=True)
            comparison = source / "compiler_rt/comparef.zig"
            original = "prefix\n" + OLD + "\nsuffix\n"
            comparison.write_text(original)
            (source / "std.zig").write_text("other contents")
            output = Path(root) / "cache/overlay"
            with patch(
                "subprocess.check_output",
                side_effect=["0.17.0\n", f".lib_dir = {json.dumps(str(source))},\n"],
            ):
                prepare("zig", output, comparison)
            self.assertEqual(comparison.read_text(), original)
            self.assertEqual(
                (output / "compiler_rt/comparef.zig").read_text(),
                original.replace(OLD, NEW),
            )
            self.assertEqual((output / "std.zig").read_text(), "other contents")
            self.assertNotEqual(
                comparison.stat().st_ino,
                (output / "compiler_rt/comparef.zig").stat().st_ino,
            )
            self.assertEqual(list(output.parent.glob("wasm64-lib-*")), [])

    def test_copy_failure_leaves_no_partial_overlay(self):
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "installed"
            (source / "compiler_rt").mkdir(parents=True)
            comparison = source / "compiler_rt/comparef.zig"
            comparison.write_text(OLD)
            output = Path(root) / "cache/overlay"
            with (
                patch(
                    "subprocess.check_output",
                    side_effect=[
                        "0.17.0\n",
                        f".lib_dir = {json.dumps(str(source))},\n",
                    ],
                ),
                patch("shutil.copytree", side_effect=OSError("copy failed")),
            ):
                with self.assertRaisesRegex(OSError, "copy failed"):
                    prepare("zig", output, comparison)
            self.assertFalse(output.exists())
            self.assertEqual(comparison.read_text(), OLD)
            self.assertEqual(list(output.parent.iterdir()), [])

    def test_other_toolchain_version_is_rejected(self):
        with patch("subprocess.check_output", return_value="0.18.0\n"):
            with self.assertRaisesRegex(RuntimeError, "only validated"):
                prepare("zig", Path("unused"), Path("unused"))

    def test_installed_and_occupied_destinations_are_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "installed"
            (source / "compiler_rt").mkdir(parents=True)
            comparison = source / "compiler_rt/comparef.zig"
            comparison.write_text(OLD)
            occupied = Path(root) / "occupied"
            occupied.mkdir()
            neighbor = occupied / "keep"
            neighbor.write_text("published")
            for destination in (source, source / "overlay", occupied):
                with patch(
                    "subprocess.check_output",
                    side_effect=[
                        "0.17.0\n",
                        f".lib_dir = {json.dumps(str(source))},\n",
                    ],
                ):
                    with self.assertRaises(RuntimeError):
                        prepare("zig", destination, comparison)
            self.assertEqual(comparison.read_text(), OLD)
            self.assertEqual(neighbor.read_text(), "published")

    def test_cross_volume_copy_fallback(self):
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "source"
            source.write_text("unchanged")
            output = Path(root) / "output"
            with patch("os.link", side_effect=OSError(errno.EXDEV, "cross volume")):
                clone_file(str(source), str(output))
            self.assertEqual(output.read_text(), "unchanged")
            self.assertEqual(source.read_text(), "unchanged")


if __name__ == "__main__":
    unittest.main()
