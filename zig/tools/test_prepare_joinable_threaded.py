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

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from tools import prepare_joinable_threaded as patcher


class OverlayTests(unittest.TestCase):
    def test_unknown_source_leaves_installation_and_output_untouched(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "lib/std/Io/Threaded.zig"
            source.parent.mkdir(parents=True)
            source.write_text("unrecognized toolchain source\n")
            output = root / "output"
            with self.assertRaises(RuntimeError):
                patcher.prepare(
                    root / "lib", source, root / "lib/std/Thread.zig", output
                )
            self.assertFalse(output.exists())
            self.assertEqual(source.read_text(), "unrecognized toolchain source\n")

    def test_unknown_thread_source_fails_before_copying_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "lib/std/Io/Threaded.zig"
            source.parent.mkdir(parents=True)
            source.write_text("original threaded")
            thread = root / "lib/std/Thread.zig"
            thread.write_text("unknown thread source")
            output = root / "output"
            with (
                mock.patch.object(patcher, "patch_threaded", return_value="patched"),
                self.assertRaises(RuntimeError),
            ):
                patcher.prepare(root / "lib", source, thread, output)
            self.assertFalse(output.exists())
            self.assertEqual(thread.read_text(), "unknown thread source")
            self.assertEqual(source.read_text(), "original threaded")

    def test_no_symlink_privileges_copy_resources_without_mutating_toolchain(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "lib/std/Io/Threaded.zig"
            source.parent.mkdir(parents=True)
            source.write_text("original")
            (root / "lib/std/Thread.zig").write_text("original thread")
            resource = root / "lib/libc/fixture.h"
            resource.parent.mkdir()
            resource.write_text("immutable resource")
            output = root / "output"
            with (
                mock.patch.object(patcher, "patch_threaded", return_value="patched"),
                mock.patch.object(
                    patcher, "patch_thread", return_value="patched thread"
                ),
                mock.patch.object(Path, "symlink_to", side_effect=PermissionError),
            ):
                patcher.prepare(
                    root / "lib", source, root / "lib/std/Thread.zig", output
                )
            self.assertEqual(source.read_text(), "original")
            self.assertEqual((output / "std/Io/Threaded.zig").read_text(), "patched")
            self.assertEqual(
                (root / "lib/std/Thread.zig").read_text(), "original thread"
            )
            self.assertEqual((output / "std/Thread.zig").read_text(), "patched thread")
            (output / "libc/fixture.h").write_text("changed copy")
            self.assertEqual(resource.read_text(), "immutable resource")
            metadata = json.loads(
                (output / "antfly-joinable-threaded.json").read_text()
            )
            self.assertEqual(
                metadata["original_sha256"], hashlib.sha256(b"original").hexdigest()
            )
            self.assertEqual(
                metadata["patched_sha256"], hashlib.sha256(b"patched").hexdigest()
            )
            self.assertEqual(
                metadata["original_thread_sha256"],
                hashlib.sha256(b"original thread").hexdigest(),
            )
            self.assertEqual(
                metadata["patched_thread_sha256"],
                hashlib.sha256(b"patched thread").hexdigest(),
            )


if __name__ == "__main__":
    unittest.main()
