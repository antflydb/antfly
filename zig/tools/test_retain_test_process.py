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
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "retain_test_process", Path(__file__).with_name("retain_test_process.py")
)
retention = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(retention)


class RetainTestProcess(unittest.TestCase):
    def test_preserves_named_and_anonymous_tests_with_exact_arguments(self):
        for name in (
            "test",
            "test-local",
            "metadata-unit-lane-a-tests",
            "storage-db-core-tests-local",
        ):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                executable = root / name
                executable.write_bytes(b"retained executable")
                process = root / "proc/17"
                process.mkdir(parents=True)
                (process / "exe").symlink_to(executable)
                (process / "cwd").symlink_to(root)
                argv = [str(executable), "--test-filter", "test with spaces"]
                (process / "cmdline").write_bytes("\0".join(argv).encode() + b"\0")
                self.assertTrue(retention.retain(17, root / "out", root / "proc"))
                output = root / "out/test-17"
                self.assertEqual(output.read_bytes(), executable.read_bytes())
                self.assertEqual(output.stat().st_mode & 0o777, 0o755)
                record = json.loads(output.with_suffix(".json").read_text())
                self.assertEqual(record["argv"], argv)
                self.assertEqual(record["cwd"], str(root))
                self.assertEqual(
                    record["sha256"], hashlib.sha256(output.read_bytes()).hexdigest()
                )

    def test_does_not_copy_compilers_or_build_runners(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            process = root / "proc/17"
            process.mkdir(parents=True)
            (process / "exe").symlink_to(root / "zig")
            (process / "cmdline").write_bytes(b"zig\0build\0unit-test\0")
            self.assertFalse(retention.retain(17, root / "out", root / "proc"))
            self.assertFalse((root / "out").exists())

    def test_retains_compiler_inputs_and_memory_without_copying_toolchain(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            process = root / "proc/17"
            process.mkdir(parents=True)
            (process / "exe").symlink_to(root / "zig")
            (process / "cwd").symlink_to(root)
            args = root / "cache/args/hash"
            args.parent.mkdir(parents=True)
            args.write_text('-Mroot=pkg/owner.zig --test-filter "a test"')
            (process / "cmdline").write_bytes(b"zig\0test\0@cache/args/hash\0")
            (process / "status").write_text("VmHWM:\t1000 kB\nVmRSS:\t900 kB\n")
            self.assertTrue(retention.retain(17, root / "out", root / "proc"))
            output = root / "out/compiler-17"
            self.assertEqual((output / "args-0.txt").read_text(), args.read_text())
            self.assertEqual(
                (output / "status").read_text(), (process / "status").read_text()
            )
            record = json.loads((output / "command.json").read_text())
            self.assertEqual(record["argv"], ["zig", "test", "@cache/args/hash"])
            self.assertEqual(record["cwd"], str(root))
            self.assertEqual(
                sorted(path.name for path in output.iterdir()),
                ["args-0.txt", "command.json", "status"],
            )


if __name__ == "__main__":
    unittest.main()
