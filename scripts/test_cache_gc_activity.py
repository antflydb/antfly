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

"""Keep cache GC away from active native builds, including old cache entries."""

import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parent


class NativeActivityTests(unittest.TestCase):
    def run_detector(self, commands, status=0, whole_script=False):
        with tempfile.TemporaryDirectory(prefix="antfly-cache-gc-test-") as directory:
            fake_ps = Path(directory) / "ps"
            fake_ps.write_text(
                "#!/bin/sh\nprintf '%s\\n' "
                + shlex.quote(commands)
                + "\nexit "
                + str(status)
                + "\n"
            )
            fake_ps.chmod(0o755)
            # Even a regression in the guard must never run destructive GC.
            env = dict(
                os.environ,
                PATH=directory + os.pathsep + os.environ["PATH"],
                DRY_RUN="1",
            )
            if whole_script:
                command = ["bash", str(SCRIPTS / "cache_gc.sh")]
            else:
                command = [
                    "bash",
                    "-c",
                    'source "$1"; native_builds_active',
                    "bash",
                    str(SCRIPTS / "cache_gc_activity.sh"),
                ]
            return subprocess.run(
                command,
                env=env,
                cwd=directory,
                text=True,
                capture_output=True,
                check=False,
            )

    def test_native_compilers_and_cached_build_driver_protect_caches(self):
        for name in ("zig", "maker", "cargo", "rustc", "clippy-driver", "rust-lld"):
            with self.subTest(name=name):
                self.assertEqual(
                    self.run_detector(" /some/cache/o/old/" + name).returncode, 0
                )

    def test_idle_and_unrelated_processes_do_not_block_gc(self):
        for commands in ("", "/bin/zsh\n/usr/bin/python3", "/tmp/maker-not-a-compiler"):
            with self.subTest(commands=commands):
                self.assertEqual(self.run_detector(commands).returncode, 1)

    def test_unavailable_process_inspection_preserves_caches(self):
        self.assertEqual(self.run_detector("", status=1).returncode, 0)

    def test_active_gc_exits_before_repository_or_deletion_work(self):
        result = self.run_detector("/build/.zig-cache/o/old/maker", whole_script=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("skipping cache cleanup", result.stdout)


if __name__ == "__main__":
    unittest.main()
