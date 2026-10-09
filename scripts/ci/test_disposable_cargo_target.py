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

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

HELPER = Path(__file__).with_name("disposable_cargo_target.sh")


class DisposableCargoTargetTests(unittest.TestCase):
    def command(self, child):
        return [
            "bash",
            "-c",
            'source "$1"; shift; with_disposable_cargo_target "$@"',
            "cargo-phase-test",
            str(HELPER),
            sys.executable,
            "-c",
            child,
        ]

    def run_phase(self, status):
        with tempfile.TemporaryDirectory(prefix="cargo phase ") as directory:
            root = Path(directory)
            shared = root / "shared-target"
            shared.mkdir()
            sentinel = shared / "published"
            sentinel.write_text("keep")
            env = dict(os.environ, RUNNER_TEMP=str(root), CARGO_TARGET_DIR=str(shared))
            child = (
                """import json, os
from pathlib import Path
p=Path(os.environ['CARGO_TARGET_DIR']);(p/'compiled').write_text('scratch')
print(json.dumps(str(p)), flush=True)
raise SystemExit(%d)
"""
                % status
            )
            result = subprocess.run(
                self.command(child), env=env, capture_output=True, text=True
            )
            self.assertEqual(status, result.returncode, result.stderr)
            target = Path(json.loads(result.stdout))
            self.assertEqual(root, target.parent)
            self.assertNotEqual(shared, target)
            self.assertFalse(target.exists())
            self.assertEqual("keep", sentinel.read_text())

    def test_success_retires_only_owned_compiler_outputs(self):
        self.run_phase(0)

    def test_failure_preserves_exit_status_and_retires_outputs(self):
        self.run_phase(42)

    @unittest.skipUnless(os.name == "posix", "Bash CI process groups require POSIX")
    def test_cancellation_joins_child_before_retiring_outputs(self):
        with tempfile.TemporaryDirectory(prefix="cargo cancel ") as directory:
            root = Path(directory)
            marker = root / "active"
            child = """import os, signal, sys, time
from pathlib import Path
p=Path(os.environ['CARGO_TARGET_DIR']);(p/'compiled').write_text('scratch')
marker=Path(os.environ['TEST_MARKER'])
def stop(signum, frame):
 time.sleep(0.2)
 (p/'shutdown').write_text('joined')
 marker.with_name('completed').write_text('joined')
 sys.exit(0)
signal.signal(signal.SIGTERM, stop)
marker.write_text(str(p))
time.sleep(60)
"""
            env = dict(os.environ, RUNNER_TEMP=str(root), TEST_MARKER=str(marker))
            process = subprocess.Popen(
                self.command(child),
                env=env,
                start_new_session=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                deadline = time.monotonic() + 10
                while (
                    not marker.exists()
                    and process.poll() is None
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.02)
                self.assertTrue(marker.exists())
                target = Path(marker.read_text())
                os.killpg(process.pid, signal.SIGTERM)
                process.communicate(timeout=10)
                self.assertEqual("joined", (root / "completed").read_text())
                self.assertNotEqual(0, process.returncode)
                self.assertFalse(target.exists())
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.communicate()


if __name__ == "__main__":
    unittest.main()
