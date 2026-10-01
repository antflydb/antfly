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

"""Offline inventory and failure-propagation checks for prebuilt FK lanes."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class HostedFkRecoveryTest(unittest.TestCase):
    def run_fixture(
        self, suite="initial", exit_code=0, timeout=False, missing=False, skipped=False
    ):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            scripts = root / "scripts/ci"
            scripts.mkdir(parents=True)
            script = scripts / "zig-hosted-fk-recovery.sh"
            shutil.copyfile(ROOT / "scripts/ci/zig-hosted-fk-recovery.sh", script)
            binaries = root / "zig/zig-out/bin"
            binaries.mkdir(parents=True)
            if not missing:
                binary = binaries / f"antfly-hosted-{suite}-fk-recovery"
                summary = (
                    "1 passed; 1 skipped; 0 failed; 0 leaked."
                    if skipped
                    else "1 passed; 0 skipped; 0 failed; 0 leaked."
                )
                binary.write_text(f"#!/bin/sh\necho '{summary}'\nexit {exit_code}\n")
                binary.chmod(0o755)
            commands = root / "commands"
            commands.mkdir()
            # Assert the real script uses a finite timeout, forwards its
            # status through tee, and never requests a compiler fallback.
            watchdog = commands / "timeout"
            watchdog.write_text(
                '#!/bin/sh\n[ "$1" = --signal=TERM ] || exit 90\n'
                '[ "$2" = --kill-after=30s ] || exit 91\n'
                '[ "$3" = 15m ] || exit 92\n'
                + ("exit 124\n" if timeout else 'shift 3\nexec "$@"\n')
            )
            watchdog.chmod(0o755)
            compiler = commands / "zig"
            compiler.write_text("#!/bin/sh\necho forbidden-compiler\nexit 99\n")
            compiler.chmod(0o755)
            logs = root / "logs"
            result = subprocess.run(
                ["bash", str(script), suite],
                env={
                    **os.environ,
                    "PATH": f"{commands}:{os.environ['PATH']}",
                    "ANTFLY_FK_RECOVERY_LOG_DIR": str(logs),
                },
                text=True,
                capture_output=True,
                timeout=10,
            )
            self.assertNotIn("forbidden-compiler", result.stdout + result.stderr)
            log = logs / f"{suite}.log"
            return result.returncode, log.read_text() if log.exists() else ""

    def test_success_all_prebuilt_lanes(self):
        for suite in ("initial", "self", "truncate"):
            self.assertEqual(
                self.run_fixture(suite),
                (0, "1 passed; 0 skipped; 0 failed; 0 leaked.\n"),
            )

    def test_failed_proof_and_watchdog_fail_the_gate(self):
        self.assertEqual(self.run_fixture(exit_code=7)[0], 7)
        self.assertEqual(self.run_fixture(timeout=True)[0], 124)
        self.assertEqual(self.run_fixture(skipped=True)[0], 1)

    def test_missing_artifact_never_builds(self):
        self.assertEqual(self.run_fixture(missing=True)[0], 1)
        self.assertEqual(self.run_fixture(suite="unknown")[0], 2)

    def test_pr_and_full_inventory(self):
        workflow = (ROOT / ".github/workflows/zig-tests.yml").read_text()
        self.assertEqual(workflow.count('ANTFLY_CI_BUILD_FK_RECOVERY: "true"'), 2)
        self.assertEqual(workflow.count("name: Run prebuilt hosted FK recovery"), 2)
        self.assertEqual(workflow.count("name: Retain hosted FK recovery logs"), 2)
        for line in workflow.splitlines():
            if "run: tar -C zig -czf" in line and "antfly-e2e-" in line:
                for suite in ("initial", "self", "truncate"):
                    self.assertIn(
                        f"zig-out/bin/antfly-hosted-{suite}-fk-recovery", line
                    )
        build = (ROOT / "zig/pkg/antfly/build/api_tests.zig").read_text()
        consumers = next(
            line for line in build.splitlines() if ".linked_consumer_tests =" in line
        )
        for suite in ("initial", "self", "truncate"):
            self.assertIn(f"hosted_{suite}_fk_ci_tests", consumers)
        self.assertIn("- suite: fk-truncate", workflow)
        self.assertIn('"fk-truncate"', workflow)
        truncate_suite = build.split("const hosted_truncate_fk_ci_tests =", 1)[1].split(
            "hosted_fk_recovery_install.dependOn", 1
        )[0]
        self.assertIn("mounted hosted external-parent FK TRUNCATE", truncate_suite)
        self.assertIn("mounted hosted graph TRUNCATE", truncate_suite)
        self_suite = build.split("const hosted_self_fk_ci_tests =", 1)[1].split(
            "hosted_fk_recovery_install.dependOn", 1
        )[0]
        self.assertNotIn("remains guarded", self_suite)
        self.assertIn("lost owner and metadata replies", self_suite)
        self.assertIn("three-voter owner leadership transfer", self_suite)


if __name__ == "__main__":
    unittest.main()
