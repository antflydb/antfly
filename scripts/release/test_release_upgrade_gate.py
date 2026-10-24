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

"""Execute the release upgrade gate with tiny archives and a stand-in harness."""

import hashlib
import io
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = (ROOT / ".github/workflows/antfly-artifact-build.yml").read_text()
STEP = WORKFLOW.split(
    "      - name: Qualify in-place upgrade from released v0.2.5 data\n", 1
)[1].split("      - name:", 1)[0]
SCRIPT = textwrap.dedent(STEP.split("        run: |\n", 1)[1])
PIN = "4fc5a948be9e14cefc8393de22ce1ea6e35b927aa79cc35b7d80bef55b712ac8"


class ReleaseUpgradeGateTests(unittest.TestCase):
    def run_gate(self, *, corrupt=False, harness_exit=0):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "old.tar.gz"

            def pack(path, data):
                path.parent.mkdir(parents=True, exist_ok=True)
                with tarfile.open(path, "w:gz") as tar:
                    entry = tarfile.TarInfo("antfly")
                    entry.size = len(data)
                    tar.addfile(entry, io.BytesIO(data))

            pack(archive, b"released-v025")
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            if corrupt:
                archive.write_bytes(b"corrupt download")
            pack(
                root / "dist/release-archives/antfly_0.3.0-rc.3_Darwin_arm64.tar.gz",
                b"candidate-rc3",
            )
            commands = root / "commands"
            commands.mkdir()
            curl = commands / "curl"
            curl.write_text('#!/bin/bash\nset -eu\ncp "$FIXTURE_ARCHIVE" "${@: -1}"\n')
            curl.chmod(0o755)
            harness = root / "scripts/ci/qualify-v02-server-upgrade.py"
            harness.parent.mkdir(parents=True)
            harness.write_text(
                textwrap.dedent("""\
                import argparse
                from pathlib import Path
                p = argparse.ArgumentParser()
                p.add_argument('--old-bin', required=True)
                p.add_argument('--new-bin', required=True, type=Path)
                p.add_argument('--work-dir', required=True, type=Path)
                args = p.parse_args()
                release, old = args.old_bin.split('=', 1)
                assert release == 'v0.2.5'
                assert Path(old).read_bytes() == b'released-v025'
                assert args.new_bin.read_bytes() == b'candidate-rc3'
                args.work_dir.mkdir(parents=True)
                (args.work_dir / 'called').write_text('verified')
                """)
                + f"raise SystemExit({harness_exit})\n"
            )
            result = subprocess.run(
                ["bash", "-c", SCRIPT.replace(PIN, digest)],
                cwd=root,
                env={
                    **os.environ,
                    "PATH": f"{commands}:{os.environ['PATH']}",
                    "RUNNER_TEMP": str(root / "runner temp"),
                    "RELEASE_TAG": "v0.3.0-rc.3",
                    "FIXTURE_ARCHIVE": str(archive),
                },
                capture_output=True,
                text=True,
            )
            called = (root / "runner temp/v02-upgrade/evidence/called").exists()
            return result, called

    def test_built_archive_reaches_official_harness(self):
        result, called = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(called)

    def test_corrupt_download_stops_before_harness(self):
        result, called = self.run_gate(corrupt=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(called)

    def test_harness_failure_fails_release_gate(self):
        result, called = self.run_gate(harness_exit=17)
        self.assertEqual(result.returncode, 17, result.stderr)
        self.assertTrue(called)

    def test_upgrade_is_required_before_recording_release(self):
        self.assertIn("inputs.release_line == '0.3'", STEP)
        self.assertIn("inputs.release_line == 'nightly'", STEP)
        self.assertNotIn("continue-on-error", STEP)
        record = WORKFLOW.split("  release-request:\n", 1)[1]
        self.assertIn("      - test-macos-homebrew\n", record)


if __name__ == "__main__":
    unittest.main()
