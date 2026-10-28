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

"""Cross-target release assembly without executing the packaged binary."""

import hashlib
import json
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class GrepPackagingTests(unittest.TestCase):
    def test_archive_assets_hashes_and_reproducibility(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "antfly-grep.exe"
            binary.write_bytes(b"cross-target fixture, must not execute")
            archives = [root / "a.tar.gz", root / "b.tar.gz"]
            for archive in archives:
                subprocess.run(
                    [
                        "python3",
                        str(ROOT / "scripts/packaging/package_grep_release.py"),
                        "--binary",
                        str(binary),
                        "--output",
                        str(archive),
                        "--mtime",
                        "42",
                    ],
                    check=True,
                )
            self.assertEqual(archives[0].read_bytes(), archives[1].read_bytes())
            with tarfile.open(archives[0]) as archive:
                for name in [
                    "antfly-grep.exe",
                    "install.sh",
                    "LICENSE",
                    "THIRD_PARTY_NOTICES.md",
                    "LICENSES/Apache-2.0.txt",
                    "completions/antfly-grep.bash",
                    "completions/antfly-grep.zsh",
                    "completions/antfly-grep.fish",
                    "completions/antfly-grep.ps1",
                    "share/man/man1/antfly-grep.1",
                    "share/antfly-grep/schema/document-v1.json",
                    "share/antfly-grep/schema/document-v2.json",
                    "share/antfly-grep/schema/workspace-request-v1.json",
                    "share/antfly-grep/COMPATIBILITY.md",
                ]:
                    self.assertTrue(archive.getmember("./" + name).isfile(), name)
                manifest = json.load(
                    archive.extractfile("./share/antfly-grep/build.json")
                )
                self.assertEqual(
                    manifest["sha256"], hashlib.sha256(binary.read_bytes()).hexdigest()
                )
                self.assertEqual(manifest["binary"], "antfly-grep.exe")

    def test_release_identity_survives_cli_packaging(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "antfly-grep"
            binary.write_bytes(b"cross target")
            archive = root / "release.tar.gz"
            command = [
                "python3",
                str(ROOT / "scripts/packaging/package_grep_release.py"),
                "--source-root",
                str(ROOT),
                "--binary",
                str(binary),
                "--output",
                str(archive),
                "--version",
                "0.1.0-rc.1",
                "--target",
                "aarch64-macos.15.0",
                "--commit",
                "a" * 40,
            ]
            subprocess.run(command, check=True)
            with tarfile.open(archive) as tar:
                metadata = json.load(tar.extractfile("./share/antfly-grep/build.json"))
                self.assertEqual(metadata["version"], "0.1.0-rc.1")
                self.assertEqual(metadata["target"], "aarch64-macos.15.0")
                self.assertEqual(metadata["source_commit"], "a" * 40)
                self.assertEqual(tar.getmember("./install.sh").mode, 0o755)
            incomplete = subprocess.run(command[:-2], capture_output=True, text=True)
            self.assertNotEqual(incomplete.returncode, 0)
            self.assertIn("release identity requires", incomplete.stderr)


if __name__ == "__main__":
    unittest.main()
