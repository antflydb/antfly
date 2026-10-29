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

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(
    shutil.which("node"), "Node is required for the publishing workflow"
)
class AuthPublicationTests(unittest.TestCase):
    def run_publication(self, version, tag):
        workflow = (ROOT / ".github/workflows/ts-npm-publish.yml").read_text()
        step = workflow.split("      - name: Publish Better Auth integration\n", 1)[1]
        block = step.split("        run: |\n", 1)[1]
        script = "\n".join(
            line[10:] for line in block.splitlines() if line.startswith("          ")
        )
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            package = root / "packages/better-auth"
            package.mkdir(parents=True)
            (package / "package.json").write_text(json.dumps({"version": version}))
            binary = root / "bin"
            binary.mkdir()
            stub = binary / "pnpm"
            stub.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$PUBLISH_CAPTURE"\n')
            stub.chmod(0o755)
            capture = root / "args"
            result = subprocess.run(
                ["bash", "-e", "-c", script],
                cwd=root,
                env={
                    **os.environ,
                    "PATH": str(binary) + os.pathsep + os.environ["PATH"],
                    "GITHUB_REF_NAME": tag,
                    "PUBLISH_CAPTURE": str(capture),
                },
                capture_output=True,
                text=True,
            )
            return (
                result.returncode,
                capture.read_text().splitlines() if capture.exists() else [],
            )

    def test_release_candidate_uses_next(self):
        code, args = self.run_publication(
            "0.1.0-rc.1", "ts/antfly/better-auth/v0.1.0-rc.1"
        )
        self.assertEqual(code, 0)
        self.assertEqual(args[-2:], ["--tag", "next"])
        self.assertIn("--provenance", args)

    def test_stable_uses_latest(self):
        code, args = self.run_publication("0.1.0", "ts/antfly/better-auth/v0.1.0")
        self.assertEqual(code, 0)
        self.assertEqual(args[-2:], ["--tag", "latest"])

    def test_tag_mismatch_never_publishes(self):
        code, args = self.run_publication("0.1.0-rc.1", "ts/antfly/better-auth/v0.1.0")
        self.assertNotEqual(code, 0)
        self.assertEqual(args, [])

    def test_xz_is_installed_before_zig(self):
        workflow = (
            ROOT / ".github/workflows/better-auth-compatibility.yml"
        ).read_text()
        self.assertLess(
            workflow.index("xz-utils"), workflow.index("uses: mlugg/setup-zig")
        )


if __name__ == "__main__":
    unittest.main()
