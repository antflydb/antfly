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
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("install_ubuntu_packages.sh")


class InstallUbuntuPackagesTests(unittest.TestCase):
    def run_install(self, failures, install_status=0):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = {
                "sudo": '#!/bin/sh\nexec "$@"\n',
                "sleep": "#!/bin/sh\nexit 0\n",
                "apt-get": """#!/usr/bin/env python3
import json, os, pathlib, sys
root = pathlib.Path(os.environ["APT_TEST_ROOT"])
with (root / "calls").open("a") as output:
    output.write(json.dumps(sys.argv[1:]) + "\\n")
if sys.argv[-1] == "update":
    count = root / "count"
    current = int(count.read_text()) + 1 if count.exists() else 1
    count.write_text(str(current))
    if current <= int(os.environ["APT_TEST_FAILURES"]):
        sys.exit(100)
else:
    sys.exit(int(os.environ["APT_TEST_INSTALL_STATUS"]))
""",
            }
            for name, body in commands.items():
                path = root / name
                path.write_text(body)
                path.chmod(0o755)
            result = subprocess.run(
                ["bash", str(SCRIPT), "pkg-config", "build-essential"],
                env={
                    **os.environ,
                    "PATH": f"{root}:{os.environ['PATH']}",
                    "APT_TEST_ROOT": directory,
                    "APT_TEST_FAILURES": str(failures),
                    "APT_TEST_INSTALL_STATUS": str(install_status),
                },
                check=False,
            )
            calls = [
                json.loads(line) for line in (root / "calls").read_text().splitlines()
            ]
            return result.returncode, calls

    def test_success_installs_once_after_verified_refresh(self):
        status, calls = self.run_install(0)
        self.assertEqual(status, 0)
        self.assertEqual(len(calls), 2)
        self.assertIn("APT::Update::Error-Mode=any", calls[0])
        self.assertEqual(
            calls[1][-4:], ["install", "-y", "pkg-config", "build-essential"]
        )

    def test_transient_index_failure_retries_before_installing(self):
        status, calls = self.run_install(1)
        self.assertEqual(status, 0)
        self.assertEqual([call[-1] for call in calls[:2]], ["update", "update"])
        self.assertEqual(len(calls), 3)

    def test_install_failure_is_reported_without_retrying_refresh(self):
        status, calls = self.run_install(0, install_status=42)
        self.assertEqual(status, 42)
        self.assertEqual(len(calls), 2)

    def test_persistent_index_failure_never_installs_from_stale_lists(self):
        status, calls = self.run_install(5)
        self.assertEqual(status, 100)
        self.assertEqual(len(calls), 3)
        self.assertTrue(all(call[-1] == "update" for call in calls))


if __name__ == "__main__":
    unittest.main()
