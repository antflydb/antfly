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

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / ".github/actions/setup-native-build-tools/install.sh"
)
TOOLS = ("cc", "c++", "make", "nm", "pkg-config", "xz")


class NativeBuildToolsTests(unittest.TestCase):
    def run_setup(self, missing=(), fail_update=False):
        with tempfile.TemporaryDirectory(prefix="native tools ") as directory:
            root = Path(directory)
            for name in TOOLS:
                if name not in missing:
                    tool = root / name
                    tool.write_text("#!/bin/bash\nexit 0\n")
                    tool.chmod(0o755)
            sudo = root / "sudo"
            sudo.write_text(
                '#!/bin/bash\nprintf \'%s\\n\' "$*" >> "$NATIVE_TOOLS_LOG"\nif [[ "${FAIL_UPDATE:-}" == 1 && "${!#}" == update ]]; then exit 7; fi\n'
            )
            sudo.chmod(0o755)
            sleep = root / "sleep"
            sleep.write_text("#!/bin/bash\nexit 0\n")
            sleep.chmod(0o755)
            log = root / "calls"
            result = subprocess.run(
                ["/bin/bash", str(SCRIPT)],
                capture_output=True,
                text=True,
                env={
                    **os.environ,
                    "PATH": str(root),
                    "NATIVE_TOOLS_LOG": str(log),
                    "FAIL_UPDATE": "1" if fail_update else "0",
                },
            )
            return result, log.read_text().splitlines() if log.exists() else []

    def test_prepared_runner_needs_no_package_access(self):
        result, calls = self.run_setup()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, [])

    def test_each_missing_consumer_or_extraction_tool_installs_complete_set(self):
        for tool in TOOLS:
            with self.subTest(tool=tool):
                result, calls = self.run_setup(missing=(tool,))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    calls,
                    [
                        "apt-get -o Acquire::Retries=3 -o Acquire::http::No-Cache=true -o Acquire::https::No-Cache=true -o APT::Update::Error-Mode=any update",
                        "apt-get -o Acquire::Retries=3 install -y --no-install-recommends build-essential binutils pkg-config xz-utils",
                    ],
                )

    def test_package_failure_stops_before_install(self):
        result, calls = self.run_setup(missing=("nm",), fail_update=True)
        self.assertEqual(result.returncode, 7)
        self.assertEqual(len(calls), 3)
        self.assertTrue(all(call.endswith(" update") for call in calls))
        self.assertTrue(all("APT::Update::Error-Mode=any" in call for call in calls))


if __name__ == "__main__":
    unittest.main()
