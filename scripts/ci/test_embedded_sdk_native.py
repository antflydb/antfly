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

"""Prevent native SDK CI from silently passing without its installed library."""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ci/test-embedded-sdk-native.sh"


class NativeSDKAdmissionTests(unittest.TestCase):
    def test_missing_library_fails_before_invoking_sdk_tools(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                ["bash", str(SCRIPT), tmp, "python"],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("missing required libantfly installation file", result.stderr)

    def test_relocated_install_overrides_missing_library_and_requires_native_tests(
        self,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lib = root / "installation/lib"
            (lib / "pkgconfig").mkdir(parents=True)
            (root / "installation/include").mkdir()
            for filename in ("libantfly.so", "libantfly.dylib"):
                (lib / filename).write_bytes(b"fixture")
            (root / "installation/include/antfly.h").write_text("fixture")
            subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts/packaging/render_libantfly_pkgconfig.py"),
                    "--version",
                    "test",
                    "--out",
                    str(lib / "pkgconfig/libantfly.pc"),
                ],
                check=True,
            )
            tools = root / "tools"
            tools.mkdir()
            uv = tools / "uv"
            uv.write_text("""#!/bin/sh
set -eu
[ "$ANTFLY_LITE_REQUIRE_LIBRARY" = 1 ]
[ "$ANTFLY_LIB_DIR" = "$EXPECTED_NATIVE_LIB" ]
[ "$(dirname "$ANTFLY_LIBRARY")" = "$EXPECTED_NATIVE_LIB" ]
[ "$*" = 'run --locked pytest -q --ignore=tests/test_inference.py' ]
[ -s "$ANTFLY_LIBRARY" ]
printf 'native-test-invoked\n'
""")
            uv.chmod(0o755)
            env = {
                **os.environ,
                "PATH": str(tools) + os.pathsep + os.environ["PATH"],
                "ANTFLY_LIBRARY": "/nonexistent",
                "EXPECTED_NATIVE_LIB": str(lib),
            }
            result = subprocess.run(
                ["bash", str(SCRIPT), str(root / "installation"), "python"],
                check=False,
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("native-test-invoked", result.stdout)


if __name__ == "__main__":
    unittest.main()
