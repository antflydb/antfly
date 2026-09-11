# Copyright 2026 Antfly, Inc.
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

"""Build outputs and depfiles share the build runner's working directory."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class OpenApiBuildPathsTest(unittest.TestCase):
    def test_relative_build_outputs_and_comparison(self):
        scripts = Path(__file__).resolve().parent
        for script, mode in (
            ("join_openapi.py", ["--joined-only"]),
            ("join_public_openapi.py", []),
        ):
            with self.subTest(script=script), tempfile.TemporaryDirectory() as tmp:
                cwd = Path(tmp)
                (cwd / "cache").mkdir()
                output = "cache/openapi.yaml"
                depfile = "cache/openapi.d"

                def run(args):
                    result = subprocess.run(
                        [sys.executable, str(scripts / script), *args],
                        cwd=cwd,
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    self.assertEqual(
                        result.returncode, 0, result.stdout + result.stderr
                    )

                run([*mode, "--depfile", depfile, output])
                self.assertIn("openapi:", (cwd / output).read_text())
                self.assertIn("/specs/openapi/", (cwd / depfile).read_text())
                if not mode:
                    run(["--compare", "--depfile", depfile, output])


if __name__ == "__main__":
    unittest.main()
