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

"""Check the real runtime constructor's schema dependencies with tiny bodies."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ZIG_ROOT = Path(__file__).resolve().parents[1]
UNRELATED = ("cli", "distributed", "inference", "serverless")
SCHEMAS = (
    "schemas/ard/api.yaml",
    "public.yaml",
    "schemas/antfly/metadata.yaml",
    "schemas/extensions/api.yaml",
    "schemas/auth/api.yaml",
    "schemas/inference/config.yaml",
)


class RuntimeSchemaCacheTest(unittest.TestCase):
    def test_schema_edits_rebuild_only_the_api_kernel(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            # Use the current owner code and source modules. Only schema inputs
            # live in this disposable tree; repository files are never edited.
            for name in ("pkg", "lib", "tools"):
                (root / name).symlink_to(ZIG_ROOT / name, target_is_directory=True)
            shutil.copyfile(
                ZIG_ROOT / "tools/fixtures/runtime_schema_cache.zig", root / "build.zig"
            )
            for name in SCHEMAS:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(f"# original {name}\n")

            def build(*targets, succeeds=True):
                result = subprocess.run(
                    [
                        "zig",
                        "build",
                        *targets,
                        "--summary",
                        "all",
                        "--color",
                        "off",
                        "--cache-dir",
                        str(root / "cache"),
                        "-j2",
                    ],
                    cwd=root,
                    text=True,
                    capture_output=True,
                    check=False,
                    timeout=180,
                )
                output = result.stdout + result.stderr
                if succeeds:
                    self.assertEqual(result.returncode, 0, output)
                else:
                    self.assertNotEqual(result.returncode, 0, output)
                return output

            def assert_cached(output, unit):
                name = (
                    "antfly-storage-kernel"
                    if unit == "distributed"
                    else f"antfly-runtime-{unit}"
                )
                self.assertIn(f"compile lib {name} Debug native cached", output)

            build()
            warm = build()
            for unit in (*UNRELATED, "api_kernel"):
                assert_cached(warm, unit)
            for name in SCHEMAS:
                with self.subTest(schema=name):
                    (root / name).write_text(f"# modified {name}\n")
                    output = build()
                    self.assertIn(
                        "compile lib antfly-runtime-api_kernel Debug native success",
                        output,
                    )
                    for unit in UNRELATED:
                        assert_cached(output, unit)

            # Unrelated units must also build when a schema is unavailable.
            (root / SCHEMAS[0]).unlink()
            output = build(*(f"runtime-unit-{unit}" for unit in UNRELATED))
            for unit in UNRELATED:
                assert_cached(output, unit)
            missing = build("runtime-unit-api_kernel", succeeds=False)
            self.assertIn("FileNotFound", missing)


if __name__ == "__main__":
    unittest.main()
