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

"""Exercise the dependency preparation/archive cache handoff."""

import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = (ROOT / ".github/workflows/antfly-artifact-build.yml").read_text()
JOB = WORKFLOW.split("  build-zig-runtime-archives:\n", 1)[1].split(
    "\n  test-macos-homebrew:", 1
)[0]


class ReleaseDependencyTests(unittest.TestCase):
    def test_preparation_uses_controller_tools_and_mirror_only_reader(self):
        checkout = JOB.split(
            "      - name: Check out pinned dependency preparation tools\n", 1
        )[1].split("\n      - ", 1)[0]
        self.assertIn("ref: ${{ inputs.build_controller_commit }}", checkout)
        self.assertIn("path: release-controller", checkout)
        for source in (
            ".github/actions/prepare-zig-dependencies",
            "scripts/ci/prepare_zig_dependencies.py",
            "scripts/ci/zig-dependencies.json",
        ):
            self.assertIn(source, checkout)
        preparation = JOB.split(
            "      - name: Prepare verified release dependencies\n", 1
        )[1].split("\n      - ", 1)[0]
        self.assertIn(
            "uses: ./release-controller/.github/actions/prepare-zig-dependencies",
            preparation,
        )
        self.assertIn("mirror-only: 'true'", preparation)
        self.assertLess(
            JOB.index("Prepare verified release dependencies"),
            JOB.index("Build source-declared runtime archives"),
        )
        self.assertIn("      id-token: write", JOB)
        controller = (
            (ROOT / ".github/workflows/antfly-release-build-controller.yml")
            .read_text()
            .split("\n  build:\n", 1)[1]
        )
        self.assertIn("      id-token: write", controller)

    def test_prepared_packages_reach_both_product_builds(self):
        scratch = ROOT / ".zig-cache/release-dependency-test"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            root = Path(directory)
            env = {
                **os.environ,
                "GITHUB_WORKSPACE": str(root),
                "SOURCE_DATE_EPOCH": "1",
                "RUNNER_TEMP": str(root / "tmp"),
            }
            # Resolve the actual job environment, including any future cache changes.
            for key in ("ANTFLY_ZIG_CACHE_ROOT", "ZIG_GLOBAL_CACHE_DIR"):
                value = re.search(
                    r"^      " + key + r": (.+)$", JOB, re.MULTILINE
                ).group(1)
                env[key] = value.replace("${{ github.workspace }}", str(root))
            action_root = (
                root / "release-controller/.github/actions/prepare-zig-dependencies"
            )
            action_root.mkdir(parents=True)
            policy = root / "release-controller/scripts/ci"
            policy.mkdir(parents=True)
            shutil.copyfile(
                ROOT / "scripts/ci/prepare_zig_dependencies.py",
                policy / "prepare_zig_dependencies.py",
            )
            identity = "fixture-package"
            (policy / "zig-dependencies.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "mirror": "https://storage.googleapis.com/antfly-image-artifacts-zig-dependencies",
                        "packages": [
                            {
                                "hash": identity,
                                "url": "https://offline.invalid/source.tar.gz",
                            }
                        ],
                    }
                )
            )
            cached = Path(env["ZIG_GLOBAL_CACHE_DIR"]) / "p" / (identity + ".tar.gz")
            cached.parent.mkdir(parents=True)
            with tarfile.open(cached, "w:gz") as archive:
                data = b".{ .dependencies = .{} }"
                member = tarfile.TarInfo(identity + "/build.zig.zon")
                member.size = len(data)
                archive.addfile(member, io.BytesIO(data))
            (root / "zig/tools").mkdir(parents=True)
            (root / "zig/build.zig.zon").write_text(".{ .dependencies = .{} }")
            action = (
                ROOT / ".github/actions/prepare-zig-dependencies/action.yml"
            ).read_text()
            shell = textwrap.dedent(action.split("      run: |\n", 1)[1])
            subprocess.run(
                ["bash", "-euo", "pipefail", "-c", shell],
                cwd=root,
                env={**env, "ACTION_ROOT": str(action_root), "MIRROR_ONLY": "true"},
                check=True,
                capture_output=True,
                text=True,
            )
            packaging = root / "scripts/packaging/build_zig_release_archive.sh"
            packaging.parent.mkdir(parents=True)
            shutil.copyfile(
                ROOT / "scripts/packaging/build_zig_release_archive.sh", packaging
            )
            # Stop at the compiler boundary after checking the actual build environment.
            (root / "zig/tools/run_bounded_zig_build.py").write_text(
                "import os, pathlib, sys\n"
                "assert 'ANTFLY_ZIG_MIRROR_TOKEN' not in os.environ\n"
                "cache = pathlib.Path(os.environ['ZIG_GLOBAL_CACHE_DIR'])\n"
                "assert (cache / 'p/fixture-package.tar.gz').is_file(), str(cache)\n"
                "print('prepared package reached compiler', flush=True)\n"
                "sys.exit(42)\n"
            )
            env.pop("ANTFLY_ZIG_MIRROR_TOKEN", None)
            for product in ("server", "embedded"):
                with self.subTest(product=product):
                    result = subprocess.run(
                        [
                            "bash",
                            str(packaging),
                            "--product",
                            product,
                            "--version",
                            "test",
                            "--target",
                            "aarch64-linux-musl",
                            "--archive-name",
                            "test.tar.gz",
                            "--out-dir",
                            str(root / "out"),
                        ],
                        cwd=root,
                        env=env,
                        capture_output=True,
                        text=True,
                    )
                    self.assertEqual(
                        result.returncode, 42, result.stdout + result.stderr
                    )
                    self.assertIn("prepared package reached compiler", result.stdout)


if __name__ == "__main__":
    unittest.main()
