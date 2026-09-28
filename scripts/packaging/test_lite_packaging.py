#!/usr/bin/env python3
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

"""Exercise the complete Apache Lite archive-to-wheel/npm path with fixtures."""

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "package_lite_release_tested", ROOT / "scripts/packaging/package_lite_release.py"
)
assert spec and spec.loader
sys.path.insert(0, str(ROOT / "scripts/packaging"))
package = importlib.util.module_from_spec(spec)
spec.loader.exec_module(package)
verify_spec = importlib.util.spec_from_file_location(
    "verify_lite_release_tested", ROOT / "scripts/packaging/verify_lite_release.py"
)
assert verify_spec and verify_spec.loader
verifier = importlib.util.module_from_spec(verify_spec)
verify_spec.loader.exec_module(verifier)
snapshot_spec = importlib.util.spec_from_file_location(
    "lite_snapshot_tested", ROOT / "scripts/release/lite_snapshot.py"
)
assert snapshot_spec and snapshot_spec.loader
snapshot = importlib.util.module_from_spec(snapshot_spec)
snapshot_spec.loader.exec_module(snapshot)


class LitePackagingTests(unittest.TestCase):
    def test_platform_packages_bundle_apache_library_and_worker(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            archives = root / "archives"
            archives.mkdir()
            source = ROOT / "py/packages/lite"
            shutil.copytree(source / "src", root / "py/packages/lite/src")
            (root / "py/packages/lite/pyproject.toml").write_bytes(
                (source / "pyproject.toml").read_bytes()
            )
            (root / "LICENSES").mkdir()
            apache = (ROOT / "LICENSES/Apache-2.0.txt").read_bytes()
            (root / "LICENSES/Apache-2.0.txt").write_bytes(apache)
            shutil.copytree(
                ROOT / "ts/packages/lite",
                root / "ts/packages/lite",
                ignore=shutil.ignore_patterns("node_modules", "dist"),
            )
            for platform in package.PACKAGE_PLATFORMS:
                suffix = platform.npm_package_dir.replace("cli-", "lite-")
                shutil.copytree(
                    ROOT / "ts/packages" / suffix, root / "ts/packages" / suffix
                )
                stage = root / ("stage-" + platform.key)
                (stage / "lib").mkdir(parents=True)
                (stage / "include").mkdir()
                (stage / "LICENSES/third-party").mkdir(parents=True)
                (stage / "antfly-lite").write_text("lite executable")
                (stage / "antfly-inference").write_text("worker executable")
                (stage / "lib" / package.lite_library_name(platform)).write_text(
                    "apache native library"
                )
                (stage / "include/antfly.h").write_text("/* ABI */")
                (stage / "LICENSE").write_bytes(apache)
                (stage / "LICENSES/Apache-2.0.txt").write_bytes(apache)
                (stage / "LICENSES/third-party/example.txt").write_text(
                    "Upstream license"
                )
                (stage / "THIRD_PARTY_NOTICES.md").write_text("Third party notices")
                with tarfile.open(
                    archives / package.archive_name("1.2.3", platform), "w:gz"
                ) as tar:
                    for item in stage.iterdir():
                        tar.add(item, arcname=item.name)

            with (
                mock.patch.object(package, "ROOT", root),
                mock.patch.object(
                    sys,
                    "argv",
                    [
                        "package_lite_release.py",
                        "--version",
                        "1.2.3",
                        "--archive-dir",
                        str(archives),
                        "--out-dir",
                        str(root / "out"),
                    ],
                ),
            ):
                self.assertEqual(0, package.main())

            for platform in package.PACKAGE_PLATFORMS:
                suffix = platform.npm_package_dir.replace("cli-", "lite-")
                npm = root / "ts/packages" / suffix
                self.assertEqual(
                    "1.2.3", json.loads((npm / "package.json").read_text())["version"]
                )
                self.assertTrue(
                    (npm / "lib" / package.lite_library_name(platform)).is_file()
                )
                self.assertTrue((npm / "lib/antfly-inference").is_file())
                self.assertTrue((npm / "LICENSES/third-party/example.txt").is_file())
                wheel = (
                    root
                    / "out/python"
                    / f"antfly_lite-1.2.3-py3-none-{platform.wheel_platform}.whl"
                )
                with zipfile.ZipFile(wheel) as archive:
                    names = set(archive.namelist())
                    self.assertIn("antfly_lite/_lib/antfly-inference", names)
                    self.assertIn(
                        "antfly_lite/_lib/" + package.lite_library_name(platform), names
                    )
                    self.assertIn("antfly_lite/_bin/antfly-lite", names)
                    self.assertIn(
                        "antfly_lite-1.2.3.dist-info/LICENSES/third-party/example.txt",
                        names,
                    )
                    metadata = archive.read("antfly_lite-1.2.3.dist-info/METADATA")
                    self.assertIn(b"License-Expression: Apache-2.0", metadata)

            if sys.version_info >= (3, 11):
                install_dir = root / "installed-wheel"
                wheel = (
                    root
                    / "out/python"
                    / "antfly_lite-1.2.3-py3-none-manylinux_2_28_x86_64.whl"
                )
                subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "pip",
                        "install",
                        "--disable-pip-version-check",
                        "--no-index",
                        "--no-deps",
                        "--platform",
                        "manylinux_2_28_x86_64",
                        "--target",
                        str(install_dir),
                        str(wheel),
                    ],
                    check=True,
                    capture_output=True,
                )
                for location in (
                    "_bin/antfly-lite",
                    "_bin/antfly-inference",
                    "_lib/antfly-inference",
                ):
                    installed = install_dir / "antfly_lite" / location
                    self.assertTrue(
                        os.access(installed, os.X_OK),
                        f"{installed}: {oct(installed.stat().st_mode & 0o777)}",
                    )

            npm_dir = root / "out/npm"
            npm_dir.mkdir()
            for name in (
                "lite-darwin-arm64",
                "lite-linux-arm64",
                "lite-linux-x64",
                "lite",
            ):
                subprocess.run(
                    [
                        "npm",
                        "pack",
                        "--ignore-scripts",
                        "--pack-destination",
                        str(npm_dir),
                        str(root / "ts/packages" / name),
                    ],
                    check=True,
                    capture_output=True,
                )
            with mock.patch.object(verifier, "ROOT", root):
                verifier.verify("1.2.3", archives, root / "out/python", npm_dir)
            sealed = root / "snapshot"
            commit = "a" * 40
            snapshot.build("1.2.3", commit, npm_dir, root / "out/python", sealed)
            snapshot.verify(sealed, "1.2.3", commit)
            manifest_path = sealed / "lite-snapshot.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["artifacts"].append(manifest["artifacts"][0])
            manifest_path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "artifact set differs"):
                snapshot.verify(sealed, "1.2.3", commit)
            manifest["artifacts"].pop()
            manifest_path.write_text(json.dumps(manifest))
            wheel = next(sealed.glob("*.whl"))
            wheel.write_bytes(wheel.read_bytes() + b"tampered")
            with self.assertRaisesRegex(ValueError, "digest differs"):
                snapshot.verify(sealed, "1.2.3", commit)

    def test_server_executable_is_rejected(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            platform = package.PACKAGE_PLATFORMS[0]
            stage = root / "stage"
            (stage / "lib").mkdir(parents=True)
            (stage / "include").mkdir()
            (stage / "LICENSES").mkdir()
            for name in (
                "antfly",
                "antfly-lite",
                "antfly-inference",
                "include/antfly.h",
                "THIRD_PARTY_NOTICES.md",
            ):
                (stage / name).write_text("fixture")
            (stage / "lib" / package.lite_library_name(platform)).write_text("fixture")
            (stage / "LICENSE").write_bytes(
                (ROOT / "LICENSES/Apache-2.0.txt").read_bytes()
            )
            (stage / "LICENSES/Apache-2.0.txt").write_bytes(
                (ROOT / "LICENSES/Apache-2.0.txt").read_bytes()
            )
            archive_path = root / package.archive_name("1.2.3", platform)
            with tarfile.open(archive_path, "w:gz") as archive:
                for item in stage.iterdir():
                    archive.add(item, arcname=item.name)
            with self.assertRaisesRegex(ValueError, "server artifacts"):
                package.extract_lite_archive(root, "1.2.3", platform, root / "extract")


if __name__ == "__main__":
    unittest.main()
