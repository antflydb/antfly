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

"""Tests for read-only npm promotion preflight."""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import io
import json
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).with_name("prepare_npm_promotion.py")


def load_module():
    spec = importlib.util.spec_from_file_location("prepare_npm_promotion", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class NpmPromotionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.tarball = Path(self.tmp.name) / "package.tgz"
        self.tarball.write_bytes(b"immutable npm package")
        digest = hashlib.sha512(self.tarball.read_bytes()).digest()
        self.integrity = "sha512-" + base64.b64encode(digest).decode("ascii")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write_snapshot(self, module, version: str) -> Path:
        root = Path(self.tmp.name)
        artifacts = []
        for package, stem in module.NPM_PACKAGES.items():
            name = f"{stem}-{version}.tgz"
            (root / name).write_bytes(package.encode())
            artifacts.append({"kind": "npm", "package": package, "name": name})
        manifest = {
            "schema_version": 1,
            "version": version,
            "registry_versions": {"npm": version, "python": "unused"},
            "artifacts": artifacts,
        }
        (root / "cli-snapshot.json").write_text(json.dumps(manifest))
        return root

    def test_built_snapshots_pass_npm_promotion(self) -> None:
        module = load_module()
        scripts = SCRIPT.parent
        commit = "0123456789abcdef0123456789abcdef01234567"
        for version, tag in (("1.2.3", "latest"), ("1.2.3-rc.1", "next")):
            for legacy in (False, True):
                with self.subTest(version=version, legacy=legacy):
                    root = Path(self.tmp.name) / f"{version}-{legacy}"
                    npm = root / "npm"
                    npm.mkdir(parents=True)
                    for package, stem in module.NPM_PACKAGES.items():
                        payload = json.dumps(
                            {"name": package, "version": version}
                        ).encode()
                        info = tarfile.TarInfo("package/package.json")
                        info.size = len(payload)
                        with tarfile.open(
                            npm / f"{stem}-{version}.tgz", "w:gz"
                        ) as archive:
                            archive.addfile(info, io.BytesIO(payload))
                    snapshot = root / "snapshot"
                    build = [
                        sys.executable,
                        str(scripts / "build_cli_snapshot.py"),
                        "--version",
                        version,
                        "--commit",
                        commit,
                        "--npm-dir",
                        str(npm),
                        "--out-dir",
                        str(snapshot),
                    ]
                    if legacy:
                        from release_channels import python_version_from_release

                        wheels = root / "wheels"
                        wheels.mkdir()
                        python_version = python_version_from_release(version)
                        for platform in (
                            "macosx_15_0_arm64",
                            "manylinux_2_28_aarch64",
                            "manylinux_2_28_x86_64",
                        ):
                            wheel = (
                                wheels
                                / f"antfly_cli-{python_version}-py3-none-{platform}.whl"
                            )
                            with zipfile.ZipFile(wheel, "w") as archive:
                                archive.writestr(
                                    f"antfly_cli-{python_version}.dist-info/METADATA",
                                    f"Name: antfly-cli\nVersion: {python_version}\n",
                                )
                        build.extend(["--python-dir", str(wheels)])
                    subprocess.run(build, check=True, capture_output=True, text=True)
                    subprocess.run(
                        [
                            sys.executable,
                            str(scripts / "verify_cli_snapshot.py"),
                            "--snapshot-dir",
                            str(snapshot),
                            "--version",
                            version,
                            "--commit",
                            commit,
                        ],
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    preflight_package = module.preflight_package

                    def preflight(package, release_version, release_tag, tarball):
                        preflight_package(
                            package,
                            release_version,
                            release_tag,
                            tarball,
                            integrity_reader=lambda _package, _version: (
                                module.npm_integrity(tarball)
                            ),
                            tag_reader=lambda _package, _tag: version,
                        )

                    with (
                        mock.patch.object(
                            sys,
                            "argv",
                            [
                                str(SCRIPT),
                                "--snapshot-dir",
                                str(snapshot),
                                "--version",
                                version,
                                "--tag",
                                tag,
                            ],
                        ),
                        mock.patch.object(
                            module, "preflight_package", side_effect=preflight
                        ) as checked,
                    ):
                        self.assertEqual(module.main(), 0)
                    self.assertEqual(checked.call_count, len(module.NPM_PACKAGES))
                    self.assertEqual(
                        {call.args[0] for call in checked.call_args_list},
                        set(module.NPM_PACKAGES),
                    )
                    self.assertTrue(
                        all(
                            call.args[1:3] == (version, tag)
                            for call in checked.call_args_list
                        )
                    )

    def test_snapshot_rejects_unknown_schema(self) -> None:
        module = load_module()
        root = self.write_snapshot(module, "1.2.3")
        manifest = root / "cli-snapshot.json"
        document = json.loads(manifest.read_text())
        document["schema_version"] = 3
        manifest.write_text(json.dumps(document))
        with self.assertRaisesRegex(SystemExit, "invalid npm release identity"):
            module.snapshot_packages(root, "1.2.3")

    def test_missing_version_passes_without_reading_mutable_tag(self) -> None:
        module = load_module()

        def unexpected_tag(_package: str, _tag: str) -> str | None:
            raise AssertionError("a missing immutable version has no tag constraint")

        module.preflight_package(
            "@antfly/cli",
            "1.2.3",
            "latest",
            self.tarball,
            lambda _package, _version: None,
            unexpected_tag,
        )

    def test_identical_version_and_tag_are_resumable(self) -> None:
        module = load_module()
        module.preflight_package(
            "@antfly/cli",
            "1.2.3",
            "latest",
            self.tarball,
            lambda _package, _version: self.integrity,
            lambda _package, _tag: "1.2.3",
        )

    def test_existing_version_content_drift_fails(self) -> None:
        module = load_module()
        with self.assertRaisesRegex(SystemExit, "different contents"):
            module.preflight_package(
                "@antfly/cli",
                "1.2.3",
                "latest",
                self.tarball,
                lambda _package, _version: "sha512-different",
                lambda _package, _tag: "1.2.3",
            )

    def test_existing_version_tag_drift_fails(self) -> None:
        module = load_module()
        with self.assertRaisesRegex(SystemExit, "dist-tag latest points to 1.2.2"):
            module.preflight_package(
                "@antfly/cli",
                "1.2.3",
                "latest",
                self.tarball,
                lambda _package, _version: self.integrity,
                lambda _package, _tag: "1.2.2",
            )

    def test_snapshot_requires_the_exact_npm_package_set(self) -> None:
        module = load_module()
        root = self.write_snapshot(module, "1.2.3")
        self.assertEqual(
            set(module.snapshot_packages(root, "1.2.3")), set(module.NPM_PACKAGES)
        )

        manifest = json.loads((root / "cli-snapshot.json").read_text())
        manifest["artifacts"].pop()
        (root / "cli-snapshot.json").write_text(json.dumps(manifest))
        with self.assertRaisesRegex(SystemExit, "package set differs"):
            module.snapshot_packages(root, "1.2.3")

    def test_snapshot_accepts_an_authenticated_legacy_registry_version(self) -> None:
        module = load_module()
        root = self.write_snapshot(module, "1.2.3-rc2")
        self.assertEqual(
            set(module.snapshot_packages(root, "1.2.3-rc2")),
            set(module.NPM_PACKAGES),
        )
        with (
            mock.patch.object(
                sys,
                "argv",
                [
                    str(SCRIPT),
                    "--snapshot-dir",
                    str(root),
                    "--version",
                    "1.2.3-rc2",
                    "--tag",
                    "next",
                ],
            ),
            mock.patch.object(module, "preflight_package") as preflight,
        ):
            self.assertEqual(module.main(), 0)
        self.assertEqual(preflight.call_count, len(module.NPM_PACKAGES))


if __name__ == "__main__":
    unittest.main()
