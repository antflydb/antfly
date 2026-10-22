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

"""Tests for immutable CLI package snapshot construction and verification."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import unittest
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BUILD = REPO_ROOT / "scripts" / "release" / "build_cli_snapshot.py"
VERIFY = REPO_ROOT / "scripts" / "release" / "verify_cli_snapshot.py"
VERSION = "1.2.3"
COMMIT = "0123456789abcdef0123456789abcdef01234567"
NPM_PACKAGES = {
    "antfly-cli": "@antfly/cli",
    "antfly-cli-darwin-arm64": "@antfly/cli-darwin-arm64",
    "antfly-cli-linux-arm64": "@antfly/cli-linux-arm64",
    "antfly-cli-linux-x64": "@antfly/cli-linux-x64",
}


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load module spec for {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def write_npm_package(path: Path, package_name: str) -> None:
    payload = json.dumps({"name": package_name, "version": VERSION}).encode()
    info = tarfile.TarInfo("package/package.json")
    info.size = len(payload)
    with tarfile.open(path, "w:gz") as archive:
        archive.addfile(info, io.BytesIO(payload))


def write_wheel(path: Path) -> None:
    with zipfile.ZipFile(path, "w") as wheel:
        wheel.writestr(
            "antfly_cli-1.2.3.dist-info/METADATA", "Name: antfly-cli\nVersion: 1.2.3\n"
        )


class CLISnapshotTests(unittest.TestCase):
    def test_promotion_materializes_and_verifies_declared_cli_artifacts(self) -> None:
        workflow = (REPO_ROOT / ".github/workflows/antfly-release.yml").read_text()
        step = workflow.split(
            "      - name: Materialize and verify typed bundle scopes\n", 1
        )[1].split("\n      - ", 1)[0]
        command = textwrap.dedent(step.split("        run: |\n", 1)[1])
        for legacy, damage in (
            (False, None),
            (True, None),
            (False, "missing_npm"),
            (True, "missing_wheel"),
            (False, "tampered"),
            (False, "unsafe_name"),
        ):
            with (
                self.subTest(legacy=legacy, damage=damage),
                tempfile.TemporaryDirectory() as raw,
            ):
                root = Path(raw)
                payload = root / "dist/release-payload"
                payload.mkdir(parents=True)
                (root / "scripts").symlink_to(
                    REPO_ROOT / "scripts", target_is_directory=True
                )
                bin_dir = root / "bin"
                bin_dir.mkdir()
                (bin_dir / "python").symlink_to(sys.executable)
                artifacts = []
                for index, package in enumerate(NPM_PACKAGES.values()):
                    name = f"cli-{index}.tgz"
                    (payload / name).write_bytes(package.encode())
                    artifacts.append({"name": name, "kind": "npm", "package": package})
                if legacy:
                    for index in range(3):
                        name = f"cli-{index}.whl"
                        (payload / name).write_bytes(f"wheel-{index}".encode())
                        artifacts.append(
                            {
                                "name": name,
                                "kind": "python",
                                "package": "antfly-cli",
                                "package_version": VERSION,
                            }
                        )
                for artifact in artifacts:
                    data = (payload / artifact["name"]).read_bytes()
                    artifact.update(
                        size=len(data), sha256=hashlib.sha256(data).hexdigest()
                    )
                manifest = {
                    "schema_version": 1 if legacy else 2,
                    "version": VERSION,
                    "commit": COMMIT,
                    "registry_versions": {
                        "npm": VERSION,
                        **({"python": VERSION} if legacy else {}),
                    },
                    "artifacts": artifacts,
                }
                if damage == "unsafe_name":
                    artifacts[0]["name"] = "../outside.tgz"
                    (payload.parent / "outside.tgz").write_bytes(b"outside")
                (payload / "cli-snapshot.json").write_text(json.dumps(manifest))
                runtime_name = f"antfly_{VERSION}_Linux_x86_64.tar.gz"
                (payload / runtime_name).write_bytes(b"runtime")
                entries = [
                    {
                        "name": p.name,
                        "scope": "runtime" if p.name == runtime_name else "cli",
                        "size": p.stat().st_size,
                        "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
                    }
                    for p in payload.iterdir()
                ]
                ledger = payload / "artifacts.json"
                ledger.write_text(
                    json.dumps(
                        {
                            "schema_version": 2,
                            "tag": f"v{VERSION}",
                            "commit": COMMIT,
                            "artifacts": entries,
                        }
                    )
                )
                if damage in {"missing_npm", "missing_wheel"}:
                    (payload / ("cli-0.whl" if legacy else "cli-0.tgz")).unlink()
                elif damage == "tampered":
                    path = payload / "cli-0.tgz"
                    path.write_bytes(b"x" * path.stat().st_size)
                result = subprocess.run(
                    ["bash", "-e", "-c", command],
                    cwd=root,
                    env={
                        **os.environ,
                        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                        "PYTHONDONTWRITEBYTECODE": "1",
                        "RELEASE_TAG": f"v{VERSION}",
                        "RELEASE_VERSION": VERSION,
                        "RELEASE_COMMIT": COMMIT,
                        "LEDGER_SHA256": hashlib.sha256(
                            ledger.read_bytes()
                        ).hexdigest(),
                    },
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if damage:
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    expected = {
                        "missing_npm": "cli-0.tgz",
                        "missing_wheel": "cli-0.whl",
                        "tampered": "promoted artifact differs from release ledger",
                        "unsafe_name": "invalid CLI snapshot artifact name",
                    }[damage]
                    self.assertIn(expected, result.stderr)
                else:
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn("verified CLI snapshot", result.stdout)
                    self.assertEqual(
                        {
                            p.name
                            for p in (root / "dist/verified-cli-snapshot").iterdir()
                        },
                        {"cli-snapshot.json", *(a["name"] for a in artifacts)},
                    )

    def test_npm_only_snapshot_is_strict_and_pypi_is_paused(self) -> None:
        policy = load_module(
            "paused_cli_channels", REPO_ROOT / "scripts/release/release_channels.py"
        ).load_policy()
        self.assertTrue(
            all(not channel["publish_pypi"] for channel in policy["channels"].values())
        )
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            npm = root / "npm"
            npm.mkdir()
            for filename, package in NPM_PACKAGES.items():
                write_npm_package(npm / f"{filename}-{VERSION}.tgz", package)
            snapshot = root / "snapshot"
            subprocess.run(
                [
                    sys.executable,
                    str(BUILD),
                    "--version",
                    VERSION,
                    "--commit",
                    COMMIT,
                    "--npm-dir",
                    str(npm),
                    "--out-dir",
                    str(snapshot),
                ],
                check=True,
            )
            manifest = json.loads((snapshot / "cli-snapshot.json").read_text())
            self.assertEqual(manifest["schema_version"], 2)
            self.assertEqual(manifest["registry_versions"], {"npm": VERSION})
            self.assertEqual(len(manifest["artifacts"]), 4)
            command = [
                sys.executable,
                str(VERIFY),
                "--snapshot-dir",
                str(snapshot),
                "--version",
                VERSION,
                "--commit",
                COMMIT,
            ]
            subprocess.run(command, check=True)
            write_wheel(snapshot / "unexpected.whl")
            result = subprocess.run(
                command, text=True, capture_output=True, check=False
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("file set mismatch", result.stderr)

    def test_python_registry_version_matches_packager(self) -> None:
        snapshot = load_module("cli_snapshot_builder", BUILD)
        packager = load_module(
            "cli_release_packager",
            REPO_ROOT / "scripts" / "packaging" / "package_cli_release.py",
        )
        for version in ("1.2.3", "1.2.3-alpha.4", "1.2.3-beta.2", "1.2.3-rc.2"):
            self.assertEqual(
                snapshot.python_version_from_release(version),
                packager.python_version_from_release(version),
            )
        for unsupported in (
            "1.2",
            "1.2.3+CUDA.1",
            "1.2.3-experimental.1",
            "1.2.3-dev4",
            "1.2.3-rc2",
        ):
            with self.subTest(unsupported=unsupported), self.assertRaises(SystemExit):
                snapshot.python_version_from_release(unsupported)

    def test_darwin_deployment_target_matches_wheel_floor(self) -> None:
        policy_module = load_module(
            "apple_release_platforms",
            REPO_ROOT / "scripts/release/release_platforms.py",
        )
        policy = policy_module.load_policy()
        darwin = next(
            row for row in policy["platforms"] if row["archive_os"] == "Darwin"
        )
        with tempfile.TemporaryDirectory() as raw_tmp:
            path = Path(raw_tmp) / "platforms.json"
            darwin["wheel_platform"] = "macosx_11_0_arm64"
            path.write_text(json.dumps(policy))
            with self.assertRaisesRegex(SystemExit, "wheel tag must match"):
                policy_module.load_policy(path)
            darwin["zig_target"] = "aarch64-macos"
            path.write_text(json.dumps(policy))
            with self.assertRaisesRegex(SystemExit, "explicit macOS deployment target"):
                policy_module.load_policy(path)

    def test_snapshot_rejects_mutated_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            npm_dir = root / "npm"
            python_dir = root / "python"
            snapshot_dir = root / "snapshot"
            npm_dir.mkdir()
            python_dir.mkdir()
            for filename, package_name in NPM_PACKAGES.items():
                write_npm_package(npm_dir / f"{filename}-{VERSION}.tgz", package_name)
            for platform in (
                "macosx_15_0_arm64",
                "manylinux_2_28_aarch64",
                "manylinux_2_28_x86_64",
            ):
                write_wheel(
                    python_dir / f"antfly_cli-{VERSION}-py3-none-{platform}.whl"
                )

            subprocess.run(
                [
                    sys.executable,
                    str(BUILD),
                    "--version",
                    VERSION,
                    "--commit",
                    COMMIT,
                    "--npm-dir",
                    str(npm_dir),
                    "--python-dir",
                    str(python_dir),
                    "--out-dir",
                    str(snapshot_dir),
                ],
                check=True,
            )
            verify = [
                sys.executable,
                str(VERIFY),
                "--snapshot-dir",
                str(snapshot_dir),
                "--version",
                VERSION,
                "--commit",
                COMMIT,
            ]
            subprocess.run(verify, check=True)

            artifact = snapshot_dir / f"antfly-cli-{VERSION}.tgz"
            artifact.write_bytes(artifact.read_bytes() + b"tampered")
            result = subprocess.run(verify, capture_output=True, text=True, check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("size mismatch", result.stderr)


if __name__ == "__main__":
    unittest.main()
