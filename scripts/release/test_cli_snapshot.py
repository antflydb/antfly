#!/usr/bin/env python3
"""Tests for immutable CLI package snapshot construction and verification."""

from __future__ import annotations

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
                "macosx_11_0_arm64",
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
