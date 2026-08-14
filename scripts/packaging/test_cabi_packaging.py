#!/usr/bin/env python3
"""Packaging regression tests for Antfly C ABI artifacts."""

from __future__ import annotations

import importlib.util
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGING_DIR = Path(__file__).resolve().parent


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load module spec for {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


package_cli_release = load_module(
    "package_cli_release_for_cabi_test",
    PACKAGING_DIR / "package_cli_release.py",
)


def write_release_archive(root: Path, version: str, os_name: str, arch: str) -> Path:
    stage = root / f"{os_name}-{arch}"
    (stage / "include").mkdir(parents=True)
    (stage / "lib").mkdir()
    (stage / "share" / "antfly").mkdir(parents=True)
    (stage / "completions").mkdir()

    antfly = stage / "antfly"
    antfly.write_text("#!/bin/sh\n")
    antfly.chmod(antfly.stat().st_mode | stat.S_IXUSR)

    (stage / "include" / "antfly.h").write_text("/* antfly header */\n")
    lib_name = "libantfly.dylib" if os_name == "Darwin" else "libantfly.so"
    (stage / "lib" / lib_name).write_text("antfly library\n")
    (stage / "share" / "antfly" / "asset.txt").write_text("asset\n")
    for shell in ("bash", "zsh", "fish"):
        (stage / "completions" / f"antfly.{shell}").write_text(f"{shell} completion\n")

    archive = root / f"antfly_{version}_{os_name}_{arch}.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        for child in stage.iterdir():
            tar.add(child, arcname=child.name)
    return archive


class CAbiPackagingTests(unittest.TestCase):
    def test_generated_completions_include_nested_commands(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            output_dir = Path(raw) / "completions"
            subprocess.run(
                [str(REPO_ROOT / "scripts" / "completions.sh"), str(output_dir)],
                cwd=REPO_ROOT,
                check=True,
            )

            zsh = (output_dir / "antfly.zsh").read_text()
            self.assertIn('case "$words[2]" in', zsh)
            self.assertIn("subcommands=(run embed classify generate chat", zsh)
            self.assertIn("_describe 'subcommand' subcommands", zsh)

    def test_python_and_npm_packages_preserve_cabi_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            repo = root / "repo"
            extracted = root / "extracted"
            out = root / "out"
            archive_dir = root / "archives"
            archive_dir.mkdir()

            platform = package_cli_release.PLATFORMS[0]
            write_release_archive(archive_dir, "1.2.3", platform.release_os, platform.release_arch)

            (repo / "ts" / "packages" / "cli-darwin-arm64").mkdir(parents=True)
            (repo / "ts" / "packages" / "cli-darwin-arm64" / "package.json").write_text(
                '{"name":"@antfly/cli-darwin-arm64","version":"0.0.0","files":["bin","include","lib","share","README.md"]}\n'
            )
            (repo / "zig" / "pkg" / "antfly" / "antfarm").mkdir(parents=True)
            (repo / "zig" / "pkg" / "antfly" / "antfarm" / "index.html").write_text("antfarm\n")
            (repo / "py" / "packages" / "cli" / "src" / "antfly_cli").mkdir(parents=True)
            (repo / "py" / "packages" / "cli" / "src" / "antfly_cli" / "__init__.py").write_text(
                "def main(): return 0\n"
            )

            package_cli_release.extract_archive(archive_dir, "1.2.3", platform, extracted)
            package_cli_release.copy_antfarm(repo, extracted)
            package_cli_release.populate_npm_package(repo, platform, extracted)

            npm_package = repo / "ts" / "packages" / "cli-darwin-arm64"
            self.assertEqual((npm_package / "include" / "antfly.h").read_text(), "/* antfly header */\n")
            self.assertEqual((npm_package / "lib" / "libantfly.dylib").read_text(), "antfly library\n")

            wheel = package_cli_release.package_python_wheel(repo, out, "1.2.3", platform, extracted)
            with zipfile.ZipFile(wheel) as zf:
                names = set(zf.namelist())
            self.assertIn("antfly_cli/include/antfly.h", names)
            self.assertIn("antfly_cli/lib/libantfly.dylib", names)
            self.assertIn("antfly_cli/share/antfly/asset.txt", names)

    def test_homebrew_formula_installs_cabi_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            archive_dir = root / "archives"
            archive_dir.mkdir()
            for os_name, arch in (("Darwin", "arm64"), ("Linux", "arm64"), ("Linux", "x86_64")):
                write_release_archive(archive_dir, "1.2.3", os_name, arch)

            formula = root / "Formula" / "antfly.rb"
            subprocess.run(
                [
                    sys.executable,
                    str(PACKAGING_DIR / "render_homebrew_antfly_formula.py"),
                    "--version",
                    "1.2.3",
                    "--tag",
                    "v1.2.3",
                    "--archive-dir",
                    str(archive_dir),
                    "--out",
                    str(formula),
                ],
                cwd=REPO_ROOT,
                check=True,
            )

            rendered = formula.read_text()
            self.assertIn('include.install Dir["include/*"] if Dir.exist?("include")', rendered)
            self.assertIn('lib.install Dir["lib/*"] if Dir.exist?("lib")', rendered)
            self.assertIn('(share/"antfly").install Dir["share/antfly/*"] if Dir.exist?("share/antfly")', rendered)
            self.assertIn('bash_completion.install "completions/antfly.bash" => "antfly"', rendered)
            self.assertIn('zsh_completion.install "completions/antfly.zsh" => "_antfly"', rendered)
            self.assertIn('fish_completion.install "completions/antfly.fish"', rendered)


if __name__ == "__main__":
    unittest.main()
