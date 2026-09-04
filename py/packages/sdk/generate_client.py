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

"""Generate the Python OpenAPI client or compare every managed output."""

from __future__ import annotations

import argparse
import difflib
import filecmp
import subprocess
import sys
import tempfile
from pathlib import Path

SDK_ROOT = Path(__file__).resolve().parent
REPO_ROOT = SDK_ROOT.parents[2]
OPENAPI_SPEC = REPO_ROOT / "openapi.yaml"
CONFIG = SDK_ROOT / ".openapi-generator-config.yaml"
TEMPLATES = SDK_ROOT / "templates"
CHECKED_IN_OUTPUT = SDK_ROOT / "src" / "antfly"
MANAGED_OUTPUTS = (Path(".gitignore"), Path("README.md"), Path("pyproject.toml"), Path("client_generated"))
IGNORED_NAMES = {"__pycache__", ".ruff_cache"}


def run(command: list[str]) -> None:
    subprocess.run(command, cwd=SDK_ROOT, check=True)


def generate(output: Path) -> None:
    run(
        [
            "openapi-python-client",
            "generate",
            "--path",
            str(OPENAPI_SPEC),
            "--output-path",
            str(output),
            "--overwrite",
            "--config",
            str(CONFIG),
            "--custom-template-path",
            str(TEMPLATES),
            "--fail-on-warning",
        ]
    )
    generated_client = output / "client_generated"
    run([sys.executable, str(SDK_ROOT / "fix_generated_client.py"), str(generated_client)])
    run(["ruff", "check", str(generated_client), "--fix"])
    run(["ruff", "format", str(generated_client)])


def files_below(path: Path) -> dict[Path, Path]:
    if path.is_file():
        return {Path(path.name): path}
    return {
        child.relative_to(path): child
        for child in path.rglob("*")
        if child.is_file() and not any(part in IGNORED_NAMES for part in child.parts)
    }


def compare_file(expected: Path, actual: Path, display_path: Path) -> bool:
    if not actual.exists():
        print(f"missing generated output: {display_path}")
        return False
    if filecmp.cmp(expected, actual, shallow=False):
        return True
    try:
        expected_lines = expected.read_text().splitlines(keepends=True)
        actual_lines = actual.read_text().splitlines(keepends=True)
    except UnicodeDecodeError:
        print(f"generated output differs: {display_path}")
        return False
    sys.stdout.writelines(
        difflib.unified_diff(
            actual_lines,
            expected_lines,
            fromfile=str(display_path),
            tofile=f"generated/{display_path}",
        )
    )
    return False


def compare(expected_root: Path) -> bool:
    clean = True
    for managed in MANAGED_OUTPUTS:
        expected = expected_root / managed
        actual = CHECKED_IN_OUTPUT / managed
        if expected.is_file():
            clean &= compare_file(expected, actual, Path("src/antfly") / managed)
            continue

        expected_files = files_below(expected)
        actual_files = files_below(actual) if actual.exists() else {}
        for relative_path in sorted(expected_files.keys() | actual_files.keys()):
            display_path = Path("src/antfly") / managed / relative_path
            if relative_path not in expected_files:
                print(f"obsolete generated output: {display_path}")
                clean = False
            elif relative_path not in actual_files:
                print(f"missing generated output: {display_path}")
                clean = False
            else:
                clean &= compare_file(expected_files[relative_path], actual_files[relative_path], display_path)
    return clean


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    if not args.check:
        generate(CHECKED_IN_OUTPUT)
        return 0

    with tempfile.TemporaryDirectory() as raw_temporary_dir:
        generated_root = Path(raw_temporary_dir) / "antfly"
        generate(generated_root)
        if not compare(generated_root):
            print("Python generated client is stale; run `make -C py/packages/sdk generate`")
            return 1
    print("Python generated client is current")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
