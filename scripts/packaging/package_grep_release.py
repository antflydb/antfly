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

"""Stage a portable grep distribution; cross-target binaries are never executed."""

import argparse
import hashlib
import json
import shutil
import tempfile
from pathlib import Path

from create_reproducible_tar import create_archive

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "zig/pkg/antfly-grep"


def stage(binary, destination):
    destination.mkdir(parents=True, exist_ok=True)
    name = "antfly-grep.exe" if binary.suffix == ".exe" else "antfly-grep"
    shutil.copy2(binary, destination / name)
    (destination / name).chmod(0o755)
    for relative in ("completions", "share/man/man1", "share/antfly-grep/schema"):
        (destination / relative).mkdir(parents=True, exist_ok=True)
    for asset in (PACKAGE / "generated").iterdir():
        target = "share/man/man1" if asset.suffix == ".1" else "completions"
        shutil.copy2(asset, destination / target / asset.name)
    for schema in (PACKAGE / "schema").glob("*.json"):
        shutil.copy2(schema, destination / "share/antfly-grep/schema" / schema.name)
    for documentation in ("README.md", "COMPATIBILITY.md"):
        shutil.copy2(
            PACKAGE / documentation, destination / "share/antfly-grep" / documentation
        )
    (destination / "share/antfly-grep/build.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "binary": "antfly-grep.exe"
                if binary.suffix == ".exe"
                else "antfly-grep",
                "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            },
            indent=2,
        )
        + "\n"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--stage", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--mtime", type=int, default=0)
    args = parser.parse_args()
    if (args.stage is None) == (args.output is None):
        parser.error("select exactly one of --stage or --output")
    if args.stage:
        stage(args.binary, args.stage)
        return
    with tempfile.TemporaryDirectory(prefix="antfly-grep-release-") as directory:
        root = Path(directory)
        stage(args.binary, root)
        shutil.copy2(PACKAGE / "README.md", root / "README.md")
        shutil.copy2(ROOT / "LICENSES/Apache-2.0.txt", root / "LICENSE")
        shutil.copy2(ROOT / "THIRD_PARTY_NOTICES.md", root / "THIRD_PARTY_NOTICES.md")
        (root / "LICENSES").mkdir()
        shutil.copy2(ROOT / "LICENSES/Apache-2.0.txt", root / "LICENSES/Apache-2.0.txt")
        shutil.copytree(ROOT / "LICENSES/third-party", root / "LICENSES/third-party")
        create_archive(root, args.output, args.mtime)


if __name__ == "__main__":
    main()
