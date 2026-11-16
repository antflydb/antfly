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


def stage(
    binary, destination, *, source_root=ROOT, version=None, target=None, commit=None
):
    package = source_root / "zig/pkg/antfly-grep"
    destination.mkdir(parents=True, exist_ok=True)
    name = "antfly-grep.exe" if binary.suffix == ".exe" else "antfly-grep"
    shutil.copy2(binary, destination / name)
    (destination / name).chmod(0o755)
    for relative in ("completions", "share/man/man1", "share/antfly-grep/schema"):
        (destination / relative).mkdir(parents=True, exist_ok=True)
    for asset in (package / "generated").iterdir():
        asset_directory = "share/man/man1" if asset.suffix == ".1" else "completions"
        shutil.copy2(asset, destination / asset_directory / asset.name)
    for schema in (package / "schema").glob("*.json"):
        shutil.copy2(schema, destination / "share/antfly-grep/schema" / schema.name)
    for documentation in ("README.md", "COMPATIBILITY.md"):
        shutil.copy2(
            package / documentation, destination / "share/antfly-grep" / documentation
        )
    (destination / "share/antfly-grep/build.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "binary": "antfly-grep.exe"
                if binary.suffix == ".exe"
                else "antfly-grep",
                "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                **(
                    {"version": version, "target": target, "source_commit": commit}
                    if version is not None
                    else {}
                ),
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
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--version")
    parser.add_argument("--target")
    parser.add_argument("--commit")
    args = parser.parse_args()
    if (args.stage is None) == (args.output is None):
        parser.error("select exactly one of --stage or --output")
    identity = {"version": args.version, "target": args.target, "commit": args.commit}
    if any(identity.values()) and not all(identity.values()):
        parser.error(
            "release identity requires --version, --target and --commit together"
        )
    if args.stage:
        stage(args.binary, args.stage, source_root=args.source_root, **identity)
        return
    with tempfile.TemporaryDirectory(prefix="antfly-grep-release-") as directory:
        root = Path(directory)
        stage(args.binary, root, source_root=args.source_root, **identity)
        package = args.source_root / "zig/pkg/antfly-grep"
        shutil.copy2(package / "install.sh", root / "install.sh")
        (root / "install.sh").chmod(0o755)
        shutil.copy2(package / "README.md", root / "README.md")
        shutil.copy2(args.source_root / "LICENSES/Apache-2.0.txt", root / "LICENSE")
        shutil.copy2(
            args.source_root / "THIRD_PARTY_NOTICES.md", root / "THIRD_PARTY_NOTICES.md"
        )
        (root / "LICENSES").mkdir()
        shutil.copy2(
            args.source_root / "LICENSES/Apache-2.0.txt",
            root / "LICENSES/Apache-2.0.txt",
        )
        shutil.copytree(
            args.source_root / "LICENSES/third-party", root / "LICENSES/third-party"
        )
        create_archive(root, args.output, args.mtime)


if __name__ == "__main__":
    main()
