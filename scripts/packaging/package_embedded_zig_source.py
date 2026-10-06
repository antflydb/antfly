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

"""Build a self-contained Apache Zig source package from an immutable commit."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

from create_reproducible_tar import create_archive

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from license_headers import ELV2_ROOTS, is_under  # noqa: E402

PACKAGE = "zig/pkg/antfly-embedded"
MANIFEST = "embedded-zig-source.json"


def selected(path: str) -> bool:
    # Include the Apache composition's authored inputs and tooling, not language
    # bindings, apps, server implementations, or the server's test source tree.
    if is_under(path, ELV2_ROOTS):
        return False
    if path.startswith("LICENSES/Elastic-"):
        return False
    return (
        path.startswith(("zig/", "scripts/", "specs/", "LICENSES/"))
        or path == "THIRD_PARTY_NOTICES.md"
    )


def stage(
    repository: Path, commit: str, destination: Path, working_tree: bool = False
) -> None:
    if working_tree:
        listing = subprocess.check_output(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=repository,
        )
        for raw in listing.split(b"\0"):
            if not raw:
                continue
            name = raw.decode()
            source = repository / name
            if not selected(name) or not source.is_file():
                continue
            if source.is_symlink():
                raise ValueError(f"source package cannot contain a symlink: {name}")
            target = destination / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
    else:
        with tempfile.TemporaryFile() as archive:
            subprocess.run(
                ["git", "archive", "--format=tar", commit],
                cwd=repository,
                stdout=archive,
                check=True,
            )
            archive.seek(0)
            with tarfile.open(fileobj=archive) as source:
                for member in source:
                    if not selected(member.name) or member.isdir():
                        continue
                    if not member.isfile():
                        raise ValueError(
                            f"source package cannot contain a link: {member.name}"
                        )
                    target = destination / member.name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    stream = source.extractfile(member)
                    assert stream is not None
                    with target.open("wb") as output:
                        shutil.copyfileobj(stream, output)
                    target.chmod(member.mode)
    # Fail closed even for newly introduced ELv2 owners not yet in the map.
    for path in destination.rglob("*"):
        if path.is_file() and path.suffix in {".zig", ".c", ".h", ".py", ".sh"}:
            for line in path.read_bytes()[:8192].splitlines():
                stripped = line.strip()
                if stripped and not stripped.startswith((b"#", b"//", b"/*", b"*")):
                    break
                if b"SPDX-License-Identifier: Elastic-2.0" in stripped:
                    raise ValueError(f"ELv2 source selected for Apache archive: {path}")


def configure(stage_root: Path, version: str, commit: str, working_tree: bool) -> None:
    owner = stage_root / PACKAGE
    (stage_root / "build.zig").write_bytes((owner / "build.zig").read_bytes())
    manifest = (owner / "build.zig.zon").read_text()
    manifest = manifest.replace('.path = "../.."', '.path = "zig"')
    manifest = re.sub(
        r'\.version = "[^"]+"', f'.version = "{version}"', manifest, count=1
    )
    manifest = re.sub(
        r"\.paths = \.\{.*?\},",
        '.paths = .{ "build.zig", "build.zig.zon", "README.md", "LICENSE", "LICENSES", "THIRD_PARTY_NOTICES.md", "SOURCE-MANIFEST.json", "zig", "scripts", "specs" },',
        manifest,
        flags=re.S,
    )
    (stage_root / "build.zig.zon").write_text(manifest)
    (owner / "build.zig").unlink()
    (owner / "build.zig.zon").unlink()
    # The composition root is Apache-only. No authored server build graph is
    # shipped or invoked; imports keep their normal owner-relative layout.
    header = (stage_root / "zig/embedded.build.zig").read_text().split("const std")[0]
    (stage_root / "zig/build.zig").write_text(
        header
        + 'const std = @import("std");\npub fn build(b: *std.Build) void {\n    _ = b.option(bool, "embedded-only", "Apache composition");\n    @import("embedded.build.zig").buildDependency(b, @This());\n}\n'
    )
    (stage_root / "LICENSE").write_bytes(
        (stage_root / "LICENSES/Apache-2.0.txt").read_bytes()
    )
    (stage_root / "README.md").write_bytes((owner / "README.md").read_bytes())
    files = [
        {
            "path": str(path.relative_to(stage_root)),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
        for path in sorted(stage_root.rglob("*"))
        if path.is_file()
    ]
    (stage_root / "SOURCE-MANIFEST.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "version": version,
                "commit": commit,
                "working_tree": working_tree,
                "files": files,
            },
            indent=2,
        )
        + "\n"
    )


def build(
    repository: Path,
    commit: str,
    version: str,
    out_dir: Path,
    zig: str,
    working_tree: bool = False,
) -> dict:
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("source commit must be a full SHA-1")
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.+-]+)?", version):
        raise ValueError("source package version must be SemVer without a v prefix")
    if out_dir.exists() and any(out_dir.iterdir()):
        raise ValueError(f"source output directory must be empty: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)
    name = f"antfly-embedded-source_{version}.tar.gz"
    mtime = int(
        subprocess.check_output(
            ["git", "show", "-s", "--format=%ct", commit], cwd=repository
        )
    )
    with tempfile.TemporaryDirectory(prefix="antfly-zig-source-") as raw:
        destination = Path(raw)
        stage(repository, commit, destination, working_tree)
        configure(destination, version, commit, working_tree)
        create_archive(destination, out_dir / name, mtime)
    package_hash = subprocess.check_output(
        [zig, "fetch", str((out_dir / name).resolve())], text=True
    ).strip()
    (out_dir / (name + ".zig-hash")).write_text(package_hash + "\n")
    document = {
        "schema_version": 1,
        "version": version,
        "commit": commit,
        "working_tree": working_tree,
        "archive": name,
        "sha256": hashlib.sha256((out_dir / name).read_bytes()).hexdigest(),
        "zig_hash": package_hash,
    }
    (out_dir / MANIFEST).write_text(json.dumps(document, indent=2) + "\n")
    return document


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--zig", default="zig")
    parser.add_argument(
        "--working-tree",
        action="store_true",
        help="Development only; release promotion rejects these artifacts",
    )
    args = parser.parse_args()
    print(
        json.dumps(
            build(
                ROOT,
                args.commit,
                args.version,
                args.out_dir,
                args.zig,
                args.working_tree,
            )
        )
    )


if __name__ == "__main__":
    main()
