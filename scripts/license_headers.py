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

"""Apply Antfly license headers to first-party source files."""

from __future__ import annotations

import argparse
import fnmatch
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent

ELV2_ROOTS = (
    "pkg/antfly",
    "pkg/antfly-embedded",
    "e2e/antfly",
)

APACHE_ROOTS = (
    "build.zig",
    "build.zig.zon",
    "pkg/antfly-client",
    "pkg/termite",
    "pkg/termite-client",
    "lib",
    "e2e/termite",
    "scripts",
    "tools",
    "specs",
    "bench",
    "compat",
)

EXCLUDED_PARTS = {
    ".git",
    ".pytest_cache",
    ".venv",
    ".zig-cache",
    ".zig-global-cache",
    ".zig-local-cache",
    ".debug",
    "__pycache__",
    "generated",
    "node_modules",
    "proto",
    "protos",
    "testdata",
    "vendor",
    "zig-out",
}

EXCLUDED_GLOBS = (
    "lib/httpx/**",
    "lib/lmdb/**",
    "deps/**",
    "pkg/termite/onnxruntime/**",
    "specs/tla/*etcdraft*",
    "scripts/uv.lock",
    "e2e/*/uv.lock",
)

SLASH_EXTS = {
    ".c",
    ".cc",
    ".cjs",
    ".cpp",
    ".cu",
    ".go",
    ".h",
    ".js",
    ".m",
    ".metal",
    ".mjs",
    ".ts",
    ".tsx",
    ".wgsl",
    ".zig",
    ".zon",
}

HASH_EXTS = {
    ".bash",
    ".csh",
    ".fish",
    ".nu",
    ".ps1",
    ".py",
    ".sh",
}

TLA_EXTS = {
    ".cfg",
    ".tla",
}

SOURCE_EXTS = SLASH_EXTS | HASH_EXTS | TLA_EXTS


@dataclass(frozen=True)
class Header:
    name: str
    body: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="check files without modifying them",
    )
    parser.add_argument(
        "--group",
        choices=("all", "elv2", "apache"),
        default="all",
        help="which license group to process",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="print changed or non-compliant files",
    )
    return parser.parse_args()


def read_header(name: str) -> Header:
    header_path = {
        "apache": REPO_ROOT / ".license-header-apache.txt",
        "elv2": REPO_ROOT / ".license-header.txt",
    }[name]
    return Header(name=name, body=header_path.read_text())


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def is_under(path: str, roots: tuple[str, ...]) -> bool:
    for root in roots:
        if "/" not in root and "." in Path(root).name:
            if path == root:
                return True
            continue
        if path == root or path.startswith(root.rstrip("/") + "/"):
            return True
    return False


def excluded(path: str) -> bool:
    parts = set(Path(path).parts)
    if parts & EXCLUDED_PARTS:
        return True
    return any(fnmatch.fnmatch(path, pattern) for pattern in EXCLUDED_GLOBS)


def group_for(path: str, selected_group: str) -> str | None:
    group: str | None = None
    if is_under(path, ELV2_ROOTS):
        group = "elv2"
    elif is_under(path, APACHE_ROOTS):
        group = "apache"

    if group is None or selected_group not in ("all", group):
        return None
    return group


def discover(selected_group: str) -> list[tuple[Path, str]]:
    files: list[tuple[Path, str]] = []
    result = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT,
        check=True,
        stdout=subprocess.PIPE,
    )
    for raw_path in result.stdout.decode().split("\0"):
        if not raw_path:
            continue
        path = ROOT / raw_path
        if not path.is_file():
            continue
        if excluded(raw_path) or path.suffix not in SOURCE_EXTS:
            continue
        group = group_for(raw_path, selected_group)
        if group is not None:
            files.append((path, group))
    return sorted(files, key=lambda item: rel(item[0]))


def comment_prefix(path: Path) -> str:
    suffix = path.suffix
    if suffix in SLASH_EXTS:
        return "//"
    if suffix in HASH_EXTS:
        return "#"
    if suffix in TLA_EXTS:
        return r"\*"
    raise ValueError(f"unsupported source extension for {path}")


def render_header(path: Path, header: Header) -> str:
    prefix = comment_prefix(path)
    rendered = []
    for line in header.body.rstrip("\n").splitlines():
        if line:
            rendered.append(f"{prefix} {line}")
        else:
            rendered.append(prefix)
    return "\n".join(rendered) + "\n\n"


def insertion_offset(lines: list[str]) -> int:
    if lines and lines[0].startswith("#!"):
        return 1
    return 0


def strip_existing_antfly_header(text: str, path: Path) -> tuple[str, int]:
    lines = text.splitlines(keepends=True)
    offset = insertion_offset(lines)
    prefix = comment_prefix(path)

    if offset >= len(lines):
        return text, offset

    first = lines[offset].strip()
    if not first.startswith(prefix) or "Copyright " not in first or "Antfly, Inc." not in first:
        return text, offset

    end = offset + 1
    scan_limit = min(len(lines), offset + 40)
    while end < scan_limit:
        line = lines[end].strip()
        if line and not line.startswith(prefix):
            return text, offset
        end += 1
        if "limitations" in line:
            break
    else:
        return text, offset

    while end < len(lines) and lines[end].strip() == "":
        end += 1

    return "".join(lines[:offset] + lines[end:]), offset


def apply_header(text: str, path: Path, header: Header) -> str:
    stripped, offset = strip_existing_antfly_header(text, path)
    lines = stripped.splitlines(keepends=True)
    rendered = render_header(path, header)
    if not "".join(lines[offset:]).strip():
        rendered = rendered.rstrip("\n") + "\n"
    return "".join(lines[:offset]) + rendered + "".join(lines[offset:])


def main() -> int:
    args = parse_args()
    headers = {
        "apache": read_header("apache"),
        "elv2": read_header("elv2"),
    }
    changed: list[str] = []

    for path, group in discover(args.group):
        original = path.read_text()
        updated = apply_header(original, path, headers[group])
        if updated == original:
            continue
        changed.append(rel(path))
        if not args.check:
            path.write_text(updated)

    if args.check and changed:
        for path in changed:
            print(f"missing or stale license header: {path}", file=sys.stderr)
        return 1

    if args.verbose:
        action = "would update" if args.check else "updated"
        for path in changed:
            print(f"{action}: {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
