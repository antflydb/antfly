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

"""Apply Antfly license headers to first-party source files."""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from asset_licenses import check_asset_records
from qualification_provenance import check_frozen_helpers, frozen_pins

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
REPO_ROOT = ROOT

ELV2_ROOTS = (
    "zig/pkg/antfly",
    "zig/e2e/antfly",
)

APACHE_ROOTS = (
    "zig/build.zig",
    "zig/build.zig.zon",
    "zig/pkg/antfly-embedded",
    "zig/pkg/antfly-client",
    "zig/pkg/inference",
    "zig/pkg/inference-client",
    "zig/lib",
    "zig/e2e/inference",
    "go/pkg/embedded",
    "py/packages/embedded",
    "ts/packages/embedded",
    "rs",
    "examples",
    "go/pkg/docsaf",
    "go/pkg/evalaf",
    "go/pkg/genkit",
    "go/pkg/memoryaf",
    "go/pkg/operator",
    "go/pkg/proxy",
    "go/pkg/sdk",
    "scripts",
    "tools",
    "specs",
    "bench",
    "compat",
)

# Files inside an ELv2 root that are Apache-2.0 anyway. The public C ABI
# header is vendored or transcribed by the Apache-licensed Lite bindings.
APACHE_FILES = {
    line.strip()
    for line in (SCRIPT_DIR / "apache_engine_files.txt").read_text().splitlines()
    if line.strip() and not line.startswith("#")
}
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
    "deps/**",
    # Vendored MIT fork: package license covers sources without per-file notices.
    "zig/lib/httpx/**",
    # Bundled frontend output retains upstream and generator headers.
    "zig/pkg/antfly/antfarm/assets/**",
    "specs/tla/*etcdraft*",
    "scripts/uv.lock",
    "e2e/*/uv.lock",
    # Its generator emits the Apache header; CI checks the file is current.
    "rs/crates/sdk/src/graph_identifier_policy_generated.rs",
)

# These adapted/vendored sources retain their upstream or combined notices.
# Validate them separately rather than replacing them with a first-party header.
PRESERVED_NOTICES = json.loads(
    (SCRIPT_DIR / "preserved_license_notices.json").read_text()
)
# Retained qualification tooling is licensed by its Apache package LICENSE.
# Its exact bytes identify recorded evidence, so check pins instead of rewriting.
FROZEN_FILES = {name for name, _, _ in frozen_pins(ROOT)}


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
    ".rs",
    ".ts",
    ".tsx",
    ".wgsl",
    ".zig",
    ".zon",
    ".y",
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
    parser.add_argument(
        "paths",
        nargs="*",
        help="optional source paths to normalize (relative to the current directory)",
    )
    return parser.parse_args()


def read_header(name: str) -> Header:
    header_path = {
        "apache": SCRIPT_DIR / "license-header-apache.txt",
        "elv2": SCRIPT_DIR / "license-header-elv2.txt",
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
    if path in PRESERVED_NOTICES or path in FROZEN_FILES or parts & EXCLUDED_PARTS:
        return True
    return any(fnmatch.fnmatch(path, pattern) for pattern in EXCLUDED_GLOBS)


def group_for(path: str, selected_group: str) -> str | None:
    group: str | None = None
    if path in APACHE_FILES:
        group = "apache"
    elif is_under(path, ELV2_ROOTS):
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
    # A shebang keeps its line; a Rust inner attribute (`#![...]`) does not.
    if lines and lines[0].startswith("#!") and not lines[0].startswith("#!["):
        return 1
    return 0


def strip_existing_antfly_header(text: str, path: Path) -> tuple[str, int]:
    lines = text.splitlines(keepends=True)
    offset = insertion_offset(lines)
    prefix = comment_prefix(path)

    if offset >= len(lines):
        return text, offset

    first = lines[offset].strip()
    if (
        not first.startswith(prefix)
        or "Copyright " not in first
        or not any(
            owner in first for owner in ("Antfly, Inc.", "The Antfly Contributors")
        )
    ):
        return text, offset

    # Stop at the legal notice's final line, even when usage comments follow
    # without an uncommented blank line. Comments after the notice are source.
    end = offset + 1
    while end < min(len(lines), offset + 40):
        line = lines[end].strip()
        if (
            not line
            or not line.startswith(prefix)
            or line.startswith((prefix + "!", prefix + "/"))
        ):
            break
        end += 1
        body = line[len(prefix) :].strip()
        if (
            body == "limitations."
            or body == "except in compliance with the Elastic License 2.0."
            or body == "Licensed under the Elastic License 2.0 (ELv2)."
        ):
            break
    notice = "".join(lines[offset:end])
    if not any(
        marker in notice for marker in ("Licensed under", "SPDX-License-Identifier:")
    ) or not any(name in notice for name in ("Apache", "Elastic", "ELv2")):
        return text, offset
    while end < len(lines) and lines[end].strip() == "":
        end += 1
    return "".join(lines[:offset] + lines[end:]), offset


def apply_header(text: str, path: Path, header: Header) -> str:
    stripped, offset = strip_existing_antfly_header(text, path)
    while True:
        cleaned, offset = strip_existing_antfly_header(stripped, path)
        if cleaned == stripped:
            break
        stripped = cleaned
    lines = stripped.splitlines(keepends=True)
    # The rendered header ends with its own blank line; drop any the body
    # already starts with so applying twice gives the same result.
    while offset < len(lines) and lines[offset].strip() == "":
        del lines[offset]
    rendered = render_header(path, header)
    if not "".join(lines[offset:]).strip():
        rendered = rendered.rstrip("\n") + "\n"
    return "".join(lines[:offset]) + rendered + "".join(lines[offset:])


def normalized_notice(text: str) -> str:
    # Ignore source comment decoration and whitespace, while retaining every
    # copyright, condition, and disclaimer word from the canonical notice.
    lines = [re.sub(r"^\s*(?://|\*) ?", "", line) for line in text.splitlines()]
    return " ".join("\n".join(lines).split())


def check_preserved_notices(selected_group: str) -> list[str]:
    errors = []
    bundle = ROOT / "THIRD_PARTY_NOTICES.md"
    bundle_text = normalized_notice(bundle.read_text()) if bundle.is_file() else ""
    for name, definition in PRESERVED_NOTICES.items():
        # The repository-wide check must cover vendored sources outside the
        # first-party Apache and ELv2 trees as well.
        if selected_group != "all" and group_for(name, selected_group) is None:
            continue
        canonical = ROOT / definition["file"]
        if not canonical.is_file() or not canonical.read_text().strip():
            errors.append(f"missing canonical license notice: {canonical}")
            continue
        expected = normalized_notice(canonical.read_text())
        path = ROOT / name
        if not path.is_file():
            errors.append(f"missing source with preserved license: {name}")
        elif expected not in normalized_notice(path.read_text()) or any(
            required not in path.read_text()
            for required in definition.get("required", ())
        ):
            errors.append(f"missing or changed preserved license notice: {name}")
        if expected not in bundle_text:
            errors.append(f"missing bundled license notice: {definition['file']}")
    return sorted(set(errors))


def main() -> int:
    args = parse_args()
    headers = {
        "apache": read_header("apache"),
        "elv2": read_header("elv2"),
    }
    errors = check_preserved_notices(args.group)
    if args.group in ("all", "apache"):
        errors.extend(check_frozen_helpers(ROOT))
        errors.extend(check_asset_records(ROOT))
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    changed: list[str] = []

    selected_paths = {Path(name).resolve() for name in args.paths}
    files = discover(args.group)
    unknown_paths = selected_paths - {path.resolve() for path, _ in files}
    if unknown_paths:
        for path in sorted(unknown_paths):
            print(
                f"source path is outside the selected first-party policy: {path}",
                file=sys.stderr,
            )
        return 1
    for path, group in files:
        if selected_paths and path.resolve() not in selected_paths:
            continue
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
