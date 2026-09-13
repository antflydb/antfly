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

"""Compare or synchronize directories wholly owned by a source generator."""

from __future__ import annotations

import argparse
import os
import shutil
import tempfile
from pathlib import Path


def files(root: Path) -> dict[Path, Path]:
    if root.is_symlink():
        raise ValueError(f"generated directory must not be a symlink: {root}")
    if not root.exists():
        return {}
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"generated files must not be symlinks: {path}")
        if path.is_file():
            result[path.relative_to(root)] = path
    return result


def reconcile(source: Path, destination: Path, *, check: bool) -> list[str]:
    """Return drift descriptions; checking never writes, syncing preserves matches."""
    source = source.absolute()
    destination = destination.absolute()
    if source.resolve() == destination.resolve() or (
        source.resolve() in destination.resolve().parents
        or destination.resolve() in source.resolve().parents
    ):
        raise ValueError("generated source and destination must not overlap")
    if not source.is_dir():
        raise ValueError(f"missing generator output: {source}")
    expected = files(source)
    actual = files(destination)
    changes = []
    for relative in sorted(expected.keys() | actual.keys()):
        target = destination / relative
        if relative not in expected:
            changes.append(f"extra: {target}")
            if not check:
                target.unlink()
        elif (
            relative not in actual
            or expected[relative].read_bytes() != actual[relative].read_bytes()
        ):
            changes.append(
                f"{'missing' if relative not in actual else 'changed'}: {target}"
            )
            if not check:
                target.parent.mkdir(parents=True, exist_ok=True)
                # Install each complete file atomically; leave unchanged files alone.
                with tempfile.NamedTemporaryFile(
                    dir=target.parent, delete=False
                ) as temporary:
                    temporary_path = Path(temporary.name)
                try:
                    shutil.copyfile(expected[relative], temporary_path)
                    shutil.copymode(expected[relative], temporary_path)
                    os.replace(temporary_path, target)
                finally:
                    temporary_path.unlink(missing_ok=True)
    if not check and destination.exists():
        for directory in sorted(destination.rglob("*"), reverse=True):
            if directory.is_dir() and not any(directory.iterdir()):
                directory.rmdir()
    return changes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("check", "sync"))
    parser.add_argument("paths", nargs="+", type=Path, help="source/destination pairs")
    args = parser.parse_args()
    if len(args.paths) % 2:
        parser.error("expected source/destination pairs")
    changes = []
    for source, destination in zip(args.paths[::2], args.paths[1::2]):
        changes.extend(reconcile(source, destination, check=args.mode == "check"))
    for change in changes:
        print(change)
    if changes and args.mode == "check":
        print("Generated sources differ; run make -C zig openapi-generate.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
