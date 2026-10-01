#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""Compile public C API and browser artifacts without server implementations."""
from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import subprocess
import tempfile

from audit_embedded_source_boundary import server_source


def stage_sources(repository: Path, destination: Path) -> int:
    listing = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=repository
    )
    removed = 0
    for raw in listing.split(b"\0"):
        if not raw:
            continue
        relative = Path(raw.decode())
        # Build inputs and source-generation tooling; bindings and application
        # assets are unrelated to either compilation owner.
        if relative.parts[0] not in {"zig", "specs", "scripts"}:
            continue
        source = repository / relative
        if not source.is_file():
            continue
        prefix = "zig/pkg/antfly/src/"
        if relative.as_posix().startswith(prefix) and server_source(relative.as_posix().removeprefix(prefix)):
            removed += 1
            # Zig scans relative imports in dormant test/target branches for
            # cache inputs. Retain only an unconditional compile-time trap,
            # never a server implementation: a live import must fail closed.
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text('comptime { @compileError("server implementation unavailable in embedded build"); }\n')
            continue
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    return removed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zig", default="zig")
    parser.add_argument("build_flags", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    flags = args.build_flags
    if flags[:1] == ["--"]:
        flags = flags[1:]
    repository = Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory(prefix="antfly-embedded-isolated-") as directory:
        stage = Path(directory)
        removed = stage_sources(repository, stage)
        print(f"Staged embedded build: {removed} server implementations replaced with compile-time traps", flush=True)
        subprocess.run(
            [args.zig, "build", "embedded-capi-check", "embedded-native-module-boundary-check",
             "embedded-wasm-module-boundary-check", "wasm", "-Dmetal=false", *flags],
            cwd=stage / "zig", check=True,
        )


if __name__ == "__main__":
    main()
