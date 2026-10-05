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

"""Compile lake, public C API, and browser artifacts without server implementations."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path


def stage_sources(repository: Path, destination: Path) -> int:
    listing = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=repository,
    )
    removed = 0
    for raw in listing.split(b"\0"):
        if not raw:
            continue
        relative = Path(raw.decode())
        # Build inputs and source-generation tooling; bindings and application
        # assets are unrelated to either compilation owner.
        if relative.parts[0] not in {
            "zig",
            "specs",
            "scripts",
        } and not relative.as_posix().startswith(
            "ts/packages/design-system/src/fonts/"
        ):
            continue
        source = repository / relative
        if not source.is_file():
            continue
        # No stubs or copied server source: even dormant literal imports must
        # belong to embedded, a shared library, or an explicit test capability.
        if relative.parts[:3] == ("zig", "pkg", "antfly"):
            removed += 1
            continue
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    return removed


def smoke_lite(stage: Path) -> None:
    """Exercise durable local operations using the server-free executable."""
    executable = stage / "zig/zig-out/bin/antfly-lite"
    database = stage / "smoke.aflite"
    restored = stage / "restored.aflite"
    backup = stage / "smoke.afb"
    request = stage / "batch.json"
    request.write_text(json.dumps({"inserts": {"doc:smoke": {"title": "embedded"}}}))

    def run(*args: object) -> str:
        result = subprocess.run(
            [str(executable), *(str(arg) for arg in args)],
            cwd=stage,
            text=True,
            capture_output=True,
        )
        if result.returncode:
            raise RuntimeError(f"Lite command failed: {args}\n{result.stderr}")
        return result.stdout

    run("init", database)
    run("batch", database, "--file", request)
    run("backup", database, "--out", backup)
    run("restore", backup, "--out", restored)
    document = json.loads(run("lookup", restored, "--key", "doc:smoke", "--readonly"))
    if document.get("_source") != {"title": "embedded"}:
        raise RuntimeError(f"Lite backup/restore lost the document: {document}")
    run("check", restored)
    worker = subprocess.run(
        [str(executable), "inference", "_worker"],
        cwd=stage,
        input="",
        text=True,
        capture_output=True,
        timeout=15,
    )
    if worker.returncode:
        raise RuntimeError(
            f"Lite inference worker failed to shut down on EOF: {worker.stderr}"
        )
    print(
        "Server-free Lite init/batch/backup/restore/lookup/check and worker shutdown passed",
        flush=True,
    )


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
        print(
            f"Staged embedded build: {removed} server files omitted",
            flush=True,
        )
        subprocess.run(
            [
                args.zig,
                "build",
                "--build-file",
                "embedded.build.zig",
                "lite",
                "capi-smoke",
                "embedded-capi-check",
                "embedded-lake-test",
                "embedded-package-test",
                "aws-credentials-test",
                "embedded-native-module-boundary-check",
                "embedded-wasm-module-boundary-check",
                "wasm-test",
                "-Dmetal=false",
                *flags,
            ],
            cwd=stage / "zig",
            check=True,
        )
        smoke_lite(stage)


if __name__ == "__main__":
    main()
