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

"""Compare embedded import libraries using identical fresh destinations.

Example: python3 scripts/bench/embedded_sql_import.py --importer /path/to/codex-antfly-import
  --thread <uuid> --library-dir /path/to/new/lib --baseline-library-dir /path/to/old/lib

The importer executable (and its SQLx adapter) stays fixed. Rebuild the importer
against the workspace Rust crate to measure SQLx changes too. Use a frozen
--codex-home when the source thread is still active. Thread data and
model downloads are read from the existing Codex/inference homes. No existing
database is written. `foreground_seconds` reports the verbose importer timing;
`seconds` includes startup and shutdown. Asynchronous index readiness is a
separate measurement.
"""

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
import tempfile
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--importer", type=Path, required=True)
    parser.add_argument("--thread", required=True)
    parser.add_argument("--library-dir", type=Path, required=True)
    parser.add_argument("--baseline-library-dir", type=Path)
    parser.add_argument("--codex-home", type=Path)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    libraries = [("candidate", args.library_dir)]
    if args.baseline_library_dir:
        libraries.insert(0, ("baseline", args.baseline_library_dir))
    importer_fingerprint = hashlib.sha256(args.importer.read_bytes()).hexdigest()
    for _, directory in libraries:
        filename = (
            "libantfly.dylib" if platform.system() == "Darwin" else "libantfly.so"
        )
        if not (directory / filename).is_file():
            parser.error(
                f"Build the library before benchmarking: {directory / filename}"
            )
    root = args.output_dir or Path(tempfile.mkdtemp(prefix="antfly-sql-import-"))
    if args.output_dir:
        root.mkdir(parents=True, exist_ok=False)
    rows = []
    # Pair libraries within each repeat, reversing their order on alternate
    # repeats so model warm-up and machine load do not always favor one build.
    runs = [
        (label, directory, repeat)
        for repeat in range(args.repeats)
        for label, directory in (libraries if repeat % 2 == 0 else libraries[::-1])
    ]
    for label, directory, repeat in runs:
        directory = directory.resolve()
        dylib = directory / (
            "libantfly.dylib" if platform.system() == "Darwin" else "libantfly.so"
        )
        fingerprint = hashlib.sha256(dylib.read_bytes()).hexdigest()
        # A compatibility adapter may reexport a separately named native core.
        # Fingerprint that code too, so identical adapters cannot conceal a
        # change of the actual storage/SQL library being measured.
        core = directory / ("libantfly-core" + dylib.suffix)
        core_fingerprint = (
            hashlib.sha256(core.read_bytes()).hexdigest() if core.is_file() else None
        )
        # Alternate mode ordering to reduce a systematic warm-model bias.
        for semantic in [False, True] if repeat % 2 == 0 else [True, False]:
            tag = f"{label}-{repeat}-{'semantic' if semantic else 'text'}"
            command = [
                str(args.importer.resolve()),
                "--thread",
                args.thread,
                "--to",
                str(root / (tag + ".aflite")),
                "--verbose",
            ]
            if args.codex_home:
                command.extend(["--codex-home", str(args.codex_home.resolve())])
            if not semantic:
                command.append("--no-semantic")
            env = os.environ.copy()
            env[
                "DYLD_LIBRARY_PATH"
                if platform.system() == "Darwin"
                else "LD_LIBRARY_PATH"
            ] = str(directory)
            started = time.monotonic()
            completed = subprocess.run(
                command, check=False, env=env, capture_output=True, text=True
            )
            elapsed = time.monotonic() - started
            (root / (tag + ".log")).write_text(completed.stdout + completed.stderr)
            match = re.search(
                r"done: imported (\d+), already present (\d+), failed (\d+)",
                completed.stdout,
            )
            counts = list(map(int, match.groups())) if match else None
            duration = re.search(
                rf"^\s+{re.escape(args.thread)} ([\d.]+)(ns|µs|ms|s)$",
                completed.stdout,
                re.MULTILINE,
            )
            foreground_seconds = (
                float(duration[1])
                * {"ns": 1e-9, "µs": 1e-6, "ms": 1e-3, "s": 1}[duration[2]]
                if duration
                else None
            )
            row = {
                "library": label,
                "library_sha256": fingerprint,
                "reexported_library_sha256": core_fingerprint,
                "importer_sha256": importer_fingerprint,
                "repeat": repeat,
                "semantic": semantic,
                "seconds": elapsed,
                "foreground_seconds": foreground_seconds,
                "returncode": completed.returncode,
                "counts": counts,
            }
            rows.append(row)
            (root / "results.json").write_text(json.dumps(rows, indent=2) + "\n")
            print(json.dumps(row), flush=True)
            if completed.returncode != 0 or counts != [1, 0, 0]:
                raise SystemExit(
                    f"Import failed or selected thread missing; inspect {root / (tag + '.log')}"
                )
    print(f"Results and fresh databases: {root}")


if __name__ == "__main__":
    main()
