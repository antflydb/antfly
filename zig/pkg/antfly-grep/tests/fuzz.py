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

"""Run finite, separately selected coverage fuzz campaigns; retain reports/logs.

Each target uses ReleaseSafe native modules, allocator quotas and bounded inputs.
The Zig fuzzer may exceed its requested limit by a worker batch; actual accumulated
run counts and coverage are recorded verbatim. This is finite evidence, not a
claim that arbitrary malformed inputs have been proven safe.
"""

import argparse
import hashlib
import json
import os
import platform
import re
import signal
import subprocess
import time
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--zig", default="zig")
    p.add_argument("--iterations", type=int, default=50000)
    p.add_argument("--timeout", type=float, default=600)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    if args.iterations < 1 or args.timeout <= 0:
        p.error("positive iteration count and timeout required")
    root = Path(__file__).resolve().parents[4]
    output = args.output.resolve()
    logdir = root / ".zig-cache" / "grep-fuzz-logs"
    logdir.mkdir(parents=True, exist_ok=True)
    env = dict(
        os.environ,
        ZIG_LOCAL_CACHE_DIR=".zig-cache",
        ZIG_GLOBAL_CACHE_DIR=".zig-cache/global",
    )
    sources = [
        *root.glob("zig/lib/regex/src/*.zig"),
        *root.glob("zig/lib/syntax/src/**/*.zig"),
        *root.glob("zig/lib/compress/src/**/*.zig"),
        Path(__file__).with_name("harden.zig"),
    ]
    sources.extend(root.glob("zig/lib/syntax/src/grammars/*.bin"))
    sources.extend(root.glob("zig/lib/hash/src/**/*.zig"))
    sources.extend(
        [
            root / "zig/lib/tokenizer/src/grapheme.zig",
            root / "zig/pkg/antfly-grep/build.zig",
        ]
    )
    report = {
        "toolchain": subprocess.check_output([args.zig, "version"], text=True).strip(),
        "platform": platform.platform(),
        "requested_iterations_per_target": args.iterations,
        "sources": {
            str(v.relative_to(root)): hashlib.sha256(v.read_bytes()).hexdigest()
            for v in sorted(set(sources))
        },
        "campaigns": {},
    }
    for target in ("regex", "parsers", "dictionaries"):
        cmd = [
            args.zig,
            "build",
            "--build-file",
            "zig/build.zig",
            "-Dgrep-only=true",
            "-Doptimize=ReleaseFast",
            f"-Dgrep-fuzz-target={target}",
            "grep-harden",
            f"--fuzz={args.iterations}",
        ]
        start = time.monotonic()
        log = logdir / f"{target}.log"
        with log.open("w") as stream:
            try:
                run = subprocess.Popen(
                    cmd,
                    cwd=root,
                    env=env,
                    stdout=stream,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                code = run.wait(timeout=args.timeout)
            except subprocess.TimeoutExpired:
                os.killpg(run.pid, signal.SIGTERM)
                try:
                    run.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(run.pid, signal.SIGKILL)
                    run.wait()
                code = "timeout"
        text = log.read_text(errors="replace")
        counts = re.search(r"Runs: (\d+) -> (\d+)", text)
        if code == 0 and (counts is None or int(counts[2]) <= int(counts[1])):
            code = "missing-fuzz-progress"
        report["campaigns"][target] = {
            "command": cmd,
            "exit": code,
            "seconds": time.monotonic() - start,
            "runs_added": int(counts[2]) - int(counts[1]) if counts else 0,
            "log_sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
            "fuzzer_report": text[text.rfind("======= FUZZING REPORT") :].strip()
            if "======= FUZZING REPORT" in text
            else text[-8000:],
        }
        output.write_text(json.dumps(report, indent=2) + "\n")
        print(target, code, flush=True)
        if code != 0:
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
