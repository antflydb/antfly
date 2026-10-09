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

"""Build separately from inference and bind the binary to an unchanged source snapshot."""

import argparse
import json
import re
from pathlib import Path
import subprocess
import sys

import run_family_performance as perf


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    output = a.output.resolve()
    if output.exists():
        raise RuntimeError("refusing to overwrite build receipt")
    output.parent.mkdir(parents=True, exist_ok=True)
    source = perf.source_identity()
    command = [
        sys.executable,
        "tools/run_bounded_zig_build.py",
        "build",
        "inference-bench-server",
        "-Doptimize=fast",
        "-Dcuda=false",
        "-Dmetal=true",
        "-j1",
    ]
    log = output.with_suffix(".log")
    with log.open("w") as stream:
        result = subprocess.run(
            command, cwd=perf.ROOT / "zig", stdout=stream, stderr=subprocess.STDOUT
        )
    binary = perf.ROOT / "zig/zig-out/bin/antfly-inference-bench-server"
    receipt = dict(
        source=source,
        command=command,
        build_log=str(log),
        exit_code=result.returncode,
        source_unchanged=source == perf.source_identity(),
        finished_utc=perf.utc_now(),
    )
    receipt["build_error_reported"] = bool(re.search(r"(?m)^error:", log.read_text()))
    receipt["passed"] = (
        result.returncode == 0
        and receipt["source_unchanged"]
        and not receipt["build_error_reported"]
    )
    if receipt["passed"]:
        receipt.update(binary=str(binary), binary_sha256=perf.sha256(binary))
    output.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
