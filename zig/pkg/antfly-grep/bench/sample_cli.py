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

"""Sample a macOS CLI process; retain raw stacks and input/executable identities."""

import argparse
import hashlib
import json
import os
import platform
import subprocess
import time
from pathlib import Path


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def source_identity(binary, subject, env):
    if subject.is_file():
        return {
            "subject_sha256": digest(subject),
            "subject_bytes": subject.stat().st_size,
        }
    paths = subprocess.check_output(
        [str(binary), "--files", "--null", str(subject)], env=env
    ).split(b"\0")
    value, size, count = hashlib.sha256(), 0, 0
    for name in sorted(filter(None, paths)):
        path = Path(os.fsdecode(name))
        value.update(os.fsencode(path.relative_to(subject)))
        value.update(b"\0")
        value.update(bytes.fromhex(digest(path)))
        size += path.stat().st_size
        count += 1
    return {
        "subject_sha256": value.hexdigest(),
        "subject_bytes": size,
        "subject_files": count,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("subject", type=Path)
    parser.add_argument("pattern")
    parser.add_argument("--seconds", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--engine", choices=("default", "pcre2"), default="pcre2")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--count", action="store_true")
    mode.add_argument("--files-with-matches", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if (
        platform.system() != "Darwin"
        or args.seconds < 1
        or args.timeout <= args.seconds
    ):
        parser.error("this sampling adapter requires macOS and a positive duration")
    binary, subject = args.binary.resolve(), args.subject.resolve()
    stacks = args.output.with_suffix(".sample.txt")
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ANTFLY_GREP_", "RIPGREP_"))
    }
    report = {
        "complete": False,
        "platform": platform.platform(),
        "load_average": list(os.getloadavg()),
        "binary_sha256": digest(binary),
        **source_identity(binary, subject, env),
        "seconds_requested": args.seconds,
        "version": subprocess.check_output(
            [str(binary), "--version"], text=True
        ).strip(),
    }
    command = [
        str(binary),
        *(["-P"] if args.engine == "pcre2" else []),
        "-l" if args.files_with_matches else "-c" if args.count else "-o",
        "--color=never",
        "-e",
        args.pattern,
        str(subject),
    ]
    report["command"] = command
    stacks.unlink(missing_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    start = time.monotonic()
    process = subprocess.Popen(
        command, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
    )
    try:
        try:
            sample = subprocess.run(
                [
                    "/usr/bin/sample",
                    str(process.pid),
                    str(args.seconds),
                    "1",
                    "-mayDie",
                    "-file",
                    str(stacks),
                ],
                capture_output=True,
                timeout=args.timeout,
            )
            sampler_exit, diagnostic = sample.returncode, sample.stderr
        except subprocess.TimeoutExpired as error:
            sampler_exit, diagnostic = "timeout", error.stderr or b""
        try:
            _, errors = process.communicate(timeout=args.timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            _, errors = process.communicate()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
    report.update(
        complete=process.returncode == 0 and sampler_exit == 0 and stacks.exists(),
        exit=process.returncode,
        elapsed_seconds=time.monotonic() - start,
        sampler_exit=sampler_exit,
        sampler_diagnostic=diagnostic.decode(errors="replace"),
        stderr=errors.decode(errors="replace"),
        stack_file=stacks.name,
        stack_sha256=digest(stacks) if stacks.exists() else None,
    )
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    return int(not report["complete"])


if __name__ == "__main__":
    raise SystemExit(main())
