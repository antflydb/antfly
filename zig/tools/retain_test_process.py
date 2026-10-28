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

"""Retain live Linux tests or compiler inputs before watchdog termination."""

import argparse
import hashlib
import json
import shutil
from pathlib import Path


def retain(pid: int, destination: Path, proc: Path = Path("/proc")) -> bool:
    process = proc / str(pid)
    executable = (process / "exe").readlink()
    name = executable.name.removesuffix(" (deleted)")
    if name == "zig":
        return retain_compiler(pid, destination, proc)
    if name != "test" and not name.endswith(("-tests", "-tests-local")):
        return False
    # Open through proc: even an unlinked cache executable remains recoverable.
    command = (process / "cmdline").read_bytes().rstrip(b"\0").split(b"\0")
    cwd = str((process / "cwd").readlink())
    destination.mkdir(parents=True, exist_ok=True)
    output = destination / f"test-{pid}"
    shutil.copyfile(process / "exe", output)
    output.chmod(0o755)
    digest = hashlib.sha256()
    with output.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    output.with_suffix(".json").write_text(
        json.dumps(
            {
                "executable": str(executable),
                "argv": [arg.decode(errors="replace") for arg in command],
                "cwd": cwd,
                "sha256": digest.hexdigest(),
            },
            indent=2,
        )
        + "\n"
    )
    return True


def retain_compiler(pid: int, destination: Path, proc: Path) -> bool:
    process = proc / str(pid)
    command = (process / "cmdline").read_bytes().rstrip(b"\0").split(b"\0")
    if len(command) < 2 or command[1] not in (
        b"test",
        b"test-obj",
        b"build-exe",
        b"build-lib",
        b"build-obj",
    ):
        return False
    # The response file identifies the actual module, filters, backend and
    # target. A PID plus `zig test @cache/args/hash` cannot identify an expensive
    # compile once the runner's private cache has disappeared.
    output = destination / f"compiler-{pid}"
    output.mkdir(parents=True, exist_ok=True)
    cwd = (process / "cwd").readlink()
    response_files = []
    for index, argument in enumerate(command[2:]):
        if not argument.startswith(b"@"):
            continue
        path = Path(argument[1:].decode())
        if not path.is_absolute():
            path = cwd / path
        target = output / f"args-{index}.txt"
        shutil.copyfile(path, target)
        response_files.append({"original": str(path), "retained": target.name})
    for metric in ("status", "io", "stat"):
        path = process / metric
        if path.exists():
            shutil.copyfile(path, output / metric)
    (output / "command.json").write_text(
        json.dumps(
            {
                "argv": [argument.decode(errors="replace") for argument in command],
                "cwd": str(cwd),
                "response_files": response_files,
            },
            indent=2,
        )
        + "\n"
    )
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pid", type=int)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    retain(args.pid, args.destination)
