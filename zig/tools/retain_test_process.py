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

"""Retain a live Linux test executable before the unit watchdog terminates it."""

import argparse
import hashlib
import json
import shutil
from pathlib import Path


def retain(pid: int, destination: Path, proc: Path = Path("/proc")) -> bool:
    process = proc / str(pid)
    executable = (process / "exe").readlink()
    name = executable.name.removesuffix(" (deleted)")
    if name != "test" and not name.endswith("-tests"):
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pid", type=int)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    retain(args.pid, args.destination)
