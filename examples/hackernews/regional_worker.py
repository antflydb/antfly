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

"""Detached, singleflight benchmark worker; credentials remain on stdin only."""

import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def atomic_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value) + "\n")
    temporary.replace(path)


def main():
    mode, directory, *command = sys.argv[1:]
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    token = sys.stdin.readline(65537).strip()
    if not token or len(token) > 65536:
        raise ValueError("Expected a bounded stdin credential")
    digest = hashlib.sha256(json.dumps(command).encode()).hexdigest()
    with (root / "descriptor.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        descriptor = root / "descriptor.json"
        if descriptor.exists():
            if json.loads(descriptor.read_text())["command_sha256"] != digest:
                raise ValueError("Job command changed")
        else:
            atomic_json(descriptor, {"command_sha256": digest})
    if mode == "launch":
        if not (root / "exit.json").exists():
            with (root / "output.log").open("a") as log:
                child = subprocess.Popen(
                    [sys.executable, __file__, "supervise", directory, *command],
                    stdin=subprocess.PIPE,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                try:
                    child.stdin.write((token + "\n").encode())
                    child.stdin.close()
                except BrokenPipeError:
                    pass  # A duplicate supervisor exits without running the job.
        print(json.dumps({"job": root.name, "command_sha256": digest}))
        return
    if mode != "supervise":
        raise ValueError("Unknown worker mode")
    with (root / "running.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        if (root / "exit.json").exists():
            return
        atomic_json(
            root / "running.json", {"pid": os.getpid(), "command_sha256": digest}
        )
        try:
            result = subprocess.run(command, input=token + "\n", text=True, check=False)
            code = result.returncode
        except Exception as exc:
            code = 1
            print(json.dumps({"worker_error": type(exc).__name__}), flush=True)
        atomic_json(
            root / "exit.json",
            {"exit_code": code, "command_sha256": digest, "finished_unix": time.time()},
        )


if __name__ == "__main__":
    main()
