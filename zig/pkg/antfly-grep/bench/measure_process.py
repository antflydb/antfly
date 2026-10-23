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

"""Measure a Unix child without attributing Python harness CPU/RSS to the CLI."""

import argparse
import json
import os
import sys
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or not hasattr(os, "wait4"):
        parser.error("a command and Unix wait4 are required")
    read_fd, write_fd = os.pipe()
    started = time.monotonic_ns()
    child = os.fork()
    if child == 0:
        try:
            os.close(read_fd)
            os.dup2(write_fd, 1)
            os.close(write_fd)
            os.execvpe(command[0], command, os.environ)
        except BaseException as error:
            os.write(2, (str(error) + "\n").encode())
            os._exit(127)
    os.close(write_fd)
    first_stdout = None
    try:
        while data := os.read(read_fd, 65536):
            if first_stdout is None:
                first_stdout = time.monotonic_ns() - started
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()
    finally:
        os.close(read_fd)
        _, status, usage = os.wait4(child, 0)
    args.report.write_text(
        json.dumps(
            {
                "elapsed_ns": time.monotonic_ns() - started,
                "first_stdout_ns": first_stdout,
                "user_cpu_ns": round(usage.ru_utime * 1_000_000_000),
                "system_cpu_ns": round(usage.ru_stime * 1_000_000_000),
                "peak_rss_bytes": usage.ru_maxrss
                * (1 if sys.platform == "darwin" else 1024),
            }
        )
        + "\n"
    )
    code = os.waitstatus_to_exitcode(status)
    return code if code >= 0 else 128 - code


if __name__ == "__main__":
    sys.exit(main())
