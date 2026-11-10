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

"""Linux 1 CPU/2 GiB regression for terminal owner cleanup and live admission.

Run inside a no-swap 2 GiB cgroup with a Linux inference test binary compiled
with -Dtest-filter=memory. The writer exits before the runtime denies work
under measured pressure, cleans the file through terminal owner advice, and admits the same request.
The hot mapping can follow a sparse prefix to exercise large file tails.
No model download or external service is required. Test data stays in the
caller-supplied directory and is deleted.
"""

import argparse
import json
import mmap
import os
from pathlib import Path
import subprocess
import sys
import time

MIB = 1024 * 1024
CGROUP = Path("/sys/fs/cgroup")


def snapshot(stage):
    stats = dict(
        line.split() for line in (CGROUP / "memory.stat").read_text().splitlines()
    )
    events = dict(
        line.split() for line in (CGROUP / "memory.events").read_text().splitlines()
    )
    return {
        "stage": stage,
        "memory_current": int((CGROUP / "memory.current").read_text()),
        "memory_peak": int((CGROUP / "memory.peak").read_text()),
        **{
            key: int(stats.get(key, 0))
            for key in (
                "anon",
                "active_file",
                "inactive_file",
                "file_dirty",
                "file_writeback",
            )
        },
        "oom_kill": int(events["oom_kill"]),
    }


def write_cache(path, hot_offset_mib):
    size = 1664 * MIB
    with path.open("xb+") as stream:
        prefix = hot_offset_mib * MIB
        stream.truncate(prefix + size)
        with mmap.mmap(stream.fileno(), size, offset=prefix) as pages:
            for offset in range(0, size, 4096):
                pages[offset] = 49
            pages.flush()
            os.fsync(stream.fileno())
            checksum = 0
            for _ in range(3):
                for offset in range(0, size, 4096):
                    checksum ^= pages[offset]
            assert checksum == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-binary", type=Path)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument(
        "--hot-offset-mib",
        type=int,
        default=0,
        help="Offset of the cached mapping; test large sparse file tails",
    )
    parser.add_argument("--writer", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    path = args.directory / "antfly-retired-pressure.bin"
    if args.hot_offset_mib < 0:
        parser.error("--hot-offset-mib must be nonnegative")
    if args.writer:
        write_cache(path, args.hot_offset_mib)
        return
    if args.test_binary is None:
        parser.error("--test-binary is required")
    if int((CGROUP / "memory.max").read_text()) != 2048 * MIB:
        parser.error("requires a 2 GiB cgroup")
    if int((CGROUP / "memory.swap.max").read_text()) != 0:
        parser.error("requires no additional swap allowance")
    args.directory.mkdir(parents=True, exist_ok=True)
    # Never overwrite or remove an existing caller-owned file.
    if path.exists():
        raise FileExistsError(path)
    baseline = snapshot("baseline")
    print(json.dumps(baseline), flush=True)
    try:
        subprocess.run(
            [
                sys.executable,
                __file__,
                "--directory",
                str(args.directory),
                "--hot-offset-mib",
                str(args.hot_offset_mib),
                "--writer",
            ],
            check=True,
            timeout=120,
        )
        before = snapshot("writer_exited")
        print(json.dumps(before), flush=True)
        assert before["active_file"] > 1600 * MIB, before
        started = time.monotonic()
        subprocess.run(
            [
                str(args.test_binary.resolve()),
                "--test-filter",
                "live pressure recovers after file owner cleanup",
            ],
            env={**os.environ, "ANTFLY_PRESSURE_TEST_FILE": str(path.resolve())},
            check=True,
            timeout=60,
        )
        print(
            json.dumps(
                {
                    "stage": "cleanup_and_admission_timing",
                    "elapsed_seconds": time.monotonic() - started,
                }
            ),
            flush=True,
        )
        after = snapshot("reclaimed_and_admitted")
        print(json.dumps(after), flush=True)
        assert after["active_file"] < before["active_file"] // 4, after
        assert after["oom_kill"] == baseline["oom_kill"], after
    finally:
        path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
