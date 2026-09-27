#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
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

"""Bound one E2E invocation and stop its server descendants before returning."""

import argparse
import os
import sys

from zig_vopr_soak import run_process


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--timeout",
        type=int,
        default=int(os.environ.get("ANTFLY_E2E_CASE_TIMEOUT_SECONDS", "600")),
    )
    parser.add_argument("--grace", type=int, default=5)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not args.command:
        parser.error("a command is required")
    timeout = args.timeout
    if timeout <= 0 or args.grace < 0:
        raise ValueError("timeout must be positive and grace must be nonnegative")
    result = run_process(
        args.command,
        stdout=None,
        timeout=timeout,
        grace=args.grace,
        clean_descendants=True,
    )
    print(
        f"E2E process status={result.status} exit={result.returncode} "
        f"elapsed={result.elapsed_seconds:.2f}s",
        flush=True,
    )
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
