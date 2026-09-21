#!/usr/bin/env python3
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
