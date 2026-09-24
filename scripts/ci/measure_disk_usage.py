#!/usr/bin/env python3
"""Record peak filesystem use while running a CI command."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import time


def usage(path: Path) -> dict[str, int]:
    stat = os.statvfs(path)
    return {
        "capacity_bytes": stat.f_blocks * stat.f_frsize,
        "used_bytes": (stat.f_blocks - stat.f_bfree) * stat.f_frsize,
        "available_bytes": stat.f_bavail * stat.f_frsize,
    }


def measure(command: list[str], paths: list[Path], interval: float) -> tuple[int, dict]:
    samples = {str(path): {"start": usage(path)} for path in paths}
    for value in samples.values():
        value["peak_used_bytes"] = value["start"]["used_bytes"]
        value["minimum_available_bytes"] = value["start"]["available_bytes"]
    started = time.monotonic()
    process = subprocess.Popen(command)
    try:
        while True:
            for path in paths:
                current = usage(path)
                value = samples[str(path)]
                value["peak_used_bytes"] = max(
                    value["peak_used_bytes"], current["used_bytes"]
                )
                value["minimum_available_bytes"] = min(
                    value["minimum_available_bytes"], current["available_bytes"]
                )
            try:
                status = process.wait(timeout=interval)
                break
            except subprocess.TimeoutExpired:
                continue
    except BaseException:
        process.terminate()
        process.wait()
        raise
    for path in paths:
        samples[str(path)]["finish"] = usage(path)
    return status, {
        "elapsed_seconds": time.monotonic() - started,
        "command": command,
        "filesystems": samples,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--path", type=Path, action="append", required=True)
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if (
        args.interval <= 0
        or not args.command
        or args.command[0] != "--"
        or len(args.command) < 2
    ):
        parser.error("provide a positive interval and a command after --")
    paths = [path for path in args.path if path.exists()]
    if not paths:
        parser.error("at least one monitored path must exist")
    status, report = measure(args.command[1:], paths, args.interval)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps({"exit_code": status, "disk_usage": report["filesystems"]}),
        flush=True,
    )
    return status


if __name__ == "__main__":
    raise SystemExit(main())
