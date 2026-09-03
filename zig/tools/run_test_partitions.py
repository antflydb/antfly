#!/usr/bin/env python3
"""Run one compiled Zig test artifact in a filtered lane and its complement."""

from __future__ import annotations

import argparse
import subprocess
import sys
import threading
from pathlib import Path
from typing import Sequence


def build_commands(
    executable: Path,
    partition_filters: Sequence[str],
    common_skip_filters: Sequence[str],
    runtime_args: Sequence[str],
) -> tuple[list[str], list[str]]:
    common = [str(executable)]
    for test_filter in common_skip_filters:
        common.extend(("--skip-test-filter", test_filter))

    partition = [str(executable)]
    for test_filter in partition_filters:
        partition.extend(("--test-filter", test_filter))
    partition.extend(common[1:])
    partition.extend(runtime_args)

    complement = [str(executable), "--test-filter", "storage."]
    complement.extend(common[1:])
    for test_filter in partition_filters:
        complement.extend(("--skip-test-filter", test_filter))
    complement.extend(runtime_args)
    return partition, complement


def stream_output(
    label: str, process: subprocess.Popen[str], lock: threading.Lock
) -> None:
    assert process.stdout is not None
    for line in process.stdout:
        with lock:
            sys.stderr.write(f"[{label}] {line}")
            sys.stderr.flush()


def run_partitions(commands: Sequence[tuple[str, Sequence[str]]]) -> int:
    processes: list[tuple[str, subprocess.Popen[str]]] = []
    try:
        for label, command in commands:
            processes.append(
                (
                    label,
                    subprocess.Popen(
                        command,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        errors="replace",
                    ),
                )
            )
    except BaseException:
        for _, process in processes:
            process.terminate()
        for _, process in processes:
            process.wait()
        raise

    lock = threading.Lock()
    readers = [
        threading.Thread(target=stream_output, args=(label, process, lock), daemon=True)
        for label, process in processes
    ]
    for reader in readers:
        reader.start()

    return_codes = [process.wait() for _, process in processes]
    for reader in readers:
        reader.join()
    for _, process in processes:
        assert process.stdout is not None
        process.stdout.close()
    for return_code in return_codes:
        if return_code != 0:
            return return_code if return_code > 0 else 128 - return_code
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--executable", type=Path, required=True)
    parser.add_argument("--partition-filter", action="append", default=[])
    parser.add_argument("--common-skip-filter", action="append", default=[])
    args, runtime_args = parser.parse_known_args(argv)
    if not args.partition_filter:
        parser.error("at least one --partition-filter is required")
    if runtime_args[:1] == ["--"]:
        runtime_args = runtime_args[1:]

    partition, complement = build_commands(
        args.executable,
        args.partition_filter,
        args.common_skip_filter,
        runtime_args,
    )
    return run_partitions(
        (
            ("db-core-category", partition),
            ("db-core-complement", complement),
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
