#!/usr/bin/env python3
"""Run one compiled Zig test artifact in a filtered lane and its complement."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import BinaryIO, Sequence


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
    label: str,
    process: subprocess.Popen[bytes],
    lock: threading.Lock,
    log: BinaryIO | None,
) -> None:
    assert process.stdout is not None
    # Keep the last in-flight test visible even without a trailing newline.
    # Zig buffers checked Run output until the entire partition exits.
    try:
        while chunk := process.stdout.read1(64 * 1024):
            if log is not None:
                log.write(chunk)
            with lock:
                sys.stderr.write(f"[{label}] {chunk.decode(errors='replace')}")
                sys.stderr.flush()
    finally:
        if log is not None:
            log.close()


def run_partitions(
    commands: Sequence[tuple[str, Sequence[str]]],
    log_directory: Path | None = None,
) -> int:
    if log_directory is not None:
        log_directory.mkdir(parents=True, exist_ok=True)
    processes: list[tuple[str, subprocess.Popen[bytes]]] = []
    logs: dict[int, BinaryIO] = {}
    try:
        for label, command in commands:
            processes.append(
                (
                    label,
                    subprocess.Popen(
                        command,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                    ),
                )
            )
            process = processes[-1][1]
            if log_directory is not None:
                logs[process.pid] = (log_directory / f"{label}-{process.pid}.log").open(
                    "wb", buffering=0
                )
    except BaseException:
        for _, process in processes:
            process.terminate()
        for _, process in processes:
            process.wait()
        for log in logs.values():
            log.close()
        raise

    lock = threading.Lock()
    readers = [
        threading.Thread(
            target=stream_output,
            args=(label, process, lock, logs.get(process.pid)),
            daemon=True,
        )
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
        ),
        log_directory=(
            Path(os.environ["ANTFLY_TEST_LOG_DIR"])
            if os.environ.get("ANTFLY_TEST_LOG_DIR")
            else None
        ),
    )


if __name__ == "__main__":
    raise SystemExit(main())
