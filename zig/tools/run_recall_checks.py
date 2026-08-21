#!/usr/bin/env python3
"""Run storage-backed recall and per-metric harness checks concurrently."""

from __future__ import annotations

import argparse
import subprocess
import sys
import threading
from pathlib import Path
from typing import Sequence


METRICS = ("l2_squared", "inner_product", "cosine")


def build_commands(
    test_executable: Path,
    harness_executable: Path,
    dataset_dir: Path,
) -> tuple[tuple[str, list[str]], ...]:
    commands: list[tuple[str, list[str]]] = [
        (
            "storage-hbc",
            [str(test_executable), "--test-filter", "HBC recall"],
        )
    ]
    for metric in METRICS:
        commands.append(
            (
                f"harness-{metric}",
                [
                    str(harness_executable),
                    "--dataset-dir",
                    str(dataset_dir),
                    "--metric",
                    metric,
                ],
            )
        )
    return tuple(commands)


def stream_output(label: str, process: subprocess.Popen[str], lock: threading.Lock) -> None:
    assert process.stdout is not None
    for line in process.stdout:
        with lock:
            sys.stderr.write(f"[{label}] {line}")
            sys.stderr.flush()


def run_checks(commands: Sequence[tuple[str, Sequence[str]]]) -> int:
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
    parser.add_argument("--test-executable", type=Path, required=True)
    parser.add_argument("--harness-executable", type=Path, required=True)
    parser.add_argument("--dataset-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    return run_checks(
        build_commands(
            args.test_executable,
            args.harness_executable,
            args.dataset_dir,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
