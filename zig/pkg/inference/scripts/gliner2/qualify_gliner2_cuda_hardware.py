#!/usr/bin/env python3
"""Run one fail-closed GLiNER2 CUDA FP32 hardware qualification lane."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from typing import Any


CONTRACT = "gliner2_cuda_hardware_qualification_lane/v1"
PRECISION = "fp32"
ARCHITECTURES = {
    "8.0": ("sm80", "A100"),
    "8.9": ("sm89", "L4"),
    "9.0": ("sm90", "H100"),
}
FULL_PARITY_FILTERS = (
    "gradient",
    "attention",
    "compiled-update",
    "checkpoint-resume",
    "training-primitives",
)
SOURCE_ROOTS = (
    "build/finetune",
    "src/architectures",
    "src/finetune",
    "src/graph",
    "src/ops",
    "../../lib/ml/src",
)
SOURCE_SUFFIXES = frozenset({".zig", ".cu", ".ptx", ".fatbin", ".cubin"})
FIXED_SOURCE_FILES = (
    "../../lib/ml/build.zig",
    "build.zig",
    "scripts/regen-cuda-artifacts.sh",
)
EXPECTED_LOG_NAMES = (
    "embedded-artifacts.log",
    "full-parity.log",  # Removed monolithic runner; clean evidence from older lanes.
    *(f"full-parity-{name}.log" for name in FULL_PARITY_FILTERS),
    "memcheck.log",
    "initcheck.log",
    "racecheck.log",
)
TEST_EXECUTABLE_PATTERN = re.compile(
    r"(?m)^(?P<path>(?:\./|/)?\S*?/o/[0-9a-f]+/test) "
    r"--cache-dir=.*--listen=-\s*$"
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def qualification_source_files(package_dir: Path) -> tuple[str, ...]:
    files = set(FIXED_SOURCE_FILES)
    for relative_root in SOURCE_ROOTS:
        root = package_dir / relative_root
        if not root.is_dir():
            raise ValueError(f"qualification source directory is missing: {root}")
        for path in root.rglob("*"):
            if path.is_file() and path.suffix in SOURCE_SUFFIXES:
                files.add(Path(os.path.relpath(path, package_dir)).as_posix())
    scripts = package_dir / "scripts"
    for path in scripts.glob("*gliner2*"):
        if path.is_file() and path.suffix in {".py", ".sh"}:
            files.add(path.relative_to(package_dir).as_posix())
    return tuple(sorted(files))


def source_fingerprint(package_dir: Path) -> str:
    digest = hashlib.sha256()
    for relative in qualification_source_files(package_dir):
        path = package_dir / relative
        if not path.is_file():
            raise ValueError(f"qualification source is missing: {path}")
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(bytes.fromhex(sha256_file(path).removeprefix("sha256:")))
    return f"sha256:{digest.hexdigest()}"


def run_command(
    name: str,
    argv: list[str],
    *,
    cwd: Path,
    log_dir: Path,
    env: dict[str, str] | None = None,
    timeout_seconds: int,
) -> dict[str, Any]:
    log_path = log_dir / f"{name}.log"
    try:
        completed = subprocess.run(
            argv,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
            check=False,
            timeout=timeout_seconds,
        )
        output = completed.stdout
        returncode = completed.returncode
        timed_out = False
    except subprocess.TimeoutExpired as exc:
        output = str(exc.stdout or "") + str(exc.stderr or "")
        returncode = 124
        timed_out = True
    log_path.write_text(output, encoding="utf-8")
    return {
        "pass": returncode == 0,
        "returncode": returncode,
        "timed_out": timed_out,
        "argv": argv,
        # Reports and their log directories are uploaded as siblings. Keep the
        # reference relocatable so the matrix aggregator can re-hash evidence
        # after downloading it on another runner.
        "log": f"{log_dir.name}/{log_path.name}",
        "log_sha256": sha256_file(log_path),
    }


def run_filtered_suite(
    argv: list[str],
    *,
    cwd: Path,
    log_dir: Path,
    base_env: dict[str, str],
    timeout_seconds: int,
) -> dict[str, Any]:
    subchecks: dict[str, dict[str, Any]] = {}
    for test_filter in FULL_PARITY_FILTERS:
        env = dict(base_env)
        env["TERMITE_CUDA_PARITY_TEST_FILTER"] = test_filter
        subchecks[test_filter] = run_command(
            f"full-parity-{test_filter}",
            argv,
            cwd=cwd,
            log_dir=log_dir,
            env=env,
            timeout_seconds=timeout_seconds,
        )
    failures = [
        name for name, result in subchecks.items() if result["pass"] is not True
    ]
    return {
        "pass": not failures,
        "returncode": 0
        if not failures
        else next(int(subchecks[name]["returncode"]) for name in failures),
        "timed_out": any(result["timed_out"] is True for result in subchecks.values()),
        "argv": argv,
        "failures": failures,
        "subchecks": subchecks,
    }


def zig_local_cache_object_dir(package_dir: Path, env: dict[str, str]) -> Path:
    configured = env.get("ZIG_LOCAL_CACHE_DIR")
    cache_dir = (
        Path(configured).expanduser() if configured else package_dir / ".zig-cache"
    )
    if not cache_dir.is_absolute():
        cache_dir = package_dir / cache_dir
    return (cache_dir / "o").resolve()


def standalone_test_executable(
    package_dir: Path,
    log_dir: Path,
    env: dict[str, str] | None = None,
) -> Path:
    candidates: set[Path] = set()
    cache_root = zig_local_cache_object_dir(package_dir, env or os.environ)
    for test_filter in FULL_PARITY_FILTERS:
        log_path = log_dir / f"full-parity-{test_filter}.log"
        try:
            output = log_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ValueError(
                f"cannot read parity log needed for sanitizer target: {exc}"
            ) from exc
        for match in TEST_EXECUTABLE_PATTERN.finditer(output):
            candidate_path = Path(match.group("path"))
            candidate = (
                candidate_path
                if candidate_path.is_absolute()
                else package_dir / candidate_path
            ).resolve()
            try:
                candidate.relative_to(cache_root)
            except ValueError as exc:
                raise ValueError(
                    f"sanitizer test executable escapes Zig cache: {candidate}"
                ) from exc
            if candidate.is_file():
                candidates.add(candidate)
    if len(candidates) != 1:
        raise ValueError(
            "qualification requires exactly one verbose-built standalone test executable, "
            f"found {len(candidates)}"
        )
    return next(iter(candidates))


def gpu_identity() -> dict[str, str]:
    command = [
        "nvidia-smi",
        "--query-gpu=name,compute_cap,driver_version,memory.total",
        "--format=csv,noheader,nounits",
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        raise ValueError(f"nvidia-smi failed: {completed.stderr.strip()}")
    rows = [row.strip() for row in completed.stdout.splitlines() if row.strip()]
    if len(rows) != 1:
        raise ValueError(
            f"qualification requires exactly one visible GPU, found {len(rows)}"
        )
    fields = [field.strip() for field in rows[0].split(",")]
    if len(fields) != 4:
        raise ValueError(f"unexpected nvidia-smi output: {rows[0]}")
    name, capability, driver, memory_mib = fields
    try:
        architecture, family = ARCHITECTURES[capability]
    except KeyError as exc:
        raise ValueError(
            f"unsupported qualification compute capability: {capability}"
        ) from exc
    if family.lower() not in name.lower():
        raise ValueError(
            f"GPU {name!r} does not match required {family} identity for {architecture}"
        )
    return {
        "name": name,
        "family": family,
        "compute_capability": capability,
        "architecture": architecture,
        "driver_version": driver,
        "memory_mib": memory_mib,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--zig", default="zig")
    parser.add_argument(
        "--cuda-artifacts", choices=("fatbin", "portable", "sm89"), default="fatbin"
    )
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    package_dir = script_dir.parents[1]
    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    log_dir = output.parent / f"{output.stem}-logs"
    try:
        # A failed rerun must never leave a current-looking report from an
        # earlier invocation. Remove only files owned by this exact output
        # contract; do not recursively delete a caller-controlled directory.
        output.unlink(missing_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)
        for name in EXPECTED_LOG_NAMES:
            (log_dir / name).unlink(missing_ok=True)
    except OSError as exc:
        parser.error(f"cannot prepare qualification output: {exc}")

    failures: list[str] = []
    try:
        gpu = gpu_identity()
        fingerprint = source_fingerprint(package_dir)
    except ValueError as exc:
        parser.error(str(exc))

    sanitizer = shutil.which("compute-sanitizer")
    if sanitizer is None:
        parser.error("compute-sanitizer is required for CUDA hardware qualification")

    zig_build = [
        args.zig,
        "build",
        "test-gliner2-backend-grad-parity",
        "-Dcuda=true",
        f"-Dcuda-artifacts={args.cuda_artifacts}",
        "-Doptimize=ReleaseSafe",
        "--verbose",
    ]
    required_env = dict(os.environ)
    required_env.pop("TERMITE_CUDA_PARITY_TEST_FILTER", None)
    required_env["TERMITE_REQUIRE_CUDA_TESTS"] = "1"

    checks: dict[str, dict[str, Any]] = {}
    checks["embedded_artifacts"] = run_command(
        "embedded-artifacts",
        [str(script_dir.parent / "regen-cuda-artifacts.sh"), "--check", "--all"],
        cwd=package_dir,
        log_dir=log_dir,
        env=required_env,
        timeout_seconds=args.timeout_seconds,
    )
    checks["full_parity"] = run_filtered_suite(
        zig_build,
        cwd=package_dir,
        log_dir=log_dir,
        base_env=required_env,
        timeout_seconds=args.timeout_seconds,
    )
    try:
        sanitizer_test_executable = standalone_test_executable(
            package_dir, log_dir, required_env
        )
    except ValueError as exc:
        parser.error(str(exc))

    sanitizer_specs = {
        "memcheck": ("gradient-full", ["--leak-check", "full"]),
        "initcheck": ("attention", []),
        "racecheck": ("attention", []),
    }
    for tool, (test_filter, extra) in sanitizer_specs.items():
        tool_env = dict(required_env)
        tool_env["TERMITE_CUDA_PARITY_TEST_FILTER"] = test_filter
        checks[tool] = run_command(
            tool,
            [
                sanitizer,
                "--tool",
                tool,
                "--target-processes",
                "all",
                "--error-exitcode",
                "99",
                *extra,
                str(sanitizer_test_executable),
            ],
            cwd=package_dir,
            log_dir=log_dir,
            env=tool_env,
            timeout_seconds=args.timeout_seconds,
        )

    failures.extend(name for name, check in checks.items() if check["pass"] is not True)
    report = {
        "contract": CONTRACT,
        "pass": not failures,
        "failures": failures,
        "training_precision": PRECISION,
        "optimizer_state_precision": PRECISION,
        "gpu": gpu,
        "cuda_artifacts": args.cuda_artifacts,
        "source_fingerprint_sha256": fingerprint,
        "source_files": list(qualification_source_files(package_dir)),
        "checks": checks,
    }
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(output)
    print(f"CUDA qualification lane report: {output}")
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
