#!/usr/bin/env python3
"""Run a Zig build with a host-aware max-RSS budget and the fixed 0.16 runner."""

from __future__ import annotations

import argparse
import ast
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Sequence

from patch_zig_0_16_build_runner_maxrss import patch_build_runner


DEFAULT_MEMORY_FRACTION = 4 / 5
MAX_RSS_ENV = "ANTFLY_ZIG_MAX_RSS"
CGROUP_MEMORY_LIMITS = (
    Path("/sys/fs/cgroup/memory.max"),
    Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
)


def _positive_integer(value: str) -> int | None:
    try:
        parsed = int(value.strip())
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def detect_max_rss() -> int:
    override = os.environ.get(MAX_RSS_ENV)
    if override is not None:
        parsed = _positive_integer(override)
        if parsed is None:
            raise RuntimeError(f"{MAX_RSS_ENV} must be a positive byte count")
        return parsed

    return int(detect_memory_limit() * DEFAULT_MEMORY_FRACTION)


def detect_memory_limit() -> int:
    for limit_path in CGROUP_MEMORY_LIMITS:
        try:
            parsed = _positive_integer(limit_path.read_text(encoding="utf-8"))
        except OSError:
            continue
        if parsed is not None and parsed < 1 << 60:
            return parsed

    if sys.platform == "darwin":
        try:
            parsed = _positive_integer(
                subprocess.check_output(
                    ["sysctl", "-n", "hw.memsize"],
                    text=True,
                    stderr=subprocess.DEVNULL,
                )
            )
        except (OSError, subprocess.CalledProcessError):
            parsed = None
        if parsed is not None:
            return parsed

    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        physical_pages = os.sysconf("SC_PHYS_PAGES")
    except (OSError, ValueError):
        page_size = physical_pages = 0
    if page_size > 0 and physical_pages > 0:
        return page_size * physical_pages

    raise RuntimeError(
        f"could not detect available memory; set {MAX_RSS_ENV} to a byte count"
    )


def zig_lib_dir(zig: str) -> Path:
    output = subprocess.check_output([zig, "env"], text=True)
    match = re.search(
        r'^\s*\.lib_dir\s*=\s*("(?:[^"\\]|\\.)*"),\s*$',
        output,
        re.MULTILINE,
    )
    if match is None:
        raise RuntimeError("could not locate .lib_dir in `zig env` output")
    return Path(ast.literal_eval(match.group(1)))


def zig_version(zig: str) -> tuple[int, int, int]:
    output = subprocess.check_output([zig, "version"], text=True).strip()
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", output)
    if match is None:
        raise RuntimeError(f"could not parse `zig version` output: {output!r}")
    return tuple(int(component) for component in match.groups())


def prepare_build_runner(zig: str, destination: Path) -> Path | None:
    source = zig_lib_dir(zig) / "compiler" / "build_runner.zig"
    try:
        patch_build_runner(source, destination)
    except (OSError, RuntimeError):
        # A newer Zig may reorganize the runner after incorporating the fix.
        # In that case retain --maxrss and let the toolchain use its stock
        # runner. The known affected release remains fail-closed if its exact
        # loop cannot be recognized.
        if zig_version(zig) <= (0, 16, 0):
            raise
        return None
    return destination


def has_build_option(arguments: Sequence[str], option: str) -> bool:
    for argument in arguments:
        if argument == "--":
            return False
        if argument == option or argument.startswith(f"{option}="):
            return True
    return False


def build_command(
    zig: str,
    build_arguments: Sequence[str],
    patched_runner: Path | None,
    max_rss: int | None,
) -> list[str]:
    try:
        option_end = build_arguments.index("--")
    except ValueError:
        option_end = len(build_arguments)
    command = [zig, *build_arguments[:option_end]]
    if patched_runner is not None and not has_build_option(
        build_arguments,
        "--build-runner",
    ):
        command.extend(("--build-runner", str(patched_runner)))
    if max_rss is not None and not has_build_option(build_arguments, "--maxrss"):
        command.extend(("--maxrss", str(max_rss)))
    command.extend(build_arguments[option_end:])
    return command


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zig", default="zig", help="Zig executable")
    parser.add_argument("build_arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    build_arguments = args.build_arguments
    if build_arguments[:1] == ["--"]:
        build_arguments = build_arguments[1:]
    if not build_arguments:
        parser.error("a Zig command is required after --")

    needs_runner = not has_build_option(build_arguments, "--build-runner")
    needs_budget = not has_build_option(build_arguments, "--maxrss")
    max_rss = detect_max_rss() if needs_budget else None

    if not needs_runner:
        return subprocess.run(
            build_command(args.zig, build_arguments, None, max_rss),
            check=False,
        ).returncode

    with tempfile.TemporaryDirectory(prefix="antfly-zig-build-runner-") as temporary:
        patched_runner = prepare_build_runner(
            args.zig,
            Path(temporary) / "build_runner.zig",
        )
        return subprocess.run(
            build_command(args.zig, build_arguments, patched_runner, max_rss),
            check=False,
        ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
