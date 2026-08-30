#!/usr/bin/env python3
"""Verify every repository Zig pin agrees with zig/toolchain.env."""

from __future__ import annotations

import hashlib
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
TOOLCHAIN_PATH = ROOT / "zig" / "toolchain.env"
REQUIRED_KEYS = {
    "ZIG_VERSION",
    "ZIG_MINIMUM_VERSION",
    "ZIG_X86_64_LINUX_SHA256",
    "ZIG_AARCH64_LINUX_SHA256",
}
FULL_NIGHTLY_RE = re.compile(r"\b0\.\d+\.\d+-dev\.\d+\+[0-9a-f]+\b")
MINIMUM_RE = re.compile(r'\.minimum_zig_version\s*=\s*"([^"]+)"')
ZIG_HASH_RE = re.compile(
    r"\b(ZIG_(?:X86_64|AARCH64)_LINUX_SHA256)\s*[:=]\s*[\"']?([0-9a-f]{64})"
)
HISTORICAL_NIGHTLY_FILES = {"zig/lib/httpx/src/tls/client_compat.zig"}


def load_toolchain() -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(TOOLCHAIN_PATH.read_text().splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"([A-Z][A-Z0-9_]*)=([^\s]+)", line)
        if not match:
            raise ValueError(f"{TOOLCHAIN_PATH}:{line_number}: invalid assignment")
        key, value = match.groups()
        if key in values:
            raise ValueError(f"{TOOLCHAIN_PATH}:{line_number}: duplicate {key}")
        values[key] = value

    missing = REQUIRED_KEYS - values.keys()
    extra = values.keys() - REQUIRED_KEYS
    if missing or extra:
        raise ValueError(f"toolchain keys mismatch: missing={sorted(missing)}, extra={sorted(extra)}")
    if values["ZIG_MINIMUM_VERSION"] != values["ZIG_VERSION"].split("+", 1)[0]:
        raise ValueError("ZIG_MINIMUM_VERSION must be ZIG_VERSION without the commit suffix")
    for key in ("ZIG_X86_64_LINUX_SHA256", "ZIG_AARCH64_LINUX_SHA256"):
        if len(values[key]) != hashlib.sha256().digest_size * 2:
            raise ValueError(f"{key} must be a lowercase SHA-256 digest")
    return values


def tracked_files() -> list[Path]:
    output = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT)
    return [ROOT / item.decode() for item in output.split(b"\0") if item]


def main() -> int:
    try:
        toolchain = load_toolchain()
    except (OSError, ValueError) as error:
        print(f"zig toolchain verification failed: {error}", file=sys.stderr)
        return 1

    expected_version = toolchain["ZIG_VERSION"]
    expected_minimum = toolchain["ZIG_MINIMUM_VERSION"]
    errors: list[str] = []
    direct_download_files = 0
    direct_download_marker = "https://ziglang.org/builds/" + "zig-"

    for path in tracked_files():
        relative = path.relative_to(ROOT).as_posix()
        try:
            text = path.read_text()
        except (OSError, UnicodeDecodeError):
            continue

        if relative not in HISTORICAL_NIGHTLY_FILES:
            for match in FULL_NIGHTLY_RE.finditer(text):
                if match.group() != expected_version:
                    line = text.count("\n", 0, match.start()) + 1
                    errors.append(f"{relative}:{line}: stale Zig nightly {match.group()}")

        for match in MINIMUM_RE.finditer(text):
            if match.group(1) != expected_minimum:
                line = text.count("\n", 0, match.start()) + 1
                errors.append(
                    f"{relative}:{line}: minimum Zig {match.group(1)} != {expected_minimum}"
                )

        for match in ZIG_HASH_RE.finditer(text):
            key, digest = match.groups()
            if digest != toolchain[key]:
                line = text.count("\n", 0, match.start()) + 1
                errors.append(f"{relative}:{line}: {key} does not match zig/toolchain.env")

        if direct_download_marker in text:
            direct_download_files += 1
            if "sha256sum" not in text:
                errors.append(f"{relative}: direct Zig download is not checksum verified")

    if direct_download_files == 0:
        errors.append("no direct Zig download files were found")

    if errors:
        print("zig toolchain verification failed:", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        return 1

    print(
        f"Zig toolchain pins agree: {expected_version}; "
        f"{direct_download_files} download files checksum-verified"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
