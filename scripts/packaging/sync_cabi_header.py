#!/usr/bin/env python3
"""Sync the vendored Go C ABI header from the canonical Zig header."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CANONICAL_HEADER = REPO_ROOT / "zig" / "pkg" / "antfly" / "include" / "antfly.h"
GO_HEADER = REPO_ROOT / "go" / "pkg" / "antflylite" / "include" / "antfly.h"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the Go vendored header is current without rewriting it",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    canonical = CANONICAL_HEADER.read_bytes()
    current = GO_HEADER.read_bytes() if GO_HEADER.exists() else None

    if args.check:
        if current == canonical:
            return 0
        print(
            f"{GO_HEADER.relative_to(REPO_ROOT)} is out of date; run "
            "python scripts/packaging/sync_cabi_header.py",
            file=sys.stderr,
        )
        return 1

    GO_HEADER.parent.mkdir(parents=True, exist_ok=True)
    if current != canonical:
        GO_HEADER.write_bytes(canonical)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
