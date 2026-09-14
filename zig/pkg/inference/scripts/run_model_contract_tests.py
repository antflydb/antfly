#!/usr/bin/env python3
"""Run one or more model-contract unittest suites from a single CI entrypoint."""

from __future__ import annotations

import argparse
import sys
import unittest
from pathlib import Path


SCRIPT_ROOT = Path(__file__).resolve().parent
KNOWN_SUITES = ("qwen3_embedding", "qwen3vl")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "suites",
        nargs="+",
        choices=KNOWN_SUITES,
        help="model contract suites to run",
    )
    return parser.parse_args()


def load_suites(names: list[str]) -> unittest.TestSuite:
    loader = unittest.TestLoader()
    combined = unittest.TestSuite()
    for name in names:
        suite_dir = SCRIPT_ROOT / name
        combined.addTests(
            loader.discover(
                start_dir=str(suite_dir),
                pattern="test_*.py",
                top_level_dir=str(suite_dir),
            )
        )
    return combined


def main() -> int:
    args = parse_args()
    result = unittest.TextTestRunner(verbosity=2).run(load_suites(args.suites))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
