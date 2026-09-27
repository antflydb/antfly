#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
