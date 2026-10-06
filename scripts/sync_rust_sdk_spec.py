#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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

"""Keep the publishable Rust SDK's bundled spec identical to the public spec."""

from __future__ import annotations

import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def synchronize(root: Path, check: bool) -> bool:
    source = (root / "openapi.yaml").read_bytes()
    target = root / "rs/crates/sdk/openapi.yaml"
    if target.exists() and target.read_bytes() == source:
        return True
    if check:
        return False
    target.write_bytes(source)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if not synchronize(ROOT, args.check):
        raise SystemExit(
            "Rust SDK spec is stale; run python scripts/sync_rust_sdk_spec.py"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
