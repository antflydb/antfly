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

"""Verify shared fixture payloads; discard identical regenerated aliases with --apply."""

import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
MANIFEST = ROOT / "zig/lib/video/testdata/shared-payloads.json"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    aliases = []
    groups = json.loads(MANIFEST.read_text())["groups"]
    # Validate the entire set before removing any regenerated output. A changed
    # oracle needs deliberate fixture/test updates, never silent deduplication.
    for group in groups:
        canonical = ROOT / group["path"]
        if hashlib.sha256(canonical.read_bytes()).hexdigest() != group["sha256"]:
            parser.error("shared payload changed: " + group["path"])
        for name in group["aliases"]:
            path = ROOT / name
            if not path.exists():
                continue
            if hashlib.sha256(path.read_bytes()).hexdigest() != group["sha256"]:
                parser.error("regenerated fixture differs from shared payload: " + name)
            aliases.append(path)
    if aliases and not args.apply:
        parser.error("duplicate payloads regenerated; run with --apply to canonicalize")
    for path in aliases:
        path.unlink()
    print(
        f"Verified {len(groups)} shared payloads; removed {len(aliases)} regenerated copies"
    )


if __name__ == "__main__":
    main()
