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

"""Print compact progress from persistent vector qualification receipts."""

import argparse
import json
from pathlib import Path
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    receipts = json.loads((args.root / "ab-runs.json").read_text())
    current = receipts[-1]
    name = f"{current['case']}-{current['pair']}-{current['mode']}"
    arm = args.root / name
    result = {
        "arms": [[r["pair"], r["mode"], r.get("exit_code")] for r in receipts],
        "current": name,
        "elapsed_s": round(time.time() - current["started_at"]),
    }
    phases = arm / "phases.jsonl"
    if phases.exists():
        lines = phases.read_text().splitlines()
        if lines:
            result["phase"] = json.loads(lines[-1])["phase"]
    log = arm / "vdbbench-live.log"
    if log.exists():
        for line in reversed(log.read_text(errors="replace").splitlines()):
            marker = "antfly_bench_status "
            if marker in line:
                status, _ = json.JSONDecoder().raw_decode(line.split(marker, 1)[1])
                index = status.get("index") or {}
                result["latest_ingest_status"] = {
                    "phase": status.get("phase"),
                    "published_docs": index.get("published_doc_count"),
                    "rebuilding": index.get("rebuilding"),
                }
                break
    samples = args.root / (name + "-resources.jsonl")
    if samples.exists():
        lines = samples.read_text().splitlines()
        if lines:
            try:
                sample = json.loads(lines[-1])
            except json.JSONDecodeError:
                sample = json.loads(lines[-2]) if len(lines) > 1 else {}
            result["resources"] = {
                k: sample[k]
                for k in ("pid", "phys_footprint_bytes", "logical_written_bytes")
                if k in sample
            }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
