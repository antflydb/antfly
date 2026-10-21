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

"""Diagnostic workspace phase timings from a -Dgrep-workspace-profile=true binary.

Clock reads perturb short phases. CPU is main-thread CPU, not worker CPU. These
instrumented numbers locate work; use uninstrumented paired benchmarks for wins.
Stderr goes to a file, never an unread pipe. Refresh and verification are outside
search timing. No network or providers are used.
"""

import argparse
import hashlib
import json
import os
import platform
import statistics
import tempfile
from pathlib import Path
from registered import Client


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--files", type=int, default=1000)
    parser.add_argument("--iterations", type=int, default=101)
    args = parser.parse_args()
    if min(args.files, args.iterations) < 1:
        parser.error("positive sizes required")
    binary = args.binary.resolve()
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
    }
    with tempfile.TemporaryDirectory(prefix="grep-phases-") as tmp:
        root = Path(tmp)
        (root / "src").mkdir()
        for n in range(args.files):
            (root / "src" / f"f{n:06}.txt").write_text("STOP original\n")
        env.update(HOME=tmp, XDG_CONFIG_HOME=tmp)
        with (root / "phases.jsonl").open("wb") as log:
            client = Client(binary, root, "src", [], env, stderr=log)
            try:
                _, rows = client.exchange("register", args=["STOP", "--threads=2"])
                handle = rows[0]["query_handle"]
                _, rows = client.exchange(
                    "search",
                    revision=client.ready["revision"],
                    query_handle=handle,
                    delta=True,
                    if_result_token="0" * 64,
                )
                token = next(
                    r["result_token"] for r in rows if r["type"] == "result_token"
                )
                trials = []
                for n in range(args.iterations):
                    (root / "src" / "f000000.txt").write_text(f"STOP change {n}\n")
                    client.exchange("refresh", force_rescan=True)
                    result, rows = client.exchange(
                        "search",
                        revision=client.ready["revision"],
                        query_handle=handle,
                        delta=True,
                        if_result_token=token,
                    )
                    assert any(r["type"] == "workspace_lexical_delta" for r in rows)
                    assert result["summary"]["stats"]["lexical_entries_visited"] == 1
                    token = next(
                        r["result_token"] for r in rows if r["type"] == "result_token"
                    )
                    trials.append(result["seconds"])
            finally:
                client.close()
        frames = [
            json.loads(line)
            for line in (root / "phases.jsonl").read_bytes().splitlines()
        ]
    # First registered search populates the full index. Remaining samples are deltas.
    records = [record for frame in frames for record in frame["records"]]
    registered = [row for row in records if row.get("scope") == "registered"][1:]
    runs = [row for row in records if row.get("scope") == "run"][1:]
    if len(registered) != args.iterations:
        raise RuntimeError(
            "use a diagnostic binary built with -Dgrep-workspace-profile=true"
        )
    report = {
        "measurement": __doc__.strip(),
        "host": platform.platform(),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "files": args.files,
        "iterations": args.iterations,
        "request_seconds": trials,
        "registered_samples": registered,
        "run_samples": runs,
        "median": {
            p["name"]: {
                key: statistics.median(row["phases"][i][key] for row in registered)
                for key in ("wall_ns", "cpu_ns")
            }
            for i, p in enumerate(registered[0]["phases"])
        },
    }
    report["run_median"] = {
        phase["name"]: {
            key: statistics.median(row["phases"][i][key] for row in runs)
            for key in ("wall_ns", "cpu_ns")
        }
        for i, phase in enumerate(runs[0]["phases"])
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("median", "run_median")}, indent=2))


if __name__ == "__main__":
    main()
