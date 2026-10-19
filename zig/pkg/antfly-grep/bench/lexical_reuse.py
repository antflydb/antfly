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

"""Alternating lexical full delivery, conditional acknowledgement and single edits.

Warm process/filesystem data; capture/refresh is outside search timing. Baselines
without lexical tokens deliver ordinary results for an unknown valid token.
Every full/edit delivery is compared by exact match/context data, outside timing.
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


def signature(rows):
    return sorted(
        json.dumps(row["data"], sort_keys=True)
        for row in rows
        if row["type"] in ("match", "context")
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("current", type=Path)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--files", type=int, default=64)
    parser.add_argument("--bytes-per-file", type=int, default=1024 * 1024)
    parser.add_argument("--iterations", type=int, default=9)
    args = parser.parse_args()
    if min(args.files, args.bytes_per_file, args.iterations) < 1:
        parser.error("sizes and iterations must be positive")
    binaries = {"current": args.current.resolve(), "baseline": args.baseline.resolve()}
    report = {
        "measurement": __doc__.strip(),
        "host": platform.platform(),
        "cpu_count": os.cpu_count(),
        "cgroup_cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip()
        if Path("/sys/fs/cgroup/cpu.max").exists()
        else None,
        "files": args.files,
        "bytes_per_file": args.bytes_per_file,
        "binaries": {
            name: {"sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
            for name, binary in binaries.items()
        },
        "trials": [],
    }
    with tempfile.TemporaryDirectory(prefix="grep-lexical-") as tmp:
        root = Path(tmp)
        (root / "src").mkdir()
        content = b"ordinary text here\n" * (args.bytes_per_file // 19) + b"STOP\n"
        for i in range(args.files):
            (root / "src" / f"f{i:05}.txt").write_bytes(content)
        env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
        }
        env.update(HOME=tmp, XDG_CONFIG_HOME=tmp)
        services, handles, tokens = {}, {}, {}
        try:
            for name, binary in binaries.items():
                service = Client(binary, root, "src", [], env)
                services[name] = service
                _, rows = service.exchange(
                    "register", args=["S(?:T|X)+OP", "--threads=2"]
                )
                handles[name] = rows[0]["query_handle"]
                _, rows = service.exchange(
                    "search",
                    revision=service.ready["revision"],
                    query_handle=handles[name],
                )
                tokens[name] = next(
                    (r["result_token"] for r in rows if r["type"] == "result_token"),
                    "0" * 64,
                )
            for iteration in range(-1, args.iterations):
                trial = {"load_average": os.getloadavg()}
                for mode in ("full", "conditional", "edit"):
                    trial[mode] = {}
                    evidence = None
                    if mode == "edit":
                        (root / "src/f00000.txt").write_bytes(
                            content + f"STOP {iteration}\n".encode()
                        )
                        for service in services.values():
                            service.exchange("refresh", force_rescan=True)
                    for name in binaries if iteration % 2 else reversed(binaries):
                        service = services[name]
                        fields = (
                            {"if_result_token": tokens[name]}
                            if mode == "conditional"
                            else {}
                        )
                        value, rows = service.exchange(
                            "search",
                            revision=service.ready["revision"],
                            query_handle=handles[name],
                            **fields,
                        )
                        if value["summary"]["requests"] != 0:
                            raise RuntimeError("lexical search made provider requests")
                        tokens[name] = next(
                            (
                                r["result_token"]
                                for r in rows
                                if r["type"] == "result_token"
                            ),
                            tokens[name],
                        )
                        unchanged = any(
                            r["type"] == "workspace_unchanged" for r in rows
                        )
                        value["unchanged"] = unchanged
                        if name == "current":
                            stats = value["summary"]["stats"]
                            if mode == "conditional":
                                if (
                                    not unchanged
                                    or stats["lexical_files_evaluated"] != 0
                                ):
                                    raise RuntimeError(
                                        "certificate performed lexical work"
                                    )
                            elif mode == "edit" and len(content) >= 4096:
                                if (
                                    stats["lexical_files_evaluated"] < 1
                                    or stats["lexical_cache_hits"]
                                    + stats["lexical_files_evaluated"]
                                    != args.files
                                ):
                                    raise RuntimeError(
                                        "single edit lost file accounting"
                                    )
                        if not unchanged:
                            current = signature(rows)
                            if evidence is not None and evidence != current:
                                raise RuntimeError("full/edit evidence mismatch")
                            evidence = current
                        trial[mode][name] = value
                if iteration >= 0:
                    report["trials"].append(trial)
        finally:
            for service in services.values():
                service.close()
    report["median"] = {
        mode: {
            name: {
                field: statistics.median(t[mode][name][field] for t in report["trials"])
                for field in ("seconds", "producer_seconds", "response_bytes")
            }
            for name in binaries
        }
        for mode in ("full", "conditional", "edit")
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
