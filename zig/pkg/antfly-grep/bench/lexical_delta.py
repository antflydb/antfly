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

"""Qualify changed-path lexical delivery against complete baseline results.

Warm filesystem/process data. Forced refresh is excluded from query timings.
The client applies each patch transactionally, binds compact file IDs to source
identities, and compares its materialized result with the baseline full delivery.
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


def path_key(value):
    return ("text", value["text"]) if "text" in value else ("bytes", value["bytes"])


def apply(view, rows):
    patch = next((r for r in rows if r["type"] == "workspace_lexical_delta"), None)
    result = (
        {
            path: {
                **value,
                "source": {**value["source"], "snapshot": patch["revision"]},
            }
            for path, value in view.items()
        }
        if patch
        else {}
    )
    if patch:
        for path in patch["removed"]:
            result.pop(path_key(path), None)
    files = {}
    for row in rows:
        if row["type"] == "begin":
            path = path_key(row["data"]["path"])
            files[row["file_id"]] = path
            source = dict(row["source"])
            source.pop("observed_at", None)
            result[path] = {"source": source, "rows": []}
        elif row["type"] in ("match", "context"):
            path = files[row["file_id"]]
            result[path]["rows"].append(
                {key: row[key] for key in ("type", "data", "probability", "scope")}
            )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("current", type=Path)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--files", type=int, default=10000)
    parser.add_argument("--iterations", type=int, default=9)
    args = parser.parse_args()
    if min(args.files, args.iterations) < 1:
        parser.error("sizes must be positive")
    binaries = {"current": args.current.resolve(), "baseline": args.baseline.resolve()}
    report = {
        "measurement": __doc__.strip(),
        "host": platform.platform(),
        "cpu_count": os.cpu_count(),
        "cgroup_cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip()
        if Path("/sys/fs/cgroup/cpu.max").exists()
        else None,
        "files": args.files,
        "binaries": {
            name: {"sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
            for name, binary in binaries.items()
        },
        "trials": [],
    }
    with tempfile.TemporaryDirectory(prefix="grep-delta-") as tmp:
        root = Path(tmp)
        (root / "src").mkdir()
        for i in range(args.files):
            (root / "src" / f"f{i:06}.txt").write_text(f"STOP {i}\n")
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
        }
        env.update(HOME=tmp, XDG_CONFIG_HOME=tmp)
        services, handles, tokens, views = {}, {}, {}, {}
        try:
            for name, binary in binaries.items():
                service = Client(binary, root, "src", [], env)
                services[name] = service
                _, rows = service.exchange("register", args=["STOP", "--threads=2"])
                handles[name] = rows[0]["query_handle"]
                _, rows = service.exchange(
                    "search",
                    revision=service.ready["revision"],
                    query_handle=handles[name],
                    delta=True,
                    if_result_token="0" * 64,
                )
                tokens[name] = next(
                    r["result_token"] for r in rows if r["type"] == "result_token"
                )
                views[name] = apply({}, rows)
            if views["current"] != views["baseline"]:
                raise RuntimeError("bootstrap result/source mismatch")
            for iteration in range(-1, args.iterations):
                path = root / "src/f000000.txt"
                path.write_text(f"STOP edit {iteration}\n")
                for service in services.values():
                    service.exchange("refresh", force_rescan=True)
                trial = {"load_average": os.getloadavg()}
                for name in binaries if iteration % 2 else reversed(binaries):
                    service = services[name]
                    value, rows = service.exchange(
                        "search",
                        revision=service.ready["revision"],
                        query_handle=handles[name],
                        delta=True,
                        if_result_token=tokens[name],
                    )
                    tokens[name] = next(
                        r["result_token"] for r in rows if r["type"] == "result_token"
                    )
                    views[name] = apply(views[name], rows)
                    trial[name] = value
                    if name == "current":
                        patch = next(
                            r for r in rows if r["type"] == "workspace_lexical_delta"
                        )
                        if patch["revision"] != service.ready["revision"]:
                            raise RuntimeError("patch snapshot mismatch")
                        stats = value["summary"]["stats"]
                        if (
                            stats["lexical_entries_visited"] != 1
                            or stats["lexical_files_evaluated"] != 1
                            or stats["files"] != args.files
                            or value["summary"]["requests"] != 0
                        ):
                            raise RuntimeError(
                                "delta did unrelated work or lost logical counts"
                            )
                if views["current"] != views["baseline"]:
                    raise RuntimeError(
                        "materialized delta differs from complete baseline"
                    )
                if iteration >= 0:
                    report["trials"].append(trial)
        finally:
            for service in services.values():
                service.close()
    report["median"] = {
        name: {
            key: statistics.median(t[name][key] for t in report["trials"])
            for key in (
                "seconds",
                "producer_seconds",
                "first_event_seconds",
                "response_bytes",
            )
        }
        for name in binaries
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["median"], indent=2))


if __name__ == "__main__":
    main()
