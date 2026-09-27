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

"""Detect workload failures hidden by successful VectorDBBench retries."""

from collections import Counter
from pathlib import Path


def inspect_workload_errors(root: Path):
    required = [root / "vdbbench-live.log", root / "antfly-initial.log"]
    paths = sorted(
        {*required, root / "vdbbench-framework.log", *root.glob("antfly-*.log")}
    )
    counts = Counter()
    examples = []
    for path in paths:
        if not path.is_file():
            continue
        with path.open(errors="replace") as stream:
            for number, line in enumerate(stream, 1):
                kind = None
                if "Antfly insert error:" in line:
                    kind = "client_insert_error"
                elif "Insert failed," in line:
                    kind = "client_insert_retry"
                elif "VectorDB search_embedding error:" in line:
                    kind = "client_query_error"
                elif (
                    "public table query read failed" in line
                    or "public table query execution failed" in line
                ):
                    kind = "server_query_error"
                elif "public table batch failed" in line:
                    kind = "server_batch_error"
                elif "VectorPayloadStorePoisoned" in line:
                    kind = "source_store_poisoned"
                elif path.name.startswith("antfly-") and "OutOfMemory" in line:
                    kind = "server_out_of_memory"
                if kind is not None:
                    counts[kind] += 1
                    if counts[kind] <= 4:
                        examples.append(
                            {
                                "kind": kind,
                                "log": path.name,
                                "line": number,
                                "message": line.strip()[:1000],
                            }
                        )
    missing = [p.name for p in required if not p.is_file()]
    return {
        "qualified": not counts and not missing,
        "counts": dict(counts),
        "missing_logs": missing,
        "examples": examples,
    }
