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

"""AA/AB/BA model smoke comparison using the unified-execution benchmark.

Uses immutable existing model fixtures; never downloads models. Keeps every
process log/output, first-output setup, latency and provider memory/counters.
Fixed before samples: 5 warmups, 31 measured forwards, p50<=1.05/p95<=1.10.
This complements focused kernel/graph qualification; it does not prove that a
particular fused route was selected unless the recorded counters establish it.
"""

import argparse
import hashlib
import json
import subprocess
from pathlib import Path


def digest(p):
    h = hashlib.sha256()
    with p.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("baseline", "candidate", "model-dir", "results"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--backend", choices=("metal", "cuda", "native"), required=True)
    args = parser.parse_args()
    args.results.mkdir(parents=True, exist_ok=True)
    binaries = {"A": args.baseline.resolve(), "B": args.candidate.resolve()}
    record = {
        "backend": args.backend,
        "protocol": {
            "warmup": 5,
            "measured": 31,
            "orders": ["AA", "AB", "BA"],
            "limits": {"p50": 1.05, "p95": 1.10},
        },
        "binaries": {
            k: {"path": str(v), "sha256": digest(v)} for k, v in binaries.items()
        },
        "fixtures": {
            str(p.relative_to(args.model_dir)): digest(p)
            for p in sorted(args.model_dir.rglob("*"))
            if p.is_file() and not p.name.endswith(".antfly-sha256-cache")
        },
        "cases": [],
    }
    for batch, sequence in ((1, 16), (1, 128)):
        label = f"b{batch}-s{sequence}"
        case = {"case": label, "arms": []}
        reference = None
        record["cases"].append(case)
        for order in ("AA", "AB", "BA"):
            pair = []
            for side, arm in enumerate(order):
                name = f"{label}-{order}-{side}-{arm}"
                log = args.results / (name + ".log")
                out = args.results / (name + ".f32")
                command = [
                    str(binaries[arm]),
                    "--model-dir",
                    str(args.model_dir.resolve()),
                    "--backend",
                    args.backend,
                    "--batch",
                    str(batch),
                    "--seq-len",
                    str(sequence),
                    "--warmup-iters",
                    "5",
                    "--measure-iters",
                    "31",
                    "--diagnostics",
                    "false",
                    "--output",
                    str(out.resolve()),
                ]
                with log.open("wb") as stream:
                    run = subprocess.run(
                        command, check=False, stdout=stream, stderr=subprocess.STDOUT
                    )
                text = log.read_text()
                fields = {}
                for line in text.splitlines():
                    kind = line.split(" ", 1)[0]
                    if kind in (
                        "unified_setup",
                        "unified_execution",
                        "memory",
                        "kernels",
                    ):
                        fields[kind] = dict(
                            item.split("=", 1)
                            for item in line.split()[1:]
                            if "=" in item
                        )
                item = {
                    "order": order,
                    "arm": arm,
                    "side": side,
                    "returncode": run.returncode,
                    "log": str(log),
                    "metrics": fields,
                }
                if run.returncode == 0:
                    data = out.read_bytes()
                    reference = data if reference is None else reference
                    item["byte_identical"] = data == reference
                    item["sha256"] = hashlib.sha256(data).hexdigest()
                case["arms"].append(item)
                pair.append(item)
                (args.results / "receipt.json").write_text(
                    json.dumps(record, indent=2) + "\n"
                )
                print(json.dumps(item), flush=True)
                if run.returncode:
                    raise SystemExit(run.returncode)
            left, right = pair if order != "BA" else pair[::-1]
            ratios = {
                key: int(right["metrics"]["unified_execution"][key])
                / int(left["metrics"]["unified_execution"][key])
                for key in ("p50_ns", "p95_ns")
            }
            gate = {
                "order": order,
                **ratios,
                "passed": ratios["p50_ns"] <= 1.05 and ratios["p95_ns"] <= 1.10,
            }
            case.setdefault("timings", []).append(gate)
            print(json.dumps(gate), flush=True)
        case["numerical_pass"] = all(
            v.get("byte_identical", False) for v in case["arms"]
        )
        case["performance_pass"] = all(v["passed"] for v in case["timings"])
        (args.results / "receipt.json").write_text(json.dumps(record, indent=2) + "\n")
    assert all(c["numerical_pass"] for c in record["cases"]), (
        "model output parity failed"
    )
    # A failed timing/control is retained and reported, never retried here.
    print(
        json.dumps(
            {
                "complete": True,
                "performance_pass": all(c["performance_pass"] for c in record["cases"]),
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
