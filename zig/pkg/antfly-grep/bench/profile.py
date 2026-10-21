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

"""Paired in-process VM/JIT profiles with immutable corpus and executable identity."""

import argparse
import hashlib
import json
import os
import platform
import statistics
import subprocess
import tempfile
from pathlib import Path

TAGS = "consume repeat_consume look_shift split save assertion accept look reference atomic_start atomic_end reset progress call conditional grapheme verb callout alt_start alt_end script_start script_end scan".split()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--rounds", type=int, default=11)
    parser.add_argument("--records", type=int, default=100000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.rounds < 3 or args.records < 1:
        parser.error("at least three rounds and one record required")
    binaries = {"current": args.binary.resolve()}
    if args.baseline:
        binaries["baseline"] = args.baseline.resolve()
    report = {
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "load_average": list(os.getloadavg()),
        "rounds": args.rounds,
        "measurement": "in-process, compilation/source reads excluded; paired alternating binaries and VM/JIT modes; one warmup; checksum equality required",
        "binaries": {
            k: {
                "sha256": hashlib.sha256(v.read_bytes()).hexdigest(),
                "bytes": v.stat().st_size,
            }
            for k, v in binaries.items()
        },
        "cases": {},
    }
    cases = {
        "log-captures": (
            r"(2026)-(10)-(16) (info) (worker) (request) id=(123456) status=(success)",
            "2026-10-16 info worker request id=123456 status=success\n",
        ),
        "capture-reset": (r"(request)=(abc)\K(42)", "request=abc42\n"),
        "reset-only": (r"request=abc\K42", "request=abc42\n"),
        "log-reset": (
            r"(id)=(123)(456)\K status=(success)",
            "2026-10-15 info worker processed request id=123456 status=success\n",
        ),
        "unicode-captures": (r"(é)(α)(β)(Σ)(42)", "éαβΣ42\n"),
        "simple-repeat": (r"a{1,64}b", "a" * 32 + "b\n"),
        "control-fallback": (r"(?<n>ab)(?=ab)\k<n>", "abab\n"),
    }
    with tempfile.TemporaryDirectory(prefix="antfly-grep-profile-") as directory:
        path = Path(directory) / "subject"
        for name, (pattern, row) in cases.items():
            data = (row * args.records).encode()
            path.write_bytes(data)
            case = {
                "pattern": pattern,
                "bytes": len(data),
                "corpus_sha256": hashlib.sha256(data).hexdigest(),
                "binaries": {},
            }
            checksums = set()
            runs = {label: [] for label in binaries}
            dispatch = {}
            for binary in binaries.values():
                subprocess.run(
                    [str(binary), pattern, str(path), "1"],
                    check=True,
                    capture_output=True,
                )
            for round_index in range(args.rounds):
                ordered = list(binaries.items())
                if round_index % 2:
                    ordered.reverse()
                for label, binary in ordered:
                    samples = [
                        json.loads(line)
                        for line in subprocess.check_output(
                            [str(binary), pattern, str(path), "2"], text=True
                        ).splitlines()
                    ]
                    dispatch[label] = {
                        tag: samples[0]["vm_dispatch"][i]
                        for i, tag in enumerate(TAGS)
                        if samples[0]["vm_dispatch"][i]
                    }
                    if not dispatch[label]:
                        raise RuntimeError(
                            "profiling utility has no VM dispatch counters"
                        )
                    for sample in samples:
                        sample.pop("vm_dispatch")
                        sample["inner_round"] = sample["round"]
                        sample["round"] = round_index
                        sample["load_average"] = list(os.getloadavg())
                    runs[label].extend(samples)
            for label, samples in runs.items():
                checksums.update(s["checksum"] for s in samples)
                if not all(s["jit_available"] for s in samples):
                    raise RuntimeError("JIT unavailable on profiling host")
                modes = {
                    str(mode): statistics.median(
                        s["ns"] for s in samples if s["jit"] == mode
                    )
                    for mode in (False, True)
                }
                case["binaries"][label] = {
                    "median_ns": modes,
                    "vm_dispatch": dispatch[label],
                    "runs": samples,
                }
            if len(checksums) != 1:
                raise RuntimeError(f"VM/JIT/baseline checksum mismatch: {name}")
            case["checksum"] = checksums.pop()
            report["cases"][name] = case
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    for name, case in report["cases"].items():
        print(
            name,
            {
                k: round(v["median_ns"]["True"] / 1e6, 3)
                for k, v in case["binaries"].items()
            },
        )


if __name__ == "__main__":
    main()
