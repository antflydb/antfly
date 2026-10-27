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

# /// script
# requires-python = ">=3.11"
# dependencies = ["torch==2.14.1", "torchvision", "transformers==5.5.4", "pillow"]
# ///
"""Sequential native/Transformers OCR smoke comparison; no GPU overlap.

Pass the ReleaseFast inference test executable built with a LightOnOCR3 filter.
Run with the same uv dependencies as benchmark_transformers_mps.py.
"""

import argparse
from datetime import datetime
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import sys


def normalize(text):
    return " ".join(text.split())


def plain_text(text):
    """Content-only smoke metric; retain the raw text and raw CER too."""

    class Parser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.parts = []

        def handle_data(self, data):
            self.parts.append(data)

    parser = Parser()
    parser.feed(text)
    text = " ".join(parser.parts)
    return re.sub(r"(?m)^\s{0,3}#{1,6}\s+", "", text).replace("**", "")


def cer(expected, actual):
    expected, actual = normalize(expected), normalize(actual)
    row = list(range(len(actual) + 1))
    for i, a in enumerate(expected, 1):
        nxt = [i]
        for j, b in enumerate(actual, 1):
            nxt.append(min(nxt[-1] + 1, row[j] + 1, row[j - 1] + (a != b)))
        row = nxt
    return row[-1] / max(1, len(expected))


def native_report(log):
    pattern = r"LIGHTONOCR3 warm_run=(\d+) ms=(\d+) text=(.*?)(?=\ngenerate_timing_ms:|\nLIGHTONOCR3 warm_run=|\nOK|\n\d+/|\nAll |\Z)"
    runs = [
        {"run": int(i), "seconds": int(ms) / 1000, "text": text.strip()}
        for i, ms, text in re.findall(pattern, log, re.S)
    ]
    if not runs or "LIGHTONOCR3 backend=metal" not in log:
        raise RuntimeError("Native benchmark did not produce warm Metal results")
    phases = []
    for line in log.splitlines():
        match = re.search(r"generate_timing_ms: (.*)", line)
        if match:
            phases.append(
                {key: int(value) for key, value in re.findall(r"(\w+)=(\d+)", match[1])}
            )
    cold = re.search(r"LIGHTONOCR3 cold_request_ms=(\d+)", log)
    return {
        "backend": "metal",
        "runs": runs,
        "phase_timings_ms": phases,
        "warm_phase_timings_ms": phases[-len(runs) :],
        "cold_request_seconds": int(cold[1]) / 1000 if cold else None,
    }


def benchmark_reference(here, args, case, image):
    print(f"{case['name']}: PyTorch", flush=True)
    output = args.output_dir / f"{case['name']}-pytorch.json"
    command = [
        sys.executable,
        str(here / "benchmark_transformers_mps.py"),
        "--model-dir",
        str(args.model_dir.resolve()),
        "--image",
        str(image),
        "--warm-runs",
        str(args.warm_runs),
        "--max-new-tokens",
        str(case["max_new_tokens"]),
        "--output",
        str(output),
    ]
    if args.profile_phases:
        command.append("--profile-phases")
    with (args.output_dir / f"{case['name']}-pytorch.log").open("w") as log:
        subprocess.run(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=True,
            timeout=args.timeout_seconds,
        )
    return json.loads(output.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-test", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cases", nargs="*", default=[])
    parser.add_argument(
        "--modes", nargs="+", choices=["baseline", "f32", "bf16"], default=["f32"]
    )
    parser.add_argument("--warm-runs", type=int, default=3)
    parser.add_argument("--process-limit-gib", type=int, default=13)
    parser.add_argument("--shared-limit-gib", type=int, default=24)
    parser.add_argument("--profile-phases", action="store_true")
    parser.add_argument("--pytorch-first", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=600)
    args = parser.parse_args()
    if args.warm_runs < 1 or args.timeout_seconds < 1:
        parser.error("warm-runs and timeout-seconds must be positive")
    here = Path(__file__).resolve().parent
    args.output_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "date": datetime.now().astimezone().isoformat(),
        "platform": platform.platform(),
        "warm_runs_per_case": args.warm_runs,
        "pytorch_first": args.pytorch_first,
        "timeout_seconds": args.timeout_seconds,
        "native_executable_sha256": hashlib.sha256(
            args.native_test.read_bytes()
        ).hexdigest(),
        "process_limit_gib": args.process_limit_gib,
        "shared_limit_gib": args.shared_limit_gib,
        "limitations": [
            "Four synthetic smoke pages, not a representative accuracy evaluation.",
            "Shared machine; sequential measurements, loading excluded from warm latency.",
            "Native F32 state/accumulation; BF16 mode rounds multi-token projection inputs only.",
            "CER includes Markdown formatting differences after whitespace normalization.",
        ],
        "model_dir": str(args.model_dir),
        "cases": [],
    }
    for case in json.loads((here / "corpus/cases.json").read_text())["cases"]:
        if args.cases and case["name"] not in args.cases:
            continue
        image = (here / "corpus" / case["image"]).resolve()
        entry = {
            **case,
            "image_sha256": hashlib.sha256(image.read_bytes()).hexdigest(),
            "native": {},
        }
        reference = (
            benchmark_reference(here, args, case, image) if args.pytorch_first else None
        )
        for mode in args.modes:
            env = os.environ.copy()
            for key in list(env):
                if (
                    key.startswith("TERMITE_METAL_DISABLE_LIGHTONOCR3")
                    or key == "TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS"
                ):
                    del env[key]
            env.update(
                ANTFLY_TEST_LIGHTONOCR3_MODEL_DIR=str(args.model_dir.resolve()),
                ANTFLY_TEST_LIGHTONOCR3_IMAGE_PATH=str(image),
                ANTFLY_TEST_LIGHTONOCR3_MAX_TOKENS=str(case["max_new_tokens"]),
                ANTFLY_TEST_LIGHTONOCR3_WARM_RUNS=str(args.warm_runs),
                ANTFLY_TEST_LIGHTONOCR3_MEMORY_LIMIT_GIB=str(args.process_limit_gib),
                ANTFLY_TEST_LIGHTONOCR3_SHARED_LIMIT_GIB=str(args.shared_limit_gib),
                ANTFLY_BENCH_LIGHTONOCR3="1",
                TERMITE_SERVER_GENERATE_TIMING="1",
                ANTFLY_INFERENCE_PREFERRED_BACKEND="metal",
                ANTFLY_INFERENCE_REQUIRED_BACKEND="metal",
            )
            if mode == "baseline":
                env["TERMITE_METAL_DISABLE_LIGHTONOCR3_PROJECTIONS"] = "1"
            elif mode == "bf16":
                env["TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS"] = "1"
            log_path = args.output_dir / f"{case['name']}-{mode}.log"
            print(f"{case['name']}: native {mode}", flush=True)
            with log_path.open("w") as log:
                subprocess.run(
                    [
                        str(args.native_test.resolve()),
                        "--test-filter",
                        "LightOnOCR3 real checkpoint",
                    ],
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    check=True,
                    timeout=args.timeout_seconds,
                )
            result = native_report(log_path.read_text())
            if len(result["runs"]) != args.warm_runs:
                raise RuntimeError("Unexpected number of native warm runs")
            result["median_seconds"] = statistics.median(
                run["seconds"] for run in result["runs"]
            )
            result["warm_text_stable"] = (
                len({run["text"] for run in result["runs"]}) == 1
            )
            result["cer"] = cer(case["expected"], result["runs"][0]["text"])
            result["plain_text_cer"] = cer(
                plain_text(case["expected"]), plain_text(result["runs"][0]["text"])
            )
            result["required_present"] = {
                text: text in normalize(result["runs"][0]["text"])
                for text in case["required"]
            }
            entry["native"][mode] = result
            print(f"  median {result['median_seconds']:.3f}s", flush=True)
        if reference is None:
            reference = benchmark_reference(here, args, case, image)
        reference["median_seconds"] = statistics.median(
            run["seconds"] for run in reference["runs"] if run["warm"]
        )
        reference["cer"] = cer(case["expected"], reference["runs"][-1]["text"])
        reference["plain_text_cer"] = cer(
            plain_text(case["expected"]), plain_text(reference["runs"][-1]["text"])
        )
        reference["required_present"] = {
            text: text in normalize(reference["runs"][-1]["text"])
            for text in case["required"]
        }
        reference["warm_text_stable"] = (
            len({run["text"] for run in reference["runs"] if run["warm"]}) == 1
        )
        reference["output_budget_reached"] = any(
            run["tokens"] >= case["max_new_tokens"] for run in reference["runs"]
        )
        entry["pytorch"] = reference
        for result in entry["native"].values():
            result["speedup_vs_pytorch"] = (
                reference["median_seconds"] / result["median_seconds"]
            )
            result["normalized_text_matches_pytorch"] = all(
                normalize(run["text"]) == normalize(reference["runs"][-1]["text"])
                for run in result["runs"]
            )
        report["cases"].append(entry)
        (args.output_dir / "report.json").write_text(
            json.dumps(report, indent=2) + "\n"
        )
        print(f"  PyTorch median {reference['median_seconds']:.3f}s", flush=True)


if __name__ == "__main__":
    main()
