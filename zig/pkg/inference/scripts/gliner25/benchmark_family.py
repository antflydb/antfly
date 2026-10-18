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

"""Paired loaded-model CUDA classification comparison for the pinned family.

Run separately for each native precision and Fastino attention/dtype candidate.
Both arms must first match the pinned FP32 oracle on every case. This diagnostic
writes raw alternating pairs, confidence evidence and bootstrap intervals; it
never grants release qualification or substitutes for the serving/holdout suite.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import random
import statistics

import benchmark_cpu as cpu
from check_family_decisions import compare, compare_boundary, compare_full_boundary
from family_oracle import FIXTURES, verify_model


def request(worker, command, timeout):
    worker.sequence += 1
    command = dict(command, request_id=worker.sequence)
    worker.process.stdin.write(
        (json.dumps(command, ensure_ascii=False) + "\n").encode()
    )
    worker.process.stdin.flush()
    result = worker.receive(timeout)
    if result.get("request_id") != worker.sequence:
        raise ValueError("worker response identity mismatch")
    duration = result.get("duration_ns")
    if type(duration) is not int or duration <= 0:
        raise ValueError("invalid worker timing")
    return result


def reference_agreement(actual, oracle, tolerance):
    if isinstance(oracle, dict):
        if not isinstance(actual, dict) or set(actual) != set(oracle):
            raise ValueError("reference output keys changed")
        for key in oracle:
            reference_agreement(actual[key], oracle[key], tolerance)
    elif isinstance(oracle, list):
        if not isinstance(actual, list) or len(actual) != len(oracle):
            raise ValueError("reference output cardinality changed")
        # Match multi-label presentation by identity, preserving document order.
        if oracle and isinstance(oracle[0], dict) and "label" in oracle[0]:
            actual = sorted(actual, key=lambda x: x["label"])
            oracle = sorted(oracle, key=lambda x: x["label"])
        for x, y in zip(actual, oracle, strict=True):
            reference_agreement(x, y, tolerance)
    elif isinstance(oracle, float):
        if (
            not isinstance(actual, (int, float))
            or not math.isfinite(actual)
            or abs(actual - oracle) > tolerance
        ):
            raise ValueError("reference confidence outside FP32 tolerance")
    elif actual != oracle:
        raise ValueError("reference selected label changed")


def aggregate_interval(cells, draws=10000):
    if not cells or any(not pairs for pairs in cells):
        raise ValueError("empty paired cells")
    logs = [[math.log(python / native) for native, python in pairs] for pairs in cells]
    rng = random.Random(20261006)
    values = []
    for _ in range(draws):
        values.append(
            math.exp(
                statistics.mean(
                    statistics.median(rng.choices(cell, k=len(cell))) for cell in logs
                )
            )
        )
    return dict(
        geomean=math.exp(statistics.mean(statistics.median(cell) for cell in logs)),
        lower_95=cpu.paired_benchmark.percentile(values, 0.025),
        upper_95=cpu.paired_benchmark.percentile(values, 0.975),
        estimator="equal_weight_cells_median_paired_log_speedup",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument(
        "--model", choices=("decide_1b", "multi_decide", "multi"), required=True
    )
    parser.add_argument("--precision", choices=("fp32", "fp16", "bf16"), default="fp32")
    parser.add_argument(
        "--reference-dtype", choices=("fp32", "fp16", "bf16"), default="fp32"
    )
    parser.add_argument(
        "--reference-weight-dtype", choices=("fp32", "fp16", "bf16"), default="fp32"
    )
    parser.add_argument(
        "--attention", choices=("eager", "sdpa", "flashdeberta"), default="eager"
    )
    parser.add_argument("--cases", type=Path)
    parser.add_argument(
        "--native-cases", type=Path, help="Matching pinned boundary worker fixture"
    )
    parser.add_argument("--oracle", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pairs", type=int, default=30)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--tail-samples", type=int, default=200)
    parser.add_argument("--timeout", type=float, default=1200)
    parser.add_argument("--max-rss-gib", type=float, default=13)
    parser.add_argument(
        "--stop-on-regression",
        action="store_true",
        help="Retain completed cells and stop at the first failed performance guard",
    )
    parser.add_argument(
        "--attention-only",
        action="store_true",
        help="Boundary FP16 attention with FP32 encoder projections",
    )
    args = parser.parse_args()
    if (
        args.reference_weight_dtype != "fp32"
        and args.reference_weight_dtype != args.reference_dtype
    ):
        parser.error("reduced reference weights require the matching autocast dtype")
    if args.attention_only and (args.model == "decide_1b" or args.precision != "fp16"):
        parser.error("--attention-only requires a boundary model with --precision fp16")
    compute_policy = (
        "fp32"
        if args.precision == "fp32"
        else (
            "fp16_attention"
            if args.attention_only
            else "fp16_encoder_matrices_and_attention"
        )
    )
    default_cases = FIXTURES / (
        "multi_requests.json" if args.model == "multi" else "decision_cases.json"
    )
    args.cases = args.cases or default_cases
    if args.pairs < 30 or args.tail_samples < 200 or args.warmup < 1:
        parser.error("requires >=30 paired, >=200 tail samples, and warmup")
    if args.model != "decide_1b" and (
        args.precision == "bf16"
        or (args.cases != default_cases and not args.native_cases)
    ):
        parser.error(
            "boundary worker requires FP32/FP16 and matching --native-cases for custom cases"
        )
    pin = verify_model(args.model_dir, args.model)
    cases = json.loads(args.cases.read_text())["cases"]
    command_count = (
        len(cases) * (1 + args.warmup + max(args.pairs, args.tail_samples)) + 1
    )
    if command_count > (65536 if args.model == "decide_1b" else 4096):
        parser.error("campaign exceeds the bounded worker command budget")
    captures = [
        json.loads(line)
        for line in (args.oracle or FIXTURES / args.model / "oracle_fp32.jsonl")
        .read_text()
        .splitlines()
    ]
    ready, *rows = captures
    if (
        ready["dtype"] != "fp32"
        or ready.get("weight_dtype", "fp32") != "fp32"
        or ready["model_files"] != pin["files"]
        or ready["model_id"] != pin["model_id"]
    ):
        raise ValueError("FP32 oracle provenance mismatch")
    oracles = {row["request_id"]: row for row in rows}
    args.output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, TOKENIZERS_PARALLELISM="false")
    env.update({key: "1" for key in cpu.THREAD_ENV})
    guard = cpu.ResourceGuard(int(args.max_rss_gib * 1024**3))
    workers = []
    current_case = None
    phase = "startup"
    report = dict(
        model=pin,
        qualification=False,
        scope="loaded_boundary_tasks"
        if args.model == "multi"
        else "loaded_classification_fixtures",
        native_binary_sha256=hashlib.sha256(args.native.read_bytes()).hexdigest(),
        cases_sha256=hashlib.sha256(args.cases.read_bytes()).hexdigest(),
        oracle_sha256=hashlib.sha256(
            (args.oracle or FIXTURES / args.model / "oracle_fp32.jsonl").read_bytes()
        ).hexdigest(),
        precision=args.precision,
        attention_only=args.attention_only,
        reference_dtype=args.reference_dtype,
        reference_weight_dtype=args.reference_weight_dtype,
        attention=args.attention,
        pairs=args.pairs,
        tail_samples=args.tail_samples,
        warmup=args.warmup,
        cells=[],
    )
    try:
        python = cpu.Worker(
            "python",
            [
                str(args.python),
                str(Path(__file__).with_name("family_oracle.py")),
                "--model-dir",
                str(args.model_dir),
                "--model",
                args.model,
                "--dtype",
                args.reference_dtype,
                "--weight-dtype",
                args.reference_weight_dtype,
                "--attention",
                args.attention,
            ],
            env,
            args.output,
            guard,
        )
        workers.append(python)
        report["python"] = python.receive(args.timeout)
        if (
            report["python"].get("event") != "ready"
            or report["python"]["model_files"] != pin["files"]
            or report["python"]["dtype"] != args.reference_dtype
            or report["python"]["weight_dtype"] != args.reference_weight_dtype
            or report["python"]["attention"] != args.attention
        ):
            raise ValueError("Python worker provenance mismatch")
        command = [str(args.native.resolve()), "--model-dir", str(args.model_dir)]
        if args.model == "decide_1b":
            command += [
                "--backend",
                "cuda",
                "--precision",
                args.precision,
                "--cases",
                str(args.cases),
                "--worker",
                "1",
            ]
        else:
            command += [
                "--cases",
                str(args.native_cases or FIXTURES / args.model / "cases.json"),
                "--threads",
                "1",
                "--max-commands",
                str(command_count),
                "--max-text-words",
                "4096",
                "--max-sequence-tokens",
                "2048",
                "--precision",
                args.precision,
                "--attention-only",
                str(int(args.attention_only)),
            ]
        native = cpu.Worker("native", command, env, args.output, guard)
        workers.append(native)
        report["native"] = native.receive(args.timeout)
        (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        if report["native"].get("event") != "ready" or report["native"].get(
            "build_mode"
        ) not in ("fast", "ReleaseFast"):
            raise ValueError("native timing requires an optimized worker")
        if (
            report["native"].get("backend") != "cuda"
            or report["native"].get("precision") != args.precision
        ):
            raise ValueError("native worker precision/backend mismatch")
        if (
            args.model != "decide_1b"
            and report["native"].get("compute_policy") != compute_policy
        ):
            raise ValueError("native worker compute policy mismatch")
        all_pairs = []
        with (args.output / "raw.jsonl").open("w") as raw:
            for index, case in enumerate(cases):
                current_case = case["id"]
                phase = "quality_validation"

                def call(arm, op):
                    if arm == "python":
                        payload = {
                            key: case[key]
                            for key in ("texts", "tasks", "schema", "kind")
                            if key in case
                        }
                        return request(python, dict(op=op, **payload), args.timeout)
                    key = (
                        dict(case_index=index)
                        if args.model == "decide_1b"
                        else dict(case_id=case["id"])
                    )
                    return request(native, dict(op=op, **key), args.timeout)

                p = call("python", "validate")
                n = call("native", "validate")
                oracle = oracles[case["id"]]
                # Persist rejected profiles too, so a failed quality gate is
                # inspectable and cannot be mistaken for a performance loss.
                raw.write(
                    json.dumps(
                        dict(case=case["id"], validation=dict(native=n, python=p)),
                        allow_nan=False,
                    )
                    + "\n"
                )
                raw.flush()
                reference_agreement(
                    p["outputs"],
                    oracle["outputs"],
                    5e-4 if args.reference_dtype == "fp32" else 5e-3,
                )
                if p["encoder_calls"] != oracle["encoder_calls"]:
                    raise ValueError("reference token IDs changed")
                comparator = {
                    "decide_1b": compare,
                    "multi_decide": compare_boundary,
                    "multi": compare_full_boundary,
                }[args.model]
                error = comparator(
                    case, n, oracle, 5e-4 if args.precision == "fp32" else 5e-3
                )
                phase = "timing"
                for _ in range(args.warmup):
                    call("native", "run")
                    call("python", "run")
                pairs = []
                tails = dict(native=[], python=[])
                for pair in range(max(args.pairs, args.tail_samples)):
                    order = cpu.paired_benchmark.balanced_pair_order(
                        pair + 1, "native", "python"
                    )
                    measured = {arm: call(arm, "run")["duration_ns"] for arm in order}
                    raw.write(
                        json.dumps(
                            dict(case=case["id"], pair=pair, order=order, **measured)
                        )
                        + "\n"
                    )
                    raw.flush()
                    if pair < args.pairs:
                        pairs.append((measured["native"], measured["python"]))
                    for arm in tails:
                        tails[arm].append(measured[arm])
                all_pairs.append(pairs)
                interval = cpu.paired_benchmark.paired_log_ratio_ci(pairs)
                report["cells"].append(
                    dict(
                        id=case["id"],
                        batch_size=len(case["texts"]),
                        max_confidence_error=error,
                        native_ns=cpu.paired_benchmark.distribution(tails["native"]),
                        python_ns=cpu.paired_benchmark.distribution(tails["python"]),
                        native_over_python=interval,
                        regression_guard_passed=interval["upper_95"] <= 1.1,
                    )
                )
                (args.output / "report.json").write_text(
                    json.dumps(report, indent=2) + "\n"
                )
                print(
                    f"{case['id']}: native/python {interval['median']:.3f}", flush=True
                )
                if (
                    args.stop_on_regression
                    and not report["cells"][-1]["regression_guard_passed"]
                ):
                    raise RuntimeError("completed cell failed the 10% regression guard")
        report["aggregate_speedup"] = aggregate_interval(all_pairs)
        report["measured_cells_passed"] = report["aggregate_speedup"][
            "lower_95"
        ] >= 1 and all(c["regression_guard_passed"] for c in report["cells"])
        report["peak_worker_rss_bytes"] = guard.peak_rss_bytes
        report["status"] = "complete"
        (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        if not report["measured_cells_passed"]:
            # A completed measurement is still a failed performance check.
            # Preserve every cell and close both workers through finally.
            raise SystemExit(1)
    except Exception as error:
        report.update(
            status="failed",
            measured_cells_passed=False,
            failure=dict(case=current_case, phase=phase, error=str(error)),
        )
        (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        raise
    finally:
        for worker in reversed(workers):
            worker.close()


if __name__ == "__main__":
    main()
