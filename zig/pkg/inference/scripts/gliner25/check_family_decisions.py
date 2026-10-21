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

"""Compare real native decision runs with family_oracle.py validation captures.

Timings in this diagnostic are not paired performance qualification. Token IDs,
every task selection, finite logits and confidence tolerances are checked for
every document; agreement on the first document alone cannot pass a batch.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess

from family_oracle import FIXTURES, verify_model


def compare_values(actual, expected, tolerance):
    """Compare public structure/coordinates exactly and confidence numerically."""
    if isinstance(expected, dict):
        if not isinstance(actual, dict) or set(actual) != set(expected):
            raise ValueError("public output keys changed")
        return max(
            (compare_values(actual[k], v, tolerance) for k, v in expected.items()),
            default=0,
        )
    if isinstance(expected, list):
        if not isinstance(actual, list) or len(actual) != len(expected):
            raise ValueError("public output cardinality changed")
        if expected and isinstance(expected[0], dict) and "label" in expected[0]:
            actual = sorted(actual, key=lambda x: x["label"])
            expected = sorted(expected, key=lambda x: x["label"])
        return max(
            (
                compare_values(x, y, tolerance)
                for x, y in zip(actual, expected, strict=True)
            ),
            default=0,
        )
    if isinstance(expected, float):
        if (
            not isinstance(actual, (int, float))
            or not math.isfinite(actual)
            or abs(actual - expected) > tolerance
        ):
            raise ValueError("confidence outside FP32 tolerance")
        return abs(actual - expected)
    if actual != expected:
        raise ValueError("public value or coordinate changed")
    return 0


def compare_full_boundary(case, native, reference, tolerance=5e-4):
    from benchmark_cpu import canonical_result

    batch, width = native["encoder_shape"]
    calls = reference["encoder_calls"]
    if (
        len(calls) != 1
        or batch != len(case["texts"])
        or len(calls[0]["input_ids"]) != batch
    ):
        raise ValueError("boundary batch shape mismatch")
    if any(len(row) != width for row in calls[0]["input_ids"]) or native[
        "input_ids"
    ] != [v for row in calls[0]["input_ids"] for v in row]:
        raise ValueError("encoder token identity mismatch")
    outputs = native["outputs"] if batch > 1 else [native["output"]]
    transfers = native.get("cuda_transfers")
    if (
        transfers is None
        or transfers["host_fallback_calls"] != 0
        or transfers["kernel_launches"] == 0
    ):
        raise ValueError("missing CUDA execution evidence")
    return compare_values(
        [canonical_result(row) for row in outputs], reference["outputs"], tolerance
    )


def compare(case, native, reference, tolerance=5e-4):
    expected_ids = []
    for call in reference["encoder_calls"]:
        if len(call["input_ids"]) != len(call["attention_mask"]):
            raise ValueError("invalid reference batch shape")
        for ids, mask in zip(call["input_ids"], call["attention_mask"], strict=True):
            if len(ids) != len(mask) or any(x not in (0, 1) for x in mask):
                raise ValueError("invalid reference attention mask")
            expected_ids.append(
                [token for token, valid in zip(ids, mask, strict=True) if valid]
            )
    if native["prepared_input_ids"] != expected_ids:
        raise ValueError(f"{case['id']}: encoder token identity mismatch")
    if native["batch_size"] != len(case["texts"]) or len(native["decisions"]) != len(
        case["texts"]
    ):
        raise ValueError("native batch cardinality mismatch")
    if len(reference["outputs"]) != len(case["texts"]):
        raise ValueError("reference batch cardinality mismatch")
    if native.get("backend") == "cuda":
        transfers = native.get("cuda_transfers")
        # The graph only downloads final padded marker logits. These fixtures
        # share one schema, so the output count is exact even for split batches.
        repeats = native["warmup"] + native["reps"]
        output_bytes = 4 * sum(len(row["raw_logits"]) for row in native["decisions"])
        if (
            not transfers
            or transfers["kernel_launches"] <= 0
            or transfers["d2h_bytes"] != output_bytes * repeats
        ):
            raise ValueError(
                "missing CUDA execution evidence or intermediate host download"
            )
    max_error = 0.0
    for actual, expected in zip(native["decisions"], reference["outputs"], strict=True):
        if len(actual["tasks"]) != len(case["tasks"]):
            raise ValueError("missing decision task")
        if any(not math.isfinite(value) for value in actual["raw_logits"]):
            raise ValueError("non-finite decision logits")
        for task_index, ((name, config), result) in enumerate(
            zip(case["tasks"].items(), actual["tasks"], strict=True)
        ):
            labels = list(config["labels"] if isinstance(config, dict) else config)
            wanted = expected[name]
            wanted = wanted if isinstance(wanted, list) else [wanted]
            got = result["selections"]
            if result["task_index"] != task_index or len(wanted) != len(got):
                raise ValueError(f"{case['id']}/{name}: selection count/order mismatch")
            # Multi-label output order is presentation-only. Match by label.
            wanted = {item["label"]: item["confidence"] for item in wanted}
            if {labels[item["label_index"]] for item in got} != set(wanted):
                raise ValueError(f"{case['id']}/{name}: decision mismatch")
            for selection in got:
                value = selection["probability"]
                error = abs(value - wanted[labels[selection["label_index"]]])
                if not math.isfinite(value) or error > tolerance:
                    raise ValueError(
                        f"{case['id']}/{name}: confidence error {error} > {tolerance}"
                    )
                max_error = max(max_error, error)
    return max_error


def compare_boundary(case, native, reference, tolerance=5e-4):
    batch, width = native["encoder_shape"]
    calls = reference["encoder_calls"]
    if (
        len(calls) != 1
        or batch != len(case["texts"])
        or len(calls[0]["input_ids"]) != batch
    ):
        raise ValueError("boundary batch shape mismatch")
    if any(len(row) != width for row in calls[0]["input_ids"]):
        raise ValueError("boundary padded width mismatch")
    if native["input_ids"] != [token for row in calls[0]["input_ids"] for token in row]:
        raise ValueError(f"{case['id']}: encoder token identity mismatch")
    outputs = native["outputs"] if batch > 1 else [native["output"]]
    if len(outputs) != batch or len(reference["outputs"]) != batch:
        raise ValueError("boundary output cardinality mismatch")
    error = 0.0
    for actual, expected in zip(outputs, reference["outputs"], strict=True):
        if [task["name"] for task in actual["classifications"]] != list(case["tasks"]):
            raise ValueError("boundary task identity mismatch")
        for task in actual["classifications"]:
            wanted = expected[task["name"]]
            wanted = wanted if isinstance(wanted, list) else [wanted]
            wanted = {item["label"]: item["confidence"] for item in wanted}
            got = task["labels"]
            if len(got) != len(wanted) or {item["label"] for item in got} != set(
                wanted
            ):
                raise ValueError(f"{case['id']}: decision mismatch")
            for selection in got:
                delta = abs(selection["confidence"] - wanted[selection["label"]])
                if not math.isfinite(delta) or delta > tolerance:
                    raise ValueError(f"{case['id']}: confidence error {delta}")
                error = max(error, delta)
    transfers = native.get("cuda_transfers")
    if (
        transfers is None
        or transfers["host_fallback_calls"] != 0
        or transfers["kernel_launches"] == 0
    ):
        raise ValueError("missing CUDA execution evidence")
    return error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--model", choices=("decide_1b", "multi_decide", "multi"), default="decide_1b"
    )
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--oracle", type=Path, required=True)
    parser.add_argument("--cases", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--native-cases", type=Path, help="Matching pinned boundary worker fixture"
    )
    parser.add_argument("--backend", choices=("native", "cuda"), default="cuda")
    parser.add_argument("--precision", choices=("fp32", "fp16", "bf16"), default="fp32")
    parser.add_argument(
        "--attention-only",
        action="store_true",
        help="Boundary FP16 attention with FP32 encoder projections",
    )
    args = parser.parse_args()
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
    args.cases = args.cases or FIXTURES / (
        "multi_requests.json" if args.model == "multi" else "decision_cases.json"
    )
    pin = verify_model(args.model_dir, args.model)
    captures = [json.loads(line) for line in args.oracle.read_text().splitlines()]
    ready, *responses = captures
    if (
        ready["model_id"] != pin["model_id"]
        or ready["model_files"] != pin["files"]
        or ready["dtype"] != "fp32"
        or ready.get("weight_dtype", "fp32") != "fp32"
    ):
        raise ValueError("oracle artifact/profile mismatch")
    references = {row["request_id"]: row for row in responses}
    cases_path = args.cases
    cases = json.loads(cases_path.read_text())["cases"]
    report = dict(
        model_id=pin["model_id"],
        revision=pin["revision"],
        backend=args.backend,
        precision=args.precision,
        attention_only=args.attention_only,
        qualification=False,
        status="running",
        expected_cases=len(cases),
        cases=[],
        native_binary_sha256=hashlib.sha256(args.native.read_bytes()).hexdigest(),
        cases_sha256=hashlib.sha256(args.cases.read_bytes()).hexdigest(),
        oracle_sha256=hashlib.sha256(args.oracle.read_bytes()).hexdigest(),
    )
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    from benchmark_cpu import THREAD_ENV

    env = dict(os.environ, TOKENIZERS_PARALLELISM="false")
    env.update({name: "1" for name in THREAD_ENV})
    try:
        if args.model != "decide_1b":
            if args.backend != "cuda" or args.precision == "bf16":
                raise ValueError(
                    "boundary diagnostic requires CUDA FP32 or an FP16 candidate"
                )
            commands = [
                dict(request_id=i, op="validate", case_id=case["id"])
                for i, case in enumerate(cases)
            ]
            commands.append(dict(request_id=len(cases), op="stop"))
            result = subprocess.run(
                [
                    str(args.native.resolve()),
                    "--model-dir",
                    str(args.model_dir),
                    "--cases",
                    str(args.native_cases or FIXTURES / args.model / "cases.json"),
                    "--threads",
                    "1",
                    "--max-text-words",
                    "4096",
                    "--max-sequence-tokens",
                    "2048",
                    "--precision",
                    args.precision,
                    "--attention-only",
                    str(int(args.attention_only)),
                ],
                input="".join(json.dumps(c) + "\n" for c in commands),
                check=False,
                capture_output=True,
                text=True,
                timeout=1200,
                env=env,
            )
        else:
            result = subprocess.run(
                [
                    str(args.native.resolve()),
                    "--model-dir",
                    str(args.model_dir),
                    "--backend",
                    args.backend,
                    "--cases",
                    str(cases_path),
                    "--all-cases",
                    "1",
                    "--precision",
                    args.precision,
                    "--warmup",
                    "1",
                    "--reps",
                    "3",
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=1200,
                env=env,
            )
        args.output.with_suffix(".raw.jsonl").write_text(result.stdout)
        args.output.with_suffix(".stderr.log").write_text(result.stderr)
        result.check_returncode()
        native_rows = [json.loads(line) for line in result.stdout.splitlines()]
        if args.model != "decide_1b":
            if (
                not native_rows
                or native_rows[0].get("backend") != "cuda"
                or native_rows[0].get("precision") != args.precision
            ):
                raise ValueError("native worker precision/backend mismatch")
            if native_rows[0].get("compute_policy") != compute_policy:
                raise ValueError("native worker compute policy mismatch")
            report["native"] = native_rows[0]
            native_rows = [row for row in native_rows if row.get("event") == "result"]
        if len(native_rows) != len(cases):
            raise ValueError("missing native cases")
        for index, (case, native) in enumerate(zip(cases, native_rows, strict=True)):
            if args.model != "decide_1b":
                if native["case_id"] != case["id"]:
                    raise ValueError("native case order mismatch")
                comparator = (
                    compare_full_boundary if args.model == "multi" else compare_boundary
                )
                error = comparator(
                    case,
                    native,
                    references[case["id"]],
                    5e-4 if args.precision == "fp32" else 5e-3,
                )
            else:
                if native["case_index"] != index:
                    raise ValueError("native case order mismatch")
                error = compare(
                    case,
                    native,
                    references[case["id"]],
                    5e-4 if args.precision == "fp32" else 5e-3,
                )
            report["cases"].append(
                dict(id=case["id"], max_confidence_error=error, native=native)
            )
            args.output.write_text(
                json.dumps(report, ensure_ascii=False, indent=2) + "\n"
            )
            print(f"{case['id']}: passed, max confidence error {error:.6g}", flush=True)
        report["status"] = "complete"
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    except Exception as error:
        report.update(status="failed", failure=str(error))
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        raise


if __name__ == "__main__":
    main()
