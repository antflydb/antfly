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

"""Check a Fastino CUDA profile against a pinned FP32 public holdout capture.

Performance comparisons must select a quality-valid reference. This checks
every document and language before a faster attention/dtype profile can be
treated as the reference; it does not grant native release qualification.
"""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path

import benchmark_cpu as cpu
from benchmark_family import reference_agreement, request
from check_family_holdout import slice_summary
from family_oracle import FIXTURES, verify_model
from source_span_policy import original_text_reference


def compare_reference_rows(
    case, actual, expected, tolerance, source_span_policy="strict"
):
    if actual["encoder_calls"] != expected["encoder_calls"]:
        raise ValueError("reference token IDs or attention masks changed")
    if len(actual["outputs"]) != len(case["texts"]) or len(expected["outputs"]) != len(
        case["texts"]
    ):
        raise ValueError("reference batch cardinality mismatch")
    rows = []
    for source_id, text, got, want in zip(
        case["source_ids"],
        case["texts"],
        actual["outputs"],
        expected["outputs"],
        strict=True,
    ):
        try:
            reference_agreement(got, want, tolerance)
            result = dict(source_id=source_id, passed=True)
        except ValueError as error:
            result = dict(source_id=source_id, passed=False, error=str(error))
        if source_span_policy == "original_text":
            strict = dict(result)
            candidate_exclusions, reference_exclusions = [], []
            try:
                got, candidate_exclusions = original_text_reference(text, got)
                want, reference_exclusions = original_text_reference(text, want)
                reference_agreement(got, want, tolerance)
                result = dict(source_id=source_id, passed=True)
            except ValueError as error:
                result = dict(source_id=source_id, passed=False, error=str(error))
            result.update(
                strict_comparison=strict,
                candidate_exclusions=candidate_exclusions,
                reference_exclusions=reference_exclusions,
            )
        elif source_span_policy != "strict":
            raise ValueError("unknown source span policy")
        rows.append(result)
    return rows


def pinned_bytes(path, manifest):
    key = str(path.resolve().relative_to(FIXTURES.resolve()))
    expected = manifest["fixtures"][key]
    data = path.read_bytes()
    if (
        len(data) != expected["size_bytes"]
        or hashlib.sha256(data).hexdigest() != expected["sha256"]
    ):
        raise ValueError(f"fixture hash mismatch: {key}")
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument(
        "--model", choices=("multi", "multi_decide", "decide_1b"), required=True
    )
    parser.add_argument("--dtype", choices=("fp32", "fp16", "bf16"), required=True)
    parser.add_argument(
        "--weight-dtype", choices=("fp32", "fp16", "bf16"), default="fp32"
    )
    parser.add_argument(
        "--attention", choices=("eager", "sdpa", "flashdeberta"), required=True
    )
    parser.add_argument(
        "--oracle-report",
        type=Path,
        required=True,
        help="Archived, manifest-pinned native holdout report containing an FP32 reference",
    )
    parser.add_argument(
        "--source-span-policy", choices=("strict", "original_text"), default="strict"
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.weight_dtype != "fp32" and args.weight_dtype != args.dtype:
        parser.error("reduced resident weights require the matching autocast dtype")
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    pin = verify_model(args.model_dir, args.model, manifest)
    baseline = json.loads(pinned_bytes(args.oracle_report, manifest))
    reference = baseline["reference"]
    if (
        baseline["status"] != "complete"
        or baseline["model"] != pin
        or reference["dtype"] != "fp32"
        or reference.get("weight_dtype", "fp32") != "fp32"
        or reference["model_files"] != pin["files"]
    ):
        raise ValueError("oracle identity/profile mismatch")
    task = baseline.get("task", "classification")
    if args.source_span_policy != "strict" and task == "classification":
        parser.error("source span exceptions require extraction")
    if task not in ("classification", "entities", "structured") or (
        task != "classification" and args.model != "multi"
    ):
        raise ValueError("unsupported holdout task")
    if task == "structured" and not baseline.get("all_heads_exercised"):
        raise ValueError("oracle does not exercise every extraction head")
    if baseline["scope"] != f"public_test_{task}_parity":
        raise ValueError("oracle scope mismatch")
    source = json.loads(pinned_bytes(FIXTURES / "holdout/source.json", manifest))
    if baseline["dataset"] != source:
        raise ValueError("oracle dataset mismatch")
    capture = baseline["raw_capture"]
    if (
        capture.get("encoding", "gzip" if capture["path"].endswith(".gz") else None)
        != "gzip"
    ):
        raise ValueError("expected compressed pinned oracle")
    raw = gzip.decompress(
        pinned_bytes(args.oracle_report.parent / capture["path"], manifest)
    )
    if hashlib.sha256(raw).hexdigest() != capture["uncompressed_sha256"]:
        raise ValueError("oracle capture hash mismatch")
    gold = {}
    for line in raw.splitlines():
        row = json.loads(line)
        key = row["locale"], row["case"]
        if key in gold:
            raise ValueError("duplicate oracle case")
        gold[key] = row["reference"]
    args.output.mkdir(parents=True, exist_ok=False)
    report = dict(
        status="running",
        qualification=False,
        scope="fastino_public_holdout_profile_quality",
        model=pin,
        task=task,
        dtype=args.dtype,
        weight_dtype=args.weight_dtype,
        attention=args.attention,
        dataset=source,
        source_span_policy=args.source_span_policy,
        oracle_report=str(args.oracle_report.resolve().relative_to(FIXTURES.resolve())),
        oracle_sha256=hashlib.sha256(raw).hexdigest(),
        slices=[],
    )

    def save():
        (args.output / "report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n"
        )

    env = dict(os.environ, TOKENIZERS_PARALLELISM="false")
    env.update({name: "1" for name in cpu.THREAD_ENV})
    guard = cpu.ResourceGuard(13 * 1024**3)
    worker = None
    try:
        worker = cpu.Worker(
            "python",
            [
                str(args.python),
                str(Path(__file__).with_name("family_oracle.py")),
                "--model-dir",
                str(args.model_dir),
                "--model",
                args.model,
                "--dtype",
                args.dtype,
                "--weight-dtype",
                args.weight_dtype,
                "--attention",
                args.attention,
            ],
            env,
            args.output,
            guard,
        )
        ready = worker.receive(1200)
        if (
            ready.get("event") != "ready"
            or ready["model_files"] != pin["files"]
            or ready["dtype"] != args.dtype
            or ready["weight_dtype"] != args.weight_dtype
            or ready["attention"] != args.attention
        ):
            raise ValueError("reference worker identity mismatch")
        report["reference"] = ready
        save()
        visited = set()
        with (args.output / "raw.jsonl").open("w") as stream:
            for filename in source["files"]:
                locale = filename.split("/")[0]
                fixture = json.loads(
                    pinned_bytes(
                        FIXTURES / "holdout/cases" / (locale + ".json"), manifest
                    )
                )
                rows = []
                for case in fixture["cases"]:
                    key = locale, case["id"]
                    if key in visited:
                        raise ValueError("duplicate holdout case")
                    visited.add(key)
                    payload = dict(texts=case["texts"])
                    if task == "entities":
                        payload["schema"] = dict(entities=baseline["entity_types"])
                    elif task == "structured":
                        payload["schema"] = baseline["extraction_schema"]
                    else:
                        payload["tasks"] = case["tasks"]
                    actual = request(worker, dict(op="validate", **payload), 1200)
                    stream.write(
                        json.dumps(
                            dict(
                                locale=locale,
                                case=case["id"],
                                candidate=actual,
                                reference=gold[key],
                            ),
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    stream.flush()
                    rows += compare_reference_rows(
                        case,
                        actual,
                        gold[key],
                        5e-4 if args.dtype == "fp32" else 5e-3,
                        args.source_span_policy,
                    )
                result = dict(locale=locale, **slice_summary(rows), rows=rows)
                if args.source_span_policy == "original_text":
                    result["strict_comparison"] = slice_summary(
                        [row["strict_comparison"] for row in rows]
                    )
                    for arm in ("candidate", "reference"):
                        result[f"excluded_{arm}_predictions"] = sum(
                            len(row[f"{arm}_exclusions"]) for row in rows
                        )
                report["slices"].append(result)
                save()
                print(
                    f"{locale}: {result['matched']}/{result['documents']} documents match",
                    flush=True,
                )
        if visited != gold.keys():
            raise ValueError("oracle cases do not match complete holdout")
        report.update(
            status="complete",
            measured_slices_passed=all(s["passed"] for s in report["slices"]),
            peak_worker_rss_bytes=guard.peak_rss_bytes,
        )
        if args.source_span_policy == "original_text":
            report["strict_slices_passed"] = all(
                s["strict_comparison"]["passed"] for s in report["slices"]
            )
            for arm in ("candidate", "reference"):
                report[f"excluded_{arm}_predictions"] = sum(
                    s[f"excluded_{arm}_predictions"] for s in report["slices"]
                )
        save()
        if not report["measured_slices_passed"]:
            raise SystemExit(1)
    except Exception as error:
        report.update(status="failed", measured_slices_passed=False, failure=str(error))
        save()
        raise
    finally:
        if worker is not None:
            worker.close()


if __name__ == "__main__":
    main()
