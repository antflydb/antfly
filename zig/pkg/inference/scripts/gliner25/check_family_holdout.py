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

"""Check CUDA classification or extraction parity on pinned MASSIVE test slices.

Measures implementation agreement with Fastino FP32, not gold-label accuracy or
training-data independence. Token IDs and GPU execution must match on every row;
at least 99.5% of documents in EACH language must preserve all task selections
and confidence tolerances. Entity mode also checks every source coordinate and
output structure. Structured mode additionally covers attributes, relations and
records and requires nonempty reference coverage of each head. These checks
cannot qualify release capabilities, JointIE, serving concurrency or performance.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path

import benchmark_cpu as cpu
from benchmark_family import request
from check_family_decisions import compare, compare_boundary, compare_full_boundary
from family_oracle import FIXTURES, verify_model
from source_span_policy import original_text_reference, validate_source_spans


def compare_rows(
    case, native, reference, model, tolerance, source_span_policy="strict"
):
    """Validate execution/identity globally, then score each document once."""
    count = len(case["texts"])
    calls = reference["encoder_calls"]
    transfers = native.get("cuda_transfers")
    if not transfers or transfers["kernel_launches"] <= 0:
        raise ValueError("missing CUDA execution evidence")
    if len(reference["outputs"]) != count or len(calls) != 1:
        raise ValueError("reference batch shape mismatch")
    call = calls[0]
    if len(call["input_ids"]) != count or len(call["attention_mask"]) != count:
        raise ValueError("reference encoder batch mismatch")
    if model == "decide_1b":
        expected = []
        for ids, mask in zip(call["input_ids"], call["attention_mask"], strict=True):
            if len(ids) != len(mask) or any(x not in (0, 1) for x in mask):
                raise ValueError("invalid reference attention mask")
            expected.append(
                [token for token, valid in zip(ids, mask, strict=True) if valid]
            )
        if native["prepared_input_ids"] != expected:
            raise ValueError("encoder token identity mismatch")
        if native["batch_size"] != count or len(native["decisions"]) != count:
            raise ValueError("native batch cardinality mismatch")
        size = 4 * sum(len(row["raw_logits"]) for row in native["decisions"])
        if transfers["d2h_bytes"] != size * (native["warmup"] + native["reps"]):
            raise ValueError("intermediate host download")
    else:
        batch, width = native["encoder_shape"]
        if batch != count or any(len(row) != width for row in call["input_ids"]):
            raise ValueError("native encoder shape mismatch")
        if native["input_ids"] != [token for row in call["input_ids"] for token in row]:
            raise ValueError("encoder token identity mismatch")
        outputs = native["outputs"] if count > 1 else [native["output"]]
        if len(outputs) != count or transfers["host_fallback_calls"] != 0:
            raise ValueError("native output cardinality or host fallback")
    results = []
    for index, text in enumerate(case["texts"]):
        row_case = dict(case, texts=[text])
        row_reference = dict(
            outputs=[reference["outputs"][index]],
            encoder_calls=[
                dict(
                    input_ids=[call["input_ids"][index]],
                    attention_mask=[call["attention_mask"][index]],
                )
            ],
        )
        if model == "decide_1b":
            # Whole-batch device transfers were checked above. This view checks
            # values only and does not invent per-document transfer counters.
            row_native = dict(
                batch_size=1,
                prepared_input_ids=[native["prepared_input_ids"][index]],
                decisions=[native["decisions"][index]],
            )
            comparator = compare
        else:
            row_native = dict(
                encoder_shape=[1, width],
                input_ids=call["input_ids"][index],
                output=outputs[index],
                cuda_transfers=transfers,
            )
            comparator = compare_full_boundary if "schema" in case else compare_boundary
        try:
            error = comparator(row_case, row_native, row_reference, tolerance)
            result = dict(
                source_id=case["source_ids"][index],
                passed=True,
                max_confidence_error=error,
            )
        except ValueError as error:
            result = dict(
                source_id=case["source_ids"][index], passed=False, error=str(error)
            )
        if source_span_policy == "original_text":
            if model != "multi" or "schema" not in case:
                raise ValueError("original_text span policy requires extraction")
            strict = dict(result)
            excluded = []
            try:
                adjusted, excluded = original_text_reference(
                    text, row_reference["outputs"][0]
                )
                validate_source_spans(text, row_native["output"])
                error = comparator(
                    row_case,
                    row_native,
                    dict(row_reference, outputs=[adjusted]),
                    tolerance,
                )
                result = dict(
                    source_id=strict["source_id"],
                    passed=True,
                    max_confidence_error=error,
                )
            except ValueError as error:
                result = dict(
                    source_id=strict["source_id"], passed=False, error=str(error)
                )
            result.update(strict_comparison=strict, reference_exclusions=excluded)
        elif source_span_policy != "strict":
            raise ValueError("unknown source span policy")
        results.append(result)
    return results


def slice_summary(rows):
    if len(rows) != 200 or len({row["source_id"] for row in rows}) != 200:
        raise ValueError("holdout slice must contain 200 unique source rows")
    passed = sum(row["passed"] for row in rows)
    return dict(
        documents=len(rows),
        matched=passed,
        agreement=passed / len(rows),
        passed=passed / len(rows) >= 0.995,
    )


def decision_worker_fixture(cases):
    # The worker accepts an inference fixture, while the pinned holdout also
    # carries dataset provenance and per-document source IDs. Retain those in
    # the report; pass only the strict worker schema over this boundary.
    return dict(
        format_version=1,
        cases=[{key: case[key] for key in ("id", "texts", "tasks")} for case in cases],
    )


def extraction_coverage(outputs):
    """Count actual reference predictions, so empty outputs cannot cover a head."""
    counts = dict(entities=0, attributes=0, relations=0, records=0)
    for output in outputs:
        for entity in output["entities"]:
            counts["entities"] += len(entity["values"])
            counts["attributes"] += sum(
                len(value["attributes"]) for value in entity["values"]
            )
        counts["relations"] += len(output["relations"])
        counts["records"] += sum(
            len(record["instances"]) for record in output["structures"]
        )
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument(
        "--model", choices=("multi", "multi_decide", "decide_1b"), required=True
    )
    parser.add_argument("--precision", choices=("fp32", "fp16", "bf16"), default="fp32")
    parser.add_argument(
        "--attention-only",
        action="store_true",
        help="Boundary FP16 attention with FP32 encoder projections",
    )
    parser.add_argument(
        "--task",
        choices=("classification", "entities", "structured"),
        default="classification",
    )
    parser.add_argument(
        "--source-span-policy", choices=("strict", "original_text"), default="strict"
    )
    parser.add_argument(
        "--locales", nargs="+", help="Defaults to all eight pinned language slices"
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.model != "decide_1b" and args.precision == "bf16":
        parser.error("boundary worker supports FP32/FP16")
    if args.attention_only and (args.model == "decide_1b" or args.precision != "fp16"):
        parser.error("--attention-only requires a boundary model with --precision fp16")
    if args.task != "classification" and args.model != "multi":
        parser.error("extraction requires Multi")
    if args.source_span_policy != "strict" and args.task == "classification":
        parser.error("source span exceptions require extraction")
    pin = verify_model(args.model_dir, args.model)
    source = json.loads((FIXTURES / "holdout/source.json").read_text())
    available = [filename.split("/")[0] for filename in source["files"]]
    locales = args.locales or available
    if len(set(locales)) != len(locales) or any(
        locale not in available for locale in locales
    ):
        parser.error("locales must be unique pinned slices")
    args.output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, TOKENIZERS_PARALLELISM="false")
    env.update({name: "1" for name in cpu.THREAD_ENV})
    guard = cpu.ResourceGuard(13 * 1024**3)
    entity_types = ["person", "organization", "location", "date", "time", "number"]
    schema = dict(entities=entity_types) if args.task == "entities" else None
    if args.task == "structured":
        path = FIXTURES / "holdout/structured_schema.json"
        data = path.read_bytes()
        expected = json.loads((FIXTURES / "manifest.json").read_text())["fixtures"][
            str(path.relative_to(FIXTURES))
        ]
        if (
            len(data) != expected["size_bytes"]
            or hashlib.sha256(data).hexdigest() != expected["sha256"]
        ):
            raise ValueError("structured schema hash mismatch")
        schema = json.loads(data)
    report = dict(
        status="running",
        qualification=False,
        scope=f"public_test_{args.task}_parity",
        task=args.task,
        entity_types=entity_types if args.task == "entities" else None,
        extraction_schema=schema,
        source_span_policy=args.source_span_policy,
        attention_only=args.attention_only,
        model=pin,
        precision=args.precision,
        dataset=source,
        slices=[],
        native_binary_sha256=hashlib.sha256(args.native.read_bytes()).hexdigest(),
    )
    python = native = None

    def save():
        (args.output / "report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n"
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
                "fp32",
                "--attention",
                "eager",
            ],
            env,
            args.output,
            guard,
        )
        ready = python.receive(1200)
        if (
            ready.get("event") != "ready"
            or ready["model_files"] != pin["files"]
            or ready["dtype"] != "fp32"
            or ready.get("weight_dtype", "fp32") != "fp32"
        ):
            raise ValueError("reference identity mismatch")
        report["reference"] = ready
        save()
        with (args.output / "raw.jsonl").open("w") as raw:
            for locale in locales:
                path = FIXTURES / "holdout/cases" / (locale + ".json")
                data = path.read_bytes()
                fixture = json.loads(data)
                expected = json.loads((FIXTURES / "manifest.json").read_text())[
                    "fixtures"
                ][str(path.relative_to(FIXTURES))]
                if (
                    len(data) != expected["size_bytes"]
                    or hashlib.sha256(data).hexdigest() != expected["sha256"]
                ):
                    raise ValueError("holdout fixture hash mismatch")
                if (
                    fixture["revision"] != source["revision"]
                    or fixture["split"] != "test"
                    or fixture["locale"] != locale
                ):
                    raise ValueError("holdout provenance mismatch")
                cases = fixture["cases"]
                if schema is not None:
                    cases = [
                        dict(
                            id=c["id"],
                            texts=c["texts"],
                            source_ids=c["source_ids"],
                            schema=schema,
                        )
                        for c in cases
                    ]
                directory = args.output / locale
                directory.mkdir()
                command = [
                    str(args.native.resolve()),
                    "--model-dir",
                    str(args.model_dir),
                    "--precision",
                    args.precision,
                ]
                if args.model == "decide_1b":
                    native_path = directory / "native-cases.json"
                    native_path.write_text(
                        json.dumps(decision_worker_fixture(cases), ensure_ascii=False)
                        + "\n"
                    )
                    # This worker reads its fixture before each command. Reuse
                    # the loaded model across languages; update the active
                    # fixture only while the worker is idle between commands,
                    # retaining each language's exact bytes alongside its log.
                    active_path = args.output / "active-native-cases.json"
                    active_path.write_bytes(native_path.read_bytes())
                    command += [
                        "--backend",
                        "cuda",
                        "--cases",
                        str(active_path),
                        "--worker",
                        "1",
                    ]
                else:
                    metadata = json.loads(
                        (FIXTURES / args.model / "cases.json").read_text()
                    )
                    metadata["cases"] = [
                        dict(
                            id=case["id"],
                            texts=case["texts"],
                            schema=cpu.adaptation.schema_for(
                                dict(kind="extract", schema=case["schema"])
                            )
                            if "schema" in case
                            else dict(
                                classifications=[
                                    dict(name=name, labels=labels)
                                    for name, labels in case["tasks"].items()
                                ]
                            ),
                        )
                        for case in cases
                    ]
                    boundary_path = directory / "native-cases.json"
                    boundary_path.write_text(
                        json.dumps(metadata, ensure_ascii=False) + "\n"
                    )
                    command += [
                        "--cases",
                        str(boundary_path),
                        "--threads",
                        "1",
                        "--max-text-words",
                        "4096",
                        "--max-sequence-tokens",
                        "2048",
                        "--attention-only",
                        str(int(args.attention_only)),
                    ]
                if native is None:
                    native = cpu.Worker(
                        "native",
                        command,
                        env,
                        args.output if args.model == "decide_1b" else directory,
                        guard,
                    )
                    ready = native.receive(1200)
                if (
                    ready.get("event") != "ready"
                    or ready.get("backend") != "cuda"
                    or ready.get("precision") != args.precision
                ):
                    raise ValueError("native identity mismatch")
                expected_policy = (
                    "fp32"
                    if args.precision == "fp32"
                    else (
                        "fp16_attention"
                        if args.attention_only
                        else "fp16_encoder_matrices_and_attention"
                    )
                )
                if (
                    args.model != "decide_1b"
                    and ready.get("compute_policy") != expected_policy
                ):
                    raise ValueError("native compute policy mismatch")
                rows = []
                coverage = dict(entities=0, attributes=0, relations=0, records=0)
                for index, case in enumerate(cases):
                    payload = {
                        key: case[key]
                        for key in ("texts", "tasks", "schema")
                        if key in case
                    }
                    reference = request(python, dict(op="validate", **payload), 1200)
                    if schema is not None:
                        counted = reference["outputs"]
                        if args.source_span_policy == "original_text":
                            counted = [
                                original_text_reference(text, output)[0]
                                for text, output in zip(
                                    case["texts"], counted, strict=True
                                )
                            ]
                        for head, count in extraction_coverage(counted).items():
                            coverage[head] += count
                    key = (
                        dict(case_index=index)
                        if args.model == "decide_1b"
                        else dict(case_id=case["id"])
                    )
                    actual = request(native, dict(op="validate", **key), 1200)
                    raw.write(
                        json.dumps(
                            dict(
                                locale=locale,
                                case=case["id"],
                                native=actual,
                                reference=reference,
                            ),
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    raw.flush()
                    rows += compare_rows(
                        case,
                        actual,
                        reference,
                        args.model,
                        5e-4 if args.precision == "fp32" else 5e-3,
                        args.source_span_policy,
                    )
                result = dict(
                    locale=locale,
                    fixture_sha256=hashlib.sha256(data).hexdigest(),
                    native=ready,
                    **slice_summary(rows),
                    rows=rows,
                )
                if schema is not None:
                    result["reference_prediction_counts"] = coverage
                if args.source_span_policy == "original_text":
                    result["strict_comparison"] = slice_summary(
                        [row["strict_comparison"] for row in rows]
                    )
                    result["excluded_reference_predictions"] = sum(
                        len(row["reference_exclusions"]) for row in rows
                    )
                report["slices"].append(result)
                save()
                print(
                    f"{locale}: {result['matched']}/{result['documents']} documents match",
                    flush=True,
                )
                if args.model != "decide_1b":
                    native.close()
                    native = None
        coverage_passed = True
        if args.task == "structured":
            counts = {
                head: sum(
                    s["reference_prediction_counts"][head] for s in report["slices"]
                )
                for head in ("entities", "attributes", "relations", "records")
            }
            coverage_passed = all(counts.values())
            report.update(
                reference_prediction_counts=counts, all_heads_exercised=coverage_passed
            )
        report.update(
            status="complete",
            measured_slices_passed=coverage_passed
            and all(s["passed"] for s in report["slices"]),
            peak_worker_rss_bytes=guard.peak_rss_bytes,
        )
        if args.source_span_policy == "original_text":
            report.update(
                strict_slices_passed=all(
                    s["strict_comparison"]["passed"] for s in report["slices"]
                ),
                excluded_reference_predictions=sum(
                    s["excluded_reference_predictions"] for s in report["slices"]
                ),
            )
        save()
        if not report["measured_slices_passed"]:
            raise SystemExit(1)
    except Exception as error:
        report.update(status="failed", measured_slices_passed=False, failure=str(error))
        save()
        raise
    finally:
        if native is not None:
            native.close()
        if python is not None:
            python.close()


if __name__ == "__main__":
    main()
