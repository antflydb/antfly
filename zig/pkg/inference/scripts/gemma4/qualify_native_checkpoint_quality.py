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

"""Score the fixed quality suite through the public native-checkpoint SDK.

Run with the SDK venv. Unchanged fallback is reported separately from successful
compaction. This harness does not establish the separate sampling/stress gates.
"""

import argparse
import copy
import json
import math
import sys
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "py/packages/sdk/src"))
from antfly import AntflyClient
from antfly.client_generated.models import InferenceGenerateRequest
from native_quality_score import evaluate, score, validate_request_models
from qualify_attention_match_queries import validate_quantization_evidence
from qualify_native_checkpoint_http import (
    server,
    generate,
    git_provenance,
    metal_ledger_finish,
    metal_ledger_start,
    normalize_request_body,
    sha256_file,
    validate_backend_configuration,
    validate_metal_memory,
)


def resident_representation(metadata, encoding):
    """Keep wire encoding distinct from the codec that survived validation."""
    if (
        encoding not in ("f16", "fp8", "rotated4")
        or metadata.get("encoding") != encoding
    ):
        raise ValueError("checkpoint encoding differs from requested quality campaign")
    if encoding == "f16":
        return "f16"
    validate_quantization_evidence([metadata], encoding, require_actual=False)
    return encoding if metadata[encoding + "_heads"] else "f16"


def coverage(cases, baseline, resumed, checkpoints, encoding=None):
    if not (len(cases) == len(baseline) == len(resumed) == len(checkpoints)):
        raise ValueError("incomplete quality coverage")
    categories = {}
    for case, before, after, checkpoint in zip(cases, baseline, resumed, checkpoints):
        if checkpoint["case_id"] != case["case_id"]:
            raise ValueError("changed or reordered checkpoint coverage")
        metadata = checkpoint["metadata"]
        if encoding is not None and metadata.get("encoding") != encoding:
            raise ValueError(
                "checkpoint encoding differs from requested quality campaign"
            )
        fp8 = metadata.get("fp8_heads", 0)
        fallback = metadata.get("fp8_fallback_heads", 0)
        if any(type(count) is not int or count < 0 for count in (fp8, fallback)):
            raise ValueError("invalid FP8 head accounting")
        if metadata.get("encoding") == "fp8":
            if (
                "fp8_heads" not in metadata
                or "fp8_fallback_heads" not in metadata
                or fp8 + fallback != metadata["validated_heads"]
            ):
                raise ValueError("missing or inconsistent FP8 validation accounting")
        elif fp8 or fallback:
            raise ValueError("non-FP8 checkpoint claims quantization")
        rotated4 = metadata.get("rotated4_compressed_rows", 0)
        rotated4_eligible = metadata.get("rotated4_eligible_rows", 0)
        rotated4_exact = metadata.get("rotated4_exact_rows", 0)
        if any(
            type(count) is not int or count < 0
            for count in (rotated4, rotated4_eligible, rotated4_exact)
        ):
            raise ValueError("invalid rotated4 row accounting")
        if metadata.get("encoding") == "rotated4":
            try:
                validate_quantization_evidence(
                    [metadata],
                    "rotated4",
                    require_actual=metadata.get("outcome") == "compacted",
                )
            except AssertionError as error:
                raise ValueError(str(error)) from error
            if (
                not all(
                    field in metadata
                    for field in (
                        "rotated4_compressed_rows",
                        "rotated4_eligible_rows",
                        "rotated4_exact_rows",
                    )
                )
                or rotated4 + rotated4_exact != rotated4_eligible
            ):
                raise ValueError("missing or inconsistent rotated4 row accounting")
        elif rotated4 or rotated4_eligible or rotated4_exact:
            raise ValueError("non-rotated4 checkpoint claims rotated4 rows")
        if not math.isfinite(metadata["global_savings_ratio"]):
            raise ValueError("nonfinite savings ratio")
        outcome = metadata["outcome"]
        if outcome not in ("compacted", "unchanged_insufficient_savings"):
            raise ValueError("unknown checkpoint outcome")
        if outcome == "compacted" and (
            metadata["global_savings_ratio"] < 1.5 or metadata["validated_heads"] < 1
        ):
            raise ValueError("claimed compaction failed savings/validation threshold")
        if outcome == "unchanged_insufficient_savings" and (
            metadata["global_savings_ratio"] != 1 or metadata["validated_heads"] != 0
        ):
            raise ValueError("fallback incorrectly claims compaction")
        group = categories.setdefault(
            case["category"],
            {
                "compacted": 0,
                "unchanged_insufficient_savings": 0,
                "compacted_baseline_correct": 0,
                "compacted_resume_correct": 0,
                "compacted_case_ids": [],
                "fallback_case_ids": [],
                "fp8_cases": 0,
                "fp8_case_ids": [],
                "fp8_regression_case_ids": [],
                "fp8_baseline_correct": 0,
                "fp8_resume_correct": 0,
                "rotated4_cases": 0,
                "rotated4_case_ids": [],
                "rotated4_regression_case_ids": [],
            },
        )
        if fp8:
            group["fp8_cases"] += 1
            group["fp8_case_ids"].append(case["case_id"])
            before_correct = score(case, before["response"])
            after_correct = score(case, after["response"])
            group["fp8_baseline_correct"] += before_correct
            group["fp8_resume_correct"] += after_correct
            if before_correct and not after_correct:
                group["fp8_regression_case_ids"].append(case["case_id"])
        if rotated4:
            group["rotated4_cases"] += 1
            group["rotated4_case_ids"].append(case["case_id"])
            if score(case, before["response"]) and not score(case, after["response"]):
                group["rotated4_regression_case_ids"].append(case["case_id"])
        group[outcome] += 1
        if outcome == "compacted":
            group["compacted_baseline_correct"] += score(case, before["response"])
            group["compacted_resume_correct"] += score(case, after["response"])
            group["compacted_case_ids"].append(case["case_id"])
        else:
            group["fallback_case_ids"].append(case["case_id"])
    for group in categories.values():
        group["compacted_drop_percentage_points"] = (
            100
            * (group["compacted_baseline_correct"] - group["compacted_resume_correct"])
            / group["compacted"]
            if group["compacted"]
            else None
        )
    count = sum(g["compacted"] for g in categories.values())
    drop = (
        100
        * sum(
            g["compacted_baseline_correct"] - g["compacted_resume_correct"]
            for g in categories.values()
        )
        / count
        if count
        else None
    )
    exercised = bool(categories) and all(
        g["compacted"] > 0 for g in categories.values()
    )
    fp8_exercised = bool(categories) and all(
        g["fp8_cases"] > 0 for g in categories.values()
    )
    rotated4_exercised = bool(categories) and all(
        g["rotated4_cases"] > 0 for g in categories.values()
    )
    return {
        "categories": categories,
        "outcomes": dict(Counter(c["metadata"]["outcome"] for c in checkpoints)),
        "every_category_exercised_compaction": exercised,
        "every_category_exercised_fp8": fp8_exercised,
        "fp8_quality_passed": fp8_exercised
        and not any(g["fp8_regression_case_ids"] for g in categories.values()),
        "every_category_exercised_rotated4": rotated4_exercised,
        "rotated4_quality_passed": rotated4_exercised
        and not any(g["rotated4_regression_case_ids"] for g in categories.values()),
        "compacted_drop_percentage_points": drop,
        "compacted_quality_passed": exercised
        and drop <= 2
        and all(
            g["compacted_drop_percentage_points"] <= 5 for g in categories.values()
        ),
    }


def completed_summary(cases, baseline, resumed, checkpoints):
    """Report only complete pairs; an interrupted run cannot satisfy full gates."""
    count = len(resumed)
    if count > len(cases) or count > len(baseline) or count > len(checkpoints):
        raise ValueError("incomplete paired quality evidence")
    if not count:
        return {"completed_cases": 0, "requested_cases": len(cases)}
    return {
        "completed_cases": count,
        "requested_cases": len(cases),
        "quality": evaluate(cases[:count], baseline[:count], resumed),
        "coverage": coverage(
            cases[:count], baseline[:count], resumed, checkpoints[:count]
        ),
    }


def bounded_quality(cases, baseline, resumed, checkpoints, fp16_reference=None):
    """User-approved 16-case gate; no added failures versus full KV.

    Shared model failures remain visible. Exact fallbacks never satisfy the
    per-category compaction requirement. FP8 also needs its separate codec gate.
    """
    counts = Counter(case["category"] for case in cases)
    count_met = (
        len(cases) == 16
        and counts == {"fact": 4, "instruction": 4, "code": 4, "tool_call": 4}
        and all(case["critical"] is True for case in cases)
    )
    quality = evaluate(cases, baseline, resumed)
    exercised = coverage(cases, baseline, resumed, checkpoints)
    fitted = (
        evaluate(cases, baseline, fp16_reference)
        if fp16_reference is not None
        else None
    )
    return {
        "quality_profile": "bounded",
        "sample_count_met": count_met,
        "passed": count_met
        and exercised["every_category_exercised_compaction"]
        and not quality["regression_case_ids"]
        and (fitted is None or not fitted["regression_case_ids"]),
        "baseline_failure_case_ids": quality["baseline_failure_case_ids"],
        "regression_case_ids": quality["regression_case_ids"],
        "fp16_vs_full_kv": fitted,
        "scope": "16 critical cases, four per category; zero new failures versus full KV; shared model failures reported separately",
    }


def fp8_noninferiority(
    cases, reference, resumed, checkpoints, quality_profile="extended", encoding="fp8"
):
    """Separate codec regressions from existing model/fitter failures.

    This report does not relax the runner's whole-system release gate.
    """
    quality = evaluate(cases, reference, resumed)
    if encoding not in ("fp8", "rotated4"):
        raise ValueError("unsupported quantized noninferiority encoding")
    exercised = coverage(cases, reference, resumed, checkpoints, encoding=encoding)
    critical_count = sum(case["critical"] is True for case in cases)
    if quality_profile not in ("bounded", "extended"):
        raise ValueError("unknown quality profile")
    counts = Counter(case["category"] for case in cases)
    count_met = (
        quality["sample_count_met"]
        if quality_profile == "extended"
        else (
            len(cases) == 16
            and counts == {"fact": 4, "instruction": 4, "code": 4, "tool_call": 4}
            and critical_count == 16
        )
    )
    return {
        "scope": f"Added {encoding} failures versus separately prepared FP16, not absolute model accuracy or full production qualification",
        "passed": count_met
        and critical_count >= 16
        and exercised[encoding + "_quality_passed"]
        and not quality["regression_case_ids"],
        "sample_count_met": count_met,
        "quality_profile": quality_profile,
        "critical_cases": critical_count,
        "reference_failure_case_ids": quality["baseline_failure_case_ids"],
        "regression_case_ids": quality["regression_case_ids"],
        "coverage": exercised,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=["cuda", "metal"], default="cuda")
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument(
        "--server-kind", choices=["inference", "standalone", "metal-test"], default="inference"
    )
    parser.add_argument("--models-dir", type=Path, default=Path(".models"))
    parser.add_argument("--model", default="google/gemma-4-E4B-it-qat-q4_0-gguf")
    parser.add_argument(
        "--suite",
        type=Path,
        required=True,
        help="frozen JSONL written by prepare_native_quality_suite.py",
    )
    parser.add_argument("--cases", nargs="+")
    parser.add_argument(
        "--quality-profile",
        choices=("bounded", "extended"),
        default="extended",
        help="Bounded requires 16 critical cases, four per category, and zero new failures versus full KV.",
    )
    parser.add_argument(
        "--encoding", choices=["f32", "f16", "fp8", "rotated4"], default="f16"
    )
    parser.add_argument(
        "--size",
        choices=("8k", "32k"),
        help="Required context bucket for bounded qualification",
    )
    parser.add_argument("--port", type=int, default=8506)
    parser.add_argument("--timeout", type=float, default=1800)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    args = parser.parse_args()
    args.experimental_rotated4 = args.encoding == "rotated4"
    try:
        validate_backend_configuration(args)
    except ValueError as error:
        parser.error(str(error))
    if args.quality_profile == "bounded" and args.size is None:
        parser.error("bounded qualification requires --size")
    if args.output.exists() or args.checkpoint.exists():
        raise FileExistsError(
            "use new evidence/checkpoint paths; preserve prior results"
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.checkpoint.parent.mkdir(parents=True, exist_ok=True)
    cases = [
        json.loads(line) for line in args.suite.read_text().splitlines() if line.strip()
    ]
    validate_request_models(cases, args.model)
    if len({c["case_id"] for c in cases}) != len(cases):
        raise ValueError("duplicate case IDs")
    if args.cases:
        wanted = set(args.cases)
        cases = [c for c in cases if c["case_id"] in wanted]
        if {c["case_id"] for c in cases} != wanted:
            raise ValueError("unknown case")
    if not cases:
        raise ValueError("empty suite")
    evidence = {
        "passed": False,
        "production_ready": False,
        "scope": "Public API/SDK quality with explicit unchanged fallback; excludes sustained and latency sampling gates",
        "server_kind": args.server_kind,
        "encoding": args.encoding,
        "quality_profile": args.quality_profile,
        "size": args.size,
        "source": git_provenance(ROOT),
        "binary_sha256": sha256_file(args.server),
        "suite_sha256": sha256_file(args.suite),
        "case_ids": [c["case_id"] for c in cases],
        "sources": {
            str(p): sha256_file(p)
            for p in [
                Path(__file__),
                Path(__file__).with_name("native_quality_score.py"),
                Path(__file__).with_name("qualify_native_checkpoint_http.py"),
            ]
        },
        "baseline": [],
        "resumed": [],
        "checkpoints": [],
        "fp16_reference": [],
    }
    if args.backend == "metal":
        evidence.update(
            backend="metal",
            qualification_profile="metal_test_native_checkpoint_v1",
            production_ready=False,
            timing_scope="diagnostic_wall_intervals_not_matched_performance",
            execution_mode="eager",
        )

    def publish():
        temporary = args.output.with_suffix(".json.part")
        temporary.write_text(json.dumps(evidence, indent=2) + "\n")
        temporary.replace(args.output)

    try:
        with server(args, "worker", evidence) as record:
            client = AntflyClient(f"http://127.0.0.1:{args.port}", timeout=args.timeout)
            http = client._client.get_httpx_client()

            def attest(response):
                if response.status_code == 200:
                    if response.headers.get("x-antfly-inference-backend") != args.backend:
                        raise AssertionError("missing " + args.backend + " backend")
                    if (
                        int(response.headers.get("x-antfly-inference-pid", "0"))
                        != record["worker_pid"]
                    ):
                        raise AssertionError("wrong worker PID")

            http.event_hooks["response"].append(attest)
            try:
                for case in cases:
                    started = time.monotonic()
                    evidence["active_case"] = case["case_id"]
                    publish()
                    print(
                        json.dumps({"case": case["case_id"], "phase": "baseline"}),
                        flush=True,
                    )
                    baseline = generate(args, case["body"], record)
                    if args.size is not None:
                        tokens = baseline["usage"]["prompt_tokens"]
                        if not (
                            (7500 <= tokens <= 9000)
                            if args.size == "8k"
                            else (30000 <= tokens <= 32768)
                        ):
                            raise AssertionError(
                                "request length outside declared context bucket"
                            )
                    evidence["baseline"].append(
                        {
                            "fixture": case["case_id"],
                            "body": case["body"],
                            "response": baseline,
                        }
                    )
                    body = normalize_request_body(args, copy.deepcopy(case["body"]))
                    body.pop("context_management", None)
                    request = InferenceGenerateRequest.from_dict(body)
                    prepare_body = normalize_request_body(
                        args,
                        copy.deepcopy(case.get("checkpoint_body", case["body"])),
                    )
                    prepare_body.pop("context_management", None)
                    prepare_request = InferenceGenerateRequest.from_dict(prepare_body)
                    if args.encoding in ("fp8", "rotated4"):
                        # Compare to the same fitted checkpoint at serving precision,
                        # as well as full KV, to isolate added quantization failures.
                        print(
                            json.dumps(
                                {"case": case["case_id"], "phase": "fp16_reference"}
                            ),
                            flush=True,
                        )
                        ledger_offset = metal_ledger_start(args, record)
                        fp16 = client.prepare_native_kv_checkpoint(
                            prepare_request, args.checkpoint, encoding="f16"
                        )
                        metal_ledger_finish(
                            args,
                            record,
                            ledger_offset,
                            "checkpoint_prepare",
                            require_binding=False,
                            required_phase="checkpoint_source",
                        )
                        if (
                            args.backend == "metal"
                            and dict(fp16.identity)
                            != record["ready"]["loaded_identity"]
                        ):
                            raise AssertionError(
                                "FP16 checkpoint identity differs from loaded Metal model"
                            )
                        ledger_offset = metal_ledger_start(args, record)
                        reference_result = client.resume_native_kv_checkpoint(
                            request, fp16
                        ).to_dict()
                        metal_ledger_finish(
                            args,
                            record,
                            ledger_offset,
                            "checkpoint_resume",
                            require_binding=True,
                            expected_representation="f16",
                        )
                        if (
                            reference_result["usage"]["cached_prompt_tokens"]
                            != fp16.metadata["logical_tokens"]
                            or reference_result["usage"]["prompt_tokens"]
                            != baseline["usage"]["prompt_tokens"]
                        ):
                            raise AssertionError(
                                "FP16 reference did not reuse the same prefix"
                            )
                        evidence["fp16_reference"].append(
                            {
                                "fixture": case["case_id"],
                                "body": case["body"],
                                "response": reference_result,
                                "metadata": dict(fp16.metadata),
                                "sha256": fp16.sha256,
                            }
                        )
                    print(
                        json.dumps({"case": case["case_id"], "phase": "prepare"}),
                        flush=True,
                    )
                    prepare_start = time.monotonic()
                    ledger_offset = metal_ledger_start(args, record)
                    checkpoint = client.prepare_native_kv_checkpoint(
                        prepare_request, args.checkpoint, encoding=args.encoding
                    )
                    metal_ledger_finish(
                        args,
                        record,
                        ledger_offset,
                        "checkpoint_prepare",
                        require_binding=False,
                        required_phase="checkpoint_source",
                    )
                    if (
                        args.backend == "metal"
                        and dict(checkpoint.identity)
                        != record["ready"]["loaded_identity"]
                    ):
                        raise AssertionError("checkpoint identity differs from loaded Metal model")
                    if checkpoint.metadata.get("encoding") != args.encoding:
                        raise AssertionError("server did not honor requested encoding")
                    appended_tokens = (
                        baseline["usage"]["prompt_tokens"]
                        - checkpoint.metadata["logical_tokens"]
                    )
                    if (
                        "checkpoint_body" in case and not 0 < appended_tokens <= 8192
                    ) or ("checkpoint_body" not in case and appended_tokens != 0):
                        raise AssertionError(
                            "invalid checkpoint/question token boundary"
                        )
                    evidence["checkpoints"].append(
                        {
                            "case_id": case["case_id"],
                            "metadata": dict(checkpoint.metadata),
                            "identity": dict(checkpoint.identity),
                            "sha256": checkpoint.sha256,
                            "prepare_wall_seconds": time.monotonic() - prepare_start,
                            "wire_request": prepare_body,
                        }
                    )
                    print(
                        json.dumps(
                            {
                                "case": case["case_id"],
                                "phase": "resume",
                                "outcome": checkpoint.metadata["outcome"],
                            }
                        ),
                        flush=True,
                    )
                    resume_start = time.monotonic()
                    ledger_offset = metal_ledger_start(args, record)
                    result = client.resume_native_kv_checkpoint(
                        request, checkpoint
                    ).to_dict()
                    metal_ledger_finish(
                        args,
                        record,
                        ledger_offset,
                        "checkpoint_resume",
                        require_binding=True,
                        expected_representation=(
                            resident_representation(checkpoint.metadata, args.encoding)
                            if args.backend == "metal"
                            else None
                        ),
                    )
                    if (
                        result["usage"]["cached_prompt_tokens"]
                        != checkpoint.metadata["logical_tokens"]
                    ):
                        raise AssertionError("resume did not attest saved prefix reuse")
                    if (
                        result["usage"]["prompt_tokens"]
                        != baseline["usage"]["prompt_tokens"]
                    ):
                        raise AssertionError(
                            "full-KV and restored requests have different token counts"
                        )
                    evidence["resumed"].append(
                        {
                            "fixture": case["case_id"],
                            "body": case["body"],
                            "response": result,
                            "resume_wall_seconds": time.monotonic() - resume_start,
                            "appended_prompt_tokens": appended_tokens,
                        }
                    )
                    # Reuse bounded disk space. Digests and model identities remain in evidence.
                    args.checkpoint.unlink()
                    evidence["completed_cases"] = len(evidence["resumed"])
                    evidence["completed_summary"] = completed_summary(
                        cases,
                        evidence["baseline"],
                        evidence["resumed"],
                        evidence["checkpoints"],
                    )
                    publish()
                    print(
                        json.dumps(
                            {
                                "case": case["case_id"],
                                "phase": "complete",
                                "seconds": time.monotonic() - started,
                                "baseline_correct": score(case, baseline),
                                "resume_correct": score(case, result),
                            }
                        ),
                        flush=True,
                    )
            finally:
                http.close()
        evidence["quality"] = evaluate(cases, evidence["baseline"], evidence["resumed"])
        evidence["coverage"] = coverage(
            cases, evidence["baseline"], evidence["resumed"], evidence["checkpoints"]
        )
        if args.encoding in ("fp8", "rotated4"):
            evidence[args.encoding + "_vs_fp16"] = evaluate(
                cases, evidence["fp16_reference"], evidence["resumed"]
            )
            evidence[args.encoding + "_coverage"] = coverage(
                cases,
                evidence["fp16_reference"],
                evidence["resumed"],
                evidence["checkpoints"],
                encoding=args.encoding,
            )
            evidence[args.encoding + "_noninferiority"] = fp8_noninferiority(
                cases,
                evidence["fp16_reference"],
                evidence["resumed"],
                evidence["checkpoints"],
                args.quality_profile,
                args.encoding,
            )
        memory = evidence["worker"]["memory"]
        if args.backend == "metal":
            validate_metal_memory(memory)
            if not evidence["worker"].get("graceful_stop") or evidence[
                "worker"
            ].get("remaining_group_members"):
                raise AssertionError("Metal test server did not drain")
        else:
            if memory["samples"] < 2 or memory["errors"] or not memory["gpu_uuids"]:
                raise AssertionError("incomplete memory evidence")
            for key, cap in [
                ("peak_host_rss_bytes", 8 * 1024**3),
                ("peak_gpu_memory_bytes", 18 * 1024**3),
            ]:
                if memory[key] is None or not 0 < memory[key] <= cap:
                    raise AssertionError(f"{key} exceeds qualification cap")
        if git_provenance(ROOT) != evidence["source"]:
            raise AssertionError("source changed")
        if (
            sha256_file(args.server) != evidence["binary_sha256"]
            or sha256_file(args.suite) != evidence["suite_sha256"]
        ):
            raise AssertionError("binary or suite changed")
        for path, digest in evidence["sources"].items():
            if sha256_file(Path(path)) != digest:
                raise AssertionError("harness changed")
        evidence["passed"] = (
            evidence["quality"]["passed"]
            and evidence["coverage"]["compacted_quality_passed"]
        )
        if args.quality_profile == "bounded":
            evidence["bounded_quality"] = bounded_quality(
                cases,
                evidence["baseline"],
                evidence["resumed"],
                evidence["checkpoints"],
                evidence["fp16_reference"]
                if args.encoding in ("fp8", "rotated4")
                else None,
            )
            evidence["passed"] = evidence["bounded_quality"]["passed"]
        if args.encoding in ("fp8", "rotated4"):
            evidence["passed"] = (
                evidence["passed"]
                and evidence[args.encoding + "_noninferiority"]["passed"]
            )
        evidence.pop("active_case", None)
    except (Exception, KeyboardInterrupt) as error:
        evidence["error"] = f"{type(error).__name__}: {error}"
    finally:
        publish()
    print(
        json.dumps(
            {
                "passed": evidence["passed"],
                "error": evidence.get("error"),
                "output": str(args.output),
            }
        ),
        flush=True,
    )
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
