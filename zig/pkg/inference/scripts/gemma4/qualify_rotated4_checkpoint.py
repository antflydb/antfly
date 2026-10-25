#!/usr/bin/env python3
"""Experimental rotated4 full-model qualification; public preparation stays gated.

Questions are appended only after independently preparing FP16 and rotated4
checkpoints. Existing full-KV/fitter failures are separate from codec regressions.
The listener enables an explicit experimental gate for this process only.
"""

import argparse
from collections import Counter
import copy
import http.client
import json
import math
from pathlib import Path
import time

from native_checkpoint_transport import receive_checkpoint, send_resume
from native_phase_timing import (
    generation_timing,
    request_log,
    latency_sampling_qualified,
)
from native_quality_score import evaluate, score
from qualify_attention_match_queries import validate_quantization_evidence
from qualify_matched_antfly import read_timed_response
from qualify_matched_llama import percentiles, require_idle
from qualify_native_checkpoint_http import (
    generate,
    git_provenance,
    read_response,
    response_headers,
    server,
    sha256_file,
)

ROOT = Path(__file__).resolve().parents[5]


def summarize(cases, rows, quality_profile="extended"):
    if quality_profile not in ("bounded", "extended"):
        raise ValueError("unknown quality profile")
    complete = len(rows) == len(cases) and bool(cases)
    selected = cases[: len(rows)]
    if len(rows) > len(cases):
        raise ValueError("extra quality evidence")

    def responses(key):
        return [
            {"fixture": case["case_id"], "body": case["body"], "response": row[key]}
            for case, row in zip(selected, rows)
        ]

    if not rows:
        return {"passed": False, "completed_cases": 0}
    for case, row in zip(selected, rows):
        if row["case_id"] != case["case_id"]:
            raise ValueError("reordered quality evidence")
    codec = evaluate(selected, responses("f16"), responses("rotated4"))
    full = evaluate(selected, responses("full_kv"), responses("rotated4"))
    fitted = evaluate(selected, responses("full_kv"), responses("f16"))
    counts = Counter(case["category"] for case in selected)
    bounded_count_met = (
        len(selected) == 16
        and counts == {"fact": 4, "instruction": 4, "code": 4, "tool_call": 4}
        and all(case["critical"] is True for case in selected)
    )
    sample_count_met = (
        bounded_count_met if quality_profile == "bounded" else codec["sample_count_met"]
    )
    coverage = {}
    publication_mismatches = []
    for case, row in zip(selected, rows):
        metadata = row["checkpoints"]["rotated4"]["metadata"]
        validate_quantization_evidence([metadata], "rotated4", require_actual=False)
        if metadata.get("encoding") != "rotated4" or metadata.get("wire_version") != 4:
            raise ValueError("wrong experimental checkpoint encoding")
        ratio = metadata.get("global_savings_ratio")
        if type(ratio) not in (int, float) or not math.isfinite(ratio):
            raise ValueError("invalid savings ratio")
        if metadata["outcome"] == "compacted":
            if ratio < 1.5 or metadata["validated_heads"] < 1:
                raise ValueError("compacted checkpoint failed validation/savings gate")
            if row["checkpoints"]["f16"]["metadata"]["outcome"] != "compacted":
                publication_mismatches.append(case["case_id"])
        elif metadata["outcome"] == "unchanged_insufficient_savings":
            if ratio != 1 or metadata["validated_heads"] != 0:
                raise ValueError("unchanged checkpoint claims compaction")
        else:
            raise ValueError("unknown checkpoint outcome")
        group = coverage.setdefault(
            case["category"], {"quantized_cases": [], "fallback_cases": []}
        )
        group[
            "quantized_cases"
            if metadata["rotated4_compressed_rows"]
            else "fallback_cases"
        ].append(case["case_id"])
    exercised = set(coverage) == {"fact", "instruction", "code", "tool_call"} and all(
        group["quantized_cases"] for group in coverage.values()
    )
    critical = sum(case["critical"] is True for case in selected)
    return {
        "passed": complete
        and sample_count_met
        and critical >= 16
        and exercised
        and not codec["regression_case_ids"]
        and not publication_mismatches,
        "quality_profile": quality_profile,
        "sample_count_met": sample_count_met,
        "fp16_vs_full_kv": fitted,
        "full_kv_preservation_passed": complete
        and sample_count_met
        and exercised
        and not fitted["regression_case_ids"]
        and not full["regression_case_ids"],
        "completed_cases": len(rows),
        "requested_cases": len(cases),
        "publication_policy_mismatch_case_ids": publication_mismatches,
        "vs_fp16": codec,
        "vs_full_kv": full,
        "coverage": coverage,
        "every_category_exercised_rotated4": exercised,
        "row_coverage": {
            key: sum(
                row["checkpoints"]["rotated4"]["metadata"]["rotated4_" + key]
                for row in rows
            )
            for key in ("eligible_rows", "compressed_rows", "exact_rows")
        },
        "scope": "Zero new codec failures versus independent FP16; existing model/fitter failures reported separately",
    }


def timing_percentiles(values, allow_zero=False):
    result = percentiles(values, allow_zero=allow_zero)
    result["p99"] = sorted(values)[math.ceil(0.99 * len(values)) - 1]
    return result


def latency_eligible(case, row):
    """Select by correctness/coverage before measuring, never by observed speed."""
    if case["category"] == "tool_call":
        return False
    checkpoints = row["checkpoints"]
    if any(
        checkpoints[key]["metadata"]["outcome"] != "compacted"
        for key in ("f16", "rotated4")
    ):
        return False
    if not checkpoints["rotated4"]["metadata"]["rotated4_compressed_rows"]:
        return False
    return (
        score(case, row["f16"])
        and score(case, row["rotated4"])
        and row["f16"]["usage"]["completion_tokens"]
        == row["rotated4"]["usage"]["completion_tokens"]
    )


def latency_gate(seconds, qualified_samples):
    """Paired FP16 budget; this does not attest a llama.cpp comparison."""
    if not qualified_samples:
        return {"passed": False, "reason": "insufficient warm samples"}
    comparisons = {}
    for metric in (
        "total_seconds",
        "first_content_seconds",
        "first_content_to_done_seconds",
    ):
        comparisons[metric] = {}
        for percentile, limit in (("p50", 1.25), ("p95", 1.5)):
            baseline = seconds["f16"][metric][percentile]
            candidate = seconds["rotated4"][metric][percentile]
            if any(
                type(x) not in (float, int) or not math.isfinite(x) or x < 0
                for x in (baseline, candidate)
            ):
                raise ValueError("invalid latency evidence")
            comparisons[metric][percentile] = {
                "fp16_seconds": baseline,
                "rotated4_seconds": candidate,
                "ratio_limit": limit,
                "passed": candidate <= baseline * limit,
            }
    return {
        "passed": all(
            item["passed"]
            for metric in comparisons.values()
            for item in metric.values()
        ),
        "comparisons": comparisons,
        "scope": "Paired FP16 checkpoint resume, same model and answer",
    }


def prepare(args, body, encoding, path, record):
    connection = http.client.HTTPConnection(
        "127.0.0.1", args.port, timeout=args.timeout
    )
    started = time.monotonic()
    try:
        connection.request(
            "POST",
            "/ai/v1/generate/context/checkpoint",
            json.dumps(body).encode(),
            {"Content-Type": "application/json", "X-Antfly-KV-Encoding": encoding},
        )
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError(
                f"prepare {encoding}: HTTP {response.status}: {response.read(65536)!r}"
            )
        response_headers(response, record)
        result = receive_checkpoint(response, path, allow_rotated4=True)
        if result["metadata"].get("encoding") != encoding:
            raise AssertionError("wrong requested checkpoint encoding")
        result["prepare_wall_seconds"] = time.monotonic() - started
        return result
    finally:
        connection.close()


def resume(args, body, path, record, timed=False):
    request = copy.deepcopy(body)
    request.pop("context_management", None)
    request["stream"] = timed
    if timed:
        request["stream_options"] = {"include_usage": True}
    connection = http.client.HTTPConnection(
        "127.0.0.1", args.port, timeout=args.timeout
    )
    started = time.monotonic()
    try:
        response = send_resume(
            connection, "/ai/v1/generate/context/resume", request, path
        )
        if response.status != 200:
            raise RuntimeError(
                f"resume: HTTP {response.status}: {response.read(65536)!r}"
            )
        response_headers(response, record)
        return (
            read_timed_response(response, started)
            if timed
            else {
                "result": read_response(response, False),
                "total_seconds": time.monotonic() - started,
            }
        )
    finally:
        connection.close()


def validate_reuse(response, metadata, baseline):
    usage = response["usage"]
    if (
        usage.get("cached_prompt_tokens") != metadata["logical_tokens"]
        or usage["prompt_tokens"] != baseline["usage"]["prompt_tokens"]
        or not 0 < usage["prompt_tokens"] - metadata["logical_tokens"] <= 8192
    ):
        raise AssertionError(
            "checkpoint did not reuse prefix and append the held-out question"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument(
        "--server-kind", choices=("inference", "standalone"), default="inference"
    )
    parser.add_argument("--models-dir", type=Path, required=True)
    parser.add_argument("--suite", type=Path, required=True)
    parser.add_argument("--cases", nargs="+")
    parser.add_argument("--size", choices=("8k", "32k"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8621)
    parser.add_argument("--timeout", type=float, default=1800)
    parser.add_argument(
        "--latency-case",
        default="auto",
        help="Text case ID, or auto for the first eligible case in suite order (default)",
    )
    parser.add_argument("--warmups", type=int, default=5)
    parser.add_argument(
        "--sampling-profile",
        choices=("bounded", "extended"),
        default="bounded",
        help="Bounded uses 20 paired samples; extended uses 100. Neither establishes a universal tail bound.",
    )
    parser.add_argument("--samples", type=int)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument(
        "--quality-profile",
        choices=("bounded", "extended"),
        default="extended",
        help="Bounded requires exactly 16 critical cases, four per category; extended requires 64.",
    )
    parser.add_argument(
        "--require-full-kv-preservation",
        action="store_true",
        help="Also reject new FP16-compaction failures versus full KV; shared model failures remain reported.",
    )
    args = parser.parse_args()
    if args.samples is None:
        args.samples = {"bounded": 20, "extended": 100}[args.sampling_profile]
    if args.output.exists():
        raise FileExistsError(args.output)
    if (
        args.samples < 1
        or args.warmups < 0
        or not math.isfinite(args.timeout)
        or args.timeout <= 0
    ):
        parser.error("invalid sampling/timeout settings")
    if not args.smoke and not latency_sampling_qualified(
        args.sampling_profile, args.warmups, args.samples
    ):
        parser.error(
            "latency qualification requires five warmups and at least 20 bounded or 100 extended samples"
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    cases = [
        json.loads(line) for line in args.suite.read_text().splitlines() if line.strip()
    ]
    if len({case["case_id"] for case in cases}) != len(cases):
        raise ValueError("duplicate case IDs")
    if args.cases:
        cases = [case for case in cases if case["case_id"] in args.cases]
        if {case["case_id"] for case in cases} != set(args.cases):
            raise ValueError("unknown case")
    if not cases or any("checkpoint_body" not in case for case in cases):
        raise ValueError("requires nonempty appended-question suite")
    if args.latency_case != "auto" and args.latency_case not in {
        case["case_id"] for case in cases
    }:
        raise ValueError("latency case must be in selected suite")
    args.experimental_rotated4 = True
    args.generation_timing = True
    evidence = {
        "passed": False,
        "production_qualified": False,
        "experimental": True,
        "size": args.size,
        "smoke": args.smoke,
        "source": git_provenance(ROOT),
        "quality_profile": args.quality_profile,
        "require_full_kv_preservation": args.require_full_kv_preservation,
        "sampling_profile": args.sampling_profile,
        "warmups_requested": args.warmups,
        "samples_requested": args.samples,
        "sampling_scope": "Observed paired percentiles for this workload; bounded sampling does not establish extended-run tail reliability.",
        "binary_sha256": sha256_file(args.server),
        "suite_sha256": sha256_file(args.suite),
        "case_ids": [c["case_id"] for c in cases],
        "rows": [],
        "latency": {},
        "latency_selection": args.latency_case,
        "timing_scope": "HTTP wall time and pipeline wall phases. Restore prefill includes suffix execution; no isolated GPU-event timing.",
    }
    paths = {
        encoding: args.output.with_suffix("." + encoding + ".kv")
        for encoding in ("f16", "rotated4")
    }
    if any(path.exists() for path in paths.values()):
        raise FileExistsError("checkpoint evidence path exists")

    def publish():
        partial = args.output.with_suffix(".json.part")
        partial.write_text(json.dumps(evidence, indent=2) + "\n")
        partial.replace(args.output)

    try:
        require_idle()
        with server(args, "worker", evidence) as record:
            for case in cases:
                evidence["active_case"] = case["case_id"]
                publish()
                row = {"case_id": case["case_id"], "checkpoints": {}}
                evidence["active_row"] = row
                row["full_kv"] = generate(args, case["body"], record)
                prompt_tokens = row["full_kv"]["usage"]["prompt_tokens"]
                if not (
                    (7500 <= prompt_tokens <= 9000)
                    if args.size == "8k"
                    else (30000 <= prompt_tokens <= 32768)
                ):
                    raise AssertionError(
                        "request length outside declared context bucket"
                    )
                prepare_body = copy.deepcopy(case["checkpoint_body"])
                prepare_body.pop("context_management", None)
                consider_latency = not evidence["latency"] and (
                    case["case_id"] == args.latency_case
                    or (args.latency_case == "auto" and case["category"] != "tool_call")
                )
                for encoding in ("f16", "rotated4"):
                    print(
                        json.dumps(
                            {
                                "case": case["case_id"],
                                "phase": "prepare",
                                "encoding": encoding,
                            }
                        ),
                        flush=True,
                    )
                    checkpoint = prepare(
                        args, prepare_body, encoding, paths[encoding], record
                    )
                    row["checkpoints"][encoding] = checkpoint
                    result = resume(args, case["body"], paths[encoding], record)
                    row[encoding] = result["result"]
                    row[encoding + "_resume_seconds"] = result["total_seconds"]
                    validate_reuse(
                        row[encoding], checkpoint["metadata"], row["full_kv"]
                    )
                    publish()
                    if encoding == "f16" and (
                        checkpoint["metadata"]["outcome"] != "compacted"
                        or not score(case, row["f16"])
                    ):
                        consider_latency = False
                    if not consider_latency:
                        # A full-KV fallback can exceed 500 MiB at 32K. Quality
                        # needs only this completed restore and its digest.
                        paths[encoding].unlink()
                if (
                    row["checkpoints"]["f16"]["identity"]
                    != row["checkpoints"]["rotated4"]["identity"]
                ):
                    raise AssertionError(
                        "independent checkpoints use different model revisions"
                    )
                evidence["rows"].append(row)
                evidence.pop("active_row", None)
                evidence["quality"] = summarize(
                    cases, evidence["rows"], args.quality_profile
                )
                publish()
                eligible = latency_eligible(case, row)
                if case["case_id"] == args.latency_case and not eligible:
                    raise AssertionError(
                        "latency fixture requires correct equally long answers and actual compaction in both encodings"
                    )
                if consider_latency and eligible:
                    validate_quantization_evidence(
                        [row["checkpoints"]["rotated4"]["metadata"]], "rotated4"
                    )
                    observations = {encoding: [] for encoding in paths}
                    evidence["latency"]["case_id"] = case["case_id"]
                    evidence["latency"]["selection_scope"] = (
                        "First eligible text case in suite order; no latency-based selection"
                        if args.latency_case == "auto"
                        else "Explicit case ID"
                    )
                    evidence["latency"]["observations"] = observations
                    for index in range(1 + args.warmups + args.samples):
                        for encoding in (
                            ("f16", "rotated4")
                            if index % 2 == 0
                            else ("rotated4", "f16")
                        ):
                            log_path = args.output.with_suffix(".worker.log")
                            offset = log_path.stat().st_size
                            item = resume(
                                args, case["body"], paths[encoding], record, timed=True
                            )
                            validate_reuse(
                                item["result"],
                                row["checkpoints"][encoding]["metadata"],
                                row["full_kv"],
                            )
                            if (
                                not score(case, item["result"])
                                or item["result"]["usage"] != row[encoding]["usage"]
                            ):
                                raise AssertionError(
                                    "latency sample changed answer or output length"
                                )
                            item.update(
                                initial_repeat=index == 0,
                                warmup=0 < index <= args.warmups,
                                index=index,
                            )
                            item["pipeline_timing_ms"] = generation_timing(
                                request_log(log_path, offset), item["total_seconds"]
                            )
                            observations[encoding].append(item)
                            publish()
                    evidence["latency"]["seconds"] = {
                        encoding: {
                            key: timing_percentiles(
                                [
                                    item[key]
                                    for item in items
                                    if not item["initial_repeat"] and not item["warmup"]
                                ],
                                allow_zero=key == "first_content_to_done_seconds",
                            )
                            for key in (
                                "total_seconds",
                                "first_content_seconds",
                                "first_content_to_done_seconds",
                            )
                        }
                        for encoding, items in observations.items()
                    }
                    evidence["latency"]["sampling_qualified"] = (
                        latency_sampling_qualified(
                            args.sampling_profile,
                            args.warmups,
                            args.samples,
                            args.smoke,
                        )
                    )
                    evidence["latency"]["gate"] = latency_gate(
                        evidence["latency"]["seconds"],
                        evidence["latency"]["sampling_qualified"],
                    )
                for path in paths.values():
                    path.unlink(missing_ok=True)
                print(
                    json.dumps(
                        {
                            "case": case["case_id"],
                            "phase": "complete",
                            "rotated4_heads": row["checkpoints"]["rotated4"][
                                "metadata"
                            ]["rotated4_heads"],
                            "fp16_correct": score(case, row["f16"]),
                            "rotated4_correct": score(case, row["rotated4"]),
                        }
                    ),
                    flush=True,
                )
            evidence.pop("active_case", None)
        memory = evidence["worker"]["memory"]
        if memory["samples"] < 2 or memory["errors"] or not memory["gpu_uuids"]:
            raise AssertionError("incomplete memory evidence")
        if (
            not 0 < memory["peak_host_rss_bytes"] <= 8 * 1024**3
            or not 0 < memory["peak_gpu_memory_bytes"] <= 18 * 1024**3
        ):
            raise AssertionError("qualification memory cap exceeded")
        if (
            git_provenance(ROOT) != evidence["source"]
            or sha256_file(args.server) != evidence["binary_sha256"]
            or sha256_file(args.suite) != evidence["suite_sha256"]
        ):
            raise AssertionError(
                "source, binary, or suite changed during qualification"
            )
        evidence["passed"] = evidence["quality"]["passed"] and evidence["latency"].get(
            "gate", {}
        ).get("passed", False)
        if args.require_full_kv_preservation:
            evidence["passed"] = (
                evidence["passed"]
                and evidence["quality"]["full_kv_preservation_passed"]
            )
    except (Exception, KeyboardInterrupt) as error:
        evidence["error"] = f"{type(error).__name__}: {error}"
    finally:
        for path in paths.values():
            path.unlink(missing_ok=True)
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
