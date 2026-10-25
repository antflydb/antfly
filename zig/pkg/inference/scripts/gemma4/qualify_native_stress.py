"""Repeated public native-KV requests on one real worker, without retries.

The bounded profile uses 100 mixed requests and 10 actual compactions; extended
uses 1,000 requests and 100 compactions. Bounded evidence does not establish
extended-run reliability. Neither profile qualifies diverse task quality,
32K stress, cancellation, physical OOM, concurrent execution, or latency parity.
"""

import argparse
import copy
import json
import math
from pathlib import Path
import statistics
import sys
import time

sys.path.insert(0, str(Path.cwd() / "zig/pkg/inference/scripts/gemma4"))
from qualify_native_checkpoint_http import (
    REPO,
    canonical_choices,
    prepare,
    requests,
    resume,
    server,
    sha256_file,
    git_provenance,
)
from qualify_context_management import sample_cuda_process
from native_checkpoint_unified_host import sum_listener_rss

TEN_REQUESTS = (
    "prepare",
    "resume",
    "sse",
    "wrong_prefix",
    "recovery",
    "checksum",
    "recovery",
    "append",
    "sse",
    "resume",
)


def assert_equivalent(actual, expected):
    if canonical_choices(actual["choices"]) != canonical_choices(expected["choices"]):
        raise AssertionError("deterministic continuation changed")
    if actual.get("usage") != expected.get("usage"):
        raise AssertionError("saved-prefix/append token accounting changed")


def memory_observation(record):
    row = sample_cuda_process(record["worker_pid"])
    if "listener_pid" in record:
        row = sum_listener_rss(row, record["listener_pid"])
    if len(row["gpu_allocations"]) != 1:
        raise AssertionError("expected one GPU allocation for the inference worker")
    row["gpu_memory_bytes"] = row["gpu_allocations"][0]["used_memory_bytes"]
    if not 0 < row["host_rss_bytes"] <= 8 * 1024**3:
        raise AssertionError("host RSS cap exceeded")
    if not 0 < row["gpu_memory_bytes"] <= 18 * 1024**3:
        raise AssertionError("GPU memory cap exceeded")
    return row


def summarize_blocks(blocks, profile="extended"):
    minimum_blocks = {"bounded": 2, "extended": 20}[profile]
    if len(blocks) < minimum_blocks:
        raise AssertionError(f"at least {minimum_blocks} complete blocks required")
    if any(not block.get("passed") for block in blocks):
        raise AssertionError("incomplete or failed stress block")
    if any(
        tuple(op["kind"] for op in block["operations"]) != TEN_REQUESTS * 5
        for block in blocks
    ):
        raise AssertionError("incomplete or reordered mixed request block")
    operations = [op for block in blocks for op in block["operations"]]
    if len(operations) < minimum_blocks * 50 or any(
        not op.get("passed") for op in operations
    ):
        raise AssertionError(
            f"at least {minimum_blocks * 50} successful expected outcomes required"
        )
    compactions = [op for op in operations if op["kind"] == "prepare"]
    if len(compactions) < minimum_blocks * 5 or any(
        op.get("outcome") != "compacted" for op in compactions
    ):
        raise AssertionError(
            f"at least {minimum_blocks * 5} actual compactions required; unchanged fallbacks do not count"
        )
    growth = {}
    window = min(5, len(blocks) // 2)
    for key in ("host_rss_bytes", "gpu_memory_bytes"):
        values = [block["quiescent_memory"][key] for block in blocks]
        first = statistics.median(values[:window])
        last = statistics.median(values[-window:])
        # A conservative diagnostic trigger, additional to the documented hard
        # memory caps. Preserve every block sample to allow independent review.
        growth[key] = {
            "first_window_median": first,
            "last_window_median": last,
            "window_blocks": window,
            "delta_bytes": last - first,
            "maximum_bytes": max(values),
        }
        if last - first > 128 * 1024**2:
            raise AssertionError(
                f"sustained quiescent memory growth: {key}: {last - first}"
            )
    return {
        "mixed_requests": len(operations),
        "actual_compactions": len(compactions),
        "profile": profile,
        "extended_run_qualified": profile == "extended",
        "quiescent_growth": growth,
        "growth_diagnostic_limit_bytes": 128 * 1024**2,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument(
        "--server-kind", choices=["inference", "standalone"], default="standalone"
    )
    parser.add_argument("--models-dir", type=Path, default=Path(".models"))
    parser.add_argument("--model", default="google/gemma-4-E4B-it-qat-q4_0-gguf")
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--reference-checkpoint", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8516)
    parser.add_argument("--timeout", type=float, default=1800)
    parser.add_argument(
        "--profile", choices=["bounded", "extended"], default="extended"
    )
    parser.add_argument("--blocks", type=int)
    parser.add_argument(
        "--encoding", choices=["f32", "f16", "fp8", "rotated4"], default="f16"
    )
    parser.add_argument("--experimental-rotated4", action="store_true")
    args = parser.parse_args()
    minimum_blocks = {"bounded": 2, "extended": 20}[args.profile]
    if args.blocks is None:
        args.blocks = minimum_blocks
    if args.blocks < minimum_blocks:
        parser.error(f"{args.profile} requires at least {minimum_blocks} blocks")
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("invalid timeout")
    args.fixture = "fact"
    args.expected_outcome = "compacted"
    args.minimum_savings = 1.5
    args.checkpoint = args.output.with_suffix(".working.kv")
    if args.output.exists() or args.checkpoint.exists():
        raise FileExistsError(args.output)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    evidence = {
        "passed": False,
        "production_ready": False,
        "scope": __doc__,
        "server_kind": args.server_kind,
        "encoding": args.encoding,
        "source": git_provenance(REPO),
        "binary_sha256": sha256_file(args.server),
        "reference_sha256": sha256_file(args.reference),
        "reference_checkpoint_sha256": sha256_file(args.reference_checkpoint),
        "runner_sha256": sha256_file(Path(__file__)),
        "blocks": [],
        "retries": 0,
        "warmups": 5,
        "block_size": 50,
        "profile": args.profile,
    }

    def publish():
        temporary = args.output.with_suffix(".json.part")
        temporary.write_text(json.dumps(evidence, indent=2) + "\n")
        temporary.replace(args.output)

    try:
        reference = json.loads(args.reference.read_text())
        if not reference.get("passed"):
            raise AssertionError("reference campaign did not pass")
        prior = reference["exporter"]["requests"]

        def prior_result(kind):
            matches = [row["result"] for row in prior if row["kind"] == kind]
            if len(matches) != 1:
                raise AssertionError(f"missing/ambiguous reference: {kind}")
            return matches[0]

        prepared = prior_result("prepare")
        if prepared["metadata"].get("encoding", "f32") != args.encoding:
            raise AssertionError("reference encoding differs from stress campaign")
        if args.encoding == "rotated4":
            from qualify_attention_match_queries import validate_quantization_evidence

            validate_quantization_evidence([prepared["metadata"]], "rotated4")
        if args.encoding == "fp8" and prepared["metadata"].get("fp8_heads", 0) < 1:
            raise AssertionError("FP8 stress requires actually quantized state")
        if prepared["file_sha256"] != evidence["reference_checkpoint_sha256"]:
            raise AssertionError("reference checkpoint changed")
        if prepared["metadata"]["logical_tokens"] != 8189:
            raise AssertionError(
                "this stress workload requires the fixed 8,189-token fact prefix"
            )
        expected = prior_result("same_process")
        expected_append = prior_result("appended_multichunk")
        public = copy.deepcopy(
            next(iter(requests(args.model, "f16", [742], False, "fact")))
        )
        public.pop("context_management")
        public.pop("cache_dtype")
        appended = copy.deepcopy(public)
        appended["messages"] += [
            {"role": "assistant", "content": "Acknowledged."},
            {
                "role": "user",
                "content": "This follow-up log entry has no changes.\n" * 128
                + "Retrieve the access code from the original log. Reply with only the access code.",
            },
        ]
        with server(args, "worker", evidence) as record:
            # Cold load and warmups use the immutable reference. Every stress
            # block subsequently exercises only newly exported checkpoint bytes.
            working = args.checkpoint
            args.checkpoint = args.reference_checkpoint
            try:
                for index in range(6):
                    result = resume(
                        args,
                        public,
                        record,
                        prepared,
                        "cold_restore" if index == 0 else f"warmup_{index}",
                    )
                    assert_equivalent(result, expected)
            finally:
                args.checkpoint = working
            for index in range(args.blocks):
                block = {"index": index, "passed": False, "operations": []}
                evidence["blocks"].append(block)
                publish()
                for sequence, kind in enumerate(TEN_REQUESTS * 5):
                    operation = {"sequence": sequence, "kind": kind, "passed": False}
                    block["operations"].append(operation)
                    started = time.monotonic()
                    if kind == "prepare":
                        new = prepare(args, public, record)
                        operation.update(
                            outcome=new["metadata"]["outcome"],
                            checkpoint_sha256=new["file_sha256"],
                        )
                        if new["file_sha256"] != prepared["file_sha256"]:
                            raise AssertionError(
                                "repeat fitting/export changed checkpoint bytes"
                            )
                        if new["metadata"] != prepared["metadata"]:
                            raise AssertionError("repeat export metadata changed")
                    else:
                        body = copy.deepcopy(appended if kind == "append" else public)
                        if kind == "sse":
                            body["stream"] = True
                        status = 200
                        mutate = None
                        if kind == "wrong_prefix":
                            body["messages"][0]["content"] = (
                                "Changed prefix.\n" + body["messages"][0]["content"]
                            )
                            body["stream"] = True
                            status = 400
                        elif kind == "checksum":
                            body["stream"] = True
                            status = 400
                            size = args.checkpoint.stat().st_size

                            def mutate(offset, data):
                                return (
                                    data[:-1] + bytes([data[-1] ^ 1])
                                    if offset + len(data) == size
                                    else data
                                )

                        result = resume(
                            args, body, record, prepared, kind, status, mutate
                        )
                        if status == 200:
                            assert_equivalent(
                                result,
                                expected_append if kind == "append" else expected,
                            )
                        elif result != prior_result(
                            "wrong_prefix"
                            if kind == "wrong_prefix"
                            else "checksum_corruption"
                        ):
                            raise AssertionError("rejection changed its error identity")
                    operation.update(passed=True, seconds=time.monotonic() - started)
                    publish()
                block["quiescent_memory"] = memory_observation(record)
                block["passed"] = True
                publish()
                print(
                    json.dumps(
                        {
                            "block": index,
                            "mixed_requests": (index + 1) * 50,
                            "actual_compactions": (index + 1) * 5,
                        }
                    ),
                    flush=True,
                )
        evidence["summary"] = summarize_blocks(evidence["blocks"], args.profile)
        memory = evidence["worker"]["memory"]
        if memory["samples"] < 2 or memory["errors"] or not memory["gpu_uuids"]:
            raise AssertionError("incomplete memory sampling")
        if (
            not 0 < memory["peak_host_rss_bytes"] <= 8 * 1024**3
            or not 0 < memory["peak_gpu_memory_bytes"] <= 18 * 1024**3
        ):
            raise AssertionError("sampled peak memory cap exceeded")
        if git_provenance(REPO) != evidence["source"]:
            raise AssertionError("source changed")
        for path, key in [
            (args.server, "binary_sha256"),
            (args.reference, "reference_sha256"),
            (args.reference_checkpoint, "reference_checkpoint_sha256"),
            (Path(__file__), "runner_sha256"),
        ]:
            if sha256_file(path) != evidence[key]:
                raise AssertionError(f"evidence changed: {path}")
        if sha256_file(args.checkpoint) != prepared["file_sha256"]:
            raise AssertionError("working checkpoint changed")
        evidence["passed"] = True
    except Exception as error:
        evidence["error"] = f"{type(error).__name__}: {error}"
    finally:
        publish()
    print(
        json.dumps({"passed": evidence["passed"], "error": evidence.get("error")}),
        flush=True,
    )
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
