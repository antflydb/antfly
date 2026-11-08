#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Sequential native full-context/closed-book QA diagnostics; never a release gate.

Only raw documents and the unchanged question enter prompts. This controlled
protocol puts literal documents between Memory delimiters in the system turn;
it is not an upstream benchmark reproduction. Closed-book omits that context.
Each condition starts a fresh native process. There is no automatic retry,
precision fallback, asset installation, or download. Run only in an authorized
GPU window after other model-serving processes have exited.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time

from assets import LOCK_PATH, verify_base_model
from quality_cases import DEFAULT_SUITE, load_suite, score_answer, score_suite

HERE = Path(__file__).parent
DEFAULT_CASES = (
    "copper_badge_a",
    "copper_badge_b",
    "lantern_courier_a",
    "lantern_courier_b",
)
SYSTEM = "Answer the question using the provided memory."
MANIFEST = b'{"type":"generator","memorilla_profile":"qwen3_decoder_f32_kv_f32_v1"}\n'
F32_WEIGHT_BYTES = 32_762_941_440
CONDITIONS = ("full_context", "closed_book")
LEGACY_RECOVERY_SOURCE = (
    "69ebc212cb81e1a302c05c0f7f4d91b745c6974228706cce5118582b9c678019"
)


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def save(path: Path, value) -> None:
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
    temporary.replace(path)


def messages(case: dict, condition: str) -> list[dict]:
    if condition not in CONDITIONS:
        raise ValueError("unknown baseline condition")
    system = SYSTEM
    if condition == "full_context":
        system += "\nMemory\n" + "\n\n".join(case["documents"]) + "\nMemory"
    # Matches production's Python-str.strip question policy, without paraphrase.
    question = case["question"].strip()
    if not question or "<|memory|>" in question:
        raise ValueError("invalid canonical question")
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": question},
    ]


def render(tokenizer, case: dict, condition: str) -> tuple[str, list[int]]:
    prompt = tokenizer.apply_chat_template(
        messages(case, condition),
        tokenize=False,
        add_generation_prompt=True,
        enable_thinking=False,
    )
    if "<|memory|>" in prompt:
        raise ValueError("ordinary baseline contains virtual memory token")
    return prompt, tokenizer.encode(prompt, add_special_tokens=False)


def printed_ids(log: str, label: str) -> list[int]:
    lines = re.findall(r"^" + re.escape(label) + r": *([^\r\n]*)$", log, re.MULTILINE)
    if len(lines) != 1 or not re.fullmatch(r"\d+(?: +\d+)*", lines[0].strip()):
        raise ValueError(f"missing or ambiguous native {label}")
    return [int(word) for word in lines[0].split()]


def validate_generation(timing: dict, log: str, maximum: int) -> tuple[list[int], dict]:
    ids = printed_ids(log, "token_ids")
    retained = timing.get("token_ids")
    reason = timing.get("finish_reason")
    finish = re.findall(r"^finish_reason=(\w+) tokens=(\d+)$", log, re.MULTILINE)
    if (
        not 1 <= len(ids) <= maximum
        or (retained is not None and retained != ids)
        or type(timing.get("tokens")) is not int
        or timing["tokens"] != len(ids)
        or any(not 0 <= i < 151936 for i in ids)
        or reason not in {"stop", "length", "eos"}
        or finish != [(reason, str(len(ids)))]
    ):
        raise ValueError(
            "invalid or inconsistent native generated token IDs/count/finish reason"
        )
    return ids, {
        "printed_ids": "native --print-token-ids",
        "timing_ids": "not_retained" if retained is None else "equal_to_printed_ids",
        "finish_reason_semantics": {
            "stop": "native stop condition; not relabeled as HTTP eos",
            "length": "native generation token limit",
            "eos": "native explicitly reports eos",
        }[reason],
    }


def arithmetic_evidence(timing: dict, log: str) -> dict:
    cuda = timing.get("cuda")
    forward = timing.get("cuda_generate")
    if (
        timing.get("backend") != "cuda"
        or not isinstance(cuda, dict)
        or not isinstance(forward, dict)
    ):
        raise ValueError("native CUDA runtime evidence missing")
    if (
        type(forward.get("kernel_launches")) is not int
        or forward["kernel_launches"] <= 0
    ):
        raise ValueError("no measured CUDA forward kernels")
    observed = {}
    for scope, counters in (("total", cuda), ("forward", forward)):
        for key, value in counters.items():
            if key.startswith(("bf16_", "f16_")) or key in {
                "cross_backend_sync_fallbacks",
                "host_attention_fallbacks",
                "rope_host_fallbacks",
                "rope_per_item_fallbacks",
                "gqa_dense_fallbacks",
                "paged_attention_fallbacks",
                "rms_norm_bf16_mirror_hits",
            }:
                if type(value) is not int or value != 0:
                    raise ValueError(
                        f"forbidden arithmetic/fallback counter: {scope}.{key}={value}"
                    )
                observed[f"{scope}.{key}"] = value
    byte_lines = re.findall(r"^cuda_runtime_bytes: (.+)$", log, re.MULTILINE)
    if len(byte_lines) != 1:
        raise ValueError("native resident weight evidence missing")
    byte_counts = {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", byte_lines[0])}
    if byte_counts.get("resident_weights") != F32_WEIGHT_BYTES:
        raise ValueError("native resident weights differ from exact F32 decoder")
    fallback_lines = re.findall(r"^cuda_fallback_counts: (.+)$", log, re.MULTILINE)
    if len(fallback_lines) != 1:
        raise ValueError("native host fallback evidence missing")
    fallbacks = {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", fallback_lines[0])}
    required = {
        "host_attention",
        "rope",
        "rope_per_item",
        "gqa_dense",
        "paged_attention",
    }
    if set(fallbacks) != required or any(fallbacks.values()):
        raise ValueError("native host fallback observed or incomplete")
    return {
        "forward_kernel_launches": forward["kernel_launches"],
        "resident_weight_bytes": byte_counts["resident_weights"],
        "observed_zero_counters": observed,
        "host_fallbacks": fallbacks,
        "requested_kv_dtype": "f32",
        "requested_profile": "qwen3_decoder_f32_kv_f32_v1",
        "complete_actual_inventory_and_capture_proof": False,
        "limitation": "CLI timing omits full resident dtype inventory and capture-gate state; no qualification claim.",
    }


def generation_snapshot(model: Path, lock: dict) -> dict:
    paths = [entry["path"] for entry in lock["files"]]
    paths += ["model_manifest.json", ".antfly-download-complete.json"]
    result = {}
    for name in paths:
        stat = (model / name).stat()
        result[name] = [
            stat.st_dev,
            stat.st_ino,
            stat.st_size,
            stat.st_mtime_ns,
            stat.st_ctime_ns,
        ]
    return result


def command(args, prompt: str, timing_path: Path) -> list[str]:
    return [
        str(args.native_binary),
        "generate",
        str(args.model),
        prompt,
        "--backend",
        "cuda",
        "--mode",
        "eager",
        "--raw-prompt",
        "--no-bos",
        "--cache-dtype",
        "f32",
        "--temperature",
        "0",
        "--max-tokens",
        str(args.max_new_tokens),
        "--print-prompt-token-ids",
        "--print-token-ids",
        "--print-finish-reason",
        "--print-token-count",
        "--print-timing",
        "--json-timing",
        str(timing_path),
        "--host-budget-mb",
        "32768",
        "--backend-budget-mb",
        "65536",
        "--combined-budget-mb",
        "98304",
        "--kv-budget-mb",
        "4096",
        "--scratch-budget-mb",
        "8192",
    ]


def run(
    args, *, tokenizer=None, verify_model=verify_base_model, execute=subprocess.run
) -> dict:
    args.native_binary = args.native_binary.resolve(strict=True)
    args.model = args.model.resolve(strict=True)
    args.output = args.output.resolve()
    resume = bool(getattr(args, "resume", False))
    if args.output.exists() and not resume:
        raise ValueError("output directory already exists; preserve previous attempts")
    if resume and not (args.output / "summary.json").is_file():
        raise ValueError("resume requires an existing failed summary")
    if not 1 <= args.max_new_tokens <= 128 or args.timeout <= 0:
        raise ValueError("invalid generation bounds")
    suite = load_suite(args.suite)
    selected = args.case_id or list(DEFAULT_CASES)
    if len(selected) != len(set(selected)) or set(selected) - {
        c["id"] for c in suite["cases"]
    }:
        raise ValueError("duplicate or unknown selected case")
    cases = [next(c for c in suite["cases"] if c["id"] == name) for name in selected]
    args.output.mkdir(parents=True, exist_ok=resume)
    record = {
        "schema": "antfly.memorilla.quality-baseline.v1",
        "completed": False,
        "execution_passed": False,
        "rollout_qualified": False,
        "selected_case_ids": selected,
        "conditions": list(CONDITIONS),
        "results": [],
        "protocol": {
            "system": SYSTEM,
            "question": "unchanged Python-str.strip",
            "full_context": "raw documents in system turn between newline-delimited Memory markers",
            "closed_book": "same system instruction, absent source and Memory markers",
            "caveat": "Different context and sequence lengths are unavoidable; this is not an upstream benchmark reproduction.",
        },
        "source_sha256": {
            p.name: digest(p)
            for p in (Path(__file__), HERE / "quality_cases.py", LOCK_PATH, args.suite)
        },
        "native_binary": str(args.native_binary),
        "native_binary_sha256": digest(args.native_binary),
        "model": str(args.model),
        "max_new_tokens": args.max_new_tokens,
    }
    summary = args.output / "summary.json"
    previous = json.loads(summary.read_text()) if resume else None
    if previous is not None:
        if previous.get("completed") or previous.get("execution_passed"):
            raise ValueError("resume requires a failed attempt")
        for key in (
            "schema",
            "selected_case_ids",
            "conditions",
            "protocol",
            "native_binary",
            "native_binary_sha256",
            "model",
            "max_new_tokens",
        ):
            if previous.get(key) != record[key]:
                raise ValueError(f"resume provenance differs: {key}")
        for key, value in record["source_sha256"].items():
            if (
                key != Path(__file__).name
                and previous.get("source_sha256", {}).get(key) != value
            ):
                raise ValueError(f"resume source differs: {key}")
    else:
        save(summary, record)
    before = None
    checkpoint_allowed = previous is None
    try:
        if (args.model / "model_manifest.json").read_bytes() != MANIFEST:
            raise ValueError(
                "requires existing exact qualified F32 decoder manifest; no asset mutation"
            )
        lock = verify_model(args.model, "decoder")
        before = generation_snapshot(args.model, lock)
        record["model_source"] = lock
        record["model_manifest_sha256"] = digest(args.model / "model_manifest.json")
        record["publication_sha256"] = digest(
            args.model / ".antfly-download-complete.json"
        )
        record["artifact_generation_before"] = before
        if previous is not None:
            for key in (
                "model_source",
                "model_manifest_sha256",
                "publication_sha256",
                "artifact_generation_before",
            ):
                if previous.get(key) != record[key]:
                    raise ValueError(f"resume model identity differs: {key}")
        if tokenizer is None:
            from transformers import AutoTokenizer

            tokenizer = AutoTokenizer.from_pretrained(
                str(args.model),
                local_files_only=True,
                trust_remote_code=False,
            )
        if resume:
            # Bind every saved prompt and command before any new native load.
            for case in cases:
                for condition in CONDITIONS:
                    stem = args.output / f"{case['id']}-{condition}"
                    paths = [
                        stem.with_suffix(suffix)
                        for suffix in (
                            ".prompt.txt",
                            ".expected-prompt-ids.json",
                            ".command.json",
                            ".stdout",
                            ".stderr",
                            ".timing.json",
                        )
                    ]
                    if not any(path.exists() for path in paths):
                        continue
                    if not all(path.is_file() for path in paths):
                        raise ValueError(
                            f"incomplete saved native artifacts: {stem.name}; no automatic rerun"
                        )
                    prompt, expected = render(tokenizer, case, condition)
                    if (
                        paths[0].read_text() != prompt
                        or json.loads(paths[1].read_text()) != expected
                        or json.loads(paths[2].read_text())
                        != command(args, prompt, paths[5])
                    ):
                        raise ValueError(
                            f"resume prompt/token IDs/actual args differ: {stem.name}"
                        )
        for case in cases:
            for condition in CONDITIONS:
                stem = args.output / f"{case['id']}-{condition}"
                prompt, expected = render(tokenizer, case, condition)
                timing_path = stem.with_suffix(".timing.json")
                argv = command(args, prompt, timing_path)
                required = [
                    stem.with_suffix(suffix)
                    for suffix in (
                        ".prompt.txt",
                        ".expected-prompt-ids.json",
                        ".command.json",
                        ".stdout",
                        ".stderr",
                        ".timing.json",
                    )
                ]
                reuse = resume and any(path.exists() for path in required)
                if reuse:
                    if not all(path.is_file() for path in required):
                        raise ValueError(
                            f"incomplete saved native artifacts: {stem.name}; no automatic rerun"
                        )
                    if (
                        required[0].read_text() != prompt
                        or json.loads(required[1].read_text()) != expected
                        or json.loads(required[2].read_text()) != argv
                    ):
                        raise ValueError(
                            f"resume prompt/token IDs/actual args differ: {stem.name}"
                        )
                    execution_path = stem.with_suffix(".execution.json")
                    if execution_path.exists():
                        execution = json.loads(execution_path.read_text())
                        if (
                            execution.get("returncode") != 0
                            or execution.get("stdout_sha256") != digest(required[3])
                            or execution.get("stderr_sha256") != digest(required[4])
                            or execution.get("timing_sha256") != digest(required[5])
                        ):
                            raise ValueError("saved native execution proof differs")
                        returncode_provenance = (
                            "persisted native exit0 and bound stdout/stderr"
                        )
                    elif previous.get("source_sha256", {}).get(
                        Path(__file__).name
                    ) == LEGACY_RECOVERY_SOURCE and previous.get("error") == {
                        "type": "ValueError",
                        "message": "invalid or inconsistent native generated token IDs",
                    }:
                        # That frozen runner reaches this exact validation error
                        # only after its explicit returncode==0 check. Preserve
                        # its summary and identify this historical inference.
                        legacy = json.loads(
                            (HERE / "fixtures/baseline-native-output.json").read_text()
                        )
                        if (
                            digest(required[4]) != legacy["source_stderr_sha256"]
                            or digest(required[5]) != legacy["source_timing_sha256"]
                        ):
                            raise ValueError(
                                "legacy native artifacts differ from archived successful invocation"
                            )
                        returncode_provenance = "exit0 inferred from archived frozen-runner control flow before exact recorded validation failure; stderr/timing bound to archived successful invocation"
                    else:
                        raise ValueError(
                            "saved artifacts lack a successful execution proof"
                        )
                    # Do not replace the failed summary until all cached
                    # provenance for this request has been checked.
                    if not checkpoint_allowed:
                        archive = args.output / f"summary.failed-{digest(summary)}.json"
                        if not archive.exists():
                            archive.write_bytes(summary.read_bytes())
                        record["resumed_from"] = {
                            "summary": str(archive),
                            "sha256": digest(archive),
                            "old_runner_sha256": previous["source_sha256"][
                                Path(__file__).name
                            ],
                        }
                        checkpoint_allowed = True
                else:
                    if not checkpoint_allowed:
                        raise ValueError(
                            "resume has no reusable successful request before new execution"
                        )
                    stem.with_suffix(".prompt.txt").write_text(prompt)
                    save(stem.with_suffix(".expected-prompt-ids.json"), expected)
                    save(stem.with_suffix(".command.json"), argv)
                started = time.monotonic()
                try:
                    if reuse:
                        stdout, stderr = (
                            required[3].read_text(),
                            required[4].read_text(),
                        )
                        returncode = 0
                    else:
                        process = execute(
                            argv,
                            capture_output=True,
                            text=True,
                            timeout=args.timeout,
                            check=False,
                        )
                        stdout, stderr, returncode = (
                            process.stdout,
                            process.stderr,
                            process.returncode,
                        )
                except subprocess.TimeoutExpired as exc:
                    for suffix, value in (
                        (".stdout", exc.stdout),
                        (".stderr", exc.stderr),
                    ):
                        stem.with_suffix(suffix).write_bytes(
                            value.encode() if isinstance(value, str) else value or b""
                        )
                    raise
                if not reuse:
                    stem.with_suffix(".stdout").write_text(stdout)
                    stem.with_suffix(".stderr").write_text(stderr)
                    save(
                        stem.with_suffix(".execution.json"),
                        {
                            "returncode": returncode,
                            "stdout_sha256": digest(stem.with_suffix(".stdout")),
                            "stderr_sha256": digest(stem.with_suffix(".stderr")),
                            "timing_sha256": digest(timing_path)
                            if timing_path.is_file()
                            else None,
                        },
                    )
                    returncode_provenance = (
                        "persisted native exit0 and bound stdout/stderr"
                    )
                if returncode != 0:
                    raise RuntimeError(
                        f"native baseline failed ({returncode}): {stem.name}; no fallback"
                    )
                log = stdout + "\n" + stderr
                if printed_ids(log, "prompt_token_ids") != expected:
                    raise ValueError("native/HF prompt token IDs differ")
                timing = json.loads(timing_path.read_text())
                ids, token_provenance = validate_generation(
                    timing, log, args.max_new_tokens
                )
                evidence = arithmetic_evidence(timing, log)
                answer = tokenizer.decode(ids, skip_special_tokens=True)
                result = {
                    "case_id": case["id"],
                    "condition": condition,
                    "answer": answer,
                    "raw_decode": tokenizer.decode(ids, skip_special_tokens=False),
                    "token_ids": ids,
                    "prompt_tokens": len(expected),
                    "prompt_ids_equal": True,
                    "finish_reason": timing["finish_reason"],
                    "token_provenance": token_provenance,
                    "native_artifacts_reused": reuse,
                    "returncode_provenance": returncode_provenance,
                    "native_artifact_sha256": {
                        path.name: digest(path) for path in required
                    },
                    "elapsed_seconds": time.monotonic() - started,
                    "arithmetic_evidence": evidence,
                    "score": score_answer(answer, case),
                }
                save(
                    stem.with_suffix(
                        ".result-resumed.json" if reuse else ".result.json"
                    ),
                    result,
                )
                record["results"].append(result)
                save(summary, record)
        record["scores"] = {}
        for condition in CONDITIONS:
            results = [r for r in record["results"] if r["condition"] == condition]
            record["scores"][condition] = {
                "selected_case_count": len(cases),
                "selected_case_accuracy": sum(r["score"]["correct"] for r in results)
                / len(cases),
                "full_suite_with_missing_cases": score_suite(suite, results),
            }
        record["completed"] = record["execution_passed"] = True
    except BaseException as exc:
        record["error"] = {"type": type(exc).__name__, "message": str(exc)}
        raise
    finally:
        if before is not None:
            try:
                after = generation_snapshot(args.model, lock)
                record["artifact_generation_after"] = after
                record["assets_unchanged"] = before == after
            except OSError as exc:
                record["assets_unchanged"] = False
                record["artifact_generation_error"] = str(exc)
            if not record["assets_unchanged"]:
                record["execution_passed"] = False
                record.setdefault(
                    "error",
                    {
                        "type": "ValueError",
                        "message": "model assets changed during run",
                    },
                )
        if checkpoint_allowed:
            save(summary, record)
    if not record.get("assets_unchanged"):
        raise ValueError("model assets changed during run")
    return record


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-binary", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--suite", type=Path, default=DEFAULT_SUITE)
    parser.add_argument("--case-id", action="append")
    parser.add_argument("--max-new-tokens", type=int, default=48)
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument(
        "--resume",
        action="store_true",
        help="explicitly reparse fully bound successful native artifacts in a failed output directory; preserve its failed summary",
    )
    args = parser.parse_args()
    result = run(args)
    print(
        json.dumps(
            {
                "completed": result["completed"],
                "scores": result["scores"],
                "rollout_qualified": False,
            }
        )
    )


if __name__ == "__main__":
    main()
