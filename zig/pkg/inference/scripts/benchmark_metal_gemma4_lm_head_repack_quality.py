#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Fail-closed live-path quality gate for the Gemma 4 Metal LM-head repack."""

from __future__ import annotations

import argparse
import array
import datetime as dt
import hashlib
import heapq
import json
import math
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import time
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_SUITE = SCRIPT_DIR / "fixtures/gemma4_lm_head_repack_quality_v2.json"
REVIEWED_SUITE_SHA256 = "f9f9240bbb6ec8ce0f0284053ac210f156711ff1fd050e7f019484ffbae52393"
SUITE_SCHEMA = "antfly.gemma4_lm_head_repack_quality_suite.v2"
EVIDENCE_SCHEMA = "antfly.gemma4_lm_head_repack_quality_evidence.v3"
REVIEWED_VOCAB_SIZE = 262144
PROMPT_IDS_RE = re.compile(r"^prompt_token_ids:(?P<ids>(?:\s+-?\d+)*)\s*$", re.MULTILINE)
TOKEN_IDS_RE = re.compile(r"^token_ids:(?P<ids>(?:\s+-?\d+)*)\s*$", re.MULTILINE)
SUPPRESS_IDS_RE = re.compile(
    r"^generate_logits_suppress_token_ids:(?P<ids>(?:\s+-?\d+)*)\s*$",
    re.MULTILINE,
)
CASE_ID_RE = re.compile(r"[a-z][a-z0-9_]{1,63}")
HEX_SHA256_RE = re.compile(r"[0-9a-f]{64}")
SAFE_PARENT_ENV = (
    "HOME",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "PATH",
    "TMPDIR",
    "TZ",
)
DEFAULT_THRESHOLDS = {
    "max_perplexity_ratio": 1.01,
    "min_top1_agreement": 0.99,
    "max_mean_kl_base_to_candidate": 0.01,
    "max_step_kl_base_to_candidate": 0.05,
    "min_mean_top10_overlap_fraction": 0.90,
    "min_step_top10_overlap_fraction": 0.60,
}
MINIMUM_THRESHOLDS = {
    "min_top1_agreement",
    "min_mean_top10_overlap_fraction",
    "min_step_top10_overlap_fraction",
}


class ContractError(RuntimeError):
    pass


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def stable_file_provenance(path: Path) -> dict[str, Any]:
    resolved = path.resolve(strict=True)
    digest = hashlib.sha256()
    with resolved.open("rb") as source:
        before = os.fstat(source.fileno())
        while chunk := source.read(8 * 1024 * 1024):
            digest.update(chunk)
        after = os.fstat(source.fileno())
    final = resolved.stat()
    identities = [
        (item.st_dev, item.st_ino, item.st_mode, item.st_size, item.st_mtime_ns)
        for item in (before, after, final)
    ]
    if identities[0] != identities[1] or identities[1] != identities[2]:
        raise ContractError(f"file changed while hashing: {resolved}")
    return {
        "path": str(resolved),
        "bytes": final.st_size,
        "sha256": digest.hexdigest(),
    }


def same_file_provenance(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return all(left.get(key) == right.get(key) for key in ("path", "bytes", "sha256"))


def git_provenance(repo: Path) -> dict[str, Any]:
    def run(*args: str) -> bytes:
        return subprocess.check_output(("git", "-C", str(repo), *args), env={**os.environ, "LC_ALL": "C"})

    revision = run("rev-parse", "HEAD").decode().strip()
    status = run("status", "--porcelain=v1", "--untracked-files=all")
    diff = run("diff", "--binary", "--no-ext-diff", "HEAD", "--")
    return {
        "revision": revision,
        "dirty": bool(status.rstrip(b"\n")),
        "status_sha256": sha256_bytes(status),
        "tracked_diff_sha256": sha256_bytes(diff),
    }


def machine_metadata() -> dict[str, Any]:
    def sysctl(name: str) -> str | None:
        try:
            return subprocess.check_output(("sysctl", "-n", name), text=True, stderr=subprocess.DEVNULL).strip()
        except (OSError, subprocess.CalledProcessError):
            return None

    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
        "byteorder": sys.byteorder,
        "cpu_brand": sysctl("machdep.cpu.brand_string"),
        "memory_bytes": sysctl("hw.memsize"),
    }


def path_is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def validate_output_directory(path: Path, repo: Path) -> Path:
    resolved = path.expanduser().resolve()
    if path_is_within(resolved, repo.resolve(strict=True)):
        raise ContractError("output directory must be outside the source repository")
    if resolved.exists():
        raise ContractError(f"output directory already exists: {resolved}")
    return resolved


def validate_thresholds(thresholds: dict[str, float]) -> dict[str, float]:
    if set(thresholds) != set(DEFAULT_THRESHOLDS):
        raise ContractError("quality threshold set does not match the reviewed contract")
    validated: dict[str, float] = {}
    for name, reviewed in DEFAULT_THRESHOLDS.items():
        raw = thresholds[name]
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise ContractError(f"{name} must be a finite number")
        value = float(raw)
        if not math.isfinite(value):
            raise ContractError(f"{name} must be a finite number")
        if name in MINIMUM_THRESHOLDS:
            if not 0.0 <= value <= 1.0:
                raise ContractError(f"{name} must be in [0, 1]")
            if value < reviewed:
                raise ContractError(f"{name} cannot be looser than reviewed value {reviewed}")
        else:
            if value < 0.0:
                raise ContractError(f"{name} must be non-negative")
            if value > reviewed:
                raise ContractError(f"{name} cannot be looser than reviewed value {reviewed}")
        validated[name] = value
    return validated


def validate_campaign_dimensions(repetitions: int, vocab_size: int, timeout_seconds: float) -> None:
    if not 2 <= repetitions <= 4:
        raise ContractError("repetitions must be in [2, 4] so determinism is observable")
    if vocab_size != REVIEWED_VOCAB_SIZE:
        raise ContractError(f"vocab-size must match the reviewed Gemma 4 value {REVIEWED_VOCAB_SIZE}")
    if not math.isfinite(timeout_seconds) or not 1.0 <= timeout_seconds <= 3600.0:
        raise ContractError("timeout-seconds must be finite and in [1, 3600]")


def load_suite(path: Path, expected_sha256: str) -> dict[str, Any]:
    if HEX_SHA256_RE.fullmatch(expected_sha256) is None:
        raise ContractError("suite SHA-256 must be a lowercase digest")
    provenance = stable_file_provenance(path)
    if provenance["sha256"] != expected_sha256:
        raise ContractError(
            f"suite SHA-256 mismatch: {provenance['sha256']} != reviewed {expected_sha256}"
        )
    raw = json.loads(path.read_text(encoding="utf-8"))
    if raw.get("schema") != SUITE_SCHEMA:
        raise ContractError(f"unsupported suite schema: {raw.get('schema')!r}")
    max_tokens = raw.get("max_continuation_tokens_per_case")
    if isinstance(max_tokens, bool) or not isinstance(max_tokens, int) or not 1 <= max_tokens <= 32:
        raise ContractError("max_continuation_tokens_per_case must be in [1, 32]")
    cases = raw.get("cases")
    if not isinstance(cases, list) or len(cases) < 8:
        raise ContractError("quality suite must contain at least eight cases")
    seen: set[str] = set()
    categories: set[str] = set()
    for case in cases:
        if not isinstance(case, dict):
            raise ContractError("suite case must be an object")
        case_id = case.get("id")
        category = case.get("category")
        prefix = case.get("prefix")
        prompt_mode = case.get("prompt_mode", "raw")
        continuation = case.get("continuation")
        continuation_token_ids = case.get("continuation_token_ids")
        if not isinstance(case_id, str) or CASE_ID_RE.fullmatch(case_id) is None or case_id in seen:
            raise ContractError(f"invalid or duplicate case id: {case_id!r}")
        if not isinstance(category, str) or not category:
            raise ContractError(f"missing category for {case_id}")
        if prompt_mode not in ("raw", "chat"):
            raise ContractError(f"invalid prompt mode for {case_id}: {prompt_mode!r}")
        if not isinstance(prefix, str) or not prefix:
            raise ContractError(f"{case_id} prefix must be nonempty")
        if prompt_mode == "raw":
            if not prefix.endswith("\n"):
                raise ContractError(f"{case_id} raw prefix must end at a newline token boundary")
            if not isinstance(continuation, str) or not continuation or continuation[0].isspace():
                raise ContractError(f"{case_id} continuation must be nonempty and left-aligned")
            if continuation_token_ids is not None:
                raise ContractError(f"{case_id} raw case cannot freeze continuation token IDs")
        else:
            if not isinstance(continuation_token_ids, list) or not continuation_token_ids:
                raise ContractError(f"{case_id} chat case must freeze continuation token IDs")
            if len(continuation_token_ids) > max_tokens or any(
                isinstance(value, bool) or not isinstance(value, int) or value < 0
                for value in continuation_token_ids
            ):
                raise ContractError(f"{case_id} has invalid chat continuation token IDs")
        seen.add(case_id)
        categories.add(category)
    if len(categories) < 8:
        raise ContractError("quality suite must cover at least eight categories")
    return {"raw": raw, "provenance": provenance}


def clean_environment(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = {key: os.environ[key] for key in SAFE_PARENT_ENV if key in os.environ}
    env.setdefault("LC_ALL", "C")
    if extra:
        env.update(extra)
    return env


def run_logged(
    command: list[str],
    *,
    env: dict[str, str],
    log_path: Path,
    timeout_seconds: float,
) -> tuple[str, float]:
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        output = exc.stdout or ""
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(output, encoding="utf-8")
        raise ContractError(f"command timed out after {timeout_seconds}s; output={log_path}") from exc
    elapsed = time.monotonic() - started
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(completed.stdout, encoding="utf-8")
    if completed.returncode != 0:
        raise ContractError(f"command failed with exit {completed.returncode}; output={log_path}")
    return completed.stdout, elapsed


def parse_id_line(pattern: re.Pattern[str], output: str, label: str) -> list[int]:
    matches = list(pattern.finditer(output))
    if len(matches) != 1:
        raise ContractError(f"expected exactly one {label} line, found {len(matches)}")
    raw = matches[0].group("ids").strip()
    return [int(value) for value in raw.split()] if raw else []


def generate_command(
    binary: Path,
    model: Path,
    prompt: str,
    max_tokens: int,
    *,
    prompt_mode: str,
) -> list[str]:
    command = [
        str(binary),
        "generate",
        str(model),
        prompt,
        "--backend",
        "metal",
        "--max-tokens",
        str(max_tokens),
        "--temperature",
        "0",
        "--top-k",
        "1",
    ]
    if prompt_mode == "raw":
        command.extend(("--raw-prompt", "--no-bos"))
    elif prompt_mode != "chat":
        raise ContractError(f"invalid prompt mode: {prompt_mode}")
    return command


def tokenize_text(
    binary: Path,
    model: Path,
    text: str,
    *,
    prompt_mode: str,
    log_path: Path,
    timeout_seconds: float,
) -> list[int]:
    command = generate_command(
        binary, model, text, 1, prompt_mode=prompt_mode
    ) + ["--print-prompt-token-ids"]
    output, _ = run_logged(
        command,
        env=clean_environment(),
        log_path=log_path,
        timeout_seconds=timeout_seconds,
    )
    ids = parse_id_line(PROMPT_IDS_RE, output, "prompt_token_ids")
    if not ids:
        raise ContractError(f"tokenizer returned no IDs; output={log_path}")
    return ids


def read_logits(path: Path, expected_count: int) -> array.array[float]:
    expected_bytes = expected_count * 4
    if path.stat().st_size != expected_bytes:
        raise ContractError(f"invalid logit dump size for {path}: {path.stat().st_size} != {expected_bytes}")
    values = array.array("f")
    with path.open("rb") as source:
        values.fromfile(source, expected_count)
    if len(values) != expected_count or any(not math.isfinite(value) for value in values):
        raise ContractError(f"non-finite or incomplete logit dump: {path}")
    return values


def top_ids(
    values: array.array[float], count: int, *, suppress_token_ids: set[int] | None = None
) -> list[int]:
    suppressed = suppress_token_ids or set()
    return heapq.nlargest(
        count,
        (token_id for token_id in range(len(values)) if token_id not in suppressed),
        key=lambda token_id: (values[token_id], -token_id),
    )


def logsumexp(values: array.array[float]) -> float:
    maximum = max(values)
    return maximum + math.log(math.fsum(math.exp(value - maximum) for value in values))


def compare_logits(
    baseline_path: Path,
    candidate_path: Path,
    *,
    expected_token_id: int,
    expected_count: int,
    suppress_token_ids: list[int],
) -> dict[str, Any]:
    baseline = read_logits(baseline_path, expected_count)
    candidate = read_logits(candidate_path, expected_count)
    if not 0 <= expected_token_id < expected_count:
        raise ContractError(f"teacher token {expected_token_id} outside vocabulary {expected_count}")

    baseline_lse = logsumexp(baseline)
    candidate_lse = logsumexp(candidate)
    sum_abs = 0.0
    sum_sq = 0.0
    kl_base_candidate = 0.0
    kl_candidate_base = 0.0
    js = 0.0
    max_abs = 0.0
    for base_value, candidate_value in zip(baseline, candidate):
        difference = float(candidate_value) - float(base_value)
        absolute = abs(difference)
        sum_abs += absolute
        sum_sq += difference * difference
        max_abs = max(max_abs, absolute)
        log_p = float(base_value) - baseline_lse
        log_q = float(candidate_value) - candidate_lse
        p = math.exp(log_p)
        q = math.exp(log_q)
        if p:
            kl_base_candidate += p * (log_p - log_q)
        if q:
            kl_candidate_base += q * (log_q - log_p)
        mixture = 0.5 * (p + q)
        if p and mixture:
            js += 0.5 * p * (log_p - math.log(mixture))
        if q and mixture:
            js += 0.5 * q * (log_q - math.log(mixture))

    suppressed = set(suppress_token_ids)
    if len(suppressed) != len(suppress_token_ids) or any(
        token_id < 0 or token_id >= expected_count for token_id in suppressed
    ):
        raise ContractError("suppress-token policy contains duplicate or out-of-range IDs")
    if expected_count - len(suppressed) < 10:
        raise ContractError("suppress-token policy leaves fewer than ten candidate tokens")
    base_top10 = top_ids(baseline, 10, suppress_token_ids=suppressed)
    candidate_top10 = top_ids(candidate, 10, suppress_token_ids=suppressed)
    production_baseline_top1 = base_top10[0]
    candidate_nomination_top8 = top_ids(candidate, 8, suppress_token_ids=suppressed)
    refined_top1 = max(
        candidate_nomination_top8,
        key=lambda token_id: (baseline[token_id], -token_id),
    )
    baseline_nll = baseline_lse - float(baseline[expected_token_id])
    candidate_nll = candidate_lse - float(candidate[expected_token_id])
    return {
        "baseline_sha256": stable_file_provenance(baseline_path)["sha256"],
        "candidate_sha256": stable_file_provenance(candidate_path)["sha256"],
        "candidate_logits_distinct": baseline != candidate,
        "expected_token_id": expected_token_id,
        "baseline_expected_rank": 1 + sum(value > baseline[expected_token_id] for value in baseline),
        "candidate_expected_rank": 1 + sum(value > candidate[expected_token_id] for value in candidate),
        "baseline_expected_nll": baseline_nll,
        "candidate_expected_nll": candidate_nll,
        "expected_nll_delta": candidate_nll - baseline_nll,
        "baseline_top1": base_top10[0],
        "candidate_top1": candidate_top10[0],
        "top1_match": base_top10[0] == candidate_top10[0],
        "baseline_top10": base_top10,
        "candidate_top10": candidate_top10,
        "top10_overlap": len(set(base_top10) & set(candidate_top10)),
        "production_baseline_top1": production_baseline_top1,
        "candidate_nomination_top8": candidate_nomination_top8,
        "refined_top1": refined_top1,
        "refined_top1_match": refined_top1 == production_baseline_top1,
        "max_abs": max_abs,
        "mean_abs": sum_abs / expected_count,
        "rmse": math.sqrt(sum_sq / expected_count),
        "kl_base_to_candidate": max(0.0, kl_base_candidate),
        "kl_candidate_to_base": max(0.0, kl_candidate_base),
        "js_divergence": max(0.0, js),
        "_sum_abs": sum_abs,
        "_sum_sq": sum_sq,
    }


def expected_dump_paths(base: Path, count: int) -> list[Path]:
    return [
        Path(f"{base}.{step}.{'prefill' if step == 0 else 'decode'}.f32")
        for step in range(count)
    ]


def run_teacher_case(
    *,
    binary: Path,
    model: Path,
    prefix: str,
    prompt_ids: list[int],
    continuation_ids: list[int],
    prompt_mode: str,
    mode: str,
    candidate_format: str,
    dump_base: Path,
    log_path: Path,
    timeout_seconds: float,
) -> dict[str, Any]:
    dump_base.parent.mkdir(parents=True, exist_ok=True)
    extra = {
        "TERMITE_METAL_DISABLE_SPLIT_SWA_KV_RING": "1",
        "TERMITE_METAL_TEACHER_FORCE_TOKEN_IDS": ",".join(str(value) for value in continuation_ids),
        "TERMITE_METAL_DUMP_GENERATE_LOGITS_F32": str(dump_base),
    }
    if mode == "candidate":
        extra["TERMITE_METAL_ENABLE_LM_HEAD_Q4_REPACK"] = candidate_format
        extra["TERMITE_METAL_LM_HEAD_Q4_REPACK_QUALITY_RAW_LOGITS"] = "1"
    elif mode != "baseline":
        raise ContractError(f"invalid mode: {mode}")
    command = generate_command(
        binary, model, prefix, len(continuation_ids), prompt_mode=prompt_mode
    ) + [
        "--ignore-eos",
        "--print-prompt-token-ids",
        "--print-token-ids",
        "--print-token-count",
    ]
    output, elapsed = run_logged(
        command,
        env=clean_environment(extra),
        log_path=log_path,
        timeout_seconds=timeout_seconds,
    )
    observed_prompt_ids = parse_id_line(PROMPT_IDS_RE, output, "prompt_token_ids")
    observed_token_ids = parse_id_line(TOKEN_IDS_RE, output, "token_ids")
    suppress_token_ids = parse_id_line(
        SUPPRESS_IDS_RE, output, "generate_logits_suppress_token_ids"
    )
    if observed_prompt_ids != prompt_ids:
        raise ContractError(f"prompt IDs changed during {mode} run; output={log_path}")
    if observed_token_ids != continuation_ids:
        raise ContractError(f"teacher-forced IDs changed during {mode} run; output={log_path}")
    repack_count = output.count(f"lm_head {candidate_format.upper()} repack:")
    if mode == "candidate" and repack_count != 1:
        raise ContractError(
            f"candidate did not prove exactly one {candidate_format.upper()} lm_head repack; "
            f"output={log_path}"
        )
    if mode == "baseline" and repack_count != 0:
        raise ContractError(f"baseline unexpectedly repacked lm_head; output={log_path}")
    dumps = expected_dump_paths(dump_base, len(continuation_ids))
    for path in dumps:
        if not path.is_file():
            raise ContractError(f"missing logit dump: {path}")
    return {
        "elapsed_seconds": elapsed,
        "log_path": str(log_path),
        "log_sha256": stable_file_provenance(log_path)["sha256"],
        "dump_paths": [str(path) for path in dumps],
        "suppress_token_ids": suppress_token_ids,
    }


def verify_ring_prefill_identity(
    *,
    binary: Path,
    model: Path,
    prefix: str,
    prompt_mode: str,
    full_history_path: Path,
    dump_base: Path,
    log_path: Path,
    timeout_seconds: float,
) -> dict[str, Any]:
    dump_base.parent.mkdir(parents=True, exist_ok=True)
    command = generate_command(
        binary, model, prefix, 1, prompt_mode=prompt_mode
    ) + ["--print-token-count"]
    output, elapsed = run_logged(
        command,
        env=clean_environment({"TERMITE_METAL_DUMP_GENERATE_LOGITS_F32": str(dump_base)}),
        log_path=log_path,
        timeout_seconds=timeout_seconds,
    )
    if "RingKvRequiresPagedAttention" in output:
        raise ContractError(f"ring prefill probe failed; output={log_path}")
    ring_path = expected_dump_paths(dump_base, 1)[0]
    ring = stable_file_provenance(ring_path)
    full = stable_file_provenance(full_history_path)
    return {
        "identical": ring["sha256"] == full["sha256"],
        "ring": ring,
        "full_history": full,
        "elapsed_seconds": elapsed,
        "log_path": str(log_path),
    }


def aggregate_metrics(steps: list[dict[str, Any]], thresholds: dict[str, float]) -> tuple[dict[str, Any], list[str]]:
    if not steps:
        raise ContractError("quality campaign produced no compared logit steps")
    token_count = len(steps)
    baseline_nll = math.fsum(step["baseline_expected_nll"] for step in steps)
    candidate_nll = math.fsum(step["candidate_expected_nll"] for step in steps)
    total_elements = math.fsum(step["element_count"] for step in steps)
    baseline_perplexity = math.exp(baseline_nll / token_count)
    candidate_perplexity = math.exp(candidate_nll / token_count)
    aggregate = {
        "teacher_forced_tokens": token_count,
        "baseline_nll": baseline_nll,
        "candidate_nll": candidate_nll,
        "nll_delta": candidate_nll - baseline_nll,
        "baseline_perplexity": baseline_perplexity,
        "candidate_perplexity": candidate_perplexity,
        "perplexity_ratio": candidate_perplexity / baseline_perplexity,
        "top1_matches": sum(step["top1_match"] for step in steps),
        "top1_agreement": sum(step["top1_match"] for step in steps) / token_count,
        "mean_top10_overlap": math.fsum(step["top10_overlap"] for step in steps) / token_count,
        "mean_top10_overlap_fraction": math.fsum(step["top10_overlap"] for step in steps) / (10 * token_count),
        "min_top10_overlap": min(step["top10_overlap"] for step in steps),
        "min_top10_overlap_fraction": min(step["top10_overlap"] for step in steps) / 10,
        "distinct_candidate_logit_steps": sum(
            step["candidate_logits_distinct"] for step in steps
        ),
        "refined_argmax_matches": sum(step["refined_top1_match"] for step in steps),
        "refined_argmax_agreement": sum(step["refined_top1_match"] for step in steps)
        / token_count,
        "mean_kl_base_to_candidate": math.fsum(step["kl_base_to_candidate"] for step in steps) / token_count,
        "max_kl_base_to_candidate": max(step["kl_base_to_candidate"] for step in steps),
        "mean_js_divergence": math.fsum(step["js_divergence"] for step in steps) / token_count,
        "max_abs": max(step["max_abs"] for step in steps),
        "mean_abs": math.fsum(step["_sum_abs"] for step in steps) / total_elements,
        "rmse": math.sqrt(math.fsum(step["_sum_sq"] for step in steps) / total_elements),
    }
    failures: list[str] = []
    checks = (
        (aggregate["perplexity_ratio"] <= thresholds["max_perplexity_ratio"], "perplexity ratio"),
        (aggregate["top1_agreement"] >= thresholds["min_top1_agreement"], "top-1 agreement"),
        (
            aggregate["mean_kl_base_to_candidate"] <= thresholds["max_mean_kl_base_to_candidate"],
            "mean KL",
        ),
        (
            aggregate["max_kl_base_to_candidate"] <= thresholds["max_step_kl_base_to_candidate"],
            "max step KL",
        ),
        (
            aggregate["mean_top10_overlap_fraction"] >= thresholds["min_mean_top10_overlap_fraction"],
            "mean top-10 overlap",
        ),
        (
            aggregate["min_top10_overlap_fraction"] >= thresholds["min_step_top10_overlap_fraction"],
            "minimum top-10 overlap",
        ),
        (
            aggregate["distinct_candidate_logit_steps"] == token_count,
            "candidate transformed-logit attestation",
        ),
        (
            aggregate["refined_argmax_matches"] == token_count,
            "top-8 Q4 nomination plus Q6 refinement",
        ),
    )
    for passed, label in checks:
        if not passed:
            failures.append(f"{label} gate failed")
    return aggregate, failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--model", required=True, type=Path)
    parser.add_argument("--model-label", required=True)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--candidate-format", choices=("q4_k",), default="q4_k")
    parser.add_argument("--suite", type=Path, default=DEFAULT_SUITE)
    parser.add_argument("--suite-sha256", default=REVIEWED_SUITE_SHA256)
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--vocab-size", type=int, default=REVIEWED_VOCAB_SIZE)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    for name, value in DEFAULT_THRESHOLDS.items():
        parser.add_argument("--" + name.replace("_", "-"), type=float, default=value)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not CASE_ID_RE.fullmatch(args.model_label):
        raise ContractError("model-label must be a lowercase identifier")
    validate_campaign_dimensions(args.repetitions, args.vocab_size, args.timeout_seconds)

    repo = SCRIPT_DIR.parents[3]
    args.out_dir = validate_output_directory(args.out_dir, repo)
    suite = load_suite(args.suite, args.suite_sha256)
    binary = args.binary.resolve(strict=True)
    model = args.model.resolve(strict=True)
    thresholds = validate_thresholds({name: getattr(args, name) for name in DEFAULT_THRESHOLDS})
    provenance_start = {
        "binary": stable_file_provenance(binary),
        "model": stable_file_provenance(model),
        "suite": suite["provenance"],
        "script": stable_file_provenance(Path(__file__)),
        "git": git_provenance(repo),
    }
    args.out_dir.mkdir(parents=True)
    cases_output: list[dict[str, Any]] = []
    all_steps: list[dict[str, Any]] = []
    deterministic_inputs: dict[tuple[str, int, str], set[str]] = {}
    ring_identity: dict[str, Any] | None = None
    max_continuation_tokens = suite["raw"]["max_continuation_tokens_per_case"]

    for case_index, case in enumerate(suite["raw"]["cases"]):
        case_id = case["id"]
        prompt_mode = case.get("prompt_mode", "raw")
        token_dir = args.out_dir / "tokenization" / case_id
        prefix_ids = tokenize_text(
            binary,
            model,
            case["prefix"],
            prompt_mode=prompt_mode,
            log_path=token_dir / "prefix.txt",
            timeout_seconds=args.timeout_seconds,
        )
        if prompt_mode == "raw":
            combined_ids = tokenize_text(
                binary,
                model,
                case["prefix"] + case["continuation"],
                prompt_mode=prompt_mode,
                log_path=token_dir / "combined.txt",
                timeout_seconds=args.timeout_seconds,
            )
            if combined_ids[: len(prefix_ids)] != prefix_ids or len(combined_ids) <= len(prefix_ids):
                raise ContractError(f"{case_id} continuation is not token-prefix aligned")
            continuation_ids = combined_ids[len(prefix_ids) :][:max_continuation_tokens]
        else:
            continuation_ids = case["continuation_token_ids"][:max_continuation_tokens]
        case_record: dict[str, Any] = {
            "id": case_id,
            "category": case["category"],
            "prompt_mode": prompt_mode,
            "prompt_token_count": len(prefix_ids),
            "prompt_token_ids_sha256": sha256_bytes(
                " ".join(str(value) for value in prefix_ids).encode("ascii")
            ),
            "continuation_token_ids": continuation_ids,
            "repetitions": [],
        }
        for repetition in range(args.repetitions):
            order = ["baseline", "candidate"] if repetition % 2 == 0 else ["candidate", "baseline"]
            runs: dict[str, dict[str, Any]] = {}
            for mode in order:
                base = args.out_dir / "logits" / case_id / f"rep-{repetition + 1}" / mode
                log = args.out_dir / "logs" / case_id / f"rep-{repetition + 1}-{mode}.txt"
                runs[mode] = run_teacher_case(
                    binary=binary,
                    model=model,
                    prefix=case["prefix"],
                    prompt_ids=prefix_ids,
                    continuation_ids=continuation_ids,
                    prompt_mode=prompt_mode,
                    mode=mode,
                    candidate_format=args.candidate_format,
                    dump_base=base,
                    log_path=log,
                    timeout_seconds=args.timeout_seconds,
                )
            repetition_steps: list[dict[str, Any]] = []
            for step_index, token_id in enumerate(continuation_ids):
                baseline_path = Path(runs["baseline"]["dump_paths"][step_index])
                candidate_path = Path(runs["candidate"]["dump_paths"][step_index])
                if runs["baseline"]["suppress_token_ids"] != runs["candidate"]["suppress_token_ids"]:
                    raise ContractError(
                        f"baseline/candidate suppress-token policy differs for {case_id}"
                    )
                metrics = compare_logits(
                    baseline_path,
                    candidate_path,
                    expected_token_id=token_id,
                    expected_count=args.vocab_size,
                    suppress_token_ids=runs["baseline"]["suppress_token_ids"],
                )
                metrics.update(
                    {
                        "case_id": case_id,
                        "repetition": repetition + 1,
                        "step": step_index,
                        "element_count": args.vocab_size,
                    }
                )
                for mode in ("baseline", "candidate"):
                    digest = metrics[f"{mode}_sha256"]
                    deterministic_inputs.setdefault((case_id, step_index, mode), set()).add(digest)
                repetition_steps.append(metrics)
                all_steps.append(metrics)
            case_record["repetitions"].append(
                {
                    "index": repetition + 1,
                    "execution_order": order,
                    "runs": runs,
                    "steps": repetition_steps,
                }
            )
            if ring_identity is None and case_index == 0 and repetition == 0:
                ring_identity = verify_ring_prefill_identity(
                    binary=binary,
                    model=model,
                    prefix=case["prefix"],
                    prompt_mode=prompt_mode,
                    full_history_path=Path(runs["baseline"]["dump_paths"][0]),
                    dump_base=args.out_dir / "ring-prefill" / "baseline",
                    log_path=args.out_dir / "ring-prefill" / "run.txt",
                    timeout_seconds=args.timeout_seconds,
                )
        cases_output.append(case_record)

    aggregate, failures = aggregate_metrics(all_steps, thresholds)
    deterministic = all(len(digests) == 1 for digests in deterministic_inputs.values())
    if not deterministic:
        failures.append("baseline or candidate logit dumps were not deterministic across repetitions")
    if ring_identity is None or not ring_identity["identical"]:
        failures.append("normal-ring and full-history prefill logits were not byte-identical")
    if suite["provenance"]["sha256"] != REVIEWED_SUITE_SHA256:
        failures.append("suite digest is not reviewed for promotion")

    provenance_end = {
        "binary": stable_file_provenance(binary),
        "model": stable_file_provenance(model),
        "suite": stable_file_provenance(args.suite),
        "script": stable_file_provenance(Path(__file__)),
        "git": git_provenance(repo),
    }
    campaign_identity_stable = True
    for name in ("binary", "model", "suite", "script"):
        if not same_file_provenance(provenance_start[name], provenance_end[name]):
            campaign_identity_stable = False
            failures.append(f"{name} identity changed during the quality campaign")
    if provenance_start["git"] != provenance_end["git"]:
        campaign_identity_stable = False
        failures.append("source worktree identity changed during the quality campaign")

    # Internal accumulation fields are not part of the evidence schema.
    for step in all_steps:
        step.pop("_sum_abs")
        step.pop("_sum_sq")
    evidence = {
        "schema": EVIDENCE_SCHEMA,
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "passed": not failures,
        "failures": failures,
        "candidate": f"Q6_K lm_head streamed to {args.candidate_format.upper()} at Metal runtime prepare",
        "contract": {
            "backend": "metal",
            "tokenization": "per-case raw/no-BOS or production chat template",
            "teacher_forcing": True,
            "split_swa_ring_disabled_for_decode_logits": True,
            "pair_order": "alternating AB/BA",
            "repetitions": args.repetitions,
            "vocab_size": args.vocab_size,
            "thresholds": thresholds,
            "candidate_format": args.candidate_format,
            "candidate_logit_path": "explicit transformed Q4_K slot",
            "refined_argmax_gate": "Q4_K top-8 nomination rescored with checkpoint Q6_K logits must match exact Q6_K argmax at every step",
            "invocation_arguments": sys.argv[1:],
        },
        "provenance": {
            "binary": provenance_start["binary"],
            "model": provenance_start["model"],
            "suite": provenance_start["suite"],
            "script": provenance_start["script"],
            "git": provenance_start["git"],
            "campaign_identity_stable": campaign_identity_stable,
            "campaign_end": provenance_end,
            "machine": machine_metadata(),
            "model_label": args.model_label,
            "output_directory": str(args.out_dir.resolve()),
        },
        "ring_prefill_identity": ring_identity,
        "deterministic_across_repetitions": deterministic,
        "aggregate": aggregate,
        "cases": cases_output,
    }
    summary_path = args.out_dir / "summary.json"
    summary_path.write_text(json.dumps(evidence, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    print(f"summary={summary_path}")
    print(
        "quality: pass={passed} candidate={candidate_format} tokens={tokens} ppl_base={base:.6f} ppl_candidate={candidate:.6f} "
        "ppl_ratio={ratio:.6f} top1={top1:.6f} mean_kl={mean_kl:.6f} max_kl={max_kl:.6f} "
        "mean_top10={top10:.6f} deterministic={deterministic} ring_prefill_identity={ring}".format(
            passed=str(not failures).lower(),
            candidate_format=args.candidate_format,
            tokens=aggregate["teacher_forced_tokens"],
            base=aggregate["baseline_perplexity"],
            candidate=aggregate["candidate_perplexity"],
            ratio=aggregate["perplexity_ratio"],
            top1=aggregate["top1_agreement"],
            mean_kl=aggregate["mean_kl_base_to_candidate"],
            max_kl=aggregate["max_kl_base_to_candidate"],
            top10=aggregate["mean_top10_overlap_fraction"],
            deterministic=str(deterministic).lower(),
            ring=str(bool(ring_identity and ring_identity["identical"])).lower(),
        )
    )
    if failures:
        for failure in failures:
            print(f"FAIL: {failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ContractError as exc:
        print(f"contract error: {exc}", file=sys.stderr)
        raise SystemExit(2)
