#!/usr/bin/env python3
"""Evaluate deterministic Gemma 4 CUDA candidates on a versioned quality suite.

This gate is deliberately separate from performance qualification.  The
default exact policy can contribute quality evidence to the existing exact
token promotion gate, but this script never enables or promotes a runtime
route.  A bounded-divergence profile is collect-only until teacher-forced
logit, top-k, margin, and distribution metrics are available.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import difflib
import hashlib
import json
import math
import os
import pathlib
import platform
import re
import subprocess
import sys
from typing import Any

import gemma4_cuda_l4_release_gate as release_provenance
import validate_gemma4_cuda_candidate as candidate_validator
from paired_benchmark import balanced_pair_order


SUITE_SCHEMA = "antfly.gemma4_cuda_quality_suite.v1"
EVIDENCE_SCHEMA = "antfly.gemma4_cuda_quality_evidence.v1"
SAMPLE_SCHEMA = "antfly.gemma4_cuda_quality_sample.v1"
PROMPT_TOKEN_IDS_RE = re.compile(r"^prompt_token_ids:(?P<ids>(?:\s+-?\d+)*)\s*$", re.MULTILINE)
TOKEN_IDS_RE = re.compile(r"^token_ids:(?P<ids>(?:\s+-?\d+)*)\s*$", re.MULTILINE)
TOKENS_RE = re.compile(r"^(?:finish_reason=(?P<finish>\S+)\s+)?tokens=(?P<count>\d+)\s*$", re.MULTILINE)
HEX_SHA256_RE = re.compile(r"[0-9a-f]{64}")
REVIEWED_SUITE_SHA256 = "f68f1910dae34f8c7a7827ea12d6b4a82c58ff85e6b4ae2f8ac6cb9138805898"
SAFE_PARENT_ENVIRONMENT_KEYS = (
    "HOME",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LD_LIBRARY_PATH",
    "PATH",
    "TMPDIR",
    "TZ",
    "CUDA_VISIBLE_DEVICES",
    "CUDA_DEVICE_ORDER",
)
HARD_BOUNDED_POLICY = {
    "max_divergent_case_fraction": 0.25,
    "max_token_divergence_rate": 0.90,
    "max_output_length_delta_tokens": 8,
    "min_first_divergence_index": 32,
    "min_first_divergence_fraction": 0.20,
    "min_normalized_text_similarity": 0.60,
}
LOGIT_CAPABILITY = {
    "available": False,
    "audited_interfaces": [
        "antfly-inference generate CLI",
        "Antfly generation server response",
    ],
    "available_observables": [
        "prompt token IDs",
        "generated token IDs",
        "decoded output text",
        "route counters",
        "phase latency",
    ],
    "unavailable_observables": [
        "teacher-forced logits",
        "teacher-forced top-k token scores",
        "chosen-token margin",
        "KL or symmetric distribution divergence",
    ],
    "consequence": (
        "non-exact generation is diagnostic-only and cannot be declared "
        "production-equivalent"
    ),
}


@dataclasses.dataclass(frozen=True)
class QualityCase:
    id: str
    description: str
    prompt: str
    prompt_contract: dict[str, Any]
    prompt_source_provenance: tuple[dict[str, Any], ...]
    max_tokens: int
    allow_candidate_divergence: bool
    output_contract: dict[str, Any]


@dataclasses.dataclass(frozen=True)
class QualitySuite:
    path: pathlib.Path
    raw: dict[str, Any]
    cases: tuple[QualityCase, ...]
    profile_name: str
    profile: dict[str, Any]


def repo_root_from_script() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[4]


def inference_root_from_script() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def reviewed_suite_path() -> pathlib.Path:
    return inference_root_from_script() / "scripts/fixtures/gemma4_cuda_quality_suite_v1.json"


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def stable_file_provenance(path: pathlib.Path) -> dict[str, Any]:
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
        raise RuntimeError(f"file changed while hashing: {resolved}")
    return {
        "path": str(resolved),
        "bytes": final.st_size,
        "mode": final.st_mode & 0o7777,
        "sha256": digest.hexdigest(),
    }


def _required_digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or HEX_SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _required_positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _prompt_fixture_text(path: pathlib.Path) -> tuple[str, str, dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if raw.get("schema") != "antfly.prompt_fixture.v1":
        raise ValueError(f"unsupported prompt fixture schema in {path}")
    segment = raw.get("segment")
    suffix = raw.get("suffix")
    repeat = raw.get("repeat")
    prefix = raw.get("reference_chat_prefix")
    chat_suffix = raw.get("reference_chat_suffix")
    if not all(isinstance(value, str) for value in (segment, suffix, prefix, chat_suffix)):
        raise ValueError(f"prompt fixture strings are incomplete in {path}")
    if isinstance(repeat, bool) or not isinstance(repeat, int) or repeat < 1:
        raise ValueError(f"prompt fixture repeat is invalid in {path}")
    user = segment * repeat + suffix
    prompt = prefix + user + chat_suffix
    return user, prompt, raw


def _inline_prompt_text(contract: dict[str, Any], case_id: str) -> tuple[str, str]:
    fields = ("reference_chat_prefix", "segment", "padding", "suffix", "reference_chat_suffix")
    if not all(isinstance(contract.get(name), str) for name in fields):
        raise ValueError(f"{case_id} inline prompt strings are incomplete")
    repeat = contract.get("repeat")
    if isinstance(repeat, bool) or not isinstance(repeat, int) or repeat < 1:
        raise ValueError(f"{case_id} prompt repeat must be a positive integer")
    user = contract["segment"] * repeat + contract["padding"] + contract["suffix"]
    return user, contract["reference_chat_prefix"] + user + contract["reference_chat_suffix"]


def _verify_prompt_contract(
    *,
    case_id: str,
    contract: dict[str, Any],
    user: str,
    prompt: str,
) -> None:
    user_bytes = user.encode("utf-8")
    prompt_bytes = prompt.encode("utf-8")
    expected_prompt_bytes = _required_positive_int(
        contract.get("expected_prompt_utf8_bytes"), f"{case_id} expected prompt bytes"
    )
    expected_prompt_tokens = _required_positive_int(
        contract.get("expected_prompt_tokens"), f"{case_id} expected prompt tokens"
    )
    expected_prompt_sha = _required_digest(
        contract.get("expected_prompt_sha256"), f"{case_id} expected prompt"
    )
    _required_digest(
        contract.get("expected_prompt_token_ids_sha256"),
        f"{case_id} expected prompt token IDs",
    )
    if len(prompt_bytes) != expected_prompt_bytes:
        raise ValueError(
            f"{case_id} prompt byte contract mismatch: {len(prompt_bytes)} != {expected_prompt_bytes}"
        )
    if sha256_bytes(prompt_bytes) != expected_prompt_sha:
        raise ValueError(f"{case_id} prompt SHA-256 contract mismatch")
    if expected_prompt_tokens < 1:
        raise ValueError(f"{case_id} expected prompt token count must be positive")
    if "expected_user_utf8_bytes" in contract:
        expected_user_bytes = _required_positive_int(
            contract.get("expected_user_utf8_bytes"), f"{case_id} expected user bytes"
        )
        expected_user_sha = _required_digest(
            contract.get("expected_user_sha256"), f"{case_id} expected user prompt"
        )
        if len(user_bytes) != expected_user_bytes or sha256_bytes(user_bytes) != expected_user_sha:
            raise ValueError(f"{case_id} user prompt contract mismatch")


def _float_field(profile: dict[str, Any], name: str) -> float:
    value = profile.get(name)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"threshold {name} must be finite")
    return float(value)


def validate_threshold_profile(name: str, profile: dict[str, Any]) -> None:
    if not isinstance(profile, dict):
        raise ValueError(f"threshold profile {name!r} must be an object")
    required = {
        "allow_token_divergence",
        "max_divergent_case_fraction",
        "max_token_divergence_rate",
        "max_output_length_delta_tokens",
        "min_first_divergence_index",
        "min_first_divergence_fraction",
        "min_normalized_text_similarity",
    }
    if set(profile) != required:
        raise ValueError(f"threshold profile {name!r} fields must be exactly {sorted(required)}")
    allow = profile.get("allow_token_divergence")
    if not isinstance(allow, bool):
        raise ValueError(f"threshold profile {name!r} allow_token_divergence must be boolean")
    max_fraction = _float_field(profile, "max_divergent_case_fraction")
    max_rate = _float_field(profile, "max_token_divergence_rate")
    min_fraction = _float_field(profile, "min_first_divergence_fraction")
    min_similarity = _float_field(profile, "min_normalized_text_similarity")
    max_delta = profile.get("max_output_length_delta_tokens")
    min_index = profile.get("min_first_divergence_index")
    if any(value < 0.0 or value > 1.0 for value in (max_fraction, max_rate, min_fraction, min_similarity)):
        raise ValueError(f"threshold profile {name!r} rate fields must be within [0, 1]")
    if isinstance(max_delta, bool) or not isinstance(max_delta, int) or max_delta < 0:
        raise ValueError(f"threshold profile {name!r} max output length delta is invalid")
    if isinstance(min_index, bool) or not isinstance(min_index, int) or min_index < 0:
        raise ValueError(f"threshold profile {name!r} minimum divergence index is invalid")
    if not allow:
        if max_fraction != 0.0 or max_rate != 0.0 or max_delta != 0:
            raise ValueError(f"exact threshold profile {name!r} permits divergence")
        return
    if max_fraction > HARD_BOUNDED_POLICY["max_divergent_case_fraction"]:
        raise ValueError(f"threshold profile {name!r} divergent-case fraction is too permissive")
    if max_rate > HARD_BOUNDED_POLICY["max_token_divergence_rate"]:
        raise ValueError(f"threshold profile {name!r} token divergence rate is too permissive")
    if max_delta > HARD_BOUNDED_POLICY["max_output_length_delta_tokens"]:
        raise ValueError(f"threshold profile {name!r} output length delta is too permissive")
    if min_index < HARD_BOUNDED_POLICY["min_first_divergence_index"]:
        raise ValueError(f"threshold profile {name!r} first divergence index is too permissive")
    if min_fraction < HARD_BOUNDED_POLICY["min_first_divergence_fraction"]:
        raise ValueError(f"threshold profile {name!r} first divergence fraction is too permissive")
    if min_similarity < HARD_BOUNDED_POLICY["min_normalized_text_similarity"]:
        raise ValueError(f"threshold profile {name!r} text similarity is too permissive")


def _validate_generation_contract(contract: dict[str, Any]) -> None:
    locked = {
        "backend": "cuda",
        "temperature": 0.0,
        "raw_prompt": True,
        "no_chat_template": True,
        "ignore_eos": False,
        "cache_dtype": "f16",
        "prefill_chunk_size": 512,
        "capture_kv_capacity": 2432,
        "combined_budget_mb": 22000,
        "backend_budget_mb": 19000,
        "kv_budget_mb": 1024,
        "scratch_budget_mb": 2048,
    }
    if contract != locked:
        raise ValueError("quality suite generation contract is not the reviewed deterministic CUDA contract")


def _validate_output_contract(contract: dict[str, Any], case_id: str) -> None:
    if not isinstance(contract, dict):
        raise ValueError(f"{case_id} output contract must be an object")
    for name in ("min_utf8_bytes", "max_utf8_bytes", "min_words", "max_words"):
        if name in contract and (
            isinstance(contract[name], bool) or not isinstance(contract[name], int) or contract[name] < 0
        ):
            raise ValueError(f"{case_id} output contract {name} is invalid")
    if contract.get("min_utf8_bytes", 1) < 1:
        raise ValueError(f"{case_id} output contract must require non-empty output")
    for name in ("required_substrings", "forbidden_substrings", "required_fullmatch_regexes"):
        values = contract.get(name, [])
        if not isinstance(values, list) or not all(isinstance(item, str) and item for item in values):
            raise ValueError(f"{case_id} output contract {name} is invalid")
    if json_contract := contract.get("json"):
        if not isinstance(json_contract, dict) or json_contract.get("type") not in {"object", "array"}:
            raise ValueError(f"{case_id} JSON output contract is invalid")
        if json_contract.get("type") == "object":
            keys = json_contract.get("exact_keys")
            values = json_contract.get("expected_values")
            if not isinstance(keys, list) or not all(isinstance(key, str) for key in keys):
                raise ValueError(f"{case_id} JSON exact keys are invalid")
            if not isinstance(values, dict) or set(values) != set(keys):
                raise ValueError(f"{case_id} JSON expected values must cover the exact keys")


def load_suite(path: pathlib.Path, profile_name: str | None = None) -> QualitySuite:
    resolved = path.resolve(strict=True)
    suite_bytes = resolved.read_bytes()
    if (
        resolved == reviewed_suite_path().resolve()
        and sha256_bytes(suite_bytes) != REVIEWED_SUITE_SHA256
    ):
        raise ValueError("reviewed CUDA quality suite bytes do not match the pinned v1 SHA-256")
    raw = json.loads(suite_bytes.decode("utf-8"))
    if raw.get("schema") != SUITE_SCHEMA or not isinstance(raw.get("id"), str):
        raise ValueError(f"unsupported CUDA quality suite: {resolved}")
    profiles = raw.get("threshold_profiles")
    if not isinstance(profiles, dict) or not profiles:
        raise ValueError("quality suite has no threshold profiles")
    for name, profile in profiles.items():
        if not isinstance(name, str) or not name:
            raise ValueError("quality suite threshold profile names must be non-empty")
        validate_threshold_profile(name, profile)
    selected = profile_name or raw.get("default_threshold_profile")
    if selected not in profiles:
        raise ValueError(f"unknown threshold profile {selected!r}")
    if raw.get("default_threshold_profile") != "exact-v1":
        raise ValueError("quality suite default threshold profile must remain exact-v1")
    hardware = raw.get("hardware_contract")
    if hardware != {
        "device_name": "NVIDIA L4",
        "compute_capability": "8.9",
        "visible_device_count": 1,
    }:
        raise ValueError("quality suite hardware contract must remain NVIDIA L4 / SM89")
    if raw.get("prompt_token_contract") != {
        "tokenizer": "embedded Gemma 4 GGUF BPE",
        "add_bos": True,
        "parse_special": True,
        "token_ids_sha256_encoding": "SHA-256 of the UTF-8 compact JSON integer array",
    }:
        raise ValueError("quality suite prompt-token hash contract is not the reviewed contract")
    generation = raw.get("generation_contract")
    if not isinstance(generation, dict):
        raise ValueError("quality suite generation contract is missing")
    _validate_generation_contract(generation)
    case_values = raw.get("cases")
    if not isinstance(case_values, list) or len(case_values) < 3:
        raise ValueError("quality suite must contain at least three prompts")
    cases: list[QualityCase] = []
    seen_ids: set[str] = set()
    for item in case_values:
        if not isinstance(item, dict):
            raise ValueError("quality suite case must be an object")
        case_id = item.get("id")
        if not isinstance(case_id, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]*", case_id):
            raise ValueError("quality suite case ID is invalid")
        if case_id in seen_ids:
            raise ValueError(f"duplicate quality suite case ID {case_id}")
        seen_ids.add(case_id)
        prompt_contract = item.get("prompt")
        if not isinstance(prompt_contract, dict):
            raise ValueError(f"{case_id} prompt contract is missing")
        source_provenance: list[dict[str, Any]] = []
        fixture_name = prompt_contract.get("fixture_path")
        if fixture_name is not None:
            if not isinstance(fixture_name, str) or pathlib.PurePosixPath(fixture_name).name != fixture_name:
                raise ValueError(f"{case_id} prompt fixture path must be a sibling filename")
            fixture_path = resolved.parent / fixture_name
            user, prompt, fixture_raw = _prompt_fixture_text(fixture_path)
            source_provenance.append(stable_file_provenance(fixture_path))
            for contract_name, fixture_field in (
                ("expected_prompt_utf8_bytes", "expected_reference_prompt_utf8_bytes"),
                ("expected_prompt_sha256", "expected_reference_prompt_sha256"),
                ("expected_prompt_tokens", "expected_reference_prompt_tokens"),
            ):
                if prompt_contract.get(contract_name) != fixture_raw.get(fixture_field):
                    raise ValueError(f"{case_id} suite and referenced fixture contracts disagree")
        else:
            user, prompt = _inline_prompt_text(prompt_contract, case_id)
        _verify_prompt_contract(case_id=case_id, contract=prompt_contract, user=user, prompt=prompt)
        max_tokens = _required_positive_int(item.get("max_tokens"), f"{case_id} max tokens")
        if len(prompt.encode("utf-8")) < 2000 or prompt_contract["expected_prompt_tokens"] < 2000:
            raise ValueError(f"{case_id} must remain a long-context prompt")
        if prompt_contract["expected_prompt_tokens"] + max_tokens > generation["capture_kv_capacity"]:
            raise ValueError(f"{case_id} exceeds the locked capture KV capacity")
        allow_divergence = item.get("allow_candidate_divergence")
        if not isinstance(allow_divergence, bool):
            raise ValueError(f"{case_id} allow_candidate_divergence must be boolean")
        output_contract = item.get("output_contract")
        _validate_output_contract(output_contract, case_id)
        cases.append(QualityCase(
            id=case_id,
            description=str(item.get("description") or ""),
            prompt=prompt,
            prompt_contract=prompt_contract,
            prompt_source_provenance=tuple(source_provenance),
            max_tokens=max_tokens,
            allow_candidate_divergence=allow_divergence,
            output_contract=output_contract,
        ))
    allowed_cases = sum(case.allow_candidate_divergence for case in cases)
    if allowed_cases / len(cases) > HARD_BOUNDED_POLICY["max_divergent_case_fraction"]:
        raise ValueError("too many suite cases permit bounded candidate divergence")
    return QualitySuite(
        path=resolved,
        raw=raw,
        cases=tuple(cases),
        profile_name=str(selected),
        profile=dict(profiles[selected]),
    )


def validate_output(text: str, contract: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    encoded = text.encode("utf-8")
    if "\ufffd" in text:
        errors.append("output contains the Unicode replacement character")
    if "\x00" in text:
        errors.append("output contains NUL")
    invalid_controls = sorted({ord(ch) for ch in text if ord(ch) < 32 and ch not in "\n\r\t"})
    if invalid_controls:
        errors.append(f"output contains control characters {invalid_controls}")
    minimum = int(contract.get("min_utf8_bytes", 1))
    maximum = contract.get("max_utf8_bytes")
    if len(encoded) < minimum:
        errors.append(f"output has {len(encoded)} UTF-8 bytes, below {minimum}")
    if maximum is not None and len(encoded) > int(maximum):
        errors.append(f"output has {len(encoded)} UTF-8 bytes, above {maximum}")
    word_count = len(re.findall(r"\b\w+(?:[-']\w+)*\b", text, re.UNICODE))
    if "min_words" in contract and word_count < int(contract["min_words"]):
        errors.append(f"output has {word_count} words, below {contract['min_words']}")
    if "max_words" in contract and word_count > int(contract["max_words"]):
        errors.append(f"output has {word_count} words, above {contract['max_words']}")
    for value in contract.get("required_substrings", []):
        if value not in text:
            errors.append(f"output is missing required substring {value!r}")
    for value in contract.get("forbidden_substrings", []):
        if value in text:
            errors.append(f"output contains forbidden substring {value!r}")
    for pattern in contract.get("required_fullmatch_regexes", []):
        try:
            matched = re.fullmatch(pattern, text.strip(), re.DOTALL)
        except re.error as exc:
            errors.append(f"invalid output-contract regex {pattern!r}: {exc}")
            continue
        if matched is None:
            errors.append(f"output does not full-match {pattern!r}")
    if json_contract := contract.get("json"):
        try:
            parsed = json.loads(text.strip())
        except json.JSONDecodeError as exc:
            errors.append(f"output is not the required bare JSON value: {exc.msg}")
        else:
            expected_type = dict if json_contract["type"] == "object" else list
            if not isinstance(parsed, expected_type):
                errors.append(f"output JSON is not a {json_contract['type']}")
            elif expected_type is dict:
                expected_keys = set(json_contract.get("exact_keys", []))
                if set(parsed) != expected_keys:
                    errors.append(
                        f"output JSON keys {sorted(parsed)} do not equal {sorted(expected_keys)}"
                    )
                for key, expected in json_contract.get("expected_values", {}).items():
                    if parsed.get(key) != expected:
                        errors.append(
                            f"output JSON value {key!r}={parsed.get(key)!r} does not equal {expected!r}"
                        )
    return errors


def parse_generate_stdout(stdout: bytes) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    try:
        text = stdout.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        return {
            "stdout_utf8": None,
            "prompt_token_ids": [],
            "token_ids": [],
            "output_text": "",
            "token_count": None,
            "finish_reason": None,
        }, [f"stdout is not valid UTF-8: {exc}"]
    prompt_matches = list(PROMPT_TOKEN_IDS_RE.finditer(text))
    token_matches = list(TOKEN_IDS_RE.finditer(text))
    if len(prompt_matches) != 1:
        errors.append(f"expected one prompt_token_ids record, found {len(prompt_matches)}")
    if len(token_matches) != 1:
        errors.append(f"expected one token_ids record, found {len(token_matches)}")
    prompt_ids = (
        [int(value) for value in prompt_matches[0].group("ids").split()]
        if len(prompt_matches) == 1
        else []
    )
    token_ids = (
        [int(value) for value in token_matches[0].group("ids").split()]
        if len(token_matches) == 1
        else []
    )
    output_text = ""
    if len(prompt_matches) == 1 and len(token_matches) == 1:
        start = prompt_matches[0].end()
        if start < len(text) and text[start] == "\n":
            start += 1
        end = token_matches[0].start()
        output_text = text[start:end]
        if output_text.endswith("\n"):
            output_text = output_text[:-1]
        if start > end:
            errors.append("token metadata ordering is invalid")
    token_records = list(TOKENS_RE.finditer(text))
    token_count: int | None = None
    finish_reason: str | None = None
    if len(token_records) != 1:
        errors.append(f"expected one generated token-count record, found {len(token_records)}")
    else:
        token_count = int(token_records[0].group("count"))
        finish_reason = token_records[0].group("finish")
        if token_count != len(token_ids):
            errors.append(f"token-count record {token_count} differs from {len(token_ids)} token IDs")
    return {
        "stdout_utf8": text,
        "prompt_token_ids": prompt_ids,
        "token_ids": token_ids,
        "output_text": output_text,
        "token_count": token_count,
        "finish_reason": finish_reason,
    }, errors


def normalized_text_similarity(baseline: str, candidate: str) -> float:
    def normalize(value: str) -> str:
        return " ".join(value.casefold().split())

    return difflib.SequenceMatcher(None, normalize(baseline), normalize(candidate), autojunk=False).ratio()


def token_divergence(baseline: list[int], candidate: list[int]) -> dict[str, Any]:
    common = min(len(baseline), len(candidate))
    mismatch_indexes = [index for index in range(common) if baseline[index] != candidate[index]]
    exact = baseline == candidate
    first = None if exact else (mismatch_indexes[0] if mismatch_indexes else common)
    mismatch_count = len(mismatch_indexes) + abs(len(baseline) - len(candidate))
    denominator = max(len(baseline), len(candidate), 1)
    fraction_denominator = max(common, 1)
    return {
        "exact_token_ids": exact,
        "baseline_tokens": len(baseline),
        "candidate_tokens": len(candidate),
        "output_length_delta_tokens": abs(len(baseline) - len(candidate)),
        "first_divergence_index": first,
        "first_divergence_fraction_of_common_length": (
            None if exact else float(first) / fraction_denominator
        ),
        "positional_mismatch_tokens": mismatch_count,
        "positional_divergence_rate": float(mismatch_count) / denominator,
    }


def divergence_errors(
    metrics: dict[str, Any],
    text_similarity: float,
    quality_case: QualityCase,
    profile: dict[str, Any],
) -> list[str]:
    if metrics["exact_token_ids"]:
        return []
    errors: list[str] = []
    if not profile["allow_token_divergence"]:
        errors.append("selected exact policy forbids generated-token divergence")
        return errors
    if not quality_case.allow_candidate_divergence:
        errors.append("this structured/exact canary forbids generated-token divergence")
        return errors
    if metrics["positional_divergence_rate"] > float(profile["max_token_divergence_rate"]):
        errors.append(
            f"token divergence rate {metrics['positional_divergence_rate']:.6f} exceeds "
            f"{profile['max_token_divergence_rate']:.6f}"
        )
    if metrics["output_length_delta_tokens"] > int(profile["max_output_length_delta_tokens"]):
        errors.append(
            f"output length delta {metrics['output_length_delta_tokens']} exceeds "
            f"{profile['max_output_length_delta_tokens']}"
        )
    if metrics["first_divergence_index"] < int(profile["min_first_divergence_index"]):
        errors.append(
            f"first divergence index {metrics['first_divergence_index']} is before "
            f"{profile['min_first_divergence_index']}"
        )
    if metrics["first_divergence_fraction_of_common_length"] < float(
        profile["min_first_divergence_fraction"]
    ):
        errors.append(
            "first divergence fraction "
            f"{metrics['first_divergence_fraction_of_common_length']:.6f} is below "
            f"{profile['min_first_divergence_fraction']:.6f}"
        )
    if text_similarity < float(profile["min_normalized_text_similarity"]):
        errors.append(
            f"normalized text similarity {text_similarity:.6f} is below "
            f"{profile['min_normalized_text_similarity']:.6f}"
        )
    return errors


def _counter_map(timing: dict[str, Any]) -> dict[str, Any]:
    value = timing.get("cuda")
    return value if isinstance(value, dict) else {}


def route_evidence(
    timing: dict[str, Any],
    spec: candidate_validator.CandidateSpec,
    enabled: bool,
    enforce_exact_qualification_counts: bool,
    generated_token_count: int | None = None,
) -> tuple[dict[str, Any], list[str]]:
    counters = _counter_map(timing)
    errors: list[str] = []
    names = {
        *(counter.name for counter in spec.required_route_counters),
        *(counter.name for counter in spec.required_baseline_route_counters),
        *(counter.name for counter in spec.forbidden_route_counters),
    }
    timing_metadata = candidate_validator.DEFAULT_TIMING_METADATA
    if spec.require_persistent_replay:
        names.update((
            timing_metadata.persistent_replay_counter,
            timing_metadata.graph_discard_counter,
            timing_metadata.graph_capacity_skip_counter,
        ))
    values: dict[str, int | None] = {}
    for name in sorted(names):
        raw = counters.get(name)
        if isinstance(raw, bool) or not isinstance(raw, (int, float)) or not math.isfinite(float(raw)):
            values[name] = None
            errors.append(f"route counter {name} is absent or non-numeric")
        elif float(raw) < 0 or not float(raw).is_integer():
            values[name] = None
            errors.append(f"route counter {name} is not a non-negative integer")
        else:
            values[name] = int(raw)
    for route in spec.required_route_counters:
        observed = values.get(route.name)
        if enabled and observed is not None and observed < 1:
            errors.append(f"candidate did not use required route {route.name}")
        if not enabled and observed is not None and observed != 0:
            errors.append(f"baseline unexpectedly used candidate route {route.name}: {observed}")
    for route in spec.required_baseline_route_counters:
        observed = values.get(route.name)
        if not enabled and observed is not None and observed < 1:
            errors.append(f"baseline did not use required route {route.name}")
    if enabled:
        for route in spec.forbidden_route_counters:
            observed = values.get(route.name)
            if observed is not None and observed != 0:
                errors.append(f"candidate reported forbidden route/fallback {route.name}: {observed}")
        if enforce_exact_qualification_counts:
            for expectation in spec.qualification_route_counts:
                observed = values.get(expectation.name)
                if observed is not None and observed != expectation.exact_count:
                    errors.append(
                        f"candidate route {expectation.name} count {observed} does not equal "
                        f"locked count {expectation.exact_count}"
                    )
    minimum_persistent_replays: int | None = None
    if spec.require_persistent_replay:
        minimum_persistent_replays = max(1, (generated_token_count or 0) - 8)
        arm = "candidate" if enabled else "baseline"
        observed_replays = values.get(timing_metadata.persistent_replay_counter)
        if observed_replays is not None and observed_replays < minimum_persistent_replays:
            errors.append(
                f"{arm} persistent replays {observed_replays} below "
                f"{minimum_persistent_replays}"
            )
        for counter_name, label in (
            (timing_metadata.graph_discard_counter, "graph capture discards"),
            (timing_metadata.graph_capacity_skip_counter, "graph capacity skips"),
        ):
            observed = values.get(counter_name)
            if observed is not None and observed != 0:
                errors.append(f"{arm} reported {label}: {observed}")
    return {
        "enabled": enabled,
        "candidate_gate": spec.environment_variable,
        "candidate_gate_value": spec.candidate_gate_value if enabled else spec.baseline_gate_value,
        "counter_group": "cuda",
        "counters": values,
        "required_candidate_routes": [counter.name for counter in spec.required_route_counters],
        "required_baseline_routes": [counter.name for counter in spec.required_baseline_route_counters],
        "forbidden_candidate_routes": [counter.name for counter in spec.forbidden_route_counters],
        "exact_qualification_counts_enforced": enforce_exact_qualification_counts,
        "persistent_graph_replay": {
            "required": spec.require_persistent_replay,
            "minimum_replays": minimum_persistent_replays,
            "replay_counter": timing_metadata.persistent_replay_counter,
            "discard_counter": timing_metadata.graph_discard_counter,
            "capacity_skip_counter": timing_metadata.graph_capacity_skip_counter,
        },
    }, errors


def qualification_workload_identity(
    workload: candidate_validator.QualificationWorkload | None,
) -> dict[str, Any] | None:
    if workload is None:
        return None
    return {
        "fixture_id": workload.fixture_id,
        "fixture_file_sha256": workload.fixture_file_sha256,
        "benchmark_prompt_sha256": workload.benchmark_prompt_sha256,
        "benchmark_prompt_tokens": workload.benchmark_prompt_tokens,
        "lengths": list(workload.lengths),
        "cache_dtype": workload.cache_dtype,
        "prefill_chunk_size": workload.prefill_chunk_size,
        "capture_kv_capacity": workload.capture_kv_capacity,
    }


def candidate_spec_identity(spec: candidate_validator.CandidateSpec) -> dict[str, Any]:
    return {
        "kernel_id": spec.kernel_id,
        "environment_variable": spec.environment_variable,
        "baseline_gate_value": spec.baseline_gate_value,
        "candidate_gate_value": spec.candidate_gate_value,
        "route_phase": spec.route_phase,
        "required_route_counters": [counter.name for counter in spec.required_route_counters],
        "required_baseline_route_counters": [
            counter.name for counter in spec.required_baseline_route_counters
        ],
        "forbidden_route_counters": [counter.name for counter in spec.forbidden_route_counters],
        "fixed_comparison_environment": dict(spec.fixed_comparison_environment),
        "qualification_route_counts": {
            item.name: item.exact_count for item in spec.qualification_route_counts
        },
        "qualification_workload": qualification_workload_identity(spec.qualification_workload),
        "require_persistent_replay": spec.require_persistent_replay,
    }


def validate_quality_candidate_contract(
    suite: QualitySuite,
    spec: candidate_validator.CandidateSpec,
) -> None:
    """Bind catalog qualification shapes to the reviewed multi-prompt suite."""
    workload = spec.qualification_workload
    if workload is None:
        return
    generation = suite.raw["generation_contract"]
    observed_generation = {
        "cache_dtype": generation.get("cache_dtype"),
        "prefill_chunk_size": generation.get("prefill_chunk_size"),
        "capture_kv_capacity": generation.get("capture_kv_capacity"),
    }
    expected_generation = {
        "cache_dtype": workload.cache_dtype,
        "prefill_chunk_size": workload.prefill_chunk_size,
        "capture_kv_capacity": workload.capture_kv_capacity,
    }
    if observed_generation != expected_generation:
        raise ValueError(
            "quality suite generation contract does not match the selected catalog workload"
        )
    matching_cases = [
        quality_case
        for quality_case in suite.cases
        if quality_case.prompt_contract["expected_prompt_sha256"]
        == workload.benchmark_prompt_sha256
        and quality_case.prompt_contract["expected_prompt_tokens"]
        == workload.benchmark_prompt_tokens
        and quality_case.max_tokens in workload.lengths
    ]
    if len(matching_cases) != 1:
        raise ValueError(
            "quality suite must contain exactly one case matching the selected catalog workload"
        )
    source_hashes = {
        source["sha256"]
        for source in matching_cases[0].prompt_source_provenance
        if isinstance(source.get("sha256"), str)
    }
    if workload.fixture_file_sha256 not in source_hashes:
        raise ValueError(
            "quality suite workload fixture does not match the selected catalog workload"
        )


def build_runtime_environment(
    parent: dict[str, str],
    spec: candidate_validator.CandidateSpec,
    enabled: bool,
    capture_kv_capacity: int,
) -> dict[str, str]:
    env = {name: parent[name] for name in SAFE_PARENT_ENVIRONMENT_KEYS if name in parent}
    env.update(spec.fixed_comparison_environment)
    env[spec.environment_variable] = (
        spec.candidate_gate_value if enabled else spec.baseline_gate_value
    )
    env[candidate_validator.CAPTURE_KV_CAPACITY_ENV] = str(capture_kv_capacity)
    env["ANTFLY_SERVER_DISABLE_CONTINUOUS_BATCHING"] = "1"
    env["ANTFLY_SERVER_DECODE_GRAPH_REPLAY"] = "required"
    return env


def runtime_environment_identity(env: dict[str, str]) -> dict[str, Any]:
    values = {name: env[name] for name in sorted(env)}
    return {
        "schema": "antfly.gemma4_cuda_quality_runtime_environment.v1",
        "values": values,
        "sha256": canonical_sha256(values),
    }


def _timing_latency(timing: dict[str, Any]) -> dict[str, Any]:
    timing_ms = timing.get("timing_ms") if isinstance(timing.get("timing_ms"), dict) else {}

    def finite(name: str) -> float | None:
        value = timing_ms.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        parsed = float(value)
        return parsed if math.isfinite(parsed) and parsed >= 0.0 else None

    return {
        "prefill_inner_ms": finite("prefill_inner"),
        "decode_inner_ms": finite("decode_inner"),
        "generate_ms": finite("generate"),
        "decode_tok_per_s": (
            float(timing["decode_tok_per_s"])
            if isinstance(timing.get("decode_tok_per_s"), (int, float))
            and not isinstance(timing.get("decode_tok_per_s"), bool)
            and math.isfinite(float(timing["decode_tok_per_s"]))
            else None
        ),
        "excluded_from_quality_verdict": True,
    }


def build_generate_command(
    *,
    wrapper: pathlib.Path,
    binary: pathlib.Path,
    model: pathlib.Path,
    prompt: str,
    max_tokens: int,
    timing_path: pathlib.Path,
    generation: dict[str, Any],
) -> list[str]:
    return [
        str(wrapper),
        str(binary),
        "generate",
        str(model),
        prompt,
        "--backend", "cuda",
        "--combined-budget-mb", str(generation["combined_budget_mb"]),
        "--backend-budget-mb", str(generation["backend_budget_mb"]),
        "--kv-budget-mb", str(generation["kv_budget_mb"]),
        "--scratch-budget-mb", str(generation["scratch_budget_mb"]),
        "--prefill-chunk-size", str(generation["prefill_chunk_size"]),
        "--cache-dtype", generation["cache_dtype"],
        "--max-tokens", str(max_tokens),
        "--temperature", "0",
        "--raw-prompt",
        "--no-chat-template",
        "--print-finish-reason",
        "--print-token-count",
        "--print-prompt-token-ids",
        "--print-token-ids",
        "--print-timing",
        "--json-timing", str(timing_path),
    ]


def run_sample(
    *,
    args: argparse.Namespace,
    suite: QualitySuite,
    quality_case: QualityCase,
    spec: candidate_validator.CandidateSpec,
    enabled: bool,
    repetition: int,
    order_index: int,
) -> dict[str, Any]:
    arm = "candidate" if enabled else "baseline"
    stem = f"{quality_case.id}-r{repetition + 1:02d}-{order_index + 1:02d}-{arm}"
    timing_path = args.output_dir / f"{stem}.timing.json"
    stdout_path = args.output_dir / f"{stem}.stdout.log"
    stderr_path = args.output_dir / f"{stem}.stderr.log"
    generation = suite.raw["generation_contract"]
    command = build_generate_command(
        wrapper=args.wrapper,
        binary=args.binary,
        model=args.model,
        prompt=quality_case.prompt,
        max_tokens=quality_case.max_tokens,
        timing_path=timing_path,
        generation=generation,
    )
    env = build_runtime_environment(
        dict(os.environ), spec, enabled, int(generation["capture_kv_capacity"])
    )
    started = dt.datetime.now(dt.timezone.utc)
    errors: list[str] = []
    try:
        completed = subprocess.run(
            command,
            cwd=repo_root_from_script(),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=args.timeout_sec,
            check=False,
        )
        returncode = completed.returncode
        stdout = completed.stdout
        stderr = completed.stderr
    except subprocess.TimeoutExpired as exc:
        returncode = 124
        stdout = exc.stdout or b""
        stderr = (exc.stderr or b"") + f"\ncommand timed out after {args.timeout_sec}s\n".encode()
    finished = dt.datetime.now(dt.timezone.utc)
    stdout_path.write_bytes(stdout)
    stderr_path.write_bytes(stderr)
    if returncode != 0:
        errors.append(f"{arm} command exited {returncode}")
    parsed, parse_errors = parse_generate_stdout(stdout)
    errors.extend(parse_errors)
    prompt_ids = parsed["prompt_token_ids"]
    expected_prompt_tokens = quality_case.prompt_contract["expected_prompt_tokens"]
    expected_prompt_ids_sha = quality_case.prompt_contract["expected_prompt_token_ids_sha256"]
    observed_prompt_ids_sha = canonical_sha256(prompt_ids)
    if len(prompt_ids) != expected_prompt_tokens:
        errors.append(
            f"prompt token count {len(prompt_ids)} does not equal locked {expected_prompt_tokens}"
        )
    if observed_prompt_ids_sha != expected_prompt_ids_sha:
        errors.append("prompt token IDs do not match the locked SHA-256 contract")
    token_ids = parsed["token_ids"]
    if not token_ids:
        errors.append("generated token IDs are empty")
    if len(token_ids) > quality_case.max_tokens:
        errors.append(
            f"generated {len(token_ids)} tokens, above max {quality_case.max_tokens}"
        )
    errors.extend(validate_output(parsed["output_text"], quality_case.output_contract))
    timing: dict[str, Any] = {}
    if not timing_path.is_file():
        errors.append("generate command did not write JSON timing evidence")
    else:
        try:
            timing = json.loads(timing_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            errors.append(f"JSON timing evidence is invalid: {exc}")
    workload = spec.qualification_workload
    enforce_exact_counts = bool(
        workload is not None
        and spec.qualification_route_counts
        and workload.benchmark_prompt_tokens == expected_prompt_tokens
        and workload.cache_dtype == generation["cache_dtype"]
        and workload.prefill_chunk_size == generation["prefill_chunk_size"]
        and workload.capture_kv_capacity == generation["capture_kv_capacity"]
        and quality_case.max_tokens in workload.lengths
    )
    routes, route_errors = route_evidence(
        timing,
        spec,
        enabled,
        enforce_exact_counts,
        generated_token_count=len(token_ids),
    )
    errors.extend(route_errors)
    timing_provenance = stable_file_provenance(timing_path) if timing_path.is_file() else None
    return {
        "schema": SAMPLE_SCHEMA,
        "case_id": quality_case.id,
        "repetition": repetition + 1,
        "order_index": order_index + 1,
        "arm": arm,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "elapsed_sec": (finished - started).total_seconds(),
        "returncode": returncode,
        "command": {
            "argv_without_prompt": command[:4] + ["<prompt-sha256>"] + command[5:],
            "prompt_sha256": quality_case.prompt_contract["expected_prompt_sha256"],
        },
        "runtime_environment": runtime_environment_identity(env),
        "files": {
            "stdout": stable_file_provenance(stdout_path),
            "stderr": stable_file_provenance(stderr_path),
            "timing": timing_provenance,
        },
        "prompt_token_ids": {
            "count": len(prompt_ids),
            "sha256": observed_prompt_ids_sha,
            "expected_count": expected_prompt_tokens,
            "expected_sha256": expected_prompt_ids_sha,
        },
        "generated": {
            "token_ids": token_ids,
            "token_ids_sha256": canonical_sha256(token_ids),
            "token_count": len(token_ids),
            "finish_reason": parsed["finish_reason"],
            "text": parsed["output_text"],
            "text_utf8_bytes": len(parsed["output_text"].encode("utf-8")),
            "text_sha256": sha256_bytes(parsed["output_text"].encode("utf-8")),
        },
        "route_evidence": routes,
        "latency": _timing_latency(timing),
        "errors": errors,
        "passed": not errors,
    }


def evaluate_case(
    quality_case: QualityCase,
    repetitions: list[dict[str, Any]],
    profile: dict[str, Any],
) -> dict[str, Any]:
    errors: list[str] = []
    baseline_samples = [item["baseline"] for item in repetitions]
    candidate_samples = [item["candidate"] for item in repetitions]
    for label, samples in (("baseline", baseline_samples), ("candidate", candidate_samples)):
        reference_tokens = samples[0]["generated"]["token_ids"]
        reference_text = samples[0]["generated"]["text"]
        for sample in samples[1:]:
            if sample["generated"]["token_ids"] != reference_tokens:
                errors.append(f"{label} greedy token IDs are not deterministic across repetitions")
            if sample["generated"]["text"] != reference_text:
                errors.append(f"{label} decoded text is not deterministic across repetitions")
    pairs: list[dict[str, Any]] = []
    for item in repetitions:
        baseline = item["baseline"]
        candidate = item["candidate"]
        divergence = token_divergence(
            baseline["generated"]["token_ids"], candidate["generated"]["token_ids"]
        )
        similarity = normalized_text_similarity(
            baseline["generated"]["text"], candidate["generated"]["text"]
        )
        pair_errors = [
            *(f"baseline: {error}" for error in baseline["errors"]),
            *(f"candidate: {error}" for error in candidate["errors"]),
            *divergence_errors(divergence, similarity, quality_case, profile),
        ]
        if (
            divergence["exact_token_ids"]
            and baseline["generated"]["text"] != candidate["generated"]["text"]
        ):
            pair_errors.append("identical token IDs decoded to different baseline/candidate text")
        if baseline["prompt_token_ids"] != candidate["prompt_token_ids"]:
            pair_errors.append("baseline and candidate prompt-token attestations differ")
        errors.extend(f"repeat {item['repetition']}: {error}" for error in pair_errors)
        pairs.append({
            "repetition": item["repetition"],
            "execution_order": item["execution_order"],
            "baseline": baseline,
            "candidate": candidate,
            "token_divergence": divergence,
            "normalized_text_similarity": similarity,
            "latency_excluded_from_quality_verdict": True,
            "errors": pair_errors,
            "passed": not pair_errors,
        })
    exact = all(pair["token_divergence"]["exact_token_ids"] for pair in pairs)
    return {
        "case_id": quality_case.id,
        "description": quality_case.description,
        "prompt_contract": {
            "utf8_bytes": quality_case.prompt_contract["expected_prompt_utf8_bytes"],
            "sha256": quality_case.prompt_contract["expected_prompt_sha256"],
            "token_count": quality_case.prompt_contract["expected_prompt_tokens"],
            "token_ids_sha256": quality_case.prompt_contract[
                "expected_prompt_token_ids_sha256"
            ],
        },
        "allow_candidate_divergence": quality_case.allow_candidate_divergence,
        "output_contract": quality_case.output_contract,
        "greedy_determinism": {
            "baseline": not any(error.startswith("baseline greedy") for error in errors),
            "candidate": not any(error.startswith("candidate greedy") for error in errors),
        },
        "exact_token_ids": exact,
        "pairs": pairs,
        "errors": errors,
        "passed": not errors,
    }


def evaluate_suite_cases(
    suite: QualitySuite,
    repetitions_by_case: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    cases = [
        evaluate_case(case, repetitions_by_case[case.id], suite.profile)
        for case in suite.cases
    ]
    divergent_cases = [case["case_id"] for case in cases if not case["exact_token_ids"]]
    divergent_fraction = len(divergent_cases) / len(cases)
    errors = [f"{case['case_id']}: {error}" for case in cases for error in case["errors"]]
    if divergent_fraction > float(suite.profile["max_divergent_case_fraction"]):
        errors.append(
            f"divergent case fraction {divergent_fraction:.6f} exceeds "
            f"{suite.profile['max_divergent_case_fraction']:.6f}"
        )
    diagnostic_passed = not errors
    exact_observed = not divergent_cases
    exact_policy = (
        suite.profile_name == "exact-v1"
        and not suite.profile["allow_token_divergence"]
    )
    reviewed_policy = exact_policy and suite.path == reviewed_suite_path().resolve()
    quality_qualified = diagnostic_passed and reviewed_policy and exact_observed
    return {
        "cases": cases,
        "case_count": len(cases),
        "divergent_cases": divergent_cases,
        "divergent_case_fraction": divergent_fraction,
        "all_generated_token_ids_exact": exact_observed,
        "diagnostic_thresholds_passed": diagnostic_passed,
        "quality_qualified": quality_qualified,
        "reviewed_suite": suite.path == reviewed_suite_path().resolve(),
        "collect_only": not reviewed_policy,
        "errors": errors,
    }


def _parse_colon_fields(output: bytes) -> dict[str, str]:
    fields: dict[str, str] = {}
    text = output.decode("utf-8", errors="replace")
    for line in text.splitlines():
        name, separator, value = line.partition(":")
        if separator and name.strip() and value.strip():
            fields[name.strip()] = value.strip()
    return fields


def embedded_artifact_identity(
    binary: pathlib.Path,
    artifact: pathlib.Path,
    environment: dict[str, str],
    timeout_sec: int,
    output_dir: pathlib.Path,
) -> dict[str, Any]:
    command = [str(binary), "cuda-info", "--artifact-identity"]
    completed = subprocess.run(
        command,
        cwd=inference_root_from_script(),
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_sec,
        check=False,
    )
    log = completed.stdout + completed.stderr
    log_path = output_dir / "embedded_cuda_artifact_identity.log"
    log_path.write_bytes(log)
    fields = _parse_colon_fields(completed.stdout)
    artifact_info = stable_file_provenance(artifact)
    expected = {
        "cuda_artifact_identity_schema": "antfly.cuda_artifact_identity.v1",
        "cuda_artifact_mode": "sm89",
        "cuda_artifact_format": "cubin",
        "cuda_artifact_target": "sm_89",
        "cuda_artifact_image_bytes": str(artifact_info["bytes"]),
        "cuda_artifact_image_sha256": artifact_info["sha256"],
    }
    errors = []
    if completed.returncode != 0:
        errors.append(f"artifact identity command exited {completed.returncode}")
    for name, value in expected.items():
        if fields.get(name) != value:
            errors.append(f"artifact identity {name}={fields.get(name)!r}, expected {value!r}")
    return {
        "command": command,
        "log": stable_file_provenance(log_path),
        "returncode": completed.returncode,
        "observed": fields,
        "expected": expected,
        "errors": errors,
        "passed": not errors,
    }


def git_content_identity(value: dict[str, Any]) -> dict[str, Any]:
    return {
        "commit": value.get("commit"),
        "tracked_diff_sha256": value.get("tracked_diff_sha256"),
        "untracked_inventory_sha256": value.get("untracked_inventory_sha256"),
        "status_sha256": value.get("status_sha256"),
    }


def collect_input_provenance(
    args: argparse.Namespace,
    suite: QualitySuite,
    spec: candidate_validator.CandidateSpec,
) -> dict[str, Any]:
    inference = inference_root_from_script()
    files = {
        "harness": stable_file_provenance(pathlib.Path(__file__)),
        "suite": stable_file_provenance(suite.path),
        "binary": stable_file_provenance(args.binary),
        "model": stable_file_provenance(args.model),
        "wrapper": stable_file_provenance(args.wrapper),
        "tuning_profile": stable_file_provenance(
            inference / "scripts/gemma4_qat_cuda_tuning.sh"
        ),
        "candidate_catalog": stable_file_provenance(
            inference / "scripts/validate_gemma4_cuda_candidate.py"
        ),
        "sm89_cubin": stable_file_provenance(args.cuda_artifact),
    }
    referenced: dict[str, dict[str, Any]] = {}
    for case in suite.cases:
        for item in case.prompt_source_provenance:
            referenced[item["path"]] = item
    files["referenced_prompt_fixtures"] = [referenced[name] for name in sorted(referenced)]
    git = release_provenance.git_provenance(repo_root_from_script())
    git_errors = release_provenance.git_content_provenance_errors(git)
    gpu = release_provenance.gpu_provenance()
    gpu_errors = release_provenance.l4_errors(gpu)
    return {
        "collected_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "python": {
            "version": sys.version,
            "executable": stable_file_provenance(pathlib.Path(sys.executable)),
            "platform": platform.platform(),
        },
        "files": files,
        "git": git,
        "git_content_identity": git_content_identity(git),
        "git_content_errors": git_errors,
        "gpu": gpu,
        "gpu_errors": gpu_errors,
        "candidate": candidate_spec_identity(spec),
        "suite_id": suite.raw["id"],
        "suite_file_sha256": files["suite"]["sha256"],
        "threshold_profile_name": suite.profile_name,
        "threshold_profile": suite.profile,
        "threshold_profile_sha256": canonical_sha256(suite.profile),
    }


def provenance_stability_errors(before: dict[str, Any], after: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    before_files = before["files"]
    after_files = after["files"]
    if before_files != after_files:
        changed = sorted(name for name in before_files if before_files.get(name) != after_files.get(name))
        errors.append("hashed inputs changed during the run: " + ", ".join(changed))
    if before["git_content_identity"] != after["git_content_identity"]:
        errors.append("Git tracked/untracked content changed during the run")
    if before["gpu"] != after["gpu"]:
        errors.append("visible GPU identity changed during the run")
    errors.extend(f"pre-run provenance: {error}" for error in before["git_content_errors"])
    errors.extend(f"post-run provenance: {error}" for error in after["git_content_errors"])
    errors.extend(f"pre-run hardware: {error}" for error in before["gpu_errors"])
    errors.extend(f"post-run hardware: {error}" for error in after["gpu_errors"])
    return errors


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    repo = repo_root_from_script()
    inference = inference_root_from_script()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kernel-id", required=True, help="cataloged CUDA candidate kernel ID")
    parser.add_argument("--model", type=pathlib.Path, required=True)
    parser.add_argument(
        "--binary", type=pathlib.Path, default=inference / "zig-out/bin/antfly-inference"
    )
    parser.add_argument(
        "--wrapper",
        type=pathlib.Path,
        default=inference / "scripts/with_gemma4_qat_cuda_tuning.sh",
    )
    parser.add_argument(
        "--suite",
        type=pathlib.Path,
        default=inference / "scripts/fixtures/gemma4_cuda_quality_suite_v1.json",
    )
    parser.add_argument(
        "--threshold-profile",
        default=None,
        help="versioned suite profile; default remains exact-v1; bounded profiles are collect-only",
    )
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--timeout-sec", type=int, default=600)
    args = parser.parse_args(argv)
    args.model = args.model.resolve()
    args.binary = args.binary.resolve()
    args.wrapper = args.wrapper.resolve()
    args.suite = args.suite.resolve()
    args.cuda_artifact = (
        inference / "src/ops/cuda/artifacts/inference_cuda_kernels_sm89.cubin"
    ).resolve()
    args.output_dir = args.output_dir.resolve()
    if args.repeats < 2:
        parser.error("--repeats must be at least 2 to attest greedy determinism")
    if args.timeout_sec < 1:
        parser.error("--timeout-sec must be positive")
    for label, path in (
        ("model", args.model),
        ("binary", args.binary),
        ("wrapper", args.wrapper),
        ("suite", args.suite),
        ("CUDA artifact", args.cuda_artifact),
    ):
        if not path.is_file():
            parser.error(f"{label} does not exist: {path}")
    try:
        args.output_dir.relative_to(repo)
    except ValueError:
        pass
    else:
        parser.error("--output-dir must be outside the repository so evidence cannot change source provenance")
    if args.output_dir.exists():
        parser.error("--output-dir must not already exist")
    return args


def resolve_catalog_spec(kernel_id: str) -> candidate_validator.CandidateSpec:
    spec = candidate_validator.CANDIDATE_CATALOG.get(kernel_id)
    if spec is None:
        raise ValueError(
            f"quality qualification requires a cataloged candidate; unknown kernel ID {kernel_id!r}"
        )
    return spec


def run_gate(args: argparse.Namespace) -> dict[str, Any]:
    suite = load_suite(args.suite, args.threshold_profile)
    spec = resolve_catalog_spec(args.kernel_id)
    validate_quality_candidate_contract(suite, spec)
    before = collect_input_provenance(args, suite, spec)
    preflight_errors = [
        *(f"source provenance: {error}" for error in before["git_content_errors"]),
        *(f"hardware provenance: {error}" for error in before["gpu_errors"]),
    ]
    if preflight_errors:
        raise RuntimeError("; ".join(preflight_errors))
    args.output_dir.mkdir(parents=True, exist_ok=False)
    baseline_env = build_runtime_environment(
        dict(os.environ), spec, False, suite.raw["generation_contract"]["capture_kv_capacity"]
    )
    artifact_identity = embedded_artifact_identity(
        args.binary,
        args.cuda_artifact,
        baseline_env,
        args.timeout_sec,
        args.output_dir,
    )
    if artifact_identity["errors"]:
        raise RuntimeError("; ".join(artifact_identity["errors"]))
    repetitions_by_case: dict[str, list[dict[str, Any]]] = {}
    global_pair = 0
    for quality_case in suite.cases:
        repetitions: list[dict[str, Any]] = []
        for repetition in range(args.repeats):
            labels = balanced_pair_order(global_pair + 1, "baseline", "candidate")
            samples: dict[str, dict[str, Any]] = {}
            for order_index, label in enumerate(labels):
                samples[label] = run_sample(
                    args=args,
                    suite=suite,
                    quality_case=quality_case,
                    spec=spec,
                    enabled=label == "candidate",
                    repetition=repetition,
                    order_index=order_index,
                )
            repetitions.append({
                "repetition": repetition + 1,
                "execution_order": list(labels),
                "baseline": samples["baseline"],
                "candidate": samples["candidate"],
            })
            global_pair += 1
        repetitions_by_case[quality_case.id] = repetitions
    evaluation = evaluate_suite_cases(suite, repetitions_by_case)
    after = collect_input_provenance(args, suite, spec)
    provenance_errors = provenance_stability_errors(before, after)
    artifact_errors = artifact_identity["errors"]
    diagnostic_passed = (
        evaluation["diagnostic_thresholds_passed"]
        and not provenance_errors
        and not artifact_errors
    )
    exact_policy = (
        suite.profile_name == "exact-v1"
        and not suite.profile["allow_token_divergence"]
        and suite.path == reviewed_suite_path().resolve()
    )
    quality_qualified = (
        diagnostic_passed
        and exact_policy
        and evaluation["all_generated_token_ids_exact"]
    )
    evidence: dict[str, Any] = {
        "schema": EVIDENCE_SCHEMA,
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "suite": {
            "id": suite.raw["id"],
            "path": str(suite.path),
            "sha256": before["files"]["suite"]["sha256"],
            "threshold_profile_name": suite.profile_name,
            "threshold_profile": suite.profile,
        },
        "candidate": candidate_spec_identity(spec),
        "repeats": args.repeats,
        "execution_order_policy": "balanced alternating baseline/candidate pairs",
        "generation_contract": suite.raw["generation_contract"],
        "logit_quality_capability": LOGIT_CAPABILITY,
        "evaluation": evaluation,
        "artifact_identity": artifact_identity,
        "provenance": {
            "before": before,
            "after": after,
            "stability_errors": provenance_errors,
        },
        "verdict": {
            "diagnostic_thresholds_passed": diagnostic_passed,
            "quality_qualified": quality_qualified,
            "collect_only": not exact_policy,
            "exact_policy_selected": exact_policy,
            "all_generated_token_ids_exact": evaluation["all_generated_token_ids_exact"],
            "performance_metrics_used": False,
            "latency_recorded_separately": True,
            "promotion_authority": "none",
            "may_enable_or_default_candidate": False,
            "non_exact_production_equivalence_available": False,
            "errors": [*evaluation["errors"], *artifact_errors, *provenance_errors],
        },
    }
    evidence["content_sha256"] = canonical_sha256(evidence)
    output_path = args.output_dir / "quality_evidence.json"
    output_path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return evidence


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        evidence = run_gate(args)
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        print(f"quality gate failed closed: {exc}", file=sys.stderr)
        return 2
    verdict = evidence["verdict"]
    print(
        json.dumps({
            "schema": EVIDENCE_SCHEMA,
            "evidence": str(args.output_dir / "quality_evidence.json"),
            "content_sha256": evidence["content_sha256"],
            "diagnostic_thresholds_passed": verdict["diagnostic_thresholds_passed"],
            "quality_qualified": verdict["quality_qualified"],
            "collect_only": verdict["collect_only"],
            "errors": verdict["errors"],
        }, sort_keys=True)
    )
    return 0 if verdict["quality_qualified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
