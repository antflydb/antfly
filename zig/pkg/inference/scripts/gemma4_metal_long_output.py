#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Parser and statistical contract for the Gemma4 Metal long-output gate."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import statistics
import subprocess
import sys
from typing import Any, Iterable


class BenchmarkContractError(RuntimeError):
    """Raised when a raw benchmark artifact violates the comparison contract."""


_FLOAT = r"(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?"
_PERF_PREFIX = (
    r"(?m)^[^\r\n]*?\b"
    r"(?P<logger>common_perf_print|llama_perf_context_print):\s*"
)
_POLICY_ENV_NAMES = (
    "TERMITE_METAL_ENABLE_PREFILL_SG_DIRECT_LOAD",
    "TERMITE_METAL_DISABLE_PREFILL_SG_DIRECT_LOAD",
    "TERMITE_METAL_Q4_0_MMV_VARIANT",
    "TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO",
    "TERMITE_METAL_TRACE_Q4_0_MMV_VARIANT",
    "TERMITE_METAL_DISABLE_SWA_SCAN_CLAMP",
    "TERMITE_METAL_DISABLE_FAST_PREPARED_FRAME",
    "TERMITE_METAL_FORCE_DIAGNOSTIC_COMMAND_BUFFERS",
    "TERMITE_METAL_DECODE_GQA_SPLIT_SWA_VARIANT",
    "TERMITE_METAL_DECODE_GQA_SPLIT_GLOBAL_VARIANT",
    "TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE",
    "EXPECT_GENERATED_FLASH_PREFILL_CALLS",
    "EXPECT_GENERATED_FLASH_PREFILL_HD512_CALLS",
    "EXPECT_PREFILL_DIRECT_KV",
    "EXPECT_FAST_PREPARED_FRAME",
    "EXPECT_Q4_0_MMV_VARIANT",
    "EXPECT_SWA_SCAN_CLAMP",
    "EXPECT_ANTFLY_METAL_DEVICE",
    "EXPECT_LLAMA_METAL_DEVICE",
    "EXPECT_LLAMA_OFFLOADED_LAYERS",
)
_POLICY_ENV_PREFIXES = (
    "TERMITE_",
    "ANTFLY_GEMMA4_",
    "ANTFLY_INFERENCE_",
    "LLAMA_ARG_",
    "LLAMA_LOG_",
    "GGML_",
)
_TRUE_ENV_VALUES = frozenset(("1", "true", "yes", "on"))
_FALSE_ENV_VALUES = frozenset(("0", "false", "no", "off"))
_BOOTSTRAP_CONFIDENCE = 0.95
_BOOTSTRAP_SAMPLES = 20_000
_CONFIDENCE_MIN_RUNS = 6
_LLAMA_BUNDLE_SCHEMA = "antfly.llama_cpp_bundle_manifest.v1"
_CANONICAL_PROMPT_TOKEN_IDS_SHA256 = (
    "d882b403c0229eb7ffc70ff2539123283996548d5eb67a4ef34db619be6e8a42"
)
_CANONICAL_OUTPUT_TOKEN_IDS_SHA256 = (
    "711ddb9890d0fd867d7cd9c1ce10fe4c407a2ec597464fe42912a0802afe7052"
)
_CANONICAL_LLAMA_CPP_BUILD = 10182
_CANONICAL_LLAMA_CPP_SHA256 = (
    "faa8b1c2a6c69f50b0fcec71af86eda757d34f78bbbddbb3f485f170bc586d2f"
)
_CANONICAL_LLAMA_CPP_BUNDLE_SHA256 = (
    "23e601e646bbd901c4d4f1c1158fd4c99053d08969e6aa07f2005e87dc05a1fc"
)
_CANONICAL_PROMPT_SHA256 = (
    "1c0477d5acd34e3c76c1db35506df4f5eb66e59084efaf3aa36d8a2fe515a01f"
)
_LOADER_ENV_PREFIXES = ("DYLD_",)
_LOADER_ENV_NAMES = ("LD_LIBRARY_PATH", "LD_PRELOAD")
_GIT_ENV_PREFIXES = ("GIT_",)
_SYSTEM_LIBRARY_PREFIXES = (
    "/usr/lib/",
    "/System/Library/",
    "/System/Cryptexes/OS/",
    "/System/Volumes/Preboot/Cryptexes/OS/",
)
_REQUIRED_LLAMA_BUNDLE_LOAD_PATTERNS = (
    ("libllama-completion-impl", r"libllama-completion-impl(?:\..*)?"),
    ("libllama-common", r"libllama-common(?:\..*)?"),
    ("libllama core", r"libllama\..+"),
    ("libggml core", r"libggml\..+"),
    ("libggml-metal", r"libggml-metal(?:\..*)?"),
)
_CANONICAL_EVIDENCE_SENTENCE = (
    "You answer questions about indexed files using only evidence. "
    "Evidence: Spella Caffe Logo.pdf is in /Users/timkaye/Downloads. "
    "Spella Caffe Logo Two Color.pdf is in /Users/timkaye/Downloads. "
    "Ignore unrelated source code. "
)
_CANONICAL_ROUTE_ENV = {
    "EXPECT_GENERATED_FLASH_PREFILL_CALLS": "35",
    "EXPECT_GENERATED_FLASH_PREFILL_HD512_CALLS": "7",
    "EXPECT_PREFILL_DIRECT_KV": "0",
    "EXPECT_FAST_PREPARED_FRAME": "1",
    "EXPECT_Q4_0_MMV_VARIANT": "nr4-nsg2",
    "EXPECT_SWA_SCAN_CLAMP": "1",
    "EXPECT_ANTFLY_METAL_DEVICE": "Apple M4",
    "EXPECT_LLAMA_METAL_DEVICE": "Apple M4",
    "EXPECT_LLAMA_OFFLOADED_LAYERS": "43",
}


@dataclass(frozen=True)
class LlamaTiming:
    logger: str
    sampling_ms: float
    prompt_ms: float
    prompt_tokens: int
    eval_ms: float
    eval_runs: int
    total_ms: float
    total_tokens: int | None
    unaccounted_ms: float | None
    graphs_reused: int | None


@dataclass(frozen=True)
class LlamaMetalRuntime:
    prepared_device: str
    found_device: str
    default_device: str
    offloaded_layers: int
    total_layers: int


def canonical_long_output_prompt(repeat: int = 36) -> str:
    """Render the raw long-context prompt shared by the gate and its tests."""

    if isinstance(repeat, bool) or not isinstance(repeat, int) or repeat <= 0:
        raise BenchmarkContractError(f"prompt repeat must be a positive integer: {repeat!r}")
    evidence = _CANONICAL_EVIDENCE_SENTENCE * repeat
    return (
        f"<|turn>user\n{evidence}\n\n"
        "where are my Spella coffee assets?<turn|>\n"
        "<|turn>model\n<|channel>thought\n<channel|>"
    )


def _sanitized_git_environment() -> dict[str, str]:
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(_GIT_ENV_PREFIXES)
    }
    environment["LC_ALL"] = "C"
    return environment


def canonical_git_untracked_paths(repo_root: Path) -> tuple[str, ...]:
    """Return all untracked paths using Git with repository overrides removed."""

    try:
        status = subprocess.check_output(
            (
                "git",
                "-C",
                str(repo_root),
                "status",
                "--porcelain=v1",
                "-z",
                "--untracked-files=all",
            ),
            env=_sanitized_git_environment(),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise BenchmarkContractError(
            f"cannot inspect canonical benchmark worktree {repo_root}: {exc}"
        ) from exc
    return tuple(
        os.fsdecode(record[3:])
        for record in status.split(b"\0")
        if record.startswith(b"?? ")
    )


def canonical_git_submodule_violations(repo_root: Path) -> tuple[str, ...]:
    """Return dirty, unavailable, or commit-drifted recursive submodules."""

    environment = _sanitized_git_environment()
    violations: list[str] = []

    def inspect(repo: Path, prefix: str) -> None:
        try:
            index = subprocess.check_output(
                ("git", "-C", str(repo), "ls-files", "--stage", "-z"),
                env=environment,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise BenchmarkContractError(
                f"cannot enumerate canonical benchmark submodules under {repo}: {exc}"
            ) from exc
        for record in index.split(b"\0"):
            if not record.startswith(b"160000 "):
                continue
            header, separator, raw_path = record.partition(b"\t")
            fields = header.split()
            if not separator or len(fields) != 3:
                violations.append(f"{prefix or '.'}: malformed gitlink index record")
                continue
            expected_commit = fields[1].decode("ascii", errors="replace")
            stage = fields[2]
            relative = os.fsdecode(raw_path)
            display = f"{prefix}/{relative}" if prefix else relative
            if stage != b"0":
                violations.append(f"{display}: unresolved gitlink stage {os.fsdecode(stage)}")
                continue
            try:
                submodule = (repo / relative).resolve(strict=True)
                submodule.relative_to(repo.resolve(strict=True))
                head = subprocess.check_output(
                    ("git", "-C", str(submodule), "rev-parse", "HEAD"),
                    text=True,
                    env=environment,
                ).strip()
                status = subprocess.check_output(
                    (
                        "git",
                        "-C",
                        str(submodule),
                        "status",
                        "--porcelain=v1",
                        "-z",
                        "--untracked-files=all",
                    ),
                    env=environment,
                )
            except (OSError, subprocess.SubprocessError, ValueError) as exc:
                violations.append(f"{display}: unavailable or outside parent ({exc})")
                continue
            if head != expected_commit:
                violations.append(
                    f"{display}: HEAD {head} differs from gitlink {expected_commit}"
                )
            if status:
                violations.append(f"{display}: dirty worktree")
            inspect(submodule, display)

    inspect(repo_root.resolve(strict=True), "")
    return tuple(violations)


def validate_canonical_git_worktree(repo_root: Path) -> None:
    untracked = canonical_git_untracked_paths(repo_root)
    if untracked:
        preview = ", ".join(repr(path) for path in untracked[:3])
        if len(untracked) > 3:
            preview += f", ... ({len(untracked)} total)"
        raise BenchmarkContractError(
            "canonical benchmark requires a worktree with no untracked files; "
            f"found {preview}"
        )
    submodule_violations = canonical_git_submodule_violations(repo_root)
    if submodule_violations:
        raise BenchmarkContractError(
            "canonical benchmark requires clean submodules pinned to their gitlinks; "
            + "; ".join(submodule_violations)
        )


def llama_bundle_manifest(root: Path) -> dict[str, Any]:
    """Return a deterministic manifest for a self-contained llama.cpp bundle."""

    try:
        resolved_root = root.resolve(strict=True)
    except OSError as exc:
        raise BenchmarkContractError(f"cannot resolve llama.cpp bundle root {root}: {exc}") from exc
    if not resolved_root.is_dir():
        raise BenchmarkContractError(f"llama.cpp bundle root is not a directory: {resolved_root}")

    entries: list[dict[str, Any]] = []
    try:
        paths = sorted(resolved_root.rglob("*"), key=lambda path: path.relative_to(resolved_root).as_posix())
    except OSError as exc:
        raise BenchmarkContractError(f"cannot enumerate llama.cpp bundle {resolved_root}: {exc}") from exc
    for path in paths:
        relative = path.relative_to(resolved_root).as_posix()
        try:
            if path.is_symlink():
                target = os.readlink(path)
                resolved_target = path.resolve(strict=True)
                try:
                    resolved_target.relative_to(resolved_root)
                except ValueError as exc:
                    raise BenchmarkContractError(
                        f"llama.cpp bundle symlink escapes root: {relative} -> {target}"
                    ) from exc
                entries.append({"path": relative, "type": "symlink", "target": target})
            elif path.is_dir():
                continue
            elif path.is_file():
                entries.append(
                    {
                        "path": relative,
                        "type": "file",
                        "size": path.stat().st_size,
                        "sha256": _file_sha256(path),
                    }
                )
            else:
                raise BenchmarkContractError(
                    f"unsupported entry in llama.cpp bundle: {relative}"
                )
        except OSError as exc:
            raise BenchmarkContractError(
                f"cannot inspect llama.cpp bundle entry {path}: {exc}"
            ) from exc
    if not entries:
        raise BenchmarkContractError(f"llama.cpp bundle is empty: {resolved_root}")
    return {
        "schema": _LLAMA_BUNDLE_SCHEMA,
        "entries": entries,
    }


def llama_bundle_manifest_bytes(manifest: dict[str, Any]) -> bytes:
    return (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()


def llama_bundle_manifest_sha256(manifest: dict[str, Any]) -> str:
    return hashlib.sha256(llama_bundle_manifest_bytes(manifest)).hexdigest()


def write_llama_bundle_manifest(root: Path, output: Path) -> str:
    manifest = llama_bundle_manifest(root)
    output.write_bytes(llama_bundle_manifest_bytes(manifest))
    return llama_bundle_manifest_sha256(manifest)


def parse_llama_verbose_prompt_token_ids(
    text: str,
    path: Path | str = "<llama-prompt-preflight>",
) -> tuple[str, list[int]]:
    """Parse the exact prompt IDs printed by b10182 --verbose-prompt."""

    count_match = _unique_match(
        text,
        r"(?m)^.*\bnumber of tokens in prompt\s*=\s*(\d+)\s*$",
        "llama.cpp verbose prompt token count",
        path,
    )
    expected_count = int(count_match.group(1))
    # b10182 prints the decoded piece verbatim. A newline token therefore
    # spans two physical log lines ("107 -> '\n'"). Match only the stable
    # record prefix and ID; requiring the closing quote on this line silently
    # drops every multiline piece from the canonical prompt.
    values = [
        int(match.group(1))
        for match in re.finditer(
            r"(?m)^[^\r\n]*?(?<![\w-])(-?\d+)\s+->\s+'",
            text,
        )
    ]
    if any(value < 0 for value in values):
        raise BenchmarkContractError(f"llama.cpp verbose prompt contains a negative token ID: {path}")
    if expected_count <= 0 or len(values) != expected_count:
        raise BenchmarkContractError(
            f"llama.cpp verbose prompt ID count={len(values)}, expected {expected_count}: {path}"
        )
    normalized = " ".join(str(value) for value in values)
    return normalized, values


def audit_llama_loaded_libraries(
    text: str,
    bundle_root: Path,
    comparator_binary: Path,
    path: Path | str = "<llama-prompt-preflight>",
) -> dict[str, Any]:
    """Validate the non-system image closure printed by dyld on preflight."""

    try:
        resolved_bundle = bundle_root.resolve(strict=True)
        resolved_binary = comparator_binary.resolve(strict=True)
    except OSError as exc:
        raise BenchmarkContractError(f"cannot resolve llama.cpp loader audit paths: {exc}") from exc
    try:
        resolved_binary.relative_to(resolved_bundle)
    except ValueError as exc:
        raise BenchmarkContractError(
            f"llama.cpp loader-audit binary is outside its bundle: {resolved_binary}"
        ) from exc

    reported = [
        match.group(1).strip()
        for match in re.finditer(
            r"(?m)^dyld\[\d+\]:\s+(?:<[^>\r\n]+>\s+|loaded:\s+)?(/[^\r\n]+?)\s*$",
            text,
        )
    ]
    if not reported:
        raise BenchmarkContractError(f"llama.cpp dyld loader audit emitted no image paths: {path}")

    bundle_images: set[str] = set()
    system_images: set[str] = set()
    for raw in reported:
        normalized_raw = os.path.normpath(raw)
        if normalized_raw.startswith(_SYSTEM_LIBRARY_PREFIXES):
            system_images.add(normalized_raw)
            continue
        try:
            resolved = Path(normalized_raw).resolve(strict=True)
        except OSError as exc:
            raise BenchmarkContractError(
                f"cannot resolve loaded llama.cpp image {raw!r}: {path}: {exc}"
            ) from exc
        try:
            relative = resolved.relative_to(resolved_bundle).as_posix()
        except ValueError as exc:
            raise BenchmarkContractError(
                f"llama.cpp loaded non-system image outside pinned bundle: {resolved}: {path}"
            ) from exc
        bundle_images.add(relative)

    binary_relative = resolved_binary.relative_to(resolved_bundle).as_posix()
    if binary_relative not in bundle_images:
        raise BenchmarkContractError(
            f"llama.cpp dyld audit did not report comparator executable {binary_relative}: {path}"
        )
    bundle_basenames = {Path(relative).name for relative in bundle_images}
    missing_patterns = [
        label
        for label, pattern in _REQUIRED_LLAMA_BUNDLE_LOAD_PATTERNS
        if not any(re.fullmatch(pattern, name) for name in bundle_basenames)
    ]
    if missing_patterns:
        raise BenchmarkContractError(
            "llama.cpp dyld audit did not report required bundle images: "
            f"{', '.join(missing_patterns)}: {path}"
        )
    normalized = {
        "mode": "dyld_print_libraries_preflight",
        "passed": True,
        "bundle_images": sorted(bundle_images),
        "bundle_image_count": len(bundle_images),
        "system_image_count": len(system_images),
        "system_library_prefixes": list(_SYSTEM_LIBRARY_PREFIXES),
        "required_bundle_load_patterns": [
            {"label": label, "pattern": pattern}
            for label, pattern in _REQUIRED_LLAMA_BUNDLE_LOAD_PATTERNS
        ],
    }
    normalized_bytes = (json.dumps(normalized, sort_keys=True, separators=(",", ":")) + "\n").encode()
    return {
        **normalized,
        "audit_sha256": hashlib.sha256(normalized_bytes).hexdigest(),
    }


def validate_llama_prompt_preflight(
    log_path: Path,
    expected_prompt_token_ids_sha256: str,
    expected_prompt_tokens: int,
    loader_audit_mode: str,
    bundle_root: Path,
    comparator_binary: Path,
) -> dict[str, Any]:
    if re.fullmatch(r"[0-9a-f]{64}", expected_prompt_token_ids_sha256) is None:
        raise BenchmarkContractError("preflight requires a lowercase prompt-token SHA-256 pin")
    if expected_prompt_tokens < 0:
        raise BenchmarkContractError("preflight expected prompt-token count must be non-negative")
    try:
        text = log_path.read_text(errors="replace")
    except OSError as exc:
        raise BenchmarkContractError(f"cannot read llama.cpp prompt preflight {log_path}: {exc}") from exc
    normalized_ids, values = parse_llama_verbose_prompt_token_ids(text, log_path)
    prompt_digest = _digest(normalized_ids)
    if prompt_digest != expected_prompt_token_ids_sha256:
        raise BenchmarkContractError(
            "llama.cpp preflight prompt token digest changed: "
            f"expected {expected_prompt_token_ids_sha256}, got {prompt_digest}: {log_path}"
        )
    if expected_prompt_tokens and len(values) != expected_prompt_tokens:
        raise BenchmarkContractError(
            f"llama.cpp preflight prompt tokens={len(values)}, expected {expected_prompt_tokens}: "
            f"{log_path}"
        )
    if loader_audit_mode == "dyld_print_libraries_preflight":
        loader_audit = audit_llama_loaded_libraries(
            text,
            bundle_root,
            comparator_binary,
            log_path,
        )
    elif loader_audit_mode == "skipped_non_macho_noncanonical":
        loader_audit = {
            "mode": loader_audit_mode,
            "passed": False,
            "reason": "non-Mach-O comparator permitted only for a noncanonical artifact",
        }
    else:
        raise BenchmarkContractError(f"unsupported llama.cpp loader-audit mode {loader_audit_mode!r}")
    return {
        "schema": "antfly.gemma4_metal_long_output.preflight.v1",
        "log_sha256": _file_sha256(log_path),
        "prompt_tokens": len(values),
        "prompt_token_ids_sha256": prompt_digest,
        "expected_prompt_token_ids_sha256": expected_prompt_token_ids_sha256,
        "loader_audit": loader_audit,
    }


def _unique_match(text: str, pattern: str, label: str, path: Path | str) -> re.Match[str]:
    matches = list(re.finditer(pattern, text))
    if len(matches) != 1:
        raise BenchmarkContractError(
            f"expected exactly one {label}; found {len(matches)}: {path}"
        )
    return matches[0]


def _optional_unique_match(
    text: str,
    pattern: str,
    label: str,
    path: Path | str,
) -> re.Match[str] | None:
    matches = list(re.finditer(pattern, text, re.MULTILINE))
    if len(matches) > 1:
        raise BenchmarkContractError(f"found multiple {label} records: {path}")
    return matches[0] if matches else None


def _last_match(text: str, pattern: str, label: str, path: Path | str) -> re.Match[str]:
    """Return the final counter snapshot; Antfly may print it before and after teardown."""

    matches = list(re.finditer(pattern, text, re.MULTILINE))
    if not matches:
        raise BenchmarkContractError(f"missing {label}: {path}")
    return matches[-1]


def _positive_finite(value: float, label: str, path: Path | str) -> float:
    if not math.isfinite(value) or value <= 0:
        raise BenchmarkContractError(f"invalid {label}={value!r}: {path}")
    return value


def parse_llama_timing(text: str, path: Path | str = "<llama log>") -> LlamaTiming:
    """Parse exactly one complete final perf block, including b10182 log prefixes."""

    sampling = _unique_match(
        text,
        _PERF_PREFIX + rf"sampling time\s*=\s*({_FLOAT}) ms",
        "llama sampling timing",
        path,
    )
    prompt = _unique_match(
        text,
        _PERF_PREFIX + rf"prompt eval time\s*=\s*({_FLOAT}) ms\s*/\s*([0-9]+) tokens",
        "llama prompt timing",
        path,
    )
    evaluation = _unique_match(
        text,
        _PERF_PREFIX + rf"eval time\s*=\s*({_FLOAT}) ms\s*/\s*([0-9]+) runs",
        "llama eval timing",
        path,
    )
    total = _unique_match(
        text,
        _PERF_PREFIX + rf"total time\s*=\s*({_FLOAT}) ms(?:\s*/\s*([0-9]+) tokens)?",
        "llama total timing",
        path,
    )
    if not (sampling.start() < prompt.start() < evaluation.start() < total.start()):
        raise BenchmarkContractError(f"llama timing records are not in final-block order: {path}")

    loggers = {match.group("logger") for match in (sampling, prompt, evaluation, total)}
    if len(loggers) != 1:
        raise BenchmarkContractError(f"llama timing block mixes logger formats: {path}")

    unaccounted = _optional_unique_match(
        text,
        _PERF_PREFIX + rf"unaccounted time\s*=\s*({_FLOAT}) ms",
        "llama unaccounted timing",
        path,
    )
    graphs = _optional_unique_match(
        text,
        _PERF_PREFIX + r"graphs reused\s*=\s*([0-9]+)",
        "llama graph reuse",
        path,
    )
    result = LlamaTiming(
        logger=next(iter(loggers)),
        sampling_ms=_positive_finite(float(sampling.group(2)), "llama sampling time", path),
        prompt_ms=_positive_finite(float(prompt.group(2)), "llama prompt time", path),
        prompt_tokens=int(prompt.group(3)),
        eval_ms=_positive_finite(float(evaluation.group(2)), "llama eval time", path),
        eval_runs=int(evaluation.group(3)),
        total_ms=_positive_finite(float(total.group(2)), "llama total time", path),
        total_tokens=int(total.group(3)) if total.group(3) else None,
        unaccounted_ms=float(unaccounted.group(2)) if unaccounted else None,
        graphs_reused=int(graphs.group(2)) if graphs else None,
    )
    if result.prompt_tokens <= 0 or result.eval_runs <= 0:
        raise BenchmarkContractError(f"llama timing block has non-positive token accounting: {path}")
    if result.unaccounted_ms is not None:
        accounted = result.sampling_ms + result.prompt_ms + result.eval_ms + result.unaccounted_ms
        tolerance_ms = max(1.0, result.total_ms * 0.001)
        if abs(accounted - result.total_ms) > tolerance_ms:
            raise BenchmarkContractError(
                f"llama timing block does not reconcile: accounted={accounted:.3f}ms "
                f"total={result.total_ms:.3f}ms tolerance={tolerance_ms:.3f}ms: {path}"
            )
    return result


def parse_llama_metal_runtime(
    text: str,
    path: Path | str = "<llama log>",
) -> LlamaMetalRuntime:
    """Prove b10182 selected Metal and offloaded the complete model."""

    prepared = _unique_match(
        text,
        r"(?m)^[^\r\n]*\bllama_prepare_model_devices:[^\r\n]*\bMTL[0-9]+\s+\(([^)\r\n]+)\)",
        "llama Metal prepared-device marker",
        path,
    )
    offload = _unique_match(
        text,
        r"(?m)^[^\r\n]*\bload_tensors:\s*offloaded\s+([0-9]+)/([0-9]+)\s+layers to GPU(?:\s|$)",
        "llama GPU layer-offload marker",
        path,
    )
    found = _unique_match(
        text,
        r"(?m)^[^\r\n]*\bggml_metal_init:\s*found device:\s*([^\r\n]+?)\s*$",
        "llama Metal found-device marker",
        path,
    )
    default = _unique_match(
        text,
        r"(?m)^[^\r\n]*\bggml_metal_init:\s*picking default device:\s*([^\r\n]+?)\s*$",
        "llama Metal default-device marker",
        path,
    )
    result = LlamaMetalRuntime(
        prepared_device=prepared.group(1).strip(),
        found_device=found.group(1).strip(),
        default_device=default.group(1).strip(),
        offloaded_layers=int(offload.group(1)),
        total_layers=int(offload.group(2)),
    )
    devices = {result.prepared_device, result.found_device, result.default_device}
    if len(devices) != 1:
        raise BenchmarkContractError(
            "llama Metal device markers disagree: "
            f"prepared={result.prepared_device!r}, found={result.found_device!r}, "
            f"default={result.default_device!r}: {path}"
        )
    if result.offloaded_layers <= 0 or result.total_layers <= 0:
        raise BenchmarkContractError(
            f"llama GPU layer offload is non-positive: "
            f"{result.offloaded_layers}/{result.total_layers}: {path}"
        )
    return result


def stats(values: Iterable[float]) -> dict[str, float]:
    collected = [float(value) for value in values]
    if not collected or any(not math.isfinite(value) or value <= 0 for value in collected):
        raise BenchmarkContractError(f"statistics require non-empty positive finite values: {collected!r}")
    mean = statistics.fmean(collected)
    return {
        "min": min(collected),
        "median": statistics.median(collected),
        "mean": mean,
        "max": max(collected),
        "cv": statistics.pstdev(collected) / mean if mean else 0.0,
    }


def ratio_stats(rows: list[dict[str, Any]], field: str) -> dict[str, float]:
    """Summarize ratios calculated within each paired sample."""

    return stats(float(row[field]) for row in rows)


def _percentile(sorted_values: list[float], quantile: float) -> float:
    if not sorted_values or not 0.0 <= quantile <= 1.0:
        raise BenchmarkContractError(
            f"percentile requires values and a quantile in [0, 1]: {quantile!r}"
        )
    position = quantile * (len(sorted_values) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[lower]
    fraction = position - lower
    return sorted_values[lower] + fraction * (sorted_values[upper] - sorted_values[lower])


def paired_bootstrap_interval(
    rows: list[dict[str, Any]],
    field: str,
    *,
    confidence: float = _BOOTSTRAP_CONFIDENCE,
    samples: int = _BOOTSTRAP_SAMPLES,
) -> dict[str, float | int]:
    """Return a reproducible percentile CI for the median of paired ratios."""

    ratios = [float(row[field]) for row in rows]
    if not ratios or any(not math.isfinite(value) or value <= 0 for value in ratios):
        raise BenchmarkContractError(
            f"paired bootstrap requires positive finite {field} ratios: {ratios!r}"
        )
    if samples <= 0 or not 0.0 < confidence < 1.0:
        raise BenchmarkContractError(
            f"invalid paired bootstrap shape: samples={samples}, confidence={confidence}"
        )
    seed_material = json.dumps(
        {
            "confidence": confidence,
            "field": field,
            "ratios": [format(value, ".17g") for value in ratios],
            "samples": samples,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    seed = int.from_bytes(hashlib.sha256(seed_material).digest()[:16], "big")
    generator = random.Random(seed)
    count = len(ratios)
    estimates = [
        statistics.median(ratios[generator.randrange(count)] for _ in range(count))
        for _ in range(samples)
    ]
    estimates.sort()
    alpha = (1.0 - confidence) / 2.0
    return {
        "confidence": confidence,
        "samples": samples,
        "median": statistics.median(ratios),
        "lower": _percentile(estimates, alpha),
        "upper": _percentile(estimates, 1.0 - alpha),
    }


def one_sided_sign_test_p(wins: int, samples: int) -> float:
    """Exact binomial tail under an equal-probability paired win/loss null."""

    if isinstance(wins, bool) or isinstance(samples, bool) or samples <= 0 or not 0 <= wins <= samples:
        raise BenchmarkContractError(
            f"invalid paired sign-test shape: wins={wins!r}, samples={samples!r}"
        )
    return sum(math.comb(samples, count) for count in range(wins, samples + 1)) / (2**samples)


def _mapping(value: Any, label: str, path: Path) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BenchmarkContractError(f"missing structured {label}: {path}")
    return value


def _exact_int(mapping: dict[str, Any], key: str, expected: int, label: str, path: Path) -> int:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise BenchmarkContractError(f"missing integer {label}.{key}: {path}")
    if value != expected:
        raise BenchmarkContractError(f"{label}.{key}={value}, expected {expected}: {path}")
    return value


def _token_line(text: str, name: str, path: Path) -> tuple[str, list[int]]:
    match = _unique_match(text, rf"(?m)^{re.escape(name)}:\s*(.*)$", f"Antfly {name}", path)
    raw = match.group(1).strip()
    try:
        values = [int(part) for part in raw.split()]
    except ValueError as exc:
        raise BenchmarkContractError(f"invalid integer in Antfly {name}: {path}") from exc
    if not values or any(value < 0 for value in values):
        raise BenchmarkContractError(f"invalid Antfly {name}: {path}")
    normalized = " ".join(str(value) for value in values)
    return normalized, values


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_metadata(root: Path, runs: int, requested_tokens: int) -> dict[str, Any]:
    if isinstance(runs, bool) or runs <= 0 or runs % 2 != 0:
        raise BenchmarkContractError(
            f"benchmark runs must be a positive even integer for balanced ordering: {runs!r}"
        )
    metadata_path = root / "metadata.json"
    metadata = _mapping(json.loads(metadata_path.read_text()), "benchmark metadata", metadata_path)
    if metadata.get("schema") != "antfly.gemma4_metal_long_output.metadata.v4":
        raise BenchmarkContractError(f"unsupported benchmark metadata schema: {metadata_path}")
    if metadata.get("runs") != runs or metadata.get("output_tokens") != requested_tokens:
        raise BenchmarkContractError(f"benchmark metadata does not match requested run shape: {metadata_path}")
    warmups = metadata.get("warmups")
    if isinstance(warmups, bool) or not isinstance(warmups, int) or warmups < 0:
        raise BenchmarkContractError(f"missing non-negative benchmark warmup count: {metadata_path}")
    cooldown_seconds = metadata.get("cooldown_seconds")
    if (
        isinstance(cooldown_seconds, bool)
        or not isinstance(cooldown_seconds, int)
        or cooldown_seconds < 0
    ):
        raise BenchmarkContractError(f"missing non-negative benchmark cooldown: {metadata_path}")
    for key in ("max_total_ratio", "min_decode_ratio", "max_cv"):
        value = metadata.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise BenchmarkContractError(f"missing finite benchmark threshold {key}: {metadata_path}")
    for key in (
        "gguf_sha256",
        "prompt_sha256",
        "llama_cpp_binary_sha256",
        "llama_cpp_bundle_sha256",
        "antfly_binary_sha256",
        "git_tracked_diff_sha256",
        "git_status_sha256",
        "git_tracked_diff_sha256_end",
        "git_status_sha256_end",
        "benchmark_harness_sha256",
        "benchmark_parser_sha256",
        "expected_token_ids_sha256",
        "expected_prompt_token_ids_sha256",
    ):
        value = metadata.get(key)
        if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise BenchmarkContractError(f"missing immutable metadata hash {key}: {metadata_path}")
    if not isinstance(metadata.get("git_dirty"), bool):
        raise BenchmarkContractError(f"missing boolean source provenance git_dirty: {metadata_path}")
    for key in ("repo_root", "git_revision", "git_revision_end"):
        if not isinstance(metadata.get(key), str) or not metadata[key].strip():
            raise BenchmarkContractError(f"missing source provenance {key}: {metadata_path}")
    if (
        metadata["git_revision_end"] != metadata["git_revision"]
        or metadata["git_tracked_diff_sha256_end"] != metadata["git_tracked_diff_sha256"]
        or metadata["git_status_sha256_end"] != metadata["git_status_sha256"]
    ):
        raise BenchmarkContractError(f"Git state changed during benchmark: {metadata_path}")
    repo_root = Path(metadata["repo_root"])
    try:
        git_environment = _sanitized_git_environment()
        resolved_repo_root = repo_root.resolve(strict=True)
        current_toplevel = Path(
            subprocess.check_output(
                ["git", "-C", str(resolved_repo_root), "rev-parse", "--show-toplevel"],
                text=True,
                env=git_environment,
            ).strip()
        ).resolve(strict=True)
        if current_toplevel != resolved_repo_root:
            raise BenchmarkContractError(
                "physical Git top-level differs from recorded benchmark repository: "
                f"recorded={resolved_repo_root}, git={current_toplevel}"
            )
        current_revision = subprocess.check_output(
            ["git", "-C", str(resolved_repo_root), "rev-parse", "HEAD"],
            text=True,
            env=git_environment,
        ).strip()
        current_tracked_diff_sha256 = hashlib.sha256(
            subprocess.check_output(
                ["git", "-C", str(resolved_repo_root), "diff", "--binary", "--no-ext-diff", "HEAD", "--"],
                env=git_environment,
            )
        ).hexdigest()
        current_status_sha256 = hashlib.sha256(
            subprocess.check_output(
                ["git", "-C", str(resolved_repo_root), "status", "--porcelain=v1", "--untracked-files=all"],
                env=git_environment,
            )
        ).hexdigest()
    except (OSError, subprocess.SubprocessError) as exc:
        raise BenchmarkContractError(f"cannot revalidate benchmark Git state: {repo_root}: {exc}") from exc
    if (
        current_revision != metadata["git_revision"]
        or current_tracked_diff_sha256 != metadata["git_tracked_diff_sha256"]
        or current_status_sha256 != metadata["git_status_sha256"]
    ):
        raise BenchmarkContractError(
            f"current Git state differs from benchmark start/end provenance: {metadata_path}"
        )
    if metadata.get("prompt_file") != "prompt.txt":
        raise BenchmarkContractError(f"missing canonical prompt artifact: {metadata_path}")
    source_paths = {
        "benchmark_harness_sha256": Path(__file__).resolve().with_name(
            "benchmark_metal_gemma4_long_output.sh"
        ),
        "benchmark_parser_sha256": Path(__file__).resolve(),
    }
    for key, source_path in source_paths.items():
        current_hash = _file_sha256(source_path)
        if metadata[key] != current_hash:
            raise BenchmarkContractError(
                f"benchmark source hash mismatch for {key}: "
                f"recorded={metadata[key]}, current={current_hash}: {metadata_path}"
            )
    artifact_paths = {
        "gguf_sha256": metadata.get("gguf"),
        "prompt_sha256": str(root / metadata["prompt_file"]),
        "antfly_binary_sha256": metadata.get("antfly_bin"),
        "llama_cpp_binary_sha256": metadata.get("llama_cpp_resolved_bin"),
    }
    for key, raw_path in artifact_paths.items():
        if not isinstance(raw_path, str) or not raw_path.strip():
            raise BenchmarkContractError(f"missing artifact path for {key}: {metadata_path}")
        artifact_path = Path(raw_path)
        try:
            current_hash = _file_sha256(artifact_path)
        except OSError as exc:
            raise BenchmarkContractError(
                f"cannot revalidate benchmark artifact for {key}: {artifact_path}: {exc}"
            ) from exc
        if metadata[key] != current_hash:
            raise BenchmarkContractError(
                f"benchmark artifact hash mismatch for {key}: "
                f"recorded={metadata[key]}, current={current_hash}: {artifact_path}"
            )
    for key in ("llama_cpp_resolved_bin", "llama_cpp_version_output", "llama_cpp_comparator_id"):
        if not isinstance(metadata.get(key), str) or not metadata[key].strip():
            raise BenchmarkContractError(f"missing comparator provenance {key}: {metadata_path}")
    gguf_path = Path(str(metadata.get("gguf", ""))).resolve()
    for key in ("antfly_model_argument", "llama_model_argument"):
        raw_model_argument = metadata.get(key)
        if not isinstance(raw_model_argument, str) or not raw_model_argument.strip():
            raise BenchmarkContractError(f"missing model-artifact binding {key}: {metadata_path}")
        if Path(raw_model_argument).resolve() != gguf_path:
            raise BenchmarkContractError(
                f"{key} does not match the hashed GGUF artifact: {metadata_path}"
            )
    for key in ("zig_bin", "zig_resolved_bin", "zig_version"):
        if not isinstance(metadata.get(key), str) or not metadata[key].strip():
            raise BenchmarkContractError(f"missing Zig toolchain provenance {key}: {metadata_path}")
    if metadata.get("execution_order_file") != "execution-order.jsonl":
        raise BenchmarkContractError(f"missing canonical execution-order artifact: {metadata_path}")
    if metadata.get("llama_prompt_preflight_file") != "llama-prompt-preflight.log":
        raise BenchmarkContractError(f"missing llama.cpp prompt preflight artifact: {metadata_path}")
    if metadata.get("llama_prompt_preflight_validation_file") != "llama-prompt-preflight-validation.json":
        raise BenchmarkContractError(
            f"missing llama.cpp prompt preflight validation artifact: {metadata_path}"
        )
    if metadata.get("llama_cpp_bundle_manifest_file") != "llama-cpp-bundle-manifest.json":
        raise BenchmarkContractError(f"missing llama.cpp bundle manifest artifact: {metadata_path}")
    bundle_root_raw = metadata.get("llama_cpp_bundle_root")
    if not isinstance(bundle_root_raw, str) or not bundle_root_raw.strip():
        raise BenchmarkContractError(f"missing llama.cpp bundle root: {metadata_path}")
    bundle_root = Path(bundle_root_raw).resolve()
    try:
        Path(metadata["llama_cpp_resolved_bin"]).resolve().relative_to(bundle_root)
    except (KeyError, TypeError, ValueError) as exc:
        raise BenchmarkContractError(
            f"llama.cpp comparator is outside its pinned bundle root: {metadata_path}"
        ) from exc
    manifest_path = root / metadata["llama_cpp_bundle_manifest_file"]
    try:
        recorded_manifest = _mapping(
            json.loads(manifest_path.read_text()), "llama.cpp bundle manifest", manifest_path
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise BenchmarkContractError(f"invalid llama.cpp bundle manifest {manifest_path}: {exc}") from exc
    current_manifest = llama_bundle_manifest(bundle_root)
    if recorded_manifest != current_manifest:
        raise BenchmarkContractError(f"llama.cpp bundle changed after benchmark: {bundle_root}")
    current_bundle_sha256 = llama_bundle_manifest_sha256(current_manifest)
    if current_bundle_sha256 != metadata["llama_cpp_bundle_sha256"]:
        raise BenchmarkContractError(f"llama.cpp bundle manifest digest mismatch: {metadata_path}")
    expected_bundle_sha256 = metadata.get("llama_cpp_expected_bundle_sha256")
    if (
        not isinstance(expected_bundle_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", expected_bundle_sha256) is None
        or expected_bundle_sha256 != current_bundle_sha256
    ):
        raise BenchmarkContractError(f"llama.cpp bundle does not match its approved pin: {metadata_path}")
    if not isinstance(metadata.get("require_confidence"), bool):
        raise BenchmarkContractError(f"missing confidence-gate provenance: {metadata_path}")
    if not isinstance(metadata.get("allow_noncanonical_policy"), bool) or not isinstance(
        metadata.get("canonical_policy"), bool
    ):
        raise BenchmarkContractError(f"missing canonical-policy provenance: {metadata_path}")
    if metadata["canonical_policy"] == metadata["allow_noncanonical_policy"]:
        raise BenchmarkContractError(f"inconsistent canonical-policy provenance: {metadata_path}")
    if metadata.get("git_environment_prefixes") != list(_GIT_ENV_PREFIXES):
        raise BenchmarkContractError(f"Git-environment isolation contract changed: {metadata_path}")
    process_git_env = metadata.get("process_git_env")
    if not isinstance(process_git_env, dict) or any(
        not isinstance(name, str)
        or not name.startswith(_GIT_ENV_PREFIXES)
        or not isinstance(value, str)
        for name, value in process_git_env.items()
    ):
        raise BenchmarkContractError(f"invalid Git-environment provenance: {metadata_path}")
    if metadata.get("canonical_untracked_policy") != "reject":
        raise BenchmarkContractError(f"untracked-worktree contract changed: {metadata_path}")
    if metadata.get("canonical_submodule_policy") != "clean_and_pinned":
        raise BenchmarkContractError(f"submodule-worktree contract changed: {metadata_path}")
    if metadata["canonical_policy"]:
        if process_git_env:
            raise BenchmarkContractError(
                f"canonical benchmark inherited a Git override: {metadata_path}"
            )
        validate_canonical_git_worktree(repo_root)
    if not isinstance(metadata.get("prompt_override_set"), bool):
        raise BenchmarkContractError(f"missing prompt-override provenance: {metadata_path}")
    if metadata["canonical_policy"]:
        canonical_violations: list[str] = []
        if metadata.get("prompt_override_set") is not False:
            canonical_violations.append("prompt_override_set must be false")
        if metadata.get("prompt_repeat") != 36:
            canonical_violations.append("prompt_repeat must equal 36")
        if requested_tokens != 300:
            canonical_violations.append("output_tokens must equal 300")
        if runs < _CONFIDENCE_MIN_RUNS or runs % 2:
            canonical_violations.append(
                f"runs must be even and at least {_CONFIDENCE_MIN_RUNS}"
            )
        if warmups < 1:
            canonical_violations.append("warmups must be at least 1")
        if cooldown_seconds < 45:
            canonical_violations.append("cooldown_seconds must be at least 45")
        if metadata["max_total_ratio"] > 0.98:
            canonical_violations.append("max_total_ratio must be at most 0.98")
        if metadata["min_decode_ratio"] < 1.02:
            canonical_violations.append("min_decode_ratio must be at least 1.02")
        if metadata["max_cv"] <= 0.0 or metadata["max_cv"] > 0.03:
            canonical_violations.append("max_cv must be in (0, 0.03]")
        if metadata.get("require_confidence") is not True:
            canonical_violations.append("require_confidence must be enabled")
        if tuple(
            str(metadata.get(key, "")).lower()
            for key in ("antfly_cache_dtype", "llama_cache_type_k", "llama_cache_type_v")
        ) != ("f16", "f16", "f16"):
            canonical_violations.append("all KV cache types must be f16")
        if metadata.get("llama_context_size") != 4096:
            canonical_violations.append("llama_context_size must equal 4096")
        if metadata.get("expected_prompt_token_ids_sha256") != _CANONICAL_PROMPT_TOKEN_IDS_SHA256:
            canonical_violations.append("expected prompt token digest is not the approved canonical digest")
        if metadata.get("expected_token_ids_sha256") != _CANONICAL_OUTPUT_TOKEN_IDS_SHA256:
            canonical_violations.append("expected output token digest is not the approved canonical digest")
        if metadata.get("prompt_sha256") != _CANONICAL_PROMPT_SHA256:
            canonical_violations.append("prompt_sha256 is not the approved canonical byte digest")
        if metadata.get("llama_cpp_build") != _CANONICAL_LLAMA_CPP_BUILD:
            canonical_violations.append(
                f"llama_cpp_build must equal approved b{_CANONICAL_LLAMA_CPP_BUILD}"
            )
        if metadata.get("llama_cpp_expected_build") != _CANONICAL_LLAMA_CPP_BUILD:
            canonical_violations.append(
                f"llama_cpp_expected_build must equal approved b{_CANONICAL_LLAMA_CPP_BUILD}"
            )
        if metadata.get("llama_cpp_binary_sha256") != _CANONICAL_LLAMA_CPP_SHA256:
            canonical_violations.append("llama_cpp_binary_sha256 is not the approved comparator")
        if metadata.get("llama_cpp_expected_sha256") != _CANONICAL_LLAMA_CPP_SHA256:
            canonical_violations.append("llama_cpp_expected_sha256 is not the approved comparator")
        if metadata.get("llama_cpp_bundle_sha256") != _CANONICAL_LLAMA_CPP_BUNDLE_SHA256:
            canonical_violations.append("llama_cpp_bundle_sha256 is not the approved comparator bundle")
        if (
            metadata.get("llama_cpp_expected_bundle_sha256")
            != _CANONICAL_LLAMA_CPP_BUNDLE_SHA256
        ):
            canonical_violations.append(
                "llama_cpp_expected_bundle_sha256 is not the approved comparator bundle"
            )
        policy_environment = metadata.get("metal_policy_env")
        if not isinstance(policy_environment, dict):
            canonical_violations.append("metal_policy_env must be a mapping")
        else:
            for name, expected in _CANONICAL_ROUTE_ENV.items():
                if policy_environment.get(name) != expected:
                    canonical_violations.append(
                        f"{name} must equal approved value {expected!r}"
                    )
        if canonical_violations:
            raise BenchmarkContractError(
                "canonical benchmark shape/thermal/gate contract violated: "
                f"{'; '.join(canonical_violations)}: {metadata_path}"
            )
    if metadata.get("policy_environment_prefixes") != list(_POLICY_ENV_PREFIXES):
        raise BenchmarkContractError(f"policy-environment isolation contract changed: {metadata_path}")
    process_policy_env = metadata.get("process_policy_env")
    if not isinstance(process_policy_env, dict) or any(
        not isinstance(name, str)
        or not name.startswith(_POLICY_ENV_PREFIXES)
        or not isinstance(value, str)
        for name, value in process_policy_env.items()
    ):
        raise BenchmarkContractError(f"invalid process policy environment provenance: {metadata_path}")
    runner_policy_env = {"TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE": "1"}
    if metadata["canonical_policy"] and process_policy_env != runner_policy_env:
        raise BenchmarkContractError(
            f"canonical benchmark inherited non-runner policy environment: {metadata_path}"
        )
    loader_audit_mode = metadata.get("llama_cpp_loader_audit_mode")
    binary_file_type = metadata.get("llama_cpp_binary_file_type")
    if not isinstance(binary_file_type, str) or not binary_file_type.strip():
        raise BenchmarkContractError(f"missing llama.cpp binary file type: {metadata_path}")
    expected_loader_audit_mode = (
        "dyld_print_libraries_preflight"
        if binary_file_type.startswith("Mach-O")
        else "skipped_non_macho_noncanonical"
    )
    if loader_audit_mode != expected_loader_audit_mode:
        raise BenchmarkContractError(f"invalid llama.cpp loader-audit mode: {metadata_path}")
    if metadata["canonical_policy"] and expected_loader_audit_mode != "dyld_print_libraries_preflight":
        raise BenchmarkContractError(
            f"canonical llama.cpp comparator is not Mach-O: {metadata_path}"
        )
    if metadata.get("loader_environment_prefixes") != list(_LOADER_ENV_PREFIXES) or metadata.get(
        "loader_environment_names"
    ) != list(_LOADER_ENV_NAMES):
        raise BenchmarkContractError(f"loader-environment isolation contract changed: {metadata_path}")
    process_loader_env = metadata.get("process_loader_env")
    if not isinstance(process_loader_env, dict) or any(
        not isinstance(name, str)
        or not (name.startswith(_LOADER_ENV_PREFIXES) or name in _LOADER_ENV_NAMES)
        or not isinstance(value, str)
        for name, value in process_loader_env.items()
    ):
        raise BenchmarkContractError(f"invalid loader-environment provenance: {metadata_path}")
    if metadata["canonical_policy"] and process_loader_env:
        raise BenchmarkContractError(
            f"canonical benchmark inherited a dynamic-loader override: {metadata_path}"
        )
    expected_runner_env = {
        "TERMITE_GEN_STAGE_DEBUG": "1",
        "TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE": "1",
    }
    if loader_audit_mode == "dyld_print_libraries_preflight":
        expected_runner_env["DYLD_PRINT_LIBRARIES"] = (
            "1 for unmeasured llama prompt preflight only"
        )
    if metadata.get("runner_injected_env") != expected_runner_env:
        raise BenchmarkContractError(f"runner-injected environment contract changed: {metadata_path}")
    expected_sha = metadata.get("llama_cpp_expected_sha256")
    if not isinstance(expected_sha, str) or re.fullmatch(r"[0-9a-f]{64}", expected_sha) is None:
        raise BenchmarkContractError(f"missing pinned llama.cpp comparator hash: {metadata_path}")
    if expected_sha != metadata["llama_cpp_binary_sha256"]:
        raise BenchmarkContractError(f"llama.cpp comparator hash does not match pinned hash: {metadata_path}")
    policy_env = metadata.get("metal_policy_env")
    if not isinstance(policy_env, dict):
        raise BenchmarkContractError(f"missing Metal policy environment provenance: {metadata_path}")
    for name in _POLICY_ENV_NAMES:
        if name not in policy_env or (
            policy_env[name] is not None and not isinstance(policy_env[name], str)
        ):
            raise BenchmarkContractError(f"missing Metal policy environment value {name}: {metadata_path}")
    for name in (
        "TERMITE_METAL_DECODE_GQA_SPLIT_SWA_VARIANT",
        "TERMITE_METAL_DECODE_GQA_SPLIT_GLOBAL_VARIANT",
    ):
        raw = policy_env[name]
        normalized = raw.strip().lower() if raw else "auto"
        if normalized != "auto":
            raise BenchmarkContractError(
                f"pinned baseline requires {name}=auto or unset, got {raw!r}: {metadata_path}"
            )
    if policy_env["TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE"] != "1":
        raise BenchmarkContractError(
            f"pinned baseline requires runner-owned split-GQA schedule tracing: {metadata_path}"
        )
    if metadata["canonical_policy"] and not _expect_split_gqa(metadata):
        raise BenchmarkContractError(
            f"canonical pinned baseline requires the default split-GQA route: {metadata_path}"
        )
    return metadata


def _expected_execution_order(runs: int, warmups: int) -> list[dict[str, Any]]:
    expected: list[dict[str, Any]] = []

    def append(phase: str, sample: int, implementation: str) -> None:
        expected.append(
            {
                "sequence": len(expected) + 1,
                "phase": phase,
                "sample": sample,
                "implementation": implementation,
            }
        )

    for sample in range(1, warmups + 1):
        append("warmup", sample, "antfly")
        append("warmup", sample, "llama")
    for sample in range(1, runs + 1):
        first, second = ("antfly", "llama") if sample % 2 == 1 else ("llama", "antfly")
        append("measured", sample, first)
        append("measured", sample, second)
    return expected


def _load_execution_order(
    root: Path,
    metadata: dict[str, Any],
    runs: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    order_path = root / metadata["execution_order_file"]
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(order_path.read_text().splitlines(), start=1):
        if not line.strip():
            raise BenchmarkContractError(
                f"blank execution-order record at line {line_number}: {order_path}"
            )
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise BenchmarkContractError(
                f"invalid execution-order JSON at line {line_number}: {order_path}: {exc}"
            ) from exc
        record = _mapping(value, "execution-order record", order_path)
        if set(record) != {"sequence", "phase", "sample", "implementation"}:
            raise BenchmarkContractError(
                f"unexpected execution-order fields at line {line_number}: {order_path}"
            )
        if (
            isinstance(record.get("sequence"), bool)
            or not isinstance(record.get("sequence"), int)
            or isinstance(record.get("sample"), bool)
            or not isinstance(record.get("sample"), int)
            or not isinstance(record.get("phase"), str)
            or not isinstance(record.get("implementation"), str)
        ):
            raise BenchmarkContractError(
                f"invalid execution-order types at line {line_number}: {order_path}"
            )
        records.append(record)

    warmups = int(metadata["warmups"])
    expected = _expected_execution_order(runs, warmups)
    if records != expected:
        mismatch = next(
            (
                index
                for index, (actual, planned) in enumerate(zip(records, expected), start=1)
                if actual != planned
            ),
            min(len(records), len(expected)) + 1,
        )
        actual = records[mismatch - 1] if mismatch <= len(records) else None
        planned = expected[mismatch - 1] if mismatch <= len(expected) else None
        raise BenchmarkContractError(
            "execution order does not match the complete balanced schedule at "
            f"sequence {mismatch}: actual={actual!r}, expected={planned!r}: {order_path}"
        )

    measured_first = [
        record["implementation"]
        for record in records
        if record["phase"] == "measured" and record["sequence"] % 2 == 1
    ]
    first_counts = {
        implementation: measured_first.count(implementation)
        for implementation in ("antfly", "llama")
    }
    if first_counts != {"antfly": runs // 2, "llama": runs // 2}:
        raise BenchmarkContractError(
            f"measured execution order is not balanced: {first_counts}: {order_path}"
        )
    return records, {
        "passed": True,
        "records": len(records),
        "warmup_pairs": warmups,
        "measured_pairs": runs,
        "measured_first_counts": first_counts,
    }


def _metadata_env_bool(metadata: dict[str, Any], name: str) -> bool | None:
    metadata_path = Path(str(metadata.get("_path", "metadata.json")))
    policy_env = _mapping(metadata.get("metal_policy_env"), "Metal policy environment", metadata_path)
    raw = policy_env.get(name)
    if raw is None or not raw.strip():
        return None
    normalized = raw.strip().lower()
    if normalized in _TRUE_ENV_VALUES:
        return True
    if normalized in _FALSE_ENV_VALUES:
        return False
    raise BenchmarkContractError(f"invalid boolean Metal policy environment {name}={raw!r}: {metadata_path}")


def _required_metadata_env(metadata: dict[str, Any], name: str) -> str:
    metadata_path = Path(str(metadata.get("_path", "metadata.json")))
    policy_env = _mapping(metadata.get("metal_policy_env"), "Metal policy environment", metadata_path)
    raw = policy_env.get(name)
    if not isinstance(raw, str) or not raw.strip():
        raise BenchmarkContractError(
            f"missing required benchmark expectation {name}: {metadata_path}"
        )
    return raw.strip()


def _required_metadata_env_bool(metadata: dict[str, Any], name: str) -> bool:
    value = _metadata_env_bool(metadata, name)
    if value is None:
        metadata_path = Path(str(metadata.get("_path", "metadata.json")))
        raise BenchmarkContractError(
            f"missing required boolean benchmark expectation {name}: {metadata_path}"
        )
    return value


def _required_metadata_env_int(
    metadata: dict[str, Any],
    name: str,
    *,
    positive: bool = False,
) -> int:
    raw = _required_metadata_env(metadata, name)
    if re.fullmatch(r"[0-9]+", raw) is None:
        raise BenchmarkContractError(f"invalid integer benchmark expectation {name}={raw!r}")
    value = int(raw)
    if positive and value <= 0:
        raise BenchmarkContractError(f"benchmark expectation {name} must be positive, got {value}")
    return value


def _expected_q4_mmv_variant(metadata: dict[str, Any]) -> str:
    variant = _required_metadata_env(metadata, "EXPECT_Q4_0_MMV_VARIANT").lower()
    valid_variants = {"any", "nr4-nsg2", "nr8-nsg2", "nr4-nsg4", "nr8-nsg4"}
    if variant not in valid_variants:
        raise BenchmarkContractError(
            f"invalid Q4_0 MMV benchmark expectation: {variant!r}; "
            f"expected one of {sorted(valid_variants)}"
        )
    return variant


def _top_level_env_bool(metadata: dict[str, Any], key: str) -> bool | None:
    raw = metadata.get(key)
    if raw is None or not str(raw).strip():
        return None
    normalized = str(raw).strip().lower()
    if normalized in _TRUE_ENV_VALUES:
        return True
    if normalized in _FALSE_ENV_VALUES:
        return False
    raise BenchmarkContractError(f"invalid benchmark metadata boolean {key}={raw!r}")


def _expect_split_gqa(metadata: dict[str, Any]) -> bool:
    explicit = _top_level_env_bool(metadata, "split_gqa_enable")
    disabled = _top_level_env_bool(metadata, "split_gqa_disable")
    return (explicit is None or explicit) and disabled is not True


def _expect_q4_mmv_portfolio(metadata: dict[str, Any]) -> bool:
    disabled = _metadata_env_bool(metadata, "TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO")
    raw_variant = metadata["metal_policy_env"]["TERMITE_METAL_Q4_0_MMV_VARIANT"]
    variant = raw_variant.strip().lower() if raw_variant else "auto"
    valid_variants = {"auto", "legacy", "nr4-nsg2", "nr8-nsg2", "nr4-nsg4", "nr8-nsg4"}
    if variant not in valid_variants:
        raise BenchmarkContractError(f"invalid Q4_0 MMV variant in benchmark metadata: {raw_variant!r}")
    return disabled is not True and variant != "legacy"


def collect_rows(
    root: Path,
    runs: int,
    requested_tokens: int,
    metadata: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], str, str, str, dict[str, Any]]:
    metadata = metadata or _load_metadata(root, runs, requested_tokens)
    rows: list[dict[str, Any]] = []
    reference_ids: str | None = None
    reference_prompt_ids: str | None = None
    preflight_path = root / metadata["llama_prompt_preflight_file"]
    preflight_text = preflight_path.read_text(errors="replace")
    llama_prompt_ids, llama_prompt_id_values = parse_llama_verbose_prompt_token_ids(
        preflight_text,
        preflight_path,
    )
    recomputed_preflight = validate_llama_prompt_preflight(
        preflight_path,
        metadata["expected_prompt_token_ids_sha256"],
        2003 if metadata["canonical_policy"] else 0,
        metadata["llama_cpp_loader_audit_mode"],
        Path(metadata["llama_cpp_bundle_root"]),
        Path(metadata["llama_cpp_resolved_bin"]),
    )
    preflight_validation_path = root / metadata["llama_prompt_preflight_validation_file"]
    try:
        recorded_preflight = json.loads(preflight_validation_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise BenchmarkContractError(
            f"invalid llama.cpp prompt preflight validation {preflight_validation_path}: {exc}"
        ) from exc
    if recorded_preflight != recomputed_preflight:
        raise BenchmarkContractError(
            f"llama.cpp prompt preflight validation artifact changed: {preflight_validation_path}"
        )
    loader_audit = recomputed_preflight["loader_audit"]
    expect_split_gqa = _expect_split_gqa(metadata)
    expect_generated_flash_prefill_calls = _required_metadata_env_int(
        metadata,
        "EXPECT_GENERATED_FLASH_PREFILL_CALLS",
    )
    expect_generated_flash_prefill_hd512_calls = _required_metadata_env_int(
        metadata,
        "EXPECT_GENERATED_FLASH_PREFILL_HD512_CALLS",
    )
    expect_prefill_direct_kv = _required_metadata_env_bool(metadata, "EXPECT_PREFILL_DIRECT_KV")
    expect_fast_prepared_frame = _required_metadata_env_bool(metadata, "EXPECT_FAST_PREPARED_FRAME")
    expect_q4_mmv_variant = _expected_q4_mmv_variant(metadata)
    expect_swa_scan_clamp = _required_metadata_env_bool(metadata, "EXPECT_SWA_SCAN_CLAMP")
    expect_antfly_metal_device = _required_metadata_env(metadata, "EXPECT_ANTFLY_METAL_DEVICE")
    expect_llama_metal_device = _required_metadata_env(metadata, "EXPECT_LLAMA_METAL_DEVICE")
    expect_llama_offloaded_layers = _required_metadata_env_int(
        metadata,
        "EXPECT_LLAMA_OFFLOADED_LAYERS",
        positive=True,
    )
    expect_q4_mmv_portfolio = _expect_q4_mmv_portfolio(metadata)
    swa_scan_clamp_enabled = (
        _metadata_env_bool(metadata, "TERMITE_METAL_DISABLE_SWA_SCAN_CLAMP") is not True
    )
    if swa_scan_clamp_enabled != expect_swa_scan_clamp:
        raise BenchmarkContractError(
            "SWA scan clamp policy does not match the benchmark expectation: "
            f"enabled={swa_scan_clamp_enabled}, expected={expect_swa_scan_clamp}"
        )
    decode_frames = requested_tokens - 1
    expected_attention = decode_frames * 42

    for index in range(1, runs + 1):
        antfly_path = root / f"antfly-{index}.json"
        antfly_log_path = root / f"antfly-{index}.log"
        llama_path = root / f"llama-{index}.log"
        try:
            antfly = json.loads(antfly_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise BenchmarkContractError(f"invalid Antfly JSON: {antfly_path}: {exc}") from exc
        antfly_log = antfly_log_path.read_text(errors="replace")
        llama_log = llama_path.read_text(errors="replace")

        if antfly.get("backend") != "metal":
            raise BenchmarkContractError(f"Antfly did not report the Metal backend: {antfly_path}")
        if antfly.get("tokens") != requested_tokens or antfly.get("finish_reason") != "length":
            raise BenchmarkContractError(
                f"Antfly did not generate exactly {requested_tokens} length-limited tokens: {antfly_path}"
            )
        if "speculative" not in antfly or antfly.get("speculative") is not None:
            raise BenchmarkContractError(f"baseline benchmark unexpectedly used MTP/speculative decode: {antfly_path}")
        for draft_field in ("draft_cuda", "draft_cuda_generate"):
            if antfly.get(draft_field) is not None:
                raise BenchmarkContractError(f"baseline benchmark reported {draft_field}: {antfly_path}")
        if "generate-setup: live whole-model executor skipped" not in antfly_log:
            raise BenchmarkContractError(f"Antfly did not enter the compiled generation pipeline: {antfly_log_path}")
        if "gen_debug: executePrefill whole-model fast path" not in antfly_log:
            raise BenchmarkContractError(
                f"Antfly silently fell back from compiled whole-model prefill: {antfly_log_path}"
            )

        ids, id_values = _token_line(antfly_log, "token_ids", antfly_log_path)
        prompt_ids, prompt_id_values = _token_line(antfly_log, "prompt_token_ids", antfly_log_path)
        if len(id_values) != requested_tokens:
            raise BenchmarkContractError(
                f"Antfly token ID count={len(id_values)}, expected {requested_tokens}: {antfly_log_path}"
            )
        json_ids = antfly.get("token_ids")
        if json_ids is not None and json_ids != id_values:
            raise BenchmarkContractError(f"Antfly JSON/log token IDs differ: {antfly_path}")
        if reference_ids is None:
            reference_ids = ids
            reference_prompt_ids = prompt_ids
        elif ids != reference_ids or prompt_ids != reference_prompt_ids:
            raise BenchmarkContractError(
                f"Antfly exact prompt/generated token IDs changed between runs: {antfly_log_path}"
            )

        timing = _mapping(antfly.get("timing_ms"), "Antfly timing", antfly_path)
        antfly_total = _positive_finite(float(timing.get("generate") or 0), "Antfly total", antfly_path)
        antfly_prefill = _positive_finite(float(timing.get("prefill_inner") or 0), "Antfly prefill", antfly_path)
        antfly_decode = _positive_finite(float(timing.get("decode_inner") or 0), "Antfly decode", antfly_path)
        timing_tolerance = max(2.0, antfly_total * 0.001)
        if abs(antfly_prefill + antfly_decode - antfly_total) > timing_tolerance:
            raise BenchmarkContractError(
                f"Antfly phase timing does not reconcile: prefill+decode={antfly_prefill + antfly_decode:.3f}ms "
                f"total={antfly_total:.3f}ms: {antfly_path}"
            )

        llama_metal = parse_llama_metal_runtime(llama_log, llama_path)
        if llama_metal.default_device != expect_llama_metal_device:
            raise BenchmarkContractError(
                f"llama.cpp Metal device={llama_metal.default_device!r}, "
                f"expected {expect_llama_metal_device!r}: {llama_path}"
            )
        if (
            llama_metal.offloaded_layers != expect_llama_offloaded_layers
            or llama_metal.total_layers != expect_llama_offloaded_layers
        ):
            raise BenchmarkContractError(
                f"llama.cpp GPU layer offload={llama_metal.offloaded_layers}/"
                f"{llama_metal.total_layers}, expected all "
                f"{expect_llama_offloaded_layers}/{expect_llama_offloaded_layers}: {llama_path}"
            )
        llama = parse_llama_timing(llama_log, llama_path)
        if llama.prompt_tokens != len(prompt_id_values):
            raise BenchmarkContractError(
                f"prompt token accounting differs: Antfly={len(prompt_id_values)} "
                f"llama={llama.prompt_tokens}: {llama_path}"
            )
        if llama_prompt_id_values != prompt_id_values:
            raise BenchmarkContractError(
                "exact cross-implementation prompt token IDs differ: "
                f"Antfly={len(prompt_id_values)} llama={len(llama_prompt_id_values)}: {llama_path}"
            )
        if llama.eval_runs != decode_frames:
            raise BenchmarkContractError(
                f"llama eval runs={llama.eval_runs}, expected {decode_frames}: {llama_path}"
            )
        if llama.total_tokens is not None and llama.total_tokens != llama.prompt_tokens + llama.eval_runs:
            raise BenchmarkContractError(
                f"llama total token accounting={llama.total_tokens}, expected "
                f"{llama.prompt_tokens + llama.eval_runs}: {llama_path}"
            )
        if llama.graphs_reused is not None and llama.graphs_reused < max(0, llama.eval_runs - 2):
            raise BenchmarkContractError(
                f"llama graph reuse={llama.graphs_reused}, expected at least "
                f"{max(0, llama.eval_runs - 2)}: {llama_path}"
            )

        dispatch = _last_match(
            antfly_log,
            (
                r"(?m)^metal_attention_dispatch:.*\bpaged_1x=(\d+)"
                r".*\bdecode_gqa_split=(\d+)"
                r".*\bgenerated_flash_prefill=(\d+)"
                r".*\bgenerated_flash_prefill_hd512=(\d+)"
                r".*\bprefill_direct_kv=(\d+)"
                r".*\bprefill_paged_kv=(\d+)"
            ),
            "attention route counters",
            antfly_log_path,
        )
        paged_calls = int(dispatch.group(1))
        split_calls = int(dispatch.group(2))
        generated_flash_prefill_calls = int(dispatch.group(3))
        generated_flash_prefill_hd512_calls = int(dispatch.group(4))
        prefill_direct_kv_calls = int(dispatch.group(5))
        prefill_paged_kv_calls = int(dispatch.group(6))
        expected_paged = 0 if expect_split_gqa else expected_attention
        expected_split = expected_attention if expect_split_gqa else 0
        if paged_calls != expected_paged or split_calls != expected_split:
            raise BenchmarkContractError(
                f"decode attention routes paged/split={paged_calls}/{split_calls}, "
                f"expected {expected_paged}/{expected_split}: {antfly_log_path}"
            )
        gqa_schedule_match = _last_match(
            antfly_log,
            (
                r"(?m)^metal_decode_gqa_split_schedule:\s+legacy_total=(\d+)"
                r"\s+swa_total=(\d+)\s+global_total=(\d+)"
                r"\s+swa_s8=(\d+)\s+swa_s16=(\d+)\s+swa_s24=(\d+)\s+swa_s32=(\d+)"
                r"\s+global_s8=(\d+)\s+global_s16=(\d+)\s+global_s24=(\d+)\s+global_s32=(\d+)"
                r"\s+fallbacks=(\d+)\s+invalid_overrides=(\d+)"
            ),
            "split-GQA schedule counters",
            antfly_log_path,
        )
        gqa_schedule = tuple(
            int(gqa_schedule_match.group(group)) for group in range(1, 14)
        )
        expected_swa_split = 35 * decode_frames if expect_split_gqa else 0
        expected_global_split = 7 * decode_frames if expect_split_gqa else 0
        expected_gqa_schedule = (
            expected_split,
            expected_swa_split,
            expected_global_split,
            0,
            0,
            0,
            expected_swa_split,
            0,
            0,
            0,
            expected_global_split,
            0,
            0,
        )
        if gqa_schedule != expected_gqa_schedule:
            raise BenchmarkContractError(
                "split-GQA AUTO schedule did not resolve one-hot to s32 or reported fallback: "
                f"observed={gqa_schedule}, expected={expected_gqa_schedule}: {antfly_log_path}"
            )
        generated_flash_prefill_total = (
            generated_flash_prefill_calls + generated_flash_prefill_hd512_calls
        )
        if (
            generated_flash_prefill_calls != expect_generated_flash_prefill_calls
            or generated_flash_prefill_hd512_calls
            != expect_generated_flash_prefill_hd512_calls
        ):
            raise BenchmarkContractError(
                "generated flash prefill routes="
                f"{generated_flash_prefill_calls}/{generated_flash_prefill_hd512_calls}, "
                "expected exactly "
                f"{expect_generated_flash_prefill_calls}/"
                f"{expect_generated_flash_prefill_hd512_calls}: {antfly_log_path}"
            )
        if prefill_direct_kv_calls + prefill_paged_kv_calls != generated_flash_prefill_total:
            raise BenchmarkContractError(
                "prefill K/V routes do not reconcile with generated flash attention: "
                f"direct+paged={prefill_direct_kv_calls + prefill_paged_kv_calls}, "
                f"flash={generated_flash_prefill_total}: {antfly_log_path}"
            )
        if expect_prefill_direct_kv is True and (
            prefill_direct_kv_calls != generated_flash_prefill_total or prefill_paged_kv_calls != 0
        ):
            raise BenchmarkContractError(
                f"prefill direct K/V route={prefill_direct_kv_calls}/{prefill_paged_kv_calls}, "
                f"expected {generated_flash_prefill_total}/0: {antfly_log_path}"
            )
        if expect_prefill_direct_kv is False and (
            prefill_direct_kv_calls != 0 or prefill_paged_kv_calls != generated_flash_prefill_total
        ):
            raise BenchmarkContractError(
                f"prefill direct K/V rollback={prefill_direct_kv_calls}/{prefill_paged_kv_calls}, "
                f"expected 0/{generated_flash_prefill_total}: {antfly_log_path}"
            )

        prepared_frame = _last_match(
            antfly_log,
            r"(?m)^metal_prepared_frame:\s+fast_path=(\d+)\s+fallback=(\d+)",
            "prepared frame route counters",
            antfly_log_path,
        )
        prepared_frame_fast_path_calls = int(prepared_frame.group(1))
        prepared_frame_fallback_calls = int(prepared_frame.group(2))
        if prepared_frame_fast_path_calls + prepared_frame_fallback_calls != decode_frames:
            raise BenchmarkContractError(
                "prepared frame routes do not reconcile with decode frames: "
                f"fast+fallback={prepared_frame_fast_path_calls + prepared_frame_fallback_calls}, "
                f"decode_frames={decode_frames}: {antfly_log_path}"
            )
        if expect_fast_prepared_frame is True and (
            prepared_frame_fast_path_calls != decode_frames or prepared_frame_fallback_calls != 0
        ):
            raise BenchmarkContractError(
                f"prepared frame routes={prepared_frame_fast_path_calls}/{prepared_frame_fallback_calls}, "
                f"expected {decode_frames}/0: {antfly_log_path}"
            )
        if expect_fast_prepared_frame is False and (
            prepared_frame_fast_path_calls != 0 or prepared_frame_fallback_calls != decode_frames
        ):
            raise BenchmarkContractError(
                f"prepared frame rollback={prepared_frame_fast_path_calls}/{prepared_frame_fallback_calls}, "
                f"expected 0/{decode_frames}: {antfly_log_path}"
            )

        runtime_memory = _unique_match(
            antfly_log,
            r"(?m)^metal_runtime_memory:.*\bframe_retained_mb=(\d+)",
            "Metal runtime memory counters",
            antfly_log_path,
        )
        if int(runtime_memory.group(1)) != 0:
            raise BenchmarkContractError(f"compiled decoder retained a speculative frame: {antfly_log_path}")

        q4_rows = _last_match(
            antfly_log,
            r"(?m)^metal_q4_0_dispatch:.*\blinear_reduce_rows=(\d+)/(\d+)/(\d+)/(\d+)",
            "Q4_0 route counters",
            antfly_log_path,
        )
        q6_rows = _last_match(
            antfly_log,
            r"(?m)^metal_q4_q6_k_dispatch:.*\bq6_linear_reduce_rows=(\d+)/(\d+)/(\d+)/(\d+)",
            "Q6_K route counters",
            antfly_log_path,
        )
        q4_exact = _optional_unique_match(
            antfly_log,
            r"^metal_jit_exact_dispatch:\s+q4_0=(\d+)",
            "Q4_0 exact route counters",
            antfly_log_path,
        )
        q4_pair = _last_match(
            antfly_log,
            r"^metal_q4_0_dispatch:.*\bpair_act_reduce=(\d+)",
            "Q4_0 pair route counters",
            antfly_log_path,
        )
        q4_encode = _optional_unique_match(
            antfly_log,
            r"^metal_q4_0_encode_us:\s+linear_reduce=(\d+)",
            "Q4_0 encode timing",
            antfly_log_path,
        )
        q4_policy = _last_match(
            antfly_log,
            (
                r"(?m)^metal_q4_0_policy:\s+mmv_nr4_nsg2=(\d+)"
                r"\s+mmv_nr8_nsg2=(\d+)"
                r"\s+mmv_nr4_nsg4=(\d+)"
                r"\s+mmv_nr8_nsg4=(\d+)"
                r"\s+mmv_variant_fallbacks=(\d+)"
                r"\s+mm_sg_aligned=(\d+)"
                r"\s+mm_sg_aligned_tail=(\d+)"
                r"\s+mm_sg_unrolled=(\d+)"
            ),
            "Q4_0 policy counters",
            antfly_log_path,
        )
        q4_mmv_variant_calls = [int(q4_policy.group(group)) for group in range(1, 5)]
        q4_mmv_variant_fallbacks = int(q4_policy.group(5))
        q4_mm_sg_aligned_calls = int(q4_policy.group(6))
        q4_mm_sg_aligned_tail_calls = int(q4_policy.group(7))
        q4_mm_sg_unrolled_calls = int(q4_policy.group(8))
        exact_q4_calls = int(q4_exact.group(1)) if q4_exact else 0
        fused_q4_pairs = int(q4_pair.group(1))
        logical_q4_calls = int(q4_rows.group(1)) + exact_q4_calls + 2 * fused_q4_pairs
        expected_q4_calls = decode_frames * 210
        if logical_q4_calls != expected_q4_calls:
            raise BenchmarkContractError(
                f"Q4_0 decode routes={logical_q4_calls}, expected exactly {expected_q4_calls}: {antfly_log_path}"
            )
        if sum(q4_mmv_variant_calls) != int(q4_rows.group(1)):
            raise BenchmarkContractError(
                f"Q4_0 MMV variants={sum(q4_mmv_variant_calls)}, "
                f"expected row-one routes={q4_rows.group(1)}: {antfly_log_path}"
            )
        if expect_q4_mmv_portfolio and q4_mmv_variant_fallbacks != 0:
            raise BenchmarkContractError(
                f"Q4_0 MMV portfolio fallbacks={q4_mmv_variant_fallbacks}, expected 0: {antfly_log_path}"
            )
        if expect_q4_mmv_variant != "any":
            variant_names = ("nr4-nsg2", "nr8-nsg2", "nr4-nsg4", "nr8-nsg4")
            expected_variant_calls = [0, 0, 0, 0]
            expected_variant_calls[variant_names.index(expect_q4_mmv_variant)] = expected_q4_calls
            if (
                int(q4_rows.group(1)) != expected_q4_calls
                or exact_q4_calls != 0
                or fused_q4_pairs != 0
                or q4_mmv_variant_calls != expected_variant_calls
                or q4_mmv_variant_fallbacks != 0
            ):
                observed = ", ".join(
                    f"{name}={calls}"
                    for name, calls in zip(variant_names, q4_mmv_variant_calls, strict=True)
                )
                raise BenchmarkContractError(
                    f"Q4_0 MMV one-hot route is not {expect_q4_mmv_variant}="
                    f"{expected_q4_calls}: {observed}, row_one={q4_rows.group(1)}, "
                    f"exact={exact_q4_calls}, pairs={fused_q4_pairs}, "
                    f"fallbacks={q4_mmv_variant_fallbacks}: {antfly_log_path}"
                )
        if int(q6_rows.group(1)) != requested_tokens:
            raise BenchmarkContractError(
                f"Q6_K LM-head routes={q6_rows.group(1)}, expected exactly {requested_tokens}: {antfly_log_path}"
            )

        runtime = _mapping(antfly.get("runtime"), "Antfly runtime counters", antfly_path)
        decoder = _mapping(
            antfly.get("generation_decoder_runtime"),
            "Antfly generation decoder counters",
            antfly_path,
        )
        _exact_int(runtime, "decode_greedy_calls", decode_frames, "runtime", antfly_path)
        _exact_int(decoder, "forward_attempts", decode_frames, "generation_decoder_runtime", antfly_path)
        metal = _mapping(antfly.get("metal"), "Metal counters", antfly_path)
        antfly_metal_device = metal.get("device")
        if antfly_metal_device != expect_antfly_metal_device:
            raise BenchmarkContractError(
                f"Antfly Metal device={antfly_metal_device!r}, "
                f"expected {expect_antfly_metal_device!r}: {antfly_path}"
            )
        antfly_metal_device_registry_id = metal.get("device_registry_id")
        if (
            isinstance(antfly_metal_device_registry_id, bool)
            or not isinstance(antfly_metal_device_registry_id, int)
            or antfly_metal_device_registry_id <= 0
        ):
            raise BenchmarkContractError(
                "Antfly Metal device_registry_id must be a positive integer, got "
                f"{antfly_metal_device_registry_id!r}: {antfly_path}"
            )
        if metal.get("native_quant_null") is not False:
            raise BenchmarkContractError(f"Metal native quant route was unavailable: {antfly_path}")
        operators = _mapping(metal.get("runtime_command_operators"), "Metal operator counters", antfly_path)
        _exact_int(operators, "fallback", 0, "metal.runtime_command_operators", antfly_path)
        frame_fallbacks = _mapping(metal.get("frame_fallbacks"), "Metal frame fallback counters", antfly_path)
        for key in ("decode_fallback", "prefill_plan_fail", "prefill_execute_fail"):
            _exact_int(frame_fallbacks, key, 0, "metal.frame_fallbacks", antfly_path)
        quant_plan = _mapping(metal.get("quant_kernel_plan"), "Metal quant plan counters", antfly_path)
        for key in ("fast_path_misses", "unsupported_routes"):
            _exact_int(quant_plan, key, 0, "metal.quant_kernel_plan", antfly_path)
        attention = _mapping(metal.get("attention_dispatch"), "Metal attention counters", antfly_path)
        _exact_int(attention, "paged_1x", expected_paged, "metal.attention_dispatch", antfly_path)
        _exact_int(attention, "decode_gqa_split", expected_split, "metal.attention_dispatch", antfly_path)
        _exact_int(
            attention,
            "generated_flash_prefill",
            generated_flash_prefill_calls,
            "metal.attention_dispatch",
            antfly_path,
        )
        _exact_int(
            attention,
            "generated_flash_prefill_hd512",
            generated_flash_prefill_hd512_calls,
            "metal.attention_dispatch",
            antfly_path,
        )
        _exact_int(
            attention,
            "prefill_direct_kv",
            prefill_direct_kv_calls,
            "metal.attention_dispatch",
            antfly_path,
        )
        _exact_int(
            attention,
            "prefill_paged_kv",
            prefill_paged_kv_calls,
            "metal.attention_dispatch",
            antfly_path,
        )
        prepared = _mapping(metal.get("prepared_frame"), "Metal prepared frame counters", antfly_path)
        _exact_int(
            prepared,
            "fast_path",
            prepared_frame_fast_path_calls,
            "metal.prepared_frame",
            antfly_path,
        )
        _exact_int(
            prepared,
            "fallback",
            prepared_frame_fallback_calls,
            "metal.prepared_frame",
            antfly_path,
        )
        q4_policy_json = _mapping(metal.get("q4_0_policy"), "Metal Q4_0 policy counters", antfly_path)
        for key, expected in zip(
            ("mmv_nr4_nsg2", "mmv_nr8_nsg2", "mmv_nr4_nsg4", "mmv_nr8_nsg4"),
            q4_mmv_variant_calls,
            strict=True,
        ):
            _exact_int(q4_policy_json, key, expected, "metal.q4_0_policy", antfly_path)
        for key, expected in (
            ("mmv_variant_fallbacks", q4_mmv_variant_fallbacks),
            ("mm_sg_aligned", q4_mm_sg_aligned_calls),
            ("mm_sg_aligned_tail", q4_mm_sg_aligned_tail_calls),
            ("mm_sg_unrolled", q4_mm_sg_unrolled_calls),
        ):
            _exact_int(q4_policy_json, key, expected, "metal.q4_0_policy", antfly_path)

        llama_decode_ms = llama.eval_ms + llama.sampling_ms
        antfly_decode_tps = decode_frames * 1000.0 / antfly_decode
        llama_decode_tps = llama.eval_runs * 1000.0 / llama_decode_ms
        rows.append(
            {
                "sample": index,
                "prompt_tokens": len(prompt_id_values),
                "antfly_total_ms": antfly_total,
                "antfly_prefill_ms": antfly_prefill,
                "antfly_decode_ms": antfly_decode,
                "antfly_decode_tok_s": antfly_decode_tps,
                "llama_total_ms": llama.total_ms,
                "llama_prompt_ms": llama.prompt_ms,
                "llama_decode_ms": llama_decode_ms,
                "llama_decode_tok_s": llama_decode_tps,
                "llama_timing_logger": llama.logger,
                "llama_graphs_reused": llama.graphs_reused,
                "llama_metal_device": llama_metal.default_device,
                "llama_offloaded_layers": llama_metal.offloaded_layers,
                "llama_total_layers": llama_metal.total_layers,
                "antfly_metal_device": antfly_metal_device,
                "antfly_metal_device_registry_id": antfly_metal_device_registry_id,
                "total_ratio": antfly_total / llama.total_ms,
                "prefill_latency_ratio": antfly_prefill / llama.prompt_ms,
                "decode_latency_ratio": antfly_decode / llama_decode_ms,
                "decode_ratio": antfly_decode_tps / llama_decode_tps,
                "paged_1x_calls": paged_calls,
                "decode_gqa_split_calls": split_calls,
                "decode_gqa_split_swa_s32_calls": gqa_schedule[6],
                "decode_gqa_split_global_s32_calls": gqa_schedule[10],
                "decode_gqa_split_schedule_fallbacks": gqa_schedule[11],
                "decode_gqa_split_schedule_invalid_overrides": gqa_schedule[12],
                "generated_flash_prefill_calls": generated_flash_prefill_calls,
                "generated_flash_prefill_hd512_calls": generated_flash_prefill_hd512_calls,
                "prefill_direct_kv_calls": prefill_direct_kv_calls,
                "prefill_paged_kv_calls": prefill_paged_kv_calls,
                "prepared_frame_fast_path_calls": prepared_frame_fast_path_calls,
                "prepared_frame_fallback_calls": prepared_frame_fallback_calls,
                "swa_scan_clamp_enabled": swa_scan_clamp_enabled,
                "frame_retained_mb": int(runtime_memory.group(1)),
                "q4_0_linear_reduce_rows_1": int(q4_rows.group(1)),
                "q4_0_mmv_nr4_nsg2_calls": q4_mmv_variant_calls[0],
                "q4_0_mmv_nr8_nsg2_calls": q4_mmv_variant_calls[1],
                "q4_0_mmv_nr4_nsg4_calls": q4_mmv_variant_calls[2],
                "q4_0_mmv_nr8_nsg4_calls": q4_mmv_variant_calls[3],
                "q4_0_mmv_variant_fallbacks": q4_mmv_variant_fallbacks,
                "q4_0_mm_sg_aligned_calls": q4_mm_sg_aligned_calls,
                "q4_0_mm_sg_aligned_tail_calls": q4_mm_sg_aligned_tail_calls,
                "q4_0_mm_sg_unrolled_calls": q4_mm_sg_unrolled_calls,
                "q4_0_exact_dispatches": exact_q4_calls,
                "q4_0_pair_activation_dispatches": fused_q4_pairs,
                "q6_k_linear_reduce_rows_1": int(q6_rows.group(1)),
                "q4_0_linear_reduce_encode_us": int(q4_encode.group(1)) if q4_encode else None,
                "no_mtp": True,
                "route_contract_passed": True,
            }
        )

    assert reference_ids is not None and reference_prompt_ids is not None
    return rows, reference_ids, reference_prompt_ids, llama_prompt_ids, loader_audit


def build_result(
    root: Path,
    runs: int,
    requested_tokens: int,
    max_total_ratio: float,
    min_decode_ratio: float,
    max_cv: float,
    expected_token_ids_sha256: str = "",
    require_confidence: bool = False,
) -> dict[str, Any]:
    for label, value in (
        ("max_total_ratio", max_total_ratio),
        ("min_decode_ratio", min_decode_ratio),
    ):
        if not math.isfinite(value) or value <= 0.0:
            raise BenchmarkContractError(f"{label} must be a positive finite value, got {value!r}")
    if not math.isfinite(max_cv) or not 0.0 < max_cv < 1.0:
        raise BenchmarkContractError(f"max_cv must be finite and in (0, 1), got {max_cv!r}")
    metadata = _load_metadata(root, runs, requested_tokens)
    for key, supplied in (
        ("max_total_ratio", max_total_ratio),
        ("min_decode_ratio", min_decode_ratio),
        ("max_cv", max_cv),
    ):
        if not math.isclose(float(metadata[key]), supplied, rel_tol=0.0, abs_tol=0.0):
            raise BenchmarkContractError(
                f"summarizer {key}={supplied} differs from recorded benchmark {key}={metadata[key]}"
            )
    execution_order, execution_order_gate = _load_execution_order(root, metadata, runs)
    require_confidence = require_confidence or metadata["require_confidence"]
    rows, token_ids, prompt_token_ids, llama_prompt_token_ids, llama_loader_audit = collect_rows(
        root,
        runs,
        requested_tokens,
        metadata,
    )
    if metadata["canonical_policy"] and any(row["prompt_tokens"] != 2003 for row in rows):
        raise BenchmarkContractError(
            "canonical benchmark prompt must tokenize to exactly 2003 tokens"
        )
    token_ids_sha256 = _digest(token_ids)
    prompt_token_ids_sha256 = _digest(prompt_token_ids)
    llama_prompt_token_ids_sha256 = _digest(llama_prompt_token_ids)
    pinned_token_ids_sha256 = metadata["expected_token_ids_sha256"]
    pinned_prompt_token_ids_sha256 = metadata["expected_prompt_token_ids_sha256"]
    if expected_token_ids_sha256:
        normalized_expected = expected_token_ids_sha256.lower()
        if re.fullmatch(r"[0-9a-f]{64}", normalized_expected) is None:
            raise BenchmarkContractError(
                f"expected token digest must be a SHA-256 value, got {expected_token_ids_sha256!r}"
            )
        if normalized_expected != pinned_token_ids_sha256:
            raise BenchmarkContractError(
                "requested token digest does not match the immutable benchmark metadata pin: "
                f"requested={normalized_expected}, recorded={pinned_token_ids_sha256}"
            )
    if token_ids_sha256 != pinned_token_ids_sha256:
        raise BenchmarkContractError(
            f"Antfly exact token digest changed: expected {pinned_token_ids_sha256}, "
            f"got {token_ids_sha256}"
        )
    if prompt_token_ids_sha256 != pinned_prompt_token_ids_sha256:
        raise BenchmarkContractError(
            "Antfly exact prompt token digest changed: "
            f"expected {pinned_prompt_token_ids_sha256}, got {prompt_token_ids_sha256}"
        )
    if llama_prompt_token_ids_sha256 != pinned_prompt_token_ids_sha256:
        raise BenchmarkContractError(
            "llama.cpp exact prompt token digest changed: "
            f"expected {pinned_prompt_token_ids_sha256}, got {llama_prompt_token_ids_sha256}"
        )
    registry_ids = {row["antfly_metal_device_registry_id"] for row in rows}
    if len(registry_ids) != 1:
        raise BenchmarkContractError(
            f"Antfly Metal device registry ID changed between samples: {sorted(registry_ids)}"
        )
    antfly_device_registry_id = next(iter(registry_ids))

    metric_fields = {
        "antfly_total_ms": "antfly_total_ms",
        "llama_total_ms": "llama_total_ms",
        "antfly_prefill_ms": "antfly_prefill_ms",
        "llama_prompt_ms": "llama_prompt_ms",
        "antfly_decode_ms": "antfly_decode_ms",
        "llama_decode_ms": "llama_decode_ms",
        "antfly_decode_tok_s": "antfly_decode_tok_s",
        "llama_decode_tok_s": "llama_decode_tok_s",
    }
    metric_stats = {
        output: stats(row[field] for row in rows)
        for output, field in metric_fields.items()
    }
    paired = {
        "total_latency_ratio": ratio_stats(rows, "total_ratio"),
        "prefill_latency_ratio": ratio_stats(rows, "prefill_latency_ratio"),
        "decode_latency_ratio": ratio_stats(rows, "decode_latency_ratio"),
        "decode_throughput_ratio": ratio_stats(rows, "decode_ratio"),
    }
    paired_confidence_intervals = {
        "total_latency_ratio": paired_bootstrap_interval(rows, "total_ratio"),
        "prefill_latency_ratio": paired_bootstrap_interval(rows, "prefill_latency_ratio"),
        "decode_latency_ratio": paired_bootstrap_interval(rows, "decode_latency_ratio"),
        "decode_throughput_ratio": paired_bootstrap_interval(rows, "decode_ratio"),
    }
    confidence_eligible = runs >= _CONFIDENCE_MIN_RUNS
    total_upper_below_parity = (
        paired_confidence_intervals["total_latency_ratio"]["upper"] < 1.0
    )
    decode_lower_above_parity = (
        paired_confidence_intervals["decode_throughput_ratio"]["lower"] > 1.0
    )
    total_parity_wins = sum(row["total_ratio"] < 1.0 for row in rows)
    decode_parity_wins = sum(row["decode_ratio"] > 1.0 for row in rows)
    total_sign_test_p = one_sided_sign_test_p(total_parity_wins, runs)
    decode_sign_test_p = one_sided_sign_test_p(decode_parity_wins, runs)
    exact_sign_tests_passed = total_sign_test_p <= 0.05 and decode_sign_test_p <= 0.05
    confidence_gate = {
        "required": require_confidence,
        "minimum_runs": _CONFIDENCE_MIN_RUNS,
        "eligible": confidence_eligible,
        "total_latency_upper_below_parity": total_upper_below_parity,
        "decode_throughput_lower_above_parity": decode_lower_above_parity,
        "total_latency_parity_wins": total_parity_wins,
        "decode_throughput_parity_wins": decode_parity_wins,
        "total_latency_one_sided_sign_p": total_sign_test_p,
        "decode_throughput_one_sided_sign_p": decode_sign_test_p,
        "exact_sign_tests_passed": exact_sign_tests_passed,
        "passed": (
            confidence_eligible
            and total_upper_below_parity
            and decode_lower_above_parity
            and exact_sign_tests_passed
        ),
    }
    cv_metrics = (
        "antfly_total_ms",
        "llama_total_ms",
        "antfly_prefill_ms",
        "llama_prompt_ms",
        "antfly_decode_ms",
        "llama_decode_ms",
    )
    cv_violations = {
        name: metric_stats[name]["cv"]
        for name in cv_metrics
        if metric_stats[name]["cv"] > max_cv
    }
    return {
        "schema": "antfly.gemma4_metal_long_output.v4",
        "metadata": metadata,
        "runs": runs,
        "canonical_policy": metadata["canonical_policy"],
        "promotion_eligible_policy": metadata["canonical_policy"] and require_confidence,
        "output_tokens": requested_tokens,
        "prompt_tokens": rows[0]["prompt_tokens"],
        **metric_stats,
        "paired_ratios": paired,
        "paired_confidence_intervals": paired_confidence_intervals,
        # Backward-compatible top-level gates now use medians of paired ratios.
        "total_ratio": paired["total_latency_ratio"]["median"],
        "max_total_ratio": max_total_ratio,
        "decode_ratio": paired["decode_throughput_ratio"]["median"],
        "min_decode_ratio": min_decode_ratio,
        "max_cv": max_cv,
        "cv_gate": {
            "metrics": list(cv_metrics),
            "violations": cv_violations,
            "passed": not cv_violations,
        },
        "confidence_gate": confidence_gate,
        "execution_order": execution_order,
        "execution_order_gate": execution_order_gate,
        "token_ids": token_ids,
        "token_ids_sha256": token_ids_sha256,
        "expected_token_ids_sha256": pinned_token_ids_sha256,
        "prompt_token_ids_sha256": prompt_token_ids_sha256,
        "llama_prompt_token_ids_sha256": llama_prompt_token_ids_sha256,
        "expected_prompt_token_ids_sha256": pinned_prompt_token_ids_sha256,
        "no_mtp": True,
        "policy_route_expectations": {
            "generated_flash_prefill_calls": _required_metadata_env_int(
                metadata,
                "EXPECT_GENERATED_FLASH_PREFILL_CALLS",
            ),
            "generated_flash_prefill_hd512_calls": _required_metadata_env_int(
                metadata,
                "EXPECT_GENERATED_FLASH_PREFILL_HD512_CALLS",
            ),
            "prefill_direct_kv": _required_metadata_env_bool(metadata, "EXPECT_PREFILL_DIRECT_KV"),
            "fast_prepared_frame": _required_metadata_env_bool(
                metadata,
                "EXPECT_FAST_PREPARED_FRAME",
            ),
            "q4_0_mmv_variant": _expected_q4_mmv_variant(metadata),
            "q4_0_mmv_portfolio": _expect_q4_mmv_portfolio(metadata),
            "swa_scan_clamp": _required_metadata_env_bool(metadata, "EXPECT_SWA_SCAN_CLAMP"),
            "decode_gqa_split_schedule": {"swa": "s32", "global": "s32"},
        },
        "llama_backend_expectations": {
            "metal_device": _required_metadata_env(metadata, "EXPECT_LLAMA_METAL_DEVICE"),
            "offloaded_layers": _required_metadata_env_int(
                metadata,
                "EXPECT_LLAMA_OFFLOADED_LAYERS",
                positive=True,
            ),
        },
        "antfly_backend_expectations": {
            "metal_device": _required_metadata_env(metadata, "EXPECT_ANTFLY_METAL_DEVICE"),
            "device_registry_id_positive": True,
            "device_registry_id": antfly_device_registry_id,
        },
        "cross_implementation_prompt_token_contract_passed": True,
        "antfly_generated_token_contract_passed": True,
        "cross_implementation_generated_token_contract": "unavailable_in_pinned_llama_completion",
        "llama_cpp_loader_audit": llama_loader_audit,
        "route_contract_passed": True,
        "rows": rows,
    }


def gate_errors(result: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    violations = result["cv_gate"]["violations"]
    if violations:
        rendered = ", ".join(f"{name}={value:.3f}" for name, value in sorted(violations.items()))
        errors.append(f"benchmark phase CV exceeded {result['max_cv']:.3f}: {rendered}")
    if result["total_ratio"] > result["max_total_ratio"]:
        errors.append(
            f"paired Antfly/llama total ratio {result['total_ratio']:.3f} "
            f"exceeds {result['max_total_ratio']:.3f}"
        )
    if result["decode_ratio"] < result["min_decode_ratio"]:
        errors.append(
            f"paired Antfly/llama decode ratio {result['decode_ratio']:.3f} "
            f"below {result['min_decode_ratio']:.3f}"
        )
    confidence_gate = result.get("confidence_gate")
    if confidence_gate and confidence_gate.get("required") and not confidence_gate.get("passed"):
        if not confidence_gate.get("eligible"):
            errors.append(
                "paired bootstrap confidence gate requires at least "
                f"{confidence_gate['minimum_runs']} measured runs; got {result['runs']}"
            )
        total_interval = result["paired_confidence_intervals"]["total_latency_ratio"]
        if not confidence_gate.get("total_latency_upper_below_parity"):
            errors.append(
                "paired bootstrap total-latency upper bound "
                f"{total_interval['upper']:.3f} is not below parity (1.000)"
            )
        decode_interval = result["paired_confidence_intervals"]["decode_throughput_ratio"]
        if not confidence_gate.get("decode_throughput_lower_above_parity"):
            errors.append(
                "paired bootstrap decode-throughput lower bound "
                f"{decode_interval['lower']:.3f} is not above parity (1.000)"
            )
        if not confidence_gate.get("exact_sign_tests_passed"):
            errors.append(
                "exact paired sign tests did not reject parity at one-sided alpha=0.05: "
                f"total p={confidence_gate['total_latency_one_sided_sign_p']:.4f}, "
                f"decode p={confidence_gate['decode_throughput_one_sided_sign_p']:.4f}"
            )
    return errors


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] not in (
        "summarize",
        "bundle-manifest",
        "validate-preflight",
        "render-prompt",
        "validate-git-worktree",
    ):
        argv.insert(0, "summarize")
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    summarize = commands.add_parser("summarize", help="validate and summarize benchmark artifacts")
    summarize.add_argument("--out-dir", type=Path, required=True)
    summarize.add_argument("--runs", type=int, required=True)
    summarize.add_argument("--output-tokens", type=int, required=True)
    summarize.add_argument("--max-total-ratio", type=float, required=True)
    summarize.add_argument("--min-decode-ratio", type=float, required=True)
    summarize.add_argument("--max-cv", type=float, required=True)
    summarize.add_argument("--expected-token-ids-sha256", default="")
    summarize.add_argument("--require-confidence", action="store_true")
    bundle = commands.add_parser("bundle-manifest", help="write and hash a llama.cpp bundle manifest")
    bundle.add_argument("--root", type=Path, required=True)
    bundle.add_argument("--output", type=Path, required=True)
    preflight = commands.add_parser(
        "validate-preflight",
        help="fail fast on prompt-token or loaded-library drift before measured runs",
    )
    preflight.add_argument("--log", type=Path, required=True)
    preflight.add_argument("--expected-prompt-token-ids-sha256", required=True)
    preflight.add_argument("--expected-prompt-tokens", type=int, default=0)
    preflight.add_argument(
        "--loader-audit-mode",
        choices=("dyld_print_libraries_preflight", "skipped_non_macho_noncanonical"),
        required=True,
    )
    preflight.add_argument("--bundle-root", type=Path, required=True)
    preflight.add_argument("--comparator-binary", type=Path, required=True)
    preflight.add_argument("--output", type=Path, required=True)
    prompt = commands.add_parser(
        "render-prompt",
        help="render the canonical raw long-context prompt",
    )
    prompt.add_argument("--repeat", type=int, default=36)
    git_worktree = commands.add_parser(
        "validate-git-worktree",
        help="reject untracked files in a canonical benchmark worktree",
    )
    git_worktree.add_argument("--repo-root", type=Path, required=True)
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    if args.command == "render-prompt":
        try:
            sys.stdout.write(canonical_long_output_prompt(args.repeat))
        except BenchmarkContractError as exc:
            raise SystemExit(str(exc)) from exc
        return
    if args.command == "validate-git-worktree":
        try:
            validate_canonical_git_worktree(args.repo_root)
        except BenchmarkContractError as exc:
            raise SystemExit(str(exc)) from exc
        return
    if args.command == "bundle-manifest":
        try:
            print(write_llama_bundle_manifest(args.root, args.output))
        except (BenchmarkContractError, OSError) as exc:
            raise SystemExit(str(exc)) from exc
        return
    if args.command == "validate-preflight":
        try:
            result = validate_llama_prompt_preflight(
                args.log,
                args.expected_prompt_token_ids_sha256.lower(),
                args.expected_prompt_tokens,
                args.loader_audit_mode,
                args.bundle_root,
                args.comparator_binary,
            )
            args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        except (BenchmarkContractError, OSError) as exc:
            raise SystemExit(str(exc)) from exc
        return
    try:
        result = build_result(
            args.out_dir,
            args.runs,
            args.output_tokens,
            args.max_total_ratio,
            args.min_decode_ratio,
            args.max_cv,
            args.expected_token_ids_sha256,
            args.require_confidence,
        )
    except (BenchmarkContractError, OSError, json.JSONDecodeError) as exc:
        raise SystemExit(str(exc)) from exc
    summary_path = args.out_dir / "summary.json"
    summary_path.write_text(json.dumps(result, indent=2) + "\n")
    print(
        f"Gemma4 long-output: prompt={result['prompt_tokens']} output={result['output_tokens']} "
        f"Antfly={result['antfly_total_ms']['median']:.1f}ms "
        f"llama={result['llama_total_ms']['median']:.1f}ms "
        f"paired_total_ratio={result['total_ratio']:.3f} "
        f"paired_decode_ratio={result['decode_ratio']:.3f}"
    )
    errors = gate_errors(result)
    if errors:
        raise SystemExit("\n".join(errors))


if __name__ == "__main__":
    main()
