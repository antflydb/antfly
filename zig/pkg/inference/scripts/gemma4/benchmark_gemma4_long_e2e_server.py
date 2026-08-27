#!/usr/bin/env python3
"""Paired warm-server Gemma 4 long-context benchmark and evidence gate.

The harness runs one engine at a time, warms each resident server, and then
measures a streaming request over the already-open HTTP connection.  Paired
samples alternate AB/BA order.  This keeps model loading, server startup, and
TCP establishment outside the measured region while avoiding two engines
contending for the same GPU.
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime
import hashlib
import http.client
import io
import json
import math
import os
import pathlib
import platform
import random
import shlex
import signal
import socket
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Iterable

SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from paired_benchmark import (
    balanced_pair_order,
    distribution as paired_distribution,
    paired_metric_log_ratio_ci,
    percentile as paired_percentile,
    write_evidence_manifest,
)


EVIDENCE_SCHEMA = "antfly.gemma4_long_e2e.v1"
LOCK_SCHEMA = "antfly.gemma4_long_e2e.lock.v1"
FIXTURE_SCHEMA = "antfly.prompt_fixture.v1"
ENGINES = ("antfly", "llama_cpp")
CHAT_TEMPLATE_KWARGS = {"enable_thinking": False}
REFERENCE_CHAT_PREFIX = "<|turn>user\n"
REFERENCE_PUBLIC_FINAL_SUFFIX = "<turn|>\n<|turn>model\n<|channel>final\n<channel|>"
TOKENIZER_BOS_TEXT = "<bos>"
LLAMA_CHAT_TEMPLATE = (
    "{%- for message in messages -%}"
    "{{ '<|turn>' + message['role'] + '\\n' + message['content'] + '<turn|>\\n' }}"
    "{%- endfor -%}"
    "{%- if add_generation_prompt -%}"
    "{%- if enable_thinking is defined and not enable_thinking -%}"
    "{{ '<|turn>model\\n<|channel>final\\n<channel|>' }}"
    "{%- else -%}"
    "{{ '<|turn>model\\n<|channel>thought\\n<channel|>' }}"
    "{%- endif -%}"
    "{%- endif -%}"
)
PROFILE_DEFAULTS = {
    "headline": {"cache_dtype": "f16", "max_baseline_regression": None},
    "e2b-regression": {"cache_dtype": "f16", "max_baseline_regression": 1.03},
    "e4b-regression": {"cache_dtype": "f16", "max_baseline_regression": 1.03},
    "f32-control": {"cache_dtype": "f32", "max_baseline_regression": 1.05},
}
CUDA_PROFILE_NAME = "antfly.gemma4_cuda_model_neutral_server.v3"
CUDA_EXECUTION_PROFILE_DEFAULT = "model-neutral-v3"
CUDA_PROFILE_DEFAULT_ENV = {
    "ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY": "required",
    "ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE": "required-fast",
    "ANTFLY_INFERENCE_CUDA_TEMP_ARENA_AUTOPLAN": "1",
    "ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN": "1",
    "ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS": "0",
}
# Reviewed L4/E2B contract. Keep every material Antfly tuning value explicit:
# this profile must remain reproducible even when the server command is not
# wrapped by with_gemma4_qat_cuda_tuning.sh.
GEMMA4_E2B_SM89_FLASH_COMMON_ENV = {
    "ANTFLY_INFERENCE_CUDA_ASYNC_I32_DOWNLOAD_STAGING": "1",
    "ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION": "0",
    "ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION_CHUNK": "12",
    "ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_PRECOMPUTE": "1",
    "ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_Q8_1_PRECOMPUTE": "0",
    "ANTFLY_INFERENCE_CUDA_Q4_0_Q8_1_PREFILL_ROWS": "1",
    "ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE": "required-flash-f16-sm89",
    "ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE4_W8": "1",
    "ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE4_W8_MIN_IN_DIM": "2048",
    "ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_ROWS8_C4": "1",
    "ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_ROWS8_C2": "1",
    "ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_ROWS16_C1": "1",
    "ANTFLY_INFERENCE_CUDA_GEMMA_PREFILL_PREWARM": "1",
    "ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN": "1",
    "ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS": "2048",
    "ANTFLY_INFERENCE_CUDA_PROFILE_PREFILL_OPS": "0",
    "ANTFLY_INFERENCE_CUDA_PROFILE_DECODE": "0",
    "ANTFLY_INFERENCE_CUDA_RMS_NORM_BF16_MIRROR": "0",
    "ANTFLY_INFERENCE_CUDA_DEQUANTIZE_Q4_0_MATRIX_WEIGHTS_BF16": "0",
    "ANTFLY_INFERENCE_CUDA_Q4_0_WEIGHTS_BF16_PREFILL": "1",
    "ANTFLY_INFERENCE_CUDA_PLE_MODEL_PROJ_BF16": "0",
    "ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE4_W10_E4B_DOWN": "1",
    "ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_Q8_1_TILE4_W8": "1",
    "ANTFLY_INFERENCE_CUDA_GREEDY_PENDING_TOKEN_READBACK": "1",
    "ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_DP4A": "1",
    "ANTFLY_INFERENCE_CUDA_Q4_0_QKV_Q8_1_DP4A": "1",
    "ANTFLY_INFERENCE_CUDA_Q4_0_QKV_Q8_1_TILE8": "1",
    "ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_Q8_1_DP4A": "1",
    "ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_DP4A": "1",
    "ANTFLY_INFERENCE_CUDA_Q4_0_ACTIVATION_SLICE_Q8_1_DP4A": "1",
    "ANTFLY_INFERENCE_CUDA_Q4_0_GATED_DOWN_Q8_1_DP4A": "1",
    "ANTFLY_INFERENCE_CUDA_Q6_K_LM_HEAD_Q8_1": "1",
    "ANTFLY_INFERENCE_CUDA_Q6_K_LM_HEAD_Q8_1_TILE8_EXACT_THREADS": "1",
    "ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY": "required",
    "ANTFLY_INFERENCE_CUDA_CAPTURE_FINAL_HIDDEN": "1",
    "ANTFLY_INFERENCE_CUDA_CAPTURE_UPDATE_EXEC": "1",
    "ANTFLY_INFERENCE_CUDA_CAPTURE_DEVICE_SCALARS": "1",
    "ANTFLY_INFERENCE_CUDA_CAPTURE_PERSISTENT_REPLAY": "1",
    "ANTFLY_INFERENCE_CUDA_CAPTURE_GREEDY_TOKEN": "1",
    "ANTFLY_INFERENCE_CUDA_SERVER_REQUEST_GRAPH_RESET": "1",
    "ANTFLY_INFERENCE_CUDA_TEMP_ARENA_AUTOPLAN": "1",
    "ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD": "0",
    "ANTFLY_INFERENCE_CUDA_TEMP_SLOT_SKIP": "0",
    "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE": "0",
    "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK": "1",
    "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK_CONSUMER": "required-tiled64",
    "ANTFLY_INFERENCE_CUDA_Q4_0_LM_HEAD_Q8_1_ARGMAX": "1",
    "ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX": "0",
    "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN": "0",
    "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_CATALOG_FFN_CANDIDATES": "0",
    "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT": "0",
    "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY": "0",
    "ANTFLY_INFERENCE_DISABLE_CONTINUOUS_BATCHING": "1",
}
CUDA_EXECUTION_PROFILES = {
    CUDA_EXECUTION_PROFILE_DEFAULT: {
        "name": CUDA_PROFILE_NAME,
        "environment": CUDA_PROFILE_DEFAULT_ENV,
        "allow_enforced_experimental_prefill": False,
        "route_contract": {
            "prefill": {
                "profile": "required-fast",
                "route": "prefill-fast",
                "gemma4_head_dims": [256, 512],
            },
            # The consumer defaults to serial when its environment variable is
            # absent.  Keeping that default out of the historical profile
            # preserves compatibility with reviewed v3 evidence.
            "decode": {
                "configured": "serial",
                "consumer": "serial",
                "gemma4_head_dims": [256, 512],
                "routes_by_head_dim": {
                    "256": "gemma4_f16_local",
                    "512": "gemma4_f16_global",
                },
                "forbid_fallback": True,
            },
        },
    },
    "gemma4-e2b-sm89-warp-tiled64-v1": {
        "name": "antfly.gemma4_e2b_sm89_warp_tiled64_server.v1",
        "environment": {
            **CUDA_PROFILE_DEFAULT_ENV,
            "ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE": "required-tiled-f16-warp",
            "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK": "1",
            "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK_CONSUMER": "required-tiled64",
        },
        # Exact generated-token parity failed for this experimental route.
        # Keep the versioned profile available for deterministic measurement,
        # but do not permit it to satisfy an enforced performance gate.
        "allow_enforced_experimental_prefill": False,
        "route_contract": {
            "prefill": {
                "profile": "required-tiled-f16-warp",
                "route": "prefill-tiled-f16-warp",
                "gemma4_head_dims": [256, 512],
            },
            "decode": {
                "configured": "required-tiled64",
                "consumer": "tiled64",
                "gemma4_head_dims": [256, 512],
                "routes_by_head_dim": {
                    "256": "gemma4_f16_local",
                    "512": "gemma4_f16_global",
                },
                "forbid_fallback": True,
            },
        },
    },
    "gemma4-e2b-sm89-flash-tiled64-v1": {
        "name": "antfly.gemma4_e2b_sm89_flash_tiled64_server.v1",
        "environment": {
            **CUDA_PROFILE_DEFAULT_ENV,
            "ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE": "required-flash-f16-sm89",
            "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK": "1",
            "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK_CONSUMER": "required-tiled64",
        },
        # The standalone kernel is qualified, but production end-to-end exact
        # generated-token parity has not passed yet.  This remains collect-only
        # until that promotion gate is attested.
        "allow_enforced_experimental_prefill": False,
        "route_contract": {
            "prefill": {
                "profile": "required-flash-f16-sm89",
                "route": "prefill-flash-f16-sm89",
                "gemma4_head_dims": [256, 512],
                "gemma4_query_lengths": [3, 512],
                "forbid_fallback": True,
            },
            "decode": {
                "configured": "required-tiled64",
                "consumer": "tiled64",
                "gemma4_head_dims": [256, 512],
                "routes_by_head_dim": {
                    "256": "gemma4_f16_local",
                    "512": "gemma4_f16_global",
                },
                "forbid_fallback": True,
            },
        },
    },
    "gemma4-e2b-sm89-flash-splitk-v1": {
        "name": "antfly.gemma4_e2b_sm89_flash_splitk_online_server.v1",
        "environment": {
            **GEMMA4_E2B_SM89_FLASH_COMMON_ENV,
            "ANTFLY_INFERENCE_CUDA_GQA_DECODE_PROFILE": "required-splitk-online-sm89",
        },
        # Both kernels remain opt-in candidates until the locked 300-token
        # exactness and paired warm-server promotion gate is attested.
        "allow_enforced_experimental_prefill": False,
        "lock_environment": True,
        "route_contract": {
            "prefill": {
                "profile": "required-flash-f16-sm89",
                "route": "prefill-flash-f16-sm89",
                "gemma4_head_dims": [256, 512],
                "gemma4_query_lengths": [3, 512],
                "forbid_fallback": True,
            },
            "decode": {
                "profile": "required-splitk-online-sm89",
                "route": "decode-splitk-online-sm89",
                "gemma4_head_dims": [256, 512],
                "forbid_fallback": True,
            },
        },
    },
}
CUDA_GQA_PREFILL_LOG_MARKER = "cuda_gqa_prefill_profile:"
CUDA_GQA_SCORE_PREWORK_LOG_MARKER = "cuda_gqa_score_prework_consumer:"
CUDA_GQA_DECODE_LOG_MARKER = "cuda_gqa_decode_profile:"
CUDA_GQA_PREFILL_PROFILES = (
    "off",
    "fast",
    "required-fast",
    "tiled",
    "mma",
    "tiled-f16-exact",
    "required-tiled-f16-exact",
    "tiled-f16-warp",
    "required-tiled-f16-warp",
    "flash-f16-sm89",
    "required-flash-f16-sm89",
)
EXPERIMENTAL_CUDA_GQA_PREFILL_PROFILES = frozenset(
    {
        "tiled-f16-exact",
        "required-tiled-f16-exact",
        "tiled-f16-warp",
        "required-tiled-f16-warp",
        "flash-f16-sm89",
        "required-flash-f16-sm89",
    }
)
CUDA_SERVER_BUDGET_MB = {
    "host": 8192,
    "backend": 19_000,
    "combined": 28_000,
    "kv": 1024,
    "scratch": 2048,
}
CUDA_TIMING_INSTRUMENTATION_ENV = (
    "ANTFLY_INFERENCE_CUDA_PROFILE_PREFILL_OPS",
    "ANTFLY_INFERENCE_CUDA_PROFILE_DECODE",
)
MATERIAL_TUNING_ENV_PREFIXES = (
    "ANTFLY_",
    "TERMITE_",
    "CUDA_",
    "NVIDIA_",
    "GGML_",
    "LLAMA_",
    "CUBLAS_",
    "CUDNN_",
    "NCCL_",
    "NVRTC_",
    "NVTE_",
    "LD_",
    "OMP_",
    "KMP_",
    "GOMP_",
    "MKL_",
    "OPENBLAS_",
    "BLIS_",
    "MALLOC_",
    "GLIBC_",
    "JEMALLOC_",
    "TCMALLOC_",
)
ANTFLY_ENV_OVERRIDE_PREFIXES = (
    "ANTFLY_",
    "TERMITE_",
)
SENSITIVE_ENV_NAME_PARTS = ("SECRET", "PASSWORD", "CREDENTIAL", "API_KEY", "PRIVATE_KEY")
SENSITIVE_ENV_TOKEN_MARKERS = ("AUTH_TOKEN", "ACCESS_TOKEN", "API_TOKEN", "BEARER_TOKEN", "SESSION_TOKEN")
SERVER_MAX_IDLE_PREFILL_CHUNK_SIZE = 512
MIN_FROZEN_BASELINE_SAMPLES = 10
MAX_FROZEN_BASELINE_CV = 0.03
MATERIAL_SHARED_OBJECT_PREFIXES = (
    "libllama",
    "libggml",
    "libmtmd",
    "libcudart",
    "libcublas",
)
SYSTEM_LIBRARY_ROOTS = (
    pathlib.Path("/lib"),
    pathlib.Path("/lib64"),
    pathlib.Path("/usr/lib"),
    pathlib.Path("/usr/lib64"),
)
STABLE_GPU_QUERY_FIELDS = (
    "index",
    "uuid",
    "name",
    "driver_version",
    "compute_cap",
    "memory.total",
    "persistence_mode",
    "power.limit",
    "clocks.max.graphics",
    "clocks.max.memory",
    "clocks.applications.graphics",
    "clocks.applications.memory",
    "mig.mode.current",
)
GPU_NUMERIC_FIELDS = {
    "index": int,
    "compute_cap": float,
    "memory.total": int,
    "power.limit": float,
    "clocks.max.graphics": int,
    "clocks.max.memory": int,
    "clocks.applications.graphics": int,
    "clocks.applications.memory": int,
}
UNSAFE_MODEL_CONTROL_FILES = {
    "antfly_inference_bundle.json",
    "antfly_inference_variants.json",
    "model_manifest.json",
}


@dataclasses.dataclass(frozen=True)
class PromptFixture:
    path: pathlib.Path
    fixture_id: str
    user_content: str
    user_sha256: str
    user_utf8_bytes: int
    reference_prompt: str
    reference_prompt_sha256: str
    reference_prompt_utf8_bytes: int
    expected_prompt_tokens: int
    chat_template_kwargs: dict[str, bool]
    response_channel: str
    expected_output_substrings: tuple[str, ...]
    minimum_visible_utf8_bytes: int
    file_sha256: str


@dataclasses.dataclass(frozen=True)
class ServerSpec:
    engine: str
    command: tuple[str, ...]
    health_path: str
    generate_path: str


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: pathlib.Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def canonical_sha256(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return sha256_bytes(encoded)


def is_material_tuning_env(name: str) -> bool:
    return any(name.startswith(prefix) for prefix in MATERIAL_TUNING_ENV_PREFIXES)


def is_antfly_env_override(name: str) -> bool:
    return any(name.startswith(prefix) for prefix in ANTFLY_ENV_OVERRIDE_PREFIXES)


def is_sensitive_environment_name(name: str) -> bool:
    normalized = name.upper()
    return any(part in normalized for part in SENSITIVE_ENV_NAME_PARTS + SENSITIVE_ENV_TOKEN_MARKERS)


def parse_antfly_env_override(raw: str) -> tuple[str, str]:
    name, separator, value = raw.partition("=")
    if not separator or not name or not is_antfly_env_override(name):
        raise ValueError(
            "--antfly-env must be NAME=VALUE for an ANTFLY_* or TERMITE_* runtime setting"
        )
    if any(character in raw for character in ("\0", "\r", "\n")):
        raise ValueError("--antfly-env contains an unsupported control character")
    return name, value


def validate_cuda_profile_value(name: str, value: str) -> None:
    if name == "ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY" and value not in {"off", "auto", "required"}:
        raise ValueError(f"{name} must be off, auto, or required")
    if name == "ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE" and value not in CUDA_GQA_PREFILL_PROFILES:
        raise ValueError(f"{name} must be one of: {', '.join(CUDA_GQA_PREFILL_PROFILES)}")
    if name == "ANTFLY_INFERENCE_CUDA_GQA_DECODE_PROFILE" and value not in {
        "off", "splitk-online-sm89", "required-splitk-online-sm89",
    }:
        raise ValueError(
            f"{name} must be off, splitk-online-sm89, or required-splitk-online-sm89"
        )
    if name in {
        "ANTFLY_INFERENCE_CUDA_TEMP_ARENA_AUTOPLAN",
        "ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN",
        "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK",
    } and value not in {"0", "1"}:
        raise ValueError(f"{name} must be 0 or 1")
    if (
        name == "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK_CONSUMER"
        and value not in {"serial", "tiled64", "required-tiled64"}
    ):
        raise ValueError(f"{name} must be serial, tiled64, or required-tiled64")
    if name in {
        "ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS",
        "ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY",
    }:
        try:
            parsed = int(value)
        except ValueError as exc:
            raise ValueError(f"{name} must be a non-negative integer") from exc
        if parsed < 0 or str(parsed) != value:
            raise ValueError(f"{name} must be a canonical non-negative integer")


def resolve_antfly_execution_profile(
    args: argparse.Namespace,
    fixture: PromptFixture,
    environment: dict[str, str],
) -> dict[str, Any]:
    inherited_material_env = {
        name: value for name, value in sorted(environment.items()) if is_material_tuning_env(name)
    }
    prefix_sha256 = {
        "antfly": sha256_bytes(args.antfly_server_prefix.encode("utf-8")),
        "llama_cpp": sha256_bytes(args.llama_server_prefix.encode("utf-8")),
    }
    server_budget_mb = antfly_server_budget_mb(args)
    if args.backend != "cuda":
        antfly_effective_env = dict(inherited_material_env)
        material_environment = {
            "antfly_effective_env": antfly_effective_env,
            "llama_cpp_inherited_env": inherited_material_env,
        }
        hashed_value = {
            "name": "none",
            "effective_env": antfly_effective_env,
            "llama_cpp_inherited_env": inherited_material_env,
            "material_environment_sha256": canonical_sha256(material_environment),
            "server_max_idle_prefill_chunk_size": SERVER_MAX_IDLE_PREFILL_CHUNK_SIZE,
            "server_budget_mb": server_budget_mb,
            "server_prefix_sha256": prefix_sha256,
        }
        return {
            **hashed_value,
            "sources": {name: "process_environment" for name in inherited_material_env},
            "sha256": canonical_sha256(hashed_value),
        }

    profile_key = getattr(args, "cuda_execution_profile", CUDA_EXECUTION_PROFILE_DEFAULT)
    try:
        profile_spec = CUDA_EXECUTION_PROFILES[profile_key]
    except KeyError as exc:
        raise ValueError(f"unknown CUDA execution profile: {profile_key!r}") from exc
    capacity = ((fixture.expected_prompt_tokens + args.output_tokens + 32 + 31) // 32) * 32
    defaults = {
        **profile_spec["environment"],
        "ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY": str(capacity),
    }
    effective = dict(defaults)
    locked_environment = bool(profile_spec.get("lock_environment"))
    default_source = "reviewed_versioned_profile" if locked_environment else "model_neutral_default"
    sources = {name: default_source for name in defaults}
    for name, value in inherited_material_env.items():
        if locked_environment and is_antfly_env_override(name):
            if name not in defaults:
                raise ValueError(
                    f"locked CUDA execution profile forbids unreviewed environment variable {name}"
                )
            if value != defaults[name]:
                raise ValueError(
                    f"locked CUDA execution profile forbids overriding {name}: "
                    f"expected {defaults[name]!r}, observed {value!r} from process environment"
                )
            continue
        effective[name] = value
        sources[name] = "process_environment"
    for raw in args.antfly_env:
        name, value = parse_antfly_env_override(raw)
        if locked_environment:
            if name not in defaults:
                raise ValueError(
                    f"locked CUDA execution profile forbids unreviewed environment variable {name}"
                )
            if value != defaults[name]:
                raise ValueError(
                    f"locked CUDA execution profile forbids overriding {name}: "
                    f"expected {defaults[name]!r}, observed {value!r} from command line"
                )
            continue
        effective[name] = value
        sources[name] = "command_line"
    for name, value in effective.items():
        validate_cuda_profile_value(name, value)
    gqa_prefill_profile = effective.get("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE")
    if (
        gqa_prefill_profile in EXPERIMENTAL_CUDA_GQA_PREFILL_PROFILES
        and getattr(args, "enforce_performance", False)
        and not profile_spec["allow_enforced_experimental_prefill"]
    ):
        raise ValueError(
            f"experimental CUDA GQA prefill profile {gqa_prefill_profile!r} is collect-only"
        )
    material_environment = {
        "antfly_effective_env": dict(sorted(effective.items())),
        "llama_cpp_inherited_env": inherited_material_env,
    }
    hashed_value = {
        "name": profile_spec["name"],
        "profile_key": profile_key,
        "route_contract": profile_spec["route_contract"],
        "effective_env": material_environment["antfly_effective_env"],
        "llama_cpp_inherited_env": material_environment["llama_cpp_inherited_env"],
        "material_environment_sha256": canonical_sha256(material_environment),
        "server_max_idle_prefill_chunk_size": SERVER_MAX_IDLE_PREFILL_CHUNK_SIZE,
        "server_budget_mb": server_budget_mb,
        "server_prefix_sha256": prefix_sha256,
    }
    return {
        **hashed_value,
        "sources": dict(sorted(sources.items())),
        "sha256": canonical_sha256(hashed_value),
    }


def validate_headline_execution_profile(
    args: argparse.Namespace,
    fixture: PromptFixture,
    profile: dict[str, Any],
) -> None:
    if args.profile != "headline" or not args.enforce_performance:
        return
    capacity = ((fixture.expected_prompt_tokens + args.output_tokens + 32 + 31) // 32) * 32
    profile_key = profile.get("profile_key", CUDA_EXECUTION_PROFILE_DEFAULT)
    try:
        profile_spec = CUDA_EXECUTION_PROFILES[profile_key]
    except KeyError as exc:
        raise ValueError(f"unknown CUDA execution profile in resolved profile: {profile_key!r}") from exc
    required = {
        **profile_spec["environment"],
        "ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY": str(capacity),
    }
    effective = profile.get("effective_env") or {}
    for name, expected in required.items():
        if effective.get(name) != expected:
            raise ValueError(
                f"headline execution profile requires {name}={expected}, observed {effective.get(name)!r}"
            )
    legacy_period = effective.get("ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD")
    if legacy_period not in {None, "0"}:
        raise ValueError("headline execution profile forbids a legacy fixed CUDA temp-slot period")
    for name in CUDA_TIMING_INSTRUMENTATION_ENV:
        if effective.get(name) not in {None, "0"}:
            raise ValueError(
                f"headline execution profile forbids synchronization-heavy timing instrumentation: {name}"
            )
    if profile.get("server_budget_mb") != CUDA_SERVER_BUDGET_MB:
        raise ValueError(
            "headline execution profile requires the locked CUDA server budgets: "
            f"expected={CUDA_SERVER_BUDGET_MB!r} observed={profile.get('server_budget_mb')!r}"
        )


def antfly_server_budget_mb(args: argparse.Namespace) -> dict[str, int]:
    backend = getattr(args, "backend", "cuda")
    result: dict[str, int] = {}
    for name, default in CUDA_SERVER_BUDGET_MB.items():
        configured = getattr(args, f"antfly_{name}_budget_mb", None)
        result[name] = int(default if configured is None and backend == "cuda" else configured or 0)
    return result


def execution_profile_provenance(profile: dict[str, Any]) -> dict[str, Any]:
    """Return a serializable profile without disclosing credential-like values."""

    def public_env(environment: dict[str, str]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for name, value in environment.items():
            if is_sensitive_environment_name(name):
                result[name] = {"redacted": True, "sha256": sha256_bytes(value.encode("utf-8"))}
            else:
                result[name] = value
        return result

    value = dict(profile)
    value["effective_env"] = public_env(profile["effective_env"])
    value["llama_cpp_inherited_env"] = public_env(profile["llama_cpp_inherited_env"])
    return value


def load_fixture(path: pathlib.Path) -> PromptFixture:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"could not read prompt fixture {path}: {exc}") from exc
    if raw.get("schema") != FIXTURE_SCHEMA:
        raise ValueError(f"unsupported prompt fixture schema: {raw.get('schema')!r}")
    segment = raw.get("segment")
    suffix = raw.get("suffix")
    repeat = raw.get("repeat")
    if not isinstance(segment, str) or not isinstance(suffix, str):
        raise ValueError("prompt fixture segment and suffix must be strings")
    if isinstance(repeat, bool) or not isinstance(repeat, int) or repeat < 1:
        raise ValueError("prompt fixture repeat must be a positive integer")
    user_content = segment * repeat + suffix
    user_encoded = user_content.encode("utf-8")
    user_sha = sha256_bytes(user_encoded)
    if len(user_encoded) != raw.get("expected_user_utf8_bytes") or user_sha != raw.get("expected_user_sha256"):
        raise ValueError("prompt fixture rendered user content does not match its byte/hash contract")
    prefix = raw.get("reference_chat_prefix")
    reference_suffix = raw.get("reference_chat_suffix")
    if not isinstance(prefix, str) or not isinstance(reference_suffix, str):
        raise ValueError("prompt fixture reference chat prefix/suffix must be strings")
    if prefix != REFERENCE_CHAT_PREFIX or reference_suffix != REFERENCE_PUBLIC_FINAL_SUFFIX:
        raise ValueError("prompt fixture must use the versioned public-final Gemma 4 chat contract")
    chat_template_kwargs = raw.get("chat_template_kwargs")
    response_channel = raw.get("response_channel")
    if chat_template_kwargs != CHAT_TEMPLATE_KWARGS or response_channel != "public_final":
        raise ValueError("prompt fixture chat-template policy must explicitly select the public final channel")
    expected_output_substrings = raw.get("expected_output_substrings")
    if (
        not isinstance(expected_output_substrings, list)
        or not expected_output_substrings
        or any(not isinstance(value, str) or not value for value in expected_output_substrings)
    ):
        raise ValueError("prompt fixture expected_output_substrings must be a non-empty string list")
    minimum_visible_utf8_bytes = raw.get("minimum_visible_utf8_bytes")
    if (
        isinstance(minimum_visible_utf8_bytes, bool)
        or not isinstance(minimum_visible_utf8_bytes, int)
        or minimum_visible_utf8_bytes < 1
    ):
        raise ValueError("prompt fixture minimum_visible_utf8_bytes must be a positive integer")
    reference_prompt = prefix + user_content + reference_suffix
    reference = reference_prompt.encode("utf-8")
    reference_sha = sha256_bytes(reference)
    if (
        len(reference) != raw.get("expected_reference_prompt_utf8_bytes")
        or reference_sha != raw.get("expected_reference_prompt_sha256")
    ):
        raise ValueError("prompt fixture reference prompt does not match its byte/hash contract")
    expected_tokens = raw.get("expected_reference_prompt_tokens")
    if isinstance(expected_tokens, bool) or not isinstance(expected_tokens, int) or expected_tokens < 1:
        raise ValueError("prompt fixture expected token count must be a positive integer")
    return PromptFixture(
        path=path.resolve(),
        fixture_id=str(raw.get("id") or path.stem),
        user_content=user_content,
        user_sha256=user_sha,
        user_utf8_bytes=len(user_encoded),
        reference_prompt=reference_prompt,
        reference_prompt_sha256=reference_sha,
        reference_prompt_utf8_bytes=len(reference),
        expected_prompt_tokens=expected_tokens,
        chat_template_kwargs=dict(chat_template_kwargs),
        response_channel=response_channel,
        expected_output_substrings=tuple(expected_output_substrings),
        minimum_visible_utf8_bytes=minimum_visible_utf8_bytes,
        file_sha256=sha256_file(path),
    )


def choose_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def paired_order(index: int) -> tuple[str, str]:
    return balanced_pair_order(index, ENGINES[0], ENGINES[1])


def percentile(values: Iterable[float], probability: float) -> float:
    return paired_percentile(values, probability)


def distribution(values: Iterable[float]) -> dict[str, float]:
    return paired_distribution(values)


def paired_bootstrap_ratio_ci(
    rows: list[dict[str, Any]],
    *,
    samples: int,
    seed: int,
    metric: str = "total_latency_ms",
) -> dict[str, float | int]:
    if not rows:
        raise ValueError("paired bootstrap requires rows")
    if samples < 1:
        raise ValueError("paired bootstrap sample count must be positive")
    random_source = random.Random(seed)
    ratios: list[float] = []
    for _ in range(samples):
        selected = [rows[random_source.randrange(len(rows))] for _ in rows]
        antfly = statistics.median(float(row["antfly"][metric]) for row in selected)
        llama = statistics.median(float(row["llama_cpp"][metric]) for row in selected)
        if llama <= 0:
            raise ValueError("llama.cpp bootstrap metric must be positive")
        ratios.append(antfly / llama)
    return {
        "lower_95": percentile(ratios, 0.025),
        "median": statistics.median(ratios),
        "upper_95": percentile(ratios, 0.975),
        "samples": samples,
        "seed": seed,
    }


def parse_sse_lines(lines: Iterable[bytes]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for raw_line in lines:
        text = raw_line.decode("utf-8", errors="strict").strip()
        if not text or text.startswith(":") or text.startswith("event:"):
            continue
        payload = text[5:].strip() if text.startswith("data:") else text
        if payload == "[DONE]":
            break
        try:
            value = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid streaming JSON event: {payload[:200]}") from exc
        if isinstance(value, dict) and value.get("error"):
            raise RuntimeError(f"streaming API error: {value['error']}")
        if isinstance(value, dict):
            events.append(value)
    return events


def is_event_stream_content_type(value: str | None) -> bool:
    return isinstance(value, str) and value.partition(";")[0].strip().lower() == "text/event-stream"


def event_delta(event: dict[str, Any]) -> tuple[bool, str, str | None]:
    choices = event.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        return False, "", None
    choice = choices[0]
    delta = choice.get("delta")
    has_content = isinstance(delta, dict) and "content" in delta and delta.get("content") is not None
    content = str(delta.get("content") or "") if isinstance(delta, dict) else ""
    # OpenAI-compatible servers may emit an initial role chunk with empty
    # content. It is not a generated token and must not become TTFT.
    has_token = has_content and content != ""
    finish_reason = choice.get("finish_reason")
    return has_token, content, str(finish_reason) if finish_reason is not None else None


def stream_completion_accounting(events: list[dict[str, Any]], content_event_count: int) -> tuple[int, str]:
    for event in reversed(events):
        usage = event.get("usage")
        if not isinstance(usage, dict) or usage.get("completion_tokens") is None:
            continue
        value = usage["completion_tokens"]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("stream usage completion_tokens must be a non-negative integer")
        return value, "final_sse_usage"
    if content_event_count < 0:
        raise ValueError("content event count must be non-negative")
    return content_event_count, "content_events"


def stream_prompt_accounting(events: list[dict[str, Any]]) -> tuple[int | None, str]:
    for event in reversed(events):
        usage = event.get("usage")
        if not isinstance(usage, dict) or usage.get("prompt_tokens") is None:
            continue
        value = usage["prompt_tokens"]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("stream usage prompt_tokens must be a non-negative integer")
        return value, "final_sse_usage"
    return None, "warmup_usage_fallback"


def nonstream_summary(response: dict[str, Any]) -> dict[str, Any]:
    choices = response.get("choices")
    usage = response.get("usage")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        raise ValueError("completion response is missing choices[0]")
    if not isinstance(usage, dict):
        raise ValueError("completion response is missing usage")
    choice = choices[0]
    message = choice.get("message") if isinstance(choice.get("message"), dict) else {}
    content = str(message.get("content", choice.get("text", "")) or "")
    content_bytes = content.encode("utf-8")
    return {
        "prompt_tokens": int(usage.get("prompt_tokens") or 0),
        "completion_tokens": int(usage.get("completion_tokens") or 0),
        "finish_reason": choice.get("finish_reason"),
        "content": content,
        "content_utf8_bytes": len(content_bytes),
        "content_sha256": sha256_bytes(content_bytes),
    }


def request_body(
    engine: str,
    model: pathlib.Path | str,
    backend: str,
    fixture: PromptFixture,
    output_tokens: int,
    cache_dtype: str,
    *,
    stream: bool,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "model": str(model),
        "messages": [{"role": "user", "content": fixture.user_content}],
        "chat_template_kwargs": dict(fixture.chat_template_kwargs),
        "max_tokens": output_tokens,
        "temperature": 0,
        "top_p": 1,
        "stream": stream,
    }
    if engine == "antfly":
        body.update({
            "backend": backend,
            "cache_dtype": cache_dtype,
            "ignore_eos": True,
            "prompt_cache": False,
        })
        if stream:
            body["stream_options"] = {"include_usage": True}
    elif engine == "llama_cpp":
        body.update({"ignore_eos": True, "seed": 0, "cache_prompt": False})
        if stream:
            body["stream_options"] = {"include_usage": True}
    else:
        raise ValueError(f"unknown engine: {engine}")
    return body


def antfly_model_identifier(model: pathlib.Path, models_dir: pathlib.Path) -> str:
    """Return the server-safe directory identifier selecting exactly ``model``."""
    resolved_model = model.resolve()
    resolved_models_dir = models_dir.resolve()
    try:
        relative_file = resolved_model.relative_to(resolved_models_dir)
    except ValueError as exc:
        raise ValueError(
            f"model must resolve within models-dir for Antfly server loading: {resolved_model}"
        ) from exc
    relative_dir = relative_file.parent
    if relative_dir == pathlib.Path("."):
        raise ValueError("benchmark GGUF must be inside a dedicated model directory below models-dir")

    def is_decoder_gguf(path: pathlib.Path) -> bool:
        name = path.name
        if not name.endswith(".gguf"):
            return False
        stem = name[:-5]
        is_projector = (
            stem == "mmproj"
            or stem.startswith("mmproj-")
            or stem.startswith("mmproj_")
            or stem.endswith("-mmproj")
            or stem.endswith("_mmproj")
        )
        is_gliner_head = (
            name in {"gliner_head.gguf", "gliner2-head.gguf"}
            or (name.startswith("gliner2-head.") and name.endswith(".gguf"))
        )
        return not is_projector and not is_gliner_head

    decoder_ggufs = sorted(
        path.resolve()
        for path in resolved_model.parent.iterdir()
        if path.is_file() and is_decoder_gguf(path)
    )
    if decoder_ggufs != [resolved_model]:
        choices = ", ".join(str(path) for path in decoder_ggufs) or "none"
        raise ValueError(
            "Antfly model directory must contain exactly the benchmark decoder GGUF; "
            f"found: {choices}"
        )
    return relative_dir.as_posix()


class ManagedServer:
    def __init__(
        self,
        spec: ServerSpec,
        log_path: pathlib.Path,
        environment: dict[str, str],
        startup_timeout: float,
        request_timeout: float,
    ):
        self.spec = spec
        self.log_path = log_path
        self.environment = environment
        self.startup_timeout = startup_timeout
        self.request_timeout = request_timeout
        self.process: subprocess.Popen[bytes] | None = None
        self.log = None
        self.connection: http.client.HTTPConnection | None = None

    def __enter__(self) -> "ManagedServer":
        self.log = self.log_path.open("wb")
        try:
            self.process = subprocess.Popen(
                list(self.spec.command),
                stdout=self.log,
                stderr=subprocess.STDOUT,
                env=self.environment,
                start_new_session=True,
            )
        except OSError:
            self.log.close()
            self.log = None
            raise
        health_url = f"http://127.0.0.1:{self.port}{self.spec.health_path}"
        deadline = time.monotonic() + self.startup_timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                returncode = self.process.returncode
                self.__exit__()
                raise RuntimeError(f"{self.spec.engine} server exited with {returncode}; see {self.log_path}")
            try:
                with urllib.request.urlopen(health_url, timeout=1.0) as response:
                    if 200 <= response.status < 300:
                        self.connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=self.request_timeout)
                        return self
            except (OSError, urllib.error.URLError):
                time.sleep(0.25)
        self.__exit__()
        raise RuntimeError(f"{self.spec.engine} server did not become ready; see {self.log_path}")

    @property
    def port(self) -> int:
        command = self.spec.command
        try:
            return int(command[command.index("--port") + 1])
        except (ValueError, IndexError) as exc:
            raise RuntimeError(f"server command has no --port: {shlex.join(command)}") from exc

    def __exit__(self, *_: object) -> None:
        if self.connection is not None:
            self.connection.close()
        if self.process is not None and self.process.poll() is None:
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self.process.wait(timeout=5)
        if self.log is not None:
            self.log.close()

    def post_json(
        self,
        body: dict[str, Any],
        *,
        path: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if self.connection is None:
            raise RuntimeError("server connection is not initialized")
        payload = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        reused = self.connection.sock is not None
        started = time.monotonic_ns()
        self.connection.request(
            "POST",
            path or self.spec.generate_path,
            body=payload,
            headers={"content-type": "application/json", "accept": "application/json"},
        )
        response = self.connection.getresponse()
        raw = response.read()
        elapsed_ms = (time.monotonic_ns() - started) / 1_000_000.0
        if response.status != 200:
            raise RuntimeError(f"{self.spec.engine} HTTP {response.status}: {raw.decode(errors='replace')[:1000]}")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise RuntimeError(f"{self.spec.engine} returned a non-object JSON response")
        return value, {"total_latency_ms": elapsed_ms, "connection_reused": reused}

    def post_nonstream(self, body: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        return self.post_json(body)

    def post_stream(self, body: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if self.connection is None:
            raise RuntimeError("server connection is not initialized")
        payload = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        reused = self.connection.sock is not None
        started = time.monotonic_ns()
        self.connection.request(
            "POST",
            self.spec.generate_path,
            body=payload,
            headers={"content-type": "application/json", "accept": "text/event-stream"},
        )
        response = self.connection.getresponse()
        if response.status != 200:
            raw = response.read()
            raise RuntimeError(f"{self.spec.engine} HTTP {response.status}: {raw.decode(errors='replace')[:1000]}")
        response_content_type = response.getheader("content-type")
        if not is_event_stream_content_type(response_content_type):
            raw = response.read()
            raise RuntimeError(
                f"{self.spec.engine} streaming request returned content-type "
                f"{response_content_type!r}, expected text/event-stream: {raw.decode(errors='replace')[:1000]}"
            )
        events: list[dict[str, Any]] = []
        token_fragments: list[str] = []
        finish_reason: str | None = None
        first_token_ns: int | None = None
        last_token_ns: int | None = None
        terminal_usage_ns: int | None = None
        for raw_line in response:
            parsed = parse_sse_lines([raw_line])
            for event in parsed:
                event_received_ns = time.monotonic_ns()
                event["_benchmark_arrival_ms"] = (event_received_ns - started) / 1_000_000.0
                events.append(event)
                usage = event.get("usage")
                if isinstance(usage, dict) and usage.get("completion_tokens") is not None:
                    terminal_usage_ns = event_received_ns
                has_token, content, event_finish = event_delta(event)
                if has_token:
                    token_received_ns = event_received_ns
                    if first_token_ns is None:
                        first_token_ns = token_received_ns
                    last_token_ns = token_received_ns
                    token_fragments.append(content)
                if event_finish is not None:
                    finish_reason = event_finish
        if first_token_ns is None or last_token_ns is None:
            raise RuntimeError(f"{self.spec.engine} stream emitted no non-empty visible content")
        response_finished_ns = time.monotonic_ns()
        total_ms = (response_finished_ns - started) / 1_000_000.0
        ttft_ms = (first_token_ns - started) / 1_000_000.0
        content_event_count = len(token_fragments)
        visible_content = "".join(token_fragments)
        visible_content_bytes = visible_content.encode("utf-8")
        if not visible_content_bytes:
            raise RuntimeError(f"{self.spec.engine} stream emitted empty visible content")
        token_count, accounting_source = stream_completion_accounting(events, content_event_count)
        prompt_token_count, prompt_accounting_source = stream_prompt_accounting(events)
        decode_end_ns = terminal_usage_ns if accounting_source == "final_sse_usage" else last_token_ns
        decode_ms = (decode_end_ns - first_token_ns) / 1_000_000.0
        visible_content_ms = (last_token_ns - first_token_ns) / 1_000_000.0
        stream_tail_ms = (response_finished_ns - decode_end_ns) / 1_000_000.0
        return {
            "total_latency_ms": total_ms,
            "ttft_ms": ttft_ms,
            "decode_ms": decode_ms,
            "stream_tail_ms": stream_tail_ms,
            "visible_content_ms": visible_content_ms,
            "inter_token_ms": decode_ms / (token_count - 1) if token_count > 1 else 0.0,
            "decode_tok_s": (token_count - 1) * 1000.0 / decode_ms if token_count > 1 and decode_ms > 0 else 0.0,
            "completion_tokens": token_count,
            "completion_token_accounting_source": accounting_source,
            "prompt_tokens": prompt_token_count,
            "prompt_token_accounting_source": prompt_accounting_source,
            "content_event_count": content_event_count,
            "content": visible_content,
            "content_utf8_bytes": len(visible_content_bytes),
            "finish_reason": finish_reason,
            "content_sha256": sha256_bytes(visible_content_bytes),
            "content_event_sha256": canonical_sha256(token_fragments),
            "connection_reused": reused,
            "response_content_type": response_content_type,
        }, events


def make_server_spec(args: argparse.Namespace, engine: str, port: int, run_dir: pathlib.Path) -> ServerSpec:
    if engine == "antfly":
        config_path = run_dir / "antfly-server.json"
        write_json(config_path, {
            "models_dir": str(args.models_dir.resolve()),
            "admission": {"inference": {"max_concurrent_requests": 1}},
            "generation_batching": {
                "mode": "off",
                "max_step_items": 1,
                "max_step_query_tokens": 512,
                "max_idle_prefill_chunk_size": SERVER_MAX_IDLE_PREFILL_CHUNK_SIZE,
            },
        })
        command = [
            str(args.antfly_bin.resolve()), "run", "--host", "127.0.0.1", "--port", str(port),
            "--models-dir", str(args.models_dir.resolve()), "--config", str(config_path),
        ]
        for name, value in antfly_server_budget_mb(args).items():
            if value > 0:
                command.extend((f"--{name}-budget-mb", str(value)))
        if args.antfly_server_prefix:
            command = shlex.split(args.antfly_server_prefix) + command
        return ServerSpec(engine, tuple(command), "/healthz", "/ai/v1/generate")
    if engine == "llama_cpp":
        template_path = run_dir / "gemma4-public-final-chat-template.jinja"
        template_path.write_text(LLAMA_CHAT_TEMPLATE, encoding="utf-8")
        command = [
            str(args.llama_server_bin.resolve()), "-m", str(args.model.resolve()),
            "--host", "127.0.0.1", "--port", str(port), "-c", str(args.context_size),
            "-b", "512", "-ub", "512", "-ngl", "999",
            "-ctk", args.cache_dtype, "-ctv", args.cache_dtype,
            "--jinja", "--chat-template-file", str(template_path.resolve()),
        ]
        if args.llama_server_prefix:
            command = shlex.split(args.llama_server_prefix) + command
        return ServerSpec(engine, tuple(command), "/health", "/v1/chat/completions")
    raise ValueError(f"unknown engine: {engine}")


def token_id_summary(token_ids: list[int]) -> dict[str, Any]:
    if not token_ids:
        raise ValueError("token ID sequence must not be empty")
    if any(isinstance(value, bool) or not isinstance(value, int) for value in token_ids):
        raise ValueError("token ID sequence contains a non-integer value")
    return {
        "count": len(token_ids),
        "sha256": canonical_sha256(token_ids),
        "first_16": token_ids[:16],
        "last_16": token_ids[-16:],
    }


def validate_llama_rendered_prompt(rendered_prompt: object, fixture: PromptFixture) -> dict[str, Any]:
    if not isinstance(rendered_prompt, str):
        raise RuntimeError("llama.cpp /apply-template response is missing a string prompt")
    encoded = rendered_prompt.encode("utf-8")
    summary = {
        "source": "llama_cpp_apply_template",
        "utf8_bytes": len(encoded),
        "sha256": sha256_bytes(encoded),
    }
    if (
        summary["utf8_bytes"] != fixture.reference_prompt_utf8_bytes
        or summary["sha256"] != fixture.reference_prompt_sha256
    ):
        raise RuntimeError(
            "llama.cpp measured chat template differs from the locked reference prompt: "
            f"bytes={summary['utf8_bytes']} expected_bytes={fixture.reference_prompt_utf8_bytes} "
            f"sha256={summary['sha256']} expected_sha256={fixture.reference_prompt_sha256}"
        )
    return summary


def collect_antfly_prompt_token_contract(
    args: argparse.Namespace,
    fixture: PromptFixture,
) -> dict[str, Any]:
    command = [
        str(args.antfly_bin.resolve()), "generate", str(args.model.resolve().parent), fixture.user_content,
        "--backend", args.backend, "--max-tokens", "0", "--temperature", "0",
        "--cache-dtype", getattr(args, "cache_dtype", "f16"),
        "--prefill-chunk-size", str(SERVER_MAX_IDLE_PREFILL_CHUNK_SIZE),
        "--disable-thinking", "--print-chat-template-status", "--print-prompt", "--print-prompt-token-ids",
    ]
    # The token-contract preflight initializes the same model/runtime as the
    # measured server even though it emits no completion tokens. Keep its KV
    # storage and memory envelope identical so a required, fail-closed CUDA
    # execution profile cannot be rejected before prompt identity is checked.
    for name, value in antfly_server_budget_mb(args).items():
        if value > 0:
            command.extend((f"--{name}-budget-mb", str(value)))
    if args.antfly_server_prefix:
        command = shlex.split(args.antfly_server_prefix) + command
    environment = os.environ.copy()
    environment.update(args.antfly_execution_profile["effective_env"])
    try:
        completed = subprocess.run(
            command,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=args.request_timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"Antfly prompt-token preflight failed to run: {exc}") from exc
    log_path = args.output_dir / "antfly_prompt_tokens.log"
    log_path.write_text(completed.stdout or "", encoding="utf-8")
    if completed.returncode != 0:
        raise RuntimeError(f"Antfly prompt-token preflight exited {completed.returncode}; see {log_path}")
    output = completed.stdout or ""
    if "chat_template=true" not in output.splitlines():
        raise RuntimeError(f"Antfly prompt-token preflight did not apply a chat template; see {log_path}")
    prompt_marker = "prompt:\n"
    token_marker = "\nprompt_token_ids:"
    prompt_start = output.find(prompt_marker)
    prompt_end = output.find(token_marker, prompt_start + len(prompt_marker))
    if prompt_start < 0 or prompt_end < 0:
        raise RuntimeError(f"Antfly prompt-token preflight emitted no rendered prompt; see {log_path}")
    serialized_prompt = output[prompt_start + len(prompt_marker) : prompt_end]
    serialized_bytes = serialized_prompt.encode("utf-8")
    if not serialized_prompt.startswith(TOKENIZER_BOS_TEXT):
        raise RuntimeError(
            "Antfly measured Gemma 4 prompt is missing its tokenizer-added BOS prefix; "
            f"see {log_path}"
        )
    rendered_prompt = serialized_prompt[len(TOKENIZER_BOS_TEXT) :]
    rendered_bytes = rendered_prompt.encode("utf-8")
    rendered_summary = {
        "source": "antfly_cli_chat_renderer_shared_with_server",
        "utf8_bytes": len(rendered_bytes),
        "sha256": sha256_bytes(rendered_bytes),
    }
    if (
        rendered_summary["utf8_bytes"] != fixture.reference_prompt_utf8_bytes
        or rendered_summary["sha256"] != fixture.reference_prompt_sha256
    ):
        raise RuntimeError(
            "Antfly measured chat template differs from the locked reference prompt: "
            f"bytes={rendered_summary['utf8_bytes']} expected_bytes={fixture.reference_prompt_utf8_bytes} "
            f"sha256={rendered_summary['sha256']} expected_sha256={fixture.reference_prompt_sha256}"
        )
    token_line = None
    for line in output.splitlines():
        if line.startswith("prompt_token_ids:"):
            token_line = line.partition(":")[2].strip()
    if token_line is None:
        raise RuntimeError(f"Antfly prompt-token preflight emitted no prompt_token_ids; see {log_path}")
    try:
        token_ids = [int(value) for value in token_line.split()]
    except ValueError as exc:
        raise RuntimeError(f"Antfly prompt-token preflight emitted invalid token IDs; see {log_path}") from exc
    return {
        **token_id_summary(token_ids),
        "source": "antfly_cli_rendered_public_channel_prompt",
        "template": rendered_summary,
        "serialized_prompt": {
            "source": "antfly_cli_prompt_after_tokenizer_special_prefix",
            "utf8_bytes": len(serialized_bytes),
            "sha256": sha256_bytes(serialized_bytes),
            "automatic_prefix": TOKENIZER_BOS_TEXT,
        },
        "model_directory": str(args.model.resolve().parent),
        "command": command,
        "log": str(log_path),
    }


def parse_key_value_log_events(log_path: pathlib.Path, marker: str) -> list[dict[str, str]]:
    """Extract one stable key/value event family from a structured server log."""

    events: list[dict[str, str]] = []
    for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
        marker_index = line.find(marker)
        if marker_index < 0:
            continue
        fields: dict[str, str] = {}
        payload = line[marker_index + len(marker) :].strip()
        for token in payload.split():
            name, separator, value = token.partition("=")
            if separator and name and value:
                # Structured Zig logs wrap the message in JSON, so the last
                # key/value token can carry the closing quote and brace.
                fields[name] = value.rstrip('\",}')
        if fields:
            events.append(fields)
    return events


def parse_cuda_gqa_prefill_telemetry(log_path: pathlib.Path) -> dict[str, Any]:
    """Extract stable key/value route evidence from an Antfly server log."""

    events = parse_key_value_log_events(log_path, CUDA_GQA_PREFILL_LOG_MARKER)

    active_events = [event for event in events if event.get("status") == "active"]
    fallback_events = [event for event in events if event.get("status") == "fallback"]
    rejected_events = [event for event in events if event.get("status") == "rejected"]
    required_fast_events = [
        event for event in active_events
        if event.get("profile") == "required-fast" and event.get("route") == "prefill-fast"
    ]
    required_fast_gemma4_geometries = sorted({
        int(event["head_dim"])
        for event in required_fast_events
        if event.get("num_heads") == "8"
        and event.get("num_kv_heads") == "1"
        and event.get("head_dim", "").isdigit()
        and int(event["head_dim"]) in {256, 512}
    })
    route_keys = sorted({
        (event.get("profile", ""), event.get("route", ""))
        for event in active_events
        if event.get("profile") and event.get("route")
    })
    route_observations = []
    for profile, route in route_keys:
        matching = [
            event for event in active_events
            if event.get("profile") == profile and event.get("route") == route
        ]
        route_observations.append({
            "profile": profile,
            "route": route,
            "active_count": len(matching),
            "gemma4_head_dims": sorted({
                int(event["head_dim"])
                for event in matching
                if event.get("num_heads") == "8"
                and event.get("num_kv_heads") == "1"
                and event.get("head_dim", "").isdigit()
                and int(event["head_dim"]) in {256, 512}
            }),
            "gemma4_query_lengths": sorted({
                int(event["q_seq_len"])
                for event in matching
                if event.get("q_seq_len", "").isdigit()
                and int(event["q_seq_len"]) in {3, 512}
            }),
        })
    return {
        "events": events,
        "configured_profiles": sorted({
            event["profile"] for event in events
            if event.get("status") == "configured" and "profile" in event
        }),
        "active_routes": sorted({event["route"] for event in active_events if "route" in event}),
        "active_count": len(active_events),
        "route_observations": route_observations,
        "required_fast_active_count": len(required_fast_events),
        "required_fast_gemma4_head_dims": required_fast_gemma4_geometries,
        "fallback_count": len(fallback_events),
        "rejected_count": len(rejected_events),
        "required_fast_route_active": required_fast_gemma4_geometries == [256, 512] and not rejected_events,
    }


def cuda_gqa_prefill_route_observation(
    telemetry: dict[str, Any],
    contract: dict[str, Any],
) -> dict[str, Any]:
    """Resolve one exact prefill route from versioned log telemetry."""

    expected_profile = contract.get("profile")
    expected_route = contract.get("route")
    for observation in telemetry.get("route_observations") or []:
        if (
            observation.get("profile") == expected_profile
            and observation.get("route") == expected_route
        ):
            return observation

    # Backward compatibility for reviewed v1 evidence synthesized before
    # route_observations was added to the parser output.
    if expected_profile == "required-fast" and expected_route == "prefill-fast":
        return {
            "profile": expected_profile,
            "route": expected_route,
            "active_count": int(telemetry.get("required_fast_active_count") or 0),
            "gemma4_head_dims": telemetry.get("required_fast_gemma4_head_dims") or [],
        }
    return {
        "profile": expected_profile,
        "route": expected_route,
        "active_count": 0,
        "gemma4_head_dims": [],
        "gemma4_query_lengths": [],
    }


def cuda_route_contract(args: argparse.Namespace) -> dict[str, Any]:
    resolved = getattr(args, "antfly_execution_profile", None)
    if isinstance(resolved, dict) and isinstance(resolved.get("route_contract"), dict):
        return resolved["route_contract"]
    profile_key = getattr(args, "cuda_execution_profile", CUDA_EXECUTION_PROFILE_DEFAULT)
    return CUDA_EXECUTION_PROFILES[profile_key]["route_contract"]


def parse_cuda_gqa_score_prework_telemetry(log_path: pathlib.Path) -> dict[str, Any]:
    """Extract bounded decode score-prework consumer evidence from a server log."""

    events = parse_key_value_log_events(log_path, CUDA_GQA_SCORE_PREWORK_LOG_MARKER)
    active_events = [event for event in events if event.get("event") == "active"]
    fallback_events = [event for event in events if event.get("event") == "fallback"]
    rejected_events = [event for event in events if event.get("event") == "rejected"]
    consumers = sorted({event["consumer"] for event in active_events if event.get("consumer")})
    observations = []
    for consumer in consumers:
        matching = [event for event in active_events if event.get("consumer") == consumer]
        routes_by_head_dim = {
            str(int(event["head_dim"])): event.get("route")
            for event in matching
            if event.get("head_dim", "").isdigit() and int(event["head_dim"]) in {256, 512}
        }
        observations.append({
            "consumer": consumer,
            "active_count": len(matching),
            "gemma4_head_dims": sorted(int(value) for value in routes_by_head_dim),
            "routes_by_head_dim": routes_by_head_dim,
        })
    return {
        "events": events,
        "configured_modes": sorted({
            event["configured"]
            for event in events
            if event.get("event") == "configured" and event.get("configured")
        }),
        "active_consumers": consumers,
        "active_count": len(active_events),
        "active_observations": observations,
        "fallback_count": len(fallback_events),
        "rejected_count": len(rejected_events),
        "fallback_reasons": sorted({
            event["fallback_reason"]
            for event in fallback_events + rejected_events
            if event.get("fallback_reason") and event.get("fallback_reason") != "none"
        }),
    }


def cuda_gqa_score_prework_observation(
    telemetry: dict[str, Any],
    contract: dict[str, Any],
) -> dict[str, Any]:
    expected_consumer = contract.get("consumer")
    for observation in telemetry.get("active_observations") or []:
        if observation.get("consumer") == expected_consumer:
            return observation
    return {
        "consumer": expected_consumer,
        "active_count": 0,
        "gemma4_head_dims": [],
        "routes_by_head_dim": {},
    }


def parse_cuda_gqa_decode_telemetry(log_path: pathlib.Path) -> dict[str, Any]:
    """Extract bounded typed split-K decode evidence from a server log."""

    events = parse_key_value_log_events(log_path, CUDA_GQA_DECODE_LOG_MARKER)
    active_events = [event for event in events if event.get("status") == "active"]
    fallback_events = [event for event in events if event.get("status") == "fallback"]
    rejected_events = [event for event in events if event.get("status") == "rejected"]
    observations = []
    for profile, route in sorted({
        (event.get("profile"), event.get("route"))
        for event in active_events
        if event.get("profile") and event.get("route")
    }):
        matching = [
            event for event in active_events
            if event.get("profile") == profile and event.get("route") == route
        ]
        observations.append({
            "profile": profile,
            "route": route,
            "active_count": len(matching),
            "gemma4_head_dims": sorted({
                int(event["head_dim"])
                for event in matching
                if event.get("head_dim", "").isdigit()
                and int(event["head_dim"]) in {256, 512}
            }),
        })
    return {
        "events": events,
        "configured_profiles": sorted({
            event["profile"] for event in events
            if event.get("status") == "configured" and event.get("profile")
        }),
        "active_routes": sorted({event["route"] for event in active_events if event.get("route")}),
        "active_count": len(active_events),
        "route_observations": observations,
        "fallback_count": len(fallback_events),
        "rejected_count": len(rejected_events),
        "fallback_reasons": sorted({
            event["reason"] for event in fallback_events + rejected_events if event.get("reason")
        }),
    }


def cuda_gqa_decode_route_observation(
    telemetry: dict[str, Any],
    contract: dict[str, Any],
) -> dict[str, Any]:
    for observation in telemetry.get("route_observations") or []:
        if (
            observation.get("profile") == contract.get("profile")
            and observation.get("route") == contract.get("route")
        ):
            return observation
    return {
        "profile": contract.get("profile"),
        "route": contract.get("route"),
        "active_count": 0,
        "gemma4_head_dims": [],
    }


def run_engine_sample(
    args: argparse.Namespace,
    fixture: PromptFixture,
    prompt_token_contract: dict[str, Any],
    pair_index: int,
    engine: str,
) -> dict[str, Any]:
    run_dir = args.output_dir / "raw" / f"pair-{pair_index:02d}-{engine}"
    run_dir.mkdir(parents=True, exist_ok=False)
    spec = make_server_spec(args, engine, choose_port(), run_dir)
    environment = os.environ.copy()
    if engine == "antfly":
        environment.update(args.antfly_execution_profile["effective_env"])
    log_path = run_dir / "server.log"
    warmups: list[dict[str, Any]] = []
    with ManagedServer(spec, log_path, environment, args.startup_timeout, args.request_timeout) as server:
        request_model = args.antfly_model_id if engine == "antfly" else args.model.resolve()
        llama_token_summary = None
        llama_template_summary = None
        if engine == "llama_cpp":
            measured_messages = request_body(
                engine, request_model, args.backend, fixture, args.output_tokens, args.cache_dtype, stream=True,
            )["messages"]
            templated, _ = server.post_json(
                {
                    "model": str(args.model.resolve()),
                    "messages": measured_messages,
                    "chat_template_kwargs": dict(fixture.chat_template_kwargs),
                },
                path="/apply-template",
            )
            rendered_prompt = templated.get("prompt")
            write_json(run_dir / "llama_apply_template_response.json", templated)
            if isinstance(rendered_prompt, str):
                (run_dir / "llama_applied_prompt.txt").write_text(rendered_prompt, encoding="utf-8")
            llama_template_summary = validate_llama_rendered_prompt(rendered_prompt, fixture)
            write_json(run_dir / "llama_prompt_template.json", llama_template_summary)
            tokenized, _ = server.post_json(
                {"content": rendered_prompt, "add_special": True, "parse_special": True},
                path="/tokenize",
            )
            token_ids = tokenized.get("tokens")
            if not isinstance(token_ids, list):
                raise RuntimeError("llama.cpp /tokenize response is missing tokens")
            try:
                llama_token_summary = {
                    **token_id_summary(token_ids),
                    "source": "llama_cpp_tokenize_applied_template",
                }
            except ValueError as exc:
                raise RuntimeError(f"invalid llama.cpp /tokenize response: {exc}") from exc
            write_json(
                run_dir / "llama_prompt_tokens.json",
                {**llama_token_summary, "token_ids": token_ids},
            )
            if llama_token_summary["sha256"] != prompt_token_contract["sha256"]:
                raise RuntimeError(
                    "prompt token IDs differ between Antfly and llama.cpp: "
                    f"Antfly={prompt_token_contract['sha256']} llama.cpp={llama_token_summary['sha256']}"
                )
        for warmup_index in range(1, args.warmups + 1):
            body = request_body(
                engine, request_model, args.backend, fixture, args.output_tokens, args.cache_dtype, stream=False,
            )
            response, transport = server.post_nonstream(body)
            warmup = {"index": warmup_index, **nonstream_summary(response), **transport}
            warmups.append(warmup)
        body = request_body(
            engine, request_model, args.backend, fixture, args.output_tokens, args.cache_dtype, stream=True,
        )
        measured, events = server.post_stream(body)

    cuda_gqa_prefill_telemetry = (
        parse_cuda_gqa_prefill_telemetry(log_path) if engine == "antfly" and args.backend == "cuda" else None
    )
    cuda_gqa_score_prework_telemetry = (
        parse_cuda_gqa_score_prework_telemetry(log_path)
        if engine == "antfly" and args.backend == "cuda"
        else None
    )
    cuda_gqa_decode_telemetry = (
        parse_cuda_gqa_decode_telemetry(log_path)
        if engine == "antfly" and args.backend == "cuda"
        else None
    )

    if not warmups:
        raise RuntimeError("at least one warmup is required to establish token accounting and HTTP reuse")
    prompt_counts = {int(row["prompt_tokens"]) for row in warmups}
    completion_counts = {int(row["completion_tokens"]) for row in warmups}
    finish_reasons = {str(row["finish_reason"]) for row in warmups}
    if len(prompt_counts) != 1 or len(completion_counts) != 1 or len(finish_reasons) != 1:
        raise RuntimeError(f"{engine} warmup accounting was not stable")
    warmup_prompt_tokens = prompt_counts.pop()
    if measured["prompt_tokens"] is None:
        measured["prompt_tokens"] = warmup_prompt_tokens
    elif measured["prompt_tokens"] != warmup_prompt_tokens:
        raise RuntimeError(
            f"{engine} measured prompt accounting differs from warmup: "
            f"measured={measured['prompt_tokens']} warmup={warmup_prompt_tokens}"
        )
    measured.update({
        "engine": engine,
        "warmups": warmups,
        "prompt_token_ids": llama_token_summary or {
            key: prompt_token_contract[key] for key in ("count", "sha256", "first_16", "last_16", "source")
        },
        "prompt_template": llama_template_summary or {
            **prompt_token_contract["template"],
        },
        "server_command": list(spec.command),
        "server_log": str(log_path),
    })
    if cuda_gqa_prefill_telemetry is not None:
        measured["cuda_gqa_prefill_telemetry"] = cuda_gqa_prefill_telemetry
    if cuda_gqa_score_prework_telemetry is not None:
        measured["cuda_gqa_score_prework_telemetry"] = cuda_gqa_score_prework_telemetry
    if cuda_gqa_decode_telemetry is not None:
        measured["cuda_gqa_decode_telemetry"] = cuda_gqa_decode_telemetry
    write_json(run_dir / "stream_events.json", events)
    write_json(run_dir / "sample.json", measured)
    return measured


def command_output(command: list[str], *, cwd: pathlib.Path | None = None) -> str | None:
    try:
        return subprocess.check_output(command, cwd=cwd, text=True, stderr=subprocess.STDOUT, timeout=30).strip()
    except (OSError, subprocess.SubprocessError):
        return None


def command_capture(command: list[str]) -> dict[str, Any]:
    try:
        completed = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {"returncode": None, "output": None, "error": str(exc)}
    return {
        "returncode": completed.returncode,
        "output": completed.stdout.strip(),
        "error": None,
    }


def parse_gpu_execution_state(
    raw: str | None,
    cuda_visible_devices: str | None,
) -> dict[str, Any]:
    if raw is None:
        raise ValueError("nvidia-smi GPU-state query failed")
    parsed: list[dict[str, Any]] = []
    for row_number, row in enumerate(csv.reader(io.StringIO(raw)), start=1):
        values = [value.strip() for value in row]
        if len(values) != len(STABLE_GPU_QUERY_FIELDS):
            raise ValueError(
                f"GPU-state row {row_number} has {len(values)} fields, expected {len(STABLE_GPU_QUERY_FIELDS)}"
            )
        record: dict[str, Any] = dict(zip(STABLE_GPU_QUERY_FIELDS, values, strict=True))
        if not str(record["uuid"]).startswith("GPU-"):
            raise ValueError(f"GPU-state row {row_number} has an invalid UUID")
        for name, parser in GPU_NUMERIC_FIELDS.items():
            value = str(record[name])
            if value in {"N/A", "[N/A]", ""}:
                raise ValueError(f"GPU-state field {name} is unavailable")
            try:
                numeric = parser(value)
            except ValueError as exc:
                raise ValueError(f"GPU-state field {name} is not numeric: {value!r}") from exc
            if numeric < 0 or (name != "index" and numeric <= 0):
                raise ValueError(f"GPU-state field {name} must be positive")
            record[name] = numeric
        if record["persistence_mode"] not in {"Enabled", "Disabled"}:
            raise ValueError(f"invalid persistence mode: {record['persistence_mode']!r}")
        if record["mig.mode.current"] not in {"Enabled", "Disabled", "N/A", "[N/A]"}:
            raise ValueError(f"invalid MIG mode: {record['mig.mode.current']!r}")
        parsed.append(record)
    if not parsed:
        raise ValueError("nvidia-smi reported no GPUs")

    if cuda_visible_devices is None:
        return {"cuda_visible_devices": None, "selected_gpus": parsed}
    selectors = [selector.strip() for selector in cuda_visible_devices.split(",")]
    if not selectors or any(not selector for selector in selectors):
        raise ValueError("CUDA_VISIBLE_DEVICES does not select a GPU")
    selected: list[dict[str, Any]] = []
    for selector in selectors:
        matches = [
            record
            for record in parsed
            if str(record["index"]) == selector or str(record["uuid"]).startswith(selector)
        ]
        if len(matches) != 1:
            raise ValueError(f"CUDA_VISIBLE_DEVICES selector {selector!r} does not resolve uniquely")
        if matches[0] in selected:
            raise ValueError(f"CUDA_VISIBLE_DEVICES selects GPU {selector!r} more than once")
        selected.append(matches[0])
    return {"cuda_visible_devices": cuda_visible_devices, "selected_gpus": selected}


def capture_gpu_execution_state(environment: dict[str, str] | None = None) -> dict[str, Any]:
    environment = os.environ if environment is None else environment
    raw = command_output([
        "nvidia-smi", f"--query-gpu={','.join(STABLE_GPU_QUERY_FIELDS)}",
        "--format=csv,noheader,nounits",
    ])
    try:
        return {**parse_gpu_execution_state(raw, environment.get("CUDA_VISIBLE_DEVICES")), "error": None}
    except ValueError as exc:
        return {
            "cuda_visible_devices": environment.get("CUDA_VISIBLE_DEVICES"),
            "selected_gpus": [],
            "error": str(exc),
        }


def capture_selected_gpu_compute_processes(gpu_state: dict[str, Any]) -> dict[str, Any]:
    raw = command_output([
        "nvidia-smi", "--query-compute-apps=gpu_uuid,pid,process_name",
        "--format=csv,noheader,nounits",
    ])
    if raw is None:
        return {"selected_gpu_processes": [], "error": "nvidia-smi compute-process query failed"}
    selected_uuids = {str(record.get("uuid")) for record in gpu_state.get("selected_gpus") or []}
    processes: list[dict[str, Any]] = []
    try:
        for row_number, row in enumerate(csv.reader(io.StringIO(raw)), start=1):
            values = [value.strip() for value in row]
            if len(values) != 3:
                raise ValueError(f"compute-process row {row_number} does not have three fields")
            gpu_uuid, pid_text, process_name = values
            if gpu_uuid not in selected_uuids:
                continue
            pid = int(pid_text)
            if pid <= 0 or not process_name:
                raise ValueError(f"compute-process row {row_number} is invalid")
            processes.append({"gpu_uuid": gpu_uuid, "pid": pid, "process_name": process_name})
    except ValueError as exc:
        return {"selected_gpu_processes": [], "error": str(exc)}
    return {"selected_gpu_processes": processes, "error": None}


def visible_model_bundle_files(model_dir: pathlib.Path) -> list[pathlib.Path]:
    return sorted(
        (
            path
            for path in model_dir.rglob("*")
            if not any(part.startswith(".") for part in path.relative_to(model_dir).parts)
            and path.is_file()
        ),
        key=lambda item: item.relative_to(model_dir).as_posix(),
    )


def model_bundle_layout_errors(model_dir: pathlib.Path) -> list[str]:
    errors: list[str] = []
    for path in sorted(model_dir.rglob("*"), key=lambda item: item.relative_to(model_dir).as_posix()):
        relative = path.relative_to(model_dir).as_posix()
        if any(part.startswith(".") for part in path.relative_to(model_dir).parts):
            errors.append(f"hidden model-bundle path is unsupported for enforced evidence: {relative}")
        if path.is_symlink():
            errors.append(f"symlinked model-bundle path is unsupported for enforced evidence: {relative}")
        elif not path.is_file() and not path.is_dir():
            errors.append(f"special model-bundle path is unsupported for enforced evidence: {relative}")
        if path.is_file() and path.name in UNSAFE_MODEL_CONTROL_FILES:
            errors.append(
                "runtime-path manifest is unsupported for enforced evidence until ModelManager "
                f"exports its resolved dependency closure: {relative}"
            )
    return errors


def path_provenance(path: pathlib.Path) -> dict[str, Any]:
    resolved = path.resolve()
    value: dict[str, Any] = {"path": str(resolved), "exists": resolved.exists()}
    if resolved.is_file():
        before = resolved.stat()
        digest = sha256_file(resolved)
        after = resolved.stat()
        before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        if before_identity != after_identity:
            raise RuntimeError(f"file changed while hashing benchmark provenance: {resolved}")
        value.update({
            "bytes": after.st_size,
            "sha256": digest,
            "file_identity": {
                "device": after.st_dev,
                "inode": after.st_ino,
                "size": after.st_size,
                "mtime_ns": after.st_mtime_ns,
            },
        })
    return value


def harness_provenance(script_path: pathlib.Path) -> dict[str, Any]:
    """Content-address the benchmark and its local behavioral dependency."""
    shared_scripts_dir = script_path.resolve().parent.parent
    files = {
        "benchmark": path_provenance(script_path),
        "pairing_support": path_provenance(shared_scripts_dir / "paired_benchmark.py"),
    }
    identity = {
        name: {"bytes": value.get("bytes"), "sha256": value.get("sha256")}
        for name, value in sorted(files.items())
    }
    return {
        "files": files,
        "sha256": canonical_sha256(identity),
    }


def antfly_model_bundle_provenance(
    model: pathlib.Path,
    decoder_provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Hash every non-hidden regular file Antfly can observe in the model directory."""
    resolved_model = model.resolve()
    model_dir = resolved_model.parent

    paths = visible_model_bundle_files(model_dir)
    before_identities: dict[str, tuple[int, int, int, int]] = {}
    for path in paths:
        stat = path.stat()
        before_identities[path.relative_to(model_dir).as_posix()] = (
            stat.st_dev,
            stat.st_ino,
            stat.st_size,
            stat.st_mtime_ns,
        )
    files: list[dict[str, Any]] = []
    for path in paths:
        relative = path.relative_to(model_dir)
        if path.resolve() == resolved_model and decoder_provenance is not None:
            item = decoder_provenance
            expected_identity = item.get("file_identity")
            observed_identity = before_identities[relative.as_posix()]
            if not isinstance(expected_identity, dict) or (
                expected_identity.get("device"),
                expected_identity.get("inode"),
                expected_identity.get("size"),
                expected_identity.get("mtime_ns"),
            ) != observed_identity:
                raise RuntimeError("benchmark decoder changed after its provenance hash was collected")
        else:
            item = path_provenance(path)
        files.append({
            "path": relative.as_posix(),
            "bytes": item.get("bytes"),
            "sha256": item.get("sha256"),
            "file_identity": item.get("file_identity"),
        })
    after_paths = visible_model_bundle_files(model_dir)
    if [path.relative_to(model_dir).as_posix() for path in after_paths] != list(before_identities):
        raise RuntimeError("Antfly model bundle inventory changed while provenance was collected")
    for path in after_paths:
        relative = path.relative_to(model_dir).as_posix()
        stat = path.stat()
        if (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns) != before_identities[relative]:
            raise RuntimeError(f"Antfly model bundle file changed while hashing provenance: {relative}")
    decoder_relative = resolved_model.relative_to(model_dir).as_posix()
    if not any(item["path"] == decoder_relative for item in files):
        raise RuntimeError("Antfly model bundle inventory does not contain the benchmark decoder")
    identity = {
        "files": [
            {"path": item["path"], "bytes": item["bytes"], "sha256": item["sha256"]}
            for item in files
        ]
    }
    return {
        "directory": str(model_dir),
        "files": files,
        "layout_errors": model_bundle_layout_errors(model_dir),
        "sha256": canonical_sha256(identity),
    }


def model_bundle_stat_errors(model: pathlib.Path, expected_bundle: dict[str, Any]) -> list[str]:
    model_dir = model.resolve().parent
    expected_files = expected_bundle.get("files")
    if not isinstance(expected_files, list):
        return ["model-bundle provenance has no file inventory"]
    current_paths = visible_model_bundle_files(model_dir)
    current_names = [path.relative_to(model_dir).as_posix() for path in current_paths]
    expected_names = [item.get("path") for item in expected_files if isinstance(item, dict)]
    errors: list[str] = []
    if current_names != expected_names:
        errors.append(
            f"model-bundle inventory changed: expected={expected_names!r} observed={current_names!r}"
        )
        return errors
    for path, expected in zip(current_paths, expected_files, strict=True):
        if not isinstance(expected, dict) or not isinstance(expected.get("file_identity"), dict):
            errors.append(f"model-bundle file lacks stable identity: {path.relative_to(model_dir)}")
            continue
        stat = path.stat()
        observed_identity = {
            "device": stat.st_dev,
            "inode": stat.st_ino,
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }
        if observed_identity != expected["file_identity"]:
            errors.append(f"model-bundle file changed: {path.relative_to(model_dir)}")
    return errors


def parse_ldd_dependencies(output: str) -> tuple[list[tuple[str, pathlib.Path]], list[str]]:
    resolved: list[tuple[str, pathlib.Path]] = []
    unresolved: list[str] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "=>" in line:
            name, target_text = (part.strip() for part in line.split("=>", 1))
            target = target_text.rsplit(" (", 1)[0].strip()
            if target == "not found":
                unresolved.append(name)
            elif target.startswith("/"):
                resolved.append((name, pathlib.Path(target).resolve()))
            continue
        target = line.rsplit(" (", 1)[0].strip()
        if target.startswith("/"):
            path = pathlib.Path(target).resolve()
            resolved.append((path.name, path))
    return sorted(set(resolved), key=lambda item: (item[0], str(item[1]))), sorted(set(unresolved))


def is_system_shared_object(path: pathlib.Path) -> bool:
    resolved = path.resolve()
    return any(resolved == root or root in resolved.parents for root in SYSTEM_LIBRARY_ROOTS)


def dynamic_dependency_provenance(path: pathlib.Path) -> dict[str, Any]:
    resolved_binary = path.resolve()
    try:
        with resolved_binary.open("rb") as binary_file:
            is_elf = binary_file.read(4) == b"\x7fELF"
    except OSError:
        is_elf = False
    inspection = command_capture(["ldd", str(resolved_binary)])
    output = inspection.get("output") or ""
    returncode = inspection.get("returncode")
    static_markers = ("not a dynamic executable", "statically linked")
    if returncode == 0:
        status = "resolved"
        dependencies, unresolved = parse_ldd_dependencies(output)
    elif is_elf and any(marker in output.lower() for marker in static_markers):
        status = "static"
        dependencies, unresolved = [], []
    else:
        status = "inspection_failed"
        dependencies, unresolved = [], []

    material: list[dict[str, Any]] = []
    system: list[dict[str, str]] = []
    for name, dependency_path in dependencies:
        if (
            not is_system_shared_object(dependency_path)
            or name.startswith(MATERIAL_SHARED_OBJECT_PREFIXES)
        ):
            material.append({"name": name, **path_provenance(dependency_path)})
        else:
            system.append({"name": name, "path": str(dependency_path)})
    closure_identity = {
        "status": status,
        "is_elf": is_elf,
        "material_dependencies": [
            {
                "name": item["name"],
                "bytes": item.get("bytes"),
                "sha256": item.get("sha256"),
            }
            for item in material
        ],
        "unresolved_dependencies": unresolved,
    }
    return {
        **closure_identity,
        "material_dependency_files": material,
        "system_dependencies": system,
        "inspection": inspection,
        "sha256": canonical_sha256(closure_identity),
    }


def git_provenance(path: pathlib.Path) -> dict[str, Any]:
    root = command_output(["git", "-C", str(path), "rev-parse", "--show-toplevel"])
    if not root:
        return {"available": False}
    status = command_output(["git", "-C", root, "status", "--porcelain=v1", "--untracked-files=all"])
    return {
        "available": True,
        "root": root,
        "commit": command_output(["git", "-C", root, "rev-parse", "HEAD"]),
        "dirty": bool(status),
        "status_sha256": sha256_bytes((status or "").encode("utf-8")),
    }


def stable_executable_build_metadata(version_output: str | None) -> str | None:
    if not version_output:
        return None
    lines = [line.strip() for line in version_output.splitlines() if line.strip()]
    stable_lines = [line for line in lines if line.startswith(("version:", "built with "))]
    return "\n".join(stable_lines or lines)


def executable_provenance(path: pathlib.Path, *, inspect_dependencies: bool = False) -> dict[str, Any]:
    value = path_provenance(path)
    value["version"] = command_output([str(path.resolve()), "--version"])
    value["build_metadata"] = stable_executable_build_metadata(value["version"])
    value["git"] = git_provenance(path.resolve().parent)
    if inspect_dependencies:
        dependencies = dynamic_dependency_provenance(path)
        value["dynamic_dependencies"] = dependencies
        bundle_identity = {
            "binary_sha256": value.get("sha256"),
            "build_metadata": value.get("build_metadata"),
            "git_commit": value["git"].get("commit"),
            "dynamic_dependency_closure_sha256": dependencies["sha256"],
        }
        value["runtime_bundle_sha256"] = canonical_sha256(bundle_identity)
    return value


def stable_runtime_identity() -> dict[str, Any]:
    gpu_execution_state = capture_gpu_execution_state()
    identity = {
        "gpu_execution_state": gpu_execution_state,
        "cuda_toolchain": command_output(["nvcc", "--version"]),
        "host_platform": platform.platform(),
        "python": platform.python_version(),
    }
    return {**identity, "sha256": canonical_sha256(identity)}


def collect_provenance(args: argparse.Namespace, fixture: PromptFixture) -> dict[str, Any]:
    script_path = pathlib.Path(__file__).resolve()
    gpu = command_output([
        "nvidia-smi", "--query-gpu=index,uuid,name,driver_version,compute_cap,pci.bus_id,persistence_mode,pstate,temperature.gpu,power.draw,memory.total",
        "--format=csv,noheader,nounits",
    ])
    runtime_identity = stable_runtime_identity()
    gpu_compute_processes = capture_selected_gpu_compute_processes(
        runtime_identity["gpu_execution_state"]
    )
    model = path_provenance(args.model)
    return {
        "schema": EVIDENCE_SCHEMA,
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "host": {"platform": platform.platform(), "python": platform.python_version()},
        "gpu": gpu,
        "gpu_compute_processes_before_benchmark": gpu_compute_processes,
        "cuda_toolchain": runtime_identity["cuda_toolchain"],
        "runtime_identity": runtime_identity,
        "antfly_git": git_provenance(script_path.parent),
        "antfly_binary": executable_provenance(args.antfly_bin),
        "llama_cpp_binary": executable_provenance(args.llama_server_bin, inspect_dependencies=True),
        "model": model,
        "antfly_model_bundle": antfly_model_bundle_provenance(args.model, model),
        "harness": harness_provenance(script_path),
        "fixture_file": path_provenance(fixture.path),
        "antfly_execution_profile": execution_profile_provenance(args.antfly_execution_profile),
    }


def provenance_validation_errors(args: argparse.Namespace, provenance: dict[str, Any]) -> list[str]:
    if not args.enforce_performance or args.backend != "cuda":
        return []
    errors: list[str] = []
    model_bundle = provenance.get("antfly_model_bundle") or {}
    if not model_bundle.get("sha256") or not model_bundle.get("files"):
        errors.append("enforced evidence requires a hashed Antfly model-bundle inventory")
    errors.extend(model_bundle.get("layout_errors") or [])
    runtime_identity = provenance.get("runtime_identity") or {}
    gpu_execution_state = runtime_identity.get("gpu_execution_state") or {}
    if gpu_execution_state.get("error") or not gpu_execution_state.get("selected_gpus"):
        errors.append(
            "enforced CUDA evidence requires stable NVIDIA GPU/driver/power/clock/MIG provenance"
        )
    if not runtime_identity.get("cuda_toolchain"):
        errors.append("enforced CUDA evidence requires CUDA toolchain provenance")
    compute_processes = provenance.get("gpu_compute_processes_before_benchmark") or {}
    if compute_processes.get("error"):
        errors.append("enforced CUDA evidence could not inspect pre-existing GPU compute processes")
    elif compute_processes.get("selected_gpu_processes"):
        errors.append(
            "enforced CUDA evidence requires an idle GPU; pre-existing compute processes were observed: "
            f"{compute_processes['selected_gpu_processes']!r}"
        )
    llama = provenance.get("llama_cpp_binary") or {}
    dependencies = llama.get("dynamic_dependencies") or {}
    status = dependencies.get("status")
    if not dependencies.get("is_elf"):
        errors.append("enforced evidence requires llama-server to be an ELF executable")
    if status not in {"resolved", "static"}:
        errors.append(f"could not resolve llama-server dynamic dependency closure: status={status!r}")
    unresolved = dependencies.get("unresolved_dependencies") or []
    if unresolved:
        errors.append(f"llama-server has unresolved dynamic dependencies: {unresolved!r}")
    for item in dependencies.get("material_dependency_files") or []:
        if not item.get("sha256"):
            errors.append(f"could not hash llama-server material dependency: {item.get('path')!r}")
    if not llama.get("runtime_bundle_sha256"):
        errors.append("llama-server runtime bundle hash is unavailable")
    if not llama.get("build_metadata"):
        errors.append("llama-server build/version metadata is unavailable")
    return errors


def capture_runtime_guard(
    args: argparse.Namespace,
    provenance: dict[str, Any],
    stage: str,
) -> dict[str, Any]:
    expected_runtime = provenance.get("runtime_identity") or {}
    expected_gpu_state = expected_runtime.get("gpu_execution_state")
    gpu_state = capture_gpu_execution_state()
    compute_processes = capture_selected_gpu_compute_processes(gpu_state)
    integrity_errors = model_bundle_stat_errors(args.model, provenance.get("antfly_model_bundle") or {})
    performance_errors: list[str] = []
    if args.backend == "cuda":
        if gpu_state.get("error") or not gpu_state.get("selected_gpus"):
            performance_errors.append(f"{stage}: could not capture stable selected-GPU execution state")
        elif gpu_state != expected_gpu_state:
            performance_errors.append(f"{stage}: selected-GPU power/clock/MIG state changed")
        if compute_processes.get("error"):
            performance_errors.append(f"{stage}: could not inspect selected-GPU compute processes")
        elif compute_processes.get("selected_gpu_processes"):
            performance_errors.append(
                f"{stage}: selected GPU is not idle: {compute_processes['selected_gpu_processes']!r}"
            )
    return {
        "stage": stage,
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "gpu_execution_state": gpu_state,
        "selected_gpu_compute_processes": compute_processes,
        "model_bundle_integrity_errors": integrity_errors,
        "performance_isolation_errors": performance_errors,
    }


def runtime_guard_errors(args: argparse.Namespace, guard: dict[str, Any]) -> list[str]:
    errors = list(guard.get("model_bundle_integrity_errors") or [])
    if args.enforce_performance:
        errors.extend(guard.get("performance_isolation_errors") or [])
    return errors


def build_lock(args: argparse.Namespace, fixture: PromptFixture, provenance: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema": LOCK_SCHEMA,
        "model_sha256": provenance["model"].get("sha256"),
        "antfly_model_bundle_sha256": provenance["antfly_model_bundle"].get("sha256"),
        "llama_cpp_commit": provenance["llama_cpp_binary"].get("git", {}).get("commit"),
        "llama_cpp_binary_sha256": provenance["llama_cpp_binary"].get("sha256"),
        "llama_cpp_runtime_bundle_sha256": provenance["llama_cpp_binary"].get("runtime_bundle_sha256"),
        "runtime_identity_sha256": provenance.get("runtime_identity", {}).get("sha256"),
        "harness_sha256": provenance.get("harness", {}).get("sha256"),
        "antfly_execution_profile_sha256": args.antfly_execution_profile["sha256"],
        "material_environment_sha256": args.antfly_execution_profile["material_environment_sha256"],
        "antfly_server_prefix_sha256": args.antfly_execution_profile["server_prefix_sha256"]["antfly"],
        "llama_cpp_server_prefix_sha256": args.antfly_execution_profile["server_prefix_sha256"]["llama_cpp"],
        "fixture_id": fixture.fixture_id,
        "fixture_file_sha256": fixture.file_sha256,
        "rendered_prompt_sha256": fixture.reference_prompt_sha256,
        "backend": args.backend,
        "cache_dtype": args.cache_dtype,
        "context_size": args.context_size,
        "output_tokens": args.output_tokens,
        "antfly_server_budget_mb": dict(args.antfly_execution_profile["server_budget_mb"]),
    }


def lock_errors(expected: object, observed: dict[str, Any]) -> list[str]:
    if not isinstance(expected, dict):
        return ["benchmark lock must be a JSON object"]
    if expected.get("schema") != LOCK_SCHEMA:
        return [f"unsupported benchmark lock schema: {expected.get('schema')!r}"]
    errors = []
    for key, value in observed.items():
        if key == "schema":
            continue
        if expected.get(key) != value:
            errors.append(f"benchmark lock mismatch for {key}: expected {expected.get(key)!r}, observed {value!r}")
    return errors


def baseline_compatibility_errors(baseline: object, observed_lock: dict[str, Any]) -> list[str]:
    if not isinstance(baseline, dict):
        return ["baseline evidence must be a JSON object"]
    if baseline.get("schema") != EVIDENCE_SCHEMA:
        return [f"unsupported baseline evidence schema: {baseline.get('schema')!r}"]
    baseline_lock = baseline.get("lock")
    if not isinstance(baseline_lock, dict):
        return ["baseline evidence is missing its benchmark lock"]
    compared_fields = (
        "model_sha256", "antfly_model_bundle_sha256", "fixture_id", "fixture_file_sha256", "rendered_prompt_sha256",
        "backend", "cache_dtype", "context_size", "output_tokens", "antfly_execution_profile_sha256",
        "material_environment_sha256", "antfly_server_prefix_sha256", "llama_cpp_server_prefix_sha256",
        "harness_sha256", "llama_cpp_runtime_bundle_sha256", "runtime_identity_sha256",
        "antfly_server_budget_mb",
    )
    errors = []
    for key in compared_fields:
        if baseline_lock.get(key) != observed_lock.get(key):
            errors.append(
                f"baseline is incompatible for {key}: baseline={baseline_lock.get(key)!r}, "
                f"current={observed_lock.get(key)!r}"
            )
    return errors


def evidence_lock_projection(evidence: dict[str, Any]) -> dict[str, Any]:
    """Reconstruct lock fields from the evidence sections they attest."""
    provenance = evidence.get("provenance") if isinstance(evidence.get("provenance"), dict) else {}
    contract = evidence.get("contract") if isinstance(evidence.get("contract"), dict) else {}
    model = provenance.get("model") if isinstance(provenance.get("model"), dict) else {}
    model_bundle = (
        provenance.get("antfly_model_bundle")
        if isinstance(provenance.get("antfly_model_bundle"), dict)
        else {}
    )
    llama = (
        provenance.get("llama_cpp_binary")
        if isinstance(provenance.get("llama_cpp_binary"), dict)
        else {}
    )
    llama_git = llama.get("git") if isinstance(llama.get("git"), dict) else {}
    runtime_identity = (
        provenance.get("runtime_identity")
        if isinstance(provenance.get("runtime_identity"), dict)
        else {}
    )
    harness = provenance.get("harness") if isinstance(provenance.get("harness"), dict) else {}
    fixture_file = (
        provenance.get("fixture_file")
        if isinstance(provenance.get("fixture_file"), dict)
        else {}
    )
    execution_profile = (
        provenance.get("antfly_execution_profile")
        if isinstance(provenance.get("antfly_execution_profile"), dict)
        else {}
    )
    prefix_sha256 = (
        execution_profile.get("server_prefix_sha256")
        if isinstance(execution_profile.get("server_prefix_sha256"), dict)
        else {}
    )
    return {
        "schema": LOCK_SCHEMA,
        "model_sha256": model.get("sha256"),
        "antfly_model_bundle_sha256": model_bundle.get("sha256"),
        "llama_cpp_commit": llama_git.get("commit"),
        "llama_cpp_binary_sha256": llama.get("sha256"),
        "llama_cpp_runtime_bundle_sha256": llama.get("runtime_bundle_sha256"),
        "runtime_identity_sha256": runtime_identity.get("sha256"),
        "harness_sha256": harness.get("sha256"),
        "antfly_execution_profile_sha256": execution_profile.get("sha256"),
        "material_environment_sha256": execution_profile.get("material_environment_sha256"),
        "antfly_server_prefix_sha256": prefix_sha256.get("antfly"),
        "llama_cpp_server_prefix_sha256": prefix_sha256.get("llama_cpp"),
        "fixture_id": contract.get("prompt_fixture_id"),
        "fixture_file_sha256": fixture_file.get("sha256"),
        "rendered_prompt_sha256": contract.get("reference_prompt_sha256"),
        "backend": contract.get("backend"),
        "cache_dtype": contract.get("cache_dtype"),
        "context_size": contract.get("context_size"),
        "output_tokens": contract.get("output_tokens"),
        "antfly_server_budget_mb": (
            dict(execution_profile["server_budget_mb"])
            if isinstance(execution_profile.get("server_budget_mb"), dict)
            else execution_profile.get("server_budget_mb")
        ),
    }


def baseline_quality_errors(baseline: object, expected_profile: str) -> list[str]:
    if not isinstance(baseline, dict):
        return ["baseline evidence must be a JSON object"]
    errors: list[str] = []
    if baseline.get("passed") is not True:
        errors.append("baseline evidence must have passed all enforced correctness checks")
    contract = baseline.get("contract")
    if not isinstance(contract, dict):
        errors.append("baseline evidence is missing its benchmark contract")
    else:
        if contract.get("profile") != expected_profile:
            errors.append(
                f"baseline profile mismatch: expected {expected_profile!r}, observed {contract.get('profile')!r}"
            )
        paired_samples = contract.get("paired_samples")
        if (
            isinstance(paired_samples, bool)
            or not isinstance(paired_samples, int)
            or paired_samples < MIN_FROZEN_BASELINE_SAMPLES
        ):
            errors.append(
                f"baseline contract requires at least {MIN_FROZEN_BASELINE_SAMPLES} paired samples"
            )
    rows_value = baseline.get("rows")
    if not isinstance(rows_value, list) or len(rows_value) < MIN_FROZEN_BASELINE_SAMPLES:
        errors.append(f"baseline evidence requires at least {MIN_FROZEN_BASELINE_SAMPLES} sample rows")
    elif isinstance(contract, dict) and contract.get("paired_samples") != len(rows_value):
        errors.append("baseline row count differs from contract.paired_samples")

    provenance = baseline.get("provenance")
    if not isinstance(provenance, dict):
        errors.append("baseline evidence is missing provenance")
    elif baseline.get("provenance_sha256") != canonical_sha256(provenance):
        errors.append("baseline provenance hash does not match its contents")
    else:
        baseline_lock_value = baseline.get("lock")
        if not isinstance(baseline_lock_value, dict):
            errors.append("baseline evidence is missing its benchmark lock")
        else:
            reconstructed = evidence_lock_projection(baseline)
            for lock_key, evidence_value in reconstructed.items():
                if baseline_lock_value.get(lock_key) != evidence_value:
                    errors.append(
                        f"baseline {lock_key} does not match baseline provenance/contract: "
                        f"lock={baseline_lock_value.get(lock_key)!r} evidence={evidence_value!r}"
                    )

    metrics = baseline.get("metrics")
    for engine in ENGINES:
        for metric in ("total_latency_ms", "ttft_ms", "decode_ms"):
            try:
                stored = metrics[engine][metric]
                stored_median = float(stored["median"])
                recomputed = distribution(float(row[engine][metric]) for row in rows_value)
            except (KeyError, TypeError, ValueError):
                errors.append(f"baseline evidence cannot verify {engine} {metric}")
                continue
            if not math.isclose(stored_median, recomputed["median"], rel_tol=1e-12, abs_tol=1e-9):
                errors.append(f"baseline stored {engine} {metric} median does not match sample rows")
            if metric == "total_latency_ms":
                try:
                    stored_cv = float(stored["cv"])
                except (KeyError, TypeError, ValueError):
                    errors.append(f"baseline evidence is missing {engine} total-latency CV")
                    continue
                if not math.isclose(stored_cv, recomputed["cv"], rel_tol=1e-12, abs_tol=1e-12):
                    errors.append(f"baseline stored {engine} total-latency CV does not match sample rows")
                if recomputed["cv"] > MAX_FROZEN_BASELINE_CV:
                    errors.append(
                        f"baseline {engine} total-latency CV {recomputed['cv']!r} "
                        f"exceeds {MAX_FROZEN_BASELINE_CV}"
                    )
    return errors


def baseline_evidence_identity(
    path: pathlib.Path,
    baseline: dict[str, Any],
    source_bytes: bytes,
) -> dict[str, Any]:
    contract = baseline.get("contract") if isinstance(baseline.get("contract"), dict) else {}
    provenance = baseline.get("provenance") if isinstance(baseline.get("provenance"), dict) else {}
    antfly_binary = provenance.get("antfly_binary") if isinstance(provenance.get("antfly_binary"), dict) else {}
    antfly_git = provenance.get("antfly_git") if isinstance(provenance.get("antfly_git"), dict) else {}
    return {
        "file": {
            "path": str(path.resolve()),
            "exists": True,
            "bytes": len(source_bytes),
            "sha256": sha256_bytes(source_bytes),
        },
        "schema": baseline.get("schema"),
        "timestamp_utc": baseline.get("timestamp_utc"),
        "profile": contract.get("profile"),
        "paired_samples": contract.get("paired_samples"),
        "passed": baseline.get("passed"),
        "provenance_sha256": baseline.get("provenance_sha256"),
        "lock_sha256": canonical_sha256(baseline.get("lock")),
        "antfly_commit": antfly_git.get("commit"),
        "antfly_binary_sha256": antfly_binary.get("sha256"),
    }


def baseline_median(baseline: dict[str, Any], metric: str) -> float:
    try:
        value = float(baseline["metrics"]["antfly"][metric]["median"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"baseline evidence is missing Antfly {metric} median") from exc
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"baseline Antfly {metric} median must be positive")
    return value


def performance_threshold_contract(args: argparse.Namespace) -> dict[str, Any]:
    """Return the exact performance policy embedded in benchmark evidence."""
    return {
        "schema": "antfly.gemma4_long_e2e.performance_thresholds.v1",
        "performance_enforced": bool(args.enforce_performance),
        "antfly_llama": {
            "median_total_latency_ratio_max_inclusive": args.max_median_ratio,
            "paired_total_latency_ratio_95_ci_upper_max_exclusive": args.max_ci_upper,
            "p95_total_latency_ratio_max_inclusive": args.max_p95_ratio,
            "median_ttft_ratio_max_inclusive": args.max_component_ratio,
            "paired_ttft_log_ratio_95_ci_upper_max_inclusive": args.max_ttft_ci_upper,
            "median_decode_latency_ratio_max_inclusive": args.max_component_ratio,
            "paired_decode_log_ratio_95_ci_upper_max_inclusive": args.max_decode_ci_upper,
        },
        "per_engine_total_latency_cv_max_inclusive": args.max_cv,
        "antfly_frozen_baseline_median_latency_ratio_max_inclusive": (
            args.max_baseline_regression
        ),
    }


def evaluate(
    args: argparse.Namespace,
    fixture: PromptFixture,
    rows: list[dict[str, Any]],
    baseline: dict[str, Any] | None,
) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for engine in ENGINES:
        metrics[engine] = {
            metric: distribution(row[engine][metric] for row in rows)
            for metric in (
                "total_latency_ms", "ttft_ms", "decode_ms", "stream_tail_ms", "inter_token_ms", "decode_tok_s",
            )
        }
    pair_ratios = [row["antfly"]["total_latency_ms"] / row["llama_cpp"]["total_latency_ms"] for row in rows]
    total_ratio = metrics["antfly"]["total_latency_ms"]["median"] / metrics["llama_cpp"]["total_latency_ms"]["median"]
    ttft_ratio = metrics["antfly"]["ttft_ms"]["median"] / metrics["llama_cpp"]["ttft_ms"]["median"]
    decode_ratio = metrics["antfly"]["decode_ms"]["median"] / metrics["llama_cpp"]["decode_ms"]["median"]
    p95_ratio = metrics["antfly"]["total_latency_ms"]["p95"] / metrics["llama_cpp"]["total_latency_ms"]["p95"]
    bootstrap = paired_bootstrap_ratio_ci(rows, samples=args.bootstrap_samples, seed=args.bootstrap_seed)
    paired_log_ratio_cis = {
        metric: paired_metric_log_ratio_ci(
            rows,
            numerator_arm="antfly",
            denominator_arm="llama_cpp",
            metric=metric,
            samples=args.bootstrap_samples,
            seed=args.bootstrap_seed,
        )
        for metric in ("total_latency_ms", "ttft_ms", "decode_ms")
    }

    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: str, *, performance: bool = False) -> None:
        enforced = not performance or args.enforce_performance
        checks.append({"name": name, "passed": bool(passed), "enforced": enforced, "detail": detail})

    for row in rows:
        for engine in ENGINES:
            sample = row[engine]
            check(
                f"pair_{row['pair']}_{engine}_prompt_tokens",
                sample["prompt_tokens"] == args.expected_prompt_tokens,
                f"observed={sample['prompt_tokens']} expected={args.expected_prompt_tokens}",
            )
            check(
                f"pair_{row['pair']}_{engine}_completion_tokens",
                sample["completion_tokens"] == args.output_tokens,
                f"observed={sample['completion_tokens']} expected={args.output_tokens}",
            )
            check(
                f"pair_{row['pair']}_{engine}_finish_reason",
                sample["finish_reason"] == "length",
                f"observed={sample['finish_reason']!r} expected='length'",
            )
            visible_content = str(sample.get("content") or "")
            check(
                f"pair_{row['pair']}_{engine}_visible_content",
                int(sample.get("content_utf8_bytes") or 0) >= fixture.minimum_visible_utf8_bytes,
                f"utf8_bytes={sample.get('content_utf8_bytes')!r} minimum={fixture.minimum_visible_utf8_bytes}",
            )
            missing_terms = [
                value for value in fixture.expected_output_substrings if value not in visible_content
            ]
            check(
                f"pair_{row['pair']}_{engine}_semantic_output",
                not missing_terms,
                f"missing_required_substrings={missing_terms!r}",
            )
            check(
                f"pair_{row['pair']}_{engine}_connection_reused",
                bool(sample["connection_reused"]),
                "measured request must reuse the warmup HTTP connection",
            )
            if args.profile == "headline" and engine == "antfly":
                telemetry = sample.get("cuda_gqa_prefill_telemetry") or {}
                prefill_contract = cuda_route_contract(args)["prefill"]
                configured_profiles = telemetry.get("configured_profiles") or []
                observation = cuda_gqa_prefill_route_observation(telemetry, prefill_contract)
                active_count = int(observation.get("active_count") or 0)
                active_head_dims = observation.get("gemma4_head_dims") or []
                active_query_lengths = observation.get("gemma4_query_lengths") or []
                fallback_count = int(telemetry.get("fallback_count") or 0)
                rejected_count = int(telemetry.get("rejected_count") or 0)
                expected_profile = str(prefill_contract["profile"])
                expected_route = str(prefill_contract["route"])
                expected_head_dims = list(prefill_contract["gemma4_head_dims"])
                expected_query_lengths = list(prefill_contract.get("gemma4_query_lengths") or [])
                check_suffix = (
                    "required_fast_gqa_prefill"
                    if expected_profile == "required-fast" and expected_route == "prefill-fast"
                    else "gqa_prefill_route_contract"
                )
                check(
                    f"pair_{row['pair']}_antfly_{check_suffix}",
                    expected_profile in configured_profiles
                    and active_head_dims == expected_head_dims
                    and (not expected_query_lengths or active_query_lengths == expected_query_lengths)
                    and (not prefill_contract.get("forbid_fallback") or fallback_count == 0)
                    and rejected_count == 0,
                    f"{expected_profile} must be configured and {expected_route} must launch for "
                    "both locked Gemma 4 attention head dimensions without rejection; "
                    f"configured={configured_profiles!r} active={active_count} "
                    f"head_dims={active_head_dims!r} expected_head_dims={expected_head_dims!r} "
                    f"query_lengths={active_query_lengths!r} expected_query_lengths={expected_query_lengths!r} "
                    f"fallbacks={fallback_count} rejected={rejected_count}",
                    performance=True,
                )
                decode_contract = cuda_route_contract(args)["decode"]
                if "profile" in decode_contract:
                    decode_telemetry = sample.get("cuda_gqa_decode_telemetry") or {}
                    decode_observation = cuda_gqa_decode_route_observation(decode_telemetry, decode_contract)
                    configured_profiles = decode_telemetry.get("configured_profiles") or []
                    expected_decode_profile = str(decode_contract["profile"])
                    expected_decode_route = str(decode_contract["route"])
                    expected_decode_head_dims = list(decode_contract["gemma4_head_dims"])
                    observed_decode_head_dims = decode_observation.get("gemma4_head_dims") or []
                    decode_fallbacks = int(decode_telemetry.get("fallback_count") or 0)
                    decode_rejections = int(decode_telemetry.get("rejected_count") or 0)
                    check(
                        f"pair_{row['pair']}_antfly_gqa_splitk_online_decode_contract",
                        expected_decode_profile in configured_profiles
                        and decode_observation.get("route") == expected_decode_route
                        and observed_decode_head_dims == expected_decode_head_dims
                        and (not decode_contract.get("forbid_fallback") or decode_fallbacks == 0)
                        and decode_rejections == 0,
                        "the required split-K online profile must launch both Gemma 4 head dimensions "
                        "without fallback or rejection; "
                        f"configured={configured_profiles!r} expected_profile={expected_decode_profile!r} "
                        f"route={decode_observation.get('route')!r} expected_route={expected_decode_route!r} "
                        f"head_dims={observed_decode_head_dims!r} expected_head_dims={expected_decode_head_dims!r} "
                        f"fallbacks={decode_fallbacks} rejections={decode_rejections}",
                        performance=True,
                    )
                else:
                    decode_telemetry = sample.get("cuda_gqa_score_prework_telemetry") or {}
                    decode_observation = cuda_gqa_score_prework_observation(decode_telemetry, decode_contract)
                    configured_modes = decode_telemetry.get("configured_modes") or []
                    expected_configured = str(decode_contract["configured"])
                    expected_consumer = str(decode_contract["consumer"])
                    expected_decode_head_dims = list(decode_contract["gemma4_head_dims"])
                    expected_decode_routes = dict(decode_contract["routes_by_head_dim"])
                    observed_decode_head_dims = decode_observation.get("gemma4_head_dims") or []
                    observed_decode_routes = decode_observation.get("routes_by_head_dim") or {}
                    decode_fallbacks = int(decode_telemetry.get("fallback_count") or 0)
                    decode_rejections = int(decode_telemetry.get("rejected_count") or 0)
                    check(
                        f"pair_{row['pair']}_antfly_gqa_score_prework_consumer_contract",
                        expected_configured in configured_modes
                        and decode_observation.get("consumer") == expected_consumer
                        and observed_decode_head_dims == expected_decode_head_dims
                        and observed_decode_routes == expected_decode_routes
                        and (not decode_contract.get("forbid_fallback") or decode_fallbacks == 0)
                        and decode_rejections == 0,
                        "the configured score-prework consumer must launch the exact local/global Gemma 4 "
                        "routes for both head dimensions without fallback or rejection; "
                        f"configured={configured_modes!r} expected_configured={expected_configured!r} "
                        f"consumer={decode_observation.get('consumer')!r} "
                        f"expected_consumer={expected_consumer!r} head_dims={observed_decode_head_dims!r} "
                        f"expected_head_dims={expected_decode_head_dims!r} routes={observed_decode_routes!r} "
                        f"expected_routes={expected_decode_routes!r} fallbacks={decode_fallbacks} "
                        f"rejections={decode_rejections}",
                        performance=True,
                    )
            if args.profile == "headline":
                check(
                    f"pair_{row['pair']}_{engine}_authoritative_completion_usage",
                    sample.get("completion_token_accounting_source") == "final_sse_usage",
                    "completion token count must come from the terminal SSE usage event",
                    performance=True,
                )
                check(
                    f"pair_{row['pair']}_{engine}_authoritative_prompt_usage",
                    sample.get("prompt_token_accounting_source") == "final_sse_usage",
                    "prompt token count must come from the measured terminal SSE usage event",
                    performance=True,
                )
            token_summary = sample.get("prompt_token_ids") or {}
            check(
                f"pair_{row['pair']}_{engine}_prompt_token_id_count",
                token_summary.get("count") == args.expected_prompt_tokens,
                f"observed={token_summary.get('count')} expected={args.expected_prompt_tokens}",
            )
            for warmup in sample.get("warmups", []):
                prefix = f"pair_{row['pair']}_{engine}_warmup_{warmup.get('index')}"
                check(
                    f"{prefix}_prompt_tokens",
                    warmup.get("prompt_tokens") == args.expected_prompt_tokens,
                    f"observed={warmup.get('prompt_tokens')} expected={args.expected_prompt_tokens}",
                )
                check(
                    f"{prefix}_completion_tokens",
                    warmup.get("completion_tokens") == args.output_tokens,
                    f"observed={warmup.get('completion_tokens')} expected={args.output_tokens}",
                )
                check(
                    f"{prefix}_finish_reason",
                    warmup.get("finish_reason") == "length",
                    f"observed={warmup.get('finish_reason')!r} expected='length'",
                )
                warmup_content = str(warmup.get("content") or "")
                missing_warmup_terms = [
                    value for value in fixture.expected_output_substrings if value not in warmup_content
                ]
                check(
                    f"{prefix}_visible_content",
                    int(warmup.get("content_utf8_bytes") or 0) >= fixture.minimum_visible_utf8_bytes,
                    f"utf8_bytes={warmup.get('content_utf8_bytes')!r} minimum={fixture.minimum_visible_utf8_bytes}",
                )
                check(
                    f"{prefix}_semantic_output",
                    not missing_warmup_terms,
                    f"missing_required_substrings={missing_warmup_terms!r}",
                )
        antfly_prompt_digest = (row["antfly"].get("prompt_token_ids") or {}).get("sha256")
        llama_prompt_digest = (row["llama_cpp"].get("prompt_token_ids") or {}).get("sha256")
        check(
            f"pair_{row['pair']}_prompt_token_ids_equal",
            bool(antfly_prompt_digest) and antfly_prompt_digest == llama_prompt_digest,
            f"antfly={antfly_prompt_digest!r} llama_cpp={llama_prompt_digest!r}",
        )
    for engine in ENGINES:
        digests = {row[engine]["content_sha256"] for row in rows}
        check(f"{engine}_deterministic_output", len(digests) == 1, f"unique_content_digests={len(digests)}")
    check(
        "prompt_fixture_token_contract",
        args.expected_prompt_tokens == fixture.expected_prompt_tokens,
        f"configured={args.expected_prompt_tokens} fixture={fixture.expected_prompt_tokens}",
    )

    if args.profile == "headline":
        check("headline_median_total_ratio", total_ratio <= args.max_median_ratio, f"ratio={total_ratio:.6f} max={args.max_median_ratio:.6f}", performance=True)
        check("headline_bootstrap_ci", bootstrap["upper_95"] < args.max_ci_upper, f"upper_95={bootstrap['upper_95']:.6f} max_exclusive={args.max_ci_upper:.6f}", performance=True)
        for engine in ENGINES:
            cv = metrics[engine]["total_latency_ms"]["cv"]
            check(f"{engine}_latency_cv", cv <= args.max_cv, f"cv={cv:.6f} max={args.max_cv:.6f}", performance=True)
        check("headline_p95", p95_ratio <= args.max_p95_ratio, f"ratio={p95_ratio:.6f} max={args.max_p95_ratio:.6f}", performance=True)
        check("headline_ttft", ttft_ratio <= args.max_component_ratio, f"ratio={ttft_ratio:.6f} max={args.max_component_ratio:.6f}", performance=True)
        check("headline_decode", decode_ratio <= args.max_component_ratio, f"ratio={decode_ratio:.6f} max={args.max_component_ratio:.6f}", performance=True)
        ttft_ci_upper = float(paired_log_ratio_cis["ttft_ms"]["upper_95"])
        decode_ci_upper = float(paired_log_ratio_cis["decode_ms"]["upper_95"])
        check(
            "headline_ttft_paired_ci",
            ttft_ci_upper <= args.max_ttft_ci_upper,
            f"upper_95={ttft_ci_upper:.6f} max={args.max_ttft_ci_upper:.6f}",
            performance=True,
        )
        check(
            "headline_decode_paired_ci",
            decode_ci_upper <= args.max_decode_ci_upper,
            f"upper_95={decode_ci_upper:.6f} max={args.max_decode_ci_upper:.6f}",
            performance=True,
        )

    baseline_ratios: dict[str, float] | None = None
    if args.max_baseline_regression is not None:
        for engine in ENGINES:
            cv = metrics[engine]["total_latency_ms"]["cv"]
            check(
                f"baseline_profile_{engine}_latency_cv",
                cv <= args.max_cv,
                f"cv={cv:.6f} max={args.max_cv:.6f}",
                performance=True,
            )
        if baseline is None:
            check("frozen_antfly_baseline", False, "--baseline-evidence is required", performance=True)
        else:
            baseline_ratios = {}
            for metric in ("total_latency_ms", "ttft_ms", "decode_ms"):
                ratio = metrics["antfly"][metric]["median"] / baseline_median(baseline, metric)
                baseline_ratios[metric] = ratio
                check(
                    f"antfly_baseline_{metric}", ratio <= args.max_baseline_regression,
                    f"ratio={ratio:.6f} max={args.max_baseline_regression:.6f}", performance=True,
                )

    return {
        "metrics": metrics,
        "comparison": {
            "antfly_llama_median_total_ratio": total_ratio,
            "antfly_llama_median_ttft_ratio": ttft_ratio,
            "antfly_llama_median_decode_latency_ratio": decode_ratio,
            "antfly_llama_p95_total_ratio": p95_ratio,
            "paired_total_ratios": pair_ratios,
            "paired_bootstrap_95_ci": bootstrap,
            "paired_log_ratio_95_ci": paired_log_ratio_cis,
            "antfly_frozen_baseline_ratios": baseline_ratios,
        },
        "checks": checks,
        "passed": all(item["passed"] for item in checks if item["enforced"]),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    repo = pathlib.Path(__file__).resolve().parents[5]
    gemma4_scripts = repo / "zig/pkg/inference/scripts/gemma4"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--antfly-bin", type=pathlib.Path, default=repo / "zig/pkg/inference/zig-out/bin/antfly-inference")
    parser.add_argument("--llama-server-bin", type=pathlib.Path, default=pathlib.Path("/tmp/llama.cpp/build/bin/llama-server"))
    parser.add_argument("--model", type=pathlib.Path, required=True, help="Byte-identical GGUF used by both engines")
    parser.add_argument("--models-dir", type=pathlib.Path, default=repo / ".models")
    parser.add_argument(
        "--fixture",
        type=pathlib.Path,
        default=gemma4_scripts / "fixtures/gemma4_long_context_v1.json",
    )
    default_output = pathlib.Path(
        "/tmp/antfly-gemma4-long-e2e-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    )
    parser.add_argument("--output-dir", type=pathlib.Path, default=default_output)
    parser.add_argument("--profile", choices=sorted(PROFILE_DEFAULTS), default="headline")
    parser.add_argument("--backend", choices=("cuda", "metal", "native"), default="cuda")
    parser.add_argument(
        "--cuda-execution-profile",
        choices=sorted(CUDA_EXECUTION_PROFILES),
        default=CUDA_EXECUTION_PROFILE_DEFAULT,
        help="Versioned CUDA route contract included in benchmark provenance",
    )
    parser.add_argument("--cache-dtype", choices=("f16", "f32"))
    parser.add_argument("--context-size", type=int, default=4096)
    parser.add_argument("--output-tokens", type=int, default=300)
    parser.add_argument("--expected-prompt-tokens", type=int)
    for budget_name in CUDA_SERVER_BUDGET_MB:
        parser.add_argument(
            f"--antfly-{budget_name}-budget-mb",
            type=int,
            help=f"Antfly server {budget_name} memory budget in MiB (CUDA profile default: {CUDA_SERVER_BUDGET_MB[budget_name]})",
        )
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--startup-timeout", type=float, default=900.0)
    parser.add_argument("--request-timeout", type=float, default=900.0)
    parser.add_argument("--bootstrap-samples", type=int, default=10_000)
    parser.add_argument("--bootstrap-seed", type=int, default=20260730)
    parser.add_argument("--max-median-ratio", type=float, default=0.95)
    parser.add_argument("--max-ci-upper", type=float, default=1.0)
    parser.add_argument("--max-cv", type=float, default=0.03)
    parser.add_argument("--max-p95-ratio", type=float, default=1.0)
    parser.add_argument("--max-component-ratio", type=float, default=1.02)
    parser.add_argument("--max-ttft-ci-upper", type=float, default=1.02)
    parser.add_argument("--max-decode-ci-upper", type=float, default=1.02)
    parser.add_argument("--max-baseline-regression", type=float)
    parser.add_argument("--baseline-evidence", type=pathlib.Path)
    parser.add_argument("--baseline-sha256", help="Expected SHA-256 of frozen baseline evidence")
    parser.add_argument("--lockfile", type=pathlib.Path, help="Validate model/engine/config provenance against this lock")
    parser.add_argument("--lockfile-sha256", help="Expected SHA-256 of the reviewed benchmark lock")
    parser.add_argument("--require-lock", action="store_true")
    parser.add_argument("--antfly-server-prefix", default=os.environ.get("ANTFLY_LONG_E2E_SERVER_PREFIX", ""))
    parser.add_argument(
        "--antfly-env",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="Explicit Antfly CUDA execution-profile override; repeat for multiple values",
    )
    parser.add_argument("--llama-server-prefix", default=os.environ.get("LLAMA_LONG_E2E_SERVER_PREFIX", ""))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--enforce-performance", dest="enforce_performance", action="store_true")
    mode.add_argument("--collect-only", dest="enforce_performance", action="store_false")
    parser.set_defaults(enforce_performance=True)
    args = parser.parse_args(argv)
    defaults = PROFILE_DEFAULTS[args.profile]
    args.cache_dtype = args.cache_dtype or defaults["cache_dtype"]
    if args.max_baseline_regression is None:
        args.max_baseline_regression = defaults["max_baseline_regression"]
    return args


def validate_args(args: argparse.Namespace, fixture: PromptFixture) -> None:
    missing = [path for path in (args.antfly_bin, args.llama_server_bin, args.model) if not path.is_file()]
    if missing:
        raise ValueError("missing required file(s): " + ", ".join(str(path) for path in missing))
    if not args.models_dir.is_dir():
        raise ValueError(f"models directory does not exist: {args.models_dir}")
    args.antfly_model_id = antfly_model_identifier(args.model, args.models_dir)
    if args.warmups < 1 or args.repeats < 1:
        raise ValueError("warmups and repeats must be positive")
    if args.enforce_performance and args.repeats < 10:
        raise ValueError("performance enforcement requires at least 10 paired samples")
    if args.output_tokens < 1 or args.context_size < 1 or args.bootstrap_samples < 1:
        raise ValueError("output/context/bootstrap sizes must be positive")
    server_budget_mb = antfly_server_budget_mb(args)
    if any(value < 0 for value in server_budget_mb.values()):
        raise ValueError("Antfly server memory budgets must be non-negative")
    if args.expected_prompt_tokens is None:
        args.expected_prompt_tokens = fixture.expected_prompt_tokens
    if args.expected_prompt_tokens < 1:
        raise ValueError("expected prompt tokens must be positive")
    if args.expected_prompt_tokens + args.output_tokens > args.context_size:
        raise ValueError("prompt plus requested output exceeds the configured context size")
    thresholds = (
        args.max_median_ratio,
        args.max_ci_upper,
        args.max_cv,
        args.max_p95_ratio,
        args.max_component_ratio,
        args.max_ttft_ci_upper,
        args.max_decode_ci_upper,
    )
    if any(not math.isfinite(value) or value <= 0 for value in thresholds):
        raise ValueError("performance thresholds must be finite positive values")
    if args.max_baseline_regression is not None and (
        not math.isfinite(args.max_baseline_regression) or args.max_baseline_regression <= 0
    ):
        raise ValueError("max baseline regression must be finite and positive")
    profile_baseline_max = PROFILE_DEFAULTS[args.profile]["max_baseline_regression"]
    if args.enforce_performance and profile_baseline_max is not None:
        if args.max_baseline_regression is None or args.max_baseline_regression > profile_baseline_max:
            raise ValueError(
                f"{args.profile} max-baseline-regression cannot be looser than {profile_baseline_max}"
            )
        if args.max_cv > MAX_FROZEN_BASELINE_CV:
            raise ValueError(
                f"{args.profile} max-cv cannot be looser than {MAX_FROZEN_BASELINE_CV}"
            )
        if args.baseline_evidence is None:
            raise ValueError(f"{args.profile} enforcement requires --baseline-evidence")
        if args.baseline_sha256 is None:
            raise ValueError(f"{args.profile} enforcement requires --baseline-sha256")
        if (
            len(args.baseline_sha256) != 64
            or args.baseline_sha256.lower() != args.baseline_sha256
            or any(character not in "0123456789abcdef" for character in args.baseline_sha256)
        ):
            raise ValueError("--baseline-sha256 must be 64 lowercase hexadecimal characters")
        if not args.require_lock:
            raise ValueError(f"{args.profile} enforcement requires a reviewed --lockfile and --require-lock")
    if args.require_lock and args.lockfile is None:
        raise ValueError("--require-lock requires --lockfile")
    if args.lockfile_sha256 is not None and args.lockfile is None:
        raise ValueError("--lockfile-sha256 requires --lockfile")
    if args.enforce_performance and args.lockfile_sha256 is None:
        raise ValueError("performance enforcement requires --lockfile-sha256 for the reviewed lock")
    if args.lockfile_sha256 is not None and (
        len(args.lockfile_sha256) != 64
        or args.lockfile_sha256.lower() != args.lockfile_sha256
        or any(character not in "0123456789abcdef" for character in args.lockfile_sha256)
    ):
        raise ValueError("--lockfile-sha256 must be 64 lowercase hexadecimal characters")
    if args.profile == "headline" and args.enforce_performance:
        if not args.require_lock:
            raise ValueError("headline enforcement requires a reviewed --lockfile and --require-lock")
        if args.antfly_server_prefix:
            raise ValueError("headline enforcement forbids an opaque Antfly server prefix; use --antfly-env")
        if args.llama_server_prefix:
            raise ValueError("headline enforcement forbids an opaque llama.cpp server prefix; use explicit arguments")
        if args.backend != "cuda" or args.cache_dtype != "f16":
            raise ValueError("headline enforcement requires CUDA with matched F16 K/V caches")
        if args.context_size != 4096 or args.output_tokens != 300 or args.warmups != 2:
            raise ValueError("headline enforcement requires context-size=4096, output-tokens=300, and warmups=2")
        if args.expected_prompt_tokens != fixture.expected_prompt_tokens:
            raise ValueError("headline enforcement requires the fixture's exact prompt token count")
        if args.bootstrap_samples < 10_000:
            raise ValueError("headline enforcement requires at least 10000 paired bootstrap resamples")
        accepted_maxima = {
            "max_median_ratio": 0.95,
            "max_ci_upper": 1.0,
            "max_cv": 0.03,
            "max_p95_ratio": 1.0,
            "max_component_ratio": 1.02,
            "max_ttft_ci_upper": 1.02,
            "max_decode_ci_upper": 1.02,
        }
        for field, accepted in accepted_maxima.items():
            if getattr(args, field) > accepted:
                raise ValueError(f"headline {field.replace('_', '-')} cannot be looser than {accepted}")
    if args.profile in {"e2b-regression", "e4b-regression"} and args.enforce_performance and args.cache_dtype != "f16":
        raise ValueError("Gemma 4 regression enforcement requires F16 K/V caches")
    if args.profile == "f32-control" and args.enforce_performance and args.cache_dtype != "f32":
        raise ValueError("F32 control enforcement requires F32 K/V caches")


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        fixture = load_fixture(args.fixture)
        validate_args(args, fixture)
        args.antfly_execution_profile = resolve_antfly_execution_profile(args, fixture, dict(os.environ))
        validate_headline_execution_profile(args, fixture, args.antfly_execution_profile)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    args.output_dir.mkdir(parents=True, exist_ok=False)

    try:
        provenance = collect_provenance(args, fixture)
    except (OSError, RuntimeError) as exc:
        raise SystemExit(f"could not collect stable benchmark provenance: {exc}") from exc
    observed_lock = build_lock(args, fixture, provenance)
    write_json(args.output_dir / "benchmark.lock.json", observed_lock)
    preflight_errors = provenance_validation_errors(args, provenance)
    reviewed_lockfile_identity = None
    if args.lockfile is not None:
        try:
            lockfile_source = args.lockfile.read_bytes()
            observed_lockfile_sha256 = sha256_bytes(lockfile_source)
            if (
                args.lockfile_sha256 is not None
                and observed_lockfile_sha256 != args.lockfile_sha256
            ):
                preflight_errors.append(
                    "reviewed lockfile SHA-256 mismatch: "
                    f"expected={args.lockfile_sha256} observed={observed_lockfile_sha256}"
                )
            expected_lock = json.loads(lockfile_source)
            reviewed_lockfile_identity = {
                "path": str(args.lockfile.resolve()),
                "bytes": len(lockfile_source),
                "sha256": observed_lockfile_sha256,
                "expected_sha256": args.lockfile_sha256,
            }
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            preflight_errors.append(f"could not read benchmark lock: {exc}")
        else:
            preflight_errors.extend(lock_errors(expected_lock, observed_lock))
    if preflight_errors:
        write_json(args.output_dir / "preflight_errors.json", preflight_errors)
        raise SystemExit("\n".join(preflight_errors))

    baseline = None
    baseline_identity = None
    if args.baseline_evidence is not None:
        try:
            baseline_source = args.baseline_evidence.read_bytes()
        except OSError as exc:
            raise SystemExit(f"could not read baseline evidence: {exc}") from exc
        baseline_sha256 = sha256_bytes(baseline_source)
        if args.baseline_sha256 is not None and baseline_sha256 != args.baseline_sha256:
            raise SystemExit(
                "frozen baseline SHA-256 mismatch: "
                f"expected={args.baseline_sha256} observed={baseline_sha256}"
            )
        try:
            baseline = json.loads(baseline_source)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SystemExit(f"could not read baseline evidence: {exc}") from exc
        compatibility_errors = [
            *baseline_compatibility_errors(baseline, observed_lock),
            *baseline_quality_errors(baseline, args.profile),
        ]
        if compatibility_errors:
            raise SystemExit("\n".join(compatibility_errors))
        baseline_identity = baseline_evidence_identity(args.baseline_evidence, baseline, baseline_source)
        baseline_identity["expected_sha256"] = args.baseline_sha256

    try:
        prompt_token_contract = collect_antfly_prompt_token_contract(args, fixture)
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc
    write_json(args.output_dir / "prompt_token_contract.json", prompt_token_contract)
    if prompt_token_contract["count"] != args.expected_prompt_tokens:
        raise SystemExit(
            f"Antfly prompt token count is {prompt_token_contract['count']}, expected {args.expected_prompt_tokens}"
        )

    runtime_guards: list[dict[str, Any]] = []

    def guard_runtime(stage: str) -> None:
        guard = capture_runtime_guard(args, provenance, stage)
        runtime_guards.append(guard)
        provenance["runtime_guards"] = runtime_guards
        write_json(args.output_dir / "runtime_guards.json", runtime_guards)
        errors = runtime_guard_errors(args, guard)
        if errors:
            raise SystemExit("\n".join(errors))

    rows: list[dict[str, Any]] = []
    for pair_index in range(1, args.repeats + 1):
        order = paired_order(pair_index)
        samples: dict[str, dict[str, Any]] = {}
        for engine in order:
            guard_runtime(f"before-pair-{pair_index:02d}-{engine}")
            print(f"RUN pair={pair_index}/{args.repeats} engine={engine}", flush=True)
            samples[engine] = run_engine_sample(args, fixture, prompt_token_contract, pair_index, engine)
            guard_runtime(f"after-pair-{pair_index:02d}-{engine}")
        rows.append({"pair": pair_index, "order": list(order), **samples})
        with (args.output_dir / "samples.jsonl").open("a", encoding="utf-8") as output:
            output.write(json.dumps(rows[-1], sort_keys=True, allow_nan=False) + "\n")

    try:
        final_model_bundle = antfly_model_bundle_provenance(args.model)
    except (OSError, RuntimeError) as exc:
        raise SystemExit(f"could not verify final model-bundle provenance: {exc}") from exc
    provenance["antfly_model_bundle_post_benchmark"] = final_model_bundle
    if final_model_bundle["sha256"] != provenance["antfly_model_bundle"]["sha256"]:
        raise SystemExit(
            "Antfly model-bundle contents changed during the benchmark: "
            f"initial={provenance['antfly_model_bundle']['sha256']} "
            f"final={final_model_bundle['sha256']}"
        )

    evaluated = evaluate(args, fixture, rows, baseline)
    contract = {
        "profile": args.profile,
        "backend": args.backend,
        "cache_dtype": args.cache_dtype,
        "prompt_fixture_id": fixture.fixture_id,
        "prompt_user_utf8_bytes": fixture.user_utf8_bytes,
        "prompt_user_sha256": fixture.user_sha256,
        "reference_prompt_utf8_bytes": fixture.reference_prompt_utf8_bytes,
        "reference_prompt_sha256": fixture.reference_prompt_sha256,
        "prompt_tokens": args.expected_prompt_tokens,
        "output_tokens": args.output_tokens,
        "warmups_per_server": args.warmups,
        "paired_samples": args.repeats,
        "paired_order": [list(paired_order(index)) for index in range(1, args.repeats + 1)],
        "gpu_process_isolation": (
            "benchmark servers run sequentially; selected-GPU state and process isolation are "
            "verified before and after every server sample"
        ),
        "measured_boundary": "HTTP request submission to final streamed event over warm keep-alive connection",
        "model_loading_included": False,
        "connection_establishment_included": False,
        "sampling": "greedy",
        "chat_template_kwargs": dict(fixture.chat_template_kwargs),
        "response_channel": fixture.response_channel,
        "expected_output_substrings": list(fixture.expected_output_substrings),
        "minimum_visible_utf8_bytes": fixture.minimum_visible_utf8_bytes,
        "prompt_cache": False,
        "context_size": args.context_size,
        "prompt_token_ids_sha256": prompt_token_contract["sha256"],
        "prompt_token_ids_contract": (
            "Antfly CLI tokenization of the locked public-channel prompt versus llama.cpp /apply-template with the "
            "measured chat-template kwargs, then /tokenize on the byte-identical locked reference prompt"
        ),
        "antfly_execution_profile": args.antfly_execution_profile["name"],
        "antfly_execution_profile_sha256": args.antfly_execution_profile["sha256"],
        "antfly_server_max_idle_prefill_chunk_size": SERVER_MAX_IDLE_PREFILL_CHUNK_SIZE,
        "frozen_baseline_sha256": (
            baseline_identity["file"].get("sha256") if baseline_identity is not None else None
        ),
        "performance_enforced": args.enforce_performance,
        "performance_thresholds": performance_threshold_contract(args),
    }
    evidence = {
        "schema": EVIDENCE_SCHEMA,
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "contract": contract,
        "provenance": provenance,
        "provenance_sha256": canonical_sha256(provenance),
        "baseline_evidence": baseline_identity,
        "reviewed_lockfile": reviewed_lockfile_identity,
        "lock": observed_lock,
        "rows": rows,
        **evaluated,
    }
    evidence_path = args.output_dir / "evidence.json"
    write_json(evidence_path, evidence)
    fields = (
        "pair", "order", "antfly_total_ms", "llama_total_ms", "total_ratio",
        "antfly_ttft_ms", "llama_ttft_ms", "antfly_decode_ms", "llama_decode_ms",
    )
    with (args.output_dir / "paired_samples.tsv").open("w", encoding="utf-8") as output:
        output.write("\t".join(fields) + "\n")
        for row in rows:
            values = (
                row["pair"], "/".join(row["order"]), row["antfly"]["total_latency_ms"],
                row["llama_cpp"]["total_latency_ms"],
                row["antfly"]["total_latency_ms"] / row["llama_cpp"]["total_latency_ms"],
                row["antfly"]["ttft_ms"], row["llama_cpp"]["ttft_ms"],
                row["antfly"]["decode_ms"], row["llama_cpp"]["decode_ms"],
            )
            output.write("\t".join(str(value) for value in values) + "\n")

    evidence_manifest = write_evidence_manifest(args.output_dir)

    ratio = evidence["comparison"]["antfly_llama_median_total_ratio"]
    ci = evidence["comparison"]["paired_bootstrap_95_ci"]
    print(
        f"gemma4_long_e2e profile={args.profile} ratio={ratio:.4f} "
        f"ci95=[{ci['lower_95']:.4f},{ci['upper_95']:.4f}] "
        f"passed={str(evidence['passed']).lower()} output={evidence_path} "
        f"manifest_sha256={evidence_manifest['files_sha256']}"
    )
    if not evidence["passed"]:
        for item in evidence["checks"]:
            if item["enforced"] and not item["passed"]:
                print(f"FAIL {item['name']}: {item['detail']}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
