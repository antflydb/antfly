#!/usr/bin/env python3
"""Validate exact tokens and paired throughput for a CUDA kernel candidate."""

from __future__ import annotations

import argparse
import dataclasses
import datetime
import enum
import hashlib
import json
import math
import os
import pathlib
import re
import signal
import statistics
import subprocess
import sys

SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))

import benchmark_gemma4_long_e2e_server as warm_server_provenance
import gemma4_cuda_l4_release_gate as release_provenance

from paired_benchmark import (
    balanced_pair_order,
    canonical_sha256,
    distribution,
    paired_log_ratio_ci,
    sha256_file,
    write_evidence_manifest,
)


TOKEN_IDS_RE = re.compile(r"^token_ids:(?P<ids>(?:\s+-?\d+)*)\s*$", re.MULTILINE)
PROMPT_TOKEN_IDS_RE = re.compile(r"^prompt_token_ids:(?P<ids>(?:\s+-?\d+)*)\s*$", re.MULTILINE)
ENVIRONMENT_VARIABLE_RE = re.compile(r"[A-Z_][A-Z0-9_]*")
DEFAULT_PROMPTS = (
    "Write one sentence about ants.",
    "Explain why the sky is blue in two sentences.",
    "List three prime numbers and nothing else.",
)
STRICT_PROVENANCE_SCHEMA = "antfly.gemma4_cuda_candidate.provenance.v1"
STRICT_RUNTIME_ENVIRONMENT_SCHEMA = "antfly.cuda_candidate.runtime_environment.v1"
ARTIFACT_FRESHNESS_SCHEMA = "antfly.gemma4_cuda_candidate.artifact_freshness.v1"
RUNTIME_GUARD_SCHEMA = "antfly.gemma4_cuda_candidate.runtime_guard.v1"
REQUIRED_CUDA_ARTIFACTS = (
    "generated_manifest",
    "renderer_source",
    "compiler_source",
    "runtime_bundle_source",
    "runtime_ptx",
    "runtime_fatbin",
    "runtime_sm89_cubin",
)
STRICT_RUNTIME_ENVIRONMENT_KEYS = (
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
STRICT_ZIG_GLOBAL_CACHE_DIR = pathlib.Path("/tmp/antfly-releasefast-global-cache")


class CandidateKind(enum.Enum):
    GENERATED_ATTENTION = "generated-attention"
    Q4_0_Q8_1_LM_HEAD_ARGMAX = "q4-0-q8-1-lm-head-argmax"
    Q6_K_Q8_1_LM_HEAD_ARGMAX = "q6-k-q8-1-lm-head-argmax"
    Q4_0_Q8_1_E2B_FFN = "q4-0-q8-1-e2b-ffn"
    Q4_0_Q8_1_E2B_FFN_PAIR_ONLY = "q4-0-q8-1-e2b-ffn-pair-only"
    Q4_0_E2B_FFN_EXACT = "q4-0-e2b-ffn-exact"

    def __str__(self) -> str:
        return self.value


@dataclasses.dataclass(frozen=True)
class RouteCounter:
    name: str
    label: str


@dataclasses.dataclass(frozen=True)
class RouteCountExpectation:
    name: str
    exact_count: int

    def __post_init__(self) -> None:
        if not self.name or self.exact_count < 1:
            raise ValueError("qualification route count requires a counter name and positive exact count")


@dataclasses.dataclass(frozen=True)
class QualificationWorkload:
    fixture_id: str
    fixture_file_sha256: str
    benchmark_prompt_sha256: str
    benchmark_prompt_tokens: int
    lengths: tuple[int, ...]
    cache_dtype: str
    prefill_chunk_size: int
    capture_kv_capacity: int

    def __post_init__(self) -> None:
        if not self.fixture_id:
            raise ValueError("qualification workload fixture ID must be non-empty")
        for label, digest in (
            ("fixture file", self.fixture_file_sha256),
            ("benchmark prompt", self.benchmark_prompt_sha256),
        ):
            if not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ValueError(f"qualification workload {label} SHA-256 must be lowercase hexadecimal")
        if self.benchmark_prompt_tokens < 1:
            raise ValueError("qualification workload prompt-token count must be positive")
        if not self.lengths or any(length < 1 for length in self.lengths):
            raise ValueError("qualification workload output lengths must be positive")
        if not self.cache_dtype:
            raise ValueError("qualification workload cache dtype must be non-empty")
        if self.prefill_chunk_size < 1 or self.capture_kv_capacity < 1:
            raise ValueError("qualification workload chunk size and capture capacity must be positive")


@dataclasses.dataclass(frozen=True)
class CandidateSpec:
    kernel_id: str
    environment_variable: str
    required_route_counters: tuple[RouteCounter, ...]
    forbidden_route_counters: tuple[RouteCounter, ...] = ()
    required_baseline_route_counters: tuple[RouteCounter, ...] = ()
    legacy_kind: CandidateKind | None = None
    requires_explicit_model: bool = False
    fixed_comparison_environment: tuple[tuple[str, str], ...] = ()
    baseline_gate_value: str = "0"
    candidate_gate_value: str = "1"
    route_phase: str = "decode"
    qualification_workload: QualificationWorkload | None = None
    qualification_route_counts: tuple[RouteCountExpectation, ...] = ()
    require_persistent_replay: bool = False

    def __post_init__(self) -> None:
        if not self.kernel_id or not re.fullmatch(r"[a-z0-9][a-z0-9._/-]*", self.kernel_id):
            raise ValueError("candidate kernel ID must be a non-empty lowercase catalog ID")
        if not self.environment_variable or not ENVIRONMENT_VARIABLE_RE.fullmatch(self.environment_variable):
            raise ValueError("candidate environment variable must be a valid environment name")
        if not self.required_route_counters:
            raise ValueError("candidate must require at least one route counter")
        names = tuple(counter.name for counter in self.required_route_counters)
        if len(set(names)) != len(names):
            raise ValueError("candidate route counters must be unique")
        forbidden_names = tuple(counter.name for counter in self.forbidden_route_counters)
        if len(set(forbidden_names)) != len(forbidden_names):
            raise ValueError("candidate forbidden counters must be unique")
        baseline_names = tuple(counter.name for counter in self.required_baseline_route_counters)
        if len(set(baseline_names)) != len(baseline_names):
            raise ValueError("required baseline route counters must be unique")
        if set(names) & set(forbidden_names):
            raise ValueError("required route counters and forbidden counters must be disjoint")
        if set(names) & set(baseline_names):
            raise ValueError("candidate and baseline required route counters must be disjoint")
        qualification_count_names = tuple(item.name for item in self.qualification_route_counts)
        if len(set(qualification_count_names)) != len(qualification_count_names):
            raise ValueError("qualification route-count counters must be unique")
        if not set(qualification_count_names).issubset(names):
            raise ValueError("qualification route-count counters must be required candidate routes")
        for counter in (
            self.required_route_counters
            + self.forbidden_route_counters
            + self.required_baseline_route_counters
        ):
            if not counter.name or not counter.label:
                raise ValueError("route counter names and labels must be non-empty")
        fixed_names = tuple(name for name, _ in self.fixed_comparison_environment)
        if len(set(fixed_names)) != len(fixed_names):
            raise ValueError("fixed comparison environment variables must be unique")
        if self.environment_variable in fixed_names:
            raise ValueError("fixed comparison environment must not override the selected candidate gate")
        for name, value in self.fixed_comparison_environment:
            if not ENVIRONMENT_VARIABLE_RE.fullmatch(name):
                raise ValueError("fixed comparison environment variable name is invalid")
            if any(ord(character) < 32 or ord(character) == 127 for character in value):
                raise ValueError("fixed comparison environment value contains an unsupported control character")
        for label, value in (
            ("baseline", self.baseline_gate_value),
            ("candidate", self.candidate_gate_value),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"{label} candidate gate value must be non-empty")
            if any(ord(character) < 32 or ord(character) == 127 for character in value):
                raise ValueError(f"{label} candidate gate value contains an unsupported control character")
        if self.baseline_gate_value == self.candidate_gate_value:
            raise ValueError("baseline and candidate gate values must differ")
        if not isinstance(self.route_phase, str) or self.route_phase not in {"decode", "prefill"}:
            raise ValueError("candidate route phase must be decode or prefill")
        if not isinstance(self.require_persistent_replay, bool):
            raise ValueError("candidate persistent-replay requirement must be boolean")

    @property
    def route_counter(self) -> str:
        """Retain the primary-counter interface used by existing JSON consumers."""
        return self.required_route_counters[0].name

    @property
    def catalog_id(self) -> str:
        return self.kernel_id

    @property
    def route_counters(self) -> tuple[RouteCounter, ...]:
        """Compatibility alias for pre-catalog callers."""
        return self.required_route_counters

    @property
    def candidate_forbidden_counters(self) -> tuple[RouteCounter, ...]:
        """Compatibility alias for pre-catalog callers."""
        return self.forbidden_route_counters

    @property
    def kind(self) -> CandidateKind | None:
        """Compatibility alias for legacy enum-backed candidates."""
        return self.legacy_kind


@dataclasses.dataclass(frozen=True)
class TimingMetadata:
    counter_group: str = "cuda"
    throughput_field: str = "decode_tok_per_s"
    throughput_unit: str = "tokens_per_second"
    require_persistent_replay: bool = True
    persistent_replay_counter: str = "graph_capture_persistent_replays"
    graph_discard_counter: str = "graph_capture_discards"
    graph_capacity_skip_counter: str = "graph_capture_capacity_skips"

    def __post_init__(self) -> None:
        for field_name in (
            self.counter_group,
            self.throughput_field,
            self.throughput_unit,
            self.persistent_replay_counter,
            self.graph_discard_counter,
            self.graph_capacity_skip_counter,
        ):
            if not field_name:
                raise ValueError("timing metadata fields must be non-empty")


DEFAULT_TIMING_METADATA = TimingMetadata()


GENERATED_ATTENTION_KERNEL_ID = "cuda.attention.gqa.decode.generated"
SCORE_PREWORK_ATTENTION_KERNEL_ID = "cuda.attention.gqa.decode.score_prework"
SCORE_PREWORK_TILED64_ATTENTION_KERNEL_ID = "cuda.attention.gqa.decode.score_prework.tiled64"
GQA_PREFILL_TILED_F16_EXACT_KERNEL_ID = "cuda.attention.gqa.prefill.tiled_f16_exact"
GQA_PREFILL_TILED_F16_WARP_KERNEL_ID = "cuda.attention.gqa.prefill.tiled_f16_warp"
GQA_PREFILL_FLASH_F16_SM89_KERNEL_ID = "cuda.attention.gqa.prefill.flash_f16_sm89"
GQA_DECODE_SPLITK_ONLINE_SM89_KERNEL_ID = "cuda.attention.gqa.decode.splitk_online_sm89"
Q4_0_Q8_1_LM_HEAD_ARGMAX_KERNEL_ID = "cuda.quant.q4_0-q8_1.lm_head.argmax"
Q6_K_Q8_1_LM_HEAD_ARGMAX_KERNEL_ID = "cuda.quant.q6_k-q8_1.lm_head.argmax"
Q4_0_Q8_1_FFN_KERNEL_ID = "cuda.quant.q4_0-q8_1.ffn.generated"
Q4_0_Q8_1_E2B_FFN_PAIR_ONLY_KERNEL_ID = "cuda.quant.q4_0-q8_1.ffn.e2b.pair_only"
Q4_0_E2B_FFN_EXACT_KERNEL_ID = "cuda.quant.q4_0.ffn.e2b.f32_exact"
Q4_0_GGML_Q8_1_E2B_FFN_KERNEL_ID = "cuda.decode.q4_0.ggml_q8_1_e2b_ffn"
CUBLASLT_BF16_PREFILL_SM89_KERNEL_ID = "cuda.cublaslt.bf16.prefill.sm89"
PLE_GATE_BF16_MIRROR_FIRST_SM89_E2B_KERNEL_ID = (
    "cuda.ple.gate.prefill.bf16_mirror_first.sm89_e2b"
)
GENERATED_ATTENTION_SPLIT_KV_MIN_TOKENS_ENV = "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SPLIT_KV_MIN_TOKENS"
GENERATED_ATTENTION_SPLIT_KV_SPLITS_ENV = "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SPLIT_KV_SPLITS"
DEFAULT_CAPTURE_KV_PROMPT_HEADROOM = 1024
SUPPORTED_CACHE_DTYPES = ("f16", "f32", "int8", "fp8", "int4", "polar4", "turbo3")
CAPTURE_KV_CAPACITY_ENV = "ANTFLY_CAPTURE_FORCE_KV_CAPACITY"
RUNTIME_CAPTURE_KV_CAPACITY_ENV = "ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY"
CAPTURE_KV_CAPACITY_ENVIRONMENT_VARIABLES = frozenset(
    (CAPTURE_KV_CAPACITY_ENV, RUNTIME_CAPTURE_KV_CAPACITY_ENV)
)
QUALIFICATION_SCHEMA = "antfly.cuda_candidate_qualification.v2"
RAW_SAMPLE_SCHEMA = "antfly.cuda_candidate_sample.v1"
LATENCY_METRICS = ("total_latency_ms", "ttft_ms", "decode_ms")
GEMMA4_LONG_CONTEXT_WORKLOAD = QualificationWorkload(
    fixture_id="gemma4-search-retrieval-long-v1",
    fixture_file_sha256="7906b53bcbf58e94f5127acaf3b531064da6180b4157d6d1f13550a25ad03ba6",
    benchmark_prompt_sha256="0f9791a0344f4e302f60a6ad2cdaee80efe9212f00d49bfccd499c21cf64a6ef",
    benchmark_prompt_tokens=2051,
    lengths=(300,),
    cache_dtype="f16",
    prefill_chunk_size=512,
    capture_kv_capacity=2432,
)
QUALIFICATION_PROFILES = {
    # Preserve the historical CLI for ad-hoc parity checks. Performance
    # promotion must select one of the fixed-sample profiles below.
    "legacy": {
        "focus": "ad_hoc",
        "require_locked_prompt_fixture": False,
        "samples": None,
        "min_candidate_ratio": 0.0,
        "max_cv": 1.0,
        "require_phase_metrics": False,
        "require_full_route_coverage": False,
        "max_total_latency_ratio": None,
        "max_ttft_ratio": None,
        "max_decode_latency_ratio": None,
        "max_total_ci_upper": None,
        "max_ttft_ci_upper": None,
        "max_decode_ci_upper": None,
    },
    "screening": {
        "focus": "decode",
        "require_locked_prompt_fixture": False,
        "samples": 5,
        "min_candidate_ratio": 0.98,
        "max_cv": 0.03,
        "require_phase_metrics": True,
        "require_full_route_coverage": True,
        "max_total_latency_ratio": 1.02,
        "max_ttft_ratio": 1.03,
        "max_decode_latency_ratio": 1.02,
        "max_total_ci_upper": 1.05,
        "max_ttft_ci_upper": 1.06,
        "max_decode_ci_upper": 1.05,
    },
    "promotion": {
        "focus": "decode",
        "require_locked_prompt_fixture": False,
        "samples": 10,
        "min_candidate_ratio": 1.02,
        "max_cv": 0.02,
        "require_phase_metrics": True,
        "require_full_route_coverage": True,
        "max_total_latency_ratio": 0.99,
        "max_ttft_ratio": 1.01,
        "max_decode_latency_ratio": 0.98,
        "max_total_ci_upper": 1.00,
        "max_ttft_ci_upper": 1.02,
        "max_decode_ci_upper": 1.00,
    },
    "prefill-screening": {
        "focus": "prefill",
        "require_locked_prompt_fixture": True,
        "samples": 5,
        # Decode is a non-regression control. Improvement is required from the
        # CLI prefill/first-token proxy and fixed-work total latency instead.
        "min_candidate_ratio": 0.98,
        "max_cv": 0.03,
        "require_phase_metrics": True,
        "require_full_route_coverage": True,
        "max_total_latency_ratio": 0.98,
        "max_ttft_ratio": 0.98,
        "max_decode_latency_ratio": 1.02,
        "max_total_ci_upper": 1.03,
        "max_ttft_ci_upper": 1.03,
        "max_decode_ci_upper": 1.05,
    },
    "prefill-promotion": {
        "focus": "prefill",
        "require_locked_prompt_fixture": True,
        "samples": 10,
        "min_candidate_ratio": 0.99,
        "max_cv": 0.02,
        "require_phase_metrics": True,
        "require_full_route_coverage": True,
        "max_total_latency_ratio": 0.98,
        "max_ttft_ratio": 0.98,
        "max_decode_latency_ratio": 1.01,
        "max_total_ci_upper": 1.00,
        "max_ttft_ci_upper": 1.00,
        "max_decode_ci_upper": 1.02,
    },
}

# The normal Gemma QAT tuning profile enables Q8_1 pair/down paths. This
# candidate instead mirrors the raw-Q4_0 F32 pair-activation and tile4 down
# kernels, so both validator arms must use that same F32 route.
EXACT_E2B_FFN_F32_COMPARISON_ENVIRONMENT = (
    ("ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_Q8_1_PRECOMPUTE", "0"),
    ("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN", "0"),
    ("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_CATALOG_FFN_CANDIDATES", "0"),
    ("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY", "0"),
    ("ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_DP4A", "0"),
    ("ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_DP4A", "0"),
    ("ANTFLY_INFERENCE_CUDA_Q4_0_GATED_DOWN_Q8_1_DP4A", "0"),
    ("ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_TILE8", "0"),
    ("ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_TILE4_W4", "0"),
    ("ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_MMV", "1"),
    ("ANTFLY_INFERENCE_CUDA_DEQUANTIZE_Q4_0_MATRIX_WEIGHTS_BF16", "0"),
    ("TERMITE_CUDA_DEQUANTIZE_QUANT_WEIGHTS", "0"),
)

E2B_FFN_COUPLED_COMPARISON_ENVIRONMENT = (
    ("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT", "0"),
    ("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY", "0"),
)

# The pair-only candidate changes exactly one boundary: generated gate/up
# activation writes the same Q8_1 bytes as the staged F32 pair + quantize path,
# while the existing handwritten Q8_1 down projection remains fixed in both
# arms. Lock competing E2B candidate routes out of the comparison.
E2B_FFN_PAIR_ONLY_COMPARISON_ENVIRONMENT = (
    ("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN", "0"),
    ("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_CATALOG_FFN_CANDIDATES", "0"),
    ("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT", "0"),
    ("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_PAIR_Q8", "0"),
    ("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_DOWN_Q8", "0"),
)

SCORE_PREWORK_ATTENTION_COMPARISON_ENVIRONMENT = (
    ("ANTFLY_CUDA_DISABLE_TURBOQUANT_COMPRESSED_V", "1"),
    ("ANTFLY_INFERENCE_CUDA_TURBOQUANT_MIN_TOKENS", "0"),
    ("ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION", "0"),
    ("ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE", "0"),
)

SCORE_PREWORK_TILED64_ATTENTION_COMPARISON_ENVIRONMENT = (
    *SCORE_PREWORK_ATTENTION_COMPARISON_ENVIRONMENT,
    ("ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK", "1"),
)

# Both arms use the exact versioned E2B/L4 tuning contract. The only changing
# value is the typed split-K decode gate owned by CandidateSpec.
SPLITK_ONLINE_SM89_COMPARISON_ENVIRONMENT = tuple(
    warm_server_provenance.GEMMA4_E2B_SM89_FLASH_COMMON_ENV.items()
)

GGML_Q8_1_E2B_FFN_COMPARISON_ENVIRONMENT = (
    ("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN", "0"),
    ("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_CATALOG_FFN_CANDIDATES", "0"),
    ("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT", "0"),
    ("ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY", "0"),
    ("ANTFLY_INFERENCE_CUDA_DEQUANTIZE_Q4_0_MATRIX_WEIGHTS_BF16", "0"),
    ("ANTFLY_INFERENCE_CUDA_Q4_0_WEIGHTS_BF16_PREFILL", "0"),
)

CUBLASLT_BF16_PREFILL_COMPARISON_ENVIRONMENT = (
    ("ANTFLY_INFERENCE_CUDA_DEQUANTIZE_Q4_0_MATRIX_WEIGHTS_BF16", "0"),
    ("ANTFLY_INFERENCE_CUDA_Q4_0_WEIGHTS_BF16_PREFILL", "1"),
    ("TERMITE_CUDA_DEQUANTIZE_QUANT_WEIGHTS", "0"),
)

# Both arms retain the locked E2B Flash-prefill + split-K decode contract and
# hybrid Q4/BF16 residency. The only changed boundary is whether the PLE gate
# consumes its already-admitted BF16 mirror through the generic cuBLASLt
# fallback or keeps the fused Q8_1 + Q4_0 path.
PLE_GATE_BF16_MIRROR_FIRST_COMPARISON_ENVIRONMENT = (
    *SPLITK_ONLINE_SM89_COMPARISON_ENVIRONMENT,
    ("ANTFLY_INFERENCE_CUDA_GQA_DECODE_PROFILE", "required-splitk-online-sm89"),
)


CANDIDATE_CATALOG = {
    GENERATED_ATTENTION_KERNEL_ID: CandidateSpec(
        kernel_id=GENERATED_ATTENTION_KERNEL_ID,
        environment_variable="ANTFLY_GENERATED_ATTENTION_DECODE",
        required_route_counters=(RouteCounter("launch_attention_gqa_decode_generated", "generated attention"),),
        legacy_kind=CandidateKind.GENERATED_ATTENTION,
    ),
    SCORE_PREWORK_ATTENTION_KERNEL_ID: CandidateSpec(
        kernel_id=SCORE_PREWORK_ATTENTION_KERNEL_ID,
        environment_variable="ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK",
        required_route_counters=(
            RouteCounter("launch_attention_gqa_decode_score_prework", "generated score-prework attention"),
        ),
        forbidden_route_counters=(
            RouteCounter("launch_attention_gqa_decode_fast_fallbacks", "score-prework attention fallbacks"),
        ),
        fixed_comparison_environment=SCORE_PREWORK_ATTENTION_COMPARISON_ENVIRONMENT,
    ),
    SCORE_PREWORK_TILED64_ATTENTION_KERNEL_ID: CandidateSpec(
        kernel_id=SCORE_PREWORK_TILED64_ATTENTION_KERNEL_ID,
        environment_variable="ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK_CONSUMER",
        required_route_counters=(
            RouteCounter(
                "launch_attention_gqa_decode_score_prework_tiled64_hd256",
                "tiled64 score-prework consumer HD256 route",
            ),
            RouteCounter(
                "launch_attention_gqa_decode_score_prework_tiled64_hd512",
                "tiled64 score-prework consumer HD512 route",
            ),
        ),
        forbidden_route_counters=(
            RouteCounter(
                "launch_attention_gqa_decode_score_prework_serial",
                "serial score-prework consumer route",
            ),
            RouteCounter(
                "launch_attention_gqa_decode_score_prework_serial_hd256",
                "serial score-prework consumer HD256 route",
            ),
            RouteCounter(
                "launch_attention_gqa_decode_score_prework_serial_hd512",
                "serial score-prework consumer HD512 route",
            ),
            RouteCounter(
                "launch_attention_gqa_decode_score_prework_tiled64_fallbacks",
                "tiled64 score-prework consumer fallbacks",
            ),
            RouteCounter(
                "launch_attention_gqa_decode_score_prework_tiled64_forbidden_routes",
                "tiled64 score-prework forbidden routes",
            ),
            RouteCounter(
                "launch_attention_gqa_decode_score_prework_tiled64_symbol_fallbacks",
                "tiled64 score-prework symbol fallbacks",
            ),
            RouteCounter("launch_attention_gqa_decode_fast", "fast decode attention route"),
            RouteCounter(
                "launch_attention_gqa_decode_fast_fallbacks",
                "fast decode attention fallbacks",
            ),
        ),
        required_baseline_route_counters=(
            RouteCounter(
                "launch_attention_gqa_decode_score_prework_serial_hd256",
                "serial score-prework consumer HD256 route",
            ),
            RouteCounter(
                "launch_attention_gqa_decode_score_prework_serial_hd512",
                "serial score-prework consumer HD512 route",
            ),
        ),
        fixed_comparison_environment=SCORE_PREWORK_TILED64_ATTENTION_COMPARISON_ENVIRONMENT,
        baseline_gate_value="serial",
        candidate_gate_value="required-tiled64",
        qualification_workload=GEMMA4_LONG_CONTEXT_WORKLOAD,
    ),
    GQA_PREFILL_TILED_F16_EXACT_KERNEL_ID: CandidateSpec(
        kernel_id=GQA_PREFILL_TILED_F16_EXACT_KERNEL_ID,
        environment_variable="ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE",
        required_route_counters=(
            RouteCounter(
                "launch_attention_gqa_prefill_tiled_f16_exact_hd256",
                "tiled F16 exact GQA prefill HD256 route",
            ),
            RouteCounter(
                "launch_attention_gqa_prefill_tiled_f16_exact_hd512",
                "tiled F16 exact GQA prefill HD512 route",
            ),
        ),
        baseline_gate_value="required-fast",
        candidate_gate_value="required-tiled-f16-exact",
        route_phase="prefill",
    ),
    GQA_PREFILL_TILED_F16_WARP_KERNEL_ID: CandidateSpec(
        kernel_id=GQA_PREFILL_TILED_F16_WARP_KERNEL_ID,
        environment_variable="ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE",
        required_route_counters=(
            RouteCounter(
                "launch_attention_gqa_prefill_tiled_f16_warp_hd256",
                "tiled F16 warp GQA prefill HD256 route",
            ),
            RouteCounter(
                "launch_attention_gqa_prefill_tiled_f16_warp_hd512",
                "tiled F16 warp GQA prefill HD512 route",
            ),
        ),
        baseline_gate_value="required-fast",
        candidate_gate_value="required-tiled-f16-warp",
        route_phase="prefill",
        qualification_workload=GEMMA4_LONG_CONTEXT_WORKLOAD,
        qualification_route_counts=(
            RouteCountExpectation(
                "launch_attention_gqa_prefill_tiled_f16_warp_hd256",
                140,
            ),
            RouteCountExpectation(
                "launch_attention_gqa_prefill_tiled_f16_warp_hd512",
                35,
            ),
        ),
    ),
    GQA_PREFILL_FLASH_F16_SM89_KERNEL_ID: CandidateSpec(
        kernel_id=GQA_PREFILL_FLASH_F16_SM89_KERNEL_ID,
        environment_variable="ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE",
        required_route_counters=(
            RouteCounter(
                "launch_attention_gqa_prefill_flash_f16_sm89_hd256_q512",
                "Flash F16 SM89 GQA prefill HD256 q512 route",
            ),
            RouteCounter(
                "launch_attention_gqa_prefill_flash_f16_sm89_hd256_q3",
                "Flash F16 SM89 GQA prefill HD256 q3 route",
            ),
            RouteCounter(
                "launch_attention_gqa_prefill_flash_f16_sm89_hd512_q512",
                "Flash F16 SM89 GQA prefill HD512 q512 route",
            ),
            RouteCounter(
                "launch_attention_gqa_prefill_flash_f16_sm89_hd512_q3",
                "Flash F16 SM89 GQA prefill HD512 q3 route",
            ),
        ),
        forbidden_route_counters=(
            RouteCounter(
                "launch_attention_gqa_prefill_flash_f16_sm89_fallbacks",
                "Flash F16 SM89 GQA prefill fallbacks",
            ),
            RouteCounter(
                "launch_attention_gqa_prefill_flash_f16_sm89_ineligible_fallbacks",
                "Flash F16 SM89 GQA prefill eligibility fallbacks",
            ),
            RouteCounter(
                "launch_attention_gqa_prefill_flash_f16_sm89_symbol_fallbacks",
                "Flash F16 SM89 GQA prefill symbol fallbacks",
            ),
        ),
        baseline_gate_value="required-fast",
        candidate_gate_value="required-flash-f16-sm89",
        route_phase="prefill",
        qualification_workload=GEMMA4_LONG_CONTEXT_WORKLOAD,
        qualification_route_counts=(
            RouteCountExpectation(
                "launch_attention_gqa_prefill_flash_f16_sm89_hd256_q512",
                112,
            ),
            RouteCountExpectation(
                "launch_attention_gqa_prefill_flash_f16_sm89_hd256_q3",
                28,
            ),
            RouteCountExpectation(
                "launch_attention_gqa_prefill_flash_f16_sm89_hd512_q512",
                28,
            ),
            RouteCountExpectation(
                "launch_attention_gqa_prefill_flash_f16_sm89_hd512_q3",
                7,
            ),
        ),
    ),
    GQA_DECODE_SPLITK_ONLINE_SM89_KERNEL_ID: CandidateSpec(
        kernel_id=GQA_DECODE_SPLITK_ONLINE_SM89_KERNEL_ID,
        environment_variable="ANTFLY_INFERENCE_CUDA_GQA_DECODE_PROFILE",
        required_route_counters=(
            RouteCounter(
                "launch_attention_gqa_decode_splitk_online_sm89",
                "SM89 split-K online GQA decode route",
            ),
            RouteCounter(
                "launch_attention_gqa_decode_splitk_online_sm89_hd256",
                "SM89 split-K online GQA decode HD256 route",
            ),
            RouteCounter(
                "launch_attention_gqa_decode_splitk_online_sm89_hd512",
                "SM89 split-K online GQA decode HD512 route",
            ),
        ),
        forbidden_route_counters=(
            RouteCounter(
                "launch_attention_gqa_decode_splitk_online_sm89_fallbacks",
                "SM89 split-K online GQA decode fallbacks",
            ),
            RouteCounter(
                "launch_attention_gqa_decode_splitk_online_sm89_ineligible_fallbacks",
                "SM89 split-K online GQA decode eligibility fallbacks",
            ),
            RouteCounter(
                "launch_attention_gqa_decode_splitk_online_sm89_symbol_fallbacks",
                "SM89 split-K online GQA decode symbol fallbacks",
            ),
            RouteCounter(
                "launch_attention_gqa_decode_splitk_online_sm89_forbidden_routes",
                "SM89 split-K online GQA decode forbidden routes",
            ),
            RouteCounter(
                "launch_attention_gqa_decode_score_prework",
                "score-prework route while split-K is active",
            ),
            RouteCounter(
                "launch_attention_gqa_decode_score_prework_tiled64_hd256",
                "tiled64 score-prework HD256 route while split-K is active",
            ),
            RouteCounter(
                "launch_attention_gqa_decode_score_prework_tiled64_hd512",
                "tiled64 score-prework HD512 route while split-K is active",
            ),
        ),
        required_baseline_route_counters=(
            RouteCounter(
                "launch_attention_gqa_decode_score_prework_tiled64_hd256",
                "baseline tiled64 score-prework HD256 route",
            ),
            RouteCounter(
                "launch_attention_gqa_decode_score_prework_tiled64_hd512",
                "baseline tiled64 score-prework HD512 route",
            ),
        ),
        fixed_comparison_environment=SPLITK_ONLINE_SM89_COMPARISON_ENVIRONMENT,
        baseline_gate_value="off",
        candidate_gate_value="required-splitk-online-sm89",
        route_phase="decode",
        qualification_workload=GEMMA4_LONG_CONTEXT_WORKLOAD,
        # A fresh CLI generation performs four host-visible decode evaluations
        # while constructing the persistent graph. Route counters record those
        # launches; the remaining decode iterations increment the independent
        # persistent-replay counter instead.
        qualification_route_counts=(
            RouteCountExpectation(
                "launch_attention_gqa_decode_splitk_online_sm89",
                140,
            ),
            RouteCountExpectation(
                "launch_attention_gqa_decode_splitk_online_sm89_hd256",
                112,
            ),
            RouteCountExpectation(
                "launch_attention_gqa_decode_splitk_online_sm89_hd512",
                28,
            ),
        ),
        require_persistent_replay=True,
    ),
    Q4_0_Q8_1_LM_HEAD_ARGMAX_KERNEL_ID: CandidateSpec(
        kernel_id=Q4_0_Q8_1_LM_HEAD_ARGMAX_KERNEL_ID,
        environment_variable="ANTFLY_INFERENCE_CUDA_Q4_0_LM_HEAD_Q8_1_ARGMAX",
        required_route_counters=(RouteCounter("lm_head_argmax_fused_q4_0_q8_1", "Q4_0 x Q8_1 LM-head argmax"),),
        forbidden_route_counters=(
            RouteCounter("lm_head_argmax_q4_0_q8_1_fallbacks", "Q4_0 x Q8_1 LM-head argmax fallbacks"),
        ),
        legacy_kind=CandidateKind.Q4_0_Q8_1_LM_HEAD_ARGMAX,
    ),
    Q6_K_Q8_1_LM_HEAD_ARGMAX_KERNEL_ID: CandidateSpec(
        kernel_id=Q6_K_Q8_1_LM_HEAD_ARGMAX_KERNEL_ID,
        environment_variable="ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX",
        required_route_counters=(
            RouteCounter("lm_head_argmax_generated_q6_k_q8_1_hits", "generated Q6_K x Q8_1 LM-head argmax"),
        ),
        forbidden_route_counters=(
            RouteCounter("lm_head_argmax_generated_q6_k_q8_1_fallbacks", "generated Q6_K x Q8_1 LM-head argmax fallbacks"),
        ),
        legacy_kind=CandidateKind.Q6_K_Q8_1_LM_HEAD_ARGMAX,
        requires_explicit_model=True,
    ),
    Q4_0_Q8_1_FFN_KERNEL_ID: CandidateSpec(
        kernel_id=Q4_0_Q8_1_FFN_KERNEL_ID,
        environment_variable="ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN",
        required_route_counters=(
            RouteCounter("q4_0_generated_e2b_pair_q8_hits", "Q4_0 x Q8_1 generated E2B FFN pair route"),
            RouteCounter("q4_0_generated_e2b_down_q8_hits", "Q4_0 x Q8_1 generated E2B FFN down route"),
        ),
        forbidden_route_counters=(
            RouteCounter("q4_0_generated_e2b_pair_q8_fallbacks", "Q4_0 x Q8_1 generated E2B FFN pair fallbacks"),
            RouteCounter("q4_0_generated_e2b_down_q8_fallbacks", "Q4_0 x Q8_1 generated E2B FFN down fallbacks"),
        ),
        legacy_kind=CandidateKind.Q4_0_Q8_1_E2B_FFN,
        fixed_comparison_environment=E2B_FFN_COUPLED_COMPARISON_ENVIRONMENT,
    ),
    Q4_0_Q8_1_E2B_FFN_PAIR_ONLY_KERNEL_ID: CandidateSpec(
        kernel_id=Q4_0_Q8_1_E2B_FFN_PAIR_ONLY_KERNEL_ID,
        environment_variable="ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY",
        required_route_counters=(
            RouteCounter("q4_0_generated_e2b_pair_only_hits", "generated E2B FFN pair-only route"),
        ),
        forbidden_route_counters=(
            RouteCounter("q4_0_generated_e2b_pair_only_fallbacks", "generated E2B FFN pair-only fallbacks"),
            RouteCounter("q4_0_generated_e2b_pair_q8_hits", "coupled generated E2B FFN pair route"),
            RouteCounter("q4_0_generated_e2b_down_q8_hits", "generated E2B FFN down route"),
            RouteCounter("q4_0_generated_e2b_exact_pair_f32_hits", "exact F32 generated E2B FFN pair route"),
            RouteCounter("q4_0_generated_e2b_exact_down_f32_hits", "exact F32 generated E2B FFN down route"),
        ),
        legacy_kind=CandidateKind.Q4_0_Q8_1_E2B_FFN_PAIR_ONLY,
        fixed_comparison_environment=E2B_FFN_PAIR_ONLY_COMPARISON_ENVIRONMENT,
    ),
    Q4_0_E2B_FFN_EXACT_KERNEL_ID: CandidateSpec(
        kernel_id=Q4_0_E2B_FFN_EXACT_KERNEL_ID,
        environment_variable="ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT",
        required_route_counters=(
            RouteCounter("q4_0_generated_e2b_exact_pair_f32_hits", "exact F32 generated E2B FFN pair route"),
            RouteCounter("q4_0_generated_e2b_exact_down_f32_hits", "exact F32 generated E2B FFN down route"),
        ),
        forbidden_route_counters=(
            RouteCounter(
                "q4_0_generated_e2b_exact_pair_f32_fallbacks",
                "exact F32 generated E2B FFN pair fallbacks",
            ),
            RouteCounter(
                "q4_0_generated_e2b_exact_down_f32_fallbacks",
                "exact F32 generated E2B FFN down fallbacks",
            ),
        ),
        legacy_kind=CandidateKind.Q4_0_E2B_FFN_EXACT,
        fixed_comparison_environment=EXACT_E2B_FFN_F32_COMPARISON_ENVIRONMENT,
    ),
    Q4_0_GGML_Q8_1_E2B_FFN_KERNEL_ID: CandidateSpec(
        kernel_id=Q4_0_GGML_Q8_1_E2B_FFN_KERNEL_ID,
        environment_variable="ANTFLY_INFERENCE_CUDA_SM89_Q4_0_Q8_1",
        required_route_counters=(
            RouteCounter("q4_0_ggml_q8_1_e2b_ffn_hits", "SM89 GGML Q8_1 E2B FFN route"),
        ),
        forbidden_route_counters=(
            RouteCounter(
                "q4_0_ggml_q8_1_e2b_ffn_fallbacks",
                "SM89 GGML Q8_1 E2B FFN fallbacks",
            ),
        ),
        fixed_comparison_environment=GGML_Q8_1_E2B_FFN_COMPARISON_ENVIRONMENT,
        baseline_gate_value="off",
        candidate_gate_value="ggml-ffn-v1",
    ),
    CUBLASLT_BF16_PREFILL_SM89_KERNEL_ID: CandidateSpec(
        kernel_id=CUBLASLT_BF16_PREFILL_SM89_KERNEL_ID,
        environment_variable="ANTFLY_INFERENCE_CUDA_CUBLASLT_BF16_TUNING_PROFILE",
        required_route_counters=(
            RouteCounter("bf16_cublaslt_tuning_tuned_calls", "SM89 BF16 cuBLASLt tuned plan"),
        ),
        forbidden_route_counters=(
            RouteCounter(
                "bf16_cublaslt_tuning_api_fallbacks",
                "SM89 BF16 cuBLASLt tuning API fallbacks",
            ),
        ),
        fixed_comparison_environment=CUBLASLT_BF16_PREFILL_COMPARISON_ENVIRONMENT,
        baseline_gate_value="off",
        candidate_gate_value="sm89-prefill",
        route_phase="prefill",
    ),
    PLE_GATE_BF16_MIRROR_FIRST_SM89_E2B_KERNEL_ID: CandidateSpec(
        kernel_id=PLE_GATE_BF16_MIRROR_FIRST_SM89_E2B_KERNEL_ID,
        environment_variable="ANTFLY_INFERENCE_CUDA_PLE_GATE_PREFILL_PROFILE",
        required_route_counters=(
            RouteCounter(
                "ple_gate_prefill_bf16_mirror_first_hits",
                "SM89 E2B PLE-gate BF16 mirror-first route",
            ),
            RouteCounter(
                "ple_gate_decode_q4_fused_preserved",
                "E2B rows==1 fused Q4 PLE-gate route preservation",
            ),
        ),
        forbidden_route_counters=(
            RouteCounter(
                "ple_gate_prefill_bf16_mirror_first_ineligible",
                "SM89 E2B PLE-gate mirror-first eligibility misses",
            ),
        ),
        required_baseline_route_counters=(
            RouteCounter(
                "linear_activation_slice_fused_q4_0",
                "baseline fused Q4_0 PLE-gate route",
            ),
        ),
        fixed_comparison_environment=PLE_GATE_BF16_MIRROR_FIRST_COMPARISON_ENVIRONMENT,
        baseline_gate_value="off",
        candidate_gate_value="mirror-first-sm89-e2b",
        route_phase="prefill",
        qualification_workload=GEMMA4_LONG_CONTEXT_WORKLOAD,
    ),
}

CANDIDATE_SPECS = {
    spec.legacy_kind: spec
    for spec in CANDIDATE_CATALOG.values()
    if spec.legacy_kind is not None
}

DEFAULT_CANDIDATE = CANDIDATE_SPECS[CandidateKind.GENERATED_ATTENTION]


def parse_args() -> argparse.Namespace:
    repo = pathlib.Path(__file__).resolve().parents[5]
    inference = repo / "zig/pkg/inference"
    parser = argparse.ArgumentParser(description=__doc__)
    candidate_group = parser.add_mutually_exclusive_group()
    candidate_group.add_argument(
        "--candidate",
        type=CandidateKind,
        choices=tuple(CandidateKind),
        default=None,
        help=(
            "legacy candidate alias; generated-attention remains the default, "
            "q4-0-q8-1-lm-head-argmax tests the generated E2B LM-head route, and "
            "q6-k-q8-1-lm-head-argmax tests generated Q6_K LM-head stage 1, and "
            "q4-0-q8-1-e2b-ffn tests the generated Q8-intermediate E2B FFN pair and down routes, and "
            "q4-0-q8-1-e2b-ffn-pair-only tests the byte-exact generated E2B pair with handwritten down, and "
            "q4-0-e2b-ffn-exact tests the exact F32 E2B FFN pair and down routes"
        ),
    )
    candidate_group.add_argument(
        "--kernel-id",
        "--catalog-id",
        "--catalog-kernel-id",
        dest="kernel_id",
        default=None,
        help="model-neutral catalog kernel ID; uncatalogued IDs require an environment variable and route counters",
    )
    parser.add_argument(
        "--candidate-environment-variable",
        "--environment-variable",
        dest="candidate_environment_variable",
        default=None,
        help="candidate gate environment variable (required for an uncatalogued kernel ID)",
    )
    parser.add_argument(
        "--candidate-env",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help=(
            "candidate-only runtime environment override; repeatable. The selected candidate gate "
            "cannot be overridden. Use this to pin experimental schedules such as "
            f"{GENERATED_ATTENTION_SPLIT_KV_MIN_TOKENS_ENV}=512 or "
            f"{GENERATED_ATTENTION_SPLIT_KV_SPLITS_ENV}=4"
        ),
    )
    parser.add_argument(
        "--common-env",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help=(
            "runtime environment override applied identically to baseline and candidate; repeatable. "
            "The selected candidate gate and capture KV capacity cannot be overridden."
        ),
    )
    parser.add_argument(
        "--required-route-counter",
        "--required-route-counters",
        action="append",
        default=None,
        help="required counter as NAME or NAME=LABEL; repeat the option or use comma-separated values",
    )
    parser.add_argument(
        "--forbidden-route-counter",
        "--forbidden-route-counters",
        action="append",
        default=None,
        help="forbidden counter as NAME or NAME=LABEL; repeat the option or use comma-separated values",
    )
    parser.add_argument("--binary", type=pathlib.Path, default=inference / "zig-out/bin/antfly-inference")
    parser.add_argument("--wrapper", type=pathlib.Path, default=inference / "scripts/gemma4/with_gemma4_qat_cuda_tuning.sh")
    parser.add_argument(
        "--artifact-check-script",
        type=pathlib.Path,
        default=inference / "scripts/regen-cuda-artifacts.sh",
        help=(
            "canonical non-mutating CUDA artifact check; strict qualification requires the "
            "checked-in script and runs it once before timing"
        ),
    )
    parser.add_argument(
        "--model",
        type=pathlib.Path,
        default=None,
        help=(
            "GGUF model under test; required for candidates whose quantized route is model-specific. "
            "Other candidates default to the local E2B QAT fixture."
        ),
    )
    parser.add_argument(
        "--model-label",
        default=None,
        help="stable model label recorded in results (defaults to the model filename without its suffix)",
    )
    parser.add_argument(
        "--config-label",
        default="default",
        help="stable benchmark configuration label, for example e4b-12b-sm89-f32",
    )
    parser.add_argument(
        "--cache-dtype",
        choices=SUPPORTED_CACHE_DTYPES,
        default="f32",
        help="KV cache dtype applied identically to baseline and candidate",
    )
    parser.add_argument(
        "--prefill-chunk-size",
        type=int,
        default=32,
        help=(
            "prefill rows per scheduler chunk for both arms; long-context production-shape "
            "screens should use the server's 512-row chunk ceiling"
        ),
    )
    parser.add_argument(
        "--timing-counter-group",
        "--counter-group",
        dest="timing_counter_group",
        default=DEFAULT_TIMING_METADATA.counter_group,
        help="dot-separated object path containing route and graph counters",
    )
    parser.add_argument(
        "--timing-throughput-field",
        "--throughput-field",
        dest="timing_throughput_field",
        default=DEFAULT_TIMING_METADATA.throughput_field,
        help="dot-separated timing field containing candidate throughput",
    )
    parser.add_argument(
        "--timing-throughput-unit",
        "--throughput-unit",
        dest="timing_throughput_unit",
        default=DEFAULT_TIMING_METADATA.throughput_unit,
        help="throughput unit label recorded in result metadata",
    )
    parser.add_argument(
        "--no-require-persistent-replay",
        action="store_true",
        help="skip CUDA graph replay checks for candidates whose benchmark route is not graph replayable",
    )
    parser.add_argument("--prompt", action="append", default=[])
    parser.add_argument(
        "--prompt-fixture",
        action="append",
        type=pathlib.Path,
        default=[],
        help=(
            "versioned antfly.prompt_fixture.v1 JSON; repeatable and additive with --prompt. "
            "The locked rendered chat prompt is verified against its byte/hash contract, then "
            "passed byte-for-byte through the raw-prompt path"
        ),
    )
    parser.add_argument("--lengths", type=int, nargs="+", default=[64, 128, 256, 512])
    parser.add_argument(
        "--qualification-profile",
        choices=tuple(QUALIFICATION_PROFILES),
        default="legacy",
        help=(
            "legacy preserves the ad-hoc validator contract; screening fixes five paired samples; "
            "promotion fixes ten paired samples and enforces material TTFT/decode/total improvements; "
            "prefill-screening and prefill-promotion require a locked prompt fixture, material "
            "prefill-proxy/total improvement, and decode non-regression"
        ),
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=None,
        help=(
            "paired repetitions per prompt and length; execution order alternates each repetition. "
            "Screening and promotion profiles require their fixed sample count"
        ),
    )
    parser.add_argument(
        "--min-candidate-ratio",
        type=float,
        default=None,
        help="require every paired candidate/baseline throughput ratio to meet this value",
    )
    parser.add_argument(
        "--max-cv",
        type=float,
        default=None,
        help="maximum baseline and candidate throughput CV within each prompt/length case",
    )
    parser.add_argument("--bootstrap-samples", type=int, default=10_000)
    parser.add_argument("--bootstrap-seed", type=int, default=20260730)
    parser.add_argument("--max-total-latency-ratio", type=float)
    parser.add_argument("--max-ttft-ratio", type=float)
    parser.add_argument("--max-decode-latency-ratio", type=float)
    parser.add_argument("--max-total-ci-upper", type=float)
    parser.add_argument("--max-ttft-ci-upper", type=float)
    parser.add_argument("--max-decode-ci-upper", type=float)
    parser.add_argument(
        "--require-full-route-coverage",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "require candidate route observation during graph construction plus persistent graph replay "
            "coverage across the stable decode steps"
        ),
    )
    parser.add_argument("--output-dir", type=pathlib.Path, default=pathlib.Path("/tmp/antfly-gemma4-cuda-candidate"))
    parser.add_argument(
        "--capture-kv-capacity",
        type=int,
        default=None,
        help=(
            "forced CUDA graph KV capacity for both paired runs. Defaults to the largest requested "
            f"decode length plus {DEFAULT_CAPTURE_KV_PROMPT_HEADROOM} tokens of prompt headroom"
        ),
    )
    parser.add_argument("--timeout-sec", type=int, default=360)
    parser.add_argument(
        "--build-timeout-sec",
        type=int,
        default=1800,
        help="strict-profile controlled ReleaseFast CUDA build timeout (separate from each model run)",
    )
    parser.add_argument(
        "--artifact-timeout-sec",
        type=int,
        default=1800,
        help="strict-profile canonical CUDA artifact check timeout",
    )
    args = parser.parse_args()
    profile = QUALIFICATION_PROFILES[args.qualification_profile]
    if args.repeats is None:
        args.repeats = profile["samples"] or 1
    if args.min_candidate_ratio is None:
        args.min_candidate_ratio = profile["min_candidate_ratio"]
    if args.max_cv is None:
        args.max_cv = profile["max_cv"]
    for name in (
        "max_total_latency_ratio",
        "max_ttft_ratio",
        "max_decode_latency_ratio",
        "max_total_ci_upper",
        "max_ttft_ci_upper",
        "max_decode_ci_upper",
    ):
        if getattr(args, name) is None:
            setattr(args, name, profile[name])
    if args.require_full_route_coverage is None:
        args.require_full_route_coverage = profile["require_full_route_coverage"]
    args.require_phase_metrics = profile["require_phase_metrics"]
    args.qualification_fixed_samples = profile["samples"]
    args.qualification_focus = profile["focus"]
    args.require_locked_prompt_fixture = profile["require_locked_prompt_fixture"]
    if args.candidate is None and args.kernel_id is None:
        args.candidate = CandidateKind.GENERATED_ATTENTION
    q6_selected = (
        args.candidate == CandidateKind.Q6_K_Q8_1_LM_HEAD_ARGMAX
        or args.kernel_id == Q6_K_Q8_1_LM_HEAD_ARGMAX_KERNEL_ID
    )
    if args.model is None and not q6_selected:
        args.model = repo / ".models/unsloth/gemma-4-E2B-it-qat-GGUF/gemma-4-E2B-it-qat-UD-Q4_K_XL.gguf"
    # run_case executes from the repository root so that the tuning wrapper has
    # a stable working directory. Resolve caller-provided launch operands here
    # while the caller's working directory is still in effect.
    args.binary = args.binary.expanduser().resolve()
    args.wrapper = args.wrapper.expanduser().resolve()
    args.artifact_check_script = args.artifact_check_script.expanduser().resolve()
    if args.model is not None:
        args.model = args.model.expanduser().resolve()
    args.prompt_fixture = [path.expanduser().resolve() for path in args.prompt_fixture]
    if args.model_label is None and args.model is not None:
        args.model_label = args.model.stem
    return args


def slug(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return cleaned[:48] or "prompt"


def load_prompt_fixture(path: pathlib.Path) -> tuple[str, dict]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"could not read prompt fixture {path}: {exc}") from exc
    if raw.get("schema") != "antfly.prompt_fixture.v1":
        raise ValueError(f"unsupported prompt fixture schema: {raw.get('schema')!r}")
    segment = raw.get("segment")
    suffix = raw.get("suffix")
    repeat = raw.get("repeat")
    if not isinstance(segment, str) or not isinstance(suffix, str):
        raise ValueError("prompt fixture segment and suffix must be strings")
    if isinstance(repeat, bool) or not isinstance(repeat, int) or repeat < 1:
        raise ValueError("prompt fixture repeat must be a positive integer")
    prompt = segment * repeat + suffix
    encoded = prompt.encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    if len(encoded) != raw.get("expected_user_utf8_bytes") or digest != raw.get("expected_user_sha256"):
        raise ValueError("prompt fixture rendered user content does not match its byte/hash contract")
    reference_prefix = raw.get("reference_chat_prefix")
    reference_suffix = raw.get("reference_chat_suffix")
    if not isinstance(reference_prefix, str) or not isinstance(reference_suffix, str):
        raise ValueError("prompt fixture reference chat prefix/suffix must be strings")
    benchmark_prompt = reference_prefix + prompt + reference_suffix
    benchmark_encoded = benchmark_prompt.encode("utf-8")
    benchmark_digest = hashlib.sha256(benchmark_encoded).hexdigest()
    if (
        len(benchmark_encoded) != raw.get("expected_reference_prompt_utf8_bytes")
        or benchmark_digest != raw.get("expected_reference_prompt_sha256")
    ):
        raise ValueError("prompt fixture rendered reference prompt does not match its byte/hash contract")
    expected_prompt_tokens = raw.get("expected_reference_prompt_tokens")
    if (
        isinstance(expected_prompt_tokens, bool)
        or not isinstance(expected_prompt_tokens, int)
        or expected_prompt_tokens < 1
    ):
        raise ValueError("prompt fixture expected reference prompt tokens must be a positive integer")
    return benchmark_prompt, {
        "schema": raw["schema"],
        "id": str(raw.get("id") or path.stem),
        "path": str(path),
        "file_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "user_utf8_bytes": len(encoded),
        "user_sha256": digest,
        "benchmark_prompt_utf8_bytes": len(benchmark_encoded),
        "benchmark_prompt_sha256": benchmark_digest,
        "benchmark_prompt_tokens": expected_prompt_tokens,
        "benchmark_prompt_transport": "raw_prompt_exact_rendered_chat_bytes",
    }


def prompt_identity(prompt: str, *, source: str, fixture_id: str | None = None) -> dict:
    encoded = prompt.encode("utf-8")
    return {
        "source": source,
        "fixture_id": fixture_id,
        "utf8_bytes": len(encoded),
        "sha256": hashlib.sha256(encoded).hexdigest(),
    }


def input_path_provenance(path: pathlib.Path) -> dict:
    resolved = path.resolve()
    if resolved.is_file():
        before = resolved.stat()
        digest = sha256_file(resolved)
        after = resolved.stat()
        before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        if before_identity != after_identity:
            raise RuntimeError(f"benchmark input changed while hashing: {resolved}")
        return {
            "path": str(resolved),
            "kind": "file",
            "bytes": after.st_size,
            "sha256": digest,
        }
    if resolved.is_dir():
        paths = sorted(item for item in resolved.rglob("*") if item.is_file())
        files = []
        for item in paths:
            item_provenance = input_path_provenance(item)
            files.append({
                "path": item.relative_to(resolved).as_posix(),
                "bytes": item_provenance["bytes"],
                "sha256": item_provenance["sha256"],
            })
        final_paths = sorted(
            item.relative_to(resolved).as_posix()
            for item in resolved.rglob("*")
            if item.is_file()
        )
        if final_paths != [item["path"] for item in files]:
            raise RuntimeError(f"benchmark input directory changed while hashing: {resolved}")
        identity = {"files": files}
        return {
            "path": str(resolved),
            "kind": "directory",
            "file_count": len(files),
            "files": files,
            "sha256": canonical_sha256(identity),
        }
    raise RuntimeError(f"benchmark input does not exist: {resolved}")


def _valid_sha256(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _required_file_provenance(path: pathlib.Path) -> dict:
    resolved = path.resolve()
    if not resolved.is_file():
        return {"path": str(resolved), "exists": False}
    return {"exists": True, **input_path_provenance(resolved)}


def candidate_cuda_artifact_provenance(repo: pathlib.Path | None = None) -> dict:
    """Hash the release-gate artifact set plus its generator sources."""
    repo = pathlib.Path(__file__).resolve().parents[5] if repo is None else repo.resolve()
    release_files = release_provenance.artifact_provenance(repo)
    files = {name: dict(value) for name, value in release_files.items()}
    graph = repo / "zig/pkg/inference/src/graph"
    files.update({
        "renderer_source": _required_file_provenance(
            graph / "quant_kernel_cuda_renderer.zig"
        ),
        "compiler_source": _required_file_provenance(
            graph / "quant_kernel_compiler.zig"
        ),
    })
    identity = {
        name: {
            "bytes": value.get("bytes"),
            "sha256": value.get("sha256"),
        }
        for name, value in sorted(files.items())
    }
    return {
        "files": files,
        "sha256": canonical_sha256(identity),
    }


def qualification_runtime_environment(
    common_environment: tuple[tuple[str, str], ...] = (),
    *,
    strict: bool = False,
) -> dict[str, str]:
    if strict:
        environment = {
            name: os.environ[name]
            for name in STRICT_RUNTIME_ENVIRONMENT_KEYS
            if name in os.environ
        }
        environment.setdefault("PATH", "/usr/local/cuda-13.2/bin:/usr/local/bin:/usr/bin:/bin")
        environment.setdefault("LANG", "C.UTF-8")
        environment.setdefault("LC_ALL", "C.UTF-8")
        environment["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
    else:
        environment = os.environ.copy()
    environment.update(dict(common_environment))
    return environment


def strict_environment_provenance(
    runtime_environment: dict[str, str] | None = None,
) -> dict:
    runtime_environment = (
        dict(os.environ) if runtime_environment is None else runtime_environment
    )
    git = release_provenance.git_provenance(pathlib.Path(__file__).resolve().parents[5])
    toolchains = release_provenance.toolchain_provenance()
    repo = pathlib.Path(__file__).resolve().parents[5]
    pinned_zig = repo / ".tools/zig-x86_64-linux-0.17.0-dev.1933+e19a73c2d/zig"
    if pinned_zig.is_file():
        # This is the exact compiler selected by run_controlled_release_build;
        # hash it even though the reproducible tool bundle is gitignored.
        toolchains["zig"] = release_provenance.executable_provenance(
            str(pinned_zig),
            "version",
        )
    gpu_execution_state = warm_server_provenance.capture_gpu_execution_state(
        runtime_environment
    )
    compute_processes = warm_server_provenance.capture_selected_gpu_compute_processes(
        gpu_execution_state
    )
    git = {**git, "sha256": canonical_sha256(git)}
    toolchain_identity = {
        name: toolchains.get(name)
        for name in ("zig", "nvcc")
    }
    toolchains = {
        **toolchains,
        "sha256": canonical_sha256(toolchain_identity),
    }
    gpu_identity = {
        "execution_state": gpu_execution_state,
        "selected_compute_processes": compute_processes,
    }
    return {
        "schema": STRICT_PROVENANCE_SCHEMA,
        "git": git,
        "toolchains": toolchains,
        "gpu": {
            **gpu_identity,
            "sha256": canonical_sha256(gpu_identity),
        },
        "cuda_artifacts": candidate_cuda_artifact_provenance(),
    }


def strict_qualification_provenance_errors(provenance: dict) -> list[str]:
    errors: list[str] = []
    provenance_identity = {
        name: item.get("sha256")
        for name, item in sorted(provenance.items())
        if isinstance(item, dict)
    }
    if provenance.get("sha256") != canonical_sha256(provenance_identity):
        errors.append("strict qualification top-level provenance hash does not match its contents")
    runtime_environment = provenance.get("runtime_environment") or {}
    runtime_values = runtime_environment.get("values")
    runtime_identity = {
        "schema": runtime_environment.get("schema"),
        "values": runtime_values,
    }
    if (
        runtime_environment.get("schema") != STRICT_RUNTIME_ENVIRONMENT_SCHEMA
        or not isinstance(runtime_values, dict)
        or runtime_environment.get("sha256") != canonical_sha256(runtime_identity)
    ):
        errors.append("strict qualification effective runtime environment is not content-addressed")
    git = provenance.get("git") or {}
    commit = git.get("commit")
    if (
        git.get("commit_returncode") != 0
        or not isinstance(commit, str)
        or re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", commit) is None
    ):
        errors.append("strict qualification requires a valid Git commit")
    for field in (
        "tracked_status_returncode",
        "source_status_returncode",
        "status_returncode",
    ):
        if git.get(field) != 0:
            errors.append(f"strict qualification could not capture Git {field}")
    for field in (
        "tracked_status_sha256",
        "source_status_sha256",
        "status_sha256",
        "sha256",
    ):
        if not _valid_sha256(git.get(field)):
            errors.append(f"strict qualification requires Git {field}")
    git_identity = {name: value for name, value in git.items() if name != "sha256"}
    if git.get("sha256") != canonical_sha256(git_identity):
        errors.append("strict qualification Git dirty-state hash does not match its contents")
    if not isinstance(git.get("dirty"), bool):
        errors.append("strict qualification requires an explicit Git dirty state")
    errors.extend(release_provenance.git_content_provenance_errors(git))

    toolchains = provenance.get("toolchains") or {}
    for name in ("zig", "nvcc"):
        tool = toolchains.get(name) or {}
        if (
            tool.get("returncode") != 0
            or not tool.get("path")
            or not _valid_sha256(tool.get("sha256"))
            or not tool.get("version")
        ):
            errors.append(f"strict qualification requires {name} path, hash, and version provenance")
    zig_version = (toolchains.get("zig") or {}).get("version")
    if zig_version and str(zig_version).strip() != "0.17.0-dev.1933+e19a73c2d":
        errors.append(f"strict qualification requires Zig 0.17.0-dev.1933+e19a73c2d, observed {zig_version!r}")
    nvcc_version = (toolchains.get("nvcc") or {}).get("version")
    if nvcc_version and "release 13.2" not in str(nvcc_version):
        errors.append("strict qualification requires the CUDA 13.2 NVCC toolchain")
    toolchain_identity = {
        name: toolchains.get(name)
        for name in ("zig", "nvcc")
    }
    if toolchains.get("sha256") != canonical_sha256(toolchain_identity):
        errors.append("strict qualification toolchain hash does not match its contents")

    gpu = provenance.get("gpu") or {}
    gpu_state = gpu.get("execution_state") or {}
    selected = gpu_state.get("selected_gpus") or []
    if gpu_state.get("error") or len(selected) != 1:
        errors.append("strict qualification requires exactly one selected NVIDIA GPU")
    else:
        device = selected[0]
        required_fields = (
            "uuid",
            "name",
            "driver_version",
            "compute_cap",
            "power.limit",
            "clocks.max.graphics",
            "clocks.max.memory",
            "clocks.applications.graphics",
            "clocks.applications.memory",
        )
        missing = [name for name in required_fields if device.get(name) in (None, "")]
        if missing:
            errors.append(
                "strict qualification GPU provenance is missing: " + ", ".join(missing)
            )
        if device.get("name") != "NVIDIA L4":
            errors.append(
                f"strict qualification requires NVIDIA L4, observed {device.get('name')!r}"
            )
        if device.get("compute_cap") != 8.9:
            errors.append(
                "strict qualification requires compute capability 8.9, "
                f"observed {device.get('compute_cap')!r}"
            )
        visible = gpu_state.get("cuda_visible_devices")
        if visible is not None:
            selectors = [selector.strip() for selector in str(visible).split(",")]
            if any(not selector.startswith("GPU-") for selector in selectors):
                errors.append(
                    "strict qualification requires UUID-based CUDA_VISIBLE_DEVICES selection"
                )
    compute_processes = gpu.get("selected_compute_processes") or {}
    if compute_processes.get("error"):
        errors.append("strict qualification could not inspect selected-GPU compute processes")
    elif compute_processes.get("selected_gpu_processes"):
        errors.append(
            "strict qualification requires an idle selected GPU; competing processes: "
            f"{compute_processes['selected_gpu_processes']!r}"
        )
    gpu_identity = {
        "execution_state": gpu_state,
        "selected_compute_processes": compute_processes,
    }
    if gpu.get("sha256") != canonical_sha256(gpu_identity):
        errors.append("strict qualification GPU-state hash does not match its contents")

    artifacts = provenance.get("cuda_artifacts") or {}
    files = artifacts.get("files") or {}
    for name in REQUIRED_CUDA_ARTIFACTS:
        artifact = files.get(name) or {}
        if (
            artifact.get("exists") is not True
            or artifact.get("kind") != "file"
            or not isinstance(artifact.get("bytes"), int)
            or artifact.get("bytes", 0) <= 0
            or not _valid_sha256(artifact.get("sha256"))
        ):
            errors.append(f"strict qualification requires hashed CUDA artifact {name}")
    if not _valid_sha256(artifacts.get("sha256")):
        errors.append("strict qualification CUDA artifact-set hash is unavailable")
    artifact_identity = {
        name: {
            "bytes": value.get("bytes"),
            "sha256": value.get("sha256"),
        }
        for name, value in sorted(files.items())
    }
    if artifacts.get("sha256") != canonical_sha256(artifact_identity):
        errors.append("strict qualification CUDA artifact-set hash does not match its contents")
    return errors


def qualification_provenance(
    args: argparse.Namespace,
    runtime_environment: dict[str, str] | None = None,
) -> dict:
    script_path = pathlib.Path(__file__).resolve()
    inputs = {
        "binary": input_path_provenance(args.binary),
        "model": input_path_provenance(args.model),
        "wrapper": input_path_provenance(args.wrapper),
    }
    harness_files = {
        "validator": input_path_provenance(script_path),
        "pairing_support": input_path_provenance(SCRIPTS_DIR / "paired_benchmark.py"),
    }
    strict = getattr(args, "qualification_profile", "legacy") != "legacy"
    if strict:
        artifact_check_script = pathlib.Path(
            getattr(
                args,
                "artifact_check_script",
                SCRIPTS_DIR / "regen-cuda-artifacts.sh",
            )
        ).resolve()
        inputs["artifact_check_script"] = input_path_provenance(artifact_check_script)
        harness_files["tuning_profile"] = input_path_provenance(
            script_path.with_name("gemma4_qat_cuda_tuning.sh")
        )
        harness_files.update({
            "release_provenance_support": input_path_provenance(
                pathlib.Path(release_provenance.__file__).resolve()
            ),
            "gpu_provenance_support": input_path_provenance(
                pathlib.Path(warm_server_provenance.__file__).resolve()
            ),
        })
    harness_identity = {
        name: {"bytes": value.get("bytes"), "sha256": value.get("sha256")}
        for name, value in sorted(harness_files.items())
    }
    value = {
        **inputs,
        "harness": {
            "files": harness_files,
            "sha256": canonical_sha256(harness_identity),
        },
    }
    if strict:
        value.update(strict_environment_provenance(runtime_environment))
        runtime_values = dict(sorted((runtime_environment or {}).items()))
        runtime_identity = {
            "schema": STRICT_RUNTIME_ENVIRONMENT_SCHEMA,
            "values": runtime_values,
        }
        value["runtime_environment"] = {
            **runtime_identity,
            "sha256": canonical_sha256(runtime_identity),
        }
    value["sha256"] = canonical_sha256({
        name: item.get("sha256")
        for name, item in sorted(value.items())
        if isinstance(item, dict)
    })
    return value


def _run_logged_in_directory(
    command: list[str],
    environment: dict[str, str],
    log_path: pathlib.Path,
    timeout_sec: int,
    cwd: pathlib.Path,
) -> tuple[int, str]:
    try:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError as exc:
        output = f"could not start command: {exc}\n"
        returncode = 127
    else:
        try:
            output, _ = process.communicate(timeout=timeout_sec)
            returncode = process.returncode
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                output, _ = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                output, _ = process.communicate()
            output = (output or "") + f"\ncommand timed out after {timeout_sec}s\n"
            returncode = 124
    log_path.write_text(output or "", encoding="utf-8")
    return returncode, output or ""


def _parse_colon_fields(output: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in output.splitlines():
        name, separator, value = line.partition(":")
        if separator and name.strip() and value.strip():
            fields[name.strip()] = value.strip()
    return fields


def run_embedded_cuda_artifact_check(
    args: argparse.Namespace,
    provenance: dict,
    environment: dict[str, str],
    inference: pathlib.Path,
) -> dict:
    """Prove the measured binary embeds the reviewed SM89 image bytes."""

    log_path = args.output_dir / "embedded_cuda_artifact_check.log"
    command = [str(args.binary.resolve()), "cuda-info", "--artifact-identity"]
    returncode, output = _run_logged_in_directory(
        command,
        environment,
        log_path,
        getattr(args, "build_timeout_sec", 1800),
        inference,
    )
    fields = _parse_colon_fields(output)
    expected = (
        ((provenance.get("cuda_artifacts") or {}).get("files") or {}).get(
            "runtime_sm89_cubin"
        )
        or {}
    )
    errors: list[str] = []
    if returncode != 0:
        errors.append(f"artifact-identity command exited {returncode}")
    expected_fields = {
        "cuda_artifact_identity_schema": "antfly.cuda_artifact_identity.v1",
        "cuda_artifact_mode": "sm89",
        "cuda_artifact_format": "cubin",
        "cuda_artifact_target": "sm_89",
        "cuda_artifact_image_sha256": expected.get("sha256"),
        "cuda_artifact_image_bytes": str(expected.get("bytes")),
    }
    for name, expected_value in expected_fields.items():
        if not expected_value or fields.get(name) != expected_value:
            errors.append(
                f"{name} mismatch: observed={fields.get(name)!r} expected={expected_value!r}"
            )
    return {
        "command": command,
        "log": str(log_path),
        "returncode": returncode,
        "observed": fields,
        "expected": expected_fields,
        "errors": errors,
        "passed": not errors,
    }


def run_controlled_release_build(args: argparse.Namespace) -> dict:
    """Build the exact host binary measured by strict qualification profiles."""

    inference = pathlib.Path(__file__).resolve().parents[2]
    repo = pathlib.Path(__file__).resolve().parents[5]
    canonical_binary = (inference / "zig-out/bin/antfly-inference").resolve()
    configured_binary = pathlib.Path(args.binary).resolve()
    pinned_zig = repo / ".tools/zig-x86_64-linux-0.17.0-dev.1933+e19a73c2d/zig"
    zig = pinned_zig if pinned_zig.is_file() else pathlib.Path("zig")
    command = [
        str(zig),
        "build",
        "-Dcuda=true",
        "-Dmetal=false",
        "-Dcuda-artifacts=sm89",
        "-Doptimize=ReleaseFast",
        "--global-cache-dir",
        str(STRICT_ZIG_GLOBAL_CACHE_DIR),
    ]
    log_path = args.output_dir / "controlled_release_build.log"
    errors: list[str] = []
    if configured_binary != canonical_binary:
        errors.append(
            f"strict qualification binary must be {canonical_binary}, observed {configured_binary}"
        )
        return {
            "command": command,
            "cwd": str(inference),
            "binary": str(configured_binary),
            "canonical_binary": str(canonical_binary),
            "log": str(log_path),
            "returncode": None,
            "errors": errors,
            "passed": False,
        }
    environment = qualification_runtime_environment(strict=True)
    returncode, _ = _run_logged_in_directory(
        command,
        environment,
        log_path,
        getattr(args, "build_timeout_sec", 1800),
        inference,
    )
    if returncode != 0:
        errors.append(f"controlled ReleaseFast SM89 build exited {returncode}")
    if not canonical_binary.is_file():
        errors.append("controlled ReleaseFast SM89 build did not produce the canonical binary")
    return {
        "command": command,
        "cwd": str(inference),
        "binary": str(configured_binary),
        "canonical_binary": str(canonical_binary),
        "log": str(log_path),
        "returncode": returncode,
        "errors": errors,
        "passed": not errors,
    }


def run_artifact_freshness_checks(
    args: argparse.Namespace,
    provenance: dict,
    controlled_release_build: dict | None = None,
) -> dict:
    """Run each expensive freshness check once, before timed qualification."""
    zig_path = ((provenance.get("toolchains") or {}).get("zig") or {}).get("path")
    inference = pathlib.Path(__file__).resolve().parents[2]
    source_log = args.output_dir / "generated_source_check.log"
    source_command = [
        str(zig_path or "zig"),
        "build",
        "quant-kernel-codegen",
        "-Dcuda=true",
        "-Dmetal=false",
        "-Dcuda-artifacts=sm89",
        "-Doptimize=ReleaseFast",
        "--",
        "--check",
    ]
    environment = os.environ.copy()
    source_returncode, _ = _run_logged_in_directory(
        source_command,
        environment,
        source_log,
        getattr(args, "build_timeout_sec", 1800),
        inference,
    )
    source_check = {
        "command": source_command,
        "log": str(source_log),
        "returncode": source_returncode,
        "passed": source_returncode == 0,
    }
    artifact_args = argparse.Namespace(**vars(args))
    artifact_args.timeout_sec = getattr(args, "artifact_timeout_sec", 1800)
    cuda_check = release_provenance.artifact_check(artifact_args, environment)
    embedded_check = run_embedded_cuda_artifact_check(
        args,
        provenance,
        environment,
        inference,
    )
    return {
        "controlled_release_build": controlled_release_build or {
            "passed": False,
            "errors": ["controlled ReleaseFast build result was not provided"],
        },
        "generated_sources": source_check,
        "canonical_cuda_artifacts": cuda_check,
        "embedded_binary_artifact": embedded_check,
    }


def artifact_freshness_attestation(
    before: dict,
    after: dict,
    checks: dict,
) -> dict:
    before_artifacts = before.get("cuda_artifacts") or {}
    after_artifacts = after.get("cuda_artifacts") or {}
    artifacts_unchanged = (
        _valid_sha256(before_artifacts.get("sha256"))
        and before_artifacts.get("sha256") == after_artifacts.get("sha256")
    )
    binding_unchanged = (
        _valid_sha256(before.get("sha256"))
        and before.get("sha256") == after.get("sha256")
    )
    checks_passed = all(
        bool((checks.get(name) or {}).get("passed"))
        for name in (
            "controlled_release_build",
            "generated_sources",
            "canonical_cuda_artifacts",
            "embedded_binary_artifact",
        )
    )
    return {
        "schema": ARTIFACT_FRESHNESS_SCHEMA,
        "stage": "single_pre_run_check",
        "checks_run_once": True,
        "checks": checks,
        "artifacts_before_sha256": before_artifacts.get("sha256"),
        "artifacts_after_sha256": after_artifacts.get("sha256"),
        "artifacts_unchanged_by_checks": artifacts_unchanged,
        "qualification_binding_before_sha256": before.get("sha256"),
        "qualification_binding_after_sha256": after.get("sha256"),
        "qualification_binding_unchanged_by_checks": binding_unchanged,
        "passed": checks_passed and artifacts_unchanged and binding_unchanged,
    }


def capture_qualification_runtime_guard(
    provenance: dict,
    stage: str,
    runtime_environment: dict[str, str] | None = None,
) -> dict:
    expected_gpu = ((provenance.get("gpu") or {}).get("execution_state") or {})
    observed_gpu = warm_server_provenance.capture_gpu_execution_state(
        runtime_environment
    )
    processes = warm_server_provenance.capture_selected_gpu_compute_processes(
        observed_gpu
    )
    errors: list[str] = []
    if observed_gpu.get("error") or len(observed_gpu.get("selected_gpus") or []) != 1:
        errors.append(f"{stage}: could not capture exactly one selected GPU")
    elif observed_gpu != expected_gpu:
        errors.append(f"{stage}: selected-GPU identity, clocks, or power state changed")
    if processes.get("error"):
        errors.append(f"{stage}: could not inspect selected-GPU compute processes")
    elif processes.get("selected_gpu_processes"):
        errors.append(
            f"{stage}: unexpected selected-GPU compute processes: "
            f"{processes['selected_gpu_processes']!r}"
        )
    return {
        "schema": RUNTIME_GUARD_SCHEMA,
        "stage": stage,
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "gpu_execution_state": observed_gpu,
        "selected_gpu_compute_processes": processes,
        "errors": errors,
        "passed": not errors,
    }


def nested_value(value: object, path: str) -> object | None:
    current = value
    for component in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(component)
    return current


def timing_counter(timing: dict, key: str, metadata: TimingMetadata = DEFAULT_TIMING_METADATA) -> int:
    group = nested_value(timing, metadata.counter_group)
    if not isinstance(group, dict):
        return 0
    try:
        return int(group.get(key) or 0)
    except (TypeError, ValueError, OverflowError):
        return 0


def cuda_counter(timing: dict, key: str) -> int:
    """Compatibility wrapper for callers using the original CUDA timing schema."""
    return timing_counter(timing, key)


def candidate_spec(candidate: CandidateKind | str) -> CandidateSpec:
    if isinstance(candidate, CandidateKind):
        return CANDIDATE_SPECS[candidate]
    return CANDIDATE_CATALOG[candidate]


def parse_route_counter(value: str) -> RouteCounter:
    raw = value.strip()
    if not raw:
        raise ValueError("route counter must not be empty")
    name, separator, label = raw.partition("=")
    name = name.strip()
    label = label.strip() if separator else name.replace("_", " ")
    if not name or not label:
        raise ValueError(f"invalid route counter descriptor: {value!r}")
    return RouteCounter(name, label)


def parse_environment_override(value: str, option: str) -> tuple[str, str]:
    """Parse one NAME=VALUE override without shell interpolation."""
    name, separator, configured_value = value.partition("=")
    if not separator:
        raise ValueError(f"{option} must use NAME=VALUE: {value!r}")
    if not ENVIRONMENT_VARIABLE_RE.fullmatch(name):
        raise ValueError(f"{option} variable name is invalid: {name!r}")
    if any(ord(character) < 32 or ord(character) == 127 for character in configured_value):
        raise ValueError(f"{option} value contains an unsupported control character: {name!r}")
    return name, configured_value


def parse_candidate_environment(value: str) -> tuple[str, str]:
    """Parse one candidate-only NAME=VALUE override without shell interpolation."""
    return parse_environment_override(value, "candidate environment override")


def parse_common_environment(value: str) -> tuple[str, str]:
    """Parse one baseline-and-candidate NAME=VALUE override without shell interpolation."""
    return parse_environment_override(value, "common environment override")


def parse_candidate_environment_list(values: list[str]) -> tuple[tuple[str, str], ...]:
    overrides = tuple(parse_candidate_environment(value) for value in values)
    names = tuple(name for name, _ in overrides)
    if len(set(names)) != len(names):
        raise ValueError("candidate environment variable overrides must be unique")
    return overrides


def parse_common_environment_list(values: list[str]) -> tuple[tuple[str, str], ...]:
    overrides = tuple(parse_common_environment(value) for value in values)
    names = tuple(name for name, _ in overrides)
    if len(set(names)) != len(names):
        raise ValueError("common environment variable overrides must be unique")
    return overrides


def parse_route_counter_list(values: list[str] | None) -> tuple[RouteCounter, ...] | None:
    if values is None:
        return None
    counters = []
    for value in values:
        counters.extend(parse_route_counter(part) for part in value.split(","))
    return tuple(counters)


def resolve_candidate_spec(args: argparse.Namespace) -> CandidateSpec:
    base = CANDIDATE_CATALOG.get(args.kernel_id) if args.kernel_id is not None else candidate_spec(args.candidate)
    required = parse_route_counter_list(args.required_route_counter)
    forbidden = parse_route_counter_list(args.forbidden_route_counter)
    if base is None:
        if args.candidate_environment_variable is None:
            raise ValueError("uncatalogued kernel ID requires --candidate-environment-variable")
        if not required:
            raise ValueError("uncatalogued kernel ID requires at least one --required-route-counter")
        return CandidateSpec(
            kernel_id=args.kernel_id,
            environment_variable=args.candidate_environment_variable,
            required_route_counters=required,
            forbidden_route_counters=forbidden or (),
        )
    return CandidateSpec(
        kernel_id=base.kernel_id,
        environment_variable=args.candidate_environment_variable or base.environment_variable,
        required_route_counters=required if required is not None else base.required_route_counters,
        forbidden_route_counters=forbidden if forbidden is not None else base.forbidden_route_counters,
        required_baseline_route_counters=base.required_baseline_route_counters,
        legacy_kind=base.legacy_kind,
        requires_explicit_model=base.requires_explicit_model,
        fixed_comparison_environment=base.fixed_comparison_environment,
        baseline_gate_value=base.baseline_gate_value,
        candidate_gate_value=base.candidate_gate_value,
        route_phase=base.route_phase,
        qualification_workload=base.qualification_workload,
        qualification_route_counts=base.qualification_route_counts,
        require_persistent_replay=base.require_persistent_replay,
    )


def resolve_candidate_environment(
    args: argparse.Namespace,
    spec: CandidateSpec,
) -> tuple[tuple[str, str], ...]:
    overrides = parse_candidate_environment_list(args.candidate_env)
    if any(name == spec.environment_variable for name, _ in overrides):
        raise ValueError(
            f"--candidate-env must not override selected candidate gate {spec.environment_variable}; "
            "use --candidate-environment-variable to select a different gate"
        )
    fixed_names = {name for name, _ in spec.fixed_comparison_environment}
    conflicting = sorted(name for name, _ in overrides if name in fixed_names)
    if conflicting:
        raise ValueError(
            "--candidate-env must not override the fixed comparison environment "
            f"({', '.join(conflicting)})"
        )
    return overrides


def resolve_common_environment(
    args: argparse.Namespace,
    spec: CandidateSpec,
    candidate_environment: tuple[tuple[str, str], ...] = (),
) -> tuple[tuple[str, str], ...]:
    overrides = parse_common_environment_list(args.common_env)
    names = {name for name, _ in overrides}
    if spec.environment_variable in names:
        raise ValueError(
            f"--common-env must not override selected candidate gate {spec.environment_variable}; "
            "use --candidate-environment-variable to select a different gate"
        )
    capture_overrides = sorted(names & CAPTURE_KV_CAPACITY_ENVIRONMENT_VARIABLES)
    if capture_overrides:
        raise ValueError(
            "--common-env must not override capture KV capacity "
            f"({', '.join(capture_overrides)}); use --capture-kv-capacity"
        )
    candidate_names = {name for name, _ in candidate_environment}
    overlapping = sorted(names & candidate_names)
    if overlapping:
        raise ValueError(
            "--common-env and --candidate-env must not override the same variable "
            f"({', '.join(overlapping)})"
        )
    fixed = dict(spec.fixed_comparison_environment)
    conflicts = sorted(
        name
        for name, value in overrides
        if name in fixed and fixed[name] != value
    )
    if conflicts:
        raise ValueError(
            "--common-env must not override the fixed comparison environment "
            f"({', '.join(conflicts)})"
        )
    return spec.fixed_comparison_environment + tuple(
        (name, value) for name, value in overrides if name not in fixed
    )


def timing_metadata_from_args(args: argparse.Namespace) -> TimingMetadata:
    return TimingMetadata(
        counter_group=args.timing_counter_group,
        throughput_field=args.timing_throughput_field,
        throughput_unit=args.timing_throughput_unit,
        require_persistent_replay=not args.no_require_persistent_replay,
    )


def timing_metadata(metadata: TimingMetadata) -> dict:
    return {
        "counter_group": metadata.counter_group,
        "throughput_field": metadata.throughput_field,
        "throughput_unit": metadata.throughput_unit,
        "require_persistent_replay": metadata.require_persistent_replay,
        "persistent_replay_counter": metadata.persistent_replay_counter,
        "graph_discard_counter": metadata.graph_discard_counter,
        "graph_capacity_skip_counter": metadata.graph_capacity_skip_counter,
    }


def candidate_environment_metadata(
    spec: CandidateSpec,
    candidate_overrides: tuple[tuple[str, str], ...],
    common_overrides: tuple[tuple[str, str], ...] = (),
) -> dict:
    configured_candidate = dict(candidate_overrides)
    configured_common = dict(common_overrides)

    def schedule_override_metadata(environment_variable: str) -> dict:
        if environment_variable in configured_candidate:
            return {
                "environment_variable": environment_variable,
                "value": configured_candidate[environment_variable],
                "source": "candidate-env",
            }
        if environment_variable in configured_common:
            return {
                "environment_variable": environment_variable,
                "value": configured_common[environment_variable],
                "source": "common-env",
            }
        inherited = os.environ.get(environment_variable)
        if inherited is not None:
            return {
                "environment_variable": environment_variable,
                "value": inherited,
                "source": "inherited-environment",
            }
        return {
            "environment_variable": environment_variable,
            "value": None,
            "source": "binary-default",
        }

    return {
        "baseline_gate": {spec.environment_variable: spec.baseline_gate_value},
        "candidate_gate": {spec.environment_variable: spec.candidate_gate_value},
        "fixed_comparison_overrides": dict(spec.fixed_comparison_environment),
        "common_overrides": configured_common,
        "candidate_overrides": configured_candidate,
        "generated_attention_split_kv_min_tokens": schedule_override_metadata(
            GENERATED_ATTENTION_SPLIT_KV_MIN_TOKENS_ENV
        ),
        "generated_attention_split_kv_splits": schedule_override_metadata(
            GENERATED_ATTENTION_SPLIT_KV_SPLITS_ENV
        ),
    }


def resolve_capture_kv_capacity(args: argparse.Namespace) -> tuple[int, str]:
    if args.capture_kv_capacity is not None:
        if args.capture_kv_capacity < 1:
            raise ValueError("capture-kv-capacity must be positive")
        return args.capture_kv_capacity, "explicit"
    if not args.lengths or any(length < 1 for length in args.lengths):
        raise ValueError("output lengths must be positive before resolving capture KV capacity")
    return max(args.lengths) + DEFAULT_CAPTURE_KV_PROMPT_HEADROOM, "max-output-plus-prompt-headroom"


def validate_qualification_contract(
    args: argparse.Namespace,
    spec: CandidateSpec | None = None,
    prompt_fixture_metadata: list[dict] | None = None,
) -> None:
    """Keep cataloged fixed-sample evidence from being weakened at invocation time."""
    if spec is None:
        spec = resolve_candidate_spec(args)
    if spec.require_persistent_replay and getattr(args, "no_require_persistent_replay", False):
        raise ValueError(f"{spec.kernel_id} requires persistent replay validation")
    if args.qualification_profile == "legacy":
        return
    catalog_spec = CANDIDATE_CATALOG.get(spec.kernel_id)
    if catalog_spec is None:
        raise ValueError(f"{args.qualification_profile} requires a cataloged kernel ID")
    if (
        args.candidate_environment_variable is not None
        or args.required_route_counter is not None
        or args.forbidden_route_counter is not None
        or spec != catalog_spec
    ):
        raise ValueError(
            f"{args.qualification_profile} requires the immutable catalog definition for {spec.kernel_id}; "
            "gate and route-counter overrides are legacy-only"
        )
    canonical_artifact_check = (
        SCRIPTS_DIR / "regen-cuda-artifacts.sh"
    ).resolve()
    canonical_wrapper = (
        pathlib.Path(__file__).resolve().parent / "with_gemma4_qat_cuda_tuning.sh"
    ).resolve()
    if pathlib.Path(args.wrapper).resolve() != canonical_wrapper:
        raise ValueError(
            f"{args.qualification_profile} requires the canonical tuning wrapper "
            f"{canonical_wrapper}"
        )
    artifact_check_script = pathlib.Path(
        getattr(args, "artifact_check_script", canonical_artifact_check)
    ).resolve()
    if artifact_check_script != canonical_artifact_check:
        raise ValueError(
            f"{args.qualification_profile} requires the canonical artifact check script "
            f"{canonical_artifact_check}"
        )
    candidate_overrides = parse_candidate_environment_list(getattr(args, "candidate_env", []))
    if candidate_overrides:
        raise ValueError(
            f"{args.qualification_profile} forbids --candidate-env; cataloged gate values and "
            "the fixed comparison environment must define every candidate-only difference"
        )
    common_overrides = parse_common_environment_list(getattr(args, "common_env", []))
    allowed_common = {"CUDA_VISIBLE_DEVICES", "CUDA_DEVICE_ORDER"}
    unsupported_common = sorted(name for name, _ in common_overrides if name not in allowed_common)
    if unsupported_common:
        raise ValueError(
            f"{args.qualification_profile} permits only GPU-selection variables in --common-env; "
            "unsupported: " + ", ".join(unsupported_common)
        )
    for name, value in common_overrides:
        if name == "CUDA_VISIBLE_DEVICES" and not re.fullmatch(r"GPU-[0-9A-Fa-f-]+", value):
            raise ValueError(
                f"{args.qualification_profile} requires a full GPU UUID in CUDA_VISIBLE_DEVICES"
            )
        if name == "CUDA_DEVICE_ORDER" and value != "PCI_BUS_ID":
            raise ValueError(
                f"{args.qualification_profile} requires CUDA_DEVICE_ORDER=PCI_BUS_ID"
            )
    if args.qualification_focus != spec.route_phase:
        raise ValueError(
            f"{args.qualification_profile} qualification focus {args.qualification_focus!r} does not match "
            f"candidate route phase {spec.route_phase!r}"
        )

    profile = QUALIFICATION_PROFILES[args.qualification_profile]
    if profile["require_locked_prompt_fixture"] and (not args.prompt_fixture or args.prompt):
        raise ValueError(
            f"{args.qualification_profile} requires --prompt-fixture and forbids unlocked --prompt cases"
        )
    if not args.require_full_route_coverage:
        raise ValueError(f"{args.qualification_profile} requires full route coverage")
    if args.no_require_persistent_replay:
        raise ValueError(f"{args.qualification_profile} requires persistent replay validation")
    if args.bootstrap_samples < 10_000:
        raise ValueError(
            f"{args.qualification_profile} requires at least 10000 bootstrap samples"
        )
    if args.min_candidate_ratio < profile["min_candidate_ratio"]:
        raise ValueError(
            f"{args.qualification_profile} min-candidate-ratio cannot be looser than "
            f"{profile['min_candidate_ratio']}"
        )
    if args.max_cv > profile["max_cv"]:
        raise ValueError(
            f"{args.qualification_profile} max-cv cannot be looser than {profile['max_cv']}"
        )
    for name in (
        "max_total_latency_ratio",
        "max_ttft_ratio",
        "max_decode_latency_ratio",
        "max_total_ci_upper",
        "max_ttft_ci_upper",
        "max_decode_ci_upper",
    ):
        if getattr(args, name) > profile[name]:
            raise ValueError(
                f"{args.qualification_profile} {name.replace('_', '-')} cannot be looser than "
                f"{profile[name]}"
            )

    workload = spec.qualification_workload
    if workload is None:
        return
    if args.prompt or len(args.prompt_fixture) != 1:
        raise ValueError(
            f"{spec.kernel_id} qualification requires exactly one locked --prompt-fixture and forbids --prompt"
        )
    if prompt_fixture_metadata is None:
        prompt_fixture_metadata = [load_prompt_fixture(path)[1] for path in args.prompt_fixture]
    if len(prompt_fixture_metadata) != 1:
        raise ValueError(f"{spec.kernel_id} qualification requires exactly one prompt fixture")
    fixture = prompt_fixture_metadata[0]
    expected_fixture_fields = {
        "id": workload.fixture_id,
        "file_sha256": workload.fixture_file_sha256,
        "benchmark_prompt_sha256": workload.benchmark_prompt_sha256,
        "benchmark_prompt_tokens": workload.benchmark_prompt_tokens,
    }
    for name, expected in expected_fixture_fields.items():
        if fixture.get(name) != expected:
            raise ValueError(
                f"{spec.kernel_id} qualification requires exact fixture {name}={expected!r}, "
                f"observed {fixture.get(name)!r}"
            )
    workload_fields = {
        "lengths": (tuple(args.lengths), workload.lengths),
        "cache-dtype": (args.cache_dtype, workload.cache_dtype),
        "prefill-chunk-size": (args.prefill_chunk_size, workload.prefill_chunk_size),
        "capture-kv-capacity": (args.capture_kv_capacity, workload.capture_kv_capacity),
    }
    for name, (observed, expected) in workload_fields.items():
        if observed != expected:
            raise ValueError(
                f"{spec.kernel_id} qualification requires {name}={expected!r}, observed {observed!r}"
            )


def result_config_metadata(
    args: argparse.Namespace,
    metadata: TimingMetadata,
    spec: CandidateSpec | None = None,
    candidate_environment: tuple[tuple[str, str], ...] = (),
    capture_kv_capacity: tuple[int, str] | None = None,
    common_environment: tuple[tuple[str, str], ...] = (),
) -> dict:
    environment = (
        candidate_environment_metadata(spec, candidate_environment, common_environment)
        if spec is not None
        else None
    )
    return {
        "model": {
            "path": str(args.model.resolve()),
            "label": args.model_label,
        },
        # Flat aliases preserve compatibility with simple JSON/CSV consumers.
        "model_path": str(args.model.resolve()),
        "model_label": args.model_label,
        "config_label": args.config_label,
        "cache_dtype": args.cache_dtype,
        "prefill_chunk_size": getattr(args, "prefill_chunk_size", 32),
        "repeats": args.repeats,
        "min_candidate_ratio": args.min_candidate_ratio,
        "max_cv": args.max_cv,
        "qualification": {
            "profile": getattr(args, "qualification_profile", "legacy"),
            "focus": getattr(args, "qualification_focus", "ad_hoc"),
            "fixed_samples": getattr(args, "qualification_fixed_samples", None),
            "bootstrap_samples": getattr(args, "bootstrap_samples", 10_000),
            "bootstrap_seed": getattr(args, "bootstrap_seed", 20260730),
            "require_phase_metrics": getattr(args, "require_phase_metrics", False),
            "require_full_route_coverage": getattr(args, "require_full_route_coverage", False),
            "require_locked_prompt_fixture": getattr(args, "require_locked_prompt_fixture", False),
            "strict_provenance_required": (
                getattr(args, "qualification_profile", "legacy") != "legacy"
            ),
            "latency_ratio_limits": {
                "total_latency_ms": getattr(args, "max_total_latency_ratio", None),
                "ttft_ms": getattr(args, "max_ttft_ratio", None),
                "decode_ms": getattr(args, "max_decode_latency_ratio", None),
            },
            "latency_ci_upper_limits": {
                "total_latency_ms": getattr(args, "max_total_ci_upper", None),
                "ttft_ms": getattr(args, "max_ttft_ci_upper", None),
                "decode_ms": getattr(args, "max_decode_ci_upper", None),
            },
        },
        "prompts": getattr(args, "prompt_contracts", []),
        "prompt_fixtures": getattr(args, "prompt_fixture_metadata", []),
        "timing": timing_metadata(metadata),
        "candidate_environment": environment,
        "capture_kv_capacity": (
            {
                "tokens": capture_kv_capacity[0],
                "source": capture_kv_capacity[1],
            }
            if capture_kv_capacity is not None
            else None
        ),
    }


def candidate_metadata(spec: CandidateSpec) -> dict:
    legacy_kind = spec.legacy_kind.value if spec.legacy_kind is not None else None
    workload = spec.qualification_workload
    return {
        "kind": legacy_kind or spec.kernel_id,
        "legacy_kind": legacy_kind,
        "kernel_id": spec.kernel_id,
        "catalog_id": spec.kernel_id,
        "environment_variable": spec.environment_variable,
        "baseline_gate_value": spec.baseline_gate_value,
        "candidate_gate_value": spec.candidate_gate_value,
        "route_phase": spec.route_phase,
        "route_counter": spec.route_counter,
        "route_counters": [route.name for route in spec.required_route_counters],
        "required_route_counters": [route.name for route in spec.required_route_counters],
        "required_baseline_route_counters": [
            route.name for route in spec.required_baseline_route_counters
        ],
        "candidate_forbidden_counters": [counter.name for counter in spec.forbidden_route_counters],
        "forbidden_route_counters": [counter.name for counter in spec.forbidden_route_counters],
        "requires_explicit_model": spec.requires_explicit_model,
        "require_persistent_replay": spec.require_persistent_replay,
        "qualification_workload": (
            {
                "fixture_id": workload.fixture_id,
                "fixture_file_sha256": workload.fixture_file_sha256,
                "benchmark_prompt_sha256": workload.benchmark_prompt_sha256,
                "benchmark_prompt_tokens": workload.benchmark_prompt_tokens,
                "lengths": list(workload.lengths),
                "cache_dtype": workload.cache_dtype,
                "prefill_chunk_size": workload.prefill_chunk_size,
                "capture_kv_capacity": workload.capture_kv_capacity,
            }
            if workload is not None
            else None
        ),
        "qualification_route_counts": [
            {"name": expectation.name, "exact_count": expectation.exact_count}
            for expectation in spec.qualification_route_counts
        ],
    }


def configure_candidate_environment(
    env: dict[str, str],
    spec: CandidateSpec,
    enabled: bool,
    overrides: tuple[tuple[str, str], ...] = (),
    common_overrides: tuple[tuple[str, str], ...] = (),
) -> None:
    env.update(common_overrides)
    env[spec.environment_variable] = (
        spec.candidate_gate_value if enabled else spec.baseline_gate_value
    )
    if enabled:
        env.update(overrides)


def execution_order(repetition: int) -> tuple[bool, bool]:
    """Return False for baseline and True for candidate."""
    return balanced_pair_order(repetition + 1, False, True)


def repetition_stem(stem: str, repetition: int, repeats: int) -> str:
    return stem if repeats == 1 else f"{stem}-r{repetition + 1:02d}"


def validate_pair(
    baseline: dict,
    candidate: dict,
    requested_tokens: int,
    spec: CandidateSpec = DEFAULT_CANDIDATE,
    timing_info: TimingMetadata = DEFAULT_TIMING_METADATA,
    require_full_route_coverage: bool = False,
    expected_prompt_tokens: int | None = None,
) -> list[str]:
    errors = []
    exact_route_counts = {
        expectation.name: expectation.exact_count
        for expectation in spec.qualification_route_counts
    }
    baseline_ids = baseline.get("token_ids") or []
    candidate_ids = candidate.get("token_ids") or []
    if not baseline_ids or not candidate_ids:
        errors.append("missing generated token IDs")
    elif baseline_ids != candidate_ids:
        mismatch = next(
            (index for index, pair in enumerate(zip(baseline_ids, candidate_ids)) if pair[0] != pair[1]),
            min(len(baseline_ids), len(candidate_ids)),
        )
        errors.append(f"token IDs differ at index {mismatch}")
    if len(baseline_ids) != requested_tokens or len(candidate_ids) != requested_tokens:
        errors.append(
            f"expected {requested_tokens} tokens, got baseline={len(baseline_ids)} candidate={len(candidate_ids)}"
        )

    baseline_prompt_ids = baseline.get("prompt_token_ids") or []
    candidate_prompt_ids = candidate.get("prompt_token_ids") or []
    if expected_prompt_tokens is not None and (not baseline_prompt_ids or not candidate_prompt_ids):
        errors.append("missing prompt token IDs for the locked prompt fixture")
    elif (baseline_prompt_ids or candidate_prompt_ids) and baseline_prompt_ids != candidate_prompt_ids:
        errors.append("baseline and candidate prompt token IDs differ")
    if expected_prompt_tokens is not None and (
        len(baseline_prompt_ids) != expected_prompt_tokens
        or len(candidate_prompt_ids) != expected_prompt_tokens
    ):
        errors.append(
            f"expected {expected_prompt_tokens} prompt tokens, "
            f"got baseline={len(baseline_prompt_ids)} candidate={len(candidate_prompt_ids)}"
        )

    if timing_info.require_persistent_replay:
        for label, timing in (("baseline", baseline), ("candidate", candidate)):
            minimum_replays = max(1, requested_tokens - 8)
            replays = timing_counter(timing, timing_info.persistent_replay_counter, timing_info)
            if replays < minimum_replays:
                errors.append(f"{label} persistent replays {replays} below {minimum_replays}")
            if timing_counter(timing, timing_info.graph_discard_counter, timing_info) != 0:
                errors.append(f"{label} reported graph capture discards")
            if timing_counter(timing, timing_info.graph_capacity_skip_counter, timing_info) != 0:
                errors.append(f"{label} reported graph capacity skips")

    for route in spec.required_baseline_route_counters:
        if timing_counter(baseline, route.name, timing_info) == 0:
            errors.append(f"baseline did not use {route.label}")
    for route in spec.required_route_counters:
        if timing_counter(baseline, route.name, timing_info) != 0:
            errors.append(f"baseline unexpectedly used {route.label}")
        candidate_hits = timing_counter(candidate, route.name, timing_info)
        if candidate_hits == 0:
            errors.append(f"candidate did not use {route.label}")
        if (
            candidate_hits != 0
            and require_full_route_coverage
            and route.name in exact_route_counts
        ):
            expected_hits = exact_route_counts[route.name]
            if candidate_hits != expected_hits:
                errors.append(
                    f"candidate {route.label} count {candidate_hits} did not match locked "
                    f"qualification count {expected_hits}"
                )
        if require_full_route_coverage and not timing_info.require_persistent_replay:
            errors.append("full route coverage requires persistent replay validation")
    for counter in spec.forbidden_route_counters:
        count = timing_counter(candidate, counter.name, timing_info)
        if count != 0:
            errors.append(f"candidate reported {counter.label}: {count}")
    for label, timing in (("baseline", baseline), ("candidate", candidate)):
        throughput = timing_throughput(timing, timing_info)
        if throughput <= 0.0:
            errors.append(f"{label} reported non-positive decode throughput")
    return errors


def pair_attestation(
    baseline: dict,
    candidate: dict,
    requested_tokens: int,
    spec: CandidateSpec = DEFAULT_CANDIDATE,
    timing_info: TimingMetadata = DEFAULT_TIMING_METADATA,
    require_full_route_coverage: bool = False,
) -> dict:
    minimum_replays = max(1, requested_tokens - 8)

    def graph(timing: dict) -> dict:
        return {
            "required": timing_info.require_persistent_replay,
            "minimum_persistent_replays": minimum_replays,
            "persistent_replays": timing_counter(timing, timing_info.persistent_replay_counter, timing_info),
            "discards": timing_counter(timing, timing_info.graph_discard_counter, timing_info),
            "capacity_skips": timing_counter(timing, timing_info.graph_capacity_skip_counter, timing_info),
        }

    candidate_replays = timing_counter(candidate, timing_info.persistent_replay_counter, timing_info)
    candidate_discards = timing_counter(candidate, timing_info.graph_discard_counter, timing_info)
    candidate_capacity_skips = timing_counter(candidate, timing_info.graph_capacity_skip_counter, timing_info)
    stable_replay_coverage = (
        timing_info.require_persistent_replay
        and candidate_replays >= minimum_replays
        and candidate_discards == 0
        and candidate_capacity_skips == 0
    )
    required_routes = {}
    exact_route_counts = {
        expectation.name: expectation.exact_count
        for expectation in spec.qualification_route_counts
    }
    for route in spec.required_route_counters:
        baseline_hits = timing_counter(baseline, route.name, timing_info)
        candidate_hits = timing_counter(candidate, route.name, timing_info)
        route_observed = candidate_hits > 0
        expected_candidate_hits = exact_route_counts.get(route.name)
        exact_count_attested = (
            candidate_hits == expected_candidate_hits
            if require_full_route_coverage and expected_candidate_hits is not None
            else True
        )
        route_attested = (
            baseline_hits == 0
            and route_observed
            and exact_count_attested
            and (stable_replay_coverage if spec.route_phase == "decode" else True)
        )
        required_routes[route.name] = {
            "label": route.label,
            "phase": spec.route_phase,
            "baseline": baseline_hits,
            "candidate": candidate_hits,
            "minimum_candidate_launch_observations": 1,
            "expected_candidate_launch_observations": (
                expected_candidate_hits if require_full_route_coverage else None
            ),
            "exact_count_attested": exact_count_attested,
            "observed_in_candidate_phase": route_observed,
            "observed_during_candidate_graph_construction": (
                route_observed if spec.route_phase == "decode" else False
            ),
            "absent_from_baseline": baseline_hits == 0,
            "stable_replay_coverage": stable_replay_coverage,
            "route_attested": route_attested,
            "route_replay_attested": route_attested and spec.route_phase == "decode",
        }
    required_baseline_routes = {}
    for route in spec.required_baseline_route_counters:
        baseline_hits = timing_counter(baseline, route.name, timing_info)
        candidate_hits = timing_counter(candidate, route.name, timing_info)
        required_baseline_routes[route.name] = {
            "label": route.label,
            "phase": spec.route_phase,
            "baseline": baseline_hits,
            "candidate": candidate_hits,
            "minimum_baseline_launch_observations": 1,
            "observed_in_baseline_phase": baseline_hits > 0,
            "baseline_route_attested": baseline_hits > 0,
        }
    forbidden_routes = {
        counter.name: {
            "label": counter.label,
            "baseline": timing_counter(baseline, counter.name, timing_info),
            "candidate": timing_counter(candidate, counter.name, timing_info),
        }
        for counter in spec.forbidden_route_counters
    }
    return {
        "requested_tokens": requested_tokens,
        "graph": {"baseline": graph(baseline), "candidate": graph(candidate)},
        "required_baseline_routes": required_baseline_routes,
        "required_routes": required_routes,
        "forbidden_routes": forbidden_routes,
        "stable_route_replay_attested": all(
            route["route_replay_attested"] for route in required_routes.values()
        ),
        "all_required_routes_attested": all(
            route["route_attested"] for route in required_routes.values()
        ),
        "all_required_baseline_routes_attested": all(
            route["baseline_route_attested"] for route in required_baseline_routes.values()
        ),
    }


def run_case(
    args: argparse.Namespace,
    prompt: str,
    tokens: int,
    enabled: bool,
    stem: str,
    spec: CandidateSpec,
    candidate_environment: tuple[tuple[str, str], ...],
    capture_kv_capacity: int,
    common_environment: tuple[tuple[str, str], ...] = (),
) -> dict:
    mode = "candidate" if enabled else "baseline"
    timing_path = args.output_dir / f"{stem}-{mode}.json"
    log_path = args.output_dir / f"{stem}-{mode}.log"
    command = [
        str(args.wrapper),
        str(args.binary),
        "generate",
        str(args.model),
        prompt,
        "--backend", "cuda",
        "--combined-budget-mb", "22000",
        "--backend-budget-mb", "19000",
        "--kv-budget-mb", "1024",
        "--scratch-budget-mb", "2048",
        "--prefill-chunk-size", str(getattr(args, "prefill_chunk_size", 32)),
        "--max-tokens", str(tokens),
        "--temperature", "0",
        "--raw-prompt",
        "--no-chat-template",
        "--ignore-eos",
        "--cache-dtype", getattr(args, "cache_dtype", "f32"),
        "--print-token-count",
        "--print-prompt-token-ids",
        "--print-token-ids",
        "--print-timing",
        "--json-timing", str(timing_path),
    ]
    env = dict(getattr(args, "qualification_runtime_environment", os.environ))
    configure_candidate_environment(env, spec, enabled, candidate_environment, common_environment)
    env[CAPTURE_KV_CAPACITY_ENV] = str(capture_kv_capacity)
    completed = subprocess.run(
        command,
        cwd=pathlib.Path(__file__).resolve().parents[5],
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=args.timeout_sec,
        check=False,
    )
    log_path.write_text(completed.stdout, encoding="utf-8")
    if completed.returncode != 0:
        raise RuntimeError(f"{mode} command failed with exit {completed.returncode}; see {log_path}")
    if not timing_path.is_file():
        raise RuntimeError(f"{mode} command did not write {timing_path}")
    timing = json.loads(timing_path.read_text(encoding="utf-8"))
    token_match = TOKEN_IDS_RE.search(completed.stdout)
    timing["token_ids"] = [int(value) for value in token_match.group("ids").split()] if token_match else []
    prompt_token_match = PROMPT_TOKEN_IDS_RE.search(completed.stdout)
    timing["prompt_token_ids"] = (
        [int(value) for value in prompt_token_match.group("ids").split()]
        if prompt_token_match else []
    )
    return timing


def timing_throughput(
    timing: dict,
    metadata: TimingMetadata = DEFAULT_TIMING_METADATA,
) -> float:
    try:
        value = float(nested_value(timing, metadata.throughput_field) or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return value if math.isfinite(value) and value > 0.0 else 0.0


def paired_throughput(
    baseline: dict,
    candidate: dict,
    metadata: TimingMetadata = DEFAULT_TIMING_METADATA,
) -> dict:
    baseline_tps = timing_throughput(baseline, metadata)
    candidate_tps = timing_throughput(candidate, metadata)
    ratio = candidate_tps / baseline_tps if baseline_tps > 0 else 0.0
    return {
        "baseline_tok_s": baseline_tps,
        "candidate_tok_s": candidate_tps,
        "candidate_ratio": ratio,
        "delta_tok_s": candidate_tps - baseline_tps,
        "delta_percent": (ratio - 1.0) * 100.0 if baseline_tps > 0 else 0.0,
    }


def _positive_finite_metric(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0.0 else None


def timing_latency_metrics(timing: dict) -> dict:
    timing_ms = timing.get("timing_ms") if isinstance(timing.get("timing_ms"), dict) else {}
    prefill = _positive_finite_metric(timing_ms.get("prefill_inner"))
    decode = _positive_finite_metric(timing_ms.get("decode_inner"))
    total = _positive_finite_metric(timing_ms.get("generate"))
    total_source = "timing_ms.generate"
    if total is None:
        total = _positive_finite_metric(timing_ms.get("total_inner"))
        total_source = "timing_ms.total_inner"
    if total is None and prefill is not None and decode is not None:
        total = prefill + decode
        total_source = "timing_ms.prefill_inner+decode_inner"

    ttft = _positive_finite_metric(timing_ms.get("ttft"))
    ttft_source = "timing_ms.ttft"
    if ttft is None:
        ttft = _positive_finite_metric(timing_ms.get("time_to_first_token"))
        ttft_source = "timing_ms.time_to_first_token"
    if ttft is None:
        # The CLI has no transport boundary. Its prefill_inner interval includes
        # the prompt and prefill-produced first-token work, so record this as a
        # declared TTFT proxy rather than presenting it as HTTP TTFT.
        ttft = prefill
        ttft_source = "timing_ms.prefill_inner_proxy"

    values = {
        "total_latency_ms": total,
        "ttft_ms": ttft,
        "decode_ms": decode,
    }
    return {
        **values,
        "available": all(values[metric] is not None for metric in LATENCY_METRICS),
        "sources": {
            "total_latency_ms": total_source if total is not None else None,
            "ttft_ms": ttft_source if ttft is not None else None,
            "decode_ms": "timing_ms.decode_inner" if decode is not None else None,
        },
    }


def paired_latency(baseline: dict, candidate: dict) -> dict:
    arms = {
        "baseline": timing_latency_metrics(baseline),
        "candidate": timing_latency_metrics(candidate),
    }
    ratios: dict[str, float | None] = {}
    for metric in LATENCY_METRICS:
        baseline_value = arms["baseline"].get(metric)
        candidate_value = arms["candidate"].get(metric)
        ratios[metric] = (
            candidate_value / baseline_value
            if baseline_value is not None and candidate_value is not None and baseline_value > 0.0
            else None
        )
    return {
        **arms,
        "candidate_baseline_ratios": ratios,
        "available": arms["baseline"]["available"] and arms["candidate"]["available"],
    }


def summarize_latency_pairs(
    pairs: list[dict],
    *,
    bootstrap_samples: int = 10_000,
    bootstrap_seed: int = 20260730,
) -> dict:
    if not pairs:
        return {"available": False, "pair_count": 0, "metrics": {}}
    if any(not (pair.get("latency") or {}).get("available") for pair in pairs):
        return {
            "available": False,
            "pair_count": len(pairs),
            "metrics": {},
            "missing_repetitions": [
                pair.get("repetition")
                for pair in pairs
                if not (pair.get("latency") or {}).get("available")
            ],
        }

    metrics = {}
    for metric_index, metric in enumerate(LATENCY_METRICS):
        baseline_values = [float(pair["latency"]["baseline"][metric]) for pair in pairs]
        candidate_values = [float(pair["latency"]["candidate"][metric]) for pair in pairs]
        metrics[metric] = {
            "baseline": distribution(baseline_values),
            "candidate": distribution(candidate_values),
            "candidate_baseline_paired_log_ratio_95_ci": paired_log_ratio_ci(
                list(zip(candidate_values, baseline_values, strict=True)),
                samples=bootstrap_samples,
                seed=bootstrap_seed + metric_index,
            ),
        }
    return {"available": True, "pair_count": len(pairs), "metrics": metrics}


def latency_promotion_errors(summary: dict, args: argparse.Namespace) -> list[str]:
    limits = {
        "total_latency_ms": (
            getattr(args, "max_total_latency_ratio", None),
            getattr(args, "max_total_ci_upper", None),
        ),
        "ttft_ms": (
            getattr(args, "max_ttft_ratio", None),
            getattr(args, "max_ttft_ci_upper", None),
        ),
        "decode_ms": (
            getattr(args, "max_decode_latency_ratio", None),
            getattr(args, "max_decode_ci_upper", None),
        ),
    }
    requires_metrics = bool(getattr(args, "require_phase_metrics", False)) or any(
        threshold is not None for pair in limits.values() for threshold in pair
    )
    if not summary.get("available"):
        return ["required TTFT/decode/total timing metrics are unavailable"] if requires_metrics else []

    errors: list[str] = []
    for metric, (median_limit, ci_limit) in limits.items():
        metric_summary = summary["metrics"][metric]
        ci = metric_summary["candidate_baseline_paired_log_ratio_95_ci"]
        if median_limit is not None and float(ci["median"]) > median_limit:
            errors.append(
                f"{metric} paired median ratio {ci['median']:.6f} above {median_limit:.6f}"
            )
        if ci_limit is not None and float(ci["upper_95"]) > ci_limit:
            errors.append(
                f"{metric} paired 95% upper ratio {ci['upper_95']:.6f} above {ci_limit:.6f}"
            )
        max_cv = float(getattr(args, "max_cv", 1.0))
        for arm in ("baseline", "candidate"):
            cv = float(metric_summary[arm]["cv"])
            if cv > max_cv:
                errors.append(f"{metric} {arm} CV {cv:.6f} above {max_cv:.6f}")
    return errors


def coefficient_of_variation(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = statistics.fmean(values)
    return statistics.pstdev(values) / mean if mean > 0.0 else 1.0


def flatten_pair_throughputs(items: list[dict]) -> list[dict]:
    flattened = []
    for item in items:
        if item.get("pairs") is not None:
            flattened.extend(pair.get("paired_throughput", pair) for pair in item["pairs"])
        else:
            flattened.append(item.get("paired_throughput", item))
    return flattened


def summarize_throughput(items: list[dict]) -> dict:
    pairs = flatten_pair_throughputs(items)
    if not pairs:
        return {
            "baseline_median_tok_s": 0.0,
            "candidate_median_tok_s": 0.0,
            "median_candidate_ratio": 0.0,
            "min_candidate_ratio": 0.0,
            "baseline_tok_s_cv": 0.0,
            "candidate_tok_s_cv": 0.0,
            "pair_count": 0,
        }
    baseline_values = [float(pair["baseline_tok_s"]) for pair in pairs]
    candidate_values = [float(pair["candidate_tok_s"]) for pair in pairs]
    ratios = [float(pair["candidate_ratio"]) for pair in pairs]
    return {
        "baseline_median_tok_s": statistics.median(baseline_values),
        "candidate_median_tok_s": statistics.median(candidate_values),
        "median_candidate_ratio": statistics.median(ratios),
        "min_candidate_ratio": min(ratios),
        "baseline_tok_s_cv": coefficient_of_variation(baseline_values),
        "candidate_tok_s_cv": coefficient_of_variation(candidate_values),
        "pair_count": len(pairs),
    }


def summarize_case(prompt: str, tokens: int, pairs: list[dict]) -> dict:
    throughput = summarize_throughput(pairs)
    pair_errors = []
    for pair in pairs:
        prefix = f"repeat {pair['repetition']}: " if len(pairs) > 1 else ""
        pair_errors.extend(prefix + error for error in pair["errors"])
    legacy_throughput = {
        "baseline_tok_s": throughput["baseline_median_tok_s"],
        "candidate_tok_s": throughput["candidate_median_tok_s"],
        "candidate_ratio": throughput["median_candidate_ratio"],
        "delta_tok_s": statistics.median(pair["delta_tok_s"] for pair in pairs),
        "delta_percent": (throughput["median_candidate_ratio"] - 1.0) * 100.0,
    }
    return {
        "prompt": prompt,
        "tokens": tokens,
        **legacy_throughput,
        "paired_throughput": {**legacy_throughput, **throughput},
        "baseline_median_tok_s": throughput["baseline_median_tok_s"],
        "candidate_median_tok_s": throughput["candidate_median_tok_s"],
        "median_candidate_ratio": throughput["median_candidate_ratio"],
        "min_candidate_ratio": throughput["min_candidate_ratio"],
        "baseline_tok_s_cv": throughput["baseline_tok_s_cv"],
        "candidate_tok_s_cv": throughput["candidate_tok_s_cv"],
        "repeats": len(pairs),
        "pairs": pairs,
        "token_ids_equal": all(pair["token_ids_equal"] for pair in pairs),
        "errors": pair_errors,
    }


def case_promotion_errors(case: dict, min_candidate_ratio: float, max_cv: float) -> list[str]:
    errors = []
    if case["min_candidate_ratio"] < min_candidate_ratio:
        errors.append(
            f"minimum candidate ratio {case['min_candidate_ratio']:.6f} below {min_candidate_ratio:.6f}"
        )
    if case["baseline_tok_s_cv"] > max_cv:
        errors.append(f"baseline throughput CV {case['baseline_tok_s_cv']:.6f} above {max_cv:.6f}")
    if case["candidate_tok_s_cv"] > max_cv:
        errors.append(f"candidate throughput CV {case['candidate_tok_s_cv']:.6f} above {max_cv:.6f}")
    return errors


def main() -> None:
    args = parse_args()
    strict_qualification = args.qualification_profile != "legacy"
    try:
        spec = resolve_candidate_spec(args)
        if args.model is None:
            if spec.requires_explicit_model:
                raise ValueError(
                    f"{spec.kernel_id} requires --model for a GGUF with a compatible Q6_K LM head"
                )
            raise ValueError("--model is required")
        if args.model_label is None:
            args.model_label = args.model.stem
        timing_info = timing_metadata_from_args(args)
        candidate_environment = resolve_candidate_environment(args, spec)
        common_environment = resolve_common_environment(args, spec, candidate_environment)
        capture_kv_capacity = resolve_capture_kv_capacity(args)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if (
        not args.wrapper.is_file()
        or not args.model.exists()
        or (not strict_qualification and not args.binary.is_file())
    ):
        raise SystemExit("binary, tuning wrapper, and model must exist")
    if strict_qualification and not args.artifact_check_script.is_file():
        raise SystemExit("strict-profile artifact checker must exist")
    if (
        args.timeout_sec <= 0
        or args.build_timeout_sec <= 0
        or args.artifact_timeout_sec <= 0
        or args.repeats <= 0
        or args.prefill_chunk_size <= 0
        or any(length < 1 for length in args.lengths)
    ):
        raise SystemExit("timeout, repeats, prefill chunk size, and output lengths must be positive")
    if strict_qualification and args.build_timeout_sec < 1800:
        raise SystemExit("strict qualification requires --build-timeout-sec of at least 1800 seconds")
    if strict_qualification and args.artifact_timeout_sec < 1800:
        raise SystemExit("strict qualification requires --artifact-timeout-sec of at least 1800 seconds")
    if args.qualification_fixed_samples is not None and args.repeats != args.qualification_fixed_samples:
        raise SystemExit(
            f"{args.qualification_profile} qualification requires exactly "
            f"{args.qualification_fixed_samples} paired samples per case"
        )
    if args.bootstrap_samples < 1:
        raise SystemExit("bootstrap-samples must be positive")
    if args.qualification_profile in {"promotion", "prefill-promotion"} and args.bootstrap_samples < 10_000:
        raise SystemExit(
            f"{args.qualification_profile} qualification requires at least 10000 bootstrap samples"
        )
    if not math.isfinite(args.min_candidate_ratio) or args.min_candidate_ratio < 0.0:
        raise SystemExit("min-candidate-ratio must be a finite non-negative number")
    if not math.isfinite(args.max_cv) or args.max_cv < 0.0:
        raise SystemExit("max-cv must be a finite non-negative number")
    for name in (
        "max_total_latency_ratio",
        "max_ttft_ratio",
        "max_decode_latency_ratio",
        "max_total_ci_upper",
        "max_ttft_ci_upper",
        "max_decode_ci_upper",
    ):
        value = getattr(args, name)
        if value is not None and (not math.isfinite(value) or value <= 0.0):
            raise SystemExit(name.replace("_", "-") + " must be a finite positive number")
    if not args.model_label.strip() or not args.config_label.strip():
        raise SystemExit("model-label and config-label must be non-empty")
    try:
        fixture_values = [load_prompt_fixture(path) for path in args.prompt_fixture]
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    fixture_prompts = [prompt for prompt, _ in fixture_values]
    args.prompt_fixture_metadata = [metadata for _, metadata in fixture_values]
    try:
        validate_qualification_contract(args, spec, args.prompt_fixture_metadata)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    prompts = [*args.prompt, *fixture_prompts]
    if not prompts:
        prompts = list(DEFAULT_PROMPTS)
    fixture_metadata_by_digest = {
        metadata["benchmark_prompt_sha256"]: metadata
        for metadata in args.prompt_fixture_metadata
    }
    args.prompt_contracts = [
        prompt_identity(
            prompt,
            source=("fixture" if hashlib.sha256(prompt.encode("utf-8")).hexdigest() in fixture_metadata_by_digest else
                    "explicit" if args.prompt else "default"),
            fixture_id=(
                fixture_metadata_by_digest.get(hashlib.sha256(prompt.encode("utf-8")).hexdigest()) or {}
            ).get("id"),
        )
        for prompt in prompts
    ]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.qualification_profile != "legacy" and any(args.output_dir.iterdir()):
        raise SystemExit("fixed-sample qualification requires a new empty output directory")
    raw_samples_path = args.output_dir / "candidate_samples.jsonl"
    manifest_path = args.output_dir / "evidence_manifest.json"
    raw_samples_path.unlink(missing_ok=True)
    manifest_path.unlink(missing_ok=True)
    controlled_release_build = None
    if strict_qualification:
        controlled_release_build = run_controlled_release_build(args)
        if not controlled_release_build["passed"]:
            preflight_path = args.output_dir / "candidate_preflight.json"
            preflight_path.write_text(
                json.dumps(
                    {
                        "schema": STRICT_PROVENANCE_SCHEMA,
                        "controlled_release_build": controlled_release_build,
                        "errors": controlled_release_build["errors"],
                        "passed": False,
                    },
                    allow_nan=False,
                    indent=2,
                    sort_keys=True,
                ) + "\n",
                encoding="utf-8",
            )
            raise SystemExit("\n".join(controlled_release_build["errors"]))
    runtime_environment = qualification_runtime_environment(
        common_environment,
        strict=strict_qualification,
    )
    args.qualification_runtime_environment = runtime_environment
    try:
        provenance = qualification_provenance(args, runtime_environment)
    except (OSError, RuntimeError) as exc:
        raise SystemExit(f"could not collect stable candidate-qualification provenance: {exc}") from exc
    if strict_qualification:
        preflight_errors = strict_qualification_provenance_errors(provenance)
        if preflight_errors:
            raise SystemExit("\n".join(preflight_errors))
        freshness_checks = run_artifact_freshness_checks(
            args,
            provenance,
            controlled_release_build,
        )
        try:
            post_check_provenance = qualification_provenance(args, runtime_environment)
        except (OSError, RuntimeError) as exc:
            raise SystemExit(
                f"could not verify candidate provenance after artifact freshness checks: {exc}"
            ) from exc
        preflight_errors.extend(
            strict_qualification_provenance_errors(post_check_provenance)
        )
        freshness = artifact_freshness_attestation(
            provenance,
            post_check_provenance,
            freshness_checks,
        )
        if not freshness["passed"]:
            preflight_errors.append(
                "strict qualification artifact/source freshness or pre-run provenance check failed"
            )
        if preflight_errors:
            preflight_path = args.output_dir / "candidate_preflight.json"
            preflight_path.write_text(
                json.dumps(
                    {
                        "schema": STRICT_PROVENANCE_SCHEMA,
                        "provenance": post_check_provenance,
                        "artifact_freshness": freshness,
                        "errors": preflight_errors,
                        "passed": False,
                    },
                    allow_nan=False,
                    indent=2,
                    sort_keys=True,
                ) + "\n",
                encoding="utf-8",
            )
            raise SystemExit("\n".join(preflight_errors))
        provenance = post_check_provenance
        provenance["artifact_freshness"] = freshness
        provenance["runtime_guards"] = []

    runtime_guards_path = args.output_dir / "runtime_guards.json"

    def enforce_runtime_guard(stage: str) -> dict | None:
        if not strict_qualification:
            return None
        guard = capture_qualification_runtime_guard(
            provenance,
            stage,
            runtime_environment,
        )
        provenance["runtime_guards"].append(guard)
        runtime_guards_path.write_text(
            json.dumps(provenance["runtime_guards"], allow_nan=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        if guard["errors"]:
            raise SystemExit("\n".join(guard["errors"]))
        return guard

    cases = []
    validation_failures = []
    promotion_failures = []
    for prompt_index, prompt in enumerate(prompts):
        for tokens in args.lengths:
            stem = f"{prompt_index:02d}-{slug(prompt)}-{tokens}"
            pairs = []
            for repetition in range(args.repeats):
                pair_stem = repetition_stem(stem, repetition, args.repeats)
                before_guard = enforce_runtime_guard(f"before-pair-{pair_stem}")
                runs = {}
                order = execution_order(repetition)
                for enabled in order:
                    runs[enabled] = run_case(
                        args,
                        prompt,
                        tokens,
                        enabled,
                        pair_stem,
                        spec,
                        candidate_environment,
                        capture_kv_capacity[0],
                        common_environment,
                    )
                after_guard = enforce_runtime_guard(f"after-pair-{pair_stem}")
                baseline = runs[False]
                candidate = runs[True]
                errors = validate_pair(
                    baseline,
                    candidate,
                    tokens,
                    spec,
                    timing_info,
                    require_full_route_coverage=args.require_full_route_coverage,
                    expected_prompt_tokens=(
                        fixture_metadata_by_digest.get(
                            hashlib.sha256(prompt.encode("utf-8")).hexdigest(), {}
                        ).get("benchmark_prompt_tokens")
                    ),
                )
                throughput = paired_throughput(baseline, candidate, timing_info)
                latency = paired_latency(baseline, candidate)
                attestation = pair_attestation(
                    baseline,
                    candidate,
                    tokens,
                    spec,
                    timing_info,
                    require_full_route_coverage=args.require_full_route_coverage,
                )
                pair = {
                    "repetition": repetition + 1,
                    "execution_order": ["candidate" if enabled else "baseline" for enabled in order],
                    **throughput,
                    "paired_throughput": throughput,
                    "latency": latency,
                    "attestation": attestation,
                    "runtime_guards": (
                        {"before": before_guard, "after": after_guard}
                        if strict_qualification
                        else None
                    ),
                    "token_ids_equal": (baseline.get("token_ids") or []) == (candidate.get("token_ids") or []),
                    "errors": errors,
                }
                pairs.append(pair)
                raw_sample = {
                    "schema": RAW_SAMPLE_SCHEMA,
                    "case": stem,
                    "prompt": prompt,
                    "prompt_identity": args.prompt_contracts[prompt_index],
                    "tokens": tokens,
                    "pair": pair,
                    "baseline": baseline,
                    "candidate": candidate,
                }
                with raw_samples_path.open("a", encoding="utf-8") as output:
                    output.write(json.dumps(raw_sample, sort_keys=True, allow_nan=False) + "\n")
                validation_failures.extend(f"{pair_stem}: {error}" for error in errors)
                candidate_name = spec.legacy_kind.value if spec.legacy_kind is not None else spec.kernel_id
                print(
                    f"candidate_parity kind={candidate_name} kernel_id={spec.kernel_id} tokens={tokens} "
                    f"repeat={repetition + 1}/{args.repeats} order={','.join(pair['execution_order'])} "
                    f"baseline={throughput['baseline_tok_s']:.3f} "
                    f"candidate={throughput['candidate_tok_s']:.3f} "
                    f"ratio={throughput['candidate_ratio']:.4f} passed={str(not errors).lower()}"
                )

            case = summarize_case(prompt, tokens, pairs)
            case["prompt_identity"] = args.prompt_contracts[prompt_index]
            case["latency"] = summarize_latency_pairs(
                pairs,
                bootstrap_samples=args.bootstrap_samples,
                bootstrap_seed=args.bootstrap_seed,
            )
            latency_errors = latency_promotion_errors(case["latency"], args)
            case["promotion_errors"] = [
                *case_promotion_errors(case, args.min_candidate_ratio, args.max_cv),
                *latency_errors,
            ]
            case["checks"] = {
                "validation": not case["errors"],
                "candidate_ratio": case["min_candidate_ratio"] >= args.min_candidate_ratio,
                "stability": case["baseline_tok_s_cv"] <= args.max_cv and case["candidate_tok_s_cv"] <= args.max_cv,
                "phase_latency": not latency_errors,
            }
            case["passed"] = all(case["checks"].values())
            cases.append(case)
            promotion_failures.extend(f"{stem}: {error}" for error in case["promotion_errors"])

    try:
        post_run_provenance = qualification_provenance(args, runtime_environment)
    except (OSError, RuntimeError) as exc:
        post_run_provenance = None
        validation_failures.append(f"could not verify post-run input provenance: {exc}")
    if strict_qualification and post_run_provenance is not None:
        validation_failures.extend(
            strict_qualification_provenance_errors(post_run_provenance)
        )
    provenance["post_run_sha256"] = (
        post_run_provenance.get("sha256") if post_run_provenance is not None else None
    )
    provenance["unchanged_during_qualification"] = (
        post_run_provenance is not None
        and post_run_provenance.get("sha256") == provenance.get("sha256")
    )
    if post_run_provenance is not None and not provenance["unchanged_during_qualification"]:
        validation_failures.append("candidate qualification inputs changed during the benchmark")
    if strict_qualification:
        post_artifact_sha256 = (
            (post_run_provenance.get("cuda_artifacts") or {}).get("sha256")
            if post_run_provenance is not None
            else None
        )
        freshness = provenance["artifact_freshness"]
        freshness["artifacts_post_qualification_sha256"] = post_artifact_sha256
        freshness["artifacts_unchanged_during_qualification"] = (
            post_artifact_sha256 == freshness["artifacts_after_sha256"]
        )
        freshness["pre_run_passed"] = freshness["passed"]
        freshness["passed"] = (
            freshness["passed"]
            and freshness["artifacts_unchanged_during_qualification"]
        )
        if not freshness["artifacts_unchanged_during_qualification"]:
            validation_failures.append("CUDA qualification artifacts changed during the benchmark")
        provenance["post_run_binding"] = (
            {
                "sha256": post_run_provenance.get("sha256"),
                "git_sha256": (post_run_provenance.get("git") or {}).get("sha256"),
                "toolchains_sha256": (
                    post_run_provenance.get("toolchains") or {}
                ).get("sha256"),
                "gpu_sha256": (post_run_provenance.get("gpu") or {}).get("sha256"),
                "cuda_artifacts_sha256": post_artifact_sha256,
            }
            if post_run_provenance is not None
            else None
        )

    throughput_summary = summarize_throughput(cases)
    throughput_summary["case_count"] = len(cases)
    throughput_summary["max_case_baseline_tok_s_cv"] = max(
        (case["baseline_tok_s_cv"] for case in cases), default=0.0
    )
    throughput_summary["max_case_candidate_tok_s_cv"] = max(
        (case["candidate_tok_s_cv"] for case in cases), default=0.0
    )
    all_pairs = [pair for case in cases for pair in case["pairs"]]
    latency_summary = summarize_latency_pairs(
        all_pairs,
        bootstrap_samples=args.bootstrap_samples,
        bootstrap_seed=args.bootstrap_seed,
    )
    failures = validation_failures + promotion_failures
    summary = {
        "schema": QUALIFICATION_SCHEMA,
        "provenance": provenance,
        "provenance_sha256": canonical_sha256(provenance),
        "candidate": candidate_metadata(spec),
        "config": result_config_metadata(
            args,
            timing_info,
            spec,
            candidate_environment,
            capture_kv_capacity,
            common_environment,
        ),
        "checks": {
            "validation": not validation_failures,
            "candidate_ratio": all(case["checks"]["candidate_ratio"] for case in cases),
            "stability": all(case["checks"]["stability"] for case in cases),
            "phase_latency": all(case["checks"]["phase_latency"] for case in cases),
            "fixed_sample_count": (
                args.qualification_fixed_samples is None
                or all(case["repeats"] == args.qualification_fixed_samples for case in cases)
            ),
            "strict_provenance": (
                not strict_qualification
                or (
                    provenance["artifact_freshness"]["passed"]
                    and provenance["artifact_freshness"][
                        "artifacts_unchanged_during_qualification"
                    ]
                    and provenance["unchanged_during_qualification"]
                    and all(guard["passed"] for guard in provenance["runtime_guards"])
                )
            ),
        },
        "passed": not failures,
        "throughput": throughput_summary,
        "latency": latency_summary,
        "raw_samples": {
            "schema": RAW_SAMPLE_SCHEMA,
            "path": raw_samples_path.name,
            "rows": len(all_pairs),
        },
        "cases": cases,
        "validation_failures": validation_failures,
        "promotion_failures": promotion_failures,
        "failures": failures,
    }
    summary_path = args.output_dir / "candidate_summary.json"
    summary_path.write_text(json.dumps(summary, allow_nan=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    manifest = write_evidence_manifest(args.output_dir, manifest_path)
    candidate_name = spec.legacy_kind.value if spec.legacy_kind is not None else spec.kernel_id
    print(
        f"candidate_parity kind={candidate_name} kernel_id={spec.kernel_id} "
        f"model={args.model_label} config={args.config_label} passed={str(not failures).lower()} "
        f"median_ratio={summary['throughput']['median_candidate_ratio']:.4f} "
        f"min_ratio={summary['throughput']['min_candidate_ratio']:.4f} "
        f"max_baseline_cv={summary['throughput']['max_case_baseline_tok_s_cv']:.4f} "
        f"max_candidate_cv={summary['throughput']['max_case_candidate_tok_s_cv']:.4f} "
        f"output={summary_path} manifest_sha256={manifest['files_sha256']}"
    )
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
