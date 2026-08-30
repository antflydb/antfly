#!/usr/bin/env python3

import argparse
import hashlib
import pathlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from validate_gemma4_cuda_candidate import (
    ARTIFACT_FRESHNESS_SCHEMA,
    CAPTURE_KV_CAPACITY_ENV,
    CANDIDATE_CATALOG,
    CANDIDATE_SPECS,
    CUBLASLT_BF16_PREFILL_COMPARISON_ENVIRONMENT,
    CUBLASLT_BF16_PREFILL_SM89_KERNEL_ID,
    DEFAULT_CAPTURE_KV_PROMPT_HEADROOM,
    E2B_FFN_PAIR_ONLY_COMPARISON_ENVIRONMENT,
    EXACT_E2B_FFN_F32_COMPARISON_ENVIRONMENT,
    GEMMA4_LONG_CONTEXT_WORKLOAD,
    GENERATED_ATTENTION_KERNEL_ID,
    GENERATED_ATTENTION_SPLIT_KV_MIN_TOKENS_ENV,
    GENERATED_ATTENTION_SPLIT_KV_SPLITS_ENV,
    GGML_Q8_1_E2B_FFN_COMPARISON_ENVIRONMENT,
    GQA_DECODE_SPLITK_ONLINE_SM89_KERNEL_ID,
    GQA_PREFILL_FLASH_F16_SM89_KERNEL_ID,
    GQA_PREFILL_TILED_F16_EXACT_KERNEL_ID,
    GQA_PREFILL_TILED_F16_WARP_KERNEL_ID,
    QUALIFICATION_PROFILES,
    PLE_GATE_BF16_MIRROR_FIRST_COMPARISON_ENVIRONMENT,
    PLE_GATE_BF16_MIRROR_FIRST_SM89_E2B_KERNEL_ID,
    REQUIRED_CUDA_ARTIFACTS,
    Q6_K_Q8_1_LM_HEAD_ARGMAX_KERNEL_ID,
    Q4_0_E2B_FFN_EXACT_KERNEL_ID,
    Q4_0_GGML_Q8_1_E2B_FFN_KERNEL_ID,
    Q4_0_Q8_1_E2B_FFN_PAIR_ONLY_KERNEL_ID,
    Q4_0_Q8_1_FFN_KERNEL_ID,
    RUNTIME_CAPTURE_KV_CAPACITY_ENV,
    SCORE_PREWORK_ATTENTION_COMPARISON_ENVIRONMENT,
    SCORE_PREWORK_ATTENTION_KERNEL_ID,
    SCORE_PREWORK_TILED64_ATTENTION_COMPARISON_ENVIRONMENT,
    SCORE_PREWORK_TILED64_ATTENTION_KERNEL_ID,
    SPLITK_ONLINE_SM89_COMPARISON_ENVIRONMENT,
    TOKEN_IDS_RE,
    CandidateKind,
    CandidateSpec,
    RouteCounter,
    TimingMetadata,
    candidate_environment_metadata,
    candidate_cuda_artifact_provenance,
    candidate_metadata,
    candidate_spec,
    canonical_sha256,
    case_promotion_errors,
    coefficient_of_variation,
    configure_candidate_environment,
    capture_qualification_runtime_guard,
    execution_order,
    input_path_provenance,
    latency_promotion_errors,
    load_prompt_fixture,
    paired_throughput,
    paired_latency,
    pair_attestation,
    parse_args,
    parse_candidate_environment,
    parse_candidate_environment_list,
    parse_common_environment,
    parse_common_environment_list,
    parse_route_counter_list,
    qualification_provenance,
    qualification_runtime_environment,
    repetition_stem,
    resolve_candidate_spec,
    resolve_candidate_environment,
    resolve_capture_kv_capacity,
    resolve_common_environment,
    result_config_metadata,
    run_controlled_release_build,
    run_artifact_freshness_checks,
    run_case,
    artifact_freshness_attestation,
    summarize_case,
    summarize_latency_pairs,
    summarize_throughput,
    timing_counter,
    timing_metadata,
    timing_metadata_from_args,
    timing_latency_metrics,
    timing_throughput,
    strict_qualification_provenance_errors,
    validate_qualification_contract,
    validate_pair,
)


SCRIPTS = pathlib.Path(__file__).resolve().parent
LONG_CONTEXT_FIXTURE = SCRIPTS / "fixtures/gemma4_long_context_v1.json"


def strict_provenance_fixture() -> dict:
    digest = "a" * 64
    untracked_files: list[dict] = []
    git = {
        "commit": "b" * 40,
        "commit_returncode": 0,
        "tracked_dirty": True,
        "tracked_status_returncode": 0,
        "tracked_status_sha256": digest,
        "source_status_returncode": 0,
        "source_status_sha256": digest,
        "status_returncode": 0,
        "status_sha256": digest,
        "dirty": True,
        "tracked_diff_returncode": 0,
        "tracked_diff_bytes": 0,
        "tracked_diff_sha256": hashlib.sha256(b"").hexdigest(),
        "tracked_diff_error": None,
        "untracked_inventory_schema": "antfly.git_untracked_inventory.v1",
        "untracked_inventory_returncode": 0,
        "untracked_inventory_sha256": canonical_sha256({
            "schema": "antfly.git_untracked_inventory.v1",
            "files": untracked_files,
        }),
        "untracked_file_count": 0,
        "untracked_files": untracked_files,
        "untracked_inventory_error": None,
    }
    git["sha256"] = canonical_sha256(git)
    toolchains = {
        "zig": {
            "returncode": 0,
            "path": "/tools/zig",
            "sha256": digest,
            "version": "0.17.0-dev.1933+e19a73c2d",
        },
        "nvcc": {
            "returncode": 0,
            "path": "/cuda/bin/nvcc",
            "sha256": digest,
            "version": "Cuda compilation tools, release 13.2, V13.2.0",
        },
    }
    toolchains["sha256"] = canonical_sha256({
        name: toolchains[name] for name in ("zig", "nvcc")
    })
    gpu_state = {
        "cuda_visible_devices": "GPU-test",
        "selected_gpus": [{
            "index": 0,
            "uuid": "GPU-test",
            "name": "NVIDIA L4",
            "driver_version": "580.159.03",
            "compute_cap": 8.9,
            "memory.total": 23034,
            "persistence_mode": "Enabled",
            "power.limit": 72.0,
            "clocks.max.graphics": 2040,
            "clocks.max.memory": 6251,
            "clocks.applications.graphics": 2040,
            "clocks.applications.memory": 6251,
            "mig.mode.current": "N/A",
        }],
        "error": None,
    }
    processes = {"selected_gpu_processes": [], "error": None}
    gpu_identity = {
        "execution_state": gpu_state,
        "selected_compute_processes": processes,
    }
    files = {
        name: {
            "path": f"/repo/{name}",
            "exists": True,
            "kind": "file",
            "bytes": 1,
            "sha256": digest,
        }
        for name in REQUIRED_CUDA_ARTIFACTS
    }
    artifact_identity = {
        name: {"bytes": value["bytes"], "sha256": value["sha256"]}
        for name, value in sorted(files.items())
    }
    provenance = {
        "git": git,
        "toolchains": toolchains,
        "gpu": {**gpu_identity, "sha256": canonical_sha256(gpu_identity)},
        "cuda_artifacts": {
            "files": files,
            "sha256": canonical_sha256(artifact_identity),
        },
        "runtime_environment": {
            "schema": "antfly.cuda_candidate.runtime_environment.v1",
            "values": {
                "CUDA_DEVICE_ORDER": "PCI_BUS_ID",
                "CUDA_VISIBLE_DEVICES": "GPU-test",
                "LANG": "C.UTF-8",
                "PATH": "/usr/bin:/bin",
            },
        },
    }
    provenance["runtime_environment"]["sha256"] = canonical_sha256({
        "schema": provenance["runtime_environment"]["schema"],
        "values": provenance["runtime_environment"]["values"],
    })
    provenance["sha256"] = canonical_sha256({
        name: item.get("sha256")
        for name, item in sorted(provenance.items())
        if isinstance(item, dict)
    })
    return provenance


def timing(
    *,
    generated: int = 0,
    score_prework: int = 0,
    score_prework_serial: int = 0,
    score_prework_serial_hd256: int = 0,
    score_prework_serial_hd512: int = 0,
    score_prework_tiled64_hd256: int = 0,
    score_prework_tiled64_hd512: int = 0,
    score_prework_tiled64_fallbacks: int = 0,
    score_prework_tiled64_forbidden_routes: int = 0,
    score_prework_tiled64_symbol_fallbacks: int = 0,
    splitk_online: int = 0,
    splitk_online_hd256: int = 0,
    splitk_online_hd512: int = 0,
    splitk_online_fallbacks: int = 0,
    splitk_online_ineligible_fallbacks: int = 0,
    splitk_online_symbol_fallbacks: int = 0,
    splitk_online_forbidden_routes: int = 0,
    decode_fast: int = 0,
    decode_fast_fallbacks: int = 0,
    lm_argmax: int = 0,
    lm_argmax_fallbacks: int = 0,
    generated_q6_lm_argmax: int = 0,
    generated_q6_lm_argmax_fallbacks: int = 0,
    ffn_pair: int = 0,
    ffn_down: int = 0,
    ffn_pair_fallbacks: int = 0,
    ffn_down_fallbacks: int = 0,
    ffn_pair_only: int = 0,
    ffn_pair_only_fallbacks: int = 0,
    exact_ffn_pair: int = 0,
    exact_ffn_down: int = 0,
    exact_ffn_pair_fallbacks: int = 0,
    exact_ffn_down_fallbacks: int = 0,
    gqa_prefill_exact_hd256: int = 0,
    gqa_prefill_exact_hd512: int = 0,
    gqa_prefill_warp_hd256: int = 0,
    gqa_prefill_warp_hd512: int = 0,
    gqa_prefill_flash_hd256_q512: int = 0,
    gqa_prefill_flash_hd256_q3: int = 0,
    gqa_prefill_flash_hd512_q512: int = 0,
    gqa_prefill_flash_hd512_q3: int = 0,
    gqa_prefill_flash_fallbacks: int = 0,
    gqa_prefill_flash_ineligible_fallbacks: int = 0,
    gqa_prefill_flash_symbol_fallbacks: int = 0,
    ggml_e2b_ffn: int = 0,
    ggml_e2b_ffn_fallbacks: int = 0,
    cublaslt_tuned_calls: int = 0,
    cublaslt_heuristic_calls: int = 0,
    cublaslt_api_fallbacks: int = 0,
    ple_gate_mirror_first: int = 0,
    ple_gate_mirror_first_ineligible: int = 0,
    ple_gate_decode_preserved: int = 0,
    ple_gate_fused_q4: int = 0,
    tokens: int = 64,
    persistent_replays: int | None = None,
    tok_s: float = 100.0,
    prefill_ms: float = 2.0,
    decode_ms: float = 8.0,
    total_ms: float = 10.0,
) -> dict:
    return {
        "token_ids": list(range(tokens)),
        "prompt_token_ids": [1, 2, 3],
        "decode_tok_per_s": tok_s,
        "timing_ms": {
            "generate": total_ms,
            "prefill_inner": prefill_ms,
            "decode_inner": decode_ms,
        },
        "cuda": {
            "graph_capture_persistent_replays": (
                tokens - 4 if persistent_replays is None else persistent_replays
            ),
            "graph_capture_discards": 0,
            "graph_capture_capacity_skips": 0,
            "launch_attention_gqa_decode_generated": generated,
            "launch_attention_gqa_decode_score_prework": score_prework,
            "launch_attention_gqa_decode_score_prework_serial": score_prework_serial,
            "launch_attention_gqa_decode_score_prework_serial_hd256": score_prework_serial_hd256,
            "launch_attention_gqa_decode_score_prework_serial_hd512": score_prework_serial_hd512,
            "launch_attention_gqa_decode_score_prework_tiled64_hd256": score_prework_tiled64_hd256,
            "launch_attention_gqa_decode_score_prework_tiled64_hd512": score_prework_tiled64_hd512,
            "launch_attention_gqa_decode_score_prework_tiled64_fallbacks": score_prework_tiled64_fallbacks,
            "launch_attention_gqa_decode_score_prework_tiled64_forbidden_routes": score_prework_tiled64_forbidden_routes,
            "launch_attention_gqa_decode_score_prework_tiled64_symbol_fallbacks": score_prework_tiled64_symbol_fallbacks,
            "launch_attention_gqa_decode_splitk_online_sm89": splitk_online,
            "launch_attention_gqa_decode_splitk_online_sm89_hd256": splitk_online_hd256,
            "launch_attention_gqa_decode_splitk_online_sm89_hd512": splitk_online_hd512,
            "launch_attention_gqa_decode_splitk_online_sm89_fallbacks": splitk_online_fallbacks,
            "launch_attention_gqa_decode_splitk_online_sm89_ineligible_fallbacks": splitk_online_ineligible_fallbacks,
            "launch_attention_gqa_decode_splitk_online_sm89_symbol_fallbacks": splitk_online_symbol_fallbacks,
            "launch_attention_gqa_decode_splitk_online_sm89_forbidden_routes": splitk_online_forbidden_routes,
            "launch_attention_gqa_decode_fast": decode_fast,
            "launch_attention_gqa_decode_fast_fallbacks": decode_fast_fallbacks,
            "lm_head_argmax_fused_q4_0_q8_1": lm_argmax,
            "lm_head_argmax_q4_0_q8_1_fallbacks": lm_argmax_fallbacks,
            "lm_head_argmax_generated_q6_k_q8_1_hits": generated_q6_lm_argmax,
            "lm_head_argmax_generated_q6_k_q8_1_fallbacks": generated_q6_lm_argmax_fallbacks,
            "q4_0_generated_e2b_pair_q8_hits": ffn_pair,
            "q4_0_generated_e2b_down_q8_hits": ffn_down,
            "q4_0_generated_e2b_pair_q8_fallbacks": ffn_pair_fallbacks,
            "q4_0_generated_e2b_down_q8_fallbacks": ffn_down_fallbacks,
            "q4_0_generated_e2b_pair_only_hits": ffn_pair_only,
            "q4_0_generated_e2b_pair_only_fallbacks": ffn_pair_only_fallbacks,
            "q4_0_generated_e2b_exact_pair_f32_hits": exact_ffn_pair,
            "q4_0_generated_e2b_exact_down_f32_hits": exact_ffn_down,
            "q4_0_generated_e2b_exact_pair_f32_fallbacks": exact_ffn_pair_fallbacks,
            "q4_0_generated_e2b_exact_down_f32_fallbacks": exact_ffn_down_fallbacks,
            "launch_attention_gqa_prefill_tiled_f16_exact_hd256": gqa_prefill_exact_hd256,
            "launch_attention_gqa_prefill_tiled_f16_exact_hd512": gqa_prefill_exact_hd512,
            "launch_attention_gqa_prefill_tiled_f16_warp_hd256": gqa_prefill_warp_hd256,
            "launch_attention_gqa_prefill_tiled_f16_warp_hd512": gqa_prefill_warp_hd512,
            "launch_attention_gqa_prefill_flash_f16_sm89_hd256_q512": gqa_prefill_flash_hd256_q512,
            "launch_attention_gqa_prefill_flash_f16_sm89_hd256_q3": gqa_prefill_flash_hd256_q3,
            "launch_attention_gqa_prefill_flash_f16_sm89_hd512_q512": gqa_prefill_flash_hd512_q512,
            "launch_attention_gqa_prefill_flash_f16_sm89_hd512_q3": gqa_prefill_flash_hd512_q3,
            "launch_attention_gqa_prefill_flash_f16_sm89_fallbacks": gqa_prefill_flash_fallbacks,
            "launch_attention_gqa_prefill_flash_f16_sm89_ineligible_fallbacks": gqa_prefill_flash_ineligible_fallbacks,
            "launch_attention_gqa_prefill_flash_f16_sm89_symbol_fallbacks": gqa_prefill_flash_symbol_fallbacks,
            "q4_0_ggml_q8_1_e2b_ffn_hits": ggml_e2b_ffn,
            "q4_0_ggml_q8_1_e2b_ffn_fallbacks": ggml_e2b_ffn_fallbacks,
            "bf16_cublaslt_tuning_tuned_calls": cublaslt_tuned_calls,
            "bf16_cublaslt_tuning_heuristic_calls": cublaslt_heuristic_calls,
            "bf16_cublaslt_tuning_api_fallbacks": cublaslt_api_fallbacks,
            "ple_gate_prefill_bf16_mirror_first_hits": ple_gate_mirror_first,
            "ple_gate_prefill_bf16_mirror_first_ineligible": ple_gate_mirror_first_ineligible,
            "ple_gate_decode_q4_fused_preserved": ple_gate_decode_preserved,
            "linear_activation_slice_fused_q4_0": ple_gate_fused_q4,
        },
    }


class CandidateParityTest(unittest.TestCase):
    def test_default_candidate_remains_generated_attention(self):
        with mock.patch.object(sys, "argv", ["validate_gemma4_cuda_candidate.py"]):
            args = parse_args()
            self.assertEqual(CandidateKind.GENERATED_ATTENTION, args.candidate)
            self.assertEqual(1, args.repeats)
            self.assertEqual(0.0, args.min_candidate_ratio)
            self.assertEqual(1.0, args.max_cv)
            self.assertIsNone(args.kernel_id)
            self.assertEqual("default", args.config_label)
            self.assertEqual("f32", args.cache_dtype)
            self.assertEqual(32, args.prefill_chunk_size)
            self.assertEqual([], args.candidate_env)
            self.assertEqual([], args.common_env)
            self.assertIsNone(args.capture_kv_capacity)

    def test_fixed_sample_qualification_profiles_apply_strict_defaults(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--qualification-profile", "screening"],
        ):
            screening = parse_args()
        self.assertEqual(5, screening.repeats)
        self.assertEqual(0.03, screening.max_cv)
        self.assertTrue(screening.require_phase_metrics)
        self.assertTrue(screening.require_full_route_coverage)

        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--qualification-profile", "promotion"],
        ):
            promotion = parse_args()
        self.assertEqual(10, promotion.repeats)
        self.assertEqual(1.02, promotion.min_candidate_ratio)
        self.assertEqual(0.98, promotion.max_decode_latency_ratio)
        self.assertEqual(1.0, promotion.max_decode_ci_upper)

        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--qualification-profile", "prefill-screening"],
        ):
            prefill_screening = parse_args()
        self.assertEqual(5, prefill_screening.repeats)
        self.assertEqual("prefill", prefill_screening.qualification_focus)
        self.assertTrue(prefill_screening.require_locked_prompt_fixture)
        self.assertEqual(0.98, prefill_screening.max_ttft_ratio)
        self.assertEqual(0.98, prefill_screening.max_total_latency_ratio)
        self.assertEqual(1.02, prefill_screening.max_decode_latency_ratio)
        self.assertEqual(1.03, prefill_screening.max_ttft_ci_upper)

        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--qualification-profile", "prefill-promotion"],
        ):
            prefill_promotion = parse_args()
        self.assertEqual(10, prefill_promotion.repeats)
        self.assertEqual(0.02, prefill_promotion.max_cv)
        self.assertEqual(0.99, prefill_promotion.min_candidate_ratio)
        self.assertEqual(0.98, prefill_promotion.max_ttft_ratio)
        self.assertEqual(1.01, prefill_promotion.max_decode_latency_ratio)
        self.assertEqual(1.0, prefill_promotion.max_total_ci_upper)
        self.assertEqual(1.02, prefill_promotion.max_decode_ci_upper)

    def test_fixed_profiles_require_locked_prefill_fixture_and_cannot_be_loosened(self):
        with mock.patch.object(
            sys,
            "argv",
            [
                "validate_gemma4_cuda_candidate.py",
                "--kernel-id",
                CUBLASLT_BF16_PREFILL_SM89_KERNEL_ID,
                "--qualification-profile",
                "prefill-screening",
            ],
        ):
            missing_fixture = parse_args()
        with self.assertRaisesRegex(ValueError, "requires --prompt-fixture"):
            validate_qualification_contract(missing_fixture)

        with mock.patch.object(
            sys,
            "argv",
            [
                "validate_gemma4_cuda_candidate.py",
                "--kernel-id",
                CUBLASLT_BF16_PREFILL_SM89_KERNEL_ID,
                "--qualification-profile",
                "prefill-promotion",
                "--prompt-fixture",
                str(LONG_CONTEXT_FIXTURE),
            ],
        ):
            strict = parse_args()
        validate_qualification_contract(strict)

        strict.prompt = ["unlocked prompt"]
        with self.assertRaisesRegex(ValueError, "forbids unlocked --prompt"):
            validate_qualification_contract(strict)
        strict.prompt = []
        strict.max_ttft_ratio = QUALIFICATION_PROFILES["prefill-promotion"]["max_ttft_ratio"] + 0.01
        with self.assertRaisesRegex(ValueError, "max-ttft-ratio cannot be looser"):
            validate_qualification_contract(strict)
        strict.max_ttft_ratio = QUALIFICATION_PROFILES["prefill-promotion"]["max_ttft_ratio"]
        strict.bootstrap_samples = 9_999
        with self.assertRaisesRegex(ValueError, "at least 10000 bootstrap samples"):
            validate_qualification_contract(strict)

    def test_fixed_profiles_reject_phase_mismatch_catalog_mutation_and_loose_decode_gates(self):
        with mock.patch.object(
            sys,
            "argv",
            [
                "validate_gemma4_cuda_candidate.py",
                "--kernel-id",
                GQA_PREFILL_TILED_F16_WARP_KERNEL_ID,
                "--qualification-profile",
                "promotion",
            ],
        ):
            phase_mismatch = parse_args()
        with self.assertRaisesRegex(ValueError, "does not match candidate route phase"):
            validate_qualification_contract(phase_mismatch)

        with mock.patch.object(
            sys,
            "argv",
            [
                "validate_gemma4_cuda_candidate.py",
                "--kernel-id",
                SCORE_PREWORK_ATTENTION_KERNEL_ID,
                "--qualification-profile",
                "promotion",
                "--required-route-counter",
                "launch_attention_gqa_decode_score_prework",
            ],
        ):
            mutable = parse_args()
        with self.assertRaisesRegex(ValueError, "immutable catalog definition"):
            validate_qualification_contract(mutable, resolve_candidate_spec(mutable))

        with mock.patch.object(
            sys,
            "argv",
            [
                "validate_gemma4_cuda_candidate.py",
                "--kernel-id",
                SCORE_PREWORK_ATTENTION_KERNEL_ID,
                "--qualification-profile",
                "promotion",
            ],
        ):
            loose = parse_args()
        loose.max_decode_ci_upper += 0.01
        with self.assertRaisesRegex(ValueError, "max-decode-ci-upper cannot be looser"):
            validate_qualification_contract(loose)

    def test_fixed_profiles_require_canonical_scripts_and_forbid_candidate_overrides(self):
        with mock.patch.object(
            sys,
            "argv",
            [
                "validate_gemma4_cuda_candidate.py",
                "--kernel-id",
                SCORE_PREWORK_ATTENTION_KERNEL_ID,
                "--qualification-profile",
                "promotion",
            ],
        ):
            args = parse_args()

        args.artifact_check_script = pathlib.Path("/tmp/not-the-canonical-check.sh")
        with self.assertRaisesRegex(ValueError, "canonical artifact check script"):
            validate_qualification_contract(args)

        args.artifact_check_script = SCRIPTS.parent / "regen-cuda-artifacts.sh"
        args.candidate_env = ["CUDA_VISIBLE_DEVICES=1"]
        with self.assertRaisesRegex(ValueError, "forbids --candidate-env"):
            validate_qualification_contract(args)

        args.candidate_env = ["LD_PRELOAD=/tmp/candidate.so"]
        with self.assertRaisesRegex(ValueError, "forbids --candidate-env"):
            validate_qualification_contract(args)

        args.candidate_env = []
        args.wrapper = pathlib.Path("/tmp/not-the-canonical-wrapper.sh")
        with self.assertRaisesRegex(ValueError, "canonical tuning wrapper"):
            validate_qualification_contract(args)

        args.wrapper = SCRIPTS / "with_gemma4_qat_cuda_tuning.sh"
        args.common_env = ["ANTFLY_INFERENCE_CUDA_TEMP_CACHE_MB=1"]
        with self.assertRaisesRegex(ValueError, "only GPU-selection variables"):
            validate_qualification_contract(args)

        args.common_env = ["CUDA_VISIBLE_DEVICES=0"]
        with self.assertRaisesRegex(ValueError, "full GPU UUID"):
            validate_qualification_contract(args)

    def test_prompt_fixture_is_content_addressed_and_contract_checked(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "prompt.json"
            prompt = "evidence " * 4 + "question"
            encoded = prompt.encode("utf-8")
            prefix = "<|turn>user\n"
            suffix = "<turn|>\n<|turn>model\n<|channel>final\n<channel|>"
            reference = (prefix + prompt + suffix).encode("utf-8")
            path.write_text(
                json.dumps({
                    "schema": "antfly.prompt_fixture.v1",
                    "id": "long-prompt-v1",
                    "segment": "evidence ",
                    "repeat": 4,
                    "suffix": "question",
                    "expected_user_utf8_bytes": len(encoded),
                    "expected_user_sha256": hashlib.sha256(encoded).hexdigest(),
                    "reference_chat_prefix": prefix,
                    "reference_chat_suffix": suffix,
                    "expected_reference_prompt_utf8_bytes": len(reference),
                    "expected_reference_prompt_sha256": hashlib.sha256(reference).hexdigest(),
                    "expected_reference_prompt_tokens": 17,
                }),
                encoding="utf-8",
            )
            rendered, metadata = load_prompt_fixture(path)
            self.assertEqual(reference.decode("utf-8"), rendered)
            self.assertEqual("long-prompt-v1", metadata["id"])
            self.assertEqual(17, metadata["benchmark_prompt_tokens"])
            self.assertEqual(hashlib.sha256(reference).hexdigest(), metadata["benchmark_prompt_sha256"])
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), metadata["file_sha256"])

            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["expected_user_sha256"] = "0" * 64
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "byte/hash contract"):
                load_prompt_fixture(path)

    def test_input_provenance_hashes_files_and_directory_inventory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            (root / "model.gguf").write_bytes(b"model")
            (root / "tokenizer.json").write_bytes(b"tokenizer")
            model = input_path_provenance(root / "model.gguf")
            bundle = input_path_provenance(root)
            self.assertEqual(hashlib.sha256(b"model").hexdigest(), model["sha256"])
            self.assertEqual(2, bundle["file_count"])
            self.assertEqual(["model.gguf", "tokenizer.json"], [item["path"] for item in bundle["files"]])

    def test_strict_provenance_accepts_bound_dirty_state_and_fails_closed(self):
        provenance = strict_provenance_fixture()
        self.assertEqual([], strict_qualification_provenance_errors(provenance))
        self.assertTrue(provenance["git"]["dirty"])

        fixed_environment = json.loads(json.dumps(provenance))
        fixed_environment["runtime_environment"]["values"].update({
            "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK": "1",
            "TERMITE_CUDA_DEQUANTIZE_QUANT_WEIGHTS": "0",
        })
        runtime_identity = {
            "schema": fixed_environment["runtime_environment"]["schema"],
            "values": fixed_environment["runtime_environment"]["values"],
        }
        fixed_environment["runtime_environment"]["sha256"] = canonical_sha256(runtime_identity)
        fixed_environment["sha256"] = canonical_sha256({
            name: item.get("sha256")
            for name, item in sorted(fixed_environment.items())
            if isinstance(item, dict)
        })
        self.assertEqual([], strict_qualification_provenance_errors(fixed_environment))

        missing_content = json.loads(json.dumps(provenance))
        missing_content["git"]["tracked_diff_sha256"] = None
        missing_content["git"]["sha256"] = canonical_sha256({
            name: value
            for name, value in missing_content["git"].items()
            if name != "sha256"
        })
        missing_content["sha256"] = canonical_sha256({
            name: item.get("sha256")
            for name, item in sorted(missing_content.items())
            if isinstance(item, dict)
        })
        self.assertTrue(any(
            "tracked-content" in error
            for error in strict_qualification_provenance_errors(missing_content)
        ))

        unbound = json.loads(json.dumps(provenance))
        unbound["sha256"] = "0" * 64
        self.assertTrue(any(
            "top-level provenance hash" in error
            for error in strict_qualification_provenance_errors(unbound)
        ))

        missing_artifact = json.loads(json.dumps(provenance))
        del missing_artifact["cuda_artifacts"]["files"]["runtime_sm89_cubin"]
        errors = strict_qualification_provenance_errors(missing_artifact)
        self.assertTrue(any("runtime_sm89_cubin" in error for error in errors))
        self.assertTrue(any("artifact-set hash does not match" in error for error in errors))

        busy = json.loads(json.dumps(provenance))
        processes = {
            "selected_gpu_processes": [{
                "gpu_uuid": "GPU-test",
                "pid": 42,
                "process_name": "competing-job",
            }],
            "error": None,
        }
        busy["gpu"]["selected_compute_processes"] = processes
        busy["gpu"]["sha256"] = canonical_sha256({
            "execution_state": busy["gpu"]["execution_state"],
            "selected_compute_processes": processes,
        })
        errors = strict_qualification_provenance_errors(busy)
        self.assertTrue(any("competing processes" in error for error in errors))

        wrong_toolchain = json.loads(json.dumps(provenance))
        wrong_toolchain["toolchains"]["zig"]["version"] = "0.15.2"
        wrong_toolchain["toolchains"]["sha256"] = canonical_sha256({
            name: wrong_toolchain["toolchains"][name] for name in ("zig", "nvcc")
        })
        errors = strict_qualification_provenance_errors(wrong_toolchain)
        self.assertTrue(any("Zig 0.17.0-dev.1933+e19a73c2d" in error for error in errors))

        wrong_gpu = json.loads(json.dumps(provenance))
        wrong_gpu["gpu"]["execution_state"]["selected_gpus"][0]["name"] = "NVIDIA GeForce RTX 4090"
        gpu_identity = {
            "execution_state": wrong_gpu["gpu"]["execution_state"],
            "selected_compute_processes": wrong_gpu["gpu"]["selected_compute_processes"],
        }
        wrong_gpu["gpu"]["sha256"] = canonical_sha256(gpu_identity)
        wrong_gpu["sha256"] = canonical_sha256({
            name: item.get("sha256")
            for name, item in sorted(wrong_gpu.items())
            if isinstance(item, dict)
        })
        errors = strict_qualification_provenance_errors(wrong_gpu)
        self.assertTrue(any("requires NVIDIA L4" in error for error in errors))

        numeric_selector = json.loads(json.dumps(provenance))
        numeric_selector["gpu"]["execution_state"]["cuda_visible_devices"] = "0"
        gpu_identity = {
            "execution_state": numeric_selector["gpu"]["execution_state"],
            "selected_compute_processes": numeric_selector["gpu"]["selected_compute_processes"],
        }
        numeric_selector["gpu"]["sha256"] = canonical_sha256(gpu_identity)
        numeric_selector["sha256"] = canonical_sha256({
            name: item.get("sha256")
            for name, item in sorted(numeric_selector.items())
            if isinstance(item, dict)
        })
        errors = strict_qualification_provenance_errors(numeric_selector)
        self.assertTrue(any("UUID-based CUDA_VISIBLE_DEVICES" in error for error in errors))

    def test_artifact_freshness_attestation_binds_before_after_hashes(self):
        before = strict_provenance_fixture()
        after = json.loads(json.dumps(before))
        checks = {
            "controlled_release_build": {"passed": True},
            "generated_sources": {"passed": True},
            "canonical_cuda_artifacts": {"passed": True},
            "embedded_binary_artifact": {"passed": True},
        }
        attestation = artifact_freshness_attestation(before, after, checks)
        self.assertEqual(ARTIFACT_FRESHNESS_SCHEMA, attestation["schema"])
        self.assertEqual("single_pre_run_check", attestation["stage"])
        self.assertTrue(attestation["checks_run_once"])
        self.assertTrue(attestation["passed"])

        mutated = json.loads(json.dumps(after))
        mutated["cuda_artifacts"]["sha256"] = "c" * 64
        mutated["sha256"] = "d" * 64
        attestation = artifact_freshness_attestation(before, mutated, checks)
        self.assertFalse(attestation["artifacts_unchanged_by_checks"])
        self.assertFalse(attestation["qualification_binding_unchanged_by_checks"])
        self.assertFalse(attestation["passed"])

        checks["canonical_cuda_artifacts"]["passed"] = False
        self.assertFalse(artifact_freshness_attestation(before, after, checks)["passed"])

    def test_freshness_checks_run_each_canonical_check_once(self):
        provenance = strict_provenance_fixture()
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            args = argparse.Namespace(
                output_dir=root,
                timeout_sec=123,
                binary=root / "antfly-inference",
                artifact_check_script=SCRIPTS.parent / "regen-cuda-artifacts.sh",
            )
            artifact_identity = "\n".join((
                "cuda_artifact_identity_schema: antfly.cuda_artifact_identity.v1",
                "cuda_artifact_mode: sm89",
                "cuda_artifact_format: cubin",
                "cuda_artifact_target: sm_89",
                "cuda_artifact_image_bytes: 1",
                f"cuda_artifact_image_sha256: {'a' * 64}",
            ))
            with (
                mock.patch(
                    "validate_gemma4_cuda_candidate._run_logged_in_directory",
                    side_effect=((0, "fresh"), (0, artifact_identity)),
                ) as source_check,
                mock.patch(
                    "validate_gemma4_cuda_candidate.release_provenance.artifact_check",
                    return_value={"returncode": 0, "passed": True},
                ) as artifact_check,
            ):
                checks = run_artifact_freshness_checks(
                    args,
                    provenance,
                    {"passed": True, "command": ["zig", "build"]},
                )
        self.assertEqual(2, source_check.call_count)
        artifact_check.assert_called_once()
        source_command = source_check.call_args_list[0].args[0]
        self.assertEqual("/tools/zig", source_command[0])
        self.assertIn("quant-kernel-codegen", source_command)
        self.assertEqual("--check", source_command[-1])
        self.assertEqual(SCRIPTS.parents[1], source_check.call_args_list[0].args[-1])
        self.assertTrue(checks["generated_sources"]["passed"])
        self.assertTrue(checks["embedded_binary_artifact"]["passed"])
        self.assertTrue(checks["controlled_release_build"]["passed"])

    def test_runtime_guard_rejects_gpu_drift_and_competing_processes(self):
        provenance = strict_provenance_fixture()
        gpu_state = provenance["gpu"]["execution_state"]
        idle = {"selected_gpu_processes": [], "error": None}
        with (
            mock.patch(
                "validate_gemma4_cuda_candidate.warm_server_provenance.capture_gpu_execution_state",
                return_value=gpu_state,
            ),
            mock.patch(
                "validate_gemma4_cuda_candidate.warm_server_provenance.capture_selected_gpu_compute_processes",
                return_value=idle,
            ),
        ):
            guard = capture_qualification_runtime_guard(provenance, "before-pair")
        self.assertTrue(guard["passed"])

        drifted = json.loads(json.dumps(gpu_state))
        drifted["selected_gpus"][0]["power.limit"] = 60.0
        with (
            mock.patch(
                "validate_gemma4_cuda_candidate.warm_server_provenance.capture_gpu_execution_state",
                return_value=drifted,
            ),
            mock.patch(
                "validate_gemma4_cuda_candidate.warm_server_provenance.capture_selected_gpu_compute_processes",
                return_value=idle,
            ),
        ):
            guard = capture_qualification_runtime_guard(provenance, "after-pair")
        self.assertFalse(guard["passed"])
        self.assertTrue(any("clocks, or power" in error for error in guard["errors"]))

        busy = {
            "selected_gpu_processes": [{
                "gpu_uuid": "GPU-test",
                "pid": 42,
                "process_name": "competing-job",
            }],
            "error": None,
        }
        with (
            mock.patch(
                "validate_gemma4_cuda_candidate.warm_server_provenance.capture_gpu_execution_state",
                return_value=gpu_state,
            ),
            mock.patch(
                "validate_gemma4_cuda_candidate.warm_server_provenance.capture_selected_gpu_compute_processes",
                return_value=busy,
            ),
        ):
            guard = capture_qualification_runtime_guard(provenance, "before-pair")
        self.assertTrue(any("unexpected selected-GPU" in error for error in guard["errors"]))

    def test_candidate_artifact_hash_covers_release_outputs_renderer_and_compiler(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            graph = root / "zig/pkg/inference/src/graph"
            artifacts = root / "zig/pkg/inference/src/ops/cuda/artifacts"
            generated = root / "zig/pkg/inference/src/ops/cuda/generated"
            graph.mkdir(parents=True)
            artifacts.mkdir(parents=True)
            generated.mkdir(parents=True)
            paths = {
                "generated_manifest": generated / "quant_kernel_artifacts.json",
                "runtime_bundle_source": artifacts / "inference_cuda_kernels.cu",
                "runtime_ptx": artifacts / "inference_cuda_kernels.ptx",
                "runtime_fatbin": artifacts / "inference_cuda_kernels.fatbin",
                "runtime_sm89_cubin": artifacts / "inference_cuda_kernels_sm89.cubin",
            }
            for index, path in enumerate(paths.values(), start=1):
                path.write_bytes(f"artifact-{index}".encode())
            (graph / "quant_kernel_cuda_renderer.zig").write_bytes(b"renderer")
            (graph / "quant_kernel_compiler.zig").write_bytes(b"compiler")
            first = candidate_cuda_artifact_provenance(root)
            paths["runtime_ptx"].write_bytes(b"changed-ptx")
            second = candidate_cuda_artifact_provenance(root)
        self.assertEqual(set(REQUIRED_CUDA_ARTIFACTS), set(first["files"]))
        self.assertNotEqual(first["sha256"], second["sha256"])

    def test_legacy_provenance_does_not_collect_strict_gpu_or_artifact_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            binary = root / "antfly"
            model = root / "model.gguf"
            wrapper = root / "wrapper.sh"
            for path in (binary, model, wrapper):
                path.write_bytes(path.name.encode())
            args = argparse.Namespace(
                binary=binary,
                model=model,
                wrapper=wrapper,
                qualification_profile="legacy",
            )
            with mock.patch(
                "validate_gemma4_cuda_candidate.strict_environment_provenance",
                side_effect=AssertionError("legacy must not collect strict provenance"),
            ):
                provenance = qualification_provenance(args)
        self.assertNotIn("git", provenance)
        self.assertNotIn("gpu", provenance)
        self.assertNotIn("cuda_artifacts", provenance)

    def test_strict_provenance_hashes_artifact_checker_and_shared_helpers(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            binary = root / "antfly"
            model = root / "model.gguf"
            wrapper = root / "wrapper.sh"
            artifact_check = root / "regen-cuda-artifacts.sh"
            for path in (binary, model, wrapper, artifact_check):
                path.write_bytes(path.name.encode())
            args = argparse.Namespace(
                binary=binary,
                model=model,
                wrapper=wrapper,
                artifact_check_script=artifact_check,
                qualification_profile="screening",
            )
            strict = strict_provenance_fixture()
            strict.pop("sha256")
            with mock.patch(
                "validate_gemma4_cuda_candidate.strict_environment_provenance",
                return_value=strict,
            ):
                first = qualification_provenance(args)
                artifact_check.write_bytes(b"changed-checker")
                second = qualification_provenance(args)
        self.assertIn("artifact_check_script", first)
        self.assertEqual(
            {
                "validator",
                "pairing_support",
                "release_provenance_support",
                "gpu_provenance_support",
                "tuning_profile",
            },
            set(first["harness"]["files"]),
        )
        self.assertNotEqual(first["sha256"], second["sha256"])

    def test_qualification_runtime_environment_applies_common_gpu_selection(self):
        with mock.patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "0"}, clear=True):
            environment = qualification_runtime_environment(
                (("CUDA_VISIBLE_DEVICES", "GPU-selected"),)
            )
        self.assertEqual("GPU-selected", environment["CUDA_VISIBLE_DEVICES"])

    def test_strict_runtime_environment_scrubs_tuning_and_loader_controls(self):
        ambient = {
            "PATH": "/usr/bin:/bin",
            "LANG": "C.UTF-8",
            "LD_LIBRARY_PATH": "/usr/local/cuda/lib64",
            "LD_PRELOAD": "/tmp/injected.so",
            "ANTFLY_INFERENCE_CUDA_TEMP_CACHE_MB": "1",
            "TERMITE_GEN_STAGE_DEBUG": "1",
            "CUDA_LAUNCH_BLOCKING": "1",
        }
        with mock.patch.dict(os.environ, ambient, clear=True):
            environment = qualification_runtime_environment(
                (("CUDA_VISIBLE_DEVICES", "GPU-test"),),
                strict=True,
            )
        self.assertEqual("GPU-test", environment["CUDA_VISIBLE_DEVICES"])
        self.assertEqual("PCI_BUS_ID", environment["CUDA_DEVICE_ORDER"])
        self.assertNotIn("LD_PRELOAD", environment)
        self.assertNotIn("ANTFLY_INFERENCE_CUDA_TEMP_CACHE_MB", environment)
        self.assertNotIn("TERMITE_GEN_STAGE_DEBUG", environment)
        self.assertNotIn("CUDA_LAUNCH_BLOCKING", environment)

    def test_controlled_release_build_pins_binary_and_build_flags(self):
        with tempfile.TemporaryDirectory() as temporary:
            canonical_binary = (SCRIPTS.parents[1] / "zig-out/bin/antfly-inference").resolve()
            args = argparse.Namespace(
                binary=canonical_binary,
                output_dir=pathlib.Path(temporary),
                timeout_sec=123,
            )
            real_is_file = pathlib.Path.is_file

            def controlled_build_output_exists(path: pathlib.Path) -> bool:
                if path.resolve() == canonical_binary:
                    return True
                return real_is_file(path)

            with mock.patch(
                "validate_gemma4_cuda_candidate._run_logged_in_directory",
                return_value=(0, "ok"),
            ) as run, mock.patch.object(
                pathlib.Path,
                "is_file",
                controlled_build_output_exists,
            ):
                result = run_controlled_release_build(args)
            self.assertTrue(result["passed"])
            command = run.call_args.args[0]
            self.assertEqual("build", command[1])
            self.assertIn("-Dcuda=true", command)
            self.assertIn("-Dmetal=false", command)
            self.assertIn("-Dcuda-artifacts=sm89", command)
            self.assertIn("-Doptimize=ReleaseFast", command)

            args.binary = pathlib.Path(temporary) / "unbound-binary"
            result = run_controlled_release_build(args)
            self.assertFalse(result["passed"])
            self.assertTrue(any("binary must be" in error for error in result["errors"]))

    def test_controlled_release_build_rejects_missing_output_binary(self):
        with tempfile.TemporaryDirectory() as temporary:
            canonical_binary = (SCRIPTS.parents[1] / "zig-out/bin/antfly-inference").resolve()
            args = argparse.Namespace(
                binary=canonical_binary,
                output_dir=pathlib.Path(temporary),
                timeout_sec=123,
            )
            real_is_file = pathlib.Path.is_file

            def controlled_build_output_is_missing(path: pathlib.Path) -> bool:
                if path.resolve() == canonical_binary:
                    return False
                return real_is_file(path)

            with mock.patch(
                "validate_gemma4_cuda_candidate._run_logged_in_directory",
                return_value=(0, "ok"),
            ), mock.patch.object(
                pathlib.Path,
                "is_file",
                controlled_build_output_is_missing,
            ):
                result = run_controlled_release_build(args)

        self.assertFalse(result["passed"])
        self.assertTrue(any("did not produce" in error for error in result["errors"]))

    def test_build_commands_receive_build_timeout_not_run_timeout(self):
        provenance = strict_provenance_fixture()
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            args = argparse.Namespace(
                binary=SCRIPTS.parents[1] / "zig-out/bin/antfly-inference",
                output_dir=root,
                timeout_sec=123,
                build_timeout_sec=1801,
                artifact_timeout_sec=1802,
                artifact_check_script=SCRIPTS.parent / "regen-cuda-artifacts.sh",
            )
            with mock.patch(
                "validate_gemma4_cuda_candidate._run_logged_in_directory",
                return_value=(0, "ok"),
            ) as run:
                run_controlled_release_build(args)
            self.assertEqual(1801, run.call_args.args[3])

            with (
                mock.patch(
                    "validate_gemma4_cuda_candidate._run_logged_in_directory",
                    return_value=(0, "ok"),
                ) as run,
                mock.patch(
                    "validate_gemma4_cuda_candidate.release_provenance.artifact_check",
                    return_value={"returncode": 0, "passed": True},
                ) as artifact_check,
            ):
                run_artifact_freshness_checks(
                    args,
                    provenance,
                    {"passed": True, "command": ["zig", "build"]},
                )
            self.assertEqual(2, run.call_count)
            source_command = run.call_args_list[0].args[0]
            self.assertIn("quant-kernel-codegen", source_command)
            self.assertEqual(1801, run.call_args_list[0].args[3])
            self.assertIn("--artifact-identity", run.call_args_list[1].args[0])
            self.assertEqual(1801, run.call_args_list[1].args[3])
            self.assertEqual(1802, artifact_check.call_args.args[0].timeout_sec)

    def test_l4_nightly_runs_fixed_long_context_candidate_screen(self):
        runner = (
            pathlib.Path(__file__).resolve().parents[5]
            / "zig/e2e/inference/run_cuda_gemma4_l4_e2e.sh"
        ).read_text(encoding="utf-8")
        lane = runner[
            runner.index("scripts/gemma4/validate_gemma4_cuda_candidate.py"):
            runner.index('python3 "$inference_dir/scripts/gemma4/benchmark_gemma4_cuda_batching.py"')
        ]
        self.assertIn("--kernel-id cuda.attention.gqa.decode.score_prework", lane)
        self.assertIn("--qualification-profile screening", lane)
        self.assertIn("fixtures/gemma4_long_context_v1.json", lane)
        self.assertIn("--lengths 300", lane)
        self.assertIn("--prefill-chunk-size 512", lane)
        self.assertIn("--cache-dtype f16", lane)
        self.assertIn("--capture-kv-capacity 2432", lane)
        self.assertIn("l4-sm89-long-context-f16-decode-score-prework-screening", lane)

    def test_cli_accepts_repeatable_candidate_environment_and_capture_capacity(self):
        with mock.patch.object(
            sys,
            "argv",
            [
                "validate_gemma4_cuda_candidate.py",
                "--candidate-env",
                "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SPLIT_KV_MIN_TOKENS=128",
                "--candidate-env",
                "ANTFLY_TEST_SCHEDULE=split=8",
                "--capture-kv-capacity",
                "4096",
            ],
        ):
            args = parse_args()
        self.assertEqual(
            (
                (GENERATED_ATTENTION_SPLIT_KV_MIN_TOKENS_ENV, "128"),
                ("ANTFLY_TEST_SCHEDULE", "split=8"),
            ),
            resolve_candidate_environment(args, resolve_candidate_spec(args)),
        )
        self.assertEqual((4096, "explicit"), resolve_capture_kv_capacity(args))

    def test_cli_records_non_default_cache_dtype(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--cache-dtype", "polar4"],
        ):
            args = parse_args()
        self.assertEqual("polar4", args.cache_dtype)
        self.assertEqual(
            "polar4",
            result_config_metadata(args, timing_metadata_from_args(args))["cache_dtype"],
        )

    def test_cli_accepts_repeatable_common_environment(self):
        with mock.patch.object(
            sys,
            "argv",
            [
                "validate_gemma4_cuda_candidate.py",
                "--common-env",
                "ANTFLY_INFERENCE_CUDA_ASYNC_I32_DOWNLOAD_STAGING=0",
                "--common-env",
                "ANTFLY_TEST_READBACK_MODE=device=handoff",
            ],
        ):
            args = parse_args()
        spec = resolve_candidate_spec(args)
        candidate_environment = resolve_candidate_environment(args, spec)
        self.assertEqual(
            (
                ("ANTFLY_INFERENCE_CUDA_ASYNC_I32_DOWNLOAD_STAGING", "0"),
                ("ANTFLY_TEST_READBACK_MODE", "device=handoff"),
            ),
            resolve_common_environment(args, spec, candidate_environment),
        )

    def test_exact_e2b_ffn_pins_the_f32_comparison_profile(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--candidate", "q4-0-e2b-ffn-exact"],
        ):
            args = parse_args()
        spec = resolve_candidate_spec(args)
        self.assertEqual(
            EXACT_E2B_FFN_F32_COMPARISON_ENVIRONMENT,
            resolve_common_environment(args, spec, resolve_candidate_environment(args, spec)),
        )

    def test_pair_only_e2b_ffn_pins_competing_routes_off(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--candidate", "q4-0-q8-1-e2b-ffn-pair-only"],
        ):
            args = parse_args()
        spec = resolve_candidate_spec(args)
        self.assertEqual(
            E2B_FFN_PAIR_ONLY_COMPARISON_ENVIRONMENT,
            resolve_common_environment(args, spec, resolve_candidate_environment(args, spec)),
        )

    def test_score_prework_catalog_pins_the_paged_f32_value_comparison_profile(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--kernel-id", SCORE_PREWORK_ATTENTION_KERNEL_ID],
        ):
            args = parse_args()
        spec = resolve_candidate_spec(args)
        self.assertEqual(CANDIDATE_CATALOG[SCORE_PREWORK_ATTENTION_KERNEL_ID], spec)
        self.assertEqual(
            SCORE_PREWORK_ATTENTION_COMPARISON_ENVIRONMENT,
            resolve_common_environment(args, spec, resolve_candidate_environment(args, spec)),
        )
        self.assertEqual(
            "launch_attention_gqa_decode_score_prework",
            spec.route_counter,
        )

    def test_tiled64_score_prework_catalog_is_isolated_and_requires_both_head_dims(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--kernel-id", SCORE_PREWORK_TILED64_ATTENTION_KERNEL_ID],
        ):
            args = parse_args()
            spec = resolve_candidate_spec(args)
        self.assertEqual(CANDIDATE_CATALOG[SCORE_PREWORK_TILED64_ATTENTION_KERNEL_ID], spec)
        self.assertEqual(
            "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK_CONSUMER",
            spec.environment_variable,
        )
        self.assertEqual("serial", spec.baseline_gate_value)
        self.assertEqual("required-tiled64", spec.candidate_gate_value)
        self.assertEqual(
            SCORE_PREWORK_TILED64_ATTENTION_COMPARISON_ENVIRONMENT,
            resolve_common_environment(args, spec, ()),
        )
        self.assertEqual(
            [
                "launch_attention_gqa_decode_score_prework_tiled64_hd256",
                "launch_attention_gqa_decode_score_prework_tiled64_hd512",
            ],
            [counter.name for counter in spec.required_route_counters],
        )
        self.assertEqual(
            [
                "launch_attention_gqa_decode_score_prework_serial_hd256",
                "launch_attention_gqa_decode_score_prework_serial_hd512",
            ],
            [counter.name for counter in spec.required_baseline_route_counters],
        )
        self.assertEqual(
            {
                "launch_attention_gqa_decode_score_prework_serial",
                "launch_attention_gqa_decode_score_prework_serial_hd256",
                "launch_attention_gqa_decode_score_prework_serial_hd512",
                "launch_attention_gqa_decode_score_prework_tiled64_fallbacks",
                "launch_attention_gqa_decode_score_prework_tiled64_forbidden_routes",
                "launch_attention_gqa_decode_score_prework_tiled64_symbol_fallbacks",
                "launch_attention_gqa_decode_fast",
                "launch_attention_gqa_decode_fast_fallbacks",
            },
            {counter.name for counter in spec.forbidden_route_counters},
        )
        self.assertEqual(GEMMA4_LONG_CONTEXT_WORKLOAD, spec.qualification_workload)

    def test_splitk_online_catalog_locks_e2b_profile_routes_and_fallbacks(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--kernel-id", GQA_DECODE_SPLITK_ONLINE_SM89_KERNEL_ID],
        ):
            args = parse_args()
            spec = resolve_candidate_spec(args)
        self.assertEqual(CANDIDATE_CATALOG[GQA_DECODE_SPLITK_ONLINE_SM89_KERNEL_ID], spec)
        self.assertEqual("ANTFLY_INFERENCE_CUDA_GQA_DECODE_PROFILE", spec.environment_variable)
        self.assertEqual("off", spec.baseline_gate_value)
        self.assertEqual("required-splitk-online-sm89", spec.candidate_gate_value)
        self.assertEqual(GEMMA4_LONG_CONTEXT_WORKLOAD, spec.qualification_workload)
        self.assertEqual(
            SPLITK_ONLINE_SM89_COMPARISON_ENVIRONMENT,
            resolve_common_environment(args, spec, ()),
        )
        self.assertEqual(
            [
                "launch_attention_gqa_decode_splitk_online_sm89",
                "launch_attention_gqa_decode_splitk_online_sm89_hd256",
                "launch_attention_gqa_decode_splitk_online_sm89_hd512",
            ],
            [counter.name for counter in spec.required_route_counters],
        )
        self.assertEqual(
            [140, 112, 28],
            [item.exact_count for item in spec.qualification_route_counts],
        )
        self.assertTrue(spec.require_persistent_replay)
        with mock.patch.object(
            sys,
            "argv",
            [
                "validate_gemma4_cuda_candidate.py",
                "--kernel-id",
                GQA_DECODE_SPLITK_ONLINE_SM89_KERNEL_ID,
                "--no-require-persistent-replay",
            ],
        ):
            replay_disabled = parse_args()
        with self.assertRaisesRegex(ValueError, "requires persistent replay validation"):
            validate_qualification_contract(replay_disabled)
        fixed_environment = dict(spec.fixed_comparison_environment)
        self.assertEqual(
            "1",
            fixed_environment["ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK"],
        )
        self.assertEqual(
            "required-tiled64",
            fixed_environment[
                "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK_CONSUMER"
            ],
        )
        self.assertEqual(
            [
                "launch_attention_gqa_decode_score_prework_tiled64_hd256",
                "launch_attention_gqa_decode_score_prework_tiled64_hd512",
            ],
            [counter.name for counter in spec.required_baseline_route_counters],
        )
        self.assertIn(
            "launch_attention_gqa_decode_splitk_online_sm89_forbidden_routes",
            {counter.name for counter in spec.forbidden_route_counters},
        )

        baseline = timing(
            score_prework=140,
            score_prework_tiled64_hd256=112,
            score_prework_tiled64_hd512=28,
            tokens=300,
            persistent_replays=295,
        )
        candidate = timing(
            splitk_online=140,
            splitk_online_hd256=112,
            splitk_online_hd512=28,
            tokens=300,
            persistent_replays=295,
        )
        self.assertEqual(
            [],
            validate_pair(baseline, candidate, 300, spec, require_full_route_coverage=True),
        )
        candidate["cuda"]["launch_attention_gqa_decode_splitk_online_sm89_symbol_fallbacks"] = 1
        errors = validate_pair(baseline, candidate, 300, spec, require_full_route_coverage=True)
        self.assertTrue(any("symbol fallbacks" in error for error in errors))

    def test_gqa_prefill_catalog_profiles_require_both_gemma4_head_dims(self):
        expected = {
            GQA_PREFILL_TILED_F16_EXACT_KERNEL_ID: (
                "required-tiled-f16-exact",
                [
                    "launch_attention_gqa_prefill_tiled_f16_exact_hd256",
                    "launch_attention_gqa_prefill_tiled_f16_exact_hd512",
                ],
            ),
            GQA_PREFILL_TILED_F16_WARP_KERNEL_ID: (
                "required-tiled-f16-warp",
                [
                    "launch_attention_gqa_prefill_tiled_f16_warp_hd256",
                    "launch_attention_gqa_prefill_tiled_f16_warp_hd512",
                ],
            ),
        }
        for kernel_id, (candidate_value, counters) in expected.items():
            with self.subTest(kernel_id=kernel_id), mock.patch.object(
                sys,
                "argv",
                ["validate_gemma4_cuda_candidate.py", "--kernel-id", kernel_id],
            ):
                spec = resolve_candidate_spec(parse_args())
            self.assertEqual("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE", spec.environment_variable)
            self.assertEqual("required-fast", spec.baseline_gate_value)
            self.assertEqual(candidate_value, spec.candidate_gate_value)
            self.assertEqual(counters, [counter.name for counter in spec.required_route_counters])
            if kernel_id == GQA_PREFILL_TILED_F16_WARP_KERNEL_ID:
                self.assertEqual(
                    [140, 35],
                    [item.exact_count for item in spec.qualification_route_counts],
                )

    def test_flash_prefill_catalog_locks_all_head_and_query_buckets(self):
        spec = CANDIDATE_CATALOG[GQA_PREFILL_FLASH_F16_SM89_KERNEL_ID]
        self.assertEqual("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE", spec.environment_variable)
        self.assertEqual("required-fast", spec.baseline_gate_value)
        self.assertEqual("required-flash-f16-sm89", spec.candidate_gate_value)
        self.assertEqual("prefill", spec.route_phase)
        self.assertEqual(GEMMA4_LONG_CONTEXT_WORKLOAD, spec.qualification_workload)
        self.assertEqual(
            [
                "launch_attention_gqa_prefill_flash_f16_sm89_hd256_q512",
                "launch_attention_gqa_prefill_flash_f16_sm89_hd256_q3",
                "launch_attention_gqa_prefill_flash_f16_sm89_hd512_q512",
                "launch_attention_gqa_prefill_flash_f16_sm89_hd512_q3",
            ],
            [counter.name for counter in spec.required_route_counters],
        )
        self.assertEqual(
            [112, 28, 28, 7],
            [item.exact_count for item in spec.qualification_route_counts],
        )
        self.assertEqual(
            {
                "launch_attention_gqa_prefill_flash_f16_sm89_fallbacks",
                "launch_attention_gqa_prefill_flash_f16_sm89_ineligible_fallbacks",
                "launch_attention_gqa_prefill_flash_f16_sm89_symbol_fallbacks",
            },
            {counter.name for counter in spec.forbidden_route_counters},
        )

        candidate = timing(
            gqa_prefill_flash_hd256_q512=112,
            gqa_prefill_flash_hd256_q3=28,
            gqa_prefill_flash_hd512_q512=28,
            gqa_prefill_flash_hd512_q3=7,
            tokens=300,
        )
        self.assertEqual(
            [],
            validate_pair(
                timing(tokens=300),
                candidate,
                300,
                spec,
                require_full_route_coverage=True,
            ),
        )
        candidate["cuda"]["launch_attention_gqa_prefill_flash_f16_sm89_fallbacks"] = 1
        errors = validate_pair(
            timing(tokens=300),
            candidate,
            300,
            spec,
            require_full_route_coverage=True,
        )
        self.assertIn("candidate reported Flash F16 SM89 GQA prefill fallbacks: 1", errors)

    def test_warp_and_tiled64_fixed_profiles_require_the_exact_long_context_workload(self):
        def parsed(kernel_id: str, profile: str, fixture: pathlib.Path = LONG_CONTEXT_FIXTURE):
            with mock.patch.object(
                sys,
                "argv",
                [
                    "validate_gemma4_cuda_candidate.py",
                    "--kernel-id",
                    kernel_id,
                    "--qualification-profile",
                    profile,
                    "--prompt-fixture",
                    str(fixture),
                    "--lengths",
                    "300",
                    "--prefill-chunk-size",
                    "512",
                    "--cache-dtype",
                    "f16",
                    "--capture-kv-capacity",
                    "2432",
                ],
            ):
                return parse_args()

        for kernel_id, profile in (
            (GQA_PREFILL_TILED_F16_WARP_KERNEL_ID, "prefill-promotion"),
            (GQA_PREFILL_FLASH_F16_SM89_KERNEL_ID, "prefill-promotion"),
            (SCORE_PREWORK_TILED64_ATTENTION_KERNEL_ID, "promotion"),
        ):
            with self.subTest(kernel_id=kernel_id):
                args = parsed(kernel_id, profile)
                validate_qualification_contract(args)
                self.assertEqual(
                    GEMMA4_LONG_CONTEXT_WORKLOAD.fixture_file_sha256,
                    load_prompt_fixture(args.prompt_fixture[0])[1]["file_sha256"],
                )

        tiled = parsed(SCORE_PREWORK_TILED64_ATTENTION_KERNEL_ID, "promotion")
        for field, value, message in (
            ("lengths", [299], "lengths"),
            ("cache_dtype", "f32", "cache-dtype"),
            ("prefill_chunk_size", 32, "prefill-chunk-size"),
            ("capture_kv_capacity", 2431, "capture-kv-capacity"),
        ):
            with self.subTest(field=field):
                original = getattr(tiled, field)
                setattr(tiled, field, value)
                with self.assertRaisesRegex(ValueError, message):
                    validate_qualification_contract(tiled)
                setattr(tiled, field, original)

        tiled.prompt_fixture = [LONG_CONTEXT_FIXTURE, LONG_CONTEXT_FIXTURE]
        with self.assertRaisesRegex(ValueError, "exactly one locked --prompt-fixture"):
            validate_qualification_contract(tiled)

        with tempfile.TemporaryDirectory() as temporary:
            substitute = pathlib.Path(temporary) / "substitute.json"
            fixture_json = json.loads(LONG_CONTEXT_FIXTURE.read_text(encoding="utf-8"))
            fixture_json["description"] += " Substituted fixture file."
            substitute.write_text(json.dumps(fixture_json), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "exact fixture file_sha256"):
                validate_qualification_contract(
                    parsed(SCORE_PREWORK_TILED64_ATTENTION_KERNEL_ID, "promotion", substitute)
                )

    def test_ggml_and_cublaslt_catalog_profiles_are_typed_and_default_off(self):
        ggml = CANDIDATE_CATALOG[Q4_0_GGML_Q8_1_E2B_FFN_KERNEL_ID]
        self.assertEqual("ANTFLY_INFERENCE_CUDA_SM89_Q4_0_Q8_1", ggml.environment_variable)
        self.assertEqual("off", ggml.baseline_gate_value)
        self.assertEqual("ggml-ffn-v1", ggml.candidate_gate_value)
        self.assertEqual(GGML_Q8_1_E2B_FFN_COMPARISON_ENVIRONMENT, ggml.fixed_comparison_environment)
        self.assertEqual(["q4_0_ggml_q8_1_e2b_ffn_hits"], [item.name for item in ggml.required_route_counters])
        self.assertEqual(
            ["q4_0_ggml_q8_1_e2b_ffn_fallbacks"],
            [item.name for item in ggml.forbidden_route_counters],
        )

        cublaslt = CANDIDATE_CATALOG[CUBLASLT_BF16_PREFILL_SM89_KERNEL_ID]
        self.assertEqual(
            "ANTFLY_INFERENCE_CUDA_CUBLASLT_BF16_TUNING_PROFILE",
            cublaslt.environment_variable,
        )
        self.assertEqual("off", cublaslt.baseline_gate_value)
        self.assertEqual("sm89-prefill", cublaslt.candidate_gate_value)
        self.assertEqual(
            CUBLASLT_BF16_PREFILL_COMPARISON_ENVIRONMENT,
            cublaslt.fixed_comparison_environment,
        )
        self.assertEqual(
            ["bf16_cublaslt_tuning_tuned_calls"],
            [item.name for item in cublaslt.required_route_counters],
        )
        self.assertEqual(
            ["bf16_cublaslt_tuning_api_fallbacks"],
            [item.name for item in cublaslt.forbidden_route_counters],
        )
        self.assertNotIn(
            "bf16_cublaslt_tuning_heuristic_calls",
            [item.name for item in cublaslt.forbidden_route_counters],
        )

    def test_ple_gate_mirror_first_catalog_is_typed_locked_and_prefill_scoped(self):
        spec = CANDIDATE_CATALOG[PLE_GATE_BF16_MIRROR_FIRST_SM89_E2B_KERNEL_ID]
        self.assertEqual(
            "ANTFLY_INFERENCE_CUDA_PLE_GATE_PREFILL_PROFILE",
            spec.environment_variable,
        )
        self.assertEqual("off", spec.baseline_gate_value)
        self.assertEqual("mirror-first-sm89-e2b", spec.candidate_gate_value)
        self.assertEqual("prefill", spec.route_phase)
        self.assertEqual(GEMMA4_LONG_CONTEXT_WORKLOAD, spec.qualification_workload)
        self.assertEqual(
            PLE_GATE_BF16_MIRROR_FIRST_COMPARISON_ENVIRONMENT,
            spec.fixed_comparison_environment,
        )
        fixed = dict(spec.fixed_comparison_environment)
        self.assertEqual("1", fixed["ANTFLY_INFERENCE_CUDA_Q4_0_WEIGHTS_BF16_PREFILL"])
        self.assertEqual(
            "required-flash-f16-sm89",
            fixed["ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE"],
        )
        self.assertEqual(
            "required-splitk-online-sm89",
            fixed["ANTFLY_INFERENCE_CUDA_GQA_DECODE_PROFILE"],
        )
        self.assertEqual(
            [
                "ple_gate_prefill_bf16_mirror_first_hits",
                "ple_gate_decode_q4_fused_preserved",
            ],
            [counter.name for counter in spec.required_route_counters],
        )
        self.assertEqual(
            ["ple_gate_prefill_bf16_mirror_first_ineligible"],
            [counter.name for counter in spec.forbidden_route_counters],
        )
        self.assertEqual(
            ["linear_activation_slice_fused_q4_0"],
            [counter.name for counter in spec.required_baseline_route_counters],
        )

    def test_exact_e2b_ffn_rejects_overrides_of_the_f32_comparison_profile(self):
        locked_name = "ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_DP4A"
        with mock.patch.object(
            sys,
            "argv",
            [
                "validate_gemma4_cuda_candidate.py",
                "--candidate",
                "q4-0-e2b-ffn-exact",
                "--candidate-env",
                f"{locked_name}=1",
            ],
        ):
            args = parse_args()
        spec = resolve_candidate_spec(args)
        with self.assertRaisesRegex(ValueError, "fixed comparison environment"):
            resolve_candidate_environment(args, spec)

        with mock.patch.object(
            sys,
            "argv",
            [
                "validate_gemma4_cuda_candidate.py",
                "--candidate",
                "q4-0-e2b-ffn-exact",
                "--common-env",
                f"{locked_name}=1",
            ],
        ):
            args = parse_args()
        spec = resolve_candidate_spec(args)
        with self.assertRaisesRegex(ValueError, "fixed comparison environment"):
            resolve_common_environment(args, spec, resolve_candidate_environment(args, spec))

    def test_candidate_environment_parser_rejects_malformed_or_unsafe_values(self):
        self.assertEqual(("ANTFLY_TEST", "with=equals"), parse_candidate_environment("ANTFLY_TEST=with=equals"))
        self.assertEqual(("ANTFLY_EMPTY", ""), parse_candidate_environment("ANTFLY_EMPTY="))
        for value, message in (
            ("ANTFLY_TEST", "NAME=VALUE"),
            ("antfly_test=value", "name is invalid"),
            ("=value", "name is invalid"),
            ("ANTFLY_TEST=line\nbreak", "control character"),
            ("ANTFLY_TEST=tab\tvalue", "control character"),
        ):
            with self.assertRaisesRegex(ValueError, message):
                parse_candidate_environment(value)
        with self.assertRaisesRegex(ValueError, "must be unique"):
            parse_candidate_environment_list(["ANTFLY_TEST=first", "ANTFLY_TEST=second"])

    def test_common_environment_parser_rejects_malformed_or_unsafe_values(self):
        self.assertEqual(("ANTFLY_TEST", "with=equals"), parse_common_environment("ANTFLY_TEST=with=equals"))
        self.assertEqual(("ANTFLY_EMPTY", ""), parse_common_environment("ANTFLY_EMPTY="))
        for value, message in (
            ("ANTFLY_TEST", "NAME=VALUE"),
            ("antfly_test=value", "name is invalid"),
            ("=value", "name is invalid"),
            ("ANTFLY_TEST=line\nbreak", "control character"),
            ("ANTFLY_TEST=tab\tvalue", "control character"),
        ):
            with self.assertRaisesRegex(ValueError, message):
                parse_common_environment(value)
        with self.assertRaisesRegex(ValueError, "must be unique"):
            parse_common_environment_list(["ANTFLY_TEST=first", "ANTFLY_TEST=second"])

    def test_candidate_environment_cannot_override_selected_gate(self):
        with mock.patch.object(
            sys,
            "argv",
            [
                "validate_gemma4_cuda_candidate.py",
                "--candidate-env",
                "ANTFLY_GENERATED_ATTENTION_DECODE=0",
            ],
        ):
            args = parse_args()
        with self.assertRaisesRegex(ValueError, "must not override selected candidate gate"):
            resolve_candidate_environment(args, resolve_candidate_spec(args))

    def test_common_environment_cannot_override_gate_or_capture_capacity(self):
        for name in (
            "ANTFLY_GENERATED_ATTENTION_DECODE",
            CAPTURE_KV_CAPACITY_ENV,
            RUNTIME_CAPTURE_KV_CAPACITY_ENV,
        ):
            with self.subTest(name=name), mock.patch.object(
                sys,
                "argv",
                ["validate_gemma4_cuda_candidate.py", "--common-env", f"{name}=0"],
            ):
                args = parse_args()
                spec = resolve_candidate_spec(args)
                candidate_environment = resolve_candidate_environment(args, spec)
                expected = "selected candidate gate" if name == spec.environment_variable else "capture KV capacity"
                with self.assertRaisesRegex(ValueError, expected):
                    resolve_common_environment(args, spec, candidate_environment)

    def test_common_and_candidate_environment_cannot_overlap(self):
        with mock.patch.object(
            sys,
            "argv",
            [
                "validate_gemma4_cuda_candidate.py",
                "--candidate-env",
                "ANTFLY_TEST_SCHEDULE=split8",
                "--common-env",
                "ANTFLY_TEST_SCHEDULE=serial",
            ],
        ):
            args = parse_args()
        spec = resolve_candidate_spec(args)
        candidate_environment = resolve_candidate_environment(args, spec)
        with self.assertRaisesRegex(ValueError, "must not override the same variable"):
            resolve_common_environment(args, spec, candidate_environment)

    def test_default_capture_capacity_has_prompt_headroom(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--lengths", "64", "768"],
        ):
            args = parse_args()
        self.assertEqual(
            (768 + DEFAULT_CAPTURE_KV_PROMPT_HEADROOM, "max-output-plus-prompt-headroom"),
            resolve_capture_kv_capacity(args),
        )
        args.capture_kv_capacity = 0
        with self.assertRaisesRegex(ValueError, "must be positive"):
            resolve_capture_kv_capacity(args)

    def test_cli_selects_e2b_ffn_candidate(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--candidate", "q4-0-q8-1-e2b-ffn"],
        ):
            self.assertEqual(CandidateKind.Q4_0_Q8_1_E2B_FFN, parse_args().candidate)

    def test_cli_selects_exact_e2b_ffn_candidate(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--candidate", "q4-0-e2b-ffn-exact"],
        ):
            self.assertEqual(CandidateKind.Q4_0_E2B_FFN_EXACT, parse_args().candidate)

    def test_cli_selects_pair_only_e2b_ffn_candidate(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--candidate", "q4-0-q8-1-e2b-ffn-pair-only"],
        ):
            self.assertEqual(CandidateKind.Q4_0_Q8_1_E2B_FFN_PAIR_ONLY, parse_args().candidate)

    def test_cli_selects_pair_only_e2b_ffn_catalog_kernel(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--kernel-id", Q4_0_Q8_1_E2B_FFN_PAIR_ONLY_KERNEL_ID],
        ):
            args = parse_args()
        self.assertEqual(
            CANDIDATE_CATALOG[Q4_0_Q8_1_E2B_FFN_PAIR_ONLY_KERNEL_ID],
            resolve_candidate_spec(args),
        )

    def test_cli_selects_q6_lm_head_candidate(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--candidate", "q6-k-q8-1-lm-head-argmax"],
        ):
            args = parse_args()
        self.assertEqual(CandidateKind.Q6_K_Q8_1_LM_HEAD_ARGMAX, args.candidate)
        self.assertIsNone(args.model)
        self.assertIsNone(args.model_label)

    def test_cli_selects_model_neutral_catalog_kernel(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--kernel-id", Q4_0_Q8_1_FFN_KERNEL_ID],
        ):
            args = parse_args()
            self.assertIsNone(args.candidate)
            self.assertEqual(Q4_0_Q8_1_FFN_KERNEL_ID, args.kernel_id)
            self.assertEqual(
                CANDIDATE_CATALOG[Q4_0_Q8_1_FFN_KERNEL_ID],
                resolve_candidate_spec(args),
            )
            self.assertEqual(resolve_candidate_spec(args), candidate_spec(Q4_0_Q8_1_FFN_KERNEL_ID))

    def test_cli_selects_exact_e2b_ffn_catalog_kernel(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--kernel-id", Q4_0_E2B_FFN_EXACT_KERNEL_ID],
        ):
            args = parse_args()
        self.assertIsNone(args.candidate)
        self.assertEqual(Q4_0_E2B_FFN_EXACT_KERNEL_ID, args.kernel_id)
        self.assertEqual(
            CANDIDATE_CATALOG[Q4_0_E2B_FFN_EXACT_KERNEL_ID],
            resolve_candidate_spec(args),
        )

    def test_cli_selects_q6_model_neutral_catalog_kernel(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--kernel-id", Q6_K_Q8_1_LM_HEAD_ARGMAX_KERNEL_ID],
        ):
            args = parse_args()
        self.assertIsNone(args.candidate)
        self.assertEqual(Q6_K_Q8_1_LM_HEAD_ARGMAX_KERNEL_ID, args.kernel_id)
        self.assertEqual(
            CANDIDATE_CATALOG[Q6_K_Q8_1_LM_HEAD_ARGMAX_KERNEL_ID],
            resolve_candidate_spec(args),
        )

    def test_cli_records_e4b_12b_model_and_config_labels(self):
        model = pathlib.Path("/models/google/gemma-4-12B-it-QAT-E4B.gguf")
        with mock.patch.object(
            sys,
            "argv",
            [
                "validate_gemma4_cuda_candidate.py",
                "--model",
                str(model),
                "--model-label",
                "gemma4-12b-e4b",
                "--config-label",
                "e4b-12b-sm89-f32",
            ],
        ):
            args = parse_args()
        config = result_config_metadata(args, timing_metadata_from_args(args))
        self.assertEqual(str(model), config["model_path"])
        self.assertEqual("gemma4-12b-e4b", config["model_label"])
        self.assertEqual("e4b-12b-sm89-f32", config["config_label"])
        self.assertEqual(
            {"path": str(model), "label": "gemma4-12b-e4b"},
            config["model"],
        )
        self.assertIsNone(config["candidate_environment"])
        self.assertIsNone(config["capture_kv_capacity"])

    def test_result_config_metadata_records_common_environment(self):
        with mock.patch.object(sys, "argv", ["validate_gemma4_cuda_candidate.py"]):
            args = parse_args()
        spec = resolve_candidate_spec(args)
        config = result_config_metadata(
            args,
            timing_metadata_from_args(args),
            spec,
            (),
            (4096, "explicit"),
            (("ANTFLY_INFERENCE_CUDA_GREEDY_PENDING_TOKEN_READBACK", "0"),),
        )
        self.assertEqual(
            {"ANTFLY_INFERENCE_CUDA_GREEDY_PENDING_TOKEN_READBACK": "0"},
            config["candidate_environment"]["common_overrides"],
        )
        self.assertEqual({"tokens": 4096, "source": "explicit"}, config["capture_kv_capacity"])

    def test_candidate_environment_metadata_records_split_threshold_provenance(self):
        attention = candidate_spec(CandidateKind.GENERATED_ATTENTION)
        metadata = candidate_environment_metadata(
            attention,
            (
                (GENERATED_ATTENTION_SPLIT_KV_MIN_TOKENS_ENV, "128"),
                (GENERATED_ATTENTION_SPLIT_KV_SPLITS_ENV, "4"),
                ("ANTFLY_TEST_SCHEDULE", "split8"),
            ),
        )
        self.assertEqual({attention.environment_variable: "0"}, metadata["baseline_gate"])
        self.assertEqual({attention.environment_variable: "1"}, metadata["candidate_gate"])
        self.assertEqual(
            "128",
            metadata["generated_attention_split_kv_min_tokens"]["value"],
        )
        self.assertEqual(
            "candidate-env",
            metadata["generated_attention_split_kv_min_tokens"]["source"],
        )
        self.assertEqual("4", metadata["generated_attention_split_kv_splits"]["value"])
        self.assertEqual("candidate-env", metadata["generated_attention_split_kv_splits"]["source"])
        self.assertEqual("split8", metadata["candidate_overrides"]["ANTFLY_TEST_SCHEDULE"])

        common = candidate_environment_metadata(
            attention,
            (),
            (
                (GENERATED_ATTENTION_SPLIT_KV_MIN_TOKENS_ENV, "512"),
                (GENERATED_ATTENTION_SPLIT_KV_SPLITS_ENV, "2"),
                ("ANTFLY_INFERENCE_CUDA_ASYNC_I32_DOWNLOAD_STAGING", "0"),
            ),
        )
        self.assertEqual(
            "512",
            common["generated_attention_split_kv_min_tokens"]["value"],
        )
        self.assertEqual(
            "common-env",
            common["generated_attention_split_kv_min_tokens"]["source"],
        )
        self.assertEqual("2", common["generated_attention_split_kv_splits"]["value"])
        self.assertEqual("common-env", common["generated_attention_split_kv_splits"]["source"])
        self.assertEqual(
            "0",
            common["common_overrides"]["ANTFLY_INFERENCE_CUDA_ASYNC_I32_DOWNLOAD_STAGING"],
        )

        with mock.patch.dict(
            os.environ,
            {
                GENERATED_ATTENTION_SPLIT_KV_MIN_TOKENS_ENV: "384",
                GENERATED_ATTENTION_SPLIT_KV_SPLITS_ENV: "8",
            },
            clear=False,
        ):
            inherited = candidate_environment_metadata(attention, ())
        self.assertEqual("384", inherited["generated_attention_split_kv_min_tokens"]["value"])
        self.assertEqual("inherited-environment", inherited["generated_attention_split_kv_min_tokens"]["source"])
        self.assertEqual("8", inherited["generated_attention_split_kv_splits"]["value"])
        self.assertEqual("inherited-environment", inherited["generated_attention_split_kv_splits"]["source"])

    def test_parses_cli_token_ids(self):
        match = TOKEN_IDS_RE.search("timing\ntoken_ids: 4 -2 17\ndone\n")
        self.assertIsNotNone(match)
        self.assertEqual([4, -2, 17], [int(value) for value in match.group("ids").split()])

    def test_accepts_equal_tokens_and_expected_routes(self):
        self.assertEqual([], validate_pair(timing(generated=0), timing(generated=7), 64))

    def test_accepts_score_prework_with_dedicated_route_counter(self):
        spec = CANDIDATE_CATALOG[SCORE_PREWORK_ATTENTION_KERNEL_ID]
        self.assertEqual([], validate_pair(timing(), timing(score_prework=64), 64, spec))

    def test_tiled64_score_prework_requires_baseline_and_candidate_head_dim_routes(self):
        spec = CANDIDATE_CATALOG[SCORE_PREWORK_TILED64_ATTENTION_KERNEL_ID]
        baseline = timing(score_prework_serial_hd256=18, score_prework_serial_hd512=9)
        candidate = timing(score_prework_tiled64_hd256=18, score_prework_tiled64_hd512=9)
        self.assertEqual([], validate_pair(baseline, candidate, 64, spec))
        attestation = pair_attestation(baseline, candidate, 64, spec)
        self.assertTrue(attestation["all_required_baseline_routes_attested"])
        self.assertTrue(attestation["all_required_routes_attested"])

        missing_baseline = validate_pair(
            timing(score_prework_serial_hd256=18),
            candidate,
            64,
            spec,
        )
        self.assertIn("baseline did not use serial score-prework consumer HD512 route", missing_baseline)

        wrong_candidate = validate_pair(
            baseline,
            timing(
                score_prework_tiled64_hd256=18,
                score_prework_tiled64_hd512=9,
                score_prework_serial_hd512=1,
                score_prework_tiled64_symbol_fallbacks=1,
                decode_fast=1,
            ),
            64,
            spec,
        )
        self.assertIn("candidate reported serial score-prework consumer HD512 route: 1", wrong_candidate)
        self.assertIn("candidate reported tiled64 score-prework symbol fallbacks: 1", wrong_candidate)
        self.assertIn("candidate reported fast decode attention route: 1", wrong_candidate)

    def test_gqa_prefill_candidates_require_hd256_and_hd512_route_evidence(self):
        exact = CANDIDATE_CATALOG[GQA_PREFILL_TILED_F16_EXACT_KERNEL_ID]
        self.assertEqual(
            [],
            validate_pair(
                timing(),
                timing(gqa_prefill_exact_hd256=18, gqa_prefill_exact_hd512=9),
                64,
                exact,
            ),
        )
        errors = validate_pair(timing(), timing(gqa_prefill_exact_hd256=18), 64, exact)
        self.assertIn("candidate did not use tiled F16 exact GQA prefill HD512 route", errors)

        warp = CANDIDATE_CATALOG[GQA_PREFILL_TILED_F16_WARP_KERNEL_ID]
        self.assertEqual(
            [],
            validate_pair(
                timing(),
                timing(gqa_prefill_warp_hd256=18, gqa_prefill_warp_hd512=9),
                64,
                warp,
            ),
        )

    def test_warp_prefill_fixed_qualification_requires_exact_locked_route_counts(self):
        spec = CANDIDATE_CATALOG[GQA_PREFILL_TILED_F16_WARP_KERNEL_ID]
        baseline = timing(tokens=300)
        candidate = timing(
            gqa_prefill_warp_hd256=140,
            gqa_prefill_warp_hd512=35,
            tokens=300,
        )
        self.assertEqual(
            [],
            validate_pair(
                baseline,
                candidate,
                300,
                spec,
                require_full_route_coverage=True,
            ),
        )
        attestation = pair_attestation(
            baseline,
            candidate,
            300,
            spec,
            require_full_route_coverage=True,
        )
        hd256 = attestation["required_routes"][
            "launch_attention_gqa_prefill_tiled_f16_warp_hd256"
        ]
        hd512 = attestation["required_routes"][
            "launch_attention_gqa_prefill_tiled_f16_warp_hd512"
        ]
        self.assertEqual(140, hd256["expected_candidate_launch_observations"])
        self.assertEqual(35, hd512["expected_candidate_launch_observations"])
        self.assertTrue(hd256["exact_count_attested"])
        self.assertTrue(attestation["all_required_routes_attested"])

        errors = validate_pair(
            baseline,
            timing(gqa_prefill_warp_hd256=139, gqa_prefill_warp_hd512=35, tokens=300),
            300,
            spec,
            require_full_route_coverage=True,
        )
        self.assertIn(
            "candidate tiled F16 warp GQA prefill HD256 route count 139 did not match "
            "locked qualification count 140",
            errors,
        )
        failed_attestation = pair_attestation(
            baseline,
            timing(gqa_prefill_warp_hd256=139, gqa_prefill_warp_hd512=35, tokens=300),
            300,
            spec,
            require_full_route_coverage=True,
        )
        self.assertFalse(failed_attestation["all_required_routes_attested"])

        # Legacy/ad-hoc comparisons retain positive-hit route semantics.
        self.assertEqual(
            [],
            validate_pair(
                baseline,
                timing(gqa_prefill_warp_hd256=18, gqa_prefill_warp_hd512=9, tokens=300),
                300,
                spec,
            ),
        )

    def test_ggml_and_cublaslt_candidate_route_attestation_is_fail_closed(self):
        ggml = CANDIDATE_CATALOG[Q4_0_GGML_Q8_1_E2B_FFN_KERNEL_ID]
        self.assertEqual([], validate_pair(timing(), timing(ggml_e2b_ffn=27), 64, ggml))
        errors = validate_pair(
            timing(),
            timing(ggml_e2b_ffn=27, ggml_e2b_ffn_fallbacks=1),
            64,
            ggml,
        )
        self.assertIn("candidate reported SM89 GGML Q8_1 E2B FFN fallbacks: 1", errors)

        cublaslt = CANDIDATE_CATALOG[CUBLASLT_BF16_PREFILL_SM89_KERNEL_ID]
        self.assertEqual(
            [],
            validate_pair(
                timing(),
                timing(cublaslt_tuned_calls=4, cublaslt_heuristic_calls=2),
                64,
                cublaslt,
            ),
        )
        errors = validate_pair(
            timing(),
            timing(cublaslt_tuned_calls=4, cublaslt_api_fallbacks=1),
            64,
            cublaslt,
        )
        self.assertIn("candidate reported SM89 BF16 cuBLASLt tuning API fallbacks: 1", errors)

    def test_ple_gate_mirror_first_attestation_requires_baseline_and_zero_misses(self):
        spec = CANDIDATE_CATALOG[PLE_GATE_BF16_MIRROR_FIRST_SM89_E2B_KERNEL_ID]
        self.assertEqual(
            [],
            validate_pair(
                timing(ple_gate_fused_q4=175),
                timing(ple_gate_mirror_first=175, ple_gate_decode_preserved=140),
                64,
                spec,
            ),
        )
        missing_baseline = validate_pair(
            timing(),
            timing(ple_gate_mirror_first=175, ple_gate_decode_preserved=140),
            64,
            spec,
        )
        self.assertIn("baseline did not use baseline fused Q4_0 PLE-gate route", missing_baseline)
        missing_decode_preservation = validate_pair(
            timing(ple_gate_fused_q4=175),
            timing(ple_gate_mirror_first=175),
            64,
            spec,
        )
        self.assertIn(
            "candidate did not use E2B rows==1 fused Q4 PLE-gate route preservation",
            missing_decode_preservation,
        )
        ineligible = validate_pair(
            timing(ple_gate_fused_q4=175),
            timing(
                ple_gate_mirror_first=174,
                ple_gate_mirror_first_ineligible=1,
                ple_gate_decode_preserved=140,
            ),
            64,
            spec,
        )
        self.assertIn(
            "candidate reported SM89 E2B PLE-gate mirror-first eligibility misses: 1",
            ineligible,
        )

    def test_rejects_token_mismatch(self):
        candidate = timing(generated=7)
        candidate["token_ids"][10] = 999
        self.assertIn("token IDs differ at index 10", validate_pair(timing(generated=0), candidate, 64))

    def test_rejects_route_and_replay_failures(self):
        candidate = timing(generated=0)
        candidate["cuda"]["graph_capture_persistent_replays"] = 0
        errors = validate_pair(timing(generated=0), candidate, 64)
        self.assertTrue(any("persistent replays" in error for error in errors))
        self.assertIn("candidate did not use generated attention", errors)

    def test_accepts_lm_head_candidate_with_exact_tokens_and_route(self):
        spec = candidate_spec(CandidateKind.Q4_0_Q8_1_LM_HEAD_ARGMAX)
        self.assertEqual([], validate_pair(timing(), timing(lm_argmax=64), 64, spec))

    def test_rejects_lm_head_baseline_hit_and_missing_candidate_hit(self):
        spec = candidate_spec(CandidateKind.Q4_0_Q8_1_LM_HEAD_ARGMAX)
        errors = validate_pair(timing(lm_argmax=1), timing(), 64, spec)
        self.assertIn("baseline unexpectedly used Q4_0 x Q8_1 LM-head argmax", errors)
        self.assertIn("candidate did not use Q4_0 x Q8_1 LM-head argmax", errors)

    def test_rejects_lm_head_candidate_fallback(self):
        spec = candidate_spec(CandidateKind.Q4_0_Q8_1_LM_HEAD_ARGMAX)
        errors = validate_pair(timing(), timing(lm_argmax=4, lm_argmax_fallbacks=1), 64, spec)
        self.assertIn("candidate reported Q4_0 x Q8_1 LM-head argmax fallbacks: 1", errors)

    def test_accepts_generated_q6_lm_head_candidate_with_exact_tokens_and_route(self):
        spec = candidate_spec(CandidateKind.Q6_K_Q8_1_LM_HEAD_ARGMAX)
        self.assertEqual([], validate_pair(timing(), timing(generated_q6_lm_argmax=64), 64, spec))

    def test_rejects_generated_q6_lm_head_baseline_hit_missing_route_and_fallback(self):
        spec = candidate_spec(CandidateKind.Q6_K_Q8_1_LM_HEAD_ARGMAX)
        errors = validate_pair(
            timing(generated_q6_lm_argmax=1),
            timing(generated_q6_lm_argmax_fallbacks=1),
            64,
            spec,
        )
        self.assertIn("baseline unexpectedly used generated Q6_K x Q8_1 LM-head argmax", errors)
        self.assertIn("candidate did not use generated Q6_K x Q8_1 LM-head argmax", errors)
        self.assertIn("candidate reported generated Q6_K x Q8_1 LM-head argmax fallbacks: 1", errors)

    def test_accepts_e2b_ffn_candidate_when_both_routes_are_covered(self):
        spec = candidate_spec(CandidateKind.Q4_0_Q8_1_E2B_FFN)
        self.assertEqual([], validate_pair(timing(), timing(ffn_pair=32, ffn_down=16), 64, spec))

    def test_rejects_e2b_ffn_baseline_hits_independently(self):
        spec = candidate_spec(CandidateKind.Q4_0_Q8_1_E2B_FFN)
        errors = validate_pair(
            timing(ffn_pair=1, ffn_down=2),
            timing(ffn_pair=32, ffn_down=16),
            64,
            spec,
        )
        self.assertIn("baseline unexpectedly used Q4_0 x Q8_1 generated E2B FFN pair route", errors)
        self.assertIn("baseline unexpectedly used Q4_0 x Q8_1 generated E2B FFN down route", errors)

    def test_rejects_e2b_ffn_partial_candidate_coverage(self):
        spec = candidate_spec(CandidateKind.Q4_0_Q8_1_E2B_FFN)
        pair_missing = validate_pair(timing(), timing(ffn_down=16), 64, spec)
        down_missing = validate_pair(timing(), timing(ffn_pair=32), 64, spec)
        self.assertIn("candidate did not use Q4_0 x Q8_1 generated E2B FFN pair route", pair_missing)
        self.assertNotIn("candidate did not use Q4_0 x Q8_1 generated E2B FFN down route", pair_missing)
        self.assertIn("candidate did not use Q4_0 x Q8_1 generated E2B FFN down route", down_missing)
        self.assertNotIn("candidate did not use Q4_0 x Q8_1 generated E2B FFN pair route", down_missing)

    def test_rejects_each_e2b_ffn_candidate_fallback(self):
        spec = candidate_spec(CandidateKind.Q4_0_Q8_1_E2B_FFN)
        errors = validate_pair(
            timing(),
            timing(ffn_pair=32, ffn_down=16, ffn_pair_fallbacks=2, ffn_down_fallbacks=3),
            64,
            spec,
        )
        self.assertIn("candidate reported Q4_0 x Q8_1 generated E2B FFN pair fallbacks: 2", errors)
        self.assertIn("candidate reported Q4_0 x Q8_1 generated E2B FFN down fallbacks: 3", errors)

    def test_pair_only_e2b_ffn_requires_hits_without_fallback_or_coupled_routes(self):
        spec = candidate_spec(CandidateKind.Q4_0_Q8_1_E2B_FFN_PAIR_ONLY)
        self.assertEqual([], validate_pair(timing(), timing(ffn_pair_only=32), 64, spec))

        errors = validate_pair(
            timing(),
            timing(ffn_pair_only=32, ffn_pair_only_fallbacks=1, ffn_down=1),
            64,
            spec,
        )
        self.assertIn("candidate reported generated E2B FFN pair-only fallbacks: 1", errors)
        self.assertIn("candidate reported generated E2B FFN down route: 1", errors)

    def test_accepts_exact_e2b_ffn_candidate_when_both_routes_are_covered(self):
        spec = candidate_spec(CandidateKind.Q4_0_E2B_FFN_EXACT)
        self.assertEqual([], validate_pair(timing(), timing(exact_ffn_pair=32, exact_ffn_down=16), 64, spec))

    def test_rejects_exact_e2b_ffn_baseline_hits_independently(self):
        spec = candidate_spec(CandidateKind.Q4_0_E2B_FFN_EXACT)
        errors = validate_pair(
            timing(exact_ffn_pair=1, exact_ffn_down=2),
            timing(exact_ffn_pair=32, exact_ffn_down=16),
            64,
            spec,
        )
        self.assertIn("baseline unexpectedly used exact F32 generated E2B FFN pair route", errors)
        self.assertIn("baseline unexpectedly used exact F32 generated E2B FFN down route", errors)

    def test_rejects_exact_e2b_ffn_partial_candidate_coverage(self):
        spec = candidate_spec(CandidateKind.Q4_0_E2B_FFN_EXACT)
        pair_missing = validate_pair(timing(), timing(exact_ffn_down=16), 64, spec)
        down_missing = validate_pair(timing(), timing(exact_ffn_pair=32), 64, spec)
        self.assertIn("candidate did not use exact F32 generated E2B FFN pair route", pair_missing)
        self.assertNotIn("candidate did not use exact F32 generated E2B FFN down route", pair_missing)
        self.assertIn("candidate did not use exact F32 generated E2B FFN down route", down_missing)
        self.assertNotIn("candidate did not use exact F32 generated E2B FFN pair route", down_missing)

    def test_rejects_each_exact_e2b_ffn_candidate_fallback(self):
        spec = candidate_spec(CandidateKind.Q4_0_E2B_FFN_EXACT)
        errors = validate_pair(
            timing(),
            timing(
                exact_ffn_pair=32,
                exact_ffn_down=16,
                exact_ffn_pair_fallbacks=2,
                exact_ffn_down_fallbacks=3,
            ),
            64,
            spec,
        )
        self.assertIn("candidate reported exact F32 generated E2B FFN pair fallbacks: 2", errors)
        self.assertIn("candidate reported exact F32 generated E2B FFN down fallbacks: 3", errors)

    def test_candidate_spec_requires_unique_route_counters(self):
        with self.assertRaisesRegex(ValueError, "at least one route counter"):
            CandidateSpec("cuda.test", "TEST_GATE", ())
        duplicate = RouteCounter("duplicate", "duplicate route")
        with self.assertRaisesRegex(ValueError, "must be unique"):
            CandidateSpec("cuda.test", "TEST_GATE", (duplicate, duplicate))
        with self.assertRaisesRegex(ValueError, "forbidden counters must be unique"):
            CandidateSpec(
                "cuda.test",
                "TEST_GATE",
                (RouteCounter("required", "required"),),
                (duplicate, duplicate),
            )
        with self.assertRaisesRegex(ValueError, "must be disjoint"):
            CandidateSpec("cuda.test", "TEST_GATE", (duplicate,), (duplicate,))
        with self.assertRaisesRegex(ValueError, "kernel ID"):
            CandidateSpec("Gemma 4 E4B", "TEST_GATE", (duplicate,))
        with self.assertRaisesRegex(ValueError, "environment variable"):
            CandidateSpec("cuda.test", "bad-env", (duplicate,))
        with self.assertRaisesRegex(ValueError, "must differ"):
            CandidateSpec(
                "cuda.test",
                "TEST_GATE",
                (duplicate,),
                baseline_gate_value="same",
                candidate_gate_value="same",
            )
        with self.assertRaisesRegex(ValueError, "must be non-empty"):
            CandidateSpec(
                "cuda.test",
                "TEST_GATE",
                (duplicate,),
                baseline_gate_value="",
            )
        with self.assertRaisesRegex(ValueError, "control character"):
            CandidateSpec(
                "cuda.test",
                "TEST_GATE",
                (duplicate,),
                candidate_gate_value="unsafe\nvalue",
            )
        with self.assertRaisesRegex(ValueError, "route phase"):
            CandidateSpec(
                "cuda.test",
                "TEST_GATE",
                (duplicate,),
                route_phase="load",
            )

    def test_candidate_metadata_preserves_primary_counter_and_lists_all(self):
        attention = candidate_metadata(candidate_spec(CandidateKind.GENERATED_ATTENTION))
        self.assertEqual(GENERATED_ATTENTION_KERNEL_ID, attention["kernel_id"])
        self.assertEqual(attention["kernel_id"], attention["catalog_id"])
        self.assertEqual("launch_attention_gqa_decode_generated", attention["route_counter"])
        self.assertEqual([attention["route_counter"]], attention["route_counters"])
        self.assertEqual(attention["route_counters"], attention["required_route_counters"])
        self.assertEqual([], attention["candidate_forbidden_counters"])
        self.assertEqual([], attention["forbidden_route_counters"])
        self.assertEqual("0", attention["baseline_gate_value"])
        self.assertEqual("1", attention["candidate_gate_value"])
        self.assertEqual("decode", attention["route_phase"])

        e2b_ffn = candidate_metadata(candidate_spec(CandidateKind.Q4_0_Q8_1_E2B_FFN))
        self.assertEqual("q4_0_generated_e2b_pair_q8_hits", e2b_ffn["route_counter"])
        self.assertEqual(
            ["q4_0_generated_e2b_pair_q8_hits", "q4_0_generated_e2b_down_q8_hits"],
            e2b_ffn["route_counters"],
        )
        self.assertEqual(
            ["q4_0_generated_e2b_pair_q8_fallbacks", "q4_0_generated_e2b_down_q8_fallbacks"],
            e2b_ffn["candidate_forbidden_counters"],
        )

        exact_e2b_ffn = candidate_metadata(candidate_spec(CandidateKind.Q4_0_E2B_FFN_EXACT))
        self.assertEqual("q4_0_generated_e2b_exact_pair_f32_hits", exact_e2b_ffn["route_counter"])
        self.assertEqual(
            ["q4_0_generated_e2b_exact_pair_f32_hits", "q4_0_generated_e2b_exact_down_f32_hits"],
            exact_e2b_ffn["route_counters"],
        )
        self.assertEqual(
            [
                "q4_0_generated_e2b_exact_pair_f32_fallbacks",
                "q4_0_generated_e2b_exact_down_f32_fallbacks",
            ],
            exact_e2b_ffn["candidate_forbidden_counters"],
        )

        q6_lm_head = candidate_metadata(candidate_spec(CandidateKind.Q6_K_Q8_1_LM_HEAD_ARGMAX))
        self.assertEqual("lm_head_argmax_generated_q6_k_q8_1_hits", q6_lm_head["route_counter"])
        self.assertTrue(q6_lm_head["requires_explicit_model"])
        self.assertEqual(
            ["lm_head_argmax_generated_q6_k_q8_1_fallbacks"],
            q6_lm_head["candidate_forbidden_counters"],
        )

    def test_candidate_environment_only_changes_selected_route(self):
        attention = CANDIDATE_SPECS[CandidateKind.GENERATED_ATTENTION]
        lm_argmax = CANDIDATE_SPECS[CandidateKind.Q4_0_Q8_1_LM_HEAD_ARGMAX]
        q6_lm_argmax = CANDIDATE_SPECS[CandidateKind.Q6_K_Q8_1_LM_HEAD_ARGMAX]
        e2b_ffn = CANDIDATE_SPECS[CandidateKind.Q4_0_Q8_1_E2B_FFN]
        exact_e2b_ffn = CANDIDATE_SPECS[CandidateKind.Q4_0_E2B_FFN_EXACT]
        env = {
            attention.environment_variable: "inherited-attention",
            lm_argmax.environment_variable: "inherited-lm",
            q6_lm_argmax.environment_variable: "inherited-q6-lm",
            e2b_ffn.environment_variable: "inherited-ffn",
            exact_e2b_ffn.environment_variable: "inherited-exact-ffn",
        }
        overrides = (("ANTFLY_TEST_OVERRIDE", "candidate-only"),)
        configure_candidate_environment(env, e2b_ffn, False, overrides)
        self.assertEqual("inherited-attention", env[attention.environment_variable])
        self.assertEqual("inherited-lm", env[lm_argmax.environment_variable])
        self.assertEqual("inherited-q6-lm", env[q6_lm_argmax.environment_variable])
        self.assertEqual("inherited-exact-ffn", env[exact_e2b_ffn.environment_variable])
        self.assertEqual("0", env[e2b_ffn.environment_variable])
        self.assertNotIn("ANTFLY_TEST_OVERRIDE", env)
        configure_candidate_environment(env, e2b_ffn, True, overrides)
        self.assertEqual("1", env[e2b_ffn.environment_variable])
        self.assertEqual("candidate-only", env["ANTFLY_TEST_OVERRIDE"])

    def test_exact_e2b_ffn_environment_only_changes_its_gate(self):
        approximate = CANDIDATE_SPECS[CandidateKind.Q4_0_Q8_1_E2B_FFN]
        exact = CANDIDATE_SPECS[CandidateKind.Q4_0_E2B_FFN_EXACT]
        env = {
            approximate.environment_variable: "inherited-approximate",
            exact.environment_variable: "inherited-exact",
        }
        configure_candidate_environment(env, exact, False)
        self.assertEqual("inherited-approximate", env[approximate.environment_variable])
        self.assertEqual("0", env[exact.environment_variable])
        configure_candidate_environment(env, exact, True)
        self.assertEqual("inherited-approximate", env[approximate.environment_variable])
        self.assertEqual("1", env[exact.environment_variable])

    def test_common_environment_applies_to_baseline_and_candidate(self):
        attention = CANDIDATE_SPECS[CandidateKind.GENERATED_ATTENTION]
        common = (("ANTFLY_TEST_COMMON", "shared"),)
        candidate_only = (("ANTFLY_TEST_CANDIDATE", "candidate"),)
        baseline = {}
        candidate = {}
        configure_candidate_environment(baseline, attention, False, candidate_only, common)
        configure_candidate_environment(candidate, attention, True, candidate_only, common)
        self.assertEqual("shared", baseline["ANTFLY_TEST_COMMON"])
        self.assertEqual("shared", candidate["ANTFLY_TEST_COMMON"])
        self.assertNotIn("ANTFLY_TEST_CANDIDATE", baseline)
        self.assertEqual("candidate", candidate["ANTFLY_TEST_CANDIDATE"])
        self.assertEqual("0", baseline[attention.environment_variable])
        self.assertEqual("1", candidate[attention.environment_variable])

    def test_typed_candidate_environment_uses_catalog_gate_values(self):
        spec = CANDIDATE_CATALOG[GQA_PREFILL_TILED_F16_EXACT_KERNEL_ID]
        baseline = {}
        candidate = {}
        configure_candidate_environment(baseline, spec, False)
        configure_candidate_environment(candidate, spec, True)
        self.assertEqual("required-fast", baseline[spec.environment_variable])
        self.assertEqual("required-tiled-f16-exact", candidate[spec.environment_variable])

        metadata = candidate_environment_metadata(spec, ())
        self.assertEqual(
            {spec.environment_variable: "required-fast"},
            metadata["baseline_gate"],
        )
        self.assertEqual(
            {spec.environment_variable: "required-tiled-f16-exact"},
            metadata["candidate_gate"],
        )

    def test_run_case_applies_common_and_candidate_environment_with_explicit_capture_capacity(self):
        attention = candidate_spec(CandidateKind.GENERATED_ATTENTION)
        overrides = ((GENERATED_ATTENTION_SPLIT_KV_MIN_TOKENS_ENV, "128"),)
        common = (("ANTFLY_INFERENCE_CUDA_ASYNC_I32_DOWNLOAD_STAGING", "0"),)
        seen_environments = []

        with tempfile.TemporaryDirectory() as temp_dir:
            args = type(
                "Args",
                (),
                {
                    "output_dir": pathlib.Path(temp_dir),
                    "wrapper": pathlib.Path("/test-wrapper"),
                    "binary": pathlib.Path("/test-binary"),
                    "model": pathlib.Path("/test-model.gguf"),
                    "timeout_sec": 1,
                },
            )()

            def fake_run(command, **kwargs):
                seen_environments.append(kwargs["env"])
                timing_path = pathlib.Path(command[command.index("--json-timing") + 1])
                timing_path.write_text(json.dumps(timing(generated=1)), encoding="utf-8")
                return subprocess.CompletedProcess(
                    command,
                    0,
                    "prompt_token_ids: 10 11 12\ntoken_ids: 0 1 2\n",
                )

            with mock.patch.dict(os.environ, {}, clear=True), mock.patch(
                "validate_gemma4_cuda_candidate.subprocess.run",
                side_effect=fake_run,
            ):
                baseline = run_case(args, "prompt", 64, False, "baseline", attention, overrides, 4096, common)
                candidate = run_case(args, "prompt", 64, True, "candidate", attention, overrides, 4096, common)

        self.assertEqual("0", seen_environments[0][attention.environment_variable])
        self.assertNotIn(GENERATED_ATTENTION_SPLIT_KV_MIN_TOKENS_ENV, seen_environments[0])
        self.assertEqual("1", seen_environments[1][attention.environment_variable])
        self.assertEqual("128", seen_environments[1][GENERATED_ATTENTION_SPLIT_KV_MIN_TOKENS_ENV])
        self.assertEqual("0", seen_environments[0]["ANTFLY_INFERENCE_CUDA_ASYNC_I32_DOWNLOAD_STAGING"])
        self.assertEqual("0", seen_environments[1]["ANTFLY_INFERENCE_CUDA_ASYNC_I32_DOWNLOAD_STAGING"])
        self.assertEqual("4096", seen_environments[0]["ANTFLY_CAPTURE_FORCE_KV_CAPACITY"])
        self.assertEqual("4096", seen_environments[1]["ANTFLY_CAPTURE_FORCE_KV_CAPACITY"])
        self.assertEqual([10, 11, 12], baseline["prompt_token_ids"])
        self.assertEqual([10, 11, 12], candidate["prompt_token_ids"])

    def test_relative_launch_paths_are_absolute_before_repo_root_subprocess(self):
        attention = candidate_spec(CandidateKind.GENERATED_ATTENTION)
        seen_commands = []
        with tempfile.TemporaryDirectory() as temp_dir:
            invocation_dir = pathlib.Path(temp_dir) / "invocation"
            invocation_dir.mkdir()
            output_dir = invocation_dir / "output"
            old_cwd = pathlib.Path.cwd()
            try:
                os.chdir(invocation_dir)
                with mock.patch.object(
                    sys,
                    "argv",
                    [
                        "validate_gemma4_cuda_candidate.py",
                        "--binary",
                        "zig-out/bin/antfly-inference",
                        "--wrapper",
                        "scripts/gemma4/with_gemma4_qat_cuda_tuning.sh",
                        "--model",
                        "models/e2b.gguf",
                        "--output-dir",
                        str(output_dir),
                    ],
                ):
                    args = parse_args()
            finally:
                os.chdir(old_cwd)

            self.assertEqual((invocation_dir / "zig-out/bin/antfly-inference").resolve(), args.binary)
            self.assertEqual(
                (invocation_dir / "scripts/gemma4/with_gemma4_qat_cuda_tuning.sh").resolve(),
                args.wrapper,
            )
            self.assertEqual((invocation_dir / "models/e2b.gguf").resolve(), args.model)
            output_dir.mkdir()

            def fake_run(command, **_kwargs):
                seen_commands.append(command)
                timing_path = pathlib.Path(command[command.index("--json-timing") + 1])
                timing_path.write_text(json.dumps(timing(generated=1)), encoding="utf-8")
                return subprocess.CompletedProcess(command, 0, "token_ids: 0 1 2\n")

            with mock.patch(
                "validate_gemma4_cuda_candidate.subprocess.run",
                side_effect=fake_run,
            ):
                run_case(args, "prompt", 64, False, "relative", attention, (), 4096)

        command = seen_commands[0]
        self.assertEqual(str(args.wrapper), command[0])
        self.assertEqual(str(args.binary), command[1])
        self.assertEqual(str(args.model), command[3])
        self.assertEqual("f32", command[command.index("--cache-dtype") + 1])
        self.assertEqual("32", command[command.index("--prefill-chunk-size") + 1])
        self.assertTrue(pathlib.Path(command[0]).is_absolute())
        self.assertTrue(pathlib.Path(command[1]).is_absolute())
        self.assertTrue(pathlib.Path(command[3]).is_absolute())

    def test_exact_e2b_ffn_run_case_applies_f32_locks_to_both_arms(self):
        spec = candidate_spec(CandidateKind.Q4_0_E2B_FFN_EXACT)
        seen_environments = []
        with tempfile.TemporaryDirectory() as temp_dir:
            args = type(
                "Args",
                (),
                {
                    "output_dir": pathlib.Path(temp_dir),
                    "wrapper": pathlib.Path("/test-wrapper"),
                    "binary": pathlib.Path("/test-binary"),
                    "model": pathlib.Path("/test-model.gguf"),
                    "timeout_sec": 1,
                },
            )()

            def fake_run(command, **kwargs):
                seen_environments.append(kwargs["env"])
                timing_path = pathlib.Path(command[command.index("--json-timing") + 1])
                timing_path.write_text(json.dumps(timing(exact_ffn_pair=1, exact_ffn_down=1)), encoding="utf-8")
                return subprocess.CompletedProcess(command, 0, "token_ids: 0 1 2\n")

            with mock.patch.dict(os.environ, {}, clear=True), mock.patch(
                "validate_gemma4_cuda_candidate.subprocess.run",
                side_effect=fake_run,
            ):
                run_case(
                    args,
                    "prompt",
                    64,
                    False,
                    "baseline",
                    spec,
                    (),
                    4096,
                    EXACT_E2B_FFN_F32_COMPARISON_ENVIRONMENT,
                )
                run_case(
                    args,
                    "prompt",
                    64,
                    True,
                    "candidate",
                    spec,
                    (),
                    4096,
                    EXACT_E2B_FFN_F32_COMPARISON_ENVIRONMENT,
                )

        for name, value in EXACT_E2B_FFN_F32_COMPARISON_ENVIRONMENT:
            self.assertEqual(value, seen_environments[0][name])
            self.assertEqual(value, seen_environments[1][name])
        self.assertEqual("0", seen_environments[0][spec.environment_variable])
        self.assertEqual("1", seen_environments[1][spec.environment_variable])

    def test_uncatalogued_e4b_kernel_uses_explicit_route_contract(self):
        with mock.patch.object(
            sys,
            "argv",
            [
                "validate_gemma4_cuda_candidate.py",
                "--kernel-id",
                "cuda.quant.q4_0-q8_1.ffn.e4b-12b",
                "--candidate-environment-variable",
                "ANTFLY_TEST_E4B_FFN",
                "--required-route-counters",
                "q4_0_generated_pair_q8_hits=E4B pair,q4_0_generated_down_q8_hits=E4B down",
                "--forbidden-route-counter",
                "q4_0_generated_pair_q8_fallbacks=E4B pair fallback",
            ],
        ):
            spec = resolve_candidate_spec(parse_args())
        self.assertIsNone(spec.legacy_kind)
        self.assertEqual("cuda.quant.q4_0-q8_1.ffn.e4b-12b", spec.kernel_id)
        self.assertEqual("ANTFLY_TEST_E4B_FFN", spec.environment_variable)
        self.assertEqual(
            ["q4_0_generated_pair_q8_hits", "q4_0_generated_down_q8_hits"],
            [counter.name for counter in spec.required_route_counters],
        )
        self.assertEqual("E4B pair", spec.required_route_counters[0].label)
        self.assertEqual(
            ["q4_0_generated_pair_q8_fallbacks"],
            [counter.name for counter in spec.forbidden_route_counters],
        )

    def test_uncatalogued_kernel_requires_gate_and_required_counter(self):
        with mock.patch.object(
            sys,
            "argv",
            ["validate_gemma4_cuda_candidate.py", "--kernel-id", "cuda.custom"],
        ):
            with self.assertRaisesRegex(ValueError, "environment-variable"):
                resolve_candidate_spec(parse_args())

        with mock.patch.object(
            sys,
            "argv",
            [
                "validate_gemma4_cuda_candidate.py",
                "--kernel-id",
                "cuda.custom",
                "--candidate-environment-variable",
                "ANTFLY_CUSTOM",
            ],
        ):
            with self.assertRaisesRegex(ValueError, "required-route-counter"):
                resolve_candidate_spec(parse_args())

    def test_route_counter_list_supports_repeated_and_comma_separated_values(self):
        counters = parse_route_counter_list(["first=First route,second", "third=Third route"])
        self.assertEqual(["first", "second", "third"], [counter.name for counter in counters])
        self.assertEqual(["First route", "second", "Third route"], [counter.label for counter in counters])

    def test_generic_timing_metadata_controls_counter_group_and_throughput_field(self):
        metadata = TimingMetadata(
            counter_group="device.routes",
            throughput_field="performance.generated_tokens_per_second",
            throughput_unit="generated_tokens_per_second",
            require_persistent_replay=False,
        )
        spec = CandidateSpec(
            kernel_id="cuda.quant.e4b-12b.test",
            environment_variable="ANTFLY_TEST_E4B",
            required_route_counters=(RouteCounter("candidate_hits", "candidate route"),),
            forbidden_route_counters=(RouteCounter("candidate_fallbacks", "candidate fallback"),),
        )

        baseline = {
            "token_ids": list(range(64)),
            "performance": {"generated_tokens_per_second": 80.0},
            "device": {"routes": {"candidate_hits": 0, "candidate_fallbacks": 0}},
        }
        candidate = {
            "token_ids": list(range(64)),
            "performance": {"generated_tokens_per_second": 100.0},
            "device": {"routes": {"candidate_hits": 64, "candidate_fallbacks": 0}},
        }
        self.assertEqual([], validate_pair(baseline, candidate, 64, spec, metadata))
        self.assertEqual(64, timing_counter(candidate, "candidate_hits", metadata))
        self.assertEqual(100.0, timing_throughput(candidate, metadata))
        self.assertEqual(1.25, paired_throughput(baseline, candidate, metadata)["candidate_ratio"])
        self.assertEqual(
            {
                "counter_group": "device.routes",
                "throughput_field": "performance.generated_tokens_per_second",
                "throughput_unit": "generated_tokens_per_second",
                "require_persistent_replay": False,
                "persistent_replay_counter": "graph_capture_persistent_replays",
                "graph_discard_counter": "graph_capture_discards",
                "graph_capacity_skip_counter": "graph_capture_capacity_skips",
            },
            timing_metadata(metadata),
        )

    def test_execution_order_alternates_by_repetition(self):
        self.assertEqual((False, True), execution_order(0))
        self.assertEqual((True, False), execution_order(1))
        self.assertEqual((False, True), execution_order(2))

    def test_locked_fixture_requires_exact_prompt_token_count_and_parity(self):
        baseline = timing(generated=0)
        candidate = timing(generated=7)
        self.assertEqual(
            [],
            validate_pair(baseline, candidate, 64, expected_prompt_tokens=3),
        )
        candidate["prompt_token_ids"] = [1, 2]
        errors = validate_pair(baseline, candidate, 64, expected_prompt_tokens=3)
        self.assertIn("baseline and candidate prompt token IDs differ", errors)
        self.assertTrue(any("expected 3 prompt tokens" in error for error in errors))

        candidate.pop("prompt_token_ids")
        errors = validate_pair(baseline, candidate, 64, expected_prompt_tokens=3)
        self.assertIn("missing prompt token IDs for the locked prompt fixture", errors)

    def test_phase_metrics_and_paired_log_ratio_summary(self):
        metrics = timing_latency_metrics(timing(prefill_ms=20.0, decode_ms=80.0, total_ms=100.0))
        self.assertTrue(metrics["available"])
        self.assertEqual(100.0, metrics["total_latency_ms"])
        self.assertEqual(20.0, metrics["ttft_ms"])
        self.assertEqual("timing_ms.prefill_inner_proxy", metrics["sources"]["ttft_ms"])

        pairs = []
        for repetition in range(1, 4):
            latency = paired_latency(
                timing(prefill_ms=20.0, decode_ms=80.0, total_ms=100.0),
                timing(prefill_ms=20.0, decode_ms=72.0, total_ms=92.0),
            )
            pairs.append({"repetition": repetition, "latency": latency})
        summary = summarize_latency_pairs(pairs, bootstrap_samples=500, bootstrap_seed=7)
        self.assertTrue(summary["available"])
        self.assertAlmostEqual(
            0.9,
            summary["metrics"]["decode_ms"]["candidate_baseline_paired_log_ratio_95_ci"]["median"],
        )

        args = argparse.Namespace(
            require_phase_metrics=True,
            max_total_latency_ratio=0.95,
            max_ttft_ratio=1.01,
            max_decode_latency_ratio=0.95,
            max_total_ci_upper=1.0,
            max_ttft_ci_upper=1.02,
            max_decode_ci_upper=1.0,
            max_cv=0.02,
        )
        self.assertEqual([], latency_promotion_errors(summary, args))

    def test_prefill_promotion_requires_ttft_and_total_gain_but_not_decode_gain(self):
        pairs = []
        for repetition in range(1, 11):
            pairs.append({
                "repetition": repetition,
                "latency": paired_latency(
                    timing(prefill_ms=20.0, decode_ms=80.0, total_ms=100.0),
                    timing(prefill_ms=17.0, decode_ms=80.8, total_ms=97.8),
                ),
            })
        summary = summarize_latency_pairs(pairs, bootstrap_samples=500, bootstrap_seed=9)
        profile = QUALIFICATION_PROFILES["prefill-promotion"]
        args = argparse.Namespace(
            require_phase_metrics=True,
            max_total_latency_ratio=profile["max_total_latency_ratio"],
            max_ttft_ratio=profile["max_ttft_ratio"],
            max_decode_latency_ratio=profile["max_decode_latency_ratio"],
            max_total_ci_upper=profile["max_total_ci_upper"],
            max_ttft_ci_upper=profile["max_ttft_ci_upper"],
            max_decode_ci_upper=profile["max_decode_ci_upper"],
            max_cv=profile["max_cv"],
        )
        self.assertEqual([], latency_promotion_errors(summary, args))

        regressed_pairs = []
        for repetition in range(1, 11):
            regressed_pairs.append({
                "repetition": repetition,
                "latency": paired_latency(
                    timing(prefill_ms=20.0, decode_ms=80.0, total_ms=100.0),
                    timing(prefill_ms=14.0, decode_ms=82.4, total_ms=96.4),
                ),
            })
        errors = latency_promotion_errors(
            summarize_latency_pairs(regressed_pairs, bootstrap_samples=500, bootstrap_seed=9),
            args,
        )
        self.assertTrue(any(error.startswith("decode_ms paired median ratio") for error in errors))

    def test_full_route_coverage_and_attestation_are_explicit(self):
        baseline = timing(generated=0, tokens=64)
        candidate = timing(generated=7, tokens=64)
        self.assertEqual([], validate_pair(baseline, candidate, 64))
        self.assertEqual([], validate_pair(
            baseline,
            candidate,
            64,
            require_full_route_coverage=True,
        ))
        attestation = pair_attestation(baseline, candidate, 64)
        route = attestation["required_routes"]["launch_attention_gqa_decode_generated"]
        self.assertEqual(7, route["candidate"])
        self.assertTrue(route["observed_during_candidate_graph_construction"])
        self.assertTrue(route["stable_replay_coverage"])
        self.assertTrue(route["route_replay_attested"])
        self.assertTrue(attestation["stable_route_replay_attested"])
        self.assertTrue(attestation["all_required_routes_attested"])
        self.assertEqual(60, attestation["graph"]["candidate"]["persistent_replays"])

        no_replay_metadata = TimingMetadata(require_persistent_replay=False)
        errors = validate_pair(
            baseline,
            candidate,
            64,
            timing_info=no_replay_metadata,
            require_full_route_coverage=True,
        )
        self.assertIn("full route coverage requires persistent replay validation", errors)

    def test_prefill_route_attestation_does_not_claim_decode_graph_construction(self):
        spec = CANDIDATE_CATALOG[GQA_PREFILL_TILED_F16_EXACT_KERNEL_ID]
        attestation = pair_attestation(
            timing(),
            timing(gqa_prefill_exact_hd256=18, gqa_prefill_exact_hd512=9),
            64,
            spec,
        )
        for route in attestation["required_routes"].values():
            self.assertEqual("prefill", route["phase"])
            self.assertTrue(route["observed_in_candidate_phase"])
            self.assertFalse(route["observed_during_candidate_graph_construction"])
            self.assertTrue(route["route_attested"])
            self.assertFalse(route["route_replay_attested"])
        self.assertTrue(attestation["all_required_routes_attested"])
        self.assertFalse(attestation["stable_route_replay_attested"])

    def test_single_repeat_preserves_filenames_and_repeats_are_distinct(self):
        self.assertEqual("case", repetition_stem("case", 0, 1))
        self.assertEqual("case-r01", repetition_stem("case", 0, 3))
        self.assertEqual("case-r02", repetition_stem("case", 1, 3))

    def test_reports_per_pair_and_median_throughput(self):
        first = paired_throughput(timing(tok_s=100.0), timing(tok_s=125.0))
        second = paired_throughput(timing(tok_s=80.0), timing(tok_s=88.0))
        self.assertEqual(1.25, first["candidate_ratio"])
        self.assertEqual(25.0, first["delta_percent"])
        summary = summarize_throughput([first, second])
        self.assertEqual(90.0, summary["baseline_median_tok_s"])
        self.assertEqual(106.5, summary["candidate_median_tok_s"])
        self.assertAlmostEqual(1.175, summary["median_candidate_ratio"])
        self.assertEqual(1.1, summary["min_candidate_ratio"])
        self.assertEqual(2, summary["pair_count"])

    def test_repeated_case_reports_medians_min_ratio_and_cvs(self):
        pairs = []
        for repetition, (baseline_tps, candidate_tps) in enumerate(
            ((100.0, 110.0), (110.0, 121.0), (90.0, 99.0)),
            start=1,
        ):
            throughput = paired_throughput(timing(tok_s=baseline_tps), timing(tok_s=candidate_tps))
            pairs.append(
                {
                    "repetition": repetition,
                    "execution_order": ["baseline", "candidate"],
                    **throughput,
                    "paired_throughput": throughput,
                    "token_ids_equal": True,
                    "errors": [],
                }
            )

        case = summarize_case("prompt", 64, pairs)
        self.assertEqual(100.0, case["baseline_tok_s"])
        self.assertEqual(110.0, case["candidate_tok_s"])
        self.assertAlmostEqual(1.1, case["candidate_ratio"])
        self.assertAlmostEqual(1.1, case["min_candidate_ratio"])
        self.assertAlmostEqual(coefficient_of_variation([100.0, 110.0, 90.0]), case["baseline_tok_s_cv"])
        self.assertAlmostEqual(coefficient_of_variation([110.0, 121.0, 99.0]), case["candidate_tok_s_cv"])
        self.assertEqual(3, case["repeats"])
        self.assertTrue(case["token_ids_equal"])

        aggregate = summarize_throughput([case])
        self.assertEqual(3, aggregate["pair_count"])
        self.assertAlmostEqual(1.1, aggregate["median_candidate_ratio"])

    def test_repeated_case_retains_each_pair_validation_error(self):
        throughput = paired_throughput(timing(tok_s=100.0), timing(tok_s=100.0))
        pairs = [
            {
                "repetition": 1,
                **throughput,
                "paired_throughput": throughput,
                "token_ids_equal": True,
                "errors": ["first failure"],
            },
            {
                "repetition": 2,
                **throughput,
                "paired_throughput": throughput,
                "token_ids_equal": False,
                "errors": ["second failure"],
            },
        ]
        case = summarize_case("prompt", 64, pairs)
        self.assertEqual(["repeat 1: first failure", "repeat 2: second failure"], case["errors"])
        self.assertFalse(case["token_ids_equal"])

    def test_promotion_gates_use_min_paired_ratio_and_both_cvs(self):
        case = {
            "min_candidate_ratio": 1.01,
            "baseline_tok_s_cv": 0.01,
            "candidate_tok_s_cv": 0.02,
        }
        self.assertEqual([], case_promotion_errors(case, 1.0, 0.02))
        self.assertTrue(any("minimum candidate ratio" in error for error in case_promotion_errors(case, 1.02, 0.02)))
        self.assertTrue(any("baseline throughput CV" in error for error in case_promotion_errors(case | {"baseline_tok_s_cv": 0.03}, 1.0, 0.02)))
        self.assertTrue(any("candidate throughput CV" in error for error in case_promotion_errors(case | {"candidate_tok_s_cv": 0.03}, 1.0, 0.02)))

    def test_non_positive_throughput_fails_pair_validation(self):
        candidate = timing(generated=1, tok_s=0.0)
        self.assertIn(
            "candidate reported non-positive decode throughput",
            validate_pair(timing(), candidate, 64),
        )


if __name__ == "__main__":
    unittest.main()
