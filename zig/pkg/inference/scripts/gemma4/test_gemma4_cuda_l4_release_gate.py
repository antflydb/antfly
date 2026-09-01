#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import merge_gemma4_long_e2e_release_summary as merge_long_e2e

from gemma4_cuda_l4_release_gate import (
    CAPTURE_KV_CAPACITY,
    DEFAULT_MIN_COMPARABLE_RATIO,
    E2B_ANTFLY_TOKENS,
    E2B_LLAMA_TOKENS,
    FROZEN_PROFILE,
    GEMMA12B_TOKENS,
    FORBIDDEN_GENERATED_Q4_0_ROUTE_COUNTERS,
    GIT_UNTRACKED_INVENTORY_SCHEMA,
    MAX_TOK_S_CV,
    RELEASE_SCHEMA,
    RELEASE_SCOPE,
    REQUIRED_Q8_PREFILL_ROUTE_COUNTERS,
    TOKEN_IDS_RE,
    benchmark_contract,
    canonical_sha256,
    disabled_candidate_errors,
    diagnostic_mode_errors,
    e2b_contract_errors,
    e2b_pair_contract_errors,
    frozen_profile,
    gemma12b_evidence,
    generation_replay_errors,
    git_provenance,
    git_content_provenance_errors,
    gpu_provenance,
    l4_errors,
    matrix_command,
    matrix_process_errors,
    matrix_timeout_sec,
    parse_token_ids,
    path_provenance,
    provenance_binding,
    provenance_errors,
    release_environment,
    reset_matrix_outputs,
    run_logged,
    toolchain_provenance,
)


def clean_git_content_provenance() -> dict:
    files: list[dict] = []
    return {
        "tracked_diff_returncode": 0,
        "tracked_diff_bytes": 0,
        "tracked_diff_sha256": hashlib.sha256(b"").hexdigest(),
        "tracked_diff_error": None,
        "untracked_inventory_schema": GIT_UNTRACKED_INVENTORY_SCHEMA,
        "untracked_inventory_returncode": 0,
        "untracked_inventory_sha256": canonical_sha256({
            "schema": GIT_UNTRACKED_INVENTORY_SCHEMA,
            "files": files,
        }),
        "untracked_file_count": 0,
        "untracked_files": files,
        "untracked_inventory_error": None,
    }


def matrix(ratio: float = 0.81, passed: bool = True) -> dict:
    return {
        "entries": [{
            "output_tokens": E2B_LLAMA_TOKENS,
            "pair_ok": True,
            "graph_replay_ok": True,
            "comparable_ratio": ratio,
        }],
        "passed": passed,
    }


def pair() -> dict:
    return {
        "ok": True,
        "ok_graph_replay": True,
        "ok_lm_head_argmax": True,
        "comparison": {
            "antfly_tokens": E2B_ANTFLY_TOKENS,
            "llama_tokens": E2B_LLAMA_TOKENS,
            "antfly_cache_dtype": "f32",
            "llama_cache_type_k": "f32",
            "llama_cache_type_v": "f32",
            "antfly_generated_attention_decode": "0",
            "antfly_generated_q6_k_q8_1_lm_head_argmax": "0",
            "antfly_generated_q4_0_e2b_ffn": "0",
            "antfly_generated_q4_0_e2b_ffn_exact": "0",
            "antfly_generated_q4_0_e2b_ffn_pair_only": "0",
            "antfly_q4_0_q8_1_lm_head_argmax": "1",
        },
        "rows": [{
            "antfly_generated_q6_lm_head_argmax": 0,
            "antfly_generated_q6_lm_head_argmax_fallbacks": 0,
            "antfly_generated_q4_0_mmv": 0,
            "antfly_generated_q4_0_mmv_fallbacks": 0,
            "antfly_generated_q4_0_mm": 0,
            "antfly_generated_q4_0_mm_fallbacks": 0,
            "antfly_generated_q4_0_pair": 0,
            "antfly_generated_q4_0_pair_fallbacks": 0,
            "antfly_generated_q4_0_pair_q8": 0,
            "antfly_generated_q4_0_pair_q8_fallbacks": 0,
            "antfly_generated_q4_0_down_q8": 0,
            "antfly_generated_q4_0_down_q8_fallbacks": 0,
            "antfly_q8_1_prefill_linear": 1,
            "antfly_q8_1_prefill_pair": 1,
            "antfly_generated_e2b_pair": 0,
            "antfly_generated_e2b_down": 0,
            "antfly_generated_e2b_pair_fallbacks": 0,
            "antfly_generated_e2b_down_fallbacks": 0,
            "antfly_generated_e2b_pair_only": 0,
            "antfly_generated_e2b_pair_only_fallbacks": 0,
            "antfly_generated_e2b_exact_pair": 0,
            "antfly_generated_e2b_exact_down": 0,
            "antfly_generated_e2b_exact_pair_fallbacks": 0,
            "antfly_generated_e2b_exact_down_fallbacks": 0,
            "antfly_generated_attention": 0,
        }],
    }


def generation_run() -> dict:
    return {
        "returncode": 0,
        "token_ids": list(range(GEMMA12B_TOKENS)),
        "timing_data": {
            "tokens": GEMMA12B_TOKENS,
            "decode_tok_per_s": 42.0,
            "cuda": {
                "graph_capture_persistent_replays": GEMMA12B_TOKENS - 1,
                "graph_capture_discards": 0,
                "graph_capture_capacity_skips": 0,
                "launch_attention_gqa_decode_generated": 0,
                "lm_head_argmax_generated_q6_k_q8_1_hits": 0,
                "lm_head_argmax_generated_q6_k_q8_1_fallbacks": 0,
                "q4_0_generated_e2b_pair_q8_hits": 0,
                "q4_0_generated_e2b_down_q8_hits": 0,
                "q4_0_generated_e2b_pair_q8_fallbacks": 0,
                "q4_0_generated_e2b_down_q8_fallbacks": 0,
                "q4_0_generated_e2b_pair_only_hits": 0,
                "q4_0_generated_e2b_pair_only_fallbacks": 0,
                "q4_0_generated_e2b_exact_pair_f32_hits": 0,
                "q4_0_generated_e2b_exact_down_f32_hits": 0,
                "q4_0_generated_e2b_exact_pair_f32_fallbacks": 0,
                "q4_0_generated_e2b_exact_down_f32_fallbacks": 0,
            },
        },
    }


class L4ReleaseGateTest(unittest.TestCase):
    def test_release_schema_versions_bound_provenance(self) -> None:
        self.assertEqual("antfly.gemma4_cuda_l4_release_gate.v2", RELEASE_SCHEMA)

    def test_release_scope_is_target_only(self) -> None:
        self.assertEqual("target_only", RELEASE_SCOPE)

    def test_cuda_evidence_is_an_explicitly_flagged_e2e_lane(self) -> None:
        repo_root = pathlib.Path(__file__).resolve().parents[5]
        self.assertFalse((repo_root / ".github/workflows/cuda-gemma4-l4.yml").exists())
        test_source = (repo_root / "zig/e2e/inference/test_cuda_gemma4_l4.py").read_text(encoding="utf-8")
        self.assertIn('ENABLE_ENV = "ANTFLY_E2E_CUDA_GEMMA4_L4"', test_source)
        self.assertIn("pytest.mark.cuda_l4", test_source)
        self.assertIn('os.environ.get(ENABLE_ENV) != "1"', test_source)

    def test_e2e_runs_mtp_only_as_non_gating_nightly_diagnostics(self) -> None:
        runner = (pathlib.Path(__file__).resolve().parents[5] / "zig/e2e/inference/run_cuda_gemma4_l4_e2e.sh").read_text(encoding="utf-8")
        start = runner.index("# Nightly-mode MTP is collection-only")
        end = runner.index('python3 - "$evidence_dir/release_summary.json"', start)
        mtp_step = runner[start:end]
        self.assertIn('if [[ "$mode" == "nightly" ]]', mtp_step)
        self.assertIn('|| mtp_status=$?', mtp_step)
        missing_draft = mtp_step[mtp_step.index('if [[ ! -f "$mtp_draft_model" ]]'):mtp_step.index("else")]
        self.assertIn("diagnostic_status=skipped_missing_draft", missing_draft)
        self.assertIn('> "$mtp_dir/mtp_collection_profile.txt"', missing_draft)
        self.assertIn("release_contract=none; experimental diagnostic only", mtp_step)

    def test_cuda_evidence_e2e_uses_local_overridable_caches(self) -> None:
        runner = (pathlib.Path(__file__).resolve().parents[5] / "zig/e2e/inference/run_cuda_gemma4_l4_e2e.sh").read_text(encoding="utf-8")
        self.assertNotIn("GITHUB_ENV", runner)
        self.assertIn('ZIG_LOCAL_CACHE_DIR="${ZIG_LOCAL_CACHE_DIR:-$evidence_dir/zig-local-cache}"', runner)
        self.assertIn('ZIG_GLOBAL_CACHE_DIR="${ZIG_GLOBAL_CACHE_DIR:-$evidence_dir/zig-global-cache}"', runner)

    def test_e2e_requires_out_of_band_long_e2e_lock_sha256(self) -> None:
        runner = (pathlib.Path(__file__).resolve().parents[5] / "zig/e2e/inference/run_cuda_gemma4_l4_e2e.sh").read_text(encoding="utf-8")
        lane = runner[
            runner.index('if [[ "$long_e2e_configured" -eq 3 ]]', runner.index('"${release_args[@]}"')):
            runner.index('if [[ -n "$e4b_qat_model" ]]', runner.index('"${release_args[@]}"'))
        ]
        self.assertIn('long_e2e_values=("$llama_server_bin" "$long_e2e_lock" "$long_e2e_lock_sha256")', runner)
        self.assertIn('[[ "$long_e2e_configured" -ne 0 && "$long_e2e_configured" -ne 3 ]]', runner)
        self.assertIn("LLAMA_SERVER_BIN, LONG_E2E_LOCK, and LONG_E2E_LOCK_SHA256 must be configured together", runner)
        self.assertIn('require_sha256 "LONG_E2E_LOCK_SHA256" "$long_e2e_lock_sha256"', runner)
        self.assertIn('--model "$e2b_model"', lane)
        self.assertIn('--lockfile-sha256 "$long_e2e_lock_sha256"', lane)
        self.assertIn('|| benchmark_rc=$?', lane)
        self.assertIn('merge_gemma4_long_e2e_release_summary.py', lane)

    def test_long_e2e_summary_merge_records_success_and_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            summary_path = root / "release_summary.json"
            evidence_path = root / "evidence.json"
            summary_path.write_text(json.dumps({"passed": True, "errors": []}))
            evidence_bytes = json.dumps({
                "schema": "antfly.gemma4_long_e2e.v1",
                "passed": True,
                "contract": {"profile": "headline"},
                "comparison": {"ratio": 0.9},
            }).encode()
            evidence_path.write_bytes(evidence_bytes)

            merged = merge_long_e2e.merge(summary_path, evidence_path, 0)
            self.assertTrue(merged["passed"])
            self.assertTrue(merged["long_e2e"]["passed"])
            self.assertEqual(hashlib.sha256(evidence_bytes).hexdigest(), merged["long_e2e"]["sha256"])

            summary_path.write_text(json.dumps({"passed": True, "errors": []}))
            merged = merge_long_e2e.merge(summary_path, root / "missing.json", 7)
            self.assertFalse(merged["passed"])
            self.assertFalse(merged["long_e2e"]["passed"])
            self.assertEqual(7, merged["long_e2e"]["benchmark_exit_code"])
            self.assertIsNone(merged["long_e2e"]["sha256"])
            self.assertIn("exit=7", merged["errors"][0])

            summary_path.write_text(json.dumps({"passed": True, "errors": []}))
            evidence_path.write_bytes(evidence_bytes)
            merged = merge_long_e2e.merge(
                summary_path,
                evidence_path,
                0,
                lane_field="e4b_regression",
                lane_label="Gemma 4 E4B warm-server correctness/regression lane",
            )
            self.assertTrue(merged["passed"])
            self.assertTrue(merged["e4b_regression"]["passed"])
            self.assertNotIn("long_e2e", merged)

            summary_path.write_text(json.dumps({"passed": True, "errors": []}))
            evidence_path.write_bytes(b"{not-json")
            merged = merge_long_e2e.merge(summary_path, evidence_path, 0)
            self.assertFalse(merged["passed"])
            self.assertFalse(merged["long_e2e"]["passed"])
            self.assertIsNotNone(merged["long_e2e"]["parse_error"])

    def test_linux_release_publication_compiles_runtime_loaded_accelerators(self) -> None:
        repo = pathlib.Path(__file__).resolve().parents[5]
        release = (repo / ".github/workflows/antfly-release.yml").read_text(encoding="utf-8")
        container = (repo / ".github/workflows/antfly-container.yml").read_text(encoding="utf-8")
        runtime = (repo / "zig/Dockerfile.runtime").read_text(encoding="utf-8")
        archive_builder = (repo / "scripts/packaging/build_zig_release_archive.sh").read_text(encoding="utf-8")
        self.assertFalse((repo / ".github/workflows/cuda-gemma4-l4.yml").exists())
        self.assertNotIn("uses: ./.github/workflows/cuda-gemma4-l4.yml", release)
        self.assertIn("*-linux-*)", archive_builder)
        self.assertIn('-Dcuda="$cuda"', archive_builder)
        self.assertIn('-Dpjrt="$pjrt"', archive_builder)
        self.assertIn("x86_64-linux-gnu", container)
        self.assertIn("aarch64-linux-gnu", container)
        self.assertNotIn("linux-musl", container)
        self.assertNotIn("Dockerfile.runtime-glibc", container)
        self.assertFalse((repo / "zig/Dockerfile.runtime-glibc").exists())
        self.assertIn("antfly-zig-linux-amd64-gnu", release)
        self.assertIn("antfly-zig-linux-arm64-gnu", release)
        self.assertIn("x86_64-linux-musl", release)
        self.assertIn("aarch64-linux-musl", release)
        self.assertIn("artifact_source: github", release)
        self.assertIn("debian:bookworm-slim", runtime)
        self.assertNotIn("alpine:", runtime)
        self.assertIn('arm64) wasmtime_arch="aarch64"', runtime)
        self.assertIn("-linux-c-api.tar.xz", runtime)
        self.assertIn("ldd /antfly", runtime)
        self.assertIn("/antfly --version", runtime)
        publish = release[release.index("  publish-release-assets:"):release.index("  package-cli-artifacts:")]
        self.assertNotIn("cuda-gemma4-release-gate", publish)

    def test_e2e_requires_an_actual_l4_sm89_device(self) -> None:
        runner = (pathlib.Path(__file__).resolve().parents[5] / "zig/e2e/inference/run_cuda_gemma4_l4_e2e.sh").read_text(encoding="utf-8")
        self.assertIn('nvidia-smi --id="$CUDA_VISIBLE_DEVICES" --query-gpu=name,driver_version,compute_cap', runner)
        self.assertIn('"$gpu_info" != *"L4"* || "$gpu_info" != *"8.9"*', runner)
        self.assertIn("cuda_l4 E2E requires an NVIDIA L4 with compute capability 8.9", runner)

    def test_e2e_uses_accepted_release_and_batching_regression_floors(self) -> None:
        runner = (pathlib.Path(__file__).resolve().parents[5] / "zig/e2e/inference/run_cuda_gemma4_l4_e2e.sh").read_text(encoding="utf-8")
        self.assertIn("--enforce-performance --min-comparable-ratio 0.70 --verify-artifacts", runner)
        batching = runner[runner.index('python3 "$inference_dir/scripts/gemma4/benchmark_gemma4_cuda_batching.py"'):runner.index("release_args=(")]
        self.assertIn("--min-c2-speedup 0.40", batching)
        self.assertNotIn('if [[ "$mode" == "nightly" ]]', batching)

    def test_profile_locks_candidate_gates_and_returns_a_copy(self) -> None:
        profile = frozen_profile()
        self.assertEqual("0", profile["ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE"])
        # The score-prework default is the automatic selector, expressed by the
        # variable being absent; the release environment scrub keeps it unset.
        self.assertNotIn("ANTFLY_GENERATED_ATTENTION_SCORE_PREWORK", profile)
        self.assertNotIn("ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK", profile)
        self.assertNotIn("antfly_generated_attention_score_prework", profile)
        # The promoted flash-prefill default is the automatic selector,
        # expressed by the GQA prefill profile being absent; the release
        # environment scrub keeps it unset.  At the 256-token contract the
        # flash query-length policy does not match, so automatic falls back to
        # the fast prefill path -- the frozen behavior.
        self.assertNotIn("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE", profile)
        self.assertNotIn("ANTFLY_GQA_PREFILL_PROFILE", profile)
        self.assertNotIn("antfly_gqa_prefill_profile", profile)
        self.assertEqual("1", profile["ANTFLY_GQA_PREFILL_USE_RUNTIME_DEFAULT"])
        self.assertEqual("0", profile["ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION"])
        self.assertEqual("1", profile["ANTFLY_INFERENCE_CUDA_TEMP_ARENA_AUTOPLAN"])
        self.assertEqual("0", profile["ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD"])
        self.assertEqual("0", profile["ANTFLY_INFERENCE_CUDA_TEMP_SLOT_SKIP"])
        self.assertEqual("0", profile["ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX"])
        self.assertEqual("0", profile["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN"])
        self.assertEqual("0", profile["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_CATALOG_FFN_CANDIDATES"])
        self.assertEqual("0", profile["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT"])
        self.assertEqual("0", profile["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY"])
        self.assertEqual("0", profile["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_MMV"])
        self.assertEqual("0", profile["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_MM"])
        self.assertEqual("0", profile["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_PAIR"])
        self.assertEqual("0", profile["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_PAIR_Q8"])
        self.assertEqual("1", profile["ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_DP4A"])
        self.assertEqual("1", profile["ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_Q8_1_DP4A"])
        self.assertEqual("0", profile["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_DOWN_Q8"])
        self.assertEqual("1", profile["ANTFLY_INFERENCE_CUDA_Q4_0_LM_HEAD_Q8_1_ARGMAX"])
        self.assertEqual("required", profile["ANTFLY_DECODE_GRAPH_REPLAY"])
        self.assertEqual(str(CAPTURE_KV_CAPACITY), profile["ANTFLY_CAPTURE_FORCE_KV_CAPACITY"])
        self.assertEqual("1", profile["ANTFLY_INFERENCE_DISABLE_CONTINUOUS_BATCHING"])
        profile["ANTFLY_DECODE_GRAPH_REPLAY"] = "off"
        self.assertEqual("required", FROZEN_PROFILE["ANTFLY_DECODE_GRAPH_REPLAY"])

    def test_release_environment_discards_inherited_experiment_switches(self) -> None:
        args = argparse.Namespace(
            binary=pathlib.Path("/binary"),
            llama_cpp_bin=pathlib.Path("/llama"),
            e2b_model=pathlib.Path("/model.gguf"),
            timeout_sec=123,
        )
        with mock.patch.dict(os.environ, {
            "CUDA_VISIBLE_DEVICES": "0",
            "ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX": "1",
            "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT": "1",
            "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY": "1",
            "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK": "1",
            "ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE": "required-flash-f16-sm89",
            "ANTFLY_EXPERIMENTAL_SWITCH": "1",
            "REQUIRE_LM_HEAD_ARGMAX": "0",
            "LLAMA_CACHE_TYPE_K": "q8_0",
        }, clear=True):
            environment = release_environment(args)
        self.assertEqual("0", environment["ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX"])
        # An inherited explicit score-prework override must not survive: the
        # release configuration is the automatic default (variable unset).
        self.assertNotIn("ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK", environment)
        # Likewise for the GQA prefill profile: the promoted flash-prefill
        # default is the automatic selector, so an inherited explicit profile
        # must be scrubbed rather than inherited into the release run.
        self.assertNotIn("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE", environment)
        self.assertEqual("1", environment["ANTFLY_GQA_PREFILL_USE_RUNTIME_DEFAULT"])
        self.assertEqual("0", environment["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT"])
        self.assertEqual("0", environment["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY"])
        self.assertEqual("1", environment["REQUIRE_LM_HEAD_ARGMAX"])
        self.assertEqual("f32", environment["LLAMA_CACHE_TYPE_K"])
        self.assertNotIn("ANTFLY_EXPERIMENTAL_SWITCH", environment)
        self.assertEqual("0", environment["CUDA_VISIBLE_DEVICES"])
        self.assertEqual("123", environment["TIMEOUT"])

    def test_matrix_ratio_is_only_a_failure_when_enforced(self) -> None:
        self.assertEqual(0.70, DEFAULT_MIN_COMPARABLE_RATIO)
        self.assertEqual([], e2b_contract_errors(matrix(0.69, passed=False), False, DEFAULT_MIN_COMPARABLE_RATIO))
        self.assertEqual([], e2b_contract_errors(matrix(0.70), True, DEFAULT_MIN_COMPARABLE_RATIO))
        errors = e2b_contract_errors(matrix(0.69, passed=False), True, DEFAULT_MIN_COMPARABLE_RATIO)
        self.assertTrue(any("below required 0.700" in error for error in errors))
        self.assertTrue(any("did not pass" in error for error in errors))

    def test_benchmark_provenance_binds_performance_and_route_contract(self) -> None:
        contract = benchmark_contract(argparse.Namespace(
            enforce_performance=True,
            min_comparable_ratio=DEFAULT_MIN_COMPARABLE_RATIO,
        ))
        self.assertTrue(contract["performance_enforced"])
        self.assertEqual(0.70, contract["min_comparable_ratio"])
        self.assertEqual(list(REQUIRED_Q8_PREFILL_ROUTE_COUNTERS), contract["required_positive_route_counters"])
        self.assertEqual(list(FORBIDDEN_GENERATED_Q4_0_ROUTE_COUNTERS), contract["required_zero_route_counters"])

    def test_matrix_contract_rejects_replay_or_pair_failures(self) -> None:
        bad = matrix()
        bad["entries"][0]["pair_ok"] = False
        bad["entries"][0]["graph_replay_ok"] = False
        errors = e2b_contract_errors(bad, False, DEFAULT_MIN_COMPARABLE_RATIO)
        self.assertTrue(any("paired benchmark" in error for error in errors))
        self.assertTrue(any("persistent graph replay" in error for error in errors))

    def test_pair_contract_rejects_generated_q6_and_profile_drift(self) -> None:
        self.assertEqual([], e2b_pair_contract_errors(pair()))
        bad = pair()
        bad["comparison"]["antfly_generated_q6_k_q8_1_lm_head_argmax"] = "1"
        bad["rows"][0]["antfly_generated_q6_lm_head_argmax"] = 1
        errors = e2b_pair_contract_errors(bad)
        self.assertTrue(any("profile drifted" in error for error in errors))
        self.assertTrue(any("generated Q6 LM-head" in error for error in errors))

    def test_pair_contract_rejects_exact_ffn_and_profile_drift(self) -> None:
        bad = pair()
        bad["comparison"]["antfly_generated_q4_0_e2b_ffn_exact"] = "1"
        bad["rows"][0]["antfly_generated_e2b_exact_pair"] = 1
        errors = e2b_pair_contract_errors(bad)
        self.assertTrue(any("antfly_generated_q4_0_e2b_ffn_exact" in error for error in errors))
        self.assertTrue(any("antfly_generated_e2b_exact_pair" in error for error in errors))

    def test_pair_contract_requires_positive_q8_prefill_routes(self) -> None:
        for key in REQUIRED_Q8_PREFILL_ROUTE_COUNTERS:
            with self.subTest(key=key):
                bad = pair()
                bad["rows"][0][key] = 0
                errors = e2b_pair_contract_errors(bad)
                self.assertTrue(any(key in error for error in errors))

    def test_pair_contract_rejects_generated_q4_route_hits_and_fallbacks(self) -> None:
        for key in FORBIDDEN_GENERATED_Q4_0_ROUTE_COUNTERS:
            with self.subTest(key=key):
                bad = pair()
                bad["rows"][0][key] = 1
                errors = e2b_pair_contract_errors(bad)
                self.assertTrue(any(key in error for error in errors))

    def test_pair_contract_rejects_missing_or_malformed_route_counters(self) -> None:
        counters = REQUIRED_Q8_PREFILL_ROUTE_COUNTERS + FORBIDDEN_GENERATED_Q4_0_ROUTE_COUNTERS
        for key in counters:
            with self.subTest(key=key, case="missing"):
                bad = pair()
                del bad["rows"][0][key]
                errors = e2b_pair_contract_errors(bad)
                self.assertTrue(any(f"missing route counter {key}" in error for error in errors))
            with self.subTest(key=key, case="malformed"):
                bad = pair()
                bad["rows"][0][key] = "1" if key in REQUIRED_Q8_PREFILL_ROUTE_COUNTERS else "0"
                errors = e2b_pair_contract_errors(bad)
                self.assertTrue(any(f"invalid route counter {key}" in error for error in errors))

    def test_12b_replay_contract_requires_exact_tokens_and_disabled_candidates(self) -> None:
        run = generation_run()
        self.assertEqual([], generation_replay_errors(run, GEMMA12B_TOKENS, "12B"))
        run["timing_data"]["cuda"]["lm_head_argmax_generated_q6_k_q8_1_hits"] = 1
        errors = generation_replay_errors(run, GEMMA12B_TOKENS, "12B")
        self.assertTrue(any("generated_q6" in error for error in errors))

    def test_12b_evidence_detects_nondeterministic_token_ids(self) -> None:
        first = generation_run()
        second = generation_run()
        second["token_ids"][17] = 999
        with tempfile.TemporaryDirectory() as temp_dir:
            args = argparse.Namespace(
                output_dir=pathlib.Path(temp_dir),
                gemma12b_q4_model=pathlib.Path("/model.gguf"),
            )
            with mock.patch(
                "gemma4_cuda_l4_release_gate.run_generation_case",
                side_effect=[first, second],
            ):
                evidence = gemma12b_evidence(args, {}, {"path": "/model.gguf", "exists": True})
        self.assertFalse(evidence["passed"])
        self.assertFalse(evidence["checks"]["deterministic_tokens"])
        self.assertTrue(any("differ at index 17" in error for error in evidence["errors"]))

    def test_disabled_candidate_counter_errors_are_specific(self) -> None:
        timing = {"cuda": {"launch_attention_gqa_decode_generated": 2}}
        errors = disabled_candidate_errors(timing, "run")
        self.assertEqual(["run unexpectedly used disabled candidate counter launch_attention_gqa_decode_generated"], errors)

        timing = {"cuda": {"launch_attention_gqa_decode_score_prework": 2}}
        errors = disabled_candidate_errors(timing, "run")
        self.assertEqual(
            ["run unexpectedly used disabled candidate counter launch_attention_gqa_decode_score_prework"],
            errors,
        )

        timing = {"cuda": {"q4_0_generated_e2b_exact_pair_f32_hits": 2}}
        errors = disabled_candidate_errors(timing, "run")
        self.assertEqual(
            ["run unexpectedly used disabled candidate counter q4_0_generated_e2b_exact_pair_f32_hits"],
            errors,
        )

    def test_l4_requirement_is_exact(self) -> None:
        l4 = {"name": "NVIDIA L4", "compute_capability": "8.9", "driver_version": "580.159.03"}
        self.assertEqual([], l4_errors({"devices": [l4]}))
        self.assertIn("expected NVIDIA L4", l4_errors({"devices": [{**l4, "name": "NVIDIA A10"}]})[0])
        self.assertIn("driver version", l4_errors({"devices": [{**l4, "driver_version": ""}]})[0])
        self.assertIn("expected exactly one", l4_errors({"devices": []})[0])

    def test_git_provenance_separates_tracked_and_source_untracked_drift(self) -> None:
        clean = {"returncode": 0, "stdout": None, "stderr": None}
        captures = [
            {**clean, "stdout": "a" * 40},
            {**clean, "stdout": "a" * 40},
            clean,
            {**clean, "stdout": "?? zig/pkg/inference/new_source.zig"},
            {**clean, "stdout": "?? artifacts/cuda-gemma4-l4/release_summary.json"},
        ]
        content = clean_git_content_provenance()
        content["untracked_files"] = [{
            "path": "artifacts/cuda-gemma4-l4/release_summary.json",
            "kind": "file",
            "mode": 0o644,
            "bytes": 2,
            "sha256": hashlib.sha256(b"{}").hexdigest(),
        }]
        content["untracked_file_count"] = 1
        content["untracked_inventory_sha256"] = canonical_sha256({
            "schema": GIT_UNTRACKED_INVENTORY_SCHEMA,
            "files": content["untracked_files"],
        })
        with (
            mock.patch("gemma4_cuda_l4_release_gate.command_capture", side_effect=captures),
            mock.patch(
                "gemma4_cuda_l4_release_gate._tracked_diff_provenance",
                return_value={
                    name: content[name]
                    for name in (
                        "tracked_diff_returncode",
                        "tracked_diff_bytes",
                        "tracked_diff_sha256",
                        "tracked_diff_error",
                    )
                },
            ),
            mock.patch(
                "gemma4_cuda_l4_release_gate._untracked_content_provenance",
                return_value={
                    name: value
                    for name, value in content.items()
                    if name.startswith("untracked_")
                },
            ),
        ):
            provenance = git_provenance(pathlib.Path("/repo"))
        self.assertIs(provenance["tracked_dirty"], False)
        self.assertIs(provenance["dirty"], True)
        self.assertEqual(["zig/pkg/inference/new_source.zig"], provenance["source_untracked_paths"])
        self.assertEqual(1, provenance["untracked_file_count"])
        self.assertEqual([], git_content_provenance_errors(provenance))

    def test_git_provenance_hashes_dirty_content_not_only_status_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            repo = pathlib.Path(temporary)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.name", "Antfly Test"], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.email", "test@antfly.invalid"], check=True)
            tracked = repo / "tracked.txt"
            tracked.write_text("base\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(repo), "add", "tracked.txt"], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", "base"], check=True)

            tracked.write_text("first dirty content\n", encoding="utf-8")
            first = git_provenance(repo)
            tracked.write_text("second dirty value\n", encoding="utf-8")
            second = git_provenance(repo)

        self.assertEqual(first["tracked_status_sha256"], second["tracked_status_sha256"])
        self.assertNotEqual(first["tracked_diff_sha256"], second["tracked_diff_sha256"])
        self.assertGreater(first["tracked_diff_bytes"], 0)
        self.assertEqual([], git_content_provenance_errors(first))
        self.assertEqual([], git_content_provenance_errors(second))

    def test_git_provenance_hashes_untracked_file_and_symlink_content(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            repo = pathlib.Path(temporary)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.name", "Antfly Test"], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.email", "test@antfly.invalid"], check=True)
            tracked = repo / "tracked.txt"
            tracked.write_text("base\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(repo), "add", "tracked.txt"], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", "base"], check=True)

            untracked = repo / "candidate.bin"
            untracked.write_bytes(b"first")
            (repo / "candidate.link").symlink_to("candidate.bin")
            first = git_provenance(repo)
            untracked.write_bytes(b"other")
            second = git_provenance(repo)

        self.assertEqual(first["status_sha256"], second["status_sha256"])
        self.assertNotEqual(first["untracked_inventory_sha256"], second["untracked_inventory_sha256"])
        self.assertEqual(["candidate.bin", "candidate.link"], [
            entry["path"] for entry in first["untracked_files"]
        ])
        self.assertEqual(["file", "symlink"], [
            entry["kind"] for entry in first["untracked_files"]
        ])
        self.assertEqual([], git_content_provenance_errors(first))
        self.assertEqual([], git_content_provenance_errors(second))

    def test_git_content_provenance_rejects_mutated_or_missing_inventory(self) -> None:
        content = clean_git_content_provenance()
        self.assertEqual([], git_content_provenance_errors(content))

        missing = {**content, "tracked_diff_sha256": None}
        self.assertTrue(any("tracked-content" in error for error in git_content_provenance_errors(missing)))

        mutated = {**content, "untracked_inventory_sha256": "f" * 64}
        self.assertTrue(any("does not match" in error for error in git_content_provenance_errors(mutated)))

    def test_provenance_errors_fail_closed_on_git_absence_and_source_drift(self) -> None:
        tool = {"returncode": 0, "path": "/tool", "sha256": "f" * 64, "version": "ok"}
        toolchains = {name: dict(tool) for name in ("python", "git", "cuobjdump", "nvidia_smi")}
        toolchains["zig"] = {**tool, "version": "0.16.0"}
        toolchains["nvcc"] = {**tool, "version": "Cuda compilation tools, release 13.2"}
        base = {
            "git": {
                "commit": "a" * 40,
                "commit_returncode": 0,
                "tracked_status_returncode": 0,
                "tracked_dirty": False,
                "source_status_returncode": 0,
                "source_untracked_paths": [],
                **clean_git_content_provenance(),
            },
            "toolchains": toolchains,
        }
        self.assertEqual([], provenance_errors(base))

        missing = {"git": {}, "toolchains": toolchains}
        errors = provenance_errors(missing)
        self.assertTrue(any("commit provenance" in error for error in errors))
        self.assertTrue(any("tracked-source status" in error for error in errors))
        self.assertTrue(any("untracked-source status" in error for error in errors))

        drift = {"git": {**base["git"], "tracked_dirty": True, "source_untracked_paths": ["zig/new.zig"]}, "toolchains": toolchains}
        errors = provenance_errors(drift)
        self.assertTrue(any("tracked source differs" in error for error in errors))
        self.assertTrue(any("untracked source files" in error for error in errors))

    def test_toolchain_provenance_shape_is_deterministic_on_missing_tools(self) -> None:
        with mock.patch("gemma4_cuda_l4_release_gate.shutil.which", return_value=None), \
                mock.patch("gemma4_cuda_l4_release_gate.pathlib.Path.is_file", return_value=False):
            provenance = toolchain_provenance()
        self.assertEqual(
            {"python", "git", "zig", "nvcc", "cuobjdump", "nvidia_smi", "platform"},
            set(provenance),
        )
        for name in ("python", "git", "zig", "nvcc", "cuobjdump", "nvidia_smi"):
            self.assertEqual({"command", "path", "sha256", "version", "returncode"}, set(provenance[name]))
            self.assertIsNone(provenance[name]["path"])
        errors = provenance_errors({
            "git": {
                "commit": "a" * 40,
                "commit_returncode": 0,
                "tracked_status_returncode": 0,
                "tracked_dirty": False,
                "source_status_returncode": 0,
                "source_untracked_paths": [],
                **clean_git_content_provenance(),
            },
            "toolchains": provenance,
        })
        self.assertTrue(any("nvcc toolchain provenance is unavailable" in error for error in errors))
        self.assertTrue(any("cuobjdump toolchain provenance is unavailable" in error for error in errors))

    def test_provenance_binding_hashes_exact_written_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = pathlib.Path(temp_dir) / "release_provenance.json"
            path.write_bytes(b'{"schema":"test"}\n')
            binding = provenance_binding(path)
        self.assertEqual("release_provenance.json", binding["provenance"])
        self.assertEqual(hashlib.sha256(b'{"schema":"test"}\n').hexdigest(), binding["provenance_sha256"])

    def test_diagnostic_bypasses_cannot_pass_release_summary(self) -> None:
        self.assertEqual([], diagnostic_mode_errors(True, False))
        self.assertEqual(1, len(diagnostic_mode_errors(False, False)))
        self.assertEqual(1, len(diagnostic_mode_errors(True, True)))
        errors = diagnostic_mode_errors(False, True)
        self.assertEqual(2, len(errors))
        self.assertTrue(any("--no-require-l4" in error for error in errors))
        self.assertTrue(any("--skip-12b" in error for error in errors))

    def test_gpu_provenance_honors_numeric_cuda_visible_devices(self) -> None:
        output = "0, NVIDIA L4, 550.54, 8.9\n1, NVIDIA L4, 550.54, 8.9\n"
        with mock.patch("gemma4_cuda_l4_release_gate.command_output", return_value=output), \
                mock.patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "1"}, clear=True):
            provenance = gpu_provenance()
        self.assertEqual(["1"], [device["index"] for device in provenance["devices"]])
        self.assertEqual([], l4_errors(provenance))

    def test_matrix_command_freezes_256_token_contract(self) -> None:
        args = argparse.Namespace(
            matrix_script=pathlib.Path("/matrix.py"),
            output_dir=pathlib.Path("/tmp/release"),
            warmups=1,
            repeats=3,
            enforce_performance=True,
            min_comparable_ratio=DEFAULT_MIN_COMPARABLE_RATIO,
        )
        command = matrix_command(args)
        self.assertIn("--lengths", command)
        self.assertEqual(str(E2B_LLAMA_TOKENS), command[command.index("--lengths") + 1])
        self.assertEqual(str(E2B_LLAMA_TOKENS), command[command.index("--target-length") + 1])
        self.assertEqual("0.7", command[command.index("--min-comparable-ratio") + 1])
        self.assertIn("--no-require-generated-attention", command)
        self.assertIn("--no-require-generated-q6-lm-head-argmax", command)
        self.assertIn("--collect-only", command)
        self.assertEqual(str(MAX_TOK_S_CV), command[command.index("--max-cv") + 1])

    def test_matrix_outputs_are_removed_before_collection(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = pathlib.Path(temp_dir)
            matrix_path = output_dir / "e2b/matrix_summary.json"
            pair_path = output_dir / f"e2b/tokens-{E2B_LLAMA_TOKENS}/paired_summary.json"
            matrix_path.parent.mkdir(parents=True)
            pair_path.parent.mkdir(parents=True)
            matrix_path.write_text("stale")
            pair_path.write_text("stale")

            self.assertEqual((matrix_path, pair_path), reset_matrix_outputs(output_dir))
            self.assertFalse(matrix_path.exists())
            self.assertFalse(pair_path.exists())

    def test_nonzero_matrix_process_is_one_infrastructure_error(self) -> None:
        self.assertEqual([], matrix_process_errors(0))
        self.assertEqual(["E2B matrix exited 17"], matrix_process_errors(17))

    def test_matrix_timeout_covers_every_paired_command(self) -> None:
        args = argparse.Namespace(timeout_sec=123, warmups=1, repeats=3)
        self.assertEqual(123 * 9, matrix_timeout_sec(args))

    def test_run_logged_terminates_the_process_group_on_timeout(self) -> None:
        process = mock.Mock(pid=4321, returncode=-signal.SIGTERM)
        process.communicate.side_effect = [subprocess.TimeoutExpired(["slow"], 1), ("stopped", None)]
        with tempfile.TemporaryDirectory() as temp_dir:
            log = pathlib.Path(temp_dir) / "command.log"
            with mock.patch("gemma4_cuda_l4_release_gate.subprocess.Popen", return_value=process) as popen, \
                    mock.patch("gemma4_cuda_l4_release_gate.os.killpg") as killpg:
                returncode, output = run_logged(["slow"], {}, log, 1)
            self.assertIn("timed out", log.read_text())
        self.assertEqual(124, returncode)
        self.assertIn("timed out", output)
        self.assertTrue(popen.call_args.kwargs["start_new_session"])
        killpg.assert_called_once_with(4321, signal.SIGTERM)

    def test_token_parsing_and_provenance_fingerprint_are_stable(self) -> None:
        self.assertIsNotNone(TOKEN_IDS_RE.search("token_ids: 1 2 3\n"))
        self.assertEqual([1, 2, 3], parse_token_ids("before\ntoken_ids: 1 2 3\nafter\n"))
        self.assertEqual([], parse_token_ids("token_ids=unavailable\n"))
        self.assertEqual(canonical_sha256({"a": 1, "b": 2}), canonical_sha256({"b": 2, "a": 1}))
        with tempfile.TemporaryDirectory() as temp_dir:
            path = pathlib.Path(temp_dir) / "artifact.bin"
            path.write_bytes(b"artifact")
            provenance = path_provenance(path)
        self.assertTrue(provenance["exists"])
        self.assertEqual(hashlib.sha256(b"artifact").hexdigest(), provenance["sha256"])


if __name__ == "__main__":
    unittest.main()
