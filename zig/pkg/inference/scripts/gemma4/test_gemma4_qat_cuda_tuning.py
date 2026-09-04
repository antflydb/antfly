#!/usr/bin/env python3

import os
import pathlib
import subprocess
import unittest


TUNING_SCRIPT = pathlib.Path(__file__).resolve().with_name("gemma4_qat_cuda_tuning.sh")
WRAPPER_SCRIPT = (
    pathlib.Path(__file__).resolve().with_name("with_gemma4_qat_cuda_tuning.sh")
)


def configured_environment(**overrides: str) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(overrides)
    completed = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; gemma4_qat_cuda_tuning_env 777; printf "%s\\n" "${GEMMA4_QAT_CUDA_ENV[@]}"',
            "bash",
            str(TUNING_SCRIPT),
        ],
        check=True,
        capture_output=True,
        env=environment,
        text=True,
    )
    return dict(line.split("=", 1) for line in completed.stdout.splitlines())


def wrapped_environment(**overrides: str) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.update(overrides)
    return subprocess.run(
        [str(WRAPPER_SCRIPT), "/usr/bin/env"],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )


class Gemma4QatCudaTuningTest(unittest.TestCase):
    def test_profiling_canonical_variables_take_precedence_over_aliases(self):
        environment = configured_environment(
            ANTFLY_INFERENCE_CUDA_PROFILE_PREFILL_OPS="canonical-prefill",
            antfly_cuda_profile_prefill_ops="lowercase-prefill-alias",
            ANTFLY_CUDA_PROFILE_PREFILL_OPS="uppercase-prefill-alias",
            ANTFLY_INFERENCE_CUDA_PROFILE_DECODE="canonical-decode",
            antfly_cuda_profile_decode="lowercase-decode-alias",
            ANTFLY_CUDA_PROFILE_DECODE="uppercase-decode-alias",
        )
        self.assertEqual(
            "canonical-prefill",
            environment["ANTFLY_INFERENCE_CUDA_PROFILE_PREFILL_OPS"],
        )
        self.assertEqual(
            "canonical-decode",
            environment["ANTFLY_INFERENCE_CUDA_PROFILE_DECODE"],
        )

    def test_gqa_prefill_uses_one_fail_closed_canonical_profile(self):
        environment = configured_environment()
        self.assertEqual(
            "required-fast",
            environment["ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE"],
        )
        self.assertNotIn("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_FAST", environment)
        self.assertNotIn("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_TILED", environment)
        self.assertNotIn("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_MMA", environment)

        for profile in (
            "mma",
            "tiled-f16-exact",
            "required-tiled-f16-exact",
            "tiled-f16-warp",
            "required-tiled-f16-warp",
            "flash-f16-sm89",
            "required-flash-f16-sm89",
        ):
            with self.subTest(profile=profile):
                environment = configured_environment(
                    ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE=profile,
                )
                self.assertEqual(
                    profile, environment["ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE"]
                )

    def test_gqa_prefill_can_preserve_the_runtime_automatic_default(self):
        environment = configured_environment(
            ANTFLY_GQA_PREFILL_USE_RUNTIME_DEFAULT="1",
        )
        self.assertNotIn("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE", environment)

        completed = wrapped_environment(
            ANTFLY_GQA_PREFILL_USE_RUNTIME_DEFAULT="1",
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        wrapped = dict(line.split("=", 1) for line in completed.stdout.splitlines())
        self.assertNotIn("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE", wrapped)

    def test_gqa_runtime_default_rejects_explicit_profile_conflicts(self):
        environment = os.environ.copy()
        environment.update(
            ANTFLY_GQA_PREFILL_USE_RUNTIME_DEFAULT="1",
            ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE="required-fast",
        )
        completed = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; gemma4_qat_cuda_tuning_env 777',
                "bash",
                str(TUNING_SCRIPT),
            ],
            check=False,
            capture_output=True,
            env=environment,
            text=True,
        )
        self.assertEqual(2, completed.returncode)
        self.assertIn(
            "conflicts with explicit GQA prefill configuration", completed.stderr
        )

    def test_gqa_prefill_rejects_unknown_typed_profile(self):
        environment = os.environ.copy()
        environment["ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE"] = "tiled-f16-typo"
        completed = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; gemma4_qat_cuda_tuning_env 777',
                "bash",
                str(TUNING_SCRIPT),
            ],
            check=False,
            capture_output=True,
            env=environment,
            text=True,
        )
        self.assertEqual(2, completed.returncode)
        self.assertIn(
            "invalid ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE", completed.stderr
        )

    def test_ple_gate_prefill_profile_is_typed_and_default_off(self):
        environment = configured_environment()
        self.assertEqual(
            "off",
            environment["ANTFLY_INFERENCE_CUDA_PLE_GATE_PREFILL_PROFILE"],
        )

        environment = configured_environment(
            ANTFLY_INFERENCE_CUDA_PLE_GATE_PREFILL_PROFILE="mirror-first-sm89-e2b",
        )
        self.assertEqual(
            "mirror-first-sm89-e2b",
            environment["ANTFLY_INFERENCE_CUDA_PLE_GATE_PREFILL_PROFILE"],
        )

    def test_ple_gate_prefill_rejects_unknown_typed_profile(self):
        environment = os.environ.copy()
        environment["ANTFLY_INFERENCE_CUDA_PLE_GATE_PREFILL_PROFILE"] = (
            "mirror_first_sm89_e2b"
        )
        completed = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; gemma4_qat_cuda_tuning_env 777',
                "bash",
                str(TUNING_SCRIPT),
            ],
            check=False,
            capture_output=True,
            env=environment,
            text=True,
        )
        self.assertEqual(2, completed.returncode)
        self.assertIn(
            "invalid ANTFLY_INFERENCE_CUDA_PLE_GATE_PREFILL_PROFILE",
            completed.stderr,
        )

    def test_gqa_prefill_maps_unambiguous_legacy_profiles(self):
        cases = {
            "ANTFLY_GQA_PREFILL_FAST": "fast",
            "antfly_gqa_prefill_tiled": "tiled",
            "ANTFLY_INFERENCE_CUDA_GQA_PREFILL_MMA": "mma",
        }
        for name, expected in cases.items():
            with self.subTest(name=name):
                environment = configured_environment(**{name: "1"})
                self.assertEqual(
                    expected, environment["ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE"]
                )

    def test_gqa_prefill_rejects_ambiguous_legacy_profiles(self):
        environment = os.environ.copy()
        environment.update(
            ANTFLY_GQA_PREFILL_FAST="1",
            ANTFLY_GQA_PREFILL_TILED="1",
        )
        completed = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; gemma4_qat_cuda_tuning_env 777',
                "bash",
                str(TUNING_SCRIPT),
            ],
            check=False,
            capture_output=True,
            env=environment,
            text=True,
        )
        self.assertEqual(2, completed.returncode)
        self.assertIn("ambiguous legacy GQA prefill booleans", completed.stderr)

    def test_bf16_residency_modes_are_mutually_exclusive(self):
        environment = os.environ.copy()
        environment.update(
            ANTFLY_INFERENCE_CUDA_DEQUANTIZE_Q4_0_MATRIX_WEIGHTS_BF16="1",
            ANTFLY_INFERENCE_CUDA_Q4_0_WEIGHTS_BF16_PREFILL="1",
        )
        completed = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; gemma4_qat_cuda_tuning_env 777',
                "bash",
                str(TUNING_SCRIPT),
            ],
            check=False,
            capture_output=True,
            env=environment,
            text=True,
        )
        self.assertEqual(2, completed.returncode)
        self.assertIn("mutually exclusive", completed.stderr)

    def test_generated_attention_defaults_disabled_and_honors_explicit_opt_in(self):
        environment = configured_environment()
        self.assertEqual(
            "0", environment["ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE"]
        )
        self.assertNotIn(
            "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK", environment
        )
        self.assertEqual(
            "0", environment["ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION"]
        )

        for name in (
            "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE",
            "ANTFLY_GENERATED_ATTENTION_DECODE",
            "antfly_generated_attention_decode",
        ):
            with self.subTest(name=name):
                environment = configured_environment(**{name: "1"})
                self.assertEqual(
                    "1", environment["ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE"]
                )

        for name in (
            "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK",
            "ANTFLY_GENERATED_ATTENTION_SCORE_PREWORK",
            "antfly_generated_attention_score_prework",
        ):
            with self.subTest(name=name):
                environment = configured_environment(**{name: "1"})
                self.assertEqual(
                    "1",
                    environment[
                        "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK"
                    ],
                )

        environment = configured_environment(
            ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK="0",
        )
        self.assertEqual(
            "0", environment["ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK"]
        )

        for name in (
            "ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION",
            "ANTFLY_TURBOQUANT_SPLIT_ATTENTION",
            "antfly_turboquant_split_attention",
        ):
            with self.subTest(name=name):
                environment = configured_environment(**{name: "1"})
                self.assertEqual(
                    "1", environment["ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION"]
                )

    def test_readback_tuning_defaults_to_enabled(self):
        environment = configured_environment()
        self.assertEqual(
            "1", environment["ANTFLY_INFERENCE_CUDA_ASYNC_I32_DOWNLOAD_STAGING"]
        )
        self.assertEqual(
            "1", environment["ANTFLY_INFERENCE_CUDA_GREEDY_PENDING_TOKEN_READBACK"]
        )
        self.assertEqual(
            "0", environment["ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX"]
        )
        self.assertEqual(
            "0", environment["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT"]
        )
        self.assertEqual(
            "0", environment["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY"]
        )
        self.assertEqual("1", environment["ANTFLY_INFERENCE_CUDA_TEMP_ARENA_AUTOPLAN"])
        self.assertEqual("0", environment["ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD"])
        self.assertEqual("0", environment["ANTFLY_INFERENCE_CUDA_TEMP_SLOT_SKIP"])
        self.assertEqual(
            "1", environment["ANTFLY_INFERENCE_CUDA_SERVER_REQUEST_GRAPH_RESET"]
        )

    def test_graph_off_removes_graph_only_temp_and_request_state(self):
        environment = configured_environment(ANTFLY_DECODE_GRAPH_REPLAY="off")
        self.assertEqual("0", environment["ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD"])
        self.assertEqual("0", environment["ANTFLY_INFERENCE_CUDA_TEMP_SLOT_SKIP"])
        self.assertEqual("0", environment["ANTFLY_INFERENCE_CUDA_TEMP_ARENA_AUTOPLAN"])
        self.assertEqual(
            "0", environment["ANTFLY_INFERENCE_CUDA_SERVER_REQUEST_GRAPH_RESET"]
        )
        self.assertEqual(
            "0", environment["ANTFLY_INFERENCE_CUDA_CAPTURE_PERSISTENT_REPLAY"]
        )

    def test_batching_wrapper_requires_graph_replay_off(self):
        rejected = wrapped_environment(
            ANTFLY_SERVER_DISABLE_CONTINUOUS_BATCHING="0",
            ANTFLY_SERVER_DECODE_GRAPH_REPLAY="required",
        )
        self.assertEqual(2, rejected.returncode)
        self.assertIn("requires ANTFLY_SERVER_DECODE_GRAPH_REPLAY=off", rejected.stderr)

        accepted = wrapped_environment(
            ANTFLY_SERVER_DISABLE_CONTINUOUS_BATCHING="0",
            ANTFLY_SERVER_DECODE_GRAPH_REPLAY="off",
        )
        self.assertEqual(0, accepted.returncode, accepted.stderr)
        environment = dict(
            line.split("=", 1) for line in accepted.stdout.splitlines() if "=" in line
        )
        self.assertEqual(
            "0", environment["ANTFLY_INFERENCE_CUDA_SERVER_REQUEST_GRAPH_RESET"]
        )
        self.assertEqual("0", environment["ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD"])
        self.assertEqual("0", environment["ANTFLY_INFERENCE_CUDA_TEMP_SLOT_SKIP"])
        self.assertEqual("0", environment["ANTFLY_INFERENCE_CUDA_TEMP_ARENA_AUTOPLAN"])

    def test_wrapper_preserves_canonical_capture_capacity(self):
        completed = wrapped_environment(
            ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY="2400",
            ANTFLY_CAPTURE_FORCE_KV_CAPACITY="544",
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        environment = dict(
            line.split("=", 1) for line in completed.stdout.splitlines() if "=" in line
        )
        self.assertEqual(
            "2400",
            environment["ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY"],
        )

    def test_temp_slot_schedule_honors_ambient_overrides(self):
        for name in (
            "ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD",
            "ANTFLY_CUDA_TEMP_SLOT_PERIOD",
            "antfly_cuda_temp_slot_period",
        ):
            with self.subTest(name=name):
                environment = configured_environment(**{name: "997"})
                self.assertEqual(
                    "997", environment["ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD"]
                )

        for name in (
            "ANTFLY_INFERENCE_CUDA_TEMP_SLOT_SKIP",
            "ANTFLY_CUDA_TEMP_SLOT_SKIP",
            "antfly_cuda_temp_slot_skip",
        ):
            with self.subTest(name=name):
                environment = configured_environment(**{name: "2112"})
                self.assertEqual(
                    "2112", environment["ANTFLY_INFERENCE_CUDA_TEMP_SLOT_SKIP"]
                )

        for name in (
            "ANTFLY_INFERENCE_CUDA_TEMP_ARENA_AUTOPLAN",
            "ANTFLY_CUDA_TEMP_ARENA_AUTOPLAN",
            "antfly_cuda_temp_arena_autoplan",
        ):
            with self.subTest(name=name):
                environment = configured_environment(**{name: "0"})
                self.assertEqual(
                    "0", environment["ANTFLY_INFERENCE_CUDA_TEMP_ARENA_AUTOPLAN"]
                )

    def test_readback_tuning_honors_ambient_overrides(self):
        environment = configured_environment(
            ANTFLY_INFERENCE_CUDA_ASYNC_I32_DOWNLOAD_STAGING="0",
            ANTFLY_INFERENCE_CUDA_GREEDY_PENDING_TOKEN_READBACK="0",
        )
        self.assertEqual(
            "0", environment["ANTFLY_INFERENCE_CUDA_ASYNC_I32_DOWNLOAD_STAGING"]
        )
        self.assertEqual(
            "0", environment["ANTFLY_INFERENCE_CUDA_GREEDY_PENDING_TOKEN_READBACK"]
        )

    def test_generated_q6_lm_head_gate_honors_ambient_override(self):
        environment = configured_environment(
            ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX="1",
        )
        self.assertEqual(
            "1", environment["ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX"]
        )

    def test_exact_e2b_ffn_gate_honors_ambient_override(self):
        environment = configured_environment(
            ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT="1",
        )
        self.assertEqual(
            "1", environment["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT"]
        )

    def test_pair_only_e2b_ffn_gate_honors_ambient_override(self):
        environment = configured_environment(
            ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY="1",
        )
        self.assertEqual(
            "1", environment["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY"]
        )

    def test_e2b_ffn_candidate_modes_are_mutually_exclusive(self):
        cases = (
            {
                "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN": "1",
                "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY": "1",
            },
            {
                "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT": "1",
                "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY": "1",
            },
            {
                "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_CATALOG_FFN_CANDIDATES": "1",
                "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT": "1",
            },
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                environment = os.environ.copy()
                environment.update(overrides)
                completed = subprocess.run(
                    [
                        "bash",
                        "-c",
                        'source "$1"; gemma4_qat_cuda_tuning_env 777',
                        "bash",
                        str(TUNING_SCRIPT),
                    ],
                    check=False,
                    capture_output=True,
                    env=environment,
                    text=True,
                )
                self.assertEqual(2, completed.returncode)
                self.assertIn("mutually exclusive", completed.stderr)

    def test_e2b_ffn_candidate_modes_reject_invalid_boolean(self):
        environment = os.environ.copy()
        environment["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY"] = (
            "sometimes"
        )
        completed = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; gemma4_qat_cuda_tuning_env 777',
                "bash",
                str(TUNING_SCRIPT),
            ],
            check=False,
            capture_output=True,
            env=environment,
            text=True,
        )
        self.assertEqual(2, completed.returncode)
        self.assertIn(
            "invalid generated E2B FFN pair-only candidate value", completed.stderr
        )

    def test_f32_ffn_comparison_q8_routes_honor_explicit_overrides(self):
        environment = configured_environment(
            ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_Q8_1_PRECOMPUTE="0",
            ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_DP4A="0",
            ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_Q8_1_DP4A="0",
            ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_DP4A="0",
            ANTFLY_INFERENCE_CUDA_Q4_0_GATED_DOWN_Q8_1_DP4A="0",
            ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN="0",
            ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT="1",
        )
        self.assertEqual(
            "0",
            environment[
                "ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_Q8_1_PRECOMPUTE"
            ],
        )
        self.assertEqual(
            "0", environment["ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_DP4A"]
        )
        self.assertEqual("0", environment["ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_Q8_1_DP4A"])
        self.assertEqual(
            "0", environment["ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_DP4A"]
        )
        self.assertEqual(
            "0", environment["ANTFLY_INFERENCE_CUDA_Q4_0_GATED_DOWN_Q8_1_DP4A"]
        )
        self.assertEqual(
            "0", environment["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN"]
        )
        self.assertEqual(
            "1", environment["ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT"]
        )


if __name__ == "__main__":
    unittest.main()
