#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys
import tempfile
import textwrap
import unittest
from unittest import mock


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import benchmark_gemma4_long_e2e_server as benchmark


SCRIPTS = pathlib.Path(__file__).resolve().parent
FIXTURE = SCRIPTS / "fixtures/gemma4_long_context_v1.json"


def sample(engine: str, total: float, ttft: float, digest: str = "stable") -> dict:
    decode = total - ttft
    prefix = (
        "Spella Caffe Logo.pdf and Spella Caffe Logo Two Color.pdf are in "
        "/Users/timkaye/Downloads. "
    )
    content = (prefix + "Verify each filename and preserve the evidence boundary. " * 30)[:900]
    result = {
        "engine": engine,
        "total_latency_ms": total,
        "ttft_ms": ttft,
        "decode_ms": decode,
        "stream_tail_ms": 0.0,
        "inter_token_ms": decode / 299,
        "decode_tok_s": 299_000 / decode,
        "prompt_tokens": 2051,
        "completion_tokens": 300,
        "completion_token_accounting_source": "final_sse_usage",
        "prompt_token_accounting_source": "final_sse_usage",
        "finish_reason": "length",
        "content": content,
        "content_utf8_bytes": len(content.encode("utf-8")),
        "content_sha256": digest,
        "prompt_token_ids": {"count": 2051, "sha256": "prompt-stable"},
        "connection_reused": True,
    }
    if engine == "antfly":
        result["cuda_gqa_prefill_telemetry"] = {
            "configured_profiles": ["required-fast"],
            "active_routes": ["prefill-fast"],
            "active_count": 2,
            "required_fast_active_count": 2,
            "required_fast_gemma4_head_dims": [256, 512],
            "rejected_count": 0,
            "required_fast_route_active": True,
        }
        result["cuda_gqa_score_prework_telemetry"] = {
            "configured_modes": ["serial"],
            "active_consumers": ["serial"],
            "active_count": 2,
            "active_observations": [{
                "consumer": "serial",
                "active_count": 2,
                "gemma4_head_dims": [256, 512],
                "routes_by_head_dim": {
                    "256": "gemma4_f16_local",
                    "512": "gemma4_f16_global",
                },
            }],
            "fallback_count": 0,
            "rejected_count": 0,
            "fallback_reasons": [],
        }
    return result


def rows(antfly_total: float = 90.0, llama_total: float = 100.0) -> list[dict]:
    return [
        {
            "pair": index,
            "order": list(benchmark.paired_order(index)),
            "antfly": sample("antfly", antfly_total + (index % 3) * 0.05, 20.0),
            "llama_cpp": sample("llama_cpp", llama_total + (index % 3) * 0.05, 21.0),
        }
        for index in range(1, 11)
    ]


def args(**overrides: object) -> argparse.Namespace:
    values = {
        "profile": "headline",
        "expected_prompt_tokens": 2051,
        "output_tokens": 300,
        "bootstrap_samples": 500,
        "bootstrap_seed": 7,
        "max_median_ratio": 0.95,
        "max_ci_upper": 1.0,
        "max_cv": 0.03,
        "max_p95_ratio": 1.0,
        "max_component_ratio": 1.02,
        "max_ttft_ci_upper": 1.02,
        "max_decode_ci_upper": 1.02,
        "max_baseline_regression": None,
        "enforce_performance": True,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def frozen_baseline(
    fixture: benchmark.PromptFixture,
    *,
    profile: str = "e2b-regression",
    passed: bool = True,
    sample_rows: list[dict] | None = None,
) -> dict:
    baseline_rows = sample_rows or rows()
    evaluated = benchmark.evaluate(args(enforce_performance=False), fixture, baseline_rows, None)
    execution_profile = {
        "sha256": "profile-stable",
        "material_environment_sha256": "environment-stable",
        "server_prefix_sha256": {
            "antfly": "antfly-prefix-stable",
            "llama_cpp": "llama-prefix-stable",
        },
        "server_budget_mb": dict(benchmark.CUDA_SERVER_BUDGET_MB),
    }
    provenance = {
        "runtime_identity": {"sha256": "runtime-stable"},
        "llama_cpp_binary": {
            "sha256": "llama-binary",
            "runtime_bundle_sha256": "llama-bundle",
            "git": {"commit": "llama-commit"},
        },
        "model": {"sha256": "model-stable"},
        "antfly_model_bundle": {"sha256": "model-bundle-stable"},
        "harness": {"sha256": "harness-stable"},
        "fixture_file": {"sha256": fixture.file_sha256},
        "antfly_execution_profile": execution_profile,
        "antfly_binary": {"sha256": "antfly-binary"},
        "antfly_git": {"commit": "antfly-commit"},
    }
    baseline = {
        "schema": benchmark.EVIDENCE_SCHEMA,
        "timestamp_utc": "2026-07-30T00:00:00+00:00",
        "contract": {
            "profile": profile,
            "paired_samples": len(baseline_rows),
            "prompt_fixture_id": fixture.fixture_id,
            "reference_prompt_sha256": fixture.reference_prompt_sha256,
            "backend": "cuda",
            "cache_dtype": "f16" if profile != "f32-control" else "f32",
            "context_size": 4096,
            "output_tokens": 300,
        },
        "provenance": provenance,
        "provenance_sha256": benchmark.canonical_sha256(provenance),
        "rows": baseline_rows,
        "metrics": evaluated["metrics"],
        "passed": passed,
    }
    baseline["lock"] = benchmark.evidence_lock_projection(baseline)
    return baseline


class FixtureTests(unittest.TestCase):
    def test_checked_in_fixture_renders_exact_reference_prompt(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        self.assertEqual(8192, fixture.user_utf8_bytes)
        self.assertEqual(8251, fixture.reference_prompt_utf8_bytes)
        self.assertEqual(2051, fixture.expected_prompt_tokens)
        self.assertEqual({"enable_thinking": False}, fixture.chat_template_kwargs)
        self.assertEqual("public_final", fixture.response_channel)
        self.assertEqual(
            ("Spella Caffe Logo.pdf", "Spella Caffe Logo Two Color.pdf", "/Users/timkaye/Downloads"),
            fixture.expected_output_substrings,
        )
        self.assertEqual(800, fixture.minimum_visible_utf8_bytes)
        self.assertEqual("622d1cf25efdaa8717d9a200e6aee381a5f7b3f5f60dae268c66bc04204e251b", fixture.user_sha256)
        self.assertEqual("0f9791a0344f4e302f60a6ad2cdaee80efe9212f00d49bfccd499c21cf64a6ef", fixture.reference_prompt_sha256)

    def test_fixture_hash_drift_is_rejected(self) -> None:
        raw = json.loads(FIXTURE.read_text())
        raw["suffix"] += " changed"
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "fixture.json"
            path.write_text(json.dumps(raw))
            with self.assertRaisesRegex(ValueError, "byte/hash contract"):
                benchmark.load_fixture(path)

    def test_fixture_rejects_private_thought_channel_contract(self) -> None:
        raw = json.loads(FIXTURE.read_text())
        raw["reference_chat_suffix"] = "<turn|>\n<|turn>model\n<|channel>thought\n<channel|>"
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "fixture.json"
            path.write_text(json.dumps(raw))
            with self.assertRaisesRegex(ValueError, "public-final"):
                benchmark.load_fixture(path)

    def test_llama_applied_template_must_match_reference_bytes_and_hash(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        summary = benchmark.validate_llama_rendered_prompt(fixture.reference_prompt, fixture)
        self.assertEqual(fixture.reference_prompt_utf8_bytes, summary["utf8_bytes"])
        self.assertEqual(fixture.reference_prompt_sha256, summary["sha256"])
        with self.assertRaisesRegex(RuntimeError, "measured chat template differs"):
            benchmark.validate_llama_rendered_prompt(fixture.reference_prompt + " ", fixture)


class SchedulingAndParsingTests(unittest.TestCase):
    def test_pair_order_is_ab_ba(self) -> None:
        self.assertEqual(("antfly", "llama_cpp"), benchmark.paired_order(1))
        self.assertEqual(("llama_cpp", "antfly"), benchmark.paired_order(2))
        self.assertEqual(("antfly", "llama_cpp"), benchmark.paired_order(3))

    def test_sse_parser_and_token_delta_preserve_token_boundaries(self) -> None:
        events = benchmark.parse_sse_lines([
            b'data: {"choices":[{"delta":{"role":"assistant"},"finish_reason":null}]}\n',
            b'data: {"choices":[{"delta":{"content":"ant"},"finish_reason":null}]}\n',
            b'data: {"choices":[{"delta":{"content":"s"},"finish_reason":null}]}\n',
            b'data: {"choices":[{"delta":{},"finish_reason":"length"}]}\n',
            b'data: [DONE]\n',
        ])
        deltas = [benchmark.event_delta(event) for event in events]
        self.assertEqual(["ant", "s"], [content for has_token, content, _ in deltas if has_token])
        self.assertEqual("length", deltas[-1][2])
        self.assertEqual((False, "", None), benchmark.event_delta({
            "choices": [{"delta": {"content": ""}, "finish_reason": None}],
        }))

    def test_stream_timing_requires_an_sse_response_content_type(self) -> None:
        self.assertTrue(benchmark.is_event_stream_content_type("text/event-stream"))
        self.assertTrue(benchmark.is_event_stream_content_type("Text/Event-Stream; charset=utf-8"))
        self.assertFalse(benchmark.is_event_stream_content_type("application/json"))
        self.assertFalse(benchmark.is_event_stream_content_type(None))

    def test_final_sse_usage_is_authoritative_over_content_fragment_count(self) -> None:
        # A transport may coalesce several tokens into one content delta or
        # split text without preserving tokenizer boundaries.
        events = [
            {"choices": [{"delta": {"content": "three token chunk"}, "finish_reason": None}]},
            {"choices": [{"delta": {"content": " plus two"}, "finish_reason": "length"}]},
            {"choices": [], "usage": {"prompt_tokens": 2003, "completion_tokens": 5, "total_tokens": 2008}},
        ]
        self.assertEqual((5, "final_sse_usage"), benchmark.stream_completion_accounting(events, 2))
        self.assertEqual((2, "content_events"), benchmark.stream_completion_accounting(events[:-1], 2))
        self.assertEqual((2003, "final_sse_usage"), benchmark.stream_prompt_accounting(events))
        self.assertEqual((None, "warmup_usage_fallback"), benchmark.stream_prompt_accounting(events[:-1]))

    def test_request_bodies_match_work_and_precision(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        antfly = benchmark.request_body(
            "antfly", pathlib.Path("/model.gguf"), "metal", fixture, 300, "f16", stream=True,
        )
        llama = benchmark.request_body(
            "llama_cpp", pathlib.Path("/model.gguf"), "cuda", fixture, 300, "f16", stream=True,
        )
        for body in (antfly, llama):
            self.assertEqual(300, body["max_tokens"])
            self.assertEqual(0, body["temperature"])
            self.assertTrue(body["stream"])
            self.assertEqual({"include_usage": True}, body["stream_options"])
            self.assertEqual(fixture.user_content, body["messages"][0]["content"])
            self.assertEqual({"enable_thinking": False}, body["chat_template_kwargs"])
        self.assertEqual("f16", antfly["cache_dtype"])
        self.assertEqual("metal", antfly["backend"])
        self.assertTrue(antfly["ignore_eos"])
        self.assertFalse(antfly["prompt_cache"])
        self.assertNotIn("draft_model", antfly)
        self.assertNotIn("speculative_k", antfly)
        self.assertNotIn("speculation_policy", antfly)
        self.assertNotIn("speculation_calibration", antfly)
        self.assertTrue(llama["ignore_eos"])
        self.assertFalse(llama["cache_prompt"])

    def test_antfly_model_identifier_selects_dedicated_model_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            models_dir = pathlib.Path(temporary)
            model = models_dir / "unsloth" / "model.gguf"
            model.parent.mkdir()
            model.write_bytes(b"model")
            self.assertEqual(
                "unsloth",
                benchmark.antfly_model_identifier(model, models_dir),
            )

    def test_antfly_model_identifier_rejects_ambiguous_decoder_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            models_dir = pathlib.Path(temporary)
            model_dir = models_dir / "model"
            model_dir.mkdir()
            model = model_dir / "q4.gguf"
            model.write_bytes(b"model")
            (model_dir / "q8.gguf").write_bytes(b"other-model")
            with self.assertRaisesRegex(ValueError, "exactly the benchmark decoder GGUF"):
                benchmark.antfly_model_identifier(model, models_dir)

    def test_antfly_model_identifier_rejects_model_outside_models_dir(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            models_dir = root / "models"
            models_dir.mkdir()
            model = root / "model.gguf"
            model.write_bytes(b"model")
            with self.assertRaisesRegex(ValueError, "within models-dir"):
                benchmark.antfly_model_identifier(model, models_dir)

    def test_prompt_token_preflight_uses_locked_public_channel_prompt(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            model_dir = root / "model"
            model_dir.mkdir()
            model = model_dir / "model.gguf"
            model.write_bytes(b"decoder-tokenizer")
            (model_dir / "tokenizer.json").write_text('{"conflicting":"sidecar-tokenizer"}')
            antfly = root / "antfly"
            antfly.write_bytes(b"binary")
            namespace = argparse.Namespace(
                antfly_bin=antfly,
                model=model,
                backend="cuda",
                antfly_server_prefix="",
                antfly_execution_profile={"effective_env": {}},
                request_timeout=5,
                output_dir=root,
            )
            completed = mock.Mock(
                returncode=0,
                stdout=(
                    "chat_template=true\n"
                    f"prompt:\n{benchmark.TOKENIZER_BOS_TEXT}{fixture.reference_prompt}\n"
                    "prompt_token_ids: 1 2 3\n"
                ),
            )
            with mock.patch.object(benchmark.subprocess, "run", return_value=completed) as run:
                contract = benchmark.collect_antfly_prompt_token_contract(namespace, fixture)
            command = run.call_args.args[0]
            self.assertEqual(str(model_dir.resolve()), command[2])
            self.assertEqual(fixture.user_content, command[3])
            self.assertIn("--disable-thinking", command)
            self.assertEqual("f16", command[command.index("--cache-dtype") + 1])
            self.assertEqual(
                str(benchmark.SERVER_MAX_IDLE_PREFILL_CHUNK_SIZE),
                command[command.index("--prefill-chunk-size") + 1],
            )
            for name, value in benchmark.CUDA_SERVER_BUDGET_MB.items():
                self.assertEqual(str(value), command[command.index(f"--{name}-budget-mb") + 1])
            self.assertEqual(str(model_dir.resolve()), contract["model_directory"])
            self.assertEqual("antfly_cli_rendered_public_channel_prompt", contract["source"])
            self.assertEqual(fixture.reference_prompt_sha256, contract["template"]["sha256"])
            self.assertEqual("<bos>", contract["serialized_prompt"]["automatic_prefix"])

    def test_model_neutral_cuda_profile_is_explicit_and_overrideable(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        base_args = argparse.Namespace(
            backend="cuda",
            output_tokens=300,
            antfly_env=[],
            antfly_server_prefix="",
            llama_server_prefix="",
        )
        profile = benchmark.resolve_antfly_execution_profile(base_args, fixture, {})
        self.assertEqual(benchmark.CUDA_PROFILE_NAME, profile["name"])
        self.assertEqual("required", profile["effective_env"]["ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY"])
        self.assertEqual(
            "required-fast",
            profile["effective_env"]["ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE"],
        )
        self.assertEqual("1", profile["effective_env"]["ANTFLY_INFERENCE_CUDA_TEMP_ARENA_AUTOPLAN"])
        self.assertEqual("1", profile["effective_env"]["ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN"])
        self.assertEqual("0", profile["effective_env"]["ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS"])
        self.assertEqual(
            2400,
            int(profile["effective_env"]["ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY"]),
        )
        self.assertNotIn("ANTFLY_CAPTURE_FORCE_KV_CAPACITY", profile["effective_env"])

        override_args = argparse.Namespace(
            backend="cuda",
            output_tokens=300,
            antfly_env=["ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS=64"],
            antfly_server_prefix="",
            llama_server_prefix="",
        )
        overridden = benchmark.resolve_antfly_execution_profile(
            override_args,
            fixture,
            {"ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY": "auto"},
        )
        self.assertEqual("auto", overridden["effective_env"]["ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY"])
        self.assertEqual("process_environment", overridden["sources"]["ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY"])
        self.assertEqual("64", overridden["effective_env"]["ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS"])
        self.assertEqual("command_line", overridden["sources"]["ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS"])
        self.assertNotEqual(profile["sha256"], overridden["sha256"])

    def test_cuda_gqa_prefill_profile_rejects_noncanonical_values(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        namespace = argparse.Namespace(
            backend="cuda",
            output_tokens=300,
            antfly_env=["ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE=FAST"],
            antfly_server_prefix="",
            llama_server_prefix="",
        )
        with self.assertRaisesRegex(ValueError, "must be one of"):
            benchmark.resolve_antfly_execution_profile(namespace, fixture, {})

    def test_experimental_cuda_gqa_prefill_profiles_are_collect_only(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        for value in (
            "tiled-f16-exact",
            "required-tiled-f16-exact",
            "tiled-f16-warp",
            "required-tiled-f16-warp",
            "flash-f16-sm89",
            "required-flash-f16-sm89",
        ):
            with self.subTest(value=value):
                exploratory = argparse.Namespace(
                    backend="cuda",
                    output_tokens=300,
                    antfly_env=[f"ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE={value}"],
                    antfly_server_prefix="",
                    llama_server_prefix="",
                    enforce_performance=False,
                )
                profile = benchmark.resolve_antfly_execution_profile(exploratory, fixture, {})
                self.assertEqual(value, profile["effective_env"]["ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE"])

                enforced = argparse.Namespace(**vars(exploratory))
                enforced.enforce_performance = True
                with self.assertRaisesRegex(ValueError, "collect-only"):
                    benchmark.resolve_antfly_execution_profile(enforced, fixture, {})

    def test_versioned_warp_tiled64_profile_is_collect_only_and_fail_closed(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        namespace = argparse.Namespace(
            backend="cuda",
            output_tokens=300,
            antfly_env=[],
            antfly_server_prefix="",
            llama_server_prefix="",
            enforce_performance=False,
            cuda_execution_profile="gemma4-e2b-sm89-warp-tiled64-v1",
        )
        profile = benchmark.resolve_antfly_execution_profile(namespace, fixture, {})
        self.assertEqual(
            "antfly.gemma4_e2b_sm89_warp_tiled64_server.v1",
            profile["name"],
        )
        self.assertEqual(
            "required-tiled-f16-warp",
            profile["effective_env"]["ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE"],
        )
        self.assertEqual(
            "required-tiled64",
            profile["effective_env"][
                "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK_CONSUMER"
            ],
        )
        self.assertEqual("tiled64", profile["route_contract"]["decode"]["consumer"])
        benchmark.validate_headline_execution_profile(
            argparse.Namespace(profile="headline", enforce_performance=False, output_tokens=300),
            fixture,
            profile,
        )

        enforced_args = argparse.Namespace(**vars(namespace))
        enforced_args.enforce_performance = True
        with self.assertRaisesRegex(ValueError, "collect-only"):
            benchmark.resolve_antfly_execution_profile(enforced_args, fixture, {})

        overridden_args = argparse.Namespace(**vars(namespace))
        overridden_args.antfly_env = [
            "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK_CONSUMER=serial"
        ]
        overridden = benchmark.resolve_antfly_execution_profile(overridden_args, fixture, {})
        with self.assertRaisesRegex(ValueError, "requires .*required-tiled64"):
            benchmark.validate_headline_execution_profile(
                argparse.Namespace(profile="headline", enforce_performance=True, output_tokens=300),
                fixture,
                overridden,
            )

    def test_versioned_flash_tiled64_profile_is_collect_only_and_fail_closed(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        namespace = argparse.Namespace(
            backend="cuda",
            output_tokens=300,
            antfly_env=[],
            antfly_server_prefix="",
            llama_server_prefix="",
            enforce_performance=False,
            cuda_execution_profile="gemma4-e2b-sm89-flash-tiled64-v1",
        )
        profile = benchmark.resolve_antfly_execution_profile(namespace, fixture, {})
        self.assertEqual("antfly.gemma4_e2b_sm89_flash_tiled64_server.v1", profile["name"])
        self.assertEqual(
            "required-flash-f16-sm89",
            profile["effective_env"]["ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE"],
        )
        self.assertEqual(
            "required-tiled64",
            profile["effective_env"][
                "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK_CONSUMER"
            ],
        )
        self.assertEqual([3, 512], profile["route_contract"]["prefill"]["gemma4_query_lengths"])
        self.assertTrue(profile["route_contract"]["prefill"]["forbid_fallback"])
        benchmark.validate_headline_execution_profile(
            argparse.Namespace(profile="headline", enforce_performance=False, output_tokens=300),
            fixture,
            profile,
        )

        enforced_args = argparse.Namespace(**vars(namespace))
        enforced_args.enforce_performance = True
        with self.assertRaisesRegex(ValueError, "collect-only"):
            benchmark.resolve_antfly_execution_profile(enforced_args, fixture, {})

    def test_versioned_flash_splitk_profile_is_explicit_locked_and_collect_only(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        namespace = argparse.Namespace(
            backend="cuda",
            output_tokens=300,
            antfly_env=[],
            antfly_server_prefix="",
            llama_server_prefix="",
            enforce_performance=False,
            cuda_execution_profile="gemma4-e2b-sm89-flash-splitk-v1",
        )
        profile = benchmark.resolve_antfly_execution_profile(namespace, fixture, {})
        self.assertEqual("antfly.gemma4_e2b_sm89_flash_splitk_online_server.v1", profile["name"])
        for name, value in benchmark.GEMMA4_E2B_SM89_FLASH_COMMON_ENV.items():
            self.assertEqual(value, profile["effective_env"][name])
            self.assertEqual("reviewed_versioned_profile", profile["sources"][name])
        self.assertEqual(
            "required-splitk-online-sm89",
            profile["effective_env"]["ANTFLY_INFERENCE_CUDA_GQA_DECODE_PROFILE"],
        )
        self.assertEqual(
            "1",
            profile["effective_env"]["ANTFLY_INFERENCE_CUDA_Q4_0_WEIGHTS_BF16_PREFILL"],
        )
        self.assertEqual(
            "0",
            profile["effective_env"]["ANTFLY_INFERENCE_CUDA_DEQUANTIZE_Q4_0_MATRIX_WEIGHTS_BF16"],
        )
        self.assertEqual("0", profile["effective_env"]["ANTFLY_INFERENCE_CUDA_RMS_NORM_BF16_MIRROR"])
        self.assertEqual("0", profile["effective_env"]["ANTFLY_INFERENCE_CUDA_PLE_MODEL_PROJ_BF16"])
        self.assertEqual("2400", profile["effective_env"]["ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY"])
        self.assertEqual(19_000, profile["server_budget_mb"]["backend"])
        self.assertEqual("decode-splitk-online-sm89", profile["route_contract"]["decode"]["route"])

        for environment, override in (
            ({"ANTFLY_INFERENCE_CUDA_Q4_0_WEIGHTS_BF16_PREFILL": "0"}, []),
            ({}, ["ANTFLY_INFERENCE_CUDA_GQA_DECODE_PROFILE=off"]),
            ({"ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY": "2432"}, []),
        ):
            with self.subTest(environment=environment, override=override):
                changed = argparse.Namespace(**vars(namespace))
                changed.antfly_env = override
                with self.assertRaisesRegex(ValueError, "locked CUDA execution profile forbids overriding"):
                    benchmark.resolve_antfly_execution_profile(changed, fixture, environment)

        for environment, override in (
            ({"ANTFLY_UNREVIEWED_CUDA_TUNING": "1"}, []),
            ({"TERMITE_UNREVIEWED_CUDA_TUNING": "1"}, []),
            ({}, ["ANTFLY_UNREVIEWED_CUDA_TUNING=1"]),
        ):
            with self.subTest(unreviewed_environment=environment, unreviewed_override=override):
                changed = argparse.Namespace(**vars(namespace))
                changed.antfly_env = override
                with self.assertRaisesRegex(ValueError, "forbids unreviewed environment variable"):
                    benchmark.resolve_antfly_execution_profile(changed, fixture, environment)

        enforced = argparse.Namespace(**vars(namespace))
        enforced.enforce_performance = True
        with self.assertRaisesRegex(ValueError, "collect-only"):
            benchmark.resolve_antfly_execution_profile(enforced, fixture, {})

    def test_cuda_gqa_prefill_telemetry_parser_requires_exact_active_route(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            log_path = pathlib.Path(temporary) / "server.log"
            log_path.write_text(
                "info: cuda_gqa_prefill_profile: status=configured profile=required-fast source=canonical\n"
                "info: cuda_gqa_prefill_profile: status=active profile=required-fast "
                "route=prefill-fast batch=1 q_seq_len=512 num_heads=8 num_kv_heads=1 head_dim=256\n"
                '{"msg":"cuda_gqa_prefill_profile: status=active profile=required-fast '
                'route=prefill-fast batch=1 q_seq_len=512 num_heads=8 num_kv_heads=1 head_dim=512"}\n',
                encoding="utf-8",
            )
            telemetry = benchmark.parse_cuda_gqa_prefill_telemetry(log_path)
        self.assertEqual(["required-fast"], telemetry["configured_profiles"])
        self.assertEqual(["prefill-fast"], telemetry["active_routes"])
        self.assertEqual(2, telemetry["required_fast_active_count"])
        self.assertEqual([256, 512], telemetry["required_fast_gemma4_head_dims"])
        self.assertEqual(0, telemetry["rejected_count"])
        self.assertTrue(telemetry["required_fast_route_active"])

    def test_cuda_gqa_prefill_telemetry_resolves_candidate_route_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            log_path = pathlib.Path(temporary) / "server.log"
            log_path.write_text(
                "info: cuda_gqa_prefill_profile: status=configured "
                "profile=required-tiled-f16-warp source=canonical\n"
                "info: cuda_gqa_prefill_profile: status=active "
                "profile=required-tiled-f16-warp route=prefill-tiled-f16-warp "
                "batch=1 q_seq_len=512 num_heads=8 num_kv_heads=1 head_dim=256\n"
                "info: cuda_gqa_prefill_profile: status=active "
                "profile=required-tiled-f16-warp route=prefill-tiled-f16-warp "
                "batch=1 q_seq_len=512 num_heads=8 num_kv_heads=1 head_dim=512\n",
                encoding="utf-8",
            )
            telemetry = benchmark.parse_cuda_gqa_prefill_telemetry(log_path)
        contract = benchmark.CUDA_EXECUTION_PROFILES[
            "gemma4-e2b-sm89-warp-tiled64-v1"
        ]["route_contract"]["prefill"]
        observation = benchmark.cuda_gqa_prefill_route_observation(telemetry, contract)
        self.assertEqual(2, observation["active_count"])
        self.assertEqual([256, 512], observation["gemma4_head_dims"])

    def test_cuda_flash_prefill_telemetry_locks_head_and_query_buckets(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            log_path = pathlib.Path(temporary) / "server.log"
            rows = [
                "info: cuda_gqa_prefill_profile: status=configured "
                "profile=required-flash-f16-sm89 source=canonical",
            ]
            for head_dim in (256, 512):
                for q_seq_len in (512, 3):
                    rows.append(
                        "info: cuda_gqa_prefill_profile: status=active "
                        "profile=required-flash-f16-sm89 route=prefill-flash-f16-sm89 "
                        f"batch=1 q_seq_len={q_seq_len} num_heads=8 num_kv_heads=1 "
                        f"head_dim={head_dim}"
                    )
            log_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
            telemetry = benchmark.parse_cuda_gqa_prefill_telemetry(log_path)
        contract = benchmark.CUDA_EXECUTION_PROFILES[
            "gemma4-e2b-sm89-flash-tiled64-v1"
        ]["route_contract"]["prefill"]
        observation = benchmark.cuda_gqa_prefill_route_observation(telemetry, contract)
        self.assertEqual(4, observation["active_count"])
        self.assertEqual([256, 512], observation["gemma4_head_dims"])
        self.assertEqual([3, 512], observation["gemma4_query_lengths"])
        self.assertEqual(0, telemetry["fallback_count"])
        self.assertEqual(0, telemetry["rejected_count"])

    def test_cuda_gqa_score_prework_telemetry_is_bounded_and_route_exact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            log_path = pathlib.Path(temporary) / "server.log"
            log_path.write_text(
                "info: cuda_gqa_score_prework_consumer: event=configured "
                "configured=required-tiled64 consumer=none route=none head_dim=0 "
                "fallback_reason=none source=environment\n"
                "info: cuda_gqa_score_prework_consumer: event=active "
                "configured=required-tiled64 consumer=tiled64 route=gemma4_f16_local "
                "head_dim=256 fallback_reason=none source=runtime\n"
                "info: cuda_gqa_score_prework_consumer: event=active "
                "configured=required-tiled64 consumer=tiled64 route=gemma4_f16_global "
                "head_dim=512 fallback_reason=none source=runtime\n",
                encoding="utf-8",
            )
            telemetry = benchmark.parse_cuda_gqa_score_prework_telemetry(log_path)
        contract = benchmark.CUDA_EXECUTION_PROFILES[
            "gemma4-e2b-sm89-warp-tiled64-v1"
        ]["route_contract"]["decode"]
        observation = benchmark.cuda_gqa_score_prework_observation(telemetry, contract)
        self.assertEqual(["required-tiled64"], telemetry["configured_modes"])
        self.assertEqual("tiled64", observation["consumer"])
        self.assertEqual([256, 512], observation["gemma4_head_dims"])
        self.assertEqual(contract["routes_by_head_dim"], observation["routes_by_head_dim"])
        self.assertEqual(0, telemetry["fallback_count"])
        self.assertEqual(0, telemetry["rejected_count"])

    def test_cuda_splitk_decode_telemetry_enforces_required_profile_and_both_head_dims(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            log_path = pathlib.Path(temporary) / "server.log"
            log_path.write_text(
                "info: cuda_gqa_decode_profile: status=configured "
                "profile=required-splitk-online-sm89 source=canonical\n"
                "info: cuda_gqa_decode_profile: status=active "
                "profile=required-splitk-online-sm89 route=decode-splitk-online-sm89 "
                "reason=qualified-contract head_dim=256\n"
                "info: cuda_gqa_decode_profile: status=active "
                "profile=required-splitk-online-sm89 route=decode-splitk-online-sm89 "
                "reason=qualified-contract head_dim=512\n",
                encoding="utf-8",
            )
            telemetry = benchmark.parse_cuda_gqa_decode_telemetry(log_path)
        contract = benchmark.CUDA_EXECUTION_PROFILES[
            "gemma4-e2b-sm89-flash-splitk-v1"
        ]["route_contract"]["decode"]
        observation = benchmark.cuda_gqa_decode_route_observation(telemetry, contract)
        self.assertEqual(["required-splitk-online-sm89"], telemetry["configured_profiles"])
        self.assertEqual("decode-splitk-online-sm89", observation["route"])
        # Server logs are bounded route observations, not CLI launch counters:
        # one active event is retained for each qualified head dimension.
        self.assertEqual(2, telemetry["active_count"])
        self.assertEqual(2, observation["active_count"])
        self.assertEqual([256, 512], observation["gemma4_head_dims"])
        self.assertEqual(0, telemetry["fallback_count"])
        self.assertEqual(0, telemetry["rejected_count"])

    def test_profile_locks_material_environment_inherited_by_both_servers(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        namespace = argparse.Namespace(
            backend="cuda",
            output_tokens=300,
            antfly_env=[],
            antfly_server_prefix="",
            llama_server_prefix="",
        )
        material = {
            "ANTFLY_CUDA_DISABLE_FAST_GQA_DECODE": "1",
            "ANTFLY_CUDA_QMATMUL_VARIANT": "tile-128",
            "TERMITE_CUDA_CUBLASLT_WORKSPACE_MB": "256",
            "CUDA_VISIBLE_DEVICES": "0",
            "NVIDIA_VISIBLE_DEVICES": "all",
            "GGML_CUDA_FORCE_MMQ": "1",
            "LLAMA_ARG_N_GPU_LAYERS": "99",
            "UNRELATED_BENCHMARK_SETTING": "ignored",
        }
        profile = benchmark.resolve_antfly_execution_profile(namespace, fixture, material)
        for name, value in material.items():
            if name == "UNRELATED_BENCHMARK_SETTING":
                self.assertNotIn(name, profile["effective_env"])
                self.assertNotIn(name, profile["llama_cpp_inherited_env"])
                continue
            self.assertEqual(value, profile["effective_env"][name])
            self.assertEqual(value, profile["llama_cpp_inherited_env"][name])
            self.assertEqual("process_environment", profile["sources"][name])

        changed = benchmark.resolve_antfly_execution_profile(
            namespace,
            fixture,
            {**material, "TERMITE_CUDA_CUBLASLT_WORKSPACE_MB": "512"},
        )
        self.assertNotEqual(profile["material_environment_sha256"], changed["material_environment_sha256"])
        self.assertNotEqual(profile["sha256"], changed["sha256"])

    def test_headline_rejects_overridden_required_graph_profile(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        resolve_args = argparse.Namespace(
            backend="cuda",
            output_tokens=300,
            antfly_env=[],
            antfly_server_prefix="",
            llama_server_prefix="",
        )
        valid = benchmark.resolve_antfly_execution_profile(resolve_args, fixture, {})
        gate_args = argparse.Namespace(profile="headline", enforce_performance=True, output_tokens=300)
        benchmark.validate_headline_execution_profile(gate_args, fixture, valid)

        overridden = benchmark.resolve_antfly_execution_profile(
            resolve_args,
            fixture,
            {"ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY": "auto"},
        )
        with self.assertRaisesRegex(ValueError, "DECODE_GRAPH_REPLAY=required"):
            benchmark.validate_headline_execution_profile(gate_args, fixture, overridden)

        legacy = benchmark.resolve_antfly_execution_profile(
            resolve_args,
            fixture,
            {"ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD": "853"},
        )
        with self.assertRaisesRegex(ValueError, "legacy fixed CUDA temp-slot period"):
            benchmark.validate_headline_execution_profile(gate_args, fixture, legacy)

        instrumented = benchmark.resolve_antfly_execution_profile(
            resolve_args,
            fixture,
            {"ANTFLY_INFERENCE_CUDA_PROFILE_PREFILL_OPS": "1"},
        )
        with self.assertRaisesRegex(ValueError, "synchronization-heavy timing instrumentation"):
            benchmark.validate_headline_execution_profile(gate_args, fixture, instrumented)

    def test_profile_redacts_sensitive_values_in_provenance_without_weakening_hash(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        namespace = argparse.Namespace(
            backend="cuda",
            output_tokens=300,
            antfly_env=[],
            antfly_server_prefix="",
            llama_server_prefix="",
        )
        profile = benchmark.resolve_antfly_execution_profile(
            namespace,
            fixture,
            {"NVIDIA_API_KEY": "do-not-print"},
        )
        public = benchmark.execution_profile_provenance(profile)
        self.assertEqual(
            {"redacted": True, "sha256": benchmark.sha256_bytes(b"do-not-print")},
            public["llama_cpp_inherited_env"]["NVIDIA_API_KEY"],
        )
        self.assertNotIn("do-not-print", json.dumps(public))

    def test_profile_hash_and_lock_change_with_either_server_prefix(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)

        def profile(antfly_prefix: str = "", llama_prefix: str = "") -> tuple[argparse.Namespace, dict]:
            namespace = argparse.Namespace(
                backend="cuda",
                cache_dtype="f16",
                context_size=4096,
                output_tokens=300,
                antfly_env=[],
                antfly_server_prefix=antfly_prefix,
                llama_server_prefix=llama_prefix,
            )
            resolved = benchmark.resolve_antfly_execution_profile(namespace, fixture, {})
            namespace.antfly_execution_profile = resolved
            return namespace, resolved

        _, base = profile()
        _, antfly = profile("numactl --membind=0")
        llama_args, llama = profile(llama_prefix="taskset -c 0-3")
        self.assertNotEqual(base["sha256"], antfly["sha256"])
        self.assertNotEqual(base["sha256"], llama["sha256"])
        self.assertNotEqual(
            base["server_prefix_sha256"]["llama_cpp"],
            llama["server_prefix_sha256"]["llama_cpp"],
        )
        lock = benchmark.build_lock(
            llama_args,
            fixture,
            {
                "model": {"sha256": "model"},
                "antfly_model_bundle": {"sha256": "model-bundle"},
                "llama_cpp_binary": {"sha256": "llama", "git": {}},
                "runtime_identity": {"sha256": "runtime"},
                "harness": {"sha256": "harness"},
            },
        )
        self.assertEqual(
            llama["server_prefix_sha256"]["llama_cpp"],
            lock["llama_cpp_server_prefix_sha256"],
        )
        self.assertEqual(llama["material_environment_sha256"], lock["material_environment_sha256"])

    def test_antfly_server_config_locks_512_row_idle_prefill_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            namespace = argparse.Namespace(
                models_dir=root,
                antfly_bin=pathlib.Path(__file__),
                antfly_server_prefix="",
            )
            spec = benchmark.make_server_spec(namespace, "antfly", 19000, root)
            config = json.loads((root / "antfly-server.json").read_text())
            self.assertEqual(
                512,
                config["generation_batching"]["max_idle_prefill_chunk_size"],
            )
            for name, value in benchmark.CUDA_SERVER_BUDGET_MB.items():
                flag = f"--{name}-budget-mb"
                self.assertIn(flag, spec.command)
                self.assertEqual(str(value), spec.command[spec.command.index(flag) + 1])

    def test_llama_server_uses_the_locked_public_final_template(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            model = root / "model.gguf"
            model.write_bytes(b"model")
            namespace = argparse.Namespace(
                llama_server_bin=pathlib.Path(__file__),
                llama_server_prefix="",
                model=model,
                context_size=4096,
                cache_dtype="f16",
            )
            spec = benchmark.make_server_spec(namespace, "llama_cpp", 19001, root)
            template = root / "gemma4-public-final-chat-template.jinja"
            self.assertEqual(benchmark.LLAMA_CHAT_TEMPLATE, template.read_text())
            self.assertLess(spec.command.index("--jinja"), spec.command.index("--chat-template-file"))
            self.assertEqual(
                str(template.resolve()),
                spec.command[spec.command.index("--chat-template-file") + 1],
            )


class StatisticsAndGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = benchmark.load_fixture(FIXTURE)

    def test_headline_accepts_statistically_stable_ten_percent_win(self) -> None:
        evidence = benchmark.evaluate(args(), self.fixture, rows(), None)
        self.assertTrue(evidence["passed"])
        self.assertLess(evidence["comparison"]["paired_bootstrap_95_ci"]["upper_95"], 1.0)
        paired_log_ci = evidence["comparison"]["paired_log_ratio_95_ci"]
        self.assertLess(paired_log_ci["total_latency_ms"]["upper_95"], 1.0)
        self.assertEqual(
            "median_paired_log_ratio",
            paired_log_ci["decode_ms"]["estimator"],
        )
        self.assertLessEqual(evidence["comparison"]["antfly_llama_median_total_ratio"], 0.95)

    def test_headline_rejects_less_than_five_percent_win(self) -> None:
        evidence = benchmark.evaluate(args(), self.fixture, rows(96.0, 100.0), None)
        self.assertFalse(evidence["passed"])
        failed = {item["name"] for item in evidence["checks"] if item["enforced"] and not item["passed"]}
        self.assertIn("headline_median_total_ratio", failed)

    def test_headline_ttft_paired_ci_rejects_tail_regressions_hidden_by_median(self) -> None:
        measured = []
        for index in range(1, 11):
            antfly_ttft = 19.0 if index <= 6 else 25.0
            measured.append({
                "pair": index,
                "order": list(benchmark.paired_order(index)),
                "antfly": sample("antfly", 90.0, antfly_ttft),
                "llama_cpp": sample("llama_cpp", 100.0, 20.0),
            })
        evidence = benchmark.evaluate(args(), self.fixture, measured, None)
        self.assertLessEqual(
            evidence["comparison"]["antfly_llama_median_ttft_ratio"],
            1.02,
        )
        self.assertGreater(
            evidence["comparison"]["paired_log_ratio_95_ci"]["ttft_ms"]["upper_95"],
            1.02,
        )
        failed = {item["name"] for item in evidence["checks"] if item["enforced"] and not item["passed"]}
        self.assertIn("headline_ttft_paired_ci", failed)

    def test_headline_decode_paired_ci_rejects_tail_regressions_hidden_by_median(self) -> None:
        measured = []
        for index in range(1, 11):
            antfly_decode = 75.0 if index <= 6 else 85.0
            measured.append({
                "pair": index,
                "order": list(benchmark.paired_order(index)),
                "antfly": sample("antfly", 90.0, 90.0 - antfly_decode),
                "llama_cpp": sample("llama_cpp", 100.0, 20.0),
            })
        evidence = benchmark.evaluate(args(), self.fixture, measured, None)
        self.assertLessEqual(
            evidence["comparison"]["antfly_llama_median_decode_latency_ratio"],
            1.02,
        )
        self.assertGreater(
            evidence["comparison"]["paired_log_ratio_95_ci"]["decode_ms"]["upper_95"],
            1.02,
        )
        failed = {item["name"] for item in evidence["checks"] if item["enforced"] and not item["passed"]}
        self.assertIn("headline_decode_paired_ci", failed)

    def test_performance_threshold_contract_is_machine_readable(self) -> None:
        contract = benchmark.performance_threshold_contract(args())
        self.assertEqual(
            "antfly.gemma4_long_e2e.performance_thresholds.v1",
            contract["schema"],
        )
        self.assertEqual(
            1.02,
            contract["antfly_llama"][
                "paired_ttft_log_ratio_95_ci_upper_max_inclusive"
            ],
        )
        self.assertEqual(
            1.02,
            contract["antfly_llama"][
                "paired_decode_log_ratio_95_ci_upper_max_inclusive"
            ],
        )

    def test_headline_requires_terminal_sse_usage_from_both_engines(self) -> None:
        measured = rows()
        measured[0]["antfly"]["completion_token_accounting_source"] = "content_events"
        evidence = benchmark.evaluate(args(), self.fixture, measured, None)
        self.assertFalse(evidence["passed"])
        failed = {item["name"] for item in evidence["checks"] if item["enforced"] and not item["passed"]}
        self.assertIn("pair_1_antfly_authoritative_completion_usage", failed)

    def test_headline_requires_required_fast_gqa_prefill_route_evidence(self) -> None:
        measured = rows()
        measured[0]["antfly"]["cuda_gqa_prefill_telemetry"]["required_fast_gemma4_head_dims"] = [256]
        measured[0]["antfly"]["cuda_gqa_prefill_telemetry"]["required_fast_route_active"] = False
        evidence = benchmark.evaluate(args(), self.fixture, measured, None)
        self.assertFalse(evidence["passed"])
        failed = {item["name"] for item in evidence["checks"] if item["enforced"] and not item["passed"]}
        self.assertIn("pair_1_antfly_required_fast_gqa_prefill", failed)

    def test_headline_requires_exact_decode_consumer_route_evidence(self) -> None:
        measured = rows()
        measured[0]["antfly"]["cuda_gqa_score_prework_telemetry"]["fallback_count"] = 1
        measured[0]["antfly"]["cuda_gqa_score_prework_telemetry"]["fallback_reasons"] = [
            "consumer_symbol_unavailable"
        ]
        evidence = benchmark.evaluate(args(), self.fixture, measured, None)
        self.assertFalse(evidence["passed"])
        failed = {item["name"] for item in evidence["checks"] if item["enforced"] and not item["passed"]}
        self.assertIn("pair_1_antfly_gqa_score_prework_consumer_contract", failed)

    def test_correctness_failures_remain_gating_in_collect_only_mode(self) -> None:
        broken = rows()
        broken[0]["antfly"]["completion_tokens"] = 299
        evidence = benchmark.evaluate(args(enforce_performance=False), self.fixture, broken, None)
        self.assertFalse(evidence["passed"])
        self.assertTrue(any(item["name"].endswith("completion_tokens") and not item["passed"] for item in evidence["checks"]))

    def test_empty_or_semantically_wrong_output_is_rejected(self) -> None:
        empty = rows()
        empty[0]["antfly"]["content"] = ""
        empty[0]["antfly"]["content_utf8_bytes"] = 0
        evidence = benchmark.evaluate(args(enforce_performance=False), self.fixture, empty, None)
        self.assertFalse(evidence["passed"])
        failed = {item["name"] for item in evidence["checks"] if item["enforced"] and not item["passed"]}
        self.assertIn("pair_1_antfly_visible_content", failed)
        self.assertIn("pair_1_antfly_semantic_output", failed)

        wrong = rows()
        wrong[0]["llama_cpp"]["content"] = "A deterministic but unrelated answer."
        wrong[0]["llama_cpp"]["content_utf8_bytes"] = len(wrong[0]["llama_cpp"]["content"])
        evidence = benchmark.evaluate(args(enforce_performance=False), self.fixture, wrong, None)
        self.assertFalse(evidence["passed"])
        self.assertTrue(any(
            item["name"] == "pair_1_llama_cpp_semantic_output" and not item["passed"]
            for item in evidence["checks"]
        ))

    def test_e2b_regression_uses_frozen_antfly_baseline(self) -> None:
        baseline = benchmark.evaluate(args(), self.fixture, rows(), None)
        current = rows(92.0, 100.0)
        accepted = benchmark.evaluate(
            args(profile="e2b-regression", max_baseline_regression=1.03), self.fixture, current, baseline,
        )
        rejected = benchmark.evaluate(
            args(profile="e2b-regression", max_baseline_regression=1.02), self.fixture, current, baseline,
        )
        self.assertTrue(accepted["passed"])
        self.assertFalse(rejected["passed"])

    def test_baseline_profile_rejects_noisy_candidate_samples(self) -> None:
        baseline = benchmark.evaluate(args(), self.fixture, rows(), None)
        noisy = rows()
        noisy[0]["antfly"]["total_latency_ms"] = 150.0
        evidence = benchmark.evaluate(
            args(profile="e2b-regression", max_baseline_regression=1.03),
            self.fixture,
            noisy,
            baseline,
        )
        self.assertFalse(evidence["passed"])
        self.assertTrue(any(
            item["name"] == "baseline_profile_antfly_latency_cv" and not item["passed"]
            for item in evidence["checks"]
        ))

    def test_f32_control_requires_baseline_when_enforced(self) -> None:
        evidence = benchmark.evaluate(
            args(profile="f32-control", max_baseline_regression=1.05), self.fixture, rows(), None,
        )
        self.assertFalse(evidence["passed"])
        self.assertTrue(any(item["name"] == "frozen_antfly_baseline" and not item["passed"] for item in evidence["checks"]))

    def test_bootstrap_is_reproducible(self) -> None:
        first = benchmark.paired_bootstrap_ratio_ci(rows(), samples=200, seed=19)
        second = benchmark.paired_bootstrap_ratio_ci(rows(), samples=200, seed=19)
        self.assertEqual(first, second)

    def test_distribution_schema_has_one_canonical_mean_field(self) -> None:
        result = benchmark.distribution([1.0, 2.0, 3.0])
        self.assertEqual({"min", "median", "mean", "p95", "max", "cv"}, set(result))
        self.assertEqual(2.0, result["mean"])


class LockTests(unittest.TestCase):
    def test_harness_identity_covers_shared_pairing_and_manifest_code(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            script = root / "benchmark_gemma4_long_e2e_server.py"
            support = root / "paired_benchmark.py"
            script.write_text("benchmark-v1\n", encoding="utf-8")
            support.write_text("pairing-v1\n", encoding="utf-8")
            first = benchmark.harness_provenance(script)
            support.write_text("pairing-v2\n", encoding="utf-8")
            second = benchmark.harness_provenance(script)
            self.assertNotEqual(first["sha256"], second["sha256"])
            self.assertEqual(
                hashlib.sha256(b"pairing-v2\n").hexdigest(),
                second["files"]["pairing_support"]["sha256"],
            )

    def test_model_bundle_hash_covers_nonhidden_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            model_dir = pathlib.Path(temporary) / "model"
            model_dir.mkdir()
            decoder = model_dir / "model.gguf"
            projector = model_dir / "mmproj-model.gguf"
            tokenizer = model_dir / "tokenizer.json"
            hidden = model_dir / ".download-state"
            decoder.write_bytes(b"decoder")
            projector.write_bytes(b"projector-v1")
            tokenizer.write_bytes(b"tokenizer")
            hidden.write_bytes(b"ignored-v1")

            decoder_provenance = benchmark.path_provenance(decoder)
            first = benchmark.antfly_model_bundle_provenance(decoder, decoder_provenance)
            self.assertEqual(
                ["mmproj-model.gguf", "model.gguf", "tokenizer.json"],
                [item["path"] for item in first["files"]],
            )
            self.assertEqual(
                decoder_provenance["sha256"],
                next(item["sha256"] for item in first["files"] if item["path"] == "model.gguf"),
            )

            hidden.write_bytes(b"ignored-v2")
            hidden_only = benchmark.antfly_model_bundle_provenance(decoder, decoder_provenance)
            self.assertEqual(first["sha256"], hidden_only["sha256"])
            self.assertTrue(any("hidden" in error for error in hidden_only["layout_errors"]))

            projector.write_bytes(b"projector-version-two")
            self.assertTrue(benchmark.model_bundle_stat_errors(decoder, first))
            changed = benchmark.antfly_model_bundle_provenance(decoder, decoder_provenance)
            self.assertNotEqual(first["sha256"], changed["sha256"])

    def test_enforced_model_bundle_rejects_unresolved_runtime_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            model_dir = pathlib.Path(temporary) / "model"
            model_dir.mkdir()
            decoder = model_dir / "model.gguf"
            decoder.write_bytes(b"decoder")
            (model_dir / "model_manifest.json").write_text('{"model_path":"/external/model.gguf"}')
            (model_dir / "linked-assets").symlink_to(pathlib.Path(temporary), target_is_directory=True)
            bundle = benchmark.antfly_model_bundle_provenance(
                decoder,
                benchmark.path_provenance(decoder),
            )
            self.assertTrue(any("runtime-path manifest" in error for error in bundle["layout_errors"]))
            self.assertTrue(any("symlinked" in error for error in bundle["layout_errors"]))

            namespace = argparse.Namespace(enforce_performance=True, backend="cuda")
            provenance = {
                "antfly_model_bundle": bundle,
                "runtime_identity": {
                    "gpu_execution_state": {"error": None, "selected_gpus": [{"uuid": "GPU-test"}]},
                    "cuda_toolchain": "cuda-toolchain",
                },
                "gpu_compute_processes_before_benchmark": {
                    "error": None,
                    "selected_gpu_processes": [],
                },
                "llama_cpp_binary": {
                    "dynamic_dependencies": {
                        "is_elf": True,
                        "status": "static",
                        "unresolved_dependencies": [],
                        "material_dependency_files": [],
                    },
                    "runtime_bundle_sha256": "llama-runtime",
                    "build_metadata": "llama-build",
                },
            }
            errors = benchmark.provenance_validation_errors(namespace, provenance)
            self.assertTrue(any("runtime-path manifest" in error for error in errors))
            self.assertTrue(any("symlinked" in error for error in errors))

    def test_gpu_state_is_structured_and_scoped_to_cuda_visible_devices(self) -> None:
        raw = "\n".join((
            "0, GPU-first, NVIDIA L4, 580.159.03, 8.9, 23034, Enabled, 72.00, 2040, 6251, 2040, 6251, [N/A]",
            "1, GPU-second, NVIDIA L4, 580.159.03, 8.9, 23034, Disabled, 72.00, 2040, 6251, 2040, 6251, Disabled",
        ))
        state = benchmark.parse_gpu_execution_state(raw, "1")
        self.assertEqual(["GPU-second"], [gpu["uuid"] for gpu in state["selected_gpus"]])
        self.assertEqual(72.0, state["selected_gpus"][0]["power.limit"])

        with self.assertRaisesRegex(ValueError, "power.limit is unavailable"):
            benchmark.parse_gpu_execution_state(raw.replace("72.00", "N/A", 1), "0")
        with self.assertRaisesRegex(ValueError, "does not resolve uniquely"):
            benchmark.parse_gpu_execution_state(raw, "2")
        with self.assertRaisesRegex(ValueError, "expected 13"):
            benchmark.parse_gpu_execution_state("0, GPU-broken", "0")

    def test_gpu_process_checks_ignore_unselected_gpus_and_catch_midrun_jobs(self) -> None:
        gpu_state = {
            "error": None,
            "cuda_visible_devices": "0",
            "selected_gpus": [{"uuid": "GPU-selected"}],
        }
        process_rows = "\n".join((
            "GPU-other, 100, unrelated-job",
            "GPU-selected, 200, interfering-job",
        ))
        with mock.patch.object(benchmark, "command_output", return_value=process_rows):
            processes = benchmark.capture_selected_gpu_compute_processes(gpu_state)
        self.assertEqual([200], [process["pid"] for process in processes["selected_gpu_processes"]])

        with tempfile.TemporaryDirectory() as temporary:
            model_dir = pathlib.Path(temporary) / "model"
            model_dir.mkdir()
            model = model_dir / "model.gguf"
            model.write_bytes(b"decoder")
            provenance = {
                "runtime_identity": {"gpu_execution_state": gpu_state},
                "antfly_model_bundle": benchmark.antfly_model_bundle_provenance(
                    model,
                    benchmark.path_provenance(model),
                ),
            }
            namespace = argparse.Namespace(
                backend="cuda",
                enforce_performance=True,
                model=model,
            )
            with (
                mock.patch.object(benchmark, "capture_gpu_execution_state", return_value=gpu_state),
                mock.patch.object(
                    benchmark,
                    "capture_selected_gpu_compute_processes",
                    return_value={
                        "error": None,
                        "selected_gpu_processes": [
                            {"gpu_uuid": "GPU-selected", "pid": 200, "process_name": "interfering-job"}
                        ],
                    },
                ),
            ):
                guard = benchmark.capture_runtime_guard(namespace, provenance, "mid-run")
            self.assertTrue(any("not idle" in error for error in benchmark.runtime_guard_errors(namespace, guard)))

    def test_enforced_provenance_requires_idle_gpu_and_stable_execution_state(self) -> None:
        namespace = argparse.Namespace(enforce_performance=True, backend="cuda")
        provenance = {
            "antfly_model_bundle": {"sha256": "bundle", "files": [{"path": "model.gguf"}]},
            "runtime_identity": {
                "gpu_execution_state": {
                    "error": None,
                    "selected_gpus": [{"uuid": "GPU-test"}],
                },
                "cuda_toolchain": "cuda-toolchain",
            },
            "gpu_compute_processes_before_benchmark": {
                "error": None,
                "selected_gpu_processes": [],
            },
            "llama_cpp_binary": {
                "dynamic_dependencies": {
                    "is_elf": True,
                    "status": "static",
                    "unresolved_dependencies": [],
                    "material_dependency_files": [],
                },
                "runtime_bundle_sha256": "llama-runtime",
                "build_metadata": "llama-build",
            },
        }
        self.assertEqual([], benchmark.provenance_validation_errors(namespace, provenance))

        busy = {
            **provenance,
            "gpu_compute_processes_before_benchmark": {
                "error": None,
                "selected_gpu_processes": [{"gpu_uuid": "GPU-uuid", "pid": 123, "name": "other-job"}],
            },
        }
        errors = benchmark.provenance_validation_errors(namespace, busy)
        self.assertTrue(any("idle GPU" in error for error in errors))

        missing_state = {
            **provenance,
            "runtime_identity": {"cuda_toolchain": "cuda-toolchain"},
        }
        errors = benchmark.provenance_validation_errors(namespace, missing_state)
        self.assertTrue(any("power/clock/MIG" in error for error in errors))

    def test_llama_build_metadata_excludes_nondeterministic_startup_log_prefix(self) -> None:
        first = benchmark.stable_executable_build_metadata(
            "0.00.019 E CUDA init diagnostic\nversion: 1 (abc123)\nbuilt with GNU 12 for Linux"
        )
        second = benchmark.stable_executable_build_metadata(
            "0.00.227 E CUDA init diagnostic\nversion: 1 (abc123)\nbuilt with GNU 12 for Linux"
        )
        self.assertEqual(first, second)
        self.assertEqual("version: 1 (abc123)\nbuilt with GNU 12 for Linux", first)

    def test_dynamic_dependency_closure_hashes_material_shared_objects(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            binary = root / "llama-server"
            library = root / "libggml-cuda.so.0"
            binary.write_bytes(b"\x7fELFfake")
            library.write_bytes(b"first-build")
            ldd_output = (
                f"libggml-cuda.so.0 => {library} (0x1)\n"
                "libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x2)\n"
            )
            inspection = {"returncode": 0, "output": ldd_output, "error": None}
            with mock.patch.object(benchmark, "command_capture", return_value=inspection):
                first = benchmark.dynamic_dependency_provenance(binary)
                library.write_bytes(b"second-build")
                second = benchmark.dynamic_dependency_provenance(binary)
            self.assertEqual("resolved", first["status"])
            self.assertEqual(["libggml-cuda.so.0"], [item["name"] for item in first["material_dependency_files"]])
            self.assertNotEqual(first["sha256"], second["sha256"])

    def test_ldd_parser_reports_unresolved_dependencies(self) -> None:
        resolved, unresolved = benchmark.parse_ldd_dependencies(
            "libllama.so.0 => /opt/llama/libllama.so.0 (0x1)\nlibmissing.so => not found\n"
        )
        self.assertEqual(["libllama.so.0"], [name for name, _ in resolved])
        self.assertEqual(["libmissing.so"], unresolved)

    def test_lock_drift_names_exact_field(self) -> None:
        observed = {
            "schema": benchmark.LOCK_SCHEMA,
            "model_sha256": "model-a",
            "llama_cpp_commit": "commit-a",
        }
        expected = {**observed, "llama_cpp_commit": "commit-b"}
        errors = benchmark.lock_errors(expected, observed)
        self.assertEqual(1, len(errors))
        self.assertIn("llama_cpp_commit", errors[0])

    def test_baseline_rejects_model_or_precision_drift(self) -> None:
        current = {
            "model_sha256": "model-a",
            "fixture_id": "fixture",
            "fixture_file_sha256": "fixture-file",
            "rendered_prompt_sha256": "prompt",
            "backend": "cuda",
            "cache_dtype": "f16",
            "context_size": 4096,
            "output_tokens": 300,
        }
        baseline = {
            "schema": benchmark.EVIDENCE_SCHEMA,
            "lock": {**current, "model_sha256": "model-b", "cache_dtype": "f32"},
        }
        errors = benchmark.baseline_compatibility_errors(baseline, current)
        self.assertEqual(2, len(errors))
        self.assertTrue(any("model_sha256" in error for error in errors))
        self.assertTrue(any("cache_dtype" in error for error in errors))

    def test_baseline_compatibility_locks_runtime_and_llama_bundle(self) -> None:
        current = {
            "runtime_identity_sha256": "runtime-current",
            "llama_cpp_runtime_bundle_sha256": "bundle-current",
        }
        baseline = {
            "schema": benchmark.EVIDENCE_SCHEMA,
            "lock": {
                "runtime_identity_sha256": "runtime-old",
                "llama_cpp_runtime_bundle_sha256": "bundle-old",
            },
        }
        errors = benchmark.baseline_compatibility_errors(baseline, current)
        self.assertTrue(any("runtime_identity_sha256" in error for error in errors))
        self.assertTrue(any("llama_cpp_runtime_bundle_sha256" in error for error in errors))

    def test_frozen_baseline_quality_fails_closed(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        valid = frozen_baseline(fixture)
        self.assertEqual([], benchmark.baseline_quality_errors(valid, "e2b-regression"))

        invalid = frozen_baseline(
            fixture,
            profile="f32-control",
            passed=False,
            sample_rows=rows()[:9],
        )
        invalid["metrics"]["antfly"]["total_latency_ms"]["cv"] = 0.5
        errors = benchmark.baseline_quality_errors(invalid, "e2b-regression")
        self.assertTrue(any("passed" in error for error in errors))
        self.assertTrue(any("profile mismatch" in error for error in errors))
        self.assertTrue(any("at least 10" in error for error in errors))
        self.assertTrue(any("CV" in error for error in errors))

    def test_frozen_baseline_lock_must_match_profile_provenance(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        tampered = frozen_baseline(fixture)
        profile = tampered["provenance"]["antfly_execution_profile"]
        profile["sha256"] = "tampered-profile"
        profile["server_budget_mb"]["backend"] += 1
        tampered["provenance_sha256"] = benchmark.canonical_sha256(tampered["provenance"])

        errors = benchmark.baseline_quality_errors(tampered, "e2b-regression")
        self.assertTrue(any("antfly_execution_profile_sha256" in error for error in errors))
        self.assertTrue(any("antfly_server_budget_mb" in error for error in errors))

    def test_regression_enforcement_requires_pinned_baseline_sha256(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            antfly = root / "antfly"
            llama = root / "llama-server"
            model = root / "benchmark-model" / "model.gguf"
            baseline = root / "baseline.json"
            lock = root / "benchmark.lock.json"
            model.parent.mkdir()
            for path in (antfly, llama, model, baseline, lock):
                path.write_bytes(b"fixture")
            namespace = benchmark.parse_args([
                "--antfly-bin", str(antfly),
                "--llama-server-bin", str(llama),
                "--model", str(model),
                "--models-dir", str(root),
                "--profile", "e2b-regression",
                "--baseline-evidence", str(baseline),
                "--lockfile", str(lock),
                "--require-lock",
            ])
            with self.assertRaisesRegex(ValueError, "requires --baseline-sha256"):
                benchmark.validate_args(namespace, fixture)

            namespace.baseline_sha256 = "A" * 64
            with self.assertRaisesRegex(ValueError, "64 lowercase hexadecimal"):
                benchmark.validate_args(namespace, fixture)

    def test_supplied_baseline_bytes_must_match_pinned_sha256(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            antfly = root / "antfly"
            llama = root / "llama-server"
            model = root / "benchmark-model" / "model.gguf"
            baseline = root / "baseline.json"
            output = root / "candidate"
            model.parent.mkdir()
            for path in (antfly, llama):
                path.write_bytes(b"not-an-elf")
            model.write_bytes(b"model")
            baseline.write_text("{}")
            with self.assertRaisesRegex(SystemExit, "frozen baseline SHA-256 mismatch"):
                benchmark.main([
                    "--antfly-bin", str(antfly),
                    "--llama-server-bin", str(llama),
                    "--model", str(model),
                    "--models-dir", str(root),
                    "--output-dir", str(output),
                    "--collect-only",
                    "--warmups", "1",
                    "--repeats", "1",
                    "--bootstrap-samples", "1",
                    "--baseline-evidence", str(baseline),
                    "--baseline-sha256", "0" * 64,
                ])

    def test_reviewed_lock_bytes_must_match_pinned_sha256(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            antfly = root / "antfly"
            llama = root / "llama-server"
            model = root / "benchmark-model" / "model.gguf"
            lock = root / "benchmark.lock.json"
            output = root / "candidate"
            model.parent.mkdir()
            for path in (antfly, llama):
                path.write_bytes(b"not-an-elf")
            model.write_bytes(b"model")
            lock.write_text("{}")
            with self.assertRaisesRegex(SystemExit, "reviewed lockfile SHA-256 mismatch"):
                benchmark.main([
                    "--antfly-bin", str(antfly),
                    "--llama-server-bin", str(llama),
                    "--model", str(model),
                    "--models-dir", str(root),
                    "--output-dir", str(output),
                    "--collect-only",
                    "--warmups", "1",
                    "--repeats", "1",
                    "--bootstrap-samples", "1",
                    "--lockfile", str(lock),
                    "--lockfile-sha256", "0" * 64,
                ])

    def test_baseline_profile_threshold_cannot_be_loosened(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            antfly = root / "antfly"
            llama = root / "llama-server"
            model = root / "benchmark-model" / "model.gguf"
            baseline = root / "baseline.json"
            model.parent.mkdir()
            for path in (antfly, llama, model, baseline):
                path.write_bytes(b"fixture")
            namespace = benchmark.parse_args([
                "--antfly-bin", str(antfly),
                "--llama-server-bin", str(llama),
                "--model", str(model),
                "--models-dir", str(root),
                "--profile", "e2b-regression",
                "--baseline-evidence", str(baseline),
                "--max-baseline-regression", "1.04",
            ])
            with self.assertRaisesRegex(ValueError, "cannot be looser than 1.03"):
                benchmark.validate_args(namespace, fixture)

    def test_e4b_regression_profile_uses_f16_and_three_percent_ceiling(self) -> None:
        namespace = benchmark.parse_args([
            "--model", "/tmp/e4b.gguf",
            "--profile", "e4b-regression",
        ])
        self.assertEqual("f16", namespace.cache_dtype)
        self.assertEqual(1.03, namespace.max_baseline_regression)

    def test_headline_enforcement_rejects_opaque_prefix_for_either_server(self) -> None:
        fixture = benchmark.load_fixture(FIXTURE)
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            antfly = root / "antfly"
            llama = root / "llama-server"
            model = root / "benchmark-model" / "model.gguf"
            lock = root / "benchmark.lock.json"
            model.parent.mkdir()
            for path in (antfly, llama, model, lock):
                path.write_bytes(b"fixture")
            namespace = benchmark.parse_args([
                "--antfly-bin", str(antfly),
                "--llama-server-bin", str(llama),
                "--model", str(model),
                "--models-dir", str(root),
                "--lockfile", str(lock),
                "--lockfile-sha256", "0" * 64,
                "--require-lock",
                "--antfly-server-prefix", "",
                "--llama-server-prefix", "",
            ])
            for field, value, message in (
                ("antfly_server_prefix", "numactl --membind=0", "Antfly server prefix"),
                ("llama_server_prefix", "taskset -c 0-3", "llama.cpp server prefix"),
            ):
                setattr(namespace, field, value)
                with self.subTest(field=field), self.assertRaisesRegex(ValueError, message):
                    benchmark.validate_args(namespace, fixture)
                setattr(namespace, field, "")

            for field in ("max_ttft_ci_upper", "max_decode_ci_upper"):
                setattr(namespace, field, 1.03)
                with self.subTest(field=field), self.assertRaisesRegex(
                    ValueError,
                    field.replace("_", "-") + " cannot be looser than 1.02",
                ):
                    benchmark.validate_args(namespace, fixture)
                setattr(namespace, field, 1.02)

    def test_l4_e2e_requires_locked_long_e2e_for_release(self) -> None:
        runner = (SCRIPTS.parents[3] / "zig/e2e/inference/run_cuda_gemma4_l4_e2e.sh").read_text()
        lane = runner[
            runner.index('if [[ "$long_e2e_configured" -eq 3 ]]', runner.index('"${release_args[@]}"')):
            runner.index('if [[ -n "$e4b_qat_model" ]]', runner.index('"${release_args[@]}"'))
        ]
        e4b_lane = runner[
            runner.index('if [[ -n "$e4b_qat_model" ]]', runner.index('"${release_args[@]}"')):
            runner.index("# Nightly-mode MTP is collection-only")
        ]
        self.assertIn("long_e2e_configured", runner)
        self.assertIn('elif [[ "$mode" == "release" ]]', runner)
        self.assertIn("release mode requires LLAMA_SERVER_BIN", runner)
        self.assertIn("release mode requires E4B_QAT_MODEL and reviewed frozen E4B regression evidence", runner)
        self.assertIn("e4b_regression_configured", runner)
        self.assertIn("E4B_BASELINE_EVIDENCE, E4B_BASELINE_SHA256, E4B_REGRESSION_LOCK, and E4B_REGRESSION_LOCK_SHA256", runner)
        self.assertIn('require_sha256 "E4B_BASELINE_SHA256" "$e4b_baseline_sha256"', runner)
        self.assertIn('require_sha256 "E4B_REGRESSION_LOCK_SHA256" "$e4b_regression_lock_sha256"', runner)
        self.assertIn('e2b_models_dir="$(dirname "$(dirname "$e2b_model")")"', runner)
        self.assertIn('e4b_models_dir="$(dirname "$(dirname "$e4b_qat_model")")"', runner)
        self.assertIn("$long_e2e_lock_sha256", runner)
        self.assertIn(
            "LLAMA_SERVER_BIN, LONG_E2E_LOCK, and LONG_E2E_LOCK_SHA256 must be configured together",
            runner,
        )
        self.assertIn("--profile headline", lane)
        self.assertIn("--warmups 2", lane)
        self.assertIn("--repeats 10", lane)
        self.assertIn('--lockfile-sha256 "$long_e2e_lock_sha256"', lane)
        self.assertIn('--model "$e2b_model"', lane)
        self.assertIn('--models-dir "$e2b_models_dir"', lane)
        self.assertIn('benchmark_rc=0', lane)
        self.assertIn('|| benchmark_rc=$?', lane)
        self.assertIn("merge_gemma4_long_e2e_release_summary.py", lane)
        self.assertIn('"$benchmark_rc"', lane)
        self.assertIn("--require-lock", lane)
        self.assertIn('--model "$e4b_qat_model"', e4b_lane)
        self.assertIn('--models-dir "$e4b_models_dir"', e4b_lane)
        self.assertIn("--profile e4b-regression", e4b_lane)
        self.assertIn('if [[ "$e4b_regression_configured" -eq 4 ]]', e4b_lane)
        self.assertIn('--baseline-evidence "$e4b_baseline_evidence"', e4b_lane)
        self.assertIn('--baseline-sha256 "$e4b_baseline_sha256"', e4b_lane)
        self.assertIn('--lockfile "$e4b_regression_lock"', e4b_lane)
        self.assertIn('--lockfile-sha256 "$e4b_regression_lock_sha256"', e4b_lane)
        self.assertIn("--enforce-performance", e4b_lane)
        self.assertIn("--collect-only", e4b_lane)
        self.assertIn("args+=(--repeats 3 --collect-only)", e4b_lane)
        self.assertIn("benchmark_rc=0", e4b_lane)
        self.assertIn("|| benchmark_rc=$?", e4b_lane)
        self.assertIn("--lane-field e4b_regression", e4b_lane)


class HarnessIntegrationTests(unittest.TestCase):
    def test_collect_only_runs_both_warm_servers_and_writes_evidence(self) -> None:
        fake_source = r'''
            #!/usr/bin/env python3
            import http.server
            import json
            import sys
            import time

            if "--version" in sys.argv:
                print("fake-engine 1.0")
                raise SystemExit(0)
            if len(sys.argv) > 1 and sys.argv[1] == "generate":
                print("chat_template=true")
                print("prompt:")
                print("<bos>", end="")
                print("<|turn>user\n" + sys.argv[3] + "<turn|>\n<|turn>model\n<|channel>final\n<channel|>")
                print("prompt_token_ids: " + " ".join(str(value) for value in range(2051)))
                raise SystemExit(0)

            port = int(sys.argv[sys.argv.index("--port") + 1])
            segment = "You answer questions about indexed files using only evidence. Evidence: Spella Caffe Logo.pdf is in /Users/timkaye/Downloads. Spella Caffe Logo Two Color.pdf is in /Users/timkaye/Downloads. Ignore unrelated source code. "
            user_content = segment * 36 + "\n\nUsing only this evidence, write a detailed asset-location handoff of 220 to 260 words. Name both Spella files and their exact directory, distinguish the two variants, explain the evidence limitations, and give concise verification steps without inventing any other path."
            reference_prompt = "<|turn>user\n" + user_content + "<turn|>\n<|turn>model\n<|channel>final\n<channel|>"
            answer_prefix = "Spella Caffe Logo.pdf and Spella Caffe Logo Two Color.pdf are in /Users/timkaye/Downloads. "
            visible_text = (answer_prefix + "Verify the exact filenames against the indexed evidence. " * 30)[:900]

            class Handler(http.server.BaseHTTPRequestHandler):
                protocol_version = "HTTP/1.1"

                def log_message(self, *_):
                    pass

                def send_bytes(self, content_type, payload):
                    self.send_response(200)
                    self.send_header("content-type", content_type)
                    self.send_header("content-length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    self.wfile.flush()

                def do_GET(self):
                    if self.path not in ("/health", "/healthz"):
                        self.send_error(404)
                        return
                    self.send_bytes("text/plain", b"ok")

                def do_POST(self):
                    size = int(self.headers.get("content-length", "0"))
                    body = json.loads(self.rfile.read(size))
                    if self.path == "/apply-template":
                        expected_messages = [{"role": "user", "content": user_content}]
                        if body.get("messages") != expected_messages:
                            self.send_error(400, "apply-template did not receive measured messages")
                            return
                        if body.get("chat_template_kwargs") != {"enable_thinking": False}:
                            self.send_error(400, "apply-template did not disable thinking")
                            return
                        self.send_bytes(
                            "application/json",
                            json.dumps({"prompt": reference_prompt}).encode(),
                        )
                        return
                    if self.path == "/tokenize":
                        if (
                            body.get("content") != reference_prompt
                            or body.get("add_special") is not True
                            or body.get("parse_special") is not True
                        ):
                            self.send_error(400, "tokenize did not receive apply-template output")
                            return
                        self.send_bytes("application/json", json.dumps({"tokens": list(range(2051))}).encode())
                        return
                    if self.path in ("/ai/v1/generate", "/v1/chat/completions"):
                        expected_model = (
                            "benchmark-model"
                            if self.path == "/ai/v1/generate"
                            else sys.argv[sys.argv.index("-m") + 1]
                        )
                        if body.get("model") != expected_model:
                            self.send_error(400, "generation used the wrong model identifier")
                            return
                        if body.get("messages") != [{"role": "user", "content": user_content}]:
                            self.send_error(400, "generation did not receive measured messages")
                            return
                        if body.get("chat_template_kwargs") != {"enable_thinking": False}:
                            self.send_error(400, "generation did not disable thinking")
                            return
                        if body.get("max_tokens") != 300 or body.get("ignore_eos") is not True:
                            self.send_error(400, "generation contract differs")
                            return
                        cache_field = "cache_prompt" if self.path == "/v1/chat/completions" else "prompt_cache"
                        if body.get(cache_field) is not False:
                            self.send_error(400, "prompt cache was not disabled")
                            return
                        if body.get("stream") and body.get("stream_options") != {"include_usage": True}:
                            self.send_error(400, "stream usage was not requested")
                            return
                    if not body.get("stream"):
                        response = {
                            "choices": [{
                                "message": {"role": "assistant", "content": visible_text},
                                "finish_reason": "length",
                            }],
                            "usage": {"prompt_tokens": 2051, "completion_tokens": 300, "total_tokens": 2351},
                        }
                        self.send_bytes("application/json", json.dumps(response).encode())
                        return
                    chunks = []
                    for offset in range(0, len(visible_text), 3):
                        token_text = visible_text[offset:offset + 3]
                        event = {"choices": [{"delta": {"content": token_text}, "finish_reason": None}]}
                        chunks.append("data: " + json.dumps(event, separators=(",", ":")) + "\n\n")
                    # Keep enough already-flushed SSE comment data after the
                    # last token to prevent HTTPResponse's buffered reader from
                    # waiting for the delayed final event before yielding it.
                    chunks.append(":" + "padding" * 10000 + "\n\n")
                    token_payload = "".join(chunks).encode()
                    tail_chunks = []
                    finish = {"choices": [{"delta": {}, "finish_reason": "length"}]}
                    tail_chunks.append("data: " + json.dumps(finish, separators=(",", ":")) + "\n\n")
                    if body.get("stream_options") == {"include_usage": True}:
                        usage = {"choices": [], "usage": {
                            "prompt_tokens": 2051, "completion_tokens": 300, "total_tokens": 2351,
                        }}
                        tail_chunks.append("data: " + json.dumps(usage, separators=(",", ":")) + "\n\n")
                    tail_chunks.append("data: [DONE]\n\n")
                    tail_payload = "".join(tail_chunks).encode()
                    self.send_response(200)
                    self.send_header("content-type", "text/event-stream")
                    self.send_header("transfer-encoding", "chunked")
                    self.end_headers()

                    def write_chunk(payload):
                        self.wfile.write(f"{len(payload):x}\r\n".encode())
                        self.wfile.write(payload)
                        self.wfile.write(b"\r\n")
                        self.wfile.flush()

                    write_chunk(token_payload)
                    self.wfile.flush()
                    time.sleep(0.02)
                    write_chunk(tail_payload)
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()

            http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
        '''
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            fake = root / "fake-engine.py"
            fake.write_text(textwrap.dedent(fake_source).lstrip())
            fake.chmod(0o755)
            model = root / "benchmark-model" / "model.gguf"
            model.parent.mkdir()
            model.write_bytes(b"fake-model")
            output = root / "evidence"
            argv = [
                "--antfly-bin", str(fake),
                "--llama-server-bin", str(fake),
                "--model", str(model),
                "--models-dir", str(root),
                "--output-dir", str(output),
                "--collect-only",
                "--warmups", "1",
                "--repeats", "1",
                "--bootstrap-samples", "20",
                "--startup-timeout", "5",
                "--request-timeout", "5",
            ]
            with mock.patch("builtins.print"):
                benchmark.main(argv)

            evidence = json.loads((output / "evidence.json").read_text())
            self.assertEqual(benchmark.EVIDENCE_SCHEMA, evidence["schema"])
            self.assertTrue(evidence["passed"])
            thresholds = evidence["contract"]["performance_thresholds"]
            self.assertEqual(
                "antfly.gemma4_long_e2e.performance_thresholds.v1",
                thresholds["schema"],
            )
            self.assertFalse(thresholds["performance_enforced"])
            self.assertEqual(
                1.02,
                thresholds["antfly_llama"][
                    "paired_decode_log_ratio_95_ci_upper_max_inclusive"
                ],
            )
            self.assertEqual(["antfly", "llama_cpp"], evidence["rows"][0]["order"])
            self.assertEqual(300, evidence["rows"][0]["antfly"]["completion_tokens"])
            self.assertEqual(2051, evidence["rows"][0]["llama_cpp"]["prompt_tokens"])
            self.assertEqual("final_sse_usage", evidence["rows"][0]["antfly"]["completion_token_accounting_source"])
            self.assertEqual("final_sse_usage", evidence["rows"][0]["llama_cpp"]["completion_token_accounting_source"])
            self.assertEqual(
                "llama_cpp_apply_template",
                evidence["rows"][0]["llama_cpp"]["prompt_template"]["source"],
            )
            self.assertEqual(
                evidence["contract"]["reference_prompt_sha256"],
                evidence["rows"][0]["llama_cpp"]["prompt_template"]["sha256"],
            )
            # The terminal usage event must extend decode timing past the last
            # visible token. Do not assert a minimum wall-clock gap here: the
            # client can spend an arbitrary portion of the fake server's delay
            # parsing the already-buffered token events under CI load.
            self.assertGreater(
                evidence["rows"][0]["antfly"]["decode_ms"],
                evidence["rows"][0]["antfly"]["visible_content_ms"],
            )
            self.assertGreaterEqual(evidence["rows"][0]["antfly"]["stream_tail_ms"], 0.0)
            self.assertAlmostEqual(
                evidence["rows"][0]["antfly"]["total_latency_ms"],
                evidence["rows"][0]["antfly"]["ttft_ms"]
                + evidence["rows"][0]["antfly"]["decode_ms"]
                + evidence["rows"][0]["antfly"]["stream_tail_ms"],
                places=6,
            )
            self.assertTrue(evidence["rows"][0]["antfly"]["connection_reused"])
            profile = evidence["provenance"]["antfly_execution_profile"]
            self.assertEqual(benchmark.CUDA_PROFILE_NAME, profile["name"])
            self.assertEqual("required", profile["effective_env"]["ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY"])
            self.assertEqual(profile["sha256"], evidence["lock"]["antfly_execution_profile_sha256"])
            self.assertEqual(profile["material_environment_sha256"], evidence["lock"]["material_environment_sha256"])
            self.assertEqual(
                profile["server_prefix_sha256"]["llama_cpp"],
                evidence["lock"]["llama_cpp_server_prefix_sha256"],
            )
            self.assertEqual(
                evidence["provenance"]["llama_cpp_binary"]["runtime_bundle_sha256"],
                evidence["lock"]["llama_cpp_runtime_bundle_sha256"],
            )
            self.assertEqual(
                evidence["provenance"]["runtime_identity"]["sha256"],
                evidence["lock"]["runtime_identity_sha256"],
            )
            prompt_contract = json.loads((output / "prompt_token_contract.json").read_text())
            self.assertEqual(str(model.parent.resolve()), prompt_contract["model_directory"])
            self.assertEqual(4, len(evidence["provenance"]["runtime_guards"]))
            self.assertEqual(
                evidence["provenance"]["antfly_model_bundle"]["sha256"],
                evidence["provenance"]["antfly_model_bundle_post_benchmark"]["sha256"],
            )
            self.assertTrue((output / "raw/pair-01-antfly/stream_events.json").is_file())
            self.assertTrue((output / "raw/pair-01-llama_cpp/stream_events.json").is_file())
            self.assertTrue((output / "raw/pair-01-llama_cpp/llama_prompt_template.json").is_file())
            manifest = json.loads((output / "evidence_manifest.json").read_text())
            paths = {item["path"] for item in manifest["files"]}
            self.assertIn("evidence.json", paths)
            self.assertIn("samples.jsonl", paths)
            self.assertNotIn("evidence_manifest.json", paths)


if __name__ == "__main__":
    unittest.main()
