#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import subprocess
import tempfile
import unittest
from unittest import mock

import validate_gemma4_cuda_candidate as candidate_validator
import validate_gemma4_cuda_quality_candidate as quality


SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
SUITE_PATH = SCRIPT_DIR / "fixtures/gemma4_cuda_quality_suite_v1.json"


def custom_spec() -> candidate_validator.CandidateSpec:
    return candidate_validator.CandidateSpec(
        kernel_id="cuda.test.quality",
        environment_variable="ANTFLY_INFERENCE_CUDA_TEST_QUALITY",
        required_route_counters=(
            candidate_validator.RouteCounter("candidate_hd256", "candidate HD256"),
            candidate_validator.RouteCounter("candidate_hd512", "candidate HD512"),
        ),
        forbidden_route_counters=(
            candidate_validator.RouteCounter("candidate_fallbacks", "candidate fallbacks"),
        ),
        required_baseline_route_counters=(
            candidate_validator.RouteCounter("baseline_route", "baseline route"),
        ),
        fixed_comparison_environment=(("ANTFLY_INFERENCE_CUDA_FIXED", "locked"),),
        baseline_gate_value="required-baseline",
        candidate_gate_value="required-candidate",
    )


def splitk_spec() -> candidate_validator.CandidateSpec:
    return quality.resolve_catalog_spec(
        candidate_validator.GQA_DECODE_SPLITK_ONLINE_SM89_KERNEL_ID
    )


def splitk_timing(enabled: bool, *, persistent_replays: int = 295) -> dict:
    spec = splitk_spec()
    counters = {
        counter.name: 0
        for counter in (
            spec.required_route_counters
            + spec.required_baseline_route_counters
            + spec.forbidden_route_counters
        )
    }
    if enabled:
        counters.update({
            item.name: item.exact_count for item in spec.qualification_route_counts
        })
    else:
        counters.update({
            "launch_attention_gqa_decode_score_prework": 140,
            "launch_attention_gqa_decode_score_prework_tiled64_hd256": 112,
            "launch_attention_gqa_decode_score_prework_tiled64_hd512": 28,
        })
    metadata = candidate_validator.DEFAULT_TIMING_METADATA
    counters.update({
        metadata.persistent_replay_counter: persistent_replays,
        metadata.graph_discard_counter: 0,
        metadata.graph_capacity_skip_counter: 0,
    })
    return {"cuda": counters}


def sample(tokens: list[int], text: str, errors: list[str] | None = None) -> dict:
    return {
        "prompt_token_ids": {
            "count": 2051,
            "sha256": "a" * 64,
            "expected_count": 2051,
            "expected_sha256": "a" * 64,
        },
        "generated": {
            "token_ids": tokens,
            "text": text,
        },
        "errors": list(errors or []),
    }


def repetitions(
    baseline_tokens: list[int],
    candidate_tokens: list[int],
    baseline_text: str = "same output text",
    candidate_text: str = "same output text",
    count: int = 2,
) -> list[dict]:
    return [
        {
            "repetition": index + 1,
            "execution_order": ["baseline", "candidate"] if index % 2 == 0 else ["candidate", "baseline"],
            "baseline": sample(baseline_tokens, baseline_text),
            "candidate": sample(candidate_tokens, candidate_text),
        }
        for index in range(count)
    ]


class SuiteTests(unittest.TestCase):
    def test_default_suite_is_exact_and_all_prompts_are_locked_long_context(self) -> None:
        suite = quality.load_suite(SUITE_PATH)
        self.assertEqual(
            quality.REVIEWED_SUITE_SHA256,
            hashlib.sha256(SUITE_PATH.read_bytes()).hexdigest(),
        )
        self.assertEqual("exact-v1", suite.profile_name)
        self.assertFalse(suite.profile["allow_token_divergence"])
        self.assertEqual(4, len(suite.cases))
        self.assertEqual(1, sum(case.allow_candidate_divergence for case in suite.cases))
        for case in suite.cases:
            self.assertEqual(2051, case.prompt_contract["expected_prompt_tokens"])
            self.assertGreaterEqual(len(case.prompt.encode("utf-8")), 2000)
            self.assertEqual(
                case.prompt_contract["expected_prompt_sha256"],
                hashlib.sha256(case.prompt.encode("utf-8")).hexdigest(),
            )

    def test_bounded_profile_is_explicit(self) -> None:
        suite = quality.load_suite(SUITE_PATH, "bounded-freeform-v1")
        self.assertTrue(suite.profile["allow_token_divergence"])
        self.assertEqual(0.25, suite.profile["max_divergent_case_fraction"])

    def test_unknown_profile_fails_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown threshold profile"):
            quality.load_suite(SUITE_PATH, "loose")

    def test_tampered_inline_prompt_fails_contract(self) -> None:
        raw = json.loads(SUITE_PATH.read_text(encoding="utf-8"))
        raw["cases"][1]["prompt"]["padding"] += "tamper"
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            path = root / "suite.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            fixture = (SUITE_PATH.parent / "gemma4_long_context_v1.json").read_bytes()
            (root / "gemma4_long_context_v1.json").write_bytes(fixture)
            with self.assertRaisesRegex(ValueError, "contract mismatch"):
                quality.load_suite(path)

    def test_tampered_referenced_fixture_contract_fails(self) -> None:
        raw = json.loads(SUITE_PATH.read_text(encoding="utf-8"))
        raw["cases"][0]["prompt"]["expected_prompt_tokens"] = 2050
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            path = root / "suite.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            fixture = json.loads((SUITE_PATH.parent / "gemma4_long_context_v1.json").read_text())
            (root / "gemma4_long_context_v1.json").write_text(json.dumps(fixture), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "referenced fixture contracts disagree"):
                quality.load_suite(path)

    def test_suite_cannot_default_to_bounded_profile(self) -> None:
        raw = json.loads(SUITE_PATH.read_text(encoding="utf-8"))
        raw["default_threshold_profile"] = "bounded-freeform-v1"
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            path = root / "suite.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            fixture = (SUITE_PATH.parent / "gemma4_long_context_v1.json").read_bytes()
            (root / "gemma4_long_context_v1.json").write_bytes(fixture)
            with self.assertRaisesRegex(ValueError, "must remain exact-v1"):
                quality.load_suite(path)

    def test_bounded_threshold_cannot_be_loosened(self) -> None:
        profile = {
            "allow_token_divergence": True,
            **quality.HARD_BOUNDED_POLICY,
        }
        profile["max_token_divergence_rate"] = 0.91
        with self.assertRaisesRegex(ValueError, "too permissive"):
            quality.validate_threshold_profile("bad", profile)

    def test_generation_contract_is_locked(self) -> None:
        raw = json.loads(SUITE_PATH.read_text(encoding="utf-8"))
        raw["generation_contract"]["ignore_eos"] = True
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            path = root / "suite.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            fixture = (SUITE_PATH.parent / "gemma4_long_context_v1.json").read_bytes()
            (root / "gemma4_long_context_v1.json").write_bytes(fixture)
            with self.assertRaisesRegex(ValueError, "reviewed deterministic CUDA contract"):
                quality.load_suite(path)


class OutputTests(unittest.TestCase):
    def test_valid_json_contract(self) -> None:
        contract = {
            "min_utf8_bytes": 2,
            "required_substrings": ["Oslo"],
            "json": {
                "type": "object",
                "exact_keys": ["city", "count"],
                "expected_values": {"city": "Oslo", "count": 3},
            },
        }
        self.assertEqual([], quality.validate_output('{"city":"Oslo","count":3}', contract))

    def test_json_contract_rejects_fence_extra_key_and_wrong_value(self) -> None:
        contract = {
            "min_utf8_bytes": 2,
            "forbidden_substrings": ["```"],
            "json": {
                "type": "object",
                "exact_keys": ["city", "count"],
                "expected_values": {"city": "Oslo", "count": 3},
            },
        }
        errors = quality.validate_output(
            '```json\n{"city":"Oslo","count":4,"extra":1}\n```', contract
        )
        self.assertTrue(any("forbidden substring" in error for error in errors))
        self.assertTrue(any("not the required bare JSON" in error for error in errors))

    def test_fullmatch_and_control_character_validation(self) -> None:
        contract = {
            "min_utf8_bytes": 1,
            "required_fullmatch_regexes": [r"TOTAL\s*=\s*42"],
        }
        self.assertEqual([], quality.validate_output("TOTAL=42", contract))
        errors = quality.validate_output("TOTAL=42\x01 extra", contract)
        self.assertTrue(any("control characters" in error for error in errors))
        self.assertTrue(any("full-match" in error for error in errors))

    def test_word_and_byte_bounds(self) -> None:
        errors = quality.validate_output(
            "one two",
            {"min_utf8_bytes": 20, "max_utf8_bytes": 30, "min_words": 3, "max_words": 5},
        )
        self.assertEqual(2, len(errors))


class ParsingTests(unittest.TestCase):
    def test_parses_clean_stdout_and_output_region(self) -> None:
        stdout = (
            b"prompt_token_ids: 1 2 3\n"
            b"first line\nsecond line\n"
            b"token_ids: 10 11\n"
            b"finish_reason=length tokens=2\n"
            b"timing_ms: generate=7\n"
        )
        parsed, errors = quality.parse_generate_stdout(stdout)
        self.assertEqual([], errors)
        self.assertEqual([1, 2, 3], parsed["prompt_token_ids"])
        self.assertEqual([10, 11], parsed["token_ids"])
        self.assertEqual("first line\nsecond line", parsed["output_text"])
        self.assertEqual("length", parsed["finish_reason"])

    def test_invalid_utf8_fails_closed(self) -> None:
        parsed, errors = quality.parse_generate_stdout(b"\xff")
        self.assertIsNone(parsed["stdout_utf8"])
        self.assertIn("not valid UTF-8", errors[0])

    def test_missing_records_and_count_mismatch_fail(self) -> None:
        _, errors = quality.parse_generate_stdout(
            b"prompt_token_ids: 1\ntext\ntoken_ids: 2 3\ntokens=1\n"
        )
        self.assertTrue(any("token-count record" in error for error in errors))


class DivergenceTests(unittest.TestCase):
    def test_exact_tokens(self) -> None:
        metrics = quality.token_divergence([1, 2], [1, 2])
        self.assertTrue(metrics["exact_token_ids"])
        self.assertIsNone(metrics["first_divergence_index"])
        self.assertEqual(0.0, metrics["positional_divergence_rate"])

    def test_divergence_position_rate_and_length(self) -> None:
        metrics = quality.token_divergence([1, 2, 3, 4], [1, 2, 9])
        self.assertEqual(2, metrics["first_divergence_index"])
        self.assertEqual(2, metrics["positional_mismatch_tokens"])
        self.assertEqual(0.5, metrics["positional_divergence_rate"])
        self.assertEqual(1, metrics["output_length_delta_tokens"])

    def test_exact_policy_rejects_any_divergence(self) -> None:
        suite = quality.load_suite(SUITE_PATH)
        metrics = quality.token_divergence(list(range(100)), list(range(99)) + [999])
        errors = quality.divergence_errors(metrics, 1.0, suite.cases[0], suite.profile)
        self.assertEqual(["selected exact policy forbids generated-token divergence"], errors)

    def test_bounded_policy_accepts_late_small_freeform_divergence(self) -> None:
        suite = quality.load_suite(SUITE_PATH, "bounded-freeform-v1")
        baseline = list(range(100))
        candidate = baseline.copy()
        candidate[40] = 999
        metrics = quality.token_divergence(baseline, candidate)
        self.assertEqual(
            [], quality.divergence_errors(metrics, 0.95, suite.cases[0], suite.profile)
        )

    def test_bounded_policy_does_not_apply_to_structured_canary(self) -> None:
        suite = quality.load_suite(SUITE_PATH, "bounded-freeform-v1")
        baseline = list(range(100))
        candidate = baseline.copy()
        candidate[40] = 999
        errors = quality.divergence_errors(
            quality.token_divergence(baseline, candidate), 0.95, suite.cases[2], suite.profile
        )
        self.assertEqual(
            ["this structured/exact canary forbids generated-token divergence"], errors
        )

    def test_bounded_policy_rejects_early_or_dissimilar_divergence(self) -> None:
        suite = quality.load_suite(SUITE_PATH, "bounded-freeform-v1")
        baseline = list(range(100))
        candidate = baseline.copy()
        candidate[4] = 999
        errors = quality.divergence_errors(
            quality.token_divergence(baseline, candidate), 0.4, suite.cases[0], suite.profile
        )
        self.assertTrue(any("first divergence index" in error for error in errors))
        self.assertTrue(any("first divergence fraction" in error for error in errors))
        self.assertTrue(any("text similarity" in error for error in errors))


class RouteAndEnvironmentTests(unittest.TestCase):
    def test_splitk_catalog_contract_is_selected_and_fully_attested(self) -> None:
        suite = quality.load_suite(SUITE_PATH)
        spec = splitk_spec()
        quality.validate_quality_candidate_contract(suite, spec)
        self.assertEqual("exact-v1", suite.profile_name)
        self.assertFalse(suite.profile["allow_token_divergence"])
        self.assertEqual("off", spec.baseline_gate_value)
        self.assertEqual("required-splitk-online-sm89", spec.candidate_gate_value)
        self.assertEqual(
            [140, 112, 28],
            [item.exact_count for item in spec.qualification_route_counts],
        )
        self.assertTrue(spec.require_persistent_replay)
        self.assertEqual(
            {
                "benchmark_prompt_tokens": 2051,
                "lengths": [300],
                "cache_dtype": "f16",
                "prefill_chunk_size": 512,
                "capture_kv_capacity": 2432,
            },
            {
                key: quality.candidate_spec_identity(spec)["qualification_workload"][key]
                for key in (
                    "benchmark_prompt_tokens",
                    "lengths",
                    "cache_dtype",
                    "prefill_chunk_size",
                    "capture_kv_capacity",
                )
            },
        )

        for enabled in (False, True):
            with self.subTest(enabled=enabled):
                evidence, errors = quality.route_evidence(
                    splitk_timing(enabled),
                    spec,
                    enabled,
                    enforce_exact_qualification_counts=True,
                    generated_token_count=300,
                )
                self.assertEqual([], errors)
                self.assertTrue(evidence["persistent_graph_replay"]["required"])
                self.assertEqual(292, evidence["persistent_graph_replay"]["minimum_replays"])
                self.assertEqual(
                    295,
                    evidence["counters"][
                        candidate_validator.DEFAULT_TIMING_METADATA.persistent_replay_counter
                    ],
                )

    def test_splitk_exact_counts_fallbacks_and_graph_replay_fail_closed(self) -> None:
        spec = splitk_spec()
        timing = splitk_timing(True, persistent_replays=291)
        timing["cuda"]["launch_attention_gqa_decode_splitk_online_sm89"] = 139
        timing["cuda"]["launch_attention_gqa_decode_splitk_online_sm89_symbol_fallbacks"] = 1
        timing["cuda"][candidate_validator.DEFAULT_TIMING_METADATA.graph_discard_counter] = 1
        _, errors = quality.route_evidence(
            timing,
            spec,
            True,
            enforce_exact_qualification_counts=True,
            generated_token_count=300,
        )
        self.assertTrue(any("locked count 140" in error for error in errors))
        self.assertTrue(any("forbidden route/fallback" in error for error in errors))
        self.assertTrue(any("persistent replays 291 below 292" in error for error in errors))
        self.assertTrue(any("graph capture discards" in error for error in errors))

    def test_candidate_route_evidence_is_fail_closed(self) -> None:
        timing = {
            "cuda": {
                "candidate_hd256": 10,
                "candidate_hd512": 2,
                "candidate_fallbacks": 0,
                "baseline_route": 0,
            }
        }
        evidence, errors = quality.route_evidence(timing, custom_spec(), True, False)
        self.assertEqual([], errors)
        self.assertEqual(10, evidence["counters"]["candidate_hd256"])

    def test_missing_or_forbidden_route_counter_fails(self) -> None:
        timing = {
            "cuda": {
                "candidate_hd256": 10,
                "candidate_fallbacks": 1,
                "baseline_route": 0,
            }
        }
        _, errors = quality.route_evidence(timing, custom_spec(), True, False)
        self.assertTrue(any("candidate_hd512 is absent" in error for error in errors))
        self.assertTrue(any("forbidden route/fallback" in error for error in errors))

    def test_baseline_route_and_candidate_absence_are_required(self) -> None:
        timing = {
            "cuda": {
                "candidate_hd256": 1,
                "candidate_hd512": 0,
                "candidate_fallbacks": 0,
                "baseline_route": 0,
            }
        }
        _, errors = quality.route_evidence(timing, custom_spec(), False, False)
        self.assertTrue(any("unexpectedly used candidate route" in error for error in errors))
        self.assertTrue(any("did not use required route baseline_route" in error for error in errors))

    def test_runtime_environment_scrubs_unapproved_candidate_inputs(self) -> None:
        parent = {
            "HOME": "/tmp/home",
            "PATH": "/bin",
            "SECRET": "do-not-record",
            "ANTFLY_INFERENCE_CUDA_UNREVIEWED": "1",
            "TERMITE_UNREVIEWED": "1",
        }
        spec = custom_spec()
        env = quality.build_runtime_environment(parent, spec, True, 2432)
        self.assertNotIn("SECRET", env)
        self.assertNotIn("ANTFLY_INFERENCE_CUDA_UNREVIEWED", env)
        self.assertNotIn("TERMITE_UNREVIEWED", env)
        self.assertEqual("required-candidate", env[spec.environment_variable])
        self.assertEqual("locked", env["ANTFLY_INFERENCE_CUDA_FIXED"])
        self.assertEqual("2432", env[candidate_validator.CAPTURE_KV_CAPACITY_ENV])

    def test_environment_identity_is_order_independent(self) -> None:
        first = quality.runtime_environment_identity({"B": "2", "A": "1"})
        second = quality.runtime_environment_identity({"A": "1", "B": "2"})
        self.assertEqual(first, second)

    def test_run_sample_binds_prompt_output_environment_and_routes(self) -> None:
        prompt = "locked prompt"
        prompt_ids = [1, 2, 3]
        contract = {
            "expected_prompt_utf8_bytes": len(prompt.encode()),
            "expected_prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
            "expected_prompt_tokens": len(prompt_ids),
            "expected_prompt_token_ids_sha256": quality.canonical_sha256(prompt_ids),
        }
        case = quality.QualityCase(
            id="test-case",
            description="test",
            prompt=prompt,
            prompt_contract=contract,
            prompt_source_provenance=(),
            max_tokens=4,
            allow_candidate_divergence=False,
            output_contract={"min_utf8_bytes": 5, "required_substrings": ["hello"]},
        )
        suite = quality.QualitySuite(
            path=SUITE_PATH,
            raw={"generation_contract": json.loads(SUITE_PATH.read_text())["generation_contract"]},
            cases=(case,),
            profile_name="exact-v1",
            profile=json.loads(SUITE_PATH.read_text())["threshold_profiles"]["exact-v1"],
        )

        def fake_run(command, **kwargs):
            timing_path = pathlib.Path(command[command.index("--json-timing") + 1])
            candidate = kwargs["env"][custom_spec().environment_variable] == "required-candidate"
            timing_path.write_text(json.dumps({
                "decode_tok_per_s": 10.0,
                "timing_ms": {"generate": 2.0},
                "cuda": {
                    "candidate_hd256": 2 if candidate else 0,
                    "candidate_hd512": 1 if candidate else 0,
                    "candidate_fallbacks": 0,
                    "baseline_route": 0 if candidate else 3,
                },
            }), encoding="utf-8")
            stdout = (
                b"prompt_token_ids: 1 2 3\nhello\n"
                b"token_ids: 10 11\nfinish_reason=length tokens=2\n"
            )
            return subprocess.CompletedProcess(command, 0, stdout, b"")

        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(
                output_dir=pathlib.Path(directory),
                wrapper=pathlib.Path("/fake/wrapper"),
                binary=pathlib.Path("/fake/binary"),
                model=pathlib.Path("/fake/model"),
                timeout_sec=5,
            )
            with mock.patch.object(quality.subprocess, "run", side_effect=fake_run):
                baseline = quality.run_sample(
                    args=args,
                    suite=suite,
                    quality_case=case,
                    spec=custom_spec(),
                    enabled=False,
                    repetition=0,
                    order_index=0,
                )
                candidate = quality.run_sample(
                    args=args,
                    suite=suite,
                    quality_case=case,
                    spec=custom_spec(),
                    enabled=True,
                    repetition=0,
                    order_index=1,
                )
        self.assertTrue(baseline["passed"], baseline["errors"])
        self.assertTrue(candidate["passed"], candidate["errors"])
        self.assertEqual("hello", candidate["generated"]["text"])
        self.assertEqual("required-candidate", candidate["route_evidence"]["candidate_gate_value"])


class EvaluationTests(unittest.TestCase):
    def test_exact_suite_is_quality_qualified_but_has_no_promotion_authority(self) -> None:
        suite = quality.load_suite(SUITE_PATH)
        values = list(range(80))
        by_case = {case.id: repetitions(values, values) for case in suite.cases}
        result = quality.evaluate_suite_cases(suite, by_case)
        self.assertTrue(result["diagnostic_thresholds_passed"])
        self.assertTrue(result["quality_qualified"])
        self.assertFalse(result["collect_only"])

    def test_bounded_suite_can_pass_diagnostics_but_never_quality_qualifies(self) -> None:
        suite = quality.load_suite(SUITE_PATH, "bounded-freeform-v1")
        baseline = list(range(100))
        candidate = baseline.copy()
        candidate[40] = 999
        by_case = {
            case.id: repetitions(
                baseline,
                candidate if case.allow_candidate_divergence else baseline,
            )
            for case in suite.cases
        }
        result = quality.evaluate_suite_cases(suite, by_case)
        self.assertTrue(result["diagnostic_thresholds_passed"])
        self.assertFalse(result["quality_qualified"])
        self.assertTrue(result["collect_only"])
        self.assertEqual([suite.cases[0].id], result["divergent_cases"])

    def test_alternate_exact_suite_is_collect_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            suite_path = root / "suite.json"
            suite_path.write_bytes(SUITE_PATH.read_bytes())
            (root / "gemma4_long_context_v1.json").write_bytes(
                (SUITE_PATH.parent / "gemma4_long_context_v1.json").read_bytes()
            )
            suite = quality.load_suite(suite_path)
            values = list(range(80))
            by_case = {case.id: repetitions(values, values) for case in suite.cases}
            result = quality.evaluate_suite_cases(suite, by_case)
            self.assertTrue(result["diagnostic_thresholds_passed"])
            self.assertFalse(result["quality_qualified"])
            self.assertTrue(result["collect_only"])

    def test_nondeterministic_candidate_fails(self) -> None:
        suite = quality.load_suite(SUITE_PATH)
        values = list(range(80))
        reps = repetitions(values, values)
        reps[1]["candidate"] = sample(values[:-1] + [999], "different")
        result = quality.evaluate_case(suite.cases[0], reps, suite.profile)
        self.assertFalse(result["passed"])
        self.assertTrue(any("not deterministic" in error for error in result["errors"]))

    def test_sample_error_propagates(self) -> None:
        suite = quality.load_suite(SUITE_PATH)
        values = list(range(80))
        reps = repetitions(values, values)
        reps[0]["candidate"] = sample(values, "same output text", ["route missing"])
        result = quality.evaluate_case(suite.cases[0], reps, suite.profile)
        self.assertTrue(any("candidate: route missing" in error for error in result["errors"]))

    def test_identical_tokens_with_different_text_fail_closed(self) -> None:
        suite = quality.load_suite(SUITE_PATH)
        values = list(range(80))
        reps = repetitions(values, values, "baseline text", "candidate text")
        result = quality.evaluate_case(suite.cases[0], reps, suite.profile)
        self.assertTrue(any("decoded to different" in error for error in result["errors"]))


class ProvenanceTests(unittest.TestCase):
    def test_stable_file_provenance_is_content_addressed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "input.bin"
            path.write_bytes(b"abc")
            value = quality.stable_file_provenance(path)
            self.assertEqual(3, value["bytes"])
            self.assertEqual(hashlib.sha256(b"abc").hexdigest(), value["sha256"])

    def test_embedded_artifact_identity_matches_exact_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            artifact = root / "image.cubin"
            artifact.write_bytes(b"cubin-bytes")
            digest = hashlib.sha256(b"cubin-bytes").hexdigest()
            binary = root / "fake-binary"
            binary.write_text(
                "#!/bin/sh\n"
                "printf '%s\\n' "
                "'cuda_artifact_identity_schema: antfly.cuda_artifact_identity.v1' "
                "'cuda_artifact_mode: sm89' "
                "'cuda_artifact_format: cubin' "
                "'cuda_artifact_target: sm_89' "
                f"'cuda_artifact_image_bytes: 11' 'cuda_artifact_image_sha256: {digest}'\n",
                encoding="utf-8",
            )
            binary.chmod(0o755)
            evidence = quality.embedded_artifact_identity(
                binary, artifact, {"PATH": os.environ.get("PATH", "/bin")}, 5, root
            )
            self.assertTrue(evidence["passed"])

    def test_logits_remain_explicitly_unavailable(self) -> None:
        self.assertFalse(quality.LOGIT_CAPABILITY["available"])
        self.assertIn("teacher-forced logits", quality.LOGIT_CAPABILITY["unavailable_observables"])
        self.assertIn("production-equivalent", quality.LOGIT_CAPABILITY["consequence"])

    def test_only_cataloged_candidates_resolve(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires a cataloged candidate"):
            quality.resolve_catalog_spec("cuda.unknown.quality")


if __name__ == "__main__":
    unittest.main()
