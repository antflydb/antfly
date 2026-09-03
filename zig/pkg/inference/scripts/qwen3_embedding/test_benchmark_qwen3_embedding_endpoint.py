#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
import json
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))

import benchmark_qwen3_embedding_endpoint as benchmark


class CorpusTests(unittest.TestCase):
    def test_short_corpus_targets_twenty_tokens(self) -> None:
        for text in benchmark.build_corpus("short", 8):
            self.assertEqual(20, benchmark.approx_token_count(text))

    def test_passage_corpus_targets_256_tokens(self) -> None:
        for text in benchmark.build_corpus("passage", 4):
            self.assertEqual(256, benchmark.approx_token_count(text))

    def test_long_corpus_targets_1k_to_8k_tokens(self) -> None:
        for text in benchmark.build_corpus("long", 8):
            self.assertGreaterEqual(benchmark.approx_token_count(text), 1024)
            self.assertLessEqual(benchmark.approx_token_count(text), 8192)

    def test_mixed_corpus_is_ragged(self) -> None:
        lengths = {benchmark.approx_token_count(text) for text in benchmark.build_corpus("mixed", 32)}
        self.assertGreater(len(lengths), 1)
        for length in lengths:
            self.assertIn(length, benchmark.CORPUS_PROFILES["mixed"]["targets"])

    def test_corpus_is_deterministic_for_same_seed(self) -> None:
        self.assertEqual(benchmark.build_corpus("mixed", 16), benchmark.build_corpus("mixed", 16))
        self.assertEqual(
            benchmark.build_corpus("long", 4, seed=7), benchmark.build_corpus("long", 4, seed=7)
        )

    def test_corpus_differs_across_seeds(self) -> None:
        self.assertNotEqual(
            benchmark.build_corpus("passage", 4, seed=1),
            benchmark.build_corpus("passage", 4, seed=2),
        )

    def test_corpus_words_come_from_fixed_vocabulary(self) -> None:
        vocabulary = set(benchmark.VOCABULARY)
        for text in benchmark.build_corpus("short", 4):
            for word in text.split():
                self.assertIn(word, vocabulary)

    def test_corpus_lengths_deterministic(self) -> None:
        self.assertEqual(benchmark.corpus_lengths("mixed", 64), benchmark.corpus_lengths("mixed", 64))


class StatsTests(unittest.TestCase):
    def test_percentile_ranks(self) -> None:
        samples = [float(value) for value in range(1, 101)]
        self.assertEqual(50.0, benchmark.percentile(samples, 0.50))
        self.assertEqual(95.0, benchmark.percentile(samples, 0.95))
        self.assertEqual(99.0, benchmark.percentile(samples, 0.99))
        self.assertEqual(3.0, benchmark.percentile([3.0], 0.99))

    def test_percentile_rejects_empty(self) -> None:
        with self.assertRaises(ValueError):
            benchmark.percentile([], 0.5)

    def test_latency_summary_fields(self) -> None:
        summary = benchmark.latency_summary([1.0, 2.0, 3.0, 4.0])
        self.assertAlmostEqual(2.5, summary["mean_ms"])
        self.assertEqual(2.0, summary["p50_ms"])
        self.assertEqual(4.0, summary["p95_ms"])
        self.assertEqual(4.0, summary["p99_ms"])
        self.assertEqual(1.0, summary["min_ms"])
        self.assertEqual(4.0, summary["max_ms"])

    def test_interleaved_schedule_balances_and_reverses_order(self) -> None:
        self.assertEqual(
            ["antfly", "reference", "reference", "antfly", "antfly", "reference"],
            benchmark.interleaved_schedule(3),
        )

    def test_bootstrap_ratio_is_deterministic(self) -> None:
        first = benchmark.bootstrap_ratio_ci([2.0, 2.0], [1.0, 1.0], 100, 7)
        second = benchmark.bootstrap_ratio_ci([2.0, 2.0], [1.0, 1.0], 100, 7)
        self.assertEqual(first, second)
        self.assertEqual(0.5, first["estimate"])
        self.assertEqual(0.5, first["lower_95"])
        self.assertEqual(0.5, first["upper_95"])


class CosineTests(unittest.TestCase):
    def test_identical_vectors_have_unit_cosine(self) -> None:
        self.assertAlmostEqual(1.0, benchmark.cosine_similarity([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]))

    def test_orthogonal_vectors_have_zero_cosine(self) -> None:
        self.assertAlmostEqual(0.0, benchmark.cosine_similarity([1.0, 0.0], [0.0, 1.0]))

    def test_zero_vector_yields_zero_cosine(self) -> None:
        self.assertEqual(0.0, benchmark.cosine_similarity([0.0, 0.0], [1.0, 1.0]))

    def test_dimension_mismatch_raises(self) -> None:
        with self.assertRaises(ValueError):
            benchmark.cosine_similarity([1.0], [1.0, 2.0])

    def test_cross_check_pass_and_fail(self) -> None:
        matched = benchmark.cross_check([[1.0, 0.0]], [[1.0, 0.0]])
        self.assertTrue(matched["pass"])
        self.assertAlmostEqual(1.0, matched["min_cosine"])
        self.assertEqual(0.0, matched["max_abs_error"])
        diverged = benchmark.cross_check([[1.0, 0.0]], [[0.0, 1.0]])
        self.assertFalse(diverged["pass"])
        self.assertAlmostEqual(0.0, diverged["min_cosine"])
        self.assertAlmostEqual(1.0, diverged["max_abs_error"])

    def test_cross_check_count_mismatch_raises(self) -> None:
        with self.assertRaises(ValueError):
            benchmark.cross_check([[1.0]], [[1.0], [1.0]])

    def test_cross_check_honors_explicit_threshold(self) -> None:
        check = benchmark.cross_check([[1.0, 0.0]], [[0.99, 0.1]], threshold=1.0)
        self.assertFalse(check["pass"])
        self.assertEqual(1.0, check["threshold"])


class RunCellTests(unittest.TestCase):
    @staticmethod
    def args(**overrides: object) -> SimpleNamespace:
        values = {
            "url": "http://antfly",
            "reference_url": "http://reference",
            "model": "model",
            "reference_model": "model",
            "warmup": 0,
            "iters": 2,
            "timeout": 1.0,
            "require_comparable": True,
            "antfly_reported_token_offset": -1,
            "reference_reported_token_offset": 0,
            "cosine_threshold": 0.995,
            "bootstrap_samples": 100,
            "seed": 7,
            "fail_below_ratio": 0.9,
            "corpus": "fixture",
        }
        values.update(overrides)
        return SimpleNamespace(**values)

    def test_strict_cell_honors_per_target_usage_offsets(self) -> None:
        def request(url: str, model: str, texts: list[str], timeout: float):
            if url == "http://antfly":
                return 10.0, [[1.0, 0.0]], model, 3
            return 20.0, [[1.0, 0.0]], model, 4

        batches = [(["first"], [4]), (["second"], [4])]
        with mock.patch.object(benchmark, "request_embeddings", side_effect=request):
            results, comparison, _ = benchmark.run_cell(self.args(), batches, "exact:test")
        self.assertEqual([3, 4], [result["reported_prompt_tokens"] for result in results])
        self.assertEqual([4, 4], [result["input_tokens"] for result in results])
        self.assertIsNotNone(comparison)
        self.assertTrue(comparison["pass"])
        self.assertEqual(2, comparison["parity_iterations"])
        self.assertEqual(2.0, comparison["throughput_ratio_antfly_over_reference"]["estimate"])

    def test_preconditioning_alternates_and_is_excluded_from_samples(self) -> None:
        calls: list[tuple[str, str]] = []

        def request(url: str, model: str, texts: list[str], timeout: float):
            calls.append((url, texts[0]))
            prompt_tokens = 3 if url == "http://antfly" else 4
            elapsed = 100.0 if texts[0].startswith("hot") else 10.0
            return elapsed, [[1.0, 0.0]], model, prompt_tokens

        batches = [(["first"], [4]), (["second"], [4])]
        precondition = [(["hot-a"], [4]), (["hot-b"], [4])]
        with mock.patch.object(benchmark, "request_embeddings", side_effect=request):
            results, comparison, _ = benchmark.run_cell(
                self.args(), batches, "exact:test", precondition
            )
        self.assertEqual(
            [
                ("http://antfly", "hot-a"),
                ("http://reference", "hot-a"),
                ("http://reference", "hot-b"),
                ("http://antfly", "hot-b"),
                ("http://antfly", "first"),
                ("http://reference", "first"),
                ("http://reference", "second"),
                ("http://antfly", "second"),
            ],
            calls,
        )
        self.assertTrue(all(result["samples_ms"] == [10.0, 10.0] for result in results))
        self.assertIsNotNone(comparison)
        self.assertEqual(2, comparison["parity_iterations"])

    def test_cell_checks_parity_on_every_measured_iteration(self) -> None:
        def request(url: str, model: str, texts: list[str], timeout: float):
            vector = [0.0, 1.0] if url == "http://reference" and texts == ["bad"] else [1.0, 0.0]
            prompt_tokens = 3 if url == "http://antfly" else 4
            return 10.0, [vector], model, prompt_tokens

        batches = [(["bad"], [4]), (["good"], [4])]
        with mock.patch.object(benchmark, "request_embeddings", side_effect=request):
            _, comparison, _ = benchmark.run_cell(self.args(), batches, "exact:test")
        self.assertIsNotNone(comparison)
        self.assertFalse(comparison["pass"])
        self.assertEqual(0.0, comparison["min_cosine"])

    def test_strict_cell_rejects_any_usage_mismatch(self) -> None:
        def request(url: str, model: str, texts: list[str], timeout: float):
            prompt_tokens = 99 if url == "http://antfly" and texts == ["bad-count"] else (
                3 if url == "http://antfly" else 4
            )
            return 10.0, [[1.0]], model, prompt_tokens

        batches = [(["bad-count"], [4]), (["good"], [4])]
        with mock.patch.object(benchmark, "request_embeddings", side_effect=request):
            with self.assertRaisesRegex(ValueError, "strict token-count mismatch"):
                benchmark.run_cell(self.args(), batches, "exact:test")

    def test_strict_cell_rejects_live_model_mismatch(self) -> None:
        def request(url: str, model: str, texts: list[str], timeout: float):
            reported_model = "wrong-model" if url == "http://reference" else model
            prompt_tokens = 3 if url == "http://antfly" else 4
            return 10.0, [[1.0]], reported_model, prompt_tokens

        batches = [(["first"], [4]), (["second"], [4])]
        with mock.patch.object(benchmark, "request_embeddings", side_effect=request):
            with self.assertRaisesRegex(ValueError, "strict live-model mismatch"):
                benchmark.run_cell(self.args(), batches, "exact:test")


class FixtureTests(unittest.TestCase):
    def test_exact_token_fixture_round_trip(self) -> None:
        payload = {
            "schema": benchmark.FIXTURE_SCHEMA,
            "cases": [
                {"id": "a", "text": "alpha", "token_ids": [1, 2]},
                {"id": "b", "text": "beta", "token_ids": [3, 4, 5]},
            ],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "fixture.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            cases = benchmark.load_fixture(path)
        texts, counts = benchmark.fixture_batch(cases, 3)
        self.assertEqual(["alpha", "beta", "alpha"], texts)
        self.assertEqual([2, 3, 2], counts)

    def test_fixture_rejects_wrong_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "fixture.json"
            path.write_text('{"schema":"wrong","cases":[]}', encoding="utf-8")
            with self.assertRaises(ValueError):
                benchmark.load_fixture(path)

    def test_fixture_rejects_model_sha_mismatch(self) -> None:
        payload = {
            "schema": benchmark.FIXTURE_SCHEMA,
            "model_sha256": "a" * 64,
            "cases": [{"id": "a", "text": "alpha", "token_ids": [1]}],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "fixture.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "does not match"):
                benchmark.load_fixture(path, "b" * 64)

    def test_fixture_case_selection_preserves_requested_order(self) -> None:
        cases = [
            {"id": "a", "text": "alpha", "token_ids": [1]},
            {"id": "b", "text": "beta", "token_ids": [2]},
        ]
        selected = benchmark.select_fixture_cases(cases, "b,a")
        self.assertEqual(["b", "a"], [case["id"] for case in selected])
        with self.assertRaises(ValueError):
            benchmark.select_fixture_cases(cases, "missing")

    def test_fixture_batch_offset_advances_without_reusing_cases(self) -> None:
        cases = [
            {"id": "a", "text": "alpha", "token_ids": [1]},
            {"id": "b", "text": "beta", "token_ids": [2, 3]},
            {"id": "c", "text": "gamma", "token_ids": [4, 5, 6]},
        ]
        texts, counts = benchmark.fixture_batch(cases, 2, offset=1)
        self.assertEqual(["beta", "gamma"], texts)
        self.assertEqual([2, 3], counts)

    def test_fixture_token_count_selection(self) -> None:
        cases = [
            {"id": "a", "text": "alpha", "token_ids": [1]},
            {"id": "b", "text": "beta", "token_ids": [2, 3]},
            {"id": "c", "text": "gamma", "token_ids": [4, 5]},
        ]
        selected = benchmark.select_fixture_token_count(cases, 2)
        self.assertEqual(["b", "c"], [case["id"] for case in selected])
        self.assertIs(cases, benchmark.select_fixture_token_count(cases, None))
        with self.assertRaises(ValueError):
            benchmark.select_fixture_token_count(cases, 3)

    def test_cache_neutral_validation_rejects_every_reuse_kind(self) -> None:
        valid = [
            {"id": "a", "text": "alpha", "token_ids": [1, 9]},
            {"id": "b", "text": "beta", "token_ids": [2, 9]},
        ]
        benchmark.validate_cache_neutral_cases(valid, 2)
        variants = {
            "case ids": [valid[0], {**valid[1], "id": "a"}],
            "texts": [valid[0], {**valid[1], "text": "alpha"}],
            "token sequences": [valid[0], {**valid[1], "token_ids": [1, 9]}],
            "first token ids": [valid[0], {**valid[1], "token_ids": [1, 8]}],
        }
        for label, cases in variants.items():
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, label):
                    benchmark.validate_cache_neutral_cases(cases, 2)

    def test_cache_neutral_validation_rejects_duplicate_case_selection(self) -> None:
        cases = [
            {"id": "a", "text": "alpha", "token_ids": [1]},
            {"id": "b", "text": "beta", "token_ids": [2]},
        ]
        selected = benchmark.select_fixture_cases(cases, "a,a")
        with self.assertRaisesRegex(ValueError, "case ids"):
            benchmark.validate_cache_neutral_cases(selected, 2)

    def test_cache_neutral_validation_rejects_insufficient_cases(self) -> None:
        cases = [{"id": "a", "text": "alpha", "token_ids": [1]}]
        with self.assertRaisesRegex(ValueError, "needs 2 distinct fixture cases"):
            benchmark.validate_cache_neutral_cases(cases, 2)

    def test_checked_in_fixture_has_cache_neutral_exact_lengths_and_eos(self) -> None:
        path = Path(__file__).resolve().parent / "fixtures/qwen3_embedding_0_6b_exact_tokens.json"
        cases = benchmark.load_fixture(path)
        by_length = {
            length: benchmark.select_fixture_token_count(cases, length)
            for length in (511, 2551)
        }
        self.assertEqual(24, len(by_length[511]))
        self.assertEqual(24, len(by_length[2551]))
        for selected in by_length.values():
            self.assertEqual(24, len({case["token_ids"][0] for case in selected}))
        self.assertTrue(all(case["token_ids"][-1] == 151643 for case in cases))

    def test_result_reports_real_token_throughput(self) -> None:
        result = benchmark.result_for_target(
            "antfly", "fixture", [10.0, 10.0], [[1.0], [1.0]], [2, 3], "exact:test"
        )
        self.assertEqual(5, result["input_tokens"])
        self.assertEqual(500.0, result["tokens_per_second"])
        self.assertEqual("exact:test", result["token_count_source"])


class ProcessProvenanceTests(unittest.TestCase):
    def test_process_provenance_attests_exact_executable_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            executable = Path(temp_dir) / "server"
            executable.write_bytes(b"exact-build")
            ps_result = SimpleNamespace(
                returncode=0,
                stdout=f"{executable} --serve --port 1234\n",
                stderr="",
            )
            with mock.patch.object(benchmark.subprocess, "run", return_value=ps_result):
                provenance = benchmark.process_provenance(
                    123, executable, "--serve --port 1234"
                )
        self.assertEqual(123, provenance["pid"])
        self.assertEqual(str(executable.resolve()), provenance["executable"])
        self.assertEqual(64, len(provenance["executable_sha256"]))
        self.assertEqual(["--serve", "--port", "1234"], provenance["argv"])

    def test_process_provenance_rejects_different_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            executable = Path(temp_dir) / "server"
            executable.write_bytes(b"exact-build")
            ps_result = SimpleNamespace(
                returncode=0,
                stdout=f"{executable} --serve --port 1234\n",
                stderr="",
            )
            with mock.patch.object(benchmark.subprocess, "run", return_value=ps_result):
                with self.assertRaisesRegex(ValueError, "arguments do not match"):
                    benchmark.process_provenance(
                        123, executable, "--serve --port 9999"
                    )

    def test_process_provenance_rejects_different_executable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            declared = Path(temp_dir) / "declared-server"
            observed = Path(temp_dir) / "observed-server"
            declared.write_bytes(b"declared")
            observed.write_bytes(b"observed")
            command_result = SimpleNamespace(
                returncode=0, stdout=f"{observed} --serve\n", stderr=""
            )
            comm_result = SimpleNamespace(
                returncode=0, stdout=f"{observed}\n", stderr=""
            )
            with mock.patch.object(
                benchmark.subprocess, "run", side_effect=[command_result, comm_result]
            ):
                with self.assertRaisesRegex(ValueError, "does not match"):
                    benchmark.process_provenance(123, declared)


class ArgTests(unittest.TestCase):
    def test_default_batch_sizes(self) -> None:
        args = benchmark.parse_args([])
        self.assertEqual([1, 8, 32, 128], args.batch_sizes)
        self.assertEqual("mixed", args.corpus)

    def test_reported_token_offsets_are_parsed(self) -> None:
        args = benchmark.parse_args(
            ["--antfly-reported-token-offset", "-1", "--reference-reported-token-offset", "0"]
        )
        self.assertEqual(-1, args.antfly_reported_token_offset)
        self.assertEqual(0, args.reference_reported_token_offset)

    def test_rejects_nonpositive_batch(self) -> None:
        with self.assertRaises(SystemExit):
            benchmark.parse_args(["--batch-sizes", "0,4"])

    def test_rejects_zero_iters(self) -> None:
        with self.assertRaises(SystemExit):
            benchmark.parse_args(["--iters", "0"])

    def test_preconditioning_requires_fixture_and_nonnegative_count(self) -> None:
        with self.assertRaises(SystemExit):
            benchmark.parse_args(["--precondition-iters", "1"])
        with self.assertRaises(SystemExit):
            benchmark.parse_args(["--precondition-iters", "-1"])
        args = benchmark.parse_args(
            ["--precondition-iters", "2", "--fixture", "fixture.json"]
        )
        self.assertEqual(2, args.precondition_iters)

    def test_strict_mode_fails_closed_without_provenance(self) -> None:
        with self.assertRaises(SystemExit):
            benchmark.parse_args(["--require-comparable"])


if __name__ == "__main__":
    unittest.main()
