#!/usr/bin/env python3

import argparse
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from benchmark_gemma4_cuda_matrix import (
    collect_entries,
    evaluate,
    main,
    parse_args,
    run_pair_case,
)


class MatrixGateTest(unittest.TestCase):
    def args(self, **overrides):
        values = {
            "target_length": 256,
            "min_antfly_tok_s": 120.0,
            "min_comparable_ratio": 0.95,
            "max_cv": 0.02,
            "require_graph_replay": True,
            "require_generated_attention": True,
            "require_generated_q6_lm_head_argmax": True,
            "warmups": 2,
            "repeats": 5,
            "prompt": "Write one sentence about ants.",
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def entry(self, length, tok_s=121.0, ratio=0.96, cv=0.01):
        return {
            "output_tokens": length,
            "pair_exit_code": 0,
            "pair_summary_ok": True,
            "pair_ok": True,
            "antfly_eval_tokens": length - 1,
            "antfly_tok_s": tok_s,
            "antfly_ms_per_token": 1000.0 / tok_s,
            "antfly_cv": cv,
            "llama_comparable_tok_s": tok_s / ratio,
            "comparable_ratio": ratio,
            "graph_replay_ok": True,
            "generated_attention_ok": True,
            "generated_q6_lm_head_argmax_ok": True,
        }

    def test_accepts_complete_promotion_evidence(self):
        result = evaluate([self.entry(128), self.entry(256)], self.args())
        self.assertTrue(result["passed"])
        self.assertEqual(1, len(result["context_slopes"]))

    def test_rejects_each_target_gate(self):
        for changes in ({"tok_s": 119.0}, {"ratio": 0.94}, {"cv": 0.03}):
            with self.subTest(changes=changes):
                result = evaluate([self.entry(256, **changes)], self.args())
                self.assertFalse(result["passed"])

    def test_rejects_missing_route_coverage(self):
        entry = self.entry(256)
        entry["generated_attention_ok"] = False
        self.assertFalse(evaluate([entry], self.args())["passed"])

    def test_generated_attention_is_an_explicit_candidate_gate(self):
        with mock.patch.object(sys, "argv", ["benchmark_gemma4_cuda_matrix.py"]):
            self.assertFalse(parse_args().require_generated_attention)
        with mock.patch.object(
            sys,
            "argv",
            ["benchmark_gemma4_cuda_matrix.py", "--require-generated-attention"],
        ):
            self.assertTrue(parse_args().require_generated_attention)

    def test_generated_q6_lm_head_is_an_explicit_candidate_gate(self):
        with mock.patch.object(sys, "argv", ["benchmark_gemma4_cuda_matrix.py"]):
            self.assertFalse(parse_args().require_generated_q6_lm_head_argmax)
        with mock.patch.object(
            sys,
            "argv",
            [
                "benchmark_gemma4_cuda_matrix.py",
                "--require-generated-q6-lm-head-argmax",
            ],
        ):
            self.assertTrue(parse_args().require_generated_q6_lm_head_argmax)

    def test_rejects_missing_generated_q6_lm_head_coverage(self):
        entry = self.entry(256)
        entry["generated_q6_lm_head_argmax_ok"] = False
        self.assertFalse(evaluate([entry], self.args())["passed"])

    def test_collects_every_length_when_pair_writes_failed_summary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            args = argparse.Namespace(
                pair_script=pathlib.Path("/fake/pair-script"),
                output_dir=pathlib.Path(temp_dir),
                lengths=[64, 128],
                target_length=64,
                warmups=0,
                repeats=1,
                prompt="test",
                require_graph_replay=True,
                require_generated_attention=False,
                require_generated_q6_lm_head_argmax=False,
                min_antfly_tok_s=0.0,
                min_comparable_ratio=0.0,
                max_cv=1.0,
            )

            def failed_pair(command, *, check, env):
                self.assertFalse(check)
                self.assertEqual("0", env["ANTFLY_GENERATED_ATTENTION_DECODE"])
                output_tokens = int(env["LLAMA_TOKENS"])
                run_dir = pathlib.Path(env["OUT_DIR"])
                run_dir.mkdir(parents=True)
                summary = {
                    "ok": False,
                    "comparison": {"antfly_tokens": output_tokens - 1},
                    "antfly_decode_tok_s": {"median": 125.0},
                    "llama_comparable_tok_s": {"median": 125.0},
                    "antfly_tok_s_cv": 0.01,
                    "ok_graph_replay": False,
                    "ok_generated_attention": True,
                    "ok_generated_q6_lm_head_argmax": False,
                }
                (run_dir / "paired_summary.json").write_text(json.dumps(summary))
                return subprocess.CompletedProcess(command, 1)

            with mock.patch(
                "benchmark_gemma4_cuda_matrix.subprocess.run", side_effect=failed_pair
            ) as run:
                entries = collect_entries(args)

            self.assertEqual(2, run.call_count)
            self.assertEqual([1, 1], [entry["pair_exit_code"] for entry in entries])
            self.assertTrue(all(not entry["pair_ok"] for entry in entries))
            result = evaluate(entries, args)
            self.assertFalse(result["checks"]["pair_benchmarks"])
            self.assertFalse(result["passed"])

    def test_collect_entries_pins_attention_candidate_to_requirement(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            args = argparse.Namespace(
                pair_script=pathlib.Path("/fake/pair-script"),
                output_dir=pathlib.Path(temp_dir),
                lengths=[64],
                target_length=64,
                warmups=0,
                repeats=1,
                prompt="test",
                require_graph_replay=False,
                require_generated_attention=True,
                require_generated_q6_lm_head_argmax=False,
                min_antfly_tok_s=0.0,
                min_comparable_ratio=0.0,
                max_cv=1.0,
            )

            def successful_pair(command, *, check, env):
                self.assertFalse(check)
                self.assertEqual("1", env["REQUIRE_GENERATED_ATTENTION"])
                self.assertEqual("1", env["ANTFLY_GENERATED_ATTENTION_DECODE"])
                run_dir = pathlib.Path(env["OUT_DIR"])
                run_dir.mkdir(parents=True)
                summary = {
                    "ok": True,
                    "comparison": {"antfly_tokens": 63},
                    "antfly_decode_tok_s": {"median": 125.0},
                    "llama_comparable_tok_s": {"median": 125.0},
                    "antfly_tok_s_cv": 0.01,
                    "ok_graph_replay": True,
                    "ok_generated_attention": True,
                    "ok_generated_q6_lm_head_argmax": False,
                }
                (run_dir / "paired_summary.json").write_text(json.dumps(summary))
                return subprocess.CompletedProcess(command, 0)

            with mock.patch(
                "benchmark_gemma4_cuda_matrix.subprocess.run",
                side_effect=successful_pair,
            ):
                entries = collect_entries(args)
            self.assertTrue(entries[0]["pair_ok"])

    def test_nonzero_pair_without_fresh_readable_summary_fails_clearly(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = pathlib.Path(temp_dir)
            stale_summary = run_dir / "paired_summary.json"
            stale_summary.write_text('{"ok": true}')
            completed = subprocess.CompletedProcess(["pair-script"], 1)
            with mock.patch(
                "benchmark_gemma4_cuda_matrix.subprocess.run", return_value=completed
            ):
                with self.assertRaisesRegex(
                    RuntimeError, "exited 1 without a readable summary"
                ):
                    run_pair_case(pathlib.Path("pair-script"), run_dir, {})
            self.assertFalse(stale_summary.exists())

    def test_collect_only_defers_failed_pair_exit_to_matrix_policy(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            args = self.args()
            args.pair_script = pathlib.Path(sys.executable)
            args.output_dir = pathlib.Path(temp_dir)
            args.lengths = [256]
            args.collect_only = True
            entry = self.entry(256)
            entry.update(
                {"pair_exit_code": 1, "pair_summary_ok": False, "pair_ok": False}
            )

            with (
                mock.patch(
                    "benchmark_gemma4_cuda_matrix.parse_args", return_value=args
                ),
                mock.patch(
                    "benchmark_gemma4_cuda_matrix.collect_entries", return_value=[entry]
                ),
                mock.patch("builtins.print"),
            ):
                main()
            result = json.loads((args.output_dir / "matrix_summary.json").read_text())
            self.assertFalse(result["passed"])

            args.collect_only = False
            with (
                mock.patch(
                    "benchmark_gemma4_cuda_matrix.parse_args", return_value=args
                ),
                mock.patch(
                    "benchmark_gemma4_cuda_matrix.collect_entries", return_value=[entry]
                ),
                mock.patch("builtins.print"),
                self.assertRaisesRegex(SystemExit, "1"),
            ):
                main()


if __name__ == "__main__":
    unittest.main()
