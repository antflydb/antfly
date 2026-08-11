#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import tempfile
import textwrap
import unittest


class PairBenchmarkAccountingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = pathlib.Path(self.temp_dir.name)
        self.repo = pathlib.Path(__file__).resolve().parents[4]
        self.pair_script = self.repo / "zig/pkg/inference/scripts/gemma4_qat_llamacpp_pair_benchmark.sh"
        self.model = self.root / "model.gguf"
        self.model.write_bytes(b"test-model")
        self.antfly = self.write_executable(
            "fake-antfly.py",
            """
            #!/usr/bin/env python3
            import json
            import os
            import pathlib
            import sys

            if order_log := os.environ.get("ORDER_LOG"):
                with pathlib.Path(order_log).open("a", encoding="utf-8") as output:
                    output.write("antfly\\n")

            if profile_log := os.environ.get("GQA_PROFILE_LOG"):
                with pathlib.Path(profile_log).open("a", encoding="utf-8") as output:
                    output.write(os.environ.get("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE", "unset") + "\\n")

            json_path = pathlib.Path(sys.argv[sys.argv.index("--json-timing") + 1])
            payload = {
                "tokens": int(os.environ.get("FAKE_ANTFLY_ACTUAL_TOKENS", "3")),
                "decode_tok_per_s": 100.0,
                "timing_ms": {
                    "generate": 10.0,
                    "prefill_inner": 2.0,
                    "decode_inner": 8.0,
                },
                "cuda": {
                    "graph_capture_replays": 8,
                    "graph_capture_persistent_replays": 8,
                    "graph_capture_discards": 0,
                    "graph_capture_capacity_skips": 0,
                    "launch_attention_gqa_decode_generated": 1,
                    "lm_head_argmax_fused_q4_0_q8_1": 1,
                    "lm_head_argmax_q4_0_q8_1_fallbacks": int(os.environ.get("FAKE_LM_FALLBACKS", "0")),
                    "lm_head_argmax_generated_q6_k_q8_1_hits": int(os.environ.get("FAKE_GENERATED_Q6_LM_HITS", "0")),
                    "lm_head_argmax_generated_q6_k_q8_1_fallbacks": int(os.environ.get("FAKE_GENERATED_Q6_LM_FALLBACKS", "0")),
                    "q4_0_generated_mmv_hits": 11,
                    "q4_0_generated_mmv_fallbacks": 0,
                    "q4_0_generated_mm_hits": 12,
                    "q4_0_generated_mm_fallbacks": 0,
                    "q4_0_generated_pair_hits": 13,
                    "q4_0_generated_pair_fallbacks": 0,
                    "q4_0_generated_pair_q8_hits": 14,
                    "q4_0_generated_pair_q8_fallbacks": 0,
                    "q4_0_generated_down_q8_hits": 15,
                    "q4_0_generated_down_q8_fallbacks": 0,
                    "q4_0_generated_e2b_pair_q8_hits": 1,
                    "q4_0_generated_e2b_down_q8_hits": 1,
                    "q4_0_generated_e2b_pair_q8_fallbacks": 0,
                    "q4_0_generated_e2b_down_q8_fallbacks": 0,
                },
            }
            json_path.write_text(json.dumps(payload))
            """,
        )
        self.llama = self.write_executable(
            "fake-llama.py",
            """
            #!/usr/bin/env python3
            import os
            import pathlib

            if order_log := os.environ.get("ORDER_LOG"):
                with pathlib.Path(order_log).open("a", encoding="utf-8") as output:
                    output.write("llama_cpp\\n")

            runs = int(os.environ.get("FAKE_LLAMA_EVAL_RUNS", "3"))
            print("common_perf_print: sampling time = 2.00 ms")
            print("common_perf_print: prompt eval time = 3.00 ms / 2 tokens, 666.67 tokens per second")
            print(f"common_perf_print: eval time = 10.00 ms / {runs} runs, 300.00 tokens per second")
            print("common_perf_print: total time = 15.00 ms")
            print("common_perf_print: graphs reused = 3")
            """,
        )
        self.case_index = 0

    def write_executable(self, name: str, source: str) -> pathlib.Path:
        path = self.root / name
        path.write_text(textwrap.dedent(source).lstrip())
        path.chmod(0o755)
        return path

    def run_case(self, **overrides: str) -> tuple[subprocess.CompletedProcess[str], pathlib.Path]:
        self.case_index += 1
        output_dir = self.root / f"case-{self.case_index}"
        env = os.environ.copy()
        # Ambient GQA prefill settings would defeat the legacy-bridge set-ness
        # detection these tests assert on, so scrub them first.
        for name in (
            "ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE",
            "ANTFLY_GQA_PREFILL_PROFILE",
            "antfly_gqa_prefill_profile",
            "ANTFLY_GQA_PREFILL_USE_RUNTIME_DEFAULT",
        ):
            env.pop(name, None)
        for suffix in ("FAST", "TILED", "MMA"):
            env.pop(f"ANTFLY_GQA_PREFILL_{suffix}", None)
            env.pop(f"ANTFLY_INFERENCE_CUDA_GQA_PREFILL_{suffix}", None)
            env.pop(f"antfly_gqa_prefill_{suffix.lower()}", None)
        env.update({
            "ANTFLY_BIN": str(self.antfly),
            "LLAMA_CPP_BIN": str(self.llama),
            "MODEL": str(self.model),
            "OUT_DIR": str(output_dir),
            "WARMUPS": "0",
            "REPEATS": "1",
            "TIMEOUT": "off",
            "ANTFLY_TOKENS": "3",
            "LLAMA_TOKENS": "4",
            "MIN_LLAMA_THROUGHPUT_RATIO": "0",
            "MIN_COMPARABLE_THROUGHPUT_RATIO": "0",
            "ANTFLY_DECODE_GRAPH_REPLAY": "off",
        })
        env.update(overrides)
        completed = subprocess.run(
            [str(self.pair_script)],
            env=env,
            text=True,
            capture_output=True,
            timeout=30,
        )
        return completed, output_dir

    def test_records_actual_token_accounting_and_tsv_width(self) -> None:
        completed, output_dir = self.run_case()
        self.assertEqual(0, completed.returncode, completed.stderr)

        summary = json.loads((output_dir / "paired_summary.json").read_text())
        self.assertEqual(3, summary["comparison"]["antfly_tokens"])
        self.assertEqual(4, summary["comparison"]["llama_tokens"])
        self.assertEqual(3, summary["comparison"]["expected_llama_eval_runs"])
        self.assertEqual([3], summary["actual_token_counts"]["antfly_generated_tokens"])
        self.assertEqual([3], summary["actual_token_counts"]["llama_eval_runs"])
        self.assertEqual(3, summary["rows"][0]["antfly_generated_tokens"])
        self.assertTrue(summary["ok_lm_head_argmax"])
        self.assertEqual("0", summary["comparison"]["antfly_generated_q4_0_e2b_ffn_exact"])
        self.assertEqual(0, summary["rows"][0]["antfly_generated_e2b_exact_pair"])
        self.assertEqual(11, summary["rows"][0]["antfly_generated_q4_0_mmv"])
        self.assertEqual(12, summary["rows"][0]["antfly_generated_q4_0_mm"])
        self.assertEqual(13, summary["rows"][0]["antfly_generated_q4_0_pair"])
        self.assertEqual(14, summary["rows"][0]["antfly_generated_q4_0_pair_q8"])
        self.assertEqual(15, summary["rows"][0]["antfly_generated_q4_0_down_q8"])

        header, row = (output_dir / "paired_summary.tsv").read_text().splitlines()
        fields = header.split("\t")
        self.assertEqual(len(fields), len(row.split("\t")))
        self.assertIn("antfly_generated_tokens", fields)
        self.assertIn("antfly_generated_q4_0_mmv", fields)

    def test_measurement_pairs_use_balanced_ab_ba_order(self) -> None:
        order_log = self.root / "order.log"
        completed, output_dir = self.run_case(REPEATS="2", ORDER_LOG=str(order_log))
        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertEqual(
            ["antfly", "llama_cpp", "llama_cpp", "antfly"],
            order_log.read_text(encoding="utf-8").splitlines(),
        )
        summary = json.loads((output_dir / "paired_summary.json").read_text())
        self.assertEqual("balanced_ab_ba", summary["comparison"]["paired_order"])
        self.assertEqual(
            [["antfly", "llama_cpp"], ["llama_cpp", "antfly"]],
            [row["execution_order"] for row in summary["rows"]],
        )

    def test_rejects_truncated_antfly_generation(self) -> None:
        completed, _ = self.run_case(FAKE_ANTFLY_ACTUAL_TOKENS="2")
        self.assertEqual(1, completed.returncode)
        self.assertIn("Antfly generated token count mismatch", completed.stderr)
        self.assertIn("expected 3, got 2", completed.stderr)

    def test_rejects_wrong_llama_eval_accounting(self) -> None:
        completed, _ = self.run_case(FAKE_LLAMA_EVAL_RUNS="2")
        self.assertEqual(1, completed.returncode)
        self.assertIn("llama.cpp eval run count mismatch", completed.stderr)
        self.assertIn("expected 3, got 2", completed.stderr)

    def test_rejects_degenerate_or_unpaired_token_requests_before_running(self) -> None:
        completed, output_dir = self.run_case(ANTFLY_TOKENS="1", LLAMA_TOKENS="1")
        self.assertEqual(2, completed.returncode)
        self.assertIn("token accounting requires", completed.stderr)
        self.assertFalse(output_dir.exists())

        completed, output_dir = self.run_case(ANTFLY_TOKENS="2", LLAMA_TOKENS="4")
        self.assertEqual(2, completed.returncode)
        self.assertIn("LLAMA_TOKENS = ANTFLY_TOKENS + 1", completed.stderr)
        self.assertFalse(output_dir.exists())

    def test_gqa_prefill_profile_defaults_to_required_fast_without_legacy_vars(self) -> None:
        profile_log = self.root / "gqa-profile-default.log"
        completed, output_dir = self.run_case(GQA_PROFILE_LOG=str(profile_log))
        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertEqual(
            ["required-fast"],
            profile_log.read_text(encoding="utf-8").splitlines(),
        )
        summary = json.loads((output_dir / "paired_summary.json").read_text())
        self.assertEqual("required-fast", summary["comparison"]["antfly_gqa_prefill_profile"])

    def test_gqa_prefill_profile_can_exercise_the_unset_runtime_default(self) -> None:
        profile_log = self.root / "gqa-profile-runtime-default.log"
        completed, output_dir = self.run_case(
            GQA_PROFILE_LOG=str(profile_log),
            ANTFLY_GQA_PREFILL_USE_RUNTIME_DEFAULT="1",
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertEqual(
            ["unset"],
            profile_log.read_text(encoding="utf-8").splitlines(),
        )
        summary = json.loads((output_dir / "paired_summary.json").read_text())
        self.assertEqual("automatic", summary["comparison"]["antfly_gqa_prefill_profile"])

    def test_gqa_prefill_profile_preserves_explicit_legacy_bridge(self) -> None:
        profile_log = self.root / "gqa-profile-legacy.log"
        completed, output_dir = self.run_case(
            GQA_PROFILE_LOG=str(profile_log),
            ANTFLY_GQA_PREFILL_FAST="1",
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertEqual(
            ["fast"],
            profile_log.read_text(encoding="utf-8").splitlines(),
        )
        summary = json.loads((output_dir / "paired_summary.json").read_text())
        self.assertEqual("fast", summary["comparison"]["antfly_gqa_prefill_profile"])

    def test_lm_route_gate_rejects_fallbacks(self) -> None:
        completed, _ = self.run_case(FAKE_LM_FALLBACKS="1")
        self.assertEqual(1, completed.returncode)
        self.assertIn("LM-head gate failed", completed.stderr)

    def test_generated_q6_lm_route_gate_requires_hit_without_fallback(self) -> None:
        completed, _ = self.run_case(REQUIRE_GENERATED_Q6_LM_HEAD_ARGMAX="1")
        self.assertEqual(1, completed.returncode)
        self.assertIn("Generated Q6 LM-head gate failed", completed.stderr)

        completed, output_dir = self.run_case(
            REQUIRE_GENERATED_Q6_LM_HEAD_ARGMAX="1",
            FAKE_GENERATED_Q6_LM_HITS="1",
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        summary = json.loads((output_dir / "paired_summary.json").read_text())
        self.assertTrue(summary["ok_generated_q6_lm_head_argmax"])

        completed, _ = self.run_case(
            REQUIRE_GENERATED_Q6_LM_HEAD_ARGMAX="1",
            FAKE_GENERATED_Q6_LM_HITS="1",
            FAKE_GENERATED_Q6_LM_FALLBACKS="1",
        )
        self.assertEqual(1, completed.returncode)
        self.assertIn("Generated Q6 LM-head gate failed", completed.stderr)


if __name__ == "__main__":
    unittest.main()
