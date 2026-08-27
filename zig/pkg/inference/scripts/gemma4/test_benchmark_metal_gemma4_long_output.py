#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Fast contract tests for the paired Gemma4 long-output benchmark."""

import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from gemma4_metal_long_output import (  # noqa: E402
    BenchmarkContractError,
    _file_sha256,
    audit_llama_loaded_libraries,
    build_result,
    canonical_git_untracked_paths,
    canonical_git_submodule_violations,
    canonical_long_output_prompt,
    gate_errors,
    llama_bundle_manifest,
    llama_bundle_manifest_sha256,
    paired_bootstrap_interval,
    one_sided_sign_test_p,
    parse_llama_verbose_prompt_token_ids,
    parse_llama_metal_runtime,
    parse_llama_timing,
    ratio_stats,
    validate_canonical_git_worktree,
)


BENCHMARK = SCRIPT_DIR / "benchmark_metal_gemma4_long_output.sh"
OUTPUT_TOKENS = 300
TOKEN_IDS = " ".join(str(index) for index in range(OUTPUT_TOKENS))
TOKEN_IDS_SHA256 = hashlib.sha256(TOKEN_IDS.encode()).hexdigest()
PROMPT_TOKEN_IDS = " ".join(str(index) for index in range(2003))
PROMPT_TOKEN_IDS_SHA256 = hashlib.sha256(PROMPT_TOKEN_IDS.encode()).hexdigest()


def executable(path: Path, source: str) -> None:
    path.write_text(source)
    path.chmod(0o755)


def b10182_perf_block(tokens: int = OUTPUT_TOKENS) -> str:
    return "\n".join(
        (
            "0.17.923.567 I common_perf_print: sampling time = 50.00 ms / 300 runs",
            "0.17.923.567 I common_perf_print: prompt eval time = 4000.00 ms / 2003 tokens",
            f"0.17.923.567 I common_perf_print: eval time = 5600.00 ms / {tokens - 1} runs",
            f"0.17.923.567 I common_perf_print: total time = 9655.00 ms / {2003 + tokens - 1} tokens",
            "0.17.923.567 I common_perf_print: unaccounted time = 5.00 ms / 0.1 %",
            f"0.17.923.567 I common_perf_print: graphs reused = {tokens - 3}",
        )
    )


class ParserContractTests(unittest.TestCase):
    def test_file_sha256_reads_incrementally(self) -> None:
        payload = b"gemma4-a4b" * 257

        class GuardedReader(io.BytesIO):
            read_sizes: list[int]

            def __init__(self, value: bytes) -> None:
                super().__init__(value)
                self.read_sizes = []

            def read(self, size: int = -1) -> bytes:
                if size <= 0:
                    raise AssertionError("file hashing must use bounded reads")
                self.read_sizes.append(size)
                return super().read(min(size, 97))

        class GuardedPath:
            def __init__(self, reader: GuardedReader) -> None:
                self.reader = reader

            def open(self, mode: str) -> GuardedReader:
                if mode != "rb":
                    raise AssertionError(f"unexpected hash open mode: {mode}")
                return self.reader

        reader = GuardedReader(payload)
        self.assertEqual(
            _file_sha256(GuardedPath(reader)),  # type: ignore[arg-type]
            hashlib.sha256(payload).hexdigest(),
        )
        self.assertGreater(len(reader.read_sizes), 1)
        self.assertTrue(all(size == 1024 * 1024 for size in reader.read_sizes))

    def test_canonical_git_worktree_rejects_existing_untracked_content(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            repo = Path(raw)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            untracked = repo / "existing-untracked.txt"
            untracked.write_text("before\n")
            self.assertEqual(
                canonical_git_untracked_paths(repo),
                ("existing-untracked.txt",),
            )
            with self.assertRaisesRegex(BenchmarkContractError, "no untracked files"):
                validate_canonical_git_worktree(repo)
            untracked.write_text("mutated during benchmark\n")
            with self.assertRaisesRegex(BenchmarkContractError, "no untracked files"):
                validate_canonical_git_worktree(repo)
        self.assertIn("validate-git-worktree", BENCHMARK.read_text())

    def test_canonical_git_worktree_rejects_dirty_submodule_content(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "source"
            parent = root / "parent"
            source.mkdir()
            parent.mkdir()
            for repo in (source, parent):
                subprocess.run(["git", "init", "-q", str(repo)], check=True)
                subprocess.run(
                    ["git", "-C", str(repo), "config", "user.email", "benchmark@test.invalid"],
                    check=True,
                )
                subprocess.run(
                    ["git", "-C", str(repo), "config", "user.name", "Benchmark Test"],
                    check=True,
                )
            (source / "tracked.txt").write_text("tracked\n")
            subprocess.run(["git", "-C", str(source), "add", "tracked.txt"], check=True)
            subprocess.run(
                ["git", "-C", str(source), "commit", "-q", "-m", "source"],
                check=True,
            )
            subprocess.run(
                [
                    "git",
                    "-c",
                    "protocol.file.allow=always",
                    "-C",
                    str(parent),
                    "submodule",
                    "add",
                    "-q",
                    str(source),
                    "deps/child",
                ],
                check=True,
            )
            subprocess.run(["git", "-C", str(parent), "commit", "-q", "-am", "parent"], check=True)
            self.assertEqual(canonical_git_submodule_violations(parent), ())
            marker = parent / "deps/child/untracked.txt"
            marker.write_text("before\n")
            self.assertIn("dirty worktree", canonical_git_submodule_violations(parent)[0])
            with self.assertRaisesRegex(BenchmarkContractError, "clean submodules"):
                validate_canonical_git_worktree(parent)
            marker.write_text("mutated during benchmark\n")
            with self.assertRaisesRegex(BenchmarkContractError, "clean submodules"):
                validate_canonical_git_worktree(parent)

    def test_canonical_prompt_byte_digest_is_locked(self) -> None:
        prompt = canonical_long_output_prompt()
        digest = hashlib.sha256(prompt.encode()).hexdigest()
        self.assertEqual(
            digest,
            "1c0477d5acd34e3c76c1db35506df4f5eb66e59084efaf3aa36d8a2fe515a01f",
        )
        self.assertIn(
            f'CANONICAL_PROMPT_SHA256="{digest}"',
            BENCHMARK.read_text(),
        )
        rendered = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / "gemma4_metal_long_output.py"), "render-prompt"],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(rendered.stdout, prompt)

    def test_b10182_timestamp_prefix_is_parsed(self) -> None:
        timing = parse_llama_timing(b10182_perf_block())
        self.assertEqual(timing.logger, "common_perf_print")
        self.assertEqual(timing.prompt_tokens, 2003)
        self.assertEqual(timing.eval_runs, 299)
        self.assertEqual(timing.total_tokens, 2302)
        self.assertEqual(timing.graphs_reused, 297)
        self.assertAlmostEqual(timing.total_ms, 9655.0)

    def test_b10182_metal_backend_markers_are_parsed(self) -> None:
        runtime = parse_llama_metal_runtime(
            "\n".join(
                (
                    "llama_prepare_model_devices: using device MTL0 (Apple M4)",
                    "load_tensors: offloaded 43/43 layers to GPU",
                    "ggml_metal_init: found device: Apple M4",
                    "ggml_metal_init: picking default device: Apple M4",
                )
            )
        )
        self.assertEqual(runtime.default_device, "Apple M4")
        self.assertEqual(runtime.offloaded_layers, 43)
        self.assertEqual(runtime.total_layers, 43)

    def test_b10182_verbose_prompt_ids_are_parsed_exactly(self) -> None:
        normalized, values = parse_llama_verbose_prompt_token_ids(
            "\n".join(
                (
                    "llama_completion: number of tokens in prompt = 3",
                    "     2 -> '<bos>'",
                    "   105 -> '<|turn>'",
                    "   236 -> ' user'",
                )
            )
        )
        self.assertEqual(normalized, "2 105 236")
        self.assertEqual(values, [2, 105, 236])

        multiline_normalized, multiline_values = parse_llama_verbose_prompt_token_ids(
            "\n".join(
                (
                    "llama_completion: number of tokens in prompt = 3",
                    "     2 -> '<bos>'",
                    "   107 -> '",
                    "'",
                    "   236 -> ' user'",
                )
            )
        )
        self.assertEqual(multiline_normalized, "2 107 236")
        self.assertEqual(multiline_values, [2, 107, 236])

        with self.assertRaisesRegex(BenchmarkContractError, "ID count"):
            parse_llama_verbose_prompt_token_ids(
                "llama_completion: number of tokens in prompt = 2\n     2 -> '<bos>'"
            )
        with self.assertRaisesRegex(BenchmarkContractError, "negative token ID"):
            parse_llama_verbose_prompt_token_ids(
                "llama_completion: number of tokens in prompt = 1\n    -1 -> '<invalid>'"
            )

    def test_dyld_preflight_closure_is_bundle_or_system_only(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gemma4-dyld-audit-") as raw:
            bundle = Path(raw) / "bundle"
            bundle.mkdir()
            names = (
                "llama-completion",
                "libllama-completion-impl.dylib",
                "libllama-common.0.dylib",
                "libllama.0.dylib",
                "libggml.0.dylib",
                "libggml-metal.0.dylib",
            )
            for name in names:
                (bundle / name).write_bytes(name.encode())
            lines = [
                f"dyld[123]: <UUID-{index}> {bundle / name}"
                for index, name in enumerate(names)
            ]
            lines.append("dyld[123]: <SYSTEM> /usr/lib/libSystem.B.dylib")
            audit = audit_llama_loaded_libraries(
                "\n".join(lines), bundle, bundle / "llama-completion"
            )
            self.assertTrue(audit["passed"])
            self.assertEqual(audit["bundle_image_count"], len(names))
            self.assertEqual(audit["system_image_count"], 1)
            self.assertRegex(audit["audit_sha256"], r"^[0-9a-f]{64}$")

            outside = Path(raw) / "libggml-metal-injected.dylib"
            outside.write_bytes(b"injected")
            with self.assertRaisesRegex(BenchmarkContractError, "outside pinned bundle"):
                audit_llama_loaded_libraries(
                    "\n".join(lines + [f"dyld[123]: <INJECTED> {outside}"]),
                    bundle,
                    bundle / "llama-completion",
                )

    def test_timing_block_must_be_unique_and_complete(self) -> None:
        block = b10182_perf_block()
        with self.assertRaisesRegex(BenchmarkContractError, "exactly one"):
            parse_llama_timing(block + "\n" + block)
        with self.assertRaisesRegex(BenchmarkContractError, "exactly one"):
            parse_llama_timing("\n".join(block.splitlines()[:-4]))

    def test_ratios_are_summarized_pairwise(self) -> None:
        rows = [
            {"ratio": 100.0},
            {"ratio": 0.1},
            {"ratio": 10.0},
        ]
        self.assertEqual(ratio_stats(rows, "ratio")["median"], 10.0)

    def test_paired_bootstrap_interval_is_deterministic(self) -> None:
        rows = [{"ratio": value} for value in (0.8, 0.9, 1.0, 1.1, 1.2, 1.3)]
        first = paired_bootstrap_interval(rows, "ratio", samples=1_000)
        second = paired_bootstrap_interval(rows, "ratio", samples=1_000)
        self.assertEqual(first, second)
        self.assertEqual(first["confidence"], 0.95)
        self.assertEqual(first["samples"], 1_000)
        self.assertLessEqual(first["lower"], first["median"])
        self.assertGreaterEqual(first["upper"], first["median"])

    def test_exact_paired_sign_test_keeps_six_run_promotion_conservative(self) -> None:
        self.assertAlmostEqual(one_sided_sign_test_p(6, 6), 1 / 64)
        self.assertGreater(one_sided_sign_test_p(5, 6), 0.05)
        with self.assertRaisesRegex(BenchmarkContractError, "sign-test shape"):
            one_sided_sign_test_p(7, 6)

    def test_phase_cv_violations_are_gated(self) -> None:
        result = {
            "cv_gate": {"violations": {"antfly_decode_ms": 0.08}},
            "max_cv": 0.03,
            "total_ratio": 1.0,
            "max_total_ratio": 1.1,
            "decode_ratio": 1.0,
            "min_decode_ratio": 0.9,
        }
        self.assertIn("antfly_decode_ms=0.080", gate_errors(result)[0])


class HarnessContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory(prefix="gemma4-long-output-test-")
        self.tmp = Path(self.tempdir.name)
        self.model = self.tmp / "model.gguf"
        self.model.write_bytes(b"fake gguf")
        self.antfly = self.tmp / "antfly"
        self.llama_bundle = self.tmp / "llama-bundle"
        self.llama_bundle.mkdir()
        self.llama = self.llama_bundle / "llama-completion"
        (self.llama_bundle / "libggml-metal.test.dylib").write_bytes(b"fake metal backend")
        self.zig = self.tmp / "zig"

        executable(
            self.zig,
            """#!/usr/bin/env python3
import sys
import os

if sys.argv[1:] != ["version"]:
    raise SystemExit("fake Zig only supports the version probe")
print("0.16.0-dev.test")
""",
        )

        executable(
            self.antfly,
            """#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
if Path(sys.argv[0]).name == "antfly":
    if not args or args[0] != "inference":
        raise SystemExit("monolithic antfly requires the inference subcommand")
    args = args[1:]
tokens = int(args[args.index("--max-tokens") + 1])
split_enable = os.environ.get("TERMITE_METAL_ENABLE_DECODE_GQA_SPLIT")
split_gqa = split_enable in (None, "1") and os.environ.get("TERMITE_METAL_DISABLE_DECODE_GQA_SPLIT") != "1"
def env_true(name):
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")

prefill_direct = (
    env_true("TERMITE_METAL_ENABLE_PREFILL_SG_DIRECT_LOAD")
    and not env_true("TERMITE_METAL_DISABLE_PREFILL_SG_DIRECT_LOAD")
)
fast_prepared = (
    os.environ.get("TERMITE_METAL_DISABLE_FAST_PREPARED_FRAME") != "1"
    and os.environ.get("TERMITE_METAL_FORCE_DIAGNOSTIC_COMMAND_BUFFERS") != "1"
)
decode_frames = tokens - 1
attention_calls = decode_frames * 42
flash_local = 35
flash_hd512 = 7
q4_row_one = decode_frames * 210
timing_path = Path(args[args.index("--json-timing") + 1])
prefill_ms = float(os.environ.get("FAKE_ANTFLY_PREFILL_MS", "4000"))
decode_ms = float(os.environ.get("FAKE_ANTFLY_DECODE_MS", "6000"))
total_ms = prefill_ms + decode_ms
timing_path.write_text(json.dumps({
    "backend": "metal",
    "tokens": tokens,
    "token_ids": list(range(tokens)),
    "finish_reason": "length",
    "timing_ms": {
        "generate": total_ms,
        "prefill_inner": prefill_ms,
        "decode_inner": decode_ms,
    },
    "speculative": None,
    "draft_cuda": None,
    "draft_cuda_generate": None,
    "runtime": {"decode_greedy_calls": decode_frames},
    "generation_decoder_runtime": {"forward_attempts": decode_frames},
    "metal": {
        "device": "Apple M4",
        "device_registry_id": 123456789,
        "native_quant_null": False,
        "runtime_command_operators": {"fallback": 0},
        "attention_dispatch": {
            "paged_1x": 0 if split_gqa else attention_calls,
            "decode_gqa_split": attention_calls if split_gqa else 0,
            "generated_flash_prefill": flash_local,
            "generated_flash_prefill_hd512": flash_hd512,
            "prefill_direct_kv": flash_local + flash_hd512 if prefill_direct else 0,
            "prefill_paged_kv": 0 if prefill_direct else flash_local + flash_hd512,
        },
        "prepared_frame": {
            "fast_path": decode_frames if fast_prepared else 0,
            "fallback": 0 if fast_prepared else decode_frames,
        },
        "q4_0_policy": {
            "mmv_nr4_nsg2": q4_row_one,
            "mmv_nr8_nsg2": 0,
            "mmv_nr4_nsg4": 0,
            "mmv_nr8_nsg4": 0,
            "mmv_variant_fallbacks": 0,
            "mm_sg_aligned": 342,
            "mm_sg_aligned_tail": 342,
            "mm_sg_unrolled": 0,
        },
        "frame_fallbacks": {
            "decode_fallback": 0,
            "prefill_plan_fail": 0,
            "prefill_execute_fail": 0,
        },
        "quant_kernel_plan": {"fast_path_misses": 0, "unsupported_routes": 0},
    },
}))
print("generate-setup: live whole-model executor skipped")
print("gen_debug: executePrefill whole-model fast path seq_len=2003")
print("prompt_token_ids:", " ".join(str(i) for i in range(2003)))
print("token_ids:", " ".join(str(i) for i in range(tokens)))
print(
    f"metal_attention_dispatch: paged_1x={0 if split_gqa else attention_calls} "
    f"decode_gqa_split={attention_calls if split_gqa else 0} "
    f"generated_decode_1x=0 generated_flash_prefill={flash_local} "
    f"generated_flash_prefill_hd512={flash_hd512} "
    f"prefill_direct_kv={flash_local + flash_hd512 if prefill_direct else 0} "
    f"prefill_paged_kv={0 if prefill_direct else flash_local + flash_hd512} "
    "generated_rms_norm=0"
)
print(
    "metal_decode_gqa_split_schedule: "
    f"legacy_total={attention_calls if split_gqa else 0} "
    f"swa_total={35 * decode_frames if split_gqa else 0} "
    f"global_total={7 * decode_frames if split_gqa else 0} "
    "swa_s8=0 swa_s16=0 swa_s24=0 "
    f"swa_s32={35 * decode_frames if split_gqa else 0} "
    "global_s8=0 global_s16=0 global_s24=0 "
    f"global_s32={7 * decode_frames if split_gqa else 0} "
    "fallbacks=0 invalid_overrides=0"
)
print(
    f"metal_prepared_frame: fast_path={decode_frames if fast_prepared else 0} "
    f"fallback={0 if fast_prepared else decode_frames}"
)
print("metal_runtime_memory: frame_retained_mb=0")
print(f"metal_q4_0_dispatch: linear_reduce_rows={q4_row_one}/0/0/0 pair_act_reduce=0")
print(
    f"metal_q4_0_policy: mmv_nr4_nsg2={q4_row_one} mmv_nr8_nsg2=0 "
    "mmv_nr4_nsg4=0 mmv_nr8_nsg4=0 mmv_variant_fallbacks=0 "
    "mm_sg_aligned=342 mm_sg_aligned_tail=342 mm_sg_unrolled=0"
)
print(f"metal_q4_q6_k_dispatch: q6_linear_reduce_rows={tokens}/0/0/0")
print("metal_jit_exact_dispatch: q4_0=0")
print("metal_q4_0_encode_us: linear_reduce=1234")
""",
        )
        executable(
            self.llama,
            """#!/usr/bin/env python3
import os
import sys

args = sys.argv[1:]
if "--version" in args:
    print("version: 10182 (afeebe103)")
    print("built with AppleClang 21.0.0 for Darwin arm64")
    raise SystemExit(0)
if "-f" not in args or "-p" in args or "--no-escape" not in args:
    raise SystemExit("benchmark must pass the exact prompt file without escape processing")
if "-lv" not in args or args[args.index("-lv") + 1] != "4":
    raise SystemExit("benchmark must request llama log verbosity 4")
if "--log-colors" not in args or args[args.index("--log-colors") + 1] != "off":
    raise SystemExit("benchmark must disable llama log colors")
tokens = int(args[args.index("-n") + 1])
if "--verbose-prompt" in args:
    print("llama_completion: number of tokens in prompt = 2003")
    drift = os.environ.get("FAKE_LLAMA_PROMPT_DRIFT") == "1"
    for token in range(2003):
        value = token + 1 if drift and token == 2002 else token
        print(f"{value:6d} -> 'piece-{value}'")
print("llama_prepare_model_devices: using device MTL0 (Apple M4)")
print("load_tensors: offloaded 43/43 layers to GPU")
print("ggml_metal_init: found device: Apple M4")
print("ggml_metal_init: picking default device: Apple M4")
print("0.17.923.567 I common_perf_print: sampling time = 50.00 ms / 300 runs")
print("0.17.923.567 I common_perf_print: prompt eval time = 4000.00 ms / 2003 tokens")
print(f"0.17.923.567 I common_perf_print: eval time = 5600.00 ms / {tokens - 1} runs")
print(f"0.17.923.567 I common_perf_print: total time = 9655.00 ms / {2003 + tokens - 1} tokens")
print("0.17.923.567 I common_perf_print: unaccounted time = 5.00 ms / 0.1 %")
print(f"0.17.923.567 I common_perf_print: graphs reused = {tokens - 3}")
""",
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def environment(self, out: Path) -> dict[str, str]:
        env = os.environ.copy()
        env.update(
            {
                "MODEL": str(self.model),
                "GGUF": str(self.model),
                "ANTFLY_BIN": str(self.antfly),
                "LLAMA_CPP_BIN": str(self.llama),
                "LLAMA_CPP_BUNDLE_ROOT": str(self.llama_bundle),
                "ZIG_BIN": str(self.zig),
                "OUT_DIR": str(out),
                "OUTPUT_TOKENS": str(OUTPUT_TOKENS),
                "WARMUPS": "0",
                "RUNS": "2",
                "COOLDOWN_SECONDS": "0",
                "MAX_TOTAL_RATIO": "1.10",
                "MIN_DECODE_RATIO": "0.90",
                "MAX_CV": "0.01",
                "EXPECTED_TOKEN_IDS_SHA256": TOKEN_IDS_SHA256,
                "EXPECTED_PROMPT_TOKEN_IDS_SHA256": PROMPT_TOKEN_IDS_SHA256,
                "EXPECTED_LLAMA_CPP_SHA256": hashlib.sha256(self.llama.read_bytes()).hexdigest(),
                "EXPECTED_LLAMA_CPP_BUNDLE_SHA256": llama_bundle_manifest_sha256(
                    llama_bundle_manifest(self.llama_bundle)
                ),
                "REQUIRE_CONFIDENCE": "0",
                "ALLOW_NONCANONICAL_POLICY": "1",
            }
        )
        env.pop("TERMITE_METAL_ENABLE_DECODE_GQA_SPLIT", None)
        env.pop("TERMITE_METAL_DISABLE_DECODE_GQA_SPLIT", None)
        env.pop("LLAMA_CPP_EXPECTED_SHA256", None)
        env.pop("FAKE_ANTFLY_PREFILL_MS", None)
        env.pop("FAKE_ANTFLY_DECODE_MS", None)
        env.pop("FAKE_LLAMA_PROMPT_DRIFT", None)
        for name in tuple(env):
            if name.startswith(("LLAMA_ARG_", "LLAMA_LOG_", "GGML_", "DYLD_", "GIT_")) or name in (
                "LD_LIBRARY_PATH",
                "LD_PRELOAD",
            ):
                env.pop(name)
        for name in (
            "TERMITE_METAL_ENABLE_PREFILL_SG_DIRECT_LOAD",
            "TERMITE_METAL_DISABLE_PREFILL_SG_DIRECT_LOAD",
            "TERMITE_METAL_Q4_0_MMV_VARIANT",
            "TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO",
            "TERMITE_METAL_TRACE_Q4_0_MMV_VARIANT",
            "TERMITE_METAL_DISABLE_SWA_SCAN_CLAMP",
            "TERMITE_METAL_DISABLE_FAST_PREPARED_FRAME",
            "TERMITE_METAL_FORCE_DIAGNOSTIC_COMMAND_BUFFERS",
            "TERMITE_METAL_DECODE_GQA_SPLIT_SWA_VARIANT",
            "TERMITE_METAL_DECODE_GQA_SPLIT_GLOBAL_VARIANT",
            "TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE",
            "EXPECT_GENERATED_FLASH_PREFILL_CALLS",
            "EXPECT_GENERATED_FLASH_PREFILL_HD512_CALLS",
            "EXPECT_PREFILL_DIRECT_KV",
            "EXPECT_FAST_PREPARED_FRAME",
            "EXPECT_Q4_0_MMV_VARIANT",
            "EXPECT_SWA_SCAN_CLAMP",
            "EXPECT_ANTFLY_METAL_DEVICE",
            "EXPECT_LLAMA_METAL_DEVICE",
            "EXPECT_LLAMA_OFFLOADED_LAYERS",
        ):
            env.pop(name, None)
        return env

    def canonical_environment(self, out: Path) -> dict[str, str]:
        env = self.environment(out)
        env.pop("ALLOW_NONCANONICAL_POLICY", None)
        env.pop("PROMPT", None)
        env.update(
            {
                "OUTPUT_TOKENS": "300",
                "RUNS": "6",
                "WARMUPS": "1",
                "COOLDOWN_SECONDS": "45",
                "MAX_TOTAL_RATIO": "0.98",
                "MIN_DECODE_RATIO": "1.02",
                "MAX_CV": "0.03",
                "REQUIRE_CONFIDENCE": "1",
                "ANTFLY_CACHE_DTYPE": "f16",
                "LLAMA_CACHE_TYPE_K": "f16",
                "LLAMA_CACHE_TYPE_V": "f16",
                "LLAMA_CONTEXT_SIZE": "4096",
                "EXPECTED_LLAMA_CPP_BUILD": "10182",
                "EXPECTED_LLAMA_CPP_SHA256": "faa8b1c2a6c69f50b0fcec71af86eda757d34f78bbbddbb3f485f170bc586d2f",
                "EXPECTED_LLAMA_CPP_BUNDLE_SHA256": "23e601e646bbd901c4d4f1c1158fd4c99053d08969e6aa07f2005e87dc05a1fc",
                "EXPECTED_PROMPT_TOKEN_IDS_SHA256": "d882b403c0229eb7ffc70ff2539123283996548d5eb67a4ef34db619be6e8a42",
                "EXPECTED_TOKEN_IDS_SHA256": "711ddb9890d0fd867d7cd9c1ce10fe4c407a2ec597464fe42912a0802afe7052",
            }
        )
        return env

    def run_harness(self, out: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(BENCHMARK)],
            env=env or self.environment(out),
            check=True,
            text=True,
            capture_output=True,
        )

    def test_full_harness_contract_and_provenance(self) -> None:
        out = self.tmp / "results"
        completed = self.run_harness(out)
        self.assertIn("prompt=2003 output=300", completed.stdout)
        summary = json.loads((out / "summary.json").read_text())
        self.assertEqual(summary["schema"], "antfly.gemma4_metal_long_output.v4")
        self.assertEqual(summary["prompt_tokens"], 2003)
        self.assertEqual(summary["output_tokens"], 300)
        self.assertLessEqual(summary["total_ratio"], 1.10)
        self.assertGreaterEqual(summary["decode_ratio"], 0.90)
        self.assertEqual(summary["total_ratio"], summary["paired_ratios"]["total_latency_ratio"]["median"])
        self.assertTrue(summary["cv_gate"]["passed"])
        self.assertFalse(summary["confidence_gate"]["required"])
        self.assertFalse(summary["canonical_policy"])
        self.assertFalse(summary["promotion_eligible_policy"])
        self.assertFalse(summary["confidence_gate"]["eligible"])
        self.assertFalse(summary["confidence_gate"]["passed"])
        self.assertEqual(summary["confidence_gate"]["minimum_runs"], 6)
        self.assertEqual(
            set(summary["paired_confidence_intervals"]),
            {
                "total_latency_ratio",
                "prefill_latency_ratio",
                "decode_latency_ratio",
                "decode_throughput_ratio",
            },
        )
        self.assertTrue(summary["execution_order_gate"]["passed"])
        self.assertEqual(
            summary["execution_order_gate"]["measured_first_counts"],
            {"antfly": 1, "llama": 1},
        )
        self.assertEqual(
            [record["implementation"] for record in summary["execution_order"]],
            ["antfly", "llama", "llama", "antfly"],
        )
        self.assertEqual(
            set(summary["cv_gate"]["metrics"]),
            {
                "antfly_total_ms",
                "llama_total_ms",
                "antfly_prefill_ms",
                "llama_prompt_ms",
                "antfly_decode_ms",
                "llama_decode_ms",
            },
        )
        self.assertEqual(summary["token_ids_sha256"], TOKEN_IDS_SHA256)
        self.assertEqual(summary["prompt_token_ids_sha256"], PROMPT_TOKEN_IDS_SHA256)
        self.assertEqual(
            summary["expected_prompt_token_ids_sha256"], PROMPT_TOKEN_IDS_SHA256
        )
        self.assertTrue(summary["cross_implementation_prompt_token_contract_passed"])
        self.assertTrue(summary["antfly_generated_token_contract_passed"])
        self.assertEqual(
            summary["cross_implementation_generated_token_contract"],
            "unavailable_in_pinned_llama_completion",
        )
        self.assertEqual(summary["llama_prompt_token_ids_sha256"], PROMPT_TOKEN_IDS_SHA256)
        self.assertTrue(summary["no_mtp"])
        self.assertTrue(summary["route_contract_passed"])
        row = summary["rows"][0]
        self.assertEqual(row["paged_1x_calls"], 0)
        self.assertEqual(row["decode_gqa_split_calls"], 12_558)
        self.assertEqual(row["decode_gqa_split_swa_s32_calls"], 10_465)
        self.assertEqual(row["decode_gqa_split_global_s32_calls"], 2_093)
        self.assertEqual(row["decode_gqa_split_schedule_fallbacks"], 0)
        self.assertEqual(row["decode_gqa_split_schedule_invalid_overrides"], 0)
        self.assertEqual(row["generated_flash_prefill_calls"], 35)
        self.assertEqual(row["generated_flash_prefill_hd512_calls"], 7)
        self.assertEqual(row["prefill_direct_kv_calls"], 0)
        self.assertEqual(row["prefill_paged_kv_calls"], 42)
        self.assertEqual(row["prepared_frame_fast_path_calls"], 299)
        self.assertEqual(row["prepared_frame_fallback_calls"], 0)
        self.assertTrue(row["swa_scan_clamp_enabled"])
        self.assertEqual(row["llama_metal_device"], "Apple M4")
        self.assertEqual(row["antfly_metal_device"], "Apple M4")
        self.assertEqual(row["antfly_metal_device_registry_id"], 123456789)
        self.assertEqual(row["llama_offloaded_layers"], 43)
        self.assertEqual(row["llama_total_layers"], 43)
        self.assertEqual(row["q4_0_linear_reduce_rows_1"], 62_790)
        self.assertEqual(row["q4_0_mmv_nr4_nsg2_calls"], 62_790)
        self.assertEqual(row["q4_0_mmv_variant_fallbacks"], 0)
        self.assertEqual(row["q4_0_mm_sg_aligned_calls"], 342)
        self.assertEqual(row["q4_0_mm_sg_aligned_tail_calls"], 342)
        self.assertEqual(row["q6_k_linear_reduce_rows_1"], 300)
        self.assertEqual(row["q4_0_linear_reduce_encode_us"], 1234)
        metadata = summary["metadata"]
        self.assertEqual(metadata["schema"], "antfly.gemma4_metal_long_output.metadata.v4")
        self.assertEqual(metadata["llama_cpp_build"], 10182)
        self.assertEqual(metadata["llama_cpp_commit"], "afeebe103")
        self.assertEqual(metadata["llama_cpp_full_commit"], "afeebe103bd99cda8f5dfaefcabadf890db7fda7")
        self.assertEqual(metadata["llama_cpp_binary_sha256"], hashlib.sha256(self.llama.read_bytes()).hexdigest())
        self.assertEqual(metadata["llama_cpp_bundle_root"], str(self.llama_bundle.resolve()))
        self.assertEqual(
            metadata["llama_cpp_bundle_sha256"],
            llama_bundle_manifest_sha256(llama_bundle_manifest(self.llama_bundle)),
        )
        self.assertEqual(metadata["antfly_binary_sha256"], hashlib.sha256(self.antfly.read_bytes()).hexdigest())
        self.assertEqual(metadata["antfly_model_argument"], str(self.model.resolve()))
        self.assertEqual(metadata["llama_model_argument"], str(self.model.resolve()))
        self.assertEqual(metadata["zig_bin"], str(self.zig))
        self.assertEqual(metadata["zig_resolved_bin"], str(self.zig.resolve()))
        self.assertEqual(metadata["zig_version"], "0.16.0-dev.test")
        self.assertEqual(metadata["prompt_file"], "prompt.txt")
        self.assertEqual(
            metadata["prompt_sha256"],
            hashlib.sha256((out / "prompt.txt").read_bytes()).hexdigest(),
        )
        self.assertEqual(
            metadata["policy_environment_prefixes"],
            [
                "TERMITE_",
                "ANTFLY_GEMMA4_",
                "ANTFLY_INFERENCE_",
                "LLAMA_ARG_",
                "LLAMA_LOG_",
                "GGML_",
            ],
        )
        self.assertEqual(
            metadata["process_policy_env"],
            {"TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE": "1"},
        )
        self.assertEqual(
            metadata["runner_injected_env"],
            {
                "TERMITE_GEN_STAGE_DEBUG": "1",
                "TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE": "1",
            },
        )
        self.assertIn("Python script", metadata["llama_cpp_binary_file_type"])
        self.assertEqual(
            metadata["llama_cpp_loader_audit_mode"],
            "skipped_non_macho_noncanonical",
        )
        self.assertEqual(metadata["process_loader_env"], {})
        self.assertEqual(
            summary["llama_cpp_loader_audit"]["mode"],
            "skipped_non_macho_noncanonical",
        )
        self.assertFalse(summary["llama_cpp_loader_audit"]["passed"])
        self.assertIn("built with AppleClang", metadata["llama_cpp_version_output"])
        repo_root = SCRIPT_DIR.parents[4]
        git_status = subprocess.check_output(
            ["git", "-C", str(repo_root), "status", "--porcelain=v1", "--untracked-files=all"],
            env={**os.environ, "LC_ALL": "C"},
        )
        tracked_diff = subprocess.check_output(
            ["git", "-C", str(repo_root), "diff", "--binary", "--no-ext-diff", "HEAD", "--"],
            env={**os.environ, "LC_ALL": "C"},
        )
        empty_status_sha256 = hashlib.sha256(b"").hexdigest()
        self.assertEqual(metadata["git_dirty"], metadata["git_status_sha256"] != empty_status_sha256)
        # Other agents may be editing this shared worktree while this test runs. Preserve the
        # stronger live comparison whenever the status snapshot is still the recorded one.
        if metadata["git_status_sha256"] == hashlib.sha256(git_status).hexdigest():
            self.assertEqual(
                metadata["git_tracked_diff_sha256"],
                hashlib.sha256(tracked_diff).hexdigest(),
            )
        for key in (
            "git_tracked_diff_sha256",
            "git_status_sha256",
            "benchmark_harness_sha256",
            "benchmark_parser_sha256",
        ):
            self.assertRegex(metadata[key], r"^[0-9a-f]{64}$")
        self.assertEqual(
            metadata["benchmark_harness_sha256"],
            hashlib.sha256(BENCHMARK.read_bytes()).hexdigest(),
        )
        self.assertEqual(
            metadata["benchmark_parser_sha256"],
            hashlib.sha256((SCRIPT_DIR / "gemma4_metal_long_output.py").read_bytes()).hexdigest(),
        )
        self.assertEqual(
            set(metadata["metal_policy_env"]),
            {
                "TERMITE_METAL_ENABLE_PREFILL_SG_DIRECT_LOAD",
                "TERMITE_METAL_DISABLE_PREFILL_SG_DIRECT_LOAD",
                "TERMITE_METAL_Q4_0_MMV_VARIANT",
                "TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO",
                "TERMITE_METAL_TRACE_Q4_0_MMV_VARIANT",
                "TERMITE_METAL_DISABLE_SWA_SCAN_CLAMP",
                "TERMITE_METAL_DISABLE_FAST_PREPARED_FRAME",
                "TERMITE_METAL_FORCE_DIAGNOSTIC_COMMAND_BUFFERS",
                "TERMITE_METAL_DECODE_GQA_SPLIT_SWA_VARIANT",
                "TERMITE_METAL_DECODE_GQA_SPLIT_GLOBAL_VARIANT",
                "TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE",
                "EXPECT_GENERATED_FLASH_PREFILL_CALLS",
                "EXPECT_GENERATED_FLASH_PREFILL_HD512_CALLS",
                "EXPECT_PREFILL_DIRECT_KV",
                "EXPECT_FAST_PREPARED_FRAME",
                "EXPECT_Q4_0_MMV_VARIANT",
                "EXPECT_SWA_SCAN_CLAMP",
                "EXPECT_ANTFLY_METAL_DEVICE",
                "EXPECT_LLAMA_METAL_DEVICE",
                "EXPECT_LLAMA_OFFLOADED_LAYERS",
            },
        )
        self.assertTrue(summary["policy_route_expectations"]["q4_0_mmv_portfolio"])
        self.assertEqual(summary["policy_route_expectations"]["generated_flash_prefill_calls"], 35)
        self.assertEqual(summary["policy_route_expectations"]["generated_flash_prefill_hd512_calls"], 7)
        self.assertFalse(summary["policy_route_expectations"]["prefill_direct_kv"])
        self.assertTrue(summary["policy_route_expectations"]["fast_prepared_frame"])
        self.assertEqual(summary["policy_route_expectations"]["q4_0_mmv_variant"], "nr4-nsg2")
        self.assertTrue(summary["policy_route_expectations"]["swa_scan_clamp"])
        self.assertEqual(summary["llama_backend_expectations"]["metal_device"], "Apple M4")
        self.assertEqual(summary["llama_backend_expectations"]["offloaded_layers"], 43)
        self.assertEqual(summary["antfly_backend_expectations"]["metal_device"], "Apple M4")
        self.assertTrue(summary["antfly_backend_expectations"]["device_registry_id_positive"])
        self.assertEqual(
            summary["antfly_backend_expectations"]["device_registry_id"], 123456789
        )

        with self.assertRaisesRegex(BenchmarkContractError, "immutable benchmark metadata pin"):
            build_result(out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01, "0" * 64)

        antfly_json = out / "antfly-1.json"
        payload = json.loads(antfly_json.read_text())
        payload["speculative"] = {"accepted": 1}
        antfly_json.write_text(json.dumps(payload))
        with self.assertRaisesRegex(BenchmarkContractError, "MTP/speculative"):
            build_result(out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)

    def test_paged_attention_rollback_route(self) -> None:
        out = self.tmp / "paged-results"
        env = self.environment(out)
        env["ALLOW_NONCANONICAL_POLICY"] = "1"
        env["TERMITE_METAL_DISABLE_DECODE_GQA_SPLIT"] = "1"
        self.run_harness(out, env)
        row = json.loads((out / "summary.json").read_text())["rows"][0]
        self.assertEqual(row["paged_1x_calls"], 12_558)
        self.assertEqual(row["decode_gqa_split_calls"], 0)

    def test_explicit_policy_rollback_routes_and_metadata(self) -> None:
        out = self.tmp / "policy-rollback-results"
        env = self.environment(out)
        env["ALLOW_NONCANONICAL_POLICY"] = "1"
        env.update(
            {
                "TERMITE_METAL_DISABLE_PREFILL_SG_DIRECT_LOAD": "1",
                "TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO": "1",
                "TERMITE_METAL_DISABLE_SWA_SCAN_CLAMP": "1",
                "TERMITE_METAL_DISABLE_FAST_PREPARED_FRAME": "1",
                "TERMITE_METAL_FORCE_DIAGNOSTIC_COMMAND_BUFFERS": "1",
                "EXPECT_PREFILL_DIRECT_KV": "0",
                "EXPECT_FAST_PREPARED_FRAME": "0",
                "EXPECT_Q4_0_MMV_VARIANT": "any",
                "EXPECT_SWA_SCAN_CLAMP": "0",
            }
        )
        self.run_harness(out, env)
        summary = json.loads((out / "summary.json").read_text())
        self.assertFalse(summary["canonical_policy"])
        self.assertFalse(summary["promotion_eligible_policy"])
        row = summary["rows"][0]
        self.assertEqual(row["prefill_direct_kv_calls"], 0)
        self.assertEqual(row["prefill_paged_kv_calls"], 42)
        self.assertEqual(row["prepared_frame_fast_path_calls"], 0)
        self.assertEqual(row["prepared_frame_fallback_calls"], 299)
        self.assertFalse(row["swa_scan_clamp_enabled"])
        self.assertFalse(summary["policy_route_expectations"]["prefill_direct_kv"])
        self.assertFalse(summary["policy_route_expectations"]["fast_prepared_frame"])
        self.assertFalse(summary["policy_route_expectations"]["q4_0_mmv_portfolio"])
        self.assertEqual(summary["policy_route_expectations"]["q4_0_mmv_variant"], "any")
        self.assertFalse(summary["policy_route_expectations"]["swa_scan_clamp"])
        policy_env = summary["metadata"]["metal_policy_env"]
        self.assertEqual(policy_env["TERMITE_METAL_DISABLE_SWA_SCAN_CLAMP"], "1")

    def test_direct_kv_route_requires_explicit_enable(self) -> None:
        out = self.tmp / "explicit-direct-route"
        env = self.environment(out)
        env["ALLOW_NONCANONICAL_POLICY"] = "1"
        env["TERMITE_METAL_ENABLE_PREFILL_SG_DIRECT_LOAD"] = "1"
        env["EXPECT_PREFILL_DIRECT_KV"] = "1"
        self.run_harness(out, env)
        row = json.loads((out / "summary.json").read_text())["rows"][0]
        self.assertEqual(row["prefill_direct_kv_calls"], 42)
        self.assertEqual(row["prefill_paged_kv_calls"], 0)

    def test_explicit_direct_kv_expectation_rejects_paged_route(self) -> None:
        out = self.tmp / "wrong-direct-route"
        env = self.environment(out)
        env["ALLOW_NONCANONICAL_POLICY"] = "1"
        env["TERMITE_METAL_DISABLE_PREFILL_SG_DIRECT_LOAD"] = "1"
        env["EXPECT_PREFILL_DIRECT_KV"] = "1"
        completed = subprocess.run(
            ["bash", str(BENCHMARK)],
            env=env,
            check=False,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("prefill direct K/V route", completed.stderr)

    def test_exact_flash_prefill_route_counts_are_required(self) -> None:
        for field, log_key, observed, expected_message in (
            ("generated_flash_prefill", "generated_flash_prefill", 34, "34/7"),
            ("generated_flash_prefill_hd512", "generated_flash_prefill_hd512", 6, "35/6"),
        ):
            with self.subTest(field=field):
                out = self.tmp / f"wrong-{field}"
                self.run_harness(out)
                antfly_log = out / "antfly-1.log"
                original = 35 if field == "generated_flash_prefill" else 7
                antfly_log.write_text(
                    antfly_log.read_text().replace(
                        f"{log_key}={original}",
                        f"{log_key}={observed}",
                    )
                )
                antfly_json = out / "antfly-1.json"
                payload = json.loads(antfly_json.read_text())
                payload["metal"]["attention_dispatch"][field] = observed
                antfly_json.write_text(json.dumps(payload))
                with self.assertRaisesRegex(
                    BenchmarkContractError,
                    f"generated flash prefill routes={expected_message}",
                ):
                    build_result(out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)

    def test_q4_mmv_canonical_variant_must_be_one_hot(self) -> None:
        out = self.tmp / "wrong-q4-variant"
        self.run_harness(out)
        antfly_log = out / "antfly-1.log"
        antfly_log.write_text(
            antfly_log.read_text().replace(
                "mmv_nr4_nsg2=62790 mmv_nr8_nsg2=0",
                "mmv_nr4_nsg2=0 mmv_nr8_nsg2=62790",
            )
        )
        antfly_json = out / "antfly-1.json"
        payload = json.loads(antfly_json.read_text())
        payload["metal"]["q4_0_policy"]["mmv_nr4_nsg2"] = 0
        payload["metal"]["q4_0_policy"]["mmv_nr8_nsg2"] = 62_790
        antfly_json.write_text(json.dumps(payload))
        with self.assertRaisesRegex(BenchmarkContractError, "Q4_0 MMV one-hot route"):
            build_result(out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)

    def test_swa_scan_clamp_requires_explicit_rollback_expectation(self) -> None:
        out = self.tmp / "unexpected-swa-rollback"
        env = self.environment(out)
        env["ALLOW_NONCANONICAL_POLICY"] = "1"
        env["TERMITE_METAL_DISABLE_SWA_SCAN_CLAMP"] = "1"
        completed = subprocess.run(
            ["bash", str(BENCHMARK)],
            env=env,
            check=False,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("SWA scan clamp policy", completed.stderr)

    def test_llama_metal_device_marker_is_required(self) -> None:
        out = self.tmp / "missing-llama-metal-marker"
        self.run_harness(out)
        llama_log = out / "llama-1.log"
        llama_log.write_text(
            llama_log.read_text().replace(
                "ggml_metal_init: found device: Apple M4",
                "ggml_metal_init: skipped device: Apple M4",
            )
        )
        with self.assertRaisesRegex(BenchmarkContractError, "Metal found-device marker"):
            build_result(out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)

    def test_llama_device_identity_and_full_offload_are_required(self) -> None:
        device_out = self.tmp / "wrong-llama-device"
        self.run_harness(device_out)
        device_log = device_out / "llama-1.log"
        device_log.write_text(device_log.read_text().replace("Apple M4", "Apple M3"))
        with self.assertRaisesRegex(BenchmarkContractError, "Metal device='Apple M3'"):
            build_result(device_out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)

        offload_out = self.tmp / "partial-llama-offload"
        self.run_harness(offload_out)
        offload_log = offload_out / "llama-1.log"
        offload_log.write_text(offload_log.read_text().replace("43/43", "42/43"))
        with self.assertRaisesRegex(BenchmarkContractError, "GPU layer offload=42/43"):
            build_result(offload_out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)

    def test_benchmark_source_provenance_is_fail_closed(self) -> None:
        out = self.tmp / "bad-source-provenance"
        self.run_harness(out)
        metadata_path = out / "metadata.json"
        metadata = json.loads(metadata_path.read_text())
        metadata["benchmark_parser_sha256"] = "0" * 64
        metadata_path.write_text(json.dumps(metadata))
        with self.assertRaisesRegex(BenchmarkContractError, "benchmark source hash mismatch"):
            build_result(out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)

    def test_both_engines_are_bound_to_the_hashed_gguf(self) -> None:
        out = self.tmp / "model-artifact-binding"
        self.run_harness(out)
        metadata_path = out / "metadata.json"
        metadata = json.loads(metadata_path.read_text())
        metadata["antfly_model_argument"] = str(self.tmp / "different-model.gguf")
        metadata_path.write_text(json.dumps(metadata))
        with self.assertRaisesRegex(BenchmarkContractError, "does not match the hashed GGUF"):
            build_result(out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)

    def test_execution_order_tampering_is_rejected(self) -> None:
        out = self.tmp / "tampered-execution-order"
        self.run_harness(out)
        order_path = out / "execution-order.jsonl"
        records = order_path.read_text().splitlines()
        record = json.loads(records[1])
        record["implementation"] = "antfly"
        records[1] = json.dumps(record)
        order_path.write_text("\n".join(records) + "\n")
        with self.assertRaisesRegex(BenchmarkContractError, "execution order does not match"):
            build_result(out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)

    def test_odd_run_count_fails_before_benchmark(self) -> None:
        out = self.tmp / "odd-runs"
        env = self.environment(out)
        env["RUNS"] = "3"
        completed = subprocess.run(
            ["bash", str(BENCHMARK)],
            env=env,
            check=False,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("RUNS must be even", completed.stderr)
        self.assertFalse(out.exists())

    def test_canonical_shape_thermal_and_gate_contract_fails_early(self) -> None:
        cases = (
            ("PROMPT", "alternate prompt", "PROMPT must be unset"),
            ("GIT_DIR", "/tmp/alternate-git-dir", "canonical benchmark Git override"),
            ("EXPECTED_LLAMA_CPP_BUILD", "10181", "EXPECTED_LLAMA_CPP_BUILD must be 10182"),
            (
                "EXPECTED_LLAMA_CPP_SHA256",
                "0" * 64,
                "EXPECTED_LLAMA_CPP_SHA256 must match the approved b10182 comparator",
            ),
            (
                "EXPECTED_LLAMA_CPP_BUNDLE_SHA256",
                "0" * 64,
                "EXPECTED_LLAMA_CPP_BUNDLE_SHA256 must match the approved b10182 bundle",
            ),
            ("PROMPT_REPEAT", "37", "PROMPT_REPEAT must be 36"),
            ("OUTPUT_TOKENS", "64", "OUTPUT_TOKENS must be 300"),
            ("RUNS", "4", "RUNS must be an even integer of at least 6"),
            ("WARMUPS", "0", "WARMUPS must be at least 1"),
            ("COOLDOWN_SECONDS", "44", "COOLDOWN_SECONDS must be at least 45"),
            ("MAX_TOTAL_RATIO", "0.981", "MAX_TOTAL_RATIO must be at most 0.98"),
            ("MIN_DECODE_RATIO", "1.019", "MIN_DECODE_RATIO must be at least 1.02"),
            ("MAX_CV", "0.031", "MAX_CV must be at most 0.03"),
            ("REQUIRE_CONFIDENCE", "0", "REQUIRE_CONFIDENCE must be enabled"),
            ("ANTFLY_CACHE_DTYPE", "q8_0", "KV cache types must all be f16"),
            ("LLAMA_CONTEXT_SIZE", "2048", "LLAMA_CONTEXT_SIZE must be 4096"),
            (
                "EXPECT_GENERATED_FLASH_PREFILL_CALLS",
                "34",
                "EXPECT_GENERATED_FLASH_PREFILL_CALLS must be 35",
            ),
            ("EXPECT_FAST_PREPARED_FRAME", "0", "EXPECT_FAST_PREPARED_FRAME must be 1"),
            ("EXPECT_Q4_0_MMV_VARIANT", "any", "EXPECT_Q4_0_MMV_VARIANT must be nr4-nsg2"),
            (
                "EXPECT_LLAMA_OFFLOADED_LAYERS",
                "42",
                "EXPECT_LLAMA_OFFLOADED_LAYERS must be 43",
            ),
        )
        for index, (name, value, message) in enumerate(cases):
            with self.subTest(name=name):
                out = self.tmp / f"noncanonical-shape-{index}"
                env = self.canonical_environment(out)
                env[name] = value
                completed = subprocess.run(
                    ["bash", str(BENCHMARK)],
                    env=env,
                    check=False,
                    text=True,
                    capture_output=True,
                )
                self.assertNotEqual(completed.returncode, 0)
                self.assertIn(message, completed.stderr)
                self.assertFalse((out / "antfly-1.json").exists())

    def test_canonical_comparator_must_be_macho(self) -> None:
        out = self.tmp / "non-macho-comparator"
        completed = subprocess.run(
            ["bash", str(BENCHMARK)],
            env=self.canonical_environment(out),
            check=False,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("must be a Mach-O executable", completed.stderr)
        self.assertFalse((out / "antfly-1.json").exists())

    def test_nonempty_output_directory_is_rejected(self) -> None:
        out = self.tmp / "existing-results"
        out.mkdir()
        marker = out / ".keep"
        marker.write_text("preserve me")
        completed = subprocess.run(
            ["bash", str(BENCHMARK)],
            env=self.environment(out),
            check=False,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("refusing to reuse nonempty directory", completed.stderr)
        self.assertEqual(marker.read_text(), "preserve me")
        self.assertFalse((out / "metadata.json").exists())

    def test_antfly_metal_device_identity_is_required(self) -> None:
        out = self.tmp / "antfly-device-identity"
        self.run_harness(out)
        antfly_json = out / "antfly-1.json"
        payload = json.loads(antfly_json.read_text())
        payload["metal"]["device"] = "Apple M3"
        antfly_json.write_text(json.dumps(payload))
        with self.assertRaisesRegex(BenchmarkContractError, "Antfly Metal device='Apple M3'"):
            build_result(out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)

        payload["metal"]["device"] = "Apple M4"
        payload["metal"]["device_registry_id"] = 0
        antfly_json.write_text(json.dumps(payload))
        with self.assertRaisesRegex(BenchmarkContractError, "device_registry_id"):
            build_result(out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)

    def test_device_registry_identity_must_be_stable_between_samples(self) -> None:
        out = self.tmp / "antfly-device-drift"
        self.run_harness(out)
        antfly_json = out / "antfly-2.json"
        payload = json.loads(antfly_json.read_text())
        payload["metal"]["device_registry_id"] += 1
        antfly_json.write_text(json.dumps(payload))
        with self.assertRaisesRegex(BenchmarkContractError, "changed between samples"):
            build_result(out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)

    def test_gate_thresholds_reject_nonfinite_or_invalid_values(self) -> None:
        out = self.tmp / "invalid-thresholds"
        self.run_harness(out)
        for args, message in (
            ((float("nan"), 0.90, 0.01), "max_total_ratio"),
            ((1.10, float("nan"), 0.01), "min_decode_ratio"),
            ((1.10, 0.90, float("nan")), "max_cv"),
            ((1.10, 0.90, 1.0), "max_cv"),
        ):
            with self.subTest(args=args), self.assertRaisesRegex(
                BenchmarkContractError, message
            ):
                build_result(out, 2, OUTPUT_TOKENS, *args)

    def test_required_digest_pins_and_auto_gqa_policy_fail_before_benchmark(self) -> None:
        cases = (
            ("EXPECTED_LLAMA_CPP_SHA256", "", "64-character SHA-256 pin"),
            ("EXPECTED_LLAMA_CPP_BUNDLE_SHA256", "", "64-character SHA-256 pin"),
            ("EXPECTED_TOKEN_IDS_SHA256", "", "64-character SHA-256 pin"),
            ("EXPECTED_PROMPT_TOKEN_IDS_SHA256", "", "64-character SHA-256 pin"),
            (
                "TERMITE_METAL_DECODE_GQA_SPLIT_SWA_VARIANT",
                "s8",
                "must be auto or unset",
            ),
            (
                "TERMITE_METAL_ENABLE_PIPELINED_DECODE_FRAME",
                "1",
                "noncanonical benchmark policy environment",
            ),
            (
                "TERMITE_METAL_QUANT_MODE",
                "eager",
                "noncanonical benchmark policy environment",
            ),
            (
                "TERMITE_GPU_EAGER_DENSE_MAX_MB",
                "4096",
                "noncanonical benchmark policy environment",
            ),
            (
                "ANTFLY_INFERENCE_METAL_DISABLE_GEMMA_FUSED_QKV",
                "1",
                "noncanonical benchmark policy environment",
            ),
            (
                "LLAMA_ARG_FLASH_ATTN",
                "off",
                "noncanonical benchmark policy environment",
            ),
            (
                "GGML_METAL_NDEBUG",
                "1",
                "noncanonical benchmark policy environment",
            ),
            (
                "DYLD_LIBRARY_PATH",
                "/tmp/injected-llama-libs",
                "canonical benchmark loader override",
            ),
            (
                "DYLD_INSERT_LIBRARIES",
                "/tmp/injected.dylib",
                "canonical benchmark loader override",
            ),
            (
                "LD_PRELOAD",
                "/tmp/injected.dylib",
                "canonical benchmark loader override",
            ),
        )
        for index, (name, value, message) in enumerate(cases):
            with self.subTest(name=name):
                out = self.tmp / f"missing-pin-{index}"
                env = self.environment(out)
                env[name] = value
                if message in (
                    "noncanonical benchmark policy environment",
                    "canonical benchmark loader override",
                ):
                    env.pop("ALLOW_NONCANONICAL_POLICY", None)
                if name.startswith("DYLD_"):
                    # macOS strips DYLD_* while launching a protected /bin/bash.
                    # Set it inside that process, then source the runner so its
                    # fail-closed environment check is actually exercised.
                    completed = subprocess.run(
                        [
                            "bash",
                            "-c",
                            'export "$1=$2"; source "$3"',
                            "loader-policy-test",
                            name,
                            value,
                            str(BENCHMARK),
                        ],
                        env=env,
                        check=False,
                        text=True,
                        capture_output=True,
                    )
                else:
                    completed = subprocess.run(
                        ["bash", str(BENCHMARK)],
                        env=env,
                        check=False,
                        text=True,
                        capture_output=True,
                    )
                self.assertNotEqual(completed.returncode, 0)
                self.assertIn(message, completed.stderr)
                self.assertFalse((out / "antfly-1.json").exists())

    def test_wrong_prompt_digest_and_mutated_binary_are_rejected(self) -> None:
        wrong_prompt_out = self.tmp / "wrong-prompt-pin"
        env = self.environment(wrong_prompt_out)
        env["EXPECTED_PROMPT_TOKEN_IDS_SHA256"] = "0" * 64
        completed = subprocess.run(
            ["bash", str(BENCHMARK)],
            env=env,
            check=False,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("preflight prompt token digest changed", completed.stderr)
        self.assertFalse((wrong_prompt_out / "antfly-1.json").exists())

        artifact_out = self.tmp / "mutated-artifact"
        self.run_harness(artifact_out)
        prompt_path = artifact_out / "prompt.txt"
        original_prompt = prompt_path.read_text()
        prompt_path.write_text(original_prompt + " changed")
        with self.assertRaisesRegex(BenchmarkContractError, "artifact hash mismatch"):
            build_result(artifact_out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)
        prompt_path.write_text(original_prompt)
        self.antfly.write_text(self.antfly.read_text() + "\n# changed after benchmark\n")
        with self.assertRaisesRegex(BenchmarkContractError, "artifact hash mismatch"):
            build_result(artifact_out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)

    def test_cross_implementation_prompt_token_drift_is_rejected(self) -> None:
        out = self.tmp / "llama-prompt-drift"
        env = self.environment(out)
        env["FAKE_LLAMA_PROMPT_DRIFT"] = "1"
        completed = subprocess.run(
            ["bash", str(BENCHMARK)],
            env=env,
            check=False,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("preflight prompt token digest changed", completed.stderr)
        self.assertFalse((out / "antfly-1.json").exists())

    def test_llama_bundle_mutation_is_rejected(self) -> None:
        out = self.tmp / "mutated-llama-bundle"
        self.run_harness(out)
        backend = self.llama_bundle / "libggml-metal.test.dylib"
        backend.write_bytes(backend.read_bytes() + b" changed")
        with self.assertRaisesRegex(BenchmarkContractError, "bundle changed"):
            build_result(out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)

    def test_git_state_end_must_match_start(self) -> None:
        out = self.tmp / "git-state-drift"
        self.run_harness(out)
        metadata_path = out / "metadata.json"
        metadata = json.loads(metadata_path.read_text())
        metadata["git_status_sha256_end"] = "0" * 64
        metadata_path.write_text(json.dumps(metadata))
        with self.assertRaisesRegex(BenchmarkContractError, "Git state changed during benchmark"):
            build_result(out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)

    def test_repository_local_output_directory_is_rejected_before_creation(self) -> None:
        out = SCRIPT_DIR / ".gemma4-benchmark-contract-test-output"
        self.assertFalse(out.exists())
        env = self.environment(out)
        completed = subprocess.run(
            ["bash", str(BENCHMARK)],
            env=env,
            check=False,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("OUT_DIR must be outside the repository", completed.stderr)
        self.assertFalse(out.exists())

    def test_default_six_runs_can_require_confidence(self) -> None:
        out = self.tmp / "confidence-results"
        env = self.environment(out)
        env.pop("RUNS")
        env.pop("REQUIRE_CONFIDENCE")
        env.update(
            {
                "FAKE_ANTFLY_PREFILL_MS": "3500",
                "FAKE_ANTFLY_DECODE_MS": "5000",
            }
        )
        self.run_harness(out, env)
        summary = json.loads((out / "summary.json").read_text())
        self.assertEqual(summary["runs"], 6)
        self.assertTrue(summary["metadata"]["require_confidence"])
        self.assertTrue(summary["confidence_gate"]["required"])
        self.assertTrue(summary["confidence_gate"]["eligible"])
        self.assertTrue(summary["confidence_gate"]["total_latency_upper_below_parity"])
        self.assertTrue(summary["confidence_gate"]["decode_throughput_lower_above_parity"])
        self.assertEqual(summary["confidence_gate"]["total_latency_parity_wins"], 6)
        self.assertEqual(summary["confidence_gate"]["decode_throughput_parity_wins"], 6)
        self.assertTrue(summary["confidence_gate"]["exact_sign_tests_passed"])
        self.assertTrue(summary["confidence_gate"]["passed"])
        self.assertEqual(
            summary["execution_order_gate"]["measured_first_counts"],
            {"antfly": 3, "llama": 3},
        )

    def test_required_confidence_rejects_too_few_runs(self) -> None:
        out = self.tmp / "insufficient-confidence-runs"
        env = self.environment(out)
        env["REQUIRE_CONFIDENCE"] = "1"
        completed = subprocess.run(
            ["bash", str(BENCHMARK)],
            env=env,
            check=False,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("requires at least 6 measured runs", completed.stderr)
        summary = json.loads((out / "summary.json").read_text())
        self.assertTrue(summary["confidence_gate"]["required"])
        self.assertFalse(summary["confidence_gate"]["eligible"])

    def test_q4_mmv_portfolio_fallback_is_rejected(self) -> None:
        out = self.tmp / "q4-mmv-fallback"
        self.run_harness(out)
        antfly_log = out / "antfly-1.log"
        antfly_log.write_text(
            antfly_log.read_text().replace("mmv_variant_fallbacks=0", "mmv_variant_fallbacks=1")
        )
        antfly_json = out / "antfly-1.json"
        payload = json.loads(antfly_json.read_text())
        payload["metal"]["q4_0_policy"]["mmv_variant_fallbacks"] = 1
        antfly_json.write_text(json.dumps(payload))
        with self.assertRaisesRegex(BenchmarkContractError, "MMV portfolio fallbacks=1"):
            build_result(out, 2, OUTPUT_TOKENS, 1.10, 0.90, 0.01)

    def test_comparator_hash_pin_fails_before_benchmark(self) -> None:
        out = self.tmp / "bad-comparator"
        env = self.environment(out)
        env["EXPECTED_LLAMA_CPP_SHA256"] = "0" * 64
        completed = subprocess.run(
            ["bash", str(BENCHMARK)],
            env=env,
            check=False,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("binary SHA-256 mismatch", completed.stderr)
        self.assertFalse((out / "antfly-1.json").exists())


if __name__ == "__main__":
    unittest.main()
