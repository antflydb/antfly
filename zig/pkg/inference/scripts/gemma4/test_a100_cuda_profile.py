#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""CPU-only checks for launching an A100 baseline from a tuned shell."""
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest


WRAPPER = Path(__file__).with_name("with_gemma4_a100_cuda.sh")


class A100CudaProfileTest(unittest.TestCase):
    def test_inherited_architecture_tuning_is_removed_without_widening_limits(self):
        inherited = {
            "ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE": "required-flash-f16-sm89",
            "ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY": "required",
            "ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY": "2048",
            "ANTFLY_INFERENCE_CUDA_E4B_HEAD_PREFETCH_L2": "1",
            "ANTFLY_INFERENCE_CUDA_RMS_Q8_1_MIRROR": "1",
            "ANTFLY_INFERENCE_CUDA_CUBLASLT_BF16_TUNING_PROFILE": "sm121-ocr",
            "ANTFLY_INFERENCE_CUDA_ALLOW_UNPROMOTED_GENERATED_KERNELS": "1",
            "ANTFLY_INFERENCE_CUDA_Q4_0_QKV_Q8_1_TILE8": "1",
            "ANTFLY_CUDA_QMATMUL_VARIANT": "experimental",
            "TERMITE_CUDA_DISABLE_TILED_TRANSPOSE_2D": "1",
            "TERMITE_DISABLE_PAGED_KV": "1",
            "ANTFLY_INFERENCE_CUDA_TEMP_CACHE_MB": "17",
            "ANTFLY_INFERENCE_CUDA_TEMP_ARENA_MAX_MB": "33",
            "ANTFLY_INFERENCE_CUDA_CUBLASLT_WORKSPACE_MB": "16",
            "ANTFLY_INFERENCE_PROCESS_MEMORY_BUDGET_MB": "32768",
            "CUDA_VISIBLE_DEVICES": "2",
            "CUDA_CACHE_PATH": "/caller/cache",
            "LD_LIBRARY_PATH": "/caller/libraries",
        }
        command = [
            "bash", str(WRAPPER), sys.executable, "-c",
            "import json,os,sys; print(json.dumps({key:os.environ.get(key) for key in json.loads(sys.argv[1])}))",
            json.dumps(list(inherited)),
        ]
        result = subprocess.run(command, env={**os.environ, **inherited}, capture_output=True, text=True, check=True)
        environment = json.loads(result.stdout)
        for key in (
            "ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE", "ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY",
            "ANTFLY_INFERENCE_CUDA_CUBLASLT_BF16_TUNING_PROFILE",
        ):
            self.assertEqual(environment[key], "off")
        for key in (
            "ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY", "ANTFLY_INFERENCE_CUDA_E4B_HEAD_PREFETCH_L2",
            "ANTFLY_INFERENCE_CUDA_RMS_Q8_1_MIRROR", "ANTFLY_INFERENCE_CUDA_ALLOW_UNPROMOTED_GENERATED_KERNELS",
        ):
            self.assertEqual(environment[key], "0")
        for key in (
            "ANTFLY_INFERENCE_CUDA_Q4_0_QKV_Q8_1_TILE8", "ANTFLY_CUDA_QMATMUL_VARIANT",
            "TERMITE_CUDA_DISABLE_TILED_TRANSPOSE_2D", "TERMITE_DISABLE_PAGED_KV",
        ):
            self.assertIsNone(environment[key])
        for key in (
            "ANTFLY_INFERENCE_CUDA_TEMP_CACHE_MB", "ANTFLY_INFERENCE_CUDA_TEMP_ARENA_MAX_MB",
            "ANTFLY_INFERENCE_CUDA_CUBLASLT_WORKSPACE_MB", "ANTFLY_INFERENCE_PROCESS_MEMORY_BUDGET_MB",
            "CUDA_VISIBLE_DEVICES", "CUDA_CACHE_PATH", "LD_LIBRARY_PATH",
        ):
            self.assertEqual(environment[key], inherited[key])

    def test_command_arguments_and_exit_status_are_preserved(self):
        argument = "path with spaces;literal-$value"
        result = subprocess.run([
            "bash", str(WRAPPER), sys.executable, "-c",
            "import json,sys; print(json.dumps(sys.argv[1:])); sys.exit(7)", argument,
        ], capture_output=True, text=True)
        self.assertEqual(result.returncode, 7)
        self.assertEqual(json.loads(result.stdout), [argument])

    def test_missing_command_is_rejected(self):
        result = subprocess.run(["bash", str(WRAPPER)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("usage:", result.stdout)


if __name__ == "__main__":
    unittest.main()
