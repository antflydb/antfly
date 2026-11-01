# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Qualification failures must survive optimized Python without loading CUDA."""
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path


class OptimizedQualificationTests(unittest.TestCase):
    def run_optimized(self, body):
        scripts = Path(__file__).resolve().parent.parent
        program = "import sys\nsys.path[:0] = " + repr([str(scripts / "gemma4"), str(scripts)]) + "\n"
        result = subprocess.run(
            [sys.executable, "-B", "-O", "-c", program + textwrap.dedent(body)],
            capture_output=True, text=True, timeout=15,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_reference_payload_checks_survive_optimized_python(self):
        self.run_optimized("""
            import verify_vllm_gguf_loader_cpu as verify
            verify.require(True, "valid payload")
            try:
                verify.require(False, "corrupt packed Q payload")
            except AssertionError as error:
                if str(error) != "corrupt packed Q payload":
                    raise
            else:
                raise RuntimeError("optimized Python skipped payload verification")
        """)

    def test_prototype_output_checks_survive_optimized_python(self):
        self.run_optimized("""
            import benchmark_cuda_rms_q8_1_prototype as probe
            try:
                probe.require(b"actual" == b"expected", "Q8 mismatch")
            except AssertionError:
                pass
            else:
                raise RuntimeError("optimized Python skipped output verification")
        """)

    def test_corrupt_device_guards_fail_without_cuda_initialization(self):
        self.run_optimized("""
            import benchmark_cuda_rms_q8_1_prototype as probe
            for corrupted in (1024, 1024 + probe.GUARD + 16):
                class FakeCuda:
                    def download(self, pointer, count):
                        return bytes([0 if pointer == corrupted else 0xA5]) * count
                buffer = object.__new__(probe.Buffer)
                buffer.cuda = FakeCuda()
                buffer.raw, buffer.ptr, buffer.size = 1024, 1024 + probe.GUARD, 16
                try:
                    buffer.check()
                except AssertionError:
                    pass
                else:
                    raise RuntimeError("optimized Python skipped guard verification")
        """)


if __name__ == "__main__":
    unittest.main()
