#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Exercise benchmark provenance across real Git staging transitions."""
import pathlib
import importlib.util
import struct
import subprocess
import tempfile
import unittest

from benchmark_decide_metal_matrix import compare, source_diff


class SourceIdentityTests(unittest.TestCase):
    def test_staging_preserves_identity_and_edits_change_it(self):
        scratch = pathlib.Path(".benchmark-results")
        scratch.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            root = pathlib.Path(directory)

            def git(*args):
                subprocess.run(["git", *args], cwd=root, check=True,
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

            git("init", "--initial-branch=fixture")
            path = root / "source.zig"
            path.write_text("baseline\n")
            git("add", "source.zig")
            git("-c", "user.name=Benchmark Fixture", "-c", "user.email=fixture@invalid",
                "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null",
                "commit", "-m", "fixture")
            path.write_text("measured source\n")
            measured = source_diff(root)
            self.assertTrue(measured)
            git("add", "source.zig")
            self.assertEqual(measured, source_diff(root))
            git("restore", "--staged", "source.zig")
            self.assertEqual(measured, source_diff(root))
            path.write_text("changed source\n")
            self.assertNotEqual(measured, source_diff(root))


class NumericReferenceTests(unittest.TestCase):
    def test_nonfinite_logit_after_finite_value_is_rejected(self):
        # A trailing NaN can disappear inside Python's max(). Reject the
        # complete numeric inventory before applying error tolerances.
        antfly = dict(prepared_input_ids=[[1]], decisions=[dict(raw_logits=[0.0, float("nan")])])
        python = dict(prepared_input_ids=[[1]], raw_logits=[[0.0, 1.0]], probabilities=[[.25, .75]])
        with self.assertRaisesRegex(ValueError, "finite"):
            compare(antfly, python)

    @unittest.skipUnless(importlib.util.find_spec("torch") and importlib.util.find_spec("numpy"),
                         "Decoded GGUF reference requires the benchmark Python environment")
    def test_q8_reference_decodes_signed_blocks_and_f32_on_cpu(self):
        from benchmark_decide_quant import decoded_weights

        def string(value):
            data = value.encode()
            return struct.pack("<Q", len(data)) + data

        header = struct.pack("<4sIQQ", b"GGUF", 3, 2, 0)
        header += string("matrix") + struct.pack("<IQQIQ", 2, 32, 1, 8, 0)
        header += string("bias") + struct.pack("<IQIQ", 1, 2, 0, 64)
        header += b"\0" * (-len(header) % 32)
        values = list(range(-16, 16))
        payload = struct.pack("<e32b", .5, *values)
        payload += b"\0" * (64 - len(payload)) + struct.pack("<2f", 1.25, -2.5)
        scratch = pathlib.Path(".benchmark-results")
        scratch.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            path = pathlib.Path(directory) / "tiny.gguf"
            path.write_bytes(header + payload)
            tensors = dict(decoded_weights(path, device="cpu"))
        self.assertEqual(tensors["matrix"].device.type, "cpu")
        self.assertEqual(tensors["matrix"].tolist(), [[.5 * x for x in values]])
        self.assertEqual(tensors["bias"].tolist(), [1.25, -2.5])


if __name__ == "__main__":
    unittest.main()
