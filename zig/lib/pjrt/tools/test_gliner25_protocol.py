# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Regression checks for short native-worker I/O and BF16 parameter transport."""

import struct
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import benchmark_gliner25 as bench
import numpy as np


class ShortWriter:
    def __init__(self):
        self.data = bytearray()

    def write(self, data):
        count = min(7, len(data))
        self.data.extend(data[:count])
        return count

    def flush(self):
        pass


class ProtocolTests(unittest.TestCase):
    def test_sparse_candidate_order_is_not_a_numerical_difference(self):
        fixture = {
            "kind": "entities",
            "inputs": [{"shape": [1, 1]}, {}, {}, {}, {"shape": [1, 1]}],
            "candidate_indices_offset": 9,
        }
        first = [0] * 7 + [0.3, 0.7, 0, 1, 1, 2, 1, 1]
        reordered = [0] * 7 + [0.7, 0.3, 1, 2, 0, 1, 1, 1]
        np.testing.assert_array_equal(
            bench.canonical_output(first, fixture),
            bench.canonical_output(reordered, fixture),
        )
        changed = reordered.copy()
        changed[7] += 0.1
        self.assertFalse(
            np.array_equal(
                bench.canonical_output(first, fixture),
                bench.canonical_output(changed, fixture),
            )
        )

    def test_small_raw_error_does_not_hide_a_multilabel_threshold_flip(self):
        candidate = np.zeros(14)
        candidate[6] = np.log(0.4 / 0.6) + 1e-5
        reference = candidate.copy()
        reference[6] -= 2e-5
        self.assertLess(np.max(np.abs(candidate - reference)), 0.002)
        self.assertNotEqual(bench.decisions(candidate), bench.decisions(reference))

    def test_small_raw_error_does_not_hide_an_argmax_flip(self):
        candidate = np.zeros(14)
        reference = candidate.copy()
        reference[1] = 1e-5
        self.assertLess(np.max(np.abs(candidate - reference)), 0.002)
        self.assertNotEqual(bench.decisions(candidate), bench.decisions(reference))

    def test_qualification_uses_the_cpu_oracle_and_keeps_raw_limit(self):
        fixture = {"outputs": [{"shape": [14]}], "cpu_logits": [-1.0] * 14}
        within = np.full(14, -0.99)
        outside = np.full(14, -0.97)
        self.assertTrue(bench.compare_to_cpu(within, fixture, 0.02)["passed"])
        self.assertFalse(bench.compare_to_cpu(outside, fixture, 0.02)["passed"])
        # A different peer's error cannot become this backend's reference.
        self.assertTrue(bench.compare_to_cpu(fixture["cpu_logits"], fixture, 0.02)["passed"])

    def test_qualification_rejects_a_small_error_argmax_flip(self):
        reference = np.zeros(14)
        reference[0] = 1e-5
        candidate = reference.copy()
        candidate[1] = 2e-5
        fixture = {"outputs": [{"shape": [14]}], "cpu_logits": reference.tolist()}
        result = bench.compare_to_cpu(candidate, fixture, 0.02)
        self.assertLess(result["max_abs_output_error"], 0.02)
        self.assertFalse(result["passed"])

    def test_qualification_requires_finite_complete_cpu_output(self):
        with self.assertRaisesRegex(ValueError, "Missing CPU"):
            bench.compare_to_cpu([0.0] * 14, {"outputs": [{"shape": [14]}]}, 0.02)
        fixture = {"outputs": [{"shape": [14]}], "cpu_logits": [0.0] * 14}
        for invalid in ([0.0] * 13, [float("nan")] * 14, [float("inf")] * 14):
            with self.assertRaisesRegex(ValueError, "Invalid qualification"):
                bench.compare_to_cpu(invalid, fixture, 0.02)

    def test_partial_reads_preserve_all_response_bytes(self):
        stream = MagicMock()
        with (
            patch.object(bench.select, "select", return_value=([stream], [], [])),
            patch.object(bench.os, "read", side_effect=[b"ab", b"c", b"def"]),
        ):
            self.assertEqual(bench.read_exact(stream, 6), b"abcdef")

    def test_closed_worker_cannot_be_mistaken_for_complete_output(self):
        stream = MagicMock()
        with (
            patch.object(bench.select, "select", return_value=([stream], [], [])),
            patch.object(bench.os, "read", side_effect=[b"ab", b""]),
            self.assertRaisesRegex(RuntimeError, "closed"),
        ):
            bench.read_exact(stream, 6)

    def test_unresponsive_worker_is_bounded(self):
        with (
            patch.object(bench.select, "select", return_value=([], [], [])),
            self.assertRaises(TimeoutError),
        ):
            bench.read_exact(MagicMock(), 16)

    def send(self, values, dtype):
        context = MagicMock()
        context.__enter__.return_value.get_tensor.return_value = values
        manifest = {
            "parameters": [
                {
                    "source": "encoder.weight",
                    "shape": list(values.shape),
                    "dtype": dtype,
                }
            ]
        }
        writer = ShortWriter()
        with patch.object(bench, "safe_open", return_value=context):
            bench.stream_parameters(writer, manifest, Path("unused"))
        return bytes(writer.data)

    def test_partial_parameter_writes_are_retried(self):
        values = np.arange(96, dtype=np.float32).reshape(8, 12)
        self.assertEqual(self.send(values, "float32"), values.tobytes())

    def test_bf16_transfer_preserves_round_to_nearest_even(self):
        values = np.array([1 + 1 / 256, 1 + 3 / 256], dtype=np.float32)
        self.assertEqual(
            self.send(values, "bfloat16"), struct.pack("<HH", 0x3F80, 0x3F82)
        )


if __name__ == "__main__":
    unittest.main()
