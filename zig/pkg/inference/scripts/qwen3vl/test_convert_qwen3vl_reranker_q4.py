#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
import sys
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parent))

from convert_qwen3vl_reranker_q4 import has_option


class Q4WrapperTests(unittest.TestCase):
    def test_decoder_quantization_accepts_split_and_equals_forms(self) -> None:
        self.assertTrue(
            has_option(
                ["--decoder-quantization", "Q8_0"], "--decoder-quantization"
            )
        )
        self.assertTrue(
            has_option(["--decoder-quantization=Q8_0"], "--decoder-quantization")
        )
        self.assertFalse(has_option(["--output", "bundle"], "--decoder-quantization"))


if __name__ == "__main__":
    unittest.main()
