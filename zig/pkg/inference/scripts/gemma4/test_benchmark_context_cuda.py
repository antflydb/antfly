# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import benchmark_context_cuda as subject


class ContextComparisonTests(unittest.TestCase):
    def test_failed_campaign_replaces_stale_success(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "comparison.json"
            output.write_text('{"evidence_passed":true}')
            with patch.object(sys, "argv", ["benchmark", "--model", "gemma4-e4b", "--output", str(output)]), \
                    patch.object(subject, "run", side_effect=AssertionError("CUDA unavailable")), \
                    self.assertRaisesRegex(AssertionError, "CUDA unavailable"):
                subject.main()
            evidence = json.loads(output.read_text())
            self.assertFalse(evidence["evidence_passed"])
            self.assertFalse(evidence["qualified"])
            self.assertEqual(evidence["error"], "CUDA unavailable")

    def test_comparison_excludes_warmups_and_requires_matching_work(self):
        record = {"warmup": False,
                  "antfly": {"elapsed_ms": 200, "usage": {"prompt_tokens": 8000, "completion_tokens": 64}},
                  "llama_cpp": {"elapsed_ms": 100, "usage": {"prompt_tokens": 8000, "completion_tokens": 64}}}
        warmup = copy.deepcopy(record)
        warmup["warmup"] = True
        warmup["antfly"]["elapsed_ms"] = 100000
        result = subject.summarize([warmup, record], 64)
        self.assertEqual(result["median_antfly_over_llama_latency"], 2)
        self.assertEqual(result["antfly_latency_ms"]["count"], 1)
        for field, value in (("prompt_tokens", 7999), ("completion_tokens", 63)):
            mismatched = copy.deepcopy(record)
            mismatched["llama_cpp"]["usage"][field] = value
            result = subject.summarize([mismatched], 64)
            self.assertFalse(result["comparable_token_counts"])
            self.assertIsNone(result["median_antfly_over_llama_latency"])
        record["antfly"]["usage"]["completion_tokens"] = 1
        record["llama_cpp"]["usage"]["completion_tokens"] = 1
        self.assertFalse(subject.summarize([record], 64)["comparable_token_counts"])

    def test_observation_rejects_missing_or_invalid_token_counts(self):
        response = {"usage": {"prompt_tokens": 8000, "completion_tokens": 64},
                    "choices": [{"message": {"content": "answer"}, "finish_reason": "length"}]}
        self.assertEqual(subject.observation(response, {"elapsed_ms": 20})["finish_reason"], "length")
        for value in (None, False, 0, -1, "64"):
            response["usage"]["completion_tokens"] = value
            with self.subTest(value=value), self.assertRaises(AssertionError):
                subject.observation(response, {"elapsed_ms": 20})


if __name__ == "__main__":
    unittest.main()
