#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
import sys
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parent))

import qualify_qwen3_embedding_cuda as qualify


class CudaQualificationPolicyTests(unittest.TestCase):
    def test_only_runtime_promoted_tiers_are_exposed(self) -> None:
        self.assertEqual(
            {"bf16": 0.999, "q8_0": 0.995},
            qualify.TIER_MIN_COSINE,
        )

    def test_model_reference_derives_cuda_tier(self) -> None:
        self.assertEqual("q8_0", qualify.model_tier("qwen3-embedding"))
        self.assertEqual("q8_0", qualify.model_tier("qwen3-embedding-0.6b"))
        self.assertEqual(
            "q8_0",
            qualify.model_tier(
                "hf:Qwen/Qwen3-Embedding-0.6B-GGUF:q8-0-bundle-v1"
            ),
        )
        self.assertEqual(
            "bf16", qualify.model_tier("qwen3-embedding-0.6b-safetensors")
        )
        self.assertEqual(
            "bf16",
            qualify.model_tier(
                "Qwen/Qwen3-Embedding-0.6B:bf16-safetensors-bundle-v1"
            ),
        )

    def test_unpromoted_model_references_fail_closed(self) -> None:
        for model in (
            "qwen3-embedding-0.6b-f16",
            "Qwen/Qwen3-Embedding-0.6B-GGUF:f16-bundle-v1",
            "Qwen/Qwen3-Embedding-0.6B-GGUF",
        ):
            with self.subTest(model=model):
                with self.assertRaisesRegex(qualify.QualificationError, "promoted"):
                    qualify.model_tier(model)

    def test_parser_requires_base_url_and_derives_tier(self) -> None:
        parsed = qualify.parse_args(
            [
                "--oracle",
                "/tmp/oracle.json",
                "--base-url",
                "http://127.0.0.1:8080",
                "--model",
                "qwen3-embedding-0.6b-safetensors",
            ]
        )
        self.assertEqual("bf16", parsed.tier)
        self.assertEqual("http://127.0.0.1:8080", parsed.base_url)

    def test_cuda_schema_is_distinct_from_shared_metal_lane(self) -> None:
        self.assertEqual(
            "antfly.qwen3_embedding.cuda_qualification.v1", qualify.SCHEMA
        )
        self.assertNotEqual(qualify.SCHEMA, qualify.common.SCHEMA)

    def test_cuda_batch_policy_excludes_truncated_documents(self) -> None:
        cases = {
            "query": {"role": "query", "truncated": False},
            "short_b": {"role": "document", "truncated": False},
            "long": {"role": "document", "truncated": True},
            "short_a": {"role": "document"},
        }
        self.assertEqual(
            ["short_a", "short_b"],
            qualify.batch_document_case_ids(cases),
        )

    def test_cuda_reduced_dimension_policy_excludes_truncated_case(self) -> None:
        self.assertTrue(qualify.needs_reduced_dimension_replay({"truncated": False}))
        self.assertTrue(qualify.needs_reduced_dimension_replay({}))
        self.assertFalse(qualify.needs_reduced_dimension_replay({"truncated": True}))


if __name__ == "__main__":
    unittest.main()
