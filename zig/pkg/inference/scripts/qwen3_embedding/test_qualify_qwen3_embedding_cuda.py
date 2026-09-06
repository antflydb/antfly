#!/usr/bin/env python3

from __future__ import annotations

import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))

import qualify_qwen3_embedding_cuda as qualify
import qualify_qwen3_embedding_metal as metal
from test_qualify_qwen3_embedding_metal import make_case, make_oracle_payload


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
            qualify.model_tier("hf:Qwen/Qwen3-Embedding-0.6B-GGUF:q8-0-bundle-v1"),
        )
        self.assertEqual("bf16", qualify.model_tier("qwen3-embedding-0.6b-safetensors"))
        self.assertEqual(
            "bf16",
            qualify.model_tier("Qwen/Qwen3-Embedding-0.6B:bf16-safetensors-bundle-v1"),
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
        self.assertEqual("antfly.qwen3_embedding.cuda_qualification.v1", qualify.SCHEMA)
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

    def test_entry_points_preserve_backend_replay_policy_and_report_contract(
        self,
    ) -> None:
        payload = make_oracle_payload()
        payload["cases"] = [
            case for case in payload["cases"] if case["id"] != "query_shared_text"
        ]
        payload["cases"].append(make_case("query_shared_text", "query", None, 4))
        long_case = make_case("doc_long", "document", None, 5)
        long_case.update(truncated=True, served_text="bounded long document")
        payload["cases"].append(long_case)
        by_input = {qualify.common.case_input(case): case for case in payload["cases"]}

        def embed(_url: str, body: dict, _timeout: float) -> list[list[float]]:
            inputs = body["input"]
            if isinstance(inputs, str):
                inputs = [inputs]
            dimensions = str(body.get("dimensions", 1024))
            return [by_input[text]["embeddings"][dimensions] for text in inputs]

        for backend, extra_args in ((qualify, []), (metal, ["--tier", "bf16"])):
            with (
                self.subTest(backend=backend.SCHEMA),
                tempfile.TemporaryDirectory() as raw,
            ):
                oracle_path = Path(raw) / "oracle.json"
                report_path = Path(raw) / "report.json"
                oracle_path.write_text(json.dumps(payload), encoding="utf-8")
                with (
                    mock.patch.object(
                        qualify.common, "post_embeddings", side_effect=embed
                    ) as post,
                    mock.patch("sys.stdout", new_callable=io.StringIO),
                ):
                    result = backend.main(
                        [
                            "--oracle",
                            str(oracle_path),
                            "--base-url",
                            "http://localhost:8080",
                            "--model",
                            "qwen3-embedding-0.6b-safetensors",
                            "--report",
                            str(report_path),
                            *extra_args,
                        ]
                    )
                self.assertEqual(0, result)
                report = json.loads(report_path.read_text(encoding="utf-8"))
                self.assertTrue(report["pass"])
                self.assertEqual(backend.SCHEMA, report["schema"])
                self.assertEqual("bf16", report["tier"])
                self.assertEqual(0.999, report["min_cosine"])
                bodies = [call.args[1] for call in post.call_args_list]
                long_requests = [
                    body for body in bodies if body["input"] == long_case["served_text"]
                ]
                self.assertEqual(1 if backend is qualify else 2, len(long_requests))
                batch = [body for body in bodies if isinstance(body["input"], list)]
                self.assertEqual(1, len(batch))
                self.assertEqual(
                    backend is metal, long_case["served_text"] in batch[0]["input"]
                )


if __name__ == "__main__":
    unittest.main()
