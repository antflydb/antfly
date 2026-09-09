from pathlib import Path
import hashlib
import json
import tempfile
import unittest
from unittest.mock import patch

import benchmark_qwen3vl_ocr_endpoint as bench


def response(text="invoice", tokens=3):
    return {
        "object": "list",
        "model": "qwen",
        "data": [{"object": "read", "index": 0, "text": text}],
        "usage": {
            "prompt_tokens": 32,
            "completion_tokens": tokens,
            "total_tokens": 32 + tokens,
        },
    }


class OcrBenchmarkTests(unittest.TestCase):
    def test_model_identity_includes_sidecars_and_excludes_download_receipts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "model.gguf").write_bytes(b"model")
            (root / "config.json").write_text("{}")
            before = bench.model_artifacts(root)
            (root / ".antfly-download-complete.json").write_text('{"time": 1}')
            self.assertEqual(before, bench.model_artifacts(root))
            (root / "config.json").write_text('{"hidden_size": 128}')
            self.assertNotEqual(before, bench.model_artifacts(root))

    def test_every_measured_response_must_match_the_golden(self):
        golden = bench.canonical_response(response(), "qwen", 1)
        changed = bench.canonical_response(response("invented"), "qwen", 1)
        replies = [(10.0, golden), (10.0, changed)]
        with patch.object(bench, "request_read", side_effect=replies):
            with self.assertRaisesRegex(ValueError, "output or token-count mismatch"):
                bench.measure_case(
                    {"id": "invoice", "request": {}, "fixture": {}},
                    {"candidate": ("unused", "qwen")},
                    golden,
                    warmup=1,
                    iters=1,
                    timeout=1,
                )

    def test_timing_excludes_warmup_and_alternates(self):
        golden = bench.canonical_response(response(), "qwen", 1)
        with patch.object(
            bench,
            "request_read",
            side_effect=[(v, golden) for v in (90, 80, 20, 10, 11, 21)],
        ) as request:
            result = bench.measure_case(
                {"id": "invoice", "request": {}, "fixture": {}},
                {"candidate": ("a", "qwen"), "reference": ("b", "qwen")},
                golden,
                warmup=1,
                iters=2,
                timeout=1,
            )
            self.assertEqual(
                result["samples_ms"], {"candidate": [10, 11], "reference": [20, 21]}
            )
            self.assertEqual(
                [call.args[0] for call in request.call_args_list],
                ["a", "b", "b", "a", "a", "b"],
            )

    def test_changed_image_fails_before_request(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            content = b"\x89PNG\r\n\x1a\nfixture"
            (root / "image.png").write_bytes(content)
            fixture = {
                "schema": bench.FIXTURE_SCHEMA,
                "cases": [
                    {
                        "id": "image",
                        "images": [
                            {
                                "path": "image.png",
                                "sha256": hashlib.sha256(content).hexdigest(),
                            }
                        ],
                        "max_tokens": 64,
                    }
                ],
            }
            path = root / "fixture.json"
            path.write_text(json.dumps(fixture))
            self.assertEqual(len(bench.load_cases(path)), 1)
            (root / "image.png").write_bytes(content + b"changed")
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                bench.load_cases(path)

    def test_response_rejects_incomplete_or_wrong_model_output(self):
        for payload in (
            response(""),
            {**response(), "model": "wrong"},
            {**response(), "data": []},
            {**response(), "usage": {}},
        ):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                bench.canonical_response(payload, "qwen", 1)


if __name__ == "__main__":
    unittest.main()
