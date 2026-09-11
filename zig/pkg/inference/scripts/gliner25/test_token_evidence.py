from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import benchmark_cpu as benchmark
import extract_token_evidence as evidence
import oracle


class TokenEvidenceTest(unittest.TestCase):
    def test_all_thirty_sequences_remain_bound_to_source_and_ordered_requests(self):
        value = oracle.read_json(oracle.FIXTURES / "token_evidence.json")
        manifest = oracle.load_manifest()
        self.assertEqual("completed_cpu_benchmark_encoder_token_evidence", value["scope"])
        self.assertIs(value["fresh_model_execution"], False)
        self.assertIs(value["native_runtime_qualified"], False)
        self.assertEqual(evidence.REPORT_SHA256, value["report_sha256"])
        self.assertEqual(evidence.DRIVER_SHA256, value["benchmark_driver_sha256"])
        self.assertEqual(evidence.NATIVE_SHA256, value["native_binary_sha256"])
        self.assertEqual(oracle.sha256_file(Path(evidence.__file__)), value["generator_sha256"])
        self.assertEqual(manifest["upstream"]["commit"], value["source_commit"])
        self.assertEqual(manifest["runtime"], value["runtime"])
        self.assertEqual({"small", "base", "multi"}, {row["model"] for row in value["models"]})
        self.assertEqual(3, len(value["models"]))
        count = 0
        for row in value["models"]:
            model = manifest["models"][row["model"]]
            self.assertEqual(model["model_id"], row["model_id"])
            self.assertEqual(model["revision"], row["revision"])
            self.assertEqual({name: {key: item[key] for key in ("sha256", "size_bytes")} for name, item in model["files"].items()}, row["model_files"])
            fixture_path = benchmark.case_fixture(row["model"])
            self.assertEqual(oracle.sha256_file(fixture_path), row["cases_sha256"])
            fixture = oracle.read_json(fixture_path)
            self.assertEqual(fixture["reference_sha256"], row["reference_sha256"])
            self.assertEqual(fixture["requests_sha256"], row["requests_sha256"])
            self.assertEqual({case["id"] for case in fixture["cases"]}, set(row["validation"]))
            for case in fixture["cases"]:
                count += 1
                packet = row["validation"][case["id"]]
                request_bytes = json.dumps({"text": case["text"], "schema": case["schema"]}, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
                self.assertEqual(hashlib.sha256(request_bytes).hexdigest(), packet["canonical_request_sha256"])
                tokens = packet["input_ids"]
                self.assertTrue(0 < len(tokens) <= oracle.MAX_ENCODED_TOKENS)
                self.assertTrue(all(type(token) is int and 0 <= token < 2**32 for token in tokens))
                self.assertEqual(hashlib.sha256(b"".join(token.to_bytes(4, "little") for token in tokens)).hexdigest(), packet["input_ids_u32_le_sha256"])
        self.assertEqual(30, count)

    def test_report_substitution_fails_before_reading_profile_or_tokens(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            path.write_text('{"status":"complete","parity_validated":true}')
            with self.assertRaisesRegex(oracle.ContractError, "exact completed benchmark report"):
                evidence.extract(path)


if __name__ == "__main__":
    unittest.main()
