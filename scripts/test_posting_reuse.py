import unittest
from pathlib import Path

from summarize_posting_reuse import FLAG, suffix_enabled_for_arm, summarize_lines


class PostingReuseTests(unittest.TestCase):
    def test_limited_harness_environment_does_not_certify_absent_flag(self):
        enabled = suffix_enabled_for_arm(
            Path("arm"), {"experiment_environment": {}}, None
        )
        self.assertIsNone(enabled)
        self.assertFalse(summarize_lines([], enabled)["evidence_consistent"])

    def test_full_runner_receipt_overrides_the_limited_harness_allowlist(self):
        config = {"experiment_environment": {}, "antfly_binary_sha256": "binary-hash"}
        receipt = {
            "command": ["harness", str(Path("arm").resolve())],
            "exit_code": 0,
            "environment": {"ANTFLY_BIN": "binary", FLAG: "1"},
            "inputs_sha256": {"binary": "binary-hash"},
        }
        self.assertTrue(suffix_enabled_for_arm(Path("arm"), config, [receipt]))
        receipt["inputs_sha256"]["binary"] = "different-binary"
        with self.assertRaises(ValueError):
            suffix_enabled_for_arm(Path("arm"), config, [receipt])

    def test_flag_and_worker_alone_do_not_qualify(self):
        result = summarize_lines(
            [
                "info: dense checkpoint worker generation=2 sequence=4 kind=compact_deltas success=true"
            ],
            True,
        )
        self.assertFalse(result["treatment_exercised"])
        self.assertFalse(result["evidence_consistent"])

    def test_only_durable_suffix_publication_qualifies(self):
        result = summarize_lines(
            [
                "info: dense posting checkpoint published generation=3 sequence=7 kind=compact_deltas"
            ],
            True,
        )
        self.assertTrue(result["treatment_exercised"])

    def test_disabled_flag_rejects_suffix_publication(self):
        result = summarize_lines(
            [
                "info: dense posting checkpoint published generation=3 sequence=7 kind=compact_deltas"
            ],
            False,
        )
        self.assertFalse(result["evidence_consistent"])

    def test_retained_bytes_are_samples_not_cumulative_savings(self):
        result = summarize_lines(
            [
                "info: dense checkpoint handoff generation=2 sequence=7 kind=delta written_bytes=10 retained_bytes=100",
                "info: dense checkpoint handoff generation=3 sequence=8 kind=delta written_bytes=20 retained_bytes=110",
            ],
            False,
        )
        self.assertEqual(result["written_bytes_by_kind"], {"delta": 30})
        self.assertEqual(
            result["retained_bytes_samples_by_kind"], {"delta": [100, 110]}
        )

    def test_malformed_evidence_does_not_qualify(self):
        result = summarize_lines(
            [
                "info: dense posting checkpoint published generation=3 sequence=7 kind=compact_deltas",
                "info: dense checkpoint handoff generation=4 sequence=8 kind=compact_deltas written_bytes=-1 retained_bytes=10",
            ],
            True,
        )
        self.assertFalse(result["treatment_exercised"])
        self.assertEqual(result["invalid_lines"], [2])

    def test_empty_authority_is_not_a_suffix_publication(self):
        result = summarize_lines(
            [
                "info: dense posting checkpoint published generation=1 sequence=0 bytes=383 source=authoritative_lsm"
            ],
            False,
        )
        self.assertTrue(result["evidence_consistent"])
        self.assertFalse(result["treatment_exercised"])
        self.assertEqual(result["published_by_kind"], {})


if __name__ == "__main__":
    unittest.main()
