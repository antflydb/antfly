import unittest

from native_preparation_experiments import checkpoint_evidence


class NativePreparationTests(unittest.TestCase):
    def test_flag_or_failed_worker_is_not_publication(self):
        flag = {"ANTFLY_EXPERIMENT_STAGE_POSTING_READERS": "1"}
        for lines in (
            [],
            ["dense checkpoint worker generation=2 readers_stage_ns=20 success=true"],
            [
                "dense checkpoint worker generation=2 readers_stage_ns=20 success=false",
                "dense posting checkpoint published generation=2",
            ],
        ):
            with self.assertRaises(RuntimeError):
                checkpoint_evidence(lines, flag)

    def test_rebase_must_publish_its_own_generation(self):
        flag = {"ANTFLY_EXPERIMENT_STAGE_POSTING_REBASE": "1"}
        lines = [
            "dense checkpoint rebase worker generation=2 rebase_stage_ns=100 success=true",
            "dense posting checkpoint published generation=3",
        ]
        with self.assertRaises(RuntimeError):
            checkpoint_evidence(lines, flag)
        lines.append("dense posting checkpoint published generation=2")
        self.assertEqual(
            checkpoint_evidence(lines, flag)["rebase_stage_ns"]["sum"], 100
        )

    def test_row_reuse_is_encoding_bytes_not_disk_savings(self):
        lines = [
            "dense checkpoint encoded row reuse generation=4 reused_bytes=4096",
            "dense posting checkpoint published generation=4",
        ]
        result = checkpoint_evidence(
            lines, {"ANTFLY_EXPERIMENT_REUSE_POSTING_ROWS": "1"}
        )
        self.assertEqual(result["reused_bytes"]["sum"], 4096)
        self.assertNotIn("saved_disk_bytes", result)

    def test_disabled_treatment_needs_no_observations(self):
        self.assertEqual({}, checkpoint_evidence([], {}))

    def test_sequence_only_reuse_requires_unique_full_publication(self):
        flag = {"ANTFLY_EXPERIMENT_REUSE_POSTING_ROWS": "1"}
        lines = [
            "dense checkpoint encoded row reuse sequence=9 reused_bytes=4096",
            "dense posting checkpoint published generation=4 sequence=9 kind=full",
        ]
        self.assertEqual(checkpoint_evidence(lines, flag)["reused_bytes"]["sum"], 4096)
        lines.append(
            "dense posting checkpoint published generation=5 sequence=9 kind=full"
        )
        with self.assertRaises(RuntimeError):
            checkpoint_evidence(lines, flag)


if __name__ == "__main__":
    unittest.main()
