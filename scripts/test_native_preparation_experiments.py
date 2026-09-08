import unittest
from unittest.mock import patch
from types import SimpleNamespace

from native_preparation_experiments import (
    checkpoint_evidence,
    require_disk_headroom,
    delete_preparation_evidence,
)


class NativePreparationTests(unittest.TestCase):
    def test_stable_origin_treatment_requires_preserved_rows(self):
        flags = {"ANTFLY_EXPERIMENT_STABLE_POSTING_ORIGINS": "1"}
        result = delete_preparation_evidence(
            ["dense delete preserved rows index=vec rows=40"], flags
        )
        self.assertEqual(40, result["preserved_vector_rows"]["sum"])
        for value in ("0", "-1", "nan"):
            with self.assertRaises(RuntimeError):
                delete_preparation_evidence(
                    [f"dense delete preserved rows index=vec rows={value}"], flags
                )
        flags["ANTFLY_EXPERIMENT_REUSE_DELETE_VECTORS"] = "1"
        reused = "dense delete apply index=vec reused_rows=0"
        result = delete_preparation_evidence(
            [reused, "dense delete preserved rows index=vec rows=40"], flags
        )
        self.assertEqual(0, result["reused_vector_rows"]["sum"])
        with self.assertRaises(RuntimeError):
            delete_preparation_evidence([reused], flags)

    def test_delete_treatments_require_saved_work(self):
        flags = {
            "ANTFLY_EXPERIMENT_COALESCE_REPLAY_DELETES": "1",
            "ANTFLY_EXPERIMENT_REUSE_DELETE_VECTORS": "1",
        }
        lines = [
            "dense replay delete plan sequence=10 requested=12 unique=7",
            "dense delete apply index=vector keys=7 vectors=7 reused_rows=40",
        ]
        result = delete_preparation_evidence(lines, flags)
        self.assertEqual(5, result["deduplicated_keys"]["sum"])
        self.assertEqual(40, result["reused_vector_rows"]["sum"])
        for bad in (
            [],
            ["dense replay delete plan requested=7 unique=7"],
            ["dense replay delete plan requested=1 unique=2"],
            ["dense replay delete plan requested=x unique=2"],
        ):
            with self.assertRaises(RuntimeError):
                delete_preparation_evidence(bad, flags)
        self.assertEqual({}, delete_preparation_evidence([], {}))

    def test_launch_guard_never_removes_data(self):
        with patch(
            "native_preparation_experiments.shutil.disk_usage",
            return_value=SimpleNamespace(free=4 * 1024**3),
        ):
            require_disk_headroom(".", "Performance1536D50K")
            with self.assertRaisesRegex(RuntimeError, "no test database was created"):
                require_disk_headroom(".", "Performance768D1M")

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
