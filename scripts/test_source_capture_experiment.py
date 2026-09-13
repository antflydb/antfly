import unittest

from source_capture_experiment import capture_preparation_evidence, checkpoint_handoffs


class CaptureEvidenceTest(unittest.TestCase):
    def lines(self, deferred=True, outside=True):
        return [
            f"dense replay collection token=3 sequence=8 records=4 applied_windows=1 deferred_capture={str(deferred).lower()} capture_before_collection={str(not outside).lower()} collect_ns=30000000 apply_ns=80000000",
            "dense replay capture finish token=3 sequence=8 success=true applied_sequence_persisted=true",
        ]

    def test_requires_committed_observed_treatment(self):
        result = capture_preparation_evidence(self.lines(), True)
        self.assertEqual(result["observations_starting_outside_capture"], 1)
        self.assertEqual(result["collection_ns"], 30000000)
        self.assertNotIn("saved_ns", result)
        self.assertEqual(
            capture_preparation_evidence(self.lines(False, False), False)[
                "observations_starting_outside_capture"
            ],
            0,
        )

    def test_missing_failed_inert_and_wrong_flag_do_not_qualify(self):
        for lines in (
            [],
            self.lines()[:1],
            self.lines(True, False),
            self.lines(False, False),
            [
                *self.lines()[:1],
                "dense replay capture finish token=3 sequence=8 success=false applied_sequence_persisted=false",
            ],
            [
                *self.lines()[:1],
                "dense replay capture finish token=4 sequence=8 success=true applied_sequence_persisted=true",
            ],
            [
                *self.lines()[:1],
                "dense replay capture finish token=3 sequence=8 success=true applied_sequence_persisted=false",
            ],
        ):
            with self.assertRaises(RuntimeError):
                capture_preparation_evidence(lines, True)

    def test_malformed_timing_and_ownership_are_rejected(self):
        for old, new in (
            ("collect_ns=30000000", "collect_ns=-1"),
            ("apply_ns=80000000", "apply_ns=nan"),
            ("capture_before_collection=false", "capture_before_collection=null"),
        ):
            with self.assertRaises(RuntimeError):
                capture_preparation_evidence(
                    [row.replace(old, new) for row in self.lines()], True
                )

    def test_coalesced_capture_requires_covering_final_watermark(self):
        lines = self.lines()
        lines[1] = lines[1].replace("sequence=8", "sequence=12")
        self.assertEqual(
            capture_preparation_evidence(lines, True)["committed_observations"], 1
        )
        lines[1] = lines[1].replace("sequence=12", "sequence=7")
        with self.assertRaises(RuntimeError):
            capture_preparation_evidence(lines, True)
        with self.assertRaises(RuntimeError):
            capture_preparation_evidence(self.lines() + self.lines()[1:], True)

    def test_handoffs_require_publication_and_preserve_nonadditive_samples(self):
        handoff = "dense checkpoint handoff generation=2 sequence=8 kind=delta completed_wait_ns=100 prepare_ns=2 install_ns=3 written_bytes=40 retained_bytes=20"
        blocker = "dense checkpoint completion blockers generation=2 source_capture_overlap_ns=90 maintenance_capture_overlap_ns=20 rebase_stage_ns=5 lock_deferrals=3"
        self.assertEqual(checkpoint_handoffs([handoff, blocker]), [])
        publication = (
            "dense posting checkpoint published generation=2 sequence=8 kind=delta"
        )
        samples = checkpoint_handoffs([handoff, blocker, publication])
        self.assertEqual(samples[0]["source_capture_overlap_ns"], 90)
        self.assertEqual(samples[0]["lock_deferrals"], 3)
        self.assertNotIn("scheduling_ns", samples[0])
        self.assertNotIn(
            "source_capture_overlap_ns", checkpoint_handoffs([handoff, publication])[0]
        )
        with self.assertRaises(RuntimeError):
            checkpoint_handoffs([handoff, handoff, publication])
        with self.assertRaises(RuntimeError):
            checkpoint_handoffs(
                [handoff.replace("install_ns=3", "install_ns=-1"), publication]
            )


if __name__ == "__main__":
    unittest.main()
