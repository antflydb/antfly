"""Safety gates for the same-data recovery diagnostic runner."""

import subprocess
import unittest
from copy import deepcopy
from unittest.mock import Mock

from run_dense_recovery_query_ab import common_treatment_flags, ready, stop
from run_posting_locality_ab import REFINEMENTS


class QueryDiagnosticTest(unittest.TestCase):
    def test_warm_routing_synergy_is_exact_union(self):
        self.assertEqual(
            set(REFINEMENTS["compact_borrowed"]),
            set(REFINEMENTS["compact_subgroup_routing"])
            | set(REFINEMENTS["borrowed_pages"]),
        )
        with self.assertRaisesRegex(ValueError, "fresh ingest"):
            common_treatment_flags(["control", "incremental_publication"], [])
        with self.assertRaisesRegex(ValueError, "fresh ingest"):
            common_treatment_flags(
                ["control", "incremental_publication"], [], mixed=True
            )
        flags = common_treatment_flags(
            ["control", "compact_subgroup_routing"],
            ["incremental_publication", "subgroups_4"],
            mixed=True,
        )
        self.assertIn("ANTFLY_SOURCE_VECTOR_APPEND_ONLY", flags)

    def test_subgroup_layout_is_fixed_not_a_query_treatment(self):
        flags = common_treatment_flags(
            ["control", "subgroup_routing"], ["aggregate", "subgroups_4"]
        )
        self.assertIn("ANTFLY_EXPERIMENT_SUBGROUPS_4", flags)
        self.assertNotIn("ANTFLY_EXPERIMENT_SUBGROUP_ROUTING", flags)
        with self.assertRaises(ValueError):
            common_treatment_flags(["control", "subgroups_4"], [])
        with self.assertRaises(ValueError):
            common_treatment_flags(
                ["control", "subgroup_routing"], ["subgroups_4", "subgroups_16"]
            )

    def test_combined_routing_arm_adds_only_routing_to_admission(self):
        self.assertEqual(
            set(REFINEMENTS["admitted_quantized_routing"]),
            set(REFINEMENTS["aggregate"]) | set(REFINEMENTS["quantized_routing"]),
        )

    def test_common_admission_does_not_enable_routing_treatment(self):
        flags = common_treatment_flags(["control", "quantized_routing"], ["aggregate"])
        self.assertEqual(
            flags,
            {
                "ANTFLY_EXPERIMENT_PHASE_ADMISSION",
                "ANTFLY_EXPERIMENT_AGGREGATE_ADMISSION",
            },
        )
        with self.assertRaises(ValueError):
            common_treatment_flags(["control", "aggregate"], ["aggregate"])

    def test_readiness_rejects_missing_or_pending_authority(self):
        self.assertFalse(ready({}))
        payload = {
            "status": {
                "readiness": {"state": "ready"},
                "backfill_active": False,
                "dense_publish_pending": False,
                "dense_vector_projection_pending": False,
                "hbc_posting": {"dirty_postings": 0},
            }
        }
        self.assertTrue(ready(payload))
        for field in (
            "backfill_active",
            "dense_publish_pending",
            "dense_vector_projection_pending",
        ):
            pending = deepcopy(payload)
            pending["status"][field] = True
            self.assertFalse(ready(pending))
        payload["status"]["hbc_posting"]["dirty_postings"] = 1
        self.assertFalse(ready(payload))

    def test_shutdown_only_signals_live_owned_process(self):
        stop(None)
        exited = Mock()
        exited.poll.return_value = 0
        stop(exited)
        exited.terminate.assert_not_called()
        live = Mock()
        live.poll.return_value = None
        stop(live)
        live.terminate.assert_called_once()
        live.wait.assert_called_once_with(timeout=30)
        live.kill.assert_not_called()

    def test_shutdown_timeout_is_bounded(self):
        live = Mock()
        live.poll.return_value = None
        live.wait.side_effect = [subprocess.TimeoutExpired("owned server", 30), 0]
        stop(live)
        live.kill.assert_called_once()
        self.assertEqual(live.wait.call_args_list[-1].kwargs, {"timeout": 10})


if __name__ == "__main__":
    unittest.main()
