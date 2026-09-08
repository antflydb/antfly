import copy
import unittest
from pathlib import Path
from unittest.mock import mock_open, patch

from validate_vdbbench_result import validate_lifecycle_log, validate_metrics


class ResultValidationTest(unittest.TestCase):
    def setUp(self):
        self.result = {
            "label": "NORMAL",
            "metrics": {
                "inserted_count": 50000,
                "recall": 0.99,
                "serial_latency_p95": 0.004,
                "conc_num_list": [1, 10, 20, 30],
                "conc_qps_list": [100, 500, 700, 800],
                "conc_latency_avg_list": [0.01] * 4,
                "conc_latency_p95_list": [0.02] * 4,
                "conc_latency_p99_list": [0.03] * 4,
            },
        }

    def test_complete_curve(self):
        validate_metrics(self.result, 50000, True, [1, 10, 20, 30])

    def test_lifecycle_failure_cannot_pass_on_complete_query_metrics(self):
        for failure in (
            "quarantined after repeated zero progress",
            "PostingWalMutationOutsideCapture",
            "PostingWalMutationStoreUnavailable",
            "PostingStoreRequiresReopen",
        ):
            with self.subTest(failure=failure):
                with patch.object(
                    Path,
                    "open",
                    mock_open(read_data=f"info: ready\nerror: {failure}\n"),
                ):
                    with self.assertRaisesRegex(ValueError, "lifecycle failure"):
                        validate_lifecycle_log(Path("server.log"))
        with patch.object(
            Path,
            "open",
            mock_open(read_data="info: publication deferred until stable source\n"),
        ):
            validate_lifecycle_log(Path("server.log"))

    def test_partial_normal_curve(self):
        self.result["metrics"]["conc_num_list"].pop()
        with self.assertRaisesRegex(ValueError, "incomplete concurrency"):
            validate_metrics(self.result, 50000, True, [1, 10, 20, 30])

    def test_invalid_measurements(self):
        for invalid in (0, -1, float("nan"), float("inf"), None, True):
            with self.subTest(invalid=invalid):
                result = copy.deepcopy(self.result)
                result["metrics"]["conc_qps_list"][-1] = invalid
                with self.assertRaises(ValueError):
                    validate_metrics(result, 50000, True, [1, 10, 20, 30])

    def test_truncated_latency_array(self):
        self.result["metrics"]["conc_latency_p95_list"].pop()
        with self.assertRaises(ValueError):
            validate_metrics(self.result, 50000, True, [1, 10, 20, 30])

    def test_serial_only_does_not_require_curve(self):
        validate_metrics(
            {"metrics": {"recall": 0.99, "serial_latency_p95": 0.004}}, 0, True, []
        )

    def test_nonfinite_recall_or_incomplete_load(self):
        self.result["metrics"]["recall"] = float("nan")
        with self.assertRaises(ValueError):
            validate_metrics(self.result, 50000, True, [])
        with self.assertRaises(ValueError):
            validate_metrics(self.result, 1000000, False, [])


if __name__ == "__main__":
    unittest.main()
