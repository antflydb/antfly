from copy import deepcopy
import json
from pathlib import Path
import statistics
import tempfile
import unittest
from unittest.mock import patch

import check_qwen_performance as gates

LIMITS = {"timeout_seconds": 60, "max_rss_mib": 8192, "min_free_percent": 15,
          "max_swap_growth_mib": 0, "min_disk_free_mib": 1024}


def case_report(candidate_ms=90.0, iterations=20):
    samples = {
        "baseline": [100.0] * iterations,
        "candidate": [candidate_ms] * iterations,
    }
    return {
        "id": "tokens_8192",
        "pairs": [
            {"iteration": i, "warmup": i < 3,
             "case_ids": [f"tokens_8192_{i}"], "min_cosine": 1.0,
             "ms": {label: values[0] for label, values in samples.items()}}
            for i in range(iterations + 3)
        ],
        "samples_ms": samples,
        "median_ms": {label: statistics.median(v) for label, v in samples.items()},
        **{
            statistic + "_speedup": gates.bootstrap_ratio_ci(
                samples["candidate"], samples["baseline"], 2000, 1729,
                statistic=statistic,
            )
            for statistic in ("median", "p95")
        },
    }


class PerformanceGateTests(unittest.TestCase):
    def test_ablation_requires_same_binary_and_one_control(self):
        case = case_report()
        case.update(id="portrait", fixture={"images": ["image.png"]},
                    golden={"rows": [{"text": "frozen output"}]})
        for samples in [case["samples_ms"], case["median_ms"], *[
            pair["ms"] for pair in case["pairs"]
        ]]:
            samples["reference"] = samples.pop("baseline")
        anchor = {
            "schema": "antfly.qwen_paired_benchmark.v1", "pass": True,
            "phase": "ocr", "model": "vl", "warmup": 3, "iters": 20,
            "fixture_sha256": "a" * 64, "runner_sha256": "b" * 64,
            "system": {"cpu": "test"}, "cases": [case],
            "servers": {
                side: {"executable_sha256": str(i + 1) * 64,
                       "argv": ["run", "--port", str(18191 + i)],
                       "pid": 1000 + i,
                       "environment": {"TERMITE_EMBED_RESIDENT_FAIL_CLOSED": "1"}}
                for i, side in enumerate(("baseline", "candidate"))
            },
        }
        resource = {
            "pass": True, "returncode": 0, "violation": None, "samples": 10,
            "peak_group_rss_mib": 1000, "min_free_percent": 50,
            "swapout_growth_mib": 0, "min_disk_free_mib": 2000,
            "elapsed_seconds": 10, "limits": LIMITS,
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "ocr-run2").mkdir()
            (root / "ocr-run2/report.json").write_text(json.dumps(anchor))
            number = 0
            for control, setting in gates.OCR_CONTROLS.items():
                for run in (1, 2):
                    label = f"ocr-ablation-{control}-run{run}"
                    report = deepcopy(anchor)
                    for server in report["servers"].values():
                        server.update(executable_sha256="2" * 64, pid=2000 + number)
                        number += 1
                    report["servers"]["baseline"]["environment"].update(setting)
                    (root / label).mkdir()
                    (root / label / "report.json").write_text(json.dumps(report))
                    (root / (label + "-resources.json")).write_text(json.dumps(resource))
            result = gates.check_ocr_ablations(root)
            self.assertTrue(result["pass"], result["errors"])
            path = root / "ocr-ablation-q6-run2/report.json"
            original = json.loads(path.read_text())
            for side, change, message in (
                ("baseline", {"executable_sha256": "1" * 64}, "executable"),
                ("baseline", {"environment": {}}, "intended control"),
                ("candidate", {"environment": {"TERMITE_METAL_STAGE_TIMING": "1"}},
                 "intended control"),
            ):
                report = deepcopy(original)
                report["servers"][side].update(change)
                path.write_text(json.dumps(report))
                result = gates.check_ocr_ablations(root)
                self.assertFalse(result["pass"])
                self.assertTrue(any(message in error for error in result["errors"]))

    def test_primary_win_and_confirmed_regression_are_separate(self):
        for latency, win, regression in ((90, True, False), (97, False, False),
                                         (104, False, True)):
            with self.subTest(latency=latency):
                result = gates.check_case(case_report(latency), "embedding", 20)
                self.assertEqual(result["primary_win"], win)
                self.assertEqual(result["confirmed_regression"], regression)

    def test_bad_warmup_recycled_input_and_missing_pairs_fail(self):
        original = case_report()
        changes = (
            lambda case: case["pairs"][0].update(min_cosine=0.99),
            lambda case: case["pairs"][0].update(warmup=False),
            lambda case: case["pairs"][0].update(case_ids=["tokens_8192_1"]),
            lambda case: case["pairs"].pop(),
            lambda case: case["samples_ms"]["candidate"].pop(),
            lambda case: case["median_speedup"].update(lower_95=2.0),
        )
        for change in changes:
            case = deepcopy(original)
            change(case)
            with self.assertRaises(ValueError):
                gates.check_case(case, "embedding", 20)

    def test_resource_success_cannot_hide_swap_or_missing_samples(self):
        resource = {
            "pass": True, "returncode": 0, "violation": None, "samples": 10,
            "peak_group_rss_mib": 1000, "min_free_percent": 50,
            "swapout_growth_mib": 0, "min_disk_free_mib": 2000,
            "elapsed_seconds": 10, "limits": LIMITS,
        }
        gates.check_resources(resource)
        for change in ({"swapout_growth_mib": 1}, {"samples": 0},
                       {"min_disk_free_mib": 1023}, {"returncode": -15},
                       {"limits": LIMITS | {"max_swap_growth_mib": 128}}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                gates.check_resources(resource | change)

    def test_incomplete_campaign_does_not_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            report = gates.check_campaign(Path(directory))
        self.assertFalse(report["pass"])
        self.assertEqual(len(report["errors"]), 6)

    def test_complete_campaign_rejects_different_builds_between_families(self):
        resource = {
            "pass": True, "returncode": 0, "violation": None, "samples": 10,
            "peak_group_rss_mib": 1000, "min_free_percent": 50,
            "swapout_growth_mib": 0, "min_disk_free_mib": 2000,
            "elapsed_seconds": 10, "limits": LIMITS,
        }
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            gates.CASES, {phase: {primary} for phase, primary in gates.PRIMARY.items()}
        ):
            root = Path(directory)
            for phase in gates.CASES:
                labels = (phase + "-run1", phase + "-run2", gates.CONFIRMATION[phase])
                for run, label in enumerate(labels):
                    iterations = 100 if run == 2 else 20
                    case = case_report(iterations=iterations)
                    if phase == "ocr":
                        case.update(id="portrait", fixture={"images": ["image.png"]},
                                    golden={"rows": [{"text": "frozen output"}]})
                        for samples in [case["samples_ms"], case["median_ms"], *[
                            pair["ms"] for pair in case["pairs"]
                        ]]:
                            samples["reference"] = samples.pop("baseline")
                    report = {
                        "schema": "antfly.qwen_paired_benchmark.v1", "pass": True,
                        "phase": phase, "model": phase, "warmup": 3,
                        "iters": iterations, "fixture_sha256": "a" * 64,
                        "runner_sha256": "b" * 64, "system": {"cpu": "test"},
                        "order": "alternating AB/BA, one active request",
                        "cases": [case], "servers": {
                            side: {
                                "executable_sha256": str(i + 1) * 64,
                                "argv": ["run", "--port", str(18191 + i)],
                                "pid": 1000 + run * 2 + i,
                                "environment": {"TERMITE_EMBED_RESIDENT_FAIL_CLOSED": "1"},
                            }
                            for i, side in enumerate(("baseline", "candidate"))
                        },
                    }
                    (root / label).mkdir()
                    (root / label / "report.json").write_text(json.dumps(report))
                    (root / (label + "-resources.json")).write_text(json.dumps(resource))
            report = gates.check_campaign(root)
            self.assertTrue(report["pass"], report["errors"])
            for path in root.glob("ocr-*/report.json"):
                report = json.loads(path.read_text())
                report["servers"]["candidate"]["executable_sha256"] = "f" * 64
                path.write_text(json.dumps(report))
            report = gates.check_campaign(root)
            self.assertFalse(report["pass"])
            self.assertTrue(any("between model families" in e for e in report["errors"]))


if __name__ == "__main__":
    unittest.main()
