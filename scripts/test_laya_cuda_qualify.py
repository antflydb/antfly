import copy
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

spec = importlib.util.spec_from_file_location(
    "laya_cuda_qualify", Path(__file__).with_name("laya_cuda_qualify.py")
)
qualify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualify)


class LayaCudaQualificationTest(unittest.TestCase):
    def setUp(self):
        self.row = {
            "task": {
                "text": "hello",
                "question": {
                    "name": "needed",
                    "kind": "noul",
                    "instruction": "needed?",
                    "labels": ["false", "true"],
                    "descriptions": ["no", "yes"],
                },
            },
            "probabilities": [0.25, 0.75],
            "act_probability": 0.8,
        }
        self.response = {
            "data": [
                {
                    "id": "0",
                    "decisions": [
                        {
                            "label": "true",
                            "probabilities": [
                                {"label": "false", "probability": 0.25},
                                {"label": "true", "probability": 0.75},
                            ],
                            "act_probability": 0.8,
                            "name": "needed",
                            "type": "boolean",
                            "confidence": 0.75,
                            "confidence_method": "max_probability",
                            "true_probability": 0.75,
                        }
                    ],
                }
            ]
        }

    def test_request_preserves_per_input_schema_and_required_envelope(self):
        body = qualify.request_body([self.row])
        self.assertEqual({}, body["schema"])
        question = body["inputs"][0]["schema"]["classifications"][0]
        self.assertEqual("boolean", question["mode"])
        self.assertEqual(["false", "true"], question["labels"])
        self.assertEqual("yes", question["label_definitions"]["true"]["description"])
        qualify.check_response(self.response, [self.row])

    def test_strict_parity_rejects_nonfinite_values_errors_and_wrong_order(self):
        for value in (float("nan"), float("inf"), 0.7501):
            response = copy.deepcopy(self.response)
            response["data"][0]["decisions"][0]["probabilities"][1]["probability"] = (
                value
            )
            with self.assertRaises(ValueError):
                qualify.check_response(response, [self.row])

        for mutation in ("order", "label", "action", "id", "derived"):
            response = copy.deepcopy(self.response)
            item = response["data"][0]
            decision = item["decisions"][0]
            if mutation == "order":
                decision["probabilities"].reverse()
            elif mutation == "label":
                decision["label"] = "false"
            elif mutation == "action":
                decision["act_probability"] = float("nan")
            elif mutation == "derived":
                decision["true_probability"] = 0.9
            else:
                item["id"] = "wrong"
            with self.assertRaises(ValueError):
                qualify.check_response(response, [self.row])

    def test_512_tasks_keep_input_and_question_identity(self):
        rows = [self.row] * 128
        body = qualify.request_body(rows, copies=4)
        self.assertEqual(128, len(body["inputs"]))
        response = {"data": []}
        for index, item in enumerate(body["inputs"]):
            names = [q["name"] for q in item["schema"]["classifications"]]
            self.assertEqual([f"needed_{i}" for i in range(4)], names)
            decisions = []
            for name in names:
                decision = copy.deepcopy(self.response["data"][0]["decisions"][0])
                decision["name"] = name
                decisions.append(decision)
            response["data"].append({"id": str(index), "decisions": decisions})
        qualify.check_response(response, rows, copies=4)
        response["data"][-1]["decisions"].reverse()
        with self.assertRaisesRegex(ValueError, "identity"):
            qualify.check_response(response, rows, copies=4)

    def test_missing_gpu_writes_failed_report_and_does_not_prepare(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            with (
                patch.object(
                    qualify, "run", side_effect=RuntimeError("CUDA unavailable")
                ),
                patch.object(qualify, "prepare") as prepare,
            ):
                result = qualify.main(
                    [
                        "--binary",
                        "missing",
                        "--tests",
                        "missing",
                        "--work-dir",
                        directory,
                        "--report",
                        str(report),
                        "--prepare",
                    ]
                )
            self.assertEqual(1, result)
            self.assertEqual("failed", json.loads(report.read_text())["status"])
            prepare.assert_not_called()

    def test_unpinned_checkpoint_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "released/model").mkdir(parents=True)
            (root / "released/model/model_manifest.json").write_text(
                '{"source":{"revision":"main"}}'
            )
            (root / "released/qualification.json").write_text(
                '{"source":{"revision":"main"}}'
            )
            with self.assertRaisesRegex(ValueError, "pinned English"):
                qualify.validate_fixtures(root)

    def test_test_gate_rejects_empty_skipped_or_wrong_executables(self):
        name = "laya released checkpoint accuracy parity batching and performance"
        output = (
            f"1/50 pipelines.laya_parity_test.test.{name}..."
            "Laya qualification backend=native load_ms=1\nOK\n"
            "1 selected; 1 passed; 0 skipped.\n"
        )
        self.assertEqual(
            {"selected": 1, "passed": 1, "skipped": 0},
            qualify.validate_test_run(output, "native"),
        )
        for invalid in (
            "",
            "0 selected; 0 passed; 0 skipped.",
            output.replace("1 passed; 0 skipped", "0 passed; 1 skipped"),
            output.replace(name, "unrelated passing test"),
            output.replace("backend=native", "backend=cuda"),
            output + "1 selected; 1 passed; 0 skipped.",
        ):
            with self.assertRaises(ValueError):
                qualify.validate_test_run(invalid, "native")
        # Even a successful released-checkpoint test cannot substitute for the
        # complete CUDA kernel, synthetic, and HTTP suite.
        with self.assertRaisesRegex(ValueError, "required test"):
            qualify.validate_test_run(
                output.replace("backend=native", "backend=cuda"), "cuda"
            )

    def test_cancellation_recovery_limits_request_timeout_and_rejects_late_success(
        self,
    ):
        for elapsed in (1, 121):
            with self.subTest(elapsed=elapsed):
                now = [0.0]
                process = MagicMock()
                process.poll.return_value = None

                def execute(rows, *, timeout, now=now, elapsed=elapsed):
                    self.assertEqual([self.row], rows)
                    self.assertEqual(120, timeout)
                    now[0] += elapsed

                with (
                    patch.object(qualify, "run", side_effect=["0", "1"]),
                    patch.object(
                        qualify.socket, "create_connection", return_value=MagicMock()
                    ),
                    patch.object(
                        qualify.time, "monotonic", side_effect=lambda now=now: now[0]
                    ),
                ):
                    if elapsed > 120:
                        with self.assertRaisesRegex(
                            RuntimeError, "exceeded 120 seconds"
                        ):
                            qualify.cancel_and_recover(
                                1, [self.row], "nvidia-smi", execute, process
                            )
                    else:
                        result = qualify.cancel_and_recover(
                            1, [self.row], "nvidia-smi", execute, process
                        )
                        self.assertEqual(1000, result["recovery_ms"])


if __name__ == "__main__":
    unittest.main()
