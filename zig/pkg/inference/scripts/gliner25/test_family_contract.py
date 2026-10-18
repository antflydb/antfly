# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""CPU-only guards for the family oracle's identity and batch contracts."""

import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

import family_oracle as oracle
from check_family_decisions import compare


class FamilyContractTests(unittest.TestCase):
    def test_gold_intent_scoring_keeps_document_and_label_order(self):
        from score_family_classification import selected_labels

        case = dict(texts=["help", "refund"], tasks=dict(intent=["refund", "support"]))
        native = dict(
            decisions=[
                dict(tasks=[dict(task_index=0, selections=[dict(label_index=i)])])
                for i in (1, 0)
            ]
        )
        self.assertEqual(
            selected_labels(case, native, "decide_1b"), ["support", "refund"]
        )
        native["decisions"][0]["tasks"][0]["selections"].append(dict(label_index=0))
        with self.assertRaisesRegex(ValueError, "one intent selection"):
            selected_labels(case, native, "decide_1b")
        boundary = dict(
            outputs=[
                dict(classifications=[dict(name="intent", labels=[dict(label=label)])])
                for label in ("support", "refund")
            ]
        )
        self.assertEqual(
            selected_labels(case, boundary, "multi_decide"), ["support", "refund"]
        )
        boundary["outputs"][0]["classifications"][0]["name"] = "unrelated"
        with self.assertRaisesRegex(ValueError, "one intent selection"):
            selected_labels(case, boundary, "multi_decide")

    def test_original_text_exception_is_limited_to_synthetic_period(self):
        from source_span_policy import original_text_reference, validate_source_spans

        text = "東京 café"
        valid = dict(text="東京", source=dict(start=0, end=2), attributes=[])
        suffix = dict(
            text="café.", source=dict(start=3, end=8), attributes=[dict(name="context")]
        )
        output = dict(
            entities=[dict(name="location", values=[valid, suffix])],
            classifications=[],
            structures=[],
            relations=[dict(head=valid, tail=suffix)],
        )
        preserved = copy.deepcopy(output)
        adjusted, excluded = original_text_reference(text, output)
        self.assertEqual(adjusted["entities"][0]["values"], [valid])
        self.assertEqual(adjusted["relations"], [])
        self.assertEqual(len(excluded), 2)
        self.assertEqual(output, preserved)
        validate_source_spans(text, adjusted)
        for bad in (
            dict(start=-1, end=8),
            dict(start=3, end=9),
            dict(start=3, end=True),
        ):
            changed = copy.deepcopy(output)
            changed["entities"][0]["values"][1]["source"] = bad
            with self.assertRaisesRegex(ValueError, "outside original"):
                original_text_reference(text, changed)
        changed = copy.deepcopy(output)
        changed["entities"][0]["values"][1]["text"] = "invented."
        with self.assertRaisesRegex(ValueError, "outside original"):
            original_text_reference(text, changed)
        changed = copy.deepcopy(output)
        changed["relations"][0]["head"] = dict(valid, source=dict(start=-1, end=2))
        with self.assertRaisesRegex(ValueError, "outside original"):
            original_text_reference(text, changed)

    def test_original_text_parity_retains_strict_results_and_valid_span_checks(self):
        from check_family_holdout import compare_rows

        case = dict(
            id="suffix", texts=["é"], source_ids=["a"], schema=dict(entities=["person"])
        )
        valid = dict(
            text="é", confidence=0.9, source=dict(start=0, end=1), attributes=[]
        )
        invalid = dict(
            text="é.", confidence=0.8, source=dict(start=0, end=2), attributes=[]
        )
        output = dict(
            entities=[dict(name="person", values=[valid])],
            classifications=[],
            structures=[],
            relations=[],
        )
        want = copy.deepcopy(output)
        want["entities"][0]["values"].append(invalid)
        reference = dict(
            encoder_calls=[dict(input_ids=[[1, 2]], attention_mask=[[1, 1]])],
            outputs=[want],
        )
        native = dict(
            encoder_shape=[1, 2],
            input_ids=[1, 2],
            output=output,
            cuda_transfers=dict(host_fallback_calls=0, kernel_launches=10),
        )
        result = compare_rows(case, native, reference, "multi", 5e-4, "original_text")[
            0
        ]
        self.assertTrue(result["passed"])
        self.assertFalse(result["strict_comparison"]["passed"])
        self.assertEqual(len(result["reference_exclusions"]), 1)
        native["output"]["entities"][0]["values"][0]["confidence"] = 0.7
        self.assertFalse(
            compare_rows(case, native, reference, "multi", 5e-4, "original_text")[0][
                "passed"
            ]
        )
        native["output"] = want
        self.assertFalse(
            compare_rows(case, native, reference, "multi", 5e-4, "original_text")[0][
                "passed"
            ]
        )

    def test_reference_holdout_rejects_labels_and_token_drift(self):
        from check_family_reference_holdout import compare_reference_rows

        case = dict(texts=["refund", "help"], source_ids=["a", "b"])
        expected = dict(
            encoder_calls=[dict(input_ids=[[1], [2]], attention_mask=[[1], [1]])],
            outputs=[
                dict(intent=dict(label="refund", confidence=0.9)),
                dict(intent=dict(label="help", confidence=0.8)),
            ],
        )
        actual = copy.deepcopy(expected)
        actual["outputs"][1]["intent"]["label"] = "refund"
        self.assertEqual(
            [r["passed"] for r in compare_reference_rows(case, actual, expected, 5e-3)],
            [True, False],
        )
        actual["encoder_calls"][0]["attention_mask"][1][0] = 0
        with self.assertRaisesRegex(ValueError, "attention masks"):
            compare_reference_rows(case, actual, expected, 5e-3)

    def test_decision_holdout_uses_strict_worker_schema(self):
        from check_family_holdout import decision_worker_fixture

        source = dict(
            id="en-US_0",
            texts=["refund"],
            tasks=dict(intent=["refund", "help"]),
            source_ids=["17"],
        )
        fixture = decision_worker_fixture([source])
        self.assertEqual(
            fixture,
            dict(
                format_version=1,
                cases=[
                    dict(
                        id="en-US_0",
                        texts=["refund"],
                        tasks=dict(intent=["refund", "help"]),
                    )
                ],
            ),
        )
        self.assertEqual(source["source_ids"], ["17"])

    def test_entity_holdout_checks_coordinates_per_document(self):
        from check_family_holdout import compare_rows

        case = dict(
            id="entities",
            texts=["John", "John"],
            source_ids=["a", "b"],
            schema=dict(entities=["person"]),
        )
        output = dict(
            entities=[
                dict(
                    name="person",
                    values=[
                        dict(
                            text="John",
                            confidence=0.9,
                            source=dict(start=0, end=4),
                            attributes=[],
                        )
                    ],
                )
            ],
            classifications=[],
            structures=[],
            relations=[],
        )
        reference = dict(
            encoder_calls=[
                dict(input_ids=[[1, 2], [1, 2]], attention_mask=[[1, 1], [1, 1]])
            ],
            outputs=[copy.deepcopy(output), copy.deepcopy(output)],
        )
        native = dict(
            encoder_shape=[2, 2],
            input_ids=[1, 2, 1, 2],
            outputs=[copy.deepcopy(output), copy.deepcopy(output)],
            cuda_transfers=dict(host_fallback_calls=0, kernel_launches=10),
        )
        native["outputs"][1]["entities"][0]["values"][0]["source"]["end"] = 3
        results = compare_rows(case, native, reference, "multi", 5e-4)
        self.assertEqual([row["passed"] for row in results], [True, False])

    def test_holdout_selection_and_language_gate(self):
        from prepare_family_holdout import select_rows
        from check_family_holdout import slice_summary

        rows = [
            dict(id=str(i), partition="test", utt=f"document {i}") for i in range(250)
        ]
        selected = select_rows(rows, 200)
        self.assertEqual(selected, select_rows(list(reversed(rows)), 200))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            select_rows(rows + [rows[0]], 200)
        results = [dict(source_id=row["id"], passed=True) for row in selected]
        results[0]["passed"] = False
        self.assertTrue(slice_summary(results)["passed"])
        results[1]["passed"] = False
        self.assertFalse(slice_summary(results)["passed"])
        with self.assertRaisesRegex(ValueError, "200 unique"):
            slice_summary(results[:-1])

    def test_holdout_counts_each_row_and_rejects_token_drift(self):
        from check_family_holdout import compare_rows

        case = dict(
            id="two",
            texts=["refund", "help"],
            source_ids=["a", "b"],
            tasks={"intent": ["refund", "help"]},
        )
        reference = dict(
            encoder_calls=[
                dict(input_ids=[[1, 2], [3, 0]], attention_mask=[[1, 1], [1, 0]])
            ],
            outputs=[
                {"intent": {"label": "refund", "confidence": 0.9}},
                {"intent": {"label": "help", "confidence": 0.8}},
            ],
        )
        native = dict(
            encoder_shape=[2, 2],
            input_ids=[1, 2, 3, 0],
            outputs=[
                dict(
                    classifications=[
                        dict(
                            name="intent", labels=[dict(label="refund", confidence=0.9)]
                        )
                    ]
                ),
                dict(
                    classifications=[
                        dict(
                            name="intent", labels=[dict(label="refund", confidence=0.8)]
                        )
                    ]
                ),
            ],
            cuda_transfers=dict(host_fallback_calls=0, kernel_launches=10),
        )
        results = compare_rows(case, native, reference, "multi_decide", 5e-3)
        self.assertEqual([r["passed"] for r in results], [True, False])
        native["input_ids"][2] = 4
        with self.assertRaisesRegex(ValueError, "token identity"):
            compare_rows(case, native, reference, "multi_decide", 5e-3)

    def test_sidecar_fixtures_are_pinned(self):
        manifest = json.loads((oracle.FIXTURES / "manifest.json").read_text())
        for name, model in manifest["models"].items():
            for filename in (
                "config.json",
                "encoder_config/config.json",
                "tokenizer_config.json",
            ):
                data = (oracle.FIXTURES / name / filename).read_bytes()
                self.assertEqual(len(data), model["files"][filename]["size_bytes"])
                self.assertEqual(
                    hashlib.sha256(data).hexdigest(), model["files"][filename]["sha256"]
                )
        for filename, pin in manifest["fixtures"].items():
            data = (oracle.FIXTURES / filename).read_bytes()
            self.assertEqual(len(data), pin["size_bytes"])
            self.assertEqual(hashlib.sha256(data).hexdigest(), pin["sha256"])
        self.assertEqual(
            set(manifest["fixtures"]),
            {
                str(path.relative_to(oracle.FIXTURES))
                for path in oracle.FIXTURES.rglob("*")
                if path.is_file() and path != oracle.FIXTURES / "manifest.json"
            },
        )

    def test_public_batch_receives_distinct_documents_once(self):
        calls = []
        model = SimpleNamespace(
            batch_classify_text=lambda *a, **kw: calls.append((a, kw)) or [{}, {}]
        )
        command = dict(
            texts=["refund this charge", "reset my password"],
            tasks={"intent": ["refund", "login"]},
        )
        self.assertEqual(len(oracle.execute(model, command)), 2)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0][0], command["texts"])
        self.assertEqual(calls[0][1]["batch_size"], 2)

    def test_bad_batch_is_rejected_before_invocation(self):
        model = SimpleNamespace(
            batch_classify_text=lambda *a, **kw: self.fail("invalid batch executed")
        )
        for texts in ([], [1], ["x"] * 65, "text"):
            with self.assertRaises(ValueError):
                oracle.execute(model, dict(texts=texts, tasks={"intent": ["a", "b"]}))

    def test_comparison_checks_every_row_tokens_and_decisions(self):
        case = dict(id="batch", texts=["one", "two"], tasks={"intent": ["a", "b"]})
        reference = dict(
            encoder_calls=[
                dict(input_ids=[[1, 2], [3, 0]], attention_mask=[[1, 1], [1, 0]])
            ],
            outputs=[
                {"intent": {"label": "a", "confidence": 0.9}},
                {"intent": {"label": "b", "confidence": 0.8}},
            ],
        )
        native = dict(
            batch_size=2,
            prepared_input_ids=[[1, 2], [3]],
            decisions=[
                dict(
                    raw_logits=[1.0, 0.0],
                    tasks=[
                        dict(
                            task_index=0,
                            selections=[dict(label_index=0, probability=0.9)],
                        )
                    ],
                ),
                dict(
                    raw_logits=[0.0, 1.0],
                    tasks=[
                        dict(
                            task_index=0,
                            selections=[dict(label_index=1, probability=0.8)],
                        )
                    ],
                ),
            ],
        )
        self.assertEqual(compare(case, native, reference), 0)
        wrong = copy.deepcopy(native)
        wrong["decisions"][1]["tasks"][0]["selections"][0]["label_index"] = 0
        with self.assertRaisesRegex(ValueError, "decision mismatch"):
            compare(case, wrong, reference)
        wrong = copy.deepcopy(native)
        wrong["prepared_input_ids"][1] = [4]
        with self.assertRaisesRegex(ValueError, "token identity"):
            compare(case, wrong, reference)
        wrong = copy.deepcopy(native)
        wrong["decisions"][1]["tasks"][0]["selections"][0]["probability"] = 0.802
        with self.assertRaisesRegex(ValueError, "confidence error"):
            compare(case, wrong, reference)
        compare(case, wrong, reference, 5e-3)
        wrong["decisions"][1]["raw_logits"][0] = float("nan")
        with self.assertRaisesRegex(ValueError, "non-finite"):
            compare(case, wrong, reference, 5e-3)
        device = copy.deepcopy(native)
        device.update(
            backend="cuda",
            warmup=0,
            reps=1,
            cuda_transfers=dict(kernel_launches=10, d2h_bytes=16),
        )
        self.assertEqual(compare(case, device, reference), 0)
        device["cuda_transfers"]["d2h_bytes"] += 4
        with self.assertRaisesRegex(ValueError, "intermediate host download"):
            compare(case, device, reference)

    def test_reference_profiles_must_preserve_labels_and_fp32_confidence(self):
        from benchmark_family import reference_agreement, aggregate_interval

        reference = [{"intent": {"label": "refund", "confidence": 0.8}}]
        reference_agreement(reference, reference, 5e-4)
        for changed in (
            [{"intent": {"label": "support", "confidence": 0.8}}],
            [{"intent": {"label": "refund", "confidence": 0.81}}],
            [],
        ):
            with self.assertRaises(ValueError):
                reference_agreement(changed, reference, 5e-4)
        interval = aggregate_interval([[(10, 20)] * 30, [(100, 200)] * 30], draws=100)
        self.assertEqual(interval["lower_95"], 2)
        self.assertEqual(interval["upper_95"], 2)
        with self.assertRaises(ValueError):
            aggregate_interval([[]])

    def test_boundary_comparison_checks_ragged_rows_and_device_execution(self):
        from check_family_decisions import compare_boundary

        case = dict(
            id="two", texts=["refund", "help"], tasks={"intent": ["refund", "help"]}
        )
        reference = dict(
            encoder_calls=[
                dict(input_ids=[[1, 2], [3, 0]], attention_mask=[[1, 1], [1, 0]])
            ],
            outputs=[
                {"intent": {"label": "refund", "confidence": 0.9}},
                {"intent": {"label": "help", "confidence": 0.8}},
            ],
        )
        native = dict(
            encoder_shape=[2, 2],
            input_ids=[1, 2, 3, 0],
            outputs=[
                dict(
                    classifications=[
                        dict(
                            name="intent", labels=[dict(label="refund", confidence=0.9)]
                        )
                    ]
                ),
                dict(
                    classifications=[
                        dict(name="intent", labels=[dict(label="help", confidence=0.8)])
                    ]
                ),
            ],
            cuda_transfers=dict(host_fallback_calls=0, kernel_launches=10),
        )
        self.assertEqual(compare_boundary(case, native, reference), 0)
        native["outputs"][1]["classifications"][0]["labels"][0]["label"] = "refund"
        with self.assertRaisesRegex(ValueError, "decision mismatch"):
            compare_boundary(case, native, reference)
        native["outputs"][1]["classifications"][0]["labels"][0]["label"] = "help"
        native["cuda_transfers"]["host_fallback_calls"] = 1
        with self.assertRaisesRegex(ValueError, "CUDA execution evidence"):
            compare_boundary(case, native, reference)

    def test_holdout_coverage_requires_predictions_not_schema_entries(self):
        from check_family_holdout import extraction_coverage

        empty = dict(
            entities=[dict(name="person", values=[])],
            structures=[dict(name="request", instances=[])],
            relations=[],
        )
        self.assertEqual(
            extraction_coverage([empty]),
            dict(entities=0, attributes=0, relations=0, records=0),
        )
        nonempty = dict(
            entities=[dict(values=[dict(attributes=[dict(name="context")])])],
            structures=[dict(instances=[dict(fields=[])])],
            relations=[dict(name="located in")],
        )
        self.assertEqual(
            extraction_coverage([empty, nonempty]),
            dict(entities=1, attributes=1, relations=1, records=1),
        )

    def test_paired_campaign_drives_all_samples_and_writes_report(self):
        import tempfile
        import sys
        from unittest.mock import patch
        import benchmark_family as campaign

        pin = dict(model_id="test", files={})
        case = dict(id="one", texts=["hello"], tasks={"intent": ["yes", "no"]})
        reference = dict(
            request_id="one",
            encoder_calls=[dict(input_ids=[[1, 2]], attention_mask=[[1, 1]])],
            outputs=[{"intent": {"label": "yes", "confidence": 0.9}}],
            duration_ns=20,
        )
        native = dict(
            batch_size=1,
            prepared_input_ids=[[1, 2]],
            decisions=[
                dict(
                    raw_logits=[1.0, 0.0],
                    tasks=[
                        dict(
                            task_index=0,
                            selections=[dict(label_index=0, probability=0.9)],
                        )
                    ],
                )
            ],
            duration_ns=10,
        )
        calls = []

        class Worker:
            def __init__(self, arm, *args):
                self.arm = arm

            def receive(self, timeout):
                return dict(
                    event="ready",
                    build_mode="fast",
                    model_files={},
                    backend="cuda",
                    precision="fp32",
                    dtype="fp32",
                    weight_dtype="fp32",
                    attention="eager",
                )

            def close(self):
                pass

        def execute(worker, command, timeout):
            calls.append((worker.arm, command["op"]))
            return reference if worker.arm == "python" else native

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cases = root / "cases.json"
            cases.write_text(json.dumps(dict(format_version=1, cases=[case])))
            capture = root / "oracle.jsonl"
            capture.write_text(
                json.dumps(
                    dict(
                        dtype="fp32", model_id=pin["model_id"], model_files=pin["files"]
                    )
                )
                + "\n"
                + json.dumps(reference)
                + "\n"
            )
            binary = root / "native"
            binary.write_bytes(b"fixture binary")
            args = [
                "benchmark_family.py",
                "--native",
                str(binary),
                "--python",
                sys.executable,
                "--model-dir",
                temp,
                "--model",
                "decide_1b",
                "--cases",
                str(cases),
                "--oracle",
                str(capture),
                "--output",
                str(root / "out"),
                "--warmup",
                "1",
            ]
            with (
                patch.object(sys, "argv", args),
                patch.object(campaign, "verify_model", return_value=pin),
                patch.object(campaign.cpu, "Worker", Worker),
                patch.object(campaign, "request", execute),
                patch.object(
                    campaign.cpu,
                    "ResourceGuard",
                    return_value=SimpleNamespace(peak_rss_bytes=0),
                ),
            ):
                campaign.main()
            report = json.loads((root / "out/report.json").read_text())
            self.assertFalse(report["qualification"])
            self.assertTrue(report["measured_cells_passed"])
            self.assertEqual(len(calls), 404)  # two validations, two warmups, 200 pairs
            pairs = [
                json.loads(line)
                for line in (root / "out/raw.jsonl").read_text().splitlines()[1:]
            ]
            self.assertNotEqual(pairs[0]["order"], pairs[1]["order"])

    def test_rejected_quality_capture_is_saved_and_marked_failed(self):
        import tempfile
        import sys
        from unittest.mock import patch
        import check_family_decisions as check

        pin = dict(model_id="test", revision="pinned", files={})
        case = dict(id="one", texts=["hello"], tasks={"intent": ["yes", "no"]})
        reference = dict(
            request_id="one",
            encoder_calls=[dict(input_ids=[[1]], attention_mask=[[1]])],
            outputs=[{"intent": {"label": "yes", "confidence": 0.9}}],
        )
        native = dict(
            case_index=0, batch_size=1, prepared_input_ids=[[2]], decisions=[]
        )
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "cases.json").write_text(
                json.dumps(dict(format_version=1, cases=[case]))
            )
            (root / "oracle.jsonl").write_text(
                json.dumps(dict(dtype="fp32", model_id="test", model_files={}))
                + "\n"
                + json.dumps(reference)
                + "\n"
            )
            (root / "native").write_bytes(b"fixture binary")
            args = [
                "check_family_decisions.py",
                "--native",
                str(root / "native"),
                "--model-dir",
                temp,
                "--cases",
                str(root / "cases.json"),
                "--oracle",
                str(root / "oracle.jsonl"),
                "--output",
                str(root / "report.json"),
            ]
            process = SimpleNamespace(
                stdout=json.dumps(native) + "\n",
                stderr="diagnostic",
                check_returncode=lambda: None,
            )
            with (
                patch.object(sys, "argv", args),
                patch.object(check, "verify_model", return_value=pin),
                patch.object(check.subprocess, "run", return_value=process),
            ):
                with self.assertRaisesRegex(ValueError, "token identity"):
                    check.main()
            report = json.loads((root / "report.json").read_text())
            self.assertEqual(report["status"], "failed")
            self.assertFalse(report["qualification"])
            self.assertEqual(report["expected_cases"], 1)
            self.assertEqual(report["cases"], [])
            self.assertEqual((root / "report.raw.jsonl").read_text(), process.stdout)

    def test_full_boundary_comparison_preserves_source_coordinates_and_records(self):
        from check_family_decisions import compare_full_boundary

        output = dict(
            entities=[
                dict(
                    name="person",
                    values=[
                        dict(
                            text="John",
                            confidence=0.9,
                            source=dict(start=0, end=4),
                            attributes=[],
                        )
                    ],
                )
            ],
            classifications=[],
            structures=[],
            relations=[],
        )
        reference = dict(
            encoder_calls=[dict(input_ids=[[1, 2]], attention_mask=[[1, 1]])],
            outputs=[output],
        )
        native = dict(
            encoder_shape=[1, 2],
            input_ids=[1, 2],
            output=copy.deepcopy(output),
            cuda_transfers=dict(host_fallback_calls=0, kernel_launches=10),
        )
        case = dict(texts=["John"])
        self.assertEqual(compare_full_boundary(case, native, reference), 0)
        native["output"]["entities"][0]["values"][0]["source"]["end"] = 3
        with self.assertRaisesRegex(ValueError, "coordinate"):
            compare_full_boundary(case, native, reference)
        native["output"] = copy.deepcopy(output)
        native["output"]["structures"] = [dict(name="record", instances=[])]
        with self.assertRaisesRegex(ValueError, "cardinality"):
            compare_full_boundary(case, native, reference)


if __name__ == "__main__":
    unittest.main()
