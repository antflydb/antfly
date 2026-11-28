#!/usr/bin/env python3
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

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

from qualify_gliner25_bf16 import compare, main, sha


FIXTURE = {
    "inputs": [{"shape": [1, 64]}, {}, {"shape": [1, 14]}],
    "outputs": [{"shape": [14]}],
}


def logits(topic):
    values = np.zeros(14)
    values[0] = values[12] = 3
    values[6:12] = -3
    values[6] = np.log(topic / (1 - topic))
    return values


class Bf16QualificationTests(unittest.TestCase):
    def test_prepared_report_requires_cuda_bf16_attention_and_ffn(self):
        fixture = dict(FIXTURE, id="case")
        values = [logits(0.2).tolist()]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, reference, native, output = (
                root / name for name in ("manifest", "reference", "native", "output")
            )
            manifest.write_text(
                json.dumps({"model_sha256": "model", "cases": [fixture]})
            )
            reference.write_text(
                json.dumps(
                    {
                        "precision": "bf16",
                        "dtype": "torch.bfloat16",
                        "model_sha256": "model",
                        "manifest_sha256": sha(manifest),
                        "inference_mode": True,
                        "autocast": False,
                        "matmul_settings": {
                            "allow_bf16_reduced_precision_reduction": True
                        },
                        "cases": [{"id": "case", "logits": values}],
                    }
                )
            )
            report = {
                "backend": "cuda",
                "encoder_dtype": "bf16",
                "cuda": {
                    "gliner25_bf16_attention_calls": 1,
                    "gliner25_bf16_ffn_calls": 1,
                },
                "cases": [{"id": "case", "logits": values, "physical_forwards": 1}],
            }
            argv = [
                "qualify",
                "--manifest",
                str(manifest),
                "--reference",
                str(reference),
                "--native",
                str(native),
                "--out",
                str(output),
            ]
            with patch("sys.argv", argv):
                fp32 = {
                    "precision": "fp32",
                    "encoder_dtype": "f32",
                    "cuda": {
                        "gliner25_bf16_attention_calls": 0,
                        "gliner25_bf16_ffn_calls": 0,
                    },
                }
                for update in (
                    fp32,
                    {"backend": "native"},
                    {"encoder_dtype": "f32"},
                    {"cuda": None},
                    {
                        "cuda": {
                            "gliner25_bf16_attention_calls": 1,
                            "gliner25_bf16_ffn_calls": 0,
                        }
                    },
                    {
                        "cuda": {
                            "gliner25_bf16_attention_calls": 0,
                            "gliner25_bf16_ffn_calls": 1,
                        }
                    },
                ):
                    with self.subTest(update=update):
                        native.write_text(json.dumps(report | update))
                        with self.assertRaisesRegex(
                            ValueError, "did not execute CUDA BF16"
                        ):
                            main()
                        self.assertFalse(output.exists())
                native.write_text(json.dumps(report))
                main()
                self.assertTrue(json.loads(output.read_text())["passed"])

    def test_threshold_crossing_requires_explicit_bounded_policy(self):
        ref, actual = logits(0.398), logits(0.413)
        self.assertFalse(compare(actual, ref, FIXTURE, 0.02)["passed"])
        result = compare(actual, ref, FIXTURE, 0.02, True)
        self.assertTrue(result["passed"])
        self.assertFalse(result["exact_decisions"])
        self.assertEqual(len(result["boundary_changes"]), 1)

    def test_confidence_drift_fails_even_when_decisions_match(self):
        self.assertFalse(
            compare(logits(0.58), logits(0.55), FIXTURE, 0.02, True)["passed"]
        )

    def test_confident_threshold_change_fails(self):
        self.assertFalse(
            compare(logits(0.42), logits(0.35), FIXTURE, 0.02, True)["passed"]
        )

    def test_argmax_tie_requires_bounded_policy(self):
        ref, actual = logits(0.2), logits(0.2)
        ref[:6] = np.log([0.48, 0.47, 0.0125, 0.0125, 0.0125, 0.0125])
        actual[:6] = ref[[1, 0, 2, 3, 4, 5]]
        self.assertFalse(compare(actual, ref, FIXTURE, 0.02)["passed"])
        self.assertTrue(compare(actual, ref, FIXTURE, 0.02, True)["passed"])

    def test_nonfinite_and_wrong_geometry_reject(self):
        for bad in (np.full(14, np.nan), np.zeros(13)):
            with self.assertRaises(ValueError):
                compare(bad, logits(0.2), FIXTURE, 0.02, True)

    def test_custom_tasks_and_threshold(self):
        fixture = {
            "inputs": [{"shape": [1, 64]}, {}, {"shape": [1, 3]}],
            "outputs": [{"shape": [3]}],
            "classification_tasks": [
                {"name": "sentiment", "size": 2},
                {"name": "flag", "size": 1, "multi_label": True, "threshold": 0.7},
            ],
        }
        r = np.array([2, 0, np.log(0.699 / 0.301)])
        a = np.array([2, 0, np.log(0.701 / 0.299)])
        self.assertTrue(compare(a, r, fixture, 0.02, True)["passed"])
        self.assertFalse(compare(a, r, fixture, 0.02, False)["passed"])
        fixture["classification_tasks"][0]["size"] = 3
        with self.assertRaises(ValueError):
            compare(a, r, fixture, 0.02, True)

    def test_entity_pool_order_is_not_a_public_decision(self):
        fixture = {
            "kind": "entities",
            "inputs": [{"shape": [1, 8]}, {}, {"shape": [1, 2]}, {}, {"shape": [1, 1]}],
            "outputs": [{"shape": [18]}],
            "candidate_indices_offset": 12,
        }
        ref = np.array([0] * 8 + [-5, 0, 2, 1, 0, 1, 1, 2, 1, 1], dtype=float)
        actual = np.array([0] * 8 + [-5, 0, 1, 2, 1, 2, 0, 1, 1, 1], dtype=float)
        self.assertTrue(compare(actual, ref, fixture, 0.02, True, {})["passed"])
        actual[-1] = 0
        self.assertFalse(compare(actual, ref, fixture, 0.02, True, {})["passed"])


if __name__ == "__main__":
    unittest.main()
