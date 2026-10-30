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

import contextlib
import io
import unittest
from unittest.mock import patch
from qualify_reader_generator_pools import Probe, main


class ProbeAssertionsTest(unittest.TestCase):
    def test_cli_qualifies_requested_reader_ceiling_and_its_exact_rejection(self):
        for ceiling in (512, 2048):
            with (
                patch(
                    "sys.argv",
                    [
                        "probe",
                        "--reader-url",
                        "http://example.invalid",
                        "--reader-max-output-tokens",
                        str(ceiling),
                        "--report",
                        "unused.json",
                    ],
                ),
                patch("pathlib.Path.read_bytes", return_value=b"fixture"),
                patch("pathlib.Path.write_text"),
                patch.object(Probe, "accepted", return_value={}) as accepted,
                patch.object(Probe, "rejected", return_value={}) as rejected,
                patch.object(Probe, "overload", return_value={}),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(0, main())
                self.assertEqual(4, accepted.call_count)
                self.assertTrue(
                    all(
                        call.args[2]["max_tokens"] == ceiling
                        for call in accepted.call_args_list
                    )
                )
                self.assertEqual(
                    ceiling + 1, rejected.call_args_list[0].args[2]["max_tokens"]
                )
                self.assertEqual(
                    "OUTPUT_TOKEN_LIMIT_EXCEEDED", rejected.call_args_list[0].args[3]
                )

    def test_reader_qualification_requires_actual_recurrent_placement(self):
        probe = Probe(1)
        for metrics in [
            "antfly_inference_models_loaded 1\n",
            "antfly_inference_recurrent_cuda_models 0\nantfly_inference_recurrent_host_models 1\n",
            "antfly_inference_recurrent_cuda_models 1\nantfly_inference_recurrent_host_models 1\n",
            "antfly_inference_recurrent_cuda_models 1\nantfly_inference_recurrent_cuda_models 1\nantfly_inference_recurrent_host_models 0\n",
            "antfly_inference_recurrent_cuda_models NaN\nantfly_inference_recurrent_host_models 0\n",
        ]:
            with patch.object(probe, "request", return_value=(200, metrics)):
                with self.assertRaises(AssertionError):
                    probe.recurrent_placement("")
        with patch.object(
            probe,
            "request",
            return_value=(
                200,
                "# TYPE antfly_inference_recurrent_cuda_models gauge\nantfly_inference_recurrent_cuda_models 1\nantfly_inference_recurrent_host_models 0\n",
            ),
        ):
            self.assertEqual({"cuda": 1, "host": 0}, probe.recurrent_placement(""))

    def test_cuda_reader_instance_does_not_substitute_for_placement(self):
        probe = Probe(1)
        with patch.object(
            probe,
            "request",
            side_effect=[
                (200, {"data": [{"text": "invoice"}]}),
                (200, {"data": [{"model": "reader", "backend": "cuda"}]}),
                (
                    200,
                    "antfly_inference_recurrent_cuda_models 0\nantfly_inference_recurrent_host_models 1\n",
                ),
            ],
        ):
            with self.assertRaises(AssertionError):
                probe.accepted("", "/read", {"model": "reader"}, ["invoice"])

    def test_policy_rejection_requires_the_exact_code(self):
        probe = Probe(1)
        for response in [
            (400, {"error": "INVALID_REQUEST"}),
            (413, {"error": "MODEL_RESOURCE_LIMIT"}),
            (413, {"error": "INPUT_TOKEN_LIMIT_EXCEEDED", "retryable": False}),
        ]:
            with patch.object(probe, "request", return_value=response):
                with self.assertRaises(AssertionError):
                    probe.rejected("", "/read", {}, "OUTPUT_TOKEN_LIMIT_EXCEEDED")
        with patch.object(
            probe,
            "request",
            return_value=(
                413,
                {"error": "OUTPUT_TOKEN_LIMIT_EXCEEDED", "retryable": False},
            ),
        ):
            probe.rejected("", "/read", {}, "OUTPUT_TOKEN_LIMIT_EXCEEDED")

    def test_overload_requires_concurrency_reason_and_valid_success(self):
        probe = Probe(1)
        valid = {
            "choices": [{"message": {"content": "READY"}, "finish_reason": "stop"}]
        }
        correct = {
            "error": "SERVICE_UNAVAILABLE",
            "reason": "inference_admission",
            "retryable": True,
        }
        for success, rejection in [
            (valid, {"error": "MODEL_RESOURCE_BUSY"}),
            ({"choices": []}, correct),
            ({"choices": [{"message": {"content": "wrong"}}]}, correct),
        ]:
            with patch.object(
                probe,
                "request",
                side_effect=[(200, success), (503, rejection), (200, {})],
            ):
                with self.assertRaises((AssertionError, IndexError)):
                    probe.overload("", "/generate", {})
        with patch.object(
            probe, "request", side_effect=[(200, valid), (503, correct), (200, {})]
        ):
            probe.overload("", "/generate", {})


if __name__ == "__main__":
    unittest.main()
