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

"""Fault qualification for acknowledged coverage across admission retries."""

import io
import json
import unittest
import urllib.error
from unittest.mock import patch

import qualify_chunk_load as load
import qualify_media_chunks as quality


def reply(start, stop, error=None, plan="plan"):
    return {
        "source_fingerprint": "source",
        "plan_id": plan,
        "model_identity": "model",
        "model_revision": "revision",
        "start_ordinal": start,
        "covered_through_ordinal": stop,
        "data": [{"descriptor": {"ordinal": value}} for value in range(start, stop)],
        "status": "failed" if error else "complete",
        "error": {"code": error} if error else None,
        "next_continuation": f"{plan}:{stop}" if error else None,
    }


class AdmissionRetryTests(unittest.TestCase):
    def test_retains_acknowledged_prefix_and_resumes_without_duplicates(self):
        replies = iter([reply(0, 2, "ResourceTemporarilyUnavailable"), reply(2, 4)])
        calls = []
        retries = []
        body = {"input": "immutable text", "stream": True}

        def request(url, value):
            calls.append(dict(value))
            return next(replies)

        with (
            patch.object(load, "chunk_response", request),
            patch.object(load.time, "sleep"),
        ):
            result = load.complete_request("local", body, retries.append)
        self.assertEqual(
            [row["descriptor"]["ordinal"] for row in result["data"]], [0, 1, 2, 3]
        )
        self.assertNotIn("continuation", calls[0])
        self.assertEqual(calls[1]["continuation"], "plan:2")
        self.assertNotIn("continuation", body)
        self.assertEqual(result["start_ordinal"], 0)
        self.assertEqual(retries, ["ResourceTemporarilyUnavailable"])

    def test_initial_admission_failure_retries_without_inventing_coverage(self):
        with (
            patch.object(
                load,
                "chunk_response",
                side_effect=[RuntimeError("chunk: HTTP 503: busy"), reply(0, 1)],
            ),
            patch.object(load.time, "sleep"),
        ):
            retries = []
            result = load.complete_request("local", {}, retries.append)
        self.assertEqual(result["covered_through_ordinal"], 1)
        self.assertEqual(retries, ["HTTP_ADMISSION"])

    def test_retry_cannot_mix_source_plan_or_model_generations(self):
        with (
            patch.object(
                load,
                "chunk_response",
                side_effect=[
                    reply(0, 1, "SharedAdmissionExceeded"),
                    reply(1, 2, plan="changed"),
                ],
            ),
            patch.object(load.time, "sleep"),
        ):
            with self.assertRaisesRegex(AssertionError, "immutable execution context"):
                load.complete_request("local", {}, lambda _: None)

    def test_non_admission_failures_remain_failures(self):
        with patch.object(
            load,
            "chunk_response",
            return_value=reply(0, 0, "EmbeddingIdentityMismatch"),
        ) as request:
            with self.assertRaises(AssertionError):
                load.complete_request(
                    "local", {}, lambda _: self.fail("must not retry")
                )
        self.assertEqual(request.call_count, 1)

    def test_repeated_denial_exhausts_a_bounded_retry_budget(self):
        with (
            patch.object(
                load,
                "chunk_response",
                return_value=reply(0, 0, "SharedAdmissionExceeded"),
            ) as request,
            patch.object(load.time, "sleep"),
        ):
            with self.assertRaisesRegex(AssertionError, "retry budget exhausted"):
                load.complete_request("local", {}, lambda _: None)
        self.assertEqual(request.call_count, 65)


class PreResponseAdmissionTests(unittest.TestCase):
    @staticmethod
    def busy(retryable=True):
        return urllib.error.HTTPError(
            "http://local/ai/v1/embed",
            503,
            "busy",
            {},
            io.BytesIO(
                json.dumps({"retryable": retryable, "retry_after_ms": 250}).encode()
            ),
        )

    def test_retries_explicit_admission_with_the_same_immutable_request(self):
        response = io.BytesIO(b"{}")
        with (
            patch.object(
                quality.urllib.request, "urlopen", side_effect=[self.busy(), response]
            ) as opened,
            patch.object(quality.time, "sleep") as sleep,
        ):
            self.assertIs(
                response,
                quality.post("http://local", "embed", {"model_identity": "pinned"}),
            )
            self.assertEqual(opened.call_count, 2)
            self.assertIs(
                opened.call_args_list[0].args[0], opened.call_args_list[1].args[0]
            )
            sleep.assert_called_once_with(0.25)

    def test_permanent_errors_fail_without_retrying(self):
        with (
            patch.object(
                quality.urllib.request, "urlopen", side_effect=self.busy(False)
            ) as opened,
            patch.object(quality.time, "sleep") as sleep,
        ):
            with self.assertRaisesRegex(RuntimeError, "HTTP 503"):
                quality.post("http://local", "embed", {})
            self.assertEqual(opened.call_count, 1)
            sleep.assert_not_called()

    def test_non_retryable_http_error_is_terminal_across_chunk_resumption(self):
        with (
            patch.object(
                load,
                "chunk_response",
                side_effect=quality.HttpError("chunk: HTTP 503: permanent", False),
            ) as request,
            patch.object(load.time, "sleep") as sleep,
        ):
            with self.assertRaisesRegex(RuntimeError, "permanent"):
                load.complete_request(
                    "http://local", {}, lambda _: self.fail("retried permanent error")
                )
            self.assertEqual(request.call_count, 1)
            sleep.assert_not_called()

    def test_retry_budget_is_bounded(self):
        with (
            patch.object(
                quality.urllib.request,
                "urlopen",
                side_effect=[self.busy() for _ in range(4)],
            ) as opened,
            patch.object(quality.time, "sleep") as sleep,
        ):
            with self.assertRaisesRegex(RuntimeError, "HTTP 503"):
                quality.post("http://local", "embed", {})
            self.assertEqual(opened.call_count, 4)
            self.assertEqual(sleep.call_count, 3)


if __name__ == "__main__":
    unittest.main()
