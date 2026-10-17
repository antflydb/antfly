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

"""Bounded admission retries retaining acknowledged chunk coverage."""

import time


class HttpError(RuntimeError):
    def __init__(self, message, retryable):
        super().__init__(message)
        self.retryable = retryable


def complete_request(url, body, request_chunk, on_retry):
    # Retry only explicit admission failures. A terminal coverage record
    # tells us exactly which acknowledged ordinals to preserve on resume.
    retryable = {
        "ResourceTemporarilyUnavailable",
        "ConcurrencyUnavailable",
        "InferenceAdmissionQueueFull",
        "QueueFull",
        "SharedAdmissionExceeded",
        "MemoryBudgetExceeded",
    }
    request = dict(body)
    acknowledged = []
    context = None
    first_ordinal = None
    deadline = time.monotonic() + 180
    for attempt in range(65):
        result = None
        try:
            result = request_chunk(url, request)
        except RuntimeError as error:
            if not getattr(error, "retryable", True) or not any(
                status in str(error) for status in ("HTTP 503:", "HTTP 429:")
            ):
                raise
            code = "HTTP_ADMISSION"
        else:
            identity = tuple(
                result[key]
                for key in (
                    "source_fingerprint",
                    "plan_id",
                    "model_identity",
                    "model_revision",
                )
            )
            if context is None:
                context = identity
                first_ordinal = result["start_ordinal"]
            assert identity == context, "retry changed immutable execution context"
            assert result["start_ordinal"] == first_ordinal + len(acknowledged)
            for index, item in enumerate(
                result["data"], first_ordinal + len(acknowledged)
            ):
                assert item["descriptor"]["ordinal"] == index
                assert not item.get("error")
            acknowledged.extend(result["data"])
            assert result["covered_through_ordinal"] == first_ordinal + len(
                acknowledged
            )
            error = result.get("error")
            if not error:
                result["data"] = acknowledged
                result["start_ordinal"] = first_ordinal
                return result
            code = error["code"]
            if code not in retryable or not result.get("next_continuation"):
                raise AssertionError(result)
            request["continuation"] = result["next_continuation"]
        if attempt == 64 or time.monotonic() >= deadline:
            raise AssertionError(f"admission retry budget exhausted: {code}")
        on_retry(code)
        time.sleep(min(1.0, 0.02 * 2 ** min(attempt, 6)))
    raise AssertionError("unreachable retry loop")
