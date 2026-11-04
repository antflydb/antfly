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

"""Opt-in BF16 Gemma CPU admission regression, with default inference budgets.

Run against a native server with an 8 or 14 GiB process/runtime envelope and
local google/embeddinggemma-2 assets. Set ANTFLY_INFERENCE_CPU_ADMISSION=1 and
ANTFLY_INFERENCE_URL. The dedicated opt-in avoids pulling a large model or
applying the default CPU policy expectations to unrelated accelerator suites.
"""

from concurrent.futures import ThreadPoolExecutor
import math
import os

import pytest
import requests

pytestmark = [
    pytest.mark.model_integration,
    pytest.mark.skipif(
        os.environ.get("ANTFLY_INFERENCE_CPU_ADMISSION") != "1",
        reason="requires the explicit native/default-budget qualification envelope",
    ),
]
MODEL = "google/embeddinggemma-2"


def _input(words, grouped):
    text = "hello " * words
    return [{"content": [{"type": "text", "text": text}]}] if grouped else [text]


def _post(api, inputs):
    # Do not use the fixture's capacity retries: self-contention must fail this
    # regression on its first response, even if a later retry might succeed.
    return requests.post(
        f"{api.url}/ai/v1/embed",
        json={"model": MODEL, "input": inputs},
        headers=dict(api.s.headers),
        timeout=120,
    )


def _vectors(response, count):
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["usage"]["prompt_tokens"] > 0
    assert len(body["data"]) == count
    assert [item["index"] for item in body["data"]] == list(range(count))
    vectors = [item["embedding"] for item in body["data"]]
    for vector in vectors:
        assert len(vector) == 768
        assert all(math.isfinite(value) for value in vector)
        assert abs(sum(value * value for value in vector) - 1) < 0.01
    return vectors


@pytest.mark.parametrize("words", [1, 128, 256, 384])
@pytest.mark.parametrize("grouped", [False, True])
def test_cpu_admission_text_lengths(api, words, grouped):
    _vectors(_post(api, _input(words, grouped)), 1)


@pytest.mark.parametrize("grouped", [False, True])
def test_cpu_admission_retains_outputs_between_cohorts(api, grouped):
    words = ["hello", "world", "good", "bad", "yes", "no", "one", "two"]
    # Deliberately shuffled lengths force flat execution to reorder length
    # buckets and restore input positions across multiple encoder cohorts.
    lengths = [256, 16, 192, 48, 128, 8, 224, 80]
    texts = [(word + " ") * length for word, length in zip(words, lengths)]
    inputs = texts
    if grouped:
        # The public grouped HTTP route dispatches to embedCached directly.
        # Multiple text parts keep this regression explicit about that path,
        # including its incremental preparation and exact-token run admission.
        inputs = [
            {
                "content": [
                    {"type": "text", "text": text[: len(text) // 2]},
                    {"type": "text", "text": text[len(text) // 2 :]},
                ]
            }
            for text in texts
        ]
    vectors = _vectors(_post(api, inputs), len(inputs))
    # Independent calls catch aliasing, later-cohort overwrites and misplaced
    # rows. Grouped references preserve the same multipart rendering semantics.
    for item, vector in zip(inputs, vectors):
        reference = _vectors(_post(api, [item]), 1)[0]
        assert max(abs(a - b) for a, b in zip(reference, vector)) < 1e-5


def test_cpu_admission_concurrent_requests_recover_after_contention(api):
    # Warm the model before competing for the execution gate.
    _vectors(_post(api, _input(256, False)), 1)
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [
            pool.submit(_post, api, _input(256, grouped))
            for grouped in [False, True] * 2
        ]
        responses = [future.result() for future in futures]
    completed = 0
    deferred = []
    for grouped, response in zip([False, True] * 2, responses):
        if response.status_code == 503:
            body = response.json()
            assert body["error"] == "MODEL_RESOURCE_BUSY", response.text
            assert body["retryable"] is True
            assert body["retry_after_ms"] > 0
            deferred.append(grouped)
        else:
            _vectors(response, 1)
            completed += 1
    assert completed > 0, "concurrent callers made no progress"
    # All competing requests have finished. One explicit serial attempt must
    # recover; this does not use the fixture's automatic capacity retry loop.
    for grouped in deferred:
        _vectors(_post(api, _input(256, grouped)), 1)


@pytest.mark.parametrize("grouped", [False, True])
def test_cpu_admission_oversized_request_is_permanent(api, grouped):
    response = _post(api, _input(4096, grouped))
    assert response.status_code == 400, response.text
    assert response.json()["error"] == "MODEL_RESOURCE_LIMIT"
    assert response.json()["retryable"] is False
