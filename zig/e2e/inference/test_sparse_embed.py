# Copyright 2026 Antfly, Inc.
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

"""Tests for sparse embedding via /embed with SPLADE-style models.

Matches Go antfly's sparse_embedders_test.go patterns.
Requires a model with "sparse" capability.
"""

import pytest
from .helpers import sparse_dot_product

pytestmark = pytest.mark.model_integration

SPARSE_MODEL = "sparse-encoder-testing/splade-bert-tiny-nq-onnx"


def assert_sparse_close(a, b, tol=1e-2):
    assert a["indices"] == b["indices"], "Repeated text should activate the same sparse indices"
    assert len(a["values"]) == len(b["values"])
    max_delta = max(abs(av - bv) for av, bv in zip(a["values"], b["values"]))
    assert max_delta <= tol, f"Repeated text values differ by {max_delta:.6f}"


@pytest.fixture
def sparse_resp(api):
    """Embed a single text using the sparse model."""
    r = api.post("/embed", json={"model": SPARSE_MODEL, "input": "machine learning"})
    if r.status_code == 404:
        pytest.skip(f"Sparse model {SPARSE_MODEL} not available")
    r.raise_for_status()
    return r.json()


def test_sparse_response_structure(sparse_resp):
    """Response should return a sparse vector in data[i].embedding."""
    assert "data" in sparse_resp, f"Expected data key, got: {list(sparse_resp.keys())}"
    svs = [item["embedding"] for item in sparse_resp["data"]]
    assert len(svs) == 1

    sv = svs[0]
    assert "indices" in sv
    assert "values" in sv
    assert len(sv["indices"]) == len(sv["values"])
    assert len(sv["indices"]) > 0, "Sparse vector should have non-zero entries"


def test_sparse_usage_counts_tokens(sparse_resp):
    """Sparse embedding usage should count tokenizer tokens, not input rows."""
    usage = sparse_resp["usage"]
    assert isinstance(usage["prompt_tokens"], int)
    assert usage["prompt_tokens"] > 1
    assert usage["total_tokens"] == usage["prompt_tokens"]


def test_sparse_indices_sorted(sparse_resp):
    """Indices should be sorted ascending."""
    sv = sparse_resp["data"][0]["embedding"]
    for i in range(1, len(sv["indices"])):
        assert sv["indices"][i] > sv["indices"][i - 1], "Indices should be sorted ascending"


def test_sparse_values_positive(sparse_resp):
    """All emitted sparse values should be positive."""
    sv = sparse_resp["data"][0]["embedding"]
    for v in sv["values"]:
        assert v > 0, f"Sparse value should be positive, got {v}"


def test_sparse_batch_is_deterministic_and_text_sensitive(api):
    """Repeated text should be stable, and clear related texts should rank above unrelated texts."""
    texts = [
        "machine learning model",
        "machine learning model",
        "machine learning algorithms",
        "training a learning model",
        "banana bread recipe",
        "garden tomato plants",
        "weather forecast rain",
        "tomorrow rain weather forecast",
        "neural network training",
    ]
    r = api.post("/embed", json={
        "model": SPARSE_MODEL,
        "input": texts,
    })
    if r.status_code == 404:
        pytest.skip(f"Sparse model {SPARSE_MODEL} not available")
    r.raise_for_status()
    resp = r.json()

    svs = [item["embedding"] for item in resp["data"]]
    assert len(svs) == len(texts)

    assert_sparse_close(svs[0], svs[1])

    ml_related = [
        sparse_dot_product(svs[0], svs[2]),
        sparse_dot_product(svs[0], svs[3]),
    ]
    ml_unrelated = [
        sparse_dot_product(svs[0], svs[4]),
        sparse_dot_product(svs[0], svs[5]),
    ]
    assert min(ml_related) > max(ml_unrelated), (
        f"machine-learning related scores ({ml_related}) should exceed unrelated scores ({ml_unrelated})"
    )

    weather_related = sparse_dot_product(svs[6], svs[7])
    weather_unrelated = sparse_dot_product(svs[6], svs[8])
    assert weather_related > weather_unrelated, (
        f"weather related score ({weather_related:.2f}) should exceed unrelated score ({weather_unrelated:.2f})"
    )


def test_sparse_batch(api):
    """Batch inference should return one sparse vector per input."""
    texts = ["hello world", "foo bar", "machine learning"]
    r = api.post("/embed", json={"model": SPARSE_MODEL, "input": texts})
    if r.status_code == 404:
        pytest.skip(f"Sparse model {SPARSE_MODEL} not available")
    r.raise_for_status()
    resp = r.json()

    svs = [item["embedding"] for item in resp["data"]]
    assert len(svs) == len(texts)
    for sv in svs:
        assert len(sv["indices"]) > 0
