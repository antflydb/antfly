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

"""Tests for /api/models endpoint."""

import pytest

from .helpers import make_text_png_uri, make_wav_b64
from .models import DEFAULT_EXTRACTOR_MODEL, listed_model_name


CLIPCLAP_MODEL = "antflydb/clipclap"


def test_models_returns_json(api):
    resp = api.models()
    assert isinstance(resp, dict)


def test_models_has_expected_keys(api):
    resp = api.models()
    # At minimum, the response should contain category keys
    expected_keys = {
        "embedders",
        "rerankers",
        "chunkers",
        "generators",
        "extractors",
        "classifiers",
        "rewriters",
        "readers",
        "transcribers",
    }
    assert expected_keys.issubset(resp.keys()), (
        f"Missing keys: {expected_keys - resp.keys()}"
    )


def test_models_has_openai_data_field(api):
    resp = api.models()
    assert resp.get("object") == "list", resp
    data = resp.get("data")
    assert isinstance(data, list), resp
    for model in data:
        assert isinstance(model.get("id"), str), model
        assert model.get("object") == "model", model
        assert isinstance(model.get("created"), int), model
        assert model.get("owned_by") == "antfly", model


def test_models_exposes_gliner2_as_extractor(api):
    resp = api.models()
    model_name = listed_model_name(set(resp["extractors"]), DEFAULT_EXTRACTOR_MODEL)
    if model_name is not None:
        caps = resp["extractors"][model_name].get("capabilities", [])
        assert "extraction" in caps
        inputs = resp["extractors"][model_name].get("inputs", [])
        assert "text" in inputs


def test_models_exposes_nli_classifiers_as_extractors(api):
    resp = api.models()
    model = resp["extractors"].get("cross-encoder/nli-distilroberta-base")
    if model is None:
        pytest.skip("NLI classifier model is not available")
    caps = model.get("capabilities", [])
    assert {"classification", "zero_shot", "multi_label"}.issubset(caps)
    assert "cross-encoder/nli-distilroberta-base" not in resp["classifiers"]


def test_models_exposes_reader_inputs(api):
    resp = api.models()
    readers = resp.get("readers", {})
    model_name = listed_model_name(set(readers), "antflydb/florence-2-base")
    if model_name is not None:
        assert "image" in readers[model_name].get("inputs", [])


@pytest.mark.model_integration
def test_composite_model_eviction_churn_stays_healthy(api):
    """Audio-sidecar teardown and reader eviction must not corrupt the server."""

    listing = api.models()
    clipclap = listed_model_name(set(listing.get("embedders", {})), CLIPCLAP_MODEL)
    if clipclap is None:
        pytest.skip(f"{CLIPCLAP_MODEL} is not available")
    reader = next(
        (name for name in listing.get("readers", {}) if "florence" in name.lower()),
        None,
    )
    if reader is None:
        pytest.skip("No Florence reader model is available")

    audio = {
        "type": "media",
        "mime_type": "audio/wav",
        "data": make_wav_b64(0.1, 48_000),
    }
    image = make_text_png_uri(["INVOICE", "TOTAL 123"], scale=6, padding=12)

    # e2e-full runs with max_loaded_models=1, making each alternation evict the
    # previous composite model. Repeating the transition catches stale sidecar
    # handles and allocator damage at the operation that introduced it.
    for _ in range(3):
        embedded = api.embed(["a short silent audio clip", audio], model=clipclap)
        assert len(embedded["data"]) == 2
        assert all(len(item["embedding"]) == 512 for item in embedded["data"])

        read = api.read([image], model=reader)
        assert len(read["data"]) == 1
        assert isinstance(read["data"][0].get("text"), str)

    assert api.readyz().get("status") == "ready"
