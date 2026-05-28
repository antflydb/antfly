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


def test_models_returns_json(api):
    resp = api.models()
    assert isinstance(resp, dict)


def test_models_has_expected_keys(api):
    resp = api.models()
    # At minimum, the response should contain category keys
    expected_keys = {"embedders", "rerankers", "chunkers", "generators",
                     "recognizers", "extractors", "classifiers", "rewriters", "readers",
                     "transcribers"}
    assert expected_keys.issubset(resp.keys()), f"Missing keys: {expected_keys - resp.keys()}"


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
    if "fastino/gliner2-base-v1" in resp["recognizers"]:
        assert "fastino/gliner2-base-v1" in resp["extractors"]
        caps = resp["extractors"]["fastino/gliner2-base-v1"].get("capabilities", [])
        assert "extraction" in caps
        inputs = resp["extractors"]["fastino/gliner2-base-v1"].get("inputs", [])
        assert "text" in inputs


def test_models_exposes_reader_inputs(api):
    resp = api.models()
    readers = resp.get("readers", {})
    if "Xenova/trocr-base-printed" in readers:
        assert "image" in readers["Xenova/trocr-base-printed"].get("inputs", [])
