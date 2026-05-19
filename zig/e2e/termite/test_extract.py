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

"""Tests for /api/extract (structured extraction) endpoint."""

import pytest
from .helpers import TINY_PNG_URI, assert_openai_list_response, make_text_png_uri

pytestmark = pytest.mark.model_integration

GLINER_MODEL = "fastino/gliner2-base-v1"


def _has_reader_model(api) -> bool:
    readers = api.models().get("readers", {})
    if not readers:
        return False

    model = next(iter(readers))
    resp = api.post(
        "/read",
        json={"model": model, "images": [{"url": TINY_PNG_URI}]},
    )
    if resp.status_code == 500 and resp.headers.get("content-type", "").startswith("application/json"):
        payload = resp.json()
        if payload.get("error") in {"MODEL_LOAD_FAILED", "INFERENCE_FAILED"} and payload.get("message") in {
            "MissingWeight",
            "ShapeMismatch",
            "UnsupportedShape",
        }:
            return False
    return resp.ok


def test_extract_basic(api):
    """Extract structured fields with instance-shaped results."""
    resp = api.extract(
        texts=["John Smith works at Google in Mountain View."],
        schema={"person": ["name::str", "company::str", "location::str"]},
        model=GLINER_MODEL,
    )
    assert_openai_list_response(resp, expected_len=1)
    results = resp["data"]
    assert len(results) == 1

    result = results[0]["results"]
    assert "person" in result
    assert isinstance(result["person"], list)
    if result["person"]:
        instance = result["person"][0]
        assert isinstance(instance, dict)
        for value in instance.values():
            if isinstance(value, list):
                assert all(isinstance(item, dict) and "value" in item for item in value)
            else:
                assert isinstance(value, dict)
                assert "value" in value


def test_extract_multiple_texts(api):
    """Extract from multiple texts."""
    resp = api.extract(
        texts=[
            "Alice works at Microsoft.",
            "Bob lives in London.",
        ],
        schema={
            "person": ["name::str"],
            "organization": ["company::str"],
            "location": ["city::str"],
        },
        model=GLINER_MODEL,
    )
    results = resp["data"]
    assert len(results) == 2


def test_extract_include_confidence_and_spans(api):
    """Optional field metadata is emitted only when requested."""
    resp = api.extract(
        texts=["Alice works at Microsoft in London."],
        schema={"person": ["name::str", "company::str", "city::str"]},
        model=GLINER_MODEL,
        include_confidence=True,
        include_spans=True,
    )
    person_instances = resp["data"][0]["results"]["person"]
    if person_instances:
        for value in person_instances[0].values():
            values = value if isinstance(value, list) else [value]
            for item in values:
                assert "value" in item
                assert "score" in item
                assert "start" in item
                assert "end" in item


def test_extract_from_images_via_reader(api):
    """Image-backed extraction should route through /read before schema extraction."""
    if not _has_reader_model(api):
        pytest.skip("No reader model is available for image-backed extraction")

    test_image = make_text_png_uri(
        [
            "JOHN SMITH",
            "GOOGLE",
        ],
        scale=8,
        padding=18,
        line_gap=12,
    )

    resp = api.extract(
        images=[test_image],
        schema={"person": ["name::str", "company::str"]},
        model=GLINER_MODEL,
    )
    results = resp["data"]
    assert len(results) == 1
    assert "person" in results[0]["results"]
    assert isinstance(results[0]["results"]["person"], list)


def test_extract_rejects_missing_texts_and_images(api):
    """The server should require exactly one extraction input source."""
    resp = api.post(
        "/extract",
        json={
            "model": GLINER_MODEL,
            "schema": {"person": ["name::str"]},
        },
    )
    assert resp.status_code == 400
    body = resp.json()
    assert body["error"] == "INVALID_REQUEST"
    assert "exactly one of texts or images" in body["message"]


def test_extract_rejects_both_texts_and_images(api):
    """The server should reject ambiguous mixed extraction input."""
    test_image = make_text_png_uri(["JOHN SMITH"], scale=6, padding=12, line_gap=8)
    resp = api.post(
        "/extract",
        json={
            "model": GLINER_MODEL,
            "texts": ["John Smith works at Google."],
            "images": [{"url": test_image}],
            "schema": {"person": ["name::str"]},
        },
    )
    assert resp.status_code == 400
    body = resp.json()
    assert body["error"] == "INVALID_REQUEST"
    assert "exactly one of texts or images" in body["message"]
