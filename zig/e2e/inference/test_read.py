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

"""Tests for /api/read (document reading / OCR) endpoint.

Matches Go termite's reader_test.go patterns.
"""

import json
import os
from pathlib import Path

import pytest
from .helpers import TINY_PNG_URI, assert_openai_list_response, load_go_sample_page_fixture, make_text_png_uri

pytestmark = pytest.mark.model_integration

_SURYA_LOCAL_EXPECTATIONS = Path(__file__).with_name("testdata") / "surya_expectations.local.json"


def _assert_read_result_shape(result: dict):
    assert "text" in result
    assert isinstance(result["text"], str)

    if "fields" in result:
        assert isinstance(result["fields"], dict)
        for key, value in result["fields"].items():
            assert isinstance(key, str)
            assert isinstance(value, str)

    if "regions" in result:
        assert isinstance(result["regions"], list)
        for region in result["regions"]:
            assert isinstance(region, dict)
            assert "text" in region
            assert isinstance(region["text"], str)
            assert "bbox" in region
            assert isinstance(region["bbox"], list)
            assert len(region["bbox"]) == 4
            for coord in region["bbox"]:
                assert isinstance(coord, (int, float))
            if "confidence" in region:
                assert isinstance(region["confidence"], (int, float))
            if "label" in region:
                assert isinstance(region["label"], str)


def _find_reader_model(api, needle: str) -> str | None:
    readers = api.models().get("readers", {})
    for name in readers:
        if needle in name.lower():
            return name
    return None


def _find_multistage_reader_model(api) -> str | None:
    override = (
        os.environ.get("TERMITE_MULTISTAGE_READER_MODEL")
        or os.environ.get("TERMITE_PADDLEOCR_MODEL")
    )
    if override:
        return override

    for needle in ("paddleocr", "paddle", "surya"):
        model = _find_reader_model(api, needle)
        if model:
            return model
    return None


def _find_surya_reader_model(api) -> str | None:
    override = os.environ.get("TERMITE_SURYA_READER_MODEL") or os.environ.get("TERMITE_SURYA_MODEL")
    if override:
        return override
    return _find_reader_model(api, "surya")


def _api_backend_enabled(api, name: str) -> bool | None:
    version = api.version()
    backends = version.get("backends")
    if not isinstance(backends, dict):
        return None
    enabled = backends.get(name)
    if not isinstance(enabled, bool):
        return None
    return enabled


def _env_csv(name: str) -> list[str]:
    raw = os.environ.get(name, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def _normalize_text(value: str) -> str:
    return " ".join(value.split()).lower()


def _donut_docvqa_prompt(question: str) -> str:
    return f"<s_docvqa><s_question>{question}</s_question><s_answer>"


def _assert_text_contains_any(observed_text: str, env_name: str) -> None:
    expected = _env_csv(env_name)
    if not expected:
        return
    normalized = _normalize_text(observed_text)
    assert any(_normalize_text(item) in normalized for item in expected)


def _assert_known_model_output(model: str, observed_text: str, fixture_present: bool) -> None:
    normalized_model = model.strip().lower()
    normalized_text = _normalize_text(observed_text)

    if fixture_present and normalized_model == "xenova/donut-base-finetuned-cord-v2":
        assert normalized_text == "box"
    elif fixture_present and normalized_model == "xenova/pix2struct-docvqa-base":
        assert "text box" in normalized_text


def _assert_fields_include_any(observed_fields: dict[str, str], env_name: str) -> None:
    expected = _env_csv(env_name)
    if not expected:
        return
    lowered = {key.strip().lower() for key in observed_fields}
    assert any(item.strip().lower() in lowered for item in expected)


def _load_surya_expectations() -> dict:
    path = os.environ.get("TERMITE_SURYA_EXPECTATIONS_JSON", "").strip()
    if path:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    if _SURYA_LOCAL_EXPECTATIONS.exists():
        return json.loads(_SURYA_LOCAL_EXPECTATIONS.read_text(encoding="utf-8"))
    return {}


def _maybe_write_surya_expectations(result: dict, regions: list[dict], labels: list[str]) -> None:
    path = os.environ.get("TERMITE_SURYA_WRITE_EXPECTATIONS_JSON", "").strip()
    if not path:
        return

    payload = {
        "require_labels": bool(labels),
        "text_any": [result["text"]] if result.get("text") else [],
        "labels_any": sorted({label for label in labels if label}),
        "region_texts": [region["text"] for region in regions if region["text"].strip()],
        "region_labels": [region.get("label", "") for region in regions],
    }
    Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_or_skip_backend_unavailable(api, *, images, model: str = "", prompt: str = "", **kwargs):
    image_objs = [{"url": img} if isinstance(img, str) else img for img in images]
    body = {"model": model, "images": image_objs, "prompt": prompt, **kwargs}
    resp = api.post("/read", json=body)

    if resp.status_code == 500 and resp.headers.get("content-type", "").startswith("application/json"):
        payload = resp.json()
        if payload.get("error") == "MODEL_LOAD_FAILED" and payload.get("message") == "NoBackendAvailable":
            pytest.skip("Reader model is installed, but this termite runtime was built without a compatible backend")
        if payload.get("error") in {"MODEL_LOAD_FAILED", "INFERENCE_FAILED"} and payload.get("message") in {
            "MissingWeight",
            "ShapeMismatch",
            "UnsupportedShape",
        }:
            pytest.skip("Reader model is installed, but this termite graph runtime does not support it yet")

    resp.raise_for_status()
    return resp.json()


@pytest.mark.multimodal
def test_read_image(api):
    """Reading an image should return some text output."""
    resp = api.read(images=[TINY_PNG_URI])
    assert_openai_list_response(resp, expected_len=1)
    results = resp["data"]
    assert len(results) == 1
    _assert_read_result_shape(results[0])


@pytest.mark.multimodal
def test_read_with_prompt(api):
    """Reading with a prompt should return text."""
    resp = api.read(images=[TINY_PNG_URI], prompt="Describe this image")
    results = resp["data"]
    assert len(results) == 1
    _assert_read_result_shape(results[0])


@pytest.mark.multimodal
def test_read_trocr_model_answers_text(api):
    """TrOCR-style readers should load explicitly and return text."""
    model = os.environ.get("TERMITE_TROCR_MODEL") or _find_reader_model(api, "trocr")
    if not model:
        pytest.skip("No TrOCR reader model is available")

    test_image = make_text_png_uri(
        [
            "INVOICE",
            "TOTAL 123",
        ],
        scale=8,
        padding=18,
        line_gap=12,
    )

    resp = _read_or_skip_backend_unavailable(
        api,
        images=[test_image],
        model=model,
        max_tokens=64,
    )
    results = resp["data"]
    assert len(results) == 1
    _assert_read_result_shape(results[0])
    assert results[0]["text"].strip()


@pytest.mark.multimodal
def test_read_donut_model_exposes_optional_fields(api):
    """Donut-family readers should round-trip through the richer result shape."""
    model = os.environ.get("TERMITE_DONUT_MODEL") or _find_reader_model(api, "donut")
    if not model:
        pytest.skip("No Donut reader model is available")

    fixture = load_go_sample_page_fixture()
    if fixture is not None:
        image, _phrases = fixture
    else:
        image = TINY_PNG_URI
    prompt = _donut_docvqa_prompt("What is the document type?") if fixture else "<s_cord-v2>"

    resp = api.read(images=[image], model=model, prompt=prompt)
    results = resp["data"]
    assert len(results) == 1
    _assert_read_result_shape(results[0])
    assert isinstance(results[0]["text"], str)
    assert results[0]["text"].strip()
    _assert_known_model_output(model, results[0]["text"], fixture is not None)
    _assert_text_contains_any(results[0]["text"], "TERMITE_DONUT_EXPECT_TEXT_ANY")

    # Donut readers may emit structured fields on some inputs; when present they must
    # use the flattened object shape expected by the Go API.
    if "fields" in results[0]:
        assert isinstance(results[0]["fields"], dict)
        _assert_fields_include_any(results[0]["fields"], "TERMITE_DONUT_EXPECT_FIELDS_ANY")


@pytest.mark.multimodal
def test_read_multistage_model_round_trips_richer_shape(api):
    """Multi-stage OCR readers should load through /api/read and return the richer result shape."""
    model = _find_multistage_reader_model(api)
    if not model:
        pytest.skip(
            "No multistage OCR reader model is available; set TERMITE_MULTISTAGE_READER_MODEL or install a Paddle/Surya reader"
        )

    if _api_backend_enabled(api, "onnx_runtime") is False:
        pytest.skip("Multi-stage OCR e2e requires a termite binary built with onnx=true")

    test_image = make_text_png_uri(
        [
            "INVOICE 42",
            "TOTAL 123",
        ],
        scale=8,
        padding=18,
        line_gap=12,
    )

    resp = _read_or_skip_backend_unavailable(api, images=[test_image], model=model)
    results = resp["data"]
    assert len(results) == 1
    _assert_read_result_shape(results[0])

    regions = results[0].get("regions", [])
    assert len(regions) >= 1
    assert any(region["text"].strip() for region in regions)

    if len(regions) >= 2:
        assert regions[0]["bbox"][1] <= regions[1]["bbox"][1]

    # The installed PaddleOCR fixture should be stable enough for a stronger assertion.
    if model == "monkt/paddleocr-onnx":
        assert results[0]["text"] == "HMORCELA2\nTOTAL128"
        assert [region["text"] for region in regions] == ["HMORCELA2", "TOTAL128"]
        assert len(regions) == 2


@pytest.mark.multimodal
def test_read_surya_model_round_trips_regions(api):
    """Surya-style readers should exercise the Vision2Seq multistage path and return OCR regions."""
    model = _find_surya_reader_model(api)
    if not model:
        pytest.skip("No Surya reader model is available")

    if _api_backend_enabled(api, "onnx_runtime") is False:
        pytest.skip("Surya e2e requires a termite binary built with onnx=true")

    fixture = load_go_sample_page_fixture()
    if fixture is not None:
        test_image, expected_phrases = fixture
    else:
        test_image = make_text_png_uri(
            [
                "RECEIPT 42",
                "TOTAL 123",
            ],
            scale=8,
            padding=18,
            line_gap=12,
        )
        expected_phrases = []

    resp = _read_or_skip_backend_unavailable(api, images=[test_image], model=model)
    results = resp["data"]
    assert len(results) == 1
    _assert_read_result_shape(results[0])

    regions = results[0].get("regions", [])
    assert len(regions) >= 1
    assert any(region["text"].strip() for region in regions)

    labels = [region["label"] for region in regions if "label" in region]
    assert all(isinstance(label, str) and label for label in labels)
    expectations = _load_surya_expectations()
    _maybe_write_surya_expectations(results[0], regions, labels)

    require_labels = expectations.get("require_labels")
    if require_labels is None:
        require_labels = os.environ.get("TERMITE_SURYA_REQUIRE_LABELS") == "1"
    if require_labels:
        assert labels, "Surya model was expected to emit layout labels"

    if expected_phrases:
        normalized_text = _normalize_text(results[0]["text"])
        assert any(phrase.lower() in normalized_text for phrase in expected_phrases[:2])

    extra_expected_phrases = expectations.get("text_any") or _env_csv("TERMITE_SURYA_EXPECT_TEXT_ANY")
    if extra_expected_phrases:
        normalized_text = _normalize_text(results[0]["text"])
        assert any(phrase.lower() in normalized_text for phrase in extra_expected_phrases)

    expected_labels = {label.lower() for label in (expectations.get("labels_any") or _env_csv("TERMITE_SURYA_EXPECT_LABELS_ANY"))}
    if expected_labels:
        observed_labels = {label.lower() for label in labels}
        assert observed_labels & expected_labels

    expected_region_texts = expectations.get("region_texts") or _env_csv("TERMITE_SURYA_EXPECT_REGION_TEXTS")
    if expected_region_texts:
        observed_region_texts = [_normalize_text(region["text"]) for region in regions if region["text"].strip()]
        assert observed_region_texts[: len(expected_region_texts)] == [
            _normalize_text(text) for text in expected_region_texts
        ]

    expected_region_labels = expectations.get("region_labels") or _env_csv("TERMITE_SURYA_EXPECT_REGION_LABELS")
    if expected_region_labels:
        observed_region_labels = [str(region.get("label", "")).strip().lower() for region in regions]
        assert observed_region_labels[: len(expected_region_labels)] == [
            label.strip().lower() for label in expected_region_labels
        ]


@pytest.mark.multimodal
def test_read_moondream_model_exposes_optional_fields(api):
    """Moondream-style reader models should return text and may expose structured fields."""
    model = os.environ.get("TERMITE_MOONDREAM_MODEL") or _find_reader_model(api, "moondream")
    if not model:
        pytest.skip("No Moondream reader model is available")

    if _api_backend_enabled(api, "onnx_runtime") is False:
        pytest.skip("Moondream e2e requires a termite binary built with onnx=true")

    image = make_text_png_uri(["HELLO"], scale=10)

    resp = _read_or_skip_backend_unavailable(
        api,
        images=[image],
        model=model,
        prompt="Describe this image.",
    )
    results = resp["data"]
    assert len(results) == 1
    _assert_read_result_shape(results[0])
    assert isinstance(results[0]["text"], str)
    assert results[0]["text"].strip()
    _assert_text_contains_any(results[0]["text"], "TERMITE_MOONDREAM_EXPECT_TEXT_ANY")

    if "fields" in results[0]:
        assert isinstance(results[0]["fields"], dict)
        _assert_fields_include_any(results[0]["fields"], "TERMITE_MOONDREAM_EXPECT_FIELDS_ANY")


@pytest.mark.multimodal
def test_read_pix2struct_model_answers_prompt(api):
    """Pix2Struct-style readers should round-trip through /api/read with a natural-language prompt."""
    configured_model = os.environ.get("TERMITE_PIX2STRUCT_MODEL")
    if configured_model:
        readers = api.models().get("readers", {})
        if configured_model not in readers:
            pytest.skip("Configured Pix2Struct reader is not loadable in this build")

    model = configured_model or _find_reader_model(api, "pix2struct")
    if not model:
        pytest.skip("No Pix2Struct reader model is available")

    if _api_backend_enabled(api, "onnx_runtime") is False:
        pytest.skip("Pix2Struct e2e requires a termite binary built with onnx=true")

    fixture = load_go_sample_page_fixture()
    if fixture is not None:
        test_image, _phrases = fixture
    else:
        test_image = make_text_png_uri(
            [
                "INVOICE",
                "TOTAL 123",
            ],
            scale=8,
            padding=18,
            line_gap=12,
        )

    resp = _read_or_skip_backend_unavailable(
        api,
        images=[test_image],
        model=model,
        prompt="What type of document is this?",
        max_tokens=128,
    )
    results = resp["data"]
    assert len(results) == 1
    _assert_read_result_shape(results[0])
    assert isinstance(results[0]["text"], str)
    assert results[0]["text"].strip()
    _assert_known_model_output(model, results[0]["text"], fixture is not None)
    _assert_text_contains_any(results[0]["text"], "TERMITE_PIX2STRUCT_EXPECT_TEXT_ANY")
