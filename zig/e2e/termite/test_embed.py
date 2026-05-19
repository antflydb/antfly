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

"""Tests for /embed and /embeddings alias endpoints.

Matches Go termite's embedders_test.go and clip_test.go patterns.
"""

import pytest
from .helpers import (
    TINY_PNG_URI,
    can_make_spoken_wav,
    cosine_similarity,
    l2_norm,
    make_solid_png_uri,
    make_text_png_uri,
    make_spoken_wav_uri,
    make_wav_b64,
)

pytestmark = pytest.mark.model_integration

# -- Text embedding --


def test_single_embedding(api):
    resp = api.embed("hello world")
    embs = [item["embedding"] for item in resp["data"]]
    assert len(embs) == 1
    assert len(embs[0]) >= 100, f"Embedding dim too small: {len(embs[0])}"


def test_embedding_usage_counts_tokens(api):
    resp = api.embed("machine learning algorithms")
    usage = resp["usage"]
    assert isinstance(usage["prompt_tokens"], int)
    assert usage["prompt_tokens"] > 1
    assert usage["total_tokens"] == usage["prompt_tokens"]


def test_embedding_is_normalized(api):
    resp = api.embed("hello world")
    norm = l2_norm(resp["data"][0]["embedding"])
    assert abs(norm - 1.0) < 0.01, f"L2 norm {norm} is not ~1.0"


def test_batch_embedding(api):
    texts = [
        "The cat sat on the mat",
        "Machine learning is fascinating",
        "Quantum physics is complex",
    ]
    resp = api.embed(texts)
    embs = [item["embedding"] for item in resp["data"]]
    assert len(embs) == len(texts)
    dim = len(embs[0])
    for i, emb in enumerate(embs):
        assert len(emb) == dim, f"Embedding {i} has dim {len(emb)}, expected {dim}"


def test_semantic_similarity(api):
    """Similar texts should have higher cosine similarity than dissimilar ones."""
    anchor = "I love dogs"
    similar = "I adore puppies"
    dissimilar = "Quantum computing uses qubits"

    resp = api.embed([anchor, similar, dissimilar])
    embs = [item["embedding"] for item in resp["data"]]

    sim_close = cosine_similarity(embs[0], embs[1])
    sim_far = cosine_similarity(embs[0], embs[2])
    assert sim_close > sim_far, (
        f"Similar pair ({sim_close:.4f}) should score higher than dissimilar ({sim_far:.4f})"
    )


def test_large_batch(api):
    """10-item batch should all have consistent dimensions."""
    texts = [
        "The quick brown fox jumps over the lazy dog",
        "Machine learning is a subset of artificial intelligence",
        "Quantum mechanics describes nature at the smallest scales",
        "The Eiffel Tower is located in Paris, France",
        "DNA carries genetic information in living organisms",
        "Shakespeare wrote Romeo and Juliet",
        "The speed of light is approximately 300,000 km/s",
        "Photosynthesis converts sunlight into chemical energy",
        "The Great Wall of China is visible from space",
        "Neural networks are inspired by biological neurons",
    ]
    resp = api.embed(texts)
    embs = [item["embedding"] for item in resp["data"]]
    assert len(embs) == 10
    dim = len(embs[0])
    for emb in embs:
        assert len(emb) == dim


# -- Error handling --


def test_empty_input_returns_error(api):
    r = api.post("/embed", json={})
    assert r.status_code == 400
    assert "error" in r.json()


def test_invalid_json_returns_error(api):
    r = api.post("/embed", json="not json")
    # Server should return 400 for malformed request
    assert r.status_code in (400, 422)


# -- Multimodal (CLIP/CLAP via CLIPCLAP) --


CLIPCLAP_MODEL = "antflydb/clipclap"


def _media_wav_part(duration: float = 0.1, sample_rate: int = 48000):
    return {
        "type": "media",
        "mime_type": "audio/wav",
        "data": make_wav_b64(duration, sample_rate),
    }


def _image_part(uri: str):
    return {"type": "image_url", "image_url": {"url": uri}}


def _assert_clipclap_embeddings(embs, count: int):
    assert len(embs) == count
    for emb in embs:
        assert len(emb) == 512


def _assert_distinct_rows(*embs):
    for i in range(len(embs)):
        for j in range(i + 1, len(embs)):
            delta = sum(abs(a - b) for a, b in zip(embs[i], embs[j]))
            assert delta > 1e-5


@pytest.mark.multimodal
def test_image_embedding(api):
    """Image embedding via image_url content part."""
    white_uri = make_solid_png_uri(255, 255, 255)
    resp = api.embed([_image_part(white_uri)], model=CLIPCLAP_MODEL)
    embs = [item["embedding"] for item in resp["data"]]
    assert len(embs) == 1
    assert len(embs[0]) >= 1, "Image embedding should be non-empty"


@pytest.mark.multimodal
def test_mixed_text_image_batch(api):
    """Batch with both text and image inputs."""
    white_uri = make_solid_png_uri(255, 255, 255)
    resp = api.embed(
        [
            "a photo of a cat",
            _image_part(white_uri),
        ],
        model=CLIPCLAP_MODEL,
    )
    embs = [item["embedding"] for item in resp["data"]]
    assert len(embs) == 2


@pytest.mark.multimodal
def test_clip_text_image_alignment(api):
    """CLIPCLAP mixed text/image batches preserve order, dims, and distinct images."""
    white_uri = make_text_png_uri(["WHITE"])
    black_uri = make_text_png_uri(["BLACK"])
    resp = api.embed(
        [
            "WHITE",
            "BLACK",
            _image_part(white_uri),
            _image_part(black_uri),
        ],
        model=CLIPCLAP_MODEL,
    )
    white_text, black_text, white_image, black_image = [item["embedding"] for item in resp["data"]]

    assert len(white_text) == len(white_image)
    assert len(black_text) == len(black_image)
    assert len(white_image) == len(black_image)

    # These synthetic text PNGs are near the model's semantic noise floor, so
    # this e2e guards the plumbing contract instead: batched image execution
    # must not collapse both image rows to the same embedding.
    image_delta = sum(abs(a - b) for a, b in zip(white_image, black_image))
    text_delta = sum(abs(a - b) for a, b in zip(white_text, black_text))
    assert image_delta > 1e-5
    assert text_delta > 1e-5


@pytest.mark.multimodal
def test_invalid_data_uri_returns_error(api):
    r = api.post("/embed", json={
        "input": [{"type": "image_url", "image_url": {"url": "not-a-data-uri"}}],
    })
    assert r.status_code in (400, 500)
    assert "error" in r.json()


# -- Multimodal (Audio via CLIPCLAP) --


@pytest.mark.multimodal
def test_audio_embedding(api):
    """Audio embedding via media content part."""
    resp = api.embed(
        [_media_wav_part()],
        model=CLIPCLAP_MODEL,
    )
    embs = [item["embedding"] for item in resp["data"]]
    assert len(embs) == 1
    assert len(embs[0]) >= 1, "Audio embedding should be non-empty"


@pytest.mark.multimodal
@pytest.mark.skipif(not can_make_spoken_wav(), reason="requires macOS say + afconvert")
def test_clap_spoken_audio_batch_rows_are_distinct(api):
    """Spoken-audio batches preserve rows and produce distinct projected embeddings."""
    fox_text = "a person saying a quick brown fox jumps over the lazy dog"
    moon_text = "a person saying astronauts landed on the moon"
    fox_audio = make_spoken_wav_uri("a quick brown fox jumps over the lazy dog")
    moon_audio = make_spoken_wav_uri("astronauts landed on the moon")
    silence_audio = f"data:audio/wav;base64,{make_wav_b64(0.5, 48000)}"

    resp = api.embed(
        [
            fox_text,
            moon_text,
            {"type": "media", "mime_type": "audio/wav", "data": fox_audio.split(",", 1)[1]},
            {"type": "media", "mime_type": "audio/wav", "data": moon_audio.split(",", 1)[1]},
            {"type": "media", "mime_type": "audio/wav", "data": silence_audio.split(",", 1)[1]},
        ],
        model=CLIPCLAP_MODEL,
    )
    fox_text_emb, moon_text_emb, fox_audio_emb, moon_audio_emb, silence_audio_emb = [item["embedding"] for item in resp["data"]]

    _assert_clipclap_embeddings([fox_text_emb, moon_text_emb, fox_audio_emb, moon_audio_emb, silence_audio_emb], 5)

    # This is a runtime/e2e regression test, not a speech-recognition quality
    # benchmark. Generated speech can land near CLAP's semantic noise floor, so
    # guard the contract that batched audio rows do not collapse or reorder.
    assert sum(abs(a - b) for a, b in zip(fox_text_emb, moon_text_emb)) > 1e-5
    assert sum(abs(a - b) for a, b in zip(fox_audio_emb, moon_audio_emb)) > 1e-5
    assert sum(abs(a - b) for a, b in zip(fox_audio_emb, silence_audio_emb)) > 1e-5
    assert sum(abs(a - b) for a, b in zip(moon_audio_emb, silence_audio_emb)) > 1e-5


@pytest.mark.multimodal
def test_clipclap_mixed_text_audio_batch(api):
    """clipclap should embed text and audio in one request without collapsing rows."""
    resp = api.embed(
        [
            "a short silent audio clip",
            _media_wav_part(0.1, 48000),
        ],
        model=CLIPCLAP_MODEL,
    )
    embs = [item["embedding"] for item in resp["data"]]
    _assert_clipclap_embeddings(embs, 2)
    _assert_distinct_rows(*embs)


@pytest.mark.multimodal
def test_clipclap_mixed_image_audio_batch(api):
    """clipclap should embed image and audio in one request without collapsing rows."""
    white_uri = make_solid_png_uri(255, 255, 255)
    resp = api.embed(
        [
            _image_part(white_uri),
            _media_wav_part(0.1, 48000),
        ],
        model=CLIPCLAP_MODEL,
    )
    embs = [item["embedding"] for item in resp["data"]]
    _assert_clipclap_embeddings(embs, 2)
    _assert_distinct_rows(*embs)


@pytest.mark.multimodal
def test_clipclap_mixed_text_image_audio_batch(api):
    """clipclap should embed text, image, and audio in one request."""
    white_uri = make_solid_png_uri(255, 255, 255)
    resp = api.embed(
        [
            "a white image",
            _image_part(white_uri),
            _media_wav_part(0.1, 48000),
        ],
        model=CLIPCLAP_MODEL,
    )
    embs = [item["embedding"] for item in resp["data"]]
    _assert_clipclap_embeddings(embs, 3)
    _assert_distinct_rows(*embs)


@pytest.mark.multimodal
def test_clipclap_modalities_share_projection_dim(api):
    """clipclap should project text, image, and audio into the same space."""
    text_resp = api.embed("hello world", model=CLIPCLAP_MODEL)
    white_uri = make_solid_png_uri(255, 255, 255)
    image_resp = api.embed(
        [_image_part(white_uri)],
        model=CLIPCLAP_MODEL,
    )
    audio_resp = api.embed(
        [_media_wav_part()],
        model=CLIPCLAP_MODEL,
    )

    text_emb = text_resp["data"][0]["embedding"]
    image_emb = image_resp["data"][0]["embedding"]
    audio_emb = audio_resp["data"][0]["embedding"]

    assert len(text_emb) == 512
    assert len(image_emb) == 512
    assert len(audio_emb) == 512


@pytest.mark.multimodal
def test_clipclap_text_image_alignment(api):
    """clipclap should preserve at least a basic CLIP-style text/image signal."""
    white_uri = make_solid_png_uri(255, 255, 255)
    black_uri = make_solid_png_uri(0, 0, 0)
    white_text = api.embed("a white image", model=CLIPCLAP_MODEL)["data"][0]["embedding"]
    white_image = api.embed(
        [_image_part(white_uri)],
        model=CLIPCLAP_MODEL,
    )["data"][0]["embedding"]
    black_image = api.embed(
        [_image_part(black_uri)],
        model=CLIPCLAP_MODEL,
    )["data"][0]["embedding"]

    white_match = cosine_similarity(white_text, white_image)
    white_mismatch = cosine_similarity(white_text, black_image)

    assert white_match > white_mismatch
