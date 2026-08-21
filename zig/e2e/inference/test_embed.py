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

Matches Go antfly's embedders_test.go and clip_test.go patterns.
"""

import base64
import hashlib
import json
import math
import subprocess
from pathlib import Path

import pytest
from .helpers import (
    TINY_PNG_URI,
    can_make_spoken_wav,
    cosine_similarity,
    l2_norm,
    make_clip_contract_png_uri,
    make_shape_png_uri,
    make_solid_png_uri,
    make_text_png_uri,
    make_spoken_wav_uri,
    make_wav_b64,
)
from .models import (
    ensure_model_by_name,
    inference_command,
    inference_download_enabled,
    listed_model_name,
    local_model_exists,
    run_clipclap_contract_tests,
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
CLIPCLAP_IMAGE_GOLDEN = (
    Path(__file__).with_name("testdata") / "clipclap_q4k_image_embedding.json"
)


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
@pytest.mark.slow
def test_published_clipclap_pair_server_contract(api):
    """The shipped CLIP and CLAP artifacts must admit and execute as one model."""
    if not run_clipclap_contract_tests():
        pytest.skip(
            "ClipClap uses a large paired model; set "
            "RUN_CLIPCLAP_CONTRACT_TESTS=1 to require its release contract"
        )

    response = api.post(
        "/embed",
        json={
            "model": CLIPCLAP_MODEL,
            "input": [
                _image_part(make_solid_png_uri(255, 255, 255)),
                _media_wav_part(),
            ],
        },
    )
    assert response.status_code == 200, (
        "published ClipClap pair failed server admission or inference: "
        f"{response.status_code} {response.text[:2000]}"
    )
    embeddings = [item["embedding"] for item in response.json()["data"]]
    _assert_clipclap_embeddings(embeddings, 2)
    _assert_distinct_rows(*embeddings)
    for embedding in embeddings:
        assert all(math.isfinite(value) for value in embedding)
        assert abs(l2_norm(embedding) - 1.0) < 1e-4

    embedders = api.models().get("embedders", {})
    assert listed_model_name(set(embedders), CLIPCLAP_MODEL) is not None, (
        f"{CLIPCLAP_MODEL} executed but is absent from /models embedders: "
        f"{sorted(embedders)}"
    )


@pytest.mark.multimodal
def test_image_embedding(api):
    """Image embedding via image_url content part."""
    white_uri = make_solid_png_uri(255, 255, 255)
    resp = api.embed([_image_part(white_uri)], model=CLIPCLAP_MODEL)
    embs = [item["embedding"] for item in resp["data"]]
    assert len(embs) == 1
    assert len(embs[0]) >= 1, "Image embedding should be non-empty"


@pytest.mark.multimodal
@pytest.mark.verification
def test_clipclap_image_embedding_golden_contract(tmp_path):
    """ClipClap image embeddings should match the pinned Q4_K model contract."""
    if not local_model_exists(CLIPCLAP_MODEL, "embedders") and not inference_download_enabled():
        pytest.skip(f"{CLIPCLAP_MODEL} is not available")

    model_dir = ensure_model_by_name(CLIPCLAP_MODEL, "embedders")
    if model_dir is None:
        pytest.skip(f"{CLIPCLAP_MODEL} is not available")

    golden = json.loads(CLIPCLAP_IMAGE_GOLDEN.read_text())
    assert golden["model"] == CLIPCLAP_MODEL
    assert golden["variant"] == "Q4_K"

    image_uri = make_clip_contract_png_uri()
    image_bytes = base64.b64decode(image_uri.split(",", 1)[1])
    assert hashlib.sha256(image_bytes).hexdigest() == golden["input"]["sha256"]
    image_path = tmp_path / "clip-contract.png"
    image_path.write_bytes(image_bytes)

    artifact = golden["vision_artifact"]
    model_path = model_dir / artifact["name"]
    manifest = json.loads((model_dir / "model_manifest.json").read_text())
    if manifest_files := manifest.get("files"):
        manifest_file = next(item for item in manifest_files if item["name"] == artifact["name"])
        assert manifest_file["digest"] == f"sha256:{artifact['sha256']}"
        assert model_path.stat().st_size == manifest_file["size"]
    with model_path.open("rb") as model_file:
        assert hashlib.file_digest(model_file, "sha256").hexdigest() == artifact["sha256"]

    def run_embedding():
        result = subprocess.run(
            [
                *inference_command(),
                "embed",
                str(model_dir),
                "--backend",
                "native",
                "--image",
                str(image_path),
            ],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        combined = [*result.stdout.splitlines(), *result.stderr.splitlines()]
        response_line = next(line for line in reversed(combined) if line.startswith("{") and "\"embeddings\"" in line)
        return json.loads(response_line)["embeddings"][0]

    first = run_embedding()
    second = run_embedding()

    assert len(first) == 512
    assert len(second) == 512
    assert all(math.isfinite(value) for value in first)
    assert all(math.isfinite(value) for value in second)
    assert abs(l2_norm(first) - 1.0) < 1e-4
    assert abs(l2_norm(second) - 1.0) < 1e-4
    assert max(abs(a - b) for a, b in zip(first, second)) < 1e-5

    reference = golden["embedding"]
    assert len(reference) == 512
    similarity = cosine_similarity(first, reference)
    assert similarity >= 0.995, (
        f"ClipClap Q4_K image embedding cosine {similarity:.8f} is below the "
        "cross-kernel golden threshold 0.995"
    )


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
    """ClipClap should preserve semantic cross-modal retrieval alignment."""
    fixtures = [
        (
            "a red circle on a white background",
            make_shape_png_uri("circle", (220, 20, 20)),
        ),
        (
            "a blue square on a white background",
            make_shape_png_uri("square", (20, 60, 220)),
        ),
        (
            "a green triangle on a white background",
            make_shape_png_uri("triangle", (20, 170, 50)),
        ),
    ]
    text_embeddings = api.embed(
        [caption for caption, _ in fixtures],
        model=CLIPCLAP_MODEL,
    )["data"]
    image_embeddings = api.embed(
        [_image_part(uri) for _, uri in fixtures],
        model=CLIPCLAP_MODEL,
    )["data"]
    text_vectors = [item["embedding"] for item in text_embeddings]
    image_vectors = [item["embedding"] for item in image_embeddings]

    for embedding in (*text_vectors, *image_vectors):
        assert len(embedding) == 512
        assert all(math.isfinite(value) for value in embedding)
        assert abs(l2_norm(embedding) - 1.0) < 1e-4

    scores = [
        [cosine_similarity(text, image) for image in image_vectors]
        for text in text_vectors
    ]
    positives = [scores[i][i] for i in range(len(fixtures))]
    negatives = [
        scores[i][j]
        for i in range(len(fixtures))
        for j in range(len(fixtures))
        if i != j
    ]
    retrieval_hits = sum(
        max(range(len(fixtures)), key=scores[i].__getitem__) == i
        for i in range(len(fixtures))
    )
    margin = sum(positives) / len(positives) - sum(negatives) / len(negatives)
    assert retrieval_hits >= 2, {"scores": scores, "retrieval_hits": retrieval_hits}
    assert margin >= 0.01, {
        "scores": scores,
        "alignment_margin": margin,
    }
