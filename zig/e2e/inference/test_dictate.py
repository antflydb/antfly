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

"""Tests for /ai/v1/dictate (push-to-talk dictation)."""

import base64
import io
import json
import wave
from pathlib import Path

import pytest
from .helpers import make_wav_b64
from .models import default_generator_model_name

pytestmark = pytest.mark.model_integration

_WHISPER_QUALITY_WAV = Path(__file__).with_name("testdata") / "whisper_quality.wav"
_EXPECTED_WORDS = ("quick", "brown", "fox", "lazy", "dog")


def _phrase_pcm() -> tuple[bytes, int]:
    with wave.open(str(_WHISPER_QUALITY_WAV)) as w:
        assert w.getnchannels() == 1 and w.getsampwidth() == 2
        return w.readframes(w.getnframes()), w.getframerate()


def _wav_b64(pcm: bytes, rate: int) -> str:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(pcm)
    return base64.b64encode(buf.getvalue()).decode()


def _cleanup_model(api) -> str:
    """Pick the suite's generator (override, default, or first listed)."""
    generators = api.models().get("generators", {})
    if (model := default_generator_model_name(set(generators.keys()))) is not None:
        return model
    if generators:
        return next(iter(generators.keys()))
    pytest.skip("No generator models available for cleanup tests")


def _assert_usage(usage: dict) -> None:
    assert usage["total_tokens"] == usage["prompt_tokens"] + usage["completion_tokens"]


@pytest.mark.multimodal
def test_dictate_without_cleanup_returns_raw_transcript(api):
    """Without cleanup_model the text is the transcript itself."""
    audio = base64.b64encode(_WHISPER_QUALITY_WAV.read_bytes()).decode()
    resp = api.post("/dictate", json={"model": "openai/whisper-tiny", "audio": audio})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["object"] == "dictation"
    assert body["id"].startswith("dict-")
    assert body["model"] == "openai/whisper-tiny"
    assert "cleanup_model" not in body or body["cleanup_model"] is None
    transcript = body["transcript"]
    assert transcript["language"] == "en"
    assert 2400 <= transcript["duration_ms"] <= 2600
    assert len(transcript["segments"]) == 1
    assert transcript["segments"][0]["start_ms"] == 0
    assert body["text"] == transcript["text"]
    lowered = " ".join(body["text"].lower().split())
    for word in _EXPECTED_WORDS:
        assert word in lowered, lowered
    _assert_usage(body["usage"])


@pytest.mark.multimodal
def test_dictate_dynamic_audio_context_matches_full_window(api):
    """Trimming the encoder to the audio present keeps the transcript."""
    audio = base64.b64encode(_WHISPER_QUALITY_WAV.read_bytes()).decode()
    resp = api.post(
        "/dictate",
        json={
            "model": "openai/whisper-tiny",
            "audio": audio,
            "audio_context": "dynamic",
        },
    )
    assert resp.status_code == 200, resp.text
    lowered = " ".join(resp.json()["text"].lower().split())
    for word in _EXPECTED_WORDS:
        assert word in lowered, lowered
    bad = api.post(
        "/dictate",
        json={"model": "openai/whisper-tiny", "audio": audio, "audio_context": "huge"},
    )
    assert bad.status_code == 400, bad.text


@pytest.mark.multimodal
def test_dictate_windows_clips_longer_than_thirty_seconds(api):
    """A 40 s clip is cut at a pause into several windows, all transcribed."""
    pcm, rate = _phrase_pcm()
    silence = b"\x00\x00" * int(rate * 1.5)
    long_pcm = (pcm + silence) * 10  # ~40 s
    resp = api.post(
        "/dictate",
        json={"model": "openai/whisper-tiny", "audio": _wav_b64(long_pcm, rate)},
        timeout=300,
    )
    assert resp.status_code == 200, resp.text
    transcript = resp.json()["transcript"]
    assert transcript["duration_ms"] >= 39_000
    segments = transcript["segments"]
    assert len(segments) >= 2
    previous_end = 0
    for segment in segments:
        assert segment["start_ms"] >= previous_end
        assert segment["end_ms"] >= segment["start_ms"]
        assert segment["end_ms"] - segment["start_ms"] <= 30_000
        previous_end = segment["end_ms"]
    assert previous_end <= transcript["duration_ms"]
    # Speech after the cut (past 30 s) was transcribed, so nothing was lost.
    assert segments[-1]["end_ms"] > 30_000, segments
    lowered = transcript["text"].lower()
    assert lowered.count("fox") >= 5, lowered


@pytest.mark.multimodal
def test_dictate_silent_clip_returns_empty_transcript(api):
    """Silence produces an empty transcript, not an error or a hallucination."""
    resp = api.post(
        "/dictate",
        json={"model": "openai/whisper-tiny", "audio": make_wav_b64(1.0)},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["transcript"]["segments"] == []
    assert body["text"] == ""


@pytest.mark.multimodal
def test_dictate_rejects_invalid_options_before_transcribing(api):
    """Validation failures never reach the model."""
    audio = base64.b64encode(_WHISPER_QUALITY_WAV.read_bytes()).decode()
    cases = [
        (
            {"model": "openai/whisper-tiny", "audio": audio, "dictionary": ["a\nb"]},
            "dictionary",
        ),
        (
            {"model": "openai/whisper-tiny", "audio": audio, "max_tokens": 0},
            "max_tokens",
        ),
        ({"model": "", "audio": audio}, "model is required"),
        ({"model": "openai/whisper-tiny", "audio": "%%%"}, "base64"),
    ]
    for body, needle in cases:
        resp = api.post("/dictate", json=body)
        assert resp.status_code == 400, resp.text
        assert needle in resp.json()["message"], resp.text


@pytest.mark.multimodal
@pytest.mark.slow
def test_dictate_cleanup_rewrites_transcript(api):
    """With a generator the response carries cleaned text and real usage."""
    audio = base64.b64encode(_WHISPER_QUALITY_WAV.read_bytes()).decode()
    cleanup_model = _cleanup_model(api)
    resp = api.post(
        "/dictate",
        json={
            "model": "openai/whisper-tiny",
            "cleanup_model": cleanup_model,
            "audio": audio,
            "dictionary": ["Antfly"],
            "context": "chat message to a colleague",
            "max_tokens": 128,
        },
        timeout=600,
    )
    if resp.status_code in (400, 404):
        pytest.skip(f"generator unavailable: {resp.text[:200]}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["cleanup_model"] == cleanup_model
    assert body["transcript"]["text"]
    cleaned = " ".join(body["text"].lower().split())
    assert cleaned, body
    for word in ("quick", "fox", "dog"):
        assert word in cleaned, cleaned
    assert body["usage"]["prompt_tokens"] > 0
    assert body["usage"]["completion_tokens"] > 0
    _assert_usage(body["usage"])


@pytest.mark.multimodal
@pytest.mark.slow
@pytest.mark.streaming
def test_dictate_streams_transcript_then_deltas_then_completion(api):
    """Streaming emits transcript, delta, completed, then [DONE]."""
    audio = base64.b64encode(_WHISPER_QUALITY_WAV.read_bytes()).decode()
    resp = api.post(
        "/dictate",
        json={
            "model": "openai/whisper-tiny",
            "cleanup_model": _cleanup_model(api),
            "audio": audio,
            "stream": True,
            "max_tokens": 128,
        },
        stream=True,
        timeout=600,
    )
    if resp.status_code in (400, 404):
        pytest.skip(f"generator unavailable: {resp.text[:200]}")
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith("text/event-stream")
    events = []
    done = False
    for line in resp.iter_lines():
        if not line:
            continue
        text = line.decode()
        assert text.startswith("data: "), text
        payload = text[len("data: ") :]
        if payload == "[DONE]":
            done = True
            break
        events.append(json.loads(payload))
    assert done
    types = [event["type"] for event in events]
    assert types[0] == "dictation.transcript"
    assert events[0]["transcript"]["text"]
    assert types[-1] == "dictation.completed"
    assert "dictation.delta" in types
    assert "error" not in types
    deltas = "".join(
        event["delta"] for event in events if event["type"] == "dictation.delta"
    )
    completed = events[-1]
    assert completed["text"]
    assert completed["text"] in deltas or deltas.strip().startswith(
        completed["text"][:8]
    )
    _assert_usage(completed["usage"])
    assert len({event["id"] for event in events}) == 1


def _attachment_envelope(metadata: dict, mime: str, data: bytes) -> bytes:
    """Encode the framed attachment transport (application/vnd.antfly.attachments.v1)."""
    import struct

    meta = json.dumps(metadata).encode()
    out = b"AFATT001" + struct.pack("<QI4x", len(meta), 1)
    out += struct.pack("<I4xQ", len(mime.encode()), len(data))
    return out + meta + mime.encode() + data


@pytest.mark.multimodal
def test_dictate_returns_timestamped_phrases_with_words(api):
    """Segments come from Whisper timestamp tokens and carry word spans."""
    audio = base64.b64encode(_WHISPER_QUALITY_WAV.read_bytes()).decode()
    resp = api.post(
        "/dictate",
        json={"model": "openai/whisper-tiny", "audio": audio, "language": "en"},
    )
    assert resp.status_code == 200, resp.text
    transcript = resp.json()["transcript"]
    assert transcript["segments"], transcript
    previous_end = 0
    for segment in transcript["segments"]:
        assert segment["start_ms"] >= previous_end
        assert segment["end_ms"] >= segment["start_ms"]
        assert segment["end_ms"] <= transcript["duration_ms"]
        assert segment["words"], segment
        assert " ".join(w["word"] for w in segment["words"]) == " ".join(
            segment["text"].split()
        )
        assert segment["words"][0]["start_ms"] == segment["start_ms"]
        assert segment["words"][-1]["end_ms"] == segment["end_ms"]
        previous_end = segment["end_ms"]
    # A spoken clip of ~2.5 s must not report a phrase spanning the full 30 s window.
    assert transcript["segments"][-1]["end_ms"] <= transcript["duration_ms"]


@pytest.mark.multimodal
def test_dictate_accepts_transcript_prompt_and_dictionary(api):
    """Prompt conditioning is accepted and does not break recognition."""
    audio = base64.b64encode(_WHISPER_QUALITY_WAV.read_bytes()).decode()
    # The prompt is treated as text that preceded the clip, so it must not
    # repeat the clip's own words or Whisper will skip them as already said.
    for body in (
        {
            "model": "openai/whisper-tiny",
            "audio": audio,
            "dictionary": ["Antfly", "Colony"],
        },
        {
            "model": "openai/whisper-tiny",
            "audio": audio,
            "transcript_prompt": "Glossary: Antfly, Colony, Roetker.",
        },
    ):
        resp = api.post("/dictate", json=body)
        assert resp.status_code == 200, resp.text
        lowered = resp.json()["transcript"]["text"].lower()
        for word in ("quick", "fox", "dog"):
            assert word in lowered, lowered
    too_long = api.post(
        "/dictate",
        json={
            "model": "openai/whisper-tiny",
            "audio": audio,
            "transcript_prompt": "x" * 1025,
        },
    )
    assert too_long.status_code == 400, too_long.text


@pytest.mark.multimodal
def test_dictate_framed_attachment_transport(api):
    """The clip may arrive as the single attachment of a framed envelope."""
    body = _attachment_envelope(
        {"model": "openai/whisper-tiny", "audio": "attachment:0", "language": "en"},
        "audio/wav",
        _WHISPER_QUALITY_WAV.read_bytes(),
    )
    resp = api.s.post(
        f"{api.url}/ai/v1/dictate",
        data=body,
        headers={"Content-Type": "application/vnd.antfly.attachments.v1"},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    lowered = resp.json()["transcript"]["text"].lower()
    assert "fox" in lowered, lowered

    # An inline JSON request may not reference attachments.
    bad = api.post(
        "/dictate", json={"model": "openai/whisper-tiny", "audio": "attachment:0"}
    )
    assert bad.status_code == 400
    assert "attachment" in bad.json()["message"]


@pytest.mark.multimodal
def test_dictate_with_silero_vad_skips_tone_windows(api):
    """Neural VAD windowing drops a long tone-only stretch instead of transcribing it."""
    probe = api.post(
        "/dictate",
        json={
            "model": "openai/whisper-tiny",
            "audio": make_wav_b64(0.2),
            "vad": {"model": "onnx-community/silero-vad"},
        },
    )
    if probe.status_code == 400 and "Silero" in probe.text or probe.status_code == 404:
        pytest.skip("onnx-community/silero-vad is not pulled")
    assert probe.status_code == 200, probe.text
    pcm, rate = _phrase_pcm()
    import math
    import struct

    tone = b"".join(
        struct.pack("<h", int(0.3 * 32767 * math.sin(2 * math.pi * 440 * i / rate)))
        for i in range(rate * 32)
    )
    clip = tone + pcm  # 32 s of tone, then the phrase: the first window is tone only
    resp = api.post(
        "/dictate",
        json={
            "model": "openai/whisper-tiny",
            "audio": _wav_b64(clip, rate),
            "vad": {"model": "onnx-community/silero-vad"},
        },
        timeout=300,
    )
    assert resp.status_code == 200, resp.text
    transcript = resp.json()["transcript"]
    assert transcript["segments"], transcript
    assert transcript["segments"][0]["start_ms"] >= 24_000, transcript["segments"][0]
    assert "fox" in transcript["text"].lower(), transcript["text"]
