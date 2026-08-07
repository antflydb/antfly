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

"""Tests for /api/transcribe (speech-to-text) endpoint.

Matches Go antfly's transcriber_test.go patterns.
"""

import base64
from pathlib import Path

import pytest
from .helpers import assert_openai_list_response, make_wav_b64

pytestmark = pytest.mark.model_integration

_WHISPER_QUALITY_WAV = Path(__file__).with_name("testdata") / "whisper_quality.wav"
_WHISPER_SPANISH_QUALITY_WAV = Path(__file__).with_name("testdata") / "whisper_spanish_quality.wav"


@pytest.mark.multimodal
def test_transcribe_audio(api):
    """Transcribing audio should return text output."""
    wav_b64 = make_wav_b64(0.5)
    audio_uri = f"data:audio/wav;base64,{wav_b64}"
    resp = api.transcribe(audio=audio_uri)
    assert_openai_list_response(resp, expected_len=1)
    assert "text" in resp["data"][0]
    # Silent audio may return empty text, but should not error


@pytest.mark.multimodal
def test_transcribe_returns_text_key(api):
    """Response should always contain a 'text' field."""
    wav_b64 = make_wav_b64(0.1)
    audio_uri = f"data:audio/wav;base64,{wav_b64}"
    resp = api.transcribe(audio=audio_uri)
    assert "text" in resp["data"][0]


@pytest.mark.multimodal
def test_whisper_tiny_transcribes_spoken_phrase(api):
    """The shipped Whisper bundle must produce words, not only a valid shape."""
    audio_uri = "data:audio/wav;base64," + base64.b64encode(_WHISPER_QUALITY_WAV.read_bytes()).decode()
    resp = api.transcribe(audio=audio_uri, model="openai/whisper-tiny")
    assert_openai_list_response(resp, expected_len=1)
    assert resp["data"][0].get("language") == "en"

    transcript = " ".join(resp["data"][0]["text"].lower().split())
    for expected_word in ("quick", "brown", "fox", "lazy", "dog"):
        assert expected_word in transcript, f"missing {expected_word!r} from transcript: {transcript!r}"


@pytest.mark.multimodal
def test_whisper_tiny_autodetects_spanish(api):
    """Automatic language detection must not regress to an English artifact prompt."""
    audio_uri = "data:audio/wav;base64," + base64.b64encode(_WHISPER_SPANISH_QUALITY_WAV.read_bytes()).decode()
    resp = api.transcribe(audio=audio_uri, model="openai/whisper-tiny")
    assert_openai_list_response(resp, expected_len=1)
    assert resp["data"][0].get("language") == "es"

    transcript = " ".join(resp["data"][0]["text"].lower().split())
    for expected_word in ("buenos", "prueba", "reconocimiento", "idioma"):
        assert expected_word in transcript, f"missing {expected_word!r} from transcript: {transcript!r}"
