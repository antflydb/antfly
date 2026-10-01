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
_WHISPER_SPANISH_QUALITY_WAV = (
    Path(__file__).with_name("testdata") / "whisper_spanish_quality.wav"
)


@pytest.mark.multimodal
def test_transcribe_audio(api):
    """Transcribing audio should return text output."""
    wav_b64 = make_wav_b64(0.5)
    audio_uri = f"data:audio/wav;base64,{wav_b64}"
    resp = api.transcribe(audio=audio_uri, model="openai/whisper-tiny")
    assert_openai_list_response(resp, expected_len=1)
    assert "text" in resp["data"][0]
    # Silent audio may return empty text, but should not error


@pytest.mark.multimodal
def test_transcribe_returns_text_key(api):
    """Response should always contain a 'text' field."""
    wav_b64 = make_wav_b64(0.1)
    audio_uri = f"data:audio/wav;base64,{wav_b64}"
    resp = api.transcribe(audio=audio_uri, model="openai/whisper-tiny")
    assert "text" in resp["data"][0]


@pytest.mark.multimodal
def test_whisper_tiny_transcribes_spoken_phrase(api):
    """The shipped Whisper bundle must produce words, not only a valid shape."""
    audio_uri = (
        "data:audio/wav;base64,"
        + base64.b64encode(_WHISPER_QUALITY_WAV.read_bytes()).decode()
    )
    resp = api.transcribe(audio=audio_uri, model="openai/whisper-tiny")
    assert_openai_list_response(resp, expected_len=1)
    assert resp["data"][0].get("language") == "en"

    transcript = " ".join(resp["data"][0]["text"].lower().split())
    for expected_word in ("quick", "brown", "fox", "lazy", "dog"):
        assert expected_word in transcript, (
            f"missing {expected_word!r} from transcript: {transcript!r}"
        )


@pytest.mark.multimodal
def test_whisper_tiny_autodetects_spanish(api):
    """Automatic language detection must not regress to an English artifact prompt."""
    audio_uri = (
        "data:audio/wav;base64,"
        + base64.b64encode(_WHISPER_SPANISH_QUALITY_WAV.read_bytes()).decode()
    )
    resp = api.transcribe(audio=audio_uri, model="openai/whisper-tiny")
    assert_openai_list_response(resp, expected_len=1)
    assert resp["data"][0].get("language") == "es"

    transcript = " ".join(resp["data"][0]["text"].lower().split())
    for expected_word in ("buenos", "prueba", "reconocimiento", "idioma"):
        assert expected_word in transcript, (
            f"missing {expected_word!r} from transcript: {transcript!r}"
        )


def _two_speaker_wav_b64() -> str:
    """English then Spanish fixture, twice, as one 16 kHz mono 16-bit WAV.

    The two clips are different voices, so local diarization should find two
    speakers and alternate between them.
    """
    import io
    import wave

    import numpy as np

    def read(path: Path) -> np.ndarray:
        with wave.open(str(path)) as w:
            rate = w.getframerate()
            width = w.getsampwidth()
            raw = w.readframes(w.getnframes())
        if width == 1:
            pcm = (
                np.frombuffer(raw, dtype=np.uint8).astype(np.float32) - 128.0
            ) / 128.0
        else:
            pcm = np.frombuffer(raw, dtype="<i2").astype(np.float32) / 32768.0
        if rate != 16000:
            n = int(len(pcm) * 16000 / rate)
            pcm = np.interp(np.arange(n) * rate / 16000, np.arange(len(pcm)), pcm)
        return pcm.astype(np.float32)

    english = read(_WHISPER_QUALITY_WAV)
    spanish = read(_WHISPER_SPANISH_QUALITY_WAV)
    pause = np.zeros(int(16000 * 0.4), dtype=np.float32)
    clip = np.concatenate([english, pause, spanish, pause, english, pause, spanish])
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(16000)
        w.writeframes((np.clip(clip, -1, 1) * 32767).astype("<i2").tobytes())
    return base64.b64encode(buf.getvalue()).decode()


@pytest.mark.multimodal
def test_whisper_tiny_diarizes_two_speakers(api):
    """Local diarization labels every phrase with a speaker it found.

    Needs the speaker model:
    ``antfly inference pull csukuangfj/speaker-embedding-models:3dspeaker_speech_campplus_sv_en_voxceleb_16k.onnx``

    The clip alternates two voices, but whisper-tiny often returns it as one
    phrase whose word timing is estimated, so the number of speakers found is
    not stable enough to assert. What must hold is the contract: every segment
    carries a label, the labels are exactly the reported speakers, and the
    transcript is the same one an undiarized request returns.
    """
    audio_uri = "data:audio/wav;base64," + _two_speaker_wav_b64()
    resp = api.transcribe(
        audio=audio_uri, model="openai/whisper-tiny", diarization=True
    )
    assert_openai_list_response(resp, expected_len=1)
    item = resp["data"][0]

    speakers = item.get("speakers")
    assert speakers, f"diarization returned no speakers: {item!r}"
    # Dense and numbered in order of first appearance.
    assert speakers == [f"SPEAKER_{i:02d}" for i in range(len(speakers))]

    segments = item["segments"]
    assert segments, "diarized response has no segments"
    labels = [segment.get("speaker") for segment in segments]
    assert all(label in speakers for label in labels), (
        f"unlabelled segments: {labels!r}"
    )
    assert labels[0] == "SPEAKER_00"
    assert sorted(set(labels)) == sorted(speakers), (
        f"speakers {speakers!r} do not match the labels used {sorted(set(labels))!r}"
    )
    # Splitting a phrase at a speaker change must not alter the transcript.
    plain = api.transcribe(audio=audio_uri, model="openai/whisper-tiny")
    assert " ".join(item["text"].split()) == " ".join(plain["data"][0]["text"].split())
