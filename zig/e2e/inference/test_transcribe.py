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

import pytest
from .helpers import assert_openai_list_response, make_wav_b64

pytestmark = pytest.mark.model_integration


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
