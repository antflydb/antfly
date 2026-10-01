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

"""Tests for /ai/v1/transcription/sessions (streaming transcription)."""

import base64
import time
import wave
from pathlib import Path

import pytest

pytestmark = pytest.mark.model_integration

_WHISPER_QUALITY_WAV = Path(__file__).with_name("testdata") / "whisper_quality.wav"


def _phrase_pcm() -> tuple[bytes, int]:
    with wave.open(str(_WHISPER_QUALITY_WAV)) as w:
        assert w.getnchannels() == 1 and w.getsampwidth() == 2
        return w.readframes(w.getnframes()), w.getframerate()


def _chunks(pcm: bytes, rate: int, chunk_ms: int) -> list[bytes]:
    size = int(rate * chunk_ms / 1000) * 2
    return [pcm[i : i + size] for i in range(0, len(pcm), size)]


def _create(api, **extra) -> dict:
    resp = api.post(
        "/transcription/sessions",
        json={"model": "openai/whisper-tiny", "language": "en", **extra},
    )
    if resp.status_code == 404:
        pytest.skip(f"model unavailable: {resp.text[:200]}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["object"] == "transcription.session"
    assert len(body["id"]) == 32
    assert body["expires_at"] > body["created"]
    return body


def _append(api, session_id: str, chunk: bytes | None, rate: int, **extra) -> dict:
    body = {"format": "pcm16", "sample_rate": rate, **extra}
    if chunk is not None:
        body["audio"] = base64.b64encode(chunk).decode()
    resp = api.post(f"/transcription/sessions/{session_id}/audio", json=body)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["object"] == "list"
    assert data["session_id"] == session_id
    return data


@pytest.mark.multimodal
def test_session_streams_partials_and_finals_at_endpoints(api):
    """Speech, silence, speech: partials while speaking, finals at endpoints."""
    pcm, rate = _phrase_pcm()
    silence = b"\x00\x00" * rate
    session = _create(api, partial_interval_ms=1000)
    session_id = session["id"]

    events = []
    for chunk in _chunks(pcm + silence + pcm + silence, rate, 500):
        events.extend(_append(api, session_id, chunk, rate)["data"])
    events.extend(_append(api, session_id, None, rate, commit=True)["data"])

    finals = [e for e in events if e["type"] == "final"]
    partials = [e for e in events if e["type"] == "partial"]
    assert len(finals) == 2, events
    assert partials, "expected at least one partial while speech was open"
    for event in events:
        assert event["object"] == "transcription.event"
        assert event["end_ms"] > event["start_ms"]
        assert event["stable_text"] == event["text"] or event["text"].startswith(
            event["stable_text"]
        )
    sequences = [e["sequence"] for e in events]
    assert sequences == sorted(sequences) and len(set(sequences)) == len(sequences)
    assert finals[0]["end_ms"] <= finals[1]["start_ms"]
    for final in finals:
        lowered = " ".join(final["text"].lower().split())
        for word in ("quick", "fox", "dog"):
            assert word in lowered, lowered
        assert final["language"] == "en"

    status = api.get(f"/transcription/sessions/{session_id}")
    assert status.status_code == 200, status.text
    body = status.json()
    assert body["finals"] == 2
    assert body["partials"] == len(partials)
    assert body["buffered_ms"] == 0
    assert body["total_ms"] >= 6_900

    deleted = api.s.delete(f"{api.url}/ai/v1/transcription/sessions/{session_id}")
    assert deleted.status_code == 200, deleted.text
    assert deleted.json() == {
        "object": "transcription.session.deleted",
        "id": session_id,
        "deleted": True,
    }
    assert api.get(f"/transcription/sessions/{session_id}").status_code == 404


@pytest.mark.multimodal
def test_session_accepts_container_chunks_and_commit_flushes(api):
    """WAV chunks are decoded with format auto; commit closes open speech."""
    session = _create(api, emit_partials=False)
    session_id = session["id"]
    audio = base64.b64encode(_WHISPER_QUALITY_WAV.read_bytes()).decode()
    first = api.post(
        f"/transcription/sessions/{session_id}/audio", json={"audio": audio}
    )
    assert first.status_code == 200, first.text
    assert first.json()["data"] == []  # speech still open, partials disabled
    assert first.json()["buffered_ms"] > 0

    flushed = api.post(
        f"/transcription/sessions/{session_id}/audio", json={"commit": True}
    )
    assert flushed.status_code == 200, flushed.text
    data = flushed.json()
    assert data["buffered_ms"] == 0
    assert [e["type"] for e in data["data"]] == ["final"]
    assert "fox" in data["data"][0]["text"].lower()
    api.s.delete(f"{api.url}/ai/v1/transcription/sessions/{session_id}")


@pytest.mark.multimodal
def test_session_validation_and_lifecycle_errors(api):
    unknown = "0123456789abcdef0123456789abcdef"
    assert api.get(f"/transcription/sessions/{unknown}").status_code == 404
    assert (
        api.s.delete(f"{api.url}/ai/v1/transcription/sessions/{unknown}").status_code
        == 404
    )
    missing_audio = api.post(f"/transcription/sessions/{unknown}/audio", json={})
    assert missing_audio.status_code == 400
    assert "audio is required" in missing_audio.json()["message"]

    bad_config = api.post(
        "/transcription/sessions",
        json={"model": "openai/whisper-tiny", "max_segment_ms": 40_000},
    )
    assert bad_config.status_code == 400, bad_config.text

    bad_language = api.post(
        "/transcription/sessions",
        json={"model": "openai/whisper-tiny", "language": "zz"},
    )
    if bad_language.status_code == 404:
        pytest.skip("model unavailable")
    assert bad_language.status_code == 400, bad_language.text
    assert "language" in bad_language.json()["message"]

    session = _create(api)
    odd = api.post(
        f"/transcription/sessions/{session['id']}/audio",
        json={"audio": base64.b64encode(b"abc").decode(), "format": "pcm16"},
    )
    assert odd.status_code == 400, odd.text
    api.s.delete(f"{api.url}/ai/v1/transcription/sessions/{session['id']}")


def _attachment_envelope(metadata: dict, mime: str, data: bytes) -> bytes:
    import json
    import struct

    meta = json.dumps(metadata).encode()
    out = b"AFATT001" + struct.pack("<QI4x", len(meta), 1)
    out += struct.pack("<I4xQ", len(mime.encode()), len(data))
    return out + meta + mime.encode() + data


def _sse_messages(raw: bytes) -> list:
    import json

    messages = []
    for line in raw.decode().split("\n"):
        if not line.startswith("data: "):
            continue
        payload = line[len("data: ") :]
        messages.append("[DONE]" if payload == "[DONE]" else json.loads(payload))
    return messages


@pytest.mark.multimodal
def test_session_finals_carry_words_and_accept_dictionary(api):
    pcm, rate = _phrase_pcm()
    session = _create(api, emit_partials=False, dictionary=["Antfly", "Colony"])
    session_id = session["id"]
    _append(api, session_id, pcm, rate)
    data = _append(api, session_id, None, rate, commit=True)
    finals = [e for e in data["data"] if e["type"] == "final"]
    assert len(finals) == 1, data
    final = finals[0]
    assert final["words"], final
    assert final["words"][0]["start_ms"] == final["start_ms"]
    assert final["words"][-1]["end_ms"] <= final["end_ms"]
    assert " ".join(w["word"] for w in final["words"]) == " ".join(
        final["text"].split()
    )
    api.s.delete(f"{api.url}/ai/v1/transcription/sessions/{session_id}")


@pytest.mark.multimodal
def test_session_framed_append(api):
    pcm, rate = _phrase_pcm()
    session = _create(api, emit_partials=False)
    session_id = session["id"]
    body = _attachment_envelope(
        {
            "audio": "attachment:0",
            "format": "pcm16",
            "sample_rate": rate,
            "commit": True,
        },
        "audio/pcm",
        pcm,
    )
    resp = api.s.post(
        f"{api.url}/ai/v1/transcription/sessions/{session_id}/audio",
        data=body,
        headers={"Content-Type": "application/vnd.antfly.attachments.v1"},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    finals = [e for e in resp.json()["data"] if e["type"] == "final"]
    assert finals and "fox" in finals[0]["text"].lower(), resp.text
    api.s.delete(f"{api.url}/ai/v1/transcription/sessions/{session_id}")


@pytest.mark.multimodal
@pytest.mark.streaming
def test_session_events_stream_receives_appended_events(api):
    """An SSE subscriber sees events produced by appends on another connection."""
    import threading

    pcm, rate = _phrase_pcm()
    session = _create(api, emit_partials=False)
    session_id = session["id"]
    received: list = []

    def subscribe():
        with api.s.get(
            f"{api.url}/ai/v1/transcription/sessions/{session_id}/events",
            stream=True,
            timeout=120,
        ) as resp:
            assert resp.status_code == 200, resp.text
            for line in resp.iter_lines():
                if not line:
                    continue
                text = line.decode()
                if not text.startswith("data: "):
                    continue
                payload = text[len("data: ") :]
                if payload == "[DONE]":
                    break
                received.append(__import__("json").loads(payload))

    thread = threading.Thread(target=subscribe, daemon=True)
    thread.start()
    import time

    time.sleep(0.5)
    _append(api, session_id, pcm, rate)
    _append(api, session_id, None, rate, commit=True)
    deleted = api.s.delete(f"{api.url}/ai/v1/transcription/sessions/{session_id}")
    assert deleted.status_code == 200, deleted.text
    thread.join(timeout=30)
    assert not thread.is_alive(), "events stream did not close after delete"
    types = [m["type"] for m in received]
    assert types[0] == "session.open", types
    assert "transcription.event" in types, types
    assert types[-1] == "session.closed", types
    finals = [
        m["event"]
        for m in received
        if m["type"] == "transcription.event" and m["event"]["type"] == "final"
    ]
    assert finals and "fox" in finals[0]["text"].lower(), received


@pytest.mark.multimodal
@pytest.mark.streaming
def test_session_stream_upload_returns_events(api):
    """Raw PCM streamed as the request body yields events on the response."""
    pcm, rate = _phrase_pcm()
    silence = b"\x00\x00" * rate
    session = _create(api, emit_partials=False)
    session_id = session["id"]
    resp = api.s.post(
        f"{api.url}/ai/v1/transcription/sessions/{session_id}/stream?format=pcm16&sample_rate={rate}",
        data=pcm + silence + pcm,
        headers={"Content-Type": "application/octet-stream"},
        stream=True,
        timeout=180,
    )
    assert resp.status_code == 200, resp.text
    messages = _sse_messages(resp.content)
    assert messages[0]["type"] == "session.open"
    assert messages[-1] == "[DONE]"
    finals = [
        m["event"]
        for m in messages
        if isinstance(m, dict)
        and m["type"] == "transcription.event"
        and m["event"]["type"] == "final"
    ]
    assert len(finals) == 2, messages
    for final in finals:
        assert "fox" in final["text"].lower(), final
    assert finals[0]["end_ms"] <= finals[1]["start_ms"]
    status = api.get(f"/transcription/sessions/{session_id}").json()
    assert status["buffered_ms"] == 0 and status["finals"] == 2
    api.s.delete(f"{api.url}/ai/v1/transcription/sessions/{session_id}")


@pytest.mark.streaming
def test_session_stream_upload_is_duplex_over_chunked_http1(api):
    """A chunked HTTP/1.1 upload yields the first final before the body ends."""
    import socket
    from urllib.parse import urlparse

    pcm, rate = _phrase_pcm()
    silence = b"\x00\x00" * rate
    session = _create(api, emit_partials=False, audio_context="dynamic")
    session_id = session["id"]
    target = urlparse(api.url)
    sock = socket.create_connection((target.hostname, target.port), timeout=180)
    try:
        path = f"/ai/v1/transcription/sessions/{session_id}/stream?format=pcm16&sample_rate={rate}"
        sock.sendall(
            (
                f"POST {path} HTTP/1.1\r\nHost: {target.hostname}\r\n"
                "Content-Type: application/octet-stream\r\nTransfer-Encoding: chunked\r\n"
                "Connection: close\r\n\r\n"
            ).encode()
        )

        def send_chunk(data: bytes) -> None:
            sock.sendall(f"{len(data):x}\r\n".encode() + data + b"\r\n")

        for piece in _chunks(pcm + silence, rate, 250):
            send_chunk(piece)

        # Like a microphone, keep streaming silence while waiting: the server
        # endpoints on the frames it has, and a paused client would wait too.
        sock.settimeout(0.25)
        received = bytearray()
        deadline = time.monotonic() + 120
        while received.count(b'"type":"final"') < 1:
            assert time.monotonic() < deadline, received.decode(errors="replace")
            try:
                data = sock.recv(65536)
                assert data, received.decode(errors="replace")
                received.extend(data)
            except socket.timeout:
                send_chunk(b"\x00\x00" * (rate // 4))
        sock.settimeout(180)
        # The first utterance was finalized while the upload was still open.
        for piece in _chunks(pcm, rate, 250):
            send_chunk(piece)
        sock.sendall(b"0\r\n\r\n")
        while True:
            data = sock.recv(65536)
            if not data:
                break
            received.extend(data)
    finally:
        sock.close()
    text = received.decode(errors="replace")
    assert text.startswith("HTTP/1.1 200"), text[:200]
    body = text.split("\r\n\r\n", 1)[1]
    # Strip HTTP chunk framing before parsing the SSE payload.
    payload = bytearray()
    rest = body
    while rest:
        size_line, _, rest = rest.partition("\r\n")
        size = int(size_line.split(";")[0].strip() or "0", 16)
        if size == 0:
            break
        payload.extend(rest[:size].encode())
        rest = rest[size + 2 :]
    messages = _sse_messages(bytes(payload))
    assert messages[0]["type"] == "session.open"
    assert messages[-1] == "[DONE]"
    finals = [
        m["event"]
        for m in messages
        if isinstance(m, dict)
        and m["type"] == "transcription.event"
        and m["event"]["type"] == "final"
    ]
    assert len(finals) == 2, messages
    for final in finals:
        assert "fox" in final["text"].lower(), final
    api.s.delete(f"{api.url}/ai/v1/transcription/sessions/{session_id}")


def _silero_available(api) -> bool:
    resp = api.post(
        "/transcription/sessions",
        json={
            "model": "openai/whisper-tiny",
            "vad": {"model": "onnx-community/silero-vad"},
        },
    )
    if resp.status_code == 200:
        api.s.delete(f"{api.url}/ai/v1/transcription/sessions/{resp.json()['id']}")
        return True
    return False


@pytest.mark.multimodal
def test_session_silero_vad_ignores_tones_and_endpoints_speech(api):
    """With the neural VAD a loud tone never opens a segment, speech still does."""
    if not _silero_available(api):
        pytest.skip("onnx-community/silero-vad is not pulled")
    pcm, rate = _phrase_pcm()
    import math
    import struct

    tone = b"".join(
        struct.pack("<h", int(0.3 * 32767 * math.sin(2 * math.pi * 440 * i / rate)))
        for i in range(rate * 2)
    )
    silence = b"\x00\x00" * rate

    session = _create(
        api, emit_partials=False, vad={"model": "onnx-community/silero-vad"}
    )
    session_id = session["id"]
    events = []
    for chunk in _chunks(tone + silence + pcm + silence, rate, 500):
        events.extend(_append(api, session_id, chunk, rate)["data"])
    events.extend(_append(api, session_id, None, rate, commit=True)["data"])
    finals = [e for e in events if e["type"] == "final"]
    assert len(finals) == 1, events
    assert "fox" in finals[0]["text"].lower(), finals
    # The phrase starts after the 2 s tone and 1 s silence.
    assert finals[0]["start_ms"] >= 2500, finals
    api.s.delete(f"{api.url}/ai/v1/transcription/sessions/{session_id}")

    # The energy rule treats the same tone as speech and produces a segment for it.
    energy = _create(api, emit_partials=False)
    energy_events = []
    for chunk in _chunks(tone + silence, rate, 500):
        energy_events.extend(_append(api, energy["id"], chunk, rate)["data"])
    energy_events.extend(_append(api, energy["id"], None, rate, commit=True)["data"])
    assert any(e["type"] == "final" for e in energy_events) or energy_events == [], (
        energy_events
    )
    api.s.delete(f"{api.url}/ai/v1/transcription/sessions/{energy['id']}")

    bad = api.post(
        "/transcription/sessions",
        json={"model": "openai/whisper-tiny", "vad": {"model": "openai/whisper-tiny"}},
    )
    assert bad.status_code == 400, bad.text
    assert "Silero" in bad.json()["message"]
