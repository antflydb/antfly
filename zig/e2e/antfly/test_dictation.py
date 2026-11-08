# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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

"""Provider-backed dictation shares cleanup and preserves runtime decoding hints."""

import base64
import io
import json
import os
import struct
import threading
import time
import wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import conftest as harness
import pytest
import requests


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.parametrize(
    "provider",
    [
        "openai",
        "antfly",
        pytest.param(
            "apple",
            marks=pytest.mark.skipif(
                os.environ.get("ANTFLY_TEST_APPLE_SPEECH") != "1",
                reason="Requires an Apple-provider build and installed en-US Speech assets",
            ),
        ),
    ],
)
def test_named_dictation_cleanup_and_streaming(tmp_path, provider):
    state = {
        "recognition": [],
        "cleanup": [],
        "fail_cleanup": False,
        "raw": "um ant fly",
        "finish": "stop",
    }

    def frame(metadata, audio, mime="audio/wav"):
        encoded = json.dumps(metadata).encode()
        mime = mime.encode()
        return (
            b"AFATT001"
            + struct.pack("<QII", len(encoded), 1, 0)
            + struct.pack("<IIQ", len(mime), 0, len(audio))
            + encoded
            + mime
            + audio
        )

    def decode_frame(body):
        assert body[:8] == b"AFATT001"
        size, count, reserved = struct.unpack_from("<QII", body, 8)
        assert count == 1 and reserved == 0
        mime_size, reserved, audio_size = struct.unpack_from("<IIQ", body, 24)
        assert reserved == 0 and len(body) == 40 + size + mime_size + audio_size
        return json.loads(body[40 : 40 + size]), body[40 + size + mime_size :]

    class Fixture(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            if self.path.endswith("/models"):
                self.reply(
                    {"object": "list", "data": [{"id": "whisper-1", "object": "model"}]}
                )
            else:
                self.send_error(404)

        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            if self.path.endswith("/dictate"):
                if self.headers.get("Content-Type", "").startswith(
                    "application/vnd.antfly.attachments.v1"
                ):
                    value, recording = decode_frame(body)
                    value["recording_bytes"] = len(recording)
                    assert recording.startswith(b"RIFF")
                else:
                    value = json.loads(body)
                state["recognition"].append(value)
                assert value["stream"] is False
                assert "cleanup_model" not in value
                self.reply(
                    {
                        "object": "dictation",
                        "text": state["raw"],
                        "transcript": {
                            "text": state["raw"],
                            "duration_ms": 1000,
                            "segments": [],
                        },
                    }
                )
            elif self.path.endswith("/audio/transcriptions"):
                state["recognition"].append({"multipart": len(body)})
                self.reply(
                    {
                        "text": state["raw"],
                        "duration": 1.0,
                        "language": "en",
                        "segments": [],
                    }
                )
            elif self.path.endswith("/chat/completions"):
                value = json.loads(body)
                state["cleanup"].append(value)
                if state["fail_cleanup"]:
                    self.reply({"error": {"message": "unavailable"}}, status=503)
                else:
                    self.reply(
                        {
                            "id": "cleanup",
                            "object": "chat.completion",
                            "choices": [
                                {
                                    "index": 0,
                                    "message": {
                                        "role": "assistant",
                                        "content": "Antfly.",
                                    },
                                    "finish_reason": state["finish"],
                                }
                            ],
                            "usage": {
                                "prompt_tokens": 12,
                                "completion_tokens": 3,
                                "total_tokens": 15,
                            },
                        }
                    )
            else:
                self.send_error(404)

        def reply(self, value, status=200):
            data = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    fixture = ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
    thread = threading.Thread(target=fixture.serve_forever, daemon=True)
    thread.start()
    server = None
    try:
        base = f"http://127.0.0.1:{fixture.server_port}"
        if provider == "apple":
            transcriber = {
                "provider": "apple",
                "language_code": "en-US",
                "timestamps": True,
                "download_assets": False,
            }
        else:
            transcriber = {
                "provider": provider,
                "model": "fixture-whisper",
                "api_key": "fixture",
            }
            transcriber["api_url" if provider == "antfly" else "base_url"] = (
                base + "/ai/v1" if provider == "antfly" else base + "/v1"
            )
        config = tmp_path / "config.json"
        config.write_text(
            json.dumps(
                {
                    "public_inference": {"enabled": True, "default_timeout_ms": 30000},
                    "resources": {
                        "inference": {"admission": {"max_concurrent_requests": 1}}
                    },
                    "transcribers": {"speech": transcriber},
                    "connections": {
                        "hosted-speech": {
                            "kind": "inference",
                            "capabilities": ["models.transcribe"],
                            "inference": {
                                "provider": "openai",
                                "url": base + "/v1",
                                "api_key": "fixture",
                            },
                        }
                    }
                    if provider == "openai"
                    else {},
                    "generators": {
                        **(
                            {"native-cleanup": {"provider": "apple", "max_tokens": 256}}
                            if provider == "apple"
                            else {}
                        ),
                        "cleanup": {
                            "provider": "openai",
                            "model": "fixture-cleanup",
                            "url": base + "/v1",
                            "api_key": "fixture",
                        },
                    },
                }
            )
        )
        binary = harness.resolve_binary_path(
            os.environ.get("ANTFLY_BIN", str(harness.DEFAULT_ANTFLY_BIN))
        )
        server = harness.StandaloneAntflyServer(
            binary, "127.0.0.1", 0, config_path=config
        )
        expected_raw = "um ant fly"
        audio = b"RIFFfixture"
        if provider == "apple":
            audio = (
                Path(__file__).resolve().parents[2]
                / "lib/transcribing/src/testdata/apple-speech.wav"
            ).read_bytes()
            expected_raw = "The quick brown fox jumps over the lazy dog."
        catalog = requests.get(server.url + "/ai/v1/models", timeout=40)
        assert catalog.status_code == 200, catalog.text
        described = next(
            item
            for item in catalog.json()["data"]
            if item["id"] == "transcribers/speech"
        )
        assert described["dictation"]["live_preview"] is (provider == "antfly")
        assert described["dictation"]["stable_segments"] is (provider == "antfly")
        assert described["dictation"]["text_cleanup"] is True
        if provider == "apple":
            assert described["dictation"]["native_status"]["speech_available"]
        request = {
            "model": "transcribers/speech",
            "audio": base64.b64encode(audio).decode(),
            "cleanup_model": "generators/cleanup",
            "dictionary": ["Antfly"],
            "context": "Slack",
            "instructions": "Keep it brief",
            "style": "formal",
        }
        response = requests.post(
            server.url + "/ai/v1/dictate", json=request, timeout=40
        )
        assert response.status_code == 200, response.text
        value = response.json()
        assert value["text"] == "Antfly."
        assert value["transcript"]["text"] == expected_raw
        assert value["usage"]["total_tokens"] == 15
        prompt = state["cleanup"][-1]["messages"][0]["content"]
        assert "Antfly" in prompt and "Slack" in prompt and "Keep it brief" in prompt
        if provider == "antfly":
            assert state["recognition"][-1]["dictionary"] == ["Antfly"]

        if provider == "openai":
            hosted = next(
                item
                for item in catalog.json()["data"]
                if item["id"] == "connections/hosted-speech/models/whisper-1"
            )
            assert hosted["dictation"]["text_cleanup"] is True
            assert hosted["dictation"]["stable_segments"] is False
            hosted_result = requests.post(
                server.url + "/ai/v1/dictate",
                data=frame(
                    {**request, "model": hosted["id"], "audio": "attachment:0"}, audio
                ),
                headers={"Content-Type": "application/vnd.antfly.attachments.v1"},
                timeout=40,
            )
            assert hosted_result.status_code == 200, hosted_result.text
            assert hosted_result.json()["text"] == "Antfly."
            assert hosted_result.json()["transcript"]["text"] == expected_raw

        # Final session segments reuse shared cleanup without recognizing audio again.
        recognized_before = len(state["recognition"])
        segment_request = {**request, "audio": "", "transcript_text": "um ant fly"}
        cleaned_segment = requests.post(
            server.url + "/ai/v1/dictate", json=segment_request, timeout=40
        )
        assert cleaned_segment.status_code == 200, cleaned_segment.text
        assert cleaned_segment.json()["transcript"]["text"] == "um ant fly"
        assert cleaned_segment.json()["text"] == "Antfly."
        assert len(state["recognition"]) == recognized_before
        for invalid in [
            {**segment_request, "audio": request["audio"]},
            {**segment_request, "transcript_text": "x" * 65537},
        ]:
            rejected = requests.post(
                server.url + "/ai/v1/dictate", json=invalid, timeout=40
            )
            assert rejected.status_code == 400, rejected.text
        clean_before = len(state["cleanup"])
        empty_segment = requests.post(
            server.url + "/ai/v1/dictate",
            json={**segment_request, "transcript_text": ""},
            timeout=40,
        )
        assert (
            empty_segment.status_code == 200 and empty_segment.json()["text"] == ""
        ), empty_segment.text
        assert len(state["cleanup"]) == clean_before

        framed_result = requests.post(
            server.url + "/ai/v1/dictate",
            data=frame({**request, "audio": "attachment:0"}, audio),
            headers={"Content-Type": "application/vnd.antfly.attachments.v1"},
            timeout=40,
        )
        assert framed_result.status_code == 200, framed_result.text
        assert framed_result.json()["transcript"]["text"] == expected_raw

        before = len(state["cleanup"])
        response = requests.post(
            server.url + "/ai/v1/dictate",
            json={**request, "style": "verbatim"},
            timeout=40,
        )
        assert response.status_code == 200, response.text
        assert response.json()["text"] == expected_raw
        assert len(state["cleanup"]) == before

        response = requests.post(
            server.url + "/ai/v1/dictate", json={**request, "stream": True}, timeout=40
        )
        assert response.status_code == 200, response.text
        events = [
            json.loads(line[6:])
            for line in response.text.splitlines()
            if line.startswith("data: ") and line != "data: [DONE]"
        ]
        assert [event["type"] for event in events] == [
            "dictation.transcript",
            "dictation.delta",
            "dictation.completed",
        ]
        assert events[-1]["text"] == "Antfly."
        assert "data: [DONE]" in response.text

        if provider != "apple":
            # Silence is a successful empty result, never a malformed request or cleanup call.
            before = len(state["cleanup"])
            state["raw"] = ""
            empty = requests.post(
                server.url + "/ai/v1/dictate", json=request, timeout=40
            )
            assert empty.status_code == 200, empty.text
            assert (
                empty.json()["text"] == "" and empty.json()["transcript"]["text"] == ""
            )
            empty = requests.post(
                server.url + "/ai/v1/dictate",
                json={**request, "stream": True},
                timeout=40,
            )
            assert (
                empty.status_code == 200
                and '"type":"dictation.completed"' in empty.text
            ), empty.text
            assert len(state["cleanup"]) == before
            state["raw"] = expected_raw

            # The app's full ten-minute PCM16 clip travels as binary through the
            # gateway and native forwarder, without exhausting the metadata arena.
            wav_data = io.BytesIO()
            with wave.open(wav_data, "wb") as recording:
                recording.setnchannels(1)
                recording.setsampwidth(2)
                recording.setframerate(16000)
                recording.writeframes(bytes(16000 * 2 * 600))
            clip = wav_data.getvalue()
            metadata = {**request, "audio": "attachment:0"}
            long_result = requests.post(
                server.url + "/ai/v1/dictate",
                data=frame(metadata, clip),
                headers={"Content-Type": "application/vnd.antfly.attachments.v1"},
                timeout=60,
            )
            assert long_result.status_code == 200, long_result.text
            assert long_result.json()["text"] == "Antfly."
            if provider == "antfly":
                assert state["recognition"][-1]["recording_bytes"] == len(clip)

            before = len(state["recognition"])
            invalid = requests.post(
                server.url + "/ai/v1/dictate",
                data=frame({**metadata, "audio": "attachment:1"}, audio),
                headers={"Content-Type": "application/vnd.antfly.attachments.v1"},
                timeout=40,
            )
            assert invalid.status_code == 400 and len(state["recognition"]) == before, (
                invalid.text
            )
            invalid = requests.post(
                server.url + "/ai/v1/dictate", json=metadata, timeout=40
            )
            assert invalid.status_code == 400 and len(state["recognition"]) == before, (
                invalid.text
            )

        for reason in ["length", "content_filter", "tool_calls", None]:
            state["finish"] = reason
            rejected = requests.post(
                server.url + "/ai/v1/dictate",
                json={**request, "stream": True},
                timeout=40,
            )
            assert rejected.status_code == 200 and '"type":"error"' in rejected.text, (
                rejected.text
            )
            assert (
                "dictation.transcript" in rejected.text
                and "dictation.completed" not in rejected.text
            )
        rejected = requests.post(
            server.url + "/ai/v1/dictate", json=request, timeout=40
        )
        assert rejected.status_code >= 400, rejected.text
        state["finish"] = "stop"

        # The example's persistence contracts are real database APIs, not UI mocks.
        table = server.url + "/db/v1/tables/dictation"
        response = requests.post(
            table,
            json={
                "num_shards": 1,
                "indexes": {"search": {"type": "full_text", "field": "text"}},
            },
            timeout=40,
        )
        assert response.status_code in (200, 201), response.text
        response = requests.post(
            table + "/batch",
            json={
                "sync_level": "full_index",
                "inserts": {
                    "settings-default": {
                        "kind": "settings",
                        "configuration": {"version": 1, "dictionary": ["Antfly"]},
                    },
                    "transcript-1": {
                        "kind": "transcript",
                        "text": "Antfly dictation example",
                        "raw": "ant fly",
                    },
                },
            },
            timeout=40,
        )
        assert response.status_code in (200, 201), response.text
        response = requests.get(table + "/documents/settings-default", timeout=40)
        assert response.status_code == 200, response.text
        assert response.json()["configuration"]["dictionary"] == ["Antfly"]
        response = requests.post(
            table + "/query",
            json={"full_text_search": {"query": "Antfly"}, "limit": 50},
            timeout=40,
        )
        assert response.status_code == 200, response.text
        assert "transcript-1" in response.text, response.text
        response = requests.post(
            table + "/batch",
            json={"deletes": ["transcript-1"], "sync_level": "full_index"},
            timeout=40,
        )
        assert response.status_code in (200, 201), response.text

        # Apple generation readiness is independent of Speech readiness.
        if (
            provider == "apple"
            and described["dictation"]["native_status"]["generation_status"] == 0
        ):
            started = time.monotonic()
            response = requests.post(
                server.url + "/ai/v1/dictate",
                json={
                    **request,
                    "cleanup_model": "generators/native-cleanup",
                    "style": "clean",
                    "stream": True,
                },
                timeout=40,
                stream=True,
            )
            assert response.status_code == 200
            stages = {}
            completed = None
            for line in response.iter_lines(chunk_size=1):
                if not line.startswith(b"data:") or line[5:].strip() == b"[DONE]":
                    continue
                event = json.loads(line[5:])
                stages[event["type"]] = round(time.monotonic() - started, 3)
                assert event["type"] != "error", event
                if event["type"] == "dictation.completed":
                    completed = event
            assert completed is not None
            assert (
                "fox" in completed["text"].lower()
                and "dog" in completed["text"].lower()
            )
            print(f"Apple dictation stage seconds: {json.dumps(stages)}", flush=True)

        if (
            provider == "apple"
            and described["dictation"]["native_status"]["generation_status"] == 0
        ):
            limited = requests.post(
                server.url + "/ai/v1/dictate",
                json={
                    **request,
                    "cleanup_model": "generators/native-cleanup",
                    "max_tokens": 1,
                    "stream": True,
                },
                timeout=40,
            )
            assert (
                '"type":"error"' in limited.text
                and "dictation.completed" not in limited.text
            ), limited.text

        state["fail_cleanup"] = True
        response = requests.post(
            server.url + "/ai/v1/dictate", json={**request, "stream": True}, timeout=40
        )
        assert (
            "dictation.transcript" in response.text
            and '"type":"error"' in response.text
        )
        assert "dictation.completed" not in response.text

        if provider != "antfly":
            before = len(state["recognition"])
            response = requests.post(
                server.url + "/ai/v1/dictate",
                json={**request, "transcript_prompt": "unsupported"},
                timeout=40,
            )
            assert response.status_code == 400, response.text
            assert len(state["recognition"]) == before
    finally:
        if server is not None:
            server.stop()
        fixture.shutdown()
        fixture.server_close()
        thread.join(timeout=5)


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.skipif(
    os.environ.get("ANTFLY_TEST_WHISPER_SESSIONS") != "1",
    reason="Requires an installed Whisper model and ready native Apple cleanup",
)
def test_real_whisper_final_segments_clean_before_release_and_flush_tail(tmp_path):
    """Real session finals compose with native cleanup, without a second clip pass."""
    models_dir = Path.home() / ".antfly/inference/models"
    model = os.environ.get("ANTFLY_TEST_WHISPER_MODEL", "openai/whisper-tiny")
    if not (models_dir / model / "model_manifest.json").exists():
        pytest.skip("Whisper model is not installed; this test never downloads models")
    config = tmp_path / "live-config.json"
    config.write_text(
        json.dumps(
            {
                "public_inference": {"enabled": True, "default_timeout_ms": 120000},
                "resources": {
                    "inference": {"admission": {"max_concurrent_requests": 1}}
                },
                "inference": {"models_dir": str(models_dir)},
                "transcribers": {"speech": {"provider": "antfly", "model": model}},
                "generators": {"cleanup": {"provider": "apple", "max_tokens": 256}},
            }
        )
    )
    binary = harness.resolve_binary_path(
        os.environ.get("ANTFLY_BIN", str(harness.DEFAULT_ANTFLY_BIN))
    )
    server = harness.StandaloneAntflyServer(binary, "127.0.0.1", 0, config_path=config)
    session_id = None
    try:
        session = requests.post(
            server.url + "/ai/v1/transcription/sessions",
            json={
                "model": "transcribers/speech",
                "language": "en",
                "emit_partials": False,
                "max_segment_ms": 10000,
            },
            timeout=120,
        )
        assert session.status_code == 200, session.text
        session_id = session.json()["id"]
        path = server.url + f"/ai/v1/transcription/sessions/{session_id}"
        fixture = (
            Path(__file__).resolve().parents[2]
            / "lib/transcribing/src/testdata/apple-speech.wav"
        )
        with wave.open(str(fixture)) as audio:
            assert audio.getnchannels() == 1 and audio.getsampwidth() == 2
            rate = audio.getframerate()
            pcm = audio.readframes(audio.getnframes())
        samples = struct.unpack(f"<{len(pcm) // 2}h", pcm)
        phrase = struct.pack(
            f"<{len(samples)}f", *(sample / 32768 for sample in samples)
        )
        step = rate * 4 * 3 // 4

        def append(audio, commit=False):
            metadata = {"format": "pcm_f32", "sample_rate": rate, "commit": commit}
            if audio:
                metadata["audio"] = "attachment:0"
                encoded = json.dumps(metadata).encode()
                mime = b"audio/pcm"
                frame = (
                    b"AFATT001"
                    + struct.pack("<QII", len(encoded), 1, 0)
                    + struct.pack("<IIQ", len(mime), 0, len(audio))
                    + encoded
                    + mime
                    + audio
                )
                response = requests.post(
                    path + "/audio",
                    data=frame,
                    headers={"Content-Type": "application/vnd.antfly.attachments.v1"},
                    timeout=120,
                )
            else:
                response = requests.post(path + "/audio", json=metadata, timeout=120)
            assert response.status_code == 200, response.text
            return [
                event for event in response.json()["data"] if event["type"] == "final"
            ]

        def cleanup(event):
            response = requests.post(
                server.url + "/ai/v1/dictate",
                json={
                    "model": "transcribers/speech",
                    "audio": "",
                    "transcript_text": event["text"],
                    "cleanup_model": "generators/cleanup",
                    "style": "clean",
                },
                timeout=120,
            )
            assert response.status_code == 200, response.text
            assert response.json()["transcript"]["text"] == event["text"]
            return response.json()["text"]

        finals = []
        first_utterance = phrase + bytes(rate * 4)
        for offset in range(0, len(first_utterance), step):
            finals.extend(append(first_utterance[offset : offset + step]))
        assert finals, "VAD should finalize the first utterance before release"
        cleaned = [cleanup(event) for event in finals]
        assert "fox" in " ".join(cleaned).lower()
        for offset in range(0, len(phrase), step):
            emitted = append(phrase[offset : offset + step])
            finals.extend(emitted)
            cleaned.extend(cleanup(event) for event in emitted)
        tail = append(b"", commit=True)
        assert tail, "Release must flush the second utterance"
        assert tail[0]["sequence"] > finals[-1]["sequence"]
        cleaned.extend(cleanup(event) for event in tail)
        assembled = " ".join(cleaned).lower()
        assert assembled.count("fox") == 2 and assembled.count("dog") == 2, assembled
    finally:
        if session_id:
            requests.delete(
                server.url + f"/ai/v1/transcription/sessions/{session_id}", timeout=40
            )
        server.stop()


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.skipif(
    os.environ.get("ANTFLY_TEST_WHISPER_SESSIONS") != "1",
    reason="Requires an installed Whisper model; never downloads models",
)
@pytest.mark.parametrize("capacity", [1, 2])
def test_recorded_whisper_cleanup_uses_one_slot_per_stage(tmp_path, capacity):
    """A fresh composite request must work with one shared inference slot."""
    models_dir = Path.home() / ".antfly/inference/models"
    model = os.environ.get("ANTFLY_TEST_WHISPER_MODEL", "openai/whisper-tiny")
    if not (models_dir / model / "model_manifest.json").exists():
        pytest.skip("Whisper model is not installed")

    class Cleanup(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            body = json.dumps(
                {
                    "choices": [
                        {
                            "message": {"role": "assistant", "content": "Clean."},
                            "finish_reason": "stop",
                        }
                    ]
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    fixture = ThreadingHTTPServer(("127.0.0.1", 0), Cleanup)
    thread = threading.Thread(target=fixture.serve_forever, daemon=True)
    thread.start()
    config = tmp_path / "recorded-config.json"
    config.write_text(
        json.dumps(
            {
                "public_inference": {"enabled": True, "default_timeout_ms": 60000},
                "resources": {
                    "inference": {"admission": {"max_concurrent_requests": capacity}}
                },
                "inference": {"models_dir": str(models_dir)},
                "transcribers": {"speech": {"provider": "antfly", "model": model}},
                "generators": {
                    "cleanup": {
                        "provider": "openai",
                        "model": "fixture",
                        "url": f"http://127.0.0.1:{fixture.server_port}/v1",
                        "api_key": "fixture",
                        "max_tokens": 256,
                    }
                },
            }
        )
    )
    binary = harness.resolve_binary_path(
        os.environ.get("ANTFLY_BIN", str(harness.DEFAULT_ANTFLY_BIN))
    )
    server = None
    try:
        server = harness.StandaloneAntflyServer(
            binary, "127.0.0.1", 0, config_path=config
        )
        audio = (
            Path(__file__).resolve().parents[2]
            / "lib/transcribing/src/testdata/apple-speech.wav"
        ).read_bytes()
        for stream in [False, True]:
            metadata = json.dumps(
                {
                    "model": "transcribers/speech",
                    "audio": "attachment:0",
                    "language": "en",
                    "cleanup_model": "generators/cleanup",
                    "stream": stream,
                }
            ).encode()
            mime = b"audio/wav"
            body = (
                b"AFATT001"
                + struct.pack("<QII", len(metadata), 1, 0)
                + struct.pack("<IIQ", len(mime), 0, len(audio))
                + metadata
                + mime
                + audio
            )
            response = requests.post(
                server.url + "/ai/v1/dictate",
                data=body,
                headers={"Content-Type": "application/vnd.antfly.attachments.v1"},
                timeout=90,
            )
            assert response.status_code == 200, response.text
            if stream:
                assert '"type":"dictation.transcript"' in response.text
                assert '"type":"dictation.completed"' in response.text
                assert "[DONE]" in response.text
                assert '"type":"error"' not in response.text
            else:
                assert response.json()["text"] == "Clean."
                assert response.json()["transcript"]["text"].strip()
        response = requests.post(
            server.url + "/ai/v1/dictate",
            json={
                "model": "transcribers/speech",
                "audio": base64.b64encode(audio).decode(),
                "language": "en",
                "style": "verbatim",
            },
            timeout=90,
        )
        assert response.status_code == 200, response.text
    finally:
        if server is not None:
            server.stop()
        fixture.shutdown()
        fixture.server_close()
        thread.join(timeout=5)
