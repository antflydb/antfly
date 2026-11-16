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

"""Real audio model qualification, separate from model-free ABI probes.

Requires the existing EmbeddingGemma2 bundle (downloads follow the shared E2E
fixture policy). Uses production HTTP, range transport, decoder, model execution,
collectors, and repeated continuation. No probe terminates execution.
"""

import io
import hashlib
import json
import math
import os
import struct
import wave

import pytest
import requests

import conftest as harness


@pytest.mark.real_model
@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.parametrize("budget_mib", [128, 1024])
def test_real_audio_chunk_execution_and_continuation(
    tmp_path, monkeypatch, embeddinggemma2_model_available, budget_mib
):
    _ = embeddinggemma2_model_available
    stream = io.BytesIO()
    with wave.open(stream, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(16000)
        wav.writeframes(
            b"".join(
                struct.pack("<h", int(1000 * math.sin(2 * math.pi * 440 * i / 16000)))
                for i in range(3200)
            )
        )
    encoded = stream.getvalue()
    path = tmp_path / "audio.wav"
    path.write_bytes(encoded)
    server = None
    try:
        config = tmp_path / "config.json"
        config.write_text(
            json.dumps(
                {
                    "public_inference": {"enabled": True, "default_timeout_ms": 180000},
                    "remote_content": {"security": {"allowed_paths": [str(tmp_path)]}},
                }
            )
        )
        monkeypatch.setenv(
            "ANTFLY_CHUNK_MAX_WORKING_BYTES", str(budget_mib * 1024 * 1024)
        )
        binary = os.environ.get("ANTFLY_BIN", str(harness.DEFAULT_ANTFLY_BIN))
        server = harness.StandaloneAntflyServer(
            binary, "127.0.0.1", 0, config_path=config
        )
        request = {
            "input": {
                "type": "source",
                "uri": path.as_uri(),
                "mime_type": "audio/wav",
                "version": hashlib.sha256(encoded).hexdigest(),
                "version_kind": "file_sha256",
                "length": len(encoded),
            },
            "embedding": {"model": harness.EMBEDDINGGEMMA2_MODEL, "execute": True},
            "config": {"model": "fixed", "max_chunks": 1},
            "allow_partial": True,
            "policy": {"window_ns": 100000000, "step_ns": 100000000},
        }

        def page(body):
            response = requests.post(
                server.url + "/ai/v1/chunk", json=body, timeout=240
            )
            assert response.status_code == 200, response.text + server.debug_logs()
            value = response.json()
            assert value.get("error") is None, value
            assert value["data"], value
            for chunk in value["data"]:
                assert chunk.get("embedding"), chunk
                assert all(math.isfinite(v) for v in chunk["embedding"]), chunk
            return value

        first = page(request)
        assert first["next_continuation"], first
        resumed_request = {**request, "continuation": first["next_continuation"]}
        second = page(resumed_request)
        replayed = page(resumed_request)
        assert first["covered_through_ordinal"] == second["start_ordinal"]
        assert first["plan_id"] == second["plan_id"] == replayed["plan_id"]
        assert (
            first["source_fingerprint"]
            == second["source_fingerprint"]
            == replayed["source_fingerprint"]
        )
        assert second["source_complete"], second
        assert second["covered_through_ordinal"] == replayed["covered_through_ordinal"]
        assert second["data"][0]["embedding"] == pytest.approx(
            replayed["data"][0]["embedding"], abs=1e-5
        )
    finally:
        if server:
            server.stop()
