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

import hashlib
import io
import json

import pytest

import assets


class Response(io.BytesIO):
    status = 206

    def __init__(self, content, start, end, total):
        super().__init__(content[start:end])
        self.headers = {"Content-Range": f"bytes {start}-{end - 1}/{total}"}


def test_range_acquisition_publishes_only_verified_content(tmp_path, monkeypatch):
    payload = bytes(range(256)) * 17
    entry = {"size_bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}
    ranges = []

    def request(req, timeout):
        start, end = map(int, req.get_header("Range").removeprefix("bytes=").split("-"))
        ranges.append((start, end))
        return Response(payload, start, end + 1, len(payload))

    monkeypatch.setattr(assets.urllib.request, "urlopen", request)
    path = tmp_path / "model"
    assets.download_file(
        "https://example.test/model", path, entry, workers=3, part_bytes=512
    )
    assert path.read_bytes() == payload
    assert len(ranges) == 9
    assert not path.with_name(path.name + ".memorilla-part").exists()
    assert not path.with_name(path.name + ".memorilla-ranges.json").exists()
    # Verified output is reused without touching the network.
    assets.download_file("https://example.test/model", path, entry)
    assert len(ranges) == 9


def test_incorrect_artifact_is_not_reused(tmp_path):
    path = tmp_path / "model"
    path.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum mismatch"):
        assets.download_file(
            "https://example.test/model", path, {"size_bytes": 7, "sha256": "0" * 64}
        )


def test_git_blob_identity_is_verified(tmp_path):
    path = tmp_path / "config.json"
    payload = b'{"model_type":"qwen3"}'
    path.write_bytes(payload)
    entry = {
        "size_bytes": len(payload),
        "git_blob_sha1": hashlib.sha1(
            f"blob {len(payload)}\0".encode() + payload
        ).hexdigest(),
    }
    assets.verify_file(path, entry)
    path.write_bytes(payload.replace(b"qwen3", b"qwen2"))
    with pytest.raises(ValueError, match="checksum mismatch"):
        assets.verify_file(path, entry)


def test_incompatible_resume_state_is_rejected(tmp_path):
    path = tmp_path / "model"
    path.with_name(path.name + ".memorilla-ranges.json").write_text(
        json.dumps({"identity": {}, "completed": []})
    )
    with pytest.raises(ValueError, match="incompatible download resume"):
        assets.download_file(
            "https://example.test/model", path, {"size_bytes": 5, "sha256": "0" * 64}
        )


def test_lock_pins_complete_exact_pair():
    encoder, decoder = assets.lock_model("encoder"), assets.lock_model("decoder")
    assert encoder["model"] == "Qwen/Qwen3-Embedding-4B"
    assert decoder["model"] == "Qwen/Qwen3-8B"
    assert len([x for x in encoder["files"] if x["path"].endswith(".safetensors")]) == 2
    assert len([x for x in decoder["files"] if x["path"].endswith(".safetensors")]) == 5


def test_wrong_range_never_publishes_and_retries_have_distinct_cache_keys(
    tmp_path, monkeypatch
):
    urls = []

    def wrong_response(request, timeout):
        urls.append(request.full_url)
        return Response(b"broken", 0, 2, 999)

    monkeypatch.setattr(assets.urllib.request, "urlopen", wrong_response)
    monkeypatch.setattr(assets.time, "sleep", lambda _: None)
    path = tmp_path / "model"
    entry = {"size_bytes": 6, "sha256": hashlib.sha256(b"broken").hexdigest()}
    with pytest.raises(ValueError, match="different byte range"):
        assets.download_file("https://example.test/model", path, entry, workers=1)
    assert not path.exists()
    assert len(urls) == 4
    assert len(set(urls)) == 4


def test_shorter_ranges_preserve_completed_bytes_including_final_partial(
    tmp_path, monkeypatch
):
    payload = bytes(range(256)) * 17
    entry = {"size_bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}
    url = "https://example.test/model"
    target = tmp_path / "model"
    old_part_bytes, new_part_bytes = 512, 128
    completed = [0, 2, 8]
    partial_bytes = bytearray(len(payload))
    for index in completed:
        start, stop = (
            index * old_part_bytes,
            min(len(payload), (index + 1) * old_part_bytes),
        )
        partial_bytes[start:stop] = payload[start:stop]
    target.with_name(target.name + ".memorilla-part").write_bytes(partial_bytes)
    target.with_name(target.name + ".memorilla-ranges.json").write_text(
        json.dumps(
            {
                "identity": {
                    "url": url,
                    "artifact": entry,
                    "part_bytes": old_part_bytes,
                },
                "completed": completed,
            }
        )
    )
    fetched = []

    def request(req, timeout):
        start, end = map(int, req.get_header("Range").removeprefix("bytes=").split("-"))
        fetched.append(start)
        return Response(payload, start, end + 1, len(payload))

    monkeypatch.setattr(assets.urllib.request, "urlopen", request)
    assets.download_file(url, target, entry, workers=3, part_bytes=new_part_bytes)
    assert target.read_bytes() == payload
    assert all(start // old_part_bytes not in completed for start in fetched)
    assert len(fetched) == 24
