"""Portable checkpoint streaming, publication, and cancellation contracts."""

import hashlib
import io
import json
import struct
from pathlib import Path

import httpx
import pytest

from antfly import AntflyClient, AntflyException, NativeKvCheckpoint
from antfly.client_generated.models import InferenceGenerateRequest
from antfly.native_kv import (
    CHECKPOINT_MIME,
    CHUNK_BYTES,
    CONTENT_TYPE,
    MAX_CHECKPOINT_BYTES,
    ChunkReader,
    checkpoint_upload,
    receive_checkpoint,
)


def fixture(payload_size=40, outcome="compacted"):
    payload = struct.pack("<Q", 12) + b"x" * payload_size
    header = struct.pack("<8sIIQ", b"ANTFLYKV", 2, 0, len(payload)) + bytes(range(128))
    checkpoint = header + payload + hashlib.sha256(header + payload).digest()
    metadata = {
        "version": "attention_match_v1",
        "wire_version": 2,
        "model": "gemma",
        "outcome": outcome,
        "logical_tokens": 12,
        "checkpoint_bytes": len(checkpoint),
        "global_savings_ratio": 2 if outcome == "compacted" else 1,
        "validated_heads": 2 if outcome == "compacted" else 0,
        "preserved_heads": 6,
    }
    encoded = json.dumps(metadata).encode()
    frame = struct.pack("<8sQIIIIQ", b"AFATT001", len(encoded), 1, 0, len(CHECKPOINT_MIME), 0, len(checkpoint))
    return frame + encoded + CHECKPOINT_MIME + checkpoint, checkpoint, metadata


class Stream(httpx.SyncByteStream):
    def __init__(self, data, *, fail=False):
        self.data = data
        self.fail = fail
        self.closed = False

    def __iter__(self):
        for i in range(0, len(self.data), 137):
            yield self.data[i : i + 137]
        if self.fail:
            raise httpx.ReadError("connection lost")

    def close(self):
        self.closed = True


def client_with_transport(handler):
    client = AntflyClient("http://test", token="secret")
    headers = client._client.get_httpx_client().headers
    client._client.set_httpx_client(
        httpx.Client(base_url="http://test", headers=headers, transport=httpx.MockTransport(handler))
    )
    return client


@pytest.mark.parametrize("outcome", ["compacted", "unchanged_insufficient_savings"])
def test_export_streams_and_publishes_verified_checkpoint(tmp_path, outcome):
    frame, checkpoint, metadata = fixture(CHUNK_BYTES * 3, outcome)
    destination = tmp_path / "saved.kv"
    destination.write_bytes(b"previous")
    stream = Stream(frame)

    def handle(request):
        assert request.headers["Authorization"] == "Bearer secret"
        assert request.headers["Accept"] == CONTENT_TYPE
        assert json.loads(request.read())["stream"] is False
        assert destination.read_bytes() == b"previous"
        return httpx.Response(200, headers={"Content-Type": CONTENT_TYPE}, stream=stream)

    client = client_with_transport(handle)
    result = client.prepare_native_kv_checkpoint(InferenceGenerateRequest(model="gemma", messages=[]), destination)
    assert isinstance(result, NativeKvCheckpoint)
    assert result.metadata == metadata
    assert destination.read_bytes() == checkpoint
    assert result.sha256 == hashlib.sha256(checkpoint).hexdigest()
    assert result.identity["weights"] == bytes(range(32)).hex()
    assert stream.closed
    assert not destination.with_suffix(".kv.part").exists()


def test_every_truncation_and_bad_integrity_preserve_existing_file(tmp_path):
    frame, _, _ = fixture()
    destination = tmp_path / "saved.kv"
    destination.write_bytes(b"previous")
    for n in range(len(frame)):
        with pytest.raises(ValueError):
            receive_checkpoint(io.BytesIO(frame[:n]), destination, "gemma")
        assert destination.read_bytes() == b"previous"
        assert not Path(str(destination) + ".part").exists()
    for offset in (0, 16, 20, 24, 28, frame.index(CHECKPOINT_MIME), len(frame) - 33, len(frame) - 1):
        corrupt = bytearray(frame)
        corrupt[offset] ^= 1
        with pytest.raises(ValueError):
            receive_checkpoint(io.BytesIO(corrupt), destination, "gemma")
        assert destination.read_bytes() == b"previous"
    with pytest.raises(ValueError, match="trailing"):
        receive_checkpoint(io.BytesIO(frame + b"extra"), destination, "gemma")


def test_failed_export_closes_response_and_keeps_previous_checkpoint(tmp_path):
    frame, _, _ = fixture()
    destination = tmp_path / "saved.kv"
    destination.write_bytes(b"previous")
    stream = Stream(frame, fail=True)
    client = client_with_transport(lambda _: httpx.Response(200, headers={"Content-Type": CONTENT_TYPE}, stream=stream))
    with pytest.raises(httpx.ReadError):
        client.prepare_native_kv_checkpoint(InferenceGenerateRequest(model="gemma", messages=[]), destination)
    assert stream.closed
    assert destination.read_bytes() == b"previous"
    assert not Path(str(destination) + ".part").exists()


def test_staging_ownership_limits_model_and_metadata_are_checked(tmp_path):
    frame, _, _ = fixture()
    destination = tmp_path / "saved.kv"
    partial = Path(str(destination) + ".part")
    partial.write_bytes(b"another writer")
    with pytest.raises(FileExistsError):
        receive_checkpoint(io.BytesIO(frame), destination, "gemma")
    assert partial.read_bytes() == b"another writer"
    partial.unlink()
    with pytest.raises(ValueError, match="model"):
        receive_checkpoint(io.BytesIO(frame), destination, "different-model")
    oversized = bytearray(frame[:40])
    struct.pack_into("<Q", oversized, 32, MAX_CHECKPOINT_BYTES + 1)
    with pytest.raises(ValueError, match="limits"):
        receive_checkpoint(io.BytesIO(oversized), destination, "gemma")
    mismatch = frame.replace(b'"logical_tokens": 12', b'"logical_tokens": 13')
    with pytest.raises(ValueError, match="logical length"):
        receive_checkpoint(io.BytesIO(mismatch), destination, "gemma")
    assert not destination.exists()
    assert not partial.exists()


def test_disk_failure_never_replaces_previous_checkpoint(tmp_path, monkeypatch):
    from antfly import native_kv

    frame, _, _ = fixture()
    destination = tmp_path / "saved.kv"
    destination.write_bytes(b"previous")

    def fail(_):
        raise OSError("disk failure")

    monkeypatch.setattr(native_kv.os, "fsync", fail)
    with pytest.raises(OSError):
        receive_checkpoint(io.BytesIO(frame), destination, "gemma")
    assert destination.read_bytes() == b"previous"
    assert not Path(str(destination) + ".part").exists()


def test_upload_validation_and_chunks_are_bounded(tmp_path):
    _, checkpoint, _ = fixture(CHUNK_BYTES * 3)
    path = tmp_path / "saved.kv"
    path.write_bytes(checkpoint)

    class BoundedFile:
        def __init__(self, file):
            self.file = file

        def read(self, n):
            assert 0 < n <= CHUNK_BYTES
            return self.file.read(n)

        def seek(self, offset):
            return self.file.seek(offset)

        def fileno(self):
            return self.file.fileno()

    with path.open("rb") as file:
        length, chunks = checkpoint_upload(BoundedFile(file), {"model": "gemma", "messages": []})
        blocks = list(chunks)
    assert all(len(block) <= CHUNK_BYTES for block in blocks)
    wire = b"".join(blocks)
    assert length == len(wire)
    assert wire.endswith(checkpoint)
    assert wire[:8] == b"AFATT001"
    path.write_bytes(checkpoint[:-1] + bytes([checkpoint[-1] ^ 1]))
    with path.open("rb") as source, pytest.raises(ValueError, match="checksum"):
        checkpoint_upload(source, {})


def test_resume_json_and_early_closed_sse_use_bounded_upload(tmp_path):
    _, checkpoint, _ = fixture(CHUNK_BYTES * 2)
    path = tmp_path / "saved.kv"
    path.write_bytes(checkpoint)
    requests = []
    stream = Stream(
        b'data: {"id":"c","object":"chat.completion.chunk","created":1,"model":"gemma",'
        b'"choices":[{"index":0,"delta":{"content":"ok"}}]}\n\ndata: [DONE]\n\n'
    )

    def handle(request):
        assert request.headers["Authorization"] == "Bearer secret"
        assert request.headers["Content-Type"] == CONTENT_TYPE
        wire = request.read()
        assert int(request.headers["Content-Length"]) == len(wire)
        assert wire.endswith(checkpoint)
        metadata_length = struct.unpack_from("<Q", wire, 8)[0]
        metadata = json.loads(wire[40 : 40 + metadata_length])
        requests.append(metadata)
        if metadata["stream"]:
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=stream)
        return httpx.Response(
            200,
            json={
                "id": "c",
                "object": "chat.completion",
                "created": 1,
                "model": "gemma",
                "choices": [],
                "usage": {"prompt_tokens": 12, "completion_tokens": 1, "total_tokens": 13, "cached_prompt_tokens": 12},
            },
        )

    client = client_with_transport(handle)
    request = InferenceGenerateRequest(model="gemma", messages=[])
    assert client.resume_native_kv_checkpoint(request, path).usage.cached_prompt_tokens == 12
    with client.resume_native_kv_checkpoint_stream(request, path) as chunks:
        assert next(chunks).choices[0].delta.content == "ok"
    assert stream.closed
    assert [item["stream"] for item in requests] == [False, True]
    assert path.read_bytes() == checkpoint


def test_resume_rejects_invalid_file_before_any_request(tmp_path):
    path = tmp_path / "saved.kv"
    path.write_bytes(b"broken")
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(500)

    client = client_with_transport(handle)
    with pytest.raises(AntflyException, match="file limits"):
        client.resume_native_kv_checkpoint(InferenceGenerateRequest(model="gemma", messages=[]), path)
    assert not calls


def test_chunk_reader_rejects_unbounded_source_chunk():
    reader = ChunkReader(iter([b"x" * (CHUNK_BYTES + 1)]))
    with pytest.raises(ValueError, match="bounded"):
        reader.read(1)


def test_chunk_reader_skips_empty_chunks_and_retains_partial_reads():
    reader = ChunkReader(iter([b"", b"abc", b"", b"d", b""]))
    assert reader.read(2) == b"ab"
    assert reader.read(2) == b"c"
    assert reader.read(2) == b"d"
    assert reader.read(2) == b""
