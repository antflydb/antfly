"""Bounded portable KV checkpoint transport. Model compatibility is server-validated."""

from __future__ import annotations

import hashlib
import json
import math
import os
import struct
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Protocol

CONTENT_TYPE = "application/vnd.antfly.attachments.v1"
CHECKPOINT_MIME = b"application/vnd.antfly.kv-checkpoint"
MAX_METADATA_BYTES = 2 * 1024 * 1024
MAX_CHECKPOINT_BYTES = 2 * 1024 * 1024 * 1024
CHUNK_BYTES = 64 * 1024


@dataclass(frozen=True)
class NativeKvCheckpoint:
    """A checksum-verified checkpoint saved atomically to ``path``.

    ``metadata['outcome']`` distinguishes compaction from an unchanged fallback.
    Fingerprints identify the model revision; the server checks compatibility
    and payload semantics on every resume. This object does not authorize reuse
    with another model or attest task quality.
    """

    path: Path
    metadata: Mapping[str, Any]
    sha256: str
    identity: Mapping[str, str]


class Reader(Protocol):
    def read(self, size: int, /) -> bytes: ...


class ChunkReader:
    """Adapt bounded HTTP chunks without accumulating a response-sized buffer."""

    def __init__(self, chunks: Iterator[bytes]):
        self.chunks = iter(chunks)
        self.pending = b""
        self.offset = 0

    def read(self, size: int) -> bytes:
        if size <= 0:
            return b""
        while self.offset == len(self.pending):
            try:
                self.pending = next(self.chunks)
            except StopIteration:
                return b""
            self.offset = 0
            if not self.pending:
                continue
            if len(self.pending) > CHUNK_BYTES:
                raise ValueError("HTTP checkpoint chunk exceeds the bounded reader capacity")
        end = min(len(self.pending), self.offset + size)
        result = self.pending[self.offset : end]
        self.offset = end
        return result


def _read_exact(reader: Reader, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        data = reader.read(min(CHUNK_BYTES, size - len(result)))
        if not data or len(data) > size - len(result):
            raise ValueError("truncated native checkpoint transfer")
        result.extend(data)
    return bytes(result)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate checkpoint metadata field")
        result[key] = value
    return result


def _invalid_constant(_: str) -> Any:
    raise ValueError("nonfinite checkpoint metadata")


def _validate_metadata(metadata: Any, size: int, model: str) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        raise ValueError("checkpoint metadata must be an object")
    if (metadata.get("version"), metadata.get("wire_version"), metadata.get("model")) != (
        "attention_match_v1",
        2,
        model,
    ):
        raise ValueError("checkpoint metadata has an unexpected version or model")
    for name in ("wire_version", "logical_tokens", "checkpoint_bytes", "validated_heads", "preserved_heads"):
        if type(metadata.get(name)) is not int or metadata[name] < 0:
            raise ValueError(f"invalid checkpoint metadata field: {name}")
    if metadata["checkpoint_bytes"] != size or not 0 < metadata["logical_tokens"] <= 32768:
        raise ValueError("checkpoint metadata has an invalid size or logical length")
    ratio = metadata.get("global_savings_ratio")
    if isinstance(ratio, bool) or not isinstance(ratio, (int, float)):
        raise ValueError("invalid checkpoint savings ratio")
    try:
        ratio = float(ratio)
    except OverflowError as exc:
        raise ValueError("invalid checkpoint savings ratio") from exc
    if not math.isfinite(ratio):
        raise ValueError("invalid checkpoint savings ratio")
    outcome = metadata.get("outcome")
    if outcome == "compacted":
        if ratio < 1.5 or metadata["validated_heads"] == 0:
            raise ValueError("checkpoint does not attest successful compaction")
    elif outcome == "unchanged_insufficient_savings":
        if ratio != 1 or metadata["validated_heads"] != 0:
            raise ValueError("invalid unchanged checkpoint outcome")
    else:
        raise ValueError("unsupported checkpoint outcome")
    return metadata


def _copy_checkpoint(reader: Reader, size: int, output: BinaryIO | None = None) -> tuple[str, dict[str, str], int]:
    if not 192 <= size <= MAX_CHECKPOINT_BYTES:
        raise ValueError("native checkpoint exceeds file limits")
    header = _read_exact(reader, 152)
    if struct.unpack("<8sIIQ", header[:24]) != (b"ANTFLYKV", 2, 0, size - 184):
        raise ValueError("invalid native checkpoint header")
    integrity = hashlib.sha256(header)
    file_hash = hashlib.sha256(header)
    if output is not None:
        output.write(header)
    remaining = size - 184
    logical_tokens: int | None = None
    while remaining:
        block = _read_exact(reader, min(CHUNK_BYTES, remaining))
        if logical_tokens is None:
            logical_tokens = struct.unpack("<Q", block[:8])[0]
        integrity.update(block)
        file_hash.update(block)
        if output is not None:
            output.write(block)
        remaining -= len(block)
    footer = _read_exact(reader, 32)
    if integrity.digest() != footer:
        raise ValueError("native checkpoint checksum mismatch")
    if output is not None:
        output.write(footer)
    file_hash.update(footer)
    assert logical_tokens is not None
    if not 0 < logical_tokens <= 32768:
        raise ValueError("native checkpoint logical length is invalid")
    identity = {
        name: header[24 + i * 32 : 56 + i * 32].hex()
        for i, name in enumerate(("weights", "tokenizer", "template", "layout"))
    }
    return file_hash.hexdigest(), identity, logical_tokens


def receive_checkpoint(reader: Reader, destination: str | Path, model: str) -> NativeKvCheckpoint:
    """Leave an existing destination intact on any failure before atomic publication."""
    header = struct.unpack("<8sQIIIIQ", _read_exact(reader, 40))
    magic, metadata_bytes, count, flags, mime_bytes, attachment_flags, size = header
    if (magic, count, flags, mime_bytes, attachment_flags) != (b"AFATT001", 1, 0, len(CHECKPOINT_MIME), 0):
        raise ValueError("invalid native checkpoint envelope")
    if not 0 < metadata_bytes <= MAX_METADATA_BYTES or not 192 <= size <= MAX_CHECKPOINT_BYTES:
        raise ValueError("native checkpoint transfer exceeds limits")
    metadata = _validate_metadata(
        json.loads(
            _read_exact(reader, metadata_bytes), object_pairs_hook=_unique_object, parse_constant=_invalid_constant
        ),
        size,
        model,
    )
    if _read_exact(reader, mime_bytes) != CHECKPOINT_MIME:
        raise ValueError("invalid checkpoint attachment MIME")
    destination = Path(destination)
    partial = destination.with_name(destination.name + ".part")
    created = False
    try:
        with partial.open("xb") as output:
            created = True
            digest, identity, logical_tokens = _copy_checkpoint(reader, size, output)
            if logical_tokens != metadata["logical_tokens"]:
                raise ValueError("checkpoint metadata disagrees with saved logical length")
            if reader.read(1):
                raise ValueError("trailing native checkpoint data")
            output.flush()
            os.fsync(output.fileno())
        os.replace(partial, destination)
    finally:
        if created:
            partial.unlink(missing_ok=True)
    return NativeKvCheckpoint(destination, metadata, digest, identity)


def checkpoint_upload(source: BinaryIO, metadata: dict[str, Any]) -> tuple[int, Iterator[bytes]]:
    """Validate a file before sending; stream the same open descriptor without replay."""
    before = os.fstat(source.fileno())
    size = before.st_size
    _copy_checkpoint(source, size)
    if source.read(1):
        raise ValueError("trailing native checkpoint data")
    after = os.fstat(source.fileno())
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError("checkpoint changed during validation")
    source.seek(0)
    encoded = json.dumps(metadata, separators=(",", ":"), allow_nan=False).encode()
    if not 0 < len(encoded) <= MAX_METADATA_BYTES:
        raise ValueError("native checkpoint metadata exceeds limits")
    prefix = struct.pack("<8sQIIIIQ", b"AFATT001", len(encoded), 1, 0, len(CHECKPOINT_MIME), 0, size)
    prefix += encoded + CHECKPOINT_MIME

    def chunks() -> Iterator[bytes]:
        for offset in range(0, len(prefix), CHUNK_BYTES):
            yield prefix[offset : offset + CHUNK_BYTES]
        remaining = size
        while remaining:
            block = _read_exact(source, min(CHUNK_BYTES, remaining))
            yield block
            remaining -= len(block)
        end = os.fstat(source.fileno())
        if source.read(1) or (before.st_size, before.st_mtime_ns) != (end.st_size, end.st_mtime_ns):
            raise ValueError("checkpoint changed during upload")

    return len(prefix) + size, chunks()
