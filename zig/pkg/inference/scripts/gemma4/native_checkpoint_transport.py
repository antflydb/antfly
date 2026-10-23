"""Bounded AFATT001 transport helpers for native-checkpoint qualification.

Integrity checking is independent of the Zig encoder. Model compatibility and
payload semantics are checked by the server against its loaded model.
"""
import hashlib
import http.client
import json
import os
import struct
from pathlib import Path

CONTENT_TYPE = 'application/vnd.antfly.attachments.v1'
MIME = b'application/vnd.antfly.kv-checkpoint'
MAX_METADATA = 2 * 1024 * 1024
MAX_CHECKPOINT = 2 * 1024 * 1024 * 1024
CHUNK = 64 * 1024


def read_exact(stream, size):
    chunks = []
    remaining = size
    while remaining:
        data = stream.read(min(remaining, CHUNK))
        if not data or len(data) > remaining:
            raise ValueError('truncated native checkpoint transfer')
        chunks.append(data)
        remaining -= len(data)
    return b''.join(chunks)


def envelope_prefix(metadata, checkpoint_bytes):
    data = json.dumps(metadata, separators=(',', ':'), allow_nan=False).encode()
    if not 0 < len(data) <= MAX_METADATA or not 184 <= checkpoint_bytes <= MAX_CHECKPOINT:
        raise ValueError('native checkpoint transfer limit')
    return struct.pack('<8sQIIIIQ', b'AFATT001', len(data), 1, 0, len(MIME), 0, checkpoint_bytes) + data + MIME


def receive_checkpoint(response, destination, *, allow_rotated4=False):
    """Publish after bounded framing, integrity and EOF; V4 needs explicit opt-in."""
    versions = (2, 3, 4) if allow_rotated4 else (2, 3)
    magic, metadata_bytes, count, flags, mime_bytes, attachment_flags, size = struct.unpack('<8sQIIIIQ', read_exact(response, 40))
    if (magic, count, flags, mime_bytes, attachment_flags) != (b'AFATT001', 1, 0, len(MIME), 0):
        raise ValueError('invalid native checkpoint envelope')
    if not 0 < metadata_bytes <= MAX_METADATA or not 184 <= size <= MAX_CHECKPOINT:
        raise ValueError('native checkpoint transfer limit')
    metadata = json.loads(read_exact(response, metadata_bytes))
    if not isinstance(metadata, dict):
        raise ValueError('invalid checkpoint metadata')
    if read_exact(response, mime_bytes) != MIME:
        raise ValueError('invalid checkpoint MIME')
    if metadata.get('version') != 'attention_match_v1' or metadata.get('wire_version') not in versions or metadata.get('checkpoint_bytes') != size:
        raise ValueError('checkpoint metadata does not attest payload')
    if metadata.get('outcome') not in ('compacted', 'unchanged_insufficient_savings'):
        raise ValueError('invalid checkpoint outcome')
    destination = Path(destination)
    partial = destination.with_name(destination.name + '.part')
    # Exclusive creation prevents concurrent transfers from sharing staging.
    created = False
    try:
        with partial.open('xb') as output:
            created = True
            header = read_exact(response, 152)
            magic, version, reserved, payload_bytes = struct.unpack('<8sIIQ', header[:24])
            if (magic != b'ANTFLYKV' or version not in versions or payload_bytes != size - 184
                    or (version in (2, 4) and reserved != 0) or (version == 3 and reserved not in (0, 1))
                    or version != metadata['wire_version']
                    or (version == 4 and metadata.get('encoding') != 'rotated4')
                    or (version == 2 and metadata.get('encoding', 'f32') != 'f32')
                    or (version == 3 and metadata.get('encoding') != ('fp8' if reserved else 'f16'))):
                raise ValueError('invalid native checkpoint header')
            integrity = hashlib.sha256(header)
            file_hash = hashlib.sha256(header)
            output.write(header)
            remaining = size - 184
            while remaining:
                data = read_exact(response, min(CHUNK, remaining))
                integrity.update(data)
                file_hash.update(data)
                output.write(data)
                remaining -= len(data)
            footer = read_exact(response, 32)
            if integrity.digest() != footer:
                raise ValueError('native checkpoint checksum mismatch')
            output.write(footer)
            file_hash.update(footer)
            if response.read(1):
                raise ValueError('trailing native checkpoint data')
            output.flush()
            os.fsync(output.fileno())
        os.replace(partial, destination)
    finally:
        if created:
            partial.unlink(missing_ok=True)
    return {'metadata': metadata, 'file_sha256': file_hash.hexdigest(), 'bytes': size,
            'identity': {name: header[24 + i * 32:56 + i * 32].hex() for i, name in enumerate(('weights', 'tokenizer', 'template', 'layout'))}}


def send_resume(connection, path, metadata, checkpoint, mutate=None):
    """Stream a checkpoint from disk without a file-sized request allocation."""
    checkpoint = Path(checkpoint)
    with checkpoint.open('rb') as source:
        size = os.fstat(source.fileno()).st_size
        prefix = envelope_prefix(metadata, size)
        connection.putrequest('POST', path)
        connection.putheader('Content-Type', CONTENT_TYPE)
        connection.putheader('Content-Length', str(len(prefix) + size))
        connection.endheaders()
        try:
            connection.send(prefix)
            offset = 0
            while offset < size:
                block = read_exact(source, min(CHUNK, size - offset))
                if mutate is not None:
                    expected_length = len(block)
                    block = mutate(offset, block)
                    if len(block) != expected_length:
                        raise ValueError('test mutation must preserve transfer length')
                connection.send(block)
                offset += len(block)
        except (BrokenPipeError, ConnectionResetError) as upload_error:
            # A server can reject metadata and close before consuming the file.
            # Preserve that HTTP rejection, but never accept a partial upload.
            try:
                response = connection.getresponse()
            except (OSError, http.client.HTTPException) as response_error:
                raise upload_error from response_error
            if response.status < 400:
                response.close()
                raise upload_error
            return response
        if source.read(1):
            raise ValueError('checkpoint changed while uploading')
    return connection.getresponse()
