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

"""Extract benchmark token IDs from an already qualified V2/V3/V4 checkpoint.

This verifies bounded framing, token counts/IDs, model identity, the full integrity
footer and the digest recorded by qualification. It does not revalidate numerical
KV payload semantics; the server's successful roundtrip supplies that evidence.
"""

import hashlib
import json
from pathlib import Path
import struct
import sys

sys.path.insert(0, str(Path.cwd() / "zig/pkg/inference/scripts/gemma4"))
from native_checkpoint_transport import CHUNK, MAX_CHECKPOINT, read_exact

MODEL_IDENTITY = {
    "weights": "fd904d45d8efe0c9c71bc74586f43b6f48334c65c164bdcc0b4963b4a6707987",
    "tokenizer": "68550a189bc1bb931accaedd10d8f7b6818a357730714e52f790b6b6d1cf65cb",
    "template": "611d5f75642790e55749a538066e24a7f13b6a1b58697c855ed078a148ee5dbb",
    "layout": "01ab7e3601f70c514b6a5c6fb1d40af6eca5b81f15ede879e8666ee46192e947",
}
LOGICAL_TOKENS = {"8k": 8189, "32k": 32752}


def extract_tokens(path, expected_sha256, expected_identity):
    path = Path(path)
    size = path.stat().st_size
    if not 208 <= size <= MAX_CHECKPOINT:
        raise ValueError("checkpoint file exceeds limits")
    with path.open("rb") as source:
        header = read_exact(source, 152)
        magic, version, flags, payload_bytes = struct.unpack("<8sIIQ", header[:24])
        if (
            magic != b"ANTFLYKV"
            or version not in (2, 3, 4)
            or payload_bytes != size - 184
            or (version in (2, 4) and flags != 0)
            or (version == 3 and flags not in (0, 1))
        ):
            raise ValueError("invalid native checkpoint header")
        identity = {
            name: header[24 + i * 32 : 56 + i * 32].hex()
            for i, name in enumerate(("weights", "tokenizer", "template", "layout"))
        }
        if identity != expected_identity:
            raise ValueError("model identity mismatch")
        counts = read_exact(source, 24)
        logical, tokens, vocab, layers, donors = struct.unpack("<QIIII", counts)
        if not (
            0 < logical <= 32768
            and tokens == logical
            and 0 < vocab <= 262144
            and 0 < layers <= 256
            and 0 < donors <= layers
        ):
            raise ValueError("invalid checkpoint metadata counts")
        minimum = 24 + 4 * (tokens + vocab + layers) + 12 * donors
        if minimum > size - 184:
            raise ValueError("checkpoint counts exceed payload")
        token_bytes = read_exact(source, tokens * 4)
        token_ids = list(struct.unpack("<" + "I" * tokens, token_bytes))
        if any(token >= vocab for token in token_ids):
            raise ValueError("token outside vocabulary")
        digest = hashlib.sha256(header)
        digest.update(counts)
        digest.update(token_bytes)
        remaining = size - 184 - 24 - len(token_bytes)
        while remaining:
            block = read_exact(source, min(CHUNK, remaining))
            digest.update(block)
            remaining -= len(block)
        footer = read_exact(source, 32)
        if digest.digest() != footer:
            raise ValueError("checkpoint integrity mismatch")
        if source.read(1):
            raise ValueError("checkpoint changed size during extraction")
        digest.update(footer)
        if digest.hexdigest() != expected_sha256:
            raise ValueError("checkpoint differs from qualified digest")
    return {
        "checkpoint": str(path.resolve()),
        "checkpoint_sha256_to_reverify": digest.hexdigest(),
        "checkpoint_bytes": size,
        "identity": identity,
        "logical_tokens": logical,
        "token_bytes_sha256": hashlib.sha256(token_bytes).hexdigest(),
        "token_ids": token_ids,
    }


def qualified_fixture(reference_path, checkpoint_path, size):
    reference = json.loads(Path(reference_path).read_text())
    if reference.get("passed") is not True:
        raise ValueError("reference campaign did not pass")
    prepared = [
        r["result"] for r in reference["exporter"]["requests"] if r["kind"] == "prepare"
    ]
    if len(prepared) != 1:
        raise ValueError("ambiguous reference checkpoint")
    prepared = prepared[0]
    fixture = extract_tokens(checkpoint_path, prepared["file_sha256"], MODEL_IDENTITY)
    if (
        fixture["logical_tokens"] != LOGICAL_TOKENS[size]
        or prepared["metadata"]["logical_tokens"] != fixture["logical_tokens"]
    ):
        raise ValueError("wrong designated workload length")
    if prepared["identity"] != fixture["identity"]:
        raise ValueError("reference model identity mismatch")
    fixture["label"] = size
    return reference, fixture
