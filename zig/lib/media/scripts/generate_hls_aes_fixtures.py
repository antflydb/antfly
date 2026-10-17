#!/usr/bin/env python3
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

"""Independent OpenSSL AES-128-CBC/PKCS7 HLS qualification fixtures.

Keys are public synthetic bytes. OpenSSL is a qualification tool only.
"""

import hashlib
import json
from pathlib import Path
import struct
import subprocess

ROOT = Path(__file__).resolve().parents[4]
OUTPUT = ROOT / "zig/lib/media/testdata/hls/aes"


def sha(data):
    return hashlib.sha256(data).hexdigest()


def encrypt(data, key, iv):
    return subprocess.run(
        ["openssl", "enc", "-aes-128-cbc", "-K", key.hex(), "-iv", iv.hex()],
        input=data,
        stdout=subprocess.PIPE,
        check=True,
    ).stdout


def split(data):
    cursor = 0
    fragments = []
    while cursor < len(data):
        size, kind = struct.unpack_from(">I4s", data, cursor)
        if size < 8 or cursor + size > len(data):
            raise ValueError("invalid fixture box")
        if kind == b"moof":
            fragments.append(cursor)
        cursor += size
    if not fragments:
        raise ValueError("expected source fragments")
    return fragments


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    source = ROOT / "zig/lib/media/testdata/fragmented.mp4"
    changed = ROOT / "zig/lib/media/testdata/live-reconfigure.mp4"
    data, new_data = source.read_bytes(), changed.read_bytes()
    offsets, new_offsets = split(data), split(new_data)
    if len(offsets) < 3:
        raise ValueError("original source must have three fragments")
    new_end = new_offsets[1] if len(new_offsets) > 1 else len(new_data)
    key1, key2 = bytes(range(16)), bytes(range(16, 32))
    cases = {
        "init1": (data[: offsets[0]], key1, 1),
        "media10": (data[offsets[0] : offsets[1]], key1, 10),
        "media11": (data[offsets[1] : offsets[2]], key2, 11),
        "init2": (new_data[: new_offsets[0]], key2, 2),
        "changed12": (new_data[new_offsets[0] : new_end], key2, 12),
        "reused10": (data[offsets[0] : offsets[1]], key2, 10),
    }
    manifest = {
        "generator_sha256": sha(Path(__file__).read_bytes()),
        "openssl": subprocess.check_output(["openssl", "version"], text=True).strip(),
        "source_sha256": sha(data),
        "changed_source_sha256": sha(new_data),
        "source_offsets": offsets,
        "changed_offsets": new_offsets,
        "cases": {},
        "vectors": [],
    }
    for name, (plain, key, iv) in cases.items():
        cipher = encrypt(plain, key, iv.to_bytes(16, "big"))
        (OUTPUT / (name + ".bin")).write_bytes(cipher)
        manifest["cases"][name] = {
            "plaintext_sha256": sha(plain),
            "ciphertext_sha256": sha(cipher),
            "plaintext_bytes": len(plain),
            "key": key.hex(),
            "iv": iv,
        }
    vectors = bytearray()
    for length in (0, 1, 15, 16, 17, 31, 32, 33, 64, 65, 4097):
        plain = bytes((i * 37 + 11) % 256 for i in range(length))
        iv = bytes(range(15, -1, -1))
        cipher = encrypt(plain, key1, iv)
        vectors.extend(struct.pack("<II", length, len(cipher)))
        vectors.extend(cipher)
        manifest["vectors"].append(
            {
                "plaintext_bytes": length,
                "ciphertext_bytes": len(cipher),
                "ciphertext_sha256": sha(cipher),
            }
        )
    (OUTPUT / "vectors.bin").write_bytes(vectors)
    (OUTPUT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
