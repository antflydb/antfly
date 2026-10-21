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

"""Regenerate native Brotli dictionary fixtures using an independent test oracle.

Requires Google Brotli v1.2.0 built with BROTLI_EXPERIMENTAL for development only.
Normal builds/tests use the checked-in original-text fixtures and native Zig.
On macOS: cmake -S brotli-1.2.0 -B oracle -DCMAKE_BUILD_TYPE=Release
-DCMAKE_C_FLAGS=-DBROTLI_EXPERIMENTAL -DBUILD_SHARED_LIBS=ON; cmake --build oracle.
Then run this script with the oracle build directory and zig fmt the vectors.
"""

import argparse
import base64
import ctypes as c
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "reference",
    type=Path,
    help="Pinned Brotli v1.2.0 experimental shared-library build directory",
)
args = parser.parse_args()
REPO = Path(__file__).resolve().parents[4]
lib = c.CDLL(str(args.reference / "libbrotlienc.dylib"))


def fn(name, restype, *args):
    f = getattr(lib, name)
    f.restype = restype
    f.argtypes = args
    return f


ptr = c.c_void_p
size = c.c_size_t
u8 = c.c_uint8
p8 = c.POINTER(u8)
create = fn("BrotliEncoderCreateInstance", ptr, ptr, ptr, ptr)
setparam = fn("BrotliEncoderSetParameter", c.c_int, ptr, c.c_int, c.c_uint32)
prepare = fn(
    "BrotliEncoderPrepareDictionary", ptr, c.c_int, size, p8, c.c_int, ptr, ptr, ptr
)
attach = fn("BrotliEncoderAttachPreparedDictionary", c.c_int, ptr, ptr)
destroy = fn("BrotliEncoderDestroyInstance", None, ptr)
destroydict = fn("BrotliEncoderDestroyPreparedDictionary", None, ptr)
compress = fn(
    "BrotliEncoderCompressStream",
    c.c_int,
    ptr,
    c.c_int,
    c.POINTER(size),
    c.POINTER(p8),
    c.POINTER(size),
    c.POINTER(p8),
    c.POINTER(size),
)
finished = fn("BrotliEncoderIsFinished", c.c_int, ptr)


def encode(data, dictionary, quality=11, window=22):
    state = create(None, None, None)
    prepared = None
    source = (u8 * len(data)).from_buffer_copy(data)
    dictionary_buf = (u8 * len(dictionary)).from_buffer_copy(dictionary)
    try:
        for k, v in [(1, quality), (6, 1), (2, window)]:
            assert setparam(state, k, v)
        if dictionary:
            prepared = prepare(1, len(dictionary), dictionary_buf, 11, None, None, None)
            assert prepared
            assert attach(state, prepared)
        n = size(len(data))
        inp = c.cast(source, p8)
        out = (u8 * (len(data) * 2 + 65536))()
        outp = c.cast(out, p8)
        remaining = size(len(out))
        total = size()
        while not finished(state):
            assert compress(
                state,
                2,
                c.byref(n),
                c.byref(inp),
                c.byref(remaining),
                c.byref(outp),
                c.byref(total),
            )
        assert n.value == 0
        return bytes(out[: total.value])
    finally:
        destroy(state)
        if prepared:
            destroydict(prepared)


# Custom word lists use two words per occupied length (size_bits=1).
def dictionary(google, word_lists, transforms, pairs, context=None, prefix=b""):
    def vint(n):
        out = bytearray()
        while n > 127:
            out.append((n & 127) | 128)
            n >>= 7
        return bytes(out) + bytes([n])

    out = b"\x91\x00" + vint(len(prefix)) + prefix + bytes([len(word_lists)])
    for words in word_lists:
        bits = bytearray(28)
        payload = b""
        for length in range(4, 32):
            group = [w for w in words if len(w) == length]
            if group:
                assert len(group) == 2
                bits[length - 4] = 1
                payload += b"".join(group)
        out += bytes(bits) + payload
    out += bytes([len(transforms)])
    for operations in transforms:
        strings = b"\x01<\x01>\x00"
        out += len(strings).to_bytes(2, "little") + strings + bytes([len(operations)])
        out += b"".join(
            bytes([p, op, s] if google else [p, s, op])
            for p, s, op, param in operations
        )
        if any(op >= 21 for p, s, op, param in operations):
            out += b"".join(
                param.to_bytes(2, "little") for p, s, op, param in operations
            )
    if word_lists or transforms:
        out += (
            bytes([len(pairs)])
            + b"".join(bytes(p) for p in pairs)
            + bytes([context is not None])
        )
        if context is not None:
            out += bytes(context)
    return out


decoder = c.CDLL(str(args.reference / "libbrotlidec.dylib"))
dcreate = decoder.BrotliDecoderCreateInstance
dcreate.restype = ptr
dcreate.argtypes = [ptr, ptr, ptr]
dattach = decoder.BrotliDecoderAttachDictionary
dattach.restype = c.c_int
dattach.argtypes = [ptr, c.c_int, size, p8]
dstream = decoder.BrotliDecoderDecompressStream
dstream.restype = c.c_int
dstream.argtypes = [
    ptr,
    c.POINTER(size),
    c.POINTER(p8),
    c.POINTER(size),
    c.POINTER(p8),
    c.POINTER(size),
]
dparam = decoder.BrotliDecoderSetParameter
dparam.argtypes = [ptr, c.c_int, c.c_uint32]
ddestroy = decoder.BrotliDecoderDestroyInstance
ddestroy.argtypes = [ptr]


def verify(encoded, dictionary, plain):
    state = dcreate(None, None, None)
    assert dparam(state, 1, 1)
    dictionary_buffer = (u8 * len(dictionary)).from_buffer_copy(dictionary)
    source = (u8 * len(encoded)).from_buffer_copy(encoded)
    inp = c.cast(source, p8)
    n = size(len(encoded))
    output = (u8 * (len(plain) + 1024))()
    outp = c.cast(output, p8)
    available = size(len(output))
    total = size()
    try:
        assert dattach(state, 1, len(dictionary), dictionary_buffer)
        assert (
            dstream(
                state,
                c.byref(n),
                c.byref(inp),
                c.byref(available),
                c.byref(outp),
                c.byref(total),
            )
            == 1
        )
        assert n.value == 0 and bytes(output[: total.value]) == plain
    finally:
        ddestroy(state)


fixtures = []
words = [
    b"antfly",
    b"zigzag",
    b"customsearch",
    b"boundedcodec",
    b"abcdefghijklmnopqrstuvwxy123456",
    b"ABCDEFGHIJKLMNOPQRSTUVWXY123456",
]
operations = [(2, 2, i, 1 if i >= 21 else 0) for i in range(23)] + [(0, 1, 0, 0)]
variants = [
    ("custom", [words], [operations], [(0, 0)], None, b""),
    (
        "mixed",
        [words],
        [operations],
        [(0, 0), (1, 1)],
        None,
        b"Antfly original raw dictionary text." * 100,
    ),
    (
        "context",
        [words],
        [operations],
        [(1, 1), (0, 0)],
        [i % 2 for i in range(64)],
        b"",
    ),
    ("builtin_words", [], [operations], [(0, 0)], None, b""),
    ("builtin_transforms", [words], [], [(0, 0)], None, b""),
]
for name, wl, tl, pairs, ctx, prefix in variants:
    gd = dictionary(True, wl, tl, pairs, ctx, prefix)
    rd = dictionary(False, wl, tl, pairs, ctx, prefix)
    raw = (
        b"antfly zigzag <customsearch> BOUNDEDCODEC "
        + words[-2]
        + b" "
        + prefix[:120]
        + b" time Time first source code function search dictionary.\n"
    ) * 12
    encoded = encode(raw, gd)
    verify(encoded, gd, raw)
    fixtures.append(
        {
            "name": name,
            "compressed": base64.b64encode(encoded).decode(),
            "plain": base64.b64encode(raw).decode(),
            "dictionary": base64.b64encode(rd).decode(),
            "google_dictionary": base64.b64encode(gd).decode(),
        }
    )
(REPO / "zig/pkg/antfly-grep/tests/brotli_serialized_fixtures.json").write_text(
    json.dumps(
        {
            "reference": "Google Brotli 1.2.0 encoder; original Antfly text. RFC triplet order is an explicit transposition of Google wire order.",
            "fixtures": fixtures,
        },
        indent=2,
    )
    + "\n"
)
print([(f["name"], len(base64.b64decode(f["compressed"]))) for f in fixtures])


class Transforms(c.Structure):
    _fields_ = [
        ("size", c.c_uint16),
        ("strings", c.c_void_p),
        ("offsets", c.c_void_p),
        ("n", c.c_uint32),
        ("triplets", c.c_void_p),
        ("params", c.c_void_p),
        ("cutoff", c.c_int16 * 10),
    ]


lib = c.CDLL(str(args.reference / "libbrotlicommon.dylib"))
f = lib.BrotliTransformDictionaryWord
f.restype = c.c_int
f.argtypes = [c.c_void_p, c.c_void_p, c.c_int, c.POINTER(Transforms), c.c_int]
words = [
    b"antfly",
    "éAb中😀".encode(),
    bytes([0, 0x80, 0xC0, 0x41, 0xE0, 0x22, 0x43, 0xF0, 0x21, 0x42, 0x63, 0xF8]),
    b"abcdefghijklmnopqrstuvwxy1234567",
]
strings = c.create_string_buffer(b"\x01<\x01>\0")
offsets = (c.c_uint16 * 3)(0, 2, 4)


def z(b):
    return '"' + "".join(f"\\x{x:02x}" for x in b) + '"'


header = (
    (REPO / "zig/pkg/antfly-grep/src/options.zig").read_text().split("const std")[0]
)
rows = []
for word in words:
    for op in range(23):
        for parameter in [0, 1, 0x7FFF, 0x8000, 0xFFFF] if op >= 21 else [0]:
            triplets = (c.c_uint8 * 3)(0, op, 1)
            params = (c.c_uint16 * 1)(parameter)
            t = Transforms(
                5,
                c.addressof(strings),
                c.addressof(offsets),
                1,
                c.addressof(triplets),
                c.addressof(params),
                (c.c_int16 * 10)(),
            )
            out = c.create_string_buffer(1024)
            n = f(out, word, len(word), c.byref(t), 0)
            rows.append(
                f"    .{{ .word = {z(word)}, .operation = {op}, .parameter = {parameter}, .expected = {z(out.raw[:n])} }},"
            )
(REPO / "zig/lib/compress/src/brotli/transform_fixtures.zig").write_text(
    header
    + "// Original test words, transformed by Google Brotli v1.2.0 (test oracle only).\n// Regenerate with zig/pkg/antfly-grep/tests/generate_brotli_serialized.py and an experimental oracle.\npub const cases = [_]struct { word: []const u8, operation: u8, parameter: u16, expected: []const u8 }{\n"
    + "\n".join(rows)
    + "\n};\n"
)
print(len(rows))
