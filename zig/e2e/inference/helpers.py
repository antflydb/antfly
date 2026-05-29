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

"""Shared helpers for Antfly inference E2E tests."""

import base64
import io
import os
import shutil
import struct
import subprocess
import tempfile
import zlib
from pathlib import Path

import numpy as np


def cosine_similarity(a: list[float], b: list[float]) -> float:
    a, b = np.array(a), np.array(b)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))


def l2_norm(v: list[float]) -> float:
    return float(np.linalg.norm(v))


def assert_openai_list_response(resp: dict, expected_len: int | None = None) -> None:
    assert resp["object"] == "list", resp
    assert isinstance(resp["data"], list), resp
    if expected_len is not None:
        assert len(resp["data"]) == expected_len, resp
    assert isinstance(resp["model"], str), resp
    usage = resp["usage"]
    assert isinstance(usage["prompt_tokens"], int), resp
    assert isinstance(usage["completion_tokens"], int), resp
    assert isinstance(usage["total_tokens"], int), resp
    assert usage["total_tokens"] == usage["prompt_tokens"] + usage["completion_tokens"], resp


# -- Test data generators --

# 1x1 white PNG, pre-encoded
TINY_PNG_B64 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8"
    "/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg=="
)
TINY_PNG_URI = f"data:image/png;base64,{TINY_PNG_B64}"
_GO_ANTFLY_INFERENCE_E2E_DIR = Path(__file__).resolve().parents[2] / "antfly" / "antfly" / "e2e" / "testdata"


_FONT_5X7 = {
    " ": [
        "00000",
        "00000",
        "00000",
        "00000",
        "00000",
        "00000",
        "00000",
    ],
    "-": [
        "00000",
        "00000",
        "00000",
        "11111",
        "00000",
        "00000",
        "00000",
    ],
    ".": [
        "00000",
        "00000",
        "00000",
        "00000",
        "00000",
        "00110",
        "00110",
    ],
    ":": [
        "00000",
        "00110",
        "00110",
        "00000",
        "00110",
        "00110",
        "00000",
    ],
    "0": [
        "01110",
        "10001",
        "10011",
        "10101",
        "11001",
        "10001",
        "01110",
    ],
    "1": [
        "00100",
        "01100",
        "00100",
        "00100",
        "00100",
        "00100",
        "01110",
    ],
    "2": [
        "01110",
        "10001",
        "00001",
        "00010",
        "00100",
        "01000",
        "11111",
    ],
    "3": [
        "11110",
        "00001",
        "00001",
        "01110",
        "00001",
        "00001",
        "11110",
    ],
    "4": [
        "00010",
        "00110",
        "01010",
        "10010",
        "11111",
        "00010",
        "00010",
    ],
    "5": [
        "11111",
        "10000",
        "10000",
        "11110",
        "00001",
        "00001",
        "11110",
    ],
    "6": [
        "01110",
        "10000",
        "10000",
        "11110",
        "10001",
        "10001",
        "01110",
    ],
    "7": [
        "11111",
        "00001",
        "00010",
        "00100",
        "01000",
        "01000",
        "01000",
    ],
    "8": [
        "01110",
        "10001",
        "10001",
        "01110",
        "10001",
        "10001",
        "01110",
    ],
    "9": [
        "01110",
        "10001",
        "10001",
        "01111",
        "00001",
        "00001",
        "01110",
    ],
    "A": [
        "01110",
        "10001",
        "10001",
        "11111",
        "10001",
        "10001",
        "10001",
    ],
    "B": [
        "11110",
        "10001",
        "10001",
        "11110",
        "10001",
        "10001",
        "11110",
    ],
    "C": [
        "01110",
        "10001",
        "10000",
        "10000",
        "10000",
        "10001",
        "01110",
    ],
    "D": [
        "11110",
        "10001",
        "10001",
        "10001",
        "10001",
        "10001",
        "11110",
    ],
    "E": [
        "11111",
        "10000",
        "10000",
        "11110",
        "10000",
        "10000",
        "11111",
    ],
    "F": [
        "11111",
        "10000",
        "10000",
        "11110",
        "10000",
        "10000",
        "10000",
    ],
    "G": [
        "01110",
        "10001",
        "10000",
        "10111",
        "10001",
        "10001",
        "01110",
    ],
    "H": [
        "10001",
        "10001",
        "10001",
        "11111",
        "10001",
        "10001",
        "10001",
    ],
    "I": [
        "01110",
        "00100",
        "00100",
        "00100",
        "00100",
        "00100",
        "01110",
    ],
    "J": [
        "00001",
        "00001",
        "00001",
        "00001",
        "10001",
        "10001",
        "01110",
    ],
    "K": [
        "10001",
        "10010",
        "10100",
        "11000",
        "10100",
        "10010",
        "10001",
    ],
    "L": [
        "10000",
        "10000",
        "10000",
        "10000",
        "10000",
        "10000",
        "11111",
    ],
    "M": [
        "10001",
        "11011",
        "10101",
        "10101",
        "10001",
        "10001",
        "10001",
    ],
    "N": [
        "10001",
        "11001",
        "10101",
        "10011",
        "10001",
        "10001",
        "10001",
    ],
    "O": [
        "01110",
        "10001",
        "10001",
        "10001",
        "10001",
        "10001",
        "01110",
    ],
    "P": [
        "11110",
        "10001",
        "10001",
        "11110",
        "10000",
        "10000",
        "10000",
    ],
    "Q": [
        "01110",
        "10001",
        "10001",
        "10001",
        "10101",
        "10010",
        "01101",
    ],
    "R": [
        "11110",
        "10001",
        "10001",
        "11110",
        "10100",
        "10010",
        "10001",
    ],
    "S": [
        "01111",
        "10000",
        "10000",
        "01110",
        "00001",
        "00001",
        "11110",
    ],
    "T": [
        "11111",
        "00100",
        "00100",
        "00100",
        "00100",
        "00100",
        "00100",
    ],
    "U": [
        "10001",
        "10001",
        "10001",
        "10001",
        "10001",
        "10001",
        "01110",
    ],
    "V": [
        "10001",
        "10001",
        "10001",
        "10001",
        "10001",
        "01010",
        "00100",
    ],
    "W": [
        "10001",
        "10001",
        "10001",
        "10101",
        "10101",
        "10101",
        "01010",
    ],
    "X": [
        "10001",
        "10001",
        "01010",
        "00100",
        "01010",
        "10001",
        "10001",
    ],
    "Y": [
        "10001",
        "10001",
        "01010",
        "00100",
        "00100",
        "00100",
        "00100",
    ],
    "Z": [
        "11111",
        "00001",
        "00010",
        "00100",
        "01000",
        "10000",
        "11111",
    ],
}


def make_solid_png_uri(r: int, g: int, b: int, size: int = 8) -> str:
    """Generate a small solid-color PNG as a data URI without extra dependencies."""
    def chunk(tag: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + tag
            + data
            + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
        )

    row = b"\x00" + bytes([r, g, b]) * size
    raw = row * size
    ihdr = struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0)
    png = b"".join([
        b"\x89PNG\r\n\x1a\n",
        chunk(b"IHDR", ihdr),
        chunk(b"IDAT", zlib.compress(raw)),
        chunk(b"IEND", b""),
    ])
    return f"data:image/png;base64,{base64.b64encode(png).decode()}"


def make_text_png_uri(lines: list[str], scale: int = 6, padding: int = 12, line_gap: int = 8) -> str:
    """Generate a simple black-on-white bitmap text PNG as a data URI."""
    rows_per_char = 7
    cols_per_char = 5
    char_gap = 1

    normalized = [line.upper() for line in lines]
    max_chars = max((len(line) for line in normalized), default=1)
    width = padding * 2 + max_chars * (cols_per_char + char_gap) * scale
    height = padding * 2 + len(normalized) * rows_per_char * scale + max(0, len(normalized) - 1) * line_gap

    canvas = bytearray([255] * (width * height * 3))

    def put_pixel(x: int, y: int, value: int):
        idx = (y * width + x) * 3
        canvas[idx:idx + 3] = bytes((value, value, value))

    for line_idx, line in enumerate(normalized):
        y0 = padding + line_idx * (rows_per_char * scale + line_gap)
        for char_idx, ch in enumerate(line):
            glyph = _FONT_5X7.get(ch, _FONT_5X7[" "])
            x0 = padding + char_idx * (cols_per_char + char_gap) * scale
            for gy, row in enumerate(glyph):
                for gx, bit in enumerate(row):
                    if bit != "1":
                        continue
                    for sy in range(scale):
                        for sx in range(scale):
                            put_pixel(x0 + gx * scale + sx, y0 + gy * scale + sy, 0)

    def chunk(tag: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + tag
            + data
            + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
        )

    raw_rows = []
    stride = width * 3
    for y in range(height):
        raw_rows.append(b"\x00" + bytes(canvas[y * stride:(y + 1) * stride]))
    raw = b"".join(raw_rows)
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    png = b"".join([
        b"\x89PNG\r\n\x1a\n",
        chunk(b"IHDR", ihdr),
        chunk(b"IDAT", zlib.compress(raw)),
        chunk(b"IEND", b""),
    ])
    return f"data:image/png;base64,{base64.b64encode(png).decode()}"


def png_file_to_data_uri(path: str | Path) -> str:
    raw = Path(path).read_bytes()
    return f"data:image/png;base64,{base64.b64encode(raw).decode()}"


def load_go_sample_page_fixture() -> tuple[str, list[str]] | None:
    image_path = _GO_ANTFLY_INFERENCE_E2E_DIR / "sample-page-1.png"
    text_path = _GO_ANTFLY_INFERENCE_E2E_DIR / "sample-page-1.txt"
    if not image_path.exists() or not text_path.exists():
        return None

    phrases = [
        line.strip()
        for line in text_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return png_file_to_data_uri(image_path), phrases


def make_wav_b64(duration_s: float = 0.1, sample_rate: int = 16000) -> str:
    """Generate a minimal silent WAV file and return as base64."""
    n_samples = int(sample_rate * duration_s)
    data_size = n_samples * 2  # 16-bit PCM
    buf = io.BytesIO()
    # RIFF header
    buf.write(b"RIFF")
    buf.write(struct.pack("<I", 36 + data_size))
    buf.write(b"WAVE")
    # fmt chunk
    buf.write(b"fmt ")
    buf.write(struct.pack("<IHHIIHH", 16, 1, 1, sample_rate, sample_rate * 2, 2, 16))
    # data chunk
    buf.write(b"data")
    buf.write(struct.pack("<I", data_size))
    buf.write(b"\x00" * data_size)
    return base64.b64encode(buf.getvalue()).decode()


def make_wav_uri(duration_s: float = 0.1, sample_rate: int = 16000) -> str:
    return f"data:audio/wav;base64,{make_wav_b64(duration_s, sample_rate)}"


def can_make_spoken_wav() -> bool:
    return shutil.which("say") is not None and shutil.which("afconvert") is not None


def make_spoken_wav_uri(text: str, sample_rate: int = 48000) -> str:
    """Generate deterministic spoken WAV audio as a data URI on macOS hosts."""
    fd_aiff, aiff_path = tempfile.mkstemp(suffix=".aiff", prefix="antfly-e2e-")
    os.close(fd_aiff)
    fd_wav, wav_path = tempfile.mkstemp(suffix=".wav", prefix="antfly-e2e-")
    os.close(fd_wav)

    try:
        subprocess.run(
            ["say", "-o", aiff_path, text],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            ["afconvert", "-f", "WAVE", "-d", f"LEI16@{sample_rate}", "-c", "1", aiff_path, wav_path],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        data = Path(wav_path).read_bytes()
        return f"data:audio/wav;base64,{base64.b64encode(data).decode()}"
    finally:
        for path in (aiff_path, wav_path):
            try:
                os.remove(path)
            except FileNotFoundError:
                pass


def sparse_dot_product(a: dict, b: dict) -> float:
    """Dot product between two sparse vectors (dict with 'indices' and 'values')."""
    a_map = dict(zip(a["indices"], a["values"]))
    b_map = dict(zip(b["indices"], b["values"]))
    shared = set(a_map.keys()) & set(b_map.keys())
    return sum(a_map[k] * b_map[k] for k in shared)
