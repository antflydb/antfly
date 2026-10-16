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

"""Generate synthetic GPU eligibility/parity PDFs; these are not a real corpus."""

import argparse
import zlib
from pathlib import Path


def stream(data, attributes=b""):
    return (
        b"<< /Length "
        + str(len(data)).encode()
        + b" "
        + attributes
        + b">>\nstream\n"
        + data
        + b"\nendstream"
    )


def save_pdf(path, objects):
    data = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for i, obj in enumerate(objects, 1):
        offsets.append(len(data))
        data += f"{i} 0 obj\n".encode() + obj + b"\nendobj\n"
    xref = len(data)
    data += f"xref\n0 {len(offsets)}\n0000000000 65535 f \n".encode()
    for offset in offsets[1:]:
        data += f"{offset:010d} 00000 n \n".encode()
    data += f"trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    path.write_bytes(data)


def generate(directory):
    directory.mkdir(parents=True, exist_ok=True)
    common = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
    ]
    shapes = []
    for y in range(12):
        for x in range(12):
            shapes.append(
                f"{x / 12:.6f} {y / 12:.6f} 0.5 rg {x * 40} {y * 50} 40 50 re f\n"
            )
    save_pdf(
        directory / "vector-validation.pdf",
        common
        + [
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 480 600] /Resources << >> /Contents 4 0 R >>",
            stream("".join(shapes).encode()),
        ],
    )
    save_pdf(
        directory / "image-validation.pdf",
        common
        + [
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 480 600] /Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
            stream(b"q 480 0 0 600 0 0 cm /Im0 Do Q\n"),
            stream(
                zlib.compress(bytes([255, 0, 0, 0, 255, 0, 0, 0, 255, 255, 255, 0])),
                b"/Type /XObject /Subtype /Image /Width 2 /Height 2 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /FlateDecode ",
            ),
        ],
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    generate(parser.parse_args().directory)
