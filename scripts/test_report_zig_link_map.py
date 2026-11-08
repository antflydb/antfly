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

import struct
import unittest

from report_zig_link_map import macho_sections, parse_map, report

MAP = b"""# Path: cache/lib.dylib
# Arch: arm64
# Object files:
[ 0] linker synthesized
[ 1] one.a(a.o)
[ 2] two.a(a.o)
# Sections:
0x1000 0x40 __TEXT __text
0x2000 0x10 __DATA __bss
# Symbols:
0x1000 0x10 [ 1] _decode
0x1000 0x10 [ 1] _alias
0x1020 0x10 [ 2] _decode
0x2000 0x10 [ 1] _buffer
# Dead Stripped Symbols:
<<dead>> 0x80 [ 2] _unused
"""
SECTIONS = {
    "__TEXT,__text": dict(address=0x1000, bytes=0x40, zero_fill=False),
    "__DATA,__bss": dict(address=0x2000, bytes=0x10, zero_fill=True),
}


class LinkMapTests(unittest.TestCase):
    def test_live_dead_alias_padding_and_zero_fill(self):
        value = report(parse_map(MAP), SECTIONS)
        text, bss = value["sections"]
        self.assertEqual(text["covered_atom_bytes"], 32)
        self.assertEqual(text["overlapping_atom_bytes"], 16)
        self.assertEqual(text["unattributed_bytes"], 32)
        self.assertTrue(bss["zero_fill"])
        self.assertEqual(value["dead_atoms"][0]["bytes"], 128)
        self.assertEqual(value["objects"][0]["sections"]["__TEXT,__text"], 16)
        self.assertEqual(
            value["repeated_name_candidates"][0]["nominal_extra_bytes"], 16
        )

    def test_invalid_symbol_bytes_preserve_extent(self):
        parsed = parse_map(MAP.replace(b"_alias", b"_alias\xff\x1f\x0b"))
        self.assertEqual(parsed["live"][1]["name"], "_alias\ufffd\x1f\x0b")
        self.assertEqual(parsed["live"][1]["bytes"], 16)

    def test_invalid_map_rows_rejected(self):
        for source in (
            MAP.replace(b"[ 2] _decode", b"[ 9] _decode"),
            MAP.replace(b"0x1020 0x10", b"0x1038 0x10"),
            MAP.replace(b"0x2000 0x10 __DATA", b"0x1020 0x10 __DATA"),
            MAP.replace(b"0x1020 0x10", b"broken"),
            b"# Path: x\n",
        ):
            with self.subTest(source=source), self.assertRaises(ValueError):
                parse_map(source)

    def test_product_section_mismatch_rejected(self):
        with self.assertRaisesRegex(ValueError, "map/binary"):
            report(parse_map(MAP), {})

    def test_macho_flags_and_truncation(self):
        # Use an unusual name to prove zero-fill uses section flags.
        section = struct.pack(
            "<16s16sQQIIIIIIII",
            b"custom",
            b"__DATA",
            0x2000,
            16,
            0,
            0,
            0,
            0,
            0x12,
            0,
            0,
            0,
        )
        segment = struct.pack(
            "<II16sQQQQIIII", 0x19, 152, b"__DATA", 0, 0, 0, 0, 0, 0, 1, 0
        )
        header = struct.pack("<8I", 0xFEEDFACF, 0, 0, 0, 1, 152, 0, 0)
        binary = header + segment + section
        self.assertTrue(macho_sections(binary)["__DATA,custom"]["zero_fill"])
        for invalid in (b"ELF", binary[:-1], binary[:32] + b"\0" * 152):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                macho_sections(invalid)


if __name__ == "__main__":
    unittest.main()
