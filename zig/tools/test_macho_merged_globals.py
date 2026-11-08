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

import struct
import tempfile
import unittest
from pathlib import Path

from macho_merged_globals import (
    N_ALT_ENTRY,
    prepare_entry_object,
    restore_merged_global_aliases,
)


def macho(symbols):
    """Two data sections, with real 64-bit nlist/string-table encodings."""
    command_size = 72 + 2 * 80
    data_offset = 32 + command_size + 24
    symbol_offset = data_offset + 512
    strings = bytearray(b"\0")
    table = bytearray()
    for name, kind, section, description, value in symbols:
        at = len(strings)
        strings.extend(name.encode() + b"\0")
        table.extend(struct.pack("<IBBHQ", at, kind, section, description, value))
    header = struct.pack("<8I", 0xFEEDFACF, 0x100000C, 0, 1, 2, command_size + 24, 0, 0)
    segment = struct.pack(
        "<II16sQQQQIIII",
        0x19,
        command_size,
        b"",
        0x1000,
        0x2000,
        data_offset,
        512,
        7,
        7,
        2,
        0,
    )
    sections = bytearray()
    for name, address, flags in ((b"__data", 0x1000, 0), (b"__bss", 0x2000, 1)):
        sections.extend(
            struct.pack(
                "<16s16sQQIIIIIIII",
                name,
                b"__DATA",
                address,
                512,
                data_offset if flags == 0 else 0,
                3,
                0,
                0,
                flags,
                0,
                0,
                0,
            )
        )
    command = struct.pack(
        "<6I",
        2,
        24,
        symbol_offset,
        len(symbols),
        symbol_offset + len(table),
        len(strings),
    )
    return header + segment + sections + command + bytes(
        512
    ) + table + strings, symbol_offset


class MergedGlobals(unittest.TestCase):
    def test_exported_members_stay_in_their_aggregate(self):
        symbols = [
            ("ordinary_export", 0xF, 1, 0, 0x1000),
            ("__MergedGlobals", 0xE, 1, 0, 0x1040),
            ("member", 0xF, 1, 0x80, 0x1050),
            ("debug_record", 0x26, 1, 0, 0x1058),
            ("member2", 0xF, 1, N_ALT_ENTRY, 0x1060),
            ("__MergedGlobals.1", 0xE, 2, 0, 0x2000),
            ("zero_member", 0xF, 2, 0, 0x2008),
            ("ordinary_local", 0xE, 1, 0, 0x1100),
            ("later_export", 0xF, 1, 0, 0x1120),
        ]
        data, at = macho(symbols)
        expected = bytearray(data)
        for index in (2, 6):
            struct.pack_into(
                "<H", expected, at + 16 * index + 6, symbols[index][3] | N_ALT_ENTRY
            )
        result = restore_merged_global_aliases(data)
        self.assertEqual(bytes(expected), result)
        self.assertIs(result, restore_merged_global_aliases(result))

    def test_same_address_alias_and_symbol_table_order(self):
        data, at = macho(
            [
                ("member", 0xF, 1, 0, 0x1040),
                ("__MergedGlobals", 0xE, 1, 0, 0x1040),
            ]
        )
        result = restore_merged_global_aliases(data)
        self.assertEqual(N_ALT_ENTRY, struct.unpack_from("<H", result, at + 6)[0])

    def test_existing_private_alias_does_not_end_the_aggregate(self):
        data, at = macho(
            [
                ("__MergedGlobals", 0xE, 1, 0, 0x1040),
                ("private_alias", 0xE, 1, N_ALT_ENTRY, 0x1050),
                ("exported_alias", 0xF, 1, 0, 0x1060),
            ]
        )
        result = restore_merged_global_aliases(data)
        self.assertEqual(N_ALT_ENTRY, struct.unpack_from("<H", result, at + 38)[0])

    def test_ordinary_and_non_macho_objects_are_unchanged(self):
        data, _ = macho([("ordinary", 0xF, 1, 0, 0x1050)])
        self.assertIs(data, restore_merged_global_aliases(data))
        elf = b"\x7fELF" + bytes(60)
        self.assertIs(elf, restore_merged_global_aliases(elf))

    def test_invalid_recognized_object_is_rejected(self):
        data, _ = macho([])
        with self.assertRaisesRegex(ValueError, "load commands"):
            restore_merged_global_aliases(data[:64])
        bad = bytearray(data)
        struct.pack_into("<I", bad, 36, 4)
        with self.assertRaisesRegex(ValueError, "command size"):
            restore_merged_global_aliases(bytes(bad))

    def test_staging_preserves_the_compiler_object(self):
        data, _ = macho(
            [
                ("__MergedGlobals", 0xE, 1, 0, 0x1040),
                ("member", 0xF, 1, 0, 0x1050),
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "compiler.o"
            source.write_bytes(data)
            staged = prepare_entry_object(source, Path(directory) / "tests")
            self.assertNotEqual(source, staged)
            self.assertEqual(data, source.read_bytes())
            self.assertEqual(restore_merged_global_aliases(data), staged.read_bytes())
            source.write_bytes(b"not Mach-O")
            self.assertEqual(
                source, prepare_entry_object(source, Path(directory) / "tests")
            )


if __name__ == "__main__":
    unittest.main()
