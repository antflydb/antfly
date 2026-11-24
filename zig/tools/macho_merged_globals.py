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

"""Preserve LLVM merged-data atoms in Zig's relocatable Mach-O entry objects.

The relocatable link can drop N_ALT_ENTRY on exported members of LLVM's
__MergedGlobals aggregates. Generated code still addresses private members by
base + offset. Letting ld split/dead-strip the exported aliases changes those
offsets, including the debug I/O singleton used before a test's first statement.
Restore the alias metadata; keep ordinary code/data dead-stripping enabled.
"""

from __future__ import annotations

import struct
from pathlib import Path

MH_MAGIC_64 = 0xFEEDFACF
LC_SEGMENT_64 = 0x19
LC_SYMTAB = 2
N_STAB = 0xE0
N_TYPE = 0x0E
N_SECT = 0x0E
N_EXT = 1
N_ALT_ENTRY = 0x0200


def restore_merged_global_aliases(data: bytes) -> bytes:
    if len(data) < 32 or struct.unpack_from("<I", data)[0] != MH_MAGIC_64:
        return data
    count, commands_size = struct.unpack_from("<II", data, 16)
    end = 32 + commands_size
    if end > len(data):
        raise ValueError("truncated Mach-O load commands")
    position = 32
    symbols = None
    sections = {}
    for _ in range(count):
        if position + 8 > end:
            raise ValueError("truncated Mach-O load command")
        command, size = struct.unpack_from("<II", data, position)
        if size < 8 or position + size > end:
            raise ValueError("invalid Mach-O load command size")
        if command == LC_SEGMENT_64:
            if size < 72:
                raise ValueError("truncated Mach-O segment")
            section_count = struct.unpack_from("<I", data, position + 64)[0]
            if 72 + 80 * section_count > size:
                raise ValueError("truncated Mach-O sections")
            for i in range(section_count):
                offset = position + 72 + 80 * i
                flags = struct.unpack_from("<I", data, offset + 64)[0]
                sections[len(sections) + 1] = flags
        elif command == LC_SYMTAB:
            if size < 24:
                raise ValueError("truncated Mach-O symbol command")
            symbols = struct.unpack_from("<IIII", data, position + 8)
        position += size
    if symbols is None:
        return data
    offset, count, strings, strings_size = symbols
    if offset + 16 * count > len(data) or strings + strings_size > len(data):
        raise ValueError("truncated Mach-O symbol table")
    groups = {}
    for i in range(count):
        at = offset + 16 * i
        name_at, kind, section, description, value = struct.unpack_from(
            "<IBBHQ", data, at
        )
        if kind & N_STAB or kind & N_TYPE != N_SECT:
            continue
        flags = sections.get(section)
        if flags is None or flags & 0x80000000:  # Never alter instructions.
            continue
        if name_at >= strings_size:
            raise ValueError("invalid Mach-O symbol name")
        name_end = data.find(b"\0", strings + name_at, strings + strings_size)
        if name_end == -1:
            raise ValueError("unterminated Mach-O symbol name")
        name = data[strings + name_at : name_end]
        merged = name == b"__MergedGlobals" or name.startswith(b"__MergedGlobals.")
        groups.setdefault(section, []).append(
            (value, not merged, at, kind, description)
        )
    output = None
    for entries in groups.values():
        in_group = False
        for _, ordinary, at, kind, description in sorted(entries):
            if not ordinary:
                in_group = True
            elif not kind & N_EXT and not description & N_ALT_ENTRY:
                # A new ordinary local atom ends the preceding aggregate.
                in_group = False
            elif in_group and not description & N_ALT_ENTRY:
                if output is None:
                    output = bytearray(data)
                struct.pack_into("<H", output, at + 6, description | N_ALT_ENTRY)
    return bytes(output) if output is not None else data


def shorten_private_symbol_names(data: bytes) -> bytes:
    """Bound private entry-object names before passing them to Apple's ld.

    Zig can include the contents of an embedded kernel in a compiler-private
    symbol name. Relocations use symbol indices, so local definitions can
    point to an appended short name without changing any original string,
    relocation, or external symbol. The entry object's string table is last.
    """
    if len(data) < 32 or struct.unpack_from("<I", data)[0] != MH_MAGIC_64:
        return data
    count, commands_size = struct.unpack_from("<II", data, 16)
    position = 32
    commands_end = position + commands_size
    for _ in range(count):
        if position + 8 > commands_end or commands_end > len(data):
            raise ValueError("truncated Mach-O load command")
        command, size = struct.unpack_from("<II", data, position)
        if size < 8 or position + size > commands_end:
            raise ValueError("invalid Mach-O load command size")
        if command != LC_SYMTAB:
            position += size
            continue
        if size < 24:
            raise ValueError("truncated Mach-O symbol command")
        symbols, count, strings, strings_size = struct.unpack_from(
            "<IIII", data, position + 8
        )
        strings_end = strings + strings_size
        if strings_end != len(data) or symbols + count * 16 > strings:
            return data
        output = None
        for index in range(count):
            offset = symbols + index * 16
            name, kind = struct.unpack_from("<IB", data, offset)
            if kind & (N_EXT | N_STAB) or kind & N_TYPE != N_SECT:
                continue
            start = strings + name
            if name >= strings_size:
                raise ValueError("invalid Mach-O symbol name")
            if data.find(b"\0", start, min(start + 1025, strings_end)) != -1:
                continue
            if data.find(b"\0", start, strings_end) == -1:
                raise ValueError("unterminated Mach-O symbol name")
            if output is None:
                output = bytearray(data)
            # Keep Darwin's temporary-label convention on names using it.
            prefix = "L" if data[start] == ord("L") else "_"
            struct.pack_into("<I", output, offset, len(output) - strings)
            output.extend(f"{prefix}antfly_private_{index}\0".encode("ascii"))
        if output is None:
            return data
        output.extend(b"\0" * (-len(output) % 8))
        struct.pack_into("<I", output, position + 20, len(output) - strings)
        return bytes(output)
    return data


def prepare_entry_object(source: Path, output: Path) -> Path:
    original = source.read_bytes()
    fixed = shorten_private_symbol_names(restore_merged_global_aliases(original))
    if fixed is original:
        return source
    retained = output.with_name(output.name + ".entry.o")
    retained.write_bytes(fixed)
    return retained
