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

"""Attribute live Mach-O linker atoms to input objects, retaining dead atoms.

Exact linked extents describe this link, not compiler work or equivalent code.
Repeated names are candidates, not proven removable implementations. Zero-fill
sections reserve virtual memory and do not occupy their stated size in the file.
The raw map remains authoritative for names containing invalid UTF-8 bytes.
"""

import argparse
from bisect import bisect_right
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import re
import struct


def parse_map(data):
    result = dict(path=None, arch=None, objects={}, sections=[], live=[], dead=[])
    mode = None
    for number, line in enumerate(
        data.decode("utf-8", errors="replace").split("\n"), 1
    ):
        if line.startswith("# Path: "):
            result["path"] = line[8:]
        elif line.startswith("# Arch: "):
            result["arch"] = line[8:]
        elif line in (
            "# Object files:",
            "# Sections:",
            "# Symbols:",
            "# Dead Stripped Symbols:",
        ):
            mode = {
                "# Object files:": "objects",
                "# Sections:": "sections",
                "# Symbols:": "live",
                "# Dead Stripped Symbols:": "dead",
            }[line]
        elif not line or line.startswith("#"):
            continue
        elif mode == "objects":
            match = re.fullmatch(r"\[\s*(\d+)\]\s+(.+)", line)
            if not match or int(match[1]) in result["objects"]:
                raise ValueError(f"invalid or repeated object at line {number}")
            result["objects"][int(match[1])] = match[2]
        elif mode == "sections":
            match = re.fullmatch(
                r"(0x[\da-fA-F]+)\s+(0x[\da-fA-F]+)\s+(\S+)\s+(\S+)", line
            )
            if not match:
                raise ValueError(f"invalid section at line {number}")
            result["sections"].append(
                dict(
                    address=int(match[1], 16),
                    bytes=int(match[2], 16),
                    segment=match[3],
                    section=match[4],
                )
            )
        elif mode in ("live", "dead"):
            match = re.fullmatch(
                r"(0x[\da-fA-F]+|<<dead>>)\s+(0x[\da-fA-F]+)\s+\[\s*(\d+)\]\s+(.+)",
                line,
            )
            if not match or (mode == "live" and match[1] == "<<dead>>"):
                raise ValueError(f"invalid atom at line {number}")
            result[mode].append(
                dict(
                    address=int(match[1], 16) if match[1] != "<<dead>>" else None,
                    bytes=int(match[2], 16),
                    object=int(match[3]),
                    name=match[4],
                )
            )
        else:
            raise ValueError(f"unexpected input at line {number}")
    if (
        not result["path"]
        or not result["arch"]
        or not result["sections"]
        or not result["live"]
    ):
        raise ValueError("incomplete link map")
    for atom in result["live"] + result["dead"]:
        if atom["object"] not in result["objects"]:
            raise ValueError(f"unknown object {atom['object']}")
    sections = sorted(result["sections"], key=lambda row: row["address"])
    for previous, current in zip(sections, sections[1:]):
        if previous["address"] + previous["bytes"] > current["address"]:
            raise ValueError("overlapping sections")
    starts = [row["address"] for row in sections]
    for atom in result["live"]:
        index = bisect_right(starts, atom["address"]) - 1
        if index < 0:
            raise ValueError("atom outside sections")
        section = sections[index]
        if atom["address"] + atom["bytes"] > section["address"] + section["bytes"]:
            raise ValueError("atom crosses section boundary")
        atom["section"] = f"{section['segment']},{section['section']}"
    return result


def macho_sections(data):
    """Read load-command section flags; section names alone cannot prove zero-fill."""
    if len(data) < 32 or struct.unpack_from("<I", data)[0] != 0xFEEDFACF:
        raise ValueError("expected thin little-endian 64-bit Mach-O")
    ncmds, sizeofcmds = struct.unpack_from("<II", data, 16)
    end = 32 + sizeofcmds
    if end > len(data):
        raise ValueError("truncated load commands")
    position = 32
    sections = {}
    for _ in range(ncmds):
        if position + 8 > end:
            raise ValueError("truncated load command")
        cmd, size = struct.unpack_from("<II", data, position)
        if size < 8 or position + size > end:
            raise ValueError("invalid load command size")
        if cmd == 0x19:  # LC_SEGMENT_64
            if size < 72:
                raise ValueError("truncated segment")
            count = struct.unpack_from("<I", data, position + 64)[0]
            if 72 + count * 80 > size:
                raise ValueError("truncated sections")
            for index in range(count):
                row = struct.unpack_from(
                    "<16s16sQQIIIIIIII", data, position + 72 + index * 80
                )
                name, segment = (
                    value.rstrip(b"\0").decode("ascii") for value in row[:2]
                )
                sections[f"{segment},{name}"] = dict(
                    address=row[2],
                    bytes=row[3],
                    zero_fill=(row[8] & 0xFF) in (1, 0xC, 0x12),
                )
        position += size
    if position != end or not sections:
        raise ValueError("invalid load-command table")
    return sections


def union_bytes(atoms):
    total = 0
    end = -1
    for atom in sorted(atoms, key=lambda row: row["address"]):
        stop = atom["address"] + atom["bytes"]
        total += max(0, stop - max(end, atom["address"]))
        end = max(end, stop)
    return total


def report(parsed, binary_sections):
    sections = []
    objects = defaultdict(lambda: defaultdict(int))
    repeated = defaultdict(list)
    for section in parsed["sections"]:
        key = f"{section['segment']},{section['section']}"
        binary = binary_sections.get(key)
        if binary is None or any(
            binary[field] != section[field] for field in ("address", "bytes")
        ):
            raise ValueError(f"map/binary section mismatch: {key}")
        atoms = [atom for atom in parsed["live"] if atom["section"] == key]
        covered = union_bytes(atoms)
        sections.append(
            dict(
                **section,
                zero_fill=binary["zero_fill"],
                covered_atom_bytes=covered,
                unattributed_bytes=section["bytes"] - covered,
                overlapping_atom_bytes=sum(atom["bytes"] for atom in atoms) - covered,
            )
        )
        # Exact aliases count once per object. Partial overlaps remain visible
        # in section accounting; per-object totals must not be summed as file size.
        by_object = defaultdict(list)
        for atom in atoms:
            by_object[atom["object"]].append(atom)
            if key == "__TEXT,__text" and "__func_" not in atom["name"]:
                repeated[atom["name"]].append(atom)
        for owner, owner_atoms in by_object.items():
            objects[owner][key] += union_bytes(owner_atoms)
    candidates = []
    for name, atoms in repeated.items():
        owners = {atom["object"] for atom in atoms}
        if len(owners) < 2:
            continue
        # Names alone are not equivalence proof. Do not normalize generic IDs.
        extents = {(atom["address"], atom["bytes"]) for atom in atoms}
        nominal = sum(size for _, size in extents) - max(size for _, size in extents)
        candidates.append(dict(name=name, nominal_extra_bytes=nominal, instances=atoms))
    return dict(
        format_version=1,
        arch=parsed["arch"],
        sections=sections,
        objects=[
            dict(id=owner, path=parsed["objects"][owner], sections=dict(sizes))
            for owner, sizes in sorted(objects.items())
        ],
        repeated_name_candidates=sorted(
            candidates, key=lambda row: -row["nominal_extra_bytes"]
        ),
        live_atoms=parsed["live"],
        dead_atoms=parsed["dead"],
        limitations=__doc__,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("map", type=Path)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument(
        "--map-root",
        type=Path,
        required=True,
        help="Directory against which the map's linked-output path resolves",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = args.map.read_bytes()
    parsed = parse_map(raw)
    linked_path = args.map_root / parsed["path"]
    with linked_path.open("rb") as handle:
        linked_hash = hashlib.file_digest(handle, "sha256").hexdigest()
    binary = args.binary.read_bytes()
    digest = hashlib.sha256(binary).hexdigest()
    if linked_hash != digest:
        parser.error("map's linked output differs from the requested product")
    value = report(parsed, macho_sections(binary))
    value.update(
        binary=str(args.binary),
        binary_sha256=digest,
        file_bytes=len(binary),
        map=str(args.map),
        map_sha256=hashlib.sha256(raw).hexdigest(),
        linked_output=str(linked_path),
        name_decode_errors="\ufffd" in raw.decode("utf-8", "replace"),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
