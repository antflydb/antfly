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

"""Read-only logical catalog byte accounting for unreleased Lite revisions.

This diagnostic validates the pages it visits, but does not replace native
integrity checking. Values stay on disk; only aggregate lengths are emitted.
An inode shared lock prevents reuse/shrink while the captured roots are read.
"""

from __future__ import annotations

import argparse
import collections
import fcntl
import functools
import hashlib
import json
import mmap
import os
import struct
import zlib
from pathlib import Path


class CatalogAccounting:
    def __init__(self, path: Path):
        self.file = path.open("rb")
        fcntl.flock(self.file, fcntl.LOCK_SH)
        self.data = mmap.mmap(self.file.fileno(), 0, access=mmap.ACCESS_READ)
        header = self.data[:4096]
        if header[:6] != b"AFLITE" or header[6] not in (3, 4, 5, 6, 7):
            raise ValueError("unsupported diagnostic file signature")
        self.version = struct.unpack_from("<I", header, 8)[0]
        if self.version != header[6]:
            raise ValueError("inconsistent file revision")
        self.page_size = struct.unpack_from("<I", header, 12)[0]
        if (
            self.page_size < 4096
            or self.page_size > 65536
            or self.page_size & (self.page_size - 1)
        ):
            raise ValueError("invalid page size")
        slots = []
        for i in range(2):
            raw = header[64 + i * 72 : 136 + i * 72]
            extra = header[208 + i * 48 : 256 + i * 48] if self.version == 7 else b""
            checksum = struct.unpack_from("<I", raw, 64)[0]
            if checksum != zlib.crc32(raw[:64] + extra) and not (
                self.version == 3 and checksum == 0
            ):
                continue
            slot = struct.unpack_from("<8Q", raw)
            if 0 < slot[5] <= len(self.data) // self.page_size:
                slots.append(slot)
        if not slots:
            raise ValueError("no complete checksummed checkpoint")
        self.slot = max(slots, key=lambda slot: slot[0])

    def close(self):
        self.page.cache_clear()
        self.data.close()
        self.file.close()

    @functools.lru_cache(maxsize=512)
    def page(self, page_id: int):
        if not 0 < page_id < self.slot[5]:
            raise ValueError("page outside captured checkpoint")
        start = page_id * self.page_size
        raw = self.data[start : start + self.page_size]
        if raw[:4] != b"AFLP":
            raise ValueError("invalid page magic")
        length, checksum = struct.unpack_from("<II", raw, 8)
        if length > self.page_size - 16:
            raise ValueError("invalid page length")
        payload = raw[16 : 16 + length]
        if checksum != zlib.crc32(raw[:12] + payload):
            raise ValueError("page checksum mismatch")
        return raw[4], payload

    def payload(self, reference: int):
        packed = reference >> 63
        kind, payload = self.page(reference & ((1 << 47) - 1) if packed else reference)
        if packed:
            if kind != 9:
                raise ValueError("packed reference does not name a bundle")
            wanted = (reference >> 47) & 0xFFFF
            position = 0
            while position < len(payload):
                length, record_kind, reserved = struct.unpack_from(
                    "<HBB", payload, position
                )
                end = position + 4 + length
                if reserved or end > len(payload):
                    raise ValueError("invalid bundled record")
                if position == wanted:
                    kind, payload = record_kind, payload[position + 4 : end]
                    break
                position = end
            else:
                raise ValueError("record offset is not a boundary")
        return kind, payload

    def record(self, reference: int):
        kind, payload = self.payload(reference)
        if kind != 2 or len(payload) < 16:
            raise ValueError("expected catalog record")
        previous, flags, value_length = struct.unpack_from("<QII", payload)
        key_length = flags & 0xFFFFFF
        if flags & ~0xC0FFFFFF:
            raise ValueError("invalid catalog flags")
        needed = 16 + key_length + (8 if flags & 0x40000000 else value_length)
        if needed > len(payload):
            raise ValueError("truncated catalog record")
        return (
            previous,
            payload[16 : 16 + key_length],
            value_length,
            bool(flags & 0x80000000),
        )

    def records(self, root: int):
        if root == 0:
            return
        kind, payload = self.page(root & ((1 << 47) - 1) if root & (1 << 63) else root)
        if kind in (2, 9):
            seen_keys, seen_pages = set(), set()
            while root:
                if root in seen_pages:
                    raise ValueError("catalog history cycle")
                seen_pages.add(root)
                root, key, length, deleted = self.record(root)
                if key not in seen_keys:
                    seen_keys.add(key)
                    if not deleted:
                        yield key, length
            return
        if kind != 8 or len(payload) != 16:
            raise ValueError("expected catalog descriptor")
        index = struct.unpack_from("<Q", payload, 8)[0]
        for reference in self.references(index):
            _, key, length, deleted = self.record(reference)
            if not deleted:
                yield key, length

    def references(self, index: int):
        stack, visited = ([index] if index else []), set()
        while stack:
            node = stack.pop()
            if node in visited:
                raise ValueError("catalog index cycle or duplicate child")
            visited.add(node)
            kind, payload = self.page(node)
            if (
                kind != 6
                or payload[:8] != b"AFDIDX02"
                or payload[8] not in (1, 2)
                or payload[9]
            ):
                raise ValueError("invalid catalog index node")
            leaf = payload[8] == 1
            count = struct.unpack_from("<H", payload, 10)[0]
            position = 12
            children = []
            if not leaf:
                children.append(struct.unpack_from("<Q", payload, position)[0])
                position += 8
            for _ in range(count):
                key_length, pointer = struct.unpack_from("<HQ", payload, position)
                position += 10
                position += 8 if key_length == 0xFFFF else key_length
                if position > len(payload):
                    raise ValueError("truncated index entry")
                if leaf:
                    yield pointer
                else:
                    children.append(pointer)
            if position != len(payload):
                raise ValueError("trailing index bytes")
            stack.extend(reversed(children))

    def document(self, reference: int):
        kind, payload = self.payload(reference)
        if kind != 3 or len(payload) < 28:
            raise ValueError("expected document record")
        previous = struct.unpack_from("<Q", payload)[0]
        flags = payload[8]
        if flags & ~15 or not flags & 4 or (flags & 1 and flags & 2):
            raise ValueError("invalid document flags")
        key_length, value_length = struct.unpack_from("<II", payload, 12)
        header = 60 if flags & 8 else 28
        end = header + key_length
        if end + (8 if flags & 2 else value_length) > len(payload):
            raise ValueError("truncated document record")
        if flags & 2 and (
            not value_length or not struct.unpack_from("<Q", payload, end)[0]
        ):
            raise ValueError("invalid external document value")
        return previous, payload[header:end], value_length, bool(flags & 1)

    def documents(self):
        if self.slot[7]:
            for reference in self.references(self.slot[7]):
                _, key, length, deleted = self.document(reference)
                if not deleted:
                    yield key, length
            return
        reference = self.slot[2]
        seen_records, seen_keys = set(), set()
        while reference:
            if reference in seen_records:
                raise ValueError("document history cycle")
            seen_records.add(reference)
            reference, key, length, deleted = self.document(reference)
            if key not in seen_keys:
                seen_keys.add(key)
                if not deleted:
                    yield key, length

    def account(self, include_documents: bool = False):
        result = {
            "format_version": self.version,
            "commit_sequence": self.slot[0],
            "file_bytes": os.fstat(self.file.fileno()).st_size,
        }
        for label, root in [
            ("metadata_catalog", self.slot[1]),
            ("index_catalog", self.slot[3]),
        ]:
            row = {"records": 0, "key_bytes": 0, "value_bytes": 0}
            sizes = collections.Counter()
            digest = hashlib.sha256()
            for key, length in self.records(root):
                if label == "metadata_catalog" and key.startswith(b"\x00AFLEASE/"):
                    continue
                row["records"] += 1
                row["key_bytes"] += len(key)
                row["value_bytes"] += length
                sizes[str(max(0, length.bit_length() - 1))] += 1
                digest.update(struct.pack("<QQ", len(key), length))
                digest.update(key)
            row["value_size_log2_histogram"] = dict(
                sorted(sizes.items(), key=lambda item: int(item[0]))
            )
            row["key_and_length_sha256"] = digest.hexdigest()
            result[label] = row
        if include_documents:
            for label in ("documents", "control_documents"):
                result[label] = {"records": 0, "key_bytes": 0, "value_bytes": 0}
            for key, length in self.documents():
                label = "control_documents" if is_control_record(key) else "documents"
                row = result[label]
                row["records"] += 1
                row["key_bytes"] += len(key)
                row["value_bytes"] += length
            categories = (
                "documents",
                "control_documents",
                "metadata_catalog",
                "index_catalog",
            )
            result["live_bytes"] = sum(
                result[label]["key_bytes"] + result[label]["value_bytes"]
                for label in categories
            )
            result["live_records"] = sum(
                result[label]["records"] for label in categories
            )
        return result


def is_control_record(key: bytes) -> bool:
    """Match storage/internal_keys.zig after stripping one physical namespace."""
    if key.startswith(b"\x02db/"):
        end = key.find(b"\x00")
        if end < 0:
            return True
        key = key[end + 1 :]
    if key[:1] in (b"\x02", b"\x03"):
        return True
    if key.startswith(b"\x01"):
        position = 1
        while position + 1 < len(key):
            if key[position] != 0:
                position += 1
            elif key[position + 1] == 0:
                return not (
                    position + 3 == len(key) and key[position + 2] in (0x10, 0x12)
                )
            elif key[position + 1] == 0xFF:
                position += 2
            else:
                return True
        return True
    return key.startswith((b"\x00\x00__", b"\x00antfly/"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument(
        "--include-documents",
        action="store_true",
        help="also split physical authored/control rows and report total logical bytes",
    )
    args = parser.parse_args()
    source = CatalogAccounting(args.database)
    try:
        print(json.dumps(source.account(args.include_documents), indent=2))
    finally:
        source.close()


if __name__ == "__main__":
    main()
