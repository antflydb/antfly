"""Export authenticated immutable vectors and complete bounded live read batches.

Requires an offline diagnostic clone. Never changes source data. Rejects WAL
requests, stale/missing locations, unsupported formats, duplicate identities,
or truncated trace batches rather than silently improving the measured sample.
"""

import argparse
import hashlib
import json
import mmap
import struct
import zlib
from pathlib import Path

import numpy as np


def integer(data, offset, size=4):
    return int.from_bytes(data[offset : offset + size], "big")


def verify_crc(data, start, end, expected):
    if zlib.crc32(data[start:end]) != expected:
        raise ValueError("checksum mismatch in immutable input")


def segments(root):
    data = (root / "CURRENT").read_bytes()
    if len(data) < 84 or data[:8] != b"AFVBMAN\0" or integer(data, 8, 2) != 6:
        raise ValueError("unsupported vector manifest")
    verify_crc(data, 0, 68, integer(data, 68))
    verify_crc(data, 0, len(data) - 12, integer(data, len(data) - 12))
    extents, count, coverages = (
        integer(data, 10, 2),
        integer(data, 56),
        integer(data, 60),
    )
    start = 72 + 48 * extents
    if start + count * 40 + coverages * 32 + 12 != len(data) or data[-8:] != data[:8]:
        raise ValueError("invalid manifest framing")
    return [
        (integer(data, i, 8), integer(data, i + 16), integer(data, i + 24, 8))
        for i in range(start, start + count * 40, 40)
    ]


def block_rows(path, expected_size):
    with path.open("rb") as file, mmap.mmap(
        file.fileno(), 0, access=mmap.ACCESS_READ
    ) as data:
        if (
            len(data) != expected_size
            or data[:8] != b"AFVBLK\0\0"
            or integer(data, 8, 2) != 4
        ):
            raise ValueError("unsupported or truncated immutable block")
        encoding = integer(data, 10, 2)
        if encoding != 1:
            raise ValueError("packing screen requires actual float16 source planes")
        footer = len(data) - 60
        verify_crc(data, 0, 36, integer(data, 36))
        verify_crc(data, footer, footer + 48, integer(data, footer + 48))
        start, count = integer(data, footer, 8), integer(data, footer + 8, 8)
        if start + count * 88 != footer or data[-8:] != data[:8]:
            raise ValueError("invalid block framing")
        verify_crc(data, start, footer, integer(data, footer + 32))
        for pos in range(start, footer, 88):
            key_offset, key_len = integer(data, pos + 8, 8), integer(data, pos + 16)
            offset, dims = integer(data, pos + 40, 8), integer(data, pos + 48)
            if integer(data, pos + 52) & 1:
                continue
            if (
                key_offset < 40
                or key_offset + key_len > start
                or offset < 40
                or offset + 2 * dims > start
                or dims == 0
            ):
                raise ValueError("invalid key/vector range")
            verify_crc(data, key_offset, key_offset + key_len, integer(data, pos + 20))
            checksum = integer(data, pos + 56)
            verify_crc(data, offset, offset + 2 * dims, checksum)
            scale = struct.unpack_from(">f", data, pos + 60)[0]
            vector = (
                np.frombuffer(data[offset : offset + 2 * dims], dtype="<f2").astype(
                    np.float32
                )
                * scale
            )
            yield data[key_offset : key_offset + key_len], integer(
                data, pos + 32, 8
            ), offset, checksum, vector


def parse_trace(text):
    batches = {}
    for line in text.splitlines():
        marker = "antfly_projection_trace "
        if marker not in line:
            continue
        row = json.loads(line.split(marker, 1)[1])
        batch = batches.setdefault(row["batch"], {"count": None, "rows": {}})
        if "count" in row:
            if batch["count"] is not None:
                raise ValueError("duplicate batch header")
            batch["count"] = row["count"]
        else:
            if row.get("wal") or row["slot"] in batch["rows"]:
                raise ValueError("WAL or duplicate request in trace")
            batch["rows"][row["slot"]] = row
    if not batches:
        raise ValueError("no projection trace")
    result = []
    for _, batch in sorted(batches.items()):
        count = batch["count"]
        if (
            type(count) is not int
            or not 0 < count <= 256
            or sorted(batch["rows"]) != list(range(count))
        ):
            raise ValueError("truncated projection trace batch")
        result.append([batch["rows"][i] for i in range(count)])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path)
    parser.add_argument("log", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    batches = parse_trace(args.log.read_text())
    roots = sorted({Path(row["root"]).resolve() for batch in batches for row in batch})
    if any(not root.is_relative_to(args.data.resolve()) for root in roots):
        raise ValueError("trace root is outside the offline diagnostic clone")
    vectors, lengths, files, offsets = [], [], [], []
    locations, identities, hashes = {}, set(), {}
    for root in roots:
        manifest_path = root / "CURRENT"
        hashes[str(manifest_path)] = hashlib.sha256(
            manifest_path.read_bytes()
        ).hexdigest()
        for generation, shard, size in segments(root):
            path = root / f"block-{generation}-{shard}.afvb"
            hashes[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
            file_id = len(hashes)
            for key, revision, offset, checksum, vector in block_rows(path, size):
                identity = (root, key, revision)
                if identity in identities:
                    raise ValueError(
                        "duplicate source revision: cannot model single-copy baseline"
                    )
                identities.add(identity)
                locations[
                    (str(root), generation, shard, offset, len(vector) * 2, checksum)
                ] = len(vectors)
                vectors.append(vector)
                lengths.append(len(vector) * 2)
                files.append(file_id)
                offsets.append(offset)
    indices = [
        [
            locations[
                (
                    str(Path(r["root"]).resolve()),
                    r["generation"],
                    r["shard"],
                    r["offset"],
                    r["length"],
                    r["checksum"],
                )
            ]
            for r in batch
        ]
        for batch in batches
    ]
    if any(
        hashlib.sha256(Path(p).read_bytes()).hexdigest() != h for p, h in hashes.items()
    ):
        raise RuntimeError("offline source changed during export")
    args.output.mkdir(parents=True, exist_ok=False)
    np.savez(
        args.output / "source.npz",
        vectors=np.asarray(vectors),
        lengths=lengths,
        file_ids=files,
        offsets=offsets,
    )
    (args.output / "trace.json").write_text(
        json.dumps(
            {
                "batches": indices,
                "source_sha256": hashes,
                "trace_sha256": hashlib.sha256(args.log.read_bytes()).hexdigest(),
            }
        )
        + "\n"
    )
    print(f"Exported {len(vectors)} source rows and {len(batches)} actual read batches")


if __name__ == "__main__":
    main()
