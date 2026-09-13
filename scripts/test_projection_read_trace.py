import json
import struct
import tempfile
import unittest
import zlib
from pathlib import Path

import numpy as np
from export_projection_read_trace import block_rows, parse_trace, segments


def trace(*rows):
    return "\n".join(
        "[info] antfly_projection_trace " + json.dumps(row) for row in rows
    )


class TraceTest(unittest.TestCase):
    def test_complete_trace_and_interleaving(self):
        rows = parse_trace(
            trace(
                {"batch": 0, "count": 2},
                {"batch": 1, "count": 1},
                {"batch": 0, "slot": 1},
                {"batch": 1, "slot": 0},
                {"batch": 0, "slot": 0},
            )
        )
        self.assertEqual([len(b) for b in rows], [2, 1])
        self.assertEqual([r["slot"] for r in rows[0]], [0, 1])

    def test_partial_duplicate_and_wal_requests_rejected(self):
        for text in (
            "",
            trace({"batch": 0, "count": 2}, {"batch": 0, "slot": 0}),
            trace({"batch": 0, "count": 1}, {"batch": 0, "slot": 0, "wal": True}),
            trace(
                {"batch": 0, "count": 1},
                {"batch": 0, "slot": 0},
                {"batch": 0, "slot": 0},
            ),
        ):
            with self.assertRaises(ValueError):
                parse_trace(text)

    def test_block_export_authenticates_metadata_and_payload(self):
        key = b"key0"
        projection = np.asarray([0.5, -0.25], dtype="<f2").tobytes()
        start = 40 + len(key) + len(projection)
        data = bytearray(start + 88 + 60)
        data[:8] = b"AFVBLK\0\0"
        struct.pack_into(">HHQIIQ", data, 8, 4, 1, 2, 0, 1, 9)
        struct.pack_into(">I", data, 36, zlib.crc32(data[:36]))
        data[40:44], data[44:48] = key, projection
        struct.pack_into(
            ">QQIIQQQIIIfffQII",
            data,
            start,
            0,
            40,
            4,
            zlib.crc32(key),
            9,
            3,
            44,
            2,
            0,
            zlib.crc32(projection),
            1.0,
            0.0,
            0.5,
            0,
            0,
            0,
        )
        footer = start + 88
        struct.pack_into(
            ">QQQQIHHII",
            data,
            footer,
            start,
            1,
            9,
            2,
            zlib.crc32(data[start:footer]),
            4,
            1,
            0,
            1,
        )
        struct.pack_into(
            ">I", data, footer + 48, zlib.crc32(data[footer : footer + 48])
        )
        data[-8:] = data[:8]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "block.afvb"
            path.write_bytes(data)
            rows = list(block_rows(path, len(data)))
            self.assertEqual(rows[0][:4], (key, 3, 44, zlib.crc32(projection)))
            np.testing.assert_array_equal(rows[0][4], [0.5, -0.25])
            data[44] ^= 1
            path.write_bytes(data)
            with self.assertRaisesRegex(ValueError, "checksum"):
                list(block_rows(path, len(data)))

    def test_manifest_lists_only_referenced_files(self):
        data = bytearray(72 + 40 + 12)
        data[:8] = b"AFVBMAN\0"
        struct.pack_into(">HH", data, 8, 6, 0)
        struct.pack_into(">I", data, 56, 1)
        struct.pack_into(">I", data, 68, zlib.crc32(data[:68]))
        struct.pack_into(">QQIIQII", data, 72, 2, 9, 3, 0, 100, 0, 0)
        struct.pack_into(">I", data, len(data) - 12, zlib.crc32(data[:-12]))
        data[-8:] = data[:8]
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "CURRENT").write_bytes(data)
            self.assertEqual(segments(Path(directory)), [(2, 3, 100)])


if __name__ == "__main__":
    unittest.main()
