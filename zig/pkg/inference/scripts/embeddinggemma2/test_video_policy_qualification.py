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

from pathlib import Path
import struct
import unittest

import video_policy_qualification as policy


class PinnedVideoPolicyFixtures(unittest.TestCase):
    def setUp(self):
        self.source = (
            Path(__file__).parent / "testdata/video/video-mjpeg.mov"
        ).read_bytes()

    def test_all_variants_preserve_compressed_pixels_and_vfr_fixture_matches(self):
        original = [p for k, p in policy.boxes(self.source) if k == b"mdat"]
        variants = policy.variants(self.source)
        for name, (encoded, _) in variants.items():
            with self.subTest(name=name):
                self.assertEqual(
                    original, [p for k, p in policy.boxes(encoded) if k == b"mdat"]
                )
        vfr = variants["vfr_average_fps"][0]
        index = vfr.index(b"stts") + 4
        entries = struct.unpack_from(">I", vfr, index + 4)[0]
        durations = [
            struct.unpack_from(">II", vfr, index + 8 + i * 8) for i in range(entries)
        ]
        self.assertEqual(sum(count for count, _ in durations), 8)
        self.assertEqual(sum(count * ticks for count, ticks in durations), 32768)
        self.assertGreater(len(set(ticks for _, ticks in durations)), 1)
        native_fixture = (
            Path(__file__).resolve().parents[5]
            / "zig/pkg/inference/src/testdata/video/mjpeg-vfr.mov"
        )
        self.assertEqual(vfr, native_fixture.read_bytes())

    def test_changed_metadata_and_unsupported_layouts_cannot_reuse_oracle(self):
        wrong_clock = bytearray(self.source)
        index = wrong_clock.index(b"stts") + 4
        struct.pack_into(">I", wrong_clock, index + 12, 4095)
        with self.assertRaisesRegex(ValueError, "clock changed"):
            policy.variants(bytes(wrong_clock))
        top = list(policy.boxes(self.source))
        front_loaded = b"".join(
            policy.box(k, p) for k, p in sorted(top, key=lambda b: b[0] != b"moov")
        )
        with self.assertRaisesRegex(ValueError, "metadata must follow"):
            policy.variants(front_loaded)
        for malformed in [b"", b"123", struct.pack(">I4s", 100, b"moov")]:
            with self.assertRaises(ValueError):
                policy.variants(malformed)


if __name__ == "__main__":
    unittest.main()
