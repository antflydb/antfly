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

"""Compiler-free identity checks; numerical and regeneration gates are separate."""

import importlib.util
import hashlib
import json
from pathlib import Path
import struct
import unittest

spec = importlib.util.spec_from_file_location(
    "boundary_attention_regen",
    Path(__file__).with_name("regen-cuda-boundary-attention.py"),
)
regen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(regen)


class BoundaryAttentionArtifactsTest(unittest.TestCase):
    def test_manifest_binds_all_architectures_and_sources(self):
        metadata = json.loads(
            (regen.ARTIFACTS / "gliner25_boundary_attention.json").read_text()
        )
        expected = {
            f"gliner25_boundary_attention_{direction}.{suffix}"
            for direction in regen.DIRECTIONS
            for suffix in regen.ARCHITECTURES.values()
        }
        self.assertEqual(set(metadata["artifacts"]), expected)
        self.assertIn("cuda compiler", metadata["compiler"].lower())
        self.assertEqual(metadata["options"], list(regen.OPTIONS))
        self.assertEqual(
            metadata["profile"], "gliner25_boundary_attention_native_d32_v1"
        )
        self.assertEqual(
            set(metadata["source_sha256"]),
            {name for arch in regen.ARCHITECTURES for name in regen.source_files(arch)},
        )
        self.assertEqual(metadata["source_files_by_arch"], {arch: regen.source_files(arch) for arch in regen.ARCHITECTURES})
        self.assertEqual(metadata["profile_by_arch"]["sm_120"], "gliner25_boundary_attention_warp_d32_v2")
        for name, expected_hash in {
            **metadata["artifacts"],
            **metadata["source_sha256"],
        }.items():
            with self.subTest(name=name):
                data = (regen.ARTIFACTS / name).read_bytes()
                self.assertEqual(hashlib.sha256(data).hexdigest(), expected_hash)
                if name.endswith(".cubin"):
                    self.assertEqual(data[:6], b"\x7fELF\x02\x01")
                    flags = struct.unpack_from("<I", data, 48)[0]
                    self.assertEqual(
                        (flags >> 8) & 0xFF, 120 if ".sm120." in name else 80 if ".sm80." in name else 89
                    )
                    entry = (
                        b"boundary_backward"
                        if "_backward." in name
                        else b"boundary_forward"
                    )
                    self.assertIn(entry + b"\x00", data)

    def test_partial_regeneration_rejects_changed_retained_sources(self):
        sources = {name: "current" for arch in regen.ARCHITECTURES for name in regen.source_files(arch)}
        prior = {"source_sha256": dict(sources)}
        regen.validate_retained_sources(prior, sources, ["sm_120"])
        sources[regen.source_name("forward", "sm_80")] = "changed"
        with self.assertRaisesRegex(RuntimeError, "retained sm_89"):
            regen.validate_retained_sources(prior, sources, ["sm_120"])
        regen.validate_retained_sources(prior, sources, list(regen.ARCHITECTURES))


if __name__ == "__main__":
    unittest.main()
