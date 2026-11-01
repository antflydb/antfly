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

import copy
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("ownership", Path(__file__).with_name("kernel_ownership_inventory.py"))
ownership = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ownership)


class OwnershipInventoryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.package = self.root / "zig/pkg/inference"
        self.package.mkdir(parents=True)
        (self.package / "leaf.cuh").write_text("template<int N> void used() {}\n")
        (self.package / "program.cu").write_text('#include "leaf.cuh"\n// tile-codegen:begin projection.0 family (do not edit; run: test)\nabc\n// tile-codegen:end projection.0 family\n')
        self.inventory = {"schema": 1, "native_regions": [{"output": "program.cu", "owner": "tile_region", "family_region": "projection.0", **ownership.fingerprint(b"abc\n")} ]}

    def test_consumes_regions_and_retains_template_include(self):
        report = ownership.verify(self.root, self.inventory)
        self.assertEqual(report["verification"]["regions_and_files"], 1)
        canonical = report["verification"]["canonical_native"]
        self.assertEqual([row["classification"] for row in canonical], ["canonical_native_surrounds", "canonical_native_include"])
        self.assertEqual(canonical[1]["output"], "leaf.cuh")

    def test_records_catalog_image_and_producer_identity(self):
        cuda = self.package / "src/ops/cuda"
        (cuda / "prototypes").mkdir(parents=True)
        (cuda / "image.ptx").write_bytes(b"test PTX")
        (cuda / "prototypes/producer.cu").write_bytes(b"test producer")
        self.inventory["standalone_cuda"] = [{"file": "image.ptx", "producer": "producer.cu"}]
        report = ownership.verify(self.root, self.inventory)
        entry = report["verification"]["standalone_cuda_inputs"][0]
        self.assertEqual(entry["image_identity"], ownership.fingerprint(b"test PTX"))
        self.assertEqual(entry["producer_identity"], ownership.fingerprint(b"test producer"))
        (cuda / "image.ptx").unlink()
        with self.assertRaises(FileNotFoundError):
            ownership.verify(self.root, self.inventory)

    def test_rejects_stale_missing_and_duplicate_ownership(self):
        stale = copy.deepcopy(self.inventory)
        stale["native_regions"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "stale native"):
            ownership.verify(self.root, stale)
        missing = copy.deepcopy(self.inventory)
        missing["native_regions"][0]["family_region"] = "absent"
        with self.assertRaisesRegex(ValueError, "unregistered native"):
            ownership.verify(self.root, missing)
        duplicate = copy.deepcopy(self.inventory)
        duplicate["native_regions"] *= 2
        with self.assertRaisesRegex(ValueError, "duplicate native ownership"):
            ownership.verify(self.root, duplicate)

    def test_rejects_overlap_and_unclosed_markers(self):
        for data in (b"// tile-codegen:begin a family\n", b"// tile-codegen:begin a family\n// quant-kernel-codegen:begin b\n", b"// tile-codegen:begin a family\n// tile-codegen:end b family\n"):
            with self.assertRaises(ValueError):
                ownership.source_regions(data)

    def test_rejects_path_escape_and_whole_file_overlap(self):
        escaped = copy.deepcopy(self.inventory)
        escaped["native_regions"][0]["output"] = "../../../../secret"
        with self.assertRaisesRegex(ValueError, "escapes"):
            ownership.verify(self.root, escaped)
        (self.package / "generated.cu").write_bytes(b"abc")
        whole = {"output": "generated.cu", "family_region": "generated.cu", "owner": "generated_file", **ownership.fingerprint(b"abc")}
        self.assertEqual(ownership.verify(self.root, {"schema": 1, "native_regions": [whole]})["verification"]["regions_and_files"], 1)
        self.inventory["native_regions"].append({**whole, "output": "program.cu"})
        with self.assertRaises(ValueError):
            ownership.verify(self.root, self.inventory)


if __name__ == "__main__":
    unittest.main()
