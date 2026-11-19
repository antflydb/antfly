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

    def test_shared_tile_dependencies_stay_inside_standard_library_package(self):
        core = self.root / "zig/lib/tile/src"
        core.mkdir(parents=True)
        source = core / "operation.zig"
        source.write_text('const std = @import("std"); const layout = @import("layout.zig");\n')
        self.assertEqual(len(ownership.verify_shared_dependencies(self.root)), 1)
        source.write_text('const model = @import("../../../pkg/inference/src/models/gpt.zig");\n')
        with self.assertRaisesRegex(ValueError, "Tile core import escapes"):
            ownership.verify(self.root, self.inventory)
        source.write_text('const model = @import("inference_internal");\n')
        with self.assertRaisesRegex(ValueError, "imports application module"):
            ownership.verify(self.root, self.inventory)

    def test_retained_cache_requires_recipe_instead_of_model_import(self):
        cache = self.package / "src/ops/cuda/retained_execution_cache.zig"
        cache.parent.mkdir(parents=True)
        cache.write_text('const context = @import("context.zig"); const layout = @import("../kernel_geometry.zig"); const cleanup = @import("../../execution/cleanup_context.zig");\n')
        self.assertEqual(len(ownership.verify_shared_dependencies(self.root)), 1)
        for name in ("../../models/gpt.zig", "../../backends/embedding_gemma2_cuda.zig", "../../pipelines/generation.zig"):
            cache.write_text('const model = @import("' + name + '");\n')
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "cache imports model/backend adapter"):
                ownership.verify(self.root, self.inventory)

    def test_consumes_regions_and_retains_template_include(self):
        report = ownership.verify(self.root, self.inventory)
        self.assertEqual(report["verification"]["regions_and_files"], 1)
        canonical = report["verification"]["canonical_native"]
        self.assertEqual([row["classification"] for row in canonical], ["canonical_native_surrounds", "canonical_native_include"])
        self.assertEqual(canonical[1]["output"], "leaf.cuh")

    def test_operation_bindings_accept_descriptors_but_reject_model_binding(self):
        source = self.package / "src/ops/cuda/encoder_multimodal.zig"
        source.parent.mkdir(parents=True)
        source.write_text('const params = @import("../ops.zig"); const context = @import("context.zig");\n')
        self.assertEqual(len(ownership.verify_shared_dependencies(self.root)), 1)
        source.write_text('const config = @import("../../models/embedding_gemma2.zig");\n')
        with self.assertRaisesRegex(ValueError, "imports model/backend adapter"):
            ownership.verify_shared_dependencies(self.root)

    def test_shared_program_owner_is_a_verified_build_input(self):
        self.inventory["tile_source_inputs"] = ["leaf.cuh"]
        self.inventory["native_regions"][0]["programs"] = [{"operation": "projection.dot", "source": "leaf.cuh", "scope": "phase", "precision": "F32", "schedule": "serial"}]
        report = ownership.verify(self.root, self.inventory)
        sources = report["verification"]["shared_program_sources"]
        self.assertEqual(sources, [{"source": "leaf.cuh", **ownership.fingerprint((self.package / "leaf.cuh").read_bytes())}])
        self.inventory["tile_source_inputs"] = []
        with self.assertRaisesRegex(ValueError, "absent from build inputs"):
            ownership.verify(self.root, self.inventory)

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

    def test_shared_generated_owner_is_tracked_without_allowing_arbitrary_siblings(self):
        header = self.root / "zig/lib/vectorindex/src/kmeans_distance.msl.h"
        header.parent.mkdir(parents=True)
        header.write_bytes(b"shared distance")
        name = "../../lib/vectorindex/src/kmeans_distance.msl.h"
        entry = {"output": name, "family_region": name, "owner": "generated_file", **ownership.fingerprint(header.read_bytes())}
        self.assertEqual(ownership.verify(self.root, {"schema": 1, "native_regions": [entry]})["verification"]["regions_and_files"], 1)
        entry["output"] = "../../lib/vectorindex/src/unowned.h"
        with self.assertRaisesRegex(ValueError, "escapes"):
            ownership.verify(self.root, {"schema": 1, "native_regions": [entry]})

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
