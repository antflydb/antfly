# Copyright 2026 Antfly, Inc.
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

"""Exercise cache contracts against production construction with tiny bodies."""

from __future__ import annotations

import hashlib
import re
import shutil
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path

ZIG_ROOT = Path(__file__).resolve().parents[1]
UNITS = ("cli", "distributed", "serverless", "inference", "api_kernel")
SCHEMAS = (
    "specs/openapi/ard/api.yaml",
    "openapi.yaml",
    "specs/openapi/antfly/metadata.yaml",
    "specs/openapi/extensions/api.yaml",
    "specs/openapi/auth/api.yaml",
    "specs/openapi/inference/config.yaml",
)
METAL = "zig/pkg/inference/src/backends/metal_kernels.m"
CUDA = "zig/pkg/inference/src/ops/cuda/artifacts/inference_cuda_kernels.cu"


def link_children(source, destination):
    destination.mkdir()
    for child in source.iterdir():
        if child.name in {".git", ".worktrees", ".zig-cache", "zig-out"}:
            continue
        (destination / child.name).symlink_to(child, target_is_directory=child.is_dir())


class RuntimeCacheTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "repo"
        link_children(ZIG_ROOT.parent, self.root)
        self.own("zig/build.zig")
        shutil.copyfile(
            self.root / "zig/build.zig", self.root / "zig/project_build.zig"
        )
        shutil.copyfile(
            ZIG_ROOT / "tools/fixtures/runtime_cache.zig", self.root / "zig/build.zig"
        )

    def own(self, relative):
        """Copy only edited paths; never write through a repository symlink."""
        path = self.root
        for component in Path(relative).parts[:-1]:
            path /= component
            if path.is_symlink():
                source = path.resolve()
                path.unlink()
                link_children(source, path)
        path = self.root / relative
        if path.is_symlink():
            source = path.resolve()
            path.unlink()
            shutil.copyfile(source, path)
        return path

    def build(
        self, *targets, version="cache-before", backend=None, settings=(), succeeds=True
    ):
        result = subprocess.run(
            [
                "zig",
                "build",
                *targets,
                f"-Dmetal={'true' if backend == 'metal' else 'false'}",
                f"-Dcuda={'true' if backend == 'cuda' else 'false'}",
                "-Dsystem-blas=false",
                f"-Dantfly-version={version}",
                *settings,
                "--summary",
                "all",
                "--color",
                "off",
                "--cache-dir",
                str(self.root / "cache"),
                "-j2",
            ],
            cwd=self.root / "zig",
            text=True,
            capture_output=True,
            check=False,
            timeout=240,
        )
        output = result.stdout + result.stderr
        if succeeds:
            self.assertEqual(result.returncode, 0, output)
        else:
            self.assertNotEqual(result.returncode, 0, output)
        return output

    def assert_compile(self, output, unit, status):
        name = (
            "antfly-storage-kernel"
            if unit == "distributed"
            else f"antfly-runtime-{unit}"
        )
        self.assertRegex(output, rf"compile lib {name} Debug \S+ {status}")

    def assert_archives(self, output, rebuilt=()):
        for unit in UNITS:
            self.assert_compile(
                output, unit, "success" if unit in rebuilt else "cached"
            )

    def probe(self, output, label="CACHE_PROBE"):
        match = re.search(rf"^{label} (.+)$", output, re.MULTILINE)
        self.assertIsNotNone(match, output)
        return match.group(1)

    def test_cpu_cache_contracts(self):
        # Missing disabled-backend sources must not affect graph configuration,
        # ordinary CPU artifacts, or schedule an identity generator.
        metal = self.own(METAL)
        cuda = self.own(CUDA)
        metal_bytes, cuda_bytes = metal.read_bytes(), cuda.read_bytes()
        metal.unlink()
        cuda.unlink()
        self.build("--help")
        first = self.build("cache-probe", "cache-tokenizer")
        self.assertNotIn("jit-source-identity", first)
        warm = self.build("cache-probe", "cache-tokenizer")
        self.assert_archives(warm)
        self.assertIn("WriteFile tokenizer.json cached", warm)
        self.assertRegex(warm, r"run exe patch_sentencepiece_proto .* cached")
        self.assertRegex(warm, r"compile obj antfly-build-info Debug \S+ cached")
        before = self.probe(warm)
        tokenizer_before = self.probe(warm, "TOKENIZER_PROBE")

        metal.write_bytes(metal_bytes + b"\n// disabled source edit\n")
        cuda.write_bytes(cuda_bytes + b"\n// disabled source edit\n")
        output = self.build("cache-probe")
        self.assert_archives(output)
        self.assertNotIn("jit-source-identity", output)
        self.assertEqual(self.probe(output), before)

        # Inactive settings are absent from product cache identities, but remain
        # available to CPU-hosted backend qualification tests.
        self.build("cache-product-options", "cache-qualification-options")
        for settings, expected in (
            (("-Dcuda-artifacts=portable",), "portable auto wasm32 false"),
            (("-Dwasm-memory-model=wasm64",), "fatbin auto wasm64 false"),
            (("-Dwebgpu=true",), "fatbin auto wasm32 true"),
        ):
            with self.subTest(settings=settings):
                output = self.build(
                    "cache-probe", "cache-product-options", settings=settings
                )
                self.assert_archives(output)
                self.assertEqual(self.probe(output), before)
                self.assertEqual(
                    self.probe(output, "OPTIONS_PROBE"), "fatbin auto wasm32 false"
                )
                qualified = self.build("cache-qualification-options", settings=settings)
                self.assertEqual(self.probe(qualified, "OPTIONS_PROBE"), expected)

        enabled = self.build(
            "cache-product-options",
            backend="cuda",
            settings=("-Dcuda-artifacts=portable",),
        )
        self.assertEqual(
            self.probe(enabled, "OPTIONS_PROBE"), "portable auto wasm32 false"
        )

        # The real test module graph must stay cached when release metadata changes.
        self.build("cache-unit-tests")
        unit_versioned = self.build("cache-unit-tests", version="cache-after")
        self.assertRegex(unit_versioned, r"compile test Debug \S+ cached")
        self.assertNotRegex(unit_versioned, r"compile test Debug \S+ success")
        self.assertNotIn("antfly-build-info", unit_versioned)

        for schema in SCHEMAS:
            with self.subTest(schema=schema):
                path = self.own(schema)
                path.write_bytes(path.read_bytes() + b"\n# cache regression edit\n")
                output = self.build("cache-probe")
                self.assert_archives(output, rebuilt=("api_kernel",))
                self.assertNotEqual(self.probe(output).split()[-1], before.split()[-1])
                before = self.probe(output)

        tokenizer = self.own("zig/lib/tokenizer/testdata/embedder/tokenizer.json")
        tokenizer.write_bytes(tokenizer.read_bytes() + b"\n")
        output = self.build("cache-probe", "cache-tokenizer")
        self.assert_archives(
            output, rebuilt=tuple(unit for unit in UNITS if unit != "cli")
        )
        self.assertNotEqual(self.probe(output, "TOKENIZER_PROBE"), tokenizer_before)

        # A generator implementation change must rerun it and invalidate its
        # consumers when generated bytes change, without touching remote CLI.
        generator = self.own("zig/lib/tokenizer/tools/patch_sentencepiece_proto.zig")
        text = generator.read_text()
        self.assertIn('"root.zig", root_bytes', text)
        generator.write_text(
            text.replace(
                '"root.zig", root_bytes',
                '"root.zig", try std.fmt.allocPrint(arena, "{s}\\npub const cache_revision = 1;\\n", .{root_bytes})',
            )
        )
        output = self.build("cache-probe")
        self.assertRegex(output, r"run exe patch_sentencepiece_proto .* success")
        self.assert_archives(
            output, rebuilt=tuple(unit for unit in UNITS if unit != "cli")
        )

        versioned = self.build("cache-probe", version="cache-after")
        self.assert_archives(versioned)
        self.assertRegex(versioned, r"compile obj antfly-build-info Debug \S+ success")
        self.assertRegex(versioned, r"compile exe antfly Debug \S+ success")
        self.assertEqual(self.probe(versioned).split()[0], "cache-after")
        self.assertEqual(self.probe(versioned).split()[1:], before.split()[1:])

        # Positive control: changing real shared code must rebuild consumers
        # and change linked behavior. This edit exists only in the overlay.
        shared = self.own("zig/lib/hash/src/adler32.zig")
        text = shared.read_text()
        self.assertIn("state: u32 = 1,", text)
        shared.write_text(text.replace("state: u32 = 1,", "state: u32 = 2,"))
        changed = self.build("cache-probe", version="cache-after")
        self.assert_archives(changed, rebuilt=UNITS)
        self.assertNotEqual(
            self.probe(changed).split()[1:5], self.probe(versioned).split()[1:5]
        )
        self.assert_archives(self.build("cache-probe", version="cache-after"))

        storage_options = self.build(
            "cache-probe", version="cache-after", settings=("-Dwith_tla=true",)
        )
        self.assert_archives(
            storage_options, rebuilt=("distributed", "serverless", "api_kernel")
        )

        # Served schemas remain unnecessary to all non-HTTP archive targets.
        self.own(SCHEMAS[0]).unlink()
        unrelated = tuple(unit for unit in UNITS if unit != "api_kernel")
        output = self.build(
            *(f"runtime-unit-{unit}" for unit in unrelated), version="cache-after"
        )
        for unit in unrelated:
            self.assert_compile(output, unit, "cached")
        self.assertIn(
            "FileNotFound", self.build("runtime-unit-api_kernel", succeeds=False)
        )

    def test_host_generator_cache_contracts(self):
        names = ("openapi-zig", "antfly-quant-kernel-codegen", "protoc-zig", "yacc-zig")
        self.build("cache-host-tools")
        for settings in (
            ("-Doptimize=ReleaseFast",),
            ("-Doptimize=ReleaseSafe", "-Dcuda-artifacts=portable", "-Dwebgpu=true"),
            ("-Dtarget=x86_64-linux-musl", "-Doptimize=ReleaseFast"),
        ):
            with self.subTest(settings=settings):
                output = self.build("cache-host-tools", settings=settings)
                for name in names:
                    self.assertRegex(
                        output, rf"compile exe {name} ReleaseSafe \S+ cached"
                    )

        # Real generator source remains an input despite independence from the
        # product profile, target, and inactive backend settings.
        source = self.own("zig/pkg/inference/src/quant_kernel_codegen_main.zig")
        source.write_bytes(source.read_bytes() + b"\n// generator cache regression\n")
        output = self.build("cache-host-tools")
        self.assertRegex(
            output, r"compile exe antfly-quant-kernel-codegen ReleaseSafe \S+ success"
        )
        for name in names:
            if name != "antfly-quant-kernel-codegen":
                self.assertRegex(output, rf"compile exe {name} ReleaseSafe \S+ cached")

    def test_enabled_backend_identities(self):
        for backend, source in (("metal", METAL), ("cuda", CUDA)):
            with self.subTest(backend=backend):
                first = self.build(
                    "cache-identity", "runtime-unit-cli", backend=backend
                )
                label = f"{backend.upper()}_IDENTITY"
                before = self.probe(first, label).split()
                path = self.own(source)
                self.assertEqual(
                    before[0], hashlib.sha256(path.read_bytes()).hexdigest()
                )
                warm = self.build("cache-identity", "runtime-unit-cli", backend=backend)
                self.assertIn("jit-source-identity", warm)
                self.assert_compile(warm, "cli", "cached")
                self.assertRegex(warm, r"run exe jit-source-identity .* cached")
                path.write_bytes(path.read_bytes() + b"\n// enabled source edit\n")
                output = self.build(
                    "cache-identity", "runtime-unit-cli", backend=backend
                )
                after = self.probe(output, label).split()
                self.assertEqual(
                    after[0], hashlib.sha256(path.read_bytes()).hexdigest()
                )
                self.assertNotEqual(after[0], before[0])
                self.assertEqual(after[1:], before[1:])
                self.assert_compile(output, "cli", "cached")
                self.assertRegex(output, r"run exe jit-source-identity .* success")
                # Check the preserved bundle domain, count, length, and order
                # independently of the Zig hashing implementation.
                qualifier = (
                    ["src/backends/metal_runtime.zig"]
                    if backend == "metal"
                    else ["src/ops/cuda/kernels.zig"]
                ) + ["src/graph/kernel_jit.zig", "src/graph/quant_kernel_compiler.zig"]
                if backend == "cuda":
                    qualifier.append("src/graph/quant_kernel_cuda_renderer.zig")
                qualifier += [
                    "src/graph/quant_matmul.zig",
                    "src/gguf/quant_codec.zig",
                    "src/gguf/tensor_types.zig",
                ]
                self.assertEqual(after[1], self.bundle_digest(qualifier))
                if backend == "cuda":
                    self.assertEqual(
                        after[2],
                        self.bundle_digest(
                            [
                                "src/ops/cuda/cuda_compute.zig",
                                "src/graph/quant_kernel_compiler.zig",
                                "src/graph/quant_matmul.zig",
                                "src/gguf/tensor_types.zig",
                            ]
                        ),
                    )
                qualifier_source = self.own("zig/pkg/inference/" + qualifier[0])
                qualifier_source.write_bytes(
                    qualifier_source.read_bytes() + b"\n// qualification edit\n"
                )
                output = self.build(
                    "cache-identity", "runtime-unit-cli", backend=backend
                )
                self.assertEqual(
                    self.probe(output, label).split()[1], self.bundle_digest(qualifier)
                )
                self.assertNotEqual(self.probe(output, label).split()[1], after[1])
                self.assert_compile(output, "cli", "cached")

    def bundle_digest(self, paths):
        digest = hashlib.sha256(b"antfly-runtime-jit-source-bundle/v1")
        digest.update(struct.pack("<Q", len(paths)))
        for path in paths:
            data = (self.root / "zig/pkg/inference" / path).read_bytes()
            digest.update(struct.pack("<Q", len(data)))
            digest.update(data)
        return digest.hexdigest()


if __name__ == "__main__":
    unittest.main()
