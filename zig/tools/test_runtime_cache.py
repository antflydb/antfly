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
        self.build_directory = self.root / "zig"
        link_children(ZIG_ROOT.parent, self.root)
        self.own("zig/build.zig")
        shutil.copyfile(
            self.root / "zig/build.zig", self.root / "zig/project_build.zig"
        )
        shutil.copyfile(
            ZIG_ROOT / "tools/fixtures/runtime_cache.zig", self.root / "zig/build.zig"
        )

    def use_standalone(self):
        build = self.own("zig/pkg/inference/build.zig")
        shutil.copyfile(build, build.with_name("project_build.zig"))
        shutil.copyfile(ZIG_ROOT / "tools/fixtures/inference_cache.zig", build)
        shutil.copyfile(
            ZIG_ROOT / "tools/fixtures/build_profiles.zig",
            build.with_name("cache_profiles.zig"),
        )
        self.build_directory = build.parent

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
            cwd=self.build_directory,
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

    def test_openapi_discovered_inputs(self):
        # Help and unrelated library tests must configure without any schemas.
        self.own("specs/unused")
        schemas = self.root / "specs/openapi"
        parked = schemas.with_name("parked-openapi")
        schemas.rename(parked)
        try:
            self.build("--help")
            self.build("lib-hash-test")
        finally:
            parked.rename(schemas)

        # Reference resolution uses canonical paths, so keep the schema tree
        # within the overlay rather than mixing real and symlinked identities.
        schemas.unlink()
        shutil.copytree(ZIG_ROOT.parent / "specs/openapi", schemas)
        self.own("openapi.yaml")

        # Give scripts that resolve __file__ their own repository-relative root.
        for script in (
            "join_openapi.py",
            "join_public_openapi.py",
            "openapi_joiner.py",
        ):
            self.own(f"scripts/{script}")
        self.build("cache-openapi")
        warm = self.build("cache-openapi")
        self.assert_join(warm, "joined", "cached")
        self.assert_join(warm, "prefixed", "cached")

        # Missing repository schemas must fail instead of caching a fallback to
        # the already-generated bundle. Restoring a changed schema must be read.
        indexes = self.own("specs/openapi/antfly/indexes.yaml")
        original_indexes = indexes.read_bytes()
        indexes.unlink()
        missing = self.build("cache-openapi", succeeds=False)
        self.assertIn("indexes.yaml", missing)
        indexes.write_bytes(
            original_indexes.replace(
                b"Configuration for an index", b"Restored schema cache regression"
            )
        )
        restored = self.build("cache-openapi")
        self.assert_join(restored, "prefixed", "success")
        self.assert_join(restored, "joined", "cached")
        self.assert_fresh_public_schema()

        # A same-named file beside the generated bundle cannot shadow its owner.
        shadow = self.own("indexes.yaml")
        shadow.write_bytes(
            original_indexes.replace(
                b"Configuration for an index", b"Incorrect shadow schema"
            )
        )
        self.assert_join(self.build("cache-openapi"), "prefixed", "cached")
        self.assert_fresh_public_schema()

        unrelated = self.own("specs/openapi/cache-unrelated.yaml")
        unrelated.write_text("unrelated: true\n")
        output = self.build("cache-openapi")
        self.assert_join(output, "joined", "cached")
        self.assert_join(output, "prefixed", "cached")

        inference = self.own("specs/openapi/inference/api.yaml")
        inference.write_bytes(inference.read_bytes() + b"\n# tracked input edit\n")
        output = self.build("cache-openapi")
        self.assert_join(output, "joined", "cached")
        self.assert_join(output, "prefixed", "success")

        # Adding a reference must discover a new dependency, including paths
        # with spaces. Subsequent edits must invalidate only the bundling join.
        dependency = self.own("specs/openapi/antfly/cache dependency.yaml")
        dependency.write_text(
            '{"components":{"schemas":{"CacheProbe":{"type":"string"}}}}'
        )
        metadata = self.own("specs/openapi/antfly/metadata.yaml")
        metadata.write_bytes(
            metadata.read_bytes()
            + b'\nx-cache-probe: {$ref: "specs/openapi/antfly/cache dependency.yaml#/components/schemas/CacheProbe"}\n'
        )
        output = self.build("cache-openapi")
        self.assert_join(output, "joined", "success")
        self.assert_join(output, "prefixed", "success")
        dependency.write_text(
            '{"components":{"schemas":{"CacheProbe":{"type":"integer"}}}}'
        )
        output = self.build("cache-openapi")
        self.assert_join(output, "joined", "cached")
        self.assert_join(output, "prefixed", "success")
        warm = self.build("cache-openapi")
        self.assert_join(warm, "prefixed", "cached")

        # Track the logical reference path when a schema is supplied by symlink.
        first = dependency.with_name("first.yaml")
        second = dependency.with_name("second.yaml")
        dependency.rename(first)
        second.write_text(
            '{"components":{"schemas":{"CacheProbe":{"type":"boolean"}}}}'
        )
        dependency.symlink_to(first.name)
        self.build("cache-openapi")
        dependency.unlink()
        dependency.symlink_to(second.name)
        self.assert_join(self.build("cache-openapi"), "prefixed", "success")
        self.assert_fresh_public_schema()

    def assert_fresh_public_schema(self):
        cached = max(
            (self.root / "cache/o").glob("*/openapi.public.prefixed.yaml"),
            key=lambda path: path.stat().st_mtime_ns,
        )
        fresh = self.root / "fresh-public.yaml"
        result = subprocess.run(
            [
                "uv",
                "run",
                "--project",
                str(self.root / "scripts"),
                "--locked",
                "python",
                str(self.root / "scripts/join_public_openapi.py"),
                str(fresh),
            ],
            text=True,
            capture_output=True,
            timeout=60,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(cached.read_bytes(), fresh.read_bytes())

    def test_simulation_cache_contracts(self):
        source = self.own("zig/lib/vopr/src/root.zig")
        source.write_bytes(
            source.read_bytes() + b"\npub const cache_test_revision: u8 = 1;\n"
        )
        targets = ("cache-probe", "cache-vopr-tests")
        self.assertIn("VOPR_REVISION 1", self.build(*targets))
        self.assert_archives(self.build(*targets))
        source.write_bytes(
            source.read_bytes().replace(
                b"pub const cache_test_revision: u8 = 1;",
                b"pub const cache_test_revision: u8 = 2;",
            )
        )
        changed = self.build(*targets)
        self.assert_archives(changed)
        self.assertRegex(changed, r"compile test Debug \S+ success")
        self.assertIn("VOPR_REVISION 2", changed)
        self.assertRegex(self.build(*targets), r"compile test Debug \S+ cached")
        # The package can still configure its test artifacts without reading
        # their source; only building the simulation consumer needs that file.
        source.unlink()
        self.assert_archives(self.build("cache-probe"))
        self.assertIn("FileNotFound", self.build("cache-vopr-tests", succeeds=False))

    def test_optional_onnx_dependencies(self):
        source = self.own("zig/lib/audio/src/mod.zig")
        source.write_bytes(
            source.read_bytes()
            + b'\npub const cache_test_profile = @import("builtin").mode;\n'
        )
        settings = ("-Donnx=true", f"-Donnx-root={self.root / 'missing-onnx'}")
        for standalone in (False, True):
            with self.subTest(standalone=standalone):
                if standalone:
                    self.use_standalone()
                self.build("--help", settings=settings)
                self.assertIn(
                    "BENCH_PROFILE Debug Debug",
                    self.build("cache-antfly-inference-audio-bench", settings=settings),
                )
                if not standalone:
                    self.build("lib-hash-test", settings=settings)
                target = "cache-inference" if standalone else "cache-probe"
                self.build(target)
                failure = self.build(target, settings=settings, succeeds=False)
                self.assertIn("onnxruntime", failure)
                self.assertNotIn("panic:", failure)
                self.assertIn("Build Summary:", failure)
                self.build(target)

    def test_native_artifact_profiles(self):
        for source in ("zig/lib/audio/src/mod.zig", "zig/lib/linalg/src/mod.zig"):
            path = self.own(source)
            path.write_bytes(
                path.read_bytes()
                + b'\npub const cache_test_profile = @import("builtin").mode;\n'
            )
        targets = (
            "cache-antfly-inference-audio-bench",
            "cache-antfly-inference-linalg-bench",
        )
        for standalone in (False, True):
            if standalone:
                self.use_standalone()
            for mode in ("Debug", "ReleaseFast"):
                with self.subTest(standalone=standalone, mode=mode):
                    settings = (f"-Doptimize={mode}",)
                    output = self.build(*targets, settings=settings)
                    self.assertEqual(output.count(f"BENCH_PROFILE {mode} {mode}"), 2)
                    warm = self.build(
                        *targets, settings=settings, version="profile-change"
                    )
                    for name in ("audio", "linalg"):
                        self.assertRegex(
                            warm,
                            rf"compile exe antfly-inference-{name}-bench {mode} \S+ cached",
                        )
            # All import graphs are inspected, including foreign artifacts and
            # explicit profiles such as the isolated PDF build and WASM.
            settings = ("-Doptimize=ReleaseSafe", "-Dtarget=x86_64-linux-musl")
            if not standalone:
                settings += ("-Dpdf-optimize=Debug",)
            self.build("--help", settings=settings)

    def test_tool_metadata_consumers(self):
        self.use_standalone()
        targets = ("cache-pilot", "cache-training-version")
        first = self.build(*targets)
        self.assertIn("TRAINING_VERSION cache-before", first)
        files = list((self.root / "cache/o").glob("*/pilot.jsonl"))
        self.assertEqual(len(files), 1)
        before = files[0].read_bytes()
        self.assertEqual(len(before.splitlines()), 2)
        changed = self.build(*targets, version="cache-after")
        self.assertIn("TRAINING_VERSION cache-after", changed)
        self.assertRegex(
            changed, r"compile exe generate-gemma4-pilot-dataset Debug \S+ cached"
        )
        self.assertRegex(
            changed, r"compile exe train-gliner2-autodiff Debug \S+ success"
        )
        self.assertEqual(files[0].read_bytes(), before)
        self.assertRegex(
            self.build(*targets, version="cache-after"),
            r"compile exe train-gliner2-autodiff Debug \S+ cached",
        )

    def test_wasm_profile_cache_contracts(self):
        for source in ("zig/lib/httpx/src/httpx.zig", "zig/lib/json/src/mod.zig"):
            path = self.own(source)
            path.write_bytes(
                path.read_bytes()
                + b'\npub const cache_test_profile = @import("builtin").mode;\n'
            )
        self.build("cache-wasm")
        for settings in (
            (),
            ("-Doptimize=ReleaseFast",),
            (
                "-Dtarget=x86_64-linux-musl",
                "-Doptimize=ReleaseFast",
                "-Dlmdb_evented_async_io=true",
            ),
        ):
            with self.subTest(settings=settings):
                result = self.build("cache-wasm", settings=settings)
                self.assertRegex(
                    result,
                    r"compile exe antfly_wasm ReleaseSafe wasm32-freestanding cached",
                )

    def assert_join(self, output, kind, status):
        self.assertRegex(output, rf"run uv \(openapi.public.{kind}.yaml\) {status}")

    def test_inference_openapi_override_inputs(self):
        settings = ("-Dinference-openapi-spec=../specs/openapi/inference/api.yaml",)
        self.build("cache-probe", settings=settings)
        warm = self.build("cache-probe", settings=settings)
        self.assert_archives(warm)
        self.assertRegex(warm, r"run uv \(inference_api.json\) cached")

        # A converter edit that changes generated bytes must invalidate consumers.
        converter = self.own("scripts/yaml_to_json.py")
        source = converter.read_text()
        self.assertIn("normalize(data)", source)
        converter.write_text(
            source.replace(
                "normalize(data)",
                'normalize(data)\n    data["components"]["schemas"]["CacheProbe"] = {"type": "string"}',
            )
        )
        changed = self.build("cache-probe", settings=settings)
        self.assertRegex(changed, r"run uv \(inference_api.json\) success")
        self.assertRegex(changed, r"run exe openapi-zig \(inference_api\) success")
        self.assert_compile(changed, "inference", "success")
        self.assert_compile(changed, "cli", "cached")

        for name in ("pyproject.toml", "uv.lock"):
            project_input = self.own(f"scripts/{name}")
            project_input.write_bytes(
                project_input.read_bytes() + b"\n# cache input probe\n"
            )
            changed = self.build("cache-probe", settings=settings)
            self.assertRegex(changed, r"run uv \(inference_api.json\) success")
        # The same production graph checks also cover non-Debug configuration.
        self.build("--help", settings=("-Doptimize=ReleaseFast",))

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

        # HTTPX is a dependency of generated consumers, not of the host compiler.
        httpx = self.own("zig/lib/httpx/src/httpx.zig")
        httpx.write_bytes(httpx.read_bytes() + b"\n// unrelated HTTP runtime edit\n")
        output = self.build("cache-host-tools")
        for name in names:
            self.assertRegex(output, rf"compile exe {name} ReleaseSafe \S+ cached")

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

    def test_sql_and_snowball_generation_contracts(self):
        sql = self.own("zig/lib/sql/grammar/generated/root.zig")
        snowball_root = "zig/pkg/antfly/src/search/snowball/generated"
        for path in (self.root / snowball_root).glob("*.zig"):
            self.own(f"{snowball_root}/{path.name}")
        snowball = self.root / snowball_root / "german_stemmer.zig"

        self.build("regen-sql-grammar", "regen-snowball")
        generated = {
            path: (path.read_bytes(), path.stat().st_mtime_ns)
            for path in (self.root / "cache/o").rglob("*.zig")
        }
        expected = {path: path.read_bytes() for path in (sql, snowball)}
        checked = self.build("sql-grammar-generated-check", "check-snowball")
        self.assertRegex(checked, r"run exe yacc-zig \(sql_grammar_root.zig\) cached")
        self.assertEqual(
            len(re.findall(r"run exe snowball \(\w+_stemmer.zig\) cached", checked)),
            10,
        )
        self.assertEqual(len(re.findall(r"format Snowball [\w.]+ cached", checked)), 12)

        # Checking reports drift without repairing it, even with warm generators.
        for path in expected:
            path.write_bytes(b"// deliberately stale generated source\n")
        self.build("sql-grammar-generated-check", "check-snowball", succeeds=False)
        for path in expected:
            self.assertEqual(
                path.read_bytes(), b"// deliberately stale generated source\n"
            )
        self.build("regen-sql-grammar", "regen-snowball")
        for path, content in expected.items():
            self.assertEqual(path.read_bytes(), content)
        self.build("sql-grammar-generated-check", "check-snowball")

        # Consumers never rewrite the producer's published files.
        for path, snapshot in generated.items():
            self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns), snapshot)

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
