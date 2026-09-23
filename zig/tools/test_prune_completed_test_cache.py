# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import importlib.util
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "prune_completed_test_cache",
    Path(__file__).with_name("prune_completed_test_cache.py"),
)
assert _SPEC and _SPEC.loader
_module = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_module)
prune = _module.prune
release_completed_phase = _module.release_completed_phase


class PruneTests(unittest.TestCase):
    def test_removes_link_inputs_and_preserves_cached_executables(self):
        with tempfile.TemporaryDirectory() as root:
            cache = Path(root)
            for index, name in enumerate(
                ("test", "api-tests", "build", "libfoo.a", "test.o")
            ):
                artifact = cache / "o" / f"{index:032x}"
                artifact.mkdir(parents=True)
                output = artifact / name
                output.write_bytes(b"artifact")
                output.chmod(0o755)
                (artifact / (name + "_zcu.o")).write_bytes(b"object")
                (artifact / "keep.o").write_bytes(b"unknown")
            self.assertEqual(prune(cache, min_bytes=0), 2)
            self.assertEqual(
                {p.name for p in (cache / "o").iterdir()},
                {f"{i:032x}" for i in range(5)},
            )
            for index, name in enumerate(("test", "api-tests")):
                artifact = cache / "o" / f"{index:032x}"
                self.assertTrue((artifact / name).exists())
                self.assertFalse((artifact / (name + "_zcu.o")).exists())
                self.assertTrue((artifact / "keep.o").exists())
            self.assertEqual(prune(cache, min_bytes=0), 0)

    def test_does_not_follow_symlinks(self):
        with tempfile.TemporaryDirectory() as root:
            cache = Path(root) / "cache"
            outputs = cache / "o"
            outputs.mkdir(parents=True)
            outside = Path(root) / "outside"
            outside.mkdir()
            executable = outside / "test"
            executable.write_bytes(b"keep")
            executable.chmod(0o755)
            (outputs / ("a" * 32)).symlink_to(outside, target_is_directory=True)
            artifact = outputs / ("b" * 32)
            artifact.mkdir()
            (artifact / "test").symlink_to(executable)
            self.assertEqual(prune(cache, min_bytes=0), 0)
            self.assertEqual(executable.read_bytes(), b"keep")
            own = outputs / ("c" * 32)
            own.mkdir()
            own_test = own / "test"
            own_test.write_bytes(b"own")
            own_test.chmod(0o755)
            (own / "test_zcu.o").symlink_to(executable)
            self.assertEqual(prune(cache, min_bytes=0), 0)
            self.assertEqual(executable.read_bytes(), b"keep")

    def test_preserves_small_tests(self):
        with tempfile.TemporaryDirectory() as root:
            cache = Path(root)
            artifact = cache / "o" / ("c" * 32)
            artifact.mkdir(parents=True)
            output = artifact / "test"
            output.write_bytes(b"small")
            output.chmod(0o755)
            link_input = artifact / "test_zcu.o"
            link_input.write_bytes(b"small object")
            self.assertEqual(prune(cache), 0)
            self.assertTrue(output.exists())
            self.assertTrue(link_input.exists())

    def test_preserves_executable_without_link_inputs(self):
        # A self-hosted backend can emit only the executable.
        with tempfile.TemporaryDirectory() as root:
            cache = Path(root)
            artifact = cache / "o" / ("d" * 32)
            artifact.mkdir(parents=True)
            executable = artifact / "test"
            executable.write_bytes(b"self-hosted executable")
            executable.chmod(0o755)
            self.assertEqual(prune(cache, min_bytes=0), 0)
            self.assertEqual(executable.read_bytes(), b"self-hosted executable")

    @unittest.skipUnless(
        shutil.which("zig"), "Zig is required for the cache-hit regression"
    )
    def test_zig_cache_hit_and_source_rebuild_after_pruning(self):
        with tempfile.TemporaryDirectory() as root:
            project = Path(root).resolve()
            # The self-hosted Linux Debug backend emits no disposable object.
            # Select LLVM so this integration test actually exercises pruning.
            (project / "build.zig").write_text(
                'const std = @import("std");\n'
                "pub fn build(b: *std.Build) void {\n"
                " const t = b.addTest(.{ .root_module = b.createModule(.{\n"
                '  .root_source_file = b.path("test.zig"),\n'
                "  .target = b.graph.host, .optimize = .Debug,\n"
                " }), .use_llvm = true });\n"
                ' b.step("test", "run").dependOn(&b.addRunArtifact(t).step);\n'
                "}\n"
            )
            source = project / "test.zig"
            source.write_text(
                'test "cache prune" { try @import("std").testing.expect(true); }\n'
            )
            cache = project / "zig-local"
            command = [
                "zig",
                "build",
                "test",
                "--cache-dir",
                str(cache),
                "--global-cache-dir",
                str(project / "global"),
            ]

            def build():
                result = subprocess.run(
                    command, cwd=project, capture_output=True, text=True, check=False
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

            build()
            self.assertGreater(prune(cache, min_bytes=0), 0)
            build()  # A live compile manifest must still have its runnable output.
            source.write_text(
                'test "cache prune rebuilt" { try @import("std").testing.expectEqual(2, 1 + 1); }\n'
            )
            build()  # Recompilation must regenerate the removed link input.
            release_completed_phase(cache)
            build()  # No surviving manifest may claim a missing executable.

    def test_phase_release_removes_manifests_and_outputs_but_preserves_siblings(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root).resolve()
            cache = root / "zig-local"
            for name in (
                "zig-local/o/test",
                "zig-local/h/manifest",
                "global/dependency",
                "zig-out/antfly",
            ):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("keep outside phase")
            release_completed_phase(cache)
            self.assertEqual(list(cache.iterdir()), [])
            self.assertTrue((root / "global/dependency").exists())
            self.assertTrue((root / "zig-out/antfly").exists())
            release_completed_phase(cache)
            self.assertEqual(list(cache.iterdir()), [])

    def test_phase_release_rejects_non_private_paths_and_symlinks(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root).resolve()
            target = root / "shared"
            target.mkdir()
            (target / "keep").write_text("keep")
            link = root / "zig-local"
            link.symlink_to(target, target_is_directory=True)
            for path in (target, link, link / "zig-local"):
                with self.assertRaises(ValueError):
                    release_completed_phase(path)
            self.assertEqual((target / "keep").read_text(), "keep")

    def test_missing_cache_is_harmless(self):
        with tempfile.TemporaryDirectory() as root:
            self.assertEqual(prune(Path(root) / "missing"), 0)


if __name__ == "__main__":
    unittest.main()
