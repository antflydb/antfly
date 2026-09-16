# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import tempfile
from pathlib import Path
import unittest

import importlib.util

_SPEC = importlib.util.spec_from_file_location(
    "prune_completed_test_cache",
    Path(__file__).with_name("prune_completed_test_cache.py"),
)
assert _SPEC and _SPEC.loader
_module = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_module)
prune = _module.prune


class PruneTests(unittest.TestCase):
    def test_removes_test_link_outputs_and_preserves_reusable_artifacts(self):
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
                (artifact / "large-object.o").write_bytes(b"object")
            self.assertEqual(prune(cache, min_bytes=0), 2)
            self.assertEqual(
                {p.name for p in (cache / "o").iterdir()},
                {f"{i:032x}" for i in (2, 3, 4)},
            )
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

    def test_preserves_small_tests(self):
        with tempfile.TemporaryDirectory() as root:
            cache = Path(root)
            artifact = cache / "o" / ("c" * 32)
            artifact.mkdir(parents=True)
            output = artifact / "test"
            output.write_bytes(b"small")
            output.chmod(0o755)
            self.assertEqual(prune(cache), 0)
            self.assertTrue(output.exists())

    def test_missing_cache_is_harmless(self):
        with tempfile.TemporaryDirectory() as root:
            self.assertEqual(prune(Path(root) / "missing"), 0)


if __name__ == "__main__":
    unittest.main()
