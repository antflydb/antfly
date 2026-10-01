import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "retain_test_process", Path(__file__).with_name("retain_test_process.py")
)
retention = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(retention)


class RetainTestProcess(unittest.TestCase):
    def test_preserves_named_and_anonymous_tests_with_exact_arguments(self):
        for name in ("test", "metadata-unit-lane-a-tests"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                executable = root / name
                executable.write_bytes(b"retained executable")
                process = root / "proc/17"
                process.mkdir(parents=True)
                (process / "exe").symlink_to(executable)
                (process / "cwd").symlink_to(root)
                argv = [str(executable), "--test-filter", "test with spaces"]
                (process / "cmdline").write_bytes("\0".join(argv).encode() + b"\0")
                self.assertTrue(retention.retain(17, root / "out", root / "proc"))
                output = root / "out/test-17"
                self.assertEqual(output.read_bytes(), executable.read_bytes())
                self.assertEqual(output.stat().st_mode & 0o777, 0o755)
                record = json.loads(output.with_suffix(".json").read_text())
                self.assertEqual(record["argv"], argv)
                self.assertEqual(record["cwd"], str(root))
                self.assertEqual(
                    record["sha256"], hashlib.sha256(output.read_bytes()).hexdigest()
                )

    def test_does_not_copy_compilers_or_build_runners(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            process = root / "proc/17"
            process.mkdir(parents=True)
            (process / "exe").symlink_to(root / "zig")
            self.assertFalse(retention.retain(17, root / "out", root / "proc"))
            self.assertFalse((root / "out").exists())


if __name__ == "__main__":
    unittest.main()
