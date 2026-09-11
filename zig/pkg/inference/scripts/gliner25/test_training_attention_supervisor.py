"""Pure ownership/admission tests; no subprocess or numerical runtime executes."""
import os
from pathlib import Path
import signal
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import supervise_training_attention as capture


class TrainingAttentionSupervisorTest(unittest.TestCase):
    def test_exact_source_pin_rejects_same_size_substitution(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "source.py"
            path.write_bytes(b"original")
            expected = capture.pin(path.read_bytes())
            self.assertEqual(b"original", capture.require_pin(path, expected))
            path.write_bytes(b"modified")
            with self.assertRaisesRegex(ValueError, "frozen attention input differs"):
                capture.require_pin(path, expected)

    def test_descriptor_read_rejects_symlink_fifo_and_size_overflow(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "source"
            path.write_bytes(b"12345")
            with self.assertRaisesRegex(ValueError, "bounded regular"):
                capture.read(path, 4)
            link = root / "link"
            link.symlink_to(path)
            with self.assertRaises(OSError):
                capture.read(link)
            fifo = root / "fifo"
            os.mkfifo(fifo)
            with self.assertRaisesRegex(ValueError, "bounded regular"):
                capture.read(fifo)

    def test_artifact_guard_sums_files_and_rejects_unknown_entry(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "capture.json").write_bytes(b"abc")
            (root / "weights.safetensors").write_bytes(b"1234")
            self.assertEqual({"capture.json": 3, "weights.safetensors": 4}, capture.artifact_sizes(root, 7))
            with self.assertRaisesRegex(ValueError, "byte ceiling"):
                capture.artifact_sizes(root, 6)
            (root / "unexpected").write_bytes(b"")
            with self.assertRaisesRegex(ValueError, "unexpected attention artifact"):
                capture.artifact_sizes(root, 100)

    def test_artifact_guard_rejects_nonregular_root_or_member(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = parent / "output"
            self.assertEqual({}, capture.artifact_sizes(root))
            root.mkdir()
            member = root / "tensors.safetensors"
            member.mkdir()
            with self.assertRaises((OSError, ValueError)):
                capture.artifact_sizes(root)
            member.rmdir()
            os.mkfifo(member)
            with self.assertRaisesRegex(ValueError, "nonregular attention artifact"):
                capture.artifact_sizes(root)
            member.unlink()
            member.symlink_to(parent / "absent")
            with self.assertRaises(OSError):
                capture.artifact_sizes(root)
            link = parent / "link"
            link.symlink_to(root)
            with self.assertRaises(OSError):
                capture.artifact_sizes(link)

    def test_guard_failure_cannot_block_process_only_cleanup(self):
        class ProcessTree:
            def __init__(self, *_):
                self.samples = 0

            def sample(self):
                self.samples += 1
                return 5

            def cleanup_sample(self):
                self.sample()

            def receipt(self):
                return {"samples": self.samples}

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "bad-entry").write_bytes(b"")
            tree = capture.tree_factory(SimpleNamespace(ProcessTree=ProcessTree), root)(None, None)
            with self.assertRaisesRegex(ValueError, "unexpected attention artifact"):
                tree.sample()
            tree.cleanup_sample()
            self.assertFalse(tree.check_artifacts)
            self.assertEqual(2, tree.receipt()["samples"])

    def test_fresh_owner_does_not_accept_existing_or_dangling_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            path = root / "fresh"
            capture.fresh_path(path)
            path.symlink_to(root / "absent")
            with self.assertRaisesRegex(ValueError, "refusing to overwrite"):
                capture.fresh_path(path)
            with self.assertRaisesRegex(ValueError, "absolute"):
                capture.fresh_path(Path("relative"))

    def test_first_cooperative_signal_protects_cleanup_from_followup_signals(self):
        with mock.patch.object(capture.signal, "signal") as replace:
            with self.assertRaises(KeyboardInterrupt):
                capture.stop_signal(signal.SIGINT, None)
        self.assertEqual([mock.call(signal.SIGINT, signal.SIG_IGN), mock.call(signal.SIGTERM, signal.SIG_IGN)], replace.call_args_list)


if __name__ == "__main__":
    unittest.main()
