"""Real bounded tiny-process tests. No Torch, checkpoints, or models."""
import errno
import importlib.util
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
import unittest
from unittest import mock

import supervision
import validate


CHILD = "import os,signal,threading,time; threading.Timer(6,lambda:os._exit(92)).start(); signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(6)"


class SupervisionTests(unittest.TestCase):
    def run_program(self, source, **options):
        with tempfile.TemporaryDirectory() as folder:
            stdout = Path(folder) / "stdout"
            stderr = Path(folder) / "stderr"
            result = supervision.run([sys.executable, "-c", source], stdout, stderr,
                                     timeout_seconds=options.pop("timeout_seconds", 3),
                                     grace_seconds=0.15, kill_seconds=2,
                                     worker_grace_seconds=0.1, tick=0.01, **options)
            result["stdout"] = stdout.read_bytes() if stdout.exists() else b""
            result["stderr"] = stderr.read_bytes() if stderr.exists() else b""
            return result

    def assert_clean(self, result):
        self.assertTrue(result["cleanup"]["complete"], result)
        self.assertTrue(result["cleanup"]["direct_child_reaped"], result)
        self.assertTrue(result["cleanup"]["known_children_gone"], result)
        psutil = supervision.pinned_psutil()
        for entry in result["tracked_processes"]:
            try:
                current = psutil.Process(entry["pid"])
                self.assertNotEqual(current.create_time(), entry["create_time"], result)
            except psutil.NoSuchProcess:
                pass

    def test_pinned_runtime_no_torch_and_success_receipt(self):
        self.assertNotIn("torch", sys.modules)
        self.assertEqual(supervision.pinned_psutil().__version__, "7.1.3")
        result = self.run_program("import time; print('complete',flush=True); time.sleep(.08)")
        self.assertIsNone(result["failure"], result)
        self.assertEqual(result["stdout"], b"complete\n")
        self.assertGreater(result["peak_child_tree_rss_bytes"], 0)
        self.assertGreater(result["rss_samples"], 0)
        self.assert_clean(result)
        self.assertNotIn("torch", sys.modules)

    def test_fifo_open_rejection_is_itself_outer_bounded(self):
        with tempfile.TemporaryDirectory() as folder:
            fifo = Path(folder) / "input.fifo"
            os.mkfifo(fifo)
            source = ("import sys;sys.path.insert(0," + repr(str(validate.ROOT)) + ");import validate\n"
                      "try:\n validate.read(" + repr(str(fifo)) + ")\n"
                      "except ValueError as e:\n print(str(e),flush=True)\n"
                      "else:\n raise SystemExit('FIFO unexpectedly read')\n")
            result = self.run_program(source, timeout_seconds=2)
            self.assertIsNone(result["failure"], result)
            self.assertIn(b"not a regular file", result["stdout"])
            self.assert_clean(result)

    def test_fstat_failure_closes_the_real_descriptor(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "input"
            path.write_bytes(b"x")
            opened = []
            original = os.open
            def record(*args, **kwargs):
                result = original(*args, **kwargs)
                opened.append(result)
                return result
            with mock.patch.object(validate.os, "open", side_effect=record), mock.patch.object(validate.os, "fstat", side_effect=OSError(errno.EIO, "injected fstat failure")):
                with self.assertRaisesRegex(OSError, "injected fstat"):
                    validate.regular(path)
            self.assertEqual(len(opened), 1)
            with self.assertRaises(OSError) as failure:
                os.fstat(opened[0])
            self.assertEqual(failure.exception.errno, errno.EBADF)

    def test_repeated_cleanup_diagnostics_remain_bounded(self):
        tree = supervision.ProcessTree(None, None)
        for index in range(10000):
            tree.record_error(str(index) + "x" * 2000)
        self.assertEqual(tree.inspection_error_count, 10000)
        self.assertEqual(len(tree.inspection_errors), 64)
        self.assertTrue(all(len(item) <= 1024 for item in tree.inspection_errors))

    def test_registration_constructor_failure_still_reaps_child(self):
        def fail(_parent, _psutil):
            raise RuntimeError("injected tracker construction failure")
        result = self.run_program("import time;time.sleep(5)", tree_factory=fail)
        self.assertEqual(result["failure_phase"], "registration")
        self.assertIn("injected tracker", result["failure"])
        self.assert_clean(result)

    def test_hook_failure_after_descendant_registration_cleans_known_identity(self):
        source = ("import os,signal,subprocess,sys,threading,time\n"
                  "threading.Timer(7,lambda:os._exit(93)).start()\n"
                  "child=subprocess.Popen([sys.executable,'-c'," + repr(CHILD) + "])\n"
                  "def stop(*_):\n child.kill();child.wait();os._exit(0)\n"
                  "signal.signal(signal.SIGTERM,stop)\n"
                  "print(child.pid,flush=True)\n"
                  "time.sleep(7)\n")
        def fail_after_child(_parent, tree):
            deadline = time.monotonic() + 2
            while not any(item["relation"] == "observed_descendant" for item in tree.known.values()):
                if time.monotonic() >= deadline:
                    raise RuntimeError("test child not discovered")
                tree.sample()
                time.sleep(0.01)
            raise RuntimeError("injected registration callback failure")
        result = self.run_program(source, on_registered=fail_after_child)
        self.assertIn("injected registration callback", result["failure"])
        self.assertTrue(any(item["relation"] == "observed_descendant" for item in result["tracked_processes"]))
        self.assert_clean(result)

    def test_real_rss_cap_and_output_cap_fail_without_leaks(self):
        result = self.run_program("import time;data=bytearray(32*1024*1024);print('allocated',flush=True);time.sleep(5)", rss_limit_bytes=24 * 1024**2)
        self.assertIn("MemoryError", result["failure"])
        self.assertGreater(result["peak_child_tree_rss_bytes"], result["max_child_tree_rss_bytes"])
        self.assert_clean(result)
        result = self.run_program("print('x'*8192,flush=True)", output_limit_bytes=1024)
        self.assertIn("output limit", result["failure"])
        self.assertLessEqual(len(result["stdout"]), 1024)
        self.assert_clean(result)

    def test_timeout_hardkill_and_known_survivor_cleanup(self):
        source = ("import os,signal,subprocess,sys,threading,time\n"
                  "threading.Timer(7,lambda:os._exit(94)).start()\n"
                  "signal.signal(signal.SIGTERM,signal.SIG_IGN)\n"
                  "child=subprocess.Popen([sys.executable,'-c'," + repr(CHILD) + "])\n"
                  "print(child.pid,flush=True)\n"
                  "time.sleep(7)\n")
        result = self.run_program(source, timeout_seconds=0.4)
        self.assertIn("TimeoutError", result["failure"])
        self.assertEqual(result["returncode"], -signal.SIGKILL)
        self.assertTrue(any(item["relation"] == "observed_descendant" for item in result["tracked_processes"]))
        self.assertTrue(result["cleanup_signals"], result)
        self.assert_clean(result)


if __name__ == "__main__":
    unittest.main()
