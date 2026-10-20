"""Real-process cleanup and context-manager error propagation, without a GPU."""
import importlib.util
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SCRIPTS = Path.cwd() / 'zig/pkg/inference/scripts/gemma4'
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location('staged_host', Path(__file__).with_name('native_checkpoint_unified_host.py'))
host = importlib.util.module_from_spec(spec)
spec.loader.exec_module(host)

GRACEFUL = "import signal,time,sys; signal.signal(signal.SIGTERM,lambda *_:sys.exit(0)); print('READY',flush=True); time.sleep(30)"


class BrokenSampler:
    def __init__(self, *args): pass
    def start(self): pass
    def finish(self): raise RuntimeError('injected sampler failure')


class CleanupTests(unittest.TestCase):
    @staticmethod
    def fallback(process):
        try: os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError: pass
        process.wait(timeout=3)
        if process.stdout: process.stdout.close()
        if process.stderr: process.stderr.close()

    def start(self, code):
        process = subprocess.Popen([sys.executable, '-u', '-c', code], start_new_session=True,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.addCleanup(self.fallback, process)
        self.assertEqual(process.stdout.readline().strip(), 'READY')
        return process

    def finish(self, process, sampler=None):
        record = {}
        errors = host.finish_standalone(record, process, sampler, graceful_timeout=.2, kill_timeout=.3)
        self.assertTrue(record['stopped'])
        self.assertFalse(host.active_group_members(process.pid))
        return record, errors

    def test_clean_listener_shutdown(self):
        record, errors = self.finish(self.start(GRACEFUL))
        self.assertEqual(errors, [])
        self.assertFalse(record['forced_kill'])
        self.assertEqual(record['returncode'], 0)

    def test_sampling_failure_still_retires_listener(self):
        record, errors = self.finish(self.start(GRACEFUL), BrokenSampler())
        self.assertIn('injected sampler failure', ' '.join(errors))
        self.assertFalse(record['forced_kill'])

    def test_stubborn_listener_requires_forced_failure(self):
        code = "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('READY',flush=True); time.sleep(30)"
        record, errors = self.finish(self.start(code))
        self.assertTrue(record['forced_kill'])
        self.assertIn('listener required forced shutdown', ' '.join(errors))
        self.assertEqual(record['returncode'], -signal.SIGKILL)

    def test_descendant_is_killed_and_retirement_verified(self):
        child = "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('CHILD',flush=True); time.sleep(30)"
        code = ("import subprocess,sys,signal,time\n"
                f"child=subprocess.Popen([sys.executable,'-u','-c',{child!r}],stdout=subprocess.PIPE,text=True)\n"
                "assert child.stdout.readline().strip()=='CHILD'\n"
                "signal.signal(signal.SIGTERM,lambda *_:sys.exit(0))\n"
                "print('READY',flush=True);time.sleep(30)\n")
        record, errors = self.finish(self.start(code))
        self.assertTrue(record['forced_kill'])
        self.assertIn('descendants required forced shutdown', ' '.join(errors))

    def context_failure(self, primary):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as directory:
            args = SimpleNamespace(output=Path(directory)/'result.json', port=1, server=Path(sys.executable), generation_timing=True)
            evidence = {}
            def ready(process, port):
                self.addCleanup(self.fallback, process)
                deadline = time.monotonic()+3
                while 'READY' not in args.output.with_suffix('.worker.log').read_text():
                    if process.poll() is not None or time.monotonic()>=deadline: raise RuntimeError('test child not ready')
                    time.sleep(.01)
            with patch.object(host, 'standalone_command', return_value=[sys.executable,'-u','-c',GRACEFUL]), \
                 patch.object(host, 'wait_ready', side_effect=ready), \
                 patch.object(host, 'discover_unified_worker', side_effect=lambda pid, binary: pid), \
                 patch.object(host, 'UnifiedSampler', BrokenSampler):
                with self.assertRaisesRegex(RuntimeError, 'primary request failure' if primary else 'injected sampler failure'):
                    with host.unified_server(args, 'worker', evidence, SCRIPTS):
                        if primary: raise RuntimeError('primary request failure')
            self.assertTrue(evidence['worker']['stopped'])
            self.assertEqual(evidence['worker']['environment_overrides']['TERMITE_SERVER_GENERATE_TIMING'], '1')
            self.assertIn('injected sampler failure', ' '.join(evidence['worker']['cleanup_errors']))

    def test_context_preserves_primary_failure(self): self.context_failure(True)
    def test_context_rejects_success_when_sampling_cleanup_fails(self): self.context_failure(False)


if __name__ == '__main__': unittest.main()
