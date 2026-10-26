#!/usr/bin/env python3
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

import importlib.util
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import types
import unittest
from unittest import mock


MODULE = Path(__file__).with_name("release_attention_matching.py")
resources = types.ModuleType("benchmark_resources")
resources.DiskSpaceGuard = lambda _path: types.SimpleNamespace(sample_mib=lambda: 50 * 1024)
resources.group_rss_mib = lambda _pid: 0
resources.memory_free_percent = lambda: 100
resources.swapout_bytes = lambda: 0
spec = importlib.util.spec_from_file_location("release_driver_io_v1", MODULE)
release = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = release
with mock.patch.dict(sys.modules, {"benchmark_resources": resources}):
    spec.loader.exec_module(release)


class Process:
    def __init__(self, stream):
        self.stdin = stream

    def poll(self):
        return None


class DriverCommandIoTests(unittest.TestCase):
    def test_nonreading_driver_pipe_obeys_deadline_and_keeps_polling_guard(self):
        read_fd, write_fd = os.pipe()
        self.addCleanup(os.close, read_fd)
        stream = os.fdopen(write_fd, "wb", buffering=0)
        self.addCleanup(stream.close)
        os.set_blocking(write_fd, False)
        while True:
            try:
                os.write(write_fd, b"x" * 4096)
            except BlockingIOError:
                break
        polls = []
        started = time.monotonic()
        with self.assertRaisesRegex(ValueError, "command write timed out"):
            release._write_driver_command(Process(stream), {"command": "next"},
                                          started + .03, lambda: polls.append(time.monotonic()))
        self.assertTrue(polls)
        self.assertLess(time.monotonic() - started, .5)

    def test_command_byte_limit_is_checked_before_pipe_write(self):
        read_fd, write_fd = os.pipe()
        self.addCleanup(os.close, read_fd)
        stream = os.fdopen(write_fd, "wb", buffering=0)
        self.addCleanup(stream.close)
        with self.assertRaisesRegex(ValueError, "command exceeds byte limit"):
            release._write_driver_command(Process(stream),
                {"command": "next", "payload": "x" * release.MAX_DRIVER_LINE_BYTES},
                time.monotonic() + 1, lambda: None)

    def test_child_exit_during_write_fails_closed(self):
        class Exited(Process):
            def poll(self):
                return 7
        read_fd, write_fd = os.pipe()
        self.addCleanup(os.close, read_fd)
        stream = os.fdopen(write_fd, "wb", buffering=0)
        self.addCleanup(stream.close)
        with self.assertRaisesRegex(ValueError, "exited while accepting"):
            release._write_driver_command(Exited(stream), {"command": "next"},
                                          time.monotonic() + 1, lambda: None)

    def test_broken_command_pipe_is_a_deliberate_failure(self):
        read_fd, write_fd = os.pipe()
        os.close(read_fd)
        stream = os.fdopen(write_fd, "wb", buffering=0)
        self.addCleanup(stream.close)
        with self.assertRaisesRegex(ValueError, "closed command pipe"):
            release._write_driver_command(Process(stream), {"command": "next"},
                                          time.monotonic() + 1, lambda: None)

    def test_collector_kills_owned_descendant_after_leader_exit_without_hanging(self):
        with __import__("tempfile").TemporaryDirectory() as temporary:
            root = Path(temporary)
            child_pid = root / "child.pid"
            code = (
                "import json,os,signal,sys,time\n"
                "for line in sys.stdin:\n"
                " c=json.loads(line)\n"
                " if c['command']=='stop': break\n"
                " pid=os.fork()\n"
                " if pid==0:\n"
                "  signal.signal(signal.SIGTERM,signal.SIG_IGN)\n"
                f"  open({str(child_pid)!r},'w').write(str(os.getpid()))\n"
                "  time.sleep(60)\n"
                "  os._exit(0)\n"
                " time.sleep(.05)\n"
                " os._exit(7)\n")
            identity = {key: hashlib.sha256(key.encode()).hexdigest()
                        for key in release.SEMANTIC_BINDINGS}
            identity.update(dtype="f16_kv_f32_bias", kernel_route="metal_context_optimized")
            origin = {key: hashlib.sha256(("origin-" + key).encode()).hexdigest()
                      for key in release.PROVENANCE_BINDINGS}
            origin["binary_sha256"] = release.sha(Path(sys.executable).resolve())
            started = time.monotonic()
            try:
                with self.assertRaisesRegex(ValueError, "failed before reporting|exited"):
                    release.collect_soak_v2([sys.executable, "-c", code], identity, origin,
                                            root / "run", duration=.01, timeout=.5,
                                            max_rss_mib=256)
                self.assertLess(time.monotonic() - started, 3)
                receipt = release.read_json(root / "run" / "session.json")
                self.assertFalse(receipt["completed"])
                self.assertIsNotNone(receipt["returncode"])
                if child_pid.exists():
                    pid = int(child_pid.read_text())
                    deadline = time.monotonic() + 1
                    while True:
                        try:
                            os.kill(pid, 0)
                        except ProcessLookupError:
                            break
                        if time.monotonic() >= deadline:
                            self.fail("owned driver descendant survived cleanup")
                        time.sleep(.01)
            finally:
                if child_pid.exists():
                    try:
                        os.kill(int(child_pid.read_text()), 9)
                    except ProcessLookupError:
                        pass


if __name__ == "__main__":
    unittest.main()
