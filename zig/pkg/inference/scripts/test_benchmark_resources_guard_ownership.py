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

import os
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest import mock

REPO = Path(__file__).resolve().parents[4]
SCRIPTS = REPO / "zig/pkg/inference/scripts"
GEMMA = SCRIPTS / "gemma4"
sys.path[:0] = [str(SCRIPTS), str(GEMMA)]


import benchmark_resources as guard
import quality_corpus_pack_advance_v8_tool_history as packer


class GuardOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.free = mock.patch.object(guard, "memory_free_percent", return_value=80)
        self.swap = mock.patch.object(guard, "swapout_bytes", return_value=0)
        self.free.start(); self.swap.start()

    def tearDown(self):
        self.swap.stop(); self.free.stop()

    def helper(self, root, body):
        path = Path(root) / "helper.py"
        header = textwrap.dedent(f"""
            import importlib.util, os, pathlib, subprocess, sys, time
            sys.path[:0]=[{str(SCRIPTS)!r},{str(GEMMA)!r}]
            import quality_corpus_pack_advance_v8_tool_history as packer
        """)
        path.write_text(header + textwrap.dedent(body))
        return path

    def run_guarded(self, command, output, **kwargs):
        return guard.run_guarded(command, output, timeout=5, max_rss_mib=512,
                                 min_free_percent=1, max_swap_growth_mib=0,
                                 min_disk_free_mib=0, interval=0.02, **kwargs)

    def test_zombies_are_not_live_address_space(self):
        self.assertEqual(guard.group_rss_mib(7, "7 1024 S\n7 8192 Z+\n9 4 S\n"), 1)
        self.assertEqual(guard.group_live_processes(7, "7 S\n7 Z+\n9 S\n"), 1)

    def test_real_guard_counts_parent_driver_and_worker(self):
        with tempfile.TemporaryDirectory() as root:
            marker = Path(root) / "accepted"
            members = Path(root) / "members.json"
            worker = "import time; hold=bytearray(8*1024*1024); time.sleep(.5)"
            driver = ("import json,os,pathlib,subprocess,sys,time; hold=bytearray(8*1024*1024); "
                      f"p=subprocess.Popen([sys.executable,'-c',{worker!r}]); "
                      f"pathlib.Path({str(members)!r}).write_text(json.dumps([[os.getppid(),os.getpgid(os.getppid())],[os.getpid(),os.getpgrp()],[p.pid,os.getpgid(p.pid)]])); "
                      "time.sleep(.5); p.wait()")
            helper = self.helper(root, f"""
                packer._consume_outer_guard_witness()
                pathlib.Path({str(marker)!r}).write_text('accepted')
                hold=bytearray(8*1024*1024)
                child=subprocess.Popen([sys.executable,'-c',{driver!r}], start_new_session=False)
                packer._require_driver_in_guard_group(child)
                raise SystemExit(child.wait())
            """)
            report = self.run_guarded([sys.executable, str(helper)], Path(root) / "resource.json")
            self.assertTrue(report["pass"], report)
            self.assertTrue(marker.is_file())
            rows = json.loads(members.read_text())
            self.assertEqual(len({row[0] for row in rows}), 3)
            self.assertEqual({row[1] for row in rows}, {report["pid"]})
            self.assertGreaterEqual(report["peak_group_live_processes"], 3)
            self.assertGreater(report["peak_group_rss_mib"], 24)
            self.assertEqual(report["remaining_group_rss_mib"], 0)
            self.assertEqual(report["remaining_group_live_processes"], 0)

    def test_guard_issues_parent_bound_one_use_witness(self):
        with tempfile.TemporaryDirectory() as root:
            marker = Path(root) / "accepted"
            helper = self.helper(root, f"""
                packer._consume_outer_guard_witness()
                pathlib.Path({str(marker)!r}).write_text('accepted')
            """)
            with mock.patch.object(guard, "group_rss_mib", return_value=0), \
                 mock.patch.object(guard, "group_live_processes", return_value=0):
                report = self.run_guarded([sys.executable, str(helper)],
                                          Path(root) / "resource.json")
            self.assertTrue(report["pass"], report)
            self.assertTrue(marker.is_file())
            self.assertEqual(report["remaining_group_rss_mib"], 0)
            self.assertEqual(report["remaining_group_live_processes"], 0)

    def test_standalone_session_leader_is_rejected_before_request(self):
        with tempfile.TemporaryDirectory() as root:
            marker = Path(root) / "request"
            helper = self.helper(root, f"""
                packer._consume_outer_guard_witness()
                pathlib.Path({str(marker)!r}).write_text('sent')
            """)
            env = os.environ.copy()
            for name in (guard.GUARD_FD_ENV, guard.GUARD_TOKEN_ENV,
                         guard.GUARD_OWNER_PID_ENV):
                env.pop(name, None)
            child = subprocess.run([sys.executable, str(helper)], env=env,
                                   start_new_session=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, timeout=5)
            self.assertNotEqual(child.returncode, 0)
            self.assertFalse(marker.exists())

    def test_escaped_driver_is_rejected_before_request(self):
        with tempfile.TemporaryDirectory() as root:
            marker = Path(root) / "request"
            helper = self.helper(root, f"""
                packer._consume_outer_guard_witness()
                child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(.3)'],
                                       start_new_session=True)
                try:
                    packer._require_driver_in_guard_group(child)
                    pathlib.Path({str(marker)!r}).write_text('sent')
                finally:
                    child.terminate(); child.wait(timeout=2)
            """)
            report = self.run_guarded([sys.executable, str(helper)], Path(root) / "resource.json")
            self.assertFalse(report["pass"])
            self.assertFalse(marker.exists())
            self.assertEqual(report["remaining_group_rss_mib"], 0)
            self.assertEqual(report["remaining_group_live_processes"], 0)

    def test_surviving_same_group_child_is_sampled_and_drained(self):
        with tempfile.TemporaryDirectory() as root:
            pid_path = Path(root) / "worker.pid"
            worker = ("import os,pathlib,time; hold=bytearray(8*1024*1024); "
                      f"pathlib.Path({str(pid_path)!r}).write_text(str(os.getpid())); time.sleep(30)")
            helper = self.helper(root, f"""
                packer._consume_outer_guard_witness()
                subprocess.Popen([sys.executable,'-c',{worker!r}], start_new_session=False)
                deadline=time.monotonic()+2
                while not pathlib.Path({str(pid_path)!r}).exists():
                    if time.monotonic()>deadline: raise RuntimeError('worker did not start')
                    time.sleep(.01)
            """)
            report = self.run_guarded([sys.executable, str(helper)], Path(root) / "resource.json")
            self.assertTrue(report["pass"], report)
            self.assertGreater(report["post_exit_group_rss_mib"], 0)
            self.assertGreater(report["post_exit_group_live_processes"], 0)
            self.assertEqual(report["remaining_group_rss_mib"], 0)
            self.assertEqual(report["remaining_group_live_processes"], 0)
            pid = int(pid_path.read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)

    def test_direct_cleanup_never_signals_own_group(self):
        class Child:
            def __init__(self): self.done = False
            def poll(self): return 0 if self.done else None
            def terminate(self): self.done = True
            def kill(self): self.done = True
            def wait(self, timeout=None): return 0
        with mock.patch.object(os, "killpg") as kill_group:
            packer._terminate_direct_child(Child())
        kill_group.assert_not_called()

    def test_owner_mismatch_closes_parsed_witness_fd(self):
        read_fd, write_fd = os.pipe()
        token = os.urandom(packer.GUARD_WITNESS_BYTES)
        os.write(write_fd, token); os.close(write_fd)
        values = {packer.GUARD_FD_ENV: str(read_fd),
                  packer.GUARD_TOKEN_ENV: token.hex(),
                  packer.GUARD_OWNER_PID_ENV: str(os.getppid() + 100000)}
        with mock.patch.dict(os.environ, values, clear=False):
            with self.assertRaisesRegex(ValueError, "ownership"):
                packer._consume_outer_guard_witness()
        with self.assertRaises(OSError):
            os.fstat(read_fd)

    def test_missing_witness_is_journaled_before_model_spawn(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root = base / "campaign"; sources = base / "sources"
            sources.mkdir()
            work = {"model": "gemma4-e2b", "source_rows": {},
                    "cells": [{"context_tokens": 8192, "bucket": "code", "cases": [{}]}]}
            authority = {"schema_version": 2, "model": "gemma4-e2b", "session_id": "1"*64,
                "loaded_identity": None, "expected_artifact_sha256": "2"*64,
                "expected_implementation_sha256": "3"*64,
                "preparation_context_tokens": 16384, "reserved_output_tokens": 32,
                "settings_sha256": "4"*64, "variant_settings_sha256": "5"*64,
                "source_root": str(sources)}
            old_cases, old_max = packer.pack.CASES_PER_CELL, packer.MAX_CELL_CASES
            packer.pack.CASES_PER_CELL = packer.MAX_CELL_CASES = 1
            try:
                with self.assertRaisesRegex(ValueError, "lacks outer resource-guard"):
                    packer.run_cell(work, authority, root, 8192, "code", [sys.executable],
                                    root/"journal.jsonl", root/"result.json", 1, 2)
            finally:
                packer.pack.CASES_PER_CELL, packer.MAX_CELL_CASES = old_cases, old_max
            rows = [json.loads(line) for line in (root/"journal.jsonl").read_text().splitlines()]
            self.assertEqual(rows[-1]["stage"], "quality_loaded_tokenizer_pack_failure")
            self.assertFalse(rows[-1]["completed"])
            self.assertFalse((root/"result.json").exists())


if __name__ == "__main__":
    unittest.main()
