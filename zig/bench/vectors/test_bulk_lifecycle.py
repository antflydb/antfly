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

"""Failure-path qualification regressions, with no cloud or corpus dependency."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import ExitStack
from unittest import mock

import gcs_bulk_local as controller
import gcs_bulk_worker as worker
from bench_bulk_pipeline import (
    CancellationScope,
    OwnedProcess,
    gcs_probe_environment,
    sha256_stream,
)


class ControllerFailures(unittest.TestCase):
    def case(
        self,
        *,
        post_error=None,
        spawn_error=False,
        source_changed=False,
        worker_error=False,
    ):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            root = Path(directory)
            output = root / "output"
            inputs = output / "inputs"
            recall = inputs / "recall"
            recall.mkdir(parents=True)
            (recall / "prepare.json").write_text(json.dumps(dict(vectors=100000)))
            source = root / "train.parquet"
            source.write_bytes(b"x")
            work = output / "case"
            work.mkdir()
            (work / "result.json").write_text(json.dumps(dict(qualified=True)))
            remote = dict(
                size=1,
                custom_fields=dict(sha256="fixture"),
                generation="1",
                etag="e",
                name="source/train.parquet",
            )
            after = (
                RuntimeError("metadata failure")
                if post_error
                else dict(remote, generation="2" if source_changed else "1")
            )

            class Owner:
                process = None
                returncode = None

                def __enter__(self):
                    return self

                def __exit__(self, *_):
                    pass

                def start(self, *_args, **_kwargs):
                    if spawn_error:
                        raise OSError("fixture spawn failure")
                    self.process = object()
                    self.returncode = 0

                def wait(self, **_kwargs):
                    self.returncode = 1 if worker_error else 0
                    return self.returncode

            def command(argv, **kw):
                return subprocess.CompletedProcess(
                    argv, 0, stdout="baseline" if kw.get("text") else b""
                )

            argv = [
                "review",
                "--binary",
                str(inputs / "probe"),
                "--source",
                str(source),
                "--source-prefix",
                "source",
                "--recall",
                str(recall),
                "--output",
                str(output),
                "--timeout",
                "60",
                "--frozen-inputs",
            ]
            patches = [
                mock.patch.object(sys, "argv", argv),
                mock.patch.object(
                    controller, "__file__", str(inputs / "gcs_bulk_local.py")
                ),
                mock.patch.object(controller, "verify_inputs", return_value={}),
                mock.patch.object(
                    controller,
                    "validate_source",
                    return_value=dict(bytes=1, sha256="fixture", fixture=None),
                ),
                mock.patch.object(controller, "validate_truth"),
                mock.patch.object(controller, "digest", return_value="0" * 64),
                mock.patch.object(controller, "command", side_effect=command),
                mock.patch.object(controller, "metadata", side_effect=[remote, after]),
                mock.patch.object(controller, "OwnedProcess", Owner),
                mock.patch.object(controller, "gcs_probe_environment", return_value={}),
            ]
            for patch in patches:
                stack.enter_context(patch)
            if post_error or source_changed or spawn_error or worker_error:
                with self.assertRaises((RuntimeError, ValueError, OSError)):
                    controller.main()
            else:
                controller.main()
            return json.loads((output / "local.json").read_text())

    def test_post_build_metadata_failure_is_terminal(self):
        result = self.case(post_error=True)
        self.assertEqual(result["status"], "failed")
        self.assertFalse(result["qualified"])
        self.assertEqual(result["phase"], "revalidate")
        self.assertEqual(result["worker_exit_code"], 0)
        self.assertEqual(result["controller_error_type"], "RuntimeError")

    def test_changed_source_is_terminal(self):
        result = self.case(source_changed=True)
        self.assertEqual(result["status"], "failed")
        self.assertFalse(result["qualified"])
        self.assertEqual(result["controller_error_type"], "ValueError")

    def test_spawn_failure_does_not_leave_running_status(self):
        result = self.case(spawn_error=True)
        self.assertEqual(result["status"], "failed")
        self.assertFalse(result["worker_started"])
        self.assertIsNone(result["worker_exit_code"])
        self.assertEqual(result["controller_error_type"], "OSError")

    def test_worker_failure_is_not_masked_by_post_validation(self):
        result = self.case(worker_error=True, post_error=True)
        self.assertFalse(result["qualified"])
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["phase"], "worker")
        self.assertEqual(result["worker_exit_code"], 1)

    def test_success_requires_all_post_checks(self):
        result = self.case()
        self.assertTrue(result["qualified"])
        self.assertEqual(result["status"], "qualified")


class EnvironmentAdmission(unittest.TestCase):
    def test_adc_rejects_unattested_bearer_and_endpoint_overrides(self):
        for key in (
            "GCS_BEARER_TOKEN",
            "GOOGLE_OAUTH_ACCESS_TOKEN",
            "GCS_JSON_API_ENDPOINT",
            "GCS_JSON_API_UPLOAD_ENDPOINT",
        ):
            with (
                self.subTest(key=key),
                mock.patch.dict(os.environ, {key: "secret-must-not-appear"}),
            ):
                with self.assertRaises(ValueError) as error:
                    gcs_probe_environment("adc")
                self.assertIn(key, str(error.exception))
                self.assertNotIn("secret-must-not-appear", str(error.exception))

    def test_adc_preserves_explicit_adc_file_and_proxies(self):
        with mock.patch.dict(
            os.environ,
            {
                "GOOGLE_APPLICATION_CREDENTIALS": "fixture.json",
                "HTTPS_PROXY": "fixture-proxy",
            },
            clear=True,
        ):
            env = gcs_probe_environment("adc")
            self.assertEqual(env["GOOGLE_APPLICATION_CREDENTIALS"], "fixture.json")
            self.assertEqual(env["HTTPS_PROXY"], "fixture-proxy")


class FailureSampling(unittest.TestCase):
    def test_failed_validation_retains_samples_and_stops_thread(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = dict(qualified=False, attempts=[dict(exit_code=1)])
            sampler = worker.DiskSampler(root, report)
            with self.assertRaises(RuntimeError):
                with sampler:
                    (root / "payload").write_bytes(bytes(4096))
                    sampler.sample()
                    raise RuntimeError("native failure")
            result = json.loads((root / "result.json").read_text())
            self.assertFalse(result["qualified"])
            self.assertEqual(result["status"], "failed")
            self.assertGreaterEqual(
                result["scratch_sample"]["case_nominal_bytes"], 4096
            )
            self.assertGreater(
                result["scratch_sample"]["host_filesystem_min_free_bytes"], 0
            )
            self.assertTrue(result["scratch_sample"]["sampler_stopped"])
            self.assertTrue((root / "disk-samples.json").exists())

    def test_cancellation_persists_samples(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            CancellationScope() as cancellation,
        ):
            root = Path(directory)
            report = dict(qualified=False)
            with self.assertRaises(SystemExit):
                with worker.DiskSampler(root, report):
                    cancellation.request(signal.SIGTERM)
                    cancellation.check()
            result = json.loads((root / "result.json").read_text())
            self.assertEqual(result["status"], "cancelled")
            self.assertTrue(result["scratch_sample"]["sampler_stopped"])


class ProcessCancellation(unittest.TestCase):
    def test_repeated_signals_do_not_interrupt_cleanup(self):
        sender = None
        owner = OwnedProcess()
        try:
            with self.assertRaises(SystemExit) as error:
                with CancellationScope(), owner:
                    owner.start(
                        [
                            sys.executable,
                            "-c",
                            "import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);print('ready',flush=True);time.sleep(30)",
                        ],
                        stdout=subprocess.PIPE,
                    )
                    self.assertEqual(owner.stdout.readline(), b"ready\n")
                    sender = threading.Thread(
                        target=lambda: (
                            time.sleep(0.05),
                            os.kill(os.getpid(), signal.SIGHUP),
                        )
                    )
                    sender.start()
                    os.kill(os.getpid(), signal.SIGTERM)
                    CancellationScope.current.check()
            self.assertEqual(error.exception.code, 128 + signal.SIGTERM)
            self.assertIsNotNone(owner.returncode)
        finally:
            if sender is not None:
                sender.join(timeout=1)

    def test_large_source_hash_checks_cancellation_between_chunks(self):
        class Stream:
            reads = 0

            def readinto(self, buffer):
                self.reads += 1
                buffer[:7] = b"payload"
                CancellationScope.current.request(signal.SIGTERM)
                return 7

        stream = Stream()
        with CancellationScope(), self.assertRaises(SystemExit):
            sha256_stream(stream)
        self.assertEqual(stream.reads, 1)


class SamplingAdmission(unittest.TestCase):
    def test_sampling_failure_before_native_launch_is_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = dict(qualified=False)
            with mock.patch.object(
                worker.shutil, "disk_usage", side_effect=OSError("fixture")
            ):
                with self.assertRaises(OSError):
                    with worker.DiskSampler(root, report):
                        self.fail("sampling failure must stop admission")
            result = json.loads((root / "result.json").read_text())
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["error_type"], "OSError")
            self.assertIsNone(
                result["scratch_sample"]["host_filesystem_min_free_bytes"]
            )
            self.assertTrue(result["scratch_sample"]["sampler_stopped"])

    def test_pending_cancellation_cannot_commit_qualified_sample(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            CancellationScope() as cancellation,
        ):
            root = Path(directory)
            report = dict(qualified=True)
            with self.assertRaises(SystemExit):
                with worker.DiskSampler(root, report):
                    cancellation.request(signal.SIGTERM)
            result = json.loads((root / "result.json").read_text())
            self.assertFalse(result["qualified"])
            self.assertEqual(result["status"], "cancelled")


class ProcessDeadlines(unittest.TestCase):
    def test_wait_deadline_drains_started_child(self):
        owner = OwnedProcess()
        with self.assertRaises(subprocess.TimeoutExpired):
            with owner:
                owner.start([sys.executable, "-c", "import time;time.sleep(30)"])
                owner.wait(timeout=0.05)
        self.assertIsNotNone(owner.returncode)

    def test_capture_cancellation_closes_pipes_and_reaps_child(self):
        owner = OwnedProcess()
        stop = threading.Event()
        with CancellationScope():
            sender = threading.Thread(
                target=lambda: (
                    None if stop.wait(0.1) else os.kill(os.getpid(), signal.SIGTERM)
                )
            )
            try:
                with self.assertRaises(SystemExit):
                    with owner:
                        owner.start(
                            [
                                sys.executable,
                                "-c",
                                "import sys,time;sys.stdout.write('x'*200000);sys.stdout.flush();time.sleep(30)",
                            ],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                        )
                        sender.start()
                        owner.communicate(timeout=5)
            finally:
                stop.set()
                if sender.ident is not None:
                    sender.join(timeout=1)
        self.assertIsNotNone(owner.returncode)
        self.assertTrue(owner.stdout.closed)
        self.assertTrue(owner.stderr.closed)


class SamplingPermissions(unittest.TestCase):
    @unittest.skipIf(
        hasattr(os, "geteuid") and os.geteuid() == 0,
        "root bypasses directory permissions",
    )
    def test_unreadable_scratch_is_not_silently_omitted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "private"
            private.mkdir()
            report = dict(qualified=False)
            try:
                with self.assertRaises(PermissionError):
                    with worker.DiskSampler(root, report) as sampler:
                        private.chmod(0)
                        sampler.sample()
            finally:
                private.chmod(0o700)
            result = json.loads((root / "result.json").read_text())
            self.assertEqual(result["status"], "failed")
            self.assertFalse(result["qualified"])
            self.assertEqual(result["error_type"], "PermissionError")


class SamplingShutdown(unittest.TestCase):
    def test_success_requires_a_completed_final_sample(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = dict(qualified=False)
            with worker.DiskSampler(root, report):
                (root / "payload").write_bytes(bytes(4096))
                report["qualified"] = True
            result = json.loads((root / "result.json").read_text())
            self.assertTrue(result["qualified"])
            self.assertTrue(result["scratch_sample"]["final_sample_complete"])
            self.assertTrue(result["scratch_sample"]["sampler_stopped"])
            self.assertGreaterEqual(
                result["scratch_sample"]["case_nominal_bytes"], 4096
            )

    def test_cancelled_shutdown_retains_last_sample_without_rescanning(self):
        with tempfile.TemporaryDirectory() as directory, CancellationScope() as scope:
            root = Path(directory)
            report = dict(qualified=True)
            sampler = worker.DiskSampler(root, report)
            sampler.__enter__()
            scope.request(signal.SIGTERM)
            with mock.patch.object(
                sampler, "sample", side_effect=AssertionError("rescan")
            ):
                with self.assertRaises(SystemExit):
                    sampler.__exit__(None, None, None)
            result = json.loads((root / "result.json").read_text())
            self.assertEqual(result["status"], "cancelled")
            self.assertFalse(result["qualified"])
            self.assertFalse(result["scratch_sample"]["final_sample_complete"])
            self.assertTrue(result["scratch_sample"]["sampler_stopped"])
            self.assertIsNotNone(
                result["scratch_sample"]["host_filesystem_min_free_bytes"]
            )

    def test_stalled_final_scan_cannot_block_terminal_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = dict(qualified=True)
            sampler = worker.DiskSampler(root, report)
            sampler.__enter__()
            release = threading.Event()
            entered = threading.Event()

            def stalled():
                entered.set()
                provisional = json.loads((root / "result.json").read_text())
                self.assertFalse(provisional["qualified"])
                self.assertEqual(provisional["status"], "finalizing")
                release.wait(10)

            start = time.monotonic()
            try:
                with mock.patch.object(sampler, "sample", side_effect=stalled):
                    with self.assertRaisesRegex(RuntimeError, "scratch sampler failed"):
                        sampler.__exit__(None, None, None)
                self.assertTrue(entered.is_set())
                self.assertLess(time.monotonic() - start, 5)
                result = json.loads((root / "result.json").read_text())
                self.assertFalse(result["qualified"])
                self.assertEqual(result["status"], "failed")
                self.assertFalse(result["scratch_sample"]["sampler_stopped"])
                self.assertFalse(result["scratch_sample"]["final_sample_complete"])
            finally:
                release.set()
                sampler.thread.join(timeout=1)

    def test_scan_checks_deadline_and_cancellation_between_entries(self):
        with tempfile.TemporaryDirectory() as directory, CancellationScope() as scope:
            sampler = worker.DiskSampler(Path(directory), {})
            sampler.final_deadline = time.monotonic() - 1
            with self.assertRaises(TimeoutError):
                sampler.sample()
            sampler.final_deadline = None
            scope.request(signal.SIGTERM)
            with self.assertRaises(InterruptedError):
                sampler.sample()
