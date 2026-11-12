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

"""Offline regressions for bounded uploads and exact receipt recovery evidence."""

import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import gcs_bulk_local as controller
import gcs_bulk_worker as worker


class UploadTests(unittest.TestCase):
    def upload(self, command, payload, *, framed=False, timeout=5):
        original = subprocess.Popen
        children = []

        def launch(_argv, **kwargs):
            child = original(command, **kwargs)
            children.append(child)
            return child

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.parquet"
            source.write_bytes((b"x" * 84 if framed else b"") + payload)
            try:
                with mock.patch.object(
                    controller.subprocess, "Popen", side_effect=launch
                ):
                    controller.upload(
                        "bucket",
                        "prefix",
                        "train.parquet",
                        source,
                        {"sha256": hashlib.sha256(payload).hexdigest()},
                        framed,
                        root / "upload.log",
                        timeout=timeout,
                    )
            finally:
                for child in children:
                    self.assertIsNotNone(child.poll(), "upload left a child process")
                    self.assertTrue(child.stdin.closed)

    def test_stalled_stdin_has_a_whole_transfer_deadline(self):
        started = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            self.upload(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                b"x" * (2 * 1024 * 1024),
                timeout=0.2,
            )
        self.assertLess(time.monotonic() - started, 3)

    def test_stream_strips_frame_and_preserves_every_payload_byte(self):
        payload = bytes(range(256)) * 8192
        program = "import sys,hashlib; data=sys.stdin.buffer.read(); sys.exit(0 if hashlib.sha256(data).hexdigest()==sys.argv[1] else 2)"
        self.upload(
            [sys.executable, "-c", program, hashlib.sha256(payload).hexdigest()],
            payload,
            framed=True,
        )

    def test_early_consumer_failure_is_reaped(self):
        with self.assertRaises((BrokenPipeError, RuntimeError)):
            self.upload(
                [sys.executable, "-c", "import sys; sys.exit(3)"],
                b"x" * (2 * 1024 * 1024),
            )


class ReceiptTests(unittest.TestCase):
    def receipt(self, phase):
        return dict(name="emb", manifest="sha256:" + phase, phase=phase, rows=100000)

    def interrupted(self, phase, cfg=None):
        receipt = self.receipt(phase)
        records = (
            [self.receipt("source")]
            if phase == "source"
            else [self.receipt("source"), receipt]
        )
        text = (
            "\n".join("PARQUET_BULK_RECEIPT " + json.dumps(row) for row in records)
            + "\nerror: Cancelled"
        )
        catalog = {
            "checkpoint": {
                "construction": [
                    {"name": "emb", "manifest": {"artifact_id": receipt["manifest"]}}
                ]
            }
        }
        return worker.validate_interruption(
            cfg or dict(interrupt=1 if phase == "source" else 2, rows=100000),
            dict(exit_code=1),
            text,
            catalog,
        )

    def resumed(self, phase):
        receipt = self.receipt(phase)
        return receipt, {
            "PARQUET_BULK_RESUME": [receipt],
            "PARQUET_BULK_INPUT": [dict(rows=0, resumed=True)],
            "PARQUET_BULK_PHASES": [
                dict(
                    names=["dense_cluster", "bulk_tee", "bulk_receipt"],
                    calls=[0 if phase == "tree" else 1, 0, 0],
                )
            ],
        }

    def test_exact_phase_and_manifest_are_required(self):
        for phase in ("source", "tree"):
            self.assertEqual(self.interrupted(phase), self.receipt(phase))
        with self.assertRaises(RuntimeError):
            self.interrupted("source", dict(interrupt=2, rows=100000))
        receipt, records = self.resumed("tree")
        wrong = dict(receipt, manifest="different")
        with self.assertRaises(RuntimeError):
            worker.validate_resume(records, wrong)

    def test_rebuilds_and_input_replay_cannot_qualify_as_resume(self):
        for phase in ("source", "tree"):
            receipt, records = self.resumed(phase)
            worker.validate_resume(records, receipt)
            changed = copy.deepcopy(records)
            changed["PARQUET_BULK_INPUT"][0]["rows"] = 1
            with self.assertRaises(RuntimeError):
                worker.validate_resume(changed, receipt)
        receipt, records = self.resumed("tree")
        records["PARQUET_BULK_PHASES"][0]["calls"][0] = 1
        with self.assertRaises(RuntimeError):
            worker.validate_resume(records, receipt)
        receipt, records = self.resumed("tree")
        records["PARQUET_BULK_WORK"] = [dict(training_ns=1)]
        with self.assertRaises(RuntimeError):
            worker.validate_resume(records, receipt)


if __name__ == "__main__":
    unittest.main()
