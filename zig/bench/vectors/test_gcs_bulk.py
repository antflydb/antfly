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
import os
import signal
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


class OwnershipTests(unittest.TestCase):
    def test_frozen_executable_validator_and_truth_survive_live_edits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "probe"
            binary.write_text("#!/bin/sh\nprintf original")
            binary.chmod(0o700)
            harness = root / "harness"
            harness.mkdir()
            (harness / "gcs_bulk_local.py").write_text("# original controller\n")
            (harness / "gcs_bulk_worker.py").write_text(
                "import bench_bulk_pipeline; print(bench_bulk_pipeline.VALUE)\n"
            )
            (harness / "bench_bulk_pipeline.py").write_text("VALUE = 'original'\n")
            recall = root / "recall"
            recall.mkdir()
            for name in ("prepare.json", "queries.f32", "truth.u64"):
                (recall / name).write_text("original")
            output = root / "case"
            output.mkdir()
            retained = root / "retained"
            (retained / "case").mkdir(parents=True)
            (retained / "case.json").write_text("original case")
            (retained / "case/catalog.json").write_text("original catalog")
            with mock.patch.object(
                controller, "__file__", str(harness / "gcs_bulk_local.py")
            ):
                inputs = controller.freeze_inputs(
                    output, binary, recall, reopen_from=retained
                )
            original_hashes = controller.verify_inputs(inputs)
            (retained / "case.json").write_text("changed case")
            (retained / "case/catalog.json").write_text("changed catalog")
            self.assertEqual(
                (inputs / "reopen/catalog.json").read_text(), "original catalog"
            )
            binary.write_text("#!/bin/sh\nprintf changed")
            for path in harness.iterdir():
                path.write_text("# changed\n")
            for path in recall.iterdir():
                path.write_text("changed")
            self.assertEqual(controller.verify_inputs(inputs), original_hashes)
            self.assertEqual(
                subprocess.check_output([str(inputs / "probe")]), b"original"
            )
            self.assertEqual(
                subprocess.check_output(
                    [sys.executable, "-B", str(inputs / "gcs_bulk_worker.py")]
                ).strip(),
                b"original",
            )
            self.assertEqual((inputs / "recall/queries.f32").read_text(), "original")
            (inputs / "probe").chmod(0o700)
            (inputs / "probe").write_text("changed")
            with self.assertRaises(ValueError):
                controller.verify_inputs(inputs)

    def test_cleanup_kills_descendants_after_the_leader_was_reaped(self):
        with tempfile.TemporaryDirectory() as directory:
            pulse = Path(directory) / "pulse"
            program = """import os,signal,time,sys
read,write=os.pipe()
if os.fork():
 os.close(write);os.read(read,1);os._exit(0)
os.close(read);signal.signal(signal.SIGTERM,signal.SIG_IGN)
with open(sys.argv[1],'ab',buffering=0) as out:
 out.write(b'x');os.write(write,b'1');os.close(write)
 for _ in range(3000):out.write(b'x');time.sleep(.01)
"""
            process = subprocess.Popen(
                [sys.executable, "-c", program, str(pulse)],
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                process.wait(timeout=5)
                controller.terminate_group(process)
                time.sleep(0.1)
                before = pulse.stat().st_size
                time.sleep(0.1)
                self.assertEqual(before, pulse.stat().st_size)
                controller.terminate_group(process)  # repeated cleanup is safe
            finally:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    def test_sigterm_cancels_the_real_snapshotted_controller_and_worker_group(self):
        self.check_controller_cancellation(signal.SIGTERM)

    @unittest.skipUnless(hasattr(signal, "SIGHUP"), "POSIX hangup signal required")
    def test_sighup_cancels_the_real_snapshotted_controller_and_worker_group(self):
        self.check_controller_cancellation(signal.SIGHUP)

    def check_controller_cancellation(self, signum):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pulse = root / "pulse"
            pidfile = root / "native.pid"
            environment = root / "native-env.json"
            binary = root / "probe"
            binary.write_text(
                "#!"
                + sys.executable
                + "\nimport os,time,json\nfrom pathlib import Path\nPath(os.environ['OWNED_PID_FILE']).write_text(str(os.getpid()))\nPath(os.environ['OWNED_ENV_FILE']).write_text(json.dumps({key:value for key,value in os.environ.items() if key.startswith(('ANTFLY_LAKE_BULK_','ANTFLY_LAKE_PROBE_'))}))\nwith open(os.environ['OWNED_PULSE_FILE'],'ab',buffering=0) as out:\n while True:out.write(b'x');time.sleep(.02)\n"
            )
            binary.chmod(0o700)
            source = root / "train.parquet"
            source.write_bytes(b"test source")
            recall = root / "recall"
            recall.mkdir()
            manifest = dict(
                vectors=100000,
                dims=768,
                queries=1,
                k=100,
                metric="squared_l2",
                normalization="none",
                accumulation="float64",
                source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                outputs={},
            )
            for name, size in (("queries.f32", 768 * 4), ("truth.u64", 100 * 8)):
                (recall / name).write_bytes(bytes(size))
                manifest["outputs"][name] = hashlib.sha256(bytes(size)).hexdigest()
            (recall / "prepare.json").write_text(json.dumps(manifest))
            tools = root / "bin"
            tools.mkdir()
            meta = dict(
                size=source.stat().st_size,
                custom_fields=dict(sha256=manifest["source_sha256"]),
                generation="1",
                etag="etag",
                name="source/train.parquet",
            )
            gcloud = tools / "gcloud"
            gcloud.write_text(
                "#!" + sys.executable + "\nprint(" + repr(json.dumps(meta)) + ")\n"
            )
            gcloud.chmod(0o700)
            env = dict(
                os.environ,
                PATH=str(tools) + os.pathsep + os.environ["PATH"],
                OWNED_PULSE_FILE=str(pulse),
                OWNED_PID_FILE=str(pidfile),
                OWNED_ENV_FILE=str(environment),
                ANTFLY_LAKE_BULK_PARQUET_FILE="wrong-local-source",
                ANTFLY_LAKE_BULK_COMPACT="1",
                ANTFLY_LAKE_BULK_REOPEN_ONLY="1",
                ANTFLY_LAKE_PROBE_HEADROOM="unknown",
            )
            output = root / "case"
            with (root / "controller.log").open("w") as log:
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-B",
                        str(Path(controller.__file__).resolve()),
                        "--binary",
                        str(binary),
                        "--source",
                        str(source),
                        "--source-prefix",
                        "source",
                        "--bucket",
                        "bucket",
                        "--recall",
                        str(recall),
                        "--output",
                        str(output),
                        "--timeout",
                        "60",
                    ],
                    env=env,
                    start_new_session=True,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
                try:
                    deadline = time.monotonic() + 10
                    while (
                        not pulse.exists()
                        and process.poll() is None
                        and time.monotonic() < deadline
                    ):
                        time.sleep(0.02)
                    self.assertTrue(
                        pulse.exists(), (root / "controller.log").read_text()
                    )
                    actual_env = json.loads(environment.read_text())
                    self.assertEqual(actual_env["ANTFLY_LAKE_BULK_COMPACT"], "0")
                    for key in (
                        "ANTFLY_LAKE_BULK_PARQUET_FILE",
                        "ANTFLY_LAKE_BULK_REOPEN_ONLY",
                        "ANTFLY_LAKE_PROBE_HEADROOM",
                    ):
                        self.assertNotIn(key, actual_env)
                    process.send_signal(signum)
                    self.assertEqual(process.wait(timeout=15), 128 + signum)
                    time.sleep(0.1)
                    before = pulse.stat().st_size
                    time.sleep(0.1)
                    self.assertEqual(before, pulse.stat().st_size)
                    report = json.loads((output / "local.json").read_text())
                    self.assertFalse(report["qualified"])
                    self.assertEqual(report["status"], "cancelled")
                    self.assertEqual(report["controller_exit_code"], 128 + signum)
                    self.assertGreater(report["wall_seconds"], 0)
                    self.assertNotEqual(report["worker_exit_code"], 0)
                    self.assertTrue((output / "inputs/manifest.json").exists())
                finally:
                    controller.terminate_group(process)
                    if pidfile.exists():
                        try:
                            group = os.getpgid(int(pidfile.read_text()))
                            if group != os.getpgrp():
                                os.killpg(group, signal.SIGKILL)
                        except ProcessLookupError:
                            pass


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        report = json.loads(
            Path(__file__).with_name("gcs_bulk_review_results.json").read_text()
        )
        case = report["remote_receipt_validation"]["cases"][0]
        self.row = copy.deepcopy(case["attempts"][1])
        self.receipt = case["attempts"][0]["retained_receipt"]
        source = report["remote_receipt_validation"]["source"]
        files = [
            dict(
                uri="gs://colony-import-sources-antfly-dev-01/"
                + source["prefix"]
                + "/train.parquet",
                bytes=source["bytes"],
                version=str(report["remote_receipt_validation"]["source_generation"]),
            )
        ]
        # Adapt a copied historical record to the new inventory contract. Live
        # inventory attestation is covered separately by the GCS probe checks.
        self.row["records"]["PARQUET_BULK_INVENTORY"] = [dict(files=files)]
        self.cfg = dict(
            variant="optimized", rows=1024, interrupt=1, source_objects=files
        )

    def validate(self, row):
        worker.validate_evidence(self.cfg, row, self.receipt)

    def test_real_gcs_evidence_passes_and_empty_or_missing_phases_fail(self):
        self.validate(self.row)
        for missing in (None, "dense_upload"):
            row = copy.deepcopy(self.row)
            phase = row["records"]["PARQUET_BULK_PHASES"][0]
            if missing is None:
                phase.update(names=[], ns=[], calls=[])
            else:
                i = phase["names"].index(missing)
                for key in ("names", "ns", "calls"):
                    phase[key].pop(i)
            with self.assertRaises(ValueError):
                self.validate(row)

    def test_negative_bool_and_missing_numeric_evidence_fail(self):
        for record, key, value in [
            ("PARQUET_BULK_MEMORY", "dense_allocator_peak_bytes", -1),
            ("PARQUET_BULK_MEMORY", "source_allocator_peak_bytes", False),
            ("PARQUET_BULK_BUILD_MODE", "provider", True),
            ("PARQUET_BULK_WORK", "identity_merge_inputs", -1),
            ("PARQUET_BULK_WORK", "allocator_peak_bytes", None),
            ("PARQUET_BULK_INPUT", "rows", False),
        ]:
            row = copy.deepcopy(self.row)
            row["records"][record][0][key] = value
            with self.assertRaises(RuntimeError):
                self.validate(row)
        for value in (-1, True, 1.5):
            row = copy.deepcopy(self.row)
            row["records"]["PARQUET_BULK_PHASES"][0]["calls"][0] = value
            with self.assertRaises(ValueError):
                self.validate(row)

    def test_native_inventory_rejects_unadmitted_vectorless_files_and_versions(self):
        for change in ("extra", "version", "missing", "duplicate"):
            row = copy.deepcopy(self.row)
            files = row["records"]["PARQUET_BULK_INVENTORY"][0]["files"]
            if change == "extra":
                files.append(
                    dict(
                        uri="gs://bucket/zzz-vectorless.parquet", bytes=100, version="2"
                    )
                )
            elif change == "version":
                files[0]["version"] = "different"
            elif change == "missing":
                files.clear()
            else:
                files.append(dict(files[0]))
            with self.assertRaises(RuntimeError):
                self.validate(row)

    def test_native_inventory_accepts_only_the_admitted_generation_or_etag(self):
        row = copy.deepcopy(self.row)
        cfg = copy.deepcopy(self.cfg)
        cfg["source_objects"][0]["etag"] = "admitted-etag"
        observed = row["records"]["PARQUET_BULK_INVENTORY"][0]["files"][0]
        observed.update(version="", etag="admitted-etag")
        worker.validate_evidence(cfg, row, self.receipt)
        observed["etag"] = "wrong-etag"
        with self.assertRaises(RuntimeError):
            worker.validate_evidence(cfg, row, self.receipt)
        observed["version"] = cfg["source_objects"][0]["version"]
        with self.assertRaises(RuntimeError):
            worker.validate_evidence(cfg, row, self.receipt)

    def test_reopen_cannot_hide_construction_or_nonrelease_compilation(self):
        row = copy.deepcopy(self.row)
        records = {
            key: row["records"][key]
            for key in (
                "PARQUET_BULK_BUILD_MODE",
                "PARQUET_BULK_PHASES",
                "PARQUET_BULK_INVENTORY",
            )
        }
        records["PARQUET_BULK_PHASES"][0]["calls"] = [0] * 14
        records["PARQUET_BULK_PHASES"][0]["ns"] = [0] * 14
        row.update(records=records, native_ns=0, checkpoints=0)
        cfg = dict(self.cfg, reopen_catalog="catalog.json")
        worker.validate_evidence(cfg, row)
        for key, value in (("native_ns", 1), ("checkpoints", 1)):
            changed = copy.deepcopy(row)
            changed[key] = value
            with self.assertRaises(RuntimeError):
                worker.validate_evidence(cfg, changed)
        row["records"]["PARQUET_BULK_BUILD_MODE"][0]["provider_is_test"] = True
        with self.assertRaises(RuntimeError):
            worker.validate_evidence(cfg, row)


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
        files = [dict(uri="gs://bucket/train.parquet", bytes=100, version="1")]
        text = (
            "PARQUET_BULK_INVENTORY "
            + json.dumps(dict(files=files))
            + "\n"
            + (
                "\n".join("PARQUET_BULK_RECEIPT " + json.dumps(row) for row in records)
                + "\nerror: Cancelled"
            )
        )
        catalog = {
            "checkpoint": {
                "construction": [
                    {"name": "emb", "manifest": {"artifact_id": receipt["manifest"]}}
                ]
            }
        }
        return worker.validate_interruption(
            dict(
                cfg or dict(interrupt=1 if phase == "source" else 2, rows=100000),
                source_objects=files,
            ),
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
