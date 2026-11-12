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

"""Test benchmark evidence admission with a synthetic child, not performance."""

import hashlib
import json
import math
from pathlib import Path
import shlex
import struct
import subprocess
import sys
import tempfile
import unittest


class BulkPipelineEvidence(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=Path(__file__).parent)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.source = self.root / "train.parquet"
        self.source.write_bytes(b"synthetic source")
        self.truth = self.root / "truth"
        self.truth.mkdir()
        self.manifest = dict(
            vectors=1000,
            dims=2,
            queries=1,
            k=100,
            metric="squared_l2",
            normalization="none",
            accumulation="float64",
            source_sha256=hashlib.sha256(self.source.read_bytes()).hexdigest(),
            outputs={},
        )
        for name, payload in (("queries.f32", bytes(8)), ("truth.u64", bytes(800))):
            (self.truth / name).write_bytes(payload)
            self.manifest["outputs"][name] = hashlib.sha256(payload).hexdigest()
        self.write_manifest()
        self.records = {
            "PARQUET_BULK_BUILD_MODE": dict(
                caller="fast", caller_value=1, provider=1, provider_is_test=False
            ),
            "PARQUET_BULK_PHASES": dict(
                names=["identity", "dense_cluster", "dense_checkpoint"],
                ns=[1, 1, 1],
                calls=[1, 1, 1],
            ),
            "PARQUET_BULK_WORK": dict(
                identity_census_verified=True,
                owned_index_count=0,
                identity_merge_inputs=0,
                projection_cache_peak_bytes=20 * 1024**2,
                update_parent_calls=0,
            ),
            "PARQUET_BULK_PROJECTION": dict(runs=1, bytes=100),
            "PARQUET_BULK_SOURCE": dict(
                runs=1, reused_runs=1, reused_bytes=100, new_bytes=0
            ),
            "PARQUET_BULK_MEMORY": dict(
                dense_allocator_peak_bytes=32 * 1024**2,
                source_allocator_peak_bytes=16 * 1024**2,
            ),
        }
        self.child_output = self.root / "child-output"
        self.probe = self.root / "probe"
        self.marker = self.root / "child-started"
        self.write_probe()
        self.recall_override = {}

    def write_manifest(self):
        (self.truth / "prepare.json").write_text(json.dumps(self.manifest))

    def write_probe(self, after=""):
        self.probe.write_text(
            "#!/bin/sh\n"
            + "touch "
            + shlex.quote(str(self.marker))
            + "\n"
            + "cat "
            + shlex.quote(str(self.child_output))
            + "\n"
            + after
        )
        self.probe.chmod(0o700)

    def run_probe(self, source=None):
        lines = [
            prefix + " " + json.dumps(record) for prefix, record in self.records.items()
        ]
        lines.append(
            "PARQUET_BULK algorithm=lloyd vectors=1000 dims=2 total_ns=100 reopened_hits=10 checkpoints=2 logical_identity=true"
        )
        for probes in (512, 1024, 2048, 4096, 8192):
            lines.append(
                "PARQUET_BULK_RECALL "
                + json.dumps(
                    dict(
                        queries=1, probes=probes, k=100, recall=1, hits=100, scored=1000
                    )
                    | self.recall_override
                )
            )
        self.child_output.write_text("\n".join(lines) + "\n")
        self.report = self.root / "report.json"
        return subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("bench_bulk_pipeline.py")),
                str(self.probe),
                str(source or self.source),
                str(self.truth),
                str(self.report),
                "--logs",
                str(self.root / "logs"),
                "--dims",
                "2",
                "--trials",
                "1",
                "--variants",
                "prebound_parents",
            ],
            capture_output=True,
            text=True,
        )

    def row(self):
        return json.loads(self.report.read_text())["results"][0]

    def assert_failed_measurements_saved(self, result):
        self.assertNotEqual(result.returncode, 0)
        row = self.row()
        self.assertFalse(row["qualified"])
        self.assertGreater(row["subprocess_wall_ns"], 0)
        self.assertIn("subprocess_user_ns", row)
        self.assertIn("subprocess_peak_rss_bytes", row)
        self.assertIn("qualification_error", row)

    def test_soft_cache_target_is_not_a_hard_allocator_limit(self):
        result = self.run_probe()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.row()["qualified"])
        self.assertTrue(self.row()["cache_target_exceeded"])

    def test_hard_limit_failure_preserves_process_measurements(self):
        self.records["PARQUET_BULK_MEMORY"]["dense_allocator_peak_bytes"] = (
            256 * 1024**2 + 1
        )
        self.assert_failed_measurements_saved(self.run_probe())
        self.assertIn("quota", self.row()["qualification_error"])

    def test_bad_truth_fails_before_starting_child(self):
        (self.truth / "truth.u64").write_bytes(bytes(799) + b"!")
        self.assertNotEqual(self.run_probe().returncode, 0)
        self.assertFalse(self.marker.exists())
        self.assertFalse(self.report.exists())

    def test_source_drift_preserves_process_measurements(self):
        self.write_probe("printf x >> " + shlex.quote(str(self.source)) + "\n")
        self.assert_failed_measurements_saved(self.run_probe())

    def test_child_failure_preserves_process_measurements(self):
        self.write_probe("exit 23\n")
        self.assert_failed_measurements_saved(self.run_probe())
        self.assertEqual(self.row()["process_exit_code"], 23)

    def test_nonfinite_recall_is_rejected(self):
        self.recall_override = {"recall": math.nan}
        self.assert_failed_measurements_saved(self.run_probe())

    def test_duplicate_recall_widths_are_rejected(self):
        self.recall_override = {"probes": 512}
        self.assert_failed_measurements_saved(self.run_probe())

    def test_inconsistent_hit_counts_are_rejected(self):
        self.recall_override = {"hits": 99}
        self.assert_failed_measurements_saved(self.run_probe())

    def test_missing_phase_evidence_is_rejected(self):
        self.records.pop("PARQUET_BULK_PHASES")
        self.assert_failed_measurements_saved(self.run_probe())

    def test_partial_source_reuse_is_rejected(self):
        self.records["PARQUET_BULK_SOURCE"]["runs"] = 2
        self.assert_failed_measurements_saved(self.run_probe())

    def framed_source(self):
        fixture = self.root / "framed"
        objects = fixture / "buckets" / "antfly" / "objects"
        objects.mkdir(parents=True)
        name = "train-00-of-01.parquet"
        body = self.source.read_bytes()
        digest = self.manifest["source_sha256"]
        payload = objects / name
        payload.write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(body), 0)
            + digest.encode("ascii")
            + body
        )
        (fixture / "fixture.json").write_text(
            json.dumps(
                dict(
                    format="AFOBJ001",
                    files=[dict(name=name, bytes=len(body), sha256=digest)],
                )
            )
        )
        self.manifest.update(
            source_sha256=None, sources=[dict(name=name, rows=1000, sha256=digest)]
        )
        self.write_manifest()
        return fixture, payload

    def test_framed_inventory_and_body_are_bound_to_truth(self):
        fixture, _ = self.framed_source()
        result = self.run_probe(fixture)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.row()["qualified"])

    def test_framed_body_tampering_fails_before_child(self):
        fixture, payload = self.framed_source()
        payload.write_bytes(payload.read_bytes()[:-1] + b"!")
        self.assertNotEqual(self.run_probe(fixture).returncode, 0)
        self.assertFalse(self.marker.exists())

    def test_extra_framed_object_fails_before_child(self):
        fixture, payload = self.framed_source()
        payload.with_name("unexpected.parquet").write_bytes(payload.read_bytes())
        self.assertNotEqual(self.run_probe(fixture).returncode, 0)
        self.assertFalse(self.marker.exists())


if __name__ == "__main__":
    unittest.main()
