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

import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import unittest

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

import qualify_metal_native_checkpoint_performance as performance


def digest(data):
    return hashlib.sha256(data).hexdigest()


class PreparedReuseContract(unittest.TestCase):
    def fixture(self, root):
        root = Path(root)
        request = root / "request.json"
        request.write_text('{"model":"google/e4b"}')
        server = root / "producer-server"
        server.write_bytes(b"sealed producer")
        candidate = root / "candidate.kv"
        candidate.write_bytes(b"compacted checkpoint")
        exact = root / "candidate.kv.exact.kv"
        exact.write_bytes(b"exact checkpoint")
        output = root / "producer.json"
        identity = {field: digest(field.encode())
                    for field in performance.PORTABLE_IDENTITY_FIELDS}
        ready = {
            "schema": "antfly.metal-checkpoint-test-server.v1",
            "backend": "metal", "metal_enabled": True, "cuda_enabled": False,
            "pid": 7, "port": 1234, "model": "google/e4b",
            "load_generation": 1, "loaded_identity": identity,
        }
        ready_path = output.with_suffix(".worker.ready.json")
        ready_path.write_text(json.dumps(ready))
        request = request.resolve()
        server = server.resolve()
        candidate = candidate.resolve()
        exact = exact.resolve()
        output = output.resolve()
        ready_path = ready_path.resolve()

        def prepared(path, metadata):
            return {
                "bytes": path.stat().st_size,
                "file_sha256": performance.sha256_file(path),
                "identity": identity,
                "metadata": {**metadata, "checkpoint_bytes": path.stat().st_size,
                             "model": "google/e4b", "wire_version": 3},
            }

        exact_result = prepared(exact, {
            "encoding": "f16", "outcome": "unchanged_insufficient_savings",
            "global_savings_ratio": 1, "logical_tokens": 8128,
            "validated_heads": 0, "preserved_heads": 8,
            "fp8_heads": 0, "fp8_fallback_heads": 0,
        })
        candidate_result = prepared(candidate, {
            "encoding": "f16", "outcome": "compacted",
            "global_savings_ratio": 2.0, "logical_tokens": 8128,
            "validated_heads": 8, "preserved_heads": 0,
            "fp8_heads": 0, "fp8_fallback_heads": 0,
        })
        command = [str(server), "--test-filter",
                   "context Metal native checkpoint qualification server"]
        producer = {
            "schema": performance.SCHEMA, "backend": "metal",
            "server_kind": "metal-test", "qualification_profile":
                "metal_test_native_checkpoint_v1",
            "production_ready": False, "passed": False,
            "qualification_sized": False, "performance_qualified": False,
            "encoding": "f16", "declared_context_tokens": 8192,
            "request": {"path": str(request),
                        "sha256": performance.sha256_file(request)},
            "normalized_request_sha256": digest(b"normalized"),
            "binary_sha256": performance.sha256_file(server),
            "runner_sha256": digest(b"producer runner"),
            "source": {
                "dirty": True, "head": digest(b"head"),
                "source_snapshot_sha256": digest(b"snapshot"),
                "status_sha256": digest(b"status"),
                "tracked_diff_sha256": digest(b"diff"),
                "untracked_content_sha256": digest(b"untracked"),
            },
            "exact_control": {key: exact_result["metadata"].get(key) for key in (
                "outcome", "encoding", "checkpoint_bytes", "logical_tokens",
                "global_savings_ratio", "validated_heads")},
            "physical_ledger": {key: candidate_result["metadata"].get(key)
                                for key in (
                "outcome", "encoding", "checkpoint_bytes", "logical_tokens",
                "global_savings_ratio", "validated_heads", "preserved_heads",
                "fp8_heads", "fp8_fallback_heads")},
            "preparation_seconds": {"exact": 10.0, "compacted": 11.0},
            "error": "AssertionError: post-prepare diagnostic failed",
            "worker": {
                "ready": ready, "ready_sha256": performance.sha256_file(ready_path),
                "command": command,
                "requests": [
                    {"kind": "prepare", "result": exact_result},
                    {"kind": "prepare", "result": candidate_result},
                ],
            },
        }
        output.write_text(json.dumps(producer))
        args = SimpleNamespace(
            prepared_evidence=output, encoding="f16",
            declared_context_tokens=8192, model="google/e4b",
            checkpoint=candidate, minimum_savings=1.5,
            reserved_output_tokens=64,
        )
        return args, request, producer

    def test_context_prepared_reuse_binds_failed_producer_and_both_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            args, request, producer = self.fixture(temporary)
            result = performance.load_prepared_evidence(
                args, request.resolve(), producer["normalized_request_sha256"])
            self.assertFalse(result["producer_passed"])
            self.assertEqual(result["producer_error"], producer["error"])
            self.assertEqual(result["exact"]["sha256"],
                             producer["worker"]["requests"][0]["result"]["file_sha256"])
            self.assertEqual(result["candidate"]["sha256"],
                             producer["worker"]["requests"][1]["result"]["file_sha256"])
            self.assertEqual(result["producer_source"], producer["source"])

    def test_context_prepared_reuse_rejects_artifact_request_and_identity_changes(self):
        mutations = ("checkpoint", "request", "identity", "metadata", "binary")
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                args, request, producer = self.fixture(temporary)
                if mutation == "checkpoint":
                    args.checkpoint.write_bytes(b"changed checkpoint")
                elif mutation == "request":
                    request.write_text('{"model":"different"}')
                elif mutation == "identity":
                    producer["worker"]["ready"]["loaded_identity"]["weights"] = "0" * 64
                    args.prepared_evidence.write_text(json.dumps(producer))
                    args.prepared_evidence.with_suffix(".worker.ready.json").write_text(
                        json.dumps(producer["worker"]["ready"]))
                elif mutation == "metadata":
                    producer["worker"]["requests"][1]["result"]["metadata"][
                        "logical_tokens"] = 8127
                    args.prepared_evidence.write_text(json.dumps(producer))
                else:
                    Path(producer["worker"]["command"][0]).write_bytes(
                        b"changed server")
                with self.assertRaises(ValueError):
                    performance.load_prepared_evidence(
                        args, request.resolve(), producer["normalized_request_sha256"])

    def test_context_prepared_reuse_requires_smoke(self):
        argv = [
            "--backend", "metal", "--server-kind", "metal-test", "--server", "x",
            "--models-dir", "m", "--model", "e4b", "--port", "1", "--request", "r",
            "--declared-context-tokens", "8192", "--encoding", "f16",
            "--output", "/tmp/reuse-output", "--checkpoint", "/tmp/reuse-checkpoint",
            "--prepared-evidence", "/tmp/reuse-evidence",
        ]
        with self.assertRaisesRegex(ValueError, "requires --smoke"):
            performance.main(argv)


if __name__ == "__main__":
    unittest.main()
