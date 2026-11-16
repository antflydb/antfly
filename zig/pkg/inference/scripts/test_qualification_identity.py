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

"""Controlled comparisons must reject changed or missing model identities."""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import qualification_identity as identity
import qualify_cooperative_execution as cooperative
import qualify_read_pool_40g as q


class ArtifactIdentityTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.model = self.root / "models" / "org" / "model"
        self.model.mkdir(parents=True)
        (self.model / "config.json").write_text('{"quantization":"bf16"}')
        (self.model / "model.safetensors").write_bytes(b"original weights")
        (self.model / "tokenizer.json").write_text("{}")
        self.config = {"inference": {"models_dir": "models"}}
        self.inventory = {
            "data": [
                {
                    "id": "loaded-1",
                    "model": "org/model",
                    "state": "loaded",
                    "backend": "cuda",
                    "tasks": ["embedder"],
                }
            ]
        }
        p = patch.object(
            q,
            "http",
            side_effect=lambda *a, **k: (200, copy.deepcopy(self.inventory), 0),
        )
        p.start()
        self.addCleanup(p.stop)

    def snapshot(self):
        return {
            "model_artifacts": identity.model_artifacts(
                self.config, "unused", self.root
            )
        }

    def test_deterministic_and_complete_including_manifest_tokenizer_and_weights(self):
        (self.model / "model_manifest.json").write_text('{"name":"model"}')
        a, b = self.snapshot(), self.snapshot()
        identity.require_matching_artifacts(a, b)
        files = a["model_artifacts"]["models"][0]["files"]
        self.assertEqual(
            {item["path"] for item in files},
            {
                "config.json",
                "model_manifest.json",
                "tokenizer.json",
                "model.safetensors",
            },
        )
        self.assertTrue(all(len(item["sha256"]) == 64 for item in files))

    def test_same_path_size_and_mtime_do_not_hide_changed_weights(self):
        a = self.snapshot()
        path = self.model / "model.safetensors"
        before = path.stat()
        path.write_bytes(b"successor weight")
        import os

        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        self.assertEqual(before.st_size, path.stat().st_size)
        with self.assertRaises(q.EvidenceUnavailable):
            identity.require_matching_artifacts(a, self.snapshot())

    def test_manifest_and_tokenizer_mutation_each_invalidate_control(self):
        for name in ("config.json", "tokenizer.json"):
            a = self.snapshot()
            (self.model / name).write_text('{"changed":true}')
            with self.assertRaises(q.EvidenceUnavailable):
                identity.require_matching_artifacts(a, self.snapshot())

    def test_old_or_incomplete_evidence_cannot_be_compared(self):
        good = self.snapshot()
        for missing in (
            {},
            {"model_artifacts": {}},
            {"model_artifacts": {"version": 1, "models": []}},
        ):
            with self.assertRaises(q.EvidenceUnavailable):
                identity.require_matching_artifacts(missing, good)
        bad = copy.deepcopy(good)
        bad["model_artifacts"]["models"][0]["files"] = []
        with self.assertRaises(q.EvidenceUnavailable):
            identity.require_matching_artifacts(good, bad)

    def test_task_scoped_artifacts_resolve_without_weakening_identity(self):
        original = self.snapshot()
        task = self.inventory["data"][0]["tasks"][0]
        scoped = self.root / "models" / (task + "s") / "org/model"
        scoped.parent.mkdir(parents=True)
        self.model.rename(scoped)
        self.assertEqual(original, self.snapshot())

    def test_ambiguous_task_directory_fails_provenance(self):
        scoped = self.root / "models/embedders/org/model"
        scoped.mkdir(parents=True)
        with self.assertRaises(q.EvidenceUnavailable):
            self.snapshot()

    def test_linked_weight_shards_are_hashed_and_directory_cycles_rejected(self):
        cache = self.root / "cache"
        cache.mkdir()
        (cache / "shard.gguf").write_bytes(b"weights")
        (self.model / "shards").symlink_to(cache, target_is_directory=True)
        paths = {
            f["path"] for f in self.snapshot()["model_artifacts"]["models"][0]["files"]
        }
        self.assertIn("shards/shard.gguf", paths)
        (cache / "cycle").symlink_to(self.model, target_is_directory=True)
        with self.assertRaises(q.EvidenceUnavailable):
            self.snapshot()

    def test_cached_checksums_are_not_trusted_as_model_identity(self):
        a = self.snapshot()
        (self.model / "model.safetensors.antfly-sha256-cache").write_text("stale")
        identity.require_matching_artifacts(a, self.snapshot())

    def test_residency_change_during_hashing_is_incomplete(self):
        after = copy.deepcopy(self.inventory)
        after["data"][0]["id"] = "reloaded-2"
        with patch.object(
            q, "http", side_effect=[(200, self.inventory, 0), (200, after, 0)]
        ):
            with self.assertRaises(q.EvidenceUnavailable):
                self.snapshot()

    def test_both_performance_comparisons_reject_changed_artifacts(self):
        original = self.snapshot()
        (self.model / "config.json").write_text('{"quantization":"q8"}')
        changed = self.snapshot()
        for compare, modes, rows in (
            (cooperative.compare, ["serialized", "cooperative"], [1, 1]),
            (cooperative.compare_batching, ["cooperative", "cooperative"], [1, 2]),
        ):
            paths = [self.root / "a", self.root / "b"]
            for path, mode, row, artifacts in zip(
                paths, modes, rows, [original, changed], strict=True
            ):
                path.mkdir(exist_ok=True)
                value = {
                    "status": "passed",
                    "mode": mode,
                    "rows": row,
                    "workload": {},
                    "references": {},
                    "identity": artifacts,
                }
                (path / "comparison.json").write_text(json.dumps(value))
            with self.assertRaises(q.EvidenceUnavailable):
                compare(*paths, self.root / "comparison")
            self.assertFalse((self.root / "comparison").exists())

    def test_file_changing_during_hashing_is_incomplete(self):
        path = self.model / "model.safetensors"
        original = identity.hashlib.sha256

        class ChangingDigest:
            def __init__(self):
                self.digest = original()

            def update(self, block):
                self.digest.update(block)
                path.write_bytes(b"concurrent replacement")

            def hexdigest(self):
                return self.digest.hexdigest()

        with patch.object(identity.hashlib, "sha256", ChangingDigest):
            with self.assertRaises(q.EvidenceUnavailable):
                identity.sha256_file(path)

    def test_missing_file_is_incomplete(self):
        (self.model / "broken.gguf").symlink_to(self.root / "missing")
        with self.assertRaises(q.EvidenceUnavailable):
            self.snapshot()

    def test_missing_weight_or_configuration_is_incomplete(self):
        (self.model / "model.safetensors").unlink()
        with self.assertRaises(q.EvidenceUnavailable):
            self.snapshot()


if __name__ == "__main__":
    unittest.main()
