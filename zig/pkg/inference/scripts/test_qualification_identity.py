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


    def managed(self, *, scope="extractors", variant="safetensors@main", source_owner="org", source_name="model"):
        scoped = self.root / "models" / scope / "org" / "physical--antfly-0123456789abcdef"
        scoped.parent.mkdir(parents=True, exist_ok=True)
        self.model.rename(scoped)
        self.model = scoped
        source = {"owner": source_owner, "name": source_name, "variant": variant,
                  "requested_revision": "main", "resolved_revision": "d" * 40,
                  "selected_format": "safetensors"}
        artifacts = [{"path": path.name, "size": path.stat().st_size,
                      "sha256": identity.sha256_file(path)["sha256"]}
                     for path in sorted(self.model.iterdir())]
        receipt = {"version": 2, "source": source, "artifacts": artifacts}
        selector = identity._canonical_source(source)
        self.inventory["data"][0]["model"] = selector
        return receipt, selector, self.write_receipt(receipt, selector)

    def write_receipt(self, receipt, selector):
        path = self.model / identity.COMPLETE_RECEIPT
        path.write_text(json.dumps(receipt))
        return {selector: {"relative_path": str(self.model.relative_to(self.root / "models")),
                           "publication_sha256": identity.sha256_file(path)["sha256"]}}

    def managed_snapshot(self, mapping):
        return identity.model_artifacts(self.config, "unused", self.root, model_roots=mapping)

    def test_real_v2_source_resolves_public_selector_to_scoped_physical_root(self):
        receipt, selector, mapping = self.managed(scope="generators", source_owner="Qwen", source_name="Qwen3-0.6B")
        self.assertEqual(selector, "Qwen/Qwen3-0.6B:safetensors@" + "d" * 40)
        first = self.managed_snapshot(mapping)
        second = self.managed_snapshot(mapping)
        identity.require_matching_artifacts({"model_artifacts": first}, {"model_artifacts": second})
        publication = first["models"][0]["publication"]
        self.assertEqual(publication["sha256"], mapping[selector]["publication_sha256"])
        self.assertEqual(publication["source"], receipt["source"])
        self.assertNotIn("physical--antfly", first["models"][0]["model"])
        with self.assertRaises(q.EvidenceUnavailable):
            self.snapshot()  # no public-selector-to-path guessing

    def test_auto_requested_and_resolved_revision_follow_server_canonical_order(self):
        for variant, expected in [("auto", "org/model"), ("@main", "org/model"), ("safetensors@feature/export", "org/model:safetensors"), ("gguf:Q4_K_M", "org/model:gguf:Q4_K_M")]:
            source = {"owner": "org", "name": "model", "variant": variant, "resolved_revision": "e" * 40}
            self.assertEqual(identity._canonical_source(source), expected + "@" + "e" * 40)
        source["resolved_revision"] = "moving-ref"
        self.assertEqual(identity._canonical_source(source), "org/model:gguf:Q4_K_M")

    def test_legacy_unmanaged_scoped_runtime_name_and_ambiguity(self):
        scoped = self.root / "models" / "extractors" / "org" / "model"
        scoped.parent.mkdir(parents=True)
        self.model.rename(scoped)
        self.model = scoped
        result = self.snapshot()
        self.assertNotIn("publication", result["model_artifacts"]["models"][0])
        duplicate = self.root / "models" / "readers" / "org" / "model"
        duplicate.mkdir(parents=True)
        (duplicate / "config.json").write_text("{}")
        (duplicate / "model.safetensors").write_bytes(b"other")
        with self.assertRaises(q.EvidenceUnavailable):
            self.snapshot()

    def test_version_one_receipt_preserves_physical_relative_runtime_identity(self):
        artifacts = [{"path": path.name, "size": path.stat().st_size} for path in sorted(self.model.iterdir())]
        (self.model / identity.COMPLETE_RECEIPT).write_text(json.dumps({"version": 1, "artifacts": artifacts}))
        snapshot = self.snapshot()["model_artifacts"]
        self.assertEqual(snapshot["models"][0]["model"], "org/model")
        self.assertEqual(snapshot["models"][0]["publication"]["version"], 1)
        self.assertNotIn(identity.COMPLETE_RECEIPT, {item["path"] for item in snapshot["models"][0]["files"]})

    def test_mapping_cannot_override_receipt_source_generation_or_containment(self):
        receipt, selector, mapping = self.managed()
        for field, value in [("publication_sha256", "f" * 64), ("relative_path", "../outside")]:
            changed = copy.deepcopy(mapping)
            changed[selector][field] = value
            with self.subTest(field=field), self.assertRaises(q.EvidenceUnavailable):
                self.managed_snapshot(changed)
        receipt["source"]["name"] = "same-shaped-foreign-model"
        changed = self.write_receipt(receipt, selector)
        with self.assertRaises(q.EvidenceUnavailable):
            self.managed_snapshot(changed)

    def test_incomplete_markers_unknown_versions_and_missing_source_fail_closed(self):
        receipt, selector, mapping = self.managed()
        for name in identity.BLOCKED_MARKERS:
            marker = self.model / name
            marker.write_text("{}")
            with self.subTest(marker=name), self.assertRaises(q.EvidenceUnavailable):
                self.managed_snapshot(mapping)
            marker.unlink()
        for version in (3, True):
            receipt["version"] = version
            with self.assertRaises(q.EvidenceUnavailable):
                self.managed_snapshot(self.write_receipt(receipt, selector))
        receipt["version"] = 2
        receipt["source"] = None
        with self.assertRaises(q.EvidenceUnavailable):
            self.managed_snapshot(self.write_receipt(receipt, selector))

    def test_receipt_paths_sizes_digests_duplicates_and_foreign_aliases_are_checked(self):
        receipt, selector, _ = self.managed()
        original = copy.deepcopy(receipt)
        for field, value in [("path", "../foreign.safetensors"), ("size", 999), ("sha256", "a" * 64)]:
            changed = copy.deepcopy(original)
            changed["artifacts"][0][field] = value
            with self.subTest(field=field), self.assertRaises(q.EvidenceUnavailable):
                self.managed_snapshot(self.write_receipt(changed, selector))
        changed = copy.deepcopy(original)
        changed["artifacts"].append(copy.deepcopy(changed["artifacts"][0]))
        with self.assertRaises(q.EvidenceUnavailable):
            self.managed_snapshot(self.write_receipt(changed, selector))
        foreign = self.root / "foreign.safetensors"
        foreign.write_bytes(b"external")
        (self.model / "foreign.safetensors").symlink_to(foreign)
        changed = copy.deepcopy(original)
        changed["artifacts"].append({"path": "foreign.safetensors", "size": foreign.stat().st_size})
        with self.assertRaises(q.EvidenceUnavailable):
            self.managed_snapshot(self.write_receipt(changed, selector))

    def test_same_physical_aliases_cannot_weaken_checked_digest(self):
        receipt, selector, _ = self.managed()
        source = self.model / "model.safetensors"
        (self.model / "alias.safetensors").symlink_to("model.safetensors")
        receipt["artifacts"].append({"path": "alias.safetensors", "size": source.stat().st_size})
        self.managed_snapshot(self.write_receipt(receipt, selector))
        receipt["artifacts"][-1]["sha256"] = "f" * 64
        with self.assertRaises(q.EvidenceUnavailable):
            self.managed_snapshot(self.write_receipt(receipt, selector))

    def test_receipt_generation_and_marker_change_during_file_hashing_are_rejected(self):
        receipt, selector, mapping = self.managed()
        original_files = identity.model_files
        def mutate(directory):
            files = original_files(directory)
            with (self.model / identity.COMPLETE_RECEIPT).open("a") as stream:
                stream.write(" ")
            return files
        with patch.object(identity, "model_files", side_effect=mutate), self.assertRaises(q.EvidenceUnavailable):
            self.managed_snapshot(mapping)
        mapping = self.write_receipt(receipt, selector)
        def mark(directory):
            files = original_files(directory)
            (self.model / identity.BLOCKED_MARKERS[0]).write_text("{}")
            return files
        with patch.object(identity, "model_files", side_effect=mark), self.assertRaises(q.EvidenceUnavailable):
            self.managed_snapshot(mapping)

    def test_identical_sibling_publication_cannot_borrow_controlled_root_identity(self):
        receipt, selector, mapping = self.managed()
        other = self.root / "models" / "readers" / "org" / "other-physical"
        other.mkdir(parents=True)
        for path in self.model.iterdir():
            (other / path.name).write_bytes(path.read_bytes())
        with self.assertRaises(q.EvidenceUnavailable):
            self.managed_snapshot(mapping)

    def test_direct_configured_model_root_mapping_uses_receipt_not_selector_path(self):
        receipt, selector, mapping = self.managed()
        config = {"inference": {"models_dir": str(self.model)}}
        mapping[selector]["relative_path"] = "."
        snapshot = identity.model_artifacts(config, "unused", self.root, model_roots=mapping)
        self.assertEqual(snapshot["models"][0]["model"], selector)

    def test_public_model_or_backend_change_with_same_instance_id_is_rejected(self):
        for field, value in [("model", "foreign/model"), ("backend", "native"), ("state", "loading")]:
            after = copy.deepcopy(self.inventory)
            after["data"][0][field] = value
            with self.subTest(field=field), patch.object(q, "http", side_effect=[(200, self.inventory, 0), (200, after, 0)]), self.assertRaises(q.EvidenceUnavailable):
                self.snapshot()


if __name__ == "__main__":
    unittest.main()
