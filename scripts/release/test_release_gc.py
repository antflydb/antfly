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

"""Tests for journal-aware release object retention."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

RELEASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(RELEASE_DIR))
NOW = datetime(2026, 9, 1, tzinfo=timezone.utc)


def load_gc():
    path = RELEASE_DIR / "release_gc.py"
    spec = importlib.util.spec_from_file_location("release_gc_test", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gc = load_gc()


def load_registry_gc():
    path = RELEASE_DIR / "release_registry_gc.py"
    spec = importlib.util.spec_from_file_location("release_registry_gc_test", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


registry_gc = load_registry_gc()


class MemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, datetime] = {}
        self.stored: dict[str, gc.StoredObject] = {}
        self.deleted: list[str] = []
        self.container_digests: dict[str, str] = {}

    def put(self, key: str, body: bytes, modified: datetime = NOW) -> None:
        self.objects[key] = modified
        self.stored[key] = gc.StoredObject(
            body=body, etag=hashlib.sha256(body).hexdigest()
        )

    def add_release(
        self,
        tag: str,
        age_days: int,
        artifacts: dict[str, str] | None = None,
        *,
        legacy: bool = False,
        container_digest: str | None = None,
        with_container_identity: bool = True,
    ) -> str:
        artifacts = artifacts or {
            "antfly.tar.gz": hashlib.sha256(tag.encode()).hexdigest()
        }
        ledger = {
            "tag": tag,
            "version": tag.removeprefix("v"),
            "commit": "a" * 40,
            "artifacts": [
                {"name": name, "sha256": digest} for name, digest in artifacts.items()
            ],
        }
        if not legacy:
            ledger["schema_version"] = 4
        ledger_body = (json.dumps(ledger, sort_keys=True) + "\n").encode()
        modified = NOW - timedelta(days=age_days)
        prefix = f"antfly/{tag}/"
        self.put(prefix + "artifacts.json", ledger_body, modified)
        self.put(prefix + "metadata.json", b"{}\n", modified)
        for name, digest in artifacts.items():
            self.put(f"{gc.CONTENT_ROOT}{digest}/{name}", b"artifact", modified)
        ledger_digest = hashlib.sha256(ledger_body).hexdigest()
        container_digest = container_digest or (
            "sha256:" + hashlib.sha256((tag + "-container").encode()).hexdigest()
        )
        self.container_digests[tag] = container_digest
        channel = (
            "nightly"
            if gc.NIGHTLY_PATTERN.fullmatch(tag)
            else "next"
            if "-" in tag
            else "stable"
        )
        if with_container_identity:
            self.put(
                f"{gc.CONTAINER_IDENTITY_ROOT}{ledger_digest}.json",
                (
                    json.dumps(
                        {
                            "schema_version": 1,
                            "tag": tag,
                            "channel": channel,
                            "commit": "a" * 40,
                            "ledger_sha256": ledger_digest,
                            "container_digest": container_digest,
                        },
                        sort_keys=True,
                    )
                    + "\n"
                ).encode(),
                modified,
            )
        return ledger_digest

    def add_completion(self, tag: str, ledger: str, age_days: int) -> None:
        body = (
            json.dumps(
                {
                    "schema_version": 1,
                    "channel": "stable",
                    "tag": tag,
                    "commit": "a" * 40,
                    "ledger_sha256": ledger,
                    "container_digest": self.container_digests[tag],
                    "committed_at": (NOW - timedelta(days=age_days))
                    .isoformat(timespec="seconds")
                    .replace("+00:00", "Z"),
                },
                sort_keys=True,
            )
            + "\n"
        ).encode()
        self.put(f"{gc.COMPLETION_ROOT}{ledger}.json", body)

    def add_journal(
        self,
        channel: str,
        current: tuple[str, str] | None = None,
        pending: tuple[str, str] | None = None,
    ) -> str:
        def identity(value: tuple[str, str] | None):
            if value is None:
                return None
            return {"tag": value[0], "ledger_sha256": value[1]}

        key = f"antfly/channels/{channel}.json"
        body = json.dumps(
            {
                "schema_version": 1,
                "channel": channel,
                "current": identity(current),
                "pending": identity(pending),
            },
            sort_keys=True,
        ).encode()
        self.put(key, body)
        return key

    def list_objects(self, prefix: str) -> list[gc.ObjectInfo]:
        return [
            gc.ObjectInfo(key, modified)
            for key, modified in self.objects.items()
            if key.startswith(prefix)
        ]

    def read_optional(self, key: str) -> gc.StoredObject | None:
        return self.stored.get(key)

    def write_if_absent(self, key: str, body: bytes) -> None:
        current = self.stored.get(key)
        if current is not None and current.body != body:
            raise SystemExit("conflicting pending cleanup")
        if current is None:
            self.put(key, body)

    def delete_objects(self, keys: list[str]) -> None:
        self.deleted.extend(keys)
        for key in keys:
            self.objects.pop(key, None)
            self.stored.pop(key, None)


class ReleaseGCTests(unittest.TestCase):
    def test_stable_and_shared_content_survive_expired_prereleases(self) -> None:
        store = MemoryStore()
        shared = "1" * 64
        nightly_only = "2" * 64
        rc_only = "3" * 64
        stable_digest = store.add_release("v1.2.3", 200, {"shared.bin": shared})
        store.add_completion("v1.2.3", stable_digest, 200)
        store.add_release("v1.2.3-rc.1", 210, {"shared.bin": shared, "rc.bin": rc_only})
        nightly_digest = store.add_release(
            "v0.0.0-dev.1", 100, {"nightly.bin": nightly_only}
        )
        store.add_release("v0.0.0-dev.2", 100)
        store.add_release("v0.0.0-dev.3", 10)

        plan = gc.plan_gc(store, now=NOW, nightly_days=30, nightly_min_count=2)

        self.assertEqual(plan["retained"]["v1.2.3"], "stable")
        self.assertEqual(set(plan["expired"]), {"v1.2.3-rc.1", "v0.0.0-dev.1"})
        self.assertIn(
            f"{gc.CONTENT_ROOT}{nightly_only}/nightly.bin", plan["delete_keys"]
        )
        self.assertIn(f"{gc.CONTENT_ROOT}{rc_only}/rc.bin", plan["delete_keys"])
        self.assertNotIn(f"{gc.CONTENT_ROOT}{shared}/shared.bin", plan["delete_keys"])
        self.assertIn(
            f"{gc.CONTAINER_IDENTITY_ROOT}{nightly_digest}.json",
            {item["record_key"] for item in plan["container_deletions"]},
        )
        self.assertIn(
            f"{gc.CONTAINER_IDENTITY_ROOT}{nightly_digest}.json",
            plan["delete_keys"],
        )

    def test_expired_release_record_is_deleted_when_container_digest_is_shared(
        self,
    ) -> None:
        store = MemoryStore()
        shared_digest = f"sha256:{'7' * 64}"
        stable = store.add_release("v1.2.3", 200, container_digest=shared_digest)
        store.add_completion("v1.2.3", stable, 200)
        expired = store.add_release("v1.2.3-rc.1", 200, container_digest=shared_digest)

        plan = gc.plan_gc(store, now=NOW, prerelease_grace_days=90)

        record_key = f"{gc.CONTAINER_IDENTITY_ROOT}{expired}.json"
        self.assertEqual(plan["container_deletions"], [])
        self.assertEqual(plan["container_record_deletions"], [record_key])
        self.assertIn(record_key, plan["delete_keys"])

    def test_schema_four_release_without_container_identity_is_retained(self) -> None:
        store = MemoryStore()
        store.add_release("v0.0.0-dev.1", 200, with_container_identity=False)
        store.add_release("v0.0.0-dev.2", 1)

        plan = gc.plan_gc(store, now=NOW, nightly_days=30, nightly_min_count=1)

        self.assertEqual(plan["retained"]["v0.0.0-dev.1"], "missing-container-identity")

    def test_recent_and_newest_nightlies_are_both_retained(self) -> None:
        store = MemoryStore()
        for sequence in range(1, 13):
            store.add_release(f"v0.0.0-dev.{sequence}", 5 if sequence == 1 else 100)

        plan = gc.plan_gc(store, now=NOW, nightly_days=30, nightly_min_count=3)

        self.assertEqual(plan["retained"]["v0.0.0-dev.1"], "nightly-age-window")
        for sequence in (10, 11, 12):
            self.assertEqual(
                plan["retained"][f"v0.0.0-dev.{sequence}"],
                "newest-nightly-count",
            )
        self.assertIn("v0.0.0-dev.9", plan["expired"])

    def test_channel_current_and_pending_override_retention(self) -> None:
        store = MemoryStore()
        current = store.add_release("v0.0.0-dev.1", 500)
        pending = store.add_release("v0.0.0-dev.2", 500)
        store.add_release("v0.0.0-dev.3", 1)
        store.add_journal(
            "nightly",
            current=("v0.0.0-dev.1", current),
            pending=("v0.0.0-dev.2", pending),
        )

        plan = gc.plan_gc(store, now=NOW, nightly_days=0, nightly_min_count=1)

        self.assertEqual(plan["retained"]["v0.0.0-dev.1"], "channel-current-or-pending")
        self.assertEqual(plan["retained"]["v0.0.0-dev.2"], "channel-current-or-pending")

    def test_prerelease_waits_for_stable_and_then_for_grace_period(self) -> None:
        store = MemoryStore()
        store.add_release("v2.0.0-rc.1", 300)
        store.add_release("v3.0.0-rc.1", 300)
        stable3 = store.add_release("v3.0.0", 20)
        store.add_completion("v3.0.0", stable3, 20)
        store.add_release("v4.0.0-rc.1", 300)
        stable4 = store.add_release("v4.0.0", 100)
        store.add_completion("v4.0.0", stable4, 100)
        store.add_release("v5.0.0-rc.1", 300)
        store.add_release("v5.0.0", 200)

        plan = gc.plan_gc(store, now=NOW, prerelease_grace_days=90)

        self.assertEqual(plan["retained"]["v2.0.0-rc.1"], "awaiting-matching-stable")
        self.assertEqual(
            plan["retained"]["v3.0.0-rc.1"], "matching-stable-grace-window"
        )
        self.assertEqual(plan["expired"]["v4.0.0-rc.1"], "prerelease-grace-expired")
        self.assertEqual(plan["retained"]["v5.0.0-rc.1"], "awaiting-matching-stable")

    def test_malformed_state_fails_closed(self) -> None:
        store = MemoryStore()
        store.add_release("v0.0.0-dev.1", 100)
        store.put(
            "antfly/channels/nightly.json",
            b'{"schema_version":1,"channel":"nightly","current":"bad"}',
        )

        with self.assertRaisesRegex(SystemExit, "malformed current identity"):
            gc.plan_gc(store, now=NOW)

    def test_unschematized_legacy_ledger_is_retained_and_does_not_block_gc(
        self,
    ) -> None:
        store = MemoryStore()
        store.add_release("v1.0.0", 500, legacy=True)

        plan = gc.plan_gc(store, now=NOW)

        self.assertEqual(plan["retained"]["v1.0.0"], "stable")

    def test_missing_manifest_retains_prefix_and_blocks_shared_sweep(self) -> None:
        store = MemoryStore()
        ledger = store.add_release("v0.0.0-dev.1", 100)
        unknown = "antfly/v0.0.0-dev22/antfly.tar.gz"
        store.put(unknown, b"legacy artifact")

        # Add a newer nightly so the old, complete release can expire.
        store.add_release("v0.0.0-dev.2", 1)
        plan = gc.plan_gc(store, now=NOW, nightly_min_count=1)

        self.assertEqual(plan["retained"]["v0.0.0-dev22"], "missing-artifact-manifest")
        self.assertIn("v0.0.0-dev.1", plan["expired"])
        self.assertNotIn(unknown, plan["delete_keys"])
        self.assertNotIn("antfly/v0.0.0-dev.1/artifacts.json", plan["delete_keys"])
        self.assertIn("antfly/v0.0.0-dev.1/metadata.json", plan["delete_keys"])
        self.assertFalse(
            any(k.startswith(gc.CONTENT_ROOT) for k in plan["delete_keys"])
        )
        self.assertNotIn(
            f"{gc.CONTAINER_IDENTITY_ROOT}{ledger}.json", plan["delete_keys"]
        )
        self.assertEqual(plan["container_deletions"], [])

    def test_deferred_shared_cleanup_resumes_after_manifest_repair(self) -> None:
        for cleanup in (False, True):
            with self.subTest(cleanup=cleanup):
                store = MemoryStore()
                tag = "v0.0.0-dev.1"
                version_ledger = f"antfly/{tag}/artifacts.json"
                ledger = store.add_release(tag, 100)
                shared_ledger = f"{gc.CONTENT_ROOT}{ledger}/artifacts.json"
                store.put(shared_ledger, store.stored[version_ledger].body)
                record_key = f"{gc.CONTAINER_IDENTITY_ROOT}{ledger}.json"
                releases, _, _, _ = gc.load_releases(
                    store, store.list_objects("antfly/")
                )
                content_keys = releases[tag].content_keys
                unknown = "antfly/v9.0.0/antfly.tar.gz"
                store.put(unknown, b"legacy stable")
                store.add_release("v0.0.0-dev.2", 1)

                def plan():
                    return gc.plan_gc(
                        store,
                        now=NOW,
                        nightly_min_count=1,
                        delete_dev_releases=cleanup,
                    )

                def apply(deletion):
                    gc.apply_gc_plan(store, deletion)

                blocked = plan()
                self.assertIn(tag, blocked["expired"])
                self.assertNotIn(version_ledger, blocked["delete_keys"])
                self.assertIn(f"antfly/{tag}/metadata.json", blocked["delete_keys"])
                self.assertFalse(content_keys & set(blocked["delete_keys"]))
                self.assertEqual(blocked["container_deletions"], [])
                apply(blocked)
                # Repeated blocked sweeps must not lose the remaining manifest.
                blocked_again = plan()
                self.assertIn(tag, blocked_again["expired"])
                apply(blocked_again)
                self.assertIn(version_ledger, store.objects)
                self.assertIn(record_key, store.objects)

                # Repair the legacy stable release, then finish the deferred
                # artifact and image cleanup through the next normal plan.
                store.add_release("v9.0.0", 100)
                resumed = gc.plan_gc(store, now=NOW, nightly_min_count=1)
                self.assertEqual(resumed["policy"]["shared_sweep"], "enabled")
                self.assertIn(version_ledger, resumed["delete_keys"])
                self.assertTrue(content_keys <= set(resumed["delete_keys"]))
                self.assertIn(record_key, resumed["container_record_deletions"])
                self.assertIn(
                    store.container_digests[tag],
                    {
                        item["container_digest"]
                        for item in resumed["container_deletions"]
                    },
                )
                apply(resumed)
                self.assertFalse(content_keys & store.objects.keys())
                self.assertNotIn(version_ledger, store.objects)
                self.assertNotIn(record_key, store.objects)
                self.assertIn(unknown, store.objects)

    def pending_legacy_cleanup(self):
        store = MemoryStore()
        tag = "v0.0.0-dev22"
        ledger = store.add_release(tag, 100)
        store.put("antfly/v9.0.0/antfly.tar.gz", b"legacy stable")
        plan = gc.plan_gc(store, now=NOW, delete_dev_releases=True)
        return store, tag, ledger, plan

    def test_legacy_dev_cleanup_resumes_in_scheduled_gc(self) -> None:
        store, tag, ledger, blocked = self.pending_legacy_cleanup()
        marker = f"{gc.PENDING_ROOT}{ledger}.json"
        self.assertNotIn(marker, store.objects)  # Planning never writes intent.
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "plan.json"
            path.write_text(json.dumps(blocked))
            self.assertEqual(gc.load_plan(path), blocked)
        gc.apply_gc_plan(store, blocked)
        self.assertIn(marker, store.objects)
        self.assertNotIn(f"antfly/{tag}/metadata.json", store.objects)
        again = gc.plan_gc(store, now=NOW)
        self.assertEqual(again["expired"][tag], "explicit-dev-cleanup")
        self.assertNotIn(marker, again["delete_keys"])
        store.add_release("v9.0.0", 100)
        resumed = gc.plan_gc(store, now=NOW)
        self.assertEqual(resumed["expired"][tag], "explicit-dev-cleanup")
        self.assertIn(marker, resumed["delete_keys"])
        self.assertIn(f"antfly/{tag}/artifacts.json", resumed["delete_keys"])
        self.assertEqual(resumed["container_deletions"][0]["ledger_sha256"], ledger)
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "plan.json"
            path.write_text(json.dumps(resumed))
            self.assertEqual(gc.load_plan(path), resumed)
        gc.apply_gc_plan(store, resumed)
        self.assertNotIn(marker, store.objects)
        self.assertNotIn(tag, gc.plan_gc(store, now=NOW)["expired"])

    def test_pending_cleanup_still_honors_channel_protection(self) -> None:
        store, tag, ledger, blocked = self.pending_legacy_cleanup()
        gc.apply_gc_plan(store, blocked)
        store.add_journal("nightly", current=(tag, ledger))
        protected = gc.plan_gc(store, now=NOW)
        self.assertEqual(protected["retained"][tag], "channel-current-or-pending")
        self.assertNotIn(f"{gc.PENDING_ROOT}{ledger}.json", protected["delete_keys"])
        self.assertNotIn(f"antfly/{tag}/artifacts.json", protected["delete_keys"])

    def nightly_pending_cleanup(self):
        store = MemoryStore()
        oldest = store.add_release("v0.0.0-dev.1", 100)
        store.add_release("v0.0.0-dev.9", 100)
        pending = store.add_release("v0.0.0-dev.10", 100)
        store.add_journal("nightly", current=("v0.0.0-dev.1", oldest))
        store.put("antfly/v9.0.0/antfly.tar.gz", b"legacy stable")
        gc.apply_gc_plan(store, gc.plan_gc(store, now=NOW, delete_dev_releases=True))
        newest = store.add_release("v0.0.0-dev.11", 0)
        store.add_journal("nightly", current=("v0.0.0-dev.11", newest))
        store.add_release("v9.0.0", 100)
        return store, pending

    def test_pending_expirations_do_not_consume_nightly_retention_slots(self) -> None:
        store, _ = self.nightly_pending_cleanup()
        plan = gc.plan_gc(store, now=NOW, nightly_min_count=3)
        self.assertEqual(plan["retained"]["v0.0.0-dev.1"], "newest-nightly-count")
        self.assertEqual(
            plan["retained"]["v0.0.0-dev.11"], "channel-current-or-pending"
        )
        self.assertEqual(set(plan["expired"]), {"v0.0.0-dev.9", "v0.0.0-dev.10"})
        self.assertNotIn("antfly/v0.0.0-dev.1/artifacts.json", plan["delete_keys"])

    def test_channel_protected_pending_release_still_counts_as_retained(self) -> None:
        store, pending = self.nightly_pending_cleanup()
        newest = gc.load_releases(store, store.list_objects("antfly/"))[0][
            "v0.0.0-dev.11"
        ].ledger_sha256
        store.add_journal(
            "nightly",
            current=("v0.0.0-dev.11", newest),
            pending=("v0.0.0-dev.10", pending),
        )
        plan = gc.plan_gc(store, now=NOW, nightly_min_count=2)
        self.assertEqual(
            plan["retained"]["v0.0.0-dev.10"], "channel-current-or-pending"
        )
        self.assertEqual(set(plan["expired"]), {"v0.0.0-dev.1", "v0.0.0-dev.9"})
        self.assertNotIn("antfly/v0.0.0-dev.10/artifacts.json", plan["delete_keys"])

    def test_pending_cleanup_rejects_changed_release_identity(self) -> None:
        store, tag, _, blocked = self.pending_legacy_cleanup()
        gc.apply_gc_plan(store, blocked)
        store.add_release(tag, 100, artifacts={"different.bin": "f" * 64})
        with self.assertRaisesRegex(SystemExit, "identity changed"):
            gc.plan_gc(store, now=NOW)

    def test_unblocked_cleanup_resumes_after_interrupted_deletion(self) -> None:
        for explicit in (False, True):
            for failed_phase in ("payload", "manifest", "marker"):
                with self.subTest(explicit=explicit, failed_phase=failed_phase):
                    store = MemoryStore()
                    tag = "v0.0.0-dev.1"
                    ledger = store.add_release(tag, 100)
                    newest = store.add_release("v0.0.0-dev.11", 0)
                    store.add_journal("nightly", current=("v0.0.0-dev.11", newest))
                    manifest = f"antfly/{tag}/artifacts.json"
                    marker = f"{gc.PENDING_ROOT}{ledger}.json"
                    identity = f"{gc.CONTAINER_IDENTITY_ROOT}{ledger}.json"
                    plan = gc.plan_gc(
                        store,
                        now=NOW,
                        nightly_min_count=1,
                        delete_dev_releases=explicit,
                    )
                    self.assertEqual(plan["policy"]["shared_sweep"], "enabled")
                    with tempfile.TemporaryDirectory() as raw:
                        path = Path(raw) / "plan.json"
                        path.write_text(json.dumps(plan))
                        self.assertEqual(gc.load_plan(path), plan)

                    class Client:
                        def delete_objects(self, *, Bucket, Delete):
                            keys = [item["Key"] for item in Delete["Objects"]]
                            phase = (
                                "marker"
                                if marker in keys
                                else "manifest"
                                if manifest in keys
                                else "payload"
                            )
                            if phase == failed_phase:
                                # Multi-object deletion may partially succeed.
                                store.delete_objects(keys[1:])
                                return {
                                    "Errors": [
                                        {"Key": keys[0], "Code": "InternalError"}
                                    ]
                                }
                            store.delete_objects(keys)
                            return {}

                    remote = object.__new__(gc.S3ObjectStore)
                    remote.client, remote.bucket = Client(), "releases"

                    # Persist markers in memory, using real S3 deletion phases.
                    class ApplyingStore:
                        def write_if_absent(self, key, body):
                            store.write_if_absent(key, body)

                        def delete_objects(self, keys):
                            remote.delete_objects(keys)

                    with self.assertRaisesRegex(SystemExit, "deletion failed"):
                        gc.apply_gc_plan(ApplyingStore(), plan)
                    self.assertIn(marker, store.objects)
                    if failed_phase == "manifest":
                        self.assertIn(manifest, store.objects)
                        self.assertNotIn(identity, store.objects)
                    retry = gc.plan_gc(store, now=NOW, nightly_min_count=10)
                    if failed_phase == "marker":
                        self.assertEqual(retry["delete_keys"], [marker])
                    else:
                        self.assertEqual(retry["expired"][tag], plan["expired"][tag])
                        self.assertIn(manifest, retry["delete_keys"])
                    gc.apply_gc_plan(store, retry)
                    self.assertNotIn(marker, store.objects)
                    self.assertNotIn(manifest, store.objects)
                    self.assertNotIn(identity, store.objects)
                    self.assertEqual(gc.plan_gc(store, now=NOW)["expired"], {})

    def test_pending_write_failure_prevents_all_prefix_deletion(self) -> None:
        store, tag, _, blocked = self.pending_legacy_cleanup()

        def fail(*args):
            raise OSError("write failed")

        store.write_if_absent = fail
        with self.assertRaisesRegex(OSError, "write failed"):
            gc.apply_gc_plan(store, blocked)
        self.assertIn(f"antfly/{tag}/metadata.json", store.objects)
        self.assertEqual(store.deleted, [])

    def test_completed_pending_marker_can_be_collected_after_interruption(self) -> None:
        store, _, ledger, blocked = self.pending_legacy_cleanup()
        gc.apply_gc_plan(store, blocked)
        store.add_release("v9.0.0", 100)
        final = gc.plan_gc(store, now=NOW)
        marker = f"{gc.PENDING_ROOT}{ledger}.json"
        # Model failure in the last deletion phase, after the version ledger.
        store.delete_objects([key for key in final["delete_keys"] if key != marker])
        retry = gc.plan_gc(store, now=NOW)
        self.assertEqual(retry["delete_keys"], [marker])

    def test_pending_writes_are_covered_by_approval_contract(self) -> None:
        _, _, _, plan = self.pending_legacy_cleanup()
        changed = {**plan, "pending_writes": {}}
        with self.assertRaisesRegex(SystemExit, "changed after approval"):
            gc.verify_approved_plan(plan, changed)

    def test_invalid_pending_cleanup_cannot_expire_a_stable_release(self) -> None:
        store, _, ledger, blocked = self.pending_legacy_cleanup()
        key = f"{gc.PENDING_ROOT}{ledger}.json"
        record = {**blocked["pending_writes"][key], "tag": "v1.0.0"}
        store.put(key, json.dumps(record).encode())
        with self.assertRaisesRegex(SystemExit, "invalid pending release cleanup"):
            gc.plan_gc(store, now=NOW)

    def test_r2_pending_write_is_conditional_and_checks_conflicts(self) -> None:
        requests = []

        class Conflict(Exception):
            response = {"Error": {"Code": "PreconditionFailed"}}

        class Client:
            def put_object(self, **request):
                requests.append(request)
                if len(requests) > 1:
                    raise Conflict()

        store = object.__new__(gc.S3ObjectStore)
        store.bucket = "releases"
        store.client = Client()
        store.client_error = Conflict
        key = f"{gc.PENDING_ROOT}{'a' * 64}.json"
        body = b"pending intent"
        store.read_optional = lambda _: gc.StoredObject(body, "etag")
        store.write_if_absent(key, body)
        self.assertEqual(requests[0]["IfNoneMatch"], "*")
        self.assertEqual(requests[0]["Key"], key)
        store.write_if_absent(key, body)  # An identical retry is idempotent.
        store.read_optional = lambda _: gc.StoredObject(b"other intent", "etag")
        with self.assertRaisesRegex(SystemExit, "conflicting pending release cleanup"):
            store.write_if_absent(key, body)

    def test_dev_cleanup_is_explicit_and_preserves_other_releases(self) -> None:
        store = MemoryStore()
        store.add_release("v0.0.0-dev.1", 100)
        store.add_release("v0.0.0-dev.2", 1)
        store.add_release("v1.0.0-rc.1", 100)
        store.add_release("v1.0.0", 100)
        legacy_key = "antfly/v0.0.0-dev22/antfly.tar.gz"
        store.put(legacy_key, b"legacy artifact")
        plan = gc.plan_gc(store, now=NOW, delete_dev_releases=True)

        self.assertEqual(
            set(plan["expired"]), {"v0.0.0-dev.1", "v0.0.0-dev.2", "v0.0.0-dev22"}
        )
        self.assertIn(legacy_key, plan["delete_keys"])
        self.assertEqual(set(plan["retained"]), {"v1.0.0", "v1.0.0-rc.1"})
        self.assertFalse(
            any(k.startswith("antfly/v1.0.0") for k in plan["delete_keys"])
        )
        self.assertTrue(any(k.startswith(gc.CONTENT_ROOT) for k in plan["delete_keys"]))

    def test_dev_cleanup_preserves_current_pending_and_shared_content(self) -> None:
        store = MemoryStore()
        artifact = {"antfly.tar.gz": "f" * 64}
        current = store.add_release("v0.0.0-dev.1", 100, artifacts=artifact)
        pending = store.add_release("v0.0.0-dev.2", 100)
        store.add_release("v0.0.0-dev3", 100, artifacts=artifact)
        store.add_journal(
            "nightly",
            current=("v0.0.0-dev.1", current),
            pending=("v0.0.0-dev.2", pending),
        )

        plan = gc.plan_gc(store, now=NOW, delete_dev_releases=True)

        self.assertEqual(set(plan["expired"]), {"v0.0.0-dev3"})
        self.assertEqual(plan["retained"]["v0.0.0-dev.1"], "channel-current-or-pending")
        self.assertEqual(plan["retained"]["v0.0.0-dev.2"], "channel-current-or-pending")
        self.assertNotIn(
            f"{gc.CONTENT_ROOT}{'f' * 64}/antfly.tar.gz", plan["delete_keys"]
        )

    def test_dev_cleanup_with_unknown_stable_preserves_shared_objects(self) -> None:
        store = MemoryStore()
        store.add_release("v0.0.0-dev.1", 100)
        store.put("antfly/v1.0.0/antfly.tar.gz", b"legacy stable")
        plan = gc.plan_gc(store, now=NOW, delete_dev_releases=True)
        self.assertEqual(plan["retained"]["v1.0.0"], "missing-artifact-manifest")
        self.assertFalse(
            any(k.startswith(gc.CONTENT_ROOT) for k in plan["delete_keys"])
        )
        self.assertEqual(plan["container_deletions"], [])

    def test_missing_protected_manifest_still_fails_closed(self) -> None:
        store = MemoryStore()
        store.put("antfly/v0.0.0-dev.1/metadata.json", b"{}")
        store.add_journal("nightly", current=("v0.0.0-dev.1", "a" * 64))
        for cleanup in (False, True):
            with (
                self.subTest(cleanup=cleanup),
                self.assertRaisesRegex(SystemExit, "missing its immutable release"),
            ):
                gc.plan_gc(store, now=NOW, delete_dev_releases=cleanup)

    def test_legacy_alias_protects_manifest_free_dev_release(self) -> None:
        store = MemoryStore()
        key = "antfly/v0.0.0-dev22/antfly.tar.gz"
        store.put(key, b"legacy artifact")
        alias = gc.load_policy()["channels"]["nightly"]["object_alias"]
        store.put(f"antfly/{alias}/metadata.json", b'{"tag":"v0.0.0-dev22"}')
        for cleanup in (False, True):
            with self.subTest(cleanup=cleanup):
                plan = gc.plan_gc(store, now=NOW, delete_dev_releases=cleanup)
                self.assertEqual(
                    plan["retained"]["v0.0.0-dev22"], "channel-current-or-pending"
                )
                self.assertNotIn(key, plan["delete_keys"])

    def test_dev_cleanup_does_not_ignore_malformed_existing_manifest(self) -> None:
        store = MemoryStore()
        store.put("antfly/v0.0.0-dev22/artifacts.json", b"not JSON")
        with self.assertRaisesRegex(SystemExit, "not valid JSON"):
            gc.plan_gc(store, now=NOW, delete_dev_releases=True)

    def test_stable_completion_must_match_immutable_release_state(self) -> None:
        store = MemoryStore()
        stable = store.add_release("v1.0.0", 500)
        store.add_completion("v1.0.0", stable, 500)
        key = f"{gc.COMPLETION_ROOT}{stable}.json"
        receipt = json.loads(store.stored[key].body)
        receipt["container_digest"] = f"sha256:{'f' * 64}"
        store.put(key, json.dumps(receipt).encode())

        with self.assertRaisesRegex(
            SystemExit, "disagrees with immutable release state"
        ):
            gc.plan_gc(store, now=NOW)

    def test_apply_guard_detects_a_changed_channel_snapshot(self) -> None:
        store = MemoryStore()
        ledger = store.add_release("v0.0.0-dev.1", 100)
        key = store.add_journal("nightly", current=("v0.0.0-dev.1", ledger))
        plan = gc.plan_gc(store, now=NOW)
        store.put(key, store.stored[key].body + b"\n")

        with self.assertRaisesRegex(SystemExit, "changed while planning"):
            gc.verify_snapshots(store, plan["snapshots"])

    def test_saved_plan_cannot_target_mutable_control_plane_keys(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "plan.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 2,
                        "delete_keys": ["antfly/channels/stable.json"],
                        "container_deletions": [],
                        "container_record_deletions": [],
                        "pending_writes": {},
                        "snapshots": {},
                        "approval_sha256": "0" * 64,
                    }
                )
            )
            with self.assertRaisesRegex(SystemExit, "mutable namespace"):
                gc.load_plan(path)

    def test_fresh_plan_must_match_the_approved_deletion_contract(self) -> None:
        store = MemoryStore()
        store.add_release("v0.0.0-dev.1", 100)
        store.add_release("v0.0.0-dev.2", 1)
        approved = gc.plan_gc(store, now=NOW, nightly_days=30, nightly_min_count=1)
        fresh = gc.plan_gc(store, now=NOW, nightly_days=30, nightly_min_count=1)

        fresh["planned_at"] = "2099-01-01T00:00:00Z"
        fresh["snapshots"] = {"antfly/channels/nightly.json": '"new-etag"'}
        gc.verify_approved_plan(approved, fresh)
        fresh["delete_keys"] = []

        with self.assertRaisesRegex(SystemExit, "changed after approval"):
            gc.verify_approved_plan(approved, fresh)

    def test_saved_plan_rejects_a_tampered_approval_contract(self) -> None:
        store = MemoryStore()
        store.add_release("v0.0.0-dev.1", 100)
        store.add_release("v0.0.0-dev.2", 1)
        plan = gc.plan_gc(store, now=NOW, nightly_days=30, nightly_min_count=1)
        plan["retained"] = {}

        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "plan.json"
            path.write_text(json.dumps(plan))
            with self.assertRaisesRegex(SystemExit, "approval digest"):
                gc.load_plan(path)

    def test_saved_plan_validates_container_deletion_identity(self) -> None:
        store = MemoryStore()
        store.add_release("v0.0.0-dev.1", 100)
        store.add_release("v0.0.0-dev.2", 1)
        plan = gc.plan_gc(store, now=NOW, nightly_days=30, nightly_min_count=1)
        plan["container_deletions"][0]["container_digest"] = "not-a-digest"
        plan["approval_sha256"] = gc.approval_sha256(plan)

        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "plan.json"
            path.write_text(json.dumps(plan))
            with self.assertRaisesRegex(SystemExit, "malformed container deletion"):
                gc.load_plan(path)

    def test_r2_deletion_removes_payloads_then_ledgers_then_pending_markers(
        self,
    ) -> None:
        calls = []

        class Client:
            def delete_objects(self, **request):
                calls.append([item["Key"] for item in request["Delete"]["Objects"]])
                return {}

        store = object.__new__(gc.S3ObjectStore)
        store.bucket = "releases"
        store.client = Client()
        store.delete_objects(
            [
                f"{gc.PENDING_ROOT}{'a' * 64}.json",
                "antfly/v1.2.3/artifacts.json",
                "antfly/v1.2.3/metadata.json",
                f"{gc.CONTENT_ROOT}{'1' * 64}/artifact.bin",
            ]
        )

        self.assertEqual(calls[-1], [f"{gc.PENDING_ROOT}{'a' * 64}.json"])
        self.assertEqual(calls[-2], ["antfly/v1.2.3/artifacts.json"])
        self.assertNotIn("antfly/v1.2.3/artifacts.json", calls[0])


class RegistryGCTests(unittest.TestCase):
    def deletion(self) -> dict[str, str]:
        return {
            "tag": "v0.0.0-dev.1",
            "ledger_sha256": "1" * 64,
            "container_digest": f"sha256:{'2' * 64}",
            "record_key": f"antfly/container-identities/{'1' * 64}.json",
        }

    def runner(
        self,
        digests: dict[str, str],
        versions: list[dict] | None = None,
    ):
        calls: list[tuple[str, ...]] = []

        def run(args, **_kwargs):
            args = tuple(args)
            calls.append(args)
            if args[:2] == ("crane", "ls"):
                repository = args[2]
                tags = sorted(
                    ref.removeprefix(repository + ":")
                    for ref in digests
                    if ref.startswith(repository + ":")
                )
                return subprocess.CompletedProcess(args, 0, "\n".join(tags), "")
            if args[:2] == ("crane", "digest"):
                ref = args[2]
                if ref in digests:
                    return subprocess.CompletedProcess(args, 0, digests[ref] + "\n", "")
                return subprocess.CompletedProcess(args, 1, "", "manifest unknown")
            if args[:3] == ("gh", "api", "--paginate"):
                return subprocess.CompletedProcess(
                    args, 0, json.dumps([versions or []]), ""
                )
            return subprocess.CompletedProcess(args, 0, "", "")

        return run, calls

    def test_registry_cleanup_removes_unreferenced_gar_and_ghcr_images(self) -> None:
        deletion = self.deletion()
        digest = deletion["container_digest"]
        ledger_tag = f"release-ledger-{deletion['ledger_sha256']}"
        gar = "region.pkg.dev/project/repository/antfly"
        ghcr = "ghcr.io/antflydb/antfly"
        amd64 = f"sha256:{'3' * 64}"
        arm64 = f"sha256:{'4' * 64}"
        digests = {
            f"{gar}:{deletion['tag']}": digest,
            f"{gar}:{ledger_tag}": digest,
            f"{gar}:{deletion['tag']}-amd64": amd64,
            f"{gar}:{deletion['tag']}-arm64": arm64,
            f"{gar}@{digest}": digest,
            f"{gar}@{amd64}": amd64,
            f"{gar}@{arm64}": arm64,
            f"{ghcr}:{deletion['tag']}": digest,
            f"{ghcr}:{ledger_tag}": digest,
        }
        versions = [
            {
                "id": 7,
                "name": digest,
                "metadata": {"container": {"tags": [deletion["tag"], ledger_tag]}},
            },
            {"id": 8, "name": amd64, "metadata": {"container": {"tags": []}}},
            {"id": 9, "name": arm64, "metadata": {"container": {"tags": []}}},
        ]
        runner, calls = self.runner(digests, versions)

        registry_gc.apply_registry_gc([deletion], gar, "antflydb/antfly", runner=runner)

        self.assertEqual(
            len(
                [
                    call
                    for call in calls
                    if call[:5] == ("gcloud", "artifacts", "docker", "images", "delete")
                ]
            ),
            3,
        )
        self.assertIn(
            (
                "gh",
                "api",
                "--method",
                "DELETE",
                "/orgs/antflydb/packages/container/antfly/versions/7",
            ),
            calls,
        )
        self.assertEqual(
            len([call for call in calls if call[:3] == ("gh", "api", "--method")]),
            3,
        )

    def test_registry_cleanup_refuses_a_digest_selected_by_a_channel(self) -> None:
        deletion = self.deletion()
        gar = "region.pkg.dev/project/repository/antfly"
        runner, _calls = self.runner({f"{gar}:nightly": deletion["container_digest"]})

        with self.assertRaisesRegex(SystemExit, "still selected"):
            registry_gc.apply_registry_gc(
                [deletion], gar, "antflydb/antfly", runner=runner
            )

    def test_registry_cleanup_refuses_unplanned_ghcr_tags(self) -> None:
        deletion = self.deletion()
        digest = deletion["container_digest"]
        ledger_tag = f"release-ledger-{deletion['ledger_sha256']}"
        gar = "region.pkg.dev/project/repository/antfly"
        ghcr = "ghcr.io/antflydb/antfly"
        digests = {
            f"{gar}:{deletion['tag']}": digest,
            f"{gar}:{ledger_tag}": digest,
            f"{ghcr}:{deletion['tag']}": digest,
            f"{ghcr}:{ledger_tag}": digest,
        }
        versions = [
            {
                "id": 7,
                "name": digest,
                "metadata": {
                    "container": {"tags": [deletion["tag"], ledger_tag, "keep-me"]}
                },
            }
        ]
        runner, _calls = self.runner(digests, versions)

        with self.assertRaisesRegex(SystemExit, "unexpired tags"):
            registry_gc.apply_registry_gc(
                [deletion], gar, "antflydb/antfly", runner=runner
            )

    def test_registry_cleanup_refuses_unplanned_gar_tags(self) -> None:
        deletion = self.deletion()
        digest = deletion["container_digest"]
        ledger_tag = f"release-ledger-{deletion['ledger_sha256']}"
        gar = "region.pkg.dev/project/repository/antfly"
        ghcr = "ghcr.io/antflydb/antfly"
        digests = {
            f"{gar}:{deletion['tag']}": digest,
            f"{gar}:{ledger_tag}": digest,
            f"{gar}:keep-me": digest,
            f"{ghcr}:{deletion['tag']}": digest,
            f"{ghcr}:{ledger_tag}": digest,
        }
        versions = [
            {
                "id": 7,
                "name": digest,
                "metadata": {"container": {"tags": [deletion["tag"], ledger_tag]}},
            }
        ]
        runner, _calls = self.runner(digests, versions)

        with self.assertRaisesRegex(SystemExit, "unexpired tag"):
            registry_gc.apply_registry_gc(
                [deletion], gar, "antflydb/antfly", runner=runner
            )


if __name__ == "__main__":
    unittest.main()
