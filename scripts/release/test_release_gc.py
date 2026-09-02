#!/usr/bin/env python3
"""Tests for journal-aware release object retention."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
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


class MemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, datetime] = {}
        self.stored: dict[str, gc.StoredObject] = {}
        self.deleted: list[str] = []

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
    ) -> str:
        artifacts = artifacts or {
            "antfly.tar.gz": hashlib.sha256(tag.encode()).hexdigest()
        }
        ledger = {
            "schema_version": 4,
            "tag": tag,
            "artifacts": [
                {"name": name, "sha256": digest} for name, digest in artifacts.items()
            ],
        }
        ledger_body = (json.dumps(ledger, sort_keys=True) + "\n").encode()
        modified = NOW - timedelta(days=age_days)
        prefix = f"antfly/{tag}/"
        self.put(prefix + "artifacts.json", ledger_body, modified)
        self.put(prefix + "metadata.json", b"{}\n", modified)
        for name, digest in artifacts.items():
            self.put(f"{gc.CONTENT_ROOT}{digest}/{name}", b"artifact", modified)
        ledger_digest = hashlib.sha256(ledger_body).hexdigest()
        self.put(
            f"{gc.CONTAINER_IDENTITY_ROOT}{ledger_digest}.json",
            b"{}\n",
            modified,
        )
        return ledger_digest

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

    def delete_objects(self, keys: list[str]) -> None:
        self.deleted.extend(keys)


class ReleaseGCTests(unittest.TestCase):
    def test_stable_and_shared_content_survive_expired_prereleases(self) -> None:
        store = MemoryStore()
        shared = "1" * 64
        nightly_only = "2" * 64
        rc_only = "3" * 64
        store.add_release("v1.2.3", 200, {"shared.bin": shared})
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
            plan["delete_keys"],
        )

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
        store.add_release("v3.0.0", 20)
        store.add_release("v4.0.0-rc.1", 300)
        store.add_release("v4.0.0", 100)

        plan = gc.plan_gc(store, now=NOW, prerelease_grace_days=90)

        self.assertEqual(plan["retained"]["v2.0.0-rc.1"], "awaiting-matching-stable")
        self.assertEqual(
            plan["retained"]["v3.0.0-rc.1"], "matching-stable-grace-window"
        )
        self.assertEqual(plan["expired"]["v4.0.0-rc.1"], "prerelease-grace-expired")

    def test_malformed_state_fails_closed(self) -> None:
        store = MemoryStore()
        store.add_release("v0.0.0-dev.1", 100)
        store.put(
            "antfly/channels/nightly.json",
            b'{"schema_version":1,"channel":"nightly","current":"bad"}',
        )

        with self.assertRaisesRegex(SystemExit, "malformed current identity"):
            gc.plan_gc(store, now=NOW)

    def test_apply_guard_detects_a_changed_channel_snapshot(self) -> None:
        store = MemoryStore()
        ledger = store.add_release("v0.0.0-dev.1", 100)
        key = store.add_journal("nightly", current=("v0.0.0-dev.1", ledger))
        plan = gc.plan_gc(store, now=NOW)
        store.put(key, store.stored[key].body + b"\n")

        with self.assertRaisesRegex(SystemExit, "changed while planning"):
            gc.verify_snapshots(store, plan["snapshots"])


if __name__ == "__main__":
    unittest.main()
