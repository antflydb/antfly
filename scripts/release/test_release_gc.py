#!/usr/bin/env python3
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
            else "next" if "-" in tag else "stable"
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

    def delete_objects(self, keys: list[str]) -> None:
        self.deleted.extend(keys)


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
        plan["expired"] = {}

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

    def test_r2_deletion_removes_release_commit_markers_last(self) -> None:
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
                "antfly/v1.2.3/artifacts.json",
                "antfly/v1.2.3/metadata.json",
                f"{gc.CONTENT_ROOT}{'1' * 64}/artifact.bin",
            ]
        )

        self.assertEqual(calls[-1], ["antfly/v1.2.3/artifacts.json"])
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
