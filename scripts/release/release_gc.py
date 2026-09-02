#!/usr/bin/env python3
"""Plan or apply journal-aware retention for Antfly release objects.

Stable releases are retained permanently. Nightly snapshots are retained while
they are younger than the configured age or among the newest configured count.
Other prereleases are retained until their matching stable release has existed
for the configured grace period. Channel current and pending identities always
win over retention policy.

The default is a read-only plan. Pass --apply to delete the exact keys in the
freshly computed plan. Run this under the release-promotion concurrency group so
immutable uploads and channel journal updates cannot race the sweep.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

from release_channel_state import COMPLETION_ROOT, validate_completion_receipt
from release_channels import NIGHTLY_PATTERN, TAG_PATTERN, load_policy
from release_container_state import validate_record

DEFAULT_BUCKET = "antfly-releases"
RELEASE_ROOT = "antfly/"
CONTENT_ROOT = "antfly/artifacts/sha256/"
CONTAINER_IDENTITY_ROOT = "antfly/container-identities/"
LEDGER_NAME = "artifacts.json"
SUPPORTED_LEDGER_SCHEMAS = {1, 2, 3, 4}
SHA256 = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    modified: datetime


@dataclass(frozen=True)
class StoredObject:
    body: bytes
    etag: str


@dataclass(frozen=True)
class Release:
    tag: str
    commit: str
    published_at: datetime
    keys: frozenset[str]
    ledger_sha256: str
    content_keys: frozenset[str]


class ObjectStore(Protocol):
    def list_objects(self, prefix: str) -> list[ObjectInfo]: ...

    def read_optional(self, key: str) -> StoredObject | None: ...

    def delete_objects(self, keys: list[str]) -> None: ...


def utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_document(stored: StoredObject, description: str) -> dict[str, Any]:
    try:
        document = json.loads(stored.body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SystemExit(f"{description} is not valid JSON") from exc
    if not isinstance(document, dict):
        raise SystemExit(f"{description} must be a JSON object")
    return document


def load_protected_identities(
    store: ObjectStore, policy: dict[str, Any]
) -> tuple[set[str], set[str], dict[str, str | None]]:
    protected_tags: set[str] = set()
    protected_ledgers: set[str] = set()
    snapshots: dict[str, str | None] = {}

    for channel_name, channel in policy["channels"].items():
        journal_key = str(channel["journal_key"])
        stored = store.read_optional(journal_key)
        snapshots[journal_key] = stored.etag if stored else None
        if stored:
            document = parse_document(stored, f"release journal {journal_key}")
            if document.get("schema_version") != 1 or document.get("channel") not in {
                None,
                channel_name,
            }:
                raise SystemExit(
                    f"release journal {journal_key} has an invalid contract"
                )
            for field in ("current", "pending"):
                identity = document.get(field)
                if identity is None:
                    continue
                if not isinstance(identity, dict) or not isinstance(
                    identity.get("tag"), str
                ):
                    raise SystemExit(
                        f"release journal {journal_key} has malformed {field} identity"
                    )
                tag = str(identity["tag"])
                if TAG_PATTERN.fullmatch(tag) is None:
                    raise SystemExit(
                        f"release journal {journal_key} has invalid {field} tag"
                    )
                protected_tags.add(tag)
                ledger = identity.get("ledger_sha256")
                if ledger is not None:
                    if not isinstance(ledger, str) or SHA256.fullmatch(ledger) is None:
                        raise SystemExit(
                            f"release journal {journal_key} has invalid ledger digest"
                        )
                    protected_ledgers.add(ledger)

        alias_key = f"antfly/{channel['object_alias']}/metadata.json"
        alias = store.read_optional(alias_key)
        snapshots[alias_key] = alias.etag if alias else None
        if alias:
            document = parse_document(alias, f"release alias {alias_key}")
            tag = document.get("tag")
            if not isinstance(tag, str) or TAG_PATTERN.fullmatch(tag) is None:
                raise SystemExit(f"release alias {alias_key} has an invalid tag")
            protected_tags.add(tag)

    return protected_tags, protected_ledgers, snapshots


def release_prefix(tag: str) -> str:
    return f"{RELEASE_ROOT}{tag}/"


def load_releases(
    store: ObjectStore, objects: list[ObjectInfo]
) -> tuple[dict[str, Release], set[str], set[str]]:
    by_key = {item.key: item for item in objects}
    release_keys: dict[str, set[str]] = {}
    for item in objects:
        remainder = item.key.removeprefix(RELEASE_ROOT)
        segment, separator, _name = remainder.partition("/")
        if separator and TAG_PATTERN.fullmatch(segment):
            release_keys.setdefault(segment, set()).add(item.key)

    releases: dict[str, Release] = {}
    all_content_keys = {
        item.key for item in objects if item.key.startswith(CONTENT_ROOT)
    }
    all_container_keys = {
        item.key for item in objects if item.key.startswith(CONTAINER_IDENTITY_ROOT)
    }
    for tag, keys in sorted(release_keys.items()):
        ledger_key = f"{release_prefix(tag)}{LEDGER_NAME}"
        ledger_info = by_key.get(ledger_key)
        if ledger_info is None:
            raise SystemExit(f"release {tag} has objects but no {LEDGER_NAME}")
        stored = store.read_optional(ledger_key)
        if stored is None:
            raise SystemExit(f"release ledger disappeared while planning: {ledger_key}")
        ledger = parse_document(stored, f"release ledger {ledger_key}")
        schema = ledger.get("schema_version")
        if (
            (schema is not None and schema not in SUPPORTED_LEDGER_SCHEMAS)
            or ledger.get("tag") != tag
            or not isinstance(ledger.get("artifacts"), list)
        ):
            raise SystemExit(f"release ledger {ledger_key} has an invalid contract")
        if schema is None and (
            not isinstance(ledger.get("version"), str)
            or not isinstance(ledger.get("commit"), str)
        ):
            raise SystemExit(
                f"legacy release ledger {ledger_key} has an invalid contract"
            )
        commit = ledger.get("commit")
        if not isinstance(commit, str) or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
            raise SystemExit(f"release ledger {ledger_key} has an invalid commit")
        content_keys: set[str] = set()
        names: set[str] = set()
        for artifact in ledger["artifacts"]:
            if not isinstance(artifact, dict):
                raise SystemExit(
                    f"release ledger {ledger_key} has a malformed artifact"
                )
            name = artifact.get("name")
            digest = artifact.get("sha256")
            if (
                not isinstance(name, str)
                or not name
                or Path(name).name != name
                or name in names
                or not isinstance(digest, str)
                or SHA256.fullmatch(digest) is None
            ):
                raise SystemExit(
                    f"release ledger {ledger_key} has a malformed artifact"
                )
            names.add(name)
            content_keys.add(f"{CONTENT_ROOT}{digest}/{name}")
        ledger_digest = hashlib.sha256(stored.body).hexdigest()
        content_keys.add(f"{CONTENT_ROOT}{ledger_digest}/{LEDGER_NAME}")
        releases[tag] = Release(
            tag=tag,
            commit=commit,
            published_at=utc(ledger_info.modified),
            keys=frozenset(keys),
            ledger_sha256=ledger_digest,
            content_keys=frozenset(content_keys),
        )
    return releases, all_content_keys, all_container_keys


def load_completion_history(
    store: ObjectStore,
    objects: list[ObjectInfo],
    releases: dict[str, Release],
    container_records: dict[str, dict[str, object]],
) -> dict[str, datetime]:
    stable_completed_at: dict[str, datetime] = {}
    for item in objects:
        if not item.key.startswith(COMPLETION_ROOT):
            continue
        suffix = item.key.removeprefix(COMPLETION_ROOT)
        if not re.fullmatch(r"[0-9a-f]{64}\.json", suffix):
            raise SystemExit(f"release history contains an invalid key: {item.key}")
        stored = store.read_optional(item.key)
        if stored is None:
            raise SystemExit(
                f"release completion disappeared while planning: {item.key}"
            )
        receipt = validate_completion_receipt(
            parse_document(stored, f"release completion {item.key}")
        )
        if suffix.removesuffix(".json") != receipt["ledger_sha256"]:
            raise SystemExit(
                f"release completion key disagrees with its receipt: {item.key}"
            )
        if receipt["channel"] != "stable":
            continue
        tag = str(receipt["tag"])
        release = releases.get(tag)
        record = container_records.get(tag)
        if (
            release is None
            or release.ledger_sha256 != receipt["ledger_sha256"]
            or release.commit != receipt["commit"]
            or record is None
            or record["container_digest"] != receipt["container_digest"]
        ):
            raise SystemExit(
                f"stable completion receipt disagrees with immutable release state: {tag}"
            )
        committed_at = datetime.fromisoformat(
            str(receipt["committed_at"]).replace("Z", "+00:00")
        )
        previous = stable_completed_at.get(tag)
        if previous is not None and previous != committed_at:
            raise SystemExit(
                f"stable release has conflicting completion receipts: {tag}"
            )
        stable_completed_at[tag] = committed_at
    return stable_completed_at


def load_container_records(
    store: ObjectStore,
    releases: dict[str, Release],
    all_container_keys: set[str],
) -> dict[str, dict[str, object]]:
    records: dict[str, dict[str, object]] = {}
    for release in releases.values():
        key = f"{CONTAINER_IDENTITY_ROOT}{release.ledger_sha256}.json"
        if key not in all_container_keys:
            continue
        stored = store.read_optional(key)
        if stored is None:
            raise SystemExit(f"container identity disappeared while planning: {key}")
        record = validate_record(parse_document(stored, f"container identity {key}"))
        if (
            record["tag"] != release.tag
            or record["ledger_sha256"] != release.ledger_sha256
        ):
            raise SystemExit(f"container identity disagrees with release ledger: {key}")
        records[release.tag] = record
    return records


def stable_tag_for(tag: str) -> str:
    match = TAG_PATTERN.fullmatch(tag)
    assert match is not None
    return f"v{match.group('major')}.{match.group('minor')}.{match.group('patch')}"


def plan_gc(
    store: ObjectStore,
    *,
    policy: dict[str, Any] | None = None,
    now: datetime | None = None,
    nightly_days: int | None = None,
    nightly_min_count: int | None = None,
    prerelease_grace_days: int | None = None,
) -> dict[str, Any]:
    policy = policy or load_policy()
    nightly_retention = policy["channels"]["nightly"]["retention"]
    prerelease_retention = policy["channels"]["next"]["retention"]
    nightly_days = (
        nightly_days if nightly_days is not None else nightly_retention["days"]
    )
    nightly_min_count = (
        nightly_min_count
        if nightly_min_count is not None
        else nightly_retention["minimum_count"]
    )
    prerelease_grace_days = (
        prerelease_grace_days
        if prerelease_grace_days is not None
        else prerelease_retention["grace_days"]
    )
    if nightly_days < 0 or nightly_min_count < 1 or prerelease_grace_days < 0:
        raise SystemExit(
            "release retention values must be non-negative and keep a nightly"
        )
    now = utc(now or datetime.now(timezone.utc))
    objects = store.list_objects(RELEASE_ROOT)
    releases, all_content_keys, all_container_keys = load_releases(store, objects)
    container_records = load_container_records(store, releases, all_container_keys)
    stable_completed_at = load_completion_history(
        store, objects, releases, container_records
    )
    protected_tags, protected_ledgers, snapshots = load_protected_identities(
        store, policy
    )
    missing_protected = sorted(protected_tags - releases.keys())
    if missing_protected:
        raise SystemExit(
            "protected channel release is missing its immutable ledger: "
            + ", ".join(missing_protected)
        )
    known_ledgers = {release.ledger_sha256 for release in releases.values()}
    missing_ledgers = sorted(protected_ledgers - known_ledgers)
    if missing_ledgers:
        raise SystemExit(
            "protected channel ledger is missing its immutable release: "
            + ", ".join(missing_ledgers)
        )

    nightly = sorted(
        (
            release
            for release in releases.values()
            if NIGHTLY_PATTERN.fullmatch(release.tag)
        ),
        key=lambda release: int(
            NIGHTLY_PATTERN.fullmatch(release.tag).group("sequence")
        ),
        reverse=True,
    )
    newest_nightly = {release.tag for release in nightly[:nightly_min_count]}
    nightly_cutoff = now - timedelta(days=nightly_days)
    prerelease_grace = timedelta(days=prerelease_grace_days)
    retained: dict[str, str] = {}
    expired: dict[str, str] = {}
    for tag, release in sorted(releases.items()):
        match = TAG_PATTERN.fullmatch(tag)
        assert match is not None
        if tag in protected_tags or release.ledger_sha256 in protected_ledgers:
            retained[tag] = "channel-current-or-pending"
        elif match.group("prerelease") is None:
            retained[tag] = "stable"
        elif NIGHTLY_PATTERN.fullmatch(tag):
            if tag in newest_nightly:
                retained[tag] = "newest-nightly-count"
            elif release.published_at >= nightly_cutoff:
                retained[tag] = "nightly-age-window"
            else:
                expired[tag] = "nightly-retention-expired"
        else:
            matching_stable_completed_at = stable_completed_at.get(stable_tag_for(tag))
            if matching_stable_completed_at is None:
                retained[tag] = "awaiting-matching-stable"
            elif now < matching_stable_completed_at + prerelease_grace:
                retained[tag] = "matching-stable-grace-window"
            else:
                expired[tag] = "prerelease-grace-expired"

    retained_content = (
        set().union(*(releases[tag].content_keys for tag in retained))
        if retained
        else set()
    )
    expired_content = (
        set().union(*(releases[tag].content_keys for tag in expired))
        if expired
        else set()
    )
    delete_keys = (
        set().union(*(releases[tag].keys for tag in expired)) if expired else set()
    )
    delete_keys.update((expired_content - retained_content) & all_content_keys)

    retained_container_digests = {
        str(container_records[tag]["container_digest"])
        for tag in retained
        if tag in container_records
    }
    container_deletions: list[dict[str, str]] = []
    for tag in expired:
        record = container_records.get(tag)
        if record is None or record["container_digest"] in retained_container_digests:
            continue
        container_deletions.append(
            {
                "tag": tag,
                "ledger_sha256": str(record["ledger_sha256"]),
                "container_digest": str(record["container_digest"]),
                "record_key": f"{CONTAINER_IDENTITY_ROOT}{record['ledger_sha256']}.json",
            }
        )

    return {
        "schema_version": 1,
        "planned_at": now.isoformat().replace("+00:00", "Z"),
        "policy": {
            "nightly_days": nightly_days,
            "nightly_min_count": nightly_min_count,
            "prerelease_grace_days": prerelease_grace_days,
            "stable": "forever",
        },
        "protected_tags": sorted(protected_tags),
        "retained": retained,
        "expired": expired,
        "container_deletions": sorted(
            container_deletions, key=lambda item: item["tag"]
        ),
        "delete_keys": sorted(delete_keys),
        "snapshots": snapshots,
    }


def verify_snapshots(store: ObjectStore, snapshots: dict[str, str | None]) -> None:
    for key, expected_etag in snapshots.items():
        current = store.read_optional(key)
        current_etag = current.etag if current else None
        if current_etag != expected_etag:
            raise SystemExit(f"release channel changed while planning: {key}; retry")


def load_plan(path: Path) -> dict[str, Any]:
    try:
        plan = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"cannot read release-GC plan: {path}") from exc
    if (
        not isinstance(plan, dict)
        or plan.get("schema_version") != 1
        or not isinstance(plan.get("delete_keys"), list)
        or not isinstance(plan.get("container_deletions"), list)
        or not isinstance(plan.get("snapshots"), dict)
    ):
        raise SystemExit("malformed release-GC plan")
    for key in plan["delete_keys"]:
        if not isinstance(key, str) or not key.startswith(RELEASE_ROOT):
            raise SystemExit("release-GC plan contains an invalid deletion key")
        remainder = key.removeprefix(RELEASE_ROOT)
        tag, separator, _name = remainder.partition("/")
        if not (
            key.startswith((CONTENT_ROOT, CONTAINER_IDENTITY_ROOT))
            or separator
            and TAG_PATTERN.fullmatch(tag)
        ):
            raise SystemExit("release-GC plan attempts to delete a mutable namespace")
    for item in plan["container_deletions"]:
        if not isinstance(item, dict) or set(item) != {
            "tag",
            "ledger_sha256",
            "container_digest",
            "record_key",
        }:
            raise SystemExit("release-GC plan contains a malformed container deletion")
        tag = item["tag"]
        ledger = item["ledger_sha256"]
        digest = item["container_digest"]
        if (
            not isinstance(tag, str)
            or TAG_PATTERN.fullmatch(tag) is None
            or not isinstance(ledger, str)
            or SHA256.fullmatch(ledger) is None
            or not isinstance(digest, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None
            or item["record_key"] != f"{CONTAINER_IDENTITY_ROOT}{ledger}.json"
        ):
            raise SystemExit("release-GC plan contains a malformed container deletion")
    if any(
        not isinstance(key, str) or etag is not None and not isinstance(etag, str)
        for key, etag in plan["snapshots"].items()
    ):
        raise SystemExit("release-GC plan contains invalid channel snapshots")
    return plan


class S3ObjectStore:
    def __init__(self, endpoint: str | None, bucket: str) -> None:
        try:
            import boto3
            from botocore.exceptions import ClientError
        except ImportError as exc:
            raise SystemExit(
                "boto3 is required; install scripts/release/requirements.lock"
            ) from exc
        self.bucket = bucket
        self.client_error = ClientError
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name="auto",
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
        )

    def list_objects(self, prefix: str) -> list[ObjectInfo]:
        result: list[ObjectInfo] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item.get("Key")
                modified = item.get("LastModified")
                if not isinstance(key, str) or not isinstance(modified, datetime):
                    raise SystemExit(
                        "object-storage listing returned malformed metadata"
                    )
                result.append(ObjectInfo(key, utc(modified)))
        return result

    def read_optional(self, key: str) -> StoredObject | None:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except self.client_error as exc:
            error = str(exc.response.get("Error", {}).get("Code"))
            if error in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        return StoredObject(response["Body"].read(), str(response["ETag"]))

    def delete_objects(self, keys: list[str]) -> None:
        release_ledgers = []
        other_keys = []
        for key in keys:
            remainder = key.removeprefix(RELEASE_ROOT)
            tag, separator, name = remainder.partition("/")
            if separator and TAG_PATTERN.fullmatch(tag) and name == LEDGER_NAME:
                release_ledgers.append(key)
            else:
                other_keys.append(key)
        # The version ledger is the release prefix's commit marker. Delete it
        # only after every other object so a failed batch remains discoverable
        # and a retry can finish the same plan safely.
        for phase in (other_keys, release_ledgers):
            for offset in range(0, len(phase), 1000):
                batch = phase[offset : offset + 1000]
                response = self.client.delete_objects(
                    Bucket=self.bucket,
                    Delete={
                        "Objects": [{"Key": key} for key in batch],
                        "Quiet": True,
                    },
                )
                errors = response.get("Errors", [])
                if errors:
                    raise SystemExit(f"object-storage deletion failed: {errors}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--nightly-days", type=int)
    parser.add_argument("--nightly-min-count", type=int)
    parser.add_argument("--prerelease-grace-days", type=int)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--apply-plan", type=Path)
    parser.add_argument("--containers-cleaned", action="store_true")
    parser.add_argument("--plan-out", type=Path)
    args = parser.parse_args()

    store = S3ObjectStore(args.endpoint, args.bucket)
    if args.apply and args.apply_plan:
        parser.error("--apply and --apply-plan are mutually exclusive")
    if args.apply_plan and any(
        value is not None
        for value in (
            args.nightly_days,
            args.nightly_min_count,
            args.prerelease_grace_days,
        )
    ):
        parser.error("retention overrides cannot be combined with --apply-plan")
    plan = (
        load_plan(args.apply_plan)
        if args.apply_plan
        else plan_gc(
            store,
            nightly_days=args.nightly_days,
            nightly_min_count=args.nightly_min_count,
            prerelease_grace_days=args.prerelease_grace_days,
        )
    )
    applying = args.apply or args.apply_plan is not None
    if applying:
        if plan["container_deletions"] and not args.containers_cleaned:
            raise SystemExit(
                "container cleanup is required before R2 deletion; pass --containers-cleaned after it succeeds"
            )
        if args.containers_cleaned:
            plan["delete_keys"].extend(
                item["record_key"] for item in plan["container_deletions"]
            )
            plan["delete_keys"] = sorted(set(plan["delete_keys"]))
        verify_snapshots(store, plan["snapshots"])
    elif args.containers_cleaned:
        parser.error("--containers-cleaned requires --apply or --apply-plan")
    rendered = json.dumps(plan, indent=2, sort_keys=True) + "\n"
    print(rendered, end="")
    if args.plan_out:
        args.plan_out.parent.mkdir(parents=True, exist_ok=True)
        args.plan_out.write_text(rendered, encoding="utf-8")
    if applying:
        store.delete_objects(plan["delete_keys"])
        print(f"deleted {len(plan['delete_keys'])} release objects")
    else:
        print(f"dry run: {len(plan['delete_keys'])} release objects would be deleted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
