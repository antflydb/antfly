#!/usr/bin/env python3
"""Preflight, begin, or finish a compare-and-swap channel promotion."""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from typing import Any

from release_channels import (
    compare_channel_tags,
    load_policy,
    validate_channel_tag,
    validate_observed_channel_tag,
)


def release_identity(
    tag: str,
    commit: str,
    ledger_sha256: str,
    channel: str = "stable",
    *,
    allow_legacy: bool = False,
    container_digest: str | None = None,
) -> dict[str, str]:
    validate_channel_tag(tag, channel, load_policy(), allow_legacy=allow_legacy)
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise SystemExit(f"invalid release commit: {commit}")
    if not re.fullmatch(r"[0-9a-f]{64}", ledger_sha256):
        raise SystemExit(f"invalid release ledger digest: {ledger_sha256}")
    identity = {"tag": tag, "commit": commit, "ledger_sha256": ledger_sha256}
    if container_digest is not None:
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", container_digest):
            raise SystemExit(f"invalid container digest: {container_digest}")
        identity["container_digest"] = container_digest
    return identity


def same_identity(left: object, right: dict[str, str]) -> bool:
    return isinstance(left, dict) and all(
        left.get(key) == value for key, value in right.items()
    )


@dataclass
class StoredState:
    document: dict[str, Any]
    etag: str | None


class S3ChannelStore:
    def __init__(self, endpoint: str | None, bucket: str, key: str) -> None:
        try:
            import boto3
            from botocore.exceptions import ClientError
        except ImportError as exc:
            raise SystemExit(
                "boto3 is required; install scripts/release/requirements.lock"
            ) from exc
        self.bucket = bucket
        self.key = key
        self.client_error = ClientError
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name="auto",
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
        )

    def load(self) -> StoredState:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self.key)
        except self.client_error as exc:
            error = str(exc.response.get("Error", {}).get("Code"))
            if error in {"404", "NoSuchKey", "NotFound"}:
                return StoredState(
                    {"schema_version": 1, "current": None, "pending": None}, None
                )
            raise
        document = json.load(response["Body"])
        if not isinstance(document, dict) or document.get("schema_version") != 1:
            raise SystemExit("unsupported release-channel state")
        return StoredState(document, str(response["ETag"]))

    def compare_and_swap(self, previous: StoredState, document: dict[str, Any]) -> None:
        body = (
            json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        request: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": self.key,
            "Body": body,
            "ContentType": "application/json",
        }
        if previous.etag is None:
            request["IfNoneMatch"] = "*"
        else:
            request["IfMatch"] = previous.etag
        try:
            self.client.put_object(**request)
        except self.client_error as exc:
            error = exc.response.get("Error", {})
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if str(error.get("Code")) in {
                "409",
                "412",
                "ConditionalRequestConflict",
                "PreconditionFailed",
            } or status in {409, 412}:
                raise SystemExit("release channel changed concurrently; retry") from exc
            raise


def begin_promotion(
    store: S3ChannelStore,
    identity: dict[str, str],
    bootstrap_current: str | None,
    channel: str = "stable",
) -> None:
    stored = store.load()
    current = validate_promotion_state(
        stored.document, identity, bootstrap_current, channel
    )
    pending = stored.document.get("pending")
    if pending is not None:
        if same_identity(pending, identity):
            print(f"resuming release channel promotion for {identity['tag']}")
            return
        core_identity = {
            key: value for key, value in identity.items() if key != "container_digest"
        }
        if (
            "container_digest" in identity
            and same_identity(pending, core_identity)
            and isinstance(pending, dict)
            and pending.get("container_digest") is None
        ):
            next_state = {
                "schema_version": 1,
                "channel": channel,
                "current": current,
                "pending": identity,
            }
            store.compare_and_swap(stored, next_state)
            print(
                f"bound container digest to release channel promotion for {identity['tag']}"
            )
            return
        raise AssertionError("validated pending promotion was not resumable")
    next_state = {
        "schema_version": 1,
        "channel": channel,
        "current": current,
        "pending": identity,
    }
    store.compare_and_swap(stored, next_state)
    print(f"began release channel promotion for {identity['tag']}")


def validate_promotion_state(
    state: dict[str, Any],
    identity: dict[str, str],
    bootstrap_current: str | None,
    channel: str,
) -> dict[str, Any] | None:
    """Validate a candidate without reserving or changing channel state."""
    policy = load_policy()
    # The candidate was validated when its identity was created. Journal and
    # registry state may contain legacy spellings and are observation-only.
    validate_observed_channel_tag(identity["tag"], channel, policy)
    stored_channel = state.get("channel")
    if stored_channel not in {None, channel}:
        raise SystemExit(
            f"release channel journal belongs to {stored_channel}, not {channel}"
        )
    current = state.get("current")
    pending = state.get("pending")
    if current is not None and not (
        isinstance(current, dict) and isinstance(current.get("tag"), str)
    ):
        raise SystemExit("release channel has malformed current identity")
    if current is None and bootstrap_current:
        validate_observed_channel_tag(bootstrap_current, channel, policy)
        current = {"tag": bootstrap_current}
    if pending is not None:
        if same_identity(pending, identity):
            return current
        core_identity = {
            key: value for key, value in identity.items() if key != "container_digest"
        }
        if (
            "container_digest" in identity
            and same_identity(pending, core_identity)
            and isinstance(pending, dict)
            and pending.get("container_digest") is None
        ):
            return current
        pending_tag = pending.get("tag") if isinstance(pending, dict) else pending
        raise SystemExit(
            f"release channel promotion for {pending_tag} is incomplete; resume it before {identity['tag']}"
        )
    if bootstrap_current and isinstance(current, dict):
        validate_observed_channel_tag(bootstrap_current, channel, policy)
        if current["tag"] != bootstrap_current:
            raise SystemExit(
                f"release channel journal is {current['tag']} but observed alias is {bootstrap_current}"
            )
    if isinstance(current, dict) and isinstance(current.get("tag"), str):
        current_tag = str(current["tag"])
        validate_observed_channel_tag(current_tag, channel, policy)
        precedence = compare_channel_tags(identity["tag"], current_tag, channel, policy)
        if precedence < 0:
            raise SystemExit(
                f"release channel cannot move backward from {current_tag} to {identity['tag']}"
            )
        if precedence == 0 and current_tag != identity["tag"]:
            raise SystemExit(
                f"release channel version precedence collision: {current_tag} and {identity['tag']}"
            )
        for field in ("commit", "ledger_sha256", "container_digest"):
            if field not in identity:
                continue
            if current_tag == identity["tag"] and current.get(field) not in {
                None,
                identity[field],
            }:
                raise SystemExit(
                    f"release channel {identity['tag']} has a different {field}"
                )
    return current


def preflight_promotion(
    store: S3ChannelStore,
    identity: dict[str, str],
    bootstrap_current: str | None,
    channel: str = "stable",
) -> None:
    validate_promotion_state(
        store.load().document, identity, bootstrap_current, channel
    )
    print(f"release channel preflight passed for {identity['tag']}")


def finish_promotion(
    store: S3ChannelStore, identity: dict[str, str], channel: str = "stable"
) -> None:
    stored = store.load()
    state = stored.document
    if state.get("channel") not in {None, channel}:
        raise SystemExit(f"release channel journal does not belong to {channel}")
    if state.get("pending") is None and same_identity(state.get("current"), identity):
        print(f"release channel promotion already committed for {identity['tag']}")
        return
    if not same_identity(state.get("pending"), identity):
        raise SystemExit(
            f"release channel has no matching pending promotion for {identity['tag']}"
        )
    next_state = {
        "schema_version": 1,
        "channel": channel,
        "current": identity,
        "pending": None,
    }
    store.compare_and_swap(stored, next_state)
    print(f"committed release channel promotion for {identity['tag']}")


def journaled_container_digest(
    store: S3ChannelStore, identity: dict[str, str], channel: str = "stable"
) -> str | None:
    """Return a reusable digest already bound to this exact release identity."""
    state = store.load().document
    if state.get("channel") not in {None, channel}:
        raise SystemExit(f"release channel journal does not belong to {channel}")
    pending = state.get("pending")
    if pending is not None and not same_identity(pending, identity):
        pending_tag = pending.get("tag") if isinstance(pending, dict) else pending
        raise SystemExit(
            f"release channel promotion for {pending_tag} is incomplete; resume it before {identity['tag']}"
        )
    for stored_identity in (pending, state.get("current")):
        if not same_identity(stored_identity, identity):
            continue
        digest = stored_identity.get("container_digest")
        if digest is None:
            return None
        if not isinstance(digest, str) or not re.fullmatch(
            r"sha256:[0-9a-f]{64}", digest
        ):
            raise SystemExit("release channel has an invalid container digest")
        return digest
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("preflight", "begin", "finish", "resolve-container")
    )
    parser.add_argument("--endpoint")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--key", default="antfly/channels/stable.json")
    parser.add_argument("--channel", default="stable")
    parser.add_argument("--tag", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--ledger-sha256", required=True)
    parser.add_argument("--container-digest")
    parser.add_argument("--bootstrap-current")
    parser.add_argument("--allow-legacy-candidate", action="store_true")
    args = parser.parse_args()

    policy = load_policy()
    channel_policy = policy["channels"].get(args.channel)
    if not isinstance(channel_policy, dict):
        raise SystemExit(f"unknown release channel: {args.channel}")
    if args.key != channel_policy["journal_key"]:
        raise SystemExit(
            f"release channel {args.channel} requires journal {channel_policy['journal_key']}"
        )
    identity = release_identity(
        args.tag,
        args.commit,
        args.ledger_sha256,
        args.channel,
        allow_legacy=args.allow_legacy_candidate,
        container_digest=args.container_digest,
    )
    store = S3ChannelStore(args.endpoint, args.bucket, args.key)
    if args.command == "resolve-container":
        if args.bootstrap_current:
            parser.error("resolve-container does not accept --bootstrap-current")
        print(journaled_container_digest(store, identity, args.channel) or "")
    elif args.command == "preflight":
        preflight_promotion(store, identity, args.bootstrap_current, args.channel)
    elif args.command == "begin":
        begin_promotion(store, identity, args.bootstrap_current, args.channel)
    else:
        if args.bootstrap_current:
            parser.error("finish does not accept --bootstrap-current")
        finish_promotion(store, identity, args.channel)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
