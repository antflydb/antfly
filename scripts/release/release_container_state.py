#!/usr/bin/env python3
"""Create or resolve an immutable OCI digest record for one release ledger."""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from typing import Protocol

from release_channels import load_policy, validate_observed_channel_tag

CONTAINER_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
GIT_COMMIT = re.compile(r"[0-9a-f]{40}")
LEDGER_DIGEST = re.compile(r"[0-9a-f]{64}")
CONFLICT_CODES = {
    "409",
    "412",
    "ConditionalRequestConflict",
    "PreconditionFailed",
}


def container_record_key(ledger_sha256: str) -> str:
    if not LEDGER_DIGEST.fullmatch(ledger_sha256):
        raise SystemExit(f"invalid release ledger digest: {ledger_sha256}")
    return f"antfly/container-identities/{ledger_sha256}.json"


def container_identity(
    tag: str,
    channel: str,
    commit: str,
    ledger_sha256: str,
    container_digest: str,
) -> dict[str, object]:
    identity = container_core_identity(tag, channel, commit, ledger_sha256)
    if not CONTAINER_DIGEST.fullmatch(container_digest):
        raise SystemExit(f"invalid container digest: {container_digest}")
    return {
        "schema_version": 1,
        **identity,
        "container_digest": container_digest,
    }


def container_core_identity(
    tag: str,
    channel: str,
    commit: str,
    ledger_sha256: str,
) -> dict[str, str]:
    validate_observed_channel_tag(tag, channel, load_policy())
    if not GIT_COMMIT.fullmatch(commit):
        raise SystemExit(f"invalid release commit: {commit}")
    if not LEDGER_DIGEST.fullmatch(ledger_sha256):
        raise SystemExit(f"invalid release ledger digest: {ledger_sha256}")
    return {
        "tag": tag,
        "channel": channel,
        "commit": commit,
        "ledger_sha256": ledger_sha256,
    }


def validate_record(document: object) -> dict[str, object]:
    if not isinstance(document, dict) or set(document) != {
        "schema_version",
        "tag",
        "channel",
        "commit",
        "ledger_sha256",
        "container_digest",
    }:
        raise SystemExit("malformed immutable container identity")
    expected = container_identity(
        str(document.get("tag")),
        str(document.get("channel")),
        str(document.get("commit")),
        str(document.get("ledger_sha256")),
        str(document.get("container_digest")),
    )
    if document != expected:
        raise SystemExit("malformed immutable container identity")
    return expected


class ContainerStore(Protocol):
    def load(self) -> dict[str, object] | None: ...

    def create(self, document: dict[str, object]) -> None: ...


@dataclass
class S3ContainerStore:
    endpoint: str | None
    bucket: str
    key: str

    def __post_init__(self) -> None:
        try:
            import boto3
            from botocore.exceptions import ClientError
        except ImportError as exc:
            raise SystemExit(
                "boto3 is required; install scripts/release/requirements.lock"
            ) from exc
        self.client_error = ClientError
        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            region_name="auto",
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
        )

    def load(self) -> dict[str, object] | None:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self.key)
        except self.client_error as exc:
            error = str(exc.response.get("Error", {}).get("Code"))
            if error in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        return validate_record(json.load(response["Body"]))

    def create(self, document: dict[str, object]) -> None:
        body = (
            json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        self.client.put_object(
            Bucket=self.bucket,
            Key=self.key,
            Body=body,
            ContentType="application/json",
            IfNoneMatch="*",
        )


def resolve_container(
    store: ContainerStore,
    tag: str,
    channel: str,
    commit: str,
    ledger_sha256: str,
) -> str | None:
    expected_core = container_core_identity(tag, channel, commit, ledger_sha256)
    document = store.load()
    if document is None:
        return None
    record = validate_record(document)
    for field, expected in expected_core.items():
        if record[field] != expected:
            raise SystemExit(
                f"immutable container identity has different {field}: "
                f"expected={expected} actual={record[field]}"
            )
    return str(record["container_digest"])


def bind_container(store: ContainerStore, identity: dict[str, object]) -> None:
    existing = store.load()
    if existing is not None:
        if validate_record(existing) != identity:
            raise SystemExit(
                "immutable container identity already has different contents"
            )
        print(
            f"immutable container identity already matches {identity['container_digest']}"
        )
        return
    try:
        store.create(identity)
    except Exception as exc:
        response = getattr(exc, "response", {})
        error = response.get("Error", {}) if isinstance(response, dict) else {}
        status = (
            response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if isinstance(response, dict)
            else None
        )
        if str(error.get("Code")) not in CONFLICT_CODES and status not in {409, 412}:
            raise
        existing = store.load()
        if existing is None or validate_record(existing) != identity:
            raise SystemExit(
                "immutable container identity was created concurrently with different contents"
            ) from exc
    print(f"bound immutable container identity {identity['container_digest']}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("resolve", "bind"))
    parser.add_argument("--endpoint")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--channel", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--ledger-sha256", required=True)
    parser.add_argument("--container-digest")
    args = parser.parse_args()

    key = container_record_key(args.ledger_sha256)
    store = S3ContainerStore(args.endpoint, args.bucket, key)
    if args.command == "resolve":
        if args.container_digest:
            parser.error("resolve does not accept --container-digest")
        print(
            resolve_container(
                store,
                args.tag,
                args.channel,
                args.commit,
                args.ledger_sha256,
            )
            or ""
        )
        return 0

    if not args.container_digest:
        parser.error("bind requires --container-digest")
    identity = container_identity(
        args.tag,
        args.channel,
        args.commit,
        args.ledger_sha256,
        args.container_digest,
    )
    bind_container(store, identity)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
