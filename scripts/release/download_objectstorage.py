#!/usr/bin/env python3
"""Restore an exact immutable release payload from object storage."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Protocol


def clean_prefix(prefix: str) -> str:
    return prefix.strip("/")


class Reader(Protocol):
    def read(self, key: str) -> bytes: ...

    def list_names(self, prefix: str) -> set[str]: ...


class S3Reader:
    def __init__(self, endpoint: str | None, bucket: str, region: str) -> None:
        try:
            import boto3
        except ImportError as exc:
            raise SystemExit(
                "boto3 is required; install scripts/release/requirements.lock"
            ) from exc
        from botocore.exceptions import ClientError

        self.bucket = bucket
        self.client_error = ClientError
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name=region,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
        )

    def read(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return bytes(response["Body"].read())

    def read_optional(self, key: str) -> bytes | None:
        try:
            return self.read(key)
        except self.client_error as exc:
            code = str(exc.response.get("Error", {}).get("Code"))
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
                return None
            raise

    def list_names(self, prefix: str) -> set[str]:
        prefix = clean_prefix(prefix) + "/"
        paginator = self.client.get_paginator("list_objects_v2")
        names: set[str] = set()
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for entry in page.get("Contents", []):
                key = entry.get("Key") if isinstance(entry, dict) else None
                if not isinstance(key, str) or not key.startswith(prefix):
                    raise SystemExit("object-storage listing returned an invalid key")
                names.add(key.removeprefix(prefix))
        return names


class LocalReader:
    def __init__(self, root: Path, bucket: str) -> None:
        self.root = root
        self.bucket = bucket

    def read(self, key: str) -> bytes:
        return (self.root / self.bucket / key).read_bytes()

    def read_optional(self, key: str) -> bytes | None:
        path = self.root / self.bucket / key
        return path.read_bytes() if path.exists() else None

    def list_names(self, prefix: str) -> set[str]:
        directory = self.root / self.bucket / clean_prefix(prefix)
        if not directory.exists():
            return set()
        return {
            str(path.relative_to(directory))
            for path in directory.rglob("*")
            if path.is_file()
        }


def payload_names(ledger_bytes: bytes) -> list[str]:
    ledger = json.loads(ledger_bytes)
    if ledger.get("schema_version") not in {1, 2, 3, 4}:
        raise SystemExit("unsupported release ledger schema")
    entries = ledger.get("artifacts")
    if not isinstance(entries, list) or not entries:
        raise SystemExit("release ledger contains no artifacts")
    names: list[str] = []
    for entry in entries:
        name = entry.get("name") if isinstance(entry, dict) else None
        if (
            not isinstance(name, str)
            or not name
            or name != Path(name).name
            or name == "artifacts.json"
            or name in names
        ):
            raise SystemExit("release ledger contains an invalid artifact name")
        names.append(name)
    return names


def restore_payload(reader: Reader, prefix: str, out_dir: Path) -> None:
    prefix = clean_prefix(prefix)
    if not prefix:
        raise SystemExit("object-storage release prefix cannot be empty")
    if out_dir.exists() and any(out_dir.iterdir()):
        raise SystemExit(f"release payload directory must be empty: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)

    ledger_key = f"{prefix}/artifacts.json"
    ledger_bytes = reader.read(ledger_key)
    names = payload_names(ledger_bytes)
    (out_dir / "artifacts.json").write_bytes(ledger_bytes)
    for name in names:
        (out_dir / name).write_bytes(reader.read(f"{prefix}/{name}"))
    print(f"restored {len(names) + 1} immutable objects from {prefix}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=("s3", "local"), default="s3")
    parser.add_argument("--endpoint")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", default="auto")
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--local-root", type=Path, default=Path("dist/objectstorage"))
    args = parser.parse_args()

    reader: Reader
    if args.provider == "s3":
        reader = S3Reader(args.endpoint, args.bucket, args.region)
    else:
        reader = LocalReader(args.local_root, args.bucket)
    restore_payload(reader, args.prefix, args.out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
