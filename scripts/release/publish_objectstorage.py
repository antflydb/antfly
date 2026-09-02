#!/usr/bin/env python3
"""Publish Antfly release files to object storage.

The current release pipeline uses the S3-compatible mode for Cloudflare R2.
GCS and local modes are included so the same release payload can be pushed to a
different backing store or smoke-tested without touching remote storage.
"""

from __future__ import annotations

import argparse
import hashlib
import mimetypes
import os
import shutil
import subprocess
import tempfile
from pathlib import Path


def is_stable_tag(tag: str) -> bool:
    version = tag.removeprefix("v")
    return "-" not in version


def clean_prefix(prefix: str) -> str:
    return prefix.strip("/")


def storage_key(prefix: str, path: Path) -> str:
    prefix = clean_prefix(prefix)
    return f"{prefix}/{path.name}" if prefix else path.name


class Publisher:
    def upload(
        self, path: Path, key: str, dry_run: bool, immutable: bool = False
    ) -> None:
        raise NotImplementedError

    def list_names(self, prefix: str) -> set[str]:
        raise NotImplementedError


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class S3Publisher(Publisher):
    def __init__(
        self, endpoint: str | None, bucket: str, region: str, dry_run: bool
    ) -> None:
        self.bucket = bucket
        self.client = None
        if dry_run:
            return
        try:
            import boto3
            from botocore.exceptions import ClientError
        except ImportError as exc:
            raise SystemExit(
                "boto3 is required for --provider s3; install it with `python -m pip install boto3`"
            ) from exc
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name=region,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
        )
        self.client_error = ClientError

    def existing_sha256(self, key: str) -> str | None:
        assert self.client is not None
        try:
            head = self.client.head_object(Bucket=self.bucket, Key=key)
        except self.client_error as exc:
            if str(exc.response.get("Error", {}).get("Code")) in {
                "404",
                "NoSuchKey",
                "NotFound",
            }:
                return None
            raise
        metadata_digest = head.get("Metadata", {}).get("sha256")
        if metadata_digest:
            return str(metadata_digest)
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        digest = hashlib.sha256()
        for chunk in iter(lambda: response["Body"].read(1024 * 1024), b""):
            digest.update(chunk)
        return digest.hexdigest()

    def upload(
        self, path: Path, key: str, dry_run: bool, immutable: bool = False
    ) -> None:
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        destination = f"s3://{self.bucket}/{key}"
        print(f"{'would upload' if dry_run else 'uploading'} {destination}")
        if dry_run:
            return
        assert self.client is not None
        local_digest = sha256(path)
        if immutable:
            try:
                with path.open("rb") as src:
                    self.client.put_object(
                        Bucket=self.bucket,
                        Key=key,
                        Body=src,
                        ContentType=content_type,
                        Metadata={"sha256": local_digest},
                        IfNoneMatch="*",
                    )
                return
            except self.client_error as exc:
                error = exc.response.get("Error", {})
                status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if str(error.get("Code")) not in {
                    "409",
                    "412",
                    "ConditionalRequestConflict",
                    "PreconditionFailed",
                } and status not in {409, 412}:
                    raise
            existing_digest = self.existing_sha256(key)
            if existing_digest != local_digest:
                raise SystemExit(
                    f"immutable object differs: {destination}\nremote: {existing_digest or 'missing'}\nlocal:  {local_digest}"
                )
            print(f"immutable object already matches: {destination}")
            return
        self.client.upload_file(
            str(path),
            self.bucket,
            key,
            ExtraArgs={
                "ContentType": content_type,
                "Metadata": {"sha256": local_digest},
            },
        )

    def list_names(self, prefix: str) -> set[str]:
        assert self.client is not None
        base = clean_prefix(prefix) + "/"
        paginator = self.client.get_paginator("list_objects_v2")
        names: set[str] = set()
        for page in paginator.paginate(Bucket=self.bucket, Prefix=base):
            for entry in page.get("Contents", []):
                key = entry.get("Key") if isinstance(entry, dict) else None
                if not isinstance(key, str) or not key.startswith(base):
                    raise SystemExit("object-storage listing returned an invalid key")
                names.add(key.removeprefix(base))
        return names


class GCSPublisher(Publisher):
    def __init__(self, bucket: str) -> None:
        self.bucket = bucket

    def upload(
        self, path: Path, key: str, dry_run: bool, immutable: bool = False
    ) -> None:
        destination = f"gs://{self.bucket}/{key}"
        print(f"{'would upload' if dry_run else 'uploading'} {destination}")
        if dry_run:
            return
        if immutable:
            with tempfile.TemporaryDirectory() as raw_tmp:
                existing = Path(raw_tmp) / path.name
                result = subprocess.run(
                    ["gcloud", "storage", "cp", destination, str(existing)],
                    check=False,
                    capture_output=True,
                )
                if result.returncode == 0:
                    if sha256(existing) != sha256(path):
                        raise SystemExit(f"immutable object differs: {destination}")
                    print(f"immutable object already matches: {destination}")
                    return
        command = ["gcloud", "storage", "cp"]
        if immutable:
            command.append("--if-generation-match=0")
        subprocess.run([*command, str(path), destination], check=True)

    def list_names(self, prefix: str) -> set[str]:
        base = f"gs://{self.bucket}/{clean_prefix(prefix)}/"
        result = subprocess.run(
            ["gcloud", "storage", "ls", "--recursive", f"{base}**"],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode:
            raise SystemExit(
                "cannot list object-storage release prefix: "
                + (result.stderr.strip() or result.stdout.strip())
            )
        names: set[str] = set()
        for raw in result.stdout.splitlines():
            url = raw.strip()
            if not url or url.endswith("/"):
                continue
            if not url.startswith(base):
                raise SystemExit("object-storage listing returned an invalid URL")
            names.add(url.removeprefix(base))
        return names


class LocalPublisher(Publisher):
    def __init__(self, root: Path, bucket: str) -> None:
        self.root = root
        self.bucket = bucket

    def upload(
        self, path: Path, key: str, dry_run: bool, immutable: bool = False
    ) -> None:
        destination = self.root / self.bucket / key
        print(f"{'would copy' if dry_run else 'copying'} {destination}")
        if dry_run:
            return
        destination.parent.mkdir(parents=True, exist_ok=True)
        if immutable:
            local_digest = sha256(path)
            with tempfile.NamedTemporaryFile(
                dir=destination.parent, prefix=f".{destination.name}.", delete=False
            ) as tmp:
                temporary = Path(tmp.name)
            try:
                shutil.copy2(path, temporary)
                try:
                    os.link(temporary, destination)
                    return
                except FileExistsError:
                    if sha256(destination) != local_digest:
                        raise SystemExit(f"immutable object differs: {destination}")
                    print(f"immutable object already matches: {destination}")
                    return
            finally:
                temporary.unlink(missing_ok=True)
        shutil.copy2(path, destination)

    def list_names(self, prefix: str) -> set[str]:
        directory = self.root / self.bucket / clean_prefix(prefix)
        if not directory.exists():
            return set()
        return {
            path.relative_to(directory).as_posix()
            for path in directory.rglob("*")
            if path.is_file()
        }


def require_exact_prefix(
    publisher: Publisher, prefix: str, expected_names: set[str]
) -> None:
    actual_names = publisher.list_names(prefix)
    if actual_names != expected_names:
        raise SystemExit(
            "object-storage release member set differs: "
            f"expected {sorted(expected_names)}, got {sorted(actual_names)}"
        )
    print(f"verified exact object-storage release prefix: {clean_prefix(prefix)}")


def build_publisher(args: argparse.Namespace) -> Publisher:
    if args.provider == "s3":
        return S3Publisher(args.endpoint, args.bucket, args.region, args.dry_run)
    if args.provider == "gcs":
        return GCSPublisher(args.bucket)
    if args.provider == "local":
        return LocalPublisher(args.local_root, args.bucket)
    raise SystemExit(f"unsupported provider: {args.provider}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=Path, help="files to upload")
    parser.add_argument("--provider", choices=("s3", "gcs", "local"), default="s3")
    parser.add_argument("--bucket", required=True)
    parser.add_argument(
        "--endpoint", help="S3-compatible endpoint URL, for example Cloudflare R2"
    )
    parser.add_argument("--region", default="auto", help="S3 region; R2 accepts auto")
    parser.add_argument(
        "--prefix", required=True, help="object key prefix for the versioned release"
    )
    parser.add_argument(
        "--content-addressed-prefix",
        help="optional prefix for immutable sha256-addressed objects",
    )
    parser.add_argument(
        "--latest-prefix", help="object key prefix for the stable latest channel"
    )
    parser.add_argument(
        "--exact-prefix",
        action="store_true",
        help="require the immutable version prefix to contain exactly the supplied files",
    )
    parser.add_argument(
        "--publish-latest", action="store_true", help="also publish to --latest-prefix"
    )
    parser.add_argument(
        "--publish-latest-if-stable",
        metavar="TAG",
        help="publish latest only when TAG is stable",
    )
    parser.add_argument(
        "--local-root",
        type=Path,
        default=Path("dist/objectstorage"),
        help="root for --provider local",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.exact_prefix and args.dry_run:
        parser.error("--exact-prefix cannot be combined with --dry-run")

    files = [path for path in args.files if path.is_file()]
    if not files:
        raise SystemExit("no upload files were provided")

    publish_latest = args.publish_latest
    if args.publish_latest_if_stable:
        publish_latest = publish_latest or is_stable_tag(args.publish_latest_if_stable)
    if publish_latest and not args.latest_prefix:
        raise SystemExit(
            "--latest-prefix is required when publishing the latest channel"
        )

    publisher = build_publisher(args)
    sorted_files = sorted(files, key=lambda item: item.name)
    expected_names = {path.name for path in sorted_files}
    if len(expected_names) != len(sorted_files):
        raise SystemExit("upload files contain duplicate object names")
    for path in sorted_files:
        if args.content_addressed_prefix:
            digest_prefix = (
                f"{clean_prefix(args.content_addressed_prefix)}/sha256/{sha256(path)}"
            )
            publisher.upload(
                path, storage_key(digest_prefix, path), args.dry_run, immutable=True
            )
        publisher.upload(
            path, storage_key(args.prefix, path), args.dry_run, immutable=True
        )
    if args.exact_prefix:
        require_exact_prefix(publisher, args.prefix, expected_names)
    if publish_latest:
        assert args.latest_prefix is not None
        pointers = [path for path in sorted_files if path.name == "metadata.json"]
        if len(pointers) != 1:
            raise SystemExit(
                "latest publication requires exactly one metadata.json channel pointer"
            )
        for path in sorted_files:
            if path == pointers[0]:
                continue
            publisher.upload(path, storage_key(args.latest_prefix, path), args.dry_run)
        # metadata.json is the channel commit point. Installers read it first
        # and then fetch the immutable versioned payload named by its tag.
        publisher.upload(
            pointers[0], storage_key(args.latest_prefix, pointers[0]), args.dry_run
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
