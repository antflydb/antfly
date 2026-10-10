# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Conditional object operations; controller authority never lives in SQLite."""

import contextlib
import fcntl
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit


class Conflict(Exception):
    pass


class Unavailable(Exception):
    pass


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def encode(value) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


@dataclass(frozen=True)
class Object:
    uri: str
    version: str
    size: int
    modified_ms: int


class Store:
    """S3/GCS generation conditions, or flock + atomic rename for local tests."""

    def __init__(self, properties=None, *, max_bytes=16 * 1024 * 1024):
        self.properties = properties or {}
        self.max_bytes = max_bytes
        self._s3 = None
        self._gcs = None

    def _parts(self, uri):
        parsed = urlsplit(uri)
        if parsed.query or parsed.fragment or parsed.username or parsed.password:
            raise ValueError("object URI contains forbidden components")
        if parsed.scheme == "file":
            if parsed.netloc not in ("", "localhost"):
                raise ValueError("nonlocal filesystem authority")
            return "file", "", unquote(parsed.path)
        if parsed.scheme not in ("s3", "gs") or not parsed.netloc:
            raise ValueError("expected file://, s3:// or gs:// object URI")
        return parsed.scheme, parsed.netloc, parsed.path.lstrip("/")

    @property
    def s3(self):
        if self._s3 is None:
            import boto3
            from botocore.config import Config

            self._s3 = boto3.client(
                "s3",
                endpoint_url=self.properties.get("s3.endpoint"),
                region_name=self.properties.get("s3.region", "us-east-1"),
                aws_access_key_id=self.properties.get("s3.access-key-id"),
                aws_secret_access_key=self.properties.get("s3.secret-access-key"),
                aws_session_token=self.properties.get("s3.session-token"),
                config=Config(
                    connect_timeout=10,
                    read_timeout=20,
                    retries={"max_attempts": 0},
                    s3={"addressing_style": "path"},
                ),
            )
        return self._s3

    @property
    def gcs(self):
        if self._gcs is None:
            from google.cloud import storage

            self._gcs = storage.Client()
        return self._gcs

    @contextlib.contextmanager
    def _file_lock(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path + ".antfly-lock", "a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield

    def get(self, uri):
        scheme, bucket, key = self._parts(uri)
        if scheme == "file":
            try:
                with open(key, "rb") as stream:
                    body = stream.read(self.max_bytes + 1)
                    stat = os.fstat(stream.fileno())
            except FileNotFoundError:
                return None
            if len(body) > self.max_bytes:
                raise Unavailable("object exceeds byte budget")
            return body, f"{stat.st_ino}:{stat.st_size}:{stat.st_mtime_ns}"
        if scheme == "gs":
            from google.api_core.exceptions import NotFound

            blob = self.gcs.bucket(bucket).blob(key)
            try:
                blob.reload(timeout=20, retry=None)
                if blob.size > self.max_bytes:
                    raise Unavailable("object exceeds byte budget")
                body = blob.download_as_bytes(
                    if_generation_match=blob.generation, timeout=20, retry=None
                )
                return body, str(blob.generation)
            except NotFound:
                return None
        from botocore.exceptions import ClientError

        try:
            result = self.s3.get_object(Bucket=bucket, Key=key)
        except ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return None
            raise
        try:
            if result["ContentLength"] > self.max_bytes:
                raise Unavailable("object exceeds byte budget")
            body = result["Body"].read(self.max_bytes + 1)
            if len(body) > self.max_bytes:
                raise Unavailable("object exceeds byte budget")
            return body, result["ETag"]
        finally:
            result["Body"].close()

    def put(self, uri, body, *, version=None, absent=False):
        if len(body) > self.max_bytes:
            raise Unavailable("object exceeds byte budget")
        scheme, bucket, key = self._parts(uri)
        if scheme == "file":
            with self._file_lock(key):
                old = self.get(uri)
                if (absent and old is not None) or (
                    version is not None and (old is None or old[1] != version)
                ):
                    raise Conflict("conditional object write")
                fd, temporary = tempfile.mkstemp(dir=Path(key).parent)
                try:
                    with os.fdopen(fd, "wb") as stream:
                        stream.write(body)
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(temporary, key)
                    directory = os.open(Path(key).parent, os.O_RDONLY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
                finally:
                    if os.path.exists(temporary):
                        os.unlink(temporary)
                stat = os.stat(key)
                return f"{stat.st_ino}:{stat.st_size}:{stat.st_mtime_ns}"
        if scheme == "gs":
            from google.api_core.exceptions import PreconditionFailed

            blob = self.gcs.bucket(bucket).blob(key)
            try:
                blob.upload_from_string(
                    body,
                    if_generation_match=0
                    if absent
                    else int(version)
                    if version
                    else None,
                    timeout=20,
                    retry=None,
                )
                return str(blob.generation)
            except PreconditionFailed as error:
                raise Conflict("conditional object write") from error
        from botocore.exceptions import ClientError

        conditions = (
            {"IfNoneMatch": "*"} if absent else {"IfMatch": version} if version else {}
        )
        try:
            return self.s3.put_object(Bucket=bucket, Key=key, Body=body, **conditions)[
                "ETag"
            ]
        except ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] in (409, 412):
                raise Conflict("conditional object write") from error
            raise

    def delete(self, uri, version):
        scheme, bucket, key = self._parts(uri)
        if scheme == "file":
            with self._file_lock(key):
                try:
                    stat = os.stat(key)
                except FileNotFoundError:
                    return
                if f"{stat.st_ino}:{stat.st_size}:{stat.st_mtime_ns}" != version:
                    raise Conflict("delete object changed")
                os.unlink(key)
            return
        if scheme == "gs":
            from google.api_core.exceptions import NotFound, PreconditionFailed

            try:
                self.gcs.bucket(bucket).blob(key, generation=int(version)).delete(
                    if_generation_match=int(version), timeout=20, retry=None
                )
            except NotFound:
                pass
            except PreconditionFailed as error:
                raise Conflict("delete object changed") from error
            return
        from botocore.exceptions import ClientError

        try:
            proof = json.loads(version)
            if not isinstance(proof, dict):
                raise ValueError(
                    "S3 deletion requires an exact inventory version proof"
                )
            if not proof.get("version_id"):
                raise Unavailable("S3 deletion requires an exact version ID")
            self.s3.delete_object(Bucket=bucket, Key=key, VersionId=proof["version_id"])
        except ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] == 412:
                raise Conflict("delete object changed") from error
            if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                raise

    def inventory(self, prefix, *, all_versions=False):
        scheme, bucket, key = self._parts(prefix)
        if not prefix.endswith("/"):
            raise ValueError("inventory requires a slash-terminated prefix")
        if scheme == "file":
            for path in sorted(Path(key).rglob("*")):
                if not path.is_file() or path.name.endswith(".antfly-lock"):
                    continue
                stat = path.stat()
                yield Object(
                    path.as_uri(),
                    f"{stat.st_ino}:{stat.st_size}:{stat.st_mtime_ns}",
                    stat.st_size,
                    stat.st_mtime_ns // 1_000_000,
                )
        elif scheme == "gs":
            for blob in self.gcs.list_blobs(
                bucket, prefix=key, versions=all_versions, timeout=20, retry=None
            ):
                yield Object(
                    f"gs://{bucket}/{blob.name}",
                    str(blob.generation),
                    blob.size,
                    int(blob.updated.timestamp() * 1000),
                )
        elif all_versions:
            pages = self.s3.get_paginator("list_object_versions").paginate(
                Bucket=bucket, Prefix=key
            )
            for page in pages:
                for item in page.get("Versions", []) + page.get("DeleteMarkers", []):
                    yield Object(
                        f"s3://{bucket}/{item['Key']}",
                        encode(
                            {"etag": item.get("ETag"), "version_id": item["VersionId"]}
                        ).decode(),
                        item.get("Size", 0),
                        int(item["LastModified"].timestamp() * 1000),
                    )
        else:
            pages = self.s3.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=key
            )
            for page in pages:
                for item in page.get("Contents", []):
                    yield Object(
                        f"s3://{bucket}/{item['Key']}",
                        self._s3_version(bucket, item),
                        item["Size"],
                        int(item["LastModified"].timestamp() * 1000),
                    )

    def inventory_page(self, prefix, cursor=None, *, limit=256):
        """One exact-version page; the opaque cursor belongs to the caller's epoch."""
        if not prefix.endswith("/") or not 1 <= limit <= 1000:
            raise ValueError("invalid inventory page")
        scheme, bucket, key = self._parts(prefix)
        if scheme == "s3":
            result = self.s3.list_object_versions(
                Bucket=bucket, Prefix=key, MaxKeys=limit, **(cursor or {})
            )
            entries = [
                Object(
                    f"s3://{bucket}/{item['Key']}",
                    encode(
                        {"etag": item.get("ETag"), "version_id": item["VersionId"]}
                    ).decode(),
                    item.get("Size", 0),
                    int(item["LastModified"].timestamp() * 1000),
                )
                for item in result.get("Versions", []) + result.get("DeleteMarkers", [])
            ]
            following = (
                {
                    "KeyMarker": result["NextKeyMarker"],
                    "VersionIdMarker": result["NextVersionIdMarker"],
                }
                if result.get("IsTruncated")
                else None
            )
            return entries, following
        if scheme == "gs":
            iterator = self.gcs.list_blobs(
                bucket,
                prefix=key,
                versions=True,
                page_token=cursor,
                page_size=limit,
                max_results=limit,
                timeout=20,
                retry=None,
            )
            page = next(iterator.pages, ())
            entries = [
                Object(
                    f"gs://{bucket}/{blob.name}",
                    str(blob.generation),
                    blob.size or 0,
                    int(blob.updated.timestamp() * 1000),
                )
                for blob in page
            ]
            return entries, iterator.next_page_token
        # Local qualification uses a deterministic path cursor. Cloud inventory
        # uses provider continuation tokens rather than rescanning old pages.
        from itertools import islice

        entries = list(
            islice(
                (
                    item
                    for item in self.inventory(prefix, all_versions=True)
                    if cursor is None or item.uri > cursor
                ),
                limit + 1,
            )
        )
        return entries[:limit], entries[limit - 1].uri if len(entries) > limit else None

    def _s3_version(self, bucket, item):
        head = self.s3.head_object(Bucket=bucket, Key=item["Key"], IfMatch=item["ETag"])
        if head["ETag"] != item["ETag"]:
            raise Conflict("inventory object changed")
        return encode(
            {"etag": item["ETag"], "version_id": head.get("VersionId", "null")}
        ).decode()

    def require_versioned_deletion(self, warehouse):
        scheme, bucket, _ = self._parts(warehouse)
        if (
            scheme == "s3"
            and self.s3.get_bucket_versioning(Bucket=bucket).get("Status") != "Enabled"
        ):
            raise Unavailable(
                "S3 vacuum requires bucket versioning and exact version deletion"
            )

    def probe_authority(self, prefix):
        import uuid

        uri = prefix + "probes/" + uuid.uuid4().hex
        self.put(uri, b"conditional-authority", absent=True)
        old_version = self.get(uri)[1]
        scheme, _, _ = self._parts(uri)
        wrong_version = (
            str(int(old_version) + 1) if scheme == "gs" else '"invalid-version"'
        )
        for conditions in ({"absent": True}, {"version": wrong_version}):
            try:
                self.put(uri, b"incorrect", **conditions)
            except Conflict:
                continue
            raise Unavailable(
                "object store does not enforce conditional authority writes"
            )
        if self.get(uri)[0] != b"conditional-authority":
            raise Unavailable("conditional authority probe changed")

    def immutable(self, uri, body):
        try:
            self.put(uri, body, absent=True)
        except Conflict:
            existing = self.get(uri)
            if existing is None or existing[0] != body:
                raise Conflict("immutable object differs")

    def mutate(self, uri, function):
        for _ in range(32):
            old = self.get(uri)
            value = (
                json.loads(old[0])
                if old
                else {
                    "format": 1,
                    "sequence": 0,
                    "writer": None,
                    "vacuum": None,
                    "readers": {},
                }
            )
            result = function(value)
            value["sequence"] += 1
            try:
                self.put(
                    uri,
                    encode(value),
                    version=old[1] if old else None,
                    absent=old is None,
                )
                return result
            except Conflict:
                continue
        raise Conflict("authority contention")
