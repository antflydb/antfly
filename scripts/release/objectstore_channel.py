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

"""Conditional object writes shared by release journals and signed update feeds."""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StoredBytes:
    body: bytes | None
    etag: str | None


class ConditionalWriteConflict(RuntimeError):
    pass


class S3ConditionalStore:
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

    def load_bytes(self, max_bytes: int | None = None) -> StoredBytes:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self.key)
        except self.client_error as exc:
            code = str(exc.response.get("Error", {}).get("Code"))
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
                return StoredBytes(None, None)
            raise
        stream = response["Body"]
        try:
            body = stream.read() if max_bytes is None else stream.read(max_bytes + 1)
        finally:
            stream.close()
        if max_bytes is not None and len(body) > max_bytes:
            raise ValueError("Release channel object exceeds its size limit")
        return StoredBytes(body, str(response["ETag"]))

    def compare_and_swap_bytes(
        self,
        previous: StoredBytes,
        body: bytes,
        content_type: str,
        cache_control: str | None = None,
    ) -> None:
        request = {
            "Bucket": self.bucket,
            "Key": self.key,
            "Body": body,
            "ContentType": content_type,
        }
        if cache_control is not None:
            request["CacheControl"] = cache_control
        if previous.etag is None:
            request["IfNoneMatch"] = "*"
        else:
            request["IfMatch"] = previous.etag
        try:
            self.client.put_object(**request)
        except self.client_error as exc:
            code = str(exc.response.get("Error", {}).get("Code"))
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in {
                "409",
                "412",
                "ConditionalRequestConflict",
                "PreconditionFailed",
            } or status in {409, 412}:
                raise ConditionalWriteConflict(
                    "release channel changed concurrently; retry"
                ) from exc
            raise


class LocalConditionalStore:
    """Filesystem backend for offline qualification, with process-safe CAS."""

    def __init__(self, root: Path, bucket: str, key: str) -> None:
        self.path = root / bucket / key
        self.lock_path = (
            root
            / bucket
            / ".conditional-locks"
            / hashlib.sha256(key.encode()).hexdigest()
        )

    def load_bytes(self, max_bytes: int | None = None) -> StoredBytes:
        try:
            with self.path.open("rb") as stream:
                body = (
                    stream.read() if max_bytes is None else stream.read(max_bytes + 1)
                )
        except FileNotFoundError:
            return StoredBytes(None, None)
        if max_bytes is not None and len(body) > max_bytes:
            raise ValueError("Release channel object exceeds its size limit")
        return StoredBytes(body, hashlib.sha256(body).hexdigest())

    def compare_and_swap_bytes(
        self,
        previous: StoredBytes,
        body: bytes,
        content_type: str,
        cache_control: str | None = None,
    ) -> None:
        import fcntl

        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if self.load_bytes().etag != previous.etag:
                raise ConditionalWriteConflict(
                    "release channel changed concurrently; retry"
                )
            with tempfile.NamedTemporaryFile(
                dir=self.path.parent, delete=False
            ) as stream:
                temporary = Path(stream.name)
                try:
                    stream.write(body)
                    stream.flush()
                    os.fsync(stream.fileno())
                    os.replace(temporary, self.path)
                finally:
                    temporary.unlink(missing_ok=True)
