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

"""Dictation adapters over the repository's R2 publisher, reader, and CAS store."""

from pathlib import Path

from publish_objectstorage import (
    S3Publisher,
    LocalPublisher,
    require_exact_prefix,
    sha256,
)
from download_objectstorage import S3Reader, LocalReader
from objectstore_channel import S3ConditionalStore, LocalConditionalStore


class ObjectStorage:
    def __init__(self, publisher, reader, channel_store):
        self.publisher = publisher
        self.reader = reader
        self.channel_store = channel_store

    @classmethod
    def s3(cls, endpoint, bucket):
        return cls(
            S3Publisher(endpoint, bucket, "auto", False),
            S3Reader(endpoint, bucket, "auto"),
            lambda key: S3ConditionalStore(endpoint, bucket, key),
        )

    @classmethod
    def local(cls, root, bucket):
        root = Path(root)
        return cls(
            LocalPublisher(root, bucket),
            LocalReader(root, bucket),
            lambda key: LocalConditionalStore(root, bucket, key),
        )

    def upload_immutable(self, path, key):
        expected = (sha256(path), path.stat().st_size)
        current = self.publisher.existing_fingerprint(key)
        if current is not None:
            if current != expected:
                raise ValueError("Immutable release object differs: " + key)
            return
        self.publisher.upload(path, key, False, immutable=True)
        # Confirm size as well as digest, including an upload whose response was
        # recovered by the shared publisher after a conditional-write conflict.
        if self.publisher.existing_fingerprint(key) != expected:
            raise ValueError("Immutable release upload could not be verified: " + key)

    def exact(self, prefix, names):
        require_exact_prefix(self.publisher, prefix, set(names))
