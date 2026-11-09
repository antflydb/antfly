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

import io
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from objectstore_channel import (
    S3ConditionalStore,
    StoredBytes,
    ConditionalWriteConflict,
)
from download_objectstorage import PublicReader


class ClientFailure(Exception):
    def __init__(self, status):
        self.response = {
            "Error": {"Code": str(status)},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }


class ConditionalTransportTests(unittest.TestCase):
    def setUp(self):
        self.store = S3ConditionalStore.__new__(S3ConditionalStore)
        self.store.client = Mock()
        self.store.client_error = ClientFailure
        self.store.bucket = "bucket"
        self.store.key = "feed"

    def test_channel_create_and_update_are_conditional_and_uncached(self):
        self.store.compare_and_swap_bytes(
            StoredBytes(None, None),
            b"feed",
            "application/xml",
            "no-cache, no-transform",
        )
        self.assertEqual(
            self.store.client.put_object.call_args.kwargs["IfNoneMatch"], "*"
        )
        self.assertEqual(
            self.store.client.put_object.call_args.kwargs["CacheControl"],
            "no-cache, no-transform",
        )
        self.store.compare_and_swap_bytes(
            StoredBytes(b"old", '"etag"'), b"feed", "application/xml"
        )
        self.assertEqual(
            self.store.client.put_object.call_args.kwargs["IfMatch"], '"etag"'
        )
        self.assertNotIn("IfNoneMatch", self.store.client.put_object.call_args.kwargs)

    def test_conflict_is_distinct_from_permission_or_service_failure(self):
        for status in (409, 412, 403, 500):
            with self.subTest(status=status):
                self.store.client.put_object.side_effect = ClientFailure(status)
                with self.assertRaises(
                    ConditionalWriteConflict if status in (409, 412) else ClientFailure
                ):
                    self.store.compare_and_swap_bytes(
                        StoredBytes(None, None), b"feed", "application/xml"
                    )

    def test_only_not_found_is_absence(self):
        for status in (404, 403, 500):
            self.store.client.get_object.side_effect = ClientFailure(status)
            if status == 404:
                self.assertEqual(self.store.load_bytes(), StoredBytes(None, None))
            else:
                with self.assertRaises(ClientFailure):
                    self.store.load_bytes()

    def test_bound_reads_close_stream_even_on_oversize(self):
        stream = io.BytesIO(b"large feed")
        self.store.client.get_object.return_value = {"Body": stream, "ETag": "etag"}
        with self.assertRaisesRegex(ValueError, "size limit"):
            self.store.load_bytes(max_bytes=2)
        self.assertTrue(stream.closed)

    def test_load_preserves_exact_bytes_and_etag(self):
        stream = io.BytesIO(b"signed feed\n")
        self.store.client.get_object.return_value = {"Body": stream, "ETag": "etag"}
        self.assertEqual(
            self.store.load_bytes(max_bytes=100), StoredBytes(b"signed feed\n", "etag")
        )
        self.assertTrue(stream.closed)

    def test_public_trust_reader_only_treats_404_as_absent(self):
        reader = PublicReader("https://releases.antfly.io/antfly-dictation")
        for status in (404, 403, 500):
            with patch(
                "download_objectstorage.urlopen",
                side_effect=HTTPError("url", status, "fixture", None, None),
            ):
                if status == 404:
                    self.assertIsNone(reader.read_optional("signing/public-key.json"))
                else:
                    with self.assertRaises(HTTPError):
                        reader.read_optional("signing/public-key.json")

    def test_public_reader_bounds_and_rejects_traversal(self):
        reader = PublicReader(
            "https://releases.antfly.io/antfly-dictation", max_bytes=2
        )
        for key in ("../key", "/key", "a\\key"):
            with self.assertRaises(ValueError):
                reader.read_optional(key)
        with patch(
            "download_objectstorage.urlopen", return_value=io.BytesIO(b"large")
        ) as get:
            with self.assertRaisesRegex(ValueError, "size limit"):
                reader.read_optional("key")
            self.assertEqual(get.call_args.kwargs["timeout"], 30)
