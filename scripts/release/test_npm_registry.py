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
import json
import unittest
import urllib.error
from unittest.mock import Mock

from registry.model import RegistryError
from registry.npm import dist_tag, version_integrity


def opener(document):
    return Mock(return_value=io.BytesIO(json.dumps(document).encode()))


class NpmRegistryTests(unittest.TestCase):
    def test_tag_readback_uses_fresh_endpoint_instead_of_cached_package_metadata(self):
        def registry(request, **kwargs):
            if request.full_url.endswith("/dist-tags"):
                return io.BytesIO(b'{"next":"0.3.0-rc.1"}')
            # A stale cached package response is possible immediately after
            # publishing, even though npm's dedicated tag endpoint is current.
            return io.BytesIO(b'{"dist-tags":{"next":"0.0.0"}}')

        self.assertEqual(dist_tag("@antfly/embedded", "next", registry), "0.3.0-rc.1")

    def test_scoped_and_unscoped_package_names_are_encoded(self):
        for package, encoded in (
            ("@antfly/embedded", "%40antfly%2Fembedded"),
            ("antfly", "antfly"),
        ):
            with self.subTest(package=package):
                read = opener({"next": "0.3.0-rc.1"})
                self.assertEqual(dist_tag(package, "next", read), "0.3.0-rc.1")
                self.assertEqual(
                    read.call_args.args[0].full_url,
                    f"https://registry.npmjs.org/-/package/{encoded}/dist-tags",
                )

    def test_absent_tag_is_empty(self):
        self.assertIsNone(
            dist_tag("@antfly/embedded", "next", opener({"latest": "0.0.0"}))
        )
        self.assertIsNone(dist_tag("@antfly/embedded", "next", opener({})))

    def test_only_404_means_package_is_missing(self):
        for status in (401, 403, 429, 500, 503):
            read = Mock(
                side_effect=urllib.error.HTTPError(
                    "https://registry", status, "error", {}, io.BytesIO()
                )
            )
            with self.subTest(status=status), self.assertRaises(RegistryError):
                dist_tag("@antfly/embedded", "next", read)
            read.side_effect.close()
        error = urllib.error.HTTPError(
            "https://registry", 404, "missing", {}, io.BytesIO()
        )
        self.assertIsNone(dist_tag("@antfly/embedded", "next", Mock(side_effect=error)))
        error.close()

    def test_malformed_responses_cannot_be_treated_as_missing(self):
        for document in (
            [],
            {"next": None},
            {"next": 123},
            {"next": ""},
            {"dist-tags": {"next": "0.3.0-rc.1"}},
        ):
            with self.subTest(document=document), self.assertRaises(RegistryError):
                dist_tag("@antfly/embedded", "next", opener(document))
        with self.assertRaises(RegistryError):
            dist_tag(
                "@antfly/embedded", "next", Mock(return_value=io.BytesIO(b"not json"))
            )

    def test_transport_failure_is_not_an_absent_tag(self):
        for error in (TimeoutError("timeout"), urllib.error.URLError("unavailable")):
            with self.subTest(error=error), self.assertRaises(RegistryError):
                dist_tag("@antfly/embedded", "next", Mock(side_effect=error))

    def test_package_and_tag_are_required(self):
        for package, tag in (("", "next"), ("@antfly/embedded", "")):
            with self.assertRaises(RegistryError):
                dist_tag(package, tag, Mock())

    def test_integrity_still_checks_exact_immutable_version(self):
        read = opener(
            {"versions": {"0.3.0-rc.1": {"dist": {"integrity": "sha512-exact"}}}}
        )
        self.assertEqual(
            version_integrity("@antfly/embedded", "0.3.0-rc.1", read), "sha512-exact"
        )
        self.assertEqual(
            read.call_args.args[0].full_url,
            "https://registry.npmjs.org/%40antfly%2Fembedded",
        )
