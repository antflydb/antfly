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

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import embedded_registry_preflight as embedded
from registry.model import RegistryError


class EmbeddedRegistryPreflightTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in embedded.NPM_PACKAGES.values():
            (self.root / f"{name}-0.3.0-rc.1.tgz").write_bytes(b"sealed npm bytes")
        self.verify = self.start_patch("verify")
        self.tags = self.start_patch("dist_tag", return_value="0.0.0")
        self.npm = self.start_patch("preflight_package")
        self.expected = self.start_patch(
            "expected_release_files", return_value={"a.whl": "a" * 64}
        )
        self.pypi = self.start_patch("pypi_release_files", return_value={})
        self.missing = self.start_patch("missing_registry_files")

    def start_patch(self, name, **kwargs):
        patcher = patch.object(embedded, name, **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def preflight(self, version="0.3.0-rc.1", tag="next"):
        embedded.preflight(self.root, version, "a" * 40, tag)

    def test_rc_checks_all_npm_bytes_and_pep440_wheels(self):
        self.preflight()
        self.assertEqual(self.npm.call_count, 4)
        self.assertEqual(self.tags.call_count, 4)
        self.pypi.assert_called_once_with("antfly-embedded", "0.3.0rc1")
        self.missing.assert_called_once_with(self.expected.return_value, {})

    def test_older_build_cannot_roll_back_next_or_latest(self):
        for version, observed, tag in (
            ("0.3.0-rc.1", "0.3.0-rc.2", "next"),
            ("0.3.0", "0.3.1", "latest"),
        ):
            with self.subTest(tag=tag):
                self.tags.return_value = observed
                with self.assertRaisesRegex(ValueError, "backward"):
                    self.preflight(version, tag)
        self.npm.assert_not_called()
        self.pypi.assert_not_called()

    def test_newer_rc_and_exact_retry_are_allowed(self):
        for observed in (None, "0.0.0", "0.3.0-rc.0", "0.3.0-rc.1"):
            with self.subTest(observed=observed):
                self.tags.return_value = observed
                self.preflight()

    def test_bad_snapshot_stops_all_registry_operations(self):
        self.verify.side_effect = ValueError("snapshot digest differs")
        with self.assertRaisesRegex(ValueError, "digest"):
            self.preflight()
        self.tags.assert_not_called()
        self.pypi.assert_not_called()

    def test_registry_network_failure_is_not_missing(self):
        self.tags.side_effect = RegistryError("HTTP 503")
        with self.assertRaisesRegex(RegistryError, "503"):
            self.preflight()
        self.npm.assert_not_called()
        self.pypi.assert_not_called()

    def test_immutable_npm_collision_prevents_wheel_publication(self):
        self.npm.side_effect = SystemExit("different contents")
        with self.assertRaisesRegex(SystemExit, "different contents"):
            self.preflight()
        self.pypi.assert_not_called()

    def test_immutable_wheel_collision_is_not_skipped(self):
        self.missing.side_effect = SystemExit("wheel digest differs")
        with self.assertRaisesRegex(SystemExit, "wheel digest"):
            self.preflight()

    def test_nightlies_cannot_publish_registry_versions(self):
        with self.assertRaisesRegex(ValueError, "latest or next"):
            self.preflight(tag="nightly")
        self.tags.assert_not_called()
