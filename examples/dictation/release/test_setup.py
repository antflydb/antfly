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

"""Signing setup regressions use synthetic keys and no Keychain or GitHub writes."""

import base64
import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import release as r

spec = importlib.util.spec_from_file_location(
    "dictation_setup", Path(__file__).with_name("setup-github.py")
)
setup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(setup)
KEY = base64.b64encode(bytes([1]) * 32).decode()
OTHER_KEY = base64.b64encode(bytes([2]) * 32).decode()
PACKAGE = Path(__file__).resolve().parents[1]


class SigningIdentityTests(unittest.TestCase):
    def setUp(self):
        reader = patch.object(setup, "public_signing_key", return_value=None)
        self.public_reader = reader.start()
        self.addCleanup(reader.stop)

    def test_r2_identity_cannot_be_replaced_by_a_conflicting_github_variable(self):
        self.public_reader.return_value = OTHER_KEY
        with patch.object(setup, "gh_api", return_value={"value": KEY}):
            with self.assertRaisesRegex(ValueError, "identities differ"):
                setup.existing_signing_key()

    def test_missing_github_variable_does_not_bootstrap_over_r2(self):
        self.public_reader.return_value = KEY
        with patch.object(setup, "gh_api", side_effect=self.missing_variable()):
            with self.assertRaisesRegex(ValueError, "existing R2"):
                setup.existing_signing_key()

    def test_matching_key_is_reused_without_generating_or_overwriting_it(self):
        with (
            patch.object(setup, "existing_signing_key", return_value=KEY),
            patch.object(setup.subprocess, "run") as run,
            patch.object(setup.subprocess, "check_output", return_value=KEY + "\n"),
            patch.object(setup, "gh_api") as api,
        ):
            self.assertEqual(setup.prepare_signing_key(PACKAGE), KEY)
            run.assert_not_called()
            api.assert_not_called()

    def test_new_mac_must_restore_the_existing_private_key(self):
        with (
            patch.object(setup, "existing_signing_key", return_value=KEY),
            patch.object(setup.subprocess, "run") as run,
            patch.object(
                setup.subprocess,
                "check_output",
                side_effect=subprocess.CalledProcessError(1, ["generate_keys", "-p"]),
            ),
            patch.object(setup, "gh_api") as api,
        ):
            with self.assertRaisesRegex(ValueError, "--sparkle-key"):
                setup.prepare_signing_key(PACKAGE)
            run.assert_not_called()
            api.assert_not_called()

    def test_mismatched_local_key_cannot_change_github_configuration(self):
        with (
            patch.object(setup, "existing_signing_key", return_value=KEY),
            patch.object(setup.subprocess, "run") as run,
            patch.object(setup.subprocess, "check_output", return_value=OTHER_KEY),
            patch.object(setup, "gh_api") as api,
        ):
            with self.assertRaisesRegex(ValueError, "differs from the deployed key"):
                setup.prepare_signing_key(PACKAGE)
            run.assert_not_called()
            api.assert_not_called()

    def test_first_setup_reserves_key_using_create_only_api(self):
        with (
            patch.object(setup, "existing_signing_key", side_effect=[None, None]),
            patch.object(setup.subprocess, "run") as run,
            patch.object(setup.subprocess, "check_output", return_value=KEY),
            patch.object(setup, "gh_api") as api,
        ):
            self.assertEqual(setup.prepare_signing_key(PACKAGE), KEY)
            run.assert_called_once()
            self.assertNotIn("-f", run.call_args.args[0])
            api.assert_called_once_with(
                f"repos/{r.POLICY['repository']}/actions/variables",
                "POST",
                {"name": setup.KEY_VARIABLE, "value": KEY},
            )

    def test_concurrent_initializer_cannot_replace_a_reserved_identity(self):
        with (
            patch.object(setup, "existing_signing_key", side_effect=[None, OTHER_KEY]),
            patch.object(setup.subprocess, "run"),
            patch.object(setup.subprocess, "check_output", return_value=KEY),
            patch.object(setup, "gh_api") as api,
        ):
            with self.assertRaisesRegex(ValueError, "changed during setup"):
                setup.prepare_signing_key(PACKAGE)
            api.assert_not_called()

    def test_create_conflict_cannot_overwrite_a_concurrent_bootstrap(self):
        with (
            patch.object(setup, "existing_signing_key", side_effect=[None, None]),
            patch.object(setup.subprocess, "run"),
            patch.object(setup.subprocess, "check_output", return_value=KEY),
            patch.object(
                setup,
                "gh_api",
                side_effect=r.GitHubError("fixture", "Conflict (HTTP 409)"),
            ) as api,
        ):
            with self.assertRaises(r.GitHubError):
                setup.prepare_signing_key(PACKAGE)
            self.assertEqual(api.call_args.args[1], "POST")

    def test_wrong_backup_is_rejected_before_keychain_import(self):
        with tempfile.TemporaryDirectory() as directory:
            backup = Path(directory) / "fixture-seed"
            backup.write_bytes(b"synthetic private seed")
            with (
                patch.object(setup, "existing_signing_key", return_value=KEY),
                patch.object(setup, "derive_public_key", return_value=OTHER_KEY),
                patch.object(setup.subprocess, "run") as run,
                patch.object(setup, "gh_api") as api,
            ):
                with self.assertRaisesRegex(ValueError, "backup does not match"):
                    setup.prepare_signing_key(PACKAGE, backup)
                run.assert_not_called()
                api.assert_not_called()

    def test_original_backup_can_be_imported_without_replacing_remote_key(self):
        with tempfile.TemporaryDirectory() as directory:
            backup = Path(directory) / "fixture-seed"
            backup.write_bytes(b"synthetic private seed")
            with (
                patch.object(setup, "existing_signing_key", return_value=KEY),
                patch.object(setup, "derive_public_key", return_value=KEY),
                patch.object(
                    setup.subprocess,
                    "check_output",
                    side_effect=[
                        subprocess.CalledProcessError(1, ["generate_keys", "-p"]),
                        KEY,
                    ],
                ),
                patch.object(setup.subprocess, "run") as run,
                patch.object(setup, "gh_api") as api,
            ):
                self.assertEqual(setup.prepare_signing_key(PACKAGE, backup), KEY)
                self.assertEqual(run.call_args.args[0][-2:], ["-f", str(backup)])
                api.assert_not_called()

    def test_restoring_an_already_present_matching_backup_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            backup = Path(directory) / "fixture-seed"
            backup.write_bytes(b"synthetic private seed")
            with (
                patch.object(setup, "existing_signing_key", return_value=KEY),
                patch.object(setup, "derive_public_key", return_value=KEY),
                patch.object(setup.subprocess, "check_output", return_value=KEY),
                patch.object(setup.subprocess, "run") as run,
                patch.object(setup, "gh_api") as api,
            ):
                self.assertEqual(setup.prepare_signing_key(PACKAGE, backup), KEY)
                run.assert_not_called()
                api.assert_not_called()

    def test_restore_does_not_overwrite_a_conflicting_local_key(self):
        with tempfile.TemporaryDirectory() as directory:
            backup = Path(directory) / "fixture-seed"
            backup.write_bytes(b"synthetic private seed")
            with (
                patch.object(setup, "existing_signing_key", return_value=KEY),
                patch.object(setup, "derive_public_key", return_value=KEY),
                patch.object(setup.subprocess, "check_output", return_value=OTHER_KEY),
                patch.object(setup.subprocess, "run") as run,
                patch.object(setup, "gh_api") as api,
            ):
                with self.assertRaisesRegex(ValueError, "conflicting entry"):
                    setup.prepare_signing_key(PACKAGE, backup)
                run.assert_not_called()
                api.assert_not_called()

    def missing_variable(self):
        return r.GitHubError("fixture", "Not Found (HTTP 404)")

    def test_existing_public_key_requires_only_one_metadata_read(self):
        with patch.object(setup, "gh_api", return_value={"value": KEY}) as api:
            self.assertEqual(setup.existing_signing_key(), KEY)
            self.assertEqual(api.call_count, 1)

    def test_missing_key_for_existing_feed_cannot_bootstrap(self):
        with patch.object(
            setup,
            "gh_api",
            side_effect=[
                self.missing_variable(),
                [{"ref": "refs/heads/dictation-updates"}],
            ],
        ):
            with self.assertRaisesRegex(ValueError, "existing update feed"):
                setup.existing_signing_key()

    def test_missing_key_checks_all_release_pages_including_drafts(self):
        with patch.object(
            setup,
            "gh_api",
            side_effect=[
                self.missing_variable(),
                [],
                [{"tag_name": "other/v1"}] * 100,
                [{"tag_name": "dictation/v1.0.0", "draft": True}],
            ],
        ):
            with self.assertRaisesRegex(ValueError, "existing dictation release"):
                setup.existing_signing_key()

    def test_bootstrap_requires_no_existing_feed_or_release(self):
        with patch.object(
            setup, "gh_api", side_effect=[self.missing_variable(), [], []]
        ):
            self.assertIsNone(setup.existing_signing_key())

    def test_permission_failure_cannot_be_mistaken_for_bootstrap(self):
        with patch.object(
            setup,
            "gh_api",
            side_effect=r.GitHubError("fixture", "Forbidden (HTTP 403)"),
        ) as api:
            with self.assertRaises(r.GitHubError):
                setup.existing_signing_key()
            self.assertEqual(api.call_count, 1)

    def test_export_mismatch_cannot_upload_any_credential(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".build").mkdir()
            cert = root / "fixture.p12"
            cert.write_bytes(b"not a certificate")
            api_key = root / "fixture.p8"
            api_key.write_bytes(b"not an API key")
            argv = [
                "setup-github.py",
                "--certificate",
                str(cert),
                "--api-key",
                str(api_key),
                "--api-key-id",
                "fixture",
                "--issuer-id",
                "fixture",
                "--team-id",
                "ABCDE12345",
                "--identity",
                "Developer ID Application: Fixture (ABCDE12345)",
            ]

            def run(args, **kwargs):
                if "-x" in args:
                    Path(args[args.index("-x") + 1]).write_bytes(b"fixture")

            with (
                patch.object(sys, "argv", argv),
                patch.object(setup, "__file__", str(root / "release/setup-github.py")),
                patch.object(setup, "environment"),
                patch.object(setup, "prepare_signing_key", return_value=KEY),
                patch.object(setup, "derive_public_key", return_value=OTHER_KEY),
                patch.object(setup.subprocess, "run", side_effect=run),
                patch.object(setup.getpass, "getpass", return_value="fixture-password"),
                patch.object(setup, "gh") as gh,
                patch("builtins.print"),
            ):
                with self.assertRaisesRegex(ValueError, "No credentials were uploaded"):
                    setup.main()
                gh.assert_not_called()
            self.assertEqual(list((root / ".build").iterdir()), [])

    def test_matching_export_uploads_credentials_without_replacing_public_identity(
        self,
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".build").mkdir()
            cert = root / "fixture.p12"
            cert.write_bytes(b"not a certificate")
            api_key = root / "fixture.p8"
            api_key.write_bytes(b"not an API key")
            argv = [
                "setup-github.py",
                "--certificate",
                str(cert),
                "--api-key",
                str(api_key),
                "--api-key-id",
                "fixture",
                "--issuer-id",
                "fixture",
                "--team-id",
                "ABCDE12345",
                "--identity",
                "Developer ID Application: Fixture (ABCDE12345)",
            ]

            def run(args, **kwargs):
                if "-x" in args:
                    Path(args[args.index("-x") + 1]).write_bytes(b"fixture")

            with (
                patch.object(sys, "argv", argv),
                patch.object(setup, "__file__", str(root / "release/setup-github.py")),
                patch.object(setup, "environment"),
                patch.object(setup, "prepare_signing_key", return_value=KEY),
                patch.object(setup, "derive_public_key", return_value=KEY),
                patch.object(setup, "existing_signing_key", return_value=KEY),
                patch.object(setup.subprocess, "run", side_effect=run),
                patch.object(setup.getpass, "getpass", return_value="fixture-password"),
                patch.object(setup, "gh") as gh,
                patch("builtins.print"),
            ):
                setup.main()
                secret_calls = [
                    call
                    for call in gh.call_args_list
                    if call.args[:2] == ("secret", "set")
                ]
                self.assertEqual(len(secret_calls), 6)
                self.assertNotIn(
                    setup.KEY_VARIABLE,
                    [arg for call in gh.call_args_list for arg in call.args],
                )
            self.assertEqual(list((root / ".build").iterdir()), [])


if __name__ == "__main__":
    unittest.main()
