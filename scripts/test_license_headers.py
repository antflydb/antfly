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

"""Regression tests for header policy and retained upstream licenses."""

from __future__ import annotations

import argparse
import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import license_headers as policy


class LicenseHeaderTests(unittest.TestCase):
    def test_product_groups(self):
        self.assertEqual(
            policy.group_for("zig/pkg/antfly-embedded/src/local/lite_main.zig", "all"),
            "apache",
        )
        self.assertEqual(policy.group_for("zig/pkg/antfly/src/main.zig", "all"), "elv2")
        self.assertEqual(
            policy.group_for("zig/pkg/inference/src/main.zig", "all"), "apache"
        )
        self.assertEqual(policy.group_for("zig/lib/lmdb/src/root.zig", "all"), "apache")
        self.assertIsNone(policy.group_for("zig/deps/lmdb/mdb.c", "all"))
        self.assertEqual(
            (policy.ROOT / "zig/lib/lmdb/LICENSE").read_bytes(),
            (policy.ROOT / "LICENSES/Apache-2.0.txt").read_bytes(),
        )

    def test_physical_package_roots_have_no_server_exceptions(self):
        for name in policy.APACHE_FILES:
            self.assertFalse(name.startswith("zig/pkg/antfly/"), name)
        for name in (
            "zig/pkg/antfly-embedded/src/local/storage/db/db.zig",
            "zig/pkg/antfly-embedded/src/local/lake.zig",
            "zig/lib/credentials/src/aws.zig",
            "zig/build_support/antfly/dependencies.zig",
        ):
            self.assertEqual(policy.group_for(name, "all"), "apache")
        self.assertEqual(
            policy.group_for(
                "zig/pkg/antfly/src/storage/hot_standby/primary.zig", "all"
            ),
            "elv2",
        )

    def test_preserves_shebang_and_is_idempotent(self):
        source = "#!/usr/bin/env python3\nprint('hello')\n"
        path = Path("example.py")
        header = policy.read_header("apache")
        updated = policy.apply_header(source, path, header)
        self.assertTrue(updated.startswith("#!/usr/bin/env python3\n# Copyright"))
        self.assertTrue(updated.endswith("print('hello')\n"))
        self.assertEqual(policy.apply_header(updated, path, header), updated)

    def test_replaces_stale_header_without_losing_zig_docs(self):
        source = "// Copyright 2026 Antfly, Inc.\n// SPDX-License-Identifier: Elastic-2.0\n//! Module docs\nconst x = 1;\n"
        updated = policy.apply_header(
            source, Path("example.zig"), policy.read_header("apache")
        )
        self.assertNotIn("Elastic", updated)
        self.assertTrue(updated.endswith("//! Module docs\nconst x = 1;\n"))

    def test_preserves_usage_comments_adjacent_to_full_notice(self):
        path = Path("example.sh")
        usage = "#\n# Usage: example.sh --help\n# More instructions.\n\nexit 0\n"
        source = (
            policy.render_header(path, policy.read_header("elv2")).rstrip("\n")
            + "\n"
            + usage
        )
        updated = policy.apply_header(source, path, policy.read_header("apache"))
        self.assertTrue(updated.endswith(usage))
        self.assertEqual(
            updated, policy.apply_header(updated, path, policy.read_header("apache"))
        )

    def test_does_not_rewrite_upstream_or_combined_notices(self):
        for name in policy.PRESERVED_NOTICES:
            with self.subTest(name=name):
                self.assertTrue(policy.excluded(name))
        self.assertTrue(policy.excluded("zig/lib/httpx/src/httpx.zig"))

    def test_excludes_generator_owned_output(self):
        self.assertTrue(
            policy.excluded("zig/pkg/antfly/antfarm/assets/index-example.js")
        )

    def test_selected_paths_only_update_requested_output(self):
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first.go"
            second = Path(directory) / "second.go"
            first.write_text("package example\n")
            second.write_text("package example\n")
            args = argparse.Namespace(
                group="apache", check=False, verbose=False, paths=[str(first)]
            )
            with (
                patch.object(policy, "ROOT", Path(directory)),
                patch.object(policy, "parse_args", return_value=args),
                patch.object(policy, "check_preserved_notices", return_value=[]),
                patch.object(policy, "check_frozen_helpers", return_value=[]),
                patch.object(policy, "check_asset_records", return_value=[]),
                patch.object(
                    policy,
                    "discover",
                    return_value=[(first, "apache"), (second, "apache")],
                ),
            ):
                self.assertEqual(policy.main(), 0)
            self.assertIn("Licensed under the Apache", first.read_text())
            self.assertEqual(second.read_text(), "package example\n")

    def test_selected_path_outside_policy_fails(self):
        args = argparse.Namespace(
            group="apache", check=False, verbose=False, paths=["outside.go"]
        )
        with (
            patch.object(policy, "parse_args", return_value=args),
            patch.object(policy, "check_preserved_notices", return_value=[]),
            patch.object(policy, "check_frozen_helpers", return_value=[]),
            patch.object(policy, "check_asset_records", return_value=[]),
            patch.object(policy, "discover", return_value=[]),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(policy.main(), 1)

    def test_complete_upstream_notice_and_bundle_are_required(self):
        names = (
            "zig/lib/hash/src/sha256.zig",
            "zig/deps/lmdb/mdb.c",
            "zig/pkg/inference/src/ops/cuda/artifacts/gliner25_training_math.cu",
            "zig/pkg/inference/src/pipelines/extraction_assignment.zig",
            "zig/pkg/inference/licenses/scipy-rectangular-lsap.txt",
        )
        for name in names:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                definition = policy.PRESERVED_NOTICES[name]
                path = root / name
                path.parent.mkdir(parents=True)
                source = (policy.ROOT / name).read_text()
                canonical = root / definition["file"]
                canonical.parent.mkdir(parents=True, exist_ok=True)
                notice = (policy.ROOT / definition["file"]).read_text()
                canonical.write_text(notice)
                bundle = root / "THIRD_PARTY_NOTICES.md"
                bundle.write_text(notice)
                with (
                    patch.object(policy, "ROOT", root),
                    patch.object(policy, "PRESERVED_NOTICES", {name: definition}),
                ):
                    self.assertTrue(policy.check_preserved_notices("all"))
                    path.write_text(source)
                    self.assertEqual(policy.check_preserved_notices("all"), [])
                    # Implementation changes do not invalidate a retained notice.
                    path.write_text(source + "\n// implementation change\n")
                    self.assertEqual(policy.check_preserved_notices("all"), [])
                    path.write_text(
                        source.replace("Permission", "Changed permission").replace(
                            "Redistribution", "Changed redistribution"
                        )
                    )
                    self.assertTrue(policy.check_preserved_notices("all"))
                    path.write_text(source)
                    bundle.write_text(
                        notice.replace("Permission", "Changed permission").replace(
                            "Redistribution", "Changed redistribution"
                        )
                    )
                    self.assertTrue(policy.check_preserved_notices("all"))
                    self.assertEqual(policy.check_preserved_notices("elv2"), [])

    def test_deleted_mit_retention_condition_is_rejected(self):
        name = "zig/lib/hash/src/sha256.zig"
        definition = policy.PRESERVED_NOTICES[name]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / name
            path.parent.mkdir(parents=True)
            source = (policy.ROOT / name).read_text()
            source = source.replace(
                "// The above copyright notice and this permission notice shall be included in\n// all copies or substantial portions of the Software.\n",
                "",
            )
            path.write_text(source)
            canonical = root / definition["file"]
            canonical.parent.mkdir(parents=True)
            notice = (policy.ROOT / definition["file"]).read_text()
            canonical.write_text(notice)
            (root / "THIRD_PARTY_NOTICES.md").write_text(notice)
            with (
                patch.object(policy, "ROOT", root),
                patch.object(policy, "PRESERVED_NOTICES", {name: definition}),
            ):
                self.assertTrue(
                    any(
                        "preserved license notice" in error
                        for error in policy.check_preserved_notices("all")
                    )
                )


if __name__ == "__main__":
    unittest.main()
