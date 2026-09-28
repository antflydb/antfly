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

"""Regression guards for asset provenance and complete generation notices."""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import asset_licenses as assets
import generated_source_licenses as generated


class AssetLicenseTests(unittest.TestCase):
    def test_repository_asset_pins_and_notices(self):
        self.assertEqual(assets.check_asset_records(), [])

    def test_fonts_require_explicit_review_even_in_apache_directories(self):
        self.assertIn(
            "unreviewed embedded font",
            assets.check_embedded_asset("zig/lib/pdf/fonts/unreviewed.ttf", True),
        )
        self.assertIsNone(
            assets.check_embedded_asset(
                "zig/lib/pdf/fonts/roboto/Roboto-Regular.ttf", True
            )
        )

    def test_renamed_font_still_requires_explicit_review(self):
        name = "zig/lib/pdf/fonts/renamed.bin"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / name
            path.parent.mkdir(parents=True)
            path.write_bytes(
                (
                    assets.ROOT / "zig/lib/pdf/fonts/roboto/Roboto-Regular.ttf"
                ).read_bytes()
            )
            self.assertIn(
                "unreviewed embedded font",
                assets.check_embedded_asset(name, True, root),
            )

    def test_server_ui_assets_are_not_apache_dependencies(self):
        self.assertIn(
            "outside Apache",
            assets.check_embedded_asset(
                "zig/pkg/antfly/antfarm/assets/logo.png", False
            ),
        )
        self.assertIsNone(
            assets.check_embedded_asset("zig/lib/pdf/testdata/example.pdf", True)
        )

    def test_modified_font_and_missing_notice_fail(self):
        name = "zig/lib/pdf/fonts/roboto/Roboto-Regular.ttf"
        record = assets.ASSETS[name]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / name
            path.parent.mkdir(parents=True)
            path.write_bytes((assets.ROOT / name).read_bytes())
            for key in ("notice", "license_text"):
                target = root / record[key]
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes((assets.ROOT / record[key]).read_bytes())
            with patch.object(assets, "ASSETS", {name: record}):
                self.assertEqual(assets.check_asset_records(root), [])
                path.write_bytes(path.read_bytes() + b"changed")
                self.assertTrue(
                    any(
                        "identity differs" in error
                        for error in assets.check_asset_records(root)
                    )
                )
                (root / record["notice"]).unlink()
                self.assertTrue(
                    any(
                        "license file" in error
                        for error in assets.check_asset_records(root)
                    )
                )

    def test_unicode_generator_preserves_the_complete_notice(self):
        source = assets.ROOT / "zig/lib/tokenizer/tools/generate_unicode_classes.py"
        spec = importlib.util.spec_from_file_location(
            "unicode_classes_license_test", source
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / "UnicodeData.txt"
            properties = root / "PropList.txt"
            output = root / "classes.zig"
            data.write_text("0041;LATIN CAPITAL LETTER A;Lu;0;L;;;;;N;;;;0061;\n")
            properties.write_text("0020 ; White_Space\n")
            with patch.object(
                module.sys,
                "argv",
                [str(source), "16.0.0", str(data), str(properties), str(output)],
            ):
                module.main()
            expected = (assets.ROOT / "LICENSES/third-party/Unicode-V3.txt").read_text()
            rendered = output.read_text()
            for line in expected.splitlines():
                if line:
                    self.assertIn("// " + line, rendered)
            self.assertTrue(
                rendered.startswith(generated.unicode_source_header(str(source)))
            )


if __name__ == "__main__":
    unittest.main()
