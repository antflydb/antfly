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

"""Regression tests for the Apache engine / ELv2 server source boundary."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import check_apache_boundary as boundary


class ApacheBoundaryTests(unittest.TestCase):
    def test_server_hot_standby_owners_keep_elv2(self):
        owners = boundary.server_only_sources()
        self.assertIn("storage/hot_standby/primary.zig", owners)
        self.assertIn("storage/metadata_hot_standby_port.zig", owners)
        for owner in owners:
            with self.subTest(owner=owner):
                self.assertEqual("elv2", boundary.group_for(boundary.SOURCE_ROOT + owner, "all"))

    def test_production_closure_excludes_explicit_server_test_owners(self):
        _, errors = self.check_fixture('test "server integration" { _ = @import("main.zig"); }\n'
            'const fixture = if (builtin.is_test) @import("main.zig") else struct {};')
        self.assertEqual(errors, [])
        _, errors = self.check_fixture('fn lazy() void { _ = @import("main.zig"); }')
        self.assertTrue(any("non-Apache dependency" in error for error in errors))

    def check_fixture(self, source: str, files: dict[str, str] | None = None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / boundary.SOURCE_ROOT / "lite_main.zig"
            path.parent.mkdir(parents=True)
            path.write_text(source)
            (path.parent / "main.zig").write_text("")
            wrapper = root / "zig/pkg/antfly-embedded/src/root.zig"
            wrapper.parent.mkdir(parents=True)
            wrapper.write_text("")
            for name, content in (files or {}).items():
                dependency = root / name
                dependency.parent.mkdir(parents=True, exist_ok=True)
                dependency.write_text(content)
            with (
                patch.object(boundary, "ENTRYPOINTS", ("lite_main.zig",)),
                patch.object(
                    boundary,
                    "PACKAGE_ENTRYPOINTS",
                    ("zig/pkg/antfly-embedded/src/root.zig",),
                ),
            ):
                return boundary.check_sources(root)

    def test_rejects_embedded_server_ui_font(self):
        _, errors = self.check_fixture(
            'const font = @embedFile("../antfarm/fonts/Aeonik.ttf");',
            {"zig/pkg/antfly/antfarm/fonts/Aeonik.ttf": "font bytes"},
        )
        self.assertTrue(any("unreviewed embedded font" in error for error in errors))

    def test_rejects_unreviewed_font_in_apache_directory(self):
        _, errors = self.check_fixture(
            'const font = @embedFile("../../../lib/pdf/fonts/new.ttf");',
            {"zig/lib/pdf/fonts/new.ttf": "font bytes"},
        )
        self.assertTrue(any("unreviewed embedded font" in error for error in errors))

    def test_rejects_embedded_server_ui_data(self):
        _, errors = self.check_fixture(
            'const logo = @embedFile("../antfarm/assets/logo.svg");',
            {"zig/pkg/antfly/antfarm/assets/logo.svg": "logo"},
        )
        self.assertTrue(any("outside Apache source scope" in error for error in errors))

    def test_embed_parser_ignores_comments_and_literals(self):
        self.assertEqual(
            boundary.source_embeds(
                '// @embedFile("ignored")\nconst s = "@embedFile(\\"ignored\\")";\nconst real = @embedFile(\n"font.ttf"\n);'
            ),
            ["font.ttf"],
        )

    def test_rejects_computed_dependency_paths(self):
        for source in (
            'const server = @import("ma" ++ "in.zig");',
            'const path = "main.zig"; const server = @import(path);',
            'const font = @embedFile("../antfarm/fonts/" ++ "Aeonik.ttf");',
            'const path = "font.ttf"; const font = @embedFile(path);',
            'const server = @import("ma\\x69n.zig");',
        ):
            with self.subTest(source=source):
                _, errors = self.check_fixture(source)
                self.assertTrue(any("unresolved @" in error for error in errors))

    def test_rejects_server_import_with_comments_between_tokens(self):
        for gaps in (
            (" // comment\n", "", ""),
            ("", " // comment\n", ""),
            ("", "", " // comment\n"),
            (" // first\n // second\n", " // argument\n", " // end\n"),
        ):
            with self.subTest(gaps=gaps):
                source = (
                    f'const server = @import{gaps[0]}({gaps[1]}"main.zig"{gaps[2]});'
                )
                _, errors = self.check_fixture(source)
                self.assertTrue(
                    any("non-Apache dependency" in error for error in errors)
                )

    def test_rejects_server_asset_with_comments_between_tokens(self):
        _, errors = self.check_fixture(
            "const logo = @embedFile // builtin\n( // argument\n"
            '"../antfarm/assets/logo.svg" // end\n);',
            {"zig/pkg/antfly/antfarm/assets/logo.svg": "logo"},
        )
        self.assertTrue(any("outside Apache source scope" in error for error in errors))

    def test_rejects_computed_paths_after_builtin_comments(self):
        for builtin in ("import", "embedFile"):
            with self.subTest(builtin=builtin):
                _, errors = self.check_fixture(
                    f"const dependency = @{builtin} // first\n // second\n(path);"
                )
                self.assertTrue(
                    any(f"unresolved @{builtin}" in error for error in errors)
                )

    def test_dependency_comments_preserve_literals_and_allowed_imports(self):
        source = (
            'const url = "https://example.com/@import(path)";\n'
            'const standard = @import // @embedFile("private.txt")\n'
            '( // @import("main.zig")\n"std" // @import(path)\n);\n'
        )
        self.assertEqual(["std"], boundary.source_imports(source))
        self.assertEqual([], boundary.source_embeds(source))
        _, errors = self.check_fixture(source)
        self.assertEqual([], errors)

    def test_comment_examples_cannot_replace_trailing_comma_dependencies(self):
        _, errors = self.check_fixture(
            'const server = @import // ("std")\n("main.zig",);'
        )
        self.assertTrue(any("non-Apache dependency" in error for error in errors))
        _, errors = self.check_fixture(
            'const logo = @embedFile // ("allowed.txt")\n'
            '("../antfarm/assets/logo.svg",);',
            {"zig/pkg/antfly/antfarm/assets/logo.svg": "logo"},
        )
        self.assertTrue(any("outside Apache source scope" in error for error in errors))

    def test_comment_examples_cannot_hide_escaped_or_computed_paths(self):
        for builtin in ("import", "embedFile"):
            for argument in ('"ma\\x69n.zig"', '"ma" ++ "in.zig"', "path"):
                with self.subTest(builtin=builtin, argument=argument):
                    source = f'const dependency = @{builtin} // ("std")\n({argument});'
                    self.assertEqual([], boundary.source_imports(source))
                    self.assertEqual([], boundary.source_embeds(source))
                    _, errors = self.check_fixture(source)
                    self.assertTrue(
                        any(f"unresolved @{builtin}" in error for error in errors)
                    )

    def test_token_scanner_keeps_quoted_and_multiline_contents_opaque(self):
        source = (
            'const quoted = "escaped \\" @import(\\"main.zig\\")";\n'
            "const char = '\\'';\n"
            'const @"@import" = 1;\n'
            "const multiline =\n"
            '  \\\\ @import("main.zig") // literal contents\n'
            '  \\\\ @embedFile("private.txt")\n;\n'
            'const standard = @import // ("main.zig")\n("std", // end\n);\n'
        )
        self.assertEqual(["std"], boundary.source_imports(source))
        self.assertEqual([], boundary.source_embeds(source))
        _, errors = self.check_fixture(source)
        self.assertEqual([], errors)

    def test_unresolved_dependency_diagnostic_preserves_source_line(self):
        _, errors = self.check_fixture(
            '// first line\n\nconst dependency = @import // ("std")\n("ma\\x69n.zig");'
        )
        self.assertTrue(any("lite_main.zig:3;" in error for error in errors))

    def test_unresolved_parser_ignores_comments_and_literals(self):
        _, errors = self.check_fixture(
            '// @import(path)\nconst example = "@embedFile(path)";\n'
            "const multiline =\n  \\\\ @import(path)\n;\n"
        )
        self.assertEqual([], errors)

    def test_rejects_server_source_import(self):
        _, errors = self.check_fixture('const server = @import("main.zig");')
        self.assertTrue(any("main.zig" in error for error in errors))

    def test_rejects_unreviewed_named_module(self):
        _, errors = self.check_fixture(
            'const server = @import("database_http_server");'
        )
        self.assertTrue(any("unreviewed module" in error for error in errors))

    def test_rejects_server_only_generated_api(self):
        for module in ("antfly_admin_openapi", "antfly_metadata_server_openapi", "antfly_usermgr_server_openapi", "antfly_public_server_openapi"):
            with self.subTest(module=module):
                _, errors = self.check_fixture(
                    f'const server = @import("{module}");',
                    {
                        f"zig/pkg/antfly-server-api/src/openapi/generated/{module}/root.zig": "",
                    },
                )
                self.assertTrue(
                    any("embedded product imports server-only API" in error for error in errors)
                )

    def test_rejects_server_import_inside_shared_module(self):
        seen, errors = self.check_fixture(
            'const httpx = @import("httpx");',
            {
                "zig/lib/httpx/src/httpx.zig": 'const server = @import("../../../pkg/antfly/src/main.zig");'
            },
        )
        self.assertIn("zig/lib/httpx/src/httpx.zig", seen)
        self.assertTrue(
            any(
                "non-Apache dependency" in error and "main.zig" in error
                for error in errors
            )
        )

    def test_rejects_missing_named_source_module(self):
        _, errors = self.check_fixture('const httpx = @import("httpx");')
        self.assertTrue(
            any(
                "missing source: zig/lib/httpx/src/httpx.zig" in error
                for error in errors
            )
        )

    def test_rejects_conflicting_notice_inside_shared_module(self):
        _, errors = self.check_fixture(
            'const httpx = @import("httpx");',
            {
                "zig/lib/httpx/src/httpx.zig": "// SPDX-License-Identifier: Elastic-2.0\n"
            },
        )
        self.assertTrue(any("conflicting ELv2 notice" in error for error in errors))

    def test_rejects_unreviewed_module_inside_shared_module(self):
        _, errors = self.check_fixture(
            'const httpx = @import("httpx");',
            {
                "zig/lib/httpx/src/httpx.zig": 'const server = @import("new_database_server");'
            },
        )
        self.assertTrue(any("unreviewed module" in error for error in errors))

    def test_source_module_cycles_terminate(self):
        seen, errors = self.check_fixture(
            'const httpx = @import("httpx");',
            {
                "zig/lib/httpx/src/httpx.zig": 'const json = @import("antfly-json");',
                "zig/lib/json/src/mod.zig": 'const httpx = @import("httpx");',
            },
        )
        self.assertIn("zig/lib/json/src/mod.zig", seen)
        self.assertEqual([], errors)

    def test_rejects_named_module_symlink_outside_product_tree(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            tempfile.TemporaryDirectory() as external,
        ):
            root = Path(directory)
            source = root / boundary.SOURCE_ROOT / "lite_main.zig"
            source.parent.mkdir(parents=True)
            source.write_text('const httpx = @import("httpx");')
            outside = Path(external) / "httpx.zig"
            outside.write_text('const std = @import("std");')
            shared = root / "zig/lib/httpx/src/httpx.zig"
            shared.parent.mkdir(parents=True)
            shared.symlink_to(outside)
            with (
                patch.object(boundary, "ENTRYPOINTS", ("lite_main.zig",)),
                patch.object(boundary, "PACKAGE_ENTRYPOINTS", ()),
            ):
                _, errors = boundary.check_sources(root)
            self.assertTrue(
                any("source outside product tree" in error for error in errors)
            )

    def test_focused_inference_entrypoint_is_checked(self):
        self.assertIn("runtime_inference_main.zig", boundary.ENTRYPOINTS)
        self.assertEqual(
            "apache",
            boundary.group_for(
                boundary.SOURCE_ROOT + "runtime_inference_main.zig", "all"
            ),
        )
        self.assertEqual(
            "elv2",
            boundary.group_for(
                boundary.SOURCE_ROOT + "runtime_artifact_main.zig", "all"
            ),
        )

    def test_rejects_missing_source(self):
        _, errors = self.check_fixture('const helper = @import("missing.zig");')
        self.assertTrue(any("missing source" in error for error in errors))

    def test_rejects_conflicting_notice_in_apache_dependency(self):
        _, errors = self.check_fixture(
            '// SPDX-License-Identifier: ELv2\nconst std = @import("std");'
        )
        self.assertTrue(any("conflicting ELv2 notice" in error for error in errors))

    def test_ignores_commented_imports(self):
        _, errors = self.check_fixture(
            '// @import("main.zig")\nconst std = @import("std");'
        )
        self.assertEqual([], errors)

    def test_rejects_server_import_after_quoted_url(self):
        _, errors = self.check_fixture(
            'const url = "https://example.com"; const server = @import("main.zig");'
        )
        self.assertTrue(any("main.zig" in error for error in errors))

    def test_rejects_multiline_import(self):
        _, errors = self.check_fixture('const server = @import(\n "main.zig"\n);')
        self.assertTrue(any("main.zig" in error for error in errors))

    def test_ignores_imports_in_string_literals(self):
        source = (
            'const sample = "@import(\\"main.zig\\")";\n'
            'const quote = \'"\'; const std = @import("std");\n'
            'const multiline =\n  \\\\ @import("main.zig")\n;\n'
        )
        self.assertEqual(["std"], boundary.source_imports(source))
        _, errors = self.check_fixture(source)
        self.assertEqual([], errors)

    def test_repository_owner_dependencies_are_apache(self):
        _, errors = boundary.check_sources()
        self.assertEqual([], errors)


class LicenseNoticeTests(unittest.TestCase):
    def test_short_notices_are_replaced_without_losing_source_docs(self):
        body = '//! Engine documentation.\nconst std = @import("std");\n'
        header = boundary.read_header("apache")
        for notice in (
            "// Copyright 2026 Antfly, Inc.\n// SPDX-License-Identifier: LicenseRef-Elastic-2.0\n\n",
            "// Copyright 2026 Antfly, Inc.\n// Licensed under the Elastic License 2.0 (ELv2).\n\n",
            "// Copyright 2026 Antfly, Inc.\n//\n// Licensed under the Elastic License 2.0 (ELv2); you may not use this file\n// except in compliance with the Elastic License 2.0.\n\n",
        ):
            with self.subTest(notice=notice):
                result = boundary.apply_header(
                    notice + body, Path("engine.zig"), header
                )
                self.assertTrue(result.endswith(body))
                self.assertNotIn("Elastic", result)
                self.assertEqual(
                    result, boundary.apply_header(result, Path("engine.zig"), header)
                )

    def test_conflicting_duplicate_notices_are_removed(self):
        header = boundary.read_header("apache")
        path = Path("engine.zig")
        body = "pub const value = 1;\n"
        canonical = boundary.apply_header(body, path, header)
        duplicate = (
            "// Copyright 2026 Antfly, Inc.\n// SPDX-License-Identifier: ELv2\n\n"
        )
        self.assertEqual(
            canonical,
            boundary.apply_header(
                canonical.removesuffix(body) + duplicate + body, path, header
            ),
        )


if __name__ == "__main__":
    unittest.main()
