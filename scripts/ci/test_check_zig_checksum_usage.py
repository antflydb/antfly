# Copyright 2026 Antfly, Inc.
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

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from check_zig_checksum_usage import check, violations


class ChecksumUsageTests(unittest.TestCase):
    def test_direct_and_namespace_aliases_are_rejected(self):
        for expression in (
            "std.hash.Crc32.hash(data)",
            "std.hash.crc.Crc32IsoHdlc.hash(data)",
            "std.hash.crc.Crc32Iscsi.hash(data)",
            "std.hash.crc.Crc64Nvme.hash(data)",
            "std.hash.Adler32.hash(data)",
            '@import("std").hash.Crc32.hash(data)',
            '@field(std.hash, "Crc32").hash(data)',
            'std.@"hash".@"Crc32".hash(data)',
        ):
            with self.subTest(expression=expression):
                self.assertTrue(
                    violations('const std = @import("std");\n' + expression)
                )
        self.assertTrue(
            violations(
                'const s = @import("std"); const h = s.hash; const c = h.crc; c.Crc32Iscsi.hash(data);'
            )
        )

    def test_generic_nvme_and_ieee_are_rejected_but_other_crcs_are_allowed(self):
        for width, polynomial, mask in (
            ("u64", "0xad93_d235_94c9_3659", "0xffff_ffff_ffff_ffff"),
            ("u32", "0x04c11db7", "0xffff_ffff"),
            ("u32", "0x1edc6f41", "0xffff_ffff"),
        ):
            source = (
                'const s = @import("std"); const C = s.hash.crc.Crc; C('
                + width
                + ", .{ .polynomial = "
                + polynomial
                + ", .initial = "
                + mask
                + ", .xor_output = "
                + mask
                + ", .reflect_input = true, .reflect_output = true"
                + " });"
            )
            self.assertEqual(len(violations(source)), 1)
            self.assertFalse(
                violations(
                    source.replace(".reflect_input = true", ".reflect_input = false")
                )
            )
            self.assertFalse(
                violations(source.replace(".xor_output = " + mask, ".xor_output = 0"))
            )
        self.assertFalse(
            violations(
                'const s = @import("std"); s.hash.crc.Crc(u16, .{ .polynomial = 0x8005 });'
            )
        )

    def test_comments_strings_and_nested_test_blocks_are_ignored(self):
        self.assertFalse(
            violations(r"""
const std = @import("std");
// std.hash.Crc32.hash(bytes)
const text = "std.hash.Crc32 }";
const multiline =
    \\std.hash.Adler32.hash(bytes) }
;
test "oracle" {
    const nested = struct { fn check() void { _ = std.hash.Crc32.hash("}"); } };
    _ = nested;
}
test { _ = std.hash.Adler32.hash("{"); }
""")
        )
        found = violations(
            'const std = @import("std");\ntest { _ = std.hash.Crc32.hash("}"); }\nconst bad = std.hash.Crc32.hash(data);'
        )
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0][0], 3)

    def test_unrelated_hashes_and_shared_api_are_allowed(self):
        self.assertFalse(
            violations(
                'const std = @import("std"); std.hash.Wyhash.hash(0, data); std.hash.XxHash64.hash(0, data); std.crypto.hash.sha2.Sha256.hash(data, &out, .{}); @import("antfly_hash").Crc32.hash(data);'
            )
        )

    def test_alias_scope_does_not_leak(self):
        source = 'const s = @import("std"); fn a() void { const h = s.hash; _ = h.Wyhash; } fn b() void { const h = unrelated; _ = h.Crc32; }'
        self.assertFalse(violations(source))

    def test_forward_container_declarations_are_resolved(self):
        sources = (
            'pub fn checksum(data: []const u8) u32 { return std.hash.Crc32.hash(data); }\nconst std = @import("std");',
            'pub fn checksum(data: []const u8) u32 { return h.Crc32.hash(data); }\nconst h = s.hash; const s = @import("std");',
            'const S = struct { pub fn checksum(data: []const u8) u32 { return h.Crc32.hash(data); }\nconst h = s.hash; }; const s = @import("std");',
            'const S = union(enum) { value: u32, pub fn checksum(data: []const u8) u32 { return h.Crc32.hash(data); }\nconst h = s.hash; }; const s = @import("std");',
        )
        for source in sources:
            with self.subTest(source=source):
                found = violations(source)
                self.assertEqual(len(found), 1)
                self.assertEqual(found[0][2], "Crc32")

    def test_explicitly_typed_namespace_aliases_are_resolved(self):
        for source in (
            'const s: type = @import("std"); const h: type = s.hash; h.Adler32.hash(data);',
            'pub fn checksum(data: []const u8) u32 { return h.Crc32.hash(data); } const h: type = std.hash; const std = @import("std");',
            'const s = @import("std"); fn checksum(data: []const u8) u32 { const h: type = s.hash; return h.crc.Crc32Iscsi.hash(data); }',
        ):
            with self.subTest(source=source):
                self.assertEqual(len(violations(source)), 1)

    def test_precollection_preserves_scope_and_test_exemptions(self):
        self.assertFalse(
            violations("""
const s = @import("std");
const h = unrelated;
fn checksum() void { _ = h.Crc32; }
fn local() void { const h2: type = s.hash; _ = h2.Wyhash; }
fn sibling() void { _ = h2.Crc32; }
test "oracle" { const h: type = s.hash; _ = h.Crc32.hash("data"); }
const S = struct { const h: type = s.hash; };
""")
        )
        self.assertFalse(violations("const a = b; const b = a;"))

    def test_checksum_types_in_annotations_are_still_rejected(self):
        source = 'const std = @import("std"); var crc: std.hash.Crc32 = .{};'
        self.assertEqual(len(violations(source)), 1)

    def test_cli_rejects_the_review_reproducer(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "zig").mkdir()
            (
                root / "zig/caller.zig"
            ).write_text("""pub fn checksum(data: []const u8) u32 { return h.Crc32.hash(data); }
const std = @import("std");
const h: type = std.hash;
test "oracle" { _ = std.hash.Crc32.hash("123456789"); }
""")
            result = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).with_name("check_zig_checksum_usage.py")),
                    "--root",
                    str(root),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("zig/caller.zig:1:", result.stderr)
            self.assertNotIn("zig/caller.zig:4:", result.stderr)

    def test_quoted_test_identifier_does_not_hide_production_code(self):
        source = 'const std = @import("std"); const @"test" = struct { const crc = std.hash.Crc32; };'
        self.assertTrue(violations(source))

    def test_cli_scans_production_and_exempts_only_the_checksum_implementation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "zig/lib/hash").mkdir(parents=True)
            source = 'const std = @import("std"); const Crc32 = std.hash.Crc32;'
            (root / "zig/lib/hash/oracle.zig").write_text(source)
            self.assertFalse(check(root))
            (root / "zig/caller.zig").write_text(source)
            command = [
                sys.executable,
                str(Path(__file__).with_name("check_zig_checksum_usage.py")),
                "--root",
                str(root),
            ]
            result = subprocess.run(
                command, capture_output=True, text=True, check=False
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("zig/caller.zig:1:", result.stderr)
            self.assertIn('@import("antfly_hash").Crc32', result.stderr)


if __name__ == "__main__":
    unittest.main()
