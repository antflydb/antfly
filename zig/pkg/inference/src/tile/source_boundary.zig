// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Shared generated-source headers and Metal C-string encoding.
const std = @import("std");

/// Canonical notice for generated first-party source outside generated directories.
pub const apache_header =
    \\// Copyright 2026 Antfly, Inc.
    \\// SPDX-License-Identifier: Apache-2.0
    \\//
    \\// Licensed under the Apache License, Version 2.0 (the "License");
    \\// you may not use this file except in compliance with the License.
    \\// You may obtain a copy of the License at
    \\//
    \\//     http://www.apache.org/licenses/LICENSE-2.0
    \\//
    \\// Unless required by applicable law or agreed to in writing, software
    \\// distributed under the License is distributed on an "AS IS" BASIS,
    \\// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    \\// See the License for the specific language governing permissions and
    \\// limitations under the License.
    \\
    \\
;

/// Preserve each caller's historical line selection while sharing C escaping.
pub const MetalLines = struct { skip_empty: bool = false, terminated_only: bool = true };

fn put(output: ?[]u8, cursor: *usize, bytes: []const u8) void {
    if (output) |buffer| @memcpy(buffer[cursor.*..][0..bytes.len], bytes);
    cursor.* += bytes.len;
}

inline fn putByte(output: ?[]u8, cursor: *usize, byte: u8) void {
    if (output) |buffer| buffer[cursor.*] = byte;
    cursor.* += 1;
}

fn metalSource(output: ?[]u8, source: []const u8, indent: []const u8, policy: MetalLines) usize {
    var cursor: usize = 0;
    var start: usize = 0;
    while (start < source.len) {
        const newline = std.mem.indexOfScalarPos(u8, source, start, '\n');
        if (newline == null and policy.terminated_only) break;
        const end = newline orelse source.len;
        const line = source[start..end];
        if (!policy.skip_empty or line.len != 0) {
            put(output, &cursor, indent);
            putByte(output, &cursor, '"');
            for (line) |ch| {
                if (ch == '\\' or ch == '"') putByte(output, &cursor, '\\');
                putByte(output, &cursor, ch);
            }
            put(output, &cursor, "\\n\"\n");
        }
        start = if (newline != null) end + 1 else end;
    }
    return cursor;
}

pub fn metalEncodedSize(source: []const u8, indent: []const u8, policy: MetalLines) usize {
    return metalSource(null, source, indent, policy);
}

pub fn encodeMetal(output: []u8, source: []const u8, indent: []const u8, policy: MetalLines) void {
    const written = metalSource(output, source, indent, policy);
    std.debug.assert(written == output.len);
}

test "native source Metal encoding escapes C literals and preserves line policies" {
    const source = "a\"b\\c\n\nunterminated";
    var buffer: [128]u8 = @splat(0xcc);
    const tile_size = metalEncodedSize(source, "  ", .{});
    encodeMetal(buffer[0..tile_size], source, "  ", .{});
    try std.testing.expectEqualStrings("  \"a\\\"b\\\\c\\n\"\n  \"\\n\"\n", buffer[0..tile_size]);
    for (buffer[tile_size..]) |byte| try std.testing.expectEqual(@as(u8, 0xcc), byte);
    const policy = MetalLines{ .skip_empty = true, .terminated_only = false };
    const runtime_size = metalEncodedSize(source, "  ", policy);
    encodeMetal(buffer[0..runtime_size], source, "  ", policy);
    try std.testing.expectEqualStrings("  \"a\\\"b\\\\c\\n\"\n  \"unterminated\\n\"\n", buffer[0..runtime_size]);
    try std.testing.expectEqual(@as(usize, 0), metalEncodedSize("tail", "", .{}));
    try std.testing.expectEqual(@as(usize, 0), metalEncodedSize("", "", policy));
}
