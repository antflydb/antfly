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

//! Identifier input for catalog-reference functions. This is not SQL source:
//! comments, predicates and statement separators cannot become executable text.
const std = @import("std");
const ast = @import("ast.zig");
pub fn parse(a: std.mem.Allocator, input: []const u8) !ast.Name {
    if (input.len > 1024 or !std.unicode.utf8ValidateSlice(input)) return error.SqlInvalidName;
    var parts: [3][]const u8 = undefined;
    var count: usize = 0;
    errdefer for (parts[0..count]) |part| a.free(part);
    var position: usize = 0;
    while (true) {
        while (position < input.len and std.ascii.isWhitespace(input[position])) position += 1;
        if (position == input.len or count == parts.len) return error.SqlInvalidName;
        var value: std.ArrayList(u8) = .empty;
        errdefer value.deinit(a);
        if (input[position] == '"') {
            position += 1;
            while (true) {
                if (position == input.len) return error.SqlInvalidName;
                const ch = input[position];
                position += 1;
                if (ch == '"') {
                    if (position < input.len and input[position] == '"') {
                        position += 1;
                    } else break;
                }
                if (ch == 0 or ch < 0x20) return error.SqlInvalidName;
                try value.append(a, ch);
            }
        } else {
            while (position < input.len and input[position] != '.' and !std.ascii.isWhitespace(input[position])) : (position += 1) {
                const ch = input[position];
                // regclass input accepts punctuation which SQL source must
                // quote. It remains a literal catalog name, never SQL text.
                if (ch == '"' or ch < 0x20 or ch == 0x7f) return error.SqlInvalidName;
                try value.append(a, std.ascii.toLower(ch));
            }
        }
        if (value.items.len == 0 or value.items.len > 255) return error.SqlInvalidName;
        parts[count] = try value.toOwnedSlice(a);
        count += 1;
        while (position < input.len and std.ascii.isWhitespace(input[position])) position += 1;
        if (position == input.len) break;
        if (input[position] != '.') return error.SqlInvalidName;
        position += 1;
    }
    return switch (count) {
        1 => .{ .table = parts[0] },
        2 => .{ .namespace = parts[0], .table = parts[1] },
        3 => .{ .database = parts[0], .namespace = parts[1], .table = parts[2] },
        else => unreachable,
    };
}

test "SQL sequence object names preserve quoted components without parsing SQL" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const name = try parse(a, " app . \"Mixed.Schema\" . \"seq\"\"name\" ");
    try std.testing.expectEqualStrings("app", name.database.?);
    try std.testing.expectEqualStrings("Mixed.Schema", name.namespace.?);
    try std.testing.expectEqualStrings("seq\"name", name.table);
    try std.testing.expectEqualStrings("user", (try parse(a, "USER")).table);
    for ([_][]const u8{ "s;", "s--comment", "s/*x*/", "1seq", "a-b", "$name" }) |input|
        try std.testing.expectEqualStrings(input, (try parse(a, input)).table);
    for ([_][]const u8{ "", "s WHERE true", "a..b", "a.", "a.b.c.d", "\"broken", "s\"x" }) |input|
        try std.testing.expectError(error.SqlInvalidName, parse(a, input));
}
