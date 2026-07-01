// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

pub fn md5HexTextAlloc(alloc: std.mem.Allocator, text: []const u8) ![]u8 {
    const digest = std.crypto.hash.Md5.hashResult(text);
    const out = try alloc.alloc(u8, 32);
    for (digest, 0..) |byte, i| {
        out[i * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[i * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

pub fn initcapTextAlloc(alloc: std.mem.Allocator, text: []const u8) ![]u8 {
    const out = try alloc.dupe(u8, text);
    var at_word_start = true;
    for (out) |*byte| {
        if (std.ascii.isAlphanumeric(byte.*)) {
            byte.* = if (at_word_start) std.ascii.toUpper(byte.*) else std.ascii.toLower(byte.*);
            at_word_start = false;
        } else {
            at_word_start = true;
        }
    }
    return out;
}

test "sql expr_text transforms text" {
    const alloc = std.testing.allocator;
    const md5_hex = try md5HexTextAlloc(alloc, "hello");
    defer alloc.free(md5_hex);
    try std.testing.expectEqualStrings("5d41402abc4b2a76b9719d911017c592", md5_hex);

    const initcap = try initcapTextAlloc(alloc, "hello SQL-world");
    defer alloc.free(initcap);
    try std.testing.expectEqualStrings("Hello Sql-World", initcap);
}
