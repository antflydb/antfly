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

pub fn soundexTextAlloc(alloc: std.mem.Allocator, text: []const u8) ![]u8 {
    var out = try alloc.dupe(u8, "?000");
    errdefer alloc.free(out);

    var previous_code: u8 = 0;
    var output_index: usize = 1;
    var found_first = false;
    for (text) |byte| {
        if (!std.ascii.isAlphabetic(byte)) {
            previous_code = 0;
            continue;
        }
        const upper = std.ascii.toUpper(byte);
        const code = soundexCode(upper);
        if (!found_first) {
            out[0] = upper;
            previous_code = code;
            found_first = true;
            continue;
        }
        if (code == 0) {
            if (upper != 'H' and upper != 'W') previous_code = 0;
            continue;
        }
        if (code == previous_code) continue;
        if (output_index < out.len) {
            out[output_index] = '0' + code;
            output_index += 1;
        }
        previous_code = code;
        if (output_index == out.len) break;
    }

    return out;
}

fn soundexCode(upper: u8) u8 {
    return switch (upper) {
        'B', 'F', 'P', 'V' => 1,
        'C', 'G', 'J', 'K', 'Q', 'S', 'X', 'Z' => 2,
        'D', 'T' => 3,
        'L' => 4,
        'M', 'N' => 5,
        'R' => 6,
        else => 0,
    };
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

    const robert = try soundexTextAlloc(alloc, "Robert");
    defer alloc.free(robert);
    try std.testing.expectEqualStrings("R163", robert);

    const rupert = try soundexTextAlloc(alloc, "Rupert");
    defer alloc.free(rupert);
    try std.testing.expectEqualStrings("R163", rupert);

    const ashcraft = try soundexTextAlloc(alloc, "Ashcraft");
    defer alloc.free(ashcraft);
    try std.testing.expectEqualStrings("A261", ashcraft);

    const empty = try soundexTextAlloc(alloc, "123");
    defer alloc.free(empty);
    try std.testing.expectEqualStrings("?000", empty);
}
