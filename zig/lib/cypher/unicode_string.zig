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

//! Locale-independent Unicode default full casing. Tables are pinned so query
//! results do not depend on the host locale or native versus WASM execution.
const std = @import("std");
const data = @import("unicode_case_data.zig");

fn contains(ranges: []const data.Range, cp: u21) bool {
    var lo: usize = 0;
    var hi = ranges.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const range = ranges[mid];
        if (cp < range.first) hi = mid else if (cp > range.last) lo = mid + 1 else return true;
    }
    return false;
}

fn mapping(table: []const data.Mapping, cp: u21) ?[]const u8 {
    var lo: usize = 0;
    var hi = table.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (cp < table[mid].cp) hi = mid else if (cp > table[mid].cp) lo = mid + 1 else return table[mid].text;
    }
    return null;
}

fn followedByCased(input: std.unicode.Utf8Iterator) bool {
    var iterator = input;
    while (iterator.nextCodepoint()) |cp| {
        if (contains(&data.ignorable, cp)) continue;
        return contains(&data.cased, cp);
    }
    return false;
}

pub fn caseConvert(a: std.mem.Allocator, text: []const u8, upper: bool) ![]u8 {
    const view = std.unicode.Utf8View.init(text) catch return error.CypherTypeMismatch;
    var iterator = view.iterator();
    var output: std.ArrayList(u8) = .empty;
    errdefer output.deinit(a);
    try output.ensureTotalCapacity(a, text.len);
    var preceded_by_cased = false;
    while (iterator.nextCodepointSlice()) |original| {
        const cp = std.unicode.utf8Decode(original) catch unreachable;
        // Unicode Final_Sigma examines the original input, skipping case-ignorable
        // characters on both sides. Lookahead visits each intervening run once.
        const replacement = if (!upper and cp == 0x03a3 and preceded_by_cased and !followedByCased(iterator))
            "ς"
        else
            mapping(if (upper) &data.upper else &data.lower, cp) orelse original;
        try output.appendSlice(a, replacement);
        if (!contains(&data.ignorable, cp)) preceded_by_cased = contains(&data.cased, cp);
    }
    return output.toOwnedSlice(a);
}

// Neo4j uses Character.isWhitespace rather than Unicode White_Space. In
// particular, no-break spaces remain and U+001C..U+001F are removed.
fn whitespace(cp: u21) bool {
    return switch (cp) {
        0x09...0x0d, 0x1c...0x20, 0x1680, 0x2000...0x2006, 0x2008...0x200a, 0x2028, 0x2029, 0x205f, 0x3000 => true,
        else => false,
    };
}

pub fn trim(text: []const u8, left: bool, right: bool) ![]const u8 {
    const view = std.unicode.Utf8View.init(text) catch return error.CypherTypeMismatch;
    var iterator = view.iterator();
    var first: usize = text.len;
    var last: usize = 0;
    var offset: usize = 0;
    while (iterator.nextCodepointSlice()) |original| {
        const cp = std.unicode.utf8Decode(original) catch unreachable;
        if (!whitespace(cp)) {
            first = @min(first, offset);
            last = offset + original.len;
        }
        offset += original.len;
    }
    const start = if (left) first else 0;
    const end = if (right) @max(start, last) else text.len;
    return text[start..end];
}

test "full casing expands mappings and handles original-string sigma context" {
    const cases = [_]struct { input: []const u8, lower: []const u8, upper: []const u8 }{
        .{ .input = "ÉCOLE Straße ﬃ İ", .lower = "école straße ﬃ i̇", .upper = "ÉCOLE STRASSE FFI İ" },
        .{ .input = "ΟΣ ΟΣΑ Σ ΑΣ́ ΑΣͅ ͅΣ", .lower = "ος οσα σ ας́ αςͅ ͅσ", .upper = "ΟΣ ΟΣΑ Σ ΑΣ́ ΑΣΙ ΙΣ" },
        .{ .input = "ПРИВЕТ 𐐀𐐨", .lower = "привет 𐐨𐐨", .upper = "ПРИВЕТ 𐐀𐐀" },
        .{ .input = "", .lower = "", .upper = "" },
    };
    for (cases) |case| {
        const lower = try caseConvert(std.testing.allocator, case.input, false);
        defer std.testing.allocator.free(lower);
        const upper = try caseConvert(std.testing.allocator, case.input, true);
        defer std.testing.allocator.free(upper);
        try std.testing.expectEqualStrings(case.lower, lower);
        try std.testing.expectEqualStrings(case.upper, upper);
    }
    try std.testing.expectError(error.CypherTypeMismatch, caseConvert(std.testing.allocator, "\xff", false));
}

test "trim uses Neo4j whitespace boundaries and validates UTF-8" {
    try std.testing.expectEqualStrings("hi", try trim("\u{2003}\u{001c}hi\u{3000}\u{2028}", true, true));
    try std.testing.expectEqualStrings("hi\u{2003}", try trim("\u{2003}hi\u{2003}", true, false));
    try std.testing.expectEqualStrings("\u{2003}hi", try trim("\u{2003}hi\u{2003}", false, true));
    try std.testing.expectEqualStrings("", try trim("\u{2003}\t", true, true));
    try std.testing.expectEqualStrings("", try trim("", true, false));
    try std.testing.expectEqualStrings("\u{00a0}\u{2007}\u{202f}\u{0085}", try trim("\u{00a0}\u{2007}\u{202f}\u{0085}", true, true));
    try std.testing.expectError(error.CypherTypeMismatch, trim("a\xff", false, true));
}
