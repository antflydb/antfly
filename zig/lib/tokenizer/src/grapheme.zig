// Copyright 2026 Antfly, Inc.
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

//! Linear-time Unicode 16 extended grapheme segmentation (UAX #29).
//! State is bounded; long combining-mark and emoji sequences need no allocation.
const std = @import("std");
const data = @import("grapheme_data.zig");
const Category = enum(u4) { other, cr, lf, control, extend, zwj, ri, prepend, spacing, l, v, t, lv, lvt };
fn properties(cp: u21) u8 {
    if (cp < 0x20) return if (cp == 13) 1 else if (cp == 10) 2 else 3;
    if (cp < 0x7f) return 0;
    var lo: usize = 0;
    var hi = data.ranges.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const range = data.ranges[mid];
        if (cp < range.lo) hi = mid else if (cp > range.hi) lo = mid + 1 else return range.flags;
    }
    return 0;
}
fn category(flags: u8) Category {
    return @enumFromInt(flags & 15);
}
fn control(cat: Category) bool {
    return cat == .control or cat == .cr or cat == .lf;
}

pub const Iterator = struct {
    text: []const u8,
    position: usize = 0,
    pub fn next(self: *Iterator) !?[]const u8 {
        if (self.position == self.text.len) return null;
        const start = self.position;
        var previous: Category = .other;
        var regional_count: usize = 0;
        var emoji_chain = false;
        var emoji_zwj = false;
        var indic_consonant = false;
        var indic_linker = false;
        while (self.position < self.text.len) {
            const length = try std.unicode.utf8ByteSequenceLength(self.text[self.position]);
            if (length > self.text.len - self.position) return error.InvalidUtf8;
            const cp = try std.unicode.utf8Decode(self.text[self.position..][0..length]);
            const flags = properties(cp);
            const current = category(flags);
            if (self.position != start) {
                const joins = if (previous == .cr and current == .lf)
                    true
                else if (control(previous) or control(current))
                    false
                else if (previous == .l and (current == .l or current == .v or current == .lv or current == .lvt))
                    true
                else if ((previous == .lv or previous == .v) and (current == .v or current == .t))
                    true
                else if ((previous == .lvt or previous == .t) and current == .t)
                    true
                else if (current == .extend or current == .zwj or current == .spacing or previous == .prepend)
                    true
                else if (flags & 32 != 0 and indic_consonant and indic_linker)
                    true
                else if (flags & 16 != 0 and emoji_zwj)
                    true
                else
                    previous == .ri and current == .ri and regional_count % 2 == 1;
                if (!joins) break;
            }
            if (flags & 32 != 0) {
                indic_consonant = true;
                indic_linker = false;
            } else if (flags & 64 != 0) {
                indic_linker = indic_consonant;
            } else if (flags & 128 == 0) {
                indic_consonant = false;
                indic_linker = false;
            }
            emoji_zwj = current == .zwj and emoji_chain;
            if (flags & 16 != 0) emoji_chain = true else if (current != .extend) emoji_chain = false;
            regional_count = if (current == .ri) regional_count + 1 else 0;
            previous = current;
            self.position += length;
        }
        return self.text[start..self.position];
    }
};

test "Unicode 16 extended grapheme conformance" {
    const allocator = std.testing.allocator;
    var lines = std.mem.splitScalar(u8, @embedFile("testdata/grapheme-break-16.txt"), '\n');
    var count: usize = 0;
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var text: std.ArrayListUnmanaged(u8) = .empty;
        defer text.deinit(allocator);
        var expected: std.ArrayListUnmanaged(usize) = .empty;
        defer expected.deinit(allocator);
        var parts = std.mem.tokenizeScalar(u8, line, ' ');
        while (parts.next()) |part| {
            if (std.mem.eql(u8, part, "÷")) {
                try expected.append(allocator, text.items.len);
            } else if (!std.mem.eql(u8, part, "×")) {
                const cp = try std.fmt.parseInt(u21, part, 16);
                var bytes: [4]u8 = undefined;
                const length = try std.unicode.utf8Encode(cp, &bytes);
                try text.appendSlice(allocator, bytes[0..length]);
            }
        }
        var iterator = Iterator{ .text = text.items };
        var index: usize = 1;
        while (try iterator.next()) |_| {
            if (index >= expected.items.len or expected.items[index] != iterator.position) {
                std.debug.print("grapheme mismatch: {s}\n", .{line});
                return error.TestUnexpectedResult;
            }
            index += 1;
        }
        try std.testing.expectEqual(expected.items.len, index);
        count += 1;
    }
    try std.testing.expect(count > 1000);
}
