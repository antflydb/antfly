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

//! Source-text geometry shared by GLiNER and generated extraction. Pinned
//! Unicode behavior and immutable source offsets are independent of models.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub const unicode_tables = @import("unicode_tables.zig");
pub const case_data = @import("case_data.zig");
const unicode = unicode_tables;
pub const alignment = @import("text_alignment.zig");
pub const windows = @import("text_windows.zig");
pub const WordSplitter = enum { whitespace, char };
pub const Checkpoint = struct {
    ptr: *const anyopaque,
    check_fn: *const fn (*const anyopaque) anyerror!void,
    pub fn check(self: Checkpoint) !void {
        try self.check_fn(self.ptr);
    }
};
pub const ByteRange = Offsets;
pub const WordRangeOptions = struct {
    word_splitter: WordSplitter = .whitespace,
    max_text_bytes: usize = 1024 * 1024,
    max_words: usize = 131072,
    control: ?Checkpoint = null,
};

/// Original-text boundaries for explicit document windowing. This shares the
/// processor's exact splitter; it adds no enum prefix, terminal punctuation,
/// lowercasing, or model-token wrappers. Caller owns the returned ranges.
pub fn sourceWordRanges(allocator: Allocator, text: []const u8, options: WordRangeOptions) ![]ByteRange {
    if (options.control) |control| try control.check();
    if (text.len > options.max_text_bytes) return error.BoundaryTextLimitExceeded;
    if (!std.unicode.utf8ValidateSlice(text)) return error.InvalidUtf8;
    var result = std.ArrayListUnmanaged(ByteRange).empty;
    errdefer result.deinit(allocator);
    var position: usize = 0;
    while (position < text.len) {
        if (options.control) |control| try control.check();
        if (unicode.isWhitespace(codepoint(text, position))) {
            position = nextCodepoint(text, position);
            continue;
        }
        if (result.items.len >= options.max_words) return error.BoundaryTextLimitExceeded;
        const start = position;
        position = wordEnd(text, position, options.word_splitter);
        try result.append(allocator, .{ .start = start, .end = position });
    }
    return result.toOwnedSlice(allocator);
}

/// Whether the processor's synthetic final period consumes another word.
/// A URL suffix, and an ASCII run under the char splitter, may absorb it.
pub fn terminalWordAdded(allocator: Allocator, text: []const u8, splitter: WordSplitter) !bool {
    if (!std.unicode.utf8ValidateSlice(text)) return error.InvalidUtf8;
    if (text.len == 0) return true;
    if (std.mem.indexOfScalar(u8, ".!?", text[text.len - 1]) != null) return false;
    var position: usize = 0;
    var last: ByteRange = .{ .start = 0, .end = 0 };
    while (position < text.len) {
        if (unicode.isWhitespace(codepoint(text, position))) {
            position = nextCodepoint(text, position);
            continue;
        }
        last.start = position;
        position = wordEnd(text, position, splitter);
        last.end = position;
    }
    if (last.end != text.len) return true;
    const suffix = try std.mem.concat(allocator, u8, &.{ text[last.start..], "." });
    defer allocator.free(suffix);
    return wordEnd(suffix, 0, splitter) < suffix.len;
}
pub fn wordEnd(text: []const u8, start: usize, splitter: WordSplitter) usize {
    if (splitter == .char) {
        var end = start;
        while (end < text.len and (std.ascii.isAlphanumeric(text[end]) or std.mem.indexOfScalar(u8, "@._-+", text[end]) != null)) end += 1;
        return if (end > start) end else nextCodepoint(text, start);
    }
    if (urlPrefixEnd(text, start)) |prefix_end| if (prefix_end < text.len and !unicode.isWhitespace(codepoint(text, prefix_end))) {
        var end = prefix_end;
        while (end < text.len and !unicode.isWhitespace(codepoint(text, end))) end = nextCodepoint(text, end);
        return end;
    };
    if (emailEnd(text, start)) |end| return end;
    if (text[start] == '@' and start + 1 < text.len and asciiRegexWord(codepoint(text, start + 1))) {
        var end = start + 1;
        while (end < text.len and asciiRegexWord(codepoint(text, end))) end = nextCodepoint(text, end);
        return end;
    }
    if (unicode.isWord(codepoint(text, start))) {
        var end = start;
        while (end < text.len and unicode.isWord(codepoint(text, end))) end = nextCodepoint(text, end);
        while (end + 1 < text.len and (text[end] == '-' or text[end] == '_') and unicode.isWord(codepoint(text, end + 1))) {
            end += 1;
            while (end < text.len and unicode.isWord(codepoint(text, end))) end = nextCodepoint(text, end);
        }
        return end;
    }
    return nextCodepoint(text, start);
}
fn urlPrefixEnd(text: []const u8, start: usize) ?usize {
    for ([_][]const u8{ "https://", "http://", "www." }) |prefix| {
        var pos = start;
        for (prefix) |expected| {
            if (pos == text.len) break;
            const cp = codepoint(text, pos);
            if (unicode.simpleLower(cp) != expected and !(expected == 's' and cp == 0x017f)) break;
            pos = nextCodepoint(text, pos);
        } else return pos;
    }
    return null;
}
fn emailEnd(text: []const u8, start: usize) ?usize {
    var pos = start;
    while (pos < text.len) {
        const cp = codepoint(text, pos);
        if (!asciiRegexLetter(cp) and !(cp >= '0' and cp <= '9') and !(cp < 128 and std.mem.indexOfScalar(u8, "._%+-", @intCast(cp)) != null)) break;
        pos = nextCodepoint(text, pos);
    }
    if (pos == start or pos == text.len or text[pos] != '@') return null;
    pos += 1;
    const domain_start = pos;
    var separator_valid = false;
    var tld_letters: usize = 0;
    var best: ?usize = null;
    while (pos < text.len) {
        const cp = codepoint(text, pos);
        if (!asciiRegexLetter(cp) and !(cp >= '0' and cp <= '9') and cp != '.' and cp != '-') break;
        const end = nextCodepoint(text, pos);
        if (cp == '.') {
            separator_valid = pos > domain_start;
            tld_letters = 0;
        } else if (separator_valid and asciiRegexLetter(cp)) {
            tld_letters += 1;
            if (tld_letters >= 2) best = end;
        } else {
            separator_valid = false;
            tld_letters = 0;
        }
        pos = end;
    }
    return best;
}
fn asciiRegexLetter(cp: u21) bool {
    const folded = unicode.simpleLower(cp);
    return (folded >= 'a' and folded <= 'z') or cp == 0x0130 or cp == 0x0131 or cp == 0x017f;
}
fn asciiRegexWord(cp: u21) bool {
    return asciiRegexLetter(cp) or (cp >= '0' and cp <= '9') or cp == '_';
}
fn codepoint(text: []const u8, start: usize) u21 {
    return std.unicode.utf8Decode(text[start..nextCodepoint(text, start)]) catch unreachable;
}
fn nextCodepoint(text: []const u8, start: usize) usize {
    return start + (std.unicode.utf8ByteSequenceLength(text[start]) catch unreachable);
}
pub const OffsetUnit = enum { utf8_bytes, unicode_codepoints, utf16_codeunits };
pub const Offsets = struct { start: usize, end: usize };
const OffsetEntry = struct { byte: usize, utf16: usize };

/// One immutable index per document, reused across queries and chunk merges.
pub const OffsetMap = struct {
    allocator: std.mem.Allocator,
    entries: []OffsetEntry,

    pub fn init(allocator: std.mem.Allocator, text: []const u8, max_bytes: usize) !OffsetMap {
        if (text.len > max_bytes) return error.ExtractionTextLimitExceeded;
        const view = try std.unicode.Utf8View.init(text);
        var iter = view.iterator();
        var count: usize = 0;
        while (iter.nextCodepoint() != null) count += 1;
        const entries = try allocator.alloc(OffsetEntry, count + 1);
        iter = view.iterator();
        var utf16: usize = 0;
        var i: usize = 0;
        entries[0] = .{ .byte = 0, .utf16 = 0 };
        while (iter.nextCodepoint()) |cp| {
            utf16 += if (cp > 0xffff) @as(usize, 2) else 1;
            i += 1;
            entries[i] = .{ .byte = iter.i, .utf16 = utf16 };
        }
        return .{ .allocator = allocator, .entries = entries };
    }

    pub fn deinit(self: *OffsetMap) void {
        self.allocator.free(self.entries);
        self.* = undefined;
    }

    fn find(self: OffsetMap, byte: usize) !usize {
        var lo: usize = 0;
        var hi = self.entries.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.entries[mid].byte < byte) lo = mid + 1 else hi = mid;
        }
        if (lo == self.entries.len or self.entries[lo].byte != byte) return error.InvalidUtf8Boundary;
        return lo;
    }

    pub fn convert(self: OffsetMap, bytes: Offsets, unit: OffsetUnit) !Offsets {
        if (bytes.end < bytes.start) return error.InvalidBoundarySpan;
        const start = try self.find(bytes.start);
        const end = try self.find(bytes.end);
        return switch (unit) {
            .utf8_bytes => bytes,
            .unicode_codepoints => .{ .start = start, .end = end },
            .utf16_codeunits => .{ .start = self.entries[start].utf16, .end = self.entries[end].utf16 },
        };
    }

    fn fromUnit(self: OffsetMap, offset: usize, unit: OffsetUnit) !usize {
        if (unit == .utf8_bytes) return self.find(offset);
        if (unit == .unicode_codepoints) {
            if (offset >= self.entries.len) return error.InvalidCodepointBoundary;
            return offset;
        }
        var lo: usize = 0;
        var hi = self.entries.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.entries[mid].utf16 < offset) lo = mid + 1 else hi = mid;
        }
        // A surrogate-pair interior is not a source character boundary.
        if (lo == self.entries.len or self.entries[lo].utf16 != offset) return error.InvalidUtf16Boundary;
        return lo;
    }

    pub fn toBytes(self: OffsetMap, offsets: Offsets, unit: OffsetUnit) !Offsets {
        if (offsets.end < offsets.start) return error.InvalidBoundarySpan;
        const start = try self.fromUnit(offsets.start, unit);
        const end = try self.fromUnit(offsets.end, unit);
        return .{ .start = self.entries[start].byte, .end = self.entries[end].byte };
    }
};
