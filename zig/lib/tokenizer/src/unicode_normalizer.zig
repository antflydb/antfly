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

//! Ordered normalization profiles for WordPiece, BPE, and Unigram tokenizers.
//! Unicode canonical decomposition/composition follows Unicode 15.0.0.
const std = @import("std");
const data = @import("unicode_nfc_data.zig");

pub const Step = union(enum) {
    nfc,
    nfd,
    lowercase,
    strip_accents,
    bert: Bert,
    literal_replace: struct { pattern: []u8, content: []u8 },
    whitespace_replace,
    spaces_replace,
    precompiled: Precompiled,
    strip: struct { left: bool, right: bool },
};

pub const Profile = struct {
    steps: [32]Step = undefined,
    len: usize = 0,
    wordpiece: bool = false,

    pub fn parse(self: *Profile, allocator: std.mem.Allocator, value: std.json.Value) !void {
        try self.parseDepth(allocator, value, 0, false);
    }

    pub fn parseWordPiece(self: *Profile, allocator: std.mem.Allocator, value: std.json.Value) !void {
        self.wordpiece = true;
        try self.parseDepth(allocator, value, 0, true);
    }

    fn append(self: *Profile, step: Step) !void {
        if (self.len == self.steps.len) return error.UnsupportedTokenizerNormalizer;
        self.steps[self.len] = step;
        self.len += 1;
    }

    fn boolean(obj: std.json.ObjectMap, key: []const u8, default: bool) !bool {
        const value = obj.get(key) orelse return default;
        if (value != .bool) return error.InvalidTokenizerNormalizer;
        return value.bool;
    }

    fn parseDepth(self: *Profile, allocator: std.mem.Allocator, value: std.json.Value, depth: usize, wordpiece: bool) anyerror!void {
        if (depth >= 16 or value != .object) return error.InvalidTokenizerNormalizer;
        const obj = value.object;
        const kind = obj.get("type") orelse return error.InvalidTokenizerNormalizer;
        if (kind != .string) return error.InvalidTokenizerNormalizer;
        if (std.mem.eql(u8, kind.string, "Sequence")) {
            const children = obj.get("normalizers") orelse return error.InvalidTokenizerNormalizer;
            if (children != .array) return error.InvalidTokenizerNormalizer;
            for (children.array.items) |child| try self.parseDepth(allocator, child, depth + 1, wordpiece);
        } else if (std.mem.eql(u8, kind.string, "Precompiled")) {
            const encoded = obj.get("precompiled_charsmap") orelse return error.UnsupportedTokenizerNormalizer;
            if (encoded != .string) return error.InvalidTokenizerNormalizer;
            if (encoded.string.len == 0) return error.UnsupportedTokenizerNormalizer;
            const map = try Precompiled.init(allocator, encoded.string);
            errdefer allocator.free(map.bytes);
            try self.append(.{ .precompiled = map });
        } else if (wordpiece and std.mem.eql(u8, kind.string, "BertNormalizer")) {
            const lowercase = try boolean(obj, "lowercase", true);
            const strip_accents = if (obj.get("strip_accents")) |setting|
                if (setting == .null) lowercase else try boolean(obj, "strip_accents", lowercase)
            else
                lowercase;
            try self.append(.{ .bert = .{
                .lowercase = lowercase,
                .strip_accents = strip_accents,
                .clean_text = try boolean(obj, "clean_text", true),
                .handle_chinese_chars = try boolean(obj, "handle_chinese_chars", true),
            } });
        } else if (wordpiece and std.mem.eql(u8, kind.string, "Lowercase")) {
            try self.append(.lowercase);
        } else if (wordpiece and std.mem.eql(u8, kind.string, "StripAccents")) {
            try self.append(.strip_accents);
        } else if (wordpiece and std.mem.eql(u8, kind.string, "NFD")) {
            try self.append(.nfd);
        } else if (std.mem.eql(u8, kind.string, "NFC")) {
            try self.append(.nfc);
        } else if (std.mem.eql(u8, kind.string, "Strip")) {
            try self.append(.{ .strip = .{ .left = try boolean(obj, "strip_left", true), .right = try boolean(obj, "strip_right", true) } });
        } else if (std.mem.eql(u8, kind.string, "Replace")) {
            const pattern = obj.get("pattern") orelse return error.InvalidTokenizerNormalizer;
            const content = obj.get("content") orelse return error.InvalidTokenizerNormalizer;
            if (pattern != .object or content != .string) return error.InvalidTokenizerNormalizer;
            if (pattern.object.get("String")) |literal| {
                if (!wordpiece) return error.UnsupportedTokenizerNormalizer;
                if (literal != .string) return error.InvalidTokenizerNormalizer;
                if (literal.string.len == 0) return error.UnsupportedTokenizerNormalizer;
                const owned_pattern = try allocator.dupe(u8, literal.string);
                errdefer allocator.free(owned_pattern);
                const owned_content = try allocator.dupe(u8, content.string);
                errdefer allocator.free(owned_content);
                try self.append(.{ .literal_replace = .{ .pattern = owned_pattern, .content = owned_content } });
                return;
            }
            const regex = pattern.object.get("Regex") orelse return error.UnsupportedTokenizerNormalizer;
            if (regex != .string) return error.InvalidTokenizerNormalizer;
            if (std.mem.eql(u8, regex.string, " {2,}") and std.mem.eql(u8, content.string, " ")) {
                try self.append(.spaces_replace);
                return;
            }
            if (!std.mem.eql(u8, regex.string, "\\s{2,}|[\\n\\r\\t]") or !std.mem.eql(u8, content.string, " "))
                return error.UnsupportedTokenizerNormalizer;
            try self.append(.whitespace_replace);
        } else return error.UnsupportedTokenizerNormalizer;
    }

    pub fn deinit(self: *Profile, allocator: std.mem.Allocator) void {
        for (self.steps[0..self.len]) |step| switch (step) {
            .precompiled => |map| allocator.free(map.bytes),
            .literal_replace => |value| {
                allocator.free(value.pattern);
                allocator.free(value.content);
            },
            else => {},
        };
        self.len = 0;
    }

    /// Only local character transforms may reuse byte positions after the
    /// encoder checks their output. Replacements/composition can change source
    /// provenance even if later steps restore the original final bytes.
    pub fn supportsByteAlignedOffsets(self: *const Profile) bool {
        for (self.steps[0..self.len]) |step| switch (step) {
            .bert, .lowercase, .strip_accents => {},
            else => return false,
        };
        return true;
    }

    pub fn normalizeAligned(self: *const Profile, allocator: std.mem.Allocator, text: []const u8) !AlignedText {
        var current = try AlignedText.fromSource(allocator, text);
        errdefer current.deinit(allocator);
        for (self.steps[0..self.len]) |step| {
            const next = try normalizeAlignedStep(allocator, &current, step);
            current.deinit(allocator);
            current = next;
        }
        return current;
    }

    pub fn normalize(self: *const Profile, allocator: std.mem.Allocator, text: []const u8) ![]u8 {
        // Borrow the source until the first step produces output. The common
        // single-BERT profile performs one normalization allocation, no copy.
        var owned: ?[]u8 = null;
        errdefer if (owned) |value| allocator.free(value);
        for (self.steps[0..self.len]) |step| {
            const input = owned orelse text;
            const next = switch (step) {
                .nfc => try nfcWithProfile(allocator, input, self.wordpiece),
                .nfd => try nfdWithProfile(allocator, input, self.wordpiece),
                .lowercase => try (Bert{ .strip_accents = false, .clean_text = false }).normalize(allocator, input),
                .strip_accents => try stripAccents(allocator, input),
                .bert => |profile| try profile.normalize(allocator, input),
                .literal_replace => |value| try std.mem.replaceOwned(u8, allocator, input, value.pattern, value.content),
                .precompiled => |map| try map.normalize(allocator, input),
                .spaces_replace => try collapseSpaces(allocator, input),
                .whitespace_replace => try replaceWhitespace(allocator, input),
                .strip => |flags| try strip(allocator, input, flags.left, flags.right),
            };
            if (owned) |value| allocator.free(value);
            owned = next;
        }
        return owned orelse try allocator.dupe(u8, text);
    }
};

// SentencePiece's serialized Darts trie: byte length, little-endian u32
// units, then NUL-terminated UTF-8 replacements. Keep the upstream table;
// substituting NFC/NFKC would lose the model's exact normalization rules.
const Precompiled = struct {
    bytes: []u8,
    trie_bytes: usize,

    fn init(allocator: std.mem.Allocator, encoded: []const u8) !Precompiled {
        if (encoded.len > 16 * 1024 * 1024) return error.InvalidTokenizerNormalizer;
        const decoder = std.base64.standard.Decoder;
        const size = decoder.calcSizeForSlice(encoded) catch return error.InvalidTokenizerNormalizer;
        const bytes = try allocator.alloc(u8, size);
        errdefer allocator.free(bytes);
        decoder.decode(bytes, encoded) catch return error.InvalidTokenizerNormalizer;
        if (bytes.len < 8) return error.InvalidTokenizerNormalizer;
        const length = std.mem.readInt(u32, bytes[0..4], .little);
        if (length < 4 or length % 4 != 0 or length > bytes.len - 4 or
            !std.unicode.utf8ValidateSlice(bytes[4 + length ..])) return error.InvalidTokenizerNormalizer;
        return .{ .bytes = bytes, .trie_bytes = length };
    }

    fn unit(self: Precompiled, index: usize) !u32 {
        if (index >= self.trie_bytes / 4) return error.InvalidTokenizerNormalizer;
        return std.mem.readInt(u32, self.bytes[4 + index * 4 ..][0..4], .little);
    }

    fn offset(value: u32) usize {
        return @as(usize, value >> 10) << @intCast((value & (1 << 9)) >> 6);
    }

    fn transform(self: Precompiled, text: []const u8) !?[]const u8 {
        var cursor = offset(try self.unit(0));
        for (text) |byte| {
            if (byte == 0) break;
            cursor ^= byte;
            const value = try self.unit(cursor);
            if (value & 0x800000ff != byte) break;
            cursor ^= offset(value);
            if (value & 0x100 != 0) {
                const index = (try self.unit(cursor)) & 0x7fffffff;
                const table = self.bytes[4 + self.trie_bytes ..];
                if (index >= table.len) return error.InvalidTokenizerNormalizer;
                const end = std.mem.indexOfScalar(u8, table[index..], 0) orelse return error.InvalidTokenizerNormalizer;
                // Hugging Face spm_precompiled selects the first prefix,
                // not the longest match in the serialized SentencePiece trie.
                return table[index..][0..end];
            }
        }
        return null;
    }

    fn normalize(self: Precompiled, allocator: std.mem.Allocator, text: []const u8) ![]u8 {
        var output: std.ArrayListUnmanaged(u8) = .empty;
        errdefer output.deinit(allocator);
        try output.ensureTotalCapacity(allocator, text.len);
        var graphemes = @import("antfly_grapheme").Iterator{ .text = text };
        while (try graphemes.next()) |cluster| {
            // Match the reference's short-grapheme rule exactly, including
            // its six-byte cutoff. Longer clusters normalize each codepoint.
            if (cluster.len < 6) {
                if (try self.transform(cluster)) |replacement| {
                    try output.appendSlice(allocator, replacement);
                    continue;
                }
            }
            var position: usize = 0;
            while (position < cluster.len) {
                const length = try std.unicode.utf8ByteSequenceLength(cluster[position]);
                const codepoint = cluster[position..][0..length];
                try output.appendSlice(allocator, (try self.transform(codepoint)) orelse codepoint);
                position += length;
            }
        }
        return output.toOwnedSlice(allocator);
    }
};

fn collapseSpaces(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    var output = std.ArrayListUnmanaged(u8).empty;
    errdefer output.deinit(allocator);
    var previous_space = false;
    for (text) |byte| {
        if (byte != ' ' or !previous_space) try output.append(allocator, byte);
        previous_space = byte == ' ';
    }
    return output.toOwnedSlice(allocator);
}

fn combiningClass(cp: u21) u8 {
    var lo: usize = 0;
    var hi = data.combining.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (data.combining[mid].cp < cp) lo = mid + 1 else hi = mid;
    }
    return if (lo < data.combining.len and data.combining[lo].cp == cp) data.combining[lo].value else 0;
}

const Unit = struct {
    cp: u21,
    class: u8,
    order: usize,

    fn less(_: void, a: Unit, b: Unit) bool {
        return if (a.class != b.class) a.class < b.class else a.order < b.order;
    }
};

fn decompose(allocator: std.mem.Allocator, cp: u21, units: *std.ArrayListUnmanaged(Unit)) std.mem.Allocator.Error!void {
    if (cp >= 0xac00 and cp < 0xac00 + 11172) {
        const index = cp - 0xac00;
        try decompose(allocator, 0x1100 + index / 588, units);
        try decompose(allocator, 0x1161 + (index % 588) / 28, units);
        if (index % 28 != 0) try decompose(allocator, 0x11a7 + index % 28, units);
        return;
    }
    var lo: usize = 0;
    var hi = data.decompositions.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (data.decompositions[mid].cp < cp) lo = mid + 1 else hi = mid;
    }
    if (lo < data.decompositions.len and data.decompositions[lo].cp == cp) {
        const entry = data.decompositions[lo];
        try decompose(allocator, entry.first, units);
        if (entry.second != 0) try decompose(allocator, entry.second, units);
    } else try units.append(allocator, .{ .cp = cp, .class = combiningClass(cp), .order = units.items.len });
}

fn decomposeWordPiece(allocator: std.mem.Allocator, cp: u21, units: *std.ArrayListUnmanaged(Unit)) std.mem.Allocator.Error!void {
    if (inRanges(cp, &data.bert_decomposition_ignored)) {
        try units.append(allocator, .{ .cp = cp, .class = 0, .order = units.items.len });
        return;
    }
    const start = units.items.len;
    try decompose(allocator, cp, units);
    for (units.items[start..]) |*unit| {
        if (inRanges(unit.cp, &data.bert_combining_zero)) unit.class = 0;
    }
}

fn composeWordPiece(a: u21, b: u21) ?u21 {
    const cp = compose(a, b) orelse return null;
    return if (inRanges(cp, &data.bert_decomposition_ignored)) null else cp;
}

fn compose(a: u21, b: u21) ?u21 {
    if (a >= 0x1100 and a < 0x1100 + 19 and b >= 0x1161 and b < 0x1161 + 21)
        return 0xac00 + ((a - 0x1100) * 21 + b - 0x1161) * 28;
    if (a >= 0xac00 and a < 0xac00 + 11172 and (a - 0xac00) % 28 == 0 and b > 0x11a7 and b < 0x11a7 + 28)
        return a + b - 0x11a7;
    const pair = (@as(u64, a) << 21) | b;
    var lo: usize = 0;
    var hi = data.compositions.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (data.compositions[mid].pair < pair) lo = mid + 1 else hi = mid;
    }
    return if (lo < data.compositions.len and data.compositions[lo].pair == pair) data.compositions[lo].cp else null;
}

fn decomposedUnits(allocator: std.mem.Allocator, text: []const u8, wordpiece: bool) !std.ArrayListUnmanaged(Unit) {
    const view = try std.unicode.Utf8View.init(text);
    var iter = view.iterator();
    var units = std.ArrayListUnmanaged(Unit).empty;
    errdefer units.deinit(allocator);
    while (iter.nextCodepoint()) |cp| {
        if (wordpiece) try decomposeWordPiece(allocator, cp, &units) else try decompose(allocator, cp, &units);
    }
    // Canonical ordering must be stable. An explicit original-order key lets
    // the O(n log n) sort avoid quadratic insertion work on long mark runs.
    var run: usize = 0;
    for (units.items, 0..) |unit, i| if (unit.class == 0) {
        std.mem.sort(Unit, units.items[run..i], {}, Unit.less);
        run = i + 1;
    };
    std.mem.sort(Unit, units.items[run..], {}, Unit.less);
    return units;
}

pub fn nfd(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    return nfdWithProfile(allocator, text, false);
}

fn nfdWithProfile(allocator: std.mem.Allocator, text: []const u8, wordpiece: bool) ![]u8 {
    var units = try decomposedUnits(allocator, text, wordpiece);
    defer units.deinit(allocator);
    var output = std.ArrayListUnmanaged(u8).empty;
    errdefer output.deinit(allocator);
    for (units.items) |unit| try appendCodepoint(allocator, &output, unit.cp);
    return output.toOwnedSlice(allocator);
}

fn stripAccents(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    var output = std.ArrayListUnmanaged(u8).empty;
    errdefer output.deinit(allocator);
    var iter = (try std.unicode.Utf8View.init(text)).iterator();
    while (iter.nextCodepoint()) |cp| {
        if (!inRanges(cp, &data.strip_accents_marks)) try appendCodepoint(allocator, &output, cp);
    }
    return output.toOwnedSlice(allocator);
}

pub fn nfc(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    return nfcWithProfile(allocator, text, false);
}

fn nfcWithProfile(allocator: std.mem.Allocator, text: []const u8, wordpiece: bool) ![]u8 {
    var units = try decomposedUnits(allocator, text, wordpiece);
    defer units.deinit(allocator);
    var written: usize = 0;
    var starter: ?usize = null;
    var last_class: u8 = 0;
    for (units.items) |unit| {
        if (starter) |position| {
            if (last_class == 0 or last_class < unit.class) {
                if (if (wordpiece) composeWordPiece(units.items[position].cp, unit.cp) else compose(units.items[position].cp, unit.cp)) |composed| {
                    units.items[position].cp = composed;
                    continue;
                }
            }
        }
        if (unit.class == 0) starter = written;
        units.items[written] = unit;
        written += 1;
        last_class = unit.class;
    }
    var output = std.ArrayListUnmanaged(u8).empty;
    errdefer output.deinit(allocator);
    for (units.items[0..written]) |unit| {
        var buffer: [4]u8 = undefined;
        const size = try std.unicode.utf8Encode(unit.cp, &buffer);
        try output.appendSlice(allocator, buffer[0..size]);
    }
    return output.toOwnedSlice(allocator);
}

fn whitespace(cp: u21) bool {
    return switch (cp) {
        0x9...0xd, 0x20, 0x85, 0xa0, 0x1680, 0x2000...0x200a, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000 => true,
        else => false,
    };
}

fn replaceWhitespace(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    const view = try std.unicode.Utf8View.init(text);
    var iter = view.iterator();
    var output = std.ArrayListUnmanaged(u8).empty;
    errdefer output.deinit(allocator);
    while (iter.i < text.len) {
        const start = iter.i;
        const cp = iter.nextCodepoint().?;
        if (whitespace(cp)) {
            var count: usize = 1;
            while (iter.i < text.len) {
                var next = iter;
                if (!whitespace(next.nextCodepoint().?)) break;
                iter = next;
                count += 1;
            }
            if (count >= 2 or cp == '\n' or cp == '\r' or cp == '\t') {
                try output.append(allocator, ' ');
                continue;
            }
        }
        try output.appendSlice(allocator, text[start..iter.i]);
    }
    return output.toOwnedSlice(allocator);
}

fn strip(allocator: std.mem.Allocator, text: []const u8, left: bool, right: bool) ![]u8 {
    const view = try std.unicode.Utf8View.init(text);
    var iter = view.iterator();
    var first: ?usize = null;
    var last: usize = 0;
    while (iter.i < text.len) {
        const start = iter.i;
        if (!whitespace(iter.nextCodepoint().?)) {
            if (first == null) first = start;
            last = iter.i;
        }
    }
    const start = if (left) first orelse text.len else 0;
    const end = if (right) @max(start, last) else text.len;
    return allocator.dupe(u8, text[start..end]);
}

test "Unicode NFC composition ordering exclusions and Hangul" {
    const a = std.testing.allocator;
    const inputs = [_][]const u8{ "e\u{301}", "a\u{315}\u{300}", "\u{212b}", "\u{958}", "\u{1100}\u{1161}\u{11a8}", "🙂東京" };
    const expected = [_][]const u8{ "é", "à\u{315}", "Å", "\u{915}\u{93c}", "각", "🙂東京" };
    for (inputs, expected) |input, want| {
        const got = try nfc(a, input);
        defer a.free(got);
        try std.testing.expectEqualStrings(want, got);
    }
    try std.testing.expectError(error.InvalidUtf8, nfc(a, "\xff"));
}

test "ordered GLiNER whitespace NFC strip profile and unknown rejection" {
    const a = std.testing.allocator;
    const parsed = try std.json.parseFromSlice(std.json.Value, a,
        \\{"type":"Sequence","normalizers":[{"type":"Replace","pattern":{"Regex":"\\s{2,}|[\\n\\r\\t]"},"content":" "},{"type":"NFC"},{"type":"Strip","strip_left":false,"strip_right":true}]}
    , .{});
    defer parsed.deinit();
    var profile: Profile = .{};
    defer profile.deinit(a);
    try profile.parse(a, parsed.value);
    const got = try profile.normalize(a, " e\u{301}  x\t\n");
    defer a.free(got);
    try std.testing.expectEqualStrings(" é x", got);
    const unknown = try std.json.parseFromSlice(std.json.Value, a, "{\"type\":\"Precompiled\"}", .{});
    defer unknown.deinit();
    try std.testing.expectError(error.UnsupportedTokenizerNormalizer, profile.parse(a, unknown.value));
}

test "SentencePiece precompiled normalization rewrites Unicode and deletes controls" {
    const allocator = std.testing.allocator;
    // A small serialized Darts map with independent transitions and leaf
    // values; it exercises the format without depending on a network model.
    var units: [4096]u32 = @splat(0);
    units[0] = 256 << 10;
    var next_base: u32 = 512;
    var values = std.ArrayListUnmanaged(u8).empty;
    defer values.deinit(allocator);
    const sources = [_][]const u8{ "，", "！", "Ａ", "e\u{301}", "\x01" };
    const targets = [_][]const u8{ ",", "!", "A", "é", "" };
    for (sources, targets) |source, target| {
        var base: u32 = 256;
        var last: u32 = 0;
        for (source) |byte| {
            const index = base ^ byte;
            if (units[index] == 0) {
                units[index] = byte | ((index ^ next_base) << 10);
                next_base += 256;
            }
            base = index ^ @as(u32, @intCast(Precompiled.offset(units[index])));
            last = index;
        }
        units[last] |= 0x100;
        units[base] = 0x80000000 | @as(u32, @intCast(values.items.len));
        try values.appendSlice(allocator, target);
        try values.append(allocator, 0);
    }
    const bytes = try allocator.alloc(u8, 4 + units.len * 4 + values.items.len);
    defer allocator.free(bytes);
    std.mem.writeInt(u32, bytes[0..4], units.len * 4, .little);
    for (units, 0..) |unit, i| std.mem.writeInt(u32, bytes[4 + i * 4 ..][0..4], unit, .little);
    @memcpy(bytes[4 + units.len * 4 ..], values.items);
    const encoded = try allocator.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.len));
    defer allocator.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, bytes);
    const map = try Precompiled.init(allocator, encoded);
    defer allocator.free(map.bytes);
    const normalized = try map.normalize(allocator, "你好，世界！ Ａ e\u{301}\x01🙂");
    defer allocator.free(normalized);
    try std.testing.expectEqualStrings("你好,世界! A é🙂", normalized);
    for ([_][]const u8{ "你好，世界！ Ａ e\u{301}\x01🙂", "e\u{301}\u{327}", "e\u{301}\u{327}\u{300}" }) |source| {
        var input = try AlignedText.fromSource(allocator, source);
        defer input.deinit(allocator);
        var aligned = try normalizeAlignedStep(allocator, &input, .{ .precompiled = map });
        defer aligned.deinit(allocator);
        const plain = try map.normalize(allocator, source);
        defer allocator.free(plain);
        try std.testing.expectEqualStrings(plain, aligned.text);
        for (aligned.spans) |span| {
            try std.testing.expect(span[0] <= span[1] and span[1] <= source.len);
        }
    }
    // A short grapheme consumes the entire cluster on the FIRST prefix match.
    const short_cluster = try map.normalize(allocator, "e\u{301}\u{327}");
    defer allocator.free(short_cluster);
    try std.testing.expectEqualStrings("é", short_cluster);
    // Six or more bytes skip cluster matching and normalize each codepoint.
    const long_cluster = try map.normalize(allocator, "e\u{301}\u{327}\u{300}");
    defer allocator.free(long_cluster);
    try std.testing.expectEqualStrings("e\u{301}\u{327}\u{300}", long_cluster);
    try std.testing.expectError(error.InvalidTokenizerNormalizer, Precompiled.init(allocator, "AAAA"));
    try std.testing.expectError(error.InvalidTokenizerNormalizer, Precompiled.init(allocator, "not base64"));
}

// BERT uses per-codepoint Unicode lowercase, canonical decomposition and Mn
// removal. Reuse the pinned NFC decomposition data; no locale or OS dependency.
pub const Bert = struct {
    lowercase: bool = true,
    strip_accents: bool = true,
    clean_text: bool = true,
    handle_chinese_chars: bool = false,

    pub fn normalize(self: Bert, allocator: std.mem.Allocator, text: []const u8) ![]u8 {
        var output = std.ArrayListUnmanaged(u8).empty;
        errdefer output.deinit(allocator);
        try output.ensureTotalCapacity(allocator, text.len);
        var units = std.ArrayListUnmanaged(Unit).empty;
        defer units.deinit(allocator);
        var marks = std.ArrayListUnmanaged(Unit).empty;
        defer marks.deinit(allocator);
        var iter = (try std.unicode.Utf8View.init(text)).iterator();
        while (iter.nextCodepoint()) |original| {
            var cp = original;
            // Keep the common ASCII path linear with no Unicode table probes.
            if (cp < 0x80) {
                var byte: u8 = @intCast(cp);
                if (self.clean_text) {
                    if (byte == '\t' or byte == '\n' or byte == '\r') {
                        byte = ' ';
                    } else if (byte < 0x20 or byte == 0x7f) continue;
                }
                try flushBertMarks(allocator, &output, &marks);
                try output.append(allocator, if (self.lowercase) std.ascii.toLower(byte) else byte);
                continue;
            }
            if (self.clean_text) {
                if (cp == 0 or cp == 0xfffd) continue;
                if (cp == '\t' or cp == '\n' or cp == '\r' or inRanges(cp, &data.spaces)) {
                    cp = ' ';
                } else if (inRanges(cp, &data.controls)) continue;
            }
            const chinese = self.handle_chinese_chars and isChineseChar(cp);
            if (chinese) {
                try flushBertMarks(allocator, &output, &marks);
                try output.append(allocator, ' ');
            }
            const identity = [_]u21{cp};
            const lowered = if (self.lowercase) lowercaseCodepoint(cp) orelse &identity else &identity;
            for (lowered) |value| {
                if (self.strip_accents and value >= 0x80) {
                    units.clearRetainingCapacity();
                    try decomposeWordPiece(allocator, value, &units);
                    for (units.items) |unit| {
                        // Canonical ordering precedes Mn removal, including
                        // the uncommon retained spacing combining marks.
                        if (unit.class == 0) try flushBertMarks(allocator, &output, &marks);
                        if (inRanges(unit.cp, &data.nonspacing_marks)) continue;
                        if (unit.class == 0) {
                            try appendCodepoint(allocator, &output, unit.cp);
                        } else {
                            try marks.append(allocator, .{ .cp = unit.cp, .class = unit.class, .order = marks.items.len });
                        }
                    }
                } else {
                    try flushBertMarks(allocator, &output, &marks);
                    try appendCodepoint(allocator, &output, value);
                }
            }
            if (chinese) {
                try flushBertMarks(allocator, &output, &marks);
                try output.append(allocator, ' ');
            }
        }
        try flushBertMarks(allocator, &output, &marks);
        return output.toOwnedSlice(allocator);
    }
};

fn flushBertMarks(allocator: std.mem.Allocator, output: *std.ArrayListUnmanaged(u8), marks: *std.ArrayListUnmanaged(Unit)) !void {
    std.mem.sort(Unit, marks.items, {}, Unit.less);
    for (marks.items) |unit| try appendCodepoint(allocator, output, unit.cp);
    marks.clearRetainingCapacity();
}

fn appendCodepoint(allocator: std.mem.Allocator, output: *std.ArrayListUnmanaged(u8), cp: u21) !void {
    var bytes: [4]u8 = undefined;
    const len = try std.unicode.utf8Encode(cp, &bytes);
    try output.appendSlice(allocator, bytes[0..len]);
}

fn inRanges(cp: u21, ranges: []const data.Range) bool {
    var lo: usize = 0;
    var hi = ranges.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (ranges[mid].last < cp) lo = mid + 1 else hi = mid;
    }
    return lo < ranges.len and ranges[lo].first <= cp;
}

fn lowercaseCodepoint(cp: u21) ?[]const u21 {
    var lo: usize = 0;
    var hi = data.lowercase.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (data.lowercase[mid].cp < cp) lo = mid + 1 else hi = mid;
    }
    return if (lo < data.lowercase.len and data.lowercase[lo].cp == cp) data.lowercase[lo].values else null;
}

fn exerciseBertNormalization(backing: std.mem.Allocator) !void {
    // Fault enumeration must not depend on OS-specific in-place growth.
    // Keep all growth on allocate/copy so every allocation is exercised.
    var vtable = backing.vtable.*;
    vtable.resize = std.mem.Allocator.noResize;
    vtable.remap = std.mem.Allocator.noRemap;
    const allocator: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
    const cases = [_]struct { profile: Bert = .{}, text: []const u8, expected: []const u8 }{
        .{ .text = "CAFÉ cafe\u{301} 世界!", .expected = "cafe cafe 世界!" },
        .{ .text = "İΣẮ が ば ぱ", .expected = "iσa か は は" },
        .{ .text = "a\x00b\u{200d}c\u{a0}d\u{fffd}", .expected = "abc d" },
        .{ .profile = .{ .strip_accents = false }, .text = "CAFÉ", .expected = "café" },
        .{ .profile = .{ .lowercase = false }, .text = "CAFÉ", .expected = "CAFE" },
        .{ .profile = .{ .clean_text = false }, .text = "a\x00b\u{200d}c", .expected = "a\x00b\u{200d}c" },
        // Mn includes zero-combining-class marks (variation selectors).
        .{ .text = "x\u{fe0f}", .expected = "x" },
        .{ .text = "x\u{1d16d}\u{1d165}", .expected = "x\u{1d165}\u{1d16d}" },
        .{ .text = "ASCII\ttext\n", .expected = "ascii text " },
        .{ .text = "a\u{2028}b\u{2029}c", .expected = "a b c" },
        .{ .text = "hello\u{7fd}", .expected = "hello\u{7fd}" },
        .{ .text = "\u{11938}", .expected = "\u{11938}" },
        .{ .text = "a\u{e000}b\u{378}c", .expected = "ab\u{378}c" },
    };
    for (cases) |case| {
        const normalized = try case.profile.normalize(allocator, case.text);
        defer allocator.free(normalized);
        try std.testing.expectEqualStrings(case.expected, normalized);
    }
}

test "BERT normalization honors Unicode categories and independent configuration" {
    try exerciseBertNormalization(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseBertNormalization, .{});
}

pub fn isChineseChar(cp: u21) bool {
    return inRanges(cp, &data.chinese);
}

/// Each normalized byte retains the source interval that produced it. This
/// storage is request-local and needed only when callers request offsets after
/// nontrivial normalization; ordinary token counts use the allocation-light path.
pub const AlignedText = struct {
    text: []u8,
    spans: [][2]u32,

    pub fn deinit(self: *AlignedText, allocator: std.mem.Allocator) void {
        allocator.free(self.text);
        allocator.free(self.spans);
        self.* = undefined;
    }

    fn fromSource(allocator: std.mem.Allocator, text: []const u8) !AlignedText {
        if (text.len > std.math.maxInt(u32)) return error.InputTooLong;
        const copy = try allocator.dupe(u8, text);
        errdefer allocator.free(copy);
        const spans = try allocator.alloc([2]u32, text.len);
        errdefer allocator.free(spans);
        var iter = (try std.unicode.Utf8View.init(text)).iterator();
        while (iter.i < text.len) {
            const start = iter.i;
            _ = iter.nextCodepoint().?;
            @memset(spans[start..iter.i], .{ @intCast(start), @intCast(iter.i) });
        }
        return .{ .text = copy, .spans = spans };
    }

    pub fn sourceRange(self: *const AlignedText, start: usize, end: usize) [2]u32 {
        if (start == end) {
            const point = if (start < self.spans.len) self.spans[start][0] else if (start > 0) self.spans[start - 1][1] else 0;
            return .{ point, point };
        }
        var span = self.spans[start];
        for (self.spans[start + 1 .. end]) |next| span = joinSpans(span, next);
        return span;
    }
};

fn joinSpans(a: [2]u32, b: [2]u32) [2]u32 {
    return .{ @min(a[0], b[0]), @max(a[1], b[1]) };
}

const AlignedBuilder = struct {
    text: std.ArrayListUnmanaged(u8) = .empty,
    spans: std.ArrayListUnmanaged([2]u32) = .empty,
    last_source_byte: ?usize = null,

    fn extendPreviousSpan(self: *@This(), span: [2]u32) void {
        // A deleted trailing accent still belongs to the preceding source
        // grapheme. Skip synthetic Han spaces, which have empty provenance.
        if (self.last_source_byte) |index| self.spans.items[index] = joinSpans(self.spans.items[index], span);
    }

    fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        self.text.deinit(allocator);
        self.spans.deinit(allocator);
    }
    fn append(self: *@This(), allocator: std.mem.Allocator, bytes: []const u8, span: [2]u32) !void {
        try self.text.appendSlice(allocator, bytes);
        try self.spans.appendNTimes(allocator, span, bytes.len);
        if (bytes.len > 0 and span[0] != span[1]) self.last_source_byte = self.spans.items.len - 1;
    }
    fn codepoint(self: *@This(), allocator: std.mem.Allocator, cp: u21, span: [2]u32) !void {
        var bytes: [4]u8 = undefined;
        const len = try std.unicode.utf8Encode(cp, &bytes);
        try self.append(allocator, bytes[0..len], span);
    }
    fn copy(self: *@This(), allocator: std.mem.Allocator, source: *const AlignedText, start: usize, end: usize) !void {
        try self.text.appendSlice(allocator, source.text[start..end]);
        const base = self.spans.items.len;
        try self.spans.appendSlice(allocator, source.spans[start..end]);
        for (source.spans[start..end], 0..) |span, i| {
            if (span[0] != span[1]) self.last_source_byte = base + i;
        }
    }
    fn finish(self: *@This(), allocator: std.mem.Allocator) !AlignedText {
        const text = try self.text.toOwnedSlice(allocator);
        errdefer allocator.free(text);
        return .{ .text = text, .spans = try self.spans.toOwnedSlice(allocator) };
    }
};

fn normalizeAlignedStep(a: std.mem.Allocator, source: *const AlignedText, step: Step) !AlignedText {
    switch (step) {
        .bert => |profile| {
            var transformed = try alignedCharacters(a, source, profile, false);
            if (!profile.strip_accents) return transformed;
            defer transformed.deinit(a);
            return alignedCanonical(a, &transformed, false, true);
        },
        .lowercase => return alignedCharacters(a, source, .{ .strip_accents = false, .clean_text = false }, false),
        .strip_accents => return alignedCharacters(a, source, .{ .lowercase = false, .strip_accents = false, .clean_text = false }, true),
        .nfc => return alignedCanonical(a, source, true, false),
        .nfd => return alignedCanonical(a, source, false, false),
        else => {},
    }
    var out = AlignedBuilder{};
    defer out.deinit(a);
    switch (step) {
        .literal_replace => |replacement| {
            var cursor: usize = 0;
            while (std.mem.indexOfPos(u8, source.text, cursor, replacement.pattern)) |start| {
                try out.copy(a, source, cursor, start);
                cursor = start + replacement.pattern.len;
                try out.append(a, replacement.content, source.sourceRange(start, cursor));
            }
            try out.copy(a, source, cursor, source.text.len);
        },
        .strip => |flags| {
            var first: ?usize = null;
            var last: usize = 0;
            var iter = (try std.unicode.Utf8View.init(source.text)).iterator();
            while (iter.i < source.text.len) {
                const start = iter.i;
                if (!whitespace(iter.nextCodepoint().?)) {
                    if (first == null) first = start;
                    last = iter.i;
                }
            }
            const start = if (flags.left) first orelse source.text.len else 0;
            try out.copy(a, source, start, if (flags.right) @max(start, last) else source.text.len);
        },
        .spaces_replace, .whitespace_replace => {
            const spaces_only = step == .spaces_replace;
            var iter = (try std.unicode.Utf8View.init(source.text)).iterator();
            while (iter.i < source.text.len) {
                const start = iter.i;
                const cp = iter.nextCodepoint().?;
                var count: usize = 1;
                if (if (spaces_only) cp == ' ' else whitespace(cp)) {
                    while (iter.i < source.text.len) {
                        var next = iter;
                        const value = next.nextCodepoint().?;
                        if (!(if (spaces_only) value == ' ' else whitespace(value))) break;
                        iter = next;
                        count += 1;
                    }
                    if (spaces_only or count >= 2 or cp == '\n' or cp == '\r' or cp == '\t') {
                        try out.append(a, " ", source.sourceRange(start, iter.i));
                        continue;
                    }
                }
                try out.copy(a, source, start, iter.i);
            }
        },
        .precompiled => |map| {
            var graphemes = @import("antfly_grapheme").Iterator{ .text = source.text };
            var cursor: usize = 0;
            while (try graphemes.next()) |cluster| {
                const end = cursor + cluster.len;
                if (cluster.len < 6) {
                    if (try map.transform(cluster)) |replacement| {
                        try out.append(a, replacement, source.sourceRange(cursor, end));
                        cursor = end;
                        continue;
                    }
                }
                while (cursor < end) {
                    const len = try std.unicode.utf8ByteSequenceLength(source.text[cursor]);
                    const cp = source.text[cursor..][0..len];
                    try out.append(a, (try map.transform(cp)) orelse cp, source.sourceRange(cursor, cursor + len));
                    cursor += len;
                }
            }
        },
        else => unreachable,
    }
    return out.finish(a);
}

fn alignedCharacters(a: std.mem.Allocator, source: *const AlignedText, profile: Bert, strip_marks: bool) !AlignedText {
    var out = AlignedBuilder{};
    defer out.deinit(a);
    var iter = (try std.unicode.Utf8View.init(source.text)).iterator();
    while (iter.i < source.text.len) {
        const start = iter.i;
        var cp = iter.nextCodepoint().?;
        const span = source.sourceRange(start, iter.i);
        if (profile.clean_text) {
            if (cp < 0x80) {
                if (cp == '\t' or cp == '\n' or cp == '\r') cp = ' ' else if (cp < 0x20 or cp == 0x7f) continue;
            } else {
                if (cp == 0xfffd) continue;
                if (inRanges(cp, &data.spaces)) cp = ' ' else if (inRanges(cp, &data.controls)) continue;
            }
        }
        if (strip_marks and inRanges(cp, &data.strip_accents_marks)) {
            out.extendPreviousSpan(span);
            continue;
        }
        const chinese = profile.handle_chinese_chars and isChineseChar(cp);
        if (chinese) try out.append(a, " ", .{ span[0], span[0] });
        const identity = [_]u21{if (profile.lowercase and cp < 0x80) std.ascii.toLower(@intCast(cp)) else cp};
        const lowered = if (profile.lowercase and cp >= 0x80) lowercaseCodepoint(cp) orelse &identity else &identity;
        for (lowered) |value| try out.codepoint(a, value, span);
        if (chinese) try out.append(a, " ", .{ span[1], span[1] });
    }
    return out.finish(a);
}

fn alignedCanonical(a: std.mem.Allocator, source: *const AlignedText, composing: bool, strip_marks: bool) !AlignedText {
    const MappedUnit = struct {
        unit: Unit,
        span: [2]u32,
        fn less(_: void, left: @This(), right: @This()) bool {
            return Unit.less({}, left.unit, right.unit);
        }
    };
    var units = std.ArrayListUnmanaged(MappedUnit).empty;
    defer units.deinit(a);
    var scratch = std.ArrayListUnmanaged(Unit).empty;
    defer scratch.deinit(a);
    var iter = (try std.unicode.Utf8View.init(source.text)).iterator();
    while (iter.i < source.text.len) {
        const start = iter.i;
        const cp = iter.nextCodepoint().?;
        scratch.clearRetainingCapacity();
        try decomposeWordPiece(a, cp, &scratch);
        for (scratch.items) |unit| try units.append(a, .{ .unit = .{ .cp = unit.cp, .class = unit.class, .order = units.items.len }, .span = source.sourceRange(start, iter.i) });
    }
    var run: usize = 0;
    for (units.items, 0..) |mapped, i| if (mapped.unit.class == 0) {
        std.mem.sort(MappedUnit, units.items[run..i], {}, MappedUnit.less);
        run = i + 1;
    };
    std.mem.sort(MappedUnit, units.items[run..], {}, MappedUnit.less);
    var written: usize = 0;
    var starter: ?usize = null;
    var last_class: u8 = 0;
    for (units.items) |mapped| {
        const unit = mapped.unit;
        if (composing) if (starter) |position| {
            if (last_class == 0 or last_class < unit.class) {
                if (composeWordPiece(units.items[position].unit.cp, unit.cp)) |composed| {
                    units.items[position].unit.cp = composed;
                    units.items[position].span = joinSpans(units.items[position].span, mapped.span);
                    continue;
                }
            }
        };
        if (unit.class == 0) starter = written;
        units.items[written] = mapped;
        written += 1;
        last_class = unit.class;
    }
    var out = AlignedBuilder{};
    defer out.deinit(a);
    for (units.items[0..written]) |mapped| {
        if (strip_marks and inRanges(mapped.unit.cp, &data.nonspacing_marks)) {
            out.extendPreviousSpan(mapped.span);
        } else try out.codepoint(a, mapped.unit.cp, mapped.span);
    }
    return out.finish(a);
}
