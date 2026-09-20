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

//! Ordered Unigram normalization for the released GLiNER2.5 tokenizers.
//! Unicode canonical decomposition/composition follows Unicode 15.0.0.
const std = @import("std");
const data = @import("unicode_nfc_data.zig");

pub const Step = union(enum) {
    nfc,
    whitespace_replace,
    spaces_replace,
    precompiled: Precompiled,
    strip: struct { left: bool, right: bool },
};

pub const Profile = struct {
    steps: [32]Step = undefined,
    len: usize = 0,

    pub fn parse(self: *Profile, allocator: std.mem.Allocator, value: std.json.Value) !void {
        try self.parseDepth(allocator, value, 0);
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

    fn parseDepth(self: *Profile, allocator: std.mem.Allocator, value: std.json.Value, depth: usize) anyerror!void {
        if (depth >= 16 or value != .object) return error.InvalidTokenizerNormalizer;
        const obj = value.object;
        const kind = obj.get("type") orelse return error.InvalidTokenizerNormalizer;
        if (kind != .string) return error.InvalidTokenizerNormalizer;
        if (std.mem.eql(u8, kind.string, "Sequence")) {
            const children = obj.get("normalizers") orelse return error.InvalidTokenizerNormalizer;
            if (children != .array) return error.InvalidTokenizerNormalizer;
            for (children.array.items) |child| try self.parseDepth(allocator, child, depth + 1);
        } else if (std.mem.eql(u8, kind.string, "Precompiled")) {
            const encoded = obj.get("precompiled_charsmap") orelse return error.UnsupportedTokenizerNormalizer;
            if (encoded != .string) return error.InvalidTokenizerNormalizer;
            if (encoded.string.len == 0) return error.UnsupportedTokenizerNormalizer;
            const map = try Precompiled.init(allocator, encoded.string);
            errdefer allocator.free(map.bytes);
            try self.append(.{ .precompiled = map });
        } else if (std.mem.eql(u8, kind.string, "NFC")) {
            try self.append(.nfc);
        } else if (std.mem.eql(u8, kind.string, "Strip")) {
            try self.append(.{ .strip = .{ .left = try boolean(obj, "strip_left", true), .right = try boolean(obj, "strip_right", true) } });
        } else if (std.mem.eql(u8, kind.string, "Replace")) {
            const pattern = obj.get("pattern") orelse return error.InvalidTokenizerNormalizer;
            const content = obj.get("content") orelse return error.InvalidTokenizerNormalizer;
            if (pattern != .object or content != .string) return error.InvalidTokenizerNormalizer;
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
            else => {},
        };
        self.len = 0;
    }

    pub fn normalize(self: *const Profile, allocator: std.mem.Allocator, text: []const u8) ![]u8 {
        var owned = try allocator.dupe(u8, text);
        errdefer allocator.free(owned);
        for (self.steps[0..self.len]) |step| {
            const next = switch (step) {
                .nfc => try nfc(allocator, owned),
                .precompiled => |map| try map.normalize(allocator, owned),
                .spaces_replace => try collapseSpaces(allocator, owned),
                .whitespace_replace => try replaceWhitespace(allocator, owned),
                .strip => |flags| try strip(allocator, owned, flags.left, flags.right),
            };
            allocator.free(owned);
            owned = next;
        }
        return owned;
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

    fn normalize(self: Precompiled, allocator: std.mem.Allocator, text: []const u8) ![]u8 {
        _ = try std.unicode.Utf8View.init(text);
        var output = std.ArrayListUnmanaged(u8).empty;
        errdefer output.deinit(allocator);
        var start: usize = 0;
        while (start < text.len) {
            var cursor = offset(try self.unit(0));
            var matched: usize = 0;
            var replacement: []const u8 = "";
            // Longest matching prefix permits composed-character rules as
            // well as compatibility rewrites and deletion (empty values).
            for (text[start..], 0..) |byte, i| {
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
                    replacement = table[index..][0..end];
                    matched = i + 1;
                }
            }
            if (matched != 0) {
                try output.appendSlice(allocator, replacement);
                start += matched;
            } else {
                const length = try std.unicode.utf8ByteSequenceLength(text[start]);
                try output.appendSlice(allocator, text[start..][0..length]);
                start += length;
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

pub fn nfc(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    const view = try std.unicode.Utf8View.init(text);
    var iter = view.iterator();
    var units = std.ArrayListUnmanaged(Unit).empty;
    defer units.deinit(allocator);
    while (iter.nextCodepoint()) |cp| try decompose(allocator, cp, &units);
    // Canonical ordering must be stable. An explicit original-order key lets
    // the O(n log n) sort avoid quadratic insertion work on long mark runs.
    var run: usize = 0;
    for (units.items, 0..) |unit, i| if (unit.class == 0) {
        std.mem.sort(Unit, units.items[run..i], {}, Unit.less);
        run = i + 1;
    };
    std.mem.sort(Unit, units.items[run..], {}, Unit.less);
    var written: usize = 0;
    var starter: ?usize = null;
    var last_class: u8 = 0;
    for (units.items) |unit| {
        if (starter) |position| {
            if (last_class == 0 or last_class < unit.class) {
                if (compose(units.items[position].cp, unit.cp)) |composed| {
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
    try std.testing.expectError(error.InvalidTokenizerNormalizer, Precompiled.init(allocator, "AAAA"));
    try std.testing.expectError(error.InvalidTokenizerNormalizer, Precompiled.init(allocator, "not base64"));
}
