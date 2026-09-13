// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Ordered Unigram normalization for the released GLiNER2.5 tokenizers.
//! Unicode canonical decomposition/composition follows Unicode 15.0.0.
const std = @import("std");
const data = @import("unicode_nfc_data.zig");

pub const Step = union(enum) {
    nfc,
    whitespace_replace,
    strip: struct { left: bool, right: bool },
};

pub const Profile = struct {
    steps: [32]Step = undefined,
    len: usize = 0,

    pub fn parse(self: *Profile, value: std.json.Value) !void {
        try self.parseDepth(value, 0);
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

    fn parseDepth(self: *Profile, value: std.json.Value, depth: usize) error{ InvalidTokenizerNormalizer, UnsupportedTokenizerNormalizer }!void {
        if (depth >= 16 or value != .object) return error.InvalidTokenizerNormalizer;
        const obj = value.object;
        const kind = obj.get("type") orelse return error.InvalidTokenizerNormalizer;
        if (kind != .string) return error.InvalidTokenizerNormalizer;
        if (std.mem.eql(u8, kind.string, "Sequence")) {
            const children = obj.get("normalizers") orelse return error.InvalidTokenizerNormalizer;
            if (children != .array) return error.InvalidTokenizerNormalizer;
            for (children.array.items) |child| try self.parseDepth(child, depth + 1);
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
            if (!std.mem.eql(u8, regex.string, "\\s{2,}|[\\n\\r\\t]") or !std.mem.eql(u8, content.string, " "))
                return error.UnsupportedTokenizerNormalizer;
            try self.append(.whitespace_replace);
        } else return error.UnsupportedTokenizerNormalizer;
    }

    pub fn normalize(self: *const Profile, allocator: std.mem.Allocator, text: []const u8) ![]u8 {
        var owned = try allocator.dupe(u8, text);
        errdefer allocator.free(owned);
        for (self.steps[0..self.len]) |step| {
            const next = switch (step) {
                .nfc => try nfc(allocator, owned),
                .whitespace_replace => try replaceWhitespace(allocator, owned),
                .strip => |flags| try strip(allocator, owned, flags.left, flags.right),
            };
            allocator.free(owned);
            owned = next;
        }
        return owned;
    }
};

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
    try profile.parse(parsed.value);
    const got = try profile.normalize(a, " e\u{301}  x\t\n");
    defer a.free(got);
    try std.testing.expectEqualStrings(" é x", got);
    const unknown = try std.json.parseFromSlice(std.json.Value, a, "{\"type\":\"Precompiled\"}", .{});
    defer unknown.deinit();
    try std.testing.expectError(error.UnsupportedTokenizerNormalizer, profile.parse(unknown.value));
}
