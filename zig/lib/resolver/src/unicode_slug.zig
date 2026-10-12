// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Locale-independent NFKC/case-folded Unicode letter and digit slugs.
const std = @import("std");
const data = @import("unicode_slug_data.zig");
const Unit = struct {
    cp: u21,
    class: u8,
    order: usize,
    fn less(_: void, left: Unit, right: Unit) bool {
        return left.class < right.class or (left.class == right.class and left.order < right.order);
    }
};
fn find(comptime field: []const u8, rows: anytype, key: anytype) usize {
    var lo: usize = 0;
    var hi: usize = rows.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (@field(rows[mid], field) < key) lo = mid + 1 else hi = mid;
    }
    return lo;
}
fn combiningClass(cp: u21) u8 {
    const i = find("cp", &data.classes, cp);
    return if (i < data.classes.len and data.classes[i].cp == cp) data.classes[i].class else 0;
}
fn appendUnit(a: std.mem.Allocator, units: *std.ArrayListUnmanaged(Unit), cp: u21) !void {
    try units.append(a, .{ .cp = cp, .class = combiningClass(cp), .order = units.items.len });
}
fn decompose(a: std.mem.Allocator, units: *std.ArrayListUnmanaged(Unit), cp: u21) !void {
    if (cp >= 0xac00 and cp <= 0xd7a3) {
        const index = cp - 0xac00;
        try appendUnit(a, units, 0x1100 + index / 588);
        try appendUnit(a, units, 0x1161 + (index % 588) / 28);
        if (index % 28 != 0) try appendUnit(a, units, 0x11a7 + index % 28);
        return;
    }
    const i = find("cp", &data.mappings, cp);
    if (i < data.mappings.len and data.mappings[i].cp == cp) {
        const mapping = data.mappings[i];
        for (data.values[mapping.offset..][0..mapping.len]) |value| try appendUnit(a, units, value);
    } else try appendUnit(a, units, cp);
}
fn compose(left: u21, right: u21) ?u21 {
    if (left >= 0x1100 and left < 0x1113 and right >= 0x1161 and right < 0x1176)
        return 0xac00 + (left - 0x1100) * 588 + (right - 0x1161) * 28;
    if (left >= 0xac00 and left <= 0xd7a3 and (left - 0xac00) % 28 == 0 and right > 0x11a7 and right < 0x11c3)
        return left + right - 0x11a7;
    const pair = (@as(u42, left) << 21) | right;
    const i = find("pair", &data.compositions, pair);
    return if (i < data.compositions.len and data.compositions[i].pair == pair) data.compositions[i].cp else null;
}
fn letterOrDigit(cp: u21) bool {
    const i = find("last", &data.letters_digits, cp);
    return i < data.letters_digits.len and data.letters_digits[i].first <= cp;
}
fn mark(cp: u21) bool {
    const i = find("last", &data.marks, cp);
    return i < data.marks.len and data.marks[i].first <= cp;
}
pub fn append(a: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), text: []const u8) !void {
    const view = try std.unicode.Utf8View.init(text);
    var iter = view.iterator();
    var units: std.ArrayListUnmanaged(Unit) = .empty;
    defer units.deinit(a);
    while (iter.nextCodepoint()) |cp| try decompose(a, &units, cp);
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
                if (compose(units.items[position].cp, unit.cp)) |combined| {
                    units.items[position].cp = combined;
                    continue;
                }
            }
        }
        if (unit.class == 0) starter = written;
        units.items[written] = unit;
        written += 1;
        last_class = unit.class;
    }
    var wrote = false;
    var separator = false;
    for (units.items[0..written]) |unit| {
        if (!letterOrDigit(unit.cp) and !(wrote and !separator and mark(unit.cp))) {
            if (wrote) separator = true;
            continue;
        }
        if (separator) try out.append(a, '_');
        var buffer: [4]u8 = undefined;
        const size = try std.unicode.utf8Encode(unit.cp, &buffer);
        try out.appendSlice(a, buffer[0..size]);
        separator = false;
        wrote = true;
    }
    if (!wrote) return error.EmptyEntityKey;
}
