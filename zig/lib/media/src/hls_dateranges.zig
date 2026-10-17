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

//! Exact calendar/fraction comparison and immutable HLS date-range attributes.
const std = @import("std");
const attrs = @import("hls_attributes.zig");
const Control = @import("source.zig").Control;
pub const Instant = struct {
    seconds: i64,
    fraction: []const u8 = &.{},
    pub fn order(a: Instant, b: Instant) std.math.Order {
        const whole = std.math.order(a.seconds, b.seconds);
        return if (whole != .eq) whole else std.mem.order(u8, a.fraction, b.fraction);
    }
};
fn integer(s: []const u8) !u64 {
    if (s.len == 0) return error.MalformedHls;
    for (s) |c| if (c < '0' or c > '9') return error.MalformedHls;
    return std.fmt.parseInt(u64, s, 10) catch error.MalformedHls;
}
fn leap(y: u64) bool {
    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0);
}
fn yearDays(y: u64) i64 {
    return @intCast(365 * y + (y + 3) / 4 - (y + 99) / 100 + (y + 399) / 400);
}
pub fn instant(text: []const u8) !Instant {
    if (text.len < 19 or text[4] != '-' or text[7] != '-' or (text[10] != 'T' and text[10] != 't') or text[13] != ':' or text[16] != ':') return error.MalformedHls;
    const year = try integer(text[0..4]);
    const month = try integer(text[5..7]);
    const day = try integer(text[8..10]);
    const hour = try integer(text[11..13]);
    const minute = try integer(text[14..16]);
    const second = try integer(text[17..19]);
    if (month == 0 or month > 12 or hour > 23 or minute > 59 or second > 59) return error.MalformedHls;
    const months = [_]u64{ 31, if (leap(year)) 29 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (day == 0 or day > months[month - 1]) return error.MalformedHls;
    var days = yearDays(year) - yearDays(1970) + @as(i64, @intCast(day - 1));
    for (months[0..@intCast(month - 1)]) |count| days += @intCast(count);
    var at: usize = 19;
    var fraction: []const u8 = &.{};
    if (at < text.len and text[at] == '.') {
        at += 1;
        const first = at;
        while (at < text.len and text[at] >= '0' and text[at] <= '9') at += 1;
        if (at == first) return error.MalformedHls;
        fraction = std.mem.trimEnd(u8, text[first..at], "0");
    }
    var offset: i64 = 0;
    const zone = text[at..];
    if (zone.len != 0 and !std.mem.eql(u8, zone, "Z") and !std.mem.eql(u8, zone, "z")) {
        if ((zone[0] != '+' and zone[0] != '-') or (zone.len != 3 and zone.len != 5 and zone.len != 6)) return error.MalformedHls;
        const hh = try integer(zone[1..3]);
        if (zone.len == 6 and zone[3] != ':') return error.MalformedHls;
        const mm = if (zone.len == 3) 0 else try integer(zone[if (zone.len == 6) @as(usize, 4) else 3..]);
        if (hh > 23 or mm > 59) return error.MalformedHls;
        offset = @intCast((hh * 60 + mm) * 60);
        if (zone[0] == '-') offset = -offset;
    }
    return .{ .seconds = days * 86400 + @as(i64, @intCast(hour * 3600 + minute * 60 + second)) - offset, .fraction = fraction };
}
const Decimal = struct { whole: []const u8, fraction: []const u8 };
fn decimal(s: []const u8) !Decimal {
    var parts = std.mem.splitScalar(u8, s, '.');
    const whole = parts.next().?;
    const fraction = parts.next() orelse "";
    if (parts.next() != null or whole.len + fraction.len == 0) return error.MalformedHls;
    for (whole) |c| if (c < '0' or c > '9') return error.MalformedHls;
    for (fraction) |c| if (c < '0' or c > '9') return error.MalformedHls;
    return .{ .whole = std.mem.trimStart(u8, whole, "0"), .fraction = std.mem.trimEnd(u8, fraction, "0") };
}
// Compare a decimal duration with an exact calendar difference, without float
// cancellation or a precision cap. Reverse digit subtraction needs no scratch.
fn compareDuration(text: []const u8, start: Instant, end: Instant) !std.math.Order {
    const d = try decimal(text);
    if (Instant.order(end, start) == .lt) return .gt;
    const borrow_whole = std.mem.order(u8, end.fraction, start.fraction) == .lt;
    const seconds: u64 = @intCast(end.seconds - start.seconds - @intFromBool(borrow_whole));
    var buf: [24]u8 = undefined;
    const whole = std.mem.trimStart(u8, try std.fmt.bufPrint(&buf, "{d}", .{seconds}), "0");
    const size = std.math.order(d.whole.len, whole.len);
    if (size != .eq) return size;
    const order = std.mem.order(u8, d.whole, whole);
    if (order != .eq) return order;
    var i = @max(d.fraction.len, @max(start.fraction.len, end.fraction.len));
    var borrow: i16 = 0;
    var result: std.math.Order = .eq;
    while (i != 0) {
        i -= 1;
        var difference: i16 = @as(i16, if (i < end.fraction.len) end.fraction[i] - '0' else 0) - @as(i16, if (i < start.fraction.len) start.fraction[i] - '0' else 0) - borrow;
        borrow = if (difference < 0) 1 else 0;
        if (difference < 0) difference += 10;
        const digit: i16 = if (i < d.fraction.len) d.fraction[i] - '0' else 0;
        if (digit != difference) result = std.math.order(digit, difference);
    }
    return result;
}
fn hex(s: []const u8) !void {
    if (s.len < 3 or s[0] != '0' or (s[1] != 'x' and s[1] != 'X')) return error.MalformedHls;
    for (s[2..]) |c| if (!std.ascii.isHex(c)) return error.MalformedHls;
}
pub const Range = struct {
    attributes: attrs.List,
    start: Instant = undefined,
    explicit_end: ?Instant = null,
    /// Once a Following Range resolves END-ON-NEXT, retain that exact endpoint
    /// even if v2 subsequently removes the Following Range's metadata.
    resolved_end: ?Instant = null,
    pub fn id(self: *const Range) []const u8 {
        return self.attributes.get("ID").?.value;
    }
    pub fn class(self: *const Range) ?[]const u8 {
        return if (self.attributes.get("CLASS")) |v| v.value else null;
    }
    pub fn duration(self: *const Range) ?[]const u8 {
        return if (self.attributes.get("DURATION")) |v| v.value else null;
    }
    pub fn endOnNext(self: *const Range) bool {
        return self.attributes.get("END-ON-NEXT") != null;
    }
    fn empty(self: *const Range) bool {
        if (self.explicit_end) |end| if (Instant.order(end, self.start) == .eq) return true;
        if (self.duration()) |value| {
            const d = decimal(value) catch return false;
            return d.whole.len == 0 and d.fraction.len == 0;
        }
        return false;
    }
    fn validate(self: *Range, value_limit: usize) !void {
        _ = (try self.attributes.string("ID")) orelse return error.MalformedHls;
        self.start = try instant((try self.attributes.string("START-DATE")) orelse return error.HlsDeltaHistoryMissing);
        self.explicit_end = if (try self.attributes.string("END-DATE")) |v| try instant(v) else null;
        if (self.explicit_end) |end| if (Instant.order(end, self.start) == .lt) return error.MalformedHls;
        for (self.attributes.items[0..self.attributes.len]) |v| {
            if (v.value.len > value_limit) return error.ResourceLimitExceeded;
            if (v.quoted and v.value.len == 0) return error.MalformedHls;
            if (std.mem.eql(u8, v.name, "CLASS")) {
                _ = try self.attributes.string("CLASS");
            } else if (std.mem.eql(u8, v.name, "DURATION") or std.mem.eql(u8, v.name, "PLANNED-DURATION")) {
                if (v.quoted) return error.MalformedHls;
                _ = try decimal(v.value);
            } else if (std.mem.eql(u8, v.name, "END-ON-NEXT")) {
                if (!(try self.attributes.yesNo("END-ON-NEXT")).?) return error.MalformedHls;
            } else if (std.mem.startsWith(u8, v.name, "SCTE35-")) {
                if (v.quoted) return error.MalformedHls;
                try hex(v.value);
            } else if (std.mem.startsWith(u8, v.name, "X-")) {
                if (v.name.len == 2) return error.MalformedHls;
                if (!v.quoted) {
                    if (std.mem.startsWith(u8, v.value, "0x") or std.mem.startsWith(u8, v.value, "0X")) try hex(v.value) else _ = try decimal(if (std.mem.startsWith(u8, v.value, "-")) v.value[1..] else v.value);
                }
            } else if (std.mem.eql(u8, v.name, "CUE")) {
                if (!v.quoted) return error.MalformedHls;
                var seen: u8 = 0;
                var fields = std.mem.splitScalar(u8, v.value, ',');
                while (fields.next()) |field| {
                    const bit: u8 = if (std.mem.eql(u8, field, "PRE")) 1 else if (std.mem.eql(u8, field, "POST")) 2 else if (std.mem.eql(u8, field, "ONCE")) 4 else return error.MalformedHls;
                    if (seen & bit != 0) return error.MalformedHls;
                    seen |= bit;
                }
                if (seen & 3 == 3) return error.MalformedHls;
            }
        }
        if (self.endOnNext() and (self.class() == null or self.duration() != null or self.explicit_end != null)) return error.MalformedHls;
        if (self.duration()) |d| if (self.explicit_end) |end| if (try compareDuration(d, self.start, end) != .eq) return error.MalformedHls;
    }
};
fn sameValue(a: attrs.Attribute, b: attrs.Attribute) bool {
    if (a.quoted != b.quoted) return false;
    if (std.mem.eql(u8, a.value, b.value)) return true;
    if (!a.quoted) {
        if ((std.mem.startsWith(u8, a.value, "0x") or std.mem.startsWith(u8, a.value, "0X")) and (std.mem.startsWith(u8, b.value, "0x") or std.mem.startsWith(u8, b.value, "0X"))) return std.ascii.eqlIgnoreCase(a.value, b.value);
        const an = std.mem.startsWith(u8, a.value, "-");
        const bn = std.mem.startsWith(u8, b.value, "-");
        const x = decimal(if (an) a.value[1..] else a.value) catch return false;
        const y = decimal(if (bn) b.value[1..] else b.value) catch return false;
        const zero = x.whole.len == 0 and x.fraction.len == 0 and y.whole.len == 0 and y.fraction.len == 0;
        return (an == bn or zero) and std.mem.eql(u8, x.whole, y.whole) and std.mem.eql(u8, x.fraction, y.fraction);
    }
    return false;
}
pub fn upsert(a: std.mem.Allocator, list: *std.ArrayList(Range), fields: attrs.List, previous: []const Range, limit: usize, value_limit: usize) !void {
    const id = (try fields.string("ID")) orelse return error.MalformedHls;
    var index: ?usize = null;
    for (list.items, 0..) |row, i| if (std.mem.eql(u8, row.id(), id)) {
        index = i;
        break;
    };
    var candidate = if (index) |i| list.items[i] else blk: {
        for (previous) |row| if (std.mem.eql(u8, row.id(), id)) break :blk row;
        break :blk Range{ .attributes = .{} };
    };
    for (fields.items[0..fields.len]) |field| {
        if (candidate.attributes.get(field.name)) |old| {
            if (!sameValue(old, field)) return error.SourceVersionChanged;
        } else {
            if (candidate.attributes.len == candidate.attributes.items.len) return error.ResourceLimitExceeded;
            candidate.attributes.items[candidate.attributes.len] = field;
            candidate.attributes.len += 1;
        }
    }
    try candidate.validate(value_limit);
    if (index) |i| list.items[i] = candidate else {
        if (list.items.len >= limit) return error.ResourceLimitExceeded;
        try list.append(a, candidate);
    }
}
pub fn resolve(list: []Range, value_limit: usize, control: Control) !void {
    for (list) |*row| {
        try control.check();
        try row.validate(value_limit);
    }
    for (list) |*row| if (row.endOnNext()) {
        var following: ?Instant = null;
        for (list) |other| {
            try control.check();
            if (other.class()) |c| if (std.mem.eql(u8, row.class().?, c) and Instant.order(other.start, row.start) == .gt) {
                if (following == null or Instant.order(other.start, following.?) == .lt) following = other.start;
            };
        }
        if (following) |end| {
            if (row.resolved_end) |old| {
                if (Instant.order(end, old) == .lt) return error.SourceVersionChanged;
            } else row.resolved_end = end;
        }
    };
    // Only classes using END-ON-NEXT impose non-overlapping intervals.
    for (list, 0..) |reference, class_index| {
        if (!reference.endOnNext()) continue;
        var already = false;
        for (list[0..class_index]) |previous| if (previous.endOnNext() and std.mem.eql(u8, previous.class().?, reference.class().?)) {
            already = true;
            break;
        };
        if (already) continue;
        for (list, 0..) |left, i| {
            const c = left.class() orelse continue;
            if (!std.mem.eql(u8, c, reference.class().?)) continue;
            for (list[i + 1 ..]) |right| {
                try control.check();
                const rc = right.class() orelse continue;
                if (!std.mem.eql(u8, c, rc)) continue;
                if (left.empty() or right.empty()) continue;
                const earlier = if (Instant.order(left.start, right.start) == .gt) right else left;
                const later = if (Instant.order(left.start, right.start) == .gt) left else right;
                if (earlier.explicit_end orelse earlier.resolved_end) |end| {
                    if (Instant.order(end, later.start) == .gt) return error.MalformedHls;
                } else if (earlier.duration()) |d| {
                    if (try compareDuration(d, earlier.start, later.start) == .gt) return error.MalformedHls;
                }
            }
        }
    }
}
test "HLS date-range times compare exact fractions offsets and calendar boundaries" {
    try std.testing.expectEqual(@as(i64, 0), (try instant("1970-01-01T00:00:00Z")).seconds);
    try std.testing.expectEqual(std.math.Order.eq, Instant.order(try instant("2000-02-29T01:00:00.1234567890123+01:00"), try instant("2000-02-29T00:00:00.123456789012300Z")));
    try std.testing.expectEqual(std.math.Order.eq, try compareDuration("0.0000000000002", try instant("2026-01-01T00:00:00.9999999999999Z"), try instant("2026-01-01T00:00:01.0000000000001Z")));
    try std.testing.expectError(error.MalformedHls, instant("2100-02-29T00:00:00Z"));
}

test "HLS date-range signed numeric extensions and empty intervals remain exact" {
    const a = std.testing.allocator;
    var rows: std.ArrayList(Range) = .empty;
    defer rows.deinit(a);
    try upsert(a, &rows, try attrs.parse("ID=\"a\",CLASS=\"ad\",START-DATE=\"2026-01-01T00:00:00Z\",END-ON-NEXT=YES,X-NUM=-.5,X-ZERO=-0.0"), &.{}, 8, 256);
    try upsert(a, &rows, try attrs.parse("ID=\"a\",X-NUM=-0.50,X-ZERO=0"), &.{}, 8, 256);
    try std.testing.expectError(error.SourceVersionChanged, upsert(a, &rows, try attrs.parse("ID=\"a\",X-NUM=.5"), &.{}, 8, 256));
    try upsert(a, &rows, try attrs.parse("ID=\"empty\",CLASS=\"ad\",START-DATE=\"2026-01-01T00:00:00Z\",DURATION=.0"), &.{}, 8, 256);
    try upsert(a, &rows, try attrs.parse("ID=\"b\",CLASS=\"ad\",START-DATE=\"2026-01-01T00:00:01Z\",DURATION=1."), &.{}, 8, 256);
    try resolve(rows.items, 256, .{});
    try std.testing.expectEqual(std.math.Order.eq, Instant.order(rows.items[0].resolved_end.?, rows.items[2].start));
    try std.testing.expectError(error.MalformedHls, upsert(a, &rows, try attrs.parse("ID=\"bad\",START-DATE=\"2026-01-01T00:00:00Z\",X-NOTE=\"\""), &.{}, 8, 256));
    try std.testing.expectError(error.MalformedHls, upsert(a, &rows, try attrs.parse("ID=\"bad\",START-DATE=\"2026-01-01T00:00:00Z\",DURATION=-1"), &.{}, 8, 256));
}
