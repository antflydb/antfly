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

//! Immutable, typed PostgreSQL ranges. Endpoints borrow an invocation/row
//! region; SQL NULL, empty ranges, unbounded endpoints and subtype infinities
//! remain distinct. One execution context owns allocation, work and cancellation.
const std = @import("std");
const numeric = @import("numeric_value.zig");
const number_binary = @import("numeric_binary.zig");
const temporal = @import("temporal_value.zig");
const temporal_text = @import("temporal_text.zig");
pub const Context = numeric.Context;
pub const Kind = enum(u8) {
    numrange = 0,
    daterange = 1,
    tsrange = 2,
    tstzrange = 3,
    pub fn oid(self: Kind) u32 {
        return switch (self) {
            .numrange => 3906,
            .daterange => 3912,
            .tsrange => 3908,
            .tstzrange => 3910,
        };
    }
};
/// Shared SQL/native schema tags retain four distinct domains without adding
/// another optional payload to every primitive column/type descriptor.
pub fn kindFromType(tag: anytype) ?Kind {
    return switch (tag) {
        .numrange => .numrange,
        .daterange => .daterange,
        .tsrange => .tsrange,
        .tstzrange => .tstzrange,
        else => null,
    };
}
pub fn typeFromKind(comptime T: type, kind: Kind) T {
    return switch (kind) {
        inline else => |tag| @field(T, @tagName(tag)),
    };
}

/// Compact borrowed transport for durable expressions and index tuples.
/// The owning schema/row region supplies lifetime and canonical-byte authority.
pub const Encoded = struct {
    kind: Kind,
    bytes: []const u8,
    pub fn view(self: Encoded, context: *Context) !@import("range_binary.zig").View {
        return @import("range_binary.zig").View.open(context, self.kind, self.bytes);
    }
};

pub const Endpoint = union(enum) { number: numeric.Value, date: temporal.Date, timestamp: temporal.Timestamp };
pub const Value = struct {
    kind: Kind,
    lower: ?Endpoint = null,
    upper: ?Endpoint = null,
    lower_inc: bool = false,
    upper_inc: bool = false,
    empty: bool = false,
    pub fn flags(self: Value) u8 {
        if (self.empty) return 1;
        return @as(u8, if (self.lower_inc) 2 else 0) | @as(u8, if (self.upper_inc) 4 else 0) |
            @as(u8, if (self.lower == null) 8 else 0) | @as(u8, if (self.upper == null) 16 else 0);
    }
};

pub fn endpointOrder(ctx: *Context, a: Endpoint, b: Endpoint) !std.math.Order {
    try ctx.charge(1);
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return error.SqlTypeMismatch;
    return switch (a) {
        .number => |v| numeric.order(ctx, v, b.number),
        .date => |v| std.math.order(v.days, b.date.days),
        .timestamp => |v| std.math.order(v.micros, b.timestamp.micros),
    };
}

fn checkEndpoint(kind: Kind, point: Endpoint) !void {
    switch (kind) {
        .numrange => if (point != .number) return error.SqlTypeMismatch,
        .daterange => {
            if (point != .date) return error.SqlTypeMismatch;
            _ = try temporal.Date.init(point.date.days);
        },
        .tsrange, .tstzrange => {
            if (point != .timestamp) return error.SqlTypeMismatch;
            _ = try temporal.Timestamp.init(point.timestamp.micros);
        },
    }
}

pub fn canonical(ctx: *Context, input: Value) !Value {
    try ctx.charge(1);
    if (input.empty) return .{ .kind = input.kind, .empty = true };
    var value = input;
    if (value.lower) |point| try checkEndpoint(value.kind, point) else value.lower_inc = false;
    if (value.upper) |point| try checkEndpoint(value.kind, point) else value.upper_inc = false;
    if (value.lower != null and value.upper != null) {
        const compared = try endpointOrder(ctx, value.lower.?, value.upper.?);
        if (compared == .gt) return error.InvalidRange;
        if (compared == .eq and (!value.lower_inc or !value.upper_inc)) return .{ .kind = value.kind, .empty = true };
    }
    // DATE is discrete; explicit subtype infinities are not omitted bounds.
    if (value.kind == .daterange) {
        if (value.lower) |point| if (!value.lower_inc and point.date.infinity() == 0) {
            value.lower = .{ .date = try temporal.Date.init(std.math.add(i32, point.date.days, 1) catch return error.TemporalOutOfRange) };
            value.lower_inc = true;
        };
        if (value.upper) |point| if (value.upper_inc and point.date.infinity() == 0) {
            value.upper = .{ .date = try temporal.Date.init(std.math.add(i32, point.date.days, 1) catch return error.TemporalOutOfRange) };
            value.upper_inc = false;
        };
        if (value.lower != null and value.upper != null) {
            const compared = try endpointOrder(ctx, value.lower.?, value.upper.?);
            if (compared == .gt or (compared == .eq and (!value.lower_inc or !value.upper_inc))) return .{ .kind = value.kind, .empty = true };
        }
    }
    return value;
}

fn boundOrder(ctx: *Context, a: ?Endpoint, ai: bool, b: ?Endpoint, bi: bool, lower: bool) !std.math.Order {
    if (a == null or b == null) return if (a == null and b == null) .eq else if ((a == null) == lower) .lt else .gt;
    const compared = try endpointOrder(ctx, a.?, b.?);
    if (compared != .eq or ai == bi) return compared;
    return if (ai == lower) .lt else .gt;
}

pub fn order(ctx: *Context, a: Value, b: Value) !std.math.Order {
    if (a.kind != b.kind) return error.SqlUndefinedOperator;
    if (a.empty or b.empty) return if (a.empty == b.empty) .eq else if (a.empty) .lt else .gt;
    const lower = try boundOrder(ctx, a.lower, a.lower_inc, b.lower, b.lower_inc, true);
    return if (lower == .eq) boundOrder(ctx, a.upper, a.upper_inc, b.upper, b.upper_inc, false) else lower;
}

pub fn before(ctx: *Context, a: Value, b: Value) !bool {
    if (a.kind != b.kind) return error.SqlUndefinedOperator;
    if (a.empty or b.empty or a.upper == null or b.lower == null) return false;
    const compared = try endpointOrder(ctx, a.upper.?, b.lower.?);
    return compared == .lt or (compared == .eq and (!a.upper_inc or !b.lower_inc));
}

pub fn overlaps(ctx: *Context, a: Value, b: Value) !bool {
    if (a.kind != b.kind) return error.SqlUndefinedOperator;
    return !a.empty and !b.empty and !try before(ctx, a, b) and !try before(ctx, b, a);
}

pub fn contains(ctx: *Context, a: Value, b: Value) !bool {
    if (a.kind != b.kind) return error.SqlUndefinedOperator;
    if (b.empty) return true;
    if (a.empty) return false;
    return try boundOrder(ctx, a.lower, a.lower_inc, b.lower, b.lower_inc, true) != .gt and
        try boundOrder(ctx, a.upper, a.upper_inc, b.upper, b.upper_inc, false) != .lt;
}

pub fn containsPoint(ctx: *Context, a: Value, point: Endpoint) !bool {
    try checkEndpoint(a.kind, point);
    if (a.empty) return false;
    if (a.lower) |v| {
        const compared = try endpointOrder(ctx, v, point);
        if (compared == .gt or (compared == .eq and !a.lower_inc)) return false;
    }
    if (a.upper) |v| {
        const compared = try endpointOrder(ctx, v, point);
        if (compared == .lt or (compared == .eq and !a.upper_inc)) return false;
    }
    return true;
}

pub fn adjacent(ctx: *Context, a: Value, b: Value) !bool {
    if (a.kind != b.kind) return error.SqlUndefinedOperator;
    if (a.empty or b.empty) return false;
    if (a.upper != null and b.lower != null and a.upper_inc != b.lower_inc and try endpointOrder(ctx, a.upper.?, b.lower.?) == .eq) return true;
    return a.lower != null and b.upper != null and a.lower_inc != b.upper_inc and try endpointOrder(ctx, a.lower.?, b.upper.?) == .eq;
}

pub fn hash(ctx: *Context, value: Value, hasher: anytype) !void {
    hasher.update(&.{ @backingInt(value.kind), value.flags() });
    for ([_]?Endpoint{ value.lower, value.upper }) |maybe| if (maybe) |point| switch (point) {
        .number => |v| try numeric.hash(ctx, v, hasher),
        .date => |v| {
            var bytes: [4]u8 = undefined;
            std.mem.writeInt(i32, &bytes, v.days, .big);
            hasher.update(&bytes);
        },
        .timestamp => |v| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(i64, &bytes, v.micros, .big);
            hasher.update(&bytes);
        },
    };
}

pub fn parseEndpointLeaky(ctx: *Context, kind: Kind, input: []const u8) !Endpoint {
    try ctx.charge(input.len + 1);
    const text = std.mem.trim(u8, input, " \t\r\n");
    if (kind == .numrange) return .{ .number = (try numeric.parse(ctx, text)).value };
    const target: temporal.Kind = switch (kind) {
        .numrange => unreachable,
        .daterange => .date,
        .tsrange => .timestamp,
        .tstzrange => .timestamp_tz,
    };
    const parsed = try temporal_text.parseInput(target, text);
    return switch (parsed) {
        .date => |v| .{ .date = v },
        .timestamp, .timestamp_tz => |v| .{ .timestamp = v },
        else => unreachable,
    };
}

const TextParser = struct {
    ctx: *Context,
    bytes: []const u8,
    pos: usize = 1,
    fn endpoint(self: *@This(), terminal: u8) !?[]const u8 {
        const start = self.pos;
        var quoted = false;
        var escaped = false;
        var needed: usize = 0;
        while (self.pos < self.bytes.len - 1) : (self.pos += 1) {
            const ch = self.bytes[self.pos];
            if (escaped) {
                escaped = false;
                needed += 1;
                continue;
            }
            if (ch == '\\') {
                escaped = true;
                continue;
            }
            if (ch == '"') {
                if (quoted and self.pos + 1 < self.bytes.len - 1 and self.bytes[self.pos + 1] == '"') {
                    self.pos += 1;
                    needed += 1;
                } else quoted = !quoted;
                continue;
            }
            if (!quoted and ch == terminal) break;
            needed += 1;
        }
        if (quoted or escaped) return error.SqlInvalidTextRepresentation;
        const raw = self.bytes[start..self.pos];
        if (raw.len == 0) return null;
        if (self.pos == self.bytes.len - 1 and terminal == ',') return error.SqlInvalidTextRepresentation;
        try self.ctx.charge(raw.len + needed);
        const out = try self.ctx.alloc.alloc(u8, needed);
        var at: usize = 0;
        var index: usize = 0;
        quoted = false;
        while (index < raw.len) : (index += 1) {
            const ch = raw[index];
            if (ch == '\\') {
                index += 1;
                out[at] = raw[index];
                at += 1;
            } else if (ch == '"') {
                if (quoted and index + 1 < raw.len and raw[index + 1] == '"') {
                    index += 1;
                    out[at] = '"';
                    at += 1;
                } else quoted = !quoted;
            } else {
                out[at] = ch;
                at += 1;
            }
        }
        return out;
    }
};

pub fn parseLeaky(ctx: *Context, kind: Kind, input: []const u8) !Value {
    if (input.len > ctx.max_input_bytes) return ctx.limit();
    try ctx.charge(input.len + 1);
    const text = std.mem.trim(u8, input, " \t\r\n");
    if (std.ascii.eqlIgnoreCase(text, "empty")) return .{ .kind = kind, .empty = true };
    if (text.len < 3 or (text[0] != '[' and text[0] != '(') or (text[text.len - 1] != ']' and text[text.len - 1] != ')')) return error.SqlInvalidTextRepresentation;
    var parser: TextParser = .{ .ctx = ctx, .bytes = text };
    const lower = try parser.endpoint(',');
    if (parser.pos >= text.len - 1 or text[parser.pos] != ',') return error.SqlInvalidTextRepresentation;
    parser.pos += 1;
    const upper = try parser.endpoint(0);
    if (parser.pos != text.len - 1) return error.SqlInvalidTextRepresentation;
    return canonical(ctx, .{
        .kind = kind,
        .lower = if (lower) |v| try parseEndpointLeaky(ctx, kind, v) else null,
        .upper = if (upper) |v| try parseEndpointLeaky(ctx, kind, v) else null,
        .lower_inc = text[0] == '[',
        .upper_inc = text[text.len - 1] == ']',
    });
}

fn endpointTextLeaky(ctx: *Context, point: Endpoint, kind: Kind) ![]const u8 {
    if (point == .number) return numeric.format(ctx, point.number);
    var output: [temporal_text.output_capacity]u8 = undefined;
    const value: temporal.Value = switch (point) {
        .number => unreachable,
        .date => |v| .{ .date = v },
        .timestamp => |v| if (kind == .tsrange) .{ .timestamp = v } else .{ .timestamp_tz = v },
    };
    const text = try temporal_text.format(value, &output);
    try ctx.charge(text.len);
    return ctx.alloc.dupe(u8, text);
}

pub fn formatLeaky(ctx: *Context, value: Value) ![]const u8 {
    if (value.empty) {
        if (ctx.max_output_bytes < 5) return ctx.limit();
        try ctx.charge(5);
        return ctx.alloc.dupe(u8, "empty");
    }
    var parts: [2][]const u8 = .{ "", "" };
    var quotes: [2]bool = .{ false, false };
    var size: usize = 3;
    for ([_]?Endpoint{ value.lower, value.upper }, 0..) |point, i| if (point) |v| {
        parts[i] = try endpointTextLeaky(ctx, v, value.kind);
        quotes[i] = std.mem.indexOfAny(u8, parts[i], &.{ ' ', '\t', '\r', '\n', ',', '\\', '"', '(', ')', '[', ']' }) != null or parts[i].len == 0;
        size += parts[i].len + @as(usize, if (quotes[i]) 2 else 0);
        for (parts[i]) |ch| if (ch == '"' or ch == '\\') {
            size += 1;
        };
    };
    if (size > ctx.max_output_bytes) return ctx.limit();
    try ctx.charge(size);
    const out = try ctx.alloc.alloc(u8, size);
    var w: std.Io.Writer = .fixed(out);
    try w.writeByte(if (value.lower_inc) '[' else '(');
    for (parts, quotes, 0..) |part, quoted, i| {
        if (i != 0) try w.writeByte(',');
        if (quoted) try w.writeByte('"');
        for (part) |ch| {
            if (ch == '"' or ch == '\\') try w.writeByte('\\');
            try w.writeByte(ch);
        }
        if (quoted) try w.writeByte('"');
    }
    try w.writeByte(if (value.upper_inc) ']' else ')');
    return out;
}

fn endpointSize(ctx: *Context, point: Endpoint) !usize {
    return switch (point) {
        .number => |v| number_binary.encodedSize(ctx, v),
        .date => 4,
        .timestamp => 8,
    };
}
fn writeEndpoint(ctx: *Context, point: Endpoint, w: *std.Io.Writer) !void {
    switch (point) {
        .number => |v| try number_binary.encode(ctx, v, w),
        .date => |v| try w.writeInt(i32, v.days, .big),
        .timestamp => |v| try w.writeInt(i64, v.micros, .big),
    }
}

pub fn encodedSize(ctx: *Context, input: Value) !usize {
    const value = try canonical(ctx, input);
    var size: usize = 1;
    if (!value.empty) for ([_]?Endpoint{ value.lower, value.upper }) |point| if (point) |v| {
        size += 4 + try endpointSize(ctx, v);
    };
    if (size > ctx.max_output_bytes or size > std.math.maxInt(i32)) return ctx.limit();
    return size;
}
pub fn encode(ctx: *Context, input: Value, w: *std.Io.Writer) !void {
    const value = try canonical(ctx, input);
    try w.writeByte(value.flags());
    if (!value.empty) for ([_]?Endpoint{ value.lower, value.upper }) |point| if (point) |v| {
        try w.writeInt(u32, @intCast(try endpointSize(ctx, v)), .big);
        try writeEndpoint(ctx, v, w);
    };
}
pub fn encodeLeaky(ctx: *Context, input: Value) ![]u8 {
    const size = try encodedSize(ctx, input);
    try ctx.charge(size);
    const out = try ctx.alloc.alloc(u8, size);
    errdefer ctx.alloc.free(out);
    var w: std.Io.Writer = .fixed(out);
    try encode(ctx, input, &w);
    return out;
}
pub fn retainedSize(value: Value) usize {
    var size: usize = @sizeOf(Value);
    for ([_]?Endpoint{ value.lower, value.upper }) |point| if (point) |v| {
        if (v == .number) size += v.number.digits.len * @sizeOf(u16);
    };
    return size;
}

pub fn decodeInputLeaky(ctx: *Context, kind: Kind, bytes: []const u8) !Value {
    return decodeLeaky(ctx, kind, bytes, false);
}
pub fn decodeCanonicalLeaky(ctx: *Context, kind: Kind, bytes: []const u8) !Value {
    return decodeLeaky(ctx, kind, bytes, true);
}
fn decodeLeaky(ctx: *Context, kind: Kind, bytes: []const u8, strict: bool) !Value {
    if (bytes.len > ctx.max_input_bytes) return ctx.limit();
    try ctx.charge(bytes.len + 1);
    if (bytes.len == 0 or bytes[0] & ~@as(u8, 31) != 0) return error.InvalidSqlBinaryRepresentation;
    const flags = bytes[0];
    if (flags & 1 != 0) {
        if (flags != 1 or bytes.len != 1) return error.InvalidSqlBinaryRepresentation;
        return .{ .kind = kind, .empty = true };
    }
    var value: Value = .{ .kind = kind, .lower_inc = flags & 2 != 0, .upper_inc = flags & 4 != 0 };
    var pos: usize = 1;
    inline for (.{ "lower", "upper" }, 0..) |field, index| {
        if (flags & (@as(u8, 8) << index) == 0) {
            if (bytes.len - pos < 4) return error.InvalidSqlBinaryRepresentation;
            const len = std.mem.readInt(u32, bytes[pos..][0..4], .big);
            pos += 4;
            if (len > bytes.len - pos) return error.InvalidSqlBinaryRepresentation;
            const payload = bytes[pos..][0..len];
            pos += len;
            @field(value, field) = switch (kind) {
                .numrange => .{ .number = (try number_binary.decodeCanonical(ctx, payload)).value },
                .daterange => .{ .date = (try temporal.decode(.date, payload)).date },
                .tsrange => .{ .timestamp = (try temporal.decode(.timestamp, payload)).timestamp },
                .tstzrange => .{ .timestamp = (try temporal.decode(.timestamp_tz, payload)).timestamp_tz },
            };
        }
    }
    if (pos != bytes.len) return error.InvalidSqlBinaryRepresentation;
    const normalized = try canonical(ctx, value);
    if (!strict) return normalized;
    if (normalized.flags() != flags) return error.InvalidSqlBinaryRepresentation;
    inline for (.{ "lower", "upper" }) |field| {
        const a = @field(value, field);
        const b = @field(normalized, field);
        if ((a == null) != (b == null)) return error.InvalidSqlBinaryRepresentation;
        if (a != null and try endpointOrder(ctx, a.?, b.?) != .eq) return error.InvalidSqlBinaryRepresentation;
    }
    return value;
}

test "SQL range codecs and operators match independent PostgreSQL witnesses" {
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(struct {
        entries: []const struct { kind: Kind, input: []const u8, sqlstate: ?[]const u8, text: ?[]const u8 = null, binary: ?[]const u8 = null, lower: ?[]const u8 = null, upper: ?[]const u8 = null, empty: bool = false, lower_inc: bool = false, upper_inc: bool = false, lower_inf: bool = false, upper_inf: bool = false },
        relations: []const struct { kind: Kind, left: []const u8, right: []const u8, equal: bool, less: bool, overlaps: bool, contains: bool, contained: bool, adjacent: bool },
    }, a, @embedFile("fixtures/sql_range_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    for (fixture.value.entries) |entry| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var ctx: Context = .{ .alloc = arena.allocator() };
        const value = parseLeaky(&ctx, entry.kind, entry.input) catch |err| {
            errdefer std.debug.print("range input {s} {s}: {s}\n", .{ @tagName(entry.kind), entry.input, @errorName(err) });
            try std.testing.expectEqualStrings(entry.sqlstate orelse return err, @import("errors.zig").describe(err).code);
            continue;
        };
        if (entry.sqlstate) |code| {
            std.debug.print("range unexpectedly accepted {s} {s}: expected {s}\n", .{ @tagName(entry.kind), entry.input, code });
            return error.TestUnexpectedResult;
        }
        try std.testing.expectEqualStrings(entry.text.?, try formatLeaky(&ctx, value));
        const binary = try encodeLeaky(&ctx, value);
        const expected = try ctx.alloc.alloc(u8, entry.binary.?.len / 2);
        _ = try std.fmt.hexToBytes(expected, entry.binary.?);
        try std.testing.expectEqualSlices(u8, expected, binary);
        const decoded = try decodeCanonicalLeaky(&ctx, entry.kind, binary);
        try std.testing.expectEqualStrings(entry.text.?, try formatLeaky(&ctx, decoded));
        try std.testing.expectEqual(entry.empty, value.empty);
        try std.testing.expectEqual(entry.lower_inc, value.lower_inc);
        try std.testing.expectEqual(entry.upper_inc, value.upper_inc);
        try std.testing.expectEqual(entry.lower_inf, !value.empty and value.lower == null);
        try std.testing.expectEqual(entry.upper_inf, !value.empty and value.upper == null);
        if (value.lower) |point| try std.testing.expectEqualStrings(entry.lower.?, try endpointTextLeaky(&ctx, point, entry.kind));
        if (value.upper) |point| try std.testing.expectEqualStrings(entry.upper.?, try endpointTextLeaky(&ctx, point, entry.kind));
    }
    for (fixture.value.relations) |entry| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var ctx: Context = .{ .alloc = arena.allocator() };
        const left = try parseLeaky(&ctx, entry.kind, entry.left);
        const right = try parseLeaky(&ctx, entry.kind, entry.right);
        errdefer std.debug.print("range relation {s} left={s} right={s}\n", .{ @tagName(entry.kind), entry.left, entry.right });
        const compared = try order(&ctx, left, right);
        try std.testing.expectEqual(entry.equal, compared == .eq);
        try std.testing.expectEqual(entry.less, compared == .lt);
        try std.testing.expectEqual(entry.overlaps, try overlaps(&ctx, left, right));
        try std.testing.expectEqual(entry.contains, try contains(&ctx, left, right));
        try std.testing.expectEqual(entry.contained, try contains(&ctx, right, left));
        try std.testing.expectEqual(entry.adjacent, try adjacent(&ctx, left, right));
        if (compared == .eq) {
            var l = std.hash.Wyhash.init(0);
            var r = std.hash.Wyhash.init(0);
            try hash(&ctx, left, &l);
            try hash(&ctx, right, &r);
            try std.testing.expectEqual(l.final(), r.final());
        }
    }
}
