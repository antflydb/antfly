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

//! Allocation-free, canonical range views for physical admission, integrity
//! membership, hashing and native predicates. A view borrows immutable bytes.
//! It does not repair restore bytes or infer a subtype from a payload.
const std = @import("std");
const ranges = @import("range_value.zig");
const temporal = @import("temporal_value.zig");
const numbers = @import("../common/sql_numeric_layout.zig");
pub const Endpoint = union(enum) { number: numbers.View, date: temporal.Date, timestamp: temporal.Timestamp };
pub const View = struct {
    kind: ranges.Kind,
    bytes: []const u8,
    lower: ?Endpoint = null,
    upper: ?Endpoint = null,
    lower_inc: bool = false,
    upper_inc: bool = false,
    empty: bool = false,

    pub fn open(ctx: *ranges.Context, kind: ranges.Kind, bytes: []const u8) !View {
        if (bytes.len > ctx.max_input_bytes) return ctx.limit();
        try ctx.charge(bytes.len + 1);
        if (bytes.len == 0 or bytes[0] & ~@as(u8, 31) != 0) return error.InvalidSqlBinaryRepresentation;
        const flags = bytes[0];
        var value: View = .{ .kind = kind, .bytes = bytes, .lower_inc = flags & 2 != 0, .upper_inc = flags & 4 != 0, .empty = flags & 1 != 0 };
        if (value.empty) {
            if (flags != 1 or bytes.len != 1) return error.InvalidSqlBinaryRepresentation;
            return value;
        }
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
                    .numrange => .{ .number = try numbers.View.openWithBudget(payload, .{ .bytes = ctx.max_input_bytes, .groups = ctx.max_groups }, ctx) },
                    .daterange => .{ .date = (try temporal.decode(.date, payload)).date },
                    .tsrange => .{ .timestamp = (try temporal.decode(.timestamp, payload)).timestamp },
                    .tstzrange => .{ .timestamp = (try temporal.decode(.timestamp_tz, payload)).timestamp_tz },
                };
            }
        }
        if (pos != bytes.len or (value.lower == null and value.lower_inc) or (value.upper == null and value.upper_inc)) return error.InvalidSqlBinaryRepresentation;
        if (kind == .daterange) {
            if (value.lower) |v| if (v.date.infinity() == 0 and !value.lower_inc) return error.InvalidSqlBinaryRepresentation;
            if (value.upper) |v| if (v.date.infinity() == 0 and value.upper_inc) return error.InvalidSqlBinaryRepresentation;
        }
        if (value.lower != null and value.upper != null) {
            const compared = try endpointOrder(ctx, value.lower.?, value.upper.?);
            if (compared == .gt or (compared == .eq and (!value.lower_inc or !value.upper_inc))) return error.InvalidSqlBinaryRepresentation;
        }
        return value;
    }
    pub fn hash(self: View, ctx: *ranges.Context, hasher: anytype) !void {
        try ctx.charge(1);
        hasher.update(&.{ @backingInt(self.kind), self.bytes[0] });
        for ([_]?Endpoint{ self.lower, self.upper }) |point| if (point) |v| switch (v) {
            .number => |n| {
                try ctx.charge(@as(u64, n.count) + 1);
                n.updateLogicalHash(hasher);
            },
            .date => |d| {
                var out: [4]u8 = undefined;
                std.mem.writeInt(i32, &out, d.days, .big);
                hasher.update(&out);
            },
            .timestamp => |t| {
                var out: [8]u8 = undefined;
                std.mem.writeInt(i64, &out, t.micros, .big);
                hasher.update(&out);
            },
        };
    }
    pub fn logicalDigest(self: View, ctx: *ranges.Context) ![32]u8 {
        var hasher = std.crypto.hash.Blake3.init(.{});
        try self.hash(ctx, &hasher);
        var digest: [32]u8 = undefined;
        hasher.final(&digest);
        return digest;
    }
    pub fn order(self: View, ctx: *ranges.Context, other: View) !std.math.Order {
        if (self.kind != other.kind) return error.SqlUndefinedOperator;
        if (self.empty or other.empty) return if (self.empty == other.empty) .eq else if (self.empty) .lt else .gt;
        const lower = try boundOrder(ctx, self.lower, self.lower_inc, other.lower, other.lower_inc, true);
        return if (lower == .eq) boundOrder(ctx, self.upper, self.upper_inc, other.upper, other.upper_inc, false) else lower;
    }
    pub fn before(self: View, ctx: *ranges.Context, other: View) !bool {
        if (self.kind != other.kind) return error.SqlUndefinedOperator;
        if (self.empty or other.empty or self.upper == null or other.lower == null) return false;
        const compared = try endpointOrder(ctx, self.upper.?, other.lower.?);
        return compared == .lt or (compared == .eq and (!self.upper_inc or !other.lower_inc));
    }
    pub fn overlaps(self: View, ctx: *ranges.Context, other: View) !bool {
        if (self.kind != other.kind) return error.SqlUndefinedOperator;
        return !self.empty and !other.empty and !try self.before(ctx, other) and !try other.before(ctx, self);
    }
    pub fn contains(self: View, ctx: *ranges.Context, other: View) !bool {
        if (self.kind != other.kind) return error.SqlUndefinedOperator;
        if (other.empty) return true;
        if (self.empty) return false;
        return try boundOrder(ctx, self.lower, self.lower_inc, other.lower, other.lower_inc, true) != .gt and
            try boundOrder(ctx, self.upper, self.upper_inc, other.upper, other.upper_inc, false) != .lt;
    }
    pub fn containsPoint(self: View, ctx: *ranges.Context, point: Endpoint) !bool {
        switch (self.kind) {
            .numrange => if (point != .number) return error.SqlTypeMismatch,
            .daterange => if (point != .date) return error.SqlTypeMismatch,
            .tsrange, .tstzrange => if (point != .timestamp) return error.SqlTypeMismatch,
        }
        if (self.empty) return false;
        if (self.lower) |v| {
            const compared = try endpointOrder(ctx, v, point);
            if (compared == .gt or (compared == .eq and !self.lower_inc)) return false;
        }
        if (self.upper) |v| {
            const compared = try endpointOrder(ctx, v, point);
            if (compared == .lt or (compared == .eq and !self.upper_inc)) return false;
        }
        return true;
    }
};
pub fn endpointOrder(ctx: *ranges.Context, left: Endpoint, right: Endpoint) !std.math.Order {
    try ctx.charge(1);
    if (std.meta.activeTag(left) != std.meta.activeTag(right)) return error.SqlTypeMismatch;
    return switch (left) {
        .number => |v| v.order(right.number, ctx),
        .date => |v| std.math.order(v.days, right.date.days),
        .timestamp => |v| std.math.order(v.micros, right.timestamp.micros),
    };
}
fn boundOrder(ctx: *ranges.Context, a: ?Endpoint, ai: bool, b: ?Endpoint, bi: bool, lower: bool) !std.math.Order {
    if (a == null or b == null) return if (a == null and b == null) .eq else if ((a == null) == lower) .lt else .gt;
    const compared = try endpointOrder(ctx, a.?, b.?);
    if (compared != .eq or ai == bi) return compared;
    return if (ai == lower) .lt else .gt;
}

test "SQL range binary views retain exact PostgreSQL order and logical identity without allocation" {
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(struct { entries: []const struct { kind: ranges.Kind, input: []const u8, sqlstate: ?[]const u8, binary: ?[]const u8 = null } }, a, @embedFile("fixtures/sql_range_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    for (fixture.value.entries) |entry| {
        if (entry.sqlstate != null) continue;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var context: ranges.Context = .{ .alloc = arena.allocator() };
        const value = try ranges.parseLeaky(&context, entry.kind, entry.input);
        const bytes = try ranges.encodeLeaky(&context, value);
        var none = std.heap.FixedBufferAllocator.init(&.{});
        var no_alloc: ranges.Context = .{ .alloc = none.allocator() };
        const view = try View.open(&no_alloc, entry.kind, bytes);
        var logical = std.crypto.hash.Blake3.init(.{});
        try ranges.hash(&context, value, &logical);
        var expected: [32]u8 = undefined;
        logical.final(&expected);
        try std.testing.expectEqual(expected, try view.logicalDigest(&no_alloc));
        try std.testing.expectEqual(std.math.Order.eq, try view.order(&no_alloc, view));
        try std.testing.expectEqual(!value.empty, try view.overlaps(&no_alloc, view));
        try std.testing.expect(try view.contains(&no_alloc, view));
    }
}
