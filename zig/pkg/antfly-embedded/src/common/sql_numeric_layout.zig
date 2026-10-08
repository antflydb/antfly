// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Canonical immutable NUMERIC payloads, independent of SQL execution and
//! native storage. PostgreSQL binary input may be normalized by its receiver;
//! stored bytes may not. Checked borrowed views validate without allocating
//! coefficients and hash logical identity without display-scale metadata.
const std = @import("std");
pub const Limits = struct { bytes: usize = 1024 * 1024, groups: usize = 65535 };
pub const Kind = enum { finite, nan, positive_infinity, negative_infinity };
const NoBudget = struct {
    fn charge(_: *@This(), _: u64) !void {}
    fn limit(_: *@This()) anyerror {
        return error.SqlProgramLimitExceeded;
    }
};

pub const View = struct {
    bytes: []const u8,
    kind: Kind,
    negative: bool,
    weight: i16,
    scale: u16,
    count: u16,

    pub fn group(self: View, index: usize) u16 {
        std.debug.assert(index < self.count);
        return std.mem.readInt(u16, self.bytes[8 + index * 2 ..][0..2], .big);
    }

    /// Framing only, for pinned canonical bytes admitted at publication and
    /// protected by row/page authentication. This does not authorize arbitrary
    /// parameter, restore or producer bytes; those must pass open first.
    pub fn openAuthenticated(bytes: []const u8, limits: Limits) !View {
        if (bytes.len > limits.bytes) return error.SqlProgramLimitExceeded;
        if (bytes.len < 8) return error.InvalidSqlBinaryRepresentation;
        const count = std.mem.readInt(u16, bytes[0..2], .big);
        if (count > limits.groups) return error.SqlProgramLimitExceeded;
        if (bytes.len != 8 + @as(usize, count) * 2) return error.InvalidSqlBinaryRepresentation;
        const sign = std.mem.readInt(u16, bytes[4..6], .big);
        const kind: Kind = switch (sign) {
            0, 0x4000 => .finite,
            0xc000 => .nan,
            0xd000 => .positive_infinity,
            0xf000 => .negative_infinity,
            else => return error.InvalidSqlBinaryRepresentation,
        };
        return .{ .bytes = bytes, .count = count, .kind = kind, .negative = sign == 0x4000, .weight = std.mem.readInt(i16, bytes[2..4], .big), .scale = std.mem.readInt(u16, bytes[6..8], .big) };
    }

    pub fn open(bytes: []const u8, limits: Limits) !View {
        var budget: NoBudget = .{};
        return openWithBudget(bytes, limits, &budget);
    }

    /// Budget owners supply charge/limit, retaining their cancellation and
    /// sticky quota semantics. Validate every group before publishing a view.
    pub fn openWithBudget(bytes: []const u8, limits: Limits, budget: anytype) !View {
        try budget.charge(1);
        const view = openAuthenticated(bytes, limits) catch |err| return if (err == error.SqlProgramLimitExceeded) budget.limit() else err;
        if (view.kind != .finite) {
            const scale: u16 = if (view.kind == .nan) 0 else 32;
            if (view.count != 0 or view.weight != 0 or view.scale != scale)
                return error.InvalidSqlBinaryRepresentation;
            return view;
        }
        if (view.scale > 16383) return error.InvalidSqlBinaryRepresentation;
        if (view.count == 0) {
            if (view.negative or view.weight != 0) return error.InvalidSqlBinaryRepresentation;
            return view;
        }
        for (0..view.count) |i| {
            try budget.charge(1);
            const digit = view.group(i);
            if (digit >= 10000 or (digit == 0 and (i == 0 or i + 1 == view.count)))
                return error.InvalidSqlBinaryRepresentation;
        }
        const low = @as(i32, view.weight) - view.count + 1;
        const cut = @divFloor(-@as(i32, view.scale), 4);
        const factor = ([_]u16{ 1, 10, 100, 1000 })[@intCast(@mod(-@as(i32, view.scale), 4))];
        if (low < cut or (low == cut and view.group(view.count - 1) % factor != 0))
            return error.InvalidSqlBinaryRepresentation;
        return view;
    }

    /// Same identity bytes as the immutable logical NUMERIC kernel. Scale is
    /// presentation, not equality; coefficient bytes are already big endian.
    pub fn updateLogicalHash(self: View, hasher: anytype) void {
        const rank: u8 = switch (self.kind) {
            .negative_infinity => 0,
            .finite => 1,
            .positive_infinity => 2,
            .nan => 3,
        };
        hasher.update(&.{ rank, @intFromBool(self.negative) });
        var number: [4]u8 = undefined;
        std.mem.writeInt(i32, &number, self.weight, .big);
        hasher.update(&number);
        std.mem.writeInt(u32, &number, self.count, .big);
        hasher.update(&number);
        hasher.update(self.bytes[8..]);
    }
};
