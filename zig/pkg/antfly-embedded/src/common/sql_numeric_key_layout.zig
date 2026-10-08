// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared self-delimiting NUMERIC ordering, independent of SQL and native
//! storage. Canonical row views and logical coefficients use the same writer.
//! All views borrow immutable caller-owned bytes; no limb allocation occurs.
const std = @import("std");
const layout = @import("sql_numeric_layout.zig");
pub const Limits = layout.Limits;
pub const Rank = enum(u8) { negative_infinity, negative, zero, positive, positive_infinity, nan };

/// Only construct from a canonical row view or independently validated logical
/// coefficients. Untrusted row bytes must pass layout.View.openWithBudget first.
pub const Source = struct {
    kind: layout.Kind,
    negative: bool,
    weight: i16,
    groups: union(enum) { wire: []const u8, limbs: []const u16 },

    pub fn fromCanonicalRow(view: layout.View) Source {
        return .{ .kind = view.kind, .negative = view.negative, .weight = view.weight, .groups = .{ .wire = view.bytes[8..] } };
    }

    pub fn count(self: Source) usize {
        return switch (self.groups) {
            .wire => |bytes| bytes.len / 2,
            .limbs => |digits| digits.len,
        };
    }

    pub fn group(self: Source, index: usize) u16 {
        return switch (self.groups) {
            .wire => |bytes| std.mem.readInt(u16, bytes[index * 2 ..][0..2], .big),
            .limbs => |digits| digits[index],
        };
    }

    pub fn rank(self: Source) Rank {
        return switch (self.kind) {
            .negative_infinity => .negative_infinity,
            .positive_infinity => .positive_infinity,
            .nan => .nan,
            .finite => if (self.count() == 0) .zero else if (self.negative) .negative else .positive,
        };
    }

    pub fn encodedSize(self: Source) usize {
        return if (self.kind == .finite and self.count() != 0) 5 + self.count() * 2 else 1;
    }

    /// Complementing the complete component reverses numeric order without
    /// changing framing, including the dedicated zero/special ranks.
    pub fn write(self: Source, writer: *std.Io.Writer, descending: bool, budget: anytype) !void {
        try budget.charge(1);
        try writer.writeByte(@backingInt(self.rank()) ^ @as(u8, if (descending) 0xff else 0));
        if (self.kind != .finite or self.count() == 0) return;
        const mask: u16 = @as(u16, if (self.negative) 0xffff else 0) ^ @as(u16, if (descending) 0xffff else 0);
        try writer.writeInt(u16, @as(u16, @bitCast(self.weight)) ^ 0x8000 ^ mask, .big);
        // Reserve zero as terminator; interior zero groups encode as one.
        // Canonical nonzero trailing groups make every component prefix-free.
        for (0..self.count()) |index| {
            try budget.charge(1);
            try writer.writeInt(u16, (self.group(index) + 1) ^ mask, .big);
        }
        try writer.writeInt(u16, mask, .big);
    }
};

pub const Groups = struct {
    bytes: []const u8,
    mask: u16,
    pub fn len(self: Groups) usize {
        return self.bytes.len / 2;
    }
    pub fn at(self: Groups, index: usize) u16 {
        return (std.mem.readInt(u16, self.bytes[index * 2 ..][0..2], .big) ^ self.mask) - 1;
    }
};

pub const Shape = struct {
    kind: Rank,
    consumed: usize,
    weight: i16 = 0,
    scale: u16 = 0,
    groups: Groups = .{ .bytes = &.{}, .mask = 0 },
};

/// Validate exactly one ordered component, never copying its coefficients or
/// interpreting a following tuple component/document suffix. Native tuple
/// readers and SQL ownership adapters share this same canonical admission.
pub fn parsePrefix(bytes: []const u8, descending: bool, limits: Limits, budget: anytype) !Shape {
    try budget.charge(1);
    if (limits.bytes < 1) return budget.limit();
    if (bytes.len == 0) return error.InvalidSqlNumericKey;
    const kind = std.enums.fromInt(Rank, bytes[0] ^ @as(u8, if (descending) 0xff else 0)) orelse return error.InvalidSqlNumericKey;
    switch (kind) {
        .negative_infinity, .zero, .positive_infinity, .nan => return .{ .kind = kind, .consumed = 1 },
        else => {},
    }
    if (limits.bytes < 3) return budget.limit();
    if (bytes.len < 3) return error.InvalidSqlNumericKey;
    const mask: u16 = @as(u16, if (kind == .negative) 0xffff else 0) ^ @as(u16, if (descending) 0xffff else 0);
    const weight: i16 = @bitCast(std.mem.readInt(u16, bytes[1..3], .big) ^ mask ^ 0x8000);
    var end: usize = 3;
    while (true) : (end += 2) {
        try budget.charge(1);
        if (end > limits.bytes or 2 > limits.bytes - end) return budget.limit();
        if (end > bytes.len or 2 > bytes.len - end) return error.InvalidSqlNumericKey;
        const word = std.mem.readInt(u16, bytes[end..][0..2], .big) ^ mask;
        if (word == 0) break;
        if (word > 10000 or (end - 3) / 2 >= std.math.maxInt(u16)) return error.InvalidSqlNumericKey;
        // Stop at the first excess coefficient, not after traversing an
        // arbitrarily larger component on a constrained native tuple read.
        if ((end - 3) / 2 >= limits.groups) return budget.limit();
    }
    const groups: Groups = .{ .bytes = bytes[3..end], .mask = mask };
    if (groups.len() == 0 or groups.at(0) == 0 or groups.at(groups.len() - 1) == 0) return error.InvalidSqlNumericKey;
    const low = @as(i32, weight) - @as(i32, @intCast(groups.len())) + 1;
    var scale: i32 = @max(0, -low * 4);
    if (scale != 0) {
        var last = groups.at(groups.len() - 1);
        while (last % 10 == 0) : (last /= 10) scale -= 1;
    }
    if (scale > 16383) return error.InvalidSqlNumericKey;
    return .{ .kind = kind, .weight = weight, .scale = @intCast(scale), .groups = groups, .consumed = end + 2 };
}
