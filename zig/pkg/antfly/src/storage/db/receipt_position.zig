// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! A control receipt's actual commit authority. Native positions are owner
//! clock values, never synthetic Raft terms, indexes or applied watermarks.
const std = @import("std");
const Namespace = @import("doc_identity_namespace.zig").Namespace;

pub const Native = struct {
    namespace: Namespace,
    sequence: u64,

    pub fn validate(self: Native) !void {
        if (self.namespace.table_id == 0 or self.namespace.shard_id == 0 or
            self.namespace.range_id == 0 or self.sequence == 0) return error.InvalidControlReceiptPosition;
    }
};

pub const Position = union(enum) {
    raft: struct { term: u64, index: u64 },
    native: Native,

    pub const encoded_len = 33;

    pub fn validate(self: Position) !void {
        switch (self) {
            .raft => |value| if (value.term == 0 or value.index == 0) return error.InvalidControlReceiptPosition,
            .native => |value| try value.validate(),
        }
    }

    pub fn requireNamespace(self: Position, expected: Namespace) !void {
        try self.validate();
        if (self == .native and !self.native.namespace.eql(expected)) return error.IdentityNamespaceMismatch;
    }

    pub fn encode(self: Position) ![encoded_len]u8 {
        try self.validate();
        var result: [encoded_len]u8 = @splat(0);
        switch (self) {
            .raft => |value| {
                result[0] = 1;
                std.mem.writeInt(u64, result[1..9], value.term, .little);
                std.mem.writeInt(u64, result[9..17], value.index, .little);
            },
            .native => |value| {
                result[0] = 2;
                std.mem.writeInt(u64, result[1..9], value.namespace.table_id, .little);
                std.mem.writeInt(u64, result[9..17], value.namespace.shard_id, .little);
                std.mem.writeInt(u64, result[17..25], value.namespace.range_id, .little);
                std.mem.writeInt(u64, result[25..33], value.sequence, .little);
            },
        }
        return result;
    }

    pub fn decode(bytes: []const u8) !Position {
        if (bytes.len != encoded_len) return error.InvalidControlReceiptPosition;
        const result: Position = switch (bytes[0]) {
            1 => blk: {
                if (!std.mem.allEqual(u8, bytes[17..33], 0)) return error.InvalidControlReceiptPosition;
                break :blk .{ .raft = .{ .term = std.mem.readInt(u64, bytes[1..9], .little), .index = std.mem.readInt(u64, bytes[9..17], .little) } };
            },
            2 => .{ .native = .{ .namespace = .{
                .table_id = std.mem.readInt(u64, bytes[1..9], .little),
                .shard_id = std.mem.readInt(u64, bytes[9..17], .little),
                .range_id = std.mem.readInt(u64, bytes[17..25], .little),
            }, .sequence = std.mem.readInt(u64, bytes[25..33], .little) } },
            else => return error.InvalidControlReceiptPosition,
        };
        try result.validate();
        return result;
    }

    pub fn term(self: Position) u64 {
        return if (self == .raft) self.raft.term else 0;
    }
    pub fn index(self: Position) u64 {
        return if (self == .raft) self.raft.index else 0;
    }
    pub fn nativePosition(self: Position) ?Native {
        return if (self == .native) self.native else null;
    }
};

/// Existing Raft receipt fields remain explicitly Raft-only. An accompanying
/// native proof requires both to be zero, so callers cannot confuse domains.
pub fn fromFields(term: u64, index: u64, native: ?Native) !Position {
    const result: Position = if (native) |value| blk: {
        if (term != 0 or index != 0) return error.InvalidControlReceiptPosition;
        break :blk .{ .native = value };
    } else .{ .raft = .{ .term = term, .index = index } };
    try result.validate();
    return result;
}

test "control receipt positions distinguish native namespace clock from Raft" {
    const native: Position = .{ .native = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .sequence = 4 } };
    const raft: Position = .{ .raft = .{ .term = 3, .index = 4 } };
    for ([_]Position{ native, raft }) |value| {
        try std.testing.expectEqualDeep(value, try Position.decode(&try value.encode()));
        try std.testing.expectEqualDeep(value, try fromFields(value.term(), value.index(), value.nativePosition()));
    }
    try std.testing.expectError(error.InvalidControlReceiptPosition, fromFields(3, 4, native.native));
    try std.testing.expectError(error.IdentityNamespaceMismatch, native.requireNamespace(.{ .table_id = 1, .shard_id = 9, .range_id = 3 }));
    var corrupt = try raft.encode();
    corrupt[17] = 1;
    try std.testing.expectError(error.InvalidControlReceiptPosition, Position.decode(&corrupt));
    var invalid = native;
    invalid.native.sequence = 0;
    try std.testing.expectError(error.InvalidControlReceiptPosition, invalid.encode());
}
