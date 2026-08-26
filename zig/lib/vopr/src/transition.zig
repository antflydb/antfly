// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const ids = @import("id.zig");

pub const Kind = enum { scheduler, workload, fault, maintenance, quiescence };
pub const FaultPhase = enum { start, end, pulse };

pub const Transition = struct {
    id: ids.StableId,
    name: []const u8,
    kind: Kind,
    actor_id: ?ids.StableId = null,
    resource_id: ?ids.StableId = null,
    parameter: i64 = 0,
    /// Optional semantic content digest for transitions whose identity and
    /// scalar parameter do not fully describe the operation (for example,
    /// equal-length network packets with different bytes).
    semantic_digest: u64 = 0,
    /// Required for explicit lifecycle transitions. Legacy fault transitions
    /// default to a pulse when this is null.
    fault_phase: ?FaultPhase = null,
    /// Generation-only relative weight. It is deliberately not part of the
    /// stable transition identity or replay payload: exact replay consumes the
    /// selected ID from the artifact, while seeded exploration uses this hint
    /// to bias discovery without changing semantics.
    weight: u32 = 1,

    pub fn named(kind: Kind, name: []const u8) Transition {
        return .{ .id = ids.stable("transition", name), .name = name, .kind = kind };
    }

    pub fn eql(lhs: Transition, rhs: Transition) bool {
        return lhs.id == rhs.id and
            std.mem.eql(u8, lhs.name, rhs.name) and
            lhs.kind == rhs.kind and
            lhs.actor_id == rhs.actor_id and
            lhs.resource_id == rhs.resource_id and
            lhs.parameter == rhs.parameter and
            lhs.semantic_digest == rhs.semantic_digest and
            lhs.fault_phase == rhs.fault_phase and
            lhs.weight == rhs.weight;
    }

    pub fn payloadDigest(self: Transition) u64 {
        const actors = ids.derive("transition.payload.actors", self.actor_id orelse 0, self.resource_id orelse 0);
        const legacy = ids.derive("transition.payload", actors, @bitCast(self.parameter));
        const phased = if (self.fault_phase) |phase|
            ids.derive("transition.payload.fault-phase", legacy, @as(u64, @intFromEnum(phase)) + 1)
        else
            legacy;
        return if (self.semantic_digest == 0)
            phased
        else
            ids.derive("transition.payload.semantic", phased, self.semantic_digest);
    }
};

pub const List = struct {
    items: std.ArrayListUnmanaged(Transition) = .empty,

    pub fn deinit(self: *List, allocator: std.mem.Allocator) void {
        self.items.deinit(allocator);
        self.* = .{};
    }

    pub fn append(self: *List, allocator: std.mem.Allocator, value: Transition) !void {
        try self.items.append(allocator, value);
    }

    pub fn canonicalize(self: *List) !void {
        std.mem.sort(Transition, self.items.items, {}, lessThan);
        for (self.items.items, 0..) |item, index| {
            if (item.name.len == 0) return error.EmptyTransitionName;
            if (item.id == 0) return error.InvalidTransitionId;
            if (item.weight == 0) return error.InvalidTransitionWeight;
            if (index > 0 and self.items.items[index - 1].id == item.id) return error.DuplicateTransitionId;
        }
    }

    fn lessThan(_: void, lhs: Transition, rhs: Transition) bool {
        if (lhs.id != rhs.id) return lhs.id < rhs.id;
        return std.mem.lessThan(u8, lhs.name, rhs.name);
    }
};

test "transitions have canonical ordering" {
    var list = List{};
    defer list.deinit(std.testing.allocator);
    try list.append(std.testing.allocator, .{ .id = 9, .name = "nine", .kind = .workload });
    try list.append(std.testing.allocator, .{ .id = 2, .name = "two", .kind = .fault });
    try list.canonicalize();
    try std.testing.expectEqual(@as(u64, 2), list.items.items[0].id);
}

test "duplicate transition IDs are rejected" {
    var list = List{};
    defer list.deinit(std.testing.allocator);
    try list.append(std.testing.allocator, .{ .id = 2, .name = "a", .kind = .workload });
    try list.append(std.testing.allocator, .{ .id = 2, .name = "b", .kind = .fault });
    try std.testing.expectError(error.DuplicateTransitionId, list.canonicalize());
}

test "zero transition weight is rejected" {
    var list = List{};
    defer list.deinit(std.testing.allocator);
    try list.append(std.testing.allocator, .{ .id = 2, .name = "never", .kind = .workload, .weight = 0 });
    try std.testing.expectError(error.InvalidTransitionWeight, list.canonicalize());
}

test "semantic payload distinguishes equal-length transition contents" {
    const left = Transition{
        .id = 7,
        .name = "packet",
        .kind = .scheduler,
        .actor_id = 8,
        .resource_id = 9,
        .parameter = 3,
        .semantic_digest = ids.digest("abc"),
    };
    var right = left;
    right.semantic_digest = ids.digest("xyz");
    try std.testing.expect(left.payloadDigest() != right.payloadDigest());
}
