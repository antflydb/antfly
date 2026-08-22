// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const ids = @import("id.zig");

pub const Kind = enum { scheduler, workload, fault, maintenance, quiescence };

pub const Transition = struct {
    id: ids.StableId,
    name: []const u8,
    kind: Kind,
    actor_id: ?ids.StableId = null,
    resource_id: ?ids.StableId = null,
    parameter: i64 = 0,

    pub fn named(kind: Kind, name: []const u8) Transition {
        return .{ .id = ids.stable("transition", name), .name = name, .kind = kind };
    }

    pub fn eql(lhs: Transition, rhs: Transition) bool {
        return lhs.id == rhs.id and
            std.mem.eql(u8, lhs.name, rhs.name) and
            lhs.kind == rhs.kind and
            lhs.actor_id == rhs.actor_id and
            lhs.resource_id == rhs.resource_id and
            lhs.parameter == rhs.parameter;
    }

    pub fn payloadDigest(self: Transition) u64 {
        const actors = ids.derive("transition.payload.actors", self.actor_id orelse 0, self.resource_id orelse 0);
        return ids.derive("transition.payload", actors, @bitCast(self.parameter));
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
