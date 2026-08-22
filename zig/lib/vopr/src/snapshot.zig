// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Scenario-owned logical checkpoints. These serialize modeled identifiers and
//! values, never heap pointers or OS resources. They are an exploration
//! optimization only: every retained artifact must still replay from init.

const std = @import("std");
const ids = @import("id.zig");

pub const Logical = struct {
    transition_index: u64,
    observation_digest: u64,
    state_digest: u64,
    bytes: []u8,

    pub fn deinit(self: *Logical, allocator: std.mem.Allocator) void {
        allocator.free(self.bytes);
        self.* = undefined;
    }
};

pub fn capture(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    world: *const Scenario.World,
    transition_index: u64,
    observation_digest: u64,
) !Logical {
    comptime assertContract(Scenario);
    const bytes = try Scenario.snapshotAlloc(world, allocator);
    return .{
        .transition_index = transition_index,
        .observation_digest = observation_digest,
        .state_digest = ids.digest(bytes),
        .bytes = bytes,
    };
}

pub fn restore(comptime Scenario: type, world: *Scenario.World, checkpoint: Logical, allocator: std.mem.Allocator) !void {
    comptime assertContract(Scenario);
    if (ids.digest(checkpoint.bytes) != checkpoint.state_digest) return error.CorruptLogicalSnapshot;
    try Scenario.restoreSnapshot(world, checkpoint.bytes, allocator);
}

pub const Store = struct {
    allocator: std.mem.Allocator,
    checkpoints: std.ArrayListUnmanaged(Logical) = .empty,
    keys: std.AutoHashMapUnmanaged(u64, usize) = .empty,

    pub fn init(allocator: std.mem.Allocator) Store {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Store) void {
        for (self.checkpoints.items) |*checkpoint| checkpoint.deinit(self.allocator);
        self.checkpoints.deinit(self.allocator);
        self.keys.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn add(self: *Store, checkpoint: Logical) !struct { index: usize, inserted: bool } {
        const key = ids.derive("snapshot.key", checkpoint.observation_digest, checkpoint.state_digest);
        const gop = try self.keys.getOrPut(self.allocator, key);
        if (gop.found_existing) return .{ .index = gop.value_ptr.*, .inserted = false };
        errdefer _ = self.keys.remove(key);
        const index = self.checkpoints.items.len;
        try self.checkpoints.append(self.allocator, checkpoint);
        gop.value_ptr.* = index;
        return .{ .index = index, .inserted = true };
    }
};

fn assertContract(comptime Scenario: type) void {
    if (!@hasDecl(Scenario, "World")) @compileError("snapshot scenario is missing World");
    if (!@hasDecl(Scenario, "snapshotAlloc")) @compileError("snapshot scenario is missing snapshotAlloc");
    if (!@hasDecl(Scenario, "restoreSnapshot")) @compileError("snapshot scenario is missing restoreSnapshot");
}

test "logical snapshot restores values and detects corruption" {
    const Scenario = struct {
        pub const World = struct { value: u64 };

        pub fn snapshotAlloc(world: *const World, allocator: std.mem.Allocator) ![]u8 {
            const bytes = try allocator.alloc(u8, @sizeOf(u64));
            std.mem.writeInt(u64, bytes[0..@sizeOf(u64)], world.value, .little);
            return bytes;
        }

        pub fn restoreSnapshot(world: *World, bytes: []const u8, _: std.mem.Allocator) !void {
            if (bytes.len != @sizeOf(u64)) return error.InvalidSnapshot;
            world.value = std.mem.readInt(u64, bytes[0..@sizeOf(u64)], .little);
        }
    };

    var world = Scenario.World{ .value = 42 };
    var checkpoint = try capture(Scenario, std.testing.allocator, &world, 7, 91);
    defer checkpoint.deinit(std.testing.allocator);
    world.value = 0;
    try restore(Scenario, &world, checkpoint, std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 42), world.value);
    checkpoint.bytes[0] ^= 1;
    try std.testing.expectError(error.CorruptLogicalSnapshot, restore(Scenario, &world, checkpoint, std.testing.allocator));
}
