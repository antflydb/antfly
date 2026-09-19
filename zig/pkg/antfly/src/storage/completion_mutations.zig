// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Bounded, ordered physical mutations for completion-plan inspection.
//! This owns copied bytes, not backend memory, WAL, descriptors or drain
//! capacity. It has no apply API: a captured shared counter or replay sequence
//! must never be replayed over newer authoritative state.

const std = @import("std");

pub const Limits = struct {
    max_operations: usize,
    max_bytes: usize,
};

pub const Mutation = struct {
    kind: enum { put, delete },
    key: []const u8,
    value: []const u8,
};

pub const Plan = struct {
    alloc: std.mem.Allocator,
    storage: []u8,
    slots: []Mutation,
    used_bytes: usize = 0,
    count: usize = 0,

    pub fn init(alloc: std.mem.Allocator, limits: Limits) !Plan {
        if (limits.max_operations == 0 or limits.max_bytes == 0) return error.InvalidArgument;
        const slots = try alloc.alloc(Mutation, limits.max_operations);
        errdefer alloc.free(slots);
        return .{ .alloc = alloc, .slots = slots, .storage = try alloc.alloc(u8, limits.max_bytes) };
    }

    pub fn deinit(self: *Plan) void {
        self.alloc.free(self.slots);
        self.alloc.free(self.storage);
        self.* = undefined;
    }

    pub fn operations(self: *const Plan) []const Mutation {
        return self.slots[0..self.count];
    }

    pub fn put(self: *Plan, key: []const u8, value: []const u8) !void {
        return self.append(.put, key, value);
    }

    pub fn delete(self: *Plan, key: []const u8) !void {
        return self.append(.delete, key, "");
    }

    fn append(self: *Plan, kind: @FieldType(Mutation, "kind"), key: []const u8, value: []const u8) !void {
        const size = std.math.add(usize, key.len, value.len) catch return error.CompletionPlanCapacityExceeded;
        if (self.count == self.slots.len or size > self.storage.len - self.used_bytes)
            return error.CompletionPlanCapacityExceeded;
        const copied = self.storage[self.used_bytes..][0..size];
        @memcpy(copied[0..key.len], key);
        @memcpy(copied[key.len..], value);
        self.slots[self.count] = .{ .kind = kind, .key = copied[0..key.len], .value = copied[key.len..] };
        self.count += 1;
        self.used_bytes += size;
    }
};

test "workload admission completion mutation capture owns binary bytes and preserves duplicate order" {
    var plan = try Plan.init(std.testing.allocator, .{ .max_operations = 3, .max_bytes = 10 });
    defer plan.deinit();
    var key = [_]u8{ 0, 255 };
    var value = [_]u8{ 10, 0 };
    try plan.put(&key, &value);
    try plan.delete(&key);
    try plan.put(&key, &value);
    key[0] = 42;
    value[0] = 99;
    try std.testing.expectEqual(@as(usize, 10), plan.used_bytes);
    try std.testing.expectEqualStrings("\x00\xff", plan.operations()[0].key);
    try std.testing.expectEqualStrings("\x0a\x00", plan.operations()[2].value);
    try std.testing.expectEqual(.delete, plan.operations()[1].kind);
    try std.testing.expectError(error.CompletionPlanCapacityExceeded, plan.put("", ""));
    try std.testing.expectEqual(@as(usize, 3), plan.count);
    try std.testing.expectEqual(@as(usize, 10), plan.used_bytes);
}

test "workload admission completion mutation capacity failure preserves its prefix" {
    var plan = try Plan.init(std.testing.allocator, .{ .max_operations = 3, .max_bytes = 3 });
    defer plan.deinit();
    try plan.put("a", "b");
    try std.testing.expectError(error.CompletionPlanCapacityExceeded, plan.put("cd", ""));
    try std.testing.expectEqual(@as(usize, 1), plan.count);
    try std.testing.expectEqual(@as(usize, 2), plan.used_bytes);
    try plan.delete("c");
}

test "workload admission completion mutation allocation failures release both buffers" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var plan = try Plan.init(alloc, .{ .max_operations = 4, .max_bytes = 256 });
            defer plan.deinit();
            try plan.put("key", "value");
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
