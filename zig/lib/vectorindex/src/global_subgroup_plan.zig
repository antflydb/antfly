//! Query-local work plan. Every borrowed span belongs to the one immutable
//! generation identified by `generation`; the enclosing query owns its lease.
//! Capacity must be admitted before allocation. No representative copies.
const std = @import("std");
const proto = @import("antfly_vector").proto;
const grouping = @import("posting_subgroups.zig");
pub const weighted = @import("weighted_subgroup_selection.zig");
pub const Leaf = struct {
    ids: []const u64,
    set: proto.RaBitQuantizedVectorSet,
    plan: grouping.View,
    first_group: usize,
};
pub const Plan = struct {
    leaves: []Leaf = &.{},
    entries: []weighted.Entry = &.{},
    selected: []bool = &.{},
    leaf_count: usize = 0,
    group_count: usize = 0,
    generation: ?*const anyopaque = null,
    active: bool = false,

    pub fn bytes(self: *const Plan) u64 {
        return self.leaves.len * @sizeOf(Leaf) + self.entries.len * @sizeOf(weighted.Entry) + self.selected.len;
    }
    pub fn projectedBytes(self: *const Plan, count: usize) !u64 {
        const groups = try std.math.mul(usize, count, grouping.max_groups);
        var size = try std.math.mul(u64, @max(count, self.leaves.len), @sizeOf(Leaf));
        size = try std.math.add(u64, size, try std.math.mul(u64, @max(groups, self.entries.len), @sizeOf(weighted.Entry)));
        return std.math.add(u64, size, @max(groups, self.selected.len));
    }
    pub fn ensureCapacity(self: *Plan, alloc: std.mem.Allocator, count: usize) !void {
        const groups = try std.math.mul(usize, count, grouping.max_groups);
        if (count > self.leaves.len) self.leaves = try alloc.realloc(self.leaves, count);
        if (groups > self.entries.len) self.entries = try alloc.realloc(self.entries, groups);
        if (groups > self.selected.len) self.selected = try alloc.realloc(self.selected, groups);
    }
    pub fn reset(self: *Plan) void {
        self.leaf_count = 0;
        self.group_count = 0;
        self.generation = null;
        self.active = false;
    }
    pub fn deinit(self: *Plan, alloc: std.mem.Allocator) void {
        alloc.free(self.leaves);
        alloc.free(self.entries);
        alloc.free(self.selected);
        self.* = .{};
    }
    pub fn append(self: *Plan, ids: []const u64, set: proto.RaBitQuantizedVectorSet, groups: grouping.View) bool {
        if (!self.active or self.leaf_count == self.leaves.len or groups.ends.len > self.entries.len - self.group_count) return false;
        self.leaves[self.leaf_count] = .{ .ids = ids, .set = set, .plan = groups, .first_group = self.group_count };
        self.leaf_count += 1;
        self.group_count += groups.ends.len;
        return true;
    }
};

/// Representatives are ANN hints, not certificates or public exact scores.
pub fn dot(query: []const f32, center: []const f32) f32 {
    var sum: @Vector(16, f32) = @splat(0);
    var offset: usize = 0;
    while (offset + 16 <= query.len) : (offset += 16) {
        const a: @Vector(16, f32) = query[offset..][0..16].*;
        const b: @Vector(16, f32) = center[offset..][0..16].*;
        sum += a * b;
    }
    var result = @reduce(.Add, sum);
    while (offset < query.len) : (offset += 1) result += query[offset] * center[offset];
    return result;
}

fn allocationExercise(alloc: std.mem.Allocator) !void {
    var plan = Plan{};
    defer plan.deinit(alloc);
    const projected = try plan.projectedBytes(19);
    try plan.ensureCapacity(alloc, 19);
    try std.testing.expectEqual(projected, plan.bytes());
    try std.testing.expectEqual(projected, try plan.projectedBytes(1));
    var identity: u8 = 0;
    plan.generation = &identity;
    plan.active = true;
    plan.reset();
    try std.testing.expect(plan.generation == null and !plan.active);
    try std.testing.expectEqual(projected, plan.bytes());
}

test "global subgroup plan capacity accounting reset and allocation failures" {
    try allocationExercise(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationExercise, .{});
    const plan = Plan{};
    try std.testing.expectError(error.Overflow, plan.projectedBytes(std.math.maxInt(usize)));
}
