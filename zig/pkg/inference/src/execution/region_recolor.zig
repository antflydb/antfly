// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cold physical recoloring for outputs whose producer becomes a fused region.
//! Fixed input/literal/publication/transfer/workspace identities remain stable.
//! All mutation is committed only after successful metadata preparation.
const std = @import("std");
const ml = @import("ml");
const buffers = @import("../graph/buffer_plan.zig");
const none = std.math.maxInt(u32);

pub fn apply(plan: *buffers.BufferPlan, first_use: []const u32) !void {
    const last_use = try plan.allocator.alloc(u32, first_use.len);
    defer plan.allocator.free(last_use);
    @memset(last_use, none);
    return applyWithLastUse(plan, first_use, last_use);
}

pub fn applyWithLastUse(plan: *buffers.BufferPlan, first_use: []const u32, last_use: []const u32) !void {
    if (first_use.len != plan.node_slots.len or last_use.len != first_use.len) return error.InvalidRegionOrder;
    var changed = false;
    for (plan.slots) |slot| {
        if (slot.node_id >= first_use.len) return error.InvalidRegionOrder;
        const first = first_use[slot.node_id];
        const last = if (last_use[slot.node_id] == none) slot.last_use else @max(slot.last_use, last_use[slot.node_id]);
        if (first != none and first > last) return error.InvalidRegionOrder;
        changed = changed or last != slot.last_use;
        changed = changed or (first != none and first < slot.first_use);
    }
    if (!changed) return;
    const allocator = plan.allocator;
    const slots = try allocator.dupe(buffers.LogicalSlot, plan.slots);
    errdefer allocator.free(slots);
    var allocations: std.ArrayList(buffers.PhysicalAllocation) = .empty;
    defer allocations.deinit(allocator);
    try allocations.appendSlice(allocator, plan.allocations);
    const reusable_ids = try allocator.alloc(bool, plan.allocations.len);
    defer allocator.free(reusable_ids);
    @memset(reusable_ids, false);
    for (slots) |*slot| {
        if (first_use[slot.node_id] != none) slot.first_use = @min(slot.first_use, first_use[slot.node_id]);
        if (last_use[slot.node_id] != none) slot.last_use = @max(slot.last_use, last_use[slot.node_id]);
        if (slot.kind != .allocation) continue;
        if (slot.allocation >= reusable_ids.len or allocations.items[slot.allocation].kind != .tensor) return error.InvalidBufferPlan;
        reusable_ids[slot.allocation] = true;
    }
    // Backing becomes live when any same-target alias is first published.
    var reverse = slots.len;
    while (reverse != 0) {
        reverse -= 1;
        const slot = slots[reverse];
        if (slot.kind != .view) continue;
        const source = slot.source_slot orelse return error.InvalidBufferPlan;
        if (source >= reverse) return error.InvalidViewSource;
        if (slots[source].backend == slot.backend) {
            slots[source].first_use = @min(slots[source].first_use, slot.first_use);
            slots[source].last_use = @max(slots[source].last_use, slot.last_use);
        }
    }
    for (reusable_ids, 0..) |eligible, id| if (eligible) {
        allocations.items[id].slot_count = 0;
        allocations.items[id].first_use = none;
        allocations.items[id].last_use = 0;
    };
    const order = try allocator.alloc(usize, slots.len);
    defer allocator.free(order);
    for (order, 0..) |*index, value| index.* = value;
    const Order = struct {
        slots: []const buffers.LogicalSlot,
        positions: []const u32,
        fn less(self: @This(), a: usize, b: usize) bool {
            const left = self.slots[a];
            const right = self.slots[b];
            if (left.first_use != right.first_use) return left.first_use < right.first_use;
            return self.positions[left.node_id] < self.positions[right.node_id];
        }
    };
    std.mem.sort(usize, order, Order{ .slots = slots, .positions = plan.node_positions }, Order.less);
    for (order) |index| {
        const slot = &slots[index];
        if (slot.kind != .allocation) continue;
        const original = plan.allocations[plan.slots[index].allocation];
        const elements = slot.desc.shape.maxElements() orelse slot.desc.shape.numElements();
        const bytes = if (elements) |count| if (count > 0) try std.math.mul(u64, @intCast(count), @intCast(slot.desc.shape.dtype.byteSize())) else original.byte_size else original.byte_size;
        var selected: ?buffers.AllocationId = null;
        for (allocations.items, 0..) |allocation, id| {
            const eligible = if (id < reusable_ids.len) reusable_ids[id] else allocation.kind == .tensor;
            if (!eligible or allocation.backend != slot.backend or allocation.storage != slot.storage or allocation.byte_size < bytes) continue;
            if (allocation.slot_count != 0) {
                if (!slot.reusable or !allocation.reusable) continue;
                if (!(allocation.last_use < slot.first_use or slot.last_use < allocation.first_use)) continue;
            }
            selected = @intCast(id);
            break;
        }
        if (selected == null) {
            if (allocations.items.len >= buffers.invalid_allocation) return error.TooManyPhysicalAllocations;
            selected = @intCast(allocations.items.len);
            try allocations.append(allocator, .{ .id = selected.?, .kind = .tensor, .backend = slot.backend, .storage = slot.storage, .byte_size = @max(bytes, original.byte_size), .first_use = slot.first_use, .last_use = slot.last_use, .reusable = slot.reusable });
        }
        const allocation = &allocations.items[selected.?];
        if (allocation.slot_count == 0) {
            allocation.first_use = slot.first_use;
            allocation.last_use = slot.last_use;
            allocation.reusable = slot.reusable;
        } else {
            allocation.first_use = @min(allocation.first_use, slot.first_use);
            allocation.last_use = @max(allocation.last_use, slot.last_use);
        }
        allocation.slot_count += 1;
        slot.allocation = selected.?;
    }
    // Resolve metadata aliases after every owned primary has its final slot.
    for (slots) |*slot| {
        if (slot.kind != .view) continue;
        const source = slot.source_slot orelse return error.InvalidBufferPlan;
        if (source >= slot.id) return error.InvalidViewSource;
        if (slots[source].backend == slot.backend) {
            slot.allocation = slots[source].allocation;
        } else {
            const transfer = plan.transfer_index.get(buffers.transferKey(slots[source].node_id, slot.partition_index)) orelse return error.MissingViewTransfer;
            slot.allocation = plan.transfers[transfer].allocation;
        }
    }
    // Fixed identities may leave unused metadata banks. Pool admission counts
    // actual binding references; these reserve no backing allocation.
    for (allocations.items, 0..) |*allocation, id| if (id < reusable_ids.len and reusable_ids[id] and allocation.slot_count == 0) {
        allocation.first_use = 0;
        allocation.last_use = 0;
        allocation.reusable = false;
    };
    const owned_allocations = try allocations.toOwnedSlice(allocator);
    allocator.free(plan.slots);
    allocator.free(plan.allocations);
    plan.slots = slots;
    plan.allocations = owned_allocations;
}

const Fixture = struct {
    fn check(a: std.mem.Allocator) !void {
        const partition = @import("../graph/partition.zig");
        var graph = ml.graph.Graph.init(a);
        defer graph.deinit();
        var builder = ml.graph.Builder.init(&graph);
        const shape = ml.graph.Shape.init(.f32, &.{4});
        const input = try builder.parameter("x", shape);
        const left = try builder.neg(input);
        const consumed = try builder.neg(left);
        const right = try builder.neg(input);
        const output = try builder.add(consumed, right);
        try graph.markOutput(output);
        const capabilities = [_]partition.Capability{.{ .backend = .native, .priority = 0, .supports = &partition.supportsAll }};
        var schedule = try partition.partition(a, &graph, &capabilities);
        defer schedule.deinit();
        var plan = try buffers.build(a, &graph, &schedule, .{});
        defer plan.deinit();
        const fixed = try plan.addConstantBinding(.native, 16);
        const fixed_before = plan.allocations[fixed];
        const overrides = try a.alloc(u32, graph.nodeCount());
        defer a.free(overrides);
        @memset(overrides, none);
        overrides[right] = plan.positionForNode(left);
        try apply(&plan, overrides);
        try plan.validate(&graph, &schedule);
        try std.testing.expectEqualDeep(fixed_before, plan.allocations[fixed]);
        try std.testing.expect(plan.slotForNode(left).?.allocation != plan.slotForNode(right).?.allocation);
        try std.testing.expectEqual(plan.positionForNode(left), plan.slotForNode(right).?.first_use);
    }
};

test "frozen region recoloring separates simultaneously published outputs and preserves fixed resource identities" {
    try Fixture.check(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
}
