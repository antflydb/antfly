// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cold physical recoloring for outputs whose producer becomes a fused region.
//! Fixed input/literal/publication/transfer/workspace identities remain stable.
//! All mutation is committed only after successful metadata preparation.
const std = @import("std");
const ml = @import("ml");
const buffers = @import("../graph/buffer_plan.zig");
const none = std.math.maxInt(u32);

fn active(plan: *const buffers.BufferPlan, slot: buffers.LogicalSlot, index: usize) !bool {
    if (slot.id != index or slot.node_id >= plan.node_slots.len or plan.node_slots[slot.node_id] != slot.id) return error.InvalidBufferPlan;
    if (plan.execution_reachable) |reachable| {
        if (reachable.len != plan.node_slots.len) return error.InvalidReachabilityTable;
        if (!reachable[slot.node_id]) {
            if (slot.allocation != buffers.invalid_allocation or slot.roles.graph_output) return error.InvalidBufferPlan;
            return false;
        }
    }
    return true;
}

pub fn apply(plan: *buffers.BufferPlan, first_use: []const u32) !void {
    const last_use = try plan.allocator.alloc(u32, first_use.len);
    defer plan.allocator.free(last_use);
    @memset(last_use, none);
    return applyWithLastUse(plan, first_use, last_use);
}

pub fn applyWithLastUse(plan: *buffers.BufferPlan, first_use: []const u32, last_use: []const u32) !void {
    if (first_use.len != plan.node_slots.len or last_use.len != first_use.len or plan.node_positions.len != first_use.len) return error.InvalidRegionOrder;
    if (plan.slots.len != plan.node_slots.len or (plan.allocation_enabled.len != 0 and plan.allocation_enabled.len != plan.allocations.len)) return error.InvalidBufferPlan;
    for (plan.allocations, 0..) |allocation, id| if (allocation.id != id or allocation.first_use > allocation.last_use) return error.InvalidBufferPlan;
    var changed = false;
    for (plan.slots, 0..) |slot, index| {
        if (!try active(plan, slot, index)) continue;
        if (slot.kind == .allocation or slot.allocation != buffers.invalid_allocation) {
            if (slot.allocation >= plan.allocations.len or (plan.allocation_enabled.len != 0 and !plan.allocation_enabled[slot.allocation])) return error.InvalidBufferPlan;
        }
        if (slot.first_use > slot.last_use) return error.InvalidRegionOrder;
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
    for (slots, 0..) |*slot, index| {
        if (!try active(plan, slot.*, index)) continue;
        if (first_use[slot.node_id] != none) slot.first_use = @min(slot.first_use, first_use[slot.node_id]);
        if (last_use[slot.node_id] != none) slot.last_use = @max(slot.last_use, last_use[slot.node_id]);
        if (slot.kind != .allocation) continue;
        if (slot.allocation >= reusable_ids.len or allocations.items[slot.allocation].id != slot.allocation) return error.InvalidBufferPlan;
        // Logical allocation slots may be bound to admitted fixed banks.
        // Only actual transient tensor allocations participate in coloring.
        if (allocations.items[slot.allocation].kind == .tensor) reusable_ids[slot.allocation] = true;
    }
    // Backing becomes live when any same-target alias is first published.
    var reverse = slots.len;
    while (reverse != 0) {
        reverse -= 1;
        const slot = slots[reverse];
        if (!try active(plan, slot, reverse) or slot.kind != .view) continue;
        const source = slot.source_slot orelse return error.InvalidBufferPlan;
        if (source >= reverse or !try active(plan, slots[source], source)) return error.InvalidViewSource;
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
        if (!try active(plan, slot.*, index) or slot.kind != .allocation) continue;
        const original = plan.allocations[plan.slots[index].allocation];
        if (original.kind != .tensor) continue;
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
    for (slots, 0..) |*slot, index| {
        if (!try active(plan, slot.*, index) or slot.kind != .view) continue;
        const source = slot.source_slot orelse return error.InvalidBufferPlan;
        if (source >= slot.id or !try active(plan, slots[source], source)) return error.InvalidViewSource;
        if (slots[source].backend == slot.backend) {
            slot.allocation = slots[source].allocation;
        } else {
            const transfer = plan.transfer_index.get(buffers.transferKey(slots[source].node_id, slot.partition_index)) orelse return error.MissingViewTransfer;
            if (transfer >= plan.transfers.len) return error.MissingViewTransfer;
            slot.allocation = plan.transfers[transfer].allocation;
        }
    }
    // Aliases can expand fixed input/literal/transfer lifetimes too. Keep
    // their IDs, bytes, slot counts and reuse policy; aggregate only the bounds
    // AFTER alias propagation and final backing resolution.
    for (slots, 0..) |slot, index| {
        if (!try active(plan, slot, index) or slot.allocation == buffers.invalid_allocation) continue;
        if (slot.allocation >= allocations.items.len or allocations.items[slot.allocation].id != slot.allocation) return error.InvalidBufferPlan;
        const allocation = &allocations.items[slot.allocation];
        if (allocation.kind == .tensor) continue;
        allocation.first_use = @min(allocation.first_use, slot.first_use);
        allocation.last_use = @max(allocation.last_use, slot.last_use);
    }
    // Fixed identities may leave unused metadata banks. Pool admission counts
    // actual binding references; these reserve no backing allocation.
    for (allocations.items, 0..) |*allocation, id| if (id < reusable_ids.len and reusable_ids[id] and allocation.slot_count == 0) {
        allocation.first_use = 0;
        allocation.last_use = 0;
        allocation.reusable = false;
    };
    const enabled: []bool = if (plan.allocation_enabled.len != 0) try allocator.alloc(bool, allocations.items.len) else &.{};
    errdefer allocator.free(enabled);
    if (enabled.len != 0) {
        @memcpy(enabled[0..plan.allocation_enabled.len], plan.allocation_enabled);
        @memset(enabled[plan.allocation_enabled.len..], true);
    }
    const owned_allocations = try allocations.toOwnedSlice(allocator);
    allocator.free(plan.allocation_enabled);
    allocator.free(plan.slots);
    allocator.free(plan.allocations);
    plan.slots = slots;
    plan.allocations = owned_allocations;
    plan.allocation_enabled = enabled;
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

const BoundFixture = struct {
    fn check(a: std.mem.Allocator) !void {
        const partition = @import("../graph/partition.zig");
        const interpreter = @import("../graph/interpreter.zig");
        var graph = ml.graph.Graph.init(a);
        defer graph.deinit();
        var builder = ml.graph.Builder.init(&graph);
        const shape = ml.graph.Shape.init(.f32, &.{4});
        const input = try builder.parameter("input", shape);
        const literal = try builder.tensorConst(&.{ 1, 2, 3, 4 }, shape);
        const input_view = try builder.reshape(input, ml.graph.Shape.init(.f32, &.{ 1, 4 }));
        const literal_view = try builder.reshape(literal, ml.graph.Shape.init(.f32, &.{ 1, 4 }));
        const left = try builder.neg(input_view);
        const consumed = try builder.neg(left);
        const right = try builder.add(input_view, literal_view);
        const output = try builder.add(consumed, right);
        _ = try builder.neg(input); // projected inactive allocation is not a bank
        try graph.markOutput(output);
        const capabilities = [_]partition.Capability{.{ .backend = .native, .priority = 0, .supports = &partition.supportsAll }};
        var schedule = try partition.partition(a, &graph, &capabilities);
        defer schedule.deinit();
        var plan = try buffers.build(a, &graph, &schedule, .{});
        defer plan.deinit();
        const input_bank = try plan.addInputBinding(.native, 16);
        const constant_bank = try plan.addConstantBinding(.native, 16);
        // These model the actual physical binding refinement: logical input
        // and constant slots now name fixed admitted banks, preserving roles.
        for ([_]ml.graph.NodeId{ input, literal }, [_]buffers.AllocationId{ input_bank, constant_bank }) |node, bank| {
            const slot = &plan.slots[plan.node_slots[node]];
            slot.kind = .allocation;
            slot.allocation = bank;
            slot.storage = .host_f32;
            slot.desc.storage = .host_f32;
            slot.reusable = false;
            plan.allocations[bank].first_use = slot.first_use;
            plan.allocations[bank].last_use = slot.last_use;
        }
        const workspace = try plan.addWorkspace(.native, 32, 4);
        const auxiliary = try plan.addAuxiliaryPlane(plan.slotForNode(left).?.allocation, 32);
        const extra_transfer = try plan.addWorkspace(.native, 32, 4);
        plan.allocations[extra_transfer].kind = .transfer;
        const fixed_before = [_]buffers.PhysicalAllocation{ plan.allocations[input_bank], plan.allocations[constant_bank], plan.allocations[workspace], plan.allocations[auxiliary], plan.allocations[extra_transfer] };
        const reachable = try interpreter.computeReachable(a, &graph);
        defer a.free(reachable);
        try plan.projectReachable(&graph, reachable);
        plan.allocation_enabled = try a.alloc(bool, plan.allocations.len);
        @memset(plan.allocation_enabled, true);
        const initial_allocations = plan.allocations.len;
        const first = try a.alloc(u32, graph.nodeCount());
        defer a.free(first);
        @memset(first, none);
        first[right] = plan.positionForNode(left);
        first[literal_view] = 0;
        const last = try a.alloc(u32, graph.nodeCount());
        defer a.free(last);
        @memset(last, none);
        last[input_view] = @intCast(graph.nodeCount() + 4);
        // Allocation-failure assertions use an independent test allocator,
        // not the allocator whose failures are under examination.
        const old_slots = try std.testing.allocator.dupe(buffers.LogicalSlot, plan.slots);
        defer std.testing.allocator.free(old_slots);
        const old_allocations = try std.testing.allocator.dupe(buffers.PhysicalAllocation, plan.allocations);
        defer std.testing.allocator.free(old_allocations);
        const old_mask = try std.testing.allocator.dupe(bool, plan.allocation_enabled);
        defer std.testing.allocator.free(old_mask);
        applyWithLastUse(&plan, first, last) catch |err| {
            try std.testing.expectEqualDeep(old_slots, plan.slots);
            try std.testing.expectEqualDeep(old_allocations, plan.allocations);
            try std.testing.expectEqualSlices(bool, old_mask, plan.allocation_enabled);
            return err;
        };
        try plan.validate(&graph, &schedule);
        try std.testing.expect(plan.slotForNode(left).?.allocation != plan.slotForNode(right).?.allocation);
        try std.testing.expectEqual(plan.positionForNode(left), plan.slotForNode(right).?.first_use);
        try std.testing.expectEqual(input_bank, plan.slotForNode(input_view).?.allocation);
        try std.testing.expectEqual(constant_bank, plan.slotForNode(literal_view).?.allocation);
        try std.testing.expectEqual(last[input_view], plan.allocations[input_bank].last_use);
        try std.testing.expectEqual(@as(u32, 0), plan.allocations[constant_bank].first_use);
        for (fixed_before) |original| {
            const current = plan.allocations[original.id];
            var expected = original;
            expected.first_use = current.first_use;
            expected.last_use = current.last_use;
            try std.testing.expectEqualDeep(expected, current);
            try std.testing.expect(current.first_use <= original.first_use and current.last_use >= original.last_use);
        }
        try std.testing.expect(plan.allocations.len > initial_allocations);
        try std.testing.expectEqual(plan.allocations.len, plan.allocation_enabled.len);
        try std.testing.expectEqualSlices(bool, old_mask, plan.allocation_enabled[0..initial_allocations]);
        for (plan.allocation_enabled[initial_allocations..]) |enabled| try std.testing.expect(enabled);
        // Never hide an active poisoned allocation behind the fixed-bank or
        // projected-dead-node exceptions, even when no new bounds change.
        const slot_index = plan.node_slots[input];
        plan.slots[slot_index].allocation = buffers.invalid_allocation;
        const invalid_result = applyWithLastUse(&plan, first, last);
        plan.slots[slot_index].allocation = input_bank;
        try std.testing.expectError(error.InvalidBufferPlan, invalid_result);
        plan.slots[slot_index].roles.runtime_input = false;
        const bad_role = plan.validate(&graph, &schedule);
        plan.slots[slot_index].roles.runtime_input = true;
        try std.testing.expectError(error.InvalidBufferPlan, bad_role);
        plan.allocations[input_bank].reusable = true;
        const bad_reuse = plan.validate(&graph, &schedule);
        plan.allocations[input_bank].reusable = false;
        try std.testing.expectError(error.InvalidBufferPlan, bad_reuse);
    }
};

test "frozen region recoloring preserves bound input literal aliases fixed banks and failure atomic masks" {
    try BoundFixture.check(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, BoundFixture.check, .{});
}

const TransferFixture = struct {
    fn check(a: std.mem.Allocator) !void {
        const partition = @import("../graph/partition.zig");
        var graph = ml.graph.Graph.init(a);
        defer graph.deinit();
        var builder = ml.graph.Builder.init(&graph);
        const input = try builder.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
        const source = try builder.gelu(input);
        const view = try builder.reshape(source, ml.graph.Shape.init(.f32, &.{ 1, 4 }));
        const output = try builder.neg(view);
        try graph.markOutput(output);
        const onlyGelu = struct {
            fn supports(op: ml.graph.OpCode) bool {
                return op == .fused_gelu;
            }
        }.supports;
        const capabilities = [_]partition.Capability{
            .{ .backend = .metal, .priority = 10, .supports = &onlyGelu },
            .{ .backend = .native, .priority = 0, .supports = &partition.supportsAll },
        };
        var schedule = try partition.partition(a, &graph, &capabilities);
        defer schedule.deinit();
        var plan = try buffers.build(a, &graph, &schedule, .{});
        defer plan.deinit();
        try std.testing.expect(plan.transfers.len != 0);
        const target_bank = plan.slotForNode(view).?.allocation;
        try std.testing.expectEqual(buffers.AllocationKind.transfer, plan.allocations[target_bank].kind);
        const original = plan.allocations[target_bank];
        const old_transfers = try std.testing.allocator.dupe(buffers.TransferEdge, plan.transfers);
        defer std.testing.allocator.free(old_transfers);
        const first = try a.alloc(u32, graph.nodeCount());
        defer a.free(first);
        @memset(first, none);
        first[output] = plan.positionForNode(view);
        const last = try a.alloc(u32, graph.nodeCount());
        defer a.free(last);
        @memset(last, none);
        last[view] = @intCast(graph.nodeCount() + 3);
        try applyWithLastUse(&plan, first, last);
        try plan.validate(&graph, &schedule);
        try std.testing.expectEqualDeep(old_transfers, plan.transfers);
        try std.testing.expectEqual(target_bank, plan.slotForNode(view).?.allocation);
        try std.testing.expectEqual(last[view], plan.allocations[target_bank].last_use);
        var expected = original;
        expected.first_use = plan.allocations[target_bank].first_use;
        expected.last_use = plan.allocations[target_bank].last_use;
        try std.testing.expectEqualDeep(expected, plan.allocations[target_bank]);
    }
};

test "frozen region recoloring preserves real cross backend transfer view identity and extends its lifetime" {
    try TransferFixture.check(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, TransferFixture.check, .{});
}
