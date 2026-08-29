// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Reversible logical service-rate and operation-cost model.
//!
//! Production seams opt in by charging named operations through a node-bound
//! `Port`. Active effects compose deterministically and advance borrowed
//! `std.Io` logical time; they never sleep the host or rewrite scheduler
//! choices. Fault activation/healing belongs at scenario transition boundaries.

const std = @import("std");
const ids = @import("id.zig");

pub const parts_per_million: u64 = 1_000_000;

pub const Node = struct {
    id: ids.StableId,
    name: []const u8,
};

pub const Operation = struct {
    id: ids.StableId,
    name: []const u8,
    base_cost_ns: u64,

    pub fn named(name: []const u8, base_cost_ns: u64) Operation {
        return .{ .id = ids.stable("service-rate.operation", name), .name = name, .base_cost_ns = base_cost_ns };
    }
};

pub const Effect = struct {
    fault_id: ids.StableId,
    node_id: ids.StableId,
    /// Null slows every registered operation on the node.
    operation_id: ?ids.StableId = null,
    /// 1,000,000 is baseline; 2,000,000 doubles logical cost.
    multiplier_ppm: u64,
};

pub const Usage = struct {
    operations: u64 = 0,
    charged_ns: u64 = 0,
};

pub const Model = struct {
    allocator: std.mem.Allocator,
    nodes: []const Node,
    operations: []const Operation,
    active: std.ArrayListUnmanaged(Effect) = .empty,
    usage: []Usage,

    pub fn init(allocator: std.mem.Allocator, nodes: []const Node, operations: []const Operation) !Model {
        try validateDefinitions(nodes, operations);
        const usage = try allocator.alloc(Usage, nodes.len);
        @memset(usage, .{});
        return .{ .allocator = allocator, .nodes = nodes, .operations = operations, .usage = usage };
    }

    pub fn deinit(self: *Model) void {
        self.active.deinit(self.allocator);
        self.allocator.free(self.usage);
        self.* = undefined;
    }

    pub fn activate(self: *Model, effect: Effect) !void {
        if (effect.fault_id == 0) return error.InvalidServiceRateFault;
        if (effect.multiplier_ppm < parts_per_million) return error.InvalidServiceRateSlowdown;
        _ = self.nodeIndex(effect.node_id) orelse return error.UnknownServiceRateNode;
        if (effect.operation_id) |operation_id| _ = self.operationIndex(operation_id) orelse return error.UnknownServiceRateOperation;
        for (self.active.items) |active| if (active.fault_id == effect.fault_id) return error.DuplicateServiceRateFault;
        try self.active.append(self.allocator, effect);
    }

    pub fn heal(self: *Model, fault_id: ids.StableId) !void {
        for (self.active.items, 0..) |effect, index| if (effect.fault_id == fault_id) {
            _ = self.active.swapRemove(index);
            return;
        };
        return error.UnknownServiceRateFault;
    }

    pub fn healAll(self: *Model) void {
        self.active.clearRetainingCapacity();
    }

    pub fn effectiveCostNs(self: *const Model, node_id: ids.StableId, operation_id: ids.StableId, units: u64) !u64 {
        _ = self.nodeIndex(node_id) orelse return error.UnknownServiceRateNode;
        const operation = self.operations[self.operationIndex(operation_id) orelse return error.UnknownServiceRateOperation];
        if (units == 0) return 0;
        var cost = try multiplyChecked(operation.base_cost_ns, units);
        for (self.active.items) |effect| {
            if (effect.node_id != node_id) continue;
            if (effect.operation_id) |target| if (target != operation_id) continue;
            cost = try scaleCeilChecked(cost, effect.multiplier_ppm);
        }
        return cost;
    }

    pub fn port(self: *Model, io: std.Io, node_id: ids.StableId) !Port {
        _ = self.nodeIndex(node_id) orelse return error.UnknownServiceRateNode;
        return .{ .model = self, .io = io, .node_id = node_id };
    }

    pub fn nodeUsage(self: *const Model, node_id: ids.StableId) !Usage {
        return self.usage[self.nodeIndex(node_id) orelse return error.UnknownServiceRateNode];
    }

    pub fn activeEffectCount(self: *const Model) usize {
        return self.active.items.len;
    }

    fn nodeIndex(self: *const Model, node_id: ids.StableId) ?usize {
        for (self.nodes, 0..) |node, index| if (node.id == node_id) return index;
        return null;
    }

    fn operationIndex(self: *const Model, operation_id: ids.StableId) ?usize {
        for (self.operations, 0..) |operation, index| if (operation.id == operation_id) return index;
        return null;
    }
};

pub const Port = struct {
    model: *Model,
    io: std.Io,
    node_id: ids.StableId,

    pub fn charge(self: Port, operation_id: ids.StableId, units: u64) !u64 {
        const cost_ns = try self.model.effectiveCostNs(self.node_id, operation_id, units);
        if (units == 0) return 0;
        const node_index = self.model.nodeIndex(self.node_id).?;
        self.model.usage[node_index].operations +|= units;
        self.model.usage[node_index].charged_ns +|= cost_ns;
        try self.io.sleep(.fromNanoseconds(cost_ns), .awake);
        return cost_ns;
    }
};

fn validateDefinitions(nodes: []const Node, operations: []const Operation) !void {
    if (nodes.len == 0) return error.ServiceRateHasNoNodes;
    if (operations.len == 0) return error.ServiceRateHasNoOperations;
    for (nodes, 0..) |node, index| {
        if (node.id == 0 or node.name.len == 0) return error.InvalidServiceRateNode;
        for (nodes[0..index]) |prior| if (prior.id == node.id) return error.DuplicateServiceRateNode;
    }
    for (operations, 0..) |operation, index| {
        if (operation.id == 0 or operation.name.len == 0 or operation.base_cost_ns == 0) return error.InvalidServiceRateOperation;
        for (operations[0..index]) |prior| if (prior.id == operation.id) return error.DuplicateServiceRateOperation;
    }
}

fn multiplyChecked(left: u64, right: u64) !u64 {
    return std.math.mul(u64, left, right) catch error.ServiceRateCostOverflow;
}

fn scaleCeilChecked(cost: u64, multiplier_ppm: u64) !u64 {
    const wide: u128 = @as(u128, cost) * @as(u128, multiplier_ppm);
    const scaled = (wide + parts_per_million - 1) / parts_per_million;
    if (scaled > std.math.maxInt(u64)) return error.ServiceRateCostOverflow;
    return @intCast(scaled);
}

test "node and operation slowdowns compose and heal reversibly" {
    const node = Node{ .id = 11, .name = "node-a" };
    const read = Operation.named("read", 10);
    const write = Operation.named("write", 25);
    var model = try Model.init(std.testing.allocator, &.{node}, &.{ read, write });
    defer model.deinit();
    try std.testing.expectEqual(@as(u64, 20), try model.effectiveCostNs(node.id, read.id, 2));
    try model.activate(.{ .fault_id = 101, .node_id = node.id, .multiplier_ppm = 2 * parts_per_million });
    try model.activate(.{ .fault_id = 102, .node_id = node.id, .operation_id = read.id, .multiplier_ppm = 3 * parts_per_million });
    try std.testing.expectEqual(@as(usize, 2), model.activeEffectCount());
    try std.testing.expectEqual(@as(u64, 120), try model.effectiveCostNs(node.id, read.id, 2));
    try std.testing.expectEqual(@as(u64, 50), try model.effectiveCostNs(node.id, write.id, 1));
    try model.heal(101);
    try std.testing.expectEqual(@as(u64, 60), try model.effectiveCostNs(node.id, read.id, 2));
    try model.heal(102);
    try std.testing.expectEqual(@as(usize, 0), model.activeEffectCount());
    try std.testing.expectEqual(@as(u64, 20), try model.effectiveCostNs(node.id, read.id, 2));
    const port = try model.port(std.testing.io, node.id);
    try std.testing.expectEqual(@as(u64, 20), try port.charge(read.id, 2));
    const usage = try model.nodeUsage(node.id);
    try std.testing.expectEqual(@as(u64, 2), usage.operations);
    try std.testing.expectEqual(@as(u64, 20), usage.charged_ns);
}
