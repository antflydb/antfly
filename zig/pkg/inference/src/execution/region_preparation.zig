// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cold region selection. Programs never retain these borrowed analysis views.
const std = @import("std");
const ml = @import("ml");
const ops = @import("../ops/ops.zig");
const buffers = @import("../graph/buffer_plan.zig");
const region = @import("region_route.zig");
const kernel = @import("kernel_route.zig");
const NodeId = ml.graph.NodeId;

pub const Request = struct {
    allocator: std.mem.Allocator,
    graph: *const ml.graph.Graph,
    reachable: []const bool,
    plan: *const buffers.BufferPlan,
    backends: []const ops.ComputeBackend,
    node_facts: []const kernel.Binding,
    execution_profile: region.ExecutionProfile,
};
pub const Selected = struct {
    allocator: std.mem.Allocator,
    groups: []region.OwnedList,
    owners: []*const region.Owned,
    backend_indices: []usize,
    policy_backends: []const ops.ComputeBackend,
    policies: []TargetPolicy,
    const TargetPolicy = struct { backend_index: usize, key: u64 };
    fn currentPolicy(backend: ops.ComputeBackend) !u64 {
        const functions = backend.vtable.kernels orelse return error.PreparedRegionPolicyChanged;
        if (!functions.accepts(backend.vtable)) return error.PreparedRegionPolicyChanged;
        const query = functions.regionPolicyKey orelse return error.PreparedRegionPolicyChanged;
        const key = (try query(backend.ptr)) orelse return error.PreparedRegionPolicyChanged;
        if (key == 0) return error.PreparedRegionPolicyChanged;
        return key;
    }

    pub fn matchesReadiness(self: *const Selected) bool {
        for (self.policies) |policy| if ((currentPolicy(self.policy_backends[policy.backend_index]) catch return false) != policy.key) return false;
        for (self.groups) |group| if (group.dependency) |dependency| {
            if (!dependency.vtable.matches(dependency.context)) return false;
        };
        return true;
    }
    pub fn validateReadiness(self: *const Selected) !void {
        for (self.policies) |policy| if (try currentPolicy(self.policy_backends[policy.backend_index]) != policy.key) return error.PreparedRegionPolicyChanged;
        for (self.groups) |group| if (group.dependency) |dependency| try dependency.vtable.validate(dependency.context);
    }

    pub fn deinit(self: *Selected) void {
        for (self.groups) |*group| group.deinit();
        self.allocator.free(self.groups);
        self.allocator.free(self.owners);
        self.allocator.free(self.backend_indices);
        self.allocator.free(self.policies);
        self.* = undefined;
    }
};

pub fn select(request: Request) !?Selected {
    var enabled = false;
    for (request.backends) |backend| {
        const functions = backend.vtable.kernels orelse continue;
        enabled = enabled or (functions.accepts(backend.vtable) and functions.prepareRegionRoutes != null);
    }
    if (!enabled) return null;
    const n = request.graph.nodeCount();
    if (request.reachable.len != n or request.node_facts.len != n or request.plan.node_positions.len != n) return error.InvalidRegionOrder;
    const a = request.allocator;
    const order = try a.alloc(NodeId, n);
    defer a.free(order);
    @memset(order, ml.graph.null_node);
    const last_use = try a.alloc(u32, n);
    defer a.free(last_use);
    @memset(last_use, 0);
    var max_partition: usize = 0;
    var live: usize = 0;
    for (request.reachable, 0..) |reachable, raw| {
        if (!reachable) continue;
        const node: NodeId = @intCast(raw);
        const slot = request.plan.slotForNode(node) orelse return error.InvalidRegionOrder;
        const position = request.plan.positionForNode(node);
        if (position >= n or order[position] != ml.graph.null_node or slot.partition_index >= n) return error.InvalidRegionOrder;
        order[position] = node;
        last_use[node] = slot.last_use;
        max_partition = @max(max_partition, slot.partition_index);
        live += 1;
    }
    if (live == 0) return null;
    var used: usize = 0;
    for (order) |node| if (node != ml.graph.null_node) {
        order[used] = node;
        used += 1;
    };
    const counts = try a.alloc(usize, max_partition + 1);
    defer a.free(counts);
    @memset(counts, 0);
    for (order[0..used]) |node| counts[request.plan.slotForNode(node).?.partition_index] += 1;
    const offsets = try a.alloc(usize, counts.len);
    defer a.free(offsets);
    var end: usize = 0;
    for (counts, offsets) |count, *offset| {
        offset.* = end;
        end += count;
    }
    const nodes = try a.alloc(NodeId, used);
    defer a.free(nodes);
    const cursors = try a.dupe(usize, offsets);
    defer a.free(cursors);
    for (order[0..used]) |node| {
        const index = request.plan.slotForNode(node).?.partition_index;
        nodes[cursors[index]] = node;
        cursors[index] += 1;
    }
    var policies: std.ArrayList(Selected.TargetPolicy) = .empty;
    defer policies.deinit(a);
    var groups: std.ArrayList(region.OwnedList) = .empty;
    defer groups.deinit(a);
    errdefer for (groups.items) |*group| group.deinit();
    var owners: std.ArrayList(*const region.Owned) = .empty;
    defer owners.deinit(a);
    var backend_indices: std.ArrayList(usize) = .empty;
    defer backend_indices.deinit(a);
    for (counts, offsets, 0..) |count, offset, partition| {
        if (count == 0) continue;
        const members = nodes[offset..][0..count];
        const kind = request.plan.slotForNode(members[0]).?.backend;
        var target: ?usize = null;
        var ambiguous = false;
        for (request.backends, 0..) |backend, index| {
            if (backend.kind() != kind) continue;
            const functions = backend.vtable.kernels orelse continue;
            if (!functions.accepts(backend.vtable) or functions.prepareRegionRoutes == null) continue;
            // This physical plan has no per-slot concrete device identity.
            // Ambiguous same-kind providers remain compatibility execution.
            if (target != null) {
                ambiguous = true;
                break;
            }
            target = index;
        }
        if (ambiguous) continue;
        const backend_index = target orelse continue;
        const backend = request.backends[backend_index];
        const prepare = backend.vtable.kernels.?.prepareRegionRoutes.?;
        const query = backend.vtable.kernels.?.regionPolicyKey orelse continue;
        const policy_key = (try query(backend.ptr)) orelse continue;
        if (policy_key == 0) return error.PreparedRegionPolicyChanged;
        var produced = (try prepare(backend.ptr, .{ .allocator = a, .implementation = .{ .context = backend.ptr, .table = backend.vtable }, .policy_key = policy_key, .execution_profile = request.execution_profile, .graph = request.graph, .reachable = request.reachable, .node_order = order[0..used], .last_use = last_use, .partition_index = @intCast(partition), .partition_nodes = members, .node_facts = request.node_facts })) orelse continue;
        var transferred = false;
        defer if (!transferred) produced.deinit();
        if ((try query(backend.ptr)) != policy_key) return error.PreparedRegionPolicyChanged;
        if (produced.items.len == 0) {
            if (produced.dependency != null) {
                try policies.append(a, .{ .backend_index = backend_index, .key = policy_key });
                try groups.append(a, produced);
                transferred = true;
            }
            continue;
        }
        for (produced.items) |*owner| {
            if (owner.policy_key != policy_key) return error.PreparedRegionPolicyChanged;
            if (owner.implementation.context != backend.ptr or owner.implementation.table != @as(*const anyopaque, backend.vtable)) return error.PreparedRegionOwnerChanged;
            for (owner.covered_nodes) |node| {
                if (node >= n or !request.reachable[node] or request.plan.slotForNode(node).?.partition_index != partition) return error.InvalidRegionBoundary;
            }
            try owners.append(a, owner);
            try backend_indices.append(a, backend_index);
        }
        try policies.append(a, .{ .backend_index = backend_index, .key = policy_key });
        try groups.append(a, produced);
        transferred = true;
    }
    if (groups.items.len == 0) return null;
    try @import("region_schedule.zig").validateReachableBoundaries(a, request.graph, request.reachable, owners.items);
    const owned_groups = try groups.toOwnedSlice(a);
    errdefer {
        for (owned_groups) |*group| group.deinit();
        a.free(owned_groups);
    }
    const owned_owners = try owners.toOwnedSlice(a);
    errdefer a.free(owned_owners);
    const owned_backends = try backend_indices.toOwnedSlice(a);
    errdefer a.free(owned_backends);
    const owned_policies = try policies.toOwnedSlice(a);
    return .{ .allocator = a, .groups = owned_groups, .owners = owned_owners, .backend_indices = owned_backends, .policy_backends = request.backends, .policies = owned_policies };
}

test "zero-region readiness dependency invalidates after compatibility warms a source" {
    const Watch = struct {
        epoch: u64 = 1,
        selected_epoch: u64 = 1,
        fn matches(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.epoch == self.selected_epoch;
        }
        fn validate(raw: *anyopaque) !void {
            if (!matches(raw)) return error.PreparedRegionPolicyChanged;
        }
        fn deinit(_: *anyopaque) void {}
        const table = region.SelectionDependency.VTable{ .matches = matches, .validate = validate, .deinit = deinit };
    };
    var watch = Watch{};
    var groups = [_]region.OwnedList{.{ .allocator = std.testing.allocator, .items = &.{}, .dependency = .{ .context = &watch, .vtable = &Watch.table } }};
    const selected = Selected{ .allocator = std.testing.allocator, .groups = &groups, .owners = &.{}, .backend_indices = &.{}, .policy_backends = &.{}, .policies = &.{} };
    try std.testing.expect(selected.matchesReadiness());
    watch.epoch += 1;
    try std.testing.expect(!selected.matchesReadiness());
    try std.testing.expectError(error.PreparedRegionPolicyChanged, selected.validateReadiness());
}
