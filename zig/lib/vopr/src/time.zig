// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Deterministic global time plus per-node clock domains.
//!
//! Global scheduling time is monotonic. Per-node monotonic domains stop while
//! a node clock is paused. Reported realtime additionally supports forward or
//! backward jumps and deterministic drift. Raft logical ticks are explicit
//! counters and never change as a side effect of wall-clock faults.

const std = @import("std");
const ids = @import("id.zig");

pub const NodeId = ids.StableId;
pub const Realtime = i128;

pub const MonotonicDomain = enum {
    scheduler,
    retry,
    lease,
};

const domain_count = @typeInfo(MonotonicDomain).@"enum".fields.len;

const Node = struct {
    id: NodeId,
    paused_at_global_ns: ?u64 = null,
    paused_accumulated_ns: u64 = 0,
    monotonic_adjustment_ns: [domain_count]i64 = @splat(0),
    realtime_anchor_global_ns: u64,
    realtime_at_anchor_ns: Realtime,
    drift_ppb: i64 = 0,
    logical_ticks: u64 = 0,
};

pub const Clock = struct {
    ptr: *anyopaque,
    node_id: NodeId,
    vtable: *const VTable,

    pub const VTable = struct {
        monotonic_now: *const fn (*anyopaque, NodeId, MonotonicDomain) anyerror!u64,
        realtime_now: *const fn (*anyopaque, NodeId) anyerror!Realtime,
        logical_ticks: *const fn (*anyopaque, NodeId) anyerror!u64,
    };

    pub fn monotonicNow(self: Clock, domain: MonotonicDomain) !u64 {
        return self.vtable.monotonic_now(self.ptr, self.node_id, domain);
    }

    pub fn realtimeNow(self: Clock) !Realtime {
        return self.vtable.realtime_now(self.ptr, self.node_id);
    }

    pub fn logicalTicks(self: Clock) !u64 {
        return self.vtable.logical_ticks(self.ptr, self.node_id);
    }
};

pub const Model = struct {
    allocator: std.mem.Allocator,
    global_monotonic_ns: u64,
    nodes: []Node,

    pub fn init(
        allocator: std.mem.Allocator,
        node_ids: []const NodeId,
        initial_monotonic_ns: u64,
        initial_realtime_ns: Realtime,
    ) !Model {
        if (node_ids.len == 0) return error.EmptyClockModel;
        const nodes = try allocator.alloc(Node, node_ids.len);
        errdefer allocator.free(nodes);
        for (node_ids, 0..) |node_id, index| {
            if (node_id == 0) return error.InvalidClockNodeId;
            nodes[index] = .{
                .id = node_id,
                .realtime_anchor_global_ns = initial_monotonic_ns,
                .realtime_at_anchor_ns = initial_realtime_ns,
            };
        }
        std.mem.sort(Node, nodes, {}, struct {
            fn lessThan(_: void, lhs: Node, rhs: Node) bool {
                return lhs.id < rhs.id;
            }
        }.lessThan);
        for (nodes, 0..) |node, index| {
            if (index > 0 and nodes[index - 1].id == node.id) return error.DuplicateClockNodeId;
        }
        return .{ .allocator = allocator, .global_monotonic_ns = initial_monotonic_ns, .nodes = nodes };
    }

    pub fn deinit(self: *Model) void {
        self.allocator.free(self.nodes);
        self.* = undefined;
    }

    /// The returned copyable handle borrows this model. The model must remain
    /// at a stable address until all handles are no longer used.
    pub fn clock(self: *Model, node_id: NodeId) !Clock {
        _ = try self.nodeById(node_id);
        return .{ .ptr = self, .node_id = node_id, .vtable = &clock_vtable };
    }

    pub fn globalNow(self: *const Model) u64 {
        return self.global_monotonic_ns;
    }

    pub fn advanceGlobal(self: *Model, delta_ns: u64) !void {
        self.global_monotonic_ns = std.math.add(u64, self.global_monotonic_ns, delta_ns) catch
            return error.GlobalTimeOverflow;
    }

    pub fn pauseNode(self: *Model, node_id: NodeId) !void {
        const node = try self.nodeMut(node_id);
        if (node.paused_at_global_ns != null) return error.NodeClockAlreadyPaused;
        const realtime = try realtimeAt(node.*, self.global_monotonic_ns);
        node.realtime_at_anchor_ns = realtime;
        node.realtime_anchor_global_ns = self.global_monotonic_ns;
        node.paused_at_global_ns = self.global_monotonic_ns;
    }

    pub fn resumeNode(self: *Model, node_id: NodeId) !void {
        const node = try self.nodeMut(node_id);
        const paused_at = node.paused_at_global_ns orelse return error.NodeClockNotPaused;
        node.paused_accumulated_ns = std.math.add(
            u64,
            node.paused_accumulated_ns,
            self.global_monotonic_ns - paused_at,
        ) catch return error.NodePausedDurationOverflow;
        node.paused_at_global_ns = null;
        node.realtime_anchor_global_ns = self.global_monotonic_ns;
    }

    pub fn isPaused(self: *const Model, node_id: NodeId) !bool {
        return (try self.nodeById(node_id)).paused_at_global_ns != null;
    }

    /// Changes reported realtime only. A negative delta is an explicit
    /// backward wall-clock jump; monotonic domains and logical ticks do not
    /// change.
    pub fn jumpRealtime(self: *Model, node_id: NodeId, delta_ns: Realtime) !void {
        const node = try self.nodeMut(node_id);
        const current = try realtimeAt(node.*, self.global_monotonic_ns);
        node.realtime_at_anchor_ns = std.math.add(Realtime, current, delta_ns) catch
            return error.RealtimeOverflow;
        node.realtime_anchor_global_ns = self.global_monotonic_ns;
    }

    /// Parts per billion relative to node monotonic time. Values below -1e9
    /// would make reported realtime run backward and are rejected; explicit
    /// backward behavior belongs to jumpRealtime.
    pub fn setRealtimeDrift(self: *Model, node_id: NodeId, drift_ppb: i64) !void {
        if (drift_ppb < -999_999_999 or drift_ppb > 1_000_000_000) return error.InvalidRealtimeDrift;
        const node = try self.nodeMut(node_id);
        const current = try realtimeAt(node.*, self.global_monotonic_ns);
        node.realtime_at_anchor_ns = current;
        node.realtime_anchor_global_ns = self.global_monotonic_ns;
        node.drift_ppb = drift_ppb;
    }

    /// Explicitly adjusts one monotonic domain while preserving monotonicity.
    /// This is separate from wall jumps so faults cannot accidentally alter
    /// scheduling, retry, or lease time.
    pub fn advanceDomain(self: *Model, node_id: NodeId, domain: MonotonicDomain, delta_ns: u64) !void {
        if (delta_ns > std.math.maxInt(i64)) return error.MonotonicDomainAdjustmentOverflow;
        const node = try self.nodeMut(node_id);
        const slot = &node.monotonic_adjustment_ns[@intFromEnum(domain)];
        slot.* = std.math.add(i64, slot.*, @intCast(delta_ns)) catch
            return error.MonotonicDomainAdjustmentOverflow;
    }

    pub fn advanceLogicalTicks(self: *Model, node_id: NodeId, count: u64) !void {
        const node = try self.nodeMut(node_id);
        node.logical_ticks = std.math.add(u64, node.logical_ticks, count) catch
            return error.LogicalTickOverflow;
    }

    pub fn monotonicNow(self: *const Model, node_id: NodeId, domain: MonotonicDomain) !u64 {
        const node = try self.nodeById(node_id);
        const effective_global = node.paused_at_global_ns orelse self.global_monotonic_ns;
        const base = effective_global -| node.paused_accumulated_ns;
        const adjustment = node.monotonic_adjustment_ns[@intFromEnum(domain)];
        if (adjustment < 0) return error.InvalidMonotonicDomainAdjustment;
        return std.math.add(u64, base, @intCast(adjustment)) catch error.MonotonicTimeOverflow;
    }

    pub fn realtimeNow(self: *const Model, node_id: NodeId) !Realtime {
        return realtimeAt((try self.nodeById(node_id)).*, self.global_monotonic_ns);
    }

    pub fn logicalTicks(self: *const Model, node_id: NodeId) !u64 {
        return (try self.nodeById(node_id)).logical_ticks;
    }

    fn nodeMut(self: *Model, node_id: NodeId) !*Node {
        for (self.nodes) |*node_value| if (node_value.id == node_id) return node_value;
        return error.UnknownClockNode;
    }

    fn nodeById(self: *const Model, node_id: NodeId) !*const Node {
        for (self.nodes) |*node_value| if (node_value.id == node_id) return node_value;
        return error.UnknownClockNode;
    }

    fn clockMonotonic(ptr: *anyopaque, node_id: NodeId, domain: MonotonicDomain) !u64 {
        const self: *Model = @ptrCast(@alignCast(ptr));
        return self.monotonicNow(node_id, domain);
    }

    fn clockRealtime(ptr: *anyopaque, node_id: NodeId) !Realtime {
        const self: *Model = @ptrCast(@alignCast(ptr));
        return self.realtimeNow(node_id);
    }

    fn clockLogicalTicks(ptr: *anyopaque, node_id: NodeId) !u64 {
        const self: *Model = @ptrCast(@alignCast(ptr));
        return self.logicalTicks(node_id);
    }

    const clock_vtable: Clock.VTable = .{
        .monotonic_now = clockMonotonic,
        .realtime_now = clockRealtime,
        .logical_ticks = clockLogicalTicks,
    };
};

fn realtimeAt(node: Node, global_monotonic_ns: u64) !Realtime {
    const effective_global = node.paused_at_global_ns orelse global_monotonic_ns;
    const elapsed: Realtime = @intCast(effective_global - node.realtime_anchor_global_ns);
    const rate: Realtime = 1_000_000_000 + @as(Realtime, node.drift_ppb);
    const scaled = std.math.mul(Realtime, elapsed, rate) catch return error.RealtimeOverflow;
    return std.math.add(Realtime, node.realtime_at_anchor_ns, @divTrunc(scaled, 1_000_000_000)) catch
        error.RealtimeOverflow;
}

test "wall jumps and drift never mutate monotonic domains or Raft ticks" {
    var model = try Model.init(std.testing.allocator, &.{ 2, 1 }, 100, 1_000);
    defer model.deinit();
    const clock = try model.clock(1);
    try model.advanceLogicalTicks(1, 3);
    try model.advanceGlobal(10);
    try std.testing.expectEqual(@as(u64, 110), try clock.monotonicNow(.scheduler));
    try std.testing.expectEqual(@as(Realtime, 1_010), try clock.realtimeNow());

    try model.jumpRealtime(1, -50);
    try std.testing.expectEqual(@as(Realtime, 960), try clock.realtimeNow());
    try std.testing.expectEqual(@as(u64, 110), try clock.monotonicNow(.scheduler));
    try std.testing.expectEqual(@as(u64, 3), try clock.logicalTicks());

    try model.setRealtimeDrift(1, 1_000_000_000);
    try model.advanceGlobal(10);
    try std.testing.expectEqual(@as(Realtime, 980), try clock.realtimeNow());
    try std.testing.expectEqual(@as(u64, 120), try clock.monotonicNow(.scheduler));
    try std.testing.expectEqual(@as(u64, 3), try clock.logicalTicks());
}

test "paused node clocks freeze while other nodes and global time progress" {
    var model = try Model.init(std.testing.allocator, &.{ 1, 2 }, 0, 5_000);
    defer model.deinit();
    const first = try model.clock(1);
    const second = try model.clock(2);
    try model.advanceGlobal(10);
    try model.pauseNode(1);
    try model.advanceGlobal(20);
    try std.testing.expectEqual(@as(u64, 10), try first.monotonicNow(.retry));
    try std.testing.expectEqual(@as(Realtime, 5_010), try first.realtimeNow());
    try std.testing.expectEqual(@as(u64, 30), try second.monotonicNow(.retry));
    try std.testing.expectEqual(@as(Realtime, 5_030), try second.realtimeNow());

    try model.resumeNode(1);
    try model.advanceGlobal(5);
    try std.testing.expectEqual(@as(u64, 15), try first.monotonicNow(.retry));
    try std.testing.expectEqual(@as(Realtime, 5_015), try first.realtimeNow());
}

test "monotonic domains and logical ticks advance independently" {
    var model = try Model.init(std.testing.allocator, &.{1}, 20, 100);
    defer model.deinit();
    const clock = try model.clock(1);
    try model.advanceDomain(1, .lease, 7);
    try model.advanceLogicalTicks(1, 4);
    try std.testing.expectEqual(@as(u64, 20), try clock.monotonicNow(.scheduler));
    try std.testing.expectEqual(@as(u64, 20), try clock.monotonicNow(.retry));
    try std.testing.expectEqual(@as(u64, 27), try clock.monotonicNow(.lease));
    try std.testing.expectEqual(@as(u64, 4), try clock.logicalTicks());
}
