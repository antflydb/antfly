// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const graph_mod = @import("graph.zig");

pub const default_max_explored_nodes: usize = 100_000;
pub const default_max_explored_edges: usize = 1_000_000;
pub const default_max_explored_edge_bytes: usize = 64 * 1024 * 1024;
pub const default_max_scanned_anchors: usize = 1_000_000;
pub const default_max_intermediate_states: usize = 100_000;
pub const default_max_retained_state_bytes: usize = 64 * 1024 * 1024;

pub const Dimension = enum {
    explored_nodes,
    explored_edges,
    explored_edge_bytes,
    scanned_anchors,
    intermediate_states,
    retained_state_bytes,
};

pub const Exhaustion = struct {
    dimension: Dimension,
    maximum: usize,
};

/// One request-owned graph expansion budget. Callers pass the same instance to
/// every named graph operation, including every Yen spur search, so composing
/// operations cannot multiply CPU or retained-memory admission.
pub const WorkBudget = struct {
    max_nodes: usize,
    max_edges: usize,
    max_edge_bytes: usize,
    max_anchors: usize,
    max_retained_state_bytes: usize,
    remaining_nodes: usize,
    remaining_edges: usize,
    remaining_edge_bytes: usize,
    remaining_anchors: usize,
    retained_state_bytes: usize = 0,
    last_exhaustion: ?Exhaustion = null,

    pub fn init(max_nodes: usize, max_edges: usize) WorkBudget {
        return .{
            .max_nodes = max_nodes,
            .max_edges = max_edges,
            .max_edge_bytes = default_max_explored_edge_bytes,
            .max_anchors = default_max_scanned_anchors,
            .max_retained_state_bytes = default_max_retained_state_bytes,
            .remaining_nodes = max_nodes,
            .remaining_edges = max_edges,
            .remaining_edge_bytes = default_max_explored_edge_bytes,
            .remaining_anchors = default_max_scanned_anchors,
        };
    }

    pub fn nodeLimit(self: WorkBudget) usize {
        return self.remaining_nodes;
    }

    pub fn edgeLimit(self: WorkBudget) usize {
        return self.remaining_edges;
    }

    pub fn edgeByteLimit(self: WorkBudget) usize {
        return self.remaining_edge_bytes;
    }

    pub fn anchorLimit(self: WorkBudget) usize {
        return self.remaining_anchors;
    }

    pub fn consumeNode(self: *WorkBudget) !void {
        if (self.remaining_nodes == 0) return self.exhaust(.explored_nodes, self.max_nodes);
        self.remaining_nodes -= 1;
    }

    pub fn consumeNodes(self: *WorkBudget, count: usize) !void {
        if (count > self.remaining_nodes) return self.exhaust(.explored_nodes, self.max_nodes);
        self.remaining_nodes -= count;
    }

    pub fn consumeEdges(self: *WorkBudget, count: usize) !void {
        if (count > self.remaining_edges) return self.exhaust(.explored_edges, self.max_edges);
        self.remaining_edges -= count;
    }

    pub fn consumeEdgeBytes(self: *WorkBudget, bytes: usize) !void {
        if (bytes > self.remaining_edge_bytes) return self.exhaust(.explored_edge_bytes, self.max_edge_bytes);
        self.remaining_edge_bytes -= bytes;
    }

    pub fn consumeAnchors(self: *WorkBudget, count: usize) !void {
        if (count > self.remaining_anchors) return self.exhaust(.scanned_anchors, self.max_anchors);
        self.remaining_anchors -= count;
    }

    pub fn consumeMaterializedEdges(self: *WorkBudget, edges: []const graph_mod.Edge) !void {
        try self.consumeEdges(edges.len);
        const bytes = edgeOwnedBytes(edges) catch return self.exhaust(.explored_edge_bytes, self.max_edge_bytes);
        try self.consumeEdgeBytes(bytes);
    }

    pub fn checkIntermediateStates(self: *WorkBudget, count: usize, maximum: usize) !void {
        if (count > maximum) return self.exhaust(.intermediate_states, maximum);
    }

    pub fn retainStateBytes(self: *WorkBudget, bytes: usize) !void {
        const retained = std.math.add(usize, self.retained_state_bytes, bytes) catch
            return self.exhaust(.retained_state_bytes, self.max_retained_state_bytes);
        if (retained > self.max_retained_state_bytes)
            return self.exhaust(.retained_state_bytes, self.max_retained_state_bytes);
        self.retained_state_bytes = retained;
    }

    pub fn releaseStateBytes(self: *WorkBudget, bytes: usize) void {
        std.debug.assert(bytes <= self.retained_state_bytes);
        self.retained_state_bytes -= bytes;
    }

    pub fn exhaust(self: *WorkBudget, dimension: Dimension, maximum: usize) error{GraphWorkBudgetExceeded} {
        self.last_exhaustion = .{ .dimension = dimension, .maximum = maximum };
        return error.GraphWorkBudgetExceeded;
    }

    pub fn exhaustion(self: WorkBudget) ?Exhaustion {
        return self.last_exhaustion;
    }
};

/// A byte reservation tied to the lifetime of one transient owner. Reserve
/// before allocating, transfer the lease with the owner, and release it from
/// deinit. A null budget keeps internal/test callers lightweight.
pub const RetainedLease = struct {
    budget: ?*WorkBudget = null,
    bytes: usize = 0,

    pub fn init(budget: ?*WorkBudget, bytes: usize) !RetainedLease {
        if (budget) |value| try value.retainStateBytes(bytes);
        return .{ .budget = budget, .bytes = bytes };
    }

    pub fn resize(self: *RetainedLease, bytes: usize) !void {
        const budget = self.budget orelse {
            self.bytes = bytes;
            return;
        };
        if (bytes > self.bytes) {
            try budget.retainStateBytes(bytes - self.bytes);
        } else if (bytes < self.bytes) {
            budget.releaseStateBytes(self.bytes - bytes);
        }
        self.bytes = bytes;
    }

    pub fn deinit(self: *RetainedLease) void {
        if (self.budget) |budget| budget.releaseStateBytes(self.bytes);
        self.* = .{};
    }

    /// Convert a live reservation into a request-consumptive output charge.
    /// Use only when the owned bytes escape the budget's lifetime.
    pub fn consume(self: *RetainedLease) void {
        self.* = .{};
    }
};

fn edgeOwnedBytes(edges: []const graph_mod.Edge) !usize {
    var total: usize = 0;
    for (edges) |edge| {
        total = try std.math.add(usize, total, @sizeOf(graph_mod.Edge));
        total = try std.math.add(usize, total, edge.source.len);
        total = try std.math.add(usize, total, edge.target.len);
        total = try std.math.add(usize, total, edge.edge_type.len);
        total = try std.math.add(usize, total, edge.metadata.len);
    }
    return total;
}

test "anchor scans have an independent request-wide budget" {
    var budget = WorkBudget.init(1, 1);
    budget.max_anchors = 2;
    budget.remaining_anchors = 2;
    try budget.consumeAnchors(2);
    try std.testing.expectError(error.GraphWorkBudgetExceeded, budget.consumeAnchors(1));
    try std.testing.expectEqual(Dimension.scanned_anchors, budget.exhaustion().?.dimension);
}

test "retained expansion state has an explicit byte ceiling" {
    var budget = WorkBudget.init(1, 1);
    budget.max_retained_state_bytes = 4;
    try budget.retainStateBytes(3);
    try std.testing.expectError(
        error.GraphWorkBudgetExceeded,
        budget.retainStateBytes(2),
    );
    try std.testing.expectEqual(Dimension.retained_state_bytes, budget.exhaustion().?.dimension);
    try std.testing.expectEqual(@as(usize, 3), budget.retained_state_bytes);
    budget.releaseStateBytes(3);
    try budget.retainStateBytes(4);
    budget.releaseStateBytes(4);
    try std.testing.expectEqual(@as(usize, 0), budget.retained_state_bytes);
}
