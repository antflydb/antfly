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
pub const default_max_intermediate_states: usize = 100_000;

pub const Dimension = enum {
    explored_nodes,
    explored_edges,
    explored_edge_bytes,
    intermediate_states,
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
    remaining_nodes: usize,
    remaining_edges: usize,
    remaining_edge_bytes: usize,
    last_exhaustion: ?Exhaustion = null,

    pub fn init(max_nodes: usize, max_edges: usize) WorkBudget {
        return .{
            .max_nodes = max_nodes,
            .max_edges = max_edges,
            .max_edge_bytes = default_max_explored_edge_bytes,
            .remaining_nodes = max_nodes,
            .remaining_edges = max_edges,
            .remaining_edge_bytes = default_max_explored_edge_bytes,
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

    pub fn consumeMaterializedEdges(self: *WorkBudget, edges: []const graph_mod.Edge) !void {
        try self.consumeEdges(edges.len);
        const bytes = edgeOwnedBytes(edges) catch return self.exhaust(.explored_edge_bytes, self.max_edge_bytes);
        try self.consumeEdgeBytes(bytes);
    }

    pub fn checkIntermediateStates(self: *WorkBudget, count: usize, maximum: usize) !void {
        if (count > maximum) return self.exhaust(.intermediate_states, maximum);
    }

    pub fn exhaust(self: *WorkBudget, dimension: Dimension, maximum: usize) error{GraphWorkBudgetExceeded} {
        self.last_exhaustion = .{ .dimension = dimension, .maximum = maximum };
        return error.GraphWorkBudgetExceeded;
    }

    pub fn exhaustion(self: WorkBudget) ?Exhaustion {
        return self.last_exhaustion;
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
