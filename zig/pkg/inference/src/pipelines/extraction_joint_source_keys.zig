// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Pinned CPython-compatible keys for the Fastino 3c913c7 JointIE profile.
//! These keys describe original body-token coordinates, never byte offsets or
//! confidence-ranked compact indices. All allocations use the decoder arena.
const std = @import("std");
const repr = @import("extraction_python_repr.zig");
const Allocator = std.mem.Allocator;

pub const Span = struct { start: usize, end: usize };
pub const Identity = struct { node_spans: []const Span };
pub const Keys = struct {
    nodes: []const []const u8,
    edges: []const []const u8,
    derived: []const []const u8,
    hypotheses: []const []const u8,
    slots: []const []const u8,
};

const Writer = struct {
    allocator: Allocator,
    remaining: usize,
    fn text(self: *Writer, value: []const u8) ![]const u8 {
        const result = repr.format(self.allocator, value, self.remaining) catch |err| switch (err) {
            error.JointScoringLimitExceeded => return error.JointCandidateLimitExceeded,
            else => return err,
        };
        self.remaining -= result.len;
        return result;
    }
    fn format(self: *Writer, comptime fmt: []const u8, values: anytype) ![]const u8 {
        const size = std.fmt.count(fmt, values);
        if (size > self.remaining) return error.JointCandidateLimitExceeded;
        self.remaining -= size;
        return std.fmt.allocPrint(self.allocator, fmt, values);
    }
    fn integer(self: *Writer, value: ?u64) ![]const u8 {
        return if (value) |number| self.format("{d}", .{number}) else "None";
    }
};

pub fn prepare(allocator: Allocator, schema: anytype, nodes: anytype, edges: anytype, graph: anytype, identity: Identity, limit: usize) !Keys {
    if (limit == 0 or limit > 1024 * 1024) return error.JointCandidateLimitExceeded;
    if (identity.node_spans.len != nodes.len) return error.InvalidJointSourceIdentity;
    var writer = Writer{ .allocator = allocator, .remaining = limit };
    const names = try allocator.alloc([]const u8, schema.entities.len);
    for (schema.entities, names) |entity, *name| name.* = try writer.text(entity.name);
    const relation_names = try allocator.alloc([]const u8, schema.relations.len);
    for (schema.relations, relation_names) |relation, *name| name.* = try writer.text(relation.name);
    const node_ids = try allocator.alloc([]const u8, nodes.len);
    for (nodes, identity.node_spans, node_ids, 0..) |node, span, *key, i| {
        if (span.end <= span.start) return error.InvalidJointSourceIdentity;
        for (nodes[0..i], identity.node_spans[0..i]) |other, other_span| {
            if (node.entity_type == other.entity_type and span.start == other_span.start and span.end == other_span.end)
                return error.DuplicateJointSourceIdentity;
        }
        key.* = try writer.format("({s}, {d}, {d})", .{ names[node.entity_type], span.start, span.end });
    }
    const edge_ids = try allocator.alloc([]const u8, edges.len);
    const hypotheses = try allocator.alloc([]const u8, edges.len);
    const slots = try allocator.alloc([]const u8, edges.len);
    for (edges, edge_ids, hypotheses, slots, 0..) |edge, *key, *hypothesis, *slot, i| {
        hypothesis.* = if (edge.hypothesis) |index| blk: {
            if (index >= schema.relations.len) return error.InvalidJointSourceIdentity;
            break :blk schema.relations[@intCast(index)].name;
        } else "None";
        slot.* = try writer.integer(edge.slot);
        const alternative = try writer.integer(edge.count_alternative);
        key.* = try writer.format("({s}, {s}, {s}, {s}, {s})", .{
            relation_names[edge.relation_type], node_ids[edge.head], node_ids[edge.tail], slot.*, alternative,
        });
        for (edge_ids[0..i]) |previous| if (std.mem.eql(u8, previous, key.*)) return error.DuplicateJointSourceIdentity;
    }
    const derived = try allocator.alloc([]const u8, graph.len);
    for (graph, derived) |edge, *key| key.* = try writer.format("('derived', {s}, {s}, {s})", .{
        relation_names[edge.relation_type], node_ids[edge.head], node_ids[edge.tail],
    });
    return .{ .nodes = node_ids, .edges = edge_ids, .derived = derived, .hypotheses = hypotheses, .slots = slots };
}
