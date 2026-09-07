// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Persistent, pointer-free parent/child history graph. Nodes contain only
//! replay artifact identities and diagnostics, so a graph can survive process
//! and source revisions without pretending that an in-memory world is valid.

const std = @import("std");
const ids = @import("id.zig");
const trace = @import("trace.zig");

pub const format = "vopr-multiverse-v1";

pub const Node = struct {
    artifact_digest: u64,
    scenario: []const u8,
    scenario_version: u32,
    source_revision_digest: u64,
    final_observation_digest: u64,
    failure_fingerprint: u64,
    transitions: u64,
};

pub const Edge = struct {
    parent_digest: u64,
    child_digest: u64,
    branch_choice_index: u64,
    replacement_id: ids.StableId,
};

pub const Graph = struct {
    allocator: std.mem.Allocator,
    nodes: std.ArrayListUnmanaged(Node) = .empty,
    edges: std.ArrayListUnmanaged(Edge) = .empty,

    pub fn init(allocator: std.mem.Allocator) Graph {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Graph) void {
        for (self.nodes.items) |node| self.allocator.free(node.scenario);
        self.nodes.deinit(self.allocator);
        self.edges.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn addHistory(
        self: *Graph,
        artifact: *const trace.Trace,
        parent_digest: ?u64,
        branch_choice_index: ?usize,
        replacement_id: ?ids.StableId,
    ) !u64 {
        try artifact.validate();
        const summary = artifact.summary orelse return error.MissingTraceSummary;
        if ((branch_choice_index == null) != (replacement_id == null))
            return error.IncompleteMultiverseBranchMetadata;
        if (parent_digest) |parent| if (self.find(parent) == null) return error.UnknownMultiverseParent;
        const bytes = try artifact.renderAlloc(self.allocator);
        defer self.allocator.free(bytes);
        const digest = ids.digest(bytes);
        var inserted_node = false;
        errdefer if (inserted_node) {
            const removed = self.nodes.pop().?;
            self.allocator.free(removed.scenario);
        };
        if (self.find(digest) == null) {
            const scenario = try self.allocator.dupe(u8, artifact.header.scenario);
            errdefer self.allocator.free(scenario);
            try self.nodes.append(self.allocator, .{
                .artifact_digest = digest,
                .scenario = scenario,
                .scenario_version = artifact.header.scenario_version,
                .source_revision_digest = ids.digest(artifact.header.source_revision),
                .final_observation_digest = summary.final_observation_digest,
                .failure_fingerprint = if (artifact.failures.items.len == 0) 0 else artifact.failures.items[0].fingerprint,
                .transitions = summary.transitions,
            });
            inserted_node = true;
        }
        if (parent_digest) |parent| {
            if (parent == digest or try self.canReach(digest, parent)) return error.MultiverseCycle;
            const edge: Edge = .{
                .parent_digest = parent,
                .child_digest = digest,
                .branch_choice_index = @intCast(branch_choice_index.?),
                .replacement_id = replacement_id.?,
            };
            for (self.edges.items) |existing| if (std.meta.eql(existing, edge)) return digest;
            try self.edges.append(self.allocator, edge);
        }
        return digest;
    }

    pub fn find(self: *const Graph, digest: u64) ?*const Node {
        for (self.nodes.items) |*node| if (node.artifact_digest == digest) return node;
        return null;
    }

    pub fn children(self: *const Graph, allocator: std.mem.Allocator, parent_digest: u64) ![]const u64 {
        var result: std.ArrayListUnmanaged(u64) = .empty;
        errdefer result.deinit(allocator);
        for (self.edges.items) |edge| if (edge.parent_digest == parent_digest)
            try result.append(allocator, edge.child_digest);
        std.mem.sort(u64, result.items, {}, std.sort.asc(u64));
        return result.toOwnedSlice(allocator);
    }

    pub fn validate(self: *const Graph) !void {
        for (self.nodes.items, 0..) |node, index| {
            if (node.artifact_digest == 0 or node.scenario.len == 0)
                return error.InvalidMultiverseNode;
            for (self.nodes.items[0..index]) |earlier| if (earlier.artifact_digest == node.artifact_digest)
                return error.DuplicateMultiverseNode;
        }
        for (self.edges.items) |edge| {
            if (edge.parent_digest == edge.child_digest or self.find(edge.parent_digest) == null or self.find(edge.child_digest) == null)
                return error.InvalidMultiverseEdge;
        }
    }

    pub fn renderAlloc(self: *const Graph, allocator: std.mem.Allocator) ![]u8 {
        try self.validate();
        return std.json.Stringify.valueAlloc(allocator, .{
            .format = format,
            .nodes = self.nodes.items,
            .edges = self.edges.items,
        }, .{ .whitespace = .indent_2 });
    }

    fn canReach(self: *const Graph, start: u64, target: u64) !bool {
        if (start == target) return true;
        var frontier: std.ArrayListUnmanaged(u64) = .empty;
        defer frontier.deinit(self.allocator);
        var visited: std.AutoHashMapUnmanaged(u64, void) = .empty;
        defer visited.deinit(self.allocator);
        try frontier.append(self.allocator, start);
        while (frontier.pop()) |current| {
            if (current == target) return true;
            const gop = try visited.getOrPut(self.allocator, current);
            if (gop.found_existing) continue;
            for (self.edges.items) |edge| if (edge.parent_digest == current)
                try frontier.append(self.allocator, edge.child_digest);
        }
        return false;
    }
};

test "multiverse graph persists navigable replay history identities" {
    var root = try trace.Trace.init(std.testing.allocator, .{
        .scenario = "multiverse-test",
        .scenario_version = 1,
        .source_revision = "root-revision",
    }, .{ .transition_budget = 1 });
    defer root.deinit();
    const empty_digest = @import("observation.zig").digestFeatures(&.{});
    try root.addObservation(.{ .index = 0, .digest = empty_digest, .features = &.{} });
    root.summary = .{ .transitions = 0, .final_observation_digest = empty_digest, .property_failures = 0 };

    var child = try trace.Trace.init(std.testing.allocator, .{
        .scenario = "multiverse-test",
        .scenario_version = 1,
        .source_revision = "child-revision",
    }, .{ .transition_budget = 1 });
    defer child.deinit();
    try child.addObservation(.{ .index = 0, .digest = empty_digest, .features = &.{} });
    child.summary = .{ .transitions = 0, .final_observation_digest = empty_digest, .property_failures = 0 };

    var graph = Graph.init(std.testing.allocator);
    defer graph.deinit();
    const root_digest = try graph.addHistory(&root, null, null, null);
    const child_digest = try graph.addHistory(&child, root_digest, 3, 7);
    const child_digests = try graph.children(std.testing.allocator, root_digest);
    defer std.testing.allocator.free(child_digests);
    try std.testing.expectEqualSlices(u64, &.{child_digest}, child_digests);
    const encoded = try graph.renderAlloc(std.testing.allocator);
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, format) != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "child-revision") == null);
}
