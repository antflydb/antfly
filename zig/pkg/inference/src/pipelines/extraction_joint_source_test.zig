// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Fixed-score optimizer parity, independent of model inference. The capture
//! executes unmodified pinned source BeamOptimizer/GreedyOptimizer modules;
//! diagnostic beam states are never supplied to the native implementation.
const std = @import("std");
const joint = @import("extraction_joint_ie.zig");
const schema_mod = @import("extraction_schema.zig");
const keys_mod = @import("extraction_joint_source_keys.zig");
const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
const Allocator = std.mem.Allocator;

const InputNode = struct { entity_type: []const u8, start: usize, end: usize, score: f64, probability: f64 };
const InputEdge = struct { relation_type: []const u8, head_index: usize, tail_index: usize, score: f64, slot: ?u64, hypothesis: ?[]const u8, count_alternative: ?u64 };
const Constraint = struct { type: []const u8, relation: ?[]const u8 = null, inverse: ?[]const u8 = null, policy: ?[]const u8 = null, limit: ?usize = null };
const OutputEdge = struct {
    relation_type: []const u8,
    head_index: usize,
    tail_index: usize,
    score: f64,
    slot: ?u64,
    hypothesis: ?[]const u8,
    count_alternative: ?u64,
    source_edge_index: ?usize,
    derived: bool,
    candidate_id_string: []const u8,
};
const Solution = struct { node_indices: []const usize, edges: []const OutputEdge, score: f64, feasible_flag: bool, final_constraints_satisfied: bool };
const Case = struct {
    id: []const u8,
    input: struct { nodes: []const InputNode, edges: []const InputEdge, constraints: []const Constraint, beam_width: usize },
    source_keys: struct { node_strings: []const []const u8, edge_strings: []const []const u8, hypothesis_strings: []const []const u8, slot_strings: []const []const u8 },
    beam: Solution,
    greedy: Solution,
    greedy_strictly_beats_all_feasible_beam_finishes: bool,
    best_feasible_beam_finish_score: ?f64,
};
const Capture = struct {
    format_version: u32,
    scope: []const u8,
    source_commit: []const u8,
    qualification: bool,
    profile_hook_removed: bool,
    forbidden_imports_observed: []const []const u8,
    cases: []const Case,
};

fn indexOf(names: []const []const u8, name: []const u8) !usize {
    for (names, 0..) |item, i| if (std.mem.eql(u8, item, name)) return i;
    return error.InvalidJointSourceFixture;
}
fn addName(a: Allocator, names: *std.ArrayListUnmanaged([]const u8), name: []const u8) !void {
    _ = indexOf(names.items, name) catch {
        try names.append(a, name);
        return;
    };
}
fn checkCase(allocator: Allocator, case: Case) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var entity_names = std.ArrayListUnmanaged([]const u8).empty;
    var relation_names = std.ArrayListUnmanaged([]const u8).empty;
    for (case.input.nodes) |node| try addName(a, &entity_names, node.entity_type);
    for (case.input.edges) |edge| try addName(a, &relation_names, edge.relation_type);
    for (case.input.constraints) |constraint| {
        if (constraint.relation) |name| try addName(a, &relation_names, name);
        if (constraint.inverse) |name| try addName(a, &relation_names, name);
    }
    const entities = try a.alloc(schema_mod.JointEntity, entity_names.items.len);
    for (entities, entity_names.items) |*entity, name| entity.* = .{ .name = name };
    const relations = try a.alloc(schema_mod.JointRelation, relation_names.items.len);
    // Fixed source problems provide their own NoSelfLoops constraint. The
    // other implicit schema constraints are equivalent for these nine cases.
    for (relations, relation_names.items) |*relation, name| relation.* = .{ .name = name, .head = &.{}, .tail = &.{}, .allow_self = true };
    const constraints = try a.alloc(schema_mod.JointConstraint, case.input.constraints.len);
    for (constraints, case.input.constraints) |*output, constraint| {
        const relation = if (constraint.relation) |name| try indexOf(relation_names.items, name) else null;
        output.* = if (std.mem.eql(u8, constraint.type, "EntityOverlapPolicy")) .{ .entity_overlap = std.meta.stringToEnum(@FieldType(schema_mod.JointConstraint, "entity_overlap"), constraint.policy.?) orelse return error.InvalidJointSourceFixture } else if (std.mem.eql(u8, constraint.type, "UniqueRelationPair")) .{ .unique_pair = .{ .relation = relation, .directed = true } } else if (std.mem.eql(u8, constraint.type, "NoSelfLoops")) .{ .no_self_loops = relation } else if (std.mem.eql(u8, constraint.type, "MaxRelationsPerHead")) .{ .max_per_head = .{ .relation = relation, .limit = constraint.limit.? } } else if (std.mem.eql(u8, constraint.type, "SymmetricRelation")) .{ .symmetric = relation.? } else if (std.mem.eql(u8, constraint.type, "InverseRelation")) .{ .inverse = .{ .relation = relation.?, .inverse = try indexOf(relation_names.items, constraint.inverse.?) } } else if (std.mem.eql(u8, constraint.type, "AcyclicRelation")) .{ .acyclic = relation.? } else return error.InvalidJointSourceFixture;
    }
    const schema = schema_mod.JointSchema{ .entities = entities, .relations = relations, .constraints = constraints };
    const nodes = try a.alloc(joint.Node, case.input.nodes.len);
    const spans = try a.alloc(joint.SourceSpan, nodes.len);
    for (nodes, spans, case.input.nodes) |*node, *span, input| {
        // Byte coordinates deliberately differ from the captured body-token
        // IDs while preserving their overlap topology.
        node.* = .{ .entity_type = try indexOf(entity_names.items, input.entity_type), .start = input.start * 17 + 5, .end = input.end * 17 + 5, .utility = input.score, .probability = input.probability };
        span.* = .{ .start = input.start, .end = input.end };
    }
    const edges = try a.alloc(joint.Edge, case.input.edges.len);
    for (edges, case.input.edges) |*edge, input| edge.* = .{
        .relation_type = try indexOf(relation_names.items, input.relation_type),
        .head = input.head_index,
        .tail = input.tail_index,
        .utility = input.score,
        .probability = 0.5,
        .slot = input.slot,
        .hypothesis = if (input.hypothesis) |name| try indexOf(relation_names.items, name) else null,
        .count_alternative = input.count_alternative,
    };
    const Graph = struct { relation_type: usize, head: usize, tail: usize };
    const graph = try a.alloc(Graph, case.beam.edges.len);
    for (graph, case.beam.edges) |*edge, expected| edge.* = .{ .relation_type = try indexOf(relation_names.items, expected.relation_type), .head = expected.head_index, .tail = expected.tail_index };
    const identity = joint.SourceIdentity{ .node_spans = spans };
    const keys = try keys_mod.prepare(a, schema, nodes, edges, graph, identity, 1024 * 1024);
    try std.testing.expectEqual(case.source_keys.node_strings.len, keys.nodes.len);
    try std.testing.expectEqual(case.source_keys.edge_strings.len, keys.edges.len);
    for (case.source_keys.node_strings, keys.nodes) |expected, actual| try std.testing.expectEqualStrings(expected, actual);
    for (case.source_keys.edge_strings, keys.edges) |expected, actual| try std.testing.expectEqualStrings(expected, actual);
    for (case.source_keys.hypothesis_strings, keys.hypotheses) |expected, actual| try std.testing.expectEqualStrings(expected, actual);
    for (case.source_keys.slot_strings, keys.slots) |expected, actual| try std.testing.expectEqualStrings(expected, actual);
    for (case.beam.edges, keys.derived) |expected, actual| if (expected.derived) try std.testing.expectEqualStrings(expected.candidate_id_string, actual);

    const options = joint.Options{ .profile = .fastino_v1, .algorithm = .beam, .beam_width = case.input.beam_width };
    var result = try joint.decodeWithSourceIdentity(allocator, schema, nodes, edges, identity, options);
    defer result.deinit();
    try std.testing.expectEqualSlices(usize, case.beam.node_indices, result.node_source_indices);
    try std.testing.expectEqual(case.beam.score, result.utility);
    try std.testing.expectEqual(case.beam.feasible_flag, result.valid());
    try std.testing.expect(!result.exhausted);
    try std.testing.expectEqual(case.beam.edges.len, result.edges.len);
    // Check the source optimizer's complete output order before the separate
    // ResultBuilder presentation step assigns its e1..eN identifiers.
    for (case.beam.edges, result.edges) |expected, actual| {
        try std.testing.expectEqualStrings(expected.relation_type, relations[actual.relation_type].name);
        try std.testing.expectEqual(expected.head_index, result.node_source_indices[actual.head]);
        try std.testing.expectEqual(expected.tail_index, result.node_source_indices[actual.tail]);
        try std.testing.expectEqual(expected.score, actual.utility);
        try std.testing.expectEqual(expected.source_edge_index, actual.source_index);
        try std.testing.expectEqual(expected.derived, actual.derived);
        try std.testing.expectEqual(expected.slot, actual.slot);
        try std.testing.expectEqual(expected.count_alternative, actual.count_alternative);
        if (expected.hypothesis) |name| {
            try std.testing.expectEqual(@as(?u64, try indexOf(relation_names.items, name)), actual.hypothesis);
        } else try std.testing.expectEqual(@as(?u64, null), actual.hypothesis);
        try std.testing.expectEqual(expected.derived, actual.derived_from != null);
    }
    try std.testing.expectEqual(case.beam.final_constraints_satisfied, try joint.validateGlobal(allocator, schema, result.nodes, result.edges, options));
    if (case.greedy_strictly_beats_all_feasible_beam_finishes) {
        try std.testing.expect(case.greedy.score > case.best_feasible_beam_finish_score.?);
        try std.testing.expectEqual(case.greedy.score, result.utility);
    }
}

test "joint fastino profile matches all nine pinned source optimizer cases" {
    const a = std.testing.allocator;
    const bytes = try fixtures.fixtureBytes(a, "joint_optimizer_source_v1/capture.json");
    defer a.free(bytes);
    try std.testing.expectEqual(@as(usize, 444862), bytes.len);
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    const hex = std.fmt.bytesToHex(hash, .lower);
    try std.testing.expectEqualStrings("bbe2a7a56166988380a0b0ec2c154a9e1e519745bfc15696d7ad57c132d94967", &hex);
    const parsed = try std.json.parseFromSlice(Capture, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    defer parsed.deinit();
    const capture = parsed.value;
    try std.testing.expectEqual(@as(u32, 1), capture.format_version);
    try std.testing.expectEqualStrings("gliner25_pinned_joint_optimizer_fixed_scores_v1", capture.scope);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", capture.source_commit);
    try std.testing.expect(!capture.qualification and capture.profile_hook_removed);
    try std.testing.expectEqual(@as(usize, 0), capture.forbidden_imports_observed.len);
    try std.testing.expectEqual(@as(usize, 9), capture.cases.len);
    for (capture.cases) |case| checkCase(a, case) catch |err| {
        std.debug.print("source JointIE case {s}: {s}\n", .{ case.id, @errorName(err) });
        return err;
    };
}
