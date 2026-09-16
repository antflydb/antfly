// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Differentiable relation and dense record task graphs. Routing and masks are
//! immutable training decisions; task parameter values and encoder/candidate
//! states always remain live nodes in the caller's graph.
const std = @import("std");
const ml = @import("ml").graph;
const core = @import("gliner_boundary_graph.zig");
const candidate = @import("gliner_boundary_graph_candidates.zig");
const schema = @import("../pipelines/extraction_schema.zig");
const G = core.GraphBuilder;
const Id = ml.NodeId;
const Shape = ml.Shape;
fn mul(a: u32, b: u32) !u32 {
    return std.math.mul(u32, a, b) catch error.BoundaryTrainingGraphLimitExceeded;
}
fn admit(g: *G, dims: []const i64) !void {
    try g.check();
    const count = Shape.init(.f32, dims).numElements() orelse return error.InvalidBoundaryTrainingGraphShape;
    if (count < 0 or count > g.limits.max_tensor_elements) return error.BoundaryTrainingGraphLimitExceeded;
}

pub const RelationInput = struct {
    pairs: u32,
    relations: u32,
    text: Id, // [B*W,H], live original text states (not boundary states)
    relation_queries: Id, // [B*R,H] or [B*R,2H]
    query_indices: Id, // [P] absolute relation-query rows
    text_indices: [4]Id, // each [P]: head first,last then tail first,last
    head_prefix_start: Id, // each [P], absolute row in [B*(W+1),H]
    head_prefix_end: Id,
    tail_prefix_start: Id,
    tail_prefix_end: Id,
    head_length: Id, // [P,1], max(original_end-original_start,1)
    tail_length: Id,
    geometry: Id, // [P,2], sign(tail_start-head_start), abs(delta)/W
    valid: Id, // [P], typed route validity
};
pub const RelationOutput = struct {
    logits: Id, // [P]; invalid slots are exactly zero
    features: Id,
    hidden: Id,
    mlp_logits: Id,
    head_content: ?Id,
    tail_content: ?Id,
};

pub fn buildRelations(g: *G, input: RelationInput) !RelationOutput {
    if (!g.config.head.enable_relations) return error.UnsupportedBoundaryTrainingGraphOption;
    if (input.pairs == 0 or input.relations == 0) return error.InvalidBoundaryTrainingGraphLayout;
    const p = input.pairs;
    const h = g.config.encoder.hidden_size;
    const query_dim = if (g.config.head.directional_relation_states) try mul(2, h) else h;
    const feature_dim = try std.math.add(u32, try std.math.add(u32, try mul(4, h), query_dim), 2);
    try admit(g, &.{ p, feature_dim });
    try g.require(input.text, Shape.init(.f32, &.{ try mul(g.layout.batch, g.layout.words), h }));
    try g.require(input.relation_queries, Shape.init(.f32, &.{ try mul(g.layout.batch, input.relations), query_dim }));
    try g.require(input.query_indices, Shape.init(.i32, &.{p}));
    for (input.text_indices) |index| try g.require(index, Shape.init(.i32, &.{p}));
    try g.require(input.geometry, Shape.init(.f32, &.{ p, 2 }));
    try g.require(input.valid, Shape.init(.f32, &.{p}));
    const rel = try g.gather(input.relation_queries, input.query_indices, p, query_dim);
    var features = try g.gather(input.text, input.text_indices[0], p, h);
    for (input.text_indices[1..]) |index| features = try g.builder.concat(features, try g.gather(input.text, index, p, h), 1);
    features = try g.builder.concat(try g.builder.concat(features, rel, 1), input.geometry, 1);
    var hidden = try g.linear(features, feature_dim, h, "relation_scorer.mlp.0");
    hidden = try g.dropout(try g.builder.geluExact(hidden), "relations.hidden", .candidates);
    const mlp = try g.linear(hidden, h, 1, "relation_scorer.mlp.3");
    var score = mlp;
    var head_content: ?Id = null;
    var tail_content: ?Id = null;
    if (g.config.head.relation_biaffine_content) {
        for ([_]Id{ input.head_prefix_start, input.head_prefix_end, input.tail_prefix_start, input.tail_prefix_end }) |index| try g.require(index, Shape.init(.i32, &.{p}));
        const prefix = try g.prefixSum(input.text, g.layout.batch, g.layout.words, h);
        const heads = try candidate.meanPool(g, prefix, input.head_prefix_start, input.head_prefix_end, input.head_length, p, h);
        const tails = try candidate.meanPool(g, prefix, input.tail_prefix_start, input.tail_prefix_end, input.tail_length, p, h);
        head_content = try g.linear(heads, h, h, "relation_scorer.head_content_projection");
        tail_content = try g.linear(tails, h, h, "relation_scorer.tail_content_projection");
        const gate = try g.builder.sigmoid(try g.linear(rel, query_dim, h, "relation_scorer.relation_content_gate"));
        const weighted = try g.builder.mul(try g.builder.mul(head_content.?, gate), tail_content.?);
        const biaffine = try g.scale(try g.builder.reduceSum(weighted, &.{1}), 1 / @sqrt(@as(f32, @floatFromInt(h))));
        const joined = try g.builder.concat(try g.builder.concat(head_content.?, tail_content.?, 1), rel, 1);
        const linear = try g.linear(joined, try std.math.add(u32, try mul(2, h), query_dim), 1, "relation_scorer.content_linear");
        score = try g.builder.add(try g.builder.add(score, biaffine), linear);
    }
    const result = RelationOutput{ .logits = try g.reshape(try g.maskFill(score, try g.reshape(input.valid, &.{ p, 1 }), 0), &.{p}), .features = features, .hidden = hidden, .mlp_logits = try g.reshape(mlp, &.{p}), .head_content = head_content, .tail_content = tail_content };
    try g.check();
    return result;
}

pub const RecordInput = struct {
    mode: schema.RecordMode,
    candidates: u32,
    fields: u32,
    candidate_states: Id, // [C,H], live shared candidate encoder output
    field_queries: Id, // [F,H], live routed query states
    candidate_mask: Id, // [C]
    field_membership: Id, // [F,C], includes field/query/group validity
    instance_mask: Id, // [max(C,learned_instance_queries)]
    /// Natural object scores come from the live anchor-query pair logits;
    /// this node must be omitted in latent and anchorless modes.
    natural_object_logits: ?Id = null, // [C]
};
pub const RecordOutput = struct {
    instances: u32,
    instance_states: Id, // [I,H]
    object_logits: Id, // [I], invalid slots filled with -1e4
    assignment_logits: Id, // [I,F,1+C], column0 ABSENT
};

fn padRows(g: *G, value: Id, before: u32, after: u32, dim: u32) !Id {
    if (before > after) return error.InvalidBoundaryTrainingGraphShape;
    try g.require(value, Shape.init(.f32, &.{ before, dim }));
    if (before == after) return value;
    return g.builder.concat(value, try g.fill(&.{ after - before, dim }, 0), 0);
}

/// One dense compiled record group, preserving upstream's padded instance
/// width and a shared candidate column domain. Matching later binds gold IDs
/// to these exact columns; it must not flatten fields into repeated candidates.
pub fn buildRecordGroupDense(g: *G, input: RecordInput) !RecordOutput {
    if (!g.config.head.enable_records) return error.UnsupportedBoundaryTrainingGraphOption;
    const c = input.candidates;
    const f = input.fields;
    const h = g.config.encoder.hidden_size;
    const d = g.config.head.record_dim;
    const learned_count = g.config.head.record_instance_queries;
    const instances = @max(c, learned_count);
    if (c == 0 or f == 0) return error.InvalidBoundaryTrainingGraphLayout;
    try admit(g, &.{ instances, f, try std.math.add(u32, c, 1) });
    try admit(g, &.{ instances, f, d });
    try g.require(input.candidate_states, Shape.init(.f32, &.{ c, h }));
    try g.require(input.field_queries, Shape.init(.f32, &.{ f, h }));
    try g.require(input.candidate_mask, Shape.init(.f32, &.{c}));
    try g.require(input.field_membership, Shape.init(.f32, &.{ f, c }));
    try g.require(input.instance_mask, Shape.init(.f32, &.{instances}));
    if (input.mode == .natural) {
        try g.require(input.natural_object_logits orelse return error.MissingBoundaryTrainingNaturalObject, Shape.init(.f32, &.{c}));
    } else if (input.natural_object_logits != null) return error.InvalidBoundaryTrainingGraphLayout;
    const states = if (input.mode == .anchorless) blk: {
        const learned = try padRows(g, try g.weight("record_decoder.instance_embed", &.{ learned_count, h }), learned_count, instances, h);
        const query = try g.linear(learned, h, d, "record_decoder.q_proj");
        const key = try g.linear(input.candidate_states, h, d, "record_decoder.k_proj");
        const value = try g.linear(input.candidate_states, h, h, "record_decoder.v_proj");
        try admit(g, &.{ instances, c });
        const logits = try g.scale(try g.builder.matmul(query, try g.builder.transpose(key, &.{ 1, 0 })), 1 / @sqrt(@as(f32, @floatFromInt(d))));
        const mask = try g.expand(input.candidate_mask, &.{ instances, c }, &.{1});
        const weights = try g.builder.softmax(try g.maskFill(logits, mask, -10000));
        break :blk try g.builder.add(learned, try g.builder.matmul(weights, value));
    } else try padRows(g, input.candidate_states, c, instances, h);
    const object = switch (input.mode) {
        .natural => try padRows(g, try g.reshape(input.natural_object_logits.?, &.{ c, 1 }), c, instances, 1),
        .latent => try padRows(g, try g.linear(input.candidate_states, h, 1, "record_decoder.latent_seed_head"), c, instances, 1),
        .anchorless => try g.linear(states, h, 1, "record_decoder.object_head"),
    };
    const instance_query = try g.linear(states, h, d, "record_decoder.inst_proj");
    const field_query = try g.linear(input.field_queries, h, d, "record_decoder.field_proj");
    const query = try g.builder.add(try g.expand(instance_query, &.{ instances, f, d }, &.{ 0, 2 }), try g.expand(field_query, &.{ instances, f, d }, &.{ 1, 2 }));
    const query2 = try g.reshape(query, &.{ try mul(instances, f), d });
    const null_embedding = try g.expand(try g.weight("record_decoder.null_embed", &.{d}), &.{ d, 1 }, &.{0});
    const null_logits = try g.builder.matmul(query2, null_embedding);
    const candidate_proj = try g.linear(input.candidate_states, h, d, "record_decoder.cand_proj");
    const candidate_logits = try g.reshape(try g.builder.matmul(query2, try g.builder.transpose(candidate_proj, &.{ 1, 0 })), &.{ instances, f, c });
    const membership = try g.expand(input.field_membership, &.{ instances, f, c }, &.{ 1, 2 });
    const assignments = try g.builder.concat(try g.reshape(null_logits, &.{ instances, f, 1 }), try g.maskFill(candidate_logits, membership, -10000), 2);
    const output = RecordOutput{ .instances = instances, .instance_states = states, .object_logits = try g.maskFill(try g.reshape(object, &.{instances}), input.instance_mask, -10000), .assignment_logits = assignments };
    try g.check();
    return output;
}
