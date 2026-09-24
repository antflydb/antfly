// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Differentiable relation and dense record task graphs. Routing and masks are
//! immutable training decisions; task parameter values and encoder/candidate
//! states always remain live nodes in the caller's graph.
const std = @import("std");
const ml = @import("ml").graph;
const core = @import("boundary_graph.zig");
const candidate = @import("boundary_graph_candidates.zig");
const schema = @import("../../pipelines/extraction_schema.zig");
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
        const heads = try candidate.meanPool(g, prefix, input.head_prefix_start, input.head_prefix_end, input.head_length, p, h, .serial_v1);
        const tails = try candidate.meanPool(g, prefix, input.tail_prefix_start, input.tail_prefix_end, input.tail_length, p, h, .serial_v1);
        head_content = try g.linear(heads, h, h, "relation_scorer.head_content_projection");
        tail_content = try g.linear(tails, h, h, "relation_scorer.tail_content_projection");
        // A shared view joins the two content adjoints before the MLP adjoint,
        // preserving the reference's accumulation without copying activations.
        const content_rel = if (g.input_gradient_profile == .pytorch_v2) try g.reshape(rel, &.{ p, query_dim }) else rel;
        const gate = try g.sigmoid(try g.linear(content_rel, query_dim, h, "relation_scorer.relation_content_gate"));
        const weighted = try g.builder.mul(try g.builder.mul(head_content.?, gate), tail_content.?);
        const biaffine = try g.scale(try g.builder.reduceSum(weighted, &.{1}), 1 / @sqrt(@as(f32, @floatFromInt(h))));
        const joined = try g.builder.concat(try g.builder.concat(head_content.?, tail_content.?, 1), content_rel, 1);
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

pub const RecordProfile = enum { per_group_v1, pytorch_batch_v1 };
/// One real group in sample-major order. Candidate states/masks belong to the
/// shared batch input, so repeated groups cannot accidentally use other pools.
pub const RecordBatchGroup = struct {
    sample: u32,
    mode: schema.RecordMode,
    fields: u32,
    field_queries: Id,
    field_membership: Id,
    instance_mask: Id,
    natural_object_logits: ?Id = null,
};
pub const RecordBatchInput = struct {
    candidates: u32,
    candidate_states: Id, // [B*C,H]
    candidate_mask: Id, // [B,C]
    groups: []const RecordBatchGroup,
};

fn constantIndices(g: *G, values: []const i32) !Id {
    const bytes = try std.math.mul(usize, values.len, @sizeOf(i32));
    if (bytes > g.limits.max_constant_bytes -| g.builder.graph.constant_pool.items.len) return error.BoundaryTrainingGraphLimitExceeded;
    return g.builder.tensorConstBytes(std.mem.sliceAsBytes(values), Shape.init(.i32, &.{@intCast(values.len)}));
}
fn constantMask(g: *G, values: []const f32) !Id {
    const bytes = try std.math.mul(usize, values.len, @sizeOf(f32));
    if (bytes > g.limits.max_constant_bytes -| g.builder.graph.constant_pool.items.len) return error.BoundaryTrainingGraphLimitExceeded;
    return g.builder.tensorConst(values, Shape.init(.f32, &.{@intCast(values.len)}));
}
fn choose(g: *G, mask: Id, yes: Id, no: Id) !Id {
    const shape = g.builder.graph.node(yes).output_shape;
    try g.require(mask, shape);
    try g.require(no, shape);
    return g.builder.graph.addNode(.{ .op = .{ .where_select = {} }, .output_shape = shape, .inputs = .{ mask, yes, no, ml.null_node }, .num_inputs = 3 });
}
fn appendRows(g: *G, accumulated: *?Id, value: Id) !void {
    accumulated.* = if (accumulated.*) |previous| try g.builder.concat(previous, value, 0) else value;
    try g.check();
}
fn takeRows(g: *G, value: Id, start: u32, rows: u32, width: u32) !Id {
    const indices = try g.allocator.alloc(i32, rows);
    defer g.allocator.free(indices);
    for (indices, 0..) |*out, i| out.* = std.math.cast(i32, @as(u64, start) + i) orelse return error.BoundaryTrainingGraphLimitExceeded;
    return g.gather(value, try constantIndices(g, indices), rows, width);
}

/// Full reference batch geometry: group and field padding remains present in
/// contractions, even though returned views contain only declared groups/fields.
/// Returns an owned array of node IDs; caller frees the array with g.allocator.
/// Shared graph/BLAS primitives provide both forward execution and strict VJPs.
pub fn buildRecordBatchDense(g: *G, input: RecordBatchInput) ![]RecordOutput {
    if (!g.config.head.enable_records) return error.UnsupportedBoundaryTrainingGraphOption;
    const b = g.layout.batch;
    const c = input.candidates;
    const h = g.config.encoder.hidden_size;
    const d = g.config.head.record_dim;
    const learned_count = g.config.head.record_instance_queries;
    const instances = @max(c, learned_count);
    if (b == 0 or c == 0 or input.groups.len == 0) return error.InvalidBoundaryTrainingGraphLayout;
    if (input.groups.len > g.limits.max_nodes) return error.BoundaryTrainingGraphLimitExceeded;
    try g.require(input.candidate_states, Shape.init(.f32, &.{ try mul(b, c), h }));
    try g.require(input.candidate_mask, Shape.init(.f32, &.{ b, c }));
    var arena = std.heap.ArenaAllocator.init(g.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const counts = try a.alloc(u32, b);
    @memset(counts, 0);
    var max_fields: u32 = 0;
    var max_groups: u32 = 0;
    var any_anchorless = false;
    var any_latent = false;
    for (input.groups, 0..) |group, index| {
        if (group.sample >= b or group.fields == 0 or (index != 0 and group.sample < input.groups[index - 1].sample)) return error.InvalidBoundaryTrainingGraphLayout;
        try g.require(group.field_queries, Shape.init(.f32, &.{ group.fields, h }));
        try g.require(group.field_membership, Shape.init(.f32, &.{ group.fields, c }));
        try g.require(group.instance_mask, Shape.init(.f32, &.{instances}));
        if (group.mode == .natural) {
            try g.require(group.natural_object_logits orelse return error.MissingBoundaryTrainingNaturalObject, Shape.init(.f32, &.{c}));
        } else if (group.natural_object_logits != null) return error.InvalidBoundaryTrainingGraphLayout;
        counts[group.sample] = try std.math.add(u32, counts[group.sample], 1);
        max_groups = @max(max_groups, counts[group.sample]);
        max_fields = @max(max_fields, group.fields);
        any_anchorless = any_anchorless or group.mode == .anchorless;
        any_latent = any_latent or group.mode == .latent;
    }
    const br = try mul(b, max_groups);
    const bri = try mul(br, instances);
    const brf = try mul(br, max_fields);
    const brif = try mul(bri, max_fields);
    const columns = try std.math.add(u32, c, 1);
    for ([_][2]u32{ .{ bri, h }, .{ brf, h }, .{ brif, d }, .{ brif, columns }, .{ bri, c } }) |dims| try admit(g, &.{ dims[0], dims[1] });
    const outputs = try g.allocator.alloc(RecordOutput, input.groups.len);
    errdefer g.allocator.free(outputs);
    const slots = try a.alloc(u32, input.groups.len);
    const anchorless_modes = try a.alloc(f32, br);
    const latent_modes = try a.alloc(f32, br);
    @memset(anchorless_modes, 0);
    @memset(latent_modes, 0);
    var fields_joined: ?Id = null;
    var membership_joined: ?Id = null;
    var instance_mask_joined: ?Id = null;
    var natural_joined: ?Id = null;
    var next: usize = 0;
    for (0..b) |sample| for (0..max_groups) |local| {
        const slot: u32 = @intCast(sample * max_groups + local);
        if (local < counts[sample]) {
            const group = input.groups[next];
            std.debug.assert(group.sample == sample);
            slots[next] = slot;
            next += 1;
            anchorless_modes[slot] = if (group.mode == .anchorless) 1 else 0;
            latent_modes[slot] = if (group.mode == .latent) 1 else 0;
            try appendRows(g, &fields_joined, try padRows(g, group.field_queries, group.fields, max_fields, h));
            try appendRows(g, &membership_joined, try padRows(g, group.field_membership, group.fields, max_fields, c));
            try appendRows(g, &instance_mask_joined, group.instance_mask);
            const natural = if (group.natural_object_logits) |node| try padRows(g, try g.reshape(node, &.{ c, 1 }), c, instances, 1) else try g.fill(&.{ instances, 1 }, 0);
            try appendRows(g, &natural_joined, natural);
        } else {
            try appendRows(g, &fields_joined, try g.fill(&.{ max_fields, h }, 0));
            try appendRows(g, &membership_joined, try g.fill(&.{ max_fields, c }, 0));
            try appendRows(g, &instance_mask_joined, try g.fill(&.{instances}, 0));
            try appendRows(g, &natural_joined, try g.fill(&.{ instances, 1 }, 0));
        }
    };
    const pool3 = try g.reshape(input.candidate_states, &.{ b, c, h });
    const padded_pool = if (c == instances) pool3 else try g.builder.concat(pool3, try g.fill(&.{ b, instances - c, h }, 0), 1);
    const pool_instances = try g.expand(padded_pool, &.{ b, max_groups, instances, h }, &.{ 0, 2, 3 });
    var states = pool_instances;
    var anchorless_states: ?Id = null;
    const anchorless_mask = try g.reshape(try constantMask(g, anchorless_modes), &.{ b, max_groups });
    if (any_anchorless) {
        const learned2 = try padRows(g, try g.weight("record_decoder.instance_embed", &.{ learned_count, h }), learned_count, instances, h);
        const learned = try g.expand(learned2, &.{ b, max_groups, instances, h }, &.{ 2, 3 });
        const q = try g.reshape(try g.linear(try g.reshape(learned, &.{ bri, h }), h, d, "record_decoder.q_proj"), &.{ br, instances, d });
        const k = try g.linear(input.candidate_states, h, d, "record_decoder.k_proj");
        const v = try g.linear(input.candidate_states, h, h, "record_decoder.v_proj");
        const kb = try g.reshape(try g.expand(try g.reshape(k, &.{ b, c, d }), &.{ b, max_groups, c, d }, &.{ 0, 2, 3 }), &.{ br, c, d });
        const vb = try g.reshape(try g.expand(try g.reshape(v, &.{ b, c, h }), &.{ b, max_groups, c, h }, &.{ 0, 2, 3 }), &.{ br, c, h });
        const attention = try g.builder.matmul3DTransB(q, kb);
        const divisor: f32 = @floatCast(@sqrt(@as(f64, @floatFromInt(d))));
        const logits = try g.builder.div(attention, try g.builder.scalarConst(.f32, divisor));
        const mask = try g.reshape(try g.expand(input.candidate_mask, &.{ b, max_groups, instances, c }, &.{ 0, 3 }), &.{ br, instances, c });
        const probabilities = try g.builder.softmax(try g.maskFill(logits, mask, -10000));
        anchorless_states = try g.builder.add(learned, try g.reshape(try g.builder.matmul3D(probabilities, vb), &.{ b, max_groups, instances, h }));
        states = try choose(g, try g.expand(anchorless_mask, &.{ b, max_groups, instances, h }, &.{ 0, 1 }), anchorless_states.?, pool_instances);
    }
    var object = try g.reshape(natural_joined.?, &.{ b, max_groups, instances });
    if (any_latent) {
        const latent2 = try g.reshape(try g.linear(input.candidate_states, h, 1, "record_decoder.latent_seed_head"), &.{ b, c });
        const padded = if (c == instances) latent2 else try g.builder.concat(latent2, try g.fill(&.{ b, instances - c }, 0), 1);
        const latent = try g.expand(padded, &.{ b, max_groups, instances }, &.{ 0, 2 });
        const mask = try g.expand(try g.reshape(try constantMask(g, latent_modes), &.{ b, max_groups }), &.{ b, max_groups, instances }, &.{ 0, 1 });
        object = try choose(g, mask, latent, object);
    }
    if (anchorless_states) |all_anchorless| {
        const scored = try g.reshape(try g.linear(try g.reshape(all_anchorless, &.{ bri, h }), h, 1, "record_decoder.object_head"), &.{ b, max_groups, instances });
        object = try choose(g, try g.expand(anchorless_mask, &.{ b, max_groups, instances }, &.{ 0, 1 }), scored, object);
    }
    const object_masked = try g.maskFill(try g.reshape(object, &.{bri}), instance_mask_joined.?, -10000);
    const instance_query = try g.reshape(try g.linear(try g.reshape(states, &.{ bri, h }), h, d, "record_decoder.inst_proj"), &.{ b, max_groups, instances, d });
    const field_query = try g.reshape(try g.linear(fields_joined.?, h, d, "record_decoder.field_proj"), &.{ b, max_groups, max_fields, d });
    const query = try g.builder.add(try g.expand(instance_query, &.{ b, max_groups, instances, max_fields, d }, &.{ 0, 1, 2, 4 }), try g.expand(field_query, &.{ b, max_groups, instances, max_fields, d }, &.{ 0, 1, 3, 4 }));
    const null_embedding = try g.expand(try g.weight("record_decoder.null_embed", &.{d}), &.{ d, 1 }, &.{0});
    const null_logits = try g.builder.matmul(try g.reshape(query, &.{ brif, d }), null_embedding);
    const candidate_projection = try g.reshape(try g.linear(input.candidate_states, h, d, "record_decoder.cand_proj"), &.{ b, c, d });
    const candidate_logits = try g.builder.matmul3DTransB(try g.reshape(query, &.{ b, try mul(try mul(max_groups, instances), max_fields), d }), candidate_projection);
    const members = try g.expand(try g.reshape(membership_joined.?, &.{ b, max_groups, max_fields, c }), &.{ b, max_groups, instances, max_fields, c }, &.{ 0, 1, 3, 4 });
    const masked = try g.maskFill(try g.reshape(candidate_logits, &.{ b, max_groups, instances, max_fields, c }), members, -10000);
    const assignments = try g.reshape(try g.builder.concat(try g.reshape(null_logits, &.{ b, max_groups, instances, max_fields, 1 }), masked, 4), &.{ brif, columns });
    const flat_states = try g.reshape(states, &.{ bri, h });
    const flat_objects = try g.reshape(object_masked, &.{ bri, 1 });
    for (input.groups, slots, outputs) |group, slot, *out| {
        const count = try mul(instances, group.fields);
        const selected = try a.alloc(i32, count);
        for (0..instances) |i| for (0..group.fields) |f| {
            selected[i * group.fields + f] = std.math.cast(i32, (@as(u64, slot) * instances + i) * max_fields + f) orelse return error.BoundaryTrainingGraphLimitExceeded;
        };
        out.* = .{ .instances = instances, .instance_states = try takeRows(g, flat_states, try mul(slot, instances), instances, h), .object_logits = try g.reshape(try takeRows(g, flat_objects, try mul(slot, instances), instances, 1), &.{instances}), .assignment_logits = try g.reshape(try g.gather(assignments, try constantIndices(g, selected), count, columns), &.{ instances, group.fields, columns }) };
    }
    try g.check();
    return outputs;
}
