//! GLiNER2.5 encoder graph, typed routing, and explicit dropout replay inputs.
//!
//! The materialized reference retains quadratic attention masks. The explicit
//! replay profile uses bounded tiles and compact integer controls instead.
//! Admission is a caller-selected resource
//! policy, not an architectural sequence-length limit. A training executor
//! must additionally admit its backward graph, optimizer, and kernel scratch.
//! No inference backend, global RNG, or environment setting selects this path.
const std = @import("std");
const ml = @import("ml");
const boundary = @import("../models/gliner_boundary.zig");
const deberta = @import("../architectures/deberta_graph.zig");
const engine = @import("../architectures/gliner_boundary_engine.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const Allocator = std.mem.Allocator;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const null_node = ml.graph.null_node;

pub const Mode = enum { eval, train };
/// Arithmetic and replay semantics are separate from resource ceilings.
/// Checkpoint owners must bind this profile into their durable run identity.
pub const AttentionProfile = enum { materialized_v1, replay_tiled_v1 };
pub const ActivationProfile = enum { retained_v1, layer_recompute_v1 };
pub const Layout = struct {
    batch: u32,
    sequence: u32,
    words: u32,
    queries: u32,
    classifications: u32,
    groups: u32,
    relations: u32,
};
pub const Limits = struct {
    /// Includes independent body-word, total-word, and encoded-token limits.
    input: engine.Limits = .{},
    max_attention_score_elements: u64 = 64 * 1024 * 1024,
    max_dropout_mask_bytes: u64 = 512 * 1024 * 1024,
    max_forward_tensor_bytes: u64 = 4 * 1024 * 1024 * 1024,
    max_constant_bytes: u64 = 512 * 1024 * 1024,
};
pub const Plan = struct {
    attention_score_elements: u64,
    /// Sum across all encoder layers; score elements, not FLOPs.
    attention_work_items: u64,
    dropout_mask_bytes: u64,
    binding_bytes: u64,
    constant_bytes: u64,
    parameter_bytes: u64,
    /// Conservative sum of forward intermediate sizes, before graph lowering.
    /// Excludes backend workspace, reverse graph, gradients, and optimizer.
    forward_tensor_upper_bound_bytes: u64,
};

/// All nonempty states are flattened row-major F32 matrices. Empty route
/// families use null_node; no zero-sized backend tensors need to be created.
pub const RoutedNodes = struct {
    encoder: NodeId, // [B*S,H]
    text: NodeId, // [B*W,H]
    queries: NodeId, // [B*Q,H]
    classifications: NodeId, // [B*Cls,H]
    parents: NodeId, // [B*G,H]
    relation_queries: NodeId, // [B*R,2H], or [B*R,H] for nondirectional config
};
pub const RouteKind = enum { text, queries, classifications, parents, relation_head, relation_tail };
pub const RouteInputs = struct { indices: NodeId = null_node, valid: NodeId = null_node, width: u32 };
pub const Inputs = struct {
    input_ids: NodeId, // [B*S] i32, including multilingual vocabulary IDs
    embedding_valid: NodeId, // [B*S,1] f32
    attention_valid: NodeId, // [B*heads,S,S] f32
    attention_bias: NodeId, // same shape, zero or -max(f32)
    attention_control: NodeId = null_node, // compact physical i32 replay profile
    routes: [6]RouteInputs,
};
pub const DropoutDescriptor = struct {
    node: NodeId,
    site: deberta.DropoutSite,
    shape: Shape,
    probability: f32,

    pub fn streamId(self: DropoutDescriptor) u64 {
        return (@as(u64, self.site.layer) << 32) | (@as(u64, @intFromEnum(self.site.kind)) + 1);
    }
};
/// A stable counter tuple. Reusing it reproduces every dropout bit, including
/// during activation recomputation. This RNG is not PyTorch's RNG; parity
/// fixtures supply the same masks to both runtimes.
pub const Replay = struct { seed: u64, micro_batch: u64, replica: u64 = 0 };

pub const LayerRegion = struct { ordinal: u32, input: NodeId, output: NodeId };
pub const EncoderRegions = struct {
    embedding_output: NodeId,
    normalized_relative: NodeId,
    layers: []LayerRegion,
    output: NodeId,

    fn remap(self: *EncoderRegions, ids: []const NodeId) !void {
        try remapOne(&self.embedding_output, ids);
        try remapOne(&self.normalized_relative, ids);
        try remapOne(&self.output, ids);
        for (self.layers) |*layer| {
            try remapOne(&layer.input, ids);
            try remapOne(&layer.output, ids);
        }
    }
};

pub const Built = struct {
    allocator: Allocator,
    layout: Layout,
    encoder_config: boundary.EncoderConfig,
    directional_relations: bool,
    mode: Mode,
    attention_profile: AttentionProfile = .materialized_v1,
    activation_profile: ActivationProfile = .retained_v1,
    limits: Limits,
    admission: Plan,
    inputs: Inputs,
    nodes: RoutedNodes,
    dropouts: []DropoutDescriptor,
    regions: EncoderRegions,

    pub fn deinit(self: *Built) void {
        self.allocator.free(self.dropouts);
        self.allocator.free(self.regions.layers);
        self.* = undefined;
    }

    /// Call after graph sorting/lowering if a trainer retains these NodeIds.
    pub fn remap(self: *Built, ids: []const NodeId) !void {
        inline for (std.meta.fields(RoutedNodes)) |field| try remapOne(&@field(self.nodes, field.name), ids);
        try remapOne(&self.inputs.input_ids, ids);
        try remapOne(&self.inputs.embedding_valid, ids);
        try remapOne(&self.inputs.attention_valid, ids);
        try remapOne(&self.inputs.attention_bias, ids);
        try remapOne(&self.inputs.attention_control, ids);
        for (&self.inputs.routes) |*route| {
            try remapOne(&route.indices, ids);
            try remapOne(&route.valid, ids);
        }
        for (self.dropouts) |*dropout| try remapOne(&dropout.node, ids);
        try self.regions.remap(ids);
    }
};

fn remapOne(id: *NodeId, ids: []const NodeId) !void {
    if (id.* == null_node) return;
    if (id.* >= ids.len or ids[id.*] == null_node) return error.InvalidBoundaryGraphRemap;
    id.* = ids[id.*];
}
fn mul(a: u64, b: u64) !u64 {
    return std.math.mul(u64, a, b) catch error.ResourceLimitExceeded;
}
fn add(a: u64, b: u64) !u64 {
    return std.math.add(u64, a, b) catch error.ResourceLimitExceeded;
}
fn rows(batch: u32, width: u32) !u32 {
    const value = try mul(batch, width);
    if (value > std.math.maxInt(i32)) return error.ResourceLimitExceeded;
    return @intCast(value);
}

pub fn plan(config: *const boundary.Config, layout: Layout, mode: Mode, limits: Limits) !Plan {
    return planWithProfile(config, layout, mode, .materialized_v1, limits);
}

pub fn planWithProfile(config: *const boundary.Config, layout: Layout, mode: Mode, profile: AttentionProfile, limits: Limits) !Plan {
    return planWithProfiles(config, layout, mode, profile, .retained_v1, limits);
}

pub fn planWithProfiles(config: *const boundary.Config, layout: Layout, mode: Mode, profile: AttentionProfile, activation: ActivationProfile, limits: Limits) !Plan {
    const e = config.encoder;
    if (config.version != boundary.config_version or config.architecture_version != boundary.architecture_version)
        return error.UnsupportedGlinerBoundaryVersion;
    if (layout.batch == 0 or layout.sequence == 0 or e.hidden_size == 0 or e.num_hidden_layers == 0 or
        e.num_attention_heads == 0 or e.hidden_size % e.num_attention_heads != 0 or e.intermediate_size == 0 or
        e.vocab_size == 0 or e.max_position_embeddings == 0 or e.position_buckets == 0 or
        e.position_buckets > e.max_position_embeddings or !std.math.isFinite(e.layer_norm_eps) or e.layer_norm_eps <= 0 or
        !std.math.isFinite(e.hidden_dropout_prob) or e.hidden_dropout_prob < 0 or e.hidden_dropout_prob >= 1 or
        !std.math.isFinite(e.attention_probs_dropout_prob) or e.attention_probs_dropout_prob < 0 or e.attention_probs_dropout_prob >= 1)
        return error.InvalidGlinerBoundaryConfig;
    if (layout.batch > limits.input.max_batch or layout.sequence > limits.input.max_sequence_tokens or
        layout.words > limits.input.max_text_words or layout.queries > limits.input.max_queries or
        layout.classifications > limits.input.max_classification_labels or layout.groups > limits.input.max_groups or
        layout.relations > limits.input.max_relations)
        return error.ResourceLimitExceeded;
    const bs = try rows(layout.batch, layout.sequence);
    const bh = try rows(layout.batch, e.num_attention_heads);
    const rel = try std.math.sub(u64, try mul(layout.sequence, 2), 1);
    const ss = try mul(layout.sequence, layout.sequence);
    const scores = try mul(bh, ss);
    // Flattened relative-score gather uses physical i32 offsets.
    if ((profile == .materialized_v1 and try mul(try mul(bh, layout.sequence), rel) > std.math.maxInt(i32)) or
        try mul(bs, e.hidden_size) > std.math.maxInt(i32) or try mul(bs, e.intermediate_size) > std.math.maxInt(i32) or
        try mul(e.max_position_embeddings, e.hidden_size) > std.math.maxInt(i32) or
        try mul(e.vocab_size, e.hidden_size) > std.math.maxInt(i32)) return error.ResourceLimitExceeded;
    if (profile == .materialized_v1 and scores > limits.max_attention_score_elements) return error.ResourceLimitExceeded;
    const sweeps: u64 = if (profile == .replay_tiled_v1 and mode == .train) if (activation == .layer_recompute_v1) 4 else 3 else 1;
    const attention_work = try mul(try mul(scores, e.num_hidden_layers), sweeps);
    if (profile == .replay_tiled_v1) {
        if (attention_work > limits.input.max_attention_work_items) return error.ResourceLimitExceeded;
        _ = try @import("../ops/deberta_training_attention.zig").plan(.{
            .batch = layout.batch,
            .seq_len = layout.sequence,
            .num_heads = e.num_attention_heads,
            .head_dim = e.hidden_size / e.num_attention_heads,
            .relative_rows = e.max_position_embeddings,
            .dropout_probability = if (mode == .train) e.attention_probs_dropout_prob else 0,
            .dropout_stream_id = 0,
        }, .{
            .max_scratch_bytes = limits.input.max_attention_scratch_bytes,
            .max_work_items = limits.input.max_attention_work_items,
        });
    }
    const hidden = try mul(bs, e.hidden_size);
    const relative = try mul(e.max_position_embeddings, e.hidden_size);
    var dropout_elements: u64 = 0;
    if (mode == .train) {
        if (e.hidden_dropout_prob > 0)
            dropout_elements = try add(hidden, try mul(e.num_hidden_layers, try add(relative, try mul(hidden, 2))));
        if (e.attention_probs_dropout_prob > 0 and profile == .materialized_v1)
            dropout_elements = try add(dropout_elements, try mul(e.num_hidden_layers, scores));
    }
    const dropout_bytes = try mul(dropout_elements, @sizeOf(f32));
    const live_dropout_bytes = if (activation == .retained_v1) dropout_bytes else dropout: {
        const per_layer = try add(if (e.hidden_dropout_prob > 0) try add(relative, try mul(hidden, 2)) else 0, if (e.attention_probs_dropout_prob > 0 and profile == .materialized_v1) scores else 0);
        break :dropout if (mode == .train) try mul(@max(if (e.hidden_dropout_prob > 0) hidden else 0, per_layer), @sizeOf(f32)) else 0;
    };
    if (live_dropout_bytes > limits.max_dropout_mask_bytes) return error.ResourceLimitExceeded;
    var routed_rows: u64 = 0;
    for ([_]u32{ layout.words, layout.queries, layout.classifications, layout.groups, layout.relations, layout.relations }) |width|
        routed_rows = try add(routed_rows, try rows(layout.batch, width));
    const attention_bindings = if (profile == .materialized_v1) try mul(scores, 2) else try add(6, try add(bs, rel));
    const binding_bytes = try add(live_dropout_bytes, try mul(try add(attention_bindings, try mul(try add(bs, routed_rows), 2)), 4));
    const constant_bytes = if (profile == .materialized_v1) try add(try mul(scores, 8), try mul(rel, 8)) else try add(4096, try mul(e.num_hidden_layers, 256));
    if (constant_bytes > limits.max_constant_bytes) return error.ResourceLimitExceeded;
    const score_intermediates = if (profile == .materialized_v1) try mul(scores, 64) else 0;
    const per_layer = try add(try add(score_intermediates, try mul(hidden, 64)), try add(try mul(try mul(bs, e.intermediate_size), 16), try mul(relative, 16)));
    const checkpoints = if (activation == .layer_recompute_v1) try mul(try add(try mul(try add(e.num_hidden_layers, 1), hidden), relative), 4) else 0;
    const forward_bytes = try add(checkpoints, try add(try add(binding_bytes, constant_bytes), try mul(try add(try mul(if (activation == .retained_v1) e.num_hidden_layers else 1, per_layer), try mul(try mul(routed_rows, e.hidden_size), 4)), 4)));
    if (forward_bytes > limits.max_forward_tensor_bytes) return error.ResourceLimitExceeded;
    const layer_parameters = try add(try add(try mul(try mul(e.hidden_size, e.hidden_size), 4), try mul(try mul(e.hidden_size, e.intermediate_size), 2)), try add(try mul(e.hidden_size, 9), e.intermediate_size));
    const parameters = try add(try add(try mul(e.vocab_size, e.hidden_size), relative), try add(try mul(e.hidden_size, 4), try mul(e.num_hidden_layers, layer_parameters)));
    return .{
        .attention_score_elements = scores,
        .attention_work_items = attention_work,
        .dropout_mask_bytes = dropout_bytes,
        .binding_bytes = binding_bytes,
        .constant_bytes = constant_bytes,
        .parameter_bytes = try mul(parameters, 4),
        .forward_tensor_upper_bound_bytes = forward_bytes,
    };
}

pub fn layoutFromPrepared(config: *const boundary.Config, prepared: *const processor.PreparedBatch, limits: Limits) !Layout {
    const checked = try engine.plan(config, prepared, .{ .limits = limits.input });
    return .{
        .batch = std.math.cast(u32, prepared.samples.len) orelse return error.ResourceLimitExceeded,
        .sequence = std.math.cast(u32, prepared.sequence_length) orelse return error.ResourceLimitExceeded,
        .words = std.math.cast(u32, prepared.word_width) orelse return error.ResourceLimitExceeded,
        .queries = std.math.cast(u32, prepared.query_width) orelse return error.ResourceLimitExceeded,
        .classifications = std.math.cast(u32, prepared.classification_width) orelse return error.ResourceLimitExceeded,
        .groups = std.math.cast(u32, prepared.group_width) orelse return error.ResourceLimitExceeded,
        .relations = std.math.cast(u32, checked.relation_width) orelse return error.ResourceLimitExceeded,
    };
}

const DropoutBuilder = struct {
    config: boundary.EncoderConfig,
    descriptors: std.ArrayListUnmanaged(DropoutDescriptor) = .empty,

    fn apply(raw: *anyopaque, bld: *Builder, input: NodeId, site: deberta.DropoutSite) !NodeId {
        const self: *DropoutBuilder = @ptrCast(@alignCast(raw));
        const probability = if (site.kind == .attention_probabilities) self.config.attention_probs_dropout_prob else self.config.hidden_dropout_prob;
        if (probability == 0) return input;
        var name_buffer: [128]u8 = undefined;
        const name = try std.fmt.bufPrint(&name_buffer, "__gliner25.encoder.dropout.{s}.{d}", .{ @tagName(site.kind), site.layer });
        const shape = bld.graph.node(input).output_shape;
        const node = try bld.parameter(name, shape);
        try self.descriptors.append(bld.graph.allocator, .{ .node = node, .site = site, .shape = shape, .probability = probability });
        return bld.mul(input, node);
    }
};

const RegionBuilder = struct {
    embedding_output: NodeId = null_node,
    normalized_relative: NodeId = null_node,
    layers: []LayerRegion,
    count: usize = 0,

    fn prelude(raw: *anyopaque, embedding_output: NodeId, normalized_relative: NodeId) !void {
        const self: *RegionBuilder = @ptrCast(@alignCast(raw));
        if (self.embedding_output != null_node or embedding_output == null_node or normalized_relative == null_node)
            return error.InvalidBoundaryEncoderRegion;
        self.embedding_output = embedding_output;
        self.normalized_relative = normalized_relative;
    }

    fn layer(raw: *anyopaque, ordinal: u32, input: NodeId, output: NodeId) !void {
        const self: *RegionBuilder = @ptrCast(@alignCast(raw));
        if (ordinal != self.count or self.count >= self.layers.len or output == null_node or
            input != (if (self.count == 0) self.embedding_output else self.layers[self.count - 1].output))
            return error.InvalidBoundaryEncoderRegion;
        self.layers[self.count] = .{ .ordinal = ordinal, .input = input, .output = output };
        self.count += 1;
    }
};

pub fn build(bld: *Builder, config: *const boundary.Config, layout: Layout, mode: Mode, limits: Limits) !Built {
    return buildWithProfile(bld, config, layout, mode, .materialized_v1, limits);
}

pub fn buildWithProfile(bld: *Builder, config: *const boundary.Config, layout: Layout, mode: Mode, profile: AttentionProfile, limits: Limits) !Built {
    return buildWithProfiles(bld, config, layout, mode, profile, .retained_v1, limits);
}

pub fn buildWithProfiles(bld: *Builder, config: *const boundary.Config, layout: Layout, mode: Mode, profile: AttentionProfile, activation: ActivationProfile, limits: Limits) !Built {
    const admission = try planWithProfiles(config, layout, mode, profile, activation, limits);
    const e = config.encoder;
    const bs = try rows(layout.batch, layout.sequence);
    const bh = try rows(layout.batch, e.num_attention_heads);
    const score_shape = Shape.init(.f32, &.{ @intCast(bh), @intCast(layout.sequence), @intCast(layout.sequence) });
    var inputs = Inputs{
        .input_ids = try bld.parameter("__gliner25.encoder.input_ids", Shape.init(.i32, &.{@intCast(bs)})),
        .embedding_valid = try bld.parameter("__gliner25.encoder.embedding_valid", Shape.init(.f32, &.{ @intCast(bs), 1 })),
        .attention_valid = if (profile == .materialized_v1) try bld.parameter("__gliner25.encoder.attention_valid", score_shape) else null_node,
        .attention_bias = if (profile == .materialized_v1) try bld.parameter("__gliner25.encoder.attention_bias", score_shape) else null_node,
        .attention_control = if (profile == .replay_tiled_v1) try bld.parameter("__gliner25.encoder.attention_control_v1", Shape.init(.i32, &.{@intCast(6 + @as(u64, bs) + @as(u64, layout.sequence) * 2 - 1)})) else null_node,
        .routes = undefined,
    };
    const widths = [_]u32{ layout.words, layout.queries, layout.classifications, layout.groups, layout.relations, layout.relations };
    for (&inputs.routes, widths, 0..) |*route, width, index| {
        route.* = .{ .width = width };
        if (width == 0) continue;
        var name_buffer: [128]u8 = undefined;
        const kind: RouteKind = @enumFromInt(index);
        const count = try rows(layout.batch, width);
        route.indices = try bld.parameter(try std.fmt.bufPrint(&name_buffer, "__gliner25.encoder.route.{s}.indices", .{@tagName(kind)}), Shape.init(.i32, &.{@intCast(count)}));
        route.valid = try bld.parameter(try std.fmt.bufPrint(&name_buffer, "__gliner25.encoder.route.{s}.valid", .{@tagName(kind)}), Shape.init(.f32, &.{ @intCast(count), 1 }));
    }
    var dropout = DropoutBuilder{ .config = e };
    defer dropout.descriptors.deinit(bld.graph.allocator);
    const layers = try bld.graph.allocator.alloc(LayerRegion, e.num_hidden_layers);
    errdefer bld.graph.allocator.free(layers);
    var regions = RegionBuilder{ .layers = layers };
    const encoder = try deberta.buildForwardGraphWithOptions(bld, .{
        .vocab_size = e.vocab_size,
        .hidden_size = e.hidden_size,
        .num_hidden_layers = e.num_hidden_layers,
        .num_attention_heads = e.num_attention_heads,
        .intermediate_size = e.intermediate_size,
        .max_position_embeddings = e.max_position_embeddings,
        .position_buckets = e.position_buckets,
        .layer_norm_eps = e.layer_norm_eps,
    }, inputs.input_ids, inputs.attention_bias, inputs.embedding_valid, layout.batch, layout.sequence, .{
        .attention = if (profile == .materialized_v1) .materialized else .training_replay_v1,
        .attention_score_mask = if (profile == .materialized_v1) inputs.attention_valid else null,
        .training_attention_v1 = if (profile == .replay_tiled_v1) .{ .control = inputs.attention_control, .probability = if (mode == .train) e.attention_probs_dropout_prob else 0 } else null,
        .project_relative_before_gather = true,
        .dropout = if (mode == .train) .{ .context = &dropout, .apply = DropoutBuilder.apply } else null,
        .regions = .{ .context = &regions, .prelude = RegionBuilder.prelude, .layer = RegionBuilder.layer },
    });
    if (regions.count != layers.len or regions.normalized_relative == null_node or
        layers[layers.len - 1].output != encoder.output_node) return error.InvalidBoundaryEncoderRegion;
    var routed: [6]NodeId = .{null_node} ** 6;
    for (inputs.routes, 0..) |route, index| {
        if (route.width == 0) continue;
        const gathered = try bld.gather(encoder.output_node, route.indices, Shape.init(.f32, &.{ try rows(layout.batch, route.width), e.hidden_size }));
        routed[index] = try bld.mul(gathered, route.valid);
    }
    const relations = if (layout.relations == 0) null_node else if (config.head.directional_relation_states)
        try bld.concat(routed[4], routed[5], 1)
    else
        try bld.mul(try bld.add(routed[4], routed[5]), try bld.scalarConst(.f32, 0.5));
    return .{
        .allocator = bld.graph.allocator,
        .layout = layout,
        .encoder_config = e,
        .directional_relations = config.head.directional_relation_states,
        .mode = mode,
        .attention_profile = profile,
        .activation_profile = activation,
        .limits = limits,
        .admission = admission,
        .inputs = inputs,
        .nodes = .{ .encoder = encoder.output_node, .text = routed[0], .queries = routed[1], .classifications = routed[2], .parents = routed[3], .relation_queries = relations },
        .dropouts = try dropout.descriptors.toOwnedSlice(bld.graph.allocator),
        .regions = .{ .embedding_output = regions.embedding_output, .normalized_relative = regions.normalized_relative, .layers = layers, .output = encoder.output_node },
    };
}

pub const Values = union(enum) { f32: []const f32, i32: []const i32 };
pub const Binding = struct { node: NodeId, shape: Shape, values: Values };
pub const BoundInputs = struct {
    allocator: Allocator,
    arena: *std.heap.ArenaAllocator,
    bindings: []Binding,

    pub fn deinit(self: *BoundInputs) void {
        self.arena.deinit();
        self.allocator.destroy(self.arena);
        self.* = undefined;
    }
};

fn mix(value: u64) u64 {
    var x = value +% 0x9e3779b97f4a7c15;
    x = (x ^ (x >> 30)) *% 0xbf58476d1ce4e5b9;
    x = (x ^ (x >> 27)) *% 0x94d049bb133111eb;
    return x ^ (x >> 31);
}

pub fn fillDropout(descriptor: DropoutDescriptor, replay: Replay, output: []f32) !void {
    const count = std.math.cast(usize, descriptor.shape.numElements() orelse return error.InvalidBoundaryDropout) orelse return error.InvalidBoundaryDropout;
    if (!std.math.isFinite(descriptor.probability) or descriptor.probability < 0 or descriptor.probability >= 1 or
        output.len != count) return error.InvalidBoundaryDropout;
    const scale = 1.0 / (1.0 - descriptor.probability);
    const stream = mix(replay.seed) ^ mix(replay.micro_batch) ^ mix(replay.replica +% 0x7265706c696361) ^ mix(descriptor.streamId());
    // A 32-bit uniform grid avoids platform-dependent float RNG conversions.
    const threshold: u64 = @intFromFloat(@as(f64, descriptor.probability) * 4294967296.0);
    for (output, 0..) |*value, index| value.* = if ((mix(stream ^ mix(index)) >> 32) < threshold) 0 else scale;
}

fn bindRoute(a: Allocator, list: *std.ArrayListUnmanaged(Binding), node: RouteInputs, batch: u32, sequence: u32, indices: []const i64, mask: []const bool) !void {
    if (node.width == 0) return;
    const count = try rows(batch, node.width);
    if (indices.len != count or mask.len != count) return error.InvalidBoundaryRouting;
    const absolute = try a.alloc(i32, count);
    const valid = try a.alloc(f32, count);
    for (indices, mask, 0..) |index, present, row| {
        valid[row] = if (present) 1 else 0;
        absolute[row] = 0;
        if (!present) continue;
        if (index < 0 or index >= sequence) return error.InvalidBoundaryRouting;
        absolute[row] = std.math.cast(i32, (row / node.width) * sequence + @as(usize, @intCast(index))) orelse return error.ResourceLimitExceeded;
    }
    try list.append(a, .{ .node = node.indices, .shape = Shape.init(.i32, &.{@intCast(count)}), .values = .{ .i32 = absolute } });
    try list.append(a, .{ .node = node.valid, .shape = Shape.init(.f32, &.{ @intCast(count), 1 }), .values = .{ .f32 = valid } });
}

/// Produces owned host inputs for a strict typed executor. It never converts
/// indices to float or uploads via the legacy trainer's lossy binding helpers.
/// For reference tests callers may replace dropout binding values with masks
/// captured from the oracle, while retaining descriptor identity and shape.
pub fn bindPrepared(allocator: Allocator, built: *const Built, config: *const boundary.Config, prepared: *const processor.PreparedBatch, replay: Replay) !BoundInputs {
    return bindPreparedInternal(allocator, built, config, prepared, replay, true);
}

/// Regional execution generates each site's dropout mask only while that
/// region is live. Token IDs, routes and compact replay controls remain fixed
/// across the complete step. This avoids allocating all layer masks eagerly.
pub fn bindFixedPrepared(allocator: Allocator, built: *const Built, config: *const boundary.Config, prepared: *const processor.PreparedBatch, replay: Replay) !BoundInputs {
    return bindPreparedInternal(allocator, built, config, prepared, replay, false);
}

fn bindPreparedInternal(allocator: Allocator, built: *const Built, config: *const boundary.Config, prepared: *const processor.PreparedBatch, replay: Replay, include_dropout: bool) !BoundInputs {
    if (include_dropout and built.activation_profile == .layer_recompute_v1) return error.UnsupportedBoundaryEagerDropoutBinding;
    const layout = try layoutFromPrepared(config, prepared, built.limits);
    if (!std.meta.eql(layout, built.layout) or !std.meta.eql(config.encoder, built.encoder_config) or
        config.head.directional_relation_states != built.directional_relations) return error.BoundaryGraphShapeMismatch;
    _ = try planWithProfiles(config, layout, built.mode, built.attention_profile, built.activation_profile, built.limits);
    const arena = try allocator.create(std.heap.ArenaAllocator);
    errdefer allocator.destroy(arena);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const a = arena.allocator();
    var list: std.ArrayListUnmanaged(Binding) = .empty;
    const bs = try rows(layout.batch, layout.sequence);
    const sequence = layout.sequence;
    const ids = try a.alloc(i32, bs);
    const embedding_valid = try a.alloc(f32, bs);
    for (prepared.input_ids, prepared.attention_mask, 0..) |id, valid, index| {
        ids[index] = std.math.cast(i32, id) orelse return error.InvalidBoundaryRouting;
        embedding_valid[index] = if (valid != 0) 1 else 0;
    }
    try list.append(a, .{ .node = built.inputs.input_ids, .shape = Shape.init(.i32, &.{@intCast(bs)}), .values = .{ .i32 = ids } });
    try list.append(a, .{ .node = built.inputs.embedding_valid, .shape = Shape.init(.f32, &.{ @intCast(bs), 1 }), .values = .{ .f32 = embedding_valid } });
    if (built.attention_profile == .replay_tiled_v1) {
        const count = 6 + @as(usize, bs) + @as(usize, layout.sequence) * 2 - 1;
        const control = try a.alloc(i32, count);
        for ([_]u64{ replay.seed, replay.micro_batch, replay.replica }, 0..) |value, i| {
            control[2 * i] = @bitCast(@as(u32, @truncate(value)));
            control[2 * i + 1] = @bitCast(@as(u32, @truncate(value >> 32)));
        }
        for (prepared.attention_mask, control[6..][0..bs]) |valid, *value| value.* = if (valid != 0) 1 else 0;
        for (control[6 + bs ..], 0..) |*value, i| {
            const relative_position = @as(i64, @intCast(i)) - @as(i64, layout.sequence - 1);
            value.* = @intCast(@import("../models/deberta.zig").relativePositionBucket(relative_position, config.encoder.position_buckets, config.encoder.max_position_embeddings));
        }
        try list.append(a, .{ .node = built.inputs.attention_control, .shape = Shape.init(.i32, &.{@intCast(count)}), .values = .{ .i32 = control } });
    } else {
        const scores: usize = @intCast(built.admission.attention_score_elements);
        const attention_valid = try a.alloc(f32, scores);
        const attention_bias = try a.alloc(f32, scores);
        const heads = config.encoder.num_attention_heads;
        for (0..layout.batch) |batch| for (0..heads) |head| for (0..sequence) |query| for (0..sequence) |key| {
            const index = ((batch * heads + head) * sequence + query) * sequence + key;
            const valid = prepared.attention_mask[batch * sequence + query] != 0 and prepared.attention_mask[batch * sequence + key] != 0;
            attention_valid[index] = if (valid) 1 else 0;
            attention_bias[index] = if (valid) 0 else -std.math.floatMax(f32);
        };
        const score_shape = Shape.init(.f32, &.{ @intCast(layout.batch * heads), @intCast(sequence), @intCast(sequence) });
        try list.append(a, .{ .node = built.inputs.attention_valid, .shape = score_shape, .values = .{ .f32 = attention_valid } });
        try list.append(a, .{ .node = built.inputs.attention_bias, .shape = score_shape, .values = .{ .f32 = attention_bias } });
    }
    try bindRoute(a, &list, built.inputs.routes[0], layout.batch, sequence, prepared.text_word_indices, prepared.text_word_mask);
    try bindRoute(a, &list, built.inputs.routes[1], layout.batch, sequence, prepared.query_marker_indices, prepared.query_marker_mask);
    try bindRoute(a, &list, built.inputs.routes[2], layout.batch, sequence, prepared.cls_marker_indices, prepared.cls_marker_mask);
    try bindRoute(a, &list, built.inputs.routes[3], layout.batch, sequence, prepared.parent_marker_indices, prepared.parent_marker_mask);
    if (layout.relations != 0) {
        const count = try rows(layout.batch, layout.relations);
        const head_indices = try a.alloc(i64, count);
        const tail_indices = try a.alloc(i64, count);
        const valid = try a.alloc(bool, count);
        @memset(head_indices, 0);
        @memset(tail_indices, 0);
        @memset(valid, false);
        for (prepared.samples, 0..) |sample, batch| {
            var relation: usize = 0;
            for (sample.groups, 0..) |group, group_index| {
                if (group.kind != .relation) continue;
                const index = batch * layout.relations + relation;
                relation += 1;
                valid[index] = true;
                for (sample.queries) |query| {
                    if (query.group_index != group_index) continue;
                    switch (query.kind) {
                        .relation_head => head_indices[index] = @intCast(query.marker_index),
                        .relation_tail => tail_indices[index] = @intCast(query.marker_index),
                        else => return error.InvalidBoundaryRouting,
                    }
                }
            }
        }
        try bindRoute(a, &list, built.inputs.routes[4], layout.batch, sequence, head_indices, valid);
        try bindRoute(a, &list, built.inputs.routes[5], layout.batch, sequence, tail_indices, valid);
    }
    if (include_dropout) for (built.dropouts) |descriptor| {
        const count = std.math.cast(usize, descriptor.shape.numElements() orelse return error.InvalidBoundaryDropout) orelse return error.InvalidBoundaryDropout;
        const values = try a.alloc(f32, count);
        try fillDropout(descriptor, replay, values);
        try list.append(a, .{ .node = descriptor.node, .shape = descriptor.shape, .values = .{ .f32 = values } });
    };
    return .{ .allocator = allocator, .arena = arena, .bindings = try list.toOwnedSlice(a) };
}

fn testConfig() boundary.Config {
    var config = engine.TestBatch.config();
    config.encoder.hidden_size = 8;
    config.encoder.intermediate_size = 16;
    config.encoder.num_attention_heads = 2;
    config.encoder.num_hidden_layers = 2;
    config.encoder.max_position_embeddings = 32;
    config.encoder.position_buckets = 16;
    config.encoder.hidden_dropout_prob = 0.1;
    config.encoder.attention_probs_dropout_prob = 0.1;
    return config;
}

fn findBinding(values: *const BoundInputs, id: NodeId) !Binding {
    for (values.bindings) |binding| if (binding.node == id) return binding;
    return error.MissingTestBinding;
}

test "GLiNER2.5 encoder training graph has typed routes and every replayable dropout site" {
    var fixture = engine.TestBatch{};
    var prepared = fixture.prepared();
    defer prepared.arena.deinit();
    const config = testConfig();
    const layout = try layoutFromPrepared(&config, &prepared, .{});
    var graph = ml.graph.Graph.init(std.testing.allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);
    var built = try build(&bld, &config, layout, .train, .{});
    defer built.deinit();
    try std.testing.expectEqual(@as(usize, 9), built.dropouts.len);
    try std.testing.expectEqual(ml.graph.shape.DType.i32, graph.node(built.inputs.input_ids).output_shape.dtype);
    var relative_sites: usize = 0;
    var attention_sites: usize = 0;
    var qk_table_uses: usize = 0;
    for (built.dropouts) |descriptor| switch (descriptor.site.kind) {
        .relative_positions => {
            relative_sites += 1;
            try std.testing.expectEqual(@as(i64, 32), descriptor.shape.dim(0));
            try std.testing.expectEqual(@as(i64, 8), descriptor.shape.dim(1));
        },
        .attention_probabilities => {
            attention_sites += 1;
            try std.testing.expectEqual(@as(i64, 4), descriptor.shape.dim(0));
            try std.testing.expectEqual(@as(i64, 12), descriptor.shape.dim(1));
        },
        else => {},
    };
    for (graph.nodes.items) |node| {
        try std.testing.expect(node.op != .fused_disentangled_attention);
        if (node.op == .fused_linear and node.op.fused_linear.rows == 32) qk_table_uses += 1;
        if (node.op == .gather) {
            const dtype = graph.node(node.inputs[1]).output_shape.dtype;
            try std.testing.expectEqual(.i32, dtype);
        }
    }
    try std.testing.expectEqual(@as(usize, 2), relative_sites);
    try std.testing.expectEqual(@as(usize, 2), attention_sites);
    try std.testing.expectEqual(@as(usize, 4), qk_table_uses);
    const seed = try bld.parameter("__encoder_test_seed", graph.node(built.nodes.encoder).output_shape);
    const embedding = for (graph.parameters.items) |id| {
        if (std.mem.eql(u8, graph.parameterName(graph.node(id)), "embeddings.word_embeddings.weight")) break id;
    } else return error.MissingTestWeight;
    var gradient = try ml.graph.autodiff.gradientWithSeeds(std.testing.allocator, &graph, &.{.{ .output = built.nodes.encoder, .cotangent = seed }}, &.{embedding}, .{ .require_all_gradients = true });
    defer gradient.deinit();
}

test "GLiNER2.5 encoder training bindings preserve padding and repeated route identities" {
    var fixture = engine.TestBatch{};
    var prepared = fixture.prepared();
    defer prepared.arena.deinit();
    const config = testConfig();
    const layout = try layoutFromPrepared(&config, &prepared, .{});
    var graph = ml.graph.Graph.init(std.testing.allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);
    var built = try build(&bld, &config, layout, .train, .{});
    defer built.deinit();
    var values = try bindPrepared(std.testing.allocator, &built, &config, &prepared, .{ .seed = 2509, .micro_batch = 3 });
    defer values.deinit();
    const words = try findBinding(&values, built.inputs.routes[0].indices);
    try std.testing.expectEqualSlices(i32, &.{ 10, 11, 18, 0 }, words.values.i32);
    const queries = try findBinding(&values, built.inputs.routes[1].indices);
    try std.testing.expectEqualSlices(i32, &.{ 1, 3, 4, 13, 0, 0 }, queries.values.i32);
    const relation_heads = try findBinding(&values, built.inputs.routes[4].indices);
    try std.testing.expectEqualSlices(i32, &.{ 3, 0 }, relation_heads.values.i32);
    const relation_tails = try findBinding(&values, built.inputs.routes[5].indices);
    try std.testing.expectEqualSlices(i32, &.{ 4, 0 }, relation_tails.values.i32);
    const attention = (try findBinding(&values, built.inputs.attention_valid)).values.f32;
    // Second sample is padded after token6. Invalid query AND key pairs have
    // zero derivatives; active pairs retain the score computation.
    try std.testing.expectEqual(@as(f32, 1), attention[(2 * 12 + 6) * 12 + 6]);
    try std.testing.expectEqual(@as(f32, 0), attention[(2 * 12 + 7) * 12 + 6]);
    try std.testing.expectEqual(@as(f32, 0), attention[(2 * 12 + 6) * 12 + 7]);
    for (built.dropouts) |descriptor| {
        const first = (try findBinding(&values, descriptor.node)).values.f32;
        const replay = try std.testing.allocator.alloc(f32, first.len);
        defer std.testing.allocator.free(replay);
        try fillDropout(descriptor, .{ .seed = 2509, .micro_batch = 3 }, replay);
        try std.testing.expectEqualSlices(f32, first, replay);
        try fillDropout(descriptor, .{ .seed = 2509, .micro_batch = 4 }, replay);
        try std.testing.expect(!std.mem.eql(f32, first, replay));
    }
    // Keep metadata intact but corrupt an active route: rejection occurs before
    // ownership of any binding allocation is acquired.
    fixture.query_indices[0] = -1;
    try std.testing.expectError(error.InvalidBoundaryRouting, bindPrepared(std.testing.allocator, &built, &config, &prepared, .{ .seed = 0, .micro_batch = 0 }));
}

test "GLiNER2.5 encoder materialized training admission is resource based and allocation free" {
    const config = testConfig();
    const layout = Layout{ .batch = 1, .sequence = 513, .words = 1, .queries = 1, .classifications = 0, .groups = 1, .relations = 0 };
    const admitted = try plan(&config, layout, .train, .{});
    try std.testing.expect(admitted.attention_score_elements > 0);
    var limits = Limits{};
    limits.max_attention_score_elements = admitted.attention_score_elements - 1;
    var storage: [0]u8 = .{};
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    var graph = ml.graph.Graph.init(fixed.allocator());
    defer graph.deinit();
    var bld = Builder.init(&graph);
    try std.testing.expectError(error.ResourceLimitExceeded, build(&bld, &config, layout, .train, limits));
    try std.testing.expectEqual(@as(usize, 0), graph.nodes.items.len);
    limits = .{};
    limits.max_dropout_mask_bytes = admitted.dropout_mask_bytes - 1;
    try std.testing.expectError(error.ResourceLimitExceeded, plan(&config, layout, .train, limits));
    const eval_plan = try plan(&config, layout, .eval, .{});
    try std.testing.expectEqual(@as(u64, 0), eval_plan.dropout_mask_bytes);
    var invalid = config;
    invalid.encoder.hidden_dropout_prob = std.math.nan(f32);
    try std.testing.expectError(error.InvalidGlinerBoundaryConfig, plan(&invalid, layout, .train, .{}));
}

fn allocationFailureCase(allocator: Allocator) !void {
    var fixture = engine.TestBatch{};
    var prepared = fixture.prepared();
    defer prepared.arena.deinit();
    var config = testConfig();
    config.encoder.num_hidden_layers = 1;
    const layout = try layoutFromPrepared(&config, &prepared, .{});
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);
    var built = try build(&bld, &config, layout, .train, .{});
    defer built.deinit();
    var values = try bindPrepared(allocator, &built, &config, &prepared, .{ .seed = 1, .micro_batch = 2 });
    defer values.deinit();
}

test "GLiNER2.5 encoder training graph and bindings clean up allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationFailureCase, .{});
}

test "GLiNER2.5 replay encoder uses compact integer control full relative tables and source dropout streams" {
    const a = std.testing.allocator;
    var fixture = engine.TestBatch{};
    var prepared = fixture.prepared();
    defer prepared.arena.deinit();
    const config = testConfig();
    const layout = try layoutFromPrepared(&config, &prepared, .{});
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = Builder.init(&graph);
    var built = try buildWithProfile(&builder, &config, layout, .train, .replay_tiled_v1, .{});
    defer built.deinit();
    try std.testing.expectEqual(null_node, built.inputs.attention_valid);
    try std.testing.expectEqual(null_node, built.inputs.attention_bias);
    try std.testing.expectEqual(@as(usize, 7), built.dropouts.len);
    for (built.dropouts) |descriptor| try std.testing.expect(descriptor.site.kind != .attention_probabilities);
    var attention_count: usize = 0;
    var relative_projections: usize = 0;
    for (graph.nodes.items) |node| {
        try std.testing.expect(node.op != .fused_disentangled_attention);
        if (node.op == .fused_deberta_training_attention_v1) {
            const attrs = node.op.fused_deberta_training_attention_v1;
            try std.testing.expectEqual(config.encoder.max_position_embeddings, attrs.relative_rows);
            try std.testing.expectEqual(config.encoder.attention_probs_dropout_prob, attrs.dropout_probability);
            try std.testing.expectEqual((@as(u64, @intCast(attention_count)) << 32) | 3, attrs.dropout_stream_id);
            try std.testing.expectEqual(built.inputs.attention_control, node.inputs[2]);
            const relative_shape = graph.node(node.inputs[1]).output_shape;
            try std.testing.expect(relative_shape.eq(Shape.init(.f32, &.{ 64, 8 })));
            attention_count += 1;
        }
        if (node.op == .fused_linear and node.op.fused_linear.rows == 32) relative_projections += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), attention_count);
    try std.testing.expectEqual(@as(usize, 4), relative_projections);
    const replay = Replay{ .seed = 0xfedcba9876543210, .micro_batch = 0x80000001ffffffff, .replica = 0x12345678abcdef00 };
    var bound = try bindPrepared(a, &built, &config, &prepared, replay);
    defer bound.deinit();
    var fixed = try bindFixedPrepared(a, &built, &config, &prepared, replay);
    defer fixed.deinit();
    try std.testing.expectEqual(bound.bindings.len - built.dropouts.len, fixed.bindings.len);
    for (fixed.bindings) |actual| {
        const expected = try findBinding(&bound, actual.node);
        try std.testing.expect(expected.shape.eq(actual.shape));
        switch (actual.values) {
            .f32 => |values| try std.testing.expectEqualSlices(f32, expected.values.f32, values),
            .i32 => |values| try std.testing.expectEqualSlices(i32, expected.values.i32, values),
        }
        for (built.dropouts) |dropout| try std.testing.expect(actual.node != dropout.node);
    }
    const binding = try findBinding(&bound, built.inputs.attention_control);
    const words = binding.values.i32;
    const attrs = ml.graph.DebertaTrainingAttentionAttrs{
        .batch = layout.batch,
        .seq_len = layout.sequence,
        .num_heads = config.encoder.num_attention_heads,
        .head_dim = 4,
        .relative_rows = config.encoder.max_position_embeddings,
        .dropout_probability = config.encoder.attention_probs_dropout_prob,
        .dropout_stream_id = 3,
    };
    const decoded = try @import("../ops/deberta_training_attention.zig").validateControl(attrs, words, .{});
    try std.testing.expectEqual(replay.seed, decoded.replay.seed);
    try std.testing.expectEqual(replay.micro_batch, decoded.replay.micro_batch);
    try std.testing.expectEqual(replay.replica, decoded.replay.replica);
    for (prepared.attention_mask, decoded.token_valid) |valid, actual| try std.testing.expectEqual(@as(i32, if (valid != 0) 1 else 0), actual);
    for (decoded.buckets, 0..) |actual, i| {
        const relative_position = @as(i64, @intCast(i)) - @as(i64, layout.sequence - 1);
        try std.testing.expectEqual(@as(i32, @intCast(@import("../models/deberta.zig").relativePositionBucket(relative_position, config.encoder.position_buckets, config.encoder.max_position_embeddings))), actual);
    }
    var eval_graph = ml.graph.Graph.init(a);
    defer eval_graph.deinit();
    var eval_builder = Builder.init(&eval_graph);
    var eval_built = try buildWithProfile(&eval_builder, &config, layout, .eval, .replay_tiled_v1, .{});
    defer eval_built.deinit();
    try std.testing.expectEqual(@as(usize, 0), eval_built.dropouts.len);
    for (eval_graph.nodes.items) |node| if (node.op == .fused_deberta_training_attention_v1)
        try std.testing.expectEqual(@as(f32, 0), node.op.fused_deberta_training_attention_v1.dropout_probability);
}

test "GLiNER2.5 replay encoder regional boundaries survive shared QK LoRA and DoRA rewriting" {
    const a = std.testing.allocator;
    const peft = @import("gliner_boundary_peft_graph.zig");
    for ([_]peft.Kind{ .lora, .dora }) |kind| {
        var graph = ml.graph.Graph.init(a);
        defer graph.deinit();
        var builder = Builder.init(&graph);
        const config = testConfig();
        const layout = Layout{ .batch = 2, .sequence = 7, .words = 3, .queries = 2, .classifications = 1, .groups = 0, .relations = 0 };
        var built = try buildWithProfile(&builder, &config, layout, .train, .replay_tiled_v1, .{});
        defer built.deinit();
        const original = try a.dupe(LayerRegion, built.regions.layers);
        defer a.free(original);
        const embedding = built.regions.embedding_output;
        const relative = built.regions.normalized_relative;
        try std.testing.expectEqual(config.encoder.num_hidden_layers, original.len);
        try std.testing.expectEqual(embedding, original[0].input);
        for (original, 0..) |region, i| {
            try std.testing.expectEqual(i, region.ordinal);
            if (i != 0) try std.testing.expectEqual(original[i - 1].output, region.input);
        }
        var rewritten = try peft.inject(a, &graph, .{ .kind = kind, .rank = 2, .alpha = 3, .dropout = 0.125 }, .{});
        defer rewritten.deinit();
        try built.remap(rewritten.rewrites);
        try std.testing.expectEqual(rewritten.rewrites[embedding], built.regions.embedding_output);
        try std.testing.expectEqual(rewritten.rewrites[relative], built.regions.normalized_relative);
        try std.testing.expectEqual(built.nodes.encoder, built.regions.output);
        for (original, built.regions.layers) |before, after| {
            try std.testing.expectEqual(rewritten.rewrites[before.input], after.input);
            try std.testing.expectEqual(rewritten.rewrites[before.output], after.output);
            try std.testing.expect(rewritten.graph.node(after.input).output_shape.eq(Shape.init(.f32, &.{ 14, 8 })));
            try std.testing.expect(rewritten.graph.node(after.output).output_shape.eq(Shape.init(.f32, &.{ 14, 8 })));
        }
        try std.testing.expect(rewritten.graph.node(built.regions.normalized_relative).output_shape.eq(Shape.init(.f32, &.{ 32, 8 })));
        var shared_calls: usize = 0;
        for (rewritten.uses) |use| if (use.occurrence == 1) {
            try std.testing.expectEqual(@as(u32, 32), use.rows);
            shared_calls += 1;
        };
        try std.testing.expectEqual(@as(usize, 4), shared_calls);
    }
}

test "GLiNER2.5 replay encoder long-context graph storage is linear and work admission includes replay" {
    const a = std.testing.allocator;
    const config = testConfig();
    const layout = Layout{ .batch = 1, .sequence = 4096, .words = 0, .queries = 0, .classifications = 0, .groups = 0, .relations = 0 };
    var limits = Limits{};
    limits.max_attention_score_elements = 1; // This caps owned materialized scores only.
    limits.max_constant_bytes = 8192;
    const admitted = try planWithProfile(&config, layout, .train, .replay_tiled_v1, limits);
    try std.testing.expectEqual(@as(u64, 3 * 2 * 2 * 4096 * 4096), admitted.attention_work_items);
    try std.testing.expectError(error.ResourceLimitExceeded, plan(&config, layout, .train, limits));
    var rejected = limits;
    rejected.input.max_attention_work_items = admitted.attention_work_items - 1;
    try std.testing.expectError(error.ResourceLimitExceeded, planWithProfile(&config, layout, .train, .replay_tiled_v1, rejected));
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = Builder.init(&graph);
    var built = try buildWithProfile(&builder, &config, layout, .train, .replay_tiled_v1, limits);
    defer built.deinit();
    try std.testing.expect(graph.nodes.items.len < 256);
    try std.testing.expect(graph.constant_pool.items.len <= admitted.constant_bytes);
    var forward_bytes: u64 = 0;
    for (graph.nodes.items) |node| {
        if (node.op == .parameter) continue;
        const elements = node.output_shape.numElements() orelse return error.InvalidBoundaryGraphShape;
        try std.testing.expect(elements < 4096 * 4096);
        forward_bytes += @as(u64, @intCast(elements)) * 4;
    }
    try std.testing.expect(forward_bytes <= admitted.forward_tensor_upper_bound_bytes);
    try std.testing.expectEqual(@as(i64, 6 + 4096 + 8191), graph.node(built.inputs.attention_control).output_shape.dim(0));
}

test "GLiNER2.5 regional recomputation planning retains boundaries and admits four attention sweeps" {
    var config = testConfig();
    config.encoder.num_hidden_layers = 6;
    const layout = Layout{ .batch = 1, .sequence = 512, .words = 0, .queries = 0, .classifications = 0, .groups = 0, .relations = 0 };
    const retained = try planWithProfiles(&config, layout, .train, .replay_tiled_v1, .retained_v1, .{});
    const regional = try planWithProfiles(&config, layout, .train, .replay_tiled_v1, .layer_recompute_v1, .{});
    try std.testing.expectEqual(retained.attention_work_items / 3 * 4, regional.attention_work_items);
    try std.testing.expect(regional.forward_tensor_upper_bound_bytes < retained.forward_tensor_upper_bound_bytes);
    try std.testing.expect(regional.binding_bytes < retained.binding_bytes);
    try std.testing.expectEqual(retained.parameter_bytes, regional.parameter_bytes);
    // Reducing retained storage must not silently reduce admitted replay work.
    var limits = Limits{};
    limits.input.max_attention_work_items = retained.attention_work_items;
    _ = try planWithProfiles(&config, layout, .train, .replay_tiled_v1, .retained_v1, limits);
    try std.testing.expectError(error.ResourceLimitExceeded, planWithProfiles(&config, layout, .train, .replay_tiled_v1, .layer_recompute_v1, limits));
    limits = .{};
    limits.max_forward_tensor_bytes = regional.forward_tensor_upper_bound_bytes;
    _ = try planWithProfiles(&config, layout, .train, .replay_tiled_v1, .layer_recompute_v1, limits);
    try std.testing.expectError(error.ResourceLimitExceeded, planWithProfiles(&config, layout, .train, .replay_tiled_v1, .retained_v1, limits));
}
