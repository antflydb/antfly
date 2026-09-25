// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Differentiable Laya decision logits with the released ModernBERT layout.
//! Parameters retain upstream safetensors names. Calibration and the auxiliary
//! action head are not part of the typed-decision training objective.
const std = @import("std");
const ml = @import("ml").graph;
const modern = @import("../../architectures/modern_bert.zig");
const trunk = @import("../modern_bert_trunk.zig");
const B = ml.Builder;
const Id = ml.NodeId;
const Shape = ml.Shape;

/// `questions` counts decision rows: one per unpacked sequence, or every
/// question of every tree-packed row (pipelines/laya_tree.zig).
pub const Layout = struct { batch: u32, sequence: u32, options: u32, questions: u32 };
pub const Inputs = struct {
    ids: Id,
    kinds: Id, // per token; trunk tokens use any valid index with type_mask 0
    type_mask: Id, // [N,H], 0 where a token receives no question-type embedding
    markers: Id, // flattened batch-offset token indices, [questions*options]
    encoder_bias: Id = ml.null_node, // materialized: [B*encoder_heads,S,S], padding and tree visibility
    local_bias: Id = ml.null_node, // materialized: encoder_bias plus the logical sliding window
    head_bias: Id = ml.null_node, // materialized head: [B*head_heads,S,S], padding and tree visibility
    rope: [2][2]Id, // [global, local][cos, sin], each [N*heads, head_dim/2]
    control: Id = ml.null_node, // fused: key ranges and logical positions (`trunk.controlShape`)
};

pub const AttentionProfile = trunk.AttentionProfile;

/// The decision head's probability dropout has no fused form, so the head
/// stays materialized unless its dropout is zero.
pub fn headFused(attention: AttentionProfile, head_dropout: f32) bool {
    return attention == .fused_v1 and head_dropout == 0;
}
pub const Dropout = trunk.Dropout;
pub const Built = struct {
    inputs: Inputs,
    logits: Id,
    /// Dropout masks (`__laya_dropout_{i}`) and traced activations.
    sites: trunk.Sites = .{ .prefix = "__laya" },
    pub fn deinit(self: *Built, a: std.mem.Allocator) void {
        self.sites.deinit(a);
    }
};

pub const relu = trunk.relu;
pub const ropeTables = trunk.ropeTables;
const linear = trunk.linear;
const norm = trunk.norm;
const param = trunk.param;

pub fn validate(cfg: modern.Config, l: Layout, head_dropout: f32) !void {
    return validateProfile(cfg, l, head_dropout, .materialized_v1);
}

pub fn validateProfile(cfg: modern.Config, l: Layout, head_dropout: f32, attention: AttentionProfile) !void {
    const lc = cfg.laya orelse return error.InvalidLayaConfig;
    // The decision head uses 64-wide heads.
    if (cfg.hidden_size < 64 or cfg.hidden_size % 64 != 0 or lc.head_layers > 16 or l.sequence > (if (lc.packing.enabled()) lc.packing.max_packed_len else lc.max_len) or
        l.questions < l.batch or l.questions > 512 or (!lc.packing.enabled() and l.questions != l.batch) or
        l.options < 2 or l.options > lc.maxOptions() or !std.math.isFinite(head_dropout) or head_dropout < 0 or head_dropout >= 1)
        return error.InvalidLayaTrainingLayout;
    trunk.validate(cfg, .{ .batch = l.batch, .sequence = l.sequence }) catch return error.InvalidLayaTrainingLayout;
    // Bound the materialized reference attention of the encoder and of the
    // decision head (whose 64-wide heads can outnumber the encoder's). The
    // fused profile stores no score tensor.
    const scored_heads: u32 = @max(if (attention == .materialized_v1) cfg.num_attention_heads else 0, if (headFused(attention, head_dropout)) 0 else cfg.hidden_size / 64);
    if (@as(u64, l.batch) * l.sequence * l.sequence * scored_heads > 64 * 1024 * 1024)
        return error.LayaTrainingAttentionLimitExceeded;
}

pub fn build(b: *B, cfg: modern.Config, l: Layout, head_dropout: f32) !Built {
    return buildProfile(b, cfg, l, head_dropout, .materialized_v1);
}

pub fn buildProfile(b: *B, cfg: modern.Config, l: Layout, head_dropout: f32, attention: AttentionProfile) !Built {
    try validateProfile(cfg, l, head_dropout, attention);
    const lc = cfg.laya.?;
    const h = cfg.hidden_size;
    const n = l.batch * l.sequence;
    const nh = cfg.num_attention_heads;
    const hh = h / 64;
    const table = Shape.init(.f32, &.{ n * nh, (h / nh) / 2 });
    const fused = attention == .fused_v1;
    const head_fused = headFused(attention, head_dropout);
    const trunk_layout = trunk.Layout{ .batch = l.batch, .sequence = l.sequence };
    var result = Built{ .inputs = .{
        .ids = try b.parameter("__laya_ids", Shape.init(.i32, &.{n})),
        .kinds = try b.parameter("__laya_kinds", Shape.init(.i32, &.{n})),
        .type_mask = try b.parameter("__laya_type_mask", Shape.init(.f32, &.{ n, h })),
        .markers = try b.parameter("__laya_markers", Shape.init(.i32, &.{l.questions * l.options})),
        .encoder_bias = if (fused) ml.null_node else try b.parameter("__laya_encoder_bias", Shape.init(.f32, &.{ l.batch * nh, l.sequence, l.sequence })),
        .local_bias = if (fused) ml.null_node else try b.parameter("__laya_local_bias", Shape.init(.f32, &.{ l.batch * nh, l.sequence, l.sequence })),
        .head_bias = if (head_fused) ml.null_node else try b.parameter("__laya_head_bias", Shape.init(.f32, &.{ l.batch * hh, l.sequence, l.sequence })),
        .rope = .{
            .{ try b.parameter("__laya_rope_global_cos", table), try b.parameter("__laya_rope_global_sin", table) },
            .{ try b.parameter("__laya_rope_local_cos", table), try b.parameter("__laya_rope_local_sin", table) },
        },
        .control = if (fused) try b.parameter("__laya_attention_control", trunk.controlShape(trunk_layout)) else ml.null_node,
    }, .logits = ml.null_node };
    errdefer result.deinit(b.graph.allocator);
    var x = try trunk.encoder(b, &result.sites, cfg, trunk_layout, .{
        .ids = result.inputs.ids,
        .encoder_bias = result.inputs.encoder_bias,
        .local_bias = result.inputs.local_bias,
        .rope = result.inputs.rope,
        .profile = attention,
        .control = result.inputs.control,
    }, "encoder.");
    const types = try param(b, "type_emb", "weight", &.{ 3, h });
    x = try b.add(x, try b.mul(try b.gather(types, result.inputs.kinds, Shape.init(.f32, &.{ n, h })), result.inputs.type_mask));
    for (0..lc.head_layers) |layer| {
        var buffer: [128]u8 = undefined;
        var names: [256]u8 = undefined;
        const prefix = try std.fmt.bufPrint(&buffer, "head.layers.{d}", .{layer});
        const n1 = try norm(b, x, try std.fmt.bufPrint(&names, "{s}.norm1", .{prefix}), h, 1e-5, true);
        const w = try param(b, prefix, "self_attn.in_proj_weight", &.{ h * 3, h });
        const bias = try param(b, prefix, "self_attn.in_proj_bias", &.{h * 3});
        const qkv_fused = try b.linear(n1, w, bias, n, h, h * 3);
        const qkv = b.graph.node(qkv_fused).vjp_alternate;
        const q = try b.sliceLastDim(qkv, 0, h);
        const k = try b.sliceLastDim(qkv, h, h * 2);
        const v = try b.sliceLastDim(qkv, h * 2, h * 3);
        const attn = if (head_fused)
            try trunk.fusedAttention(b, q, k, v, result.inputs.control, trunk_layout, hh, 64, std.math.maxInt(u32))
        else
            try trunk.attention(b, &result.sites, q, k, v, result.inputs.head_bias, trunk_layout, hh, 64, head_dropout);
        const proj = try linear(b, attn, try std.fmt.bufPrint(&names, "{s}.self_attn.out_proj", .{prefix}), n, h, h, true);
        x = try b.add(x, try result.sites.drop(b, proj, head_dropout));
        const n2 = try norm(b, x, try std.fmt.bufPrint(&names, "{s}.norm2", .{prefix}), h, 1e-5, true);
        const up = try linear(b, n2, try std.fmt.bufPrint(&names, "{s}.linear1", .{prefix}), n, h, h * 4, true);
        try result.sites.trace(b.graph.allocator, try std.fmt.bufPrint(&names, "{s}.linear1", .{prefix}), up);
        const activated = try result.sites.drop(b, try relu(b, up), head_dropout);
        const down = try linear(b, activated, try std.fmt.bufPrint(&names, "{s}.linear2", .{prefix}), n, h * 4, h, true);
        x = try b.add(x, try result.sites.drop(b, down, head_dropout));
        try result.sites.trace(b.graph.allocator, prefix, x);
    }
    const m = try b.gather(x, result.inputs.markers, Shape.init(.f32, &.{ l.questions * l.options, h }));
    const normalized = try norm(b, m, "scorer.0", h, 1e-5, true);
    const up = try linear(b, normalized, "scorer.1", l.questions * l.options, h, h, true);
    const logits = try linear(b, try b.geluExact(up), "scorer.3", l.questions * l.options, h, 1, true);
    result.logits = try b.reshape(logits, Shape.init(.f32, &.{ l.questions, l.options }));
    return result;
}

test "laya training graph retains upstream parameters and all head dropout sites" {
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = B.init(&graph);
    const cfg = modern.Config{ .laya = .{ .head_layers = 1 }, .vocab_size = 64, .hidden_size = 64, .num_hidden_layers = 2, .num_attention_heads = 2, .intermediate_size = 96, .checkpoint_layout = .huggingface_fused_qkv_no_bias, .rope_interleaved = false };
    var built = try build(&b, cfg, .{ .batch = 2, .sequence = 8, .options = 3, .questions = 2 }, 0.1);
    defer built.deinit(a);
    try std.testing.expectEqual(@as(usize, 4), built.sites.dropouts.items.len);
    try std.testing.expectEqual(@as(i64, 6), graph.node(built.logits).output_shape.numElements().?);
    const seed = try b.parameter("__seed", graph.node(built.logits).output_shape);
    var wrt: std.ArrayListUnmanaged(Id) = .empty;
    defer wrt.deinit(a);
    for (graph.parameters.items) |id| {
        const name = graph.parameterName(graph.node(id));
        if (!std.mem.startsWith(u8, name, "__")) try wrt.append(a, id);
    }
    var grads = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = built.logits, .cotangent = seed }}, wrt.items, .{ .require_all_gradients = true });
    defer grads.deinit();
    try std.testing.expectEqual(wrt.items.len, grads.param_grads.len);
}
