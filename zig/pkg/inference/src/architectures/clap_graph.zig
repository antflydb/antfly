// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const ml = @import("ml");
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;

const clap_mod = @import("../models/clap.zig");
const ops = @import("../ops/ops.zig");
const graph_runtime = @import("../graph/runtime.zig");
const interpreter = @import("../graph/interpreter.zig");

const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;

pub const AudioQkvGraphResult = struct {
    q: []f32,
    k: []f32,
    v: []f32,

    pub fn deinit(self: AudioQkvGraphResult, allocator: std.mem.Allocator) void {
        allocator.free(self.q);
        allocator.free(self.k);
        allocator.free(self.v);
    }
};

pub fn runAudioTailGraph(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: clap_mod.Config,
    hidden: []const f32,
    batch: usize,
    tokens: usize,
    hidden_dim: usize,
    strategy: graph_runtime.Strategy,
) ![]f32 {
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const hidden_node = try bld.parameter("__audio_hidden", ml.graph.Shape.init(.f32, &.{ @intCast(batch * tokens), @intCast(hidden_dim) }));
    const output = try buildAudioTailGraph(&bld, cfg, hidden_node, batch, tokens, hidden_dim);
    try graph.markOutput(output);

    var rt_inputs: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
    defer rt_inputs.deinit(allocator);
    var owned_cts: std.ArrayListUnmanaged(CT) = .empty;
    defer {
        for (owned_cts.items) |ct| cb.free(ct);
        owned_cts.deinit(allocator);
    }

    try appendF32RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, hidden_node, hidden, &.{ @intCast(batch * tokens), @intCast(hidden_dim) });

    var runtime = try graph_runtime.Runtime.init(allocator, &graph, cb, strategy);
    defer runtime.deinit();
    var result = try runtime.execute(allocator, &graph, .{ .runtime_inputs = rt_inputs.items });
    defer result.deinit(&runtime);

    if (result.outputs.len == 0) return error.MissingGraphOutput;
    return cb.toFloat32(result.outputs[0], allocator);
}

pub fn runAudioQkvGraph(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    windows: []const f32,
    rows: usize,
    dim: usize,
    stage: usize,
    block: usize,
    strategy: graph_runtime.Strategy,
) !AudioQkvGraphResult {
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const windows_node = try bld.parameter("__audio_windows", ml.graph.Shape.init(.f32, &.{ @intCast(rows), @intCast(dim) }));
    const outputs = try buildAudioQkvGraph(&bld, windows_node, rows, dim, stage, block);
    try graph.markOutput(outputs[0]);
    try graph.markOutput(outputs[1]);
    try graph.markOutput(outputs[2]);

    var rt_inputs: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
    defer rt_inputs.deinit(allocator);
    var owned_cts: std.ArrayListUnmanaged(CT) = .empty;
    defer {
        for (owned_cts.items) |ct| cb.free(ct);
        owned_cts.deinit(allocator);
    }
    try appendF32RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, windows_node, windows, &.{ @intCast(rows), @intCast(dim) });

    var runtime = try graph_runtime.Runtime.init(allocator, &graph, cb, strategy);
    defer runtime.deinit();
    var result = try runtime.execute(allocator, &graph, .{ .runtime_inputs = rt_inputs.items });
    defer result.deinit(&runtime);
    if (result.outputs.len != 3) return error.MissingGraphOutput;

    const q = try cb.toFloat32(result.outputs[0], allocator);
    errdefer allocator.free(q);
    const k = try cb.toFloat32(result.outputs[1], allocator);
    errdefer allocator.free(k);
    const v = try cb.toFloat32(result.outputs[2], allocator);
    return .{ .q = q, .k = k, .v = v };
}

pub fn runAudioUnshiftedAttentionGraph(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    windows: []const f32,
    rows: usize,
    dim: usize,
    num_windows: usize,
    window_area: usize,
    num_heads: usize,
    window_size: usize,
    stage: usize,
    block: usize,
    strategy: graph_runtime.Strategy,
) ![]f32 {
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const windows_node = try bld.parameter("__audio_windows", ml.graph.Shape.init(.f32, &.{ @intCast(rows), @intCast(dim) }));
    const output = try buildAudioUnshiftedAttentionGraph(&bld, windows_node, rows, dim, num_windows, window_area, num_heads, window_size, stage, block);
    try graph.markOutput(output);

    return executeSingleF32Graph(cb, allocator, &graph, windows_node, windows, &.{ @intCast(rows), @intCast(dim) }, strategy);
}

pub fn runAudioAttentionProjectionGraph(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    attn_out: []const f32,
    rows: usize,
    dim: usize,
    stage: usize,
    block: usize,
    strategy: graph_runtime.Strategy,
) ![]f32 {
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const attn_node = try bld.parameter("__audio_attention_out", ml.graph.Shape.init(.f32, &.{ @intCast(rows), @intCast(dim) }));
    const output = try buildAudioAttentionProjectionGraph(&bld, attn_node, rows, dim, stage, block);
    try graph.markOutput(output);

    return executeSingleF32Graph(cb, allocator, &graph, attn_node, attn_out, &.{ @intCast(rows), @intCast(dim) }, strategy);
}

pub fn runAudioMlpGraph(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: []const f32,
    rows: usize,
    dim: usize,
    eps: f32,
    stage: usize,
    block: usize,
    strategy: graph_runtime.Strategy,
) ![]f32 {
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const hidden_node = try bld.parameter("__audio_block_hidden", ml.graph.Shape.init(.f32, &.{ @intCast(rows), @intCast(dim) }));
    const output = try buildAudioMlpGraph(&bld, hidden_node, rows, dim, eps, stage, block);
    try graph.markOutput(output);

    return executeSingleF32Graph(cb, allocator, &graph, hidden_node, hidden, &.{ @intCast(rows), @intCast(dim) }, strategy);
}

pub fn runAudioPatchEmbedGraph(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    ac: clap_mod.Config.AudioConfig,
    image: []const f32,
    batch: usize,
    height: usize,
    width: usize,
    strategy: graph_runtime.Strategy,
) ![]f32 {
    const out_h = (height - ac.patch_size) / ac.patch_stride[0] + 1;
    const out_w = (width - ac.patch_size) / ac.patch_stride[1] + 1;

    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const image_node = try bld.parameter("__audio_patch_image", ml.graph.Shape.init(.f32, &.{ @intCast(batch), 1, @intCast(height), @intCast(width) }));
    const output = try buildAudioPatchEmbedGraph(&bld, ac, image_node, batch, height, width, out_h, out_w);
    try graph.markOutput(output);

    var rt_inputs: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
    defer rt_inputs.deinit(allocator);
    var owned_cts: std.ArrayListUnmanaged(CT) = .empty;
    defer {
        for (owned_cts.items) |ct| cb.free(ct);
        owned_cts.deinit(allocator);
    }

    try appendF32RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, image_node, image, &.{ @intCast(batch), 1, @intCast(height), @intCast(width) });

    var runtime = try graph_runtime.Runtime.init(allocator, &graph, cb, strategy);
    defer runtime.deinit();
    var result = try runtime.execute(allocator, &graph, .{ .runtime_inputs = rt_inputs.items });
    defer result.deinit(&runtime);

    if (result.outputs.len == 0) return error.MissingGraphOutput;
    return cb.toFloat32(result.outputs[0], allocator);
}

fn executeSingleF32Graph(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    graph: *ml.graph.Graph,
    input_node: NodeId,
    input: []const f32,
    input_shape: []const i32,
    strategy: graph_runtime.Strategy,
) ![]f32 {
    var rt_inputs: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
    defer rt_inputs.deinit(allocator);
    var owned_cts: std.ArrayListUnmanaged(CT) = .empty;
    defer {
        for (owned_cts.items) |ct| cb.free(ct);
        owned_cts.deinit(allocator);
    }
    try appendF32RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, input_node, input, input_shape);

    var runtime = try graph_runtime.Runtime.init(allocator, graph, cb, strategy);
    defer runtime.deinit();
    var result = try runtime.execute(allocator, graph, .{ .runtime_inputs = rt_inputs.items });
    defer result.deinit(&runtime);

    if (result.outputs.len == 0) return error.MissingGraphOutput;
    return cb.toFloat32(result.outputs[0], allocator);
}

fn buildAudioTailGraph(
    bld: *Builder,
    cfg: clap_mod.Config,
    hidden: NodeId,
    batch: usize,
    tokens: usize,
    hidden_dim: usize,
) !NodeId {
    const H: u32 = @intCast(hidden_dim);
    const P: u32 = @intCast(cfg.projection_dim);

    const norm_w = try param1(bld, "audio_model.audio_encoder.norm.weight", hidden_dim);
    const norm_b = try param1(bld, "audio_model.audio_encoder.norm.bias", hidden_dim);
    const normed = try bld.layerNorm(hidden, norm_w, norm_b, H, cfg.audio_config.layer_norm_eps);
    const normed_3d = try bld.reshape(
        normed,
        ml.graph.Shape.init(.f32, &.{ @intCast(batch), @intCast(tokens), @intCast(hidden_dim) }),
    );
    const pooled_3d = try bld.reduceMean(normed_3d, &.{1});
    const pooled = try bld.reshape(
        pooled_3d,
        ml.graph.Shape.init(.f32, &.{ @intCast(batch), @intCast(hidden_dim) }),
    );

    const proj1_w = try param(bld, "audio_projection.linear1.weight", cfg.projection_dim, hidden_dim);
    const proj1_b = try param1(bld, "audio_projection.linear1.bias", cfg.projection_dim);
    const proj1 = try bld.linear(pooled, proj1_w, proj1_b, @intCast(batch), H, P);
    const activated = switch (cfg.projection_hidden_act) {
        .relu => try bld.relu(proj1),
        .gelu => try bld.gelu(proj1),
    };

    const proj2_w = try param(bld, "audio_projection.linear2.weight", cfg.projection_dim, cfg.projection_dim);
    const proj2_b = try param1(bld, "audio_projection.linear2.bias", cfg.projection_dim);
    return bld.linear(activated, proj2_w, proj2_b, @intCast(batch), P, P);
}

fn buildAudioQkvGraph(
    bld: *Builder,
    windows: NodeId,
    rows: usize,
    dim: usize,
    stage: usize,
    block: usize,
) ![3]NodeId {
    const D: u32 = @intCast(dim);
    const q_w = try layerParam(bld, stage, block, "attention.self.query.weight", dim, dim);
    const q_b = try layerParam1(bld, stage, block, "attention.self.query.bias", dim);
    const k_w = try layerParam(bld, stage, block, "attention.self.key.weight", dim, dim);
    const k_b = try layerParam1(bld, stage, block, "attention.self.key.bias", dim);
    const v_w = try layerParam(bld, stage, block, "attention.self.value.weight", dim, dim);
    const v_b = try layerParam1(bld, stage, block, "attention.self.value.bias", dim);
    return .{
        try bld.linear(windows, q_w, q_b, @intCast(rows), D, D),
        try bld.linear(windows, k_w, k_b, @intCast(rows), D, D),
        try bld.linear(windows, v_w, v_b, @intCast(rows), D, D),
    };
}

fn buildAudioUnshiftedAttentionGraph(
    bld: *Builder,
    windows: NodeId,
    rows: usize,
    dim: usize,
    num_windows: usize,
    window_area: usize,
    num_heads: usize,
    window_size: usize,
    stage: usize,
    block: usize,
) !NodeId {
    const qkv = try buildAudioQkvGraph(bld, windows, rows, dim, stage, block);
    const rel_bias = try layerParam1(bld, stage, block, "attention.self.relative_position_bias_table", (2 * window_size - 1) * (2 * window_size - 1) * num_heads);
    const attn = try bld.graph.addNode(.{
        .op = .{ .fused_windowed_self_attention = .{
            .batch = @intCast(num_windows),
            .height = @intCast(window_area),
            .width = @intCast(window_area),
            .dim = @intCast(dim),
            .num_heads = @intCast(num_heads),
            .window_size = @intCast(window_size),
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ @intCast(rows), @intCast(dim) }),
        .inputs = .{ qkv[0], qkv[1], qkv[2], rel_bias },
        .num_inputs = 4,
    });
    return buildAudioAttentionProjectionGraph(bld, attn, rows, dim, stage, block);
}

fn buildAudioAttentionProjectionGraph(
    bld: *Builder,
    attn_out: NodeId,
    rows: usize,
    dim: usize,
    stage: usize,
    block: usize,
) !NodeId {
    const D: u32 = @intCast(dim);
    const proj_w = try layerParam(bld, stage, block, "attention.output.dense.weight", dim, dim);
    const proj_b = try layerParam1(bld, stage, block, "attention.output.dense.bias", dim);
    return bld.linear(attn_out, proj_w, proj_b, @intCast(rows), D, D);
}

fn buildAudioMlpGraph(
    bld: *Builder,
    hidden: NodeId,
    rows: usize,
    dim: usize,
    eps: f32,
    stage: usize,
    block: usize,
) !NodeId {
    const D: u32 = @intCast(dim);
    const inner_dim = dim * 4;
    const I: u32 = @intCast(inner_dim);

    const norm_w = try layerParam1(bld, stage, block, "layernorm_after.weight", dim);
    const norm_b = try layerParam1(bld, stage, block, "layernorm_after.bias", dim);
    const after = try bld.layerNorm(hidden, norm_w, norm_b, D, eps);

    const fc1_w = try layerParam(bld, stage, block, "intermediate.dense.weight", inner_dim, dim);
    const fc1_b = try layerParam1(bld, stage, block, "intermediate.dense.bias", inner_dim);
    const fc1 = try bld.linear(after, fc1_w, fc1_b, @intCast(rows), D, I);
    const act = try bld.gelu(fc1);

    const fc2_w = try layerParam(bld, stage, block, "output.dense.weight", dim, inner_dim);
    const fc2_b = try layerParam1(bld, stage, block, "output.dense.bias", dim);
    return bld.linear(act, fc2_w, fc2_b, @intCast(rows), I, D);
}

fn buildAudioPatchEmbedGraph(
    bld: *Builder,
    ac: clap_mod.Config.AudioConfig,
    image: NodeId,
    batch: usize,
    height: usize,
    width: usize,
    out_h: usize,
    out_w: usize,
) !NodeId {
    const patch_w = try param4(bld, "audio_model.audio_encoder.patch_embed.proj.weight", ac.patch_embeds_hidden_size, 1, ac.patch_size, ac.patch_size);
    const patch_b = try param1(bld, "audio_model.audio_encoder.patch_embed.proj.bias", ac.patch_embeds_hidden_size);
    const conv = try bld.graph.addNode(.{
        .op = .{ .fused_conv2d = .{
            .batch = @intCast(batch),
            .in_channels = 1,
            .out_channels = ac.patch_embeds_hidden_size,
            .height = @intCast(height),
            .width = @intCast(width),
            .kernel_h = ac.patch_size,
            .kernel_w = ac.patch_size,
            .stride_h = ac.patch_stride[0],
            .stride_w = ac.patch_stride[1],
            .padding_h = 0,
            .padding_w = 0,
            .groups = 1,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ @intCast(batch), @intCast(ac.patch_embeds_hidden_size), @intCast(out_h), @intCast(out_w) }),
        .inputs = .{ image, patch_w, patch_b, null_node },
        .num_inputs = 3,
    });
    const token_order = try bld.transpose(conv, &.{ 0, 2, 3, 1 });
    var tokens = try bld.reshape(
        token_order,
        ml.graph.Shape.init(.f32, &.{ @intCast(batch * out_h * out_w), @intCast(ac.patch_embeds_hidden_size) }),
    );
    if (ac.enable_patch_layer_norm) {
        const norm_w = try param1(bld, "audio_model.audio_encoder.patch_embed.norm.weight", ac.patch_embeds_hidden_size);
        const norm_b = try param1(bld, "audio_model.audio_encoder.patch_embed.norm.bias", ac.patch_embeds_hidden_size);
        tokens = try bld.layerNorm(tokens, norm_w, norm_b, ac.patch_embeds_hidden_size, ac.layer_norm_eps);
    }
    return tokens;
}

fn param(bld: *Builder, name: []const u8, rows: usize, cols: usize) !NodeId {
    return bld.parameter(name, ml.graph.Shape.init(.f32, &.{ @intCast(rows), @intCast(cols) }));
}

fn param1(bld: *Builder, name: []const u8, len: usize) !NodeId {
    return bld.parameter(name, ml.graph.Shape.init(.f32, &.{@intCast(len)}));
}

fn param4(bld: *Builder, name: []const u8, a: usize, b: usize, c: usize, d: usize) !NodeId {
    return bld.parameter(name, ml.graph.Shape.init(.f32, &.{ @intCast(a), @intCast(b), @intCast(c), @intCast(d) }));
}

fn layerParam(bld: *Builder, stage: usize, block: usize, suffix: []const u8, rows: usize, cols: usize) !NodeId {
    const name = try std.fmt.allocPrint(bld.graph.allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.{s}", .{ stage, block, suffix });
    defer bld.graph.allocator.free(name);
    return param(bld, name, rows, cols);
}

fn layerParam1(bld: *Builder, stage: usize, block: usize, suffix: []const u8, len: usize) !NodeId {
    const name = try std.fmt.allocPrint(bld.graph.allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.{s}", .{ stage, block, suffix });
    defer bld.graph.allocator.free(name);
    return param1(bld, name, len);
}

fn appendF32RuntimeInput(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    rt_inputs: *std.ArrayListUnmanaged(interpreter.RuntimeInput),
    owned_cts: *std.ArrayListUnmanaged(CT),
    node_id: NodeId,
    data: []const f32,
    shape: []const i32,
) !void {
    const ct = try cb.fromFloat32Shape(data, shape);
    try owned_cts.append(allocator, ct);
    try rt_inputs.append(allocator, .{ .node_id = node_id, .value = ct });
}
