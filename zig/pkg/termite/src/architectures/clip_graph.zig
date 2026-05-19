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

const clip_mod = @import("../models/clip.zig");
const ops = @import("../ops/ops.zig");
const graph_runtime = @import("../graph/runtime.zig");
const interpreter = @import("../graph/interpreter.zig");

const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;

pub fn runTextGraph(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: clip_mod.Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    strategy: graph_runtime.Strategy,
) ![]f32 {
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const total = batch * seq_len;
    const input_ids_node = try bld.parameter("__input_ids", ml.graph.Shape.init(.i64, &.{@intCast(total)}));
    const pos_ids_node = try bld.parameter("__pos_ids", ml.graph.Shape.init(.i64, &.{@intCast(total)}));
    const eos_indices_node = try bld.parameter("__eos_indices", ml.graph.Shape.init(.i64, &.{@intCast(batch)}));

    const output = try buildTextGraph(&bld, cfg, input_ids_node, pos_ids_node, eos_indices_node, batch, seq_len);
    try graph.markOutput(output);

    const pos_ids = try buildPositionIds(allocator, batch, seq_len);
    defer allocator.free(pos_ids);
    const eos_indices = try buildEosIndices(allocator, input_ids, batch, seq_len);
    defer allocator.free(eos_indices);

    var rt_inputs: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
    defer rt_inputs.deinit(allocator);
    var owned_cts: std.ArrayListUnmanaged(CT) = .empty;
    defer {
        for (owned_cts.items) |ct| cb.free(ct);
        owned_cts.deinit(allocator);
    }

    try appendI64RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, input_ids_node, input_ids);
    try appendI64RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, pos_ids_node, pos_ids);
    try appendI64RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, eos_indices_node, eos_indices);

    var runtime = try graph_runtime.Runtime.init(allocator, &graph, cb, strategy);
    defer runtime.deinit();
    var result = try runtime.execute(allocator, &graph, .{ .runtime_inputs = rt_inputs.items });
    defer result.deinit(&runtime);

    if (result.outputs.len == 0) return error.MissingGraphOutput;
    return cb.toFloat32(result.outputs[0], allocator);
}

pub fn runVisionGraph(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: clip_mod.Config,
    pixel_values: []const f32,
    batch: usize,
    strategy: graph_runtime.Strategy,
) ![]f32 {
    const P = cfg.patch_size;
    const img_size = cfg.image_size;
    const grid = img_size / P;
    const num_patches = grid * grid;
    const patch_dim = 3 * P * P;
    const full_seq = num_patches + 1;

    const patches = try extractPatches(allocator, pixel_values, batch, img_size, P, num_patches, patch_dim);
    defer allocator.free(patches);

    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const patches_node = try bld.parameter("__patches", ml.graph.Shape.init(.f32, &.{ @intCast(batch * num_patches), @intCast(patch_dim) }));
    const patch_pos_ids_node = try bld.parameter("__patch_pos_ids", ml.graph.Shape.init(.i64, &.{@intCast(batch * num_patches)}));
    const cls_pos_ids_node = try bld.parameter("__cls_pos_ids", ml.graph.Shape.init(.i64, &.{@intCast(batch)}));
    const cls_indices_node = try bld.parameter("__cls_indices", ml.graph.Shape.init(.i64, &.{@intCast(batch)}));

    const output = try buildVisionGraph(&bld, cfg, patches_node, patch_pos_ids_node, cls_pos_ids_node, cls_indices_node, batch, num_patches, patch_dim, full_seq);
    try graph.markOutput(output);

    const patch_pos_ids = try buildPatchPositionIds(allocator, batch, num_patches);
    defer allocator.free(patch_pos_ids);
    const cls_pos_ids = try allocator.alloc(i64, batch);
    defer allocator.free(cls_pos_ids);
    @memset(cls_pos_ids, 0);
    const cls_indices = try buildClsIndices(allocator, batch, full_seq);
    defer allocator.free(cls_indices);

    var rt_inputs: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
    defer rt_inputs.deinit(allocator);
    var owned_cts: std.ArrayListUnmanaged(CT) = .empty;
    defer {
        for (owned_cts.items) |ct| cb.free(ct);
        owned_cts.deinit(allocator);
    }

    try appendF32RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, patches_node, patches, &.{ @intCast(batch * num_patches), @intCast(patch_dim) });
    try appendI64RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, patch_pos_ids_node, patch_pos_ids);
    try appendI64RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, cls_pos_ids_node, cls_pos_ids);
    try appendI64RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, cls_indices_node, cls_indices);

    var runtime = try graph_runtime.Runtime.init(allocator, &graph, cb, strategy);
    defer runtime.deinit();
    var result = try runtime.execute(allocator, &graph, .{ .runtime_inputs = rt_inputs.items });
    defer result.deinit(&runtime);

    if (result.outputs.len == 0) return error.MissingGraphOutput;
    return cb.toFloat32(result.outputs[0], allocator);
}

fn buildTextGraph(
    bld: *Builder,
    cfg: clip_mod.Config,
    input_ids: NodeId,
    pos_ids: NodeId,
    eos_indices: NodeId,
    batch: usize,
    seq_len: usize,
) !NodeId {
    const H: u32 = @intCast(cfg.text_hidden_size);
    const total: u32 = @intCast(batch * seq_len);

    const tok_w = try param(bld, "text_model.embeddings.token_embedding.weight", cfg.vocab_size, cfg.text_hidden_size);
    var hidden = try bld.embeddingLookup(tok_w, input_ids, total, H);

    const pos_w = try param(bld, "text_model.embeddings.position_embedding.weight", cfg.text_max_position_embeddings, cfg.text_hidden_size);
    const pos_emb = try bld.embeddingLookup(pos_w, pos_ids, total, H);
    hidden = try bld.add(hidden, pos_emb);

    for (0..cfg.text_num_layers) |layer| {
        hidden = try encoderBlock(
            bld,
            hidden,
            "text_model.encoder.layers",
            layer,
            batch,
            seq_len,
            cfg.text_num_heads,
            cfg.textHeadDim(),
            cfg.text_hidden_size,
            cfg.text_intermediate_size,
            true,
        );
    }

    const final_g = try param1(bld, "text_model.final_layer_norm.weight", cfg.text_hidden_size);
    const final_b = try param1(bld, "text_model.final_layer_norm.bias", cfg.text_hidden_size);
    hidden = try bld.layerNorm(hidden, final_g, final_b, H, 1e-5);

    const pooled = try bld.embeddingLookup(hidden, eos_indices, @intCast(batch), H);
    const proj_w = try param(bld, "text_projection.weight", cfg.projection_dim, cfg.text_hidden_size);
    return bld.linearNoBias(pooled, proj_w, @intCast(batch), H, @intCast(cfg.projection_dim));
}

fn buildVisionGraph(
    bld: *Builder,
    cfg: clip_mod.Config,
    patches: NodeId,
    patch_pos_ids: NodeId,
    cls_pos_ids: NodeId,
    cls_indices: NodeId,
    batch: usize,
    num_patches: usize,
    patch_dim: usize,
    full_seq: usize,
) !NodeId {
    const H: u32 = @intCast(cfg.vision_hidden_size);
    const patch_w = try param(bld, "vision_model.embeddings.patch_embedding.weight", cfg.vision_hidden_size, patch_dim);
    const patch_emb = try bld.linearNoBias(patches, patch_w, @intCast(batch * num_patches), @intCast(patch_dim), H);

    const pos_w = try param(bld, "vision_model.embeddings.position_embedding.weight", full_seq, cfg.vision_hidden_size);
    const patch_pos = try bld.embeddingLookup(pos_w, patch_pos_ids, @intCast(batch * num_patches), H);
    const patch_with_pos_flat = try bld.add(patch_emb, patch_pos);
    const patch_with_pos = try bld.reshape(
        patch_with_pos_flat,
        ml.graph.Shape.init(.f32, &.{ @intCast(batch), @intCast(num_patches), @intCast(cfg.vision_hidden_size) }),
    );

    const cls_embedding = try param1(bld, "vision_model.embeddings.class_embedding", cfg.vision_hidden_size);
    const cls_pos = try bld.embeddingLookup(pos_w, cls_pos_ids, @intCast(batch), H);
    const cls_with_pos_flat = try bld.add(cls_pos, cls_embedding);
    const cls_with_pos = try bld.reshape(
        cls_with_pos_flat,
        ml.graph.Shape.init(.f32, &.{ @intCast(batch), 1, @intCast(cfg.vision_hidden_size) }),
    );

    const full_hidden_3d = try bld.concat(cls_with_pos, patch_with_pos, 1);
    var hidden = try bld.reshape(
        full_hidden_3d,
        ml.graph.Shape.init(.f32, &.{ @intCast(batch * full_seq), @intCast(cfg.vision_hidden_size) }),
    );

    const pre_g = try param1(bld, "vision_model.pre_layrnorm.weight", cfg.vision_hidden_size);
    const pre_b = try param1(bld, "vision_model.pre_layrnorm.bias", cfg.vision_hidden_size);
    hidden = try bld.layerNorm(hidden, pre_g, pre_b, H, 1e-5);

    for (0..cfg.vision_num_layers) |layer| {
        hidden = try encoderBlock(
            bld,
            hidden,
            "vision_model.encoder.layers",
            layer,
            batch,
            full_seq,
            cfg.vision_num_heads,
            cfg.visionHeadDim(),
            cfg.vision_hidden_size,
            cfg.vision_intermediate_size,
            false,
        );
    }

    const post_g = try param1(bld, "vision_model.post_layernorm.weight", cfg.vision_hidden_size);
    const post_b = try param1(bld, "vision_model.post_layernorm.bias", cfg.vision_hidden_size);
    hidden = try bld.layerNorm(hidden, post_g, post_b, H, 1e-5);

    const pooled = try bld.embeddingLookup(hidden, cls_indices, @intCast(batch), H);
    const proj_w = try param(bld, "visual_projection.weight", cfg.projection_dim, cfg.vision_hidden_size);
    return bld.linearNoBias(pooled, proj_w, @intCast(batch), H, @intCast(cfg.projection_dim));
}

fn encoderBlock(
    bld: *Builder,
    input: NodeId,
    prefix: []const u8,
    layer: usize,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
    hidden: usize,
    intermediate: usize,
    causal: bool,
) !NodeId {
    const total: u32 = @intCast(batch * seq_len);
    const H: u32 = @intCast(hidden);
    const I: u32 = @intCast(intermediate);

    const ln1_g = try layerParam1(bld, prefix, layer, "layer_norm1.weight", hidden);
    const ln1_b = try layerParam1(bld, prefix, layer, "layer_norm1.bias", hidden);
    const normed1 = try bld.layerNorm(input, ln1_g, ln1_b, H, 1e-5);
    const attn_res = try selfAttention(bld, normed1, input, prefix, layer, batch, seq_len, num_heads, head_dim, hidden, causal);

    const ln2_g = try layerParam1(bld, prefix, layer, "layer_norm2.weight", hidden);
    const ln2_b = try layerParam1(bld, prefix, layer, "layer_norm2.bias", hidden);
    const normed2 = try bld.layerNorm(attn_res, ln2_g, ln2_b, H, 1e-5);

    const fc1_w = try layerParam(bld, prefix, layer, "mlp.fc1.weight", intermediate, hidden);
    const fc1_b = try layerParam1(bld, prefix, layer, "mlp.fc1.bias", intermediate);
    const fc1 = try bld.linear(normed2, fc1_w, fc1_b, total, H, I);
    const activated = try quickGelu(bld, fc1);

    const fc2_w = try layerParam(bld, prefix, layer, "mlp.fc2.weight", hidden, intermediate);
    const fc2_b = try layerParam1(bld, prefix, layer, "mlp.fc2.bias", hidden);
    const fc2 = try bld.linear(activated, fc2_w, fc2_b, total, I, H);
    return bld.add(attn_res, fc2);
}

fn selfAttention(
    bld: *Builder,
    input: NodeId,
    residual: NodeId,
    prefix: []const u8,
    layer: usize,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
    hidden: usize,
    causal: bool,
) !NodeId {
    const total: u32 = @intCast(batch * seq_len);
    const H: u32 = @intCast(hidden);

    const q_w = try layerParam(bld, prefix, layer, "self_attn.q_proj.weight", hidden, hidden);
    const q_b = try layerParam1(bld, prefix, layer, "self_attn.q_proj.bias", hidden);
    const k_w = try layerParam(bld, prefix, layer, "self_attn.k_proj.weight", hidden, hidden);
    const k_b = try layerParam1(bld, prefix, layer, "self_attn.k_proj.bias", hidden);
    const v_w = try layerParam(bld, prefix, layer, "self_attn.v_proj.weight", hidden, hidden);
    const v_b = try layerParam1(bld, prefix, layer, "self_attn.v_proj.bias", hidden);

    const q = try bld.linear(input, q_w, q_b, total, H, H);
    const k = try bld.linear(input, k_w, k_b, total, H, H);
    const v = try bld.linear(input, v_w, v_b, total, H, H);
    const attn = if (causal)
        try causalSelfAttention(bld, q, k, v, batch, seq_len, num_heads, head_dim)
    else
        try sdpa(bld, q, k, v, batch, seq_len, num_heads, head_dim);

    const o_w = try layerParam(bld, prefix, layer, "self_attn.out_proj.weight", hidden, hidden);
    const o_b = try layerParam1(bld, prefix, layer, "self_attn.out_proj.bias", hidden);
    const projected = try bld.linear(attn, o_w, o_b, total, H, H);
    return bld.add(residual, projected);
}

fn sdpa(
    bld: *Builder,
    q: NodeId,
    k: NodeId,
    v: NodeId,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
) !NodeId {
    const q_shape = bld.graph.node(q).output_shape;
    return bld.graph.addNode(.{
        .op = .{ .fused_sdpa = .{
            .batch = @intCast(batch),
            .seq_len = @intCast(seq_len),
            .num_heads = @intCast(num_heads),
            .head_dim = @intCast(head_dim),
        } },
        .output_shape = q_shape,
        .inputs = .{ q, k, v, null_node },
        .num_inputs = 3,
    });
}

fn causalSelfAttention(
    bld: *Builder,
    q: NodeId,
    k: NodeId,
    v: NodeId,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
) !NodeId {
    const q_shape = bld.graph.node(q).output_shape;
    return bld.graph.addNode(.{
        .op = .{ .fused_causal_self_attention = .{
            .batch = @intCast(batch),
            .seq_len = @intCast(seq_len),
            .num_heads = @intCast(num_heads),
            .head_dim = @intCast(head_dim),
        } },
        .output_shape = q_shape,
        .inputs = .{ q, k, v, null_node },
        .num_inputs = 3,
    });
}

fn quickGelu(bld: *Builder, input: NodeId) !NodeId {
    const x_shape = bld.graph.node(input).output_shape;
    const decomposed = blk: {
        const scale = try bld.scalarConst(x_shape.dtype, 1.702);
        const scaled = try bld.mul(input, scale);
        const sigmoid = try bld.sigmoid(scaled);
        break :blk try bld.mul(input, sigmoid);
    };
    return bld.graph.addNode(.{
        .op = .{ .fused_quick_gelu = {} },
        .output_shape = x_shape,
        .inputs = .{ input, null_node, null_node, null_node },
        .num_inputs = 1,
        .vjp_alternate = decomposed,
    });
}

fn param(bld: *Builder, name: []const u8, rows: usize, cols: usize) !NodeId {
    return bld.parameter(name, ml.graph.Shape.init(.f32, &.{ @intCast(rows), @intCast(cols) }));
}

fn param1(bld: *Builder, name: []const u8, len: usize) !NodeId {
    return bld.parameter(name, ml.graph.Shape.init(.f32, &.{@intCast(len)}));
}

fn layerParam(bld: *Builder, prefix: []const u8, layer: usize, suffix: []const u8, rows: usize, cols: usize) !NodeId {
    const name = try std.fmt.allocPrint(bld.graph.allocator, "{s}.{d}.{s}", .{ prefix, layer, suffix });
    defer bld.graph.allocator.free(name);
    return param(bld, name, rows, cols);
}

fn layerParam1(bld: *Builder, prefix: []const u8, layer: usize, suffix: []const u8, len: usize) !NodeId {
    const name = try std.fmt.allocPrint(bld.graph.allocator, "{s}.{d}.{s}", .{ prefix, layer, suffix });
    defer bld.graph.allocator.free(name);
    return param1(bld, name, len);
}

fn buildPositionIds(allocator: std.mem.Allocator, batch: usize, seq_len: usize) ![]i64 {
    const ids = try allocator.alloc(i64, batch * seq_len);
    for (0..batch) |b| {
        for (0..seq_len) |s| ids[b * seq_len + s] = @intCast(s);
    }
    return ids;
}

fn buildPatchPositionIds(allocator: std.mem.Allocator, batch: usize, num_patches: usize) ![]i64 {
    const ids = try allocator.alloc(i64, batch * num_patches);
    for (0..batch) |b| {
        for (0..num_patches) |p| ids[b * num_patches + p] = @intCast(p + 1);
    }
    return ids;
}

fn buildClsIndices(allocator: std.mem.Allocator, batch: usize, full_seq: usize) ![]i64 {
    const indices = try allocator.alloc(i64, batch);
    for (0..batch) |b| indices[b] = @intCast(b * full_seq);
    return indices;
}

fn buildEosIndices(allocator: std.mem.Allocator, input_ids: []const i64, batch: usize, seq_len: usize) ![]i64 {
    const indices = try allocator.alloc(i64, batch);
    for (0..batch) |b| {
        var eos_pos: usize = 0;
        for (0..seq_len) |s| {
            if (input_ids[b * seq_len + s] != 0) eos_pos = s;
        }
        indices[b] = @intCast(b * seq_len + eos_pos);
    }
    return indices;
}

fn extractPatches(
    allocator: std.mem.Allocator,
    pixel_values: []const f32,
    batch: usize,
    img_size: usize,
    patch_size: usize,
    num_patches: usize,
    patch_dim: usize,
) ![]f32 {
    const grid = img_size / patch_size;
    const patches = try allocator.alloc(f32, batch * num_patches * patch_dim);
    const channel_stride = img_size * img_size;
    const image_stride = 3 * channel_stride;
    for (0..batch) |b| {
        const img_base = b * image_stride;
        for (0..grid) |ph| {
            const row_base = ph * patch_size;
            for (0..grid) |pw| {
                const pidx = b * num_patches + ph * grid + pw;
                const patch_base = pidx * patch_dim;
                const col_base = pw * patch_size;
                for (0..3) |ch| {
                    const ch_dst_base = patch_base + ch * patch_size * patch_size;
                    const ch_src_base = img_base + ch * channel_stride;
                    for (0..patch_size) |y| {
                        const dst = patches[ch_dst_base + y * patch_size ..][0..patch_size];
                        const src = pixel_values[ch_src_base + (row_base + y) * img_size + col_base ..][0..patch_size];
                        @memcpy(dst, src);
                    }
                }
            }
        }
    }
    return patches;
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

fn appendI64RuntimeInput(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    rt_inputs: *std.ArrayListUnmanaged(interpreter.RuntimeInput),
    owned_cts: *std.ArrayListUnmanaged(CT),
    node_id: NodeId,
    data: []const i64,
) !void {
    const ct = try bindI64AsF32(cb, allocator, data);
    try owned_cts.append(allocator, ct);
    try rt_inputs.append(allocator, .{ .node_id = node_id, .value = ct });
}

fn bindI64AsF32(cb: *const ComputeBackend, allocator: std.mem.Allocator, data: []const i64) !CT {
    const f32_buf = try allocator.alloc(f32, data.len);
    defer allocator.free(f32_buf);
    for (data, 0..) |v, i| f32_buf[i] = @floatFromInt(v);
    return cb.fromFloat32Shape(f32_buf, &.{@intCast(data.len)});
}
