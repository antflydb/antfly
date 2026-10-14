// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Native Moondream2 (`moondream1` model type, vikhyatk/moondream2 2024-03-04
//! weights) over any `ComputeBackend`.
//!
//! The model has three parts, all read by their checkpoint tensor names:
//!
//! - a SigLIP so400m/14 vision tower at 378x378 (timm layout: linear patch
//!   embedding over `(c p1 p2)` patches, fused qkv, exact GELU, no pooling),
//! - a two-layer MLP projector (1152 -> 8192 -> 2048, exact GELU),
//! - a Phi-1.5 (`phi-msft`) decoder: one LayerNorm feeds attention and MLP in
//!   parallel, fused Wqkv, partial NeoX rotary over the first 32 of 64 head
//!   dimensions, gelu_new MLP, and a LayerNorm + biased linear LM head.
//!
//! Rotary embedding is applied on the host because Phi pairs dimension `j` with
//! `j + rotary_dim / 2`, while the shared backend rope pairs `j` with
//! `j + head_dim / 2` when `rope_dim < head_dim`. Attention, KV caching,
//! linears, and norms use the shared backend kernels and paged KV manager.

const std = @import("std");
const ops = @import("../ops/ops.zig");
const runtime = @import("../runtime/root.zig");
const linalg = @import("inference_linalg");

const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;

pub const model_type = "moondream1";

pub const Config = struct {
    vocab_size: usize = 51200,
    hidden_size: usize = 2048,
    num_layers: usize = 24,
    num_heads: usize = 32,
    rotary_dim: usize = 32,
    intermediate_size: usize = 8192,
    layer_norm_eps: f32 = 1e-5,
    rope_theta: f32 = 10000.0,
    max_position_embeddings: usize = 2048,
    bos_token_id: i32 = 50256,
    eos_token_id: i32 = 50256,

    image_size: usize = 378,
    patch_size: usize = 14,
    vision_hidden_size: usize = 1152,
    vision_num_layers: usize = 27,
    vision_num_heads: usize = 16,
    vision_intermediate_size: usize = 4304,
    vision_layer_norm_eps: f32 = 1e-6,
    projector_hidden_size: usize = 8192,
    image_mean: [3]f32 = .{ 0.5, 0.5, 0.5 },
    image_std: [3]f32 = .{ 0.5, 0.5, 0.5 },

    pub fn headDim(self: Config) usize {
        return self.hidden_size / self.num_heads;
    }

    pub fn visionHeadDim(self: Config) usize {
        return self.vision_hidden_size / self.vision_num_heads;
    }

    pub fn patchGrid(self: Config) usize {
        return self.image_size / self.patch_size;
    }

    /// Projected image embeddings inserted into the decoder per image.
    pub fn imageTokens(self: Config) usize {
        const grid = self.patchGrid();
        return grid * grid;
    }

    pub fn patchDim(self: Config) usize {
        return 3 * self.patch_size * self.patch_size;
    }

    pub fn validate(self: Config) !void {
        if (self.hidden_size == 0 or self.num_heads == 0 or self.hidden_size % self.num_heads != 0)
            return error.InvalidMoondreamConfig;
        const head_dim = self.headDim();
        if (self.rotary_dim == 0 or self.rotary_dim > head_dim or self.rotary_dim % 2 != 0)
            return error.InvalidMoondreamConfig;
        if (self.vision_num_heads == 0 or self.vision_hidden_size % self.vision_num_heads != 0)
            return error.InvalidMoondreamConfig;
        if (self.patch_size == 0 or self.image_size % self.patch_size != 0)
            return error.InvalidMoondreamConfig;
        if (self.num_layers == 0 or self.vision_num_layers == 0) return error.InvalidMoondreamConfig;
    }
};

/// True when a config.json declares the Moondream2 composite model.
pub fn isMoondreamConfig(allocator: std.mem.Allocator, config_json: []const u8) bool {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, config_json, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    const value = parsed.value.object.get("model_type") orelse return false;
    return value == .string and std.mem.eql(u8, value.string, model_type);
}

/// Parse the upstream `config.json`. The original checkpoint stores only
/// `{"phi_config": {"model_type": "phi-msft"}}` and relies on PhiConfig
/// defaults; transformers.js copies carry an expanded HF `PhiConfig`. Both
/// spellings are accepted, and absent fields keep the Moondream2 defaults.
pub fn parseConfig(allocator: std.mem.Allocator, config_json: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, config_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidMoondreamConfig;
    const root = parsed.value.object;
    const mt = root.get("model_type") orelse return error.InvalidMoondreamConfig;
    if (mt != .string or !std.mem.eql(u8, mt.string, model_type)) return error.InvalidMoondreamConfig;

    var config = Config{};
    if (root.get("phi_config")) |phi_value| if (phi_value == .object) {
        const phi = phi_value.object;
        readUsize(phi, &.{ "vocab_size", "n_vocab" }, &config.vocab_size);
        readUsize(phi, &.{ "hidden_size", "n_embd" }, &config.hidden_size);
        readUsize(phi, &.{ "num_hidden_layers", "n_layer" }, &config.num_layers);
        readUsize(phi, &.{ "num_attention_heads", "n_head" }, &config.num_heads);
        readUsize(phi, &.{ "intermediate_size", "n_inner" }, &config.intermediate_size);
        readUsize(phi, &.{ "max_position_embeddings", "n_positions" }, &config.max_position_embeddings);
        readF32(phi, &.{ "layer_norm_eps", "layer_norm_epsilon" }, &config.layer_norm_eps);
        readF32(phi, &.{"rope_theta"}, &config.rope_theta);
        if (phi.get("bos_token_id")) |v| if (v == .integer) {
            config.bos_token_id = @intCast(v.integer);
        };
        if (phi.get("eos_token_id")) |v| if (v == .integer) {
            config.eos_token_id = @intCast(v.integer);
        };
        const head_dim = if (config.num_heads > 0) config.hidden_size / config.num_heads else 0;
        if (phi.get("rotary_dim")) |v| if (v == .integer and v.integer > 0) {
            config.rotary_dim = @min(@as(usize, @intCast(v.integer)), head_dim);
        };
        if (phi.get("partial_rotary_factor")) |v| {
            const factor: ?f64 = switch (v) {
                .float => v.float,
                .integer => @floatFromInt(v.integer),
                else => null,
            };
            if (factor) |f| {
                if (f > 0 and f <= 1) config.rotary_dim = @intFromFloat(@round(f * @as(f64, @floatFromInt(head_dim))));
            }
        }
    };
    // PhiConfig pads the vocabulary to a multiple of 64.
    config.vocab_size = std.mem.alignForward(usize, config.vocab_size, 64);
    try config.validate();
    return config;
}

fn readUsize(obj: std.json.ObjectMap, keys: []const []const u8, out: *usize) void {
    for (keys) |key| {
        if (obj.get(key)) |v| if (v == .integer and v.integer > 0) {
            out.* = @intCast(v.integer);
            return;
        };
    }
}

fn readF32(obj: std.json.ObjectMap, keys: []const []const u8, out: *f32) void {
    for (keys) |key| {
        if (obj.get(key)) |v| switch (v) {
            .float => {
                out.* = @floatCast(v.float);
                return;
            },
            .integer => {
                out.* = @floatFromInt(v.integer);
                return;
            },
            else => {},
        };
    }
}

// --- Weight names ---------------------------------------------------------

pub const names = struct {
    pub const token_embedding = "text_model.transformer.embd.wte.weight";
    pub const lm_head_norm_weight = "text_model.lm_head.ln.weight";
    pub const lm_head_norm_bias = "text_model.lm_head.ln.bias";
    pub const lm_head_weight = "text_model.lm_head.linear.weight";
    pub const lm_head_bias = "text_model.lm_head.linear.bias";

    pub const patch_embed_weight = "vision_encoder.encoder.model.visual.patch_embed.linear.weight";
    pub const patch_embed_bias = "vision_encoder.encoder.model.visual.patch_embed.linear.bias";
    pub const position_embedding = "vision_encoder.encoder.model.visual.pos_embed";
    pub const vision_norm_weight = "vision_encoder.encoder.model.visual.norm.weight";
    pub const vision_norm_bias = "vision_encoder.encoder.model.visual.norm.bias";
    pub const projector_fc1_weight = "vision_encoder.projection.mlp.fc1.weight";
    pub const projector_fc1_bias = "vision_encoder.projection.mlp.fc1.bias";
    pub const projector_fc2_weight = "vision_encoder.projection.mlp.fc2.weight";
    pub const projector_fc2_bias = "vision_encoder.projection.mlp.fc2.bias";

    pub fn text(buf: []u8, layer: usize, comptime suffix: []const u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "text_model.transformer.h.{d}." ++ suffix, .{layer}) catch error.WeightNameTooLong;
    }

    pub fn vision(buf: []u8, layer: usize, comptime suffix: []const u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "vision_encoder.encoder.model.visual.blocks.{d}." ++ suffix, .{layer}) catch error.WeightNameTooLong;
    }
};

/// One expected checkpoint tensor and its shape, for load-time validation.
pub const ExpectedTensor = struct {
    name: []u8,
    shape: [3]usize,
    rank: u8,
};

/// Every tensor the runtime reads, with its expected shape under `config`.
/// Caller frees each name and the slice.
pub fn expectedTensors(allocator: std.mem.Allocator, config: Config) ![]ExpectedTensor {
    var list = std.ArrayListUnmanaged(ExpectedTensor).empty;
    errdefer {
        for (list.items) |item| allocator.free(item.name);
        list.deinit(allocator);
    }
    const d = config.hidden_size;
    const vd = config.vision_hidden_size;
    const Add = struct {
        fn one(a: std.mem.Allocator, l: *std.ArrayListUnmanaged(ExpectedTensor), name: []const u8, shape: []const usize) !void {
            var entry = ExpectedTensor{ .name = try a.dupe(u8, name), .shape = .{ 0, 0, 0 }, .rank = @intCast(shape.len) };
            errdefer a.free(entry.name);
            for (shape, 0..) |dim, i| entry.shape[i] = dim;
            try l.append(a, entry);
        }
    };
    try Add.one(allocator, &list, names.token_embedding, &.{ config.vocab_size, d });
    try Add.one(allocator, &list, names.lm_head_norm_weight, &.{d});
    try Add.one(allocator, &list, names.lm_head_norm_bias, &.{d});
    try Add.one(allocator, &list, names.lm_head_weight, &.{ config.vocab_size, d });
    try Add.one(allocator, &list, names.lm_head_bias, &.{config.vocab_size});
    var buf: [160]u8 = undefined;
    for (0..config.num_layers) |layer| {
        try Add.one(allocator, &list, try names.text(&buf, layer, "ln.weight"), &.{d});
        try Add.one(allocator, &list, try names.text(&buf, layer, "ln.bias"), &.{d});
        try Add.one(allocator, &list, try names.text(&buf, layer, "mixer.Wqkv.weight"), &.{ 3 * d, d });
        try Add.one(allocator, &list, try names.text(&buf, layer, "mixer.Wqkv.bias"), &.{3 * d});
        try Add.one(allocator, &list, try names.text(&buf, layer, "mixer.out_proj.weight"), &.{ d, d });
        try Add.one(allocator, &list, try names.text(&buf, layer, "mixer.out_proj.bias"), &.{d});
        try Add.one(allocator, &list, try names.text(&buf, layer, "mlp.fc1.weight"), &.{ config.intermediate_size, d });
        try Add.one(allocator, &list, try names.text(&buf, layer, "mlp.fc1.bias"), &.{config.intermediate_size});
        try Add.one(allocator, &list, try names.text(&buf, layer, "mlp.fc2.weight"), &.{ d, config.intermediate_size });
        try Add.one(allocator, &list, try names.text(&buf, layer, "mlp.fc2.bias"), &.{d});
    }
    try Add.one(allocator, &list, names.patch_embed_weight, &.{ vd, config.patchDim() });
    try Add.one(allocator, &list, names.patch_embed_bias, &.{vd});
    try Add.one(allocator, &list, names.position_embedding, &.{ 1, config.imageTokens(), vd });
    try Add.one(allocator, &list, names.vision_norm_weight, &.{vd});
    try Add.one(allocator, &list, names.vision_norm_bias, &.{vd});
    for (0..config.vision_num_layers) |layer| {
        try Add.one(allocator, &list, try names.vision(&buf, layer, "norm1.weight"), &.{vd});
        try Add.one(allocator, &list, try names.vision(&buf, layer, "norm1.bias"), &.{vd});
        try Add.one(allocator, &list, try names.vision(&buf, layer, "norm2.weight"), &.{vd});
        try Add.one(allocator, &list, try names.vision(&buf, layer, "norm2.bias"), &.{vd});
        try Add.one(allocator, &list, try names.vision(&buf, layer, "attn.qkv.weight"), &.{ 3 * vd, vd });
        try Add.one(allocator, &list, try names.vision(&buf, layer, "attn.qkv.bias"), &.{3 * vd});
        try Add.one(allocator, &list, try names.vision(&buf, layer, "attn.proj.weight"), &.{ vd, vd });
        try Add.one(allocator, &list, try names.vision(&buf, layer, "attn.proj.bias"), &.{vd});
        try Add.one(allocator, &list, try names.vision(&buf, layer, "mlp.fc1.weight"), &.{ config.vision_intermediate_size, vd });
        try Add.one(allocator, &list, try names.vision(&buf, layer, "mlp.fc1.bias"), &.{config.vision_intermediate_size});
        try Add.one(allocator, &list, try names.vision(&buf, layer, "mlp.fc2.weight"), &.{ vd, config.vision_intermediate_size });
        try Add.one(allocator, &list, try names.vision(&buf, layer, "mlp.fc2.bias"), &.{vd});
    }
    try Add.one(allocator, &list, names.projector_fc1_weight, &.{ config.projector_hidden_size, vd });
    try Add.one(allocator, &list, names.projector_fc1_bias, &.{config.projector_hidden_size});
    try Add.one(allocator, &list, names.projector_fc2_weight, &.{ d, config.projector_hidden_size });
    try Add.one(allocator, &list, names.projector_fc2_bias, &.{d});
    return list.toOwnedSlice(allocator);
}

pub fn freeExpectedTensors(allocator: std.mem.Allocator, tensors: []ExpectedTensor) void {
    for (tensors) |item| allocator.free(item.name);
    allocator.free(tensors);
}

// --- Vision tower and projector ------------------------------------------

/// Rearrange one normalized CHW image into timm's linear-patch layout:
/// `b c (h p1) (w p2) -> b (h w) (c p1 p2)`. Output is [grid*grid, 3*p*p].
pub fn patchify(allocator: std.mem.Allocator, config: Config, pixels_chw: []const f32) ![]f32 {
    const size = config.image_size;
    const p = config.patch_size;
    const grid = config.patchGrid();
    if (pixels_chw.len != 3 * size * size) return error.InvalidImageShape;
    const patch_dim = config.patchDim();
    const out = try allocator.alloc(f32, grid * grid * patch_dim);
    for (0..grid) |gy| {
        for (0..grid) |gx| {
            const row = (gy * grid + gx) * patch_dim;
            var idx: usize = 0;
            for (0..3) |c| {
                for (0..p) |py| {
                    const src = c * size * size + (gy * p + py) * size + gx * p;
                    @memcpy(out[row + idx ..][0..p], pixels_chw[src..][0..p]);
                    idx += p;
                }
            }
        }
    }
    return out;
}

/// Encode one preprocessed 378x378 image into projected decoder embeddings,
/// returned as host rows [imageTokens, hidden_size]. `host_attention` runs
/// the bidirectional attention with the head-parallel host kernel (CPU
/// backends); otherwise the backend's `scaledDotProductAttention` is used.
pub fn encodeImage(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    pixels_chw: []const f32,
    host_attention: bool,
) ![]f32 {
    const tokens = config.imageTokens();
    const vd = config.vision_hidden_size;

    const patches = try patchify(allocator, config, pixels_chw);
    defer allocator.free(patches);
    const patch_shape = [_]i32{ @intCast(tokens), @intCast(config.patchDim()) };
    const patches_ct = try cb.fromFloat32Shape(patches, &patch_shape);
    defer cb.free(patches_ct);

    var hidden = blk: {
        const w = try cb.getWeight(names.patch_embed_weight);
        defer cb.free(w);
        const b = try cb.getWeight(names.patch_embed_bias);
        defer cb.free(b);
        const embedded = try cb.linear(patches_ct, w, b, tokens, config.patchDim(), vd);
        defer cb.free(embedded);
        const host = try cb.toFloat32(embedded, allocator);
        defer allocator.free(host);
        const pos = try cb.getWeight(names.position_embedding);
        defer cb.free(pos);
        const pos_host = try cb.toFloat32(pos, allocator);
        defer allocator.free(pos_host);
        if (pos_host.len != host.len) return error.InvalidPositionEmbeddingShape;
        for (host, pos_host) |*x, p| x.* += p;
        const shape = [_]i32{ @intCast(tokens), @intCast(vd) };
        break :blk try cb.fromFloat32Shape(host, &shape);
    };
    errdefer cb.free(hidden);

    for (0..config.vision_num_layers) |layer| {
        try cb.checkExecutionControl();
        const next = try visionBlock(cb, allocator, config, hidden, tokens, layer, host_attention);
        cb.free(hidden);
        hidden = next;
    }

    {
        const g = try cb.getWeight(names.vision_norm_weight);
        defer cb.free(g);
        const b = try cb.getWeight(names.vision_norm_bias);
        defer cb.free(b);
        const normed = try cb.layerNorm(hidden, g, b, vd, config.vision_layer_norm_eps);
        cb.free(hidden);
        hidden = normed;
    }
    defer cb.free(hidden);

    const fc1_w = try cb.getWeight(names.projector_fc1_weight);
    defer cb.free(fc1_w);
    const fc1_b = try cb.getWeight(names.projector_fc1_bias);
    defer cb.free(fc1_b);
    const up = try cb.linear(hidden, fc1_w, fc1_b, tokens, vd, config.projector_hidden_size);
    defer cb.free(up);
    const act = try geluExact(cb, allocator, up);
    defer cb.free(act);
    const fc2_w = try cb.getWeight(names.projector_fc2_weight);
    defer cb.free(fc2_w);
    const fc2_b = try cb.getWeight(names.projector_fc2_bias);
    defer cb.free(fc2_b);
    const projected = try cb.linear(act, fc2_w, fc2_b, tokens, config.projector_hidden_size, config.hidden_size);
    defer cb.free(projected);
    return cb.toFloat32(projected, allocator);
}

fn visionBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    tokens: usize,
    layer: usize,
    host_attention: bool,
) !CT {
    const vd = config.vision_hidden_size;
    var buf: [160]u8 = undefined;

    const normed1 = blk: {
        const g = try cb.getWeight(try names.vision(&buf, layer, "norm1.weight"));
        defer cb.free(g);
        const b = try cb.getWeight(try names.vision(&buf, layer, "norm1.bias"));
        defer cb.free(b);
        break :blk try cb.layerNorm(input, g, b, vd, config.vision_layer_norm_eps);
    };
    defer cb.free(normed1);

    const attn_out = blk: {
        const qkv_w = try cb.getWeight(try names.vision(&buf, layer, "attn.qkv.weight"));
        defer cb.free(qkv_w);
        const qkv_b = try cb.getWeight(try names.vision(&buf, layer, "attn.qkv.bias"));
        defer cb.free(qkv_b);
        const qkv = try cb.linear(normed1, qkv_w, qkv_b, tokens, vd, 3 * vd);
        defer cb.free(qkv);
        const proj_w = try cb.getWeight(try names.vision(&buf, layer, "attn.proj.weight"));
        defer cb.free(proj_w);
        const proj_b = try cb.getWeight(try names.vision(&buf, layer, "attn.proj.bias"));
        defer cb.free(proj_b);
        if (host_attention) {
            const host = try cb.toFloat32(qkv, allocator);
            defer allocator.free(host);
            const parts = try allocator.alloc(f32, 3 * tokens * vd);
            defer allocator.free(parts);
            for (0..tokens) |r| {
                for (0..3) |part| @memcpy(parts[part * tokens * vd + r * vd ..][0..vd], host[r * 3 * vd + part * vd ..][0..vd]);
            }
            const out = try attentionHostParallel(allocator, parts[0 .. tokens * vd], parts[tokens * vd ..][0 .. tokens * vd], parts[2 * tokens * vd ..][0 .. tokens * vd], tokens, config.vision_num_heads, config.visionHeadDim(), false);
            defer allocator.free(out);
            const shape = [_]i32{ @intCast(tokens), @intCast(vd) };
            const attn = try cb.fromFloat32Shape(out, &shape);
            defer cb.free(attn);
            break :blk try cb.linear(attn, proj_w, proj_b, tokens, vd, vd);
        }
        const split = try cb.splitLastDim3(allocator, qkv, tokens, vd);
        defer {
            cb.free(split.first);
            cb.free(split.second);
            cb.free(split.third);
        }
        const mask = try allocator.alloc(i64, tokens);
        defer allocator.free(mask);
        @memset(mask, 1);
        const attn = try cb.scaledDotProductAttention(split.first, split.second, split.third, mask, null, 1, tokens, config.vision_num_heads, config.visionHeadDim());
        defer cb.free(attn);
        break :blk try cb.linear(attn, proj_w, proj_b, tokens, vd, vd);
    };
    defer cb.free(attn_out);
    const residual1 = try cb.add(input, attn_out);
    errdefer cb.free(residual1);

    const normed2 = blk: {
        const g = try cb.getWeight(try names.vision(&buf, layer, "norm2.weight"));
        defer cb.free(g);
        const b = try cb.getWeight(try names.vision(&buf, layer, "norm2.bias"));
        defer cb.free(b);
        break :blk try cb.layerNorm(residual1, g, b, vd, config.vision_layer_norm_eps);
    };
    defer cb.free(normed2);

    const mlp_out = blk: {
        const fc1_w = try cb.getWeight(try names.vision(&buf, layer, "mlp.fc1.weight"));
        defer cb.free(fc1_w);
        const fc1_b = try cb.getWeight(try names.vision(&buf, layer, "mlp.fc1.bias"));
        defer cb.free(fc1_b);
        const up = try cb.linear(normed2, fc1_w, fc1_b, tokens, vd, config.vision_intermediate_size);
        defer cb.free(up);
        const act = try geluExact(cb, allocator, up);
        defer cb.free(act);
        const fc2_w = try cb.getWeight(try names.vision(&buf, layer, "mlp.fc2.weight"));
        defer cb.free(fc2_w);
        const fc2_b = try cb.getWeight(try names.vision(&buf, layer, "mlp.fc2.bias"));
        defer cb.free(fc2_b);
        break :blk try cb.linear(act, fc2_w, fc2_b, tokens, config.vision_intermediate_size, vd);
    };
    defer cb.free(mlp_out);

    const out = try cb.add(residual1, mlp_out);
    cb.free(residual1);
    return out;
}

/// timm's SigLIP blocks and the projector use `nn.GELU()` (erf). Backends
/// without the exact kernel compute it on the host.
fn geluExact(cb: *const ComputeBackend, allocator: std.mem.Allocator, input: CT) !CT {
    if (try cb.geluExact(input)) |out| return out;
    const host = try cb.toFloat32(input, allocator);
    defer allocator.free(host);
    for (host) |*x| x.* = 0.5 * x.* * (1.0 + erf(x.* / std.math.sqrt2));
    return cb.fromFloat32(host);
}

/// Abramowitz-Stegun 7.1.26 (|error| < 1.5e-7), the same approximation the
/// native exact-GELU kernel uses.
fn erf(x: f32) f32 {
    const sign: f32 = if (x < 0) -1.0 else 1.0;
    const ax = @abs(x);
    const t = 1.0 / (1.0 + 0.3275911 * ax);
    const poly = (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t;
    return sign * (1.0 - poly * @exp(-(ax * ax)));
}

// --- Phi decoder ----------------------------------------------------------

/// Paged KV state for one decoded sequence. Positions are contiguous from 0.
pub const KvState = struct {
    manager: runtime.kv.manager.KvManager,
    pool_id: runtime.kv.block.KvPoolId,
    sequence_id: runtime.kv.manager.SequenceId,
    /// Host backends compute prefill attention with the head-parallel host
    /// flash kernel below; device backends use `gqaCausalAttention`.
    host_prefill_attention: bool = false,

    pub fn init(allocator: std.mem.Allocator, config: Config) !KvState {
        var manager = runtime.kv.manager.KvManager.init(allocator);
        errdefer manager.deinit();
        const pool_id = try manager.addPool(.{
            .backend = .native,
            .dtype = .f32,
            .page_size_tokens = 16,
            .num_layers_packed = @intCast(config.num_layers),
            .num_kv_heads = @intCast(config.num_heads),
            .head_dim = @intCast(config.headDim()),
        });
        const sequence_id = try manager.attachSequence(pool_id);
        return .{ .manager = manager, .pool_id = pool_id, .sequence_id = sequence_id };
    }

    pub fn deinit(self: *KvState) void {
        self.manager.deinit();
    }

    pub fn tokenCount(self: *KvState) usize {
        return self.manager.tokenCount(self.sequence_id) orelse 0;
    }

    fn view(self: *KvState) !ops.KvCacheView {
        const table = self.manager.blockTable(self.sequence_id) orelse return error.InvalidSequenceId;
        return .{
            .sequence_id = self.sequence_id,
            .pool_id = self.pool_id,
            .logical_block_count = table.len(),
            .tail_tokens = table.tail_tokens,
            .position_offset = 0,
        };
    }
};

/// Inverse rotary frequencies, `1 / theta^(2j / rotary_dim)` in f32 as in
/// phi-msft's RotaryEmbedding.
pub fn rotaryInverseFrequencies(out: []f32, rotary_dim: usize, theta: f32) void {
    for (out, 0..) |*f, j| {
        const exponent = @as(f32, @floatFromInt(2 * j)) / @as(f32, @floatFromInt(rotary_dim));
        f.* = 1.0 / std.math.pow(f32, theta, exponent);
    }
}

/// Phi-msft partial NeoX rotary in place on rows of [num_heads * head_dim]:
/// the first `rotary_dim` dims of each head rotate as pairs (j, j + rotary_dim/2);
/// the remaining head dims pass through.
pub fn applyPartialRotary(
    data: []f32,
    rows: usize,
    num_heads: usize,
    head_dim: usize,
    rotary_dim: usize,
    theta: f32,
    position_offset: usize,
) !void {
    if (data.len != rows * num_heads * head_dim) return error.InvalidRoPEInput;
    const half = rotary_dim / 2;
    var inv_freq_buf: [256]f32 = undefined;
    if (half > inv_freq_buf.len) return error.InvalidRoPEInput;
    const inv_freq = inv_freq_buf[0..half];
    rotaryInverseFrequencies(inv_freq, rotary_dim, theta);
    var cos_buf: [256]f32 = undefined;
    var sin_buf: [256]f32 = undefined;
    for (0..rows) |r| {
        const pos: f32 = @floatFromInt(position_offset + r);
        for (0..half) |j| {
            const angle = pos * inv_freq[j];
            cos_buf[j] = @cos(angle);
            sin_buf[j] = @sin(angle);
        }
        for (0..num_heads) |h| {
            const base = (r * num_heads + h) * head_dim;
            for (0..half) |j| {
                const x1 = data[base + j];
                const x2 = data[base + j + half];
                data[base + j] = x1 * cos_buf[j] - x2 * sin_buf[j];
                data[base + j + half] = x1 * sin_buf[j] + x2 * cos_buf[j];
            }
        }
    }
}

/// Run the decoder over `rows` new input embeddings appended to `kv`, and
/// return the next-token logits for the last row (host, [vocab_size]).
pub fn forwardLastLogits(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    embeddings: []const f32,
    rows: usize,
    kv: *KvState,
) ![]f32 {
    const d = config.hidden_size;
    if (rows == 0 or embeddings.len != rows * d) return error.InvalidEmbeddingShape;
    const past = kv.tokenCount();
    if (past + rows > config.max_position_embeddings) return error.PromptTooLong;
    try kv.manager.appendTokens(kv.sequence_id, @intCast(rows));
    const total = kv.tokenCount();

    const shape = [_]i32{ @intCast(rows), @intCast(d) };
    var hidden = try cb.fromFloat32Shape(embeddings, &shape);
    errdefer cb.free(hidden);

    for (0..config.num_layers) |layer| {
        try cb.checkExecutionControl();
        const next = try decoderBlock(cb, allocator, config, hidden, rows, past, total, layer, kv);
        cb.free(hidden);
        hidden = next;
    }
    defer cb.free(hidden);

    const last = if (rows == 1) hidden else try cb.sliceRows2D(allocator, hidden, rows - 1, 1, d);
    defer if (rows != 1) cb.free(last);

    const g = try cb.getWeight(names.lm_head_norm_weight);
    defer cb.free(g);
    const b = try cb.getWeight(names.lm_head_norm_bias);
    defer cb.free(b);
    const normed = try cb.layerNorm(last, g, b, d, config.layer_norm_eps);
    defer cb.free(normed);
    const w = try cb.getWeight(names.lm_head_weight);
    defer cb.free(w);
    const bias = try cb.getWeight(names.lm_head_bias);
    defer cb.free(bias);
    const logits = try cb.linear(normed, w, bias, 1, d, config.vocab_size);
    defer cb.free(logits);
    return cb.toFloat32(logits, allocator);
}

fn decoderBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    rows: usize,
    past: usize,
    total: usize,
    layer: usize,
    kv: *KvState,
) !CT {
    const d = config.hidden_size;
    const head_dim = config.headDim();
    var buf: [160]u8 = undefined;

    const normed = blk: {
        const g = try cb.getWeight(try names.text(&buf, layer, "ln.weight"));
        defer cb.free(g);
        const b = try cb.getWeight(try names.text(&buf, layer, "ln.bias"));
        defer cb.free(b);
        break :blk try cb.layerNorm(input, g, b, d, config.layer_norm_eps);
    };
    defer cb.free(normed);

    const attn_out = blk: {
        const qkv_w = try cb.getWeight(try names.text(&buf, layer, "mixer.Wqkv.weight"));
        defer cb.free(qkv_w);
        const qkv_b = try cb.getWeight(try names.text(&buf, layer, "mixer.Wqkv.bias"));
        defer cb.free(qkv_b);
        const qkv = try cb.linear(normed, qkv_w, qkv_b, rows, d, 3 * d);
        defer cb.free(qkv);

        // Split on the host and apply Phi's rotary layout to q and k.
        const host = try cb.toFloat32(qkv, allocator);
        defer allocator.free(host);
        if (host.len != rows * 3 * d) return error.UnexpectedOutputShape;
        const q = try allocator.alloc(f32, rows * d);
        defer allocator.free(q);
        const k = try allocator.alloc(f32, rows * d);
        defer allocator.free(k);
        const v = try allocator.alloc(f32, rows * d);
        defer allocator.free(v);
        for (0..rows) |r| {
            const src = host[r * 3 * d ..];
            @memcpy(q[r * d ..][0..d], src[0..d]);
            @memcpy(k[r * d ..][0..d], src[d..][0..d]);
            @memcpy(v[r * d ..][0..d], src[2 * d ..][0..d]);
        }
        try applyPartialRotary(q, rows, config.num_heads, head_dim, config.rotary_dim, config.rope_theta, past);
        try applyPartialRotary(k, rows, config.num_heads, head_dim, config.rotary_dim, config.rope_theta, past);
        const shape = [_]i32{ @intCast(rows), @intCast(d) };
        const q_ct = try cb.fromFloat32Shape(q, &shape);
        defer cb.free(q_ct);
        const k_ct = try cb.fromFloat32Shape(k, &shape);
        defer cb.free(k_ct);
        const v_ct = try cb.fromFloat32Shape(v, &shape);
        defer cb.free(v_ct);

        const kv_tokens = kv.tokenCount();
        // Prefill from an empty cache is a square causal block: write the
        // rows into the paged cache and use the dense flash kernel, which is
        // much faster than per-token paged reads for hundreds of queries.
        const attn = if (past == 0) blk_prefill: {
            try kv.manager.writeLayerKvSuffix(kv.sequence_id, layer, total, rows, k, v);
            if (kv.host_prefill_attention) {
                const out = try causalAttentionHostParallel(allocator, q, k, v, rows, config.num_heads, head_dim);
                defer allocator.free(out);
                break :blk_prefill try cb.fromFloat32Shape(out, &shape);
            }
            break :blk_prefill try cb.gqaCausalAttention(q_ct, k_ct, v_ct, null, 1, rows, config.num_heads, config.num_heads, head_dim);
        } else try cb.gqaPagedAttention(q_ct, k_ct, v_ct, null, .{
            .mode = .paged_decode,
            .total_sequence_len = total,
            .query_sequence_len = rows,
            .kv_sequence_len = kv_tokens,
            .kv_position_offset = total - kv_tokens,
            .kv_cache = try kv.view(),
            .kv_manager = &kv.manager,
            .layer_index = layer,
        }, 1, config.num_heads, config.num_heads, head_dim);
        defer cb.free(attn);

        const out_w = try cb.getWeight(try names.text(&buf, layer, "mixer.out_proj.weight"));
        defer cb.free(out_w);
        const out_b = try cb.getWeight(try names.text(&buf, layer, "mixer.out_proj.bias"));
        defer cb.free(out_b);
        break :blk try cb.linear(attn, out_w, out_b, rows, d, d);
    };
    defer cb.free(attn_out);

    const mlp_out = blk: {
        const fc1_w = try cb.getWeight(try names.text(&buf, layer, "mlp.fc1.weight"));
        defer cb.free(fc1_w);
        const fc1_b = try cb.getWeight(try names.text(&buf, layer, "mlp.fc1.bias"));
        defer cb.free(fc1_b);
        const up = try cb.linear(normed, fc1_w, fc1_b, rows, d, config.intermediate_size);
        defer cb.free(up);
        const act = try cb.geluNew(up);
        defer cb.free(act);
        const fc2_w = try cb.getWeight(try names.text(&buf, layer, "mlp.fc2.weight"));
        defer cb.free(fc2_w);
        const fc2_b = try cb.getWeight(try names.text(&buf, layer, "mlp.fc2.bias"));
        defer cb.free(fc2_b);
        break :blk try cb.linear(act, fc2_w, fc2_b, rows, config.intermediate_size, d);
    };
    defer cb.free(mlp_out);

    const partial = try cb.add(attn_out, mlp_out);
    defer cb.free(partial);
    return cb.add(input, partial);
}

/// Square causal attention over token-major [rows, heads * head_dim] host
/// rows, split across threads by head. Each worker runs the shared tiled
/// flash kernel on its heads, so results equal a single-threaded call.
pub fn causalAttentionHostParallel(
    allocator: std.mem.Allocator,
    q: []const f32,
    k: []const f32,
    v: []const f32,
    rows: usize,
    num_heads: usize,
    head_dim: usize,
) ![]f32 {
    return attentionHostParallel(allocator, q, k, v, rows, num_heads, head_dim, true);
}

/// `causal = false` gives full bidirectional attention (the vision tower)
/// through the same kernel by placing every query after every key.
pub fn attentionHostParallel(
    allocator: std.mem.Allocator,
    q: []const f32,
    k: []const f32,
    v: []const f32,
    rows: usize,
    num_heads: usize,
    head_dim: usize,
    causal: bool,
) ![]f32 {
    const width = num_heads * head_dim;
    if (q.len != rows * width or k.len != rows * width or v.len != rows * width) return error.InvalidAttentionShape;
    const output = try allocator.alloc(f32, rows * width);
    errdefer allocator.free(output);
    const cpus = std.Thread.getCpuCount() catch 1;
    const workers = @max(@as(usize, 1), @min(@min(cpus, 8), num_heads));

    const Worker = struct {
        q: []const f32,
        k: []const f32,
        v: []const f32,
        out: []f32,
        rows: usize,
        width: usize,
        head_dim: usize,
        head_start: usize,
        head_end: usize,
        query_offset: usize,
        failed: bool = false,

        fn run(self: *@This()) void {
            self.runInner() catch {
                self.failed = true;
            };
        }

        fn runInner(self: *@This()) !void {
            const a = std.heap.page_allocator;
            const heads = self.head_end - self.head_start;
            const sub = heads * self.head_dim;
            const col = self.head_start * self.head_dim;
            const packed_buf = try a.alloc(f32, 3 * self.rows * sub);
            defer a.free(packed_buf);
            const pq = packed_buf[0 .. self.rows * sub];
            const pk = packed_buf[self.rows * sub ..][0 .. self.rows * sub];
            const pv = packed_buf[2 * self.rows * sub ..][0 .. self.rows * sub];
            for (0..self.rows) |r| {
                @memcpy(pq[r * sub ..][0..sub], self.q[r * self.width + col ..][0..sub]);
                @memcpy(pk[r * sub ..][0..sub], self.k[r * self.width + col ..][0..sub]);
                @memcpy(pv[r * sub ..][0..sub], self.v[r * self.width + col ..][0..sub]);
            }
            const out = try linalg.flashCausalAttentionHost(a, pq, pk, pv, null, null, 0, 1, self.rows, self.rows, self.query_offset, 0, heads, heads, self.head_dim);
            defer a.free(out);
            for (0..self.rows) |r| @memcpy(self.out[r * self.width + col ..][0..sub], out[r * sub ..][0..sub]);
        }
    };

    var states: [8]Worker = undefined;
    var threads: [8]?std.Thread = @splat(null);
    for (0..workers) |w| {
        states[w] = .{
            .q = q,
            .k = k,
            .v = v,
            .out = output,
            .rows = rows,
            .width = width,
            .head_dim = head_dim,
            .head_start = num_heads * w / workers,
            .head_end = num_heads * (w + 1) / workers,
            .query_offset = if (causal) 0 else rows,
        };
    }
    // Worker 0 runs on the calling thread; spawn failures fall back inline.
    for (1..workers) |w| threads[w] = std.Thread.spawn(.{}, Worker.run, .{&states[w]}) catch null;
    states[0].run();
    for (1..workers) |w| {
        if (threads[w]) |t| t.join() else states[w].run();
    }
    for (states[0..workers]) |state| if (state.failed) return error.OutOfMemory;
    return output;
}

// --- Tests ----------------------------------------------------------------

test "parseConfig accepts the original phi-msft stub and fills Moondream2 defaults" {
    const config = try parseConfig(std.testing.allocator,
        \\{"architectures":["Moondream"],"model_type":"moondream1","phi_config":{"model_type":"phi-msft"},"torch_dtype":"float16"}
    );
    try std.testing.expectEqual(@as(usize, 51200), config.vocab_size);
    try std.testing.expectEqual(@as(usize, 2048), config.hidden_size);
    try std.testing.expectEqual(@as(usize, 24), config.num_layers);
    try std.testing.expectEqual(@as(usize, 64), config.headDim());
    try std.testing.expectEqual(@as(usize, 32), config.rotary_dim);
    try std.testing.expectEqual(@as(usize, 729), config.imageTokens());
    try std.testing.expectEqual(@as(usize, 588), config.patchDim());
    try std.testing.expectEqual(@as(usize, 72), config.visionHeadDim());
}

test "parseConfig reads the expanded transformers.js PhiConfig" {
    const config = try parseConfig(std.testing.allocator,
        \\{"model_type":"moondream1","phi_config":{"hidden_size":2048,"num_hidden_layers":24,
        \\"num_attention_heads":32,"intermediate_size":8192,"partial_rotary_factor":0.5,
        \\"layer_norm_eps":1e-05,"rope_theta":10000.0,"vocab_size":51200,"max_position_embeddings":2048}}
    );
    try std.testing.expectEqual(@as(usize, 32), config.rotary_dim);
    try std.testing.expectEqual(@as(usize, 8192), config.intermediate_size);
    try std.testing.expect(!isMoondreamConfig(std.testing.allocator, "{\"model_type\":\"phi\"}"));
    try std.testing.expect(isMoondreamConfig(std.testing.allocator, "{\"model_type\":\"moondream1\"}"));
    try std.testing.expectError(error.InvalidMoondreamConfig, parseConfig(std.testing.allocator, "{\"model_type\":\"phi\"}"));
}

test "expected tensor inventory matches the 2024-03-04 checkpoint" {
    const tensors = try expectedTensors(std.testing.allocator, .{});
    defer freeExpectedTensors(std.testing.allocator, tensors);
    // 5 head/embedding + 24*10 text + 5 vision stem/norm + 27*12 vision + 4 projector.
    try std.testing.expectEqual(@as(usize, 578), tensors.len);
    for (tensors) |t| {
        if (std.mem.eql(u8, t.name, "text_model.transformer.h.23.mixer.Wqkv.weight")) {
            try std.testing.expectEqual(@as(usize, 6144), t.shape[0]);
            try std.testing.expectEqual(@as(usize, 2048), t.shape[1]);
        }
    }
}

test "patchify uses timm (c p1 p2) order" {
    const config = Config{ .image_size = 4, .patch_size = 2 };
    var pixels: [3 * 4 * 4]f32 = undefined;
    for (&pixels, 0..) |*p, i| p.* = @floatFromInt(i);
    const out = try patchify(std.testing.allocator, config, &pixels);
    defer std.testing.allocator.free(out);
    try std.testing.expectEqual(@as(usize, 4 * 12), out.len);
    // Patch (0,1): channel 0 rows 0..1 cols 2..3, then channel 1, channel 2.
    const patch1 = out[12..24];
    try std.testing.expectEqualSlices(f32, &.{ 2, 3, 6, 7, 18, 19, 22, 23, 34, 35, 38, 39 }, patch1);
}

test "partial rotary rotates only the first rotary_dim dims with phi pairing" {
    // One row, one head of dim 8, rotary_dim 4: pairs (0,2) and (1,3); 4..7 pass.
    var data = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8 };
    try applyPartialRotary(&data, 1, 1, 8, 4, 10000.0, 0);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, &data);

    var moved = [_]f32{ 1, 0, 0, 0, 5, 6, 7, 8 };
    try applyPartialRotary(&moved, 1, 1, 8, 4, 10000.0, 1);
    // Pair (0,2) uses inv_freq[0] = 1: (cos 1, sin 1).
    try std.testing.expectApproxEqAbs(@cos(@as(f32, 1.0)), moved[0], 1e-6);
    try std.testing.expectApproxEqAbs(@sin(@as(f32, 1.0)), moved[2], 1e-6);
    try std.testing.expectEqual(@as(f32, 0), moved[1]);
    try std.testing.expectEqual(@as(f32, 0), moved[3]);
    try std.testing.expectEqualSlices(f32, &.{ 5, 6, 7, 8 }, moved[4..]);
}

test "rotary inverse frequencies follow phi-msft" {
    var inv: [16]f32 = undefined;
    rotaryInverseFrequencies(&inv, 32, 10000.0);
    try std.testing.expectEqual(@as(f32, 1.0), inv[0]);
    try std.testing.expectApproxEqRel(@as(f32, 1.0) / std.math.pow(f32, 10000.0, 30.0 / 32.0), inv[15], 1e-6);
}

test "head-parallel host causal attention matches the single-threaded kernel" {
    const a = std.testing.allocator;
    const rows = 37;
    const heads = 5;
    const hd = 8;
    const n = rows * heads * hd;
    const q = try a.alloc(f32, n);
    defer a.free(q);
    const k = try a.alloc(f32, n);
    defer a.free(k);
    const v = try a.alloc(f32, n);
    defer a.free(v);
    for (q, k, v, 0..) |*x, *y, *z, i| {
        x.* = @as(f32, @floatFromInt(i % 13)) / 13.0 - 0.5;
        y.* = @as(f32, @floatFromInt(i % 7)) / 7.0 - 0.5;
        z.* = @as(f32, @floatFromInt(i % 11)) / 11.0;
    }
    const want = try linalg.flashCausalAttentionHost(a, q, k, v, null, null, 0, 1, rows, rows, 0, 0, heads, heads, hd);
    defer a.free(want);
    const got = try causalAttentionHostParallel(a, q, k, v, rows, heads, hd);
    defer a.free(got);
    try std.testing.expectEqualSlices(f32, want, got);
}

test "bidirectional host attention averages over every key" {
    const a = std.testing.allocator;
    // Two rows, one head of dim 1, identical keys: uniform weights, so each
    // output is the mean of v regardless of position.
    const q = [_]f32{ 1, 1 };
    const k = [_]f32{ 0, 0 };
    const v = [_]f32{ 2, 4 };
    const full = try attentionHostParallel(a, &q, &k, &v, 2, 1, 1, false);
    defer a.free(full);
    try std.testing.expectApproxEqAbs(@as(f32, 3), full[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 3), full[1], 1e-6);
    const causal = try causalAttentionHostParallel(a, &q, &k, &v, 2, 1, 1);
    defer a.free(causal);
    try std.testing.expectApproxEqAbs(@as(f32, 2), causal[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 3), causal[1], 1e-6);
}
