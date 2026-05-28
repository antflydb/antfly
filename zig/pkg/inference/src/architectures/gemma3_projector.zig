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
const ops = @import("../ops/ops.zig");
const gguf_metadata = @import("../gguf/metadata.zig");
const gguf_format = @import("../gguf/format.zig");
const gguf_mod = @import("../gguf/root.zig");
const tensor_store_mod = @import("../models/tensor_store.zig");
const weight_source_mod = @import("../models/weight_source.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const compat = @import("../io/compat.zig");
const gpt_mod = @import("../models/gpt.zig");
const projector_format_mod = @import("projector_format.zig");

const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;

const ProjectorConfig = struct {
    text_hidden: usize,
    vision_hidden: usize,
    intermediate_size: usize,
    block_count: usize,
    head_count: usize,
    image_size: usize,
    patch_size: usize,
    mm_tokens_per_image: usize,
};

const LoadedF32 = struct {
    weight: weight_source_mod.LoadedWeight,
    converted: ?Tensor = null,
    data: []const f32,
    shape: []const i64,

    fn deinit(self: *LoadedF32) void {
        if (self.converted) |*converted| converted.deinit();
        self.weight.deinit();
    }
};

pub fn isSupportedProjectorPath(allocator: std.mem.Allocator, projector_path: []const u8) !bool {
    return try projector_format_mod.detectPath(allocator, projector_path) == .termite_gemma3;
}

pub fn isSupportedProjectorFile(file: *const gguf_format.File) bool {
    return projector_format_mod.detectFile(file) == .termite_gemma3;
}

pub fn encodeProjectedImageTokens(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    projector_path: []const u8,
    runtime_cfg: gpt_mod.Config,
    pixel_values: []const f32,
    batch: usize,
) ![]f32 {
    var store = try tensor_store_mod.GgufStore.initAbsolute(allocator, projector_path);
    defer store.tensorStore().deinit();

    return encodeProjectedImageTokensFromStore(cb, allocator, store, runtime_cfg, pixel_values, batch);
}

pub fn encodeProjectedImageTokensFromStore(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    runtime_cfg: gpt_mod.Config,
    pixel_values: []const f32,
    batch: usize,
) ![]f32 {
    const cfg = try parseProjectorConfig(&store.parsed, runtime_cfg);
    const vision_hidden = cfg.vision_hidden;
    const patch_size = cfg.patch_size;
    const image_size = cfg.image_size;
    const grid = image_size / patch_size;
    const num_patches = grid * grid;
    const patch_dim = 3 * patch_size * patch_size;

    const patch_embeddings = try patchEmbed(store, cb, allocator, pixel_values, batch, patch_size, image_size, grid, patch_dim, vision_hidden);
    defer allocator.free(patch_embeddings);

    const positioned = try addPositionEmbeddings(store, cb, allocator, patch_embeddings, batch, num_patches, vision_hidden);
    defer allocator.free(positioned);

    const hidden_shape = [_]i32{ @intCast(batch * num_patches), @intCast(vision_hidden) };
    var hidden = try cb.fromFloat32Shape(positioned, &hidden_shape);
    defer cb.free(hidden);

    for (0..cfg.block_count) |layer| {
        const next_hidden = try encoderBlock(store, cb, allocator, hidden, batch, num_patches, vision_hidden, cfg.head_count, cfg.intermediate_size, layer);
        cb.free(hidden);
        hidden = next_hidden;
    }

    {
        const gamma = try loadWeightCt(cb, allocator, store, "vision_tower.vision_model.post_layernorm.weight");
        defer cb.free(gamma);
        const beta = try loadWeightCt(cb, allocator, store, "vision_tower.vision_model.post_layernorm.bias");
        defer cb.free(beta);
        const normed = try cb.layerNorm(hidden, gamma, beta, vision_hidden, 1e-6);
        cb.free(hidden);
        hidden = normed;
    }

    const hidden_data = try cb.toFloat32(hidden, allocator);
    defer allocator.free(hidden_data);
    cb.free(hidden);

    const merged = try averagePoolPatches(allocator, hidden_data, batch, grid, vision_hidden, cfg.mm_tokens_per_image);
    defer allocator.free(merged);

    const merged_shape = [_]i32{ @intCast(batch * cfg.mm_tokens_per_image), @intCast(vision_hidden) };
    const merged_ct = try cb.fromFloat32Shape(merged, &merged_shape);
    defer cb.free(merged_ct);

    const soft_norm_w = try loadWeightCt(cb, allocator, store, "multi_modal_projector.mm_soft_emb_norm.weight");
    defer cb.free(soft_norm_w);
    const soft_norm = blk: {
        if (std.math.approxEqAbs(f32, runtime_cfg.norm_weight_offset, 0.0, 1e-6)) break :blk soft_norm_w;
        const soft_norm_data = try cb.toFloat32(soft_norm_w, allocator);
        defer allocator.free(soft_norm_data);
        for (soft_norm_data) |*value| value.* += runtime_cfg.norm_weight_offset;
        const soft_norm_shape = [_]i32{@intCast(vision_hidden)};
        break :blk try cb.fromFloat32Shape(soft_norm_data, &soft_norm_shape);
    };
    defer if (soft_norm != soft_norm_w) cb.free(soft_norm);
    const normed = try cb.rmsNorm(merged_ct, soft_norm, vision_hidden, 1e-6);
    defer cb.free(normed);

    const proj_w_ct = try loadLinearWeightCt(cb, allocator, store, "multi_modal_projector.mm_input_projection_weight", vision_hidden, cfg.text_hidden);
    defer cb.free(proj_w_ct);
    const projected = try cb.linearNoBias(normed, proj_w_ct, batch * cfg.mm_tokens_per_image, vision_hidden, cfg.text_hidden);
    defer cb.free(projected);
    return cb.toFloat32(projected, allocator);
}

fn parseProjectorConfig(file: *const gguf_format.File, runtime_cfg: gpt_mod.Config) !ProjectorConfig {
    if (!isSupportedProjectorFile(file)) return error.InvalidGgufProjector;
    const view = gguf_metadata.View.init(file);
    const cfg: ProjectorConfig = .{
        .text_hidden = @intCast(view.getU64("termite.projector.text_hidden_size") orelse return error.InvalidGgufProjector),
        .vision_hidden = @intCast(view.getU64("termite.projector.vision_hidden_size") orelse return error.InvalidGgufProjector),
        .intermediate_size = @intCast(view.getU64("termite.projector.vision_feed_forward_length") orelse return error.InvalidGgufProjector),
        .block_count = @intCast(view.getU64("termite.projector.vision_block_count") orelse return error.InvalidGgufProjector),
        .head_count = @intCast(view.getU64("termite.projector.vision_attention_head_count") orelse return error.InvalidGgufProjector),
        .image_size = @intCast(view.getU64("termite.projector.vision_image_size") orelse return error.InvalidGgufProjector),
        .patch_size = @intCast(view.getU64("termite.projector.vision_patch_size") orelse return error.InvalidGgufProjector),
        .mm_tokens_per_image = @intCast(view.getU64("termite.projector.mm_tokens_per_image") orelse return error.InvalidGgufProjector),
    };
    if (cfg.text_hidden != runtime_cfg.hidden_size) return error.InvalidGgufProjector;
    if (runtime_cfg.mm_tokens_per_image > 0 and cfg.mm_tokens_per_image != runtime_cfg.mm_tokens_per_image) return error.InvalidGgufProjector;
    return cfg;
}

fn patchEmbed(
    store: *tensor_store_mod.GgufStore,
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    pixel_values: []const f32,
    batch: usize,
    patch_size: usize,
    image_size: usize,
    grid: usize,
    patch_dim: usize,
    vision_hidden: usize,
) ![]f32 {
    const num_patches = grid * grid;
    const patch_w = try loadWeightCt(cb, allocator, store, "vision_tower.vision_model.embeddings.patch_embedding.weight");
    defer cb.free(patch_w);
    const patch_b = try loadWeightCt(cb, allocator, store, "vision_tower.vision_model.embeddings.patch_embedding.bias");
    defer cb.free(patch_b);
    const pixel_shape = [_]i32{ @intCast(batch), 3, @intCast(image_size), @intCast(image_size) };
    const pixels_ct = try cb.fromFloat32Shape(pixel_values, &pixel_shape);
    defer cb.free(pixels_ct);

    const conv = try cb.conv2d(
        pixels_ct,
        patch_w,
        patch_b,
        batch,
        3,
        vision_hidden,
        image_size,
        image_size,
        patch_size,
        patch_size,
        patch_size,
        patch_size,
        0,
        0,
        1,
    );
    defer cb.free(conv);
    const conv_data = try cb.toFloat32(conv, allocator);
    defer allocator.free(conv_data);
    if (conv_data.len != batch * vision_hidden * num_patches) return error.InvalidPatchEmbeddingShape;

    const embedded = try allocator.alloc(f32, batch * num_patches * vision_hidden);
    for (0..batch) |b| {
        for (0..vision_hidden) |ch| {
            const src_base = ((b * vision_hidden) + ch) * num_patches;
            for (0..num_patches) |patch_idx| {
                embedded[(b * num_patches + patch_idx) * vision_hidden + ch] = conv_data[src_base + patch_idx];
            }
        }
    }
    _ = patch_dim;
    return embedded;
}

fn addPositionEmbeddings(
    store: *tensor_store_mod.GgufStore,
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    patch_embeddings: []const f32,
    batch: usize,
    num_patches: usize,
    vision_hidden: usize,
) ![]f32 {
    var pos_w = try loadTensorF32(store, "vision_tower.vision_model.embeddings.position_embedding.weight");
    defer pos_w.deinit();
    if (pos_w.data.len != num_patches * vision_hidden) return error.InvalidPositionEmbeddingShape;

    const full = try allocator.alloc(f32, batch * num_patches * vision_hidden);
    for (0..batch) |b| {
        for (0..num_patches) |patch_idx| {
            const dst = (b * num_patches + patch_idx) * vision_hidden;
            const src = dst;
            const pos = patch_idx * vision_hidden;
            for (0..vision_hidden) |i| {
                full[dst + i] = patch_embeddings[src + i] + pos_w.data[pos + i];
            }
        }
    }
    _ = cb;
    return full;
}

fn averagePoolPatches(
    allocator: std.mem.Allocator,
    hidden: []const f32,
    batch: usize,
    grid: usize,
    vision_hidden: usize,
    expected_tokens: usize,
) ![]f32 {
    const expected_side = exactSquareRoot(expected_tokens) orelse return error.InvalidImageTokenCount;
    if (grid % expected_side != 0) return error.InvalidPatchMergeFactor;
    const merge = grid / expected_side;
    const pooled_tokens = expected_side * expected_side;
    const pooled = try allocator.alloc(f32, batch * pooled_tokens * vision_hidden);

    for (0..batch) |b| {
        for (0..expected_side) |out_y| {
            for (0..expected_side) |out_x| {
                const dst_token = b * pooled_tokens + out_y * expected_side + out_x;
                const dst = dst_token * vision_hidden;
                @memset(pooled[dst..][0..vision_hidden], 0);
                for (0..merge) |dy| {
                    for (0..merge) |dx| {
                        const src_patch = b * grid * grid + (out_y * merge + dy) * grid + (out_x * merge + dx);
                        const src = src_patch * vision_hidden;
                        for (0..vision_hidden) |i| pooled[dst + i] += hidden[src + i];
                    }
                }
                const denom: f32 = @floatFromInt(merge * merge);
                for (0..vision_hidden) |i| pooled[dst + i] /= denom;
            }
        }
    }

    return pooled;
}

fn encoderBlock(
    store: *tensor_store_mod.GgufStore,
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    batch: usize,
    seq_len: usize,
    hidden: usize,
    num_heads: usize,
    intermediate: usize,
    layer: usize,
) !CT {
    const total = batch * seq_len;
    const head_dim = hidden / num_heads;
    var buf: [160]u8 = undefined;

    const ln1_g = try loadWeightCt(cb, allocator, store, try fmt(&buf, "vision_tower.vision_model.encoder.layers.{d}.layer_norm1.weight", .{layer}));
    defer cb.free(ln1_g);
    const ln1_b = try loadWeightCt(cb, allocator, store, try fmt(&buf, "vision_tower.vision_model.encoder.layers.{d}.layer_norm1.bias", .{layer}));
    defer cb.free(ln1_b);
    const normed1 = try cb.layerNorm(input, ln1_g, ln1_b, hidden, 1e-6);
    defer cb.free(normed1);

    const attn_out = try selfAttention(store, cb, allocator, normed1, batch, seq_len, hidden, num_heads, head_dim, layer, &buf);
    defer cb.free(attn_out);
    const res1 = try cb.add(input, attn_out);

    const ln2_g = try loadWeightCt(cb, allocator, store, try fmt(&buf, "vision_tower.vision_model.encoder.layers.{d}.layer_norm2.weight", .{layer}));
    defer cb.free(ln2_g);
    const ln2_b = try loadWeightCt(cb, allocator, store, try fmt(&buf, "vision_tower.vision_model.encoder.layers.{d}.layer_norm2.bias", .{layer}));
    defer cb.free(ln2_b);
    const normed2 = try cb.layerNorm(res1, ln2_g, ln2_b, hidden, 1e-6);
    defer cb.free(normed2);

    const fc1_w = try loadWeightCt(cb, allocator, store, try fmt(&buf, "vision_tower.vision_model.encoder.layers.{d}.mlp.fc1.weight", .{layer}));
    defer cb.free(fc1_w);
    const fc1_b = try loadWeightCt(cb, allocator, store, try fmt(&buf, "vision_tower.vision_model.encoder.layers.{d}.mlp.fc1.bias", .{layer}));
    defer cb.free(fc1_b);
    const fc1_out = try cb.linear(normed2, fc1_w, fc1_b, total, hidden, intermediate);
    defer cb.free(fc1_out);
    const activated = try cb.gelu(fc1_out);
    defer cb.free(activated);

    const fc2_w = try loadWeightCt(cb, allocator, store, try fmt(&buf, "vision_tower.vision_model.encoder.layers.{d}.mlp.fc2.weight", .{layer}));
    defer cb.free(fc2_w);
    const fc2_b = try loadWeightCt(cb, allocator, store, try fmt(&buf, "vision_tower.vision_model.encoder.layers.{d}.mlp.fc2.bias", .{layer}));
    defer cb.free(fc2_b);
    const fc2_out = try cb.linear(activated, fc2_w, fc2_b, total, intermediate, hidden);
    defer cb.free(fc2_out);

    const res2 = try cb.add(res1, fc2_out);
    cb.free(res1);
    return res2;
}

fn selfAttention(
    store: *tensor_store_mod.GgufStore,
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    batch: usize,
    seq_len: usize,
    hidden: usize,
    num_heads: usize,
    head_dim: usize,
    layer: usize,
    buf: *[160]u8,
) !CT {
    const total = batch * seq_len;

    const q_w = try loadWeightCt(cb, allocator, store, try fmt(buf, "vision_tower.vision_model.encoder.layers.{d}.self_attn.q_proj.weight", .{layer}));
    defer cb.free(q_w);
    const q_b = try loadWeightCt(cb, allocator, store, try fmt(buf, "vision_tower.vision_model.encoder.layers.{d}.self_attn.q_proj.bias", .{layer}));
    defer cb.free(q_b);
    const q = try cb.linear(input, q_w, q_b, total, hidden, hidden);
    defer cb.free(q);

    const k_w = try loadWeightCt(cb, allocator, store, try fmt(buf, "vision_tower.vision_model.encoder.layers.{d}.self_attn.k_proj.weight", .{layer}));
    defer cb.free(k_w);
    const k_b = try loadWeightCt(cb, allocator, store, try fmt(buf, "vision_tower.vision_model.encoder.layers.{d}.self_attn.k_proj.bias", .{layer}));
    defer cb.free(k_b);
    const k = try cb.linear(input, k_w, k_b, total, hidden, hidden);
    defer cb.free(k);

    const v_w = try loadWeightCt(cb, allocator, store, try fmt(buf, "vision_tower.vision_model.encoder.layers.{d}.self_attn.v_proj.weight", .{layer}));
    defer cb.free(v_w);
    const v_b = try loadWeightCt(cb, allocator, store, try fmt(buf, "vision_tower.vision_model.encoder.layers.{d}.self_attn.v_proj.bias", .{layer}));
    defer cb.free(v_b);
    const v = try cb.linear(input, v_w, v_b, total, hidden, hidden);
    defer cb.free(v);

    const mask = try allocator.alloc(i64, batch * seq_len);
    defer allocator.free(mask);
    @memset(mask, 1);
    const attn = try cb.scaledDotProductAttention(q, k, v, mask, null, batch, seq_len, num_heads, head_dim);
    defer cb.free(attn);

    const out_w = try loadWeightCt(cb, allocator, store, try fmt(buf, "vision_tower.vision_model.encoder.layers.{d}.self_attn.out_proj.weight", .{layer}));
    defer cb.free(out_w);
    const out_b = try loadWeightCt(cb, allocator, store, try fmt(buf, "vision_tower.vision_model.encoder.layers.{d}.self_attn.out_proj.bias", .{layer}));
    defer cb.free(out_b);
    return cb.linear(attn, out_w, out_b, total, hidden, hidden);
}

fn loadWeightCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    name: []const u8,
) !CT {
    var tensor = try loadTensorF32(store, name);
    defer tensor.deinit();
    const shape = try shapeI32(allocator, tensor.shape);
    defer allocator.free(shape);
    return cb.fromFloat32Shape(tensor.data, shape);
}

fn loadLinearWeightCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    name: []const u8,
    in_dim: usize,
    out_dim: usize,
) !CT {
    var tensor = try loadTensorF32(store, name);
    defer tensor.deinit();
    if (tensor.shape.len != 2) return error.InvalidTensorShape;
    const rows: usize = @intCast(tensor.shape[0]);
    const cols: usize = @intCast(tensor.shape[1]);
    if (rows == out_dim and cols == in_dim) {
        const shape = [_]i32{ @intCast(out_dim), @intCast(in_dim) };
        return cb.fromFloat32Shape(tensor.data, &shape);
    }
    if (rows == in_dim and cols == out_dim) {
        const transposed = try transposeMatrix(allocator, tensor.data, in_dim, out_dim);
        defer allocator.free(transposed);
        const shape = [_]i32{ @intCast(out_dim), @intCast(in_dim) };
        return cb.fromFloat32Shape(transposed, &shape);
    }
    return error.InvalidTensorShape;
}

fn loadTensorF32(store: *tensor_store_mod.GgufStore, name: []const u8) !LoadedF32 {
    var tensor_ref = try store.tensorStore().describeTensor(store.allocator, name);
    defer tensor_ref.deinit(store.allocator);
    var loaded = try store.tensorStore().loadTensorRef(&tensor_ref);
    errdefer loaded.deinit();

    if (loaded.tensor.dtype == .f32) {
        return .{
            .weight = loaded,
            .data = loaded.tensor.asFloat32(),
            .shape = loaded.tensor.shape,
        };
    }

    if (loaded.tensor.dtype == .f16 or loaded.tensor.dtype == .bf16) {
        const converted = try weight_source_mod.convertToF32(store.allocator, &loaded.tensor);
        errdefer converted.deinit();
        return .{
            .weight = loaded,
            .converted = converted,
            .data = converted.asFloat32(),
            .shape = converted.shape,
        };
    }

    return error.UnsupportedTensorType;
}

fn shapeI32(allocator: std.mem.Allocator, shape: []const i64) ![]i32 {
    const out = try allocator.alloc(i32, shape.len);
    for (shape, 0..) |dim, i| out[i] = @intCast(dim);
    return out;
}

fn transposeMatrix(allocator: std.mem.Allocator, input: []const f32, rows: usize, cols: usize) ![]f32 {
    if (input.len != rows * cols) return error.InvalidTensorShape;
    const transposed = try allocator.alloc(f32, input.len);
    for (0..rows) |row| {
        for (0..cols) |col| {
            transposed[col * rows + row] = input[row * cols + col];
        }
    }
    return transposed;
}

fn exactSquareRoot(value: usize) ?usize {
    var n: usize = 0;
    while (n * n < value) : (n += 1) {}
    return if (n * n == value) n else null;
}

fn fmt(buf: *[160]u8, comptime format: []const u8, args: anytype) ![]const u8 {
    return std.fmt.bufPrint(buf, format, args) catch return error.WeightNameTooLong;
}

test "supports termite gemma3 projector metadata" {
    const allocator = std.testing.allocator;
    const path = try std.fs.path.join(allocator, &.{ "/tmp", "termite-gemma3-projector-classify.gguf" });
    defer allocator.free(path);
    defer compat.cwd().deleteFile(compat.io(), path) catch {};

    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "termite-projector" } },
        .{ .key = "termite.projector.source_architecture", .value = .{ .string = "gemma3" } },
        .{ .key = "termite.projector.text_hidden_size", .value = .{ .u32 = 4 } },
        .{ .key = "termite.projector.vision_hidden_size", .value = .{ .u32 = 8 } },
        .{ .key = "termite.projector.vision_feed_forward_length", .value = .{ .u32 = 16 } },
        .{ .key = "termite.projector.vision_block_count", .value = .{ .u32 = 3 } },
        .{ .key = "termite.projector.vision_attention_head_count", .value = .{ .u32 = 4 } },
        .{ .key = "termite.projector.vision_image_size", .value = .{ .u32 = 224 } },
        .{ .key = "termite.projector.vision_patch_size", .value = .{ .u32 = 14 } },
        .{ .key = "termite.projector.mm_tokens_per_image", .value = .{ .u32 = 256 } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = layout.header_bytes });

    try std.testing.expect(try isSupportedProjectorPath(allocator, path));
}

test "rejects clip projector metadata" {
    const allocator = std.testing.allocator;
    const path = try std.fs.path.join(allocator, &.{ "/tmp", "termite-gemma3-projector-reject.gguf" });
    defer allocator.free(path);
    defer compat.cwd().deleteFile(compat.io(), path) catch {};

    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.vision.projector_type", .value = .{ .string = "gemma4v" } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = layout.header_bytes });

    try std.testing.expect(!(try isSupportedProjectorPath(allocator, path)));
}
