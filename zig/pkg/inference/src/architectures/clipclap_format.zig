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

//! Strict admission contract for Antfly's ClipClap GGUF bundle.
//!
//! ClipClap is not a decoder plus projector. It is a pair of independently
//! quantized dual encoders: CLIP owns text/image and CLAP owns audio. The only
//! safe pairing invariant is their shared projection width. This module checks
//! that invariant together with every metadata dimension and tensor family the
//! native pipeline will consume, using headers only and without materializing
//! weights.

const std = @import("std");
const gguf_format = @import("../gguf/format.zig");
const gguf_metadata = @import("../gguf/metadata.zig");
const gguf_tensor_catalog = @import("../gguf/tensor_catalog.zig");
const c_file = @import("../util/c_file.zig");

pub const Contract = struct {
    projection_dim: u32,
    clip_text_hidden: u32,
    clip_vision_hidden: u32,
    clap_audio_hidden: u32,
};

const ClipContract = struct {
    projection_dim: u32,
    text_hidden: u32,
    vision_hidden: u32,
};

const ClapContract = struct {
    projection_dim: u32,
    audio_hidden: u32,
};

pub fn inspectFilePair(
    clip_file: *const gguf_format.File,
    clap_file: *const gguf_format.File,
) !Contract {
    const clip_view = gguf_metadata.View.init(clip_file);
    const clap_view = gguf_metadata.View.init(clap_file);
    try requireArchitecture(clip_view, "clip");
    try requireArchitecture(clap_view, "clap");
    if (try positiveU32(clip_view, "clip.projection_dim") !=
        try positiveU32(clap_view, "clap.projection_dim"))
    {
        return error.ClipclapProjectionMismatch;
    }
    const clip = try inspectClip(clip_file);
    const clap = try inspectClap(clap_file);
    if (clip.projection_dim != clap.projection_dim)
        return error.ClipclapProjectionMismatch;
    return .{
        .projection_dim = clip.projection_dim,
        .clip_text_hidden = clip.text_hidden,
        .clip_vision_hidden = clip.vision_hidden,
        .clap_audio_hidden = clap.audio_hidden,
    };
}

fn inspectClip(file: *const gguf_format.File) !ClipContract {
    const view = gguf_metadata.View.init(file);
    try requireArchitecture(view, "clip");
    const family = view.getString("clip.family") orelse return error.InvalidClipclapContract;
    if (!std.mem.eql(u8, family, "clip")) return error.UnsupportedClipclapArchitecture;

    const text_hidden = try positiveU32(view, "clip.text.embedding_length");
    const text_layers = try positiveU32(view, "clip.text.block_count");
    const text_heads = try positiveU32(view, "clip.text.attention.head_count");
    const text_ffn = try positiveU32(view, "clip.text.feed_forward_length");
    const text_context = try positiveU32(view, "clip.text.context_length");
    const vocab_size = try positiveU32(view, "clip.text.vocab_size");
    const vision_hidden = try positiveU32(view, "clip.vision.embedding_length");
    const vision_layers = try positiveU32(view, "clip.vision.block_count");
    const vision_heads = try positiveU32(view, "clip.vision.attention.head_count");
    const vision_ffn = try positiveU32(view, "clip.vision.feed_forward_length");
    const image_size = try positiveU32(view, "clip.vision.image_size");
    const patch_size = try positiveU32(view, "clip.vision.patch_size");
    const projection_dim = try positiveU32(view, "clip.projection_dim");
    if (text_hidden % text_heads != 0 or vision_hidden % vision_heads != 0 or
        image_size % patch_size != 0 or text_layers > file.tensors.len or
        vision_layers > file.tensors.len)
    {
        return error.InvalidClipclapContract;
    }

    const tensors = TensorValidator.init(file);
    try tensors.requireMatrix("text_model.embeddings.token_embedding.weight", text_hidden, vocab_size);
    try tensors.requireMatrix("text_model.embeddings.position_embedding.weight", text_hidden, text_context);
    try tensors.requireVector("text_model.final_layer_norm.weight", text_hidden);
    try tensors.requireVector("text_model.final_layer_norm.bias", text_hidden);
    try tensors.requireMatrix("text_projection.weight", text_hidden, projection_dim);
    try validateClipTransformer(tensors, "text_model.encoder.layers", text_layers, text_hidden, text_ffn);

    try tensors.requireConv2dElements(
        "vision_model.embeddings.patch_embedding.weight",
        patch_size,
        patch_size,
        3,
        vision_hidden,
    );
    try tensors.requireVector("vision_model.embeddings.class_embedding", vision_hidden);
    const patch_grid = image_size / patch_size;
    const patch_count = try checkedMul(patch_grid, patch_grid);
    try tensors.requireMatrix(
        "vision_model.embeddings.position_embedding.weight",
        vision_hidden,
        patch_count + 1,
    );
    try tensors.requireVector("vision_model.pre_layrnorm.weight", vision_hidden);
    try tensors.requireVector("vision_model.pre_layrnorm.bias", vision_hidden);
    try tensors.requireVector("vision_model.post_layernorm.weight", vision_hidden);
    try tensors.requireVector("vision_model.post_layernorm.bias", vision_hidden);
    try tensors.requireMatrix("visual_projection.weight", vision_hidden, projection_dim);
    try validateClipTransformer(tensors, "vision_model.encoder.layers", vision_layers, vision_hidden, vision_ffn);

    return .{
        .projection_dim = projection_dim,
        .text_hidden = text_hidden,
        .vision_hidden = vision_hidden,
    };
}

fn validateClipTransformer(
    tensors: TensorValidator,
    prefix: []const u8,
    layer_count: u32,
    hidden: u32,
    ffn: u32,
) !void {
    var name_buf: [192]u8 = undefined;
    for (0..layer_count) |layer| {
        try tensors.requireVector(try layerName(&name_buf, prefix, layer, "layer_norm1.weight"), hidden);
        try tensors.requireVector(try layerName(&name_buf, prefix, layer, "layer_norm1.bias"), hidden);
        try tensors.requireVector(try layerName(&name_buf, prefix, layer, "layer_norm2.weight"), hidden);
        try tensors.requireVector(try layerName(&name_buf, prefix, layer, "layer_norm2.bias"), hidden);
        inline for (.{ "q_proj", "k_proj", "v_proj", "out_proj" }) |projection| {
            try tensors.requireMatrix(
                try std.fmt.bufPrint(&name_buf, "{s}.{d}.self_attn.{s}.weight", .{ prefix, layer, projection }),
                hidden,
                hidden,
            );
            try tensors.requireVector(
                try std.fmt.bufPrint(&name_buf, "{s}.{d}.self_attn.{s}.bias", .{ prefix, layer, projection }),
                hidden,
            );
        }
        try tensors.requireMatrix(try layerName(&name_buf, prefix, layer, "mlp.fc1.weight"), hidden, ffn);
        try tensors.requireVector(try layerName(&name_buf, prefix, layer, "mlp.fc1.bias"), ffn);
        try tensors.requireMatrix(try layerName(&name_buf, prefix, layer, "mlp.fc2.weight"), ffn, hidden);
        try tensors.requireVector(try layerName(&name_buf, prefix, layer, "mlp.fc2.bias"), hidden);
    }
}

fn inspectClap(file: *const gguf_format.File) !ClapContract {
    const view = gguf_metadata.View.init(file);
    try requireArchitecture(view, "clap");
    const projection_dim = try positiveU32(view, "clap.projection_dim");
    const audio_hidden = try positiveU32(view, "clap.audio.embedding_length");
    const patch_hidden = try positiveU32(view, "clap.audio.patch_embeds_hidden_size");
    const input_channels = try positiveU32(view, "clap.audio.patch_embed_input_channels");
    const patch_size = try positiveU32(view, "clap.audio.patch_size");
    const num_mel_bins = try positiveU32(view, "clap.audio.num_mel_bins");
    const spec_size = try positiveU32(view, "clap.audio.spec_size");
    const window_size = try positiveU32(view, "clap.audio.window_size");
    const patch_stride = try requiredU32Array(2, view, "clap.audio.patch_stride");
    const depths = try requiredU32Array(4, view, "clap.audio.depths");
    const heads = try requiredU32Array(4, view, "clap.audio.attention_head_counts");

    const mlp_ratio = view.getF32("clap.audio.mlp_ratio") orelse
        return error.InvalidClipclapContract;
    const audio_fusion = view.getBool("clap.audio.enable_fusion") orelse
        return error.InvalidClipclapContract;
    const bundle_fusion = view.getBool("clap.enable_fusion") orelse
        return error.InvalidClipclapContract;
    const projection_activation = view.getString("clap.projection_hidden_act") orelse
        return error.InvalidClipclapContract;
    const audio_activation = view.getString("clap.audio.hidden_act") orelse
        return error.InvalidClipclapContract;
    const layer_norm_epsilon = view.getF32("clap.audio.layer_norm_epsilon") orelse
        return error.InvalidClipclapContract;
    const qkv_bias = view.getBool("clap.audio.qkv_bias") orelse
        return error.InvalidClipclapContract;
    const patch_fusion = view.getBool("clap.audio.enable_patch_fusion") orelse
        return error.InvalidClipclapContract;
    const patch_layer_norm = view.getBool("clap.audio.enable_patch_layer_norm") orelse
        return error.InvalidClipclapContract;
    if (mlp_ratio != 4.0 or audio_fusion or bundle_fusion or
        !(std.mem.eql(u8, projection_activation, "relu") or
            std.mem.eql(u8, projection_activation, "gelu")) or
        !std.mem.eql(u8, audio_activation, "gelu") or
        !std.math.isFinite(layer_norm_epsilon) or layer_norm_epsilon <= 0 or
        !qkv_bias or patch_fusion or !patch_layer_norm or input_channels != 1)
    {
        // The v1 bundle runtime uses a fixed 4x MLP and a single-channel audio
        // path. Reject metadata that would make the loader and execution
        // geometry disagree instead of silently inheriting parser defaults.
        return error.UnsupportedClipclapArchitecture;
    }
    if (patch_size > spec_size or patch_stride[0] == 0 or patch_stride[1] == 0 or
        num_mel_bins == 0 or window_size == 0 or spec_size % num_mel_bins != 0)
    {
        return error.InvalidClipclapContract;
    }
    const patch_span = spec_size - patch_size;
    if (patch_span % patch_stride[0] != 0 or patch_span % patch_stride[1] != 0)
        return error.InvalidClipclapContract;
    const patch_height = patch_span / patch_stride[0] + 1;
    const patch_width = patch_span / patch_stride[1] + 1;
    const downsample_factor = @as(u32, 1) << 3;
    if (patch_height % downsample_factor != 0 or patch_width % downsample_factor != 0 or
        patch_height / downsample_factor < window_size or
        patch_width / downsample_factor < window_size)
    {
        return error.InvalidClipclapContract;
    }
    for (0..4) |stage| {
        const stage_dim = checkedShift(patch_hidden, stage) orelse return error.InvalidClipclapContract;
        if (depths[stage] == 0 or heads[stage] == 0 or stage_dim % heads[stage] != 0)
            return error.InvalidClipclapContract;
    }
    var total_blocks: u64 = 0;
    for (depths) |depth| total_blocks += depth;
    if (total_blocks > file.tensors.len) return error.InvalidClipclapContract;
    if (checkedShift(patch_hidden, 3) != audio_hidden) return error.InvalidClipclapContract;

    const tensors = TensorValidator.init(file);
    try tensors.requireConv2dElements(
        "audio_model.audio_encoder.patch_embed.proj.weight",
        patch_size,
        patch_size,
        input_channels,
        patch_hidden,
    );
    try tensors.requireVector("audio_model.audio_encoder.patch_embed.proj.bias", patch_hidden);
    try tensors.requireVector("audio_model.audio_encoder.patch_embed.norm.weight", patch_hidden);
    try tensors.requireVector("audio_model.audio_encoder.patch_embed.norm.bias", patch_hidden);
    inline for (.{ "weight", "bias", "running_mean", "running_var" }) |suffix| {
        var name_buf: [96]u8 = undefined;
        try tensors.requireVector(
            try std.fmt.bufPrint(
                &name_buf,
                "audio_model.audio_encoder.batch_norm.{s}",
                .{suffix},
            ),
            num_mel_bins,
        );
    }
    try tensors.requireVector("audio_model.audio_encoder.norm.weight", audio_hidden);
    try tensors.requireVector("audio_model.audio_encoder.norm.bias", audio_hidden);
    try tensors.requireMatrix("audio_projection.linear1.weight", audio_hidden, projection_dim);
    try tensors.requireVector("audio_projection.linear1.bias", projection_dim);
    try tensors.requireMatrix("audio_projection.linear2.weight", projection_dim, projection_dim);
    try tensors.requireVector("audio_projection.linear2.bias", projection_dim);

    try validateClapStages(tensors, patch_hidden, &depths, &heads, window_size);
    return .{ .projection_dim = projection_dim, .audio_hidden = audio_hidden };
}

fn validateClapStages(
    tensors: TensorValidator,
    patch_hidden: u32,
    depths: []const u32,
    heads: []const u32,
    window_size: u32,
) !void {
    var name_buf: [224]u8 = undefined;
    for (0..4) |stage| {
        const dim: u64 = checkedShift(patch_hidden, stage) orelse return error.InvalidClipclapContract;
        const relative_side = @as(u64, window_size) * 2 - 1;
        const relative_positions = try checkedMul(relative_side, relative_side);
        const relative_bias_elements = try checkedMul(relative_positions, heads[stage]);
        for (0..depths[stage]) |block| {
            const prefix = try std.fmt.bufPrint(
                &name_buf,
                "audio_model.audio_encoder.layers.{d}.blocks.{d}",
                .{ stage, block },
            );
            var field_buf: [256]u8 = undefined;
            try tensors.requireVector(try fieldName(&field_buf, prefix, "layernorm_before.weight"), dim);
            try tensors.requireVector(try fieldName(&field_buf, prefix, "layernorm_before.bias"), dim);
            try tensors.requireVector(try fieldName(&field_buf, prefix, "layernorm_after.weight"), dim);
            try tensors.requireVector(try fieldName(&field_buf, prefix, "layernorm_after.bias"), dim);
            inline for (.{ "query", "key", "value" }) |projection| {
                try tensors.requireMatrix(
                    try std.fmt.bufPrint(&field_buf, "{s}.attention.self.{s}.weight", .{ prefix, projection }),
                    dim,
                    dim,
                );
                try tensors.requireVector(
                    try std.fmt.bufPrint(&field_buf, "{s}.attention.self.{s}.bias", .{ prefix, projection }),
                    dim,
                );
            }
            try tensors.validateOptionalVector(
                try fieldName(&field_buf, prefix, "attention.self.relative_position_bias_table"),
                relative_bias_elements,
            );
            try tensors.requireMatrix(try fieldName(&field_buf, prefix, "attention.output.dense.weight"), dim, dim);
            try tensors.requireVector(try fieldName(&field_buf, prefix, "attention.output.dense.bias"), dim);
            try tensors.requireMatrix(try fieldName(&field_buf, prefix, "intermediate.dense.weight"), dim, dim * 4);
            try tensors.requireVector(try fieldName(&field_buf, prefix, "intermediate.dense.bias"), dim * 4);
            try tensors.requireMatrix(try fieldName(&field_buf, prefix, "output.dense.weight"), dim * 4, dim);
            try tensors.requireVector(try fieldName(&field_buf, prefix, "output.dense.bias"), dim);
        }
        if (stage + 1 < 4) {
            const prefix = try std.fmt.bufPrint(
                &name_buf,
                "audio_model.audio_encoder.layers.{d}.downsample",
                .{stage},
            );
            var field_buf: [256]u8 = undefined;
            try tensors.requireVector(try fieldName(&field_buf, prefix, "norm.weight"), dim * 4);
            try tensors.requireVector(try fieldName(&field_buf, prefix, "norm.bias"), dim * 4);
            try tensors.requireMatrix(try fieldName(&field_buf, prefix, "reduction.weight"), dim * 4, dim * 2);
        }
    }
}

const TensorValidator = struct {
    catalog: gguf_tensor_catalog.Catalog,

    fn init(file: *const gguf_format.File) TensorValidator {
        return .{ .catalog = .init(file) };
    }

    fn require(self: TensorValidator, name: []const u8) !*const gguf_format.TensorInfo {
        return self.catalog.find(name) orelse error.InvalidClipclapTensorContract;
    }

    fn requireVector(self: TensorValidator, name: []const u8, length: u64) !void {
        const tensor = try self.require(name);
        if (tensor.dimensions.len != 1 or tensor.dimensions[0] != length)
            return error.InvalidClipclapTensorContract;
    }

    fn requireMatrix(self: TensorValidator, name: []const u8, gguf_axis_0: u64, gguf_axis_1: u64) !void {
        const tensor = try self.require(name);
        if (tensor.dimensions.len != 2 or
            tensor.dimensions[0] != gguf_axis_0 or
            tensor.dimensions[1] != gguf_axis_1)
        {
            return error.InvalidClipclapTensorContract;
        }
    }

    fn validateOptionalVector(self: TensorValidator, name: []const u8, expected: u64) !void {
        const tensor = self.catalog.find(name) orelse return;
        // GGUF axes are reversed once by the tensor store. The graph runtime
        // binds this optional parameter as a vector, so accepting an arbitrary
        // equal-element layout would pass admission and then fail graph binding.
        if (tensor.dimensions.len != 1 or tensor.dimensions[0] != expected)
            return error.InvalidClipclapTensorContract;
    }

    fn requireConv2dElements(
        self: TensorValidator,
        name: []const u8,
        kernel_h: u64,
        kernel_w: u64,
        input_channels: u64,
        output_channels: u64,
    ) !void {
        const tensor = try self.require(name);
        if (tensor.dimensions.len != 4 or
            tensor.dimensions[0] != kernel_w or
            tensor.dimensions[1] != kernel_h or
            tensor.dimensions[2] != input_channels or
            tensor.dimensions[3] != output_channels)
        {
            return error.InvalidClipclapTensorContract;
        }
    }
};

fn requireArchitecture(view: gguf_metadata.View, expected: []const u8) !void {
    const actual = view.getString("general.architecture") orelse return error.InvalidClipclapContract;
    if (!std.mem.eql(u8, actual, expected)) return error.UnsupportedClipclapArchitecture;
}

fn positiveU32(view: gguf_metadata.View, key: []const u8) !u32 {
    const value = view.getU64(key) orelse return error.InvalidClipclapContract;
    if (value == 0 or value > std.math.maxInt(i32)) return error.InvalidClipclapContract;
    return @intCast(value);
}

fn requiredU32Array(
    comptime expected_len: usize,
    view: gguf_metadata.View,
    key: []const u8,
) ![expected_len]u32 {
    const entry = view.find(key) orelse return error.InvalidClipclapContract;
    if (entry.value != .array or entry.value.array.values.len != expected_len)
        return error.InvalidClipclapContract;
    var values: [expected_len]u32 = undefined;
    for (entry.value.array.values, 0..) |value, index| {
        values[index] = switch (value) {
            .u8 => |v| v,
            .u16 => |v| v,
            .u32 => |v| v,
            .u64 => |v| std.math.cast(u32, v) orelse return error.InvalidClipclapContract,
            else => return error.InvalidClipclapContract,
        };
    }
    return values;
}

fn checkedMul(a: anytype, b: anytype) !u64 {
    return std.math.mul(u64, @intCast(a), @intCast(b)) catch error.InvalidClipclapContract;
}

fn checkedShift(value: u32, shift: usize) ?u32 {
    const max_value: u32 = std.math.maxInt(u32);
    if (shift >= @bitSizeOf(u32) or value > (max_value >> @intCast(shift))) return null;
    return value << @intCast(shift);
}

fn layerName(buf: []u8, prefix: []const u8, layer: usize, suffix: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}.{d}.{s}", .{ prefix, layer, suffix }) catch
        error.InvalidClipclapContract;
}

fn fieldName(buf: []u8, prefix: []const u8, suffix: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}.{s}", .{ prefix, suffix }) catch
        error.InvalidClipclapContract;
}

const TestTensors = struct {
    allocator: std.mem.Allocator,
    items: std.ArrayListUnmanaged(gguf_format.TensorInfo) = .empty,

    fn deinit(self: *TestTensors) void {
        for (self.items.items) |tensor| {
            self.allocator.free(tensor.name);
            self.allocator.free(tensor.dimensions);
        }
        self.items.deinit(self.allocator);
    }

    fn add(self: *TestTensors, name: []const u8, dimensions: []const u64) !void {
        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);
        const owned_dimensions = try self.allocator.dupe(u64, dimensions);
        errdefer self.allocator.free(owned_dimensions);
        try self.items.append(self.allocator, .{
            .name = owned_name,
            .dimensions = owned_dimensions,
            .tensor_type = .{ .known = .F32 },
            .offset = 0,
            .data_offset = 0,
        });
    }

    fn vector(self: *TestTensors, name: []const u8, length: u64) !void {
        try self.add(name, &.{length});
    }

    fn matrix(self: *TestTensors, name: []const u8, input: u64, output: u64) !void {
        try self.add(name, &.{ input, output });
    }
};

fn addTestClipTransformer(
    tensors: *TestTensors,
    prefix: []const u8,
    hidden: u64,
    ffn: u64,
) !void {
    inline for (.{ "layer_norm1.weight", "layer_norm1.bias", "layer_norm2.weight", "layer_norm2.bias" }) |suffix| {
        const name = try std.fmt.allocPrint(tensors.allocator, "{s}.0.{s}", .{ prefix, suffix });
        defer tensors.allocator.free(name);
        try tensors.vector(name, hidden);
    }
    inline for (.{ "q_proj", "k_proj", "v_proj", "out_proj" }) |projection| {
        const weight = try std.fmt.allocPrint(tensors.allocator, "{s}.0.self_attn.{s}.weight", .{ prefix, projection });
        defer tensors.allocator.free(weight);
        try tensors.matrix(weight, hidden, hidden);
        const bias = try std.fmt.allocPrint(tensors.allocator, "{s}.0.self_attn.{s}.bias", .{ prefix, projection });
        defer tensors.allocator.free(bias);
        try tensors.vector(bias, hidden);
    }
    const fc1_weight = try std.fmt.allocPrint(tensors.allocator, "{s}.0.mlp.fc1.weight", .{prefix});
    defer tensors.allocator.free(fc1_weight);
    try tensors.matrix(fc1_weight, hidden, ffn);
    const fc1_bias = try std.fmt.allocPrint(tensors.allocator, "{s}.0.mlp.fc1.bias", .{prefix});
    defer tensors.allocator.free(fc1_bias);
    try tensors.vector(fc1_bias, ffn);
    const fc2_weight = try std.fmt.allocPrint(tensors.allocator, "{s}.0.mlp.fc2.weight", .{prefix});
    defer tensors.allocator.free(fc2_weight);
    try tensors.matrix(fc2_weight, ffn, hidden);
    const fc2_bias = try std.fmt.allocPrint(tensors.allocator, "{s}.0.mlp.fc2.bias", .{prefix});
    defer tensors.allocator.free(fc2_bias);
    try tensors.vector(fc2_bias, hidden);
}

fn addTestClipTensors(tensors: *TestTensors) !void {
    try tensors.matrix("text_model.embeddings.token_embedding.weight", 4, 8);
    try tensors.matrix("text_model.embeddings.position_embedding.weight", 4, 4);
    try tensors.vector("text_model.final_layer_norm.weight", 4);
    try tensors.vector("text_model.final_layer_norm.bias", 4);
    try tensors.matrix("text_projection.weight", 4, 3);
    try addTestClipTransformer(tensors, "text_model.encoder.layers", 4, 8);
    try tensors.add("vision_model.embeddings.patch_embedding.weight", &.{ 2, 2, 3, 4 });
    try tensors.vector("vision_model.embeddings.class_embedding", 4);
    try tensors.matrix("vision_model.embeddings.position_embedding.weight", 4, 5);
    inline for (.{
        "vision_model.pre_layrnorm.weight",
        "vision_model.pre_layrnorm.bias",
        "vision_model.post_layernorm.weight",
        "vision_model.post_layernorm.bias",
    }) |name| try tensors.vector(name, 4);
    try tensors.matrix("visual_projection.weight", 4, 3);
    try addTestClipTransformer(tensors, "vision_model.encoder.layers", 4, 8);
}

fn addTestClapTensors(tensors: *TestTensors) !void {
    try tensors.add("audio_model.audio_encoder.patch_embed.proj.weight", &.{ 2, 2, 1, 1 });
    inline for (.{
        "audio_model.audio_encoder.patch_embed.proj.bias",
        "audio_model.audio_encoder.patch_embed.norm.weight",
        "audio_model.audio_encoder.patch_embed.norm.bias",
    }) |name| try tensors.vector(name, 1);
    try tensors.vector("audio_model.audio_encoder.norm.weight", 8);
    try tensors.vector("audio_model.audio_encoder.norm.bias", 8);
    inline for (.{ "weight", "bias", "running_mean", "running_var" }) |suffix| {
        const name = try std.fmt.allocPrint(
            tensors.allocator,
            "audio_model.audio_encoder.batch_norm.{s}",
            .{suffix},
        );
        defer tensors.allocator.free(name);
        try tensors.vector(name, 4);
    }
    try tensors.matrix("audio_projection.linear1.weight", 8, 3);
    try tensors.vector("audio_projection.linear1.bias", 3);
    try tensors.matrix("audio_projection.linear2.weight", 3, 3);
    try tensors.vector("audio_projection.linear2.bias", 3);

    for (0..4) |stage| {
        const dim: u64 = @as(u64, 1) << @intCast(stage);
        const prefix = try std.fmt.allocPrint(
            tensors.allocator,
            "audio_model.audio_encoder.layers.{d}.blocks.0",
            .{stage},
        );
        defer tensors.allocator.free(prefix);
        inline for (.{ "layernorm_before.weight", "layernorm_before.bias", "layernorm_after.weight", "layernorm_after.bias" }) |suffix| {
            const name = try std.fmt.allocPrint(tensors.allocator, "{s}.{s}", .{ prefix, suffix });
            defer tensors.allocator.free(name);
            try tensors.vector(name, dim);
        }
        inline for (.{ "query", "key", "value" }) |projection| {
            const weight = try std.fmt.allocPrint(tensors.allocator, "{s}.attention.self.{s}.weight", .{ prefix, projection });
            defer tensors.allocator.free(weight);
            try tensors.matrix(weight, dim, dim);
            const bias = try std.fmt.allocPrint(tensors.allocator, "{s}.attention.self.{s}.bias", .{ prefix, projection });
            defer tensors.allocator.free(bias);
            try tensors.vector(bias, dim);
        }
        const relative_bias = try std.fmt.allocPrint(
            tensors.allocator,
            "{s}.attention.self.relative_position_bias_table",
            .{prefix},
        );
        defer tensors.allocator.free(relative_bias);
        try tensors.vector(relative_bias, 1);
        const specs = [_]struct { suffix: []const u8, input: u64, output: u64, bias: bool }{
            .{ .suffix = "attention.output.dense", .input = dim, .output = dim, .bias = true },
            .{ .suffix = "intermediate.dense", .input = dim, .output = dim * 4, .bias = true },
            .{ .suffix = "output.dense", .input = dim * 4, .output = dim, .bias = true },
        };
        for (specs) |spec| {
            const weight = try std.fmt.allocPrint(tensors.allocator, "{s}.{s}.weight", .{ prefix, spec.suffix });
            defer tensors.allocator.free(weight);
            try tensors.matrix(weight, spec.input, spec.output);
            if (spec.bias) {
                const bias = try std.fmt.allocPrint(tensors.allocator, "{s}.{s}.bias", .{ prefix, spec.suffix });
                defer tensors.allocator.free(bias);
                try tensors.vector(bias, spec.output);
            }
        }
        if (stage < 3) {
            const downsample = try std.fmt.allocPrint(
                tensors.allocator,
                "audio_model.audio_encoder.layers.{d}.downsample",
                .{stage},
            );
            defer tensors.allocator.free(downsample);
            inline for (.{ "norm.weight", "norm.bias" }) |suffix| {
                const name = try std.fmt.allocPrint(tensors.allocator, "{s}.{s}", .{ downsample, suffix });
                defer tensors.allocator.free(name);
                try tensors.vector(name, dim * 4);
            }
            const reduction = try std.fmt.allocPrint(tensors.allocator, "{s}.reduction.weight", .{downsample});
            defer tensors.allocator.free(reduction);
            try tensors.matrix(reduction, dim * 4, dim * 2);
        }
    }
}

test "ClipClap GGUF contract validates paired encoder fixtures" {
    const allocator = std.testing.allocator;
    var patch_stride = [_]gguf_format.MetadataValue{ .{ .u32 = 2 }, .{ .u32 = 2 } };
    var depths = [_]gguf_format.MetadataValue{ .{ .u32 = 1 }, .{ .u32 = 1 }, .{ .u32 = 1 }, .{ .u32 = 1 } };
    var heads = [_]gguf_format.MetadataValue{ .{ .u32 = 1 }, .{ .u32 = 1 }, .{ .u32 = 1 }, .{ .u32 = 1 } };
    var clip_metadata = [_]gguf_format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.family", .value = .{ .string = "clip" } },
        .{ .key = "clip.text.embedding_length", .value = .{ .u32 = 4 } },
        .{ .key = "clip.text.block_count", .value = .{ .u32 = 1 } },
        .{ .key = "clip.text.attention.head_count", .value = .{ .u32 = 2 } },
        .{ .key = "clip.text.feed_forward_length", .value = .{ .u32 = 8 } },
        .{ .key = "clip.text.context_length", .value = .{ .u32 = 4 } },
        .{ .key = "clip.text.vocab_size", .value = .{ .u32 = 8 } },
        .{ .key = "clip.vision.embedding_length", .value = .{ .u32 = 4 } },
        .{ .key = "clip.vision.block_count", .value = .{ .u32 = 1 } },
        .{ .key = "clip.vision.attention.head_count", .value = .{ .u32 = 2 } },
        .{ .key = "clip.vision.feed_forward_length", .value = .{ .u32 = 8 } },
        .{ .key = "clip.vision.image_size", .value = .{ .u32 = 4 } },
        .{ .key = "clip.vision.patch_size", .value = .{ .u32 = 2 } },
        .{ .key = "clip.projection_dim", .value = .{ .u32 = 3 } },
    };
    var clap_metadata = [_]gguf_format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clap" } },
        .{ .key = "clap.projection_dim", .value = .{ .u32 = 3 } },
        .{ .key = "clap.projection_hidden_act", .value = .{ .string = "relu" } },
        .{ .key = "clap.audio.embedding_length", .value = .{ .u32 = 8 } },
        .{ .key = "clap.audio.patch_embeds_hidden_size", .value = .{ .u32 = 1 } },
        .{ .key = "clap.audio.patch_embed_input_channels", .value = .{ .u32 = 1 } },
        .{ .key = "clap.audio.patch_size", .value = .{ .u32 = 2 } },
        .{ .key = "clap.audio.patch_stride", .value = .{ .array = .{ .element_type = .u32, .values = &patch_stride } } },
        .{ .key = "clap.audio.num_mel_bins", .value = .{ .u32 = 4 } },
        .{ .key = "clap.audio.spec_size", .value = .{ .u32 = 16 } },
        .{ .key = "clap.audio.window_size", .value = .{ .u32 = 1 } },
        .{ .key = "clap.audio.depths", .value = .{ .array = .{ .element_type = .u32, .values = &depths } } },
        .{ .key = "clap.audio.attention_head_counts", .value = .{ .array = .{ .element_type = .u32, .values = &heads } } },
        .{ .key = "clap.audio.mlp_ratio", .value = .{ .f32 = 4.0 } },
        .{ .key = "clap.audio.layer_norm_epsilon", .value = .{ .f32 = 1e-5 } },
        .{ .key = "clap.audio.hidden_act", .value = .{ .string = "gelu" } },
        .{ .key = "clap.audio.qkv_bias", .value = .{ .bool_ = true } },
        .{ .key = "clap.audio.enable_patch_fusion", .value = .{ .bool_ = false } },
        .{ .key = "clap.audio.enable_patch_layer_norm", .value = .{ .bool_ = true } },
        .{ .key = "clap.audio.enable_fusion", .value = .{ .bool_ = false } },
        .{ .key = "clap.enable_fusion", .value = .{ .bool_ = false } },
    };
    var clip_tensors = TestTensors{ .allocator = allocator };
    defer clip_tensors.deinit();
    try addTestClipTensors(&clip_tensors);
    var clap_tensors = TestTensors{ .allocator = allocator };
    defer clap_tensors.deinit();
    try addTestClapTensors(&clap_tensors);

    var clip_file = gguf_format.File{
        .header = .{ .version = 3, .tensor_count = clip_tensors.items.items.len, .metadata_count = clip_metadata.len },
        .metadata = &clip_metadata,
        .tensors = clip_tensors.items.items,
        .alignment = 32,
        .data_region_offset = 0,
    };
    var clap_file = gguf_format.File{
        .header = .{ .version = 3, .tensor_count = clap_tensors.items.items.len, .metadata_count = clap_metadata.len },
        .metadata = &clap_metadata,
        .tensors = clap_tensors.items.items,
        .alignment = 32,
        .data_region_offset = 0,
    };
    const contract = try inspectFilePair(&clip_file, &clap_file);
    try std.testing.expectEqual(@as(u32, 3), contract.projection_dim);

    const text_projection = gguf_tensor_catalog.Catalog.init(&clip_file).find(
        "text_projection.weight",
    ).?;
    text_projection.dimensions[0] = 3;
    text_projection.dimensions[1] = 4;
    try std.testing.expectError(error.InvalidClipclapTensorContract, inspectFilePair(&clip_file, &clap_file));
    text_projection.dimensions[0] = 4;
    text_projection.dimensions[1] = 3;

    depths[0] = .{ .u32 = 0 };
    try std.testing.expectError(error.InvalidClipclapContract, inspectFilePair(&clip_file, &clap_file));
    depths[0] = .{ .u32 = 1 };

    const patch_tensor = gguf_tensor_catalog.Catalog.init(&clap_file).find(
        "audio_model.audio_encoder.patch_embed.proj.weight",
    ).?;
    patch_tensor.dimensions[0] = 3;
    try std.testing.expectError(error.InvalidClipclapTensorContract, inspectFilePair(&clip_file, &clap_file));
    patch_tensor.dimensions[0] = 2;

    const relative_bias = gguf_tensor_catalog.Catalog.init(&clap_file).find(
        "audio_model.audio_encoder.layers.0.blocks.0.attention.self.relative_position_bias_table",
    ).?;
    relative_bias.dimensions[0] = 2;
    try std.testing.expectError(error.InvalidClipclapTensorContract, inspectFilePair(&clip_file, &clap_file));
    relative_bias.dimensions[0] = 1;

    const mutable_relative_bias = @constCast(relative_bias);
    const vector_dimensions = mutable_relative_bias.dimensions;
    mutable_relative_bias.dimensions = try allocator.dupe(u64, &.{ 1, 1 });
    allocator.free(vector_dimensions);
    try std.testing.expectError(error.InvalidClipclapTensorContract, inspectFilePair(&clip_file, &clap_file));
    const matrix_dimensions = mutable_relative_bias.dimensions;
    mutable_relative_bias.dimensions = try allocator.dupe(u64, &.{1});
    allocator.free(matrix_dimensions);

    clap_metadata[17].value = .{ .bool_ = true };
    try std.testing.expectError(error.UnsupportedClipclapArchitecture, inspectFilePair(&clip_file, &clap_file));
    clap_metadata[17].value = .{ .bool_ = false };

    clap_metadata[1].value = .{ .u32 = 5 };
    try std.testing.expectError(error.ClipclapProjectionMismatch, inspectFilePair(&clip_file, &clap_file));
    clap_metadata[1].value = .{ .u32 = 3 };

    const full_clap_tensors = clap_file.tensors;
    clap_file.tensors = clap_file.tensors[0 .. clap_file.tensors.len - 1];
    try std.testing.expectError(error.InvalidClipclapTensorContract, inspectFilePair(&clip_file, &clap_file));
    clap_file.tensors = full_clap_tensors;

    clip_metadata[0].value = .{ .string = "clap" };
    try std.testing.expectError(error.UnsupportedClipclapArchitecture, inspectFilePair(&clip_file, &clap_file));
}

test "ClipClap GGUF contract validates published artifacts when configured" {
    const allocator = std.testing.allocator;
    const clip_env = std.c.getenv("ANTFLY_TEST_CLIPCLAP_CLIP_GGUF") orelse return error.SkipZigTest;
    const clip_path = try allocator.dupe(u8, std.mem.span(clip_env));
    defer allocator.free(clip_path);
    const clap_env = std.c.getenv("ANTFLY_TEST_CLIPCLAP_CLAP_GGUF") orelse return error.SkipZigTest;
    const clap_path = try allocator.dupe(u8, std.mem.span(clap_env));
    defer allocator.free(clap_path);

    var clip_mapped = try c_file.MmapRegion.init(allocator, clip_path);
    defer clip_mapped.deinit();
    var clap_mapped = try c_file.MmapRegion.init(allocator, clap_path);
    defer clap_mapped.deinit();
    var clip_file = try gguf_format.parseStructure(allocator, clip_mapped.data);
    defer clip_file.deinit(allocator);
    var clap_file = try gguf_format.parseStructure(allocator, clap_mapped.data);
    defer clap_file.deinit(allocator);
    try gguf_format.validateTensorDataRanges(&clip_file, clip_mapped.data.len);
    try gguf_format.validateTensorDataRanges(&clap_file, clap_mapped.data.len);

    const contract = try inspectFilePair(&clip_file, &clap_file);
    try std.testing.expect(contract.projection_dim > 0);
}
