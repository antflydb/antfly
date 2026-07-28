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
const c_file = @import("../util/c_file.zig");
const gguf_format = @import("../gguf/format.zig");
const gguf_metadata = @import("../gguf/metadata.zig");
const gguf_tensor_catalog = @import("../gguf/tensor_catalog.zig");
const gguf_mod = @import("../gguf/root.zig");
const compat = @import("../io/compat.zig");

pub const gemma4_spatial_merge_size: u64 = 3;

pub const Kind = enum {
    unknown,
    antfly_gemma3,
    clip_gemma4_image,
    clip_gemma4_audio,
    clip_gemma4_image_audio,
};

pub const Contract = struct {
    kind: Kind,
    text_hidden_size: u32,
    tokens_per_image: ?u32 = null,
    supports_image: bool,
    supports_audio: bool,
};

pub fn detectPath(allocator: std.mem.Allocator, projector_path: []const u8) !Kind {
    var mapped = try c_file.MmapRegion.init(allocator, projector_path);
    defer mapped.deinit();

    var parsed = try gguf_format.parse(allocator, mapped.data);
    defer parsed.deinit(allocator);
    return detectFile(&parsed);
}

pub fn detectBytes(allocator: std.mem.Allocator, raw: []const u8) !Kind {
    var parsed = try gguf_format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    return detectFile(&parsed);
}

pub fn detectFile(file: *const gguf_format.File) Kind {
    const view = gguf_metadata.View.init(file);
    const arch = view.getString("general.architecture") orelse return .unknown;

    if (std.mem.eql(u8, arch, "antfly-projector")) {
        const source_arch = view.getString("inference.projector.source_architecture") orelse return .unknown;
        if (std.mem.eql(u8, source_arch, "gemma3")) return .antfly_gemma3;
        return .unknown;
    }

    if (!std.mem.eql(u8, arch, "clip")) return .unknown;

    const has_image = blk: {
        const projector_type = view.getString("clip.vision.projector_type") orelse break :blk false;
        break :blk isGemma4ImageProjectorType(projector_type);
    };
    const has_audio = blk: {
        const projector_type = view.getString("clip.audio.projector_type") orelse break :blk false;
        break :blk isGemma4AudioProjectorType(projector_type);
    };

    if (has_image and has_audio) return .clip_gemma4_image_audio;
    if (has_image) return .clip_gemma4_image;
    if (has_audio) return .clip_gemma4_audio;
    return .unknown;
}

/// Inspect the projector contract using only GGUF metadata and tensor headers.
///
/// This intentionally mirrors the metadata and tensor shapes consumed by the
/// runtime before any weight materialization. Admission can therefore reject
/// truncated, wrong-size, or metadata-only projectors without paying the cost
/// of decoding media or uploading weights.
pub fn inspectFileContract(file: *const gguf_format.File) !Contract {
    return switch (detectFile(file)) {
        .antfly_gemma3 => inspectAntflyGemma3Contract(file),
        .clip_gemma4_image => inspectGemma4ClipContract(file, true, false),
        .clip_gemma4_audio => inspectGemma4ClipContract(file, false, true),
        .clip_gemma4_image_audio => inspectGemma4ClipContract(file, true, true),
        .unknown => error.UnsupportedProjectorFormat,
    };
}

fn inspectAntflyGemma3Contract(file: *const gguf_format.File) !Contract {
    const view = gguf_metadata.View.init(file);
    const text_hidden = try requiredPositiveU32(view, "inference.projector.text_hidden_size");
    const vision_hidden = try requiredPositiveU32(view, "inference.projector.vision_hidden_size");
    const intermediate = try requiredPositiveU32(view, "inference.projector.vision_feed_forward_length");
    const block_count = try requiredPositiveU32(view, "inference.projector.vision_block_count");
    const head_count = try requiredPositiveU32(view, "inference.projector.vision_attention_head_count");
    const image_size = try requiredPositiveU32(view, "inference.projector.vision_image_size");
    const patch_size = try requiredPositiveU32(view, "inference.projector.vision_patch_size");
    const tokens_per_image = try requiredPositiveU32(view, "inference.projector.mm_tokens_per_image");
    if (vision_hidden % head_count != 0 or image_size % patch_size != 0)
        return error.InvalidProjectorContract;

    const grid = image_size / patch_size;
    const token_side = exactSquareRoot(tokens_per_image) orelse return error.InvalidProjectorContract;
    if (grid % token_side != 0) return error.InvalidProjectorContract;

    const validator = TensorValidator.init(file);
    try validateAntflyGemma3TensorShapes(
        validator,
        text_hidden,
        vision_hidden,
        intermediate,
        block_count,
        image_size,
        patch_size,
    );
    return .{
        .kind = .antfly_gemma3,
        .text_hidden_size = text_hidden,
        .tokens_per_image = tokens_per_image,
        .supports_image = true,
        .supports_audio = false,
    };
}

fn inspectGemma4ClipContract(
    file: *const gguf_format.File,
    has_image: bool,
    has_audio: bool,
) !Contract {
    const view = gguf_metadata.View.init(file);
    var text_hidden: ?u32 = null;

    if (has_image) {
        const projector_type = view.getString("clip.vision.projector_type") orelse
            return error.InvalidProjectorContract;
        const projection_dim = try requiredPositiveU32(view, "clip.vision.projection_dim");
        const vision_hidden = try requiredPositiveU32(view, "clip.vision.embedding_length");
        const block_count = try requiredU32(view, "clip.vision.block_count");
        const head_count = try requiredU32(view, "clip.vision.attention.head_count");
        const intermediate = try requiredU32(view, "clip.vision.feed_forward_length");
        const image_size = try requiredPositiveU32(view, "clip.vision.image_size");
        const metadata_patch_size = try requiredPositiveU32(view, "clip.vision.patch_size");
        const direct = std.mem.eql(u8, projector_type, "gemma4uv") and block_count == 0;
        if (!direct and
            (block_count == 0 or head_count == 0 or intermediate == 0 or vision_hidden % head_count != 0))
            return error.InvalidProjectorContract;
        const patch_size = if (direct) blk: {
            const scale_factor = try optionalPositiveU32(
                view,
                "clip.vision.projector_scale_factor",
                3,
            );
            break :blk try checkedProjectorDimension(metadata_patch_size, scale_factor);
        } else metadata_patch_size;
        if (image_size < patch_size) return error.InvalidProjectorContract;
        try validateGemma4ImageTensorShapes(
            TensorValidator.init(file),
            projection_dim,
            vision_hidden,
            intermediate,
            block_count,
            head_count,
            patch_size,
            direct,
        );
        text_hidden = projection_dim;
    }

    if (has_audio) {
        const projector_type = view.getString("clip.audio.projector_type") orelse
            return error.InvalidProjectorContract;
        const projection_dim = try requiredPositiveU32(view, "clip.audio.projection_dim");
        const audio_hidden = try requiredPositiveU32(view, "clip.audio.embedding_length");
        const block_count = try requiredU32(view, "clip.audio.block_count");
        const head_count = try requiredU32(view, "clip.audio.attention.head_count");
        const intermediate = try requiredU32(view, "clip.audio.feed_forward_length");
        const direct = std.mem.eql(u8, projector_type, "gemma4ua") or
            (std.mem.eql(u8, projector_type, "gemma4uv") and block_count == 0);
        if (!direct and
            (block_count == 0 or head_count == 0 or intermediate == 0 or audio_hidden % head_count != 0))
            return error.InvalidProjectorContract;
        const projection_input = if (direct)
            try requiredPositiveU32WithFallback(
                view,
                "clip.audio.samples_per_token",
                "clip.audio.embedding_length",
            )
        else
            projection_dim;
        try validateGemma4AudioTensorShapes(
            TensorValidator.init(file),
            view,
            projection_dim,
            audio_hidden,
            intermediate,
            block_count,
            head_count,
            projection_input,
            direct,
        );
        if (text_hidden) |image_projection_dim| {
            if (image_projection_dim != projection_dim) return error.InvalidProjectorContract;
        } else {
            text_hidden = projection_dim;
        }
    }

    return .{
        .kind = detectFile(file),
        .text_hidden_size = text_hidden orelse return error.InvalidProjectorContract,
        .supports_image = has_image,
        .supports_audio = has_audio,
    };
}

fn requiredU32(view: gguf_metadata.View, key: []const u8) !u32 {
    const value = view.getU64(key) orelse return error.InvalidProjectorContract;
    if (value > std.math.maxInt(u32)) return error.InvalidProjectorContract;
    return @intCast(value);
}

fn requiredPositiveU32(view: gguf_metadata.View, key: []const u8) !u32 {
    const value = try requiredU32(view, key);
    if (value == 0) return error.InvalidProjectorContract;
    return value;
}

fn requiredPositiveU32WithFallback(
    view: gguf_metadata.View,
    primary: []const u8,
    fallback: []const u8,
) !u32 {
    if (view.getU64(primary)) |_| return requiredPositiveU32(view, primary);
    return requiredPositiveU32(view, fallback);
}

fn optionalPositiveU32(
    view: gguf_metadata.View,
    key: []const u8,
    default_value: u32,
) !u32 {
    if (view.getU64(key) == null) return default_value;
    return requiredPositiveU32(view, key);
}

fn checkedProjectorDimension(lhs: u32, rhs: u32) !u32 {
    const product = std.math.mul(u64, lhs, rhs) catch
        return error.InvalidProjectorContract;
    if (product == 0 or product > std.math.maxInt(i32)) {
        return error.InvalidProjectorContract;
    }
    return @intCast(product);
}

fn checkedRuntimeProduct(values: []const u32) !u32 {
    var product: u64 = 1;
    for (values) |value| {
        if (value == 0) return error.InvalidProjectorContract;
        product = std.math.mul(u64, product, value) catch
            return error.InvalidProjectorContract;
    }
    if (product > std.math.maxInt(i32)) return error.InvalidProjectorContract;
    return @intCast(product);
}

const TensorValidator = struct {
    catalog: gguf_tensor_catalog.Catalog,

    fn init(file: *const gguf_format.File) TensorValidator {
        return .{ .catalog = gguf_tensor_catalog.Catalog.init(file) };
    }

    fn require(self: TensorValidator, name: []const u8) !*const gguf_format.TensorInfo {
        return self.catalog.find(name) orelse error.InvalidProjectorContract;
    }

    fn requireVector(self: TensorValidator, name: []const u8, length: u64) !void {
        const tensor = try self.require(name);
        if (tensor.dimensions.len != 1 or tensor.dimensions[0] != length) {
            return error.InvalidProjectorContract;
        }
    }

    fn requireElementCount(self: TensorValidator, name: []const u8, expected: u64) !void {
        const tensor = try self.require(name);
        var count: u64 = 1;
        for (tensor.dimensions) |dimension| {
            if (dimension == 0) return error.InvalidProjectorContract;
            count = std.math.mul(u64, count, dimension) catch
                return error.InvalidProjectorContract;
        }
        if (count != expected) return error.InvalidProjectorContract;
    }

    /// Linear loaders accept either serialized orientation and normalize it
    /// before dispatch, so admission deliberately accepts the same pair.
    fn requireMatrix(
        self: TensorValidator,
        name: []const u8,
        input_dim: u64,
        output_dim: u64,
    ) !void {
        const tensor = try self.require(name);
        if (tensor.dimensions.len != 2) return error.InvalidProjectorContract;
        if (!((tensor.dimensions[0] == output_dim and tensor.dimensions[1] == input_dim) or
            (tensor.dimensions[0] == input_dim and tensor.dimensions[1] == output_dim)))
        {
            return error.InvalidProjectorContract;
        }
    }

    /// Raw backend linear weights are loaded without a runtime transpose. GGUF
    /// dimensions are the reverse of the runtime shape, hence [input, output].
    fn requireNativeMatrix(
        self: TensorValidator,
        name: []const u8,
        input_dim: u64,
        output_dim: u64,
    ) !void {
        const tensor = try self.require(name);
        if (tensor.dimensions.len != 2 or
            tensor.dimensions[0] != input_dim or
            tensor.dimensions[1] != output_dim)
        {
            return error.InvalidProjectorContract;
        }
    }

    fn requireConv2d(
        self: TensorValidator,
        name: []const u8,
        kernel_h: u64,
        kernel_w: u64,
        input_channels: u64,
        output_channels: u64,
        allow_runtime_hwio: bool,
    ) !void {
        const tensor = try self.require(name);
        if (tensor.dimensions.len != 4) return error.InvalidProjectorContract;

        // GgufStore reverses header dimensions when constructing the runtime
        // tensor shape. Compare against the exact layouts accepted by the
        // projector's convolution loaders.
        const runtime = [4]u64{
            tensor.dimensions[3],
            tensor.dimensions[2],
            tensor.dimensions[1],
            tensor.dimensions[0],
        };
        const oihw = runtime[0] == output_channels and
            runtime[1] == input_channels and
            runtime[2] == kernel_h and
            runtime[3] == kernel_w;
        const hwio = allow_runtime_hwio and
            runtime[0] == kernel_h and
            runtime[1] == kernel_w and
            runtime[2] == input_channels and
            runtime[3] == output_channels;
        if (!oihw and !hwio) return error.InvalidProjectorContract;
    }

    fn requireMatrixLikeDepthwise(
        self: TensorValidator,
        name: []const u8,
        kernel_size: u64,
        hidden: u64,
    ) !void {
        const tensor = try self.require(name);
        if (tensor.dimensions.len != 2) return error.InvalidProjectorContract;
        if (!((tensor.dimensions[0] == kernel_size and tensor.dimensions[1] == hidden) or
            (tensor.dimensions[0] == hidden and tensor.dimensions[1] == kernel_size)))
        {
            return error.InvalidProjectorContract;
        }
    }

    fn requireAxisPositionEmbedding(
        self: TensorValidator,
        name: []const u8,
        hidden: u64,
    ) !u64 {
        return axisPositionEmbeddingCapacity(try self.require(name), hidden);
    }
};

fn axisPositionEmbeddingCapacity(
    tensor: *const gguf_format.TensorInfo,
    hidden: u64,
) !u64 {
    if (tensor.dimensions.len != 3 or tensor.dimensions[1] == 0) {
        return error.InvalidProjectorContract;
    }
    const hidden_first = tensor.dimensions[0] == hidden and tensor.dimensions[2] == 2;
    const axis_first = tensor.dimensions[0] == 2 and tensor.dimensions[2] == hidden;
    if (!hidden_first and !axis_first) return error.InvalidProjectorContract;
    return tensor.dimensions[1];
}

pub fn gemma4PositionEmbeddingCapacity(
    file: *const gguf_format.File,
    hidden: u64,
) !u64 {
    const tensor = gguf_tensor_catalog.Catalog.init(file).find("v.position_embd.weight") orelse
        return error.InvalidProjectorContract;
    return axisPositionEmbeddingCapacity(tensor, hidden);
}

fn validateAntflyGemma3TensorShapes(
    validator: TensorValidator,
    text_hidden: u32,
    vision_hidden: u32,
    intermediate: u32,
    block_count: u32,
    image_size: u32,
    patch_size: u32,
) !void {
    const patch_dims = [_]u32{ patch_size, patch_size, 3 };
    _ = try checkedRuntimeProduct(&patch_dims);
    try validator.requireConv2d(
        "vision_tower.vision_model.embeddings.patch_embedding.weight",
        patch_size,
        patch_size,
        3,
        vision_hidden,
        false,
    );
    try validator.requireVector(
        "vision_tower.vision_model.embeddings.patch_embedding.bias",
        vision_hidden,
    );
    const grid: u64 = image_size / patch_size;
    const position_count = std.math.mul(
        u64,
        try std.math.mul(u64, grid, grid),
        vision_hidden,
    ) catch return error.InvalidProjectorContract;
    try validator.requireElementCount(
        "vision_tower.vision_model.embeddings.position_embedding.weight",
        position_count,
    );
    try validator.requireVector("vision_tower.vision_model.post_layernorm.weight", vision_hidden);
    try validator.requireVector("vision_tower.vision_model.post_layernorm.bias", vision_hidden);
    try validator.requireVector("multi_modal_projector.mm_soft_emb_norm.weight", vision_hidden);
    try validator.requireMatrix(
        "multi_modal_projector.mm_input_projection_weight",
        vision_hidden,
        text_hidden,
    );

    var buf: [160]u8 = undefined;
    for (0..block_count) |layer| {
        try validator.requireVector(try gemma3LayerName(&buf, layer, "layer_norm1.weight"), vision_hidden);
        try validator.requireVector(try gemma3LayerName(&buf, layer, "layer_norm1.bias"), vision_hidden);
        try validator.requireVector(try gemma3LayerName(&buf, layer, "layer_norm2.weight"), vision_hidden);
        try validator.requireVector(try gemma3LayerName(&buf, layer, "layer_norm2.bias"), vision_hidden);
        try validator.requireNativeMatrix(try gemma3LayerName(&buf, layer, "mlp.fc1.weight"), vision_hidden, intermediate);
        try validator.requireVector(try gemma3LayerName(&buf, layer, "mlp.fc1.bias"), intermediate);
        try validator.requireNativeMatrix(try gemma3LayerName(&buf, layer, "mlp.fc2.weight"), intermediate, vision_hidden);
        try validator.requireVector(try gemma3LayerName(&buf, layer, "mlp.fc2.bias"), vision_hidden);
        try validator.requireNativeMatrix(try gemma3LayerName(&buf, layer, "self_attn.q_proj.weight"), vision_hidden, vision_hidden);
        try validator.requireVector(try gemma3LayerName(&buf, layer, "self_attn.q_proj.bias"), vision_hidden);
        try validator.requireNativeMatrix(try gemma3LayerName(&buf, layer, "self_attn.k_proj.weight"), vision_hidden, vision_hidden);
        try validator.requireVector(try gemma3LayerName(&buf, layer, "self_attn.k_proj.bias"), vision_hidden);
        try validator.requireNativeMatrix(try gemma3LayerName(&buf, layer, "self_attn.v_proj.weight"), vision_hidden, vision_hidden);
        try validator.requireVector(try gemma3LayerName(&buf, layer, "self_attn.v_proj.bias"), vision_hidden);
        try validator.requireNativeMatrix(try gemma3LayerName(&buf, layer, "self_attn.out_proj.weight"), vision_hidden, vision_hidden);
        try validator.requireVector(try gemma3LayerName(&buf, layer, "self_attn.out_proj.bias"), vision_hidden);
    }
}

fn gemma3LayerName(
    buf: *[160]u8,
    layer: usize,
    suffix: []const u8,
) ![]const u8 {
    return std.fmt.bufPrint(
        buf,
        "vision_tower.vision_model.encoder.layers.{d}.{s}",
        .{ layer, suffix },
    ) catch error.InvalidProjectorContract;
}

fn validateGemma4ImageTensorShapes(
    validator: TensorValidator,
    projection_dim: u32,
    vision_hidden: u32,
    intermediate: u32,
    block_count: u32,
    head_count: u32,
    patch_size: u32,
    direct: bool,
) !void {
    try validator.requireMatrix("mm.input_projection.weight", vision_hidden, projection_dim);
    const positions_per_axis = try validator.requireAxisPositionEmbedding(
        "v.position_embd.weight",
        vision_hidden,
    );
    const spatial_merge_size: u64 = if (direct) 1 else gemma4_spatial_merge_size;
    if (positions_per_axis < spatial_merge_size) return error.InvalidProjectorContract;

    if (direct) {
        const patch_dims = [_]u32{ patch_size, patch_size, 3 };
        const patch_input = try checkedRuntimeProduct(&patch_dims);
        try validator.requireVector("v.patch_norm.1.weight", patch_input);
        try validator.requireVector("v.patch_norm.1.bias", patch_input);
        try validator.requireMatrix("v.patch_embd.weight", patch_input, vision_hidden);
        try validator.requireVector("v.patch_embd.bias", vision_hidden);
        try validator.requireVector("v.patch_norm.2.weight", vision_hidden);
        try validator.requireVector("v.patch_norm.2.bias", vision_hidden);
        try validator.requireVector("v.patch_norm.3.weight", vision_hidden);
        try validator.requireVector("v.patch_norm.3.bias", vision_hidden);
        return;
    }

    const patch_dims = [_]u32{ patch_size, patch_size, 3 };
    _ = try checkedRuntimeProduct(&patch_dims);
    try validator.requireConv2d(
        "v.patch_embd.weight",
        patch_size,
        patch_size,
        3,
        vision_hidden,
        false,
    );
    const head_dim = vision_hidden / head_count;
    var buf: [96]u8 = undefined;
    for (0..block_count) |layer| {
        try validator.requireVector(try gemma4LayerName(&buf, "v", layer, "ln1.weight"), vision_hidden);
        try validator.requireMatrix(try gemma4LayerName(&buf, "v", layer, "attn_q.weight"), vision_hidden, vision_hidden);
        try validator.requireVector(try gemma4LayerName(&buf, "v", layer, "attn_q_norm.weight"), head_dim);
        try validator.requireMatrix(try gemma4LayerName(&buf, "v", layer, "attn_k.weight"), vision_hidden, vision_hidden);
        try validator.requireVector(try gemma4LayerName(&buf, "v", layer, "attn_k_norm.weight"), head_dim);
        try validator.requireMatrix(try gemma4LayerName(&buf, "v", layer, "attn_v.weight"), vision_hidden, vision_hidden);
        try validator.requireMatrix(try gemma4LayerName(&buf, "v", layer, "attn_out.weight"), vision_hidden, vision_hidden);
        try validator.requireVector(try gemma4LayerName(&buf, "v", layer, "attn_post_norm.weight"), vision_hidden);
        try validator.requireVector(try gemma4LayerName(&buf, "v", layer, "ln2.weight"), vision_hidden);
        try validator.requireMatrix(try gemma4LayerName(&buf, "v", layer, "ffn_gate.weight"), vision_hidden, intermediate);
        try validator.requireMatrix(try gemma4LayerName(&buf, "v", layer, "ffn_up.weight"), vision_hidden, intermediate);
        try validator.requireMatrix(try gemma4LayerName(&buf, "v", layer, "ffn_down.weight"), intermediate, vision_hidden);
        try validator.requireVector(try gemma4LayerName(&buf, "v", layer, "ffn_post_norm.weight"), vision_hidden);
    }
    try validateOptionalStandardization(validator, vision_hidden);
}

fn validateGemma4AudioTensorShapes(
    validator: TensorValidator,
    view: gguf_metadata.View,
    projection_dim: u32,
    audio_hidden: u32,
    intermediate: u32,
    block_count: u32,
    head_count: u32,
    projection_input: u32,
    direct: bool,
) !void {
    try validator.requireMatrix("mm.a.input_projection.weight", projection_input, projection_dim);
    if (direct) return;

    if (audio_hidden % 2 != 0) return error.InvalidProjectorContract;
    const mel_bins = try optionalPositiveU32(view, "clip.audio.num_mel_bins", 128);
    const conv0_width = ceilHalf(mel_bins);
    const conv1_width = ceilHalf(conv0_width);
    const input_projection_dims = [_]u32{ conv1_width, 32 };
    const input_projection = try checkedRuntimeProduct(&input_projection_dims);
    try validator.requireConv2d("a.conv1d.0.weight", 3, 3, 1, 128, true);
    try validator.requireVector("a.conv1d.0.norm.weight", 128);
    try validator.requireConv2d("a.conv1d.1.weight", 3, 3, 128, 32, true);
    try validator.requireVector("a.conv1d.1.norm.weight", 32);
    try validator.requireMatrix("a.input_projection.weight", input_projection, audio_hidden);
    try validator.requireMatrix("a.pre_encode.out.weight", audio_hidden, projection_dim);
    try validator.requireVector("a.pre_encode.out.bias", projection_dim);

    const head_dim = audio_hidden / head_count;
    const doubled_hidden = try checkedProjectorDimension(audio_hidden, 2);
    var buf: [96]u8 = undefined;
    for (0..block_count) |layer| {
        try validator.requireVector(try gemma4LayerName(&buf, "a", layer, "ffn_norm.weight"), audio_hidden);
        try validator.requireMatrix(try gemma4LayerName(&buf, "a", layer, "ffn_up.weight"), audio_hidden, intermediate);
        try validator.requireMatrix(try gemma4LayerName(&buf, "a", layer, "ffn_down.weight"), intermediate, audio_hidden);
        try validator.requireVector(try gemma4LayerName(&buf, "a", layer, "ffn_post_norm.weight"), audio_hidden);
        try validator.requireVector(try gemma4LayerName(&buf, "a", layer, "attn_pre_norm.weight"), audio_hidden);
        try validator.requireMatrix(try gemma4LayerName(&buf, "a", layer, "attn_q.weight"), audio_hidden, audio_hidden);
        try validator.requireMatrix(try gemma4LayerName(&buf, "a", layer, "attn_k.weight"), audio_hidden, audio_hidden);
        try validator.requireMatrix(try gemma4LayerName(&buf, "a", layer, "attn_v.weight"), audio_hidden, audio_hidden);
        try validator.requireMatrix(try gemma4LayerName(&buf, "a", layer, "attn_k_rel.weight"), audio_hidden, audio_hidden);
        try validator.requireVector(try gemma4LayerName(&buf, "a", layer, "per_dim_scale.weight"), head_dim);
        try validator.requireMatrix(try gemma4LayerName(&buf, "a", layer, "attn_out.weight"), audio_hidden, audio_hidden);
        try validator.requireVector(try gemma4LayerName(&buf, "a", layer, "attn_post_norm.weight"), audio_hidden);
        try validator.requireVector(try gemma4LayerName(&buf, "a", layer, "norm_conv.weight"), audio_hidden);
        try validator.requireMatrix(try gemma4LayerName(&buf, "a", layer, "conv_pw1.weight"), audio_hidden, doubled_hidden);
        try validator.requireMatrixLikeDepthwise(try gemma4LayerName(&buf, "a", layer, "conv_dw.weight"), 5, audio_hidden);
        try validator.requireVector(try gemma4LayerName(&buf, "a", layer, "conv_norm.weight"), audio_hidden);
        try validator.requireMatrix(try gemma4LayerName(&buf, "a", layer, "conv_pw2.weight"), audio_hidden, audio_hidden);
        try validator.requireVector(try gemma4LayerName(&buf, "a", layer, "ffn_norm_1.weight"), audio_hidden);
        try validator.requireMatrix(try gemma4LayerName(&buf, "a", layer, "ffn_up_1.weight"), audio_hidden, intermediate);
        try validator.requireMatrix(try gemma4LayerName(&buf, "a", layer, "ffn_down_1.weight"), intermediate, audio_hidden);
        try validator.requireVector(try gemma4LayerName(&buf, "a", layer, "ffn_post_norm_1.weight"), audio_hidden);
        try validator.requireVector(try gemma4LayerName(&buf, "a", layer, "ln2.weight"), audio_hidden);
    }
}

fn ceilHalf(value: u32) u32 {
    return value / 2 + value % 2;
}

fn gemma4LayerName(
    buf: *[96]u8,
    prefix: []const u8,
    layer: usize,
    suffix: []const u8,
) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}.blk.{d}.{s}", .{ prefix, layer, suffix }) catch
        error.InvalidProjectorContract;
}

fn validateOptionalStandardization(validator: TensorValidator, hidden: u32) !void {
    const scale = validator.catalog.find("v.std_scale");
    const bias = validator.catalog.find("v.std_bias");
    if ((scale == null) != (bias == null)) return error.InvalidProjectorContract;
    if (scale != null) {
        try validator.requireVector("v.std_scale", hidden);
        try validator.requireVector("v.std_bias", hidden);
    }
}

fn exactSquareRoot(value: u32) ?u32 {
    const root: u32 = @intFromFloat(@sqrt(@as(f64, @floatFromInt(value))));
    return if (root * root == value) root else null;
}

pub fn isGemma4ImageProjectorType(projector_type: []const u8) bool {
    return std.mem.eql(u8, projector_type, "gemma4v") or
        std.mem.eql(u8, projector_type, "gemma4uv");
}

pub fn isGemma4AudioProjectorType(projector_type: []const u8) bool {
    return std.mem.eql(u8, projector_type, "gemma4a") or
        std.mem.eql(u8, projector_type, "gemma4ua") or
        std.mem.eql(u8, projector_type, "gemma4uv");
}

pub fn isAntfly(kind: Kind) bool {
    return kind == .antfly_gemma3;
}

pub fn isClip(kind: Kind) bool {
    return switch (kind) {
        .clip_gemma4_image, .clip_gemma4_audio, .clip_gemma4_image_audio => true,
        else => false,
    };
}

test "detect termite gemma3 projector" {
    const allocator = std.testing.allocator;
    const path = try std.fs.path.join(allocator, &.{ "/tmp", "antfly-projector-format-gemma3.gguf" });
    defer allocator.free(path);
    defer compat.cwd().deleteFile(compat.io(), path) catch {};

    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "antfly-projector" } },
        .{ .key = "inference.projector.source_architecture", .value = .{ .string = "gemma3" } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = layout.header_bytes });

    try std.testing.expectEqual(Kind.antfly_gemma3, try detectPath(allocator, path));
}

test "contract inspection rejects recognized but incomplete projector" {
    const allocator = std.testing.allocator;
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.vision.projector_type", .value = .{ .string = "gemma4v" } },
    };
    const dims = [_]u64{ 2, 2 };
    const tensors = [_]gguf_mod.writer.TensorSpec{
        .{ .name = "span_rep.test", .dimensions = &dims, .tensor_type = .{ .known = .F32 } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &tensors);
    defer layout.deinit(allocator);
    var parsed = try gguf_format.parse(allocator, layout.header_bytes);
    defer parsed.deinit(allocator);

    try std.testing.expectEqual(Kind.clip_gemma4_image, detectFile(&parsed));
    try std.testing.expectError(error.InvalidProjectorContract, inspectFileContract(&parsed));
}

fn buildDirectGemma4ContractFixture(
    allocator: std.mem.Allocator,
    patch_size: u32,
    scale_factor: u32,
    patch_input_dimension: u64,
    positions_per_axis: u64,
) ![]u8 {
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.vision.projector_type", .value = .{ .string = "gemma4uv" } },
        .{ .key = "clip.vision.projection_dim", .value = .{ .u32 = 4 } },
        .{ .key = "clip.vision.embedding_length", .value = .{ .u32 = 4 } },
        .{ .key = "clip.vision.feed_forward_length", .value = .{ .u32 = 0 } },
        .{ .key = "clip.vision.block_count", .value = .{ .u32 = 0 } },
        .{ .key = "clip.vision.attention.head_count", .value = .{ .u32 = 0 } },
        .{ .key = "clip.vision.image_size", .value = .{ .u32 = 224 } },
        .{ .key = "clip.vision.patch_size", .value = .{ .u32 = patch_size } },
        .{ .key = "clip.vision.projector_scale_factor", .value = .{ .u32 = scale_factor } },
    };
    const dims_patch = [_]u64{patch_input_dimension};
    const dims_hidden = [_]u64{4};
    const dims_patch_projection = [_]u64{ 4, patch_input_dimension };
    const dims_position = [_]u64{ 2, positions_per_axis, 4 };
    const dims_projection = [_]u64{ 4, 4 };
    const tensors = [_]gguf_mod.writer.TensorSpec{
        .{ .name = "v.patch_norm.1.weight", .dimensions = &dims_patch, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.patch_norm.1.bias", .dimensions = &dims_patch, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.patch_embd.weight", .dimensions = &dims_patch_projection, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.patch_embd.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.patch_norm.2.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.patch_norm.2.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.position_embd.weight", .dimensions = &dims_position, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.patch_norm.3.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.patch_norm.3.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "mm.input_projection.weight", .dimensions = &dims_projection, .tensor_type = .{ .known = .F32 } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &tensors);
    defer layout.deinit(allocator);
    return allocator.dupe(u8, layout.header_bytes);
}

test "contract inspection accepts complete direct Gemma 4 projector shapes" {
    const allocator = std.testing.allocator;
    const bytes = try buildDirectGemma4ContractFixture(allocator, 2, 1, 12, 16);
    defer allocator.free(bytes);
    var parsed = try gguf_format.parse(allocator, bytes);
    defer parsed.deinit(allocator);

    const contract = try inspectFileContract(&parsed);
    try std.testing.expectEqual(Kind.clip_gemma4_image, contract.kind);
    try std.testing.expectEqual(@as(u32, 4), contract.text_hidden_size);
}

test "contract inspection rejects zero or overflowing direct projector scale" {
    const allocator = std.testing.allocator;
    const cases = [_]struct {
        patch_size: u32,
        scale_factor: u32,
    }{
        .{ .patch_size = 2, .scale_factor = 0 },
        .{ .patch_size = std.math.maxInt(u32), .scale_factor = std.math.maxInt(u32) },
    };
    for (cases) |case| {
        const bytes = try buildDirectGemma4ContractFixture(
            allocator,
            case.patch_size,
            case.scale_factor,
            12,
            16,
        );
        defer allocator.free(bytes);
        var parsed = try gguf_format.parse(allocator, bytes);
        defer parsed.deinit(allocator);

        try std.testing.expectError(
            error.InvalidProjectorContract,
            inspectFileContract(&parsed),
        );
    }
}

test "contract inspection rejects malformed nonterminal projector tensor shape" {
    const allocator = std.testing.allocator;
    const bytes = try buildDirectGemma4ContractFixture(allocator, 2, 1, 11, 16);
    defer allocator.free(bytes);
    var parsed = try gguf_format.parse(allocator, bytes);
    defer parsed.deinit(allocator);

    try std.testing.expectError(error.InvalidProjectorContract, inspectFileContract(&parsed));
}

test "contract inspection rejects a position table smaller than spatial merge" {
    const allocator = std.testing.allocator;
    const bytes = try buildLayeredGemma4ImageContractFixtureWithPositions(
        allocator,
        2,
        2,
    );
    defer allocator.free(bytes);
    var parsed = try gguf_format.parse(allocator, bytes);
    defer parsed.deinit(allocator);

    try std.testing.expectError(error.InvalidProjectorContract, inspectFileContract(&parsed));
}

fn buildLayeredGemma4ImageContractFixture(
    allocator: std.mem.Allocator,
    q_norm_dimension: u64,
) ![]u8 {
    return buildLayeredGemma4ImageContractFixtureWithPositions(
        allocator,
        q_norm_dimension,
        16,
    );
}

fn buildLayeredGemma4ImageContractFixtureWithPositions(
    allocator: std.mem.Allocator,
    q_norm_dimension: u64,
    positions_per_axis: u64,
) ![]u8 {
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.vision.projector_type", .value = .{ .string = "gemma4v" } },
        .{ .key = "clip.vision.projection_dim", .value = .{ .u32 = 4 } },
        .{ .key = "clip.vision.embedding_length", .value = .{ .u32 = 4 } },
        .{ .key = "clip.vision.feed_forward_length", .value = .{ .u32 = 8 } },
        .{ .key = "clip.vision.block_count", .value = .{ .u32 = 1 } },
        .{ .key = "clip.vision.attention.head_count", .value = .{ .u32 = 2 } },
        .{ .key = "clip.vision.image_size", .value = .{ .u32 = 12 } },
        .{ .key = "clip.vision.patch_size", .value = .{ .u32 = 2 } },
    };
    const dims_patch = [_]u64{ 2, 2, 3, 4 };
    const dims_hidden = [_]u64{4};
    const dims_head = [_]u64{q_norm_dimension};
    const dims_position = [_]u64{ 2, positions_per_axis, 4 };
    const dims_hidden_matrix = [_]u64{ 4, 4 };
    const dims_up = [_]u64{ 4, 8 };
    const dims_down = [_]u64{ 8, 4 };
    const tensors = [_]gguf_mod.writer.TensorSpec{
        .{ .name = "v.patch_embd.weight", .dimensions = &dims_patch, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.position_embd.weight", .dimensions = &dims_position, .tensor_type = .{ .known = .F32 } },
        .{ .name = "mm.input_projection.weight", .dimensions = &dims_hidden_matrix, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.blk.0.ln1.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.blk.0.attn_q.weight", .dimensions = &dims_hidden_matrix, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.blk.0.attn_q_norm.weight", .dimensions = &dims_head, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.blk.0.attn_k.weight", .dimensions = &dims_hidden_matrix, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.blk.0.attn_k_norm.weight", .dimensions = &[_]u64{2}, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.blk.0.attn_v.weight", .dimensions = &dims_hidden_matrix, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.blk.0.attn_out.weight", .dimensions = &dims_hidden_matrix, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.blk.0.attn_post_norm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.blk.0.ln2.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.blk.0.ffn_gate.weight", .dimensions = &dims_up, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.blk.0.ffn_up.weight", .dimensions = &dims_up, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.blk.0.ffn_down.weight", .dimensions = &dims_down, .tensor_type = .{ .known = .F32 } },
        .{ .name = "v.blk.0.ffn_post_norm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &tensors);
    defer layout.deinit(allocator);
    return allocator.dupe(u8, layout.header_bytes);
}

test "contract inspection validates every layered Gemma 4 image tensor shape" {
    const allocator = std.testing.allocator;
    const valid_bytes = try buildLayeredGemma4ImageContractFixture(allocator, 2);
    defer allocator.free(valid_bytes);
    var valid = try gguf_format.parse(allocator, valid_bytes);
    defer valid.deinit(allocator);
    _ = try inspectFileContract(&valid);

    const invalid_bytes = try buildLayeredGemma4ImageContractFixture(allocator, 4);
    defer allocator.free(invalid_bytes);
    var invalid = try gguf_format.parse(allocator, invalid_bytes);
    defer invalid.deinit(allocator);
    try std.testing.expectError(error.InvalidProjectorContract, inspectFileContract(&invalid));
}

test "contract inspection accepts regular Gemma 4 audio projection width transition" {
    const allocator = std.testing.allocator;
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.audio.projector_type", .value = .{ .string = "gemma4a" } },
        .{ .key = "clip.audio.projection_dim", .value = .{ .u32 = 6 } },
        .{ .key = "clip.audio.embedding_length", .value = .{ .u32 = 4 } },
        .{ .key = "clip.audio.feed_forward_length", .value = .{ .u32 = 8 } },
        .{ .key = "clip.audio.block_count", .value = .{ .u32 = 1 } },
        .{ .key = "clip.audio.attention.head_count", .value = .{ .u32 = 2 } },
        .{ .key = "clip.audio.num_mel_bins", .value = .{ .u32 = 8 } },
    };
    const dims_hidden = [_]u64{4};
    const dims_projection = [_]u64{6};
    const dims_head = [_]u64{2};
    const dims_hidden_matrix = [_]u64{ 4, 4 };
    const dims_up = [_]u64{ 4, 8 };
    const dims_down = [_]u64{ 8, 4 };
    const dims_terminal = [_]u64{ 6, 6 };
    const dims_preencode = [_]u64{ 4, 6 };
    const dims_input_projection = [_]u64{ 64, 4 };
    const dims_conv0 = [_]u64{ 3, 3, 1, 128 };
    const dims_conv1 = [_]u64{ 3, 3, 128, 32 };
    const dims_conv0_norm = [_]u64{128};
    const dims_conv1_norm = [_]u64{32};
    const dims_depthwise = [_]u64{ 5, 4 };
    const tensors = [_]gguf_mod.writer.TensorSpec{
        .{ .name = "mm.a.input_projection.weight", .dimensions = &dims_terminal, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.conv1d.0.weight", .dimensions = &dims_conv0, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.conv1d.0.norm.weight", .dimensions = &dims_conv0_norm, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.conv1d.1.weight", .dimensions = &dims_conv1, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.conv1d.1.norm.weight", .dimensions = &dims_conv1_norm, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.input_projection.weight", .dimensions = &dims_input_projection, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.pre_encode.out.weight", .dimensions = &dims_preencode, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.pre_encode.out.bias", .dimensions = &dims_projection, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.ffn_norm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.ffn_up.weight", .dimensions = &dims_up, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.ffn_down.weight", .dimensions = &dims_down, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.ffn_post_norm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.attn_pre_norm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.attn_q.weight", .dimensions = &dims_hidden_matrix, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.attn_k.weight", .dimensions = &dims_hidden_matrix, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.attn_v.weight", .dimensions = &dims_hidden_matrix, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.attn_k_rel.weight", .dimensions = &dims_hidden_matrix, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.per_dim_scale.weight", .dimensions = &dims_head, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.attn_out.weight", .dimensions = &dims_hidden_matrix, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.attn_post_norm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.norm_conv.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.conv_pw1.weight", .dimensions = &dims_up, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.conv_dw.weight", .dimensions = &dims_depthwise, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.conv_norm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.conv_pw2.weight", .dimensions = &dims_hidden_matrix, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.ffn_norm_1.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.ffn_up_1.weight", .dimensions = &dims_up, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.ffn_down_1.weight", .dimensions = &dims_down, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.ffn_post_norm_1.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "a.blk.0.ln2.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &tensors);
    defer layout.deinit(allocator);
    var parsed = try gguf_format.parse(allocator, layout.header_bytes);
    defer parsed.deinit(allocator);

    const contract = try inspectFileContract(&parsed);
    try std.testing.expectEqual(Kind.clip_gemma4_audio, contract.kind);
    try std.testing.expectEqual(@as(u32, 6), contract.text_hidden_size);
}

test "detect clip gemma4 image projector" {
    const allocator = std.testing.allocator;
    const path = try std.fs.path.join(allocator, &.{ "/tmp", "antfly-projector-format-clip-image.gguf" });
    defer allocator.free(path);
    defer compat.cwd().deleteFile(compat.io(), path) catch {};

    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.vision.projector_type", .value = .{ .string = "gemma4v" } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = layout.header_bytes });

    try std.testing.expectEqual(Kind.clip_gemma4_image, try detectPath(allocator, path));
}

test "detect clip gemma4 unified image audio projector" {
    const allocator = std.testing.allocator;
    const path = try std.fs.path.join(allocator, &.{ "/tmp", "antfly-projector-format-clip-unified.gguf" });
    defer allocator.free(path);
    defer compat.cwd().deleteFile(compat.io(), path) catch {};

    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.vision.projector_type", .value = .{ .string = "gemma4uv" } },
        .{ .key = "clip.audio.projector_type", .value = .{ .string = "gemma4ua" } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = layout.header_bytes });

    try std.testing.expectEqual(Kind.clip_gemma4_image_audio, try detectPath(allocator, path));
}

test "detect unknown projector metadata" {
    const allocator = std.testing.allocator;
    const path = try std.fs.path.join(allocator, &.{ "/tmp", "antfly-projector-format-unknown.gguf" });
    defer allocator.free(path);
    defer compat.cwd().deleteFile(compat.io(), path) catch {};

    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.vision.projector_type", .value = .{ .string = "something-else" } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = layout.header_bytes });

    try std.testing.expectEqual(Kind.unknown, try detectPath(allocator, path));
}
