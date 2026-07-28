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
/// This intentionally mirrors the fields and terminal projection tensors that
/// the runtime must consume before any weight materialization. Admission can
/// therefore reject truncated, wrong-size, or metadata-only projectors without
/// paying the cost of decoding media or uploading weights.
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
    _ = intermediate;
    if (vision_hidden % head_count != 0 or image_size % patch_size != 0)
        return error.InvalidProjectorContract;

    const grid = image_size / patch_size;
    const token_side = exactSquareRoot(tokens_per_image) orelse return error.InvalidProjectorContract;
    if (grid % token_side != 0) return error.InvalidProjectorContract;

    try requireCompatibleMatrix(
        file,
        "multi_modal_projector.mm_input_projection_weight",
        vision_hidden,
        text_hidden,
    );
    try validateAntflyGemma3TensorNames(file, block_count);
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
        const patch_size = try requiredPositiveU32(view, "clip.vision.patch_size");
        const direct = std.mem.eql(u8, projector_type, "gemma4uv") and block_count == 0;
        if (!direct and
            (block_count == 0 or head_count == 0 or intermediate == 0 or vision_hidden % head_count != 0))
            return error.InvalidProjectorContract;
        if (image_size < patch_size) return error.InvalidProjectorContract;
        try requireCompatibleMatrix(file, "mm.input_projection.weight", vision_hidden, projection_dim);
        try validateGemma4ImageTensorNames(file, block_count, direct);
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
            audio_hidden;
        try requireCompatibleMatrix(file, "mm.a.input_projection.weight", projection_input, projection_dim);
        try validateGemma4AudioTensorNames(file, block_count, direct);
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

fn requireCompatibleMatrix(
    file: *const gguf_format.File,
    name: []const u8,
    input_dim: u32,
    output_dim: u32,
) !void {
    const tensor = gguf_tensor_catalog.Catalog.init(file).find(name) orelse
        return error.InvalidProjectorContract;
    if (tensor.dimensions.len != 2) return error.InvalidProjectorContract;
    const input: u64 = input_dim;
    const output: u64 = output_dim;
    if (!((tensor.dimensions[0] == output and tensor.dimensions[1] == input) or
        (tensor.dimensions[0] == input and tensor.dimensions[1] == output)))
        return error.InvalidProjectorContract;
}

fn validateAntflyGemma3TensorNames(file: *const gguf_format.File, block_count: u32) !void {
    const base_names = [_][]const u8{
        "vision_tower.vision_model.embeddings.patch_embedding.weight",
        "vision_tower.vision_model.embeddings.patch_embedding.bias",
        "vision_tower.vision_model.embeddings.position_embedding.weight",
        "vision_tower.vision_model.post_layernorm.weight",
        "vision_tower.vision_model.post_layernorm.bias",
        "multi_modal_projector.mm_soft_emb_norm.weight",
        "multi_modal_projector.mm_input_projection_weight",
    };
    for (base_names) |name| try requireTensor(file, name);

    const layer_suffixes = [_][]const u8{
        "layer_norm1.weight",
        "layer_norm1.bias",
        "layer_norm2.weight",
        "layer_norm2.bias",
        "mlp.fc1.weight",
        "mlp.fc1.bias",
        "mlp.fc2.weight",
        "mlp.fc2.bias",
        "self_attn.q_proj.weight",
        "self_attn.q_proj.bias",
        "self_attn.k_proj.weight",
        "self_attn.k_proj.bias",
        "self_attn.v_proj.weight",
        "self_attn.v_proj.bias",
        "self_attn.out_proj.weight",
        "self_attn.out_proj.bias",
    };
    var buf: [160]u8 = undefined;
    for (0..block_count) |layer| {
        for (layer_suffixes) |suffix| {
            const name = std.fmt.bufPrint(
                &buf,
                "vision_tower.vision_model.encoder.layers.{d}.{s}",
                .{ layer, suffix },
            ) catch return error.InvalidProjectorContract;
            try requireTensor(file, name);
        }
    }
}

fn validateGemma4ImageTensorNames(
    file: *const gguf_format.File,
    block_count: u32,
    direct: bool,
) !void {
    const base_names = if (direct)
        &[_][]const u8{
            "v.patch_norm.1.weight",
            "v.patch_norm.1.bias",
            "v.patch_embd.weight",
            "v.patch_embd.bias",
            "v.patch_norm.2.weight",
            "v.patch_norm.2.bias",
            "v.position_embd.weight",
            "v.patch_norm.3.weight",
            "v.patch_norm.3.bias",
            "mm.input_projection.weight",
        }
    else
        &[_][]const u8{
            "v.patch_embd.weight",
            "v.position_embd.weight",
            "mm.input_projection.weight",
        };
    for (base_names) |name| try requireTensor(file, name);
    if (direct) return;

    const layer_suffixes = [_][]const u8{
        "ln1.weight",
        "attn_q.weight",
        "attn_q_norm.weight",
        "attn_k.weight",
        "attn_k_norm.weight",
        "attn_v.weight",
        "attn_out.weight",
        "attn_post_norm.weight",
        "ln2.weight",
        "ffn_gate.weight",
        "ffn_up.weight",
        "ffn_down.weight",
        "ffn_post_norm.weight",
    };
    var buf: [96]u8 = undefined;
    for (0..block_count) |layer| {
        for (layer_suffixes) |suffix| {
            const name = std.fmt.bufPrint(&buf, "v.blk.{d}.{s}", .{ layer, suffix }) catch
                return error.InvalidProjectorContract;
            try requireTensor(file, name);
        }
    }
}

fn validateGemma4AudioTensorNames(
    file: *const gguf_format.File,
    block_count: u32,
    direct: bool,
) !void {
    try requireTensor(file, "mm.a.input_projection.weight");
    if (direct) return;

    const base_names = [_][]const u8{
        "a.conv1d.0.weight",
        "a.conv1d.0.norm.weight",
        "a.conv1d.1.weight",
        "a.conv1d.1.norm.weight",
        "a.input_projection.weight",
        "a.pre_encode.out.weight",
        "a.pre_encode.out.bias",
    };
    for (base_names) |name| try requireTensor(file, name);

    const layer_suffixes = [_][]const u8{
        "ffn_norm.weight",
        "ffn_up.weight",
        "ffn_down.weight",
        "ffn_post_norm.weight",
        "attn_pre_norm.weight",
        "attn_q.weight",
        "attn_k.weight",
        "attn_v.weight",
        "attn_k_rel.weight",
        "per_dim_scale.weight",
        "attn_out.weight",
        "attn_post_norm.weight",
        "norm_conv.weight",
        "conv_pw1.weight",
        "conv_dw.weight",
        "conv_norm.weight",
        "conv_pw2.weight",
        "ffn_norm_1.weight",
        "ffn_up_1.weight",
        "ffn_down_1.weight",
        "ffn_post_norm_1.weight",
        "ln2.weight",
    };
    var buf: [96]u8 = undefined;
    for (0..block_count) |layer| {
        for (layer_suffixes) |suffix| {
            const name = std.fmt.bufPrint(&buf, "a.blk.{d}.{s}", .{ layer, suffix }) catch
                return error.InvalidProjectorContract;
            try requireTensor(file, name);
        }
    }
}

fn requireTensor(file: *const gguf_format.File, name: []const u8) !void {
    if (gguf_tensor_catalog.Catalog.init(file).find(name) == null)
        return error.InvalidProjectorContract;
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
