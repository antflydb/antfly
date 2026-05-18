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

// CLIP (Contrastive Language-Image Pretraining) model configuration.
//
// Dual-encoder architecture: ViT vision encoder + text transformer encoder.
// Both encoders project to a shared embedding space for cross-modal similarity.

const std = @import("std");
const gguf_metadata = @import("../gguf/metadata.zig");

pub const ModelFamily = enum {
    clip,
    siglip,
};

pub const Config = struct {
    family: ModelFamily = .clip,

    // Text encoder config
    text_hidden_size: u32 = 512,
    text_num_layers: u32 = 12,
    text_num_heads: u32 = 8,
    text_intermediate_size: u32 = 2048,
    text_max_position_embeddings: u32 = 77,
    vocab_size: u32 = 49408,

    // Vision encoder config
    vision_hidden_size: u32 = 768,
    vision_num_layers: u32 = 12,
    vision_num_heads: u32 = 12,
    vision_intermediate_size: u32 = 3072,
    image_size: u32 = 224,
    patch_size: u32 = 32,

    // Shared
    projection_dim: u32 = 512,

    pub fn textHeadDim(self: Config) u32 {
        return self.text_hidden_size / self.text_num_heads;
    }

    pub fn visionHeadDim(self: Config) u32 {
        return self.vision_hidden_size / self.vision_num_heads;
    }

    pub fn numPatches(self: Config) u32 {
        return (self.image_size / self.patch_size) * (self.image_size / self.patch_size);
    }
};

pub fn parseConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    var config = Config{};

    // Detect family
    if (obj.get("model_type")) |v| {
        if (v == .string and
            (std.mem.eql(u8, v.string, "siglip") or
                std.mem.eql(u8, v.string, "siglip_text_model")))
        {
            config.family = .siglip;
        }
    }

    // Text config (nested under "text_config")
    const text_obj = if (obj.get("text_config")) |tc| switch (tc) {
        .object => |o| o,
        else => obj,
    } else obj;

    if (text_obj.get("hidden_size")) |v| if (jsonU32(v)) |val| {
        config.text_hidden_size = val;
    };
    if (text_obj.get("num_hidden_layers")) |v| if (jsonU32(v)) |val| {
        config.text_num_layers = val;
    };
    if (text_obj.get("num_attention_heads")) |v| if (jsonU32(v)) |val| {
        config.text_num_heads = val;
    };
    if (text_obj.get("intermediate_size")) |v| if (jsonU32(v)) |val| {
        config.text_intermediate_size = val;
    };
    if (text_obj.get("max_position_embeddings")) |v| if (jsonU32(v)) |val| {
        config.text_max_position_embeddings = val;
    };
    if (text_obj.get("vocab_size")) |v| if (jsonU32(v)) |val| {
        config.vocab_size = val;
    };

    // Vision config (nested under "vision_config")
    const vision_obj = if (obj.get("vision_config")) |vc| switch (vc) {
        .object => |o| o,
        else => obj,
    } else obj;

    if (vision_obj.get("hidden_size")) |v| if (jsonU32(v)) |val| {
        config.vision_hidden_size = val;
    };
    if (vision_obj.get("num_hidden_layers")) |v| if (jsonU32(v)) |val| {
        config.vision_num_layers = val;
    };
    if (vision_obj.get("num_attention_heads")) |v| if (jsonU32(v)) |val| {
        config.vision_num_heads = val;
    };
    if (vision_obj.get("intermediate_size")) |v| if (jsonU32(v)) |val| {
        config.vision_intermediate_size = val;
    };
    if (vision_obj.get("image_size")) |v| if (jsonU32(v)) |val| {
        config.image_size = val;
    };
    if (vision_obj.get("patch_size")) |v| if (jsonU32(v)) |val| {
        config.patch_size = val;
    };

    // Top-level projection_dim (or from nested configs)
    if (obj.get("projection_dim")) |v| if (jsonU32(v)) |val| {
        config.projection_dim = val;
    };
    // Fallback: check vision_config for projection_dim
    if (config.projection_dim == 512) {
        if (vision_obj.get("projection_dim")) |v| if (jsonU32(v)) |val| {
            config.projection_dim = val;
        };
    }

    return config;
}

pub fn isClipModel(model_type: []const u8) bool {
    return std.mem.eql(u8, model_type, "clip") or
        std.mem.eql(u8, model_type, "clip_text_model") or
        std.mem.eql(u8, model_type, "clip_vision_model") or
        std.mem.eql(u8, model_type, "siglip") or
        std.mem.eql(u8, model_type, "siglip_text_model");
}

pub fn parseGgufMetadata(view: gguf_metadata.View) ?Config {
    const arch = view.getString("general.architecture") orelse return null;
    if (!std.mem.eql(u8, arch, "clip")) return null;

    var buf: [96]u8 = undefined;
    var config = Config{};
    if (view.getString("clip.family")) |family| {
        if (std.mem.eql(u8, family, "siglip")) config.family = .siglip;
    }
    config.text_hidden_size = metaU32(view, &buf, "clip.text.embedding_length") orelse config.text_hidden_size;
    config.text_num_layers = metaU32(view, &buf, "clip.text.block_count") orelse config.text_num_layers;
    config.text_num_heads = metaU32(view, &buf, "clip.text.attention.head_count") orelse config.text_num_heads;
    config.text_intermediate_size = metaU32(view, &buf, "clip.text.feed_forward_length") orelse config.text_intermediate_size;
    config.text_max_position_embeddings = metaU32(view, &buf, "clip.text.context_length") orelse config.text_max_position_embeddings;
    config.vocab_size = metaU32(view, &buf, "clip.text.vocab_size") orelse config.vocab_size;
    config.vision_hidden_size = metaU32(view, &buf, "clip.vision.embedding_length") orelse config.vision_hidden_size;
    config.vision_num_layers = metaU32(view, &buf, "clip.vision.block_count") orelse config.vision_num_layers;
    config.vision_num_heads = metaU32(view, &buf, "clip.vision.attention.head_count") orelse config.vision_num_heads;
    config.vision_intermediate_size = metaU32(view, &buf, "clip.vision.feed_forward_length") orelse config.vision_intermediate_size;
    config.image_size = metaU32(view, &buf, "clip.vision.image_size") orelse config.image_size;
    config.patch_size = metaU32(view, &buf, "clip.vision.patch_size") orelse config.patch_size;
    config.projection_dim = metaU32(view, &buf, "clip.projection_dim") orelse config.projection_dim;
    return config;
}

fn metaU32(view: gguf_metadata.View, buf: *[96]u8, key: []const u8) ?u32 {
    _ = buf;
    return std.math.cast(u32, view.getU64(key) orelse return null);
}

fn jsonU32(val: std.json.Value) ?u32 {
    return switch (val) {
        .integer => |i| @intCast(i),
        else => null,
    };
}

// -- Tests --

test "parse clip vit-base-patch32 config" {
    const allocator = std.testing.allocator;
    const json =
        \\{"model_type": "clip", "projection_dim": 512, "text_config": {"hidden_size": 512, "num_hidden_layers": 12, "num_attention_heads": 8, "intermediate_size": 2048, "max_position_embeddings": 77, "vocab_size": 49408}, "vision_config": {"hidden_size": 768, "num_hidden_layers": 12, "num_attention_heads": 12, "intermediate_size": 3072, "image_size": 224, "patch_size": 32}}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(@as(u32, 512), config.text_hidden_size);
    try std.testing.expectEqual(@as(u32, 768), config.vision_hidden_size);
    try std.testing.expectEqual(@as(u32, 512), config.projection_dim);
    try std.testing.expectEqual(@as(u32, 224), config.image_size);
    try std.testing.expectEqual(@as(u32, 32), config.patch_size);
    try std.testing.expectEqual(@as(u32, 49), config.numPatches());
    try std.testing.expectEqual(@as(u32, 64), config.textHeadDim());
    try std.testing.expectEqual(@as(u32, 64), config.visionHeadDim());
    try std.testing.expectEqual(ModelFamily.clip, config.family);
}

test "parse clip vit-large-patch14 config" {
    const allocator = std.testing.allocator;
    const json =
        \\{"model_type": "clip", "projection_dim": 768, "text_config": {"hidden_size": 768, "num_hidden_layers": 12, "num_attention_heads": 12, "intermediate_size": 3072, "max_position_embeddings": 77}, "vision_config": {"hidden_size": 1024, "num_hidden_layers": 24, "num_attention_heads": 16, "intermediate_size": 4096, "image_size": 224, "patch_size": 14}}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(@as(u32, 768), config.text_hidden_size);
    try std.testing.expectEqual(@as(u32, 1024), config.vision_hidden_size);
    try std.testing.expectEqual(@as(u32, 768), config.projection_dim);
    try std.testing.expectEqual(@as(u32, 14), config.patch_size);
    try std.testing.expectEqual(@as(u32, 256), config.numPatches());
}

test "parse siglip config" {
    const allocator = std.testing.allocator;
    const json =
        \\{"model_type": "siglip", "projection_dim": 768, "text_config": {"hidden_size": 768, "num_hidden_layers": 12, "num_attention_heads": 12}, "vision_config": {"hidden_size": 768, "num_hidden_layers": 12, "num_attention_heads": 12, "image_size": 224, "patch_size": 16}}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.siglip, config.family);
}

test "parse siglip text model config" {
    const allocator = std.testing.allocator;
    const json =
        \\{"model_type":"siglip_text_model","projection_dim":768,"text_config":{"hidden_size":768,"num_hidden_layers":12,"num_attention_heads":12}}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.siglip, config.family);
}

test "isClipModel" {
    try std.testing.expect(isClipModel("clip"));
    try std.testing.expect(isClipModel("clip_text_model"));
    try std.testing.expect(isClipModel("siglip"));
    try std.testing.expect(isClipModel("siglip_text_model"));
    try std.testing.expect(!isClipModel("bert"));
    try std.testing.expect(!isClipModel("gpt2"));
}

test "parse clip gguf metadata" {
    const allocator = std.testing.allocator;
    const format = @import("../gguf/format.zig");
    const writer = @import("../gguf/writer.zig");

    const metadata = [_]format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.family", .value = .{ .string = "clip" } },
        .{ .key = "clip.text.embedding_length", .value = .{ .u32 = 512 } },
        .{ .key = "clip.text.block_count", .value = .{ .u32 = 12 } },
        .{ .key = "clip.text.attention.head_count", .value = .{ .u32 = 8 } },
        .{ .key = "clip.text.feed_forward_length", .value = .{ .u32 = 2048 } },
        .{ .key = "clip.text.context_length", .value = .{ .u32 = 77 } },
        .{ .key = "clip.text.vocab_size", .value = .{ .u32 = 49408 } },
        .{ .key = "clip.vision.embedding_length", .value = .{ .u32 = 768 } },
        .{ .key = "clip.vision.block_count", .value = .{ .u32 = 12 } },
        .{ .key = "clip.vision.attention.head_count", .value = .{ .u32 = 12 } },
        .{ .key = "clip.vision.feed_forward_length", .value = .{ .u32 = 3072 } },
        .{ .key = "clip.vision.image_size", .value = .{ .u32 = 224 } },
        .{ .key = "clip.vision.patch_size", .value = .{ .u32 = 32 } },
        .{ .key = "clip.projection_dim", .value = .{ .u32 = 512 } },
    };
    var layout = try writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    var parsed = try format.parse(allocator, layout.header_bytes);
    defer parsed.deinit(allocator);
    const view = gguf_metadata.View.init(&parsed);
    const config = parseGgufMetadata(view).?;
    try std.testing.expectEqual(ModelFamily.clip, config.family);
    try std.testing.expectEqual(@as(u32, 512), config.text_hidden_size);
    try std.testing.expectEqual(@as(u32, 768), config.vision_hidden_size);
    try std.testing.expectEqual(@as(u32, 512), config.projection_dim);
}
