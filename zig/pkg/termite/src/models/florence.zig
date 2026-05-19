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

// Florence-2 vision-language model configuration.
//
// Encoder-decoder architecture with DaViT vision encoder and BART-like text decoder.
// Used for OCR, captioning, object detection, and other vision-language tasks.

const std = @import("std");
const gguf_metadata = @import("../gguf/metadata.zig");
const gguf_format = @import("../gguf/format.zig");

pub const ImageFeatureSource = enum {
    spatial_avg_pool,
    temporal_avg_pool,
    last_frame,
};

pub const Config = struct {
    pub const stage_count = 4;

    // Text decoder config
    d_model: u32 = 768,
    encoder_layers: u32 = 6,
    decoder_layers: u32 = 6,
    encoder_attention_heads: u32 = 12,
    decoder_attention_heads: u32 = 12,
    encoder_ffn_dim: u32 = 3072,
    decoder_ffn_dim: u32 = 3072,
    vocab_size: u32 = 51289,
    max_position_embeddings: u32 = 1024,

    // Vision encoder config
    image_size: u32 = 768,
    vision_hidden_size: u32 = 768,
    patch_size: [stage_count]u32 = .{ 7, 3, 3, 3 },
    patch_stride: [stage_count]u32 = .{ 4, 2, 2, 2 },
    patch_padding: [stage_count]u32 = .{ 3, 1, 1, 1 },
    patch_prenorm: [stage_count]bool = .{ false, true, true, true },
    dim_embed: [stage_count]u32 = .{ 256, 512, 1024, 2048 },
    num_heads: [stage_count]u32 = .{ 8, 16, 32, 64 },
    num_groups: [stage_count]u32 = .{ 8, 16, 32, 64 },
    depths: [stage_count]u32 = .{ 1, 1, 9, 1 },
    window_size: u32 = 12,
    image_pos_embed_max_pos: u32 = 50,
    visual_temporal_max_embeddings: u32 = 100,
    image_feature_sources: [3]ImageFeatureSource = .{
        .spatial_avg_pool,
        .temporal_avg_pool,
        .last_frame,
    },
    image_feature_source_count: u32 = 2,

    // Projection
    projection_dim: u32 = 768,
    image_token_id: i32 = 51289,

    // Token IDs
    bos_token_id: i32 = 2,
    eos_token_id: i32 = 2,
    pad_token_id: i32 = 1,
    decoder_start_token_id: i32 = 2,

    pub fn decoderHeadDim(self: Config) u32 {
        return self.d_model / self.decoder_attention_heads;
    }

    pub fn encoderHeadDim(self: Config) u32 {
        return self.d_model / self.encoder_attention_heads;
    }
};

pub fn parseConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    var config = Config{};

    // Florence2 nests text params under "text_config"
    const text_obj = if (obj.get("text_config")) |tc| switch (tc) {
        .object => |o| o,
        else => obj,
    } else obj;

    if (text_obj.get("d_model")) |v| if (jsonU32(v)) |val| {
        config.d_model = val;
    };
    if (text_obj.get("encoder_layers")) |v| if (jsonU32(v)) |val| {
        config.encoder_layers = val;
    };
    if (text_obj.get("decoder_layers")) |v| if (jsonU32(v)) |val| {
        config.decoder_layers = val;
    };
    if (text_obj.get("encoder_attention_heads")) |v| if (jsonU32(v)) |val| {
        config.encoder_attention_heads = val;
    };
    if (text_obj.get("decoder_attention_heads")) |v| if (jsonU32(v)) |val| {
        config.decoder_attention_heads = val;
    };
    if (text_obj.get("encoder_ffn_dim")) |v| if (jsonU32(v)) |val| {
        config.encoder_ffn_dim = val;
    };
    if (text_obj.get("decoder_ffn_dim")) |v| if (jsonU32(v)) |val| {
        config.decoder_ffn_dim = val;
    };
    if (text_obj.get("vocab_size")) |v| if (jsonU32(v)) |val| {
        config.vocab_size = val;
    };
    if (text_obj.get("max_position_embeddings")) |v| if (jsonU32(v)) |val| {
        config.max_position_embeddings = val;
    };

    // Vision config may be nested
    const vision_obj = if (obj.get("vision_config")) |vc| switch (vc) {
        .object => |o| o,
        else => obj,
    } else obj;

    if (vision_obj.get("image_size")) |v| if (jsonU32(v)) |val| {
        config.image_size = val;
    };
    if (vision_obj.get("hidden_size")) |v| if (jsonU32(v)) |val| {
        config.vision_hidden_size = val;
    };
    if (vision_obj.get("patch_size")) |v| parseU32Array(&config.patch_size, v);
    if (vision_obj.get("patch_stride")) |v| parseU32Array(&config.patch_stride, v);
    if (vision_obj.get("patch_padding")) |v| parseU32Array(&config.patch_padding, v);
    if (vision_obj.get("patch_prenorm")) |v| parseBoolArray(&config.patch_prenorm, v);
    if (vision_obj.get("dim_embed")) |v| parseU32Array(&config.dim_embed, v);
    if (vision_obj.get("num_heads")) |v| parseU32Array(&config.num_heads, v);
    if (vision_obj.get("num_groups")) |v| parseU32Array(&config.num_groups, v);
    if (vision_obj.get("depths")) |v| parseU32Array(&config.depths, v);
    if (vision_obj.get("window_size")) |v| if (jsonU32(v)) |val| {
        config.window_size = val;
    };
    if (vision_obj.get("image_pos_embed")) |v| parseImagePosEmbed(&config, v);
    if (vision_obj.get("visual_temporal_embedding")) |v| parseVisualTemporalEmbedding(&config, v);
    if (vision_obj.get("image_feature_source")) |v| parseImageFeatureSources(&config, v);

    // Top-level fields
    if (obj.get("projection_dim")) |v| if (jsonU32(v)) |val| {
        config.projection_dim = val;
    };
    if (obj.get("image_token_id")) |v| if (jsonI32(v)) |val| {
        config.image_token_id = val;
    };
    if (obj.get("bos_token_id")) |v| if (jsonI32(v)) |val| {
        config.bos_token_id = val;
    };
    if (obj.get("eos_token_id")) |v| if (jsonI32(v)) |val| {
        config.eos_token_id = val;
    };
    if (obj.get("pad_token_id")) |v| if (jsonI32(v)) |val| {
        config.pad_token_id = val;
    };
    if (obj.get("decoder_start_token_id")) |v| if (jsonI32(v)) |val| {
        config.decoder_start_token_id = val;
    };

    return config;
}

pub fn isFlorenceModel(model_type: []const u8) bool {
    return std.mem.eql(u8, model_type, "florence2") or
        std.mem.eql(u8, model_type, "florence-2") or
        std.mem.startsWith(u8, model_type, "florence");
}

pub fn parseGgufMetadata(view: gguf_metadata.View) ?Config {
    const arch = view.getString("general.architecture") orelse return null;
    if (!std.mem.eql(u8, arch, "florence")) return null;

    var config = Config{};
    config.d_model = metaU32(view, "florence.text.d_model") orelse config.d_model;
    config.encoder_layers = metaU32(view, "florence.text.encoder_layers") orelse config.encoder_layers;
    config.decoder_layers = metaU32(view, "florence.text.decoder_layers") orelse config.decoder_layers;
    config.encoder_attention_heads = metaU32(view, "florence.text.encoder_attention_heads") orelse config.encoder_attention_heads;
    config.decoder_attention_heads = metaU32(view, "florence.text.decoder_attention_heads") orelse config.decoder_attention_heads;
    config.encoder_ffn_dim = metaU32(view, "florence.text.encoder_ffn_dim") orelse config.encoder_ffn_dim;
    config.decoder_ffn_dim = metaU32(view, "florence.text.decoder_ffn_dim") orelse config.decoder_ffn_dim;
    config.vocab_size = metaU32(view, "florence.text.vocab_size") orelse config.vocab_size;
    config.max_position_embeddings = metaU32(view, "florence.text.max_position_embeddings") orelse config.max_position_embeddings;

    config.image_size = metaU32(view, "florence.vision.image_size") orelse config.image_size;
    config.vision_hidden_size = metaU32(view, "florence.vision.hidden_size") orelse config.vision_hidden_size;
    parseFixedU32Array(&config.patch_size, view.find("florence.vision.patch_size"));
    parseFixedU32Array(&config.patch_stride, view.find("florence.vision.patch_stride"));
    parseFixedU32Array(&config.patch_padding, view.find("florence.vision.patch_padding"));
    parseFixedBoolArray(&config.patch_prenorm, view.find("florence.vision.patch_prenorm"));
    parseFixedU32Array(&config.dim_embed, view.find("florence.vision.dim_embed"));
    parseFixedU32Array(&config.num_heads, view.find("florence.vision.num_heads"));
    parseFixedU32Array(&config.num_groups, view.find("florence.vision.num_groups"));
    parseFixedU32Array(&config.depths, view.find("florence.vision.depths"));
    config.window_size = metaU32(view, "florence.vision.window_size") orelse config.window_size;
    config.image_pos_embed_max_pos = metaU32(view, "florence.vision.image_pos_embed_max_pos") orelse config.image_pos_embed_max_pos;
    config.visual_temporal_max_embeddings = metaU32(view, "florence.vision.visual_temporal_max_embeddings") orelse config.visual_temporal_max_embeddings;
    parseImageFeatureSourceArray(&config, view.find("florence.vision.image_feature_source"));

    config.projection_dim = metaU32(view, "florence.projection_dim") orelse config.projection_dim;
    config.image_token_id = metaI32(view, "florence.image_token_id") orelse config.image_token_id;
    config.bos_token_id = metaI32(view, "florence.bos_token_id") orelse config.bos_token_id;
    config.eos_token_id = metaI32(view, "florence.eos_token_id") orelse config.eos_token_id;
    config.pad_token_id = metaI32(view, "florence.pad_token_id") orelse config.pad_token_id;
    config.decoder_start_token_id = metaI32(view, "florence.decoder_start_token_id") orelse config.decoder_start_token_id;
    return config;
}

fn jsonU32(val: std.json.Value) ?u32 {
    return switch (val) {
        .integer => |i| @intCast(i),
        else => null,
    };
}

fn metaU32(view: gguf_metadata.View, key: []const u8) ?u32 {
    return std.math.cast(u32, view.getU64(key) orelse return null);
}

fn metaI32(view: gguf_metadata.View, key: []const u8) ?i32 {
    return std.math.cast(i32, view.getI64(key) orelse return null);
}

fn jsonI32(val: std.json.Value) ?i32 {
    return switch (val) {
        .integer => |i| @intCast(i),
        else => null,
    };
}

fn parseU32Array(target: anytype, val: std.json.Value) void {
    if (val != .array) return;
    const len = @min(target.len, val.array.items.len);
    for (0..len) |i| {
        if (jsonU32(val.array.items[i])) |parsed| target[i] = parsed;
    }
}

fn parseBoolArray(target: anytype, val: std.json.Value) void {
    if (val != .array) return;
    const len = @min(target.len, val.array.items.len);
    for (0..len) |i| {
        if (val.array.items[i] == .bool) target[i] = val.array.items[i].bool;
    }
}

fn parseImagePosEmbed(config: *Config, val: std.json.Value) void {
    if (val != .object) return;
    if (val.object.get("max_pos_embeddings")) |field| {
        if (jsonU32(field)) |parsed| config.image_pos_embed_max_pos = parsed;
    }
}

fn parseVisualTemporalEmbedding(config: *Config, val: std.json.Value) void {
    if (val != .object) return;
    if (val.object.get("max_temporal_embeddings")) |field| {
        if (jsonU32(field)) |parsed| config.visual_temporal_max_embeddings = parsed;
    }
}

fn parseImageFeatureSources(config: *Config, val: std.json.Value) void {
    if (val != .array) return;
    var count: u32 = 0;
    for (val.array.items) |item| {
        if (count >= config.image_feature_sources.len) break;
        if (item != .string) continue;
        const parsed: ?ImageFeatureSource = if (std.mem.eql(u8, item.string, "spatial_avg_pool"))
            .spatial_avg_pool
        else if (std.mem.eql(u8, item.string, "temporal_avg_pool"))
            .temporal_avg_pool
        else if (std.mem.eql(u8, item.string, "last_frame"))
            .last_frame
        else
            null;
        if (parsed) |source| {
            config.image_feature_sources[count] = source;
            count += 1;
        }
    }
    if (count > 0) config.image_feature_source_count = count;
}

fn parseFixedU32Array(target: anytype, entry: ?*const gguf_format.MetadataEntry) void {
    const metadata = entry orelse return;
    if (metadata.value != .array) return;
    const arr = metadata.value.array;
    const len = @min(target.len, arr.values.len);
    for (0..len) |i| {
        target[i] = switch (arr.values[i]) {
            .u8 => |value| value,
            .u16 => |value| value,
            .u32 => |value| value,
            .u64 => |value| std.math.cast(u32, value) orelse target[i],
            else => target[i],
        };
    }
}

fn parseFixedBoolArray(target: anytype, entry: ?*const gguf_format.MetadataEntry) void {
    const metadata = entry orelse return;
    if (metadata.value != .array) return;
    const arr = metadata.value.array;
    const len = @min(target.len, arr.values.len);
    for (0..len) |i| {
        target[i] = switch (arr.values[i]) {
            .bool_ => |value| value,
            else => target[i],
        };
    }
}

fn parseImageFeatureSourceArray(config: *Config, entry: ?*const gguf_format.MetadataEntry) void {
    const metadata = entry orelse return;
    if (metadata.value != .array) return;
    const arr = metadata.value.array;
    var count: u32 = 0;
    for (arr.values) |value| {
        if (count >= config.image_feature_sources.len) break;
        if (value != .string) continue;
        const parsed: ?ImageFeatureSource = if (std.mem.eql(u8, value.string, "spatial_avg_pool"))
            .spatial_avg_pool
        else if (std.mem.eql(u8, value.string, "temporal_avg_pool"))
            .temporal_avg_pool
        else if (std.mem.eql(u8, value.string, "last_frame"))
            .last_frame
        else
            null;
        if (parsed) |source| {
            config.image_feature_sources[count] = source;
            count += 1;
        }
    }
    if (count > 0) config.image_feature_source_count = count;
}

// -- Tests --

test "parse florence2 base config" {
    const allocator = std.testing.allocator;
    const json =
        \\{"model_type":"florence2","text_config":{"d_model":768,"encoder_layers":6,"decoder_layers":6,"encoder_attention_heads":12,"decoder_attention_heads":12,"encoder_ffn_dim":3072,"decoder_ffn_dim":3072,"vocab_size":51289},"vision_config":{"image_size":768,"hidden_size":768,"patch_size":[7,3,3,3],"patch_stride":[4,2,2,2],"patch_padding":[3,1,1,1],"patch_prenorm":[false,true,true,true],"dim_embed":[256,512,1024,2048],"num_heads":[8,16,32,64],"num_groups":[8,16,32,64],"depths":[1,1,9,1],"window_size":12,"image_feature_source":["spatial_avg_pool","temporal_avg_pool"]},"projection_dim":768}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(@as(u32, 768), config.d_model);
    try std.testing.expectEqual(@as(u32, 6), config.encoder_layers);
    try std.testing.expectEqual(@as(u32, 6), config.decoder_layers);
    try std.testing.expectEqual(@as(u32, 12), config.decoder_attention_heads);
    try std.testing.expectEqual(@as(u32, 768), config.image_size);
    try std.testing.expectEqual(@as(u32, 256), config.dim_embed[0]);
    try std.testing.expectEqual(@as(u32, 2), config.image_feature_source_count);
    try std.testing.expectEqual(@as(u32, 64), config.decoderHeadDim());
}

test "parse florence2 large config" {
    const allocator = std.testing.allocator;
    const json =
        \\{"model_type":"florence2","text_config":{"d_model":1024,"encoder_layers":12,"decoder_layers":12,"encoder_attention_heads":16,"decoder_attention_heads":16,"encoder_ffn_dim":4096,"decoder_ffn_dim":4096,"vocab_size":51289},"vision_config":{"image_size":768,"hidden_size":1024,"dim_embed":[256,512,1024,2048],"num_heads":[8,16,32,64],"num_groups":[8,16,32,64],"depths":[1,1,9,1],"window_size":12,"image_pos_embed":{"type":"learned_abs_2d","max_pos_embeddings":50},"visual_temporal_embedding":{"type":"COSINE","max_temporal_embeddings":100},"image_feature_source":["spatial_avg_pool","temporal_avg_pool"]},"projection_dim":1024}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(@as(u32, 1024), config.d_model);
    try std.testing.expectEqual(@as(u32, 12), config.encoder_layers);
    try std.testing.expectEqual(@as(u32, 12), config.decoder_layers);
    try std.testing.expectEqual(@as(u32, 1024), config.projection_dim);
    try std.testing.expectEqual(@as(u32, 50), config.image_pos_embed_max_pos);
    try std.testing.expectEqual(@as(u32, 100), config.visual_temporal_max_embeddings);
    try std.testing.expectEqual(.temporal_avg_pool, config.image_feature_sources[1]);
    try std.testing.expectEqual(@as(u32, 64), config.decoderHeadDim());
}

test "isFlorenceModel" {
    try std.testing.expect(isFlorenceModel("florence2"));
    try std.testing.expect(isFlorenceModel("florence-2"));
    try std.testing.expect(isFlorenceModel("florence2_base"));
    try std.testing.expect(!isFlorenceModel("bert"));
    try std.testing.expect(!isFlorenceModel("whisper"));
}

test "parse florence gguf metadata" {
    const allocator = std.testing.allocator;
    const writer = @import("../gguf/writer.zig");

    var patch_size = [_]gguf_format.MetadataValue{ .{ .u32 = 7 }, .{ .u32 = 3 }, .{ .u32 = 3 }, .{ .u32 = 3 } };
    var patch_stride = [_]gguf_format.MetadataValue{ .{ .u32 = 4 }, .{ .u32 = 2 }, .{ .u32 = 2 }, .{ .u32 = 2 } };
    var patch_padding = [_]gguf_format.MetadataValue{ .{ .u32 = 3 }, .{ .u32 = 1 }, .{ .u32 = 1 }, .{ .u32 = 1 } };
    var patch_prenorm = [_]gguf_format.MetadataValue{ .{ .bool_ = false }, .{ .bool_ = true }, .{ .bool_ = true }, .{ .bool_ = true } };
    var dim_embed = [_]gguf_format.MetadataValue{ .{ .u32 = 256 }, .{ .u32 = 512 }, .{ .u32 = 1024 }, .{ .u32 = 2048 } };
    var num_heads = [_]gguf_format.MetadataValue{ .{ .u32 = 8 }, .{ .u32 = 16 }, .{ .u32 = 32 }, .{ .u32 = 64 } };
    var num_groups = [_]gguf_format.MetadataValue{ .{ .u32 = 8 }, .{ .u32 = 16 }, .{ .u32 = 32 }, .{ .u32 = 64 } };
    var depths = [_]gguf_format.MetadataValue{ .{ .u32 = 1 }, .{ .u32 = 1 }, .{ .u32 = 9 }, .{ .u32 = 1 } };
    var image_feature_source = [_]gguf_format.MetadataValue{ .{ .string = "spatial_avg_pool" }, .{ .string = "temporal_avg_pool" } };
    const metadata = [_]gguf_format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "florence" } },
        .{ .key = "florence.text.d_model", .value = .{ .u32 = 768 } },
        .{ .key = "florence.text.encoder_layers", .value = .{ .u32 = 6 } },
        .{ .key = "florence.text.decoder_layers", .value = .{ .u32 = 6 } },
        .{ .key = "florence.text.encoder_attention_heads", .value = .{ .u32 = 12 } },
        .{ .key = "florence.text.decoder_attention_heads", .value = .{ .u32 = 12 } },
        .{ .key = "florence.text.encoder_ffn_dim", .value = .{ .u32 = 3072 } },
        .{ .key = "florence.text.decoder_ffn_dim", .value = .{ .u32 = 3072 } },
        .{ .key = "florence.text.vocab_size", .value = .{ .u32 = 51289 } },
        .{ .key = "florence.text.max_position_embeddings", .value = .{ .u32 = 1024 } },
        .{ .key = "florence.vision.image_size", .value = .{ .u32 = 768 } },
        .{ .key = "florence.vision.hidden_size", .value = .{ .u32 = 768 } },
        .{ .key = "florence.vision.patch_size", .value = .{ .array = .{ .element_type = .u32, .values = &patch_size } } },
        .{ .key = "florence.vision.patch_stride", .value = .{ .array = .{ .element_type = .u32, .values = &patch_stride } } },
        .{ .key = "florence.vision.patch_padding", .value = .{ .array = .{ .element_type = .u32, .values = &patch_padding } } },
        .{ .key = "florence.vision.patch_prenorm", .value = .{ .array = .{ .element_type = .bool_, .values = &patch_prenorm } } },
        .{ .key = "florence.vision.dim_embed", .value = .{ .array = .{ .element_type = .u32, .values = &dim_embed } } },
        .{ .key = "florence.vision.num_heads", .value = .{ .array = .{ .element_type = .u32, .values = &num_heads } } },
        .{ .key = "florence.vision.num_groups", .value = .{ .array = .{ .element_type = .u32, .values = &num_groups } } },
        .{ .key = "florence.vision.depths", .value = .{ .array = .{ .element_type = .u32, .values = &depths } } },
        .{ .key = "florence.vision.window_size", .value = .{ .u32 = 12 } },
        .{ .key = "florence.vision.image_pos_embed_max_pos", .value = .{ .u32 = 50 } },
        .{ .key = "florence.vision.visual_temporal_max_embeddings", .value = .{ .u32 = 100 } },
        .{ .key = "florence.vision.image_feature_source", .value = .{ .array = .{ .element_type = .string, .values = &image_feature_source } } },
        .{ .key = "florence.projection_dim", .value = .{ .u32 = 768 } },
        .{ .key = "florence.image_token_id", .value = .{ .i64 = 51289 } },
        .{ .key = "florence.bos_token_id", .value = .{ .i64 = 2 } },
        .{ .key = "florence.eos_token_id", .value = .{ .i64 = 2 } },
        .{ .key = "florence.pad_token_id", .value = .{ .i64 = 1 } },
        .{ .key = "florence.decoder_start_token_id", .value = .{ .i64 = 2 } },
    };
    var layout = try writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    var parsed = try gguf_format.parse(allocator, layout.header_bytes);
    defer parsed.deinit(allocator);
    const view = gguf_metadata.View.init(&parsed);
    const config = parseGgufMetadata(view).?;
    try std.testing.expectEqual(@as(u32, 768), config.d_model);
    try std.testing.expectEqual(@as(u32, 9), config.depths[2]);
    try std.testing.expectEqual(@as(u32, 2), config.image_feature_source_count);
    try std.testing.expectEqual(ImageFeatureSource.temporal_avg_pool, config.image_feature_sources[1]);
}
