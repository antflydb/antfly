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
const gguf_metadata = @import("../gguf/metadata.zig");

pub const Config = struct {
    weight_prefix: []const u8 = "layoutlmv3",
    vocab_size: u32 = 50265,
    hidden_size: u32 = 768,
    num_hidden_layers: u32 = 12,
    num_attention_heads: u32 = 12,
    intermediate_size: u32 = 3072,
    max_position_embeddings: u32 = 512,
    type_vocab_size: u32 = 2,
    max_2d_position_embeddings: u32 = 1024,
    coordinate_size: u32 = 128,
    shape_size: u32 = 128,
    input_size: u32 = 224,
    patch_size: u32 = 16,
    num_channels: usize = 3,
    num_labels: u32 = 1,
    pad_token_id: i64 = 1,
    layer_norm_eps: f32 = 1e-5,
    has_relative_attention_bias: bool = true,
    has_spatial_attention_bias: bool = true,
    rel_pos_bins: u32 = 32,
    max_rel_pos: u32 = 128,
    rel_2d_pos_bins: u32 = 64,
    max_rel_2d_pos: u32 = 256,
    visual_bbox_max_len: i64 = 1000,

    pub fn effectivePrefix(self: Config) []const u8 {
        return self.weight_prefix;
    }
};

pub fn parseConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    var config = Config{};
    if (obj.get("vocab_size")) |v| config.vocab_size = jsonU32(v) orelse config.vocab_size;
    if (obj.get("hidden_size")) |v| config.hidden_size = jsonU32(v) orelse config.hidden_size;
    if (obj.get("num_hidden_layers")) |v| config.num_hidden_layers = jsonU32(v) orelse config.num_hidden_layers;
    if (obj.get("num_attention_heads")) |v| config.num_attention_heads = jsonU32(v) orelse config.num_attention_heads;
    if (obj.get("intermediate_size")) |v| config.intermediate_size = jsonU32(v) orelse config.intermediate_size;
    if (obj.get("max_position_embeddings")) |v| config.max_position_embeddings = jsonU32(v) orelse config.max_position_embeddings;
    if (obj.get("type_vocab_size")) |v| config.type_vocab_size = jsonU32(v) orelse config.type_vocab_size;
    if (obj.get("max_2d_position_embeddings")) |v| config.max_2d_position_embeddings = jsonU32(v) orelse config.max_2d_position_embeddings;
    if (obj.get("coordinate_size")) |v| config.coordinate_size = jsonU32(v) orelse config.coordinate_size;
    if (obj.get("shape_size")) |v| config.shape_size = jsonU32(v) orelse config.shape_size;
    if (obj.get("input_size")) |v| config.input_size = jsonU32(v) orelse config.input_size;
    if (obj.get("patch_size")) |v| config.patch_size = jsonU32(v) orelse config.patch_size;
    if (obj.get("num_channels")) |v| {
        if (jsonU32(v)) |channels| config.num_channels = @intCast(channels);
    }
    if (obj.get("num_labels")) |v| config.num_labels = jsonU32(v) orelse config.num_labels;
    if (config.num_labels == 1) {
        if (inferNumLabels(obj)) |n| config.num_labels = n;
    }
    if (obj.get("pad_token_id")) |v| {
        if (v == .integer) config.pad_token_id = @intCast(v.integer);
    }
    if (obj.get("layer_norm_eps")) |v| {
        switch (v) {
            .float => |x| config.layer_norm_eps = @floatCast(x),
            .integer => |x| config.layer_norm_eps = @floatFromInt(x),
            else => {},
        }
    }
    if (obj.get("has_relative_attention_bias")) |v| {
        if (v == .bool) config.has_relative_attention_bias = v.bool;
    }
    if (obj.get("has_spatial_attention_bias")) |v| {
        if (v == .bool) config.has_spatial_attention_bias = v.bool;
    }
    if (obj.get("rel_pos_bins")) |v| config.rel_pos_bins = jsonU32(v) orelse config.rel_pos_bins;
    if (obj.get("max_rel_pos")) |v| config.max_rel_pos = jsonU32(v) orelse config.max_rel_pos;
    if (obj.get("rel_2d_pos_bins")) |v| config.rel_2d_pos_bins = jsonU32(v) orelse config.rel_2d_pos_bins;
    if (obj.get("max_rel_2d_pos")) |v| config.max_rel_2d_pos = jsonU32(v) orelse config.max_rel_2d_pos;
    return config;
}

pub fn parseGgufMetadata(view: gguf_metadata.View) ?Config {
    const arch = view.getString("general.architecture") orelse return null;
    if (!std.mem.eql(u8, arch, "layoutlmv3")) return null;

    var buf: [128]u8 = undefined;
    var config = Config{};
    config.weight_prefix = "";
    config.vocab_size = metaU32(view, &buf, arch, "vocab_size") orelse config.vocab_size;
    config.hidden_size = metaU32(view, &buf, arch, "embedding_length") orelse config.hidden_size;
    config.num_hidden_layers = metaU32(view, &buf, arch, "block_count") orelse config.num_hidden_layers;
    config.num_attention_heads = metaU32(view, &buf, arch, "attention.head_count") orelse config.num_attention_heads;
    config.intermediate_size = metaU32(view, &buf, arch, "feed_forward_length") orelse config.intermediate_size;
    config.max_position_embeddings = metaU32(view, &buf, arch, "context_length") orelse config.max_position_embeddings;
    config.type_vocab_size = metaU32(view, &buf, arch, "token_type_count") orelse config.type_vocab_size;
    config.max_2d_position_embeddings = metaU32(view, &buf, arch, "max_2d_position_embeddings") orelse config.max_2d_position_embeddings;
    config.coordinate_size = metaU32(view, &buf, arch, "coordinate_size") orelse config.coordinate_size;
    config.shape_size = metaU32(view, &buf, arch, "shape_size") orelse config.shape_size;
    config.input_size = metaU32(view, &buf, arch, "input_size") orelse config.input_size;
    config.patch_size = metaU32(view, &buf, arch, "patch_size") orelse config.patch_size;
    if (metaU32(view, &buf, arch, "num_channels")) |channels| config.num_channels = @intCast(channels);
    config.num_labels = metaU32(view, &buf, arch, "label_count") orelse config.num_labels;
    config.pad_token_id = metaI64(view, &buf, arch, "pad_token_id") orelse config.pad_token_id;
    config.layer_norm_eps = metaF32(view, &buf, arch, "layer_norm_epsilon") orelse config.layer_norm_eps;
    config.has_relative_attention_bias = metaBool(view, &buf, arch, "has_relative_attention_bias") orelse config.has_relative_attention_bias;
    config.has_spatial_attention_bias = metaBool(view, &buf, arch, "has_spatial_attention_bias") orelse config.has_spatial_attention_bias;
    config.rel_pos_bins = metaU32(view, &buf, arch, "rel_pos_bins") orelse config.rel_pos_bins;
    config.max_rel_pos = metaU32(view, &buf, arch, "max_rel_pos") orelse config.max_rel_pos;
    config.rel_2d_pos_bins = metaU32(view, &buf, arch, "rel_2d_pos_bins") orelse config.rel_2d_pos_bins;
    config.max_rel_2d_pos = metaU32(view, &buf, arch, "max_rel_2d_pos") orelse config.max_rel_2d_pos;
    config.visual_bbox_max_len = metaI64(view, &buf, arch, "visual_bbox_max_len") orelse config.visual_bbox_max_len;
    return config;
}

fn jsonU32(val: std.json.Value) ?u32 {
    return switch (val) {
        .integer => |i| @intCast(i),
        else => null,
    };
}

fn inferNumLabels(obj: std.json.ObjectMap) ?u32 {
    if (obj.get("id2label")) |v| {
        if (v == .object and v.object.count() > 0) return @intCast(v.object.count());
    }
    if (obj.get("label2id")) |v| {
        if (v == .object and v.object.count() > 0) return @intCast(v.object.count());
    }
    return null;
}

fn metaU32(view: gguf_metadata.View, buf: *[128]u8, arch: []const u8, suffix: []const u8) ?u32 {
    const key = std.fmt.bufPrint(buf, "{s}.{s}", .{ arch, suffix }) catch return null;
    return std.math.cast(u32, view.getU64(key) orelse return null);
}

fn metaI64(view: gguf_metadata.View, buf: *[128]u8, arch: []const u8, suffix: []const u8) ?i64 {
    const key = std.fmt.bufPrint(buf, "{s}.{s}", .{ arch, suffix }) catch return null;
    return view.getI64(key);
}

fn metaF32(view: gguf_metadata.View, buf: *[128]u8, arch: []const u8, suffix: []const u8) ?f32 {
    const key = std.fmt.bufPrint(buf, "{s}.{s}", .{ arch, suffix }) catch return null;
    return view.getF32(key);
}

fn metaBool(view: gguf_metadata.View, buf: *[128]u8, arch: []const u8, suffix: []const u8) ?bool {
    const key = std.fmt.bufPrint(buf, "{s}.{s}", .{ arch, suffix }) catch return null;
    return view.getBool(key);
}

test "parse gguf metadata for layoutlmv3 config" {
    const allocator = std.testing.allocator;
    const format = @import("../gguf/format.zig");

    const metadata = [_]format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "layoutlmv3" } },
        .{ .key = "layoutlmv3.vocab_size", .value = .{ .u32 = 30522 } },
        .{ .key = "layoutlmv3.embedding_length", .value = .{ .u32 = 384 } },
        .{ .key = "layoutlmv3.block_count", .value = .{ .u32 = 6 } },
        .{ .key = "layoutlmv3.attention.head_count", .value = .{ .u32 = 6 } },
        .{ .key = "layoutlmv3.feed_forward_length", .value = .{ .u32 = 1536 } },
        .{ .key = "layoutlmv3.context_length", .value = .{ .u32 = 512 } },
        .{ .key = "layoutlmv3.token_type_count", .value = .{ .u32 = 2 } },
        .{ .key = "layoutlmv3.max_2d_position_embeddings", .value = .{ .u32 = 1024 } },
        .{ .key = "layoutlmv3.coordinate_size", .value = .{ .u32 = 128 } },
        .{ .key = "layoutlmv3.shape_size", .value = .{ .u32 = 128 } },
        .{ .key = "layoutlmv3.input_size", .value = .{ .u32 = 224 } },
        .{ .key = "layoutlmv3.patch_size", .value = .{ .u32 = 16 } },
        .{ .key = "layoutlmv3.num_channels", .value = .{ .u32 = 3 } },
        .{ .key = "layoutlmv3.label_count", .value = .{ .u32 = 7 } },
        .{ .key = "layoutlmv3.pad_token_id", .value = .{ .i64 = 1 } },
        .{ .key = "layoutlmv3.layer_norm_epsilon", .value = .{ .f32 = 1e-5 } },
        .{ .key = "layoutlmv3.has_relative_attention_bias", .value = .{ .bool_ = true } },
        .{ .key = "layoutlmv3.has_spatial_attention_bias", .value = .{ .bool_ = true } },
        .{ .key = "layoutlmv3.rel_pos_bins", .value = .{ .u32 = 32 } },
        .{ .key = "layoutlmv3.max_rel_pos", .value = .{ .u32 = 128 } },
        .{ .key = "layoutlmv3.rel_2d_pos_bins", .value = .{ .u32 = 64 } },
        .{ .key = "layoutlmv3.max_rel_2d_pos", .value = .{ .u32 = 256 } },
        .{ .key = "layoutlmv3.visual_bbox_max_len", .value = .{ .i64 = 1000 } },
    };
    const writer = @import("../gguf/writer.zig");
    var layout = try writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);

    var parsed = try format.parse(allocator, layout.header_bytes);
    defer parsed.deinit(allocator);
    const view = gguf_metadata.View.init(&parsed);
    const cfg = parseGgufMetadata(view).?;
    try std.testing.expectEqualStrings("", cfg.weight_prefix);
    try std.testing.expectEqual(@as(u32, 30522), cfg.vocab_size);
    try std.testing.expectEqual(@as(u32, 384), cfg.hidden_size);
    try std.testing.expectEqual(@as(u32, 6), cfg.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 7), cfg.num_labels);
    try std.testing.expectEqual(@as(i64, 1000), cfg.visual_bbox_max_len);
    try std.testing.expect(cfg.has_relative_attention_bias);
    try std.testing.expect(cfg.has_spatial_attention_bias);
}
