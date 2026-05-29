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
const bert_mod = @import("bert.zig");

pub const ProjectionActivation = enum {
    relu,
    gelu,
};

pub const Config = struct {
    pub const audio_stage_count = 4;

    pub const AudioConfig = struct {
        hidden_size: u32 = 768,
        patch_embeds_hidden_size: u32 = 96,
        patch_embed_input_channels: u32 = 1,
        patch_size: u32 = 4,
        patch_stride: [2]u32 = .{ 4, 4 },
        num_mel_bins: u32 = 64,
        spec_size: u32 = 256,
        window_size: u32 = 8,
        depths: [audio_stage_count]u32 = .{ 2, 2, 6, 2 },
        num_attention_heads: [audio_stage_count]u32 = .{ 4, 8, 16, 32 },
        mlp_ratio: f32 = 4.0,
        layer_norm_eps: f32 = 1e-5,
        hidden_act: ProjectionActivation = .gelu,
        qkv_bias: bool = true,
        enable_fusion: bool = false,
        enable_patch_fusion: bool = false,
        enable_patch_layer_norm: bool = true,

        pub fn stageDim(self: AudioConfig, stage: usize) u32 {
            return self.patch_embeds_hidden_size << @as(u5, @intCast(stage));
        }
    };

    text_config: bert_mod.Config = .{
        .model_type = .roberta,
        .hidden_size = 768,
        .num_hidden_layers = 12,
        .num_attention_heads = 12,
        .intermediate_size = 3072,
        .max_position_embeddings = 514,
        .type_vocab_size = 1,
        .weight_prefix = "text_model",
    },
    text_pad_token_id: i32 = 1,
    audio_config: AudioConfig = .{},
    projection_dim: u32 = 512,
    projection_hidden_act: ProjectionActivation = .relu,
    logit_scale_init_value: f32 = 14.285714,
    enable_fusion: bool = false,
};

pub fn parseConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    var config = Config{};

    if (obj.get("projection_dim")) |v| {
        if (jsonU32(v)) |val| config.projection_dim = val;
    }
    if (obj.get("logit_scale_init_value")) |v| {
        if (jsonF32(v)) |val| config.logit_scale_init_value = val;
    }
    if (obj.get("projection_hidden_act")) |v| if (v == .string) {
        config.projection_hidden_act = if (std.ascii.eqlIgnoreCase(v.string, "gelu")) .gelu else .relu;
    };
    var saw_audio_hidden_size = false;
    var saw_patch_embeds_hidden_size = false;
    if (obj.get("audio_config")) |v| {
        if (v == .object) {
            if (v.object.get("hidden_size")) |field| if (jsonU32(field)) |val| {
                config.audio_config.hidden_size = val;
                saw_audio_hidden_size = true;
            };
            if (v.object.get("patch_embeds_hidden_size")) |field| if (jsonU32(field)) |val| {
                config.audio_config.patch_embeds_hidden_size = val;
                saw_patch_embeds_hidden_size = true;
            };
            if (v.object.get("patch_embed_input_channels")) |field| if (jsonU32(field)) |val| {
                config.audio_config.patch_embed_input_channels = val;
            };
            if (v.object.get("patch_size")) |field| if (jsonU32(field)) |val| {
                config.audio_config.patch_size = val;
            };
            if (v.object.get("patch_stride")) |field| parseU32Array(&config.audio_config.patch_stride, field);
            if (v.object.get("num_mel_bins")) |field| if (jsonU32(field)) |val| {
                config.audio_config.num_mel_bins = val;
            };
            if (v.object.get("spec_size")) |field| if (jsonU32(field)) |val| {
                config.audio_config.spec_size = val;
            };
            if (v.object.get("window_size")) |field| if (jsonU32(field)) |val| {
                config.audio_config.window_size = val;
            };
            if (v.object.get("depths")) |field| parseU32Array(&config.audio_config.depths, field);
            if (v.object.get("num_attention_heads")) |field| parseU32Array(&config.audio_config.num_attention_heads, field);
            if (v.object.get("mlp_ratio")) |field| if (jsonF32(field)) |val| {
                config.audio_config.mlp_ratio = val;
            };
            if (v.object.get("layer_norm_eps")) |field| if (jsonF32(field)) |val| {
                config.audio_config.layer_norm_eps = val;
            };
            if (v.object.get("hidden_act")) |field| if (field == .string) {
                config.audio_config.hidden_act = if (std.ascii.eqlIgnoreCase(field.string, "relu")) .relu else .gelu;
            };
            if (v.object.get("qkv_bias")) |field| if (jsonBool(field)) |val| {
                config.audio_config.qkv_bias = val;
            };
            if (v.object.get("enable_fusion")) |fusion| {
                if (jsonBool(fusion)) |enabled| {
                    config.audio_config.enable_fusion = enabled;
                    config.enable_fusion = enabled;
                }
            }
            if (v.object.get("enable_patch_fusion")) |field| {
                if (jsonBool(field)) |enabled| config.audio_config.enable_patch_fusion = enabled;
            }
            if (v.object.get("enable_patch_layer_norm")) |field| {
                if (jsonBool(field)) |enabled| config.audio_config.enable_patch_layer_norm = enabled;
            }
        }
    }
    if (saw_audio_hidden_size and !saw_patch_embeds_hidden_size and config.audio_config.hidden_size % 8 == 0) {
        config.audio_config.patch_embeds_hidden_size = config.audio_config.hidden_size / 8;
    }

    const text_obj = if (obj.get("text_config")) |tc| switch (tc) {
        .object => |o| o,
        else => obj,
    } else obj;

    config.text_config.model_type = .roberta;
    config.text_config.weight_prefix = "text_model";
    if (text_obj.get("vocab_size")) |v| config.text_config.vocab_size = jsonU32(v) orelse config.text_config.vocab_size;
    if (text_obj.get("hidden_size")) |v| config.text_config.hidden_size = jsonU32(v) orelse config.text_config.hidden_size;
    if (text_obj.get("num_hidden_layers")) |v| config.text_config.num_hidden_layers = jsonU32(v) orelse config.text_config.num_hidden_layers;
    if (text_obj.get("num_attention_heads")) |v| config.text_config.num_attention_heads = jsonU32(v) orelse config.text_config.num_attention_heads;
    if (text_obj.get("intermediate_size")) |v| config.text_config.intermediate_size = jsonU32(v) orelse config.text_config.intermediate_size;
    if (text_obj.get("max_position_embeddings")) |v| config.text_config.max_position_embeddings = jsonU32(v) orelse config.text_config.max_position_embeddings;
    if (text_obj.get("type_vocab_size")) |v| config.text_config.type_vocab_size = jsonU32(v) orelse config.text_config.type_vocab_size;
    if (text_obj.get("pad_token_id")) |v| config.text_pad_token_id = jsonI32(v) orelse config.text_pad_token_id;

    return config;
}

pub fn isClapModel(model_type: []const u8) bool {
    return std.mem.eql(u8, model_type, "clap");
}

pub fn parseGgufMetadata(view: gguf_metadata.View) ?Config {
    const arch = view.getString("general.architecture") orelse return null;
    if (!std.mem.eql(u8, arch, "clap")) return null;

    var config = Config{};
    config.projection_dim = metaU32(view, "clap.projection_dim") orelse config.projection_dim;
    config.logit_scale_init_value = view.getF32("clap.logit_scale_init_value") orelse config.logit_scale_init_value;
    if (view.getString("clap.projection_hidden_act")) |value| {
        config.projection_hidden_act = if (std.ascii.eqlIgnoreCase(value, "gelu")) .gelu else .relu;
    }

    config.text_config.model_type = .roberta;
    config.text_config.weight_prefix = "text_model";
    config.text_config.vocab_size = metaU32(view, "clap.text.vocab_size") orelse config.text_config.vocab_size;
    config.text_config.hidden_size = metaU32(view, "clap.text.embedding_length") orelse config.text_config.hidden_size;
    config.text_config.num_hidden_layers = metaU32(view, "clap.text.block_count") orelse config.text_config.num_hidden_layers;
    config.text_config.num_attention_heads = metaU32(view, "clap.text.attention.head_count") orelse config.text_config.num_attention_heads;
    config.text_config.intermediate_size = metaU32(view, "clap.text.feed_forward_length") orelse config.text_config.intermediate_size;
    config.text_config.max_position_embeddings = metaU32(view, "clap.text.context_length") orelse config.text_config.max_position_embeddings;
    config.text_config.type_vocab_size = metaU32(view, "clap.text.token_type_count") orelse config.text_config.type_vocab_size;
    config.text_pad_token_id = metaI32(view, "clap.text.pad_token_id") orelse config.text_pad_token_id;

    config.audio_config.hidden_size = metaU32(view, "clap.audio.embedding_length") orelse config.audio_config.hidden_size;
    config.audio_config.patch_embeds_hidden_size = metaU32(view, "clap.audio.patch_embeds_hidden_size") orelse config.audio_config.patch_embeds_hidden_size;
    config.audio_config.patch_embed_input_channels = metaU32(view, "clap.audio.patch_embed_input_channels") orelse config.audio_config.patch_embed_input_channels;
    config.audio_config.patch_size = metaU32(view, "clap.audio.patch_size") orelse config.audio_config.patch_size;
    parseFixedU32Array(&config.audio_config.patch_stride, view.find("clap.audio.patch_stride"));
    config.audio_config.num_mel_bins = metaU32(view, "clap.audio.num_mel_bins") orelse config.audio_config.num_mel_bins;
    config.audio_config.spec_size = metaU32(view, "clap.audio.spec_size") orelse config.audio_config.spec_size;
    config.audio_config.window_size = metaU32(view, "clap.audio.window_size") orelse config.audio_config.window_size;
    parseFixedU32Array(&config.audio_config.depths, view.find("clap.audio.depths"));
    parseFixedU32Array(&config.audio_config.num_attention_heads, view.find("clap.audio.attention_head_counts"));
    config.audio_config.mlp_ratio = view.getF32("clap.audio.mlp_ratio") orelse config.audio_config.mlp_ratio;
    config.audio_config.layer_norm_eps = view.getF32("clap.audio.layer_norm_epsilon") orelse config.audio_config.layer_norm_eps;
    if (view.getString("clap.audio.hidden_act")) |value| {
        config.audio_config.hidden_act = if (std.ascii.eqlIgnoreCase(value, "relu")) .relu else .gelu;
    }
    config.audio_config.qkv_bias = view.getBool("clap.audio.qkv_bias") orelse config.audio_config.qkv_bias;
    config.audio_config.enable_fusion = view.getBool("clap.audio.enable_fusion") orelse config.audio_config.enable_fusion;
    config.audio_config.enable_patch_fusion = view.getBool("clap.audio.enable_patch_fusion") orelse config.audio_config.enable_patch_fusion;
    config.audio_config.enable_patch_layer_norm = view.getBool("clap.audio.enable_patch_layer_norm") orelse config.audio_config.enable_patch_layer_norm;
    config.enable_fusion = view.getBool("clap.enable_fusion") orelse config.audio_config.enable_fusion;

    return config;
}

fn metaU32(view: gguf_metadata.View, key: []const u8) ?u32 {
    return std.math.cast(u32, view.getU64(key) orelse return null);
}

fn metaI32(view: gguf_metadata.View, key: []const u8) ?i32 {
    return std.math.cast(i32, view.getI64(key) orelse return null);
}

fn parseFixedU32Array(target: anytype, entry: ?*const @import("../gguf/format.zig").MetadataEntry) void {
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

fn jsonU32(val: std.json.Value) ?u32 {
    return switch (val) {
        .integer => |i| @intCast(i),
        else => null,
    };
}

fn jsonF32(val: std.json.Value) ?f32 {
    return switch (val) {
        .float => |f| @floatCast(f),
        .integer => |i| @floatFromInt(i),
        else => null,
    };
}

fn jsonBool(val: std.json.Value) ?bool {
    return switch (val) {
        .bool => |b| b,
        else => null,
    };
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

test "parse clap config" {
    const json =
        \\{"model_type":"clap","projection_dim":512,"projection_hidden_act":"relu","logit_scale_init_value":14.285714,"text_config":{"vocab_size":50265,"hidden_size":768,"num_hidden_layers":12,"num_attention_heads":12,"intermediate_size":3072,"max_position_embeddings":514,"type_vocab_size":1,"pad_token_id":1},"audio_config":{"hidden_size":768,"patch_embeds_hidden_size":96,"patch_size":4,"patch_stride":[4,4],"num_mel_bins":64,"spec_size":256,"window_size":8,"depths":[2,2,6,2],"num_attention_heads":[4,8,16,32],"mlp_ratio":4.0,"layer_norm_eps":1e-5,"enable_fusion":false}}
    ;
    const cfg = try parseConfig(std.testing.allocator, json);
    try std.testing.expectEqual(@as(u32, 512), cfg.projection_dim);
    try std.testing.expectEqual(@as(u32, 514), cfg.text_config.max_position_embeddings);
    try std.testing.expectEqual(bert_mod.ModelType.roberta, cfg.text_config.model_type);
    try std.testing.expectEqualStrings("text_model", cfg.text_config.weight_prefix.?);
    try std.testing.expectEqual(@as(i32, 1), cfg.text_pad_token_id);
    try std.testing.expectEqual(false, cfg.enable_fusion);
    try std.testing.expectEqual(@as(u32, 64), cfg.audio_config.num_mel_bins);
    try std.testing.expectEqual(@as(u32, 6), cfg.audio_config.depths[2]);
    try std.testing.expectEqual(@as(u32, 768), cfg.audio_config.stageDim(3));
}

test "parse clap gguf metadata" {
    const allocator = std.testing.allocator;
    const format = @import("../gguf/format.zig");
    const writer = @import("../gguf/writer.zig");

    var patch_stride = [_]format.MetadataValue{ .{ .u32 = 4 }, .{ .u32 = 4 } };
    var depths = [_]format.MetadataValue{ .{ .u32 = 2 }, .{ .u32 = 2 }, .{ .u32 = 6 }, .{ .u32 = 2 } };
    var heads = [_]format.MetadataValue{ .{ .u32 = 4 }, .{ .u32 = 8 }, .{ .u32 = 16 }, .{ .u32 = 32 } };
    const metadata = [_]format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clap" } },
        .{ .key = "clap.projection_dim", .value = .{ .u32 = 512 } },
        .{ .key = "clap.projection_hidden_act", .value = .{ .string = "relu" } },
        .{ .key = "clap.logit_scale_init_value", .value = .{ .f32 = 14.285714 } },
        .{ .key = "clap.text.vocab_size", .value = .{ .u32 = 50265 } },
        .{ .key = "clap.text.embedding_length", .value = .{ .u32 = 768 } },
        .{ .key = "clap.text.block_count", .value = .{ .u32 = 12 } },
        .{ .key = "clap.text.attention.head_count", .value = .{ .u32 = 12 } },
        .{ .key = "clap.text.feed_forward_length", .value = .{ .u32 = 3072 } },
        .{ .key = "clap.text.context_length", .value = .{ .u32 = 514 } },
        .{ .key = "clap.text.token_type_count", .value = .{ .u32 = 1 } },
        .{ .key = "clap.text.pad_token_id", .value = .{ .i64 = 1 } },
        .{ .key = "clap.audio.embedding_length", .value = .{ .u32 = 768 } },
        .{ .key = "clap.audio.patch_embeds_hidden_size", .value = .{ .u32 = 96 } },
        .{ .key = "clap.audio.patch_embed_input_channels", .value = .{ .u32 = 1 } },
        .{ .key = "clap.audio.patch_size", .value = .{ .u32 = 4 } },
        .{ .key = "clap.audio.patch_stride", .value = .{ .array = .{ .element_type = .u32, .values = &patch_stride } } },
        .{ .key = "clap.audio.num_mel_bins", .value = .{ .u32 = 64 } },
        .{ .key = "clap.audio.spec_size", .value = .{ .u32 = 256 } },
        .{ .key = "clap.audio.window_size", .value = .{ .u32 = 8 } },
        .{ .key = "clap.audio.depths", .value = .{ .array = .{ .element_type = .u32, .values = &depths } } },
        .{ .key = "clap.audio.attention_head_counts", .value = .{ .array = .{ .element_type = .u32, .values = &heads } } },
        .{ .key = "clap.audio.mlp_ratio", .value = .{ .f32 = 4.0 } },
        .{ .key = "clap.audio.layer_norm_epsilon", .value = .{ .f32 = 1e-5 } },
        .{ .key = "clap.audio.hidden_act", .value = .{ .string = "gelu" } },
        .{ .key = "clap.audio.qkv_bias", .value = .{ .bool_ = true } },
        .{ .key = "clap.audio.enable_fusion", .value = .{ .bool_ = false } },
        .{ .key = "clap.audio.enable_patch_fusion", .value = .{ .bool_ = false } },
        .{ .key = "clap.audio.enable_patch_layer_norm", .value = .{ .bool_ = true } },
        .{ .key = "clap.enable_fusion", .value = .{ .bool_ = false } },
    };
    var layout = try writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    var parsed = try format.parse(allocator, layout.header_bytes);
    defer parsed.deinit(allocator);
    const view = gguf_metadata.View.init(&parsed);
    const cfg = parseGgufMetadata(view).?;
    try std.testing.expectEqual(@as(u32, 512), cfg.projection_dim);
    try std.testing.expectEqual(@as(u32, 768), cfg.text_config.hidden_size);
    try std.testing.expectEqual(@as(u32, 96), cfg.audio_config.patch_embeds_hidden_size);
    try std.testing.expectEqual(@as(u32, 6), cfg.audio_config.depths[2]);
    try std.testing.expectEqual(@as(u32, 32), cfg.audio_config.num_attention_heads[3]);
}
