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

// T5 model configuration for encoder-decoder inference.
//
// Supports t5, mt5, and longt5 model types.
// T5 differs from BERT: RMSNorm (no bias), relative position bias,
// no bias in linear layers, and separate encoder/decoder stacks.

const std = @import("std");
const gguf_metadata = @import("../gguf/metadata.zig");
const gguf_format = @import("../gguf/format.zig");

pub const ModelType = enum {
    t5,
    mt5,
    longt5,
};

pub const Config = struct {
    model_type: ModelType = .t5,
    d_model: u32 = 512,
    d_kv: u32 = 64,
    d_ff: u32 = 2048,
    num_heads: u32 = 8,
    num_layers: u32 = 6,
    num_decoder_layers: u32 = 0, // 0 means same as num_layers
    relative_attention_num_buckets: u32 = 32,
    relative_attention_max_distance: u32 = 128,
    vocab_size: u32 = 32128,
    decoder_start_token_id: i32 = 0,
    eos_token_id: i32 = 1,
    pad_token_id: i32 = 0,
    is_gated_act: bool = false, // T5v1.1 uses gated FFN with SiLU

    pub fn effectiveDecoderLayers(self: Config) u32 {
        return if (self.num_decoder_layers > 0) self.num_decoder_layers else self.num_layers;
    }

    pub fn innerDim(self: Config) u32 {
        return self.num_heads * self.d_kv;
    }
};

pub fn parseConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    var config = Config{};

    if (obj.get("model_type")) |v| {
        if (v == .string) {
            const s = v.string;
            if (std.mem.eql(u8, s, "mt5")) {
                config.model_type = .mt5;
            } else if (std.mem.eql(u8, s, "longt5")) {
                config.model_type = .longt5;
            }
        }
    }

    if (obj.get("d_model")) |v| if (jsonU32(v)) |val| {
        config.d_model = val;
    };
    if (obj.get("d_kv")) |v| if (jsonU32(v)) |val| {
        config.d_kv = val;
    };
    if (obj.get("d_ff")) |v| if (jsonU32(v)) |val| {
        config.d_ff = val;
    };
    if (obj.get("num_heads")) |v| if (jsonU32(v)) |val| {
        config.num_heads = val;
    };
    if (obj.get("num_layers")) |v| if (jsonU32(v)) |val| {
        config.num_layers = val;
    };
    if (obj.get("num_decoder_layers")) |v| if (jsonU32(v)) |val| {
        config.num_decoder_layers = val;
    };
    if (obj.get("relative_attention_num_buckets")) |v| if (jsonU32(v)) |val| {
        config.relative_attention_num_buckets = val;
    };
    if (obj.get("relative_attention_max_distance")) |v| if (jsonU32(v)) |val| {
        config.relative_attention_max_distance = val;
    };
    if (obj.get("vocab_size")) |v| if (jsonU32(v)) |val| {
        config.vocab_size = val;
    };
    if (obj.get("decoder_start_token_id")) |v| {
        if (jsonI32(v)) |val| config.decoder_start_token_id = val;
    }
    if (obj.get("eos_token_id")) |v| {
        if (jsonI32(v)) |val| config.eos_token_id = val;
    }
    if (obj.get("pad_token_id")) |v| {
        if (jsonI32(v)) |val| config.pad_token_id = val;
    }

    // T5v1.1 detection: "feed_forward_proj" contains "gated"
    if (obj.get("feed_forward_proj")) |v| {
        if (v == .string) {
            if (std.mem.indexOf(u8, v.string, "gated") != null) {
                config.is_gated_act = true;
            }
        }
    }
    if (obj.get("is_gated_act")) |v| {
        if (v == .bool) config.is_gated_act = v.bool;
    }

    return config;
}

/// Detect if a model_type string is a T5-family model.
pub fn isT5Model(model_type: []const u8) bool {
    return std.mem.eql(u8, model_type, "t5") or
        std.mem.eql(u8, model_type, "mt5") or
        std.mem.eql(u8, model_type, "longt5");
}

pub fn parseGgufMetadata(view: gguf_metadata.View) ?Config {
    const arch = view.getString("general.architecture") orelse return null;
    if (!std.mem.eql(u8, arch, "t5")) return null;

    var config = Config{};
    if (view.getString("t5.family")) |family| {
        if (std.mem.eql(u8, family, "mt5")) {
            config.model_type = .mt5;
        } else if (std.mem.eql(u8, family, "longt5")) {
            config.model_type = .longt5;
        }
    }
    config.d_model = metaU32(view, "t5.embedding_length") orelse config.d_model;
    config.d_kv = metaU32(view, "t5.attention.key_value_length") orelse config.d_kv;
    config.d_ff = metaU32(view, "t5.feed_forward_length") orelse config.d_ff;
    config.num_heads = metaU32(view, "t5.attention.head_count") orelse config.num_heads;
    config.num_layers = metaU32(view, "t5.encoder.block_count") orelse config.num_layers;
    config.num_decoder_layers = metaU32(view, "t5.decoder.block_count") orelse config.num_decoder_layers;
    config.relative_attention_num_buckets = metaU32(view, "t5.attention.relative_buckets") orelse config.relative_attention_num_buckets;
    config.relative_attention_max_distance = metaU32(view, "t5.attention.relative_max_distance") orelse config.relative_attention_max_distance;
    config.vocab_size = metaU32(view, "t5.vocab_size") orelse config.vocab_size;
    config.decoder_start_token_id = metaI32(view, "t5.decoder_start_token_id") orelse config.decoder_start_token_id;
    config.eos_token_id = metaI32(view, "t5.eos_token_id") orelse config.eos_token_id;
    config.pad_token_id = metaI32(view, "t5.pad_token_id") orelse config.pad_token_id;
    config.is_gated_act = view.getBool("t5.is_gated_act") orelse config.is_gated_act;
    return config;
}

fn jsonU32(val: std.json.Value) ?u32 {
    return switch (val) {
        .integer => |i| @intCast(i),
        else => null,
    };
}

fn jsonI32(val: std.json.Value) ?i32 {
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

// -- Tests --

test "parse t5 config" {
    const allocator = std.testing.allocator;
    const json =
        \\{"model_type": "t5", "d_model": 512, "d_kv": 64, "d_ff": 2048, "num_heads": 8, "num_layers": 6, "vocab_size": 32128}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelType.t5, config.model_type);
    try std.testing.expectEqual(@as(u32, 512), config.d_model);
    try std.testing.expectEqual(@as(u32, 64), config.d_kv);
    try std.testing.expectEqual(@as(u32, 8), config.num_heads);
    try std.testing.expectEqual(@as(u32, 512), config.innerDim());
}

test "parse mt5 config" {
    const allocator = std.testing.allocator;
    const json =
        \\{"model_type": "mt5", "d_model": 768, "num_layers": 12, "num_decoder_layers": 12}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelType.mt5, config.model_type);
    try std.testing.expectEqual(@as(u32, 768), config.d_model);
    try std.testing.expectEqual(@as(u32, 12), config.effectiveDecoderLayers());
}

test "parse t5v1.1 gated config" {
    const allocator = std.testing.allocator;
    const json =
        \\{"model_type": "t5", "feed_forward_proj": "gated-gelu", "d_model": 512}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expect(config.is_gated_act);
}

test "isT5Model" {
    try std.testing.expect(isT5Model("t5"));
    try std.testing.expect(isT5Model("mt5"));
    try std.testing.expect(isT5Model("longt5"));
    try std.testing.expect(!isT5Model("bert"));
}

test "parse t5 gguf metadata" {
    const allocator = std.testing.allocator;
    const writer = @import("../gguf/writer.zig");
    const metadata = [_]gguf_format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "t5" } },
        .{ .key = "t5.family", .value = .{ .string = "mt5" } },
        .{ .key = "t5.embedding_length", .value = .{ .u32 = 768 } },
        .{ .key = "t5.attention.key_value_length", .value = .{ .u32 = 64 } },
        .{ .key = "t5.feed_forward_length", .value = .{ .u32 = 2048 } },
        .{ .key = "t5.attention.head_count", .value = .{ .u32 = 12 } },
        .{ .key = "t5.encoder.block_count", .value = .{ .u32 = 8 } },
        .{ .key = "t5.decoder.block_count", .value = .{ .u32 = 10 } },
        .{ .key = "t5.attention.relative_buckets", .value = .{ .u32 = 32 } },
        .{ .key = "t5.attention.relative_max_distance", .value = .{ .u32 = 128 } },
        .{ .key = "t5.vocab_size", .value = .{ .u32 = 250112 } },
        .{ .key = "t5.decoder_start_token_id", .value = .{ .i64 = 0 } },
        .{ .key = "t5.eos_token_id", .value = .{ .i64 = 1 } },
        .{ .key = "t5.pad_token_id", .value = .{ .i64 = 0 } },
        .{ .key = "t5.is_gated_act", .value = .{ .bool_ = true } },
    };
    var layout = try writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    var parsed = try gguf_format.parse(allocator, layout.header_bytes);
    defer parsed.deinit(allocator);
    const view = gguf_metadata.View.init(&parsed);
    const config = parseGgufMetadata(view).?;
    try std.testing.expectEqual(ModelType.mt5, config.model_type);
    try std.testing.expectEqual(@as(u32, 768), config.d_model);
    try std.testing.expectEqual(@as(u32, 10), config.effectiveDecoderLayers());
    try std.testing.expect(config.is_gated_act);
}
