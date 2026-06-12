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

// Whisper speech-to-text model configuration.
//
// Encoder-decoder architecture with convolutional audio frontend.
// Encoder processes mel spectrograms, decoder generates text tokens.

const std = @import("std");
const gguf_metadata = @import("../gguf/metadata.zig");
const gguf_format = @import("../gguf/format.zig");

pub const Config = struct {
    d_model: u32 = 384,
    encoder_layers: u32 = 4,
    decoder_layers: u32 = 4,
    encoder_attention_heads: u32 = 6,
    decoder_attention_heads: u32 = 6,
    encoder_ffn_dim: u32 = 1536,
    decoder_ffn_dim: u32 = 1536,
    num_mel_bins: u32 = 80,
    vocab_size: u32 = 51865,
    max_source_positions: u32 = 1500,
    max_target_positions: u32 = 448,
    scale_embedding: bool = false,

    // Token IDs
    bos_token_id: i32 = 50257,
    eos_token_id: i32 = 50257,
    pad_token_id: i32 = 50257,
    decoder_start_token_id: i32 = 50258,

    pub fn encoderHeadDim(self: Config) u32 {
        return self.d_model / self.encoder_attention_heads;
    }

    pub fn decoderHeadDim(self: Config) u32 {
        return self.d_model / self.decoder_attention_heads;
    }
};

pub fn parseConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    var config = Config{};

    if (obj.get("d_model")) |v| if (jsonU32(v)) |val| {
        config.d_model = val;
    };
    if (obj.get("encoder_layers")) |v| if (jsonU32(v)) |val| {
        config.encoder_layers = val;
    };
    if (obj.get("decoder_layers")) |v| if (jsonU32(v)) |val| {
        config.decoder_layers = val;
    };
    if (obj.get("encoder_attention_heads")) |v| if (jsonU32(v)) |val| {
        config.encoder_attention_heads = val;
    };
    if (obj.get("decoder_attention_heads")) |v| if (jsonU32(v)) |val| {
        config.decoder_attention_heads = val;
    };
    if (obj.get("encoder_ffn_dim")) |v| if (jsonU32(v)) |val| {
        config.encoder_ffn_dim = val;
    };
    if (obj.get("decoder_ffn_dim")) |v| if (jsonU32(v)) |val| {
        config.decoder_ffn_dim = val;
    };
    if (obj.get("num_mel_bins")) |v| if (jsonU32(v)) |val| {
        config.num_mel_bins = val;
    };
    if (obj.get("vocab_size")) |v| if (jsonU32(v)) |val| {
        config.vocab_size = val;
    };
    if (obj.get("max_source_positions")) |v| if (jsonU32(v)) |val| {
        config.max_source_positions = val;
    };
    if (obj.get("max_target_positions")) |v| if (jsonU32(v)) |val| {
        config.max_target_positions = val;
    };
    if (obj.get("scale_embedding")) |v| {
        if (v == .bool) config.scale_embedding = v.bool;
    }
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

pub fn isWhisperModel(model_type: []const u8) bool {
    return std.mem.eql(u8, model_type, "whisper");
}

pub fn parseGgufMetadata(view: gguf_metadata.View) ?Config {
    const arch = view.getString("general.architecture") orelse return null;
    if (!std.mem.eql(u8, arch, "whisper")) return null;

    var config = Config{};
    config.d_model = metaU32(view, "whisper.embedding_length") orelse config.d_model;
    config.encoder_layers = metaU32(view, "whisper.encoder.block_count") orelse config.encoder_layers;
    config.decoder_layers = metaU32(view, "whisper.decoder.block_count") orelse config.decoder_layers;
    config.encoder_attention_heads = metaU32(view, "whisper.encoder.attention.head_count") orelse config.encoder_attention_heads;
    config.decoder_attention_heads = metaU32(view, "whisper.decoder.attention.head_count") orelse config.decoder_attention_heads;
    config.encoder_ffn_dim = metaU32(view, "whisper.encoder.feed_forward_length") orelse config.encoder_ffn_dim;
    config.decoder_ffn_dim = metaU32(view, "whisper.decoder.feed_forward_length") orelse config.decoder_ffn_dim;
    config.num_mel_bins = metaU32(view, "whisper.num_mel_bins") orelse config.num_mel_bins;
    config.vocab_size = metaU32(view, "whisper.vocab_size") orelse config.vocab_size;
    config.max_source_positions = metaU32(view, "whisper.encoder.context_length") orelse config.max_source_positions;
    config.max_target_positions = metaU32(view, "whisper.decoder.context_length") orelse config.max_target_positions;
    config.scale_embedding = view.getBool("whisper.scale_embedding") orelse config.scale_embedding;
    config.bos_token_id = metaI32(view, "whisper.bos_token_id") orelse config.bos_token_id;
    config.eos_token_id = metaI32(view, "whisper.eos_token_id") orelse config.eos_token_id;
    config.pad_token_id = metaI32(view, "whisper.pad_token_id") orelse config.pad_token_id;
    config.decoder_start_token_id = metaI32(view, "whisper.decoder_start_token_id") orelse config.decoder_start_token_id;
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

test "parse whisper tiny config" {
    const allocator = std.testing.allocator;
    const json =
        \\{"model_type": "whisper", "d_model": 384, "encoder_layers": 4, "decoder_layers": 4, "encoder_attention_heads": 6, "decoder_attention_heads": 6, "num_mel_bins": 80, "vocab_size": 51865}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(@as(u32, 384), config.d_model);
    try std.testing.expectEqual(@as(u32, 4), config.encoder_layers);
    try std.testing.expectEqual(@as(u32, 80), config.num_mel_bins);
    try std.testing.expectEqual(@as(u32, 64), config.encoderHeadDim());
}

test "parse whisper large-v3 config" {
    const allocator = std.testing.allocator;
    const json =
        \\{"model_type": "whisper", "d_model": 1280, "encoder_layers": 32, "decoder_layers": 32, "encoder_attention_heads": 20, "decoder_attention_heads": 20, "num_mel_bins": 128, "vocab_size": 51866}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(@as(u32, 1280), config.d_model);
    try std.testing.expectEqual(@as(u32, 128), config.num_mel_bins);
    try std.testing.expectEqual(@as(u32, 64), config.encoderHeadDim());
}

test "isWhisperModel" {
    try std.testing.expect(isWhisperModel("whisper"));
    try std.testing.expect(!isWhisperModel("bert"));
    try std.testing.expect(!isWhisperModel("t5"));
}

test "parse whisper gguf metadata" {
    const allocator = std.testing.allocator;
    const writer = @import("../gguf/writer.zig");
    const metadata = [_]gguf_format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "whisper" } },
        .{ .key = "whisper.embedding_length", .value = .{ .u32 = 384 } },
        .{ .key = "whisper.encoder.block_count", .value = .{ .u32 = 4 } },
        .{ .key = "whisper.decoder.block_count", .value = .{ .u32 = 4 } },
        .{ .key = "whisper.encoder.attention.head_count", .value = .{ .u32 = 6 } },
        .{ .key = "whisper.decoder.attention.head_count", .value = .{ .u32 = 6 } },
        .{ .key = "whisper.encoder.feed_forward_length", .value = .{ .u32 = 1536 } },
        .{ .key = "whisper.decoder.feed_forward_length", .value = .{ .u32 = 1536 } },
        .{ .key = "whisper.num_mel_bins", .value = .{ .u32 = 80 } },
        .{ .key = "whisper.vocab_size", .value = .{ .u32 = 51865 } },
        .{ .key = "whisper.encoder.context_length", .value = .{ .u32 = 1500 } },
        .{ .key = "whisper.decoder.context_length", .value = .{ .u32 = 448 } },
        .{ .key = "whisper.scale_embedding", .value = .{ .bool_ = false } },
        .{ .key = "whisper.bos_token_id", .value = .{ .i64 = 50257 } },
        .{ .key = "whisper.eos_token_id", .value = .{ .i64 = 50257 } },
        .{ .key = "whisper.pad_token_id", .value = .{ .i64 = 50257 } },
        .{ .key = "whisper.decoder_start_token_id", .value = .{ .i64 = 50258 } },
    };
    var layout = try writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    var parsed = try gguf_format.parse(allocator, layout.header_bytes);
    defer parsed.deinit(allocator);
    const view = gguf_metadata.View.init(&parsed);
    const config = parseGgufMetadata(view).?;
    try std.testing.expectEqual(@as(u32, 384), config.d_model);
    try std.testing.expectEqual(@as(u32, 80), config.num_mel_bins);
    try std.testing.expectEqual(@as(i32, 50258), config.decoder_start_token_id);
}
