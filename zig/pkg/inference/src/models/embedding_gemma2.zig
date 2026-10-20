// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Typed, fail-closed configuration for EmbeddingGemma 2's text encoder.
//!
//! The upstream checkpoint nests this under `text_config`.  Keeping the
//! parser here (rather than coercing the model into the causal Gemma config)
//! makes the bidirectional attention and per-layer geometry explicit.

const std = @import("std");

pub const max_layers = 64;
pub const official_hidden_size: u32 = 512;
pub const official_layer_count: u32 = 24;
pub const official_intermediate_size: u32 = 2048;
pub const official_embedding_dim: u32 = 768;
pub const official_max_input_tokens: u32 = 8192;

/// Official SentenceTransformers prompts keyed by Antfly's Google-compatible
/// embedding task names. Keep the trailing space: the processor concatenates
/// the user's text directly after the literal prefix.
pub fn taskPrefix(task_type: []const u8) ![]const u8 {
    if (std.mem.eql(u8, task_type, "RETRIEVAL_QUERY")) return "task: search result | query: ";
    if (std.mem.eql(u8, task_type, "RETRIEVAL_DOCUMENT")) return "title: none | text: ";
    if (std.mem.eql(u8, task_type, "QUESTION_ANSWERING")) return "task: question answering | query: ";
    if (std.mem.eql(u8, task_type, "FACT_CHECKING") or std.mem.eql(u8, task_type, "FACT_VERIFICATION")) return "task: fact checking | query: ";
    if (std.mem.eql(u8, task_type, "CODE_RETRIEVAL") or std.mem.eql(u8, task_type, "CODE_RETRIEVAL_QUERY")) return "task: code retrieval | query: ";
    if (std.mem.eql(u8, task_type, "CLASSIFICATION")) return "task: classification | query: ";
    if (std.mem.eql(u8, task_type, "CLUSTERING")) return "task: clustering | query: ";
    if (std.mem.eql(u8, task_type, "SEMANTIC_SIMILARITY")) return "task: sentence similarity | query: ";
    return error.UnsupportedEmbeddingTaskType;
}

pub const LayerType = enum {
    sliding_attention,
    full_attention,
};

pub const RopeParameters = struct {
    sliding_theta: f32 = 10_000.0,
    full_theta: f32 = 1_000_000.0,
};

pub const VisionConfig = struct {
    hidden_size: u32,
    num_hidden_layers: u32,
    num_attention_heads: u32,
    patch_size: u32,
    pooling_kernel_size: u32,
    position_embedding_size: u32,
    rope_theta: f32,
    rms_norm_eps: f32,
};

pub const AudioConfig = struct {
    hidden_size: u32,
    num_hidden_layers: u32,
    num_attention_heads: u32,
    output_proj_dims: u32,
    conv_kernel_size: u32,
    hidden_act: enum { silu },
    attention_chunk_size: u32,
    left_chunk_size: u32,
    right_chunk_size: u32,
    attention_logit_cap: f32,
    rms_norm_eps: f32,
    subsampling_conv_channels: [2]u32,
    use_clipped_linears: bool,
};

pub const LayerConfig = struct {
    kind: LayerType,
    head_dim: u32,
    num_attention_heads: u32,
    num_key_value_heads: u32,
    rope_theta: f32,

    pub fn queryWidth(self: LayerConfig) u32 {
        return self.num_attention_heads * self.head_dim;
    }

    pub fn keyValueWidth(self: LayerConfig) u32 {
        return self.num_key_value_heads * self.head_dim;
    }
};

pub const Config = struct {
    vocab_size: u32,
    hidden_size: u32,
    num_hidden_layers: u32,
    intermediate_size: u32,
    embedding_dim: u32,
    max_position_embeddings: u32,
    sliding_window: u32,
    num_attention_heads: u32,
    num_key_value_heads: u32,
    head_dim: u32,
    rms_norm_eps: f32,
    layer_types: [max_layers]LayerType,
    layer_types_len: u8,
    per_layer_head_dim: [max_layers]u32,
    per_layer_num_key_value_heads: [max_layers]u32,
    rope: RopeParameters,
    vision: VisionConfig,
    audio: AudioConfig,

    pub fn outputDim(self: Config) u32 {
        return self.embedding_dim;
    }

    pub fn maxInputTokens(self: Config) u32 {
        // The trained serving contract is 8K even though RoPE metadata has a
        // larger theoretical position domain.
        return @min(self.max_position_embeddings, official_max_input_tokens);
    }

    pub fn layer(self: Config, index: usize) !LayerConfig {
        if (index >= self.layer_types_len or index >= self.num_hidden_layers)
            return error.InvalidLayerIndex;
        const kind = self.layer_types[index];
        return .{
            .kind = kind,
            .head_dim = if (self.per_layer_head_dim[index] != 0) self.per_layer_head_dim[index] else self.head_dim,
            .num_attention_heads = self.num_attention_heads,
            .num_key_value_heads = if (self.per_layer_num_key_value_heads[index] != 0)
                self.per_layer_num_key_value_heads[index]
            else
                self.num_key_value_heads,
            .rope_theta = switch (kind) {
                .sliding_attention => self.rope.sliding_theta,
                .full_attention => self.rope.full_theta,
            },
        };
    }

    pub fn validate(self: Config) !void {
        if (self.vocab_size == 0 or self.hidden_size == 0 or self.intermediate_size == 0 or
            self.embedding_dim == 0 or self.num_hidden_layers == 0 or
            self.num_hidden_layers > max_layers or self.layer_types_len != self.num_hidden_layers or
            self.num_attention_heads == 0 or self.num_key_value_heads == 0 or self.head_dim == 0 or
            self.max_position_embeddings == 0 or self.sliding_window == 0 or
            !std.math.isFinite(self.rms_norm_eps) or self.rms_norm_eps <= 0 or
            !std.math.isFinite(self.rope.sliding_theta) or self.rope.sliding_theta <= 0 or
            !std.math.isFinite(self.rope.full_theta) or self.rope.full_theta <= 0 or
            !std.math.isFinite(self.vision.rope_theta) or !std.math.isFinite(self.vision.rms_norm_eps) or
            !std.math.isFinite(self.audio.attention_logit_cap) or !std.math.isFinite(self.audio.rms_norm_eps))
            return error.InvalidEmbeddingGemma2Config;
        if (self.vision.hidden_size != 768 or self.vision.num_hidden_layers != 16 or
            self.vision.num_attention_heads != 12 or self.vision.patch_size != 16 or
            self.vision.pooling_kernel_size != 3 or self.vision.position_embedding_size != 10_240 or
            self.vision.rope_theta != 100 or self.vision.rms_norm_eps != 0.000001 or
            self.audio.hidden_size != 1024 or self.audio.num_hidden_layers != 12 or
            self.audio.num_attention_heads != 8 or self.audio.output_proj_dims != 1536 or
            self.audio.conv_kernel_size != 5 or self.audio.hidden_act != .silu or
            self.audio.attention_chunk_size != 12 or self.audio.left_chunk_size != 13 or
            self.audio.right_chunk_size != 0 or self.audio.attention_logit_cap != 50 or
            self.audio.rms_norm_eps != 0.000001 or
            !std.mem.eql(u32, &self.audio.subsampling_conv_channels, &.{ 128, 32 }) or
            !self.audio.use_clipped_linears)
            return error.UnsupportedEmbeddingGemma2MediaTopology;
        for (0..self.num_hidden_layers) |i| {
            const lc = try self.layer(i);
            if (lc.num_attention_heads % lc.num_key_value_heads != 0 or lc.head_dim % 2 != 0)
                return error.InvalidEmbeddingGemma2Config;
        }
    }

    pub fn isOfficialTextTopology(self: Config) bool {
        if (self.hidden_size != official_hidden_size or
            self.num_hidden_layers != official_layer_count or
            self.intermediate_size != official_intermediate_size or
            self.embedding_dim != official_embedding_dim or
            self.num_attention_heads != 4 or self.num_key_value_heads != 2 or
            self.head_dim != 256 or self.sliding_window != 512)
            return false;
        for (0..self.num_hidden_layers) |i| {
            const lc = self.layer(i) catch return false;
            const global = (i + 1) % 6 == 0;
            if (global != (lc.kind == .full_attention)) return false;
            if (lc.head_dim != @as(u32, if (global) 512 else 256)) return false;
            if (lc.num_key_value_heads != @as(u32, if (global) 1 else 2)) return false;
        }
        return true;
    }
};

fn requireObject(value: std.json.Value) !std.json.ObjectMap {
    return switch (value) {
        .object => |object| object,
        else => return error.InvalidEmbeddingGemma2Config,
    };
}

fn requireU32(object: std.json.ObjectMap, name: []const u8) !u32 {
    const value = object.get(name) orelse return error.InvalidEmbeddingGemma2Config;
    return switch (value) {
        .integer => |integer| std.math.cast(u32, integer) orelse error.InvalidEmbeddingGemma2Config,
        else => return error.InvalidEmbeddingGemma2Config,
    };
}

fn requireF32(object: std.json.ObjectMap, name: []const u8) !f32 {
    const value = object.get(name) orelse return error.InvalidEmbeddingGemma2Config;
    const result: f32 = switch (value) {
        .float => |float| @floatCast(float),
        .integer => |integer| @floatFromInt(integer),
        else => return error.InvalidEmbeddingGemma2Config,
    };
    if (!std.math.isFinite(result)) return error.InvalidEmbeddingGemma2Config;
    return result;
}

fn requireString(object: std.json.ObjectMap, name: []const u8) ![]const u8 {
    const value = object.get(name) orelse return error.InvalidEmbeddingGemma2Config;
    return switch (value) {
        .string => |string| string,
        else => error.InvalidEmbeddingGemma2Config,
    };
}

fn requireBool(object: std.json.ObjectMap, name: []const u8) !bool {
    const value = object.get(name) orelse return error.InvalidEmbeddingGemma2Config;
    return switch (value) {
        .bool => |boolean| boolean,
        else => error.InvalidEmbeddingGemma2Config,
    };
}

fn parseLayerType(value: std.json.Value) !LayerType {
    const name = switch (value) {
        .string => |string| string,
        else => return error.InvalidEmbeddingGemma2Config,
    };
    return std.meta.stringToEnum(LayerType, name) orelse error.InvalidEmbeddingGemma2Config;
}

fn parseRopeTheta(rope: std.json.ObjectMap, kind: []const u8) !f32 {
    const value = rope.get(kind) orelse return error.InvalidEmbeddingGemma2Config;
    return requireF32(try requireObject(value), "rope_theta");
}

/// Parse the complete upstream config.json. Unknown fields remain forward
/// compatible, while every execution-relevant text field is required.
pub fn parseConfig(allocator: std.mem.Allocator, bytes: []const u8) !Config {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, bytes, .{}) catch
        return error.InvalidEmbeddingGemma2Config;
    defer parsed.deinit();
    const root = try requireObject(parsed.value);
    const model_type = root.get("model_type") orelse return error.InvalidEmbeddingGemma2Config;
    if (model_type != .string or !std.mem.eql(u8, model_type.string, "embedding_gemma2"))
        return error.InvalidEmbeddingGemma2Config;
    const text = try requireObject(root.get("text_config") orelse return error.InvalidEmbeddingGemma2Config);
    const vision = try requireObject(root.get("vision_config") orelse return error.InvalidEmbeddingGemma2Config);
    const vision_rope = try requireObject(vision.get("rope_parameters") orelse return error.InvalidEmbeddingGemma2Config);
    const audio = try requireObject(root.get("audio_config") orelse return error.InvalidEmbeddingGemma2Config);
    const audio_channels = audio.get("subsampling_conv_channels") orelse return error.InvalidEmbeddingGemma2Config;
    if (audio_channels != .array or audio_channels.array.items.len != 2)
        return error.InvalidEmbeddingGemma2Config;

    var config = Config{
        .vocab_size = try requireU32(text, "vocab_size"),
        .hidden_size = try requireU32(text, "hidden_size"),
        .num_hidden_layers = try requireU32(text, "num_hidden_layers"),
        .intermediate_size = try requireU32(text, "intermediate_size"),
        .embedding_dim = try requireU32(text, "embedding_dim"),
        .max_position_embeddings = try requireU32(text, "max_position_embeddings"),
        .sliding_window = try requireU32(text, "sliding_window"),
        .num_attention_heads = try requireU32(text, "num_attention_heads"),
        .num_key_value_heads = try requireU32(text, "num_key_value_heads"),
        .head_dim = try requireU32(text, "head_dim"),
        .rms_norm_eps = try requireF32(text, "rms_norm_eps"),
        .layer_types = @splat(.sliding_attention),
        .layer_types_len = 0,
        .per_layer_head_dim = @splat(0),
        .per_layer_num_key_value_heads = @splat(0),
        .rope = .{},
        .vision = .{
            .hidden_size = try requireU32(vision, "hidden_size"),
            .num_hidden_layers = try requireU32(vision, "num_hidden_layers"),
            .num_attention_heads = try requireU32(vision, "num_attention_heads"),
            .patch_size = try requireU32(vision, "patch_size"),
            .pooling_kernel_size = try requireU32(vision, "pooling_kernel_size"),
            .position_embedding_size = try requireU32(vision, "position_embedding_size"),
            .rope_theta = try requireF32(vision_rope, "rope_theta"),
            .rms_norm_eps = try requireF32(vision, "rms_norm_eps"),
        },
        .audio = .{
            .hidden_size = try requireU32(audio, "hidden_size"),
            .num_hidden_layers = try requireU32(audio, "num_hidden_layers"),
            .num_attention_heads = try requireU32(audio, "num_attention_heads"),
            .output_proj_dims = try requireU32(audio, "output_proj_dims"),
            .conv_kernel_size = try requireU32(audio, "conv_kernel_size"),
            .hidden_act = if (std.mem.eql(u8, try requireString(audio, "hidden_act"), "silu")) .silu else return error.UnsupportedEmbeddingGemma2MediaTopology,
            .attention_chunk_size = try requireU32(audio, "attention_chunk_size"),
            .left_chunk_size = try requireU32(audio, "attention_context_left"),
            .right_chunk_size = try requireU32(audio, "attention_context_right"),
            .attention_logit_cap = try requireF32(audio, "attention_logit_cap"),
            .rms_norm_eps = try requireF32(audio, "rms_norm_eps"),
            .subsampling_conv_channels = .{
                switch (audio_channels.array.items[0]) {
                    .integer => |value| std.math.cast(u32, value) orelse return error.InvalidEmbeddingGemma2Config,
                    else => return error.InvalidEmbeddingGemma2Config,
                },
                switch (audio_channels.array.items[1]) {
                    .integer => |value| std.math.cast(u32, value) orelse return error.InvalidEmbeddingGemma2Config,
                    else => return error.InvalidEmbeddingGemma2Config,
                },
            },
            .use_clipped_linears = try requireBool(audio, "use_clipped_linears"),
        },
    };

    const layer_types = text.get("layer_types") orelse return error.InvalidEmbeddingGemma2Config;
    if (layer_types != .array or layer_types.array.items.len > max_layers)
        return error.InvalidEmbeddingGemma2Config;
    for (layer_types.array.items, 0..) |value, i| config.layer_types[i] = try parseLayerType(value);
    config.layer_types_len = @intCast(layer_types.array.items.len);

    const per_layer = try requireObject(text.get("per_layer_config") orelse return error.InvalidEmbeddingGemma2Config);
    var iterator = per_layer.iterator();
    while (iterator.next()) |entry| {
        const index = std.fmt.parseInt(usize, entry.key_ptr.*, 10) catch return error.InvalidEmbeddingGemma2Config;
        if (index >= config.num_hidden_layers or index >= max_layers) return error.InvalidEmbeddingGemma2Config;
        const override = try requireObject(entry.value_ptr.*);
        config.per_layer_head_dim[index] = try requireU32(override, "head_dim");
        config.per_layer_num_key_value_heads[index] = try requireU32(override, "num_key_value_heads");
    }

    const rope = try requireObject(text.get("rope_parameters") orelse return error.InvalidEmbeddingGemma2Config);
    config.rope = .{
        .sliding_theta = try parseRopeTheta(rope, "sliding_attention"),
        .full_theta = try parseRopeTheta(rope, "full_attention"),
    };
    try config.validate();
    return config;
}

test "official config resolves alternating layer geometry" {
    const json =
        \\{"model_type":"embedding_gemma2",
        \\"vision_config":{"hidden_size":768,"num_hidden_layers":16,"num_attention_heads":12,"patch_size":16,"pooling_kernel_size":3,"position_embedding_size":10240,"rms_norm_eps":0.000001,"rope_parameters":{"rope_theta":100}},
        \\"audio_config":{"hidden_size":1024,"num_hidden_layers":12,"num_attention_heads":8,"output_proj_dims":1536,"conv_kernel_size":5,"hidden_act":"silu","attention_chunk_size":12,"attention_context_left":13,"attention_context_right":0,"attention_logit_cap":50,"rms_norm_eps":0.000001,"subsampling_conv_channels":[128,32],"use_clipped_linears":true},
        \\"text_config":{"vocab_size":262144,"hidden_size":512,
        \\"num_hidden_layers":24,"intermediate_size":2048,"embedding_dim":768,"max_position_embeddings":262144,
        \\"sliding_window":512,"num_attention_heads":4,"num_key_value_heads":2,"head_dim":256,"rms_norm_eps":0.000001,
        \\"layer_types":["sliding_attention","sliding_attention","sliding_attention","sliding_attention","sliding_attention","full_attention","sliding_attention","sliding_attention","sliding_attention","sliding_attention","sliding_attention","full_attention","sliding_attention","sliding_attention","sliding_attention","sliding_attention","sliding_attention","full_attention","sliding_attention","sliding_attention","sliding_attention","sliding_attention","sliding_attention","full_attention"],
        \\"per_layer_config":{"05":{"head_dim":512,"num_key_value_heads":1},"11":{"head_dim":512,"num_key_value_heads":1},"17":{"head_dim":512,"num_key_value_heads":1},"23":{"head_dim":512,"num_key_value_heads":1}},
        \\"rope_parameters":{"sliding_attention":{"rope_theta":10000},"full_attention":{"rope_theta":1000000}}}}
    ;
    const config = try parseConfig(std.testing.allocator, json);
    try std.testing.expect(config.isOfficialTextTopology());
    try std.testing.expectEqual(@as(u32, 8192), config.maxInputTokens());
    try std.testing.expectEqual(LayerType.sliding_attention, (try config.layer(4)).kind);
    const global = try config.layer(5);
    try std.testing.expectEqual(LayerType.full_attention, global.kind);
    try std.testing.expectEqual(@as(u32, 512), global.head_dim);
    try std.testing.expectEqual(@as(u32, 1), global.num_key_value_heads);
    try std.testing.expectEqual(@as(u32, 2048), global.queryWidth());
    var unsupported = config;
    unsupported.vision.patch_size = 14;
    try std.testing.expectError(error.UnsupportedEmbeddingGemma2MediaTopology, unsupported.validate());
    var nonfinite = config;
    nonfinite.rms_norm_eps = std.math.nan(f32);
    try std.testing.expectError(error.InvalidEmbeddingGemma2Config, nonfinite.validate());
    nonfinite = config;
    nonfinite.rope.full_theta = std.math.inf(f32);
    try std.testing.expectError(error.InvalidEmbeddingGemma2Config, nonfinite.validate());
    nonfinite = config;
    nonfinite.audio.attention_logit_cap = -std.math.inf(f32);
    try std.testing.expectError(error.InvalidEmbeddingGemma2Config, nonfinite.validate());
}

test "config rejects incomplete layer schedules" {
    const json =
        \\{"model_type":"embedding_gemma2",
        \\"vision_config":{"hidden_size":768,"num_hidden_layers":16,"num_attention_heads":12,"patch_size":16,"pooling_kernel_size":3,"position_embedding_size":10240,"rms_norm_eps":0.000001,"rope_parameters":{"rope_theta":100}},
        \\"audio_config":{"hidden_size":1024,"num_hidden_layers":12,"num_attention_heads":8,"output_proj_dims":1536,"conv_kernel_size":5,"hidden_act":"silu","attention_chunk_size":12,"attention_context_left":13,"attention_context_right":0,"attention_logit_cap":50,"rms_norm_eps":0.000001,"subsampling_conv_channels":[128,32],"use_clipped_linears":true},
        \\"text_config":{"vocab_size":1,"hidden_size":512,"num_hidden_layers":24,
        \\"intermediate_size":2048,"embedding_dim":768,"max_position_embeddings":8192,"sliding_window":512,
        \\"num_attention_heads":4,"num_key_value_heads":2,"head_dim":256,"rms_norm_eps":0.000001,
        \\"layer_types":["sliding_attention"],"per_layer_config":{},
        \\"rope_parameters":{"sliding_attention":{"rope_theta":10000},"full_attention":{"rope_theta":1000000}}}}
    ;
    try std.testing.expectError(error.InvalidEmbeddingGemma2Config, parseConfig(std.testing.allocator, json));
}

test "official task prefixes preserve separators and trailing spaces" {
    try std.testing.expectEqualStrings("title: none | text: ", try taskPrefix("RETRIEVAL_DOCUMENT"));
    try std.testing.expectEqualStrings("task: fact checking | query: ", try taskPrefix("FACT_CHECKING"));
    try std.testing.expectEqualStrings("task: fact checking | query: ", try taskPrefix("FACT_VERIFICATION"));
    try std.testing.expectEqualStrings("task: code retrieval | query: ", try taskPrefix("CODE_RETRIEVAL"));
    try std.testing.expectEqualStrings("task: code retrieval | query: ", try taskPrefix("CODE_RETRIEVAL_QUERY"));
    try std.testing.expectError(error.UnsupportedEmbeddingTaskType, taskPrefix("GENERATION"));
}
