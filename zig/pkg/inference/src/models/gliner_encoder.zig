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

//! Span wrappers declare heads; the nested encoder config determines the trunk.
//! Keep this distinction explicit so an Ettin checkpoint cannot inherit DeBERTa
//! defaults merely because its wrapper has model_type="extractor".
const std = @import("std");
const deberta = @import("deberta.zig");
const modern = @import("../architectures/modern_bert.zig");

pub const Config = union(enum) {
    deberta: deberta.Config,
    modern_bert: modern.Config,

    pub fn geometry(self: Config) Geometry {
        return switch (self) {
            inline else => |cfg| .{
                .hidden_size = cfg.hidden_size,
                .intermediate_size = cfg.intermediate_size,
                .num_hidden_layers = cfg.num_hidden_layers,
                .num_attention_heads = cfg.num_attention_heads,
                .vocab_size = cfg.vocab_size,
                .max_position_embeddings = cfg.max_position_embeddings,
                .layer_norm_eps = cfg.layer_norm_eps,
            },
        };
    }
};

pub const Geometry = struct {
    hidden_size: u32,
    intermediate_size: u32,
    num_hidden_layers: u32,
    num_attention_heads: u32,
    vocab_size: u32,
    max_position_embeddings: u32,
    layer_norm_eps: f32,
};

pub fn parse(allocator: std.mem.Allocator, bytes: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidGlinerEncoderConfig;
    const obj = parsed.value.object;
    const kind = obj.get("model_type") orelse return error.MissingGlinerEncoderType;
    if (kind != .string) return error.InvalidGlinerEncoderConfig;
    if (deberta.isDebertaModel(kind.string)) return .{ .deberta = try deberta.parseConfig(allocator, bytes) };
    if (!modern.isModernBertModel(kind.string)) return error.UnsupportedGlinerEncoder;
    if (obj.get("laya") != null) return error.UnsupportedGlinerEncoder;
    // The published Ettin layout is bidirectional, bias-free and exact GELU.
    // Do not quietly accept configs whose omitted graph operations differ.
    inline for (.{ "causal_mask", "is_causal", "attention_bias", "mlp_bias", "norm_bias" }) |key| {
        if (obj.get(key)) |v| if (v != .bool or v.bool) return error.UnsupportedGlinerEncoder;
    }
    if (obj.get("hidden_activation")) |v| {
        if (v != .string or !std.mem.eql(u8, v.string, "gelu")) return error.UnsupportedGlinerEncoder;
    }
    if (obj.get("rope_scaling")) |v| if (v != .null) return error.UnsupportedGlinerEncoder;
    inline for (.{ "global_attn_every_n_layers", "local_attention" }) |key| {
        if (obj.get(key)) |v| if (v != .integer or v.integer <= 0 or v.integer > std.math.maxInt(i32)) return error.InvalidGlinerEncoderConfig;
    }
    if (obj.get("rope_parameters")) |v| {
        if (v != .object) return error.InvalidGlinerEncoderConfig;
        for (v.object.values()) |entry| {
            if (entry != .object) return error.InvalidGlinerEncoderConfig;
            if (entry.object.get("rope_type")) |rope_type| {
                if (rope_type != .string or !std.mem.eql(u8, rope_type.string, "default")) return error.UnsupportedGlinerEncoder;
            }
        }
    }
    inline for (.{ "hidden_size", "intermediate_size", "num_hidden_layers", "num_attention_heads", "vocab_size", "max_position_embeddings" }) |key| {
        const value = obj.get(key) orelse return error.InvalidGlinerEncoderConfig;
        if (value != .integer or value.integer <= 0 or value.integer > std.math.maxInt(i32)) return error.InvalidGlinerEncoderConfig;
    }
    const cfg = try modern.parseConfig(allocator, bytes);
    if (cfg.hidden_size % cfg.num_attention_heads != 0 or
        (cfg.hidden_size / cfg.num_attention_heads) % 2 != 0 or
        cfg.global_attn_every_n_layers == 0 or cfg.local_attention_window == 0 or
        !std.math.isFinite(cfg.layer_norm_eps) or cfg.layer_norm_eps <= 0 or
        !std.math.isFinite(cfg.global_rope_theta) or cfg.global_rope_theta <= 0 or
        !std.math.isFinite(cfg.local_rope_theta) or cfg.local_rope_theta <= 0)
        return error.InvalidGlinerEncoderConfig;
    return .{ .modern_bert = cfg };
}

/// Validate every tensor consumed by the ModernBERT decision graph before
/// uploading weights. The source may contain unrelated span heads, but they
/// cannot substitute for a missing or malformed decision tensor.
pub fn validateDecisionWeights(store: @import("tensor_store.zig").TensorStore, cfg: modern.Config) !void {
    const reader = store.singleSafetensorsReader() orelse return error.UnsupportedGlinerDecisionArtifact;
    const Check = struct {
        fn tensor(r: @TypeOf(reader), name: []const u8, shape: []const i64) !void {
            const meta = r.header.tensors.get(name) orelse return error.InvalidGlinerDecisionWeights;
            if (!std.mem.eql(i64, meta.shape, shape)) return error.InvalidGlinerDecisionWeights;
            switch (meta.dtype) {
                .f32, .f16, .bf16 => {},
                else => return error.InvalidGlinerDecisionWeights,
            }
        }
    };
    const h: i64 = cfg.hidden_size;
    const f: i64 = cfg.intermediate_size;
    try Check.tensor(reader, "encoder.embeddings.tok_embeddings.weight", &.{ cfg.vocab_size, h });
    try Check.tensor(reader, "encoder.embeddings.norm.weight", &.{h});
    try Check.tensor(reader, "encoder.final_norm.weight", &.{h});
    var name: [128]u8 = undefined;
    for (0..cfg.num_hidden_layers) |layer| {
        if (layer > 0) try Check.tensor(reader, try std.fmt.bufPrint(&name, "encoder.layers.{d}.attn_norm.weight", .{layer}), &.{h});
        try Check.tensor(reader, try std.fmt.bufPrint(&name, "encoder.layers.{d}.mlp_norm.weight", .{layer}), &.{h});
        try Check.tensor(reader, try std.fmt.bufPrint(&name, "encoder.layers.{d}.attn.Wqkv.weight", .{layer}), &.{ h * 3, h });
        try Check.tensor(reader, try std.fmt.bufPrint(&name, "encoder.layers.{d}.attn.Wo.weight", .{layer}), &.{ h, h });
        try Check.tensor(reader, try std.fmt.bufPrint(&name, "encoder.layers.{d}.mlp.Wi.weight", .{layer}), &.{ f * 2, h });
        try Check.tensor(reader, try std.fmt.bufPrint(&name, "encoder.layers.{d}.mlp.Wo.weight", .{layer}), &.{ h, f });
    }
    try Check.tensor(reader, "classifier.0.weight", &.{ h * 2, h });
    try Check.tensor(reader, "classifier.0.bias", &.{h * 2});
    try Check.tensor(reader, "classifier.2.weight", &.{ 1, h * 2 });
    try Check.tensor(reader, "classifier.2.bias", &.{1});
}

test "GLiNER encoder dispatches Ettin independently of span heads" {
    const cfg = try parse(std.testing.allocator,
        \\{"model_type":"modernbert","hidden_size":1792,"intermediate_size":3840,"num_hidden_layers":28,"num_attention_heads":28,"vocab_size":50378,"max_position_embeddings":7999,"rope_parameters":{"full_attention":{"rope_type":"default","rope_theta":160000},"sliding_attention":{"rope_type":"default","rope_theta":160000}}}
    );
    try std.testing.expect(cfg == .modern_bert);
    try std.testing.expectEqual(@as(u32, 7999), cfg.geometry().max_position_embeddings);
    try std.testing.expectEqual(@as(f32, 160000), cfg.modern_bert.local_rope_theta);
    try std.testing.expect(!cfg.modern_bert.rope_interleaved);
    try std.testing.expectError(error.UnsupportedGlinerEncoder, parse(std.testing.allocator, "{\"model_type\":\"llama\"}"));
    try std.testing.expectError(error.UnsupportedGlinerEncoder, parse(std.testing.allocator, "{\"model_type\":\"modernbert\",\"is_causal\":true}"));
    try std.testing.expectError(error.InvalidGlinerEncoderConfig, parse(std.testing.allocator, "{\"model_type\":\"modernbert\"}"));
}
