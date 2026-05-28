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

// DeBERTa-v2/v3 model configuration.
//
// DeBERTa differs from BERT in using disentangled attention with relative
// position encodings instead of absolute position embeddings.

const std = @import("std");
const gguf_metadata = @import("../gguf/metadata.zig");

pub const Config = struct {
    hidden_size: u32 = 768,
    num_hidden_layers: u32 = 12,
    num_attention_heads: u32 = 12,
    intermediate_size: u32 = 3072,
    vocab_size: u32 = 128011,
    max_position_embeddings: u32 = 512,
    position_buckets: u32 = 256,
    layer_norm_eps: f32 = 1e-7,
    // GLiNER label marker token IDs (from added_tokens.json).
    classification_token_id: i64 = 128004,
    entity_token_id: i64 = 128005,
    relation_token_id: i64 = 128006,
    num_labels: u32 = 1,
};

pub fn parseConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    var config = Config{};

    if (obj.get("hidden_size")) |v| config.hidden_size = jsonU32(v) orelse config.hidden_size;
    if (obj.get("num_hidden_layers")) |v| config.num_hidden_layers = jsonU32(v) orelse config.num_hidden_layers;
    if (obj.get("num_attention_heads")) |v| config.num_attention_heads = jsonU32(v) orelse config.num_attention_heads;
    if (obj.get("intermediate_size")) |v| config.intermediate_size = jsonU32(v) orelse config.intermediate_size;
    if (obj.get("vocab_size")) |v| config.vocab_size = jsonU32(v) orelse config.vocab_size;
    if (obj.get("max_position_embeddings")) |v| config.max_position_embeddings = jsonU32(v) orelse config.max_position_embeddings;
    if (obj.get("position_buckets")) |v| config.position_buckets = jsonU32(v) orelse config.position_buckets;
    if (obj.get("num_labels")) |v| config.num_labels = jsonU32(v) orelse config.num_labels;
    if (config.num_labels == 1) {
        if (inferNumLabels(obj)) |n| config.num_labels = n;
    }

    if (obj.get("layer_norm_eps")) |v| {
        config.layer_norm_eps = switch (v) {
            .float => |f| @floatCast(f),
            .integer => |i| @floatFromInt(i),
            else => config.layer_norm_eps,
        };
    }

    return config;
}

pub fn isDebertaModel(model_type: []const u8) bool {
    return std.mem.eql(u8, model_type, "deberta-v2") or
        std.mem.eql(u8, model_type, "deberta-v3") or
        std.mem.eql(u8, model_type, "deberta");
}

pub fn parseGgufMetadata(view: gguf_metadata.View) ?Config {
    const arch = view.getString("general.architecture") orelse return null;
    if (!std.mem.eql(u8, arch, "deberta")) return null;

    var buf: [96]u8 = undefined;
    var config = Config{};
    config.vocab_size = metaU32(view, &buf, arch, "vocab_size") orelse config.vocab_size;
    config.hidden_size = metaU32(view, &buf, arch, "embedding_length") orelse config.hidden_size;
    config.num_hidden_layers = metaU32(view, &buf, arch, "block_count") orelse config.num_hidden_layers;
    config.num_attention_heads = metaU32(view, &buf, arch, "attention.head_count") orelse config.num_attention_heads;
    config.intermediate_size = metaU32(view, &buf, arch, "feed_forward_length") orelse config.intermediate_size;
    config.max_position_embeddings = metaU32(view, &buf, arch, "context_length") orelse config.max_position_embeddings;
    config.position_buckets = metaU32(view, &buf, arch, "position_buckets") orelse config.position_buckets;
    config.num_labels = metaU32(view, &buf, arch, "label_count") orelse config.num_labels;
    config.layer_norm_eps = metaF32(view, &buf, arch, "layer_norm_epsilon") orelse config.layer_norm_eps;
    return config;
}

/// Compute relative position index for DeBERTa-v2/v3 disentangled attention.
/// Matches HuggingFace transformers' make_log_bucket_position + att_span offset.
///
/// For positions within (-mid, mid): returns relative_position + att_span (identity mapping).
/// For positions outside that range: logarithmic bucketing + att_span offset.
/// Returns index in [0, max_position_embeddings-1] for indexing into rel_embeddings.weight.
pub fn relativePositionBucket(relative_position: i64, num_buckets: u32, max_position: u32) u32 {
    const mid: i64 = @divTrunc(@as(i64, @intCast(num_buckets)), 2);
    // HuggingFace: att_span = self.pos_ebd_size = self.position_buckets = 256
    // c2p_pos = clamp(bucket_pos + att_span, 0, att_span * 2 - 1)
    const att_span: i64 = @intCast(num_buckets);

    // For positions within (-mid, mid): bucket_pos = relative_position (identity)
    // For positions outside: logarithmic bucketing
    if (relative_position > -mid and relative_position < mid) {
        // Identity mapping: index = relative_position + att_span
        const idx = relative_position + att_span;
        return @intCast(std.math.clamp(idx, 0, @as(i64, @intCast(max_position)) - 1));
    }

    // Logarithmic bucketing for large positions
    const sign: f64 = if (relative_position > 0) 1.0 else -1.0;
    const abs_rel: f64 = @floatFromInt(if (relative_position >= 0) relative_position else -relative_position);
    const mid_f: f64 = @floatFromInt(mid);
    const max_pos_f: f64 = @floatFromInt(max_position);

    // log_pos = ceil(log(abs_rel / mid) / log((max_position - 1) / mid) * (mid - 1)) + mid
    const log_pos = @ceil(@log(abs_rel / mid_f) / @log((max_pos_f - 1.0) / mid_f) * (mid_f - 1.0)) + mid_f;
    const bucket_pos: i64 = @intFromFloat(log_pos * sign);

    const idx = bucket_pos + att_span;
    return @intCast(std.math.clamp(idx, 0, @as(i64, @intCast(max_position)) - 1));
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

fn metaU32(view: gguf_metadata.View, buf: *[96]u8, arch: []const u8, suffix: []const u8) ?u32 {
    const key = std.fmt.bufPrint(buf, "{s}.{s}", .{ arch, suffix }) catch return null;
    return std.math.cast(u32, view.getU64(key) orelse return null);
}

fn metaF32(view: gguf_metadata.View, buf: *[96]u8, arch: []const u8, suffix: []const u8) ?f32 {
    const key = std.fmt.bufPrint(buf, "{s}.{s}", .{ arch, suffix }) catch return null;
    return view.getF32(key);
}

test "relative position bucket" {
    // Position 0 → index 256 (center of 512-row table)
    try std.testing.expectEqual(@as(u32, 256), relativePositionBucket(0, 256, 512));
    // Position 1 → index 257 (identity mapping for small positions)
    try std.testing.expectEqual(@as(u32, 257), relativePositionBucket(1, 256, 512));
    // Position -1 → index 255
    try std.testing.expectEqual(@as(u32, 255), relativePositionBucket(-1, 256, 512));
    // Position 22 → index 278
    try std.testing.expectEqual(@as(u32, 278), relativePositionBucket(22, 256, 512));
    // Position -22 → index 234
    try std.testing.expectEqual(@as(u32, 234), relativePositionBucket(-22, 256, 512));
    // Position 127 → index 383 (still within [-128, 128))
    try std.testing.expectEqual(@as(u32, 383), relativePositionBucket(127, 256, 512));
    // Position -127 → index 129
    try std.testing.expectEqual(@as(u32, 129), relativePositionBucket(-127, 256, 512));
    // Boundary: 128 still maps to identity (abs_pos=128 <= mid=128 in Python)
    try std.testing.expectEqual(@as(u32, 384), relativePositionBucket(128, 256, 512));
    try std.testing.expectEqual(@as(u32, 128), relativePositionBucket(-128, 256, 512));
    // Log-bucketed: 200 → bucket 169 → index 425
    try std.testing.expectEqual(@as(u32, 425), relativePositionBucket(200, 256, 512));
    // Log-bucketed: -200 → bucket -169 → index 87
    try std.testing.expectEqual(@as(u32, 87), relativePositionBucket(-200, 256, 512));
}

test "parse config carries num_labels" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{"model_type":"deberta-v3","hidden_size":768,"num_hidden_layers":12,"num_attention_heads":12,"intermediate_size":3072,"num_labels":3}
    ;
    const config = try parseConfig(allocator, json_str);
    try std.testing.expectEqual(@as(u32, 3), config.num_labels);
    try std.testing.expectEqual(@as(u32, 768), config.hidden_size);
}

test "parse config infers num_labels from id2label" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{"model_type":"deberta-v2","hidden_size":384,"id2label":{"0":"O","1":"B-EMAIL","2":"I-EMAIL"}}
    ;
    const config = try parseConfig(allocator, json_str);
    try std.testing.expectEqual(@as(u32, 3), config.num_labels);
}

test "isDebertaModel matches supported model types" {
    try std.testing.expect(isDebertaModel("deberta"));
    try std.testing.expect(isDebertaModel("deberta-v2"));
    try std.testing.expect(isDebertaModel("deberta-v3"));
    try std.testing.expect(!isDebertaModel("bert"));
}

test "parse gguf metadata for deberta config" {
    const allocator = std.testing.allocator;
    var data = std.ArrayList(u8).empty;
    defer data.deinit(allocator);

    const format = @import("../gguf/format.zig");
    const tensor_types = @import("../gguf/tensor_types.zig");

    try data.appendSlice(allocator, "GGUF");
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 0);
    try appendLe(u64, allocator, &data, 10);
    try appendString(allocator, &data, "general.architecture");
    try appendLe(u32, allocator, &data, 8);
    try appendString(allocator, &data, "deberta");
    try appendMetadataU32(allocator, &data, "deberta.vocab_size", 32000);
    try appendMetadataU32(allocator, &data, "deberta.embedding_length", 384);
    try appendMetadataU32(allocator, &data, "deberta.block_count", 6);
    try appendMetadataU32(allocator, &data, "deberta.attention.head_count", 6);
    try appendMetadataU32(allocator, &data, "deberta.feed_forward_length", 1536);
    try appendMetadataU32(allocator, &data, "deberta.context_length", 1024);
    try appendMetadataU32(allocator, &data, "deberta.position_buckets", 128);
    try appendMetadataU32(allocator, &data, "deberta.label_count", 9);
    try appendMetadataF32(allocator, &data, "deberta.layer_norm_epsilon", 1e-6);
    try padToAlignment(allocator, &data, format.default_alignment);

    var parsed = try format.parse(allocator, data.items);
    defer parsed.deinit(allocator);
    const view = gguf_metadata.View.init(&parsed);
    const cfg = parseGgufMetadata(view).?;
    try std.testing.expectEqual(@as(u32, 32000), cfg.vocab_size);
    try std.testing.expectEqual(@as(u32, 384), cfg.hidden_size);
    try std.testing.expectEqual(@as(u32, 6), cfg.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 6), cfg.num_attention_heads);
    try std.testing.expectEqual(@as(u32, 1536), cfg.intermediate_size);
    try std.testing.expectEqual(@as(u32, 1024), cfg.max_position_embeddings);
    try std.testing.expectEqual(@as(u32, 128), cfg.position_buckets);
    try std.testing.expectEqual(@as(u32, 9), cfg.num_labels);
    try std.testing.expectApproxEqRel(@as(f32, 1e-6), cfg.layer_norm_eps, 1e-6);
    _ = tensor_types;
}

fn appendLe(comptime T: type, allocator: std.mem.Allocator, data: *std.ArrayList(u8), value: T) !void {
    const bytes = std.mem.asBytes(&std.mem.nativeToLittle(T, value));
    try data.appendSlice(allocator, bytes);
}

fn appendString(allocator: std.mem.Allocator, data: *std.ArrayList(u8), value: []const u8) !void {
    try appendLe(u64, allocator, data, value.len);
    try data.appendSlice(allocator, value);
}

fn appendMetadataU32(allocator: std.mem.Allocator, data: *std.ArrayList(u8), key: []const u8, value: u32) !void {
    try appendString(allocator, data, key);
    try appendLe(u32, allocator, data, 4);
    try appendLe(u32, allocator, data, value);
}

fn appendMetadataF32(allocator: std.mem.Allocator, data: *std.ArrayList(u8), key: []const u8, value: f32) !void {
    try appendString(allocator, data, key);
    try appendLe(u32, allocator, data, 6);
    try appendLe(u32, allocator, data, @bitCast(value));
}

fn padToAlignment(allocator: std.mem.Allocator, data: *std.ArrayList(u8), alignment: u32) !void {
    const aligned = std.mem.alignForward(usize, data.items.len, alignment);
    if (aligned > data.items.len) {
        try data.appendNTimes(allocator, 0, aligned - data.items.len);
    }
}
