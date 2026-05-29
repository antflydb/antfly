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

// BERT model architecture for embedding and cross-encoder inference.
//
// Supports bert, roberta, and distilbert model types.
// Produces weight mappings from SafeTensors tensor names to model scope paths.

const std = @import("std");
const gguf_metadata = @import("../gguf/metadata.zig");
const WeightMapping = @import("weight_source.zig").WeightMapping;

pub const ModelType = enum {
    bert,
    roberta,
    distilbert,
};

/// BERT model configuration loaded from config.json.
pub const Config = struct {
    model_type: ModelType = .bert,
    /// Override weight prefix in SafeTensors file.
    /// null = use default for model_type ("bert", "distilbert", etc.)
    /// "" = no prefix (weights like "embeddings.word_embeddings.weight")
    weight_prefix: ?[]const u8 = null,
    vocab_size: u32 = 30522,
    hidden_size: u32 = 768,
    num_hidden_layers: u32 = 12,
    num_attention_heads: u32 = 12,
    intermediate_size: u32 = 3072,
    max_position_embeddings: u32 = 512,
    type_vocab_size: u32 = 2,
    hidden_act: []const u8 = "gelu",
    num_labels: u32 = 1,

    /// Returns the effective weight prefix for this config.
    pub fn effectivePrefix(self: Config) []const u8 {
        if (self.weight_prefix) |p| return p;
        return switch (self.model_type) {
            .bert, .roberta => "bert",
            .distilbert => "distilbert",
        };
    }
};

/// Parse a BERT config from JSON bytes.
pub fn parseConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    var config = Config{};

    if (obj.get("model_type")) |v| {
        const s = switch (v) {
            .string => |str| str,
            else => "bert",
        };
        if (std.mem.eql(u8, s, "roberta") or std.mem.eql(u8, s, "xlm-roberta")) {
            config.model_type = .roberta;
        } else if (std.mem.eql(u8, s, "distilbert")) {
            config.model_type = .distilbert;
        }
    }

    if (obj.get("vocab_size")) |v| config.vocab_size = jsonU32(v) orelse config.vocab_size;
    if (obj.get("hidden_size")) |v| config.hidden_size = jsonU32(v) orelse config.hidden_size;
    if (obj.get("num_hidden_layers")) |v| config.num_hidden_layers = jsonU32(v) orelse config.num_hidden_layers;
    if (obj.get("num_attention_heads")) |v| config.num_attention_heads = jsonU32(v) orelse config.num_attention_heads;
    if (obj.get("intermediate_size")) |v| config.intermediate_size = jsonU32(v) orelse config.intermediate_size;
    if (obj.get("max_position_embeddings")) |v| config.max_position_embeddings = jsonU32(v) orelse config.max_position_embeddings;
    if (obj.get("type_vocab_size")) |v| config.type_vocab_size = jsonU32(v) orelse config.type_vocab_size;
    if (obj.get("num_labels")) |v| config.num_labels = jsonU32(v) orelse config.num_labels;
    if (config.num_labels == 1) {
        if (inferNumLabels(obj)) |n| config.num_labels = n;
    }

    return config;
}

pub fn isBertModel(model_type: []const u8) bool {
    return std.mem.eql(u8, model_type, "bert") or
        std.mem.eql(u8, model_type, "roberta") or
        std.mem.eql(u8, model_type, "xlm-roberta") or
        std.mem.eql(u8, model_type, "distilbert");
}

pub fn parseGgufMetadata(view: gguf_metadata.View) ?Config {
    const arch = view.getString("general.architecture") orelse return null;
    if (!std.mem.eql(u8, arch, "bert")) return null;

    var config = Config{ .weight_prefix = "" };
    if (view.getString("bert.family")) |family| {
        if (std.mem.eql(u8, family, "roberta")) {
            config.model_type = .roberta;
        } else if (std.mem.eql(u8, family, "distilbert")) {
            config.model_type = .distilbert;
        }
    }

    config.vocab_size = metaU32(view, "bert.vocab_size") orelse config.vocab_size;
    config.hidden_size = metaU32(view, "bert.embedding_length") orelse config.hidden_size;
    config.num_hidden_layers = metaU32(view, "bert.block_count") orelse config.num_hidden_layers;
    config.num_attention_heads = metaU32(view, "bert.attention.head_count") orelse config.num_attention_heads;
    config.intermediate_size = metaU32(view, "bert.feed_forward_length") orelse config.intermediate_size;
    config.max_position_embeddings = metaU32(view, "bert.context_length") orelse config.max_position_embeddings;
    config.type_vocab_size = metaU32(view, "bert.token_type_count") orelse config.type_vocab_size;
    config.num_labels = metaU32(view, "bert.label_count") orelse config.num_labels;
    if (view.getString("bert.hidden_act")) |value| {
        config.hidden_act = if (std.mem.eql(u8, value, "relu")) "relu" else "gelu";
    }
    return config;
}

fn metaU32(view: gguf_metadata.View, key: []const u8) ?u32 {
    return std.math.cast(u32, view.getU64(key) orelse return null);
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

/// Generate the weight mapping from SafeTensors tensor names to model scope paths.
/// Caller owns the returned slice and all strings within it.
pub fn weightMapping(allocator: std.mem.Allocator, config: Config) ![]WeightMapping {
    var mappings = std.ArrayListUnmanaged(WeightMapping).empty;

    const prefix = config.effectivePrefix();

    // Embeddings
    try addPrefixedMapping(&mappings, allocator, prefix, "embeddings.word_embeddings.weight", "embeddings/word_embeddings");
    try addPrefixedMapping(&mappings, allocator, prefix, "embeddings.position_embeddings.weight", "embeddings/position_embeddings");

    if (config.model_type != .distilbert) {
        try addPrefixedMapping(&mappings, allocator, prefix, "embeddings.token_type_embeddings.weight", "embeddings/token_type_embeddings");
    }

    try addPrefixedMapping(&mappings, allocator, prefix, "embeddings.LayerNorm.weight", "embeddings/layer_norm/gain");
    try addPrefixedMapping(&mappings, allocator, prefix, "embeddings.LayerNorm.bias", "embeddings/layer_norm/offset");

    // Encoder layers
    for (0..config.num_hidden_layers) |i| {
        if (config.model_type == .distilbert) {
            try addDistilBertLayer(&mappings, allocator, i);
        } else {
            try addBertLayer(&mappings, allocator, prefix, i);
        }
    }

    // Pooler (not present in distilbert)
    if (config.model_type != .distilbert) {
        try addPrefixedMapping(&mappings, allocator, prefix, "pooler.dense.weight", "pooler/weights");
        try addPrefixedMapping(&mappings, allocator, prefix, "pooler.dense.bias", "pooler/biases");
    }

    try addClassifierMappings(&mappings, allocator, config);

    return try mappings.toOwnedSlice(allocator);
}

/// Build a tensor name with optional prefix: "prefix.name" or just "name".
fn addPrefixedMapping(
    mappings: *std.ArrayListUnmanaged(WeightMapping),
    allocator: std.mem.Allocator,
    prefix: []const u8,
    name: []const u8,
    scope: []const u8,
) !void {
    const tensor_name = if (prefix.len > 0)
        try std.fmt.allocPrint(allocator, "{s}.{s}", .{ prefix, name })
    else
        try allocator.dupe(u8, name);
    const scope_path = try allocator.dupe(u8, scope);
    try mappings.append(allocator, .{ .tensor_name = tensor_name, .scope_path = scope_path });
}

fn addBertLayer(
    mappings: *std.ArrayListUnmanaged(WeightMapping),
    allocator: std.mem.Allocator,
    prefix: []const u8,
    layer: usize,
) !void {
    // Self-attention: query, key, value
    inline for (.{ "query", "key", "value" }) |qkv| {
        try addLayerMapping(mappings, allocator, prefix, layer, ".attention.self." ++ qkv ++ ".weight", "attention/" ++ qkv ++ "/weights");
        try addLayerMapping(mappings, allocator, prefix, layer, ".attention.self." ++ qkv ++ ".bias", "attention/" ++ qkv ++ "/biases");
    }

    // Attention output
    try addLayerMapping(mappings, allocator, prefix, layer, ".attention.output.dense.weight", "attention/output/weights");
    try addLayerMapping(mappings, allocator, prefix, layer, ".attention.output.dense.bias", "attention/output/biases");
    try addLayerMapping(mappings, allocator, prefix, layer, ".attention.output.LayerNorm.weight", "attention/layer_norm/gain");
    try addLayerMapping(mappings, allocator, prefix, layer, ".attention.output.LayerNorm.bias", "attention/layer_norm/offset");

    // Feed-forward
    try addLayerMapping(mappings, allocator, prefix, layer, ".intermediate.dense.weight", "ffn/intermediate/weights");
    try addLayerMapping(mappings, allocator, prefix, layer, ".intermediate.dense.bias", "ffn/intermediate/biases");
    try addLayerMapping(mappings, allocator, prefix, layer, ".output.dense.weight", "ffn/output/weights");
    try addLayerMapping(mappings, allocator, prefix, layer, ".output.dense.bias", "ffn/output/biases");
    try addLayerMapping(mappings, allocator, prefix, layer, ".output.LayerNorm.weight", "ffn/layer_norm/gain");
    try addLayerMapping(mappings, allocator, prefix, layer, ".output.LayerNorm.bias", "ffn/layer_norm/offset");
}

fn addDistilBertLayer(
    mappings: *std.ArrayListUnmanaged(WeightMapping),
    allocator: std.mem.Allocator,
    layer: usize,
) !void {
    try addDistilLayerMapping(mappings, allocator, layer, ".attention.q_lin.weight", "attention/query/weights");
    try addDistilLayerMapping(mappings, allocator, layer, ".attention.q_lin.bias", "attention/query/biases");
    try addDistilLayerMapping(mappings, allocator, layer, ".attention.k_lin.weight", "attention/key/weights");
    try addDistilLayerMapping(mappings, allocator, layer, ".attention.k_lin.bias", "attention/key/biases");
    try addDistilLayerMapping(mappings, allocator, layer, ".attention.v_lin.weight", "attention/value/weights");
    try addDistilLayerMapping(mappings, allocator, layer, ".attention.v_lin.bias", "attention/value/biases");

    try addDistilLayerMapping(mappings, allocator, layer, ".attention.out_lin.weight", "attention/output/weights");
    try addDistilLayerMapping(mappings, allocator, layer, ".attention.out_lin.bias", "attention/output/biases");
    try addDistilLayerMapping(mappings, allocator, layer, ".sa_layer_norm.weight", "attention/layer_norm/gain");
    try addDistilLayerMapping(mappings, allocator, layer, ".sa_layer_norm.bias", "attention/layer_norm/offset");

    try addDistilLayerMapping(mappings, allocator, layer, ".ffn.lin1.weight", "ffn/intermediate/weights");
    try addDistilLayerMapping(mappings, allocator, layer, ".ffn.lin1.bias", "ffn/intermediate/biases");
    try addDistilLayerMapping(mappings, allocator, layer, ".ffn.lin2.weight", "ffn/output/weights");
    try addDistilLayerMapping(mappings, allocator, layer, ".ffn.lin2.bias", "ffn/output/biases");
    try addDistilLayerMapping(mappings, allocator, layer, ".output_layer_norm.weight", "ffn/layer_norm/gain");
    try addDistilLayerMapping(mappings, allocator, layer, ".output_layer_norm.bias", "ffn/layer_norm/offset");
}

fn addLayerMapping(
    mappings: *std.ArrayListUnmanaged(WeightMapping),
    allocator: std.mem.Allocator,
    prefix: []const u8,
    layer: usize,
    suffix: []const u8,
    scope_suffix: []const u8,
) !void {
    const tensor_name = if (prefix.len > 0)
        try std.fmt.allocPrint(allocator, "{s}.encoder.layer.{d}{s}", .{ prefix, layer, suffix })
    else
        try std.fmt.allocPrint(allocator, "encoder.layer.{d}{s}", .{ layer, suffix });
    const scope_path = try std.fmt.allocPrint(allocator, "encoder/layer/{d}/{s}", .{ layer, scope_suffix });
    try mappings.append(allocator, .{ .tensor_name = tensor_name, .scope_path = scope_path });
}

fn addDistilLayerMapping(
    mappings: *std.ArrayListUnmanaged(WeightMapping),
    allocator: std.mem.Allocator,
    layer: usize,
    suffix: []const u8,
    scope_suffix: []const u8,
) !void {
    const tensor_name = try std.fmt.allocPrint(allocator, "distilbert.transformer.layer.{d}{s}", .{ layer, suffix });
    const scope_path = try std.fmt.allocPrint(allocator, "encoder/layer/{d}/{s}", .{ layer, scope_suffix });
    try mappings.append(allocator, .{ .tensor_name = tensor_name, .scope_path = scope_path });
}

fn addClassifierMappings(
    mappings: *std.ArrayListUnmanaged(WeightMapping),
    allocator: std.mem.Allocator,
    config: Config,
) !void {
    const prefix = config.effectivePrefix();
    switch (config.model_type) {
        .bert => {
            try addRawMapping(mappings, allocator, "classifier.weight", "classifier/weights");
            try addRawMapping(mappings, allocator, "classifier.bias", "classifier/biases");
        },
        .roberta => {
            try addRawMapping(mappings, allocator, "classifier.dense.weight", "classifier/dense/weights");
            try addRawMapping(mappings, allocator, "classifier.dense.bias", "classifier/dense/biases");
            try addRawMapping(mappings, allocator, "classifier.out_proj.weight", "classifier/out_proj/weights");
            try addRawMapping(mappings, allocator, "classifier.out_proj.bias", "classifier/out_proj/biases");
            try addRawMapping(mappings, allocator, "classifier.weight", "classifier/weights");
            try addRawMapping(mappings, allocator, "classifier.bias", "classifier/biases");
        },
        .distilbert => {
            try addRawMapping(mappings, allocator, "pre_classifier.weight", "pre_classifier/weights");
            try addRawMapping(mappings, allocator, "pre_classifier.bias", "pre_classifier/biases");
            try addRawMapping(mappings, allocator, "classifier.weight", "classifier/weights");
            try addRawMapping(mappings, allocator, "classifier.bias", "classifier/biases");
        },
    }

    if (prefix.len > 0) {
        try addPrefixedMapping(mappings, allocator, prefix, "classifier.weight", "prefixed_classifier/weights");
        try addPrefixedMapping(mappings, allocator, prefix, "classifier.bias", "prefixed_classifier/biases");
    }
}

fn addRawMapping(
    mappings: *std.ArrayListUnmanaged(WeightMapping),
    allocator: std.mem.Allocator,
    tensor_name: []const u8,
    scope_path: []const u8,
) !void {
    try mappings.append(allocator, .{
        .tensor_name = try allocator.dupe(u8, tensor_name),
        .scope_path = try allocator.dupe(u8, scope_path),
    });
}

/// Free a weight mapping slice returned by weightMapping().
pub fn freeWeightMapping(allocator: std.mem.Allocator, mapping: []WeightMapping) void {
    for (mapping) |entry| {
        allocator.free(entry.tensor_name);
        allocator.free(entry.scope_path);
    }
    allocator.free(mapping);
}

// -- Tests --

test "parse config" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{"model_type": "bert", "vocab_size": 30522, "hidden_size": 768, "num_hidden_layers": 12, "num_attention_heads": 12, "intermediate_size": 3072}
    ;
    const config = try parseConfig(allocator, json_str);
    try std.testing.expectEqual(ModelType.bert, config.model_type);
    try std.testing.expectEqual(@as(u32, 768), config.hidden_size);
    try std.testing.expectEqual(@as(u32, 12), config.num_hidden_layers);
}

test "parse roberta config" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{"model_type": "xlm-roberta", "hidden_size": 384, "num_hidden_layers": 6}
    ;
    const config = try parseConfig(allocator, json_str);
    try std.testing.expectEqual(ModelType.roberta, config.model_type);
    try std.testing.expectEqual(@as(u32, 384), config.hidden_size);
    try std.testing.expectEqual(@as(u32, 6), config.num_hidden_layers);
}

test "weight mapping for bert" {
    const allocator = std.testing.allocator;
    const config = Config{ .model_type = .bert, .num_hidden_layers = 2 };
    const mapping = try weightMapping(allocator, config);
    defer freeWeightMapping(allocator, mapping);

    // 5 embedding + 2*16 layer + 2 pooler + 4 classifier = 43 mappings
    try std.testing.expectEqual(@as(usize, 43), mapping.len);
    try std.testing.expectEqualStrings("bert.embeddings.word_embeddings.weight", mapping[0].tensor_name);
    try std.testing.expectEqualStrings("embeddings/word_embeddings", mapping[0].scope_path);
}

test "weight mapping for distilbert" {
    const allocator = std.testing.allocator;
    const config = Config{ .model_type = .distilbert, .num_hidden_layers = 2 };
    const mapping = try weightMapping(allocator, config);
    defer freeWeightMapping(allocator, mapping);

    // 4 embedding (no token_type) + 2*16 layer + 0 pooler + 6 classifier = 42 mappings
    try std.testing.expectEqual(@as(usize, 42), mapping.len);
    try std.testing.expectEqualStrings("distilbert.embeddings.word_embeddings.weight", mapping[0].tensor_name);
}

test "weight mapping for roberta includes classifier head" {
    const allocator = std.testing.allocator;
    const config = Config{ .model_type = .roberta, .num_hidden_layers = 1 };
    const mapping = try weightMapping(allocator, config);
    defer freeWeightMapping(allocator, mapping);

    var found_dense = false;
    var found_out_proj = false;
    for (mapping) |entry| {
        if (std.mem.eql(u8, entry.tensor_name, "classifier.dense.weight")) found_dense = true;
        if (std.mem.eql(u8, entry.tensor_name, "classifier.out_proj.weight")) found_out_proj = true;
    }
    try std.testing.expect(found_dense);
    try std.testing.expect(found_out_proj);
}

test "weight mapping with no prefix" {
    const allocator = std.testing.allocator;
    const config = Config{ .model_type = .bert, .weight_prefix = "", .num_hidden_layers = 1 };
    const mapping = try weightMapping(allocator, config);
    defer freeWeightMapping(allocator, mapping);

    // No "bert." prefix
    try std.testing.expectEqualStrings("embeddings.word_embeddings.weight", mapping[0].tensor_name);
    // Layer mapping also has no prefix
    for (mapping) |m| {
        if (std.mem.startsWith(u8, m.scope_path, "encoder/layer/0/attention/query")) {
            try std.testing.expect(!std.mem.startsWith(u8, m.tensor_name, "bert."));
            break;
        }
    }
}

test "parse bert gguf metadata" {
    const allocator = std.testing.allocator;
    const format = @import("../gguf/format.zig");
    const writer = @import("../gguf/writer.zig");

    const metadata = [_]format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "bert" } },
        .{ .key = "bert.family", .value = .{ .string = "distilbert" } },
        .{ .key = "bert.vocab_size", .value = .{ .u32 = 30522 } },
        .{ .key = "bert.embedding_length", .value = .{ .u32 = 384 } },
        .{ .key = "bert.block_count", .value = .{ .u32 = 6 } },
        .{ .key = "bert.attention.head_count", .value = .{ .u32 = 6 } },
        .{ .key = "bert.feed_forward_length", .value = .{ .u32 = 1536 } },
        .{ .key = "bert.context_length", .value = .{ .u32 = 512 } },
        .{ .key = "bert.token_type_count", .value = .{ .u32 = 2 } },
        .{ .key = "bert.label_count", .value = .{ .u32 = 3 } },
        .{ .key = "bert.hidden_act", .value = .{ .string = "gelu" } },
    };
    var layout = try writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    var parsed = try format.parse(allocator, layout.header_bytes);
    defer parsed.deinit(allocator);
    const view = gguf_metadata.View.init(&parsed);
    const config = parseGgufMetadata(view).?;
    try std.testing.expectEqual(ModelType.distilbert, config.model_type);
    try std.testing.expectEqualStrings("", config.effectivePrefix());
    try std.testing.expectEqual(@as(u32, 384), config.hidden_size);
    try std.testing.expectEqual(@as(u32, 3), config.num_labels);
}
