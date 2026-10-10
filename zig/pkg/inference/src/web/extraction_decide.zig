// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
//! GLiNER2.5-Decide geometry and streamed inventory. Execution uses main's
//! schema-v2 prompt planner and marker classifier, not the legacy span head.
const std = @import("std");
const deberta = @import("../models/deberta.zig");
const contract = @import("../models/gliner_boundary.zig");
const WasmCompute = @import("../ops/wasm_compute.zig").WasmCompute;
const modern = @import("../architectures/modern_bert.zig");
const span = @import("../extractors/gliner_span_v2_executor.zig");
const access = @import("../models/tensor_access.zig");
pub const Config = span.EncoderConfig;

pub fn parseConfig(a: std.mem.Allocator, json: []const u8, encoder_json: []const u8) !Config {
    if (!try contract.declaresSpanArchitecture(a, json)) return error.UnsupportedGlinerDecideConfig;
    const root = try std.json.parseFromSlice(std.json.Value, a, encoder_json, .{});
    defer root.deinit();
    const model_type = if (root.value == .object) root.value.object.get("model_type") else null;
    if (model_type != null and model_type.? == .string and modern.isModernBertModel(model_type.?.string)) {
        const cfg = try modern.parseConfig(a, encoder_json);
        if (cfg.checkpoint_layout != .huggingface_fused_qkv_no_bias or cfg.hidden_size > 2048 or cfg.num_hidden_layers > 48 or cfg.intermediate_size > 8192 or cfg.vocab_size > 262144) return error.UnsupportedGlinerDecideConfig;
        return .{ .modern_bert = cfg };
    }
    const parsed = try std.json.parseFromSlice(struct { model_name: []const u8, counting_layer: []const u8, token_pooling: []const u8, use_moe: bool }, a, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const wrapper = parsed.value;
    if (!std.mem.eql(u8, wrapper.model_name, "microsoft/deberta-v3-large") or !std.mem.eql(u8, wrapper.counting_layer, "count_lstm") or !std.mem.eql(u8, wrapper.token_pooling, "first") or wrapper.use_moe) return error.UnsupportedGlinerDecideConfig;
    var cfg = try deberta.parseConfig(a, encoder_json);
    if (cfg.hidden_size != 1024 or cfg.num_hidden_layers != 24 or cfg.num_attention_heads != 16 or cfg.intermediate_size != 4096 or cfg.vocab_size != 128011 or cfg.max_position_embeddings != 512 or cfg.position_buckets != 256 or !std.math.isFinite(cfg.layer_norm_eps) or cfg.layer_norm_eps <= 0) return error.UnsupportedGlinerDecideConfig;
    cfg.label_marker_decision_head = true;
    cfg.gliner_count_layer = .count_lstm;
    return .{ .deberta = cfg };
}

const Spec = struct { name: []const u8, shape: []const i64 };
const roots = [_]Spec{
    .{ .name = "embeddings.word_embeddings.weight", .shape = &.{ 128011, 1024 } },
    .{ .name = "embeddings.LayerNorm.weight", .shape = &.{1024} },
    .{ .name = "embeddings.LayerNorm.bias", .shape = &.{1024} },
    .{ .name = "encoder.rel_embeddings.weight", .shape = &.{ 512, 1024 } },
    .{ .name = "encoder.LayerNorm.weight", .shape = &.{1024} },
    .{ .name = "encoder.LayerNorm.bias", .shape = &.{1024} },
    .{ .name = "classifier.0.weight", .shape = &.{ 2048, 1024 } },
    .{ .name = "classifier.0.bias", .shape = &.{2048} },
    .{ .name = "classifier.2.weight", .shape = &.{ 1, 2048 } },
    .{ .name = "classifier.2.bias", .shape = &.{1} },
    .{ .name = "count_embed.gru.weight_ih_l0", .shape = &.{ 3072, 1024 } },
    .{ .name = "count_embed.gru.weight_hh_l0", .shape = &.{ 3072, 1024 } },
    .{ .name = "count_embed.gru.bias_ih_l0", .shape = &.{3072} },
    .{ .name = "count_embed.gru.bias_hh_l0", .shape = &.{3072} },
    .{ .name = "count_embed.pos_embedding.weight", .shape = &.{ 20, 1024 } },
    .{ .name = "count_embed.projector.0.weight", .shape = &.{ 4096, 2048 } },
    .{ .name = "count_embed.projector.0.bias", .shape = &.{4096} },
    .{ .name = "count_embed.projector.2.weight", .shape = &.{ 1024, 4096 } },
    .{ .name = "count_embed.projector.2.bias", .shape = &.{1024} },
    .{ .name = "count_pred.0.weight", .shape = &.{ 2048, 1024 } },
    .{ .name = "count_pred.0.bias", .shape = &.{2048} },
    .{ .name = "count_pred.2.weight", .shape = &.{ 20, 2048 } },
    .{ .name = "count_pred.2.bias", .shape = &.{20} },
};
const layer_specs = [_]Spec{
    .{ .name = "attention.self.query_proj.weight", .shape = &.{ 1024, 1024 } },
    .{ .name = "attention.self.query_proj.bias", .shape = &.{1024} },
    .{ .name = "attention.self.key_proj.weight", .shape = &.{ 1024, 1024 } },
    .{ .name = "attention.self.key_proj.bias", .shape = &.{1024} },
    .{ .name = "attention.self.value_proj.weight", .shape = &.{ 1024, 1024 } },
    .{ .name = "attention.self.value_proj.bias", .shape = &.{1024} },
    .{ .name = "attention.output.dense.weight", .shape = &.{ 1024, 1024 } },
    .{ .name = "attention.output.dense.bias", .shape = &.{1024} },
    .{ .name = "attention.output.LayerNorm.weight", .shape = &.{1024} },
    .{ .name = "attention.output.LayerNorm.bias", .shape = &.{1024} },
    .{ .name = "intermediate.dense.weight", .shape = &.{ 4096, 1024 } },
    .{ .name = "intermediate.dense.bias", .shape = &.{4096} },
    .{ .name = "output.dense.weight", .shape = &.{ 1024, 4096 } },
    .{ .name = "output.dense.bias", .shape = &.{1024} },
    .{ .name = "output.LayerNorm.weight", .shape = &.{1024} },
    .{ .name = "output.LayerNorm.bias", .shape = &.{1024} },
};

pub fn validateWeight(cfg: Config, name: []const u8, shape: []const i64) !void {
    if (cfg == .modern_bert) return validateModernWeight(cfg.modern_bert, name, shape);
    for (roots) |spec| if (std.mem.eql(u8, spec.name, name)) {
        if (!std.mem.eql(i64, spec.shape, shape)) return error.InvalidGlinerTensorShape;
        return;
    };
    var buffer: [128]u8 = undefined;
    for (0..24) |layer| for (layer_specs) |spec| {
        const expected = try std.fmt.bufPrint(&buffer, "encoder.layer.{d}.{s}", .{ layer, spec.name });
        if (std.mem.eql(u8, expected, name)) {
            if (!std.mem.eql(i64, spec.shape, shape)) return error.InvalidGlinerTensorShape;
            return;
        }
    };
    // The released checkpoint also contains the legacy span projectors.
    for ([_][]const u8{ "project_start", "project_end", "out_project" }) |projector| {
        const input: i64 = if (std.mem.eql(u8, projector, "out_project")) 2048 else 1024;
        for ([_]Spec{
            .{ .name = "0.weight", .shape = &.{ 4096, input } },
            .{ .name = "0.bias", .shape = &.{4096} },
            .{ .name = "3.weight", .shape = &.{ 1024, 4096 } },
            .{ .name = "3.bias", .shape = &.{1024} },
        }) |spec| {
            const expected = try std.fmt.bufPrint(&buffer, "span_rep.span_rep_layer.{s}.{s}", .{ projector, spec.name });
            if (std.mem.eql(u8, expected, name)) {
                if (!std.mem.eql(i64, spec.shape, shape)) return error.InvalidGlinerTensorShape;
                return;
            }
        }
    }
    return error.UnexpectedGlinerTensor;
}

pub fn validateWeights(cfg: Config, compute: *WasmCompute) !void {
    if (cfg == .modern_bert) {
        // ModernBERT marker decisions consume the encoder plus four classifier
        // tensors. Legacy count/span heads are not part of this execution ABI.
        if (compute.weights.count() != 6 * @as(usize, cfg.modern_bert.num_hidden_layers) + 6) return error.IncompleteGlinerTensorInventory;
        return;
    }
    // Registration validates each name/shape and rejects duplicate names.
    if (compute.weights.count() != roots.len + 24 * layer_specs.len + 12) return error.IncompleteGlinerTensorInventory;
}

fn validateModernWeight(cfg: modern.Config, name: []const u8, shape: []const i64) !void {
    const h: i64 = cfg.hidden_size;
    const f: i64 = cfg.intermediate_size;
    const globals = [_]Spec{
        .{ .name = "embeddings.tok_embeddings.weight", .shape = &.{ cfg.vocab_size, h } },
        .{ .name = "embeddings.norm.weight", .shape = &.{h} },
        .{ .name = "final_norm.weight", .shape = &.{h} },
        .{ .name = "classifier.0.weight", .shape = &.{ 2 * h, h } },
        .{ .name = "classifier.0.bias", .shape = &.{2 * h} },
        .{ .name = "classifier.2.weight", .shape = &.{ 1, 2 * h } },
        .{ .name = "classifier.2.bias", .shape = &.{1} },
    };
    for (globals) |spec| if (std.mem.eql(u8, name, spec.name)) {
        if (!std.mem.eql(i64, shape, spec.shape)) return error.InvalidGlinerTensorShape;
        return;
    };
    const layers = [_]Spec{
        .{ .name = "attn_norm.weight", .shape = &.{h} },
        .{ .name = "attn.Wqkv.weight", .shape = &.{ 3 * h, h } },
        .{ .name = "attn.Wo.weight", .shape = &.{ h, h } },
        .{ .name = "mlp_norm.weight", .shape = &.{h} },
        .{ .name = "mlp.Wi.weight", .shape = &.{ 2 * f, h } },
        .{ .name = "mlp.Wo.weight", .shape = &.{ h, f } },
    };
    var buf: [128]u8 = undefined;
    for (0..cfg.num_hidden_layers) |layer| for (layers) |spec| {
        if (layer == 0 and std.mem.eql(u8, spec.name, "attn_norm.weight")) continue;
        const expected = try std.fmt.bufPrint(&buf, "layers.{d}.{s}", .{ layer, spec.name });
        if (std.mem.eql(u8, name, expected)) {
            if (!std.mem.eql(i64, shape, spec.shape)) return error.InvalidGlinerTensorShape;
            return;
        }
    };
    return error.UnexpectedGlinerTensor;
}
