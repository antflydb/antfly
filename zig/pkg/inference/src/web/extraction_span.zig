// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! GLiNER2 browser adapter: shared tokenizer, prompt construction and decoders.
const std = @import("std");
const WasmCompute = @import("../ops/wasm_compute.zig").WasmCompute;
const deberta = @import("../models/deberta.zig");
const encoder = @import("../architectures/deberta.zig");
const head = @import("../architectures/gliner_head.zig");
const gliner = @import("../pipelines/gliner.zig");
const extraction = @import("../pipelines/extraction.zig");
const backends = @import("../backends/backends.zig");
const Tensor = backends.Tensor;
const TensorInfo = @import("../backends/tensor.zig").TensorInfo;
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Config = struct { encoder: deberta.Config, max_width: u32 = 8 };
const inventory = @import("extraction_span_inventory.zig");

pub fn validateWeight(name: []const u8, shape: []const i64) !void {
    for (inventory.tensors) |spec| {
        if (!std.mem.eql(u8, spec.name, name)) continue;
        if (!std.mem.eql(i64, spec.shape, shape)) return error.InvalidGlinerTensorShape;
        return;
    }
    return error.UnexpectedGlinerTensor;
}

pub fn parseConfig(a: std.mem.Allocator, json: []const u8, encoder_json: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(struct { max_width: u32 = 8, model_name: []const u8 }, a, json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (!std.mem.eql(u8, parsed.value.model_name, "microsoft/deberta-v3-base") or parsed.value.max_width == 0 or parsed.value.max_width > 32) return error.UnsupportedGlinerConfig;
    const cfg = try deberta.parseConfig(a, encoder_json);
    if (cfg.hidden_size != 768 or cfg.num_hidden_layers != 12 or cfg.num_attention_heads != 12 or cfg.intermediate_size != 3072 or cfg.vocab_size != 128011) return error.UnsupportedGlinerConfig;
    return .{ .encoder = cfg, .max_width = parsed.value.max_width };
}
pub fn validateWeights(compute: *WasmCompute, _: Config) !void {
    if (compute.weights.count() != inventory.tensors.len) return error.IncompleteGlinerTensorInventory;
    for (inventory.tensors) |spec| if (!compute.weights.contains(spec.name)) return error.IncompleteGlinerTensorInventory;
}
const Adapter = struct {
    compute: *WasmCompute,
    config: deberta.Config,
    fn run(raw: *anyopaque, inputs: []const Tensor, a: std.mem.Allocator) anyerror![]Tensor {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        if (inputs.len < 4 or inputs[0].shape.len != 2) return error.InvalidInputShape;
        const batch: usize = @intCast(inputs[0].shape[0]);
        const seq: usize = @intCast(inputs[0].shape[1]);
        if (batch != 1 or seq > @min(2048, self.config.max_position_embeddings)) return error.ExtractionTokenLimitExceeded;
        var cb = self.compute.computeBackend();
        const hidden = try encoder.forwardCt(&cb, a, self.config, inputs[0].asInt64(), inputs[1].asInt64(), batch, seq, true);
        defer cb.free(hidden);
        const out = try head.forwardCtWithLabelMarkers(&cb, a, hidden, inputs[0].asInt64(), inputs[2].asInt64(), inputs[3].asInt64(), batch, seq, self.config.hidden_size, .{ .classification = self.config.classification_token_id, .entity = self.config.entity_token_id, .relation = self.config.relation_token_id });
        defer cb.free(out.logits);
        const values = try cb.toFloat32(out.logits, a);
        defer a.free(values);
        const tensors = try a.alloc(Tensor, 1);
        errdefer a.free(tensors);
        tensors[0] = try Tensor.initFloat32(a, "logits", &.{ @intCast(batch), @intCast(out.num_words), @intCast(out.max_width), @intCast(out.num_labels) }, values);
        return tensors;
    }
    fn controlled(raw: *anyopaque, inputs: []const Tensor, a: std.mem.Allocator, control: Control) anyerror![]Tensor {
        try control.check();
        return Adapter.run(raw, inputs, a);
    }
    fn inputInfo(_: *anyopaque) []const TensorInfo {
        return &.{ .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } }, .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } }, .{ .name = "words_mask", .dtype = .i64, .shape = &.{ -1, -1 } }, .{ .name = "span_idx", .dtype = .i64, .shape = &.{ -1, -1, 2 } } };
    }
    fn outputInfo(_: *anyopaque) []const TensorInfo {
        return &.{.{ .name = "logits", .dtype = .f32, .shape = &.{ -1, -1, -1, -1 } }};
    }
    fn backend(_: *anyopaque) backends.BackendType {
        return .wasm;
    }
    fn close(_: *anyopaque) void {}
    fn session(self: *Adapter) backends.Session {
        return .{ .ptr = self, .vtable = &.{ .run = Adapter.run, .runWithControl = controlled, .inputInfo = inputInfo, .outputInfo = outputInfo, .backend = backend, .close = close } };
    }
};

// Browser v1 envelope uses the native GLiNER2 schema vocabulary. v2 requests
// are never coerced into legacy spans; unsupported fields fail closed.
const Request = struct {
    schema_version: u32 = 1,
    model: []const u8,
    text: []const u8,
    task: enum { entities, classification, structures, relations } = .entities,
    labels: []const []const u8 = &.{},
    relation_labels: []const []const u8 = &.{},
    schema: std.json.ArrayHashMap([]const []const u8) = .{},
    threshold: f32 = 0.5,
    flat_ner: bool = true,
    multi_label: bool = false,
};
pub fn run(a: std.mem.Allocator, compute: *WasmCompute, config: Config, tok: Tokenizer, json: []const u8, validate_only: bool) ![]u8 {
    const parsed = try std.json.parseFromSlice(Request, a, json, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    const req = parsed.value;
    if (req.schema_version != 1) return error.UnsupportedExtractionSchemaVersion;
    if (req.text.len > 256 * 1024 or req.labels.len > 128 or req.relation_labels.len > 64) return error.ExtractionRequestLimitExceeded;
    if (!std.math.isFinite(req.threshold) or req.threshold < 0 or req.threshold > 1) return error.InvalidExtractionThreshold;
    const schema_json = try std.json.Stringify.valueAlloc(a, .{ .labels = req.labels, .relation_labels = req.relation_labels, .schema = req.schema }, .{});
    defer a.free(schema_json);
    if (schema_json.len > 64 * 1024) return error.ExtractionSchemaLimitExceeded;
    const schemas: []extraction.ExtractionSchema = if (req.task == .structures) try extraction.parseSchemas(a, &req.schema) else &.{};
    defer if (req.task == .structures) {
        for (schemas) |*s| s.deinit(a);
        a.free(schemas);
    };
    var adapter = Adapter{ .compute = compute, .config = config.encoder };
    var pipeline = gliner.GlinerPipeline{ .allocator = a, .session = adapter.session(), .tok = tok, .config = .{
        .model_type = "gliner2",
        .max_width = config.max_width,
        .max_length = config.encoder.max_position_embeddings,
        .threshold = req.threshold,
        .flat_ner = req.flat_ner,
        .token_p = 128003,
        .token_c = 128004,
        .token_e = 128005,
        .token_r = 128006,
        .token_sep_text = 128002,
    } };
    const texts = &[_][]const u8{req.text};
    const ceiling = @min(2048, config.encoder.max_position_embeddings);
    const tokens = switch (req.task) {
        .entities, .relations => try pipeline.maxExtractionInputTokens(texts, req.labels, if (req.task == .relations) req.relation_labels else &.{}),
        .classification => try pipeline.maxClassificationInputTokens(texts, req.labels),
        .structures => blk: {
            if (schemas.len == 0) return error.InvalidExtractionSchema;
            var largest: usize = 0;
            for (schemas) |schema| {
                const names = try a.alloc([]const u8, schema.fields.len);
                defer a.free(names);
                for (schema.fields, names) |field, *name| name.* = field.name;
                largest = @max(largest, try pipeline.maxExtractionInputTokens(texts, names, &.{}));
            }
            break :blk largest;
        },
    };
    if (tokens > ceiling) return error.ExtractionTokenLimitExceeded;
    if (validate_only) {
        return std.json.Stringify.valueAlloc(a, .{ .valid = true, .encoded_tokens = tokens }, .{});
    }
    const response = switch (req.task) {
        .entities => blk: {
            const rows = try pipeline.recognizeBatch(texts, req.labels);
            defer {
                for (rows) |row| {
                    for (row) |entity| a.free(entity.text);
                    a.free(row);
                }
                a.free(rows);
            }
            break :blk try std.json.Stringify.valueAlloc(a, .{ .schema_version = 1, .offset_unit = "utf8_bytes", .entities = rows[0] }, .{});
        },
        .classification => blk: {
            const rows = try pipeline.classifyBatch(texts, req.labels, .{ .threshold = req.threshold, .multi_label = req.multi_label });
            defer {
                for (rows) |row| a.free(row);
                a.free(rows);
            }
            break :blk try std.json.Stringify.valueAlloc(a, .{ .schema_version = 1, .classifications = rows[0] }, .{});
        },
        .relations => blk: {
            const rows = try pipeline.extractRelationsBatch(texts, req.labels, req.relation_labels);
            defer {
                for (rows.entities) |row| {
                    for (row) |entity| a.free(entity.text);
                    a.free(row);
                }
                a.free(rows.entities);
                for (rows.relations) |row| {
                    for (row) |*relation| relation.deinit(a);
                    a.free(row);
                }
                a.free(rows.relations);
            }
            break :blk try std.json.Stringify.valueAlloc(a, .{ .schema_version = 1, .offset_unit = "utf8_bytes", .entities = rows.entities[0], .relations = rows.relations[0] }, .{});
        },
        .structures => blk: {
            const rows = try extraction.extractBatch(a, &pipeline, texts, schemas, .{ .threshold = req.threshold, .flat_ner = req.flat_ner, .include_confidence = true, .include_spans = true, .max_input_tokens_per_item = ceiling });
            defer {
                for (rows) |*row| row.deinit(a);
                a.free(rows);
            }
            break :blk try std.json.Stringify.valueAlloc(a, .{ .schema_version = 1, .offset_unit = "utf8_bytes", .structures = rows[0].structures }, .{});
        },
    };
    errdefer a.free(response);
    if (response.len > 4 * 1024 * 1024) return error.ExtractionResponseLimitExceeded;
    return response;
}
