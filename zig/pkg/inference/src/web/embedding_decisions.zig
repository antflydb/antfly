// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
//! Text decisions on the shared EmbeddingGemma2 encoder. Similarities remain
//! cosine scores; calibration, identity and prototypes use native contracts.
const std = @import("std");
const api = @import("antfly_decisions");
const encoder = @import("../architectures/embedding_gemma2.zig");
const pipeline = @import("../pipelines/embedding_gemma2.zig");
const calibration = @import("../extractors/embedding_calibration.zig");
const prototypes = @import("../extractors/embedding_prototypes.zig");
const access = @import("../models/tensor_access.zig");
const DType = @import("../backends/tensor.zig").DType;
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const WasmCompute = @import("../ops/wasm_compute.zig").WasmCompute;
const V = std.json.Value;
const A = std.mem.Allocator;

pub const DescriptorStore = struct {
    descriptors: []const access.Descriptor,
    pub fn kind(_: @This()) @import("../models/tensor_store.zig").StoreKind {
        return .safetensors;
    }
    pub const Ref = struct {
        shape: []const i64,
        dtype: DType,
        pub fn deinit(_: *@This(), _: A) void {}
    };
    pub fn describeTensorRange(self: @This(), _: A, name: []const u8) !?Ref {
        for (self.descriptors) |d| if (std.mem.eql(u8, d.name, name)) {
            const dtype: DType = switch (d.encoding.gguf.known) {
                .F32 => .f32,
                .BF16 => .bf16,
                else => return error.UnsupportedEmbeddingGemma2Precision,
            };
            return .{ .shape = d.shape, .dtype = dtype };
        };
        return null;
    }
};
pub fn validateWeights(a: A, cfg: encoder.Config, descriptors: []const access.Descriptor) !void {
    if (descriptors.len != 5 + cfg.num_hidden_layers * 17) return error.InvalidEmbeddingGemma2Weight;
    try encoder.validateWeights(a, DescriptorStore{ .descriptors = descriptors }, cfg);
}

fn put(a: A, object: *std.json.ObjectMap, key: []const u8, value: V) !void {
    try object.put(a, key, value);
}
fn string(s: []const u8) V {
    return .{ .string = s };
}
fn array(a: A, values: []const V) !V {
    var out: std.array_list.Managed(V) = .init(a);
    try out.appendSlice(values);
    return .{ .array = out };
}

const Embedder = struct {
    backing: A,
    request: A,
    compute: *WasmCompute,
    cfg: encoder.Config,
    tok: Tokenizer,
    validate_only: bool,
    input_tokens: usize = 0,
    encoded_texts: usize = 0,
    fn vector(self: *@This(), text: []const u8, options: api.scoring.Options) ![]f32 {
        self.encoded_texts += 1;
        if (self.encoded_texts > 128 or text.len > 256 * 1024) return error.DecideRequestLimitExceeded;
        const rendered = try std.fmt.allocPrint(self.request, "{s}{s}", .{ try encoder.taskPrefix(options.task_type), text });
        try pipeline.validateGroup(.{ .content = &.{.{ .text = text }} }, .{ .task_type = options.task_type, .dimensions = options.dimensions });
        var tokens = try self.tok.encodeForModel(self.request, rendered, 2049);
        defer tokens.deinit();
        var count: usize = 0;
        for (tokens.attention_mask) |m| count += @intFromBool(m != 0);
        const encoded = tokens.ids[0..count];
        // Never truncate a decision input or prototype to fit the browser.
        if (encoded.len == 0 or encoded.len > 2048 or self.input_tokens + encoded.len > 8192) return error.DecideRequestLimitExceeded;
        self.input_tokens += encoded.len;
        if (self.validate_only) {
            const out = try self.request.alloc(f32, options.dimensions);
            @memset(out, 0);
            out[0] = 1;
            return out;
        }
        const ids = try self.request.alloc(i64, encoded.len);
        for (encoded, ids) |id, *out| out.* = id;
        const mask = try self.request.alloc(i64, ids.len);
        @memset(mask, 1);
        var cb = self.compute.computeBackend();
        return pipeline.preparedText(&cb, self.backing, self.cfg, ids, mask, options.dimensions);
    }
    fn freeVector(self: *@This(), values: []f32) void {
        if (!self.validate_only) self.backing.free(values);
    }
};

pub fn run(backing: A, compute: *WasmCompute, cfg: encoder.Config, tok: Tokenizer, request: api.Request, identity: []const u8, calibrations_json: []const u8, validate_only: bool) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    if (request.items.len != 1 or request.inner.questions.len > 16) return error.DecideRequestLimitExceeded;
    if (request.inner.model_identity) |expected| if (!std.mem.eql(u8, identity, expected)) return error.EmbeddingIdentityMismatch;
    const calibrations = try std.json.parseFromSlice(V, a, calibrations_json, .{ .duplicate_field_behavior = .@"error" });
    if (calibrations.value != .object) return error.InvalidEmbeddingCalibration;
    var embedder: Embedder = .{ .backing = backing, .request = a, .compute = compute, .cfg = cfg, .tok = tok, .validate_only = validate_only };
    var answers: std.array_list.Managed(V) = .init(a);
    for (request.inner.questions, request.policies) |question, policy| {
        if (policy.kind != .choice and policy.kind != .multi_choice) return error.UnsupportedEmbeddingDecisionKind;
        const mode = if (policy.kind == .multi_choice) "multi" else "single";
        const acceptance: calibration.Policy = if (policy.options.calibration_id) |id|
            try calibration.parse(a, calibrations.value.object.get(id) orelse return error.InvalidEmbeddingCalibration, identity, question, policy.options, mode)
        else
            .{ .options = policy.options };
        const thresholds = policy.thresholds orelse acceptance.thresholds;
        if (policy.kind == .multi_choice and thresholds == null) return error.EmbeddingMultiLabelThresholdRequired;
        const input = try embedder.vector(try api.scoring.renderInput(a, question.instructions, request.items[0].input), policy.options);
        defer embedder.freeVector(input);
        const scores = try a.alloc(f64, question.labels.len);
        for (question.descriptions, scores, 0..) |description, *score, index| {
            const examples = if (question.examples.len == 0) &.{} else question.examples[index];
            const vectors = try a.alloc([]const f32, @max(examples.len, 1));
            var initialized: usize = 0;
            defer for (vectors[0..initialized]) |vector| embedder.freeVector(@constCast(vector));
            for (vectors, 0..) |*vector, i| {
                const rendered = if (examples.len == 0) try api.scoring.renderCategory(a, question.instructions, description) else try api.scoring.renderInput(a, question.instructions, examples[i]);
                vector.* = try embedder.vector(rendered, policy.options);
                initialized += 1;
            }
            const prototype = try api.scoring.centroid(a, vectors, policy.options.dimensions);
            score.* = try api.scoring.cosine(input, prototype, policy.options.dimensions);
        }
        if (validate_only) continue;
        var answer = std.json.ObjectMap{};
        try put(a, &answer, "name", string(question.name));
        try put(a, &answer, "type", string(@tagName(policy.kind)));
        try put(a, &answer, "decision_method", string("embedding_similarity"));
        try put(a, &answer, "similarity_metric", string("cosine"));
        try put(a, &answer, "prototype_set_hash", string(try a.dupe(u8, &try prototypes.prototypeSetHash(a, question, policy.options, mode))));
        if (policy.options.calibration_id) |id| try put(a, &answer, "calibration_id", string(id));
        var similarities: std.array_list.Managed(V) = .init(a);
        for (question.labels, scores) |label, score| {
            var item = std.json.ObjectMap{};
            try put(a, &item, "value", string(label));
            try put(a, &item, "similarity", .{ .float = score });
            try similarities.append(.{ .object = item });
        }
        try put(a, &answer, "similarities", .{ .array = similarities });
        if (policy.kind == .choice) {
            const selection = try api.scoring.select(scores, acceptance.options);
            try put(a, &answer, "choice", if (selection.selected) |i| string(question.labels[i]) else .null);
            try put(a, &answer, "margin", .{ .float = selection.margin });
            try put(a, &answer, "status", string(if (selection.selected != null) "selected" else "abstained"));
            if (selection.reason) |reason| try put(a, &answer, "abstention_reason", string(reason));
        } else {
            const selection = try api.scoring.selectMulti(a, scores, thresholds.?, policy.options.min_margin);
            var choices: std.array_list.Managed(V) = .init(a);
            for (selection.indices) |index| try choices.append(string(question.labels[index]));
            var threshold_map = std.json.ObjectMap{};
            for (question.labels, thresholds.?) |label, threshold| try put(a, &threshold_map, label, .{ .float = threshold });
            try put(a, &answer, "choices", .{ .array = choices });
            try put(a, &answer, "similarity_thresholds", .{ .object = threshold_map });
            try put(a, &answer, "margin", .{ .float = selection.margin });
            try put(a, &answer, "status", string(selection.status));
            if (selection.reason) |reason| try put(a, &answer, "abstention_reason", string(reason));
        }
        try answers.append(.{ .object = answer });
    }
    if (validate_only) return std.json.Stringify.valueAlloc(backing, .{ .valid = true, .encoded_tokens = embedder.input_tokens }, .{});
    var root = std.json.ObjectMap{};
    try put(a, &root, "model", string(request.inner.model));
    try put(a, &root, "model_identity", string(identity));
    try put(a, &root, "renderer_version", string(api.scoring.renderer_version));
    var usage = std.json.ObjectMap{};
    try put(a, &usage, "input_tokens", .{ .integer = @intCast(embedder.input_tokens) });
    try put(a, &usage, "output_tokens", .{ .integer = 0 });
    try put(a, &root, "usage", .{ .object = usage });
    if (request.batched) {
        var row = std.json.ObjectMap{};
        try put(a, &row, "input_index", .{ .integer = 0 });
        if (request.items[0].id) |id| try put(a, &row, "id", string(id));
        try put(a, &row, "answers", .{ .array = answers });
        try put(a, &root, "data", try array(a, &.{.{ .object = row }}));
    } else try put(a, &root, "answers", .{ .array = answers });
    return std.json.Stringify.valueAlloc(backing, V{ .object = root }, .{});
}
