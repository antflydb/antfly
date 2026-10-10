// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
//! Browser adapter for the shared Laya preprocessing, packed rows and heads.
const std = @import("std");
const encoder = @import("../architectures/modern_bert.zig");
const head = @import("../architectures/laya_head.zig");
const packed_rows = @import("../architectures/laya_packed.zig");
const tree = @import("../pipelines/laya_tree.zig");
const types = @import("../gguf/tensor_types.zig");
const quant = @import("../gguf/quant_codec.zig");
const Cache = @import("../architectures/laya_trunk_cache.zig").Cache;
const model = @import("../models/laya.zig");
const pipeline = @import("../pipelines/laya.zig");
const wire = @import("../extractors/laya.zig");
const access = @import("../models/tensor_access.zig");
const WasmCompute = @import("../ops/wasm_compute.zig").WasmCompute;
const backends = @import("../backends/backends.zig");
const Tensor = backends.Tensor;
const TensorInfo = @import("../backends/tensor.zig").TensorInfo;
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Config = encoder.Config;

pub fn parseConfig(a: std.mem.Allocator, json: []const u8, precision: []const u8) !Config {
    if (!std.mem.eql(u8, precision, "fp16") and !std.mem.eql(u8, precision, "fp32") and !std.mem.eql(u8, precision, "bf16")) return error.UnsupportedLayaArtifact;
    const cfg = try encoder.parseConfig(a, json);
    const laya = cfg.laya orelse return error.InvalidLayaConfig;
    if (cfg.global_attn_every_n_layers == 0 or cfg.local_attention_window == 0 or cfg.intermediate_size == 0 or cfg.vocab_size == 0 or !std.math.isFinite(cfg.layer_norm_eps) or cfg.layer_norm_eps <= 0 or !std.math.isFinite(cfg.global_rope_theta) or cfg.global_rope_theta <= 0 or !std.math.isFinite(cfg.local_rope_theta) or cfg.local_rope_theta <= 0) return error.InvalidLayaConfig;
    if (cfg.checkpoint_layout != .huggingface_fused_qkv_no_bias or cfg.hidden_size > 1024 or cfg.num_hidden_layers > 32 or cfg.intermediate_size > 4096 or cfg.vocab_size > 65536 or laya.max_len > 2048) return error.UnsupportedLayaConfig;
    return cfg;
}

/// Match main's serving-only Q8_0 policy; the checkpoint and its descriptors
/// remain dense, and embeddings, norms and scorers retain their precision.
pub fn registerWeight(a: std.mem.Allocator, compute: *WasmCompute, cfg: Config, name: []const u8, runtime_name: []const u8, shape: []const i64, bytes: []const u8, kind: types.KnownTensorType) !void {
    if (cfg.laya.?.weight_quantization != .q8_0 or !model.quantizedLinear(name)) return compute.registerShapedWeight(runtime_name, shape, bytes, kind);
    if (shape.len != 2 or shape[0] <= 0 or shape[1] <= 0 or @mod(shape[1], 32) != 0) return error.InvalidLayaWeights;
    const count = try std.math.mul(usize, @intCast(shape[0]), @intCast(shape[1]));
    const width: usize = if (kind == .F32) 4 else if (kind == .F16 or kind == .BF16) 2 else return error.UnsupportedLayaArtifact;
    if (bytes.len != try std.math.mul(usize, count, width)) return error.InvalidWeightByteLength;
    const dense = try a.alloc(f32, count);
    defer a.free(dense);
    if (kind == .F32) {
        @memcpy(std.mem.sliceAsBytes(dense), bytes);
    } else {
        for (dense, 0..) |*value, i| {
            const bits = std.mem.readInt(u16, bytes[i * 2 ..][0..2], .little);
            value.* = if (kind == .BF16) @bitCast(@as(u32, bits) << 16) else @floatCast(@as(f16, @bitCast(bits)));
        }
    }
    const raw = try quant.quantizeQ8_0FromF32(a, dense);
    defer a.free(raw);
    return compute.registerShapedWeight(runtime_name, shape, raw, .Q8_0);
}

pub fn validateWeights(a: std.mem.Allocator, cfg: Config, descriptors: []const access.Descriptor) !void {
    const Meta = struct { shape: []const i64, dtype: @import("../backends/tensor.zig").DType };
    var tensors = std.StringHashMap(Meta).init(a);
    defer tensors.deinit();
    for (descriptors) |d| {
        const kind = d.encoding.gguf.known;
        if (kind != .F32 and kind != .F16 and kind != .BF16 or tensors.contains(d.name)) return error.InvalidLayaWeights;
        try tensors.put(d.name, .{ .shape = d.shape, .dtype = if (kind == .F32) .f32 else if (kind == .F16) .f16 else .bf16 });
    }
    const reader = .{ .header = .{ .tensors = tensors } };
    try model.validateTensorInventory(&reader, cfg.laya.?, cfg);
    const calibration = tensors.get("temperature");
    if (calibration) |value| {
        if (value.dtype != .f32 or !std.mem.eql(i64, value.shape, &.{3})) return error.InvalidLayaWeights;
    }
    var decision = cfg.laya.?;
    var head_count: usize = if (decision.format == .opendecider) 6 else 11 + 12 * decision.head_layers;
    if (decision.format == .laya and (decision.decision_head == .pointer or tensors.contains("pointer.norm.weight"))) {
        decision.decision_head = .pointer;
        try model.validateTensorInventory(&reader, decision, cfg);
        head_count += 6;
    }
    // Encoder: three root tensors, six/layer, except layer 0's attn norm.
    if (descriptors.len != 6 * @as(usize, cfg.num_hidden_layers) + 2 + head_count + @as(usize, @intFromBool(calibration != null))) return error.InvalidLayaWeights;
}

fn checkPackedWorkspace(cfg: Config, tokens: usize) !void {
    if (try packed_rows.workspaceBytes(cfg, tokens) > 512 * 1024 * 1024) return error.ExtractionRequestLimitExceeded;
}

const Adapter = struct {
    compute: *WasmCompute,
    config: Config,
    cache: *Cache,
    fn run(raw: *anyopaque, inputs: []const Tensor, a: std.mem.Allocator) anyerror![]Tensor {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        var cb = self.compute.computeBackend();
        if (self.config.laya.?.packing.enabled()) {
            if (inputs.len != packed_rows.input_count or inputs[0].shape.len != 2 or inputs[0].shape[1] <= 0) return error.InvalidLayaInputs;
            try checkPackedWorkspace(self.config, @intCast(inputs[0].shape[1]));
            return packed_rows.run(&cb, a, self.config, inputs, self.cache);
        }
        if (inputs.len != 4 or inputs[0].shape.len != 2 or inputs[3].shape.len != 2) return error.InvalidLayaInputs;
        const batch: usize = @intCast(inputs[0].shape[0]);
        const seq: usize = @intCast(inputs[0].shape[1]);
        const count: usize = @intCast(inputs[3].shape[1]);
        if (batch != 1 or seq > self.config.laya.?.max_len) return error.ExtractionRequestLimitExceeded;
        const hidden = try encoder.forwardCT(&cb, a, self.config, inputs[0].asInt64(), inputs[1].asInt64(), batch, seq);
        defer cb.free(hidden);
        return head.forward(&cb, a, self.config.laya.?, hidden, inputs[1].asInt64(), inputs[2].asInt64(), inputs[3].asInt64(), batch, seq, count, self.config.hidden_size);
    }
    fn controlled(raw: *anyopaque, inputs: []const Tensor, a: std.mem.Allocator, control: Control) anyerror![]Tensor {
        try control.check();
        return Adapter.run(raw, inputs, a);
    }
    fn inputInfo(_: *anyopaque) []const TensorInfo {
        return &.{};
    }
    fn outputInfo(_: *anyopaque) []const TensorInfo {
        return &.{};
    }
    fn backend(_: *anyopaque) backends.BackendType {
        return .wasm;
    }
    fn close(_: *anyopaque) void {}
    fn session(self: *Adapter) backends.Session {
        return .{ .ptr = self, .vtable = &.{ .run = Adapter.run, .runWithControl = controlled, .inputInfo = inputInfo, .outputInfo = outputInfo, .backend = backend, .close = close } };
    }
};

pub fn run(a: std.mem.Allocator, compute: *WasmCompute, cache: *Cache, cfg: Config, tok: Tokenizer, json: []const u8, validate_only: bool) ![]u8 {
    // Parsing/decisions use a request arena; encoder temporaries use the freeing
    // model allocator, matching the serving ownership fix in PR #815.
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const request_allocator = arena.allocator();
    const parsed = try std.json.parseFromSlice(std.json.Value, request_allocator, json, .{ .duplicate_field_behavior = .@"error" });
    const request = try wire.parse(request_allocator, parsed.value);
    if (request.items.len != 1 or request.tasks.len > 16 or request.schema_bytes > 64 * 1024) return error.ExtractionRequestLimitExceeded;
    var tokens: usize = 0;
    for (request.tasks) |task| if (task.text.len > 256 * 1024) return error.ExtractionTextLimitExceeded;
    if (cfg.laya.?.packing.enabled()) {
        // One browser input shares a state trunk across all its questions.
        const questions = try request_allocator.alloc(pipeline.Question, request.tasks.len);
        for (request.tasks, questions) |task, *question| question.* = task.question;
        for (try tree.build(request_allocator, tok, cfg.laya.?, request.tasks[0].text, questions, null)) |row| {
            try checkPackedWorkspace(cfg, row.ids.len);
            tokens += row.ids.len;
        }
    } else {
        for (request.tasks) |task| {
            const sequence = try pipeline.prepare(request_allocator, tok, cfg.laya.?, task);
            tokens += sequence.ids.len;
        }
    }
    if (validate_only) return std.json.Stringify.valueAlloc(a, .{ .valid = true, .encoded_tokens = tokens }, .{});
    var adapter = Adapter{ .compute = compute, .config = cfg, .cache = cache };
    if (cfg.laya.?.packing.enabled()) {
        const output = try pipeline.executeWithScratch(request_allocator, a, adapter.session(), tok, cfg.laya.?, request.tasks, null, cfg.laya.?.packing.max_packed_len);
        const response = try wire.response(request_allocator, request, output, 4 * 1024 * 1024);
        return a.dupe(u8, response);
    }
    const decisions = try request_allocator.alloc(pipeline.Decision, request.tasks.len);
    // Serial question execution bounds activation memory independently of the
    // number of typed decisions, while retaining native result ordering.
    for (request.tasks, decisions) |task, *decision| {
        const result = try pipeline.executeWithScratch(request_allocator, a, adapter.session(), tok, cfg.laya.?, &.{task}, null, 2048);
        decision.* = result.decisions[0];
    }
    const response = try wire.response(request_allocator, request, .{ .decisions = decisions, .prompt_tokens = tokens }, 4 * 1024 * 1024);
    return a.dupe(u8, response);
}
