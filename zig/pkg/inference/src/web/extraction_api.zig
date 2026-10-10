// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Browser-owned extraction session. No filesystem, server, or native serving
//! qualification entrypoint is used here. All offsets and decoding stay in Zig.
const std = @import("std");
const ctx = @import("entry_context.zig");
const WasmCompute = @import("../ops/wasm_compute.zig").WasmCompute;
const HfTokenizer = @import("inference_hf_tokenizer").HfTokenizer;
const boundary = @import("../models/gliner_boundary.zig");
const policy = @import("../models/gliner_boundary_artifact.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const access = @import("../models/tensor_access.zig");
const types = @import("../gguf/tensor_types.zig");
const wire = @import("../extractors/extraction_v2.zig");
const executor = @import("../extractors/gliner_boundary_executor.zig");
const BoundedAllocator = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const legacy = @import("extraction_span.zig");
const decide = @import("extraction_decide.zig");
const span_v2 = @import("../extractors/gliner_span_v2_executor.zig");
const regex = @import("../pipelines/extraction_regex.zig");
const laya = @import("extraction_laya.zig");
const LayaCache = @import("../architectures/laya_trunk_cache.zig").Cache;

pub const Metadata = struct {
    config: []const u8,
    encoder_config: []const u8,
    tokenizer_config: []const u8,
    precision: []const u8,
    gliner_config: []const u8 = "{}",
};

pub const Model = struct {
    budget: BoundedAllocator,
    compute: WasmCompute,
    laya_cache: LayaCache,
    encoder_gpu: bool,
    metadata: std.json.Parsed(Metadata),
    tokenizer: ?*HfTokenizer = null,
    config: union(enum) { span: legacy.Config, decide: decide.Config, boundary: boundary.Config, laya: laya.Config },
    descriptors: std.ArrayListUnmanaged(access.Descriptor) = .empty,
    identity: ?bundle.Identity = null,
    tokenizer_digest: ?bundle.Digest = null,
    ready: bool = false,
    handle: u32,

    fn deinit(self: *Model) void {
        const a = self.budget.allocator();
        if (self.tokenizer) |tok| tok.deinitSelf();
        for (self.descriptors.items) |d| {
            a.free(d.name);
            a.free(d.shape);
        }
        self.descriptors.deinit(a);
        self.laya_cache.deinit();
        self.compute.computeBackend().deinit();
        self.metadata.deinit();
        ctx.allocator.destroy(self);
    }
};

var active: ?*Model = null;
var next_handle: u32 = 1;
pub var last_error: []const u8 = "";
pub var result: []u8 = &.{};
var hasher = std.crypto.hash.sha2.Sha256.init(.{});
var hash_size: u64 = 0;
var digest_buffer: [64]u8 = undefined;

pub fn fail(err: anyerror) u32 {
    last_error = @errorName(err);
    return 0;
}
pub fn clearResult() void {
    if (result.len != 0) ctx.allocator.free(result);
    result = &.{};
}
pub fn get(handle: u32) !*Model {
    const model = active orelse return error.ModelNotLoaded;
    if (handle != model.handle) return error.StaleModelHandle;
    return model;
}
pub fn unload() void {
    clearResult();
    if (active) |model| model.deinit();
    active = null;
}
pub fn create(json: []const u8) !u32 {
    if (active != null) return error.ModelAlreadyLoaded;
    if (json.len > 3 * 1024 * 1024) return error.MetadataLimitExceeded;
    const model = try ctx.allocator.create(Model);
    errdefer ctx.allocator.destroy(model);
    model.budget = .{ .backing = ctx.allocator, .limit = 1536 * 1024 * 1024 };
    const a = model.budget.allocator();
    const parsed = try std.json.parseFromSlice(Metadata, a, json, .{ .allocate = .alloc_always, .duplicate_field_behavior = .@"error" });
    errdefer parsed.deinit();
    const root = try std.json.parseFromSlice(std.json.Value, a, parsed.value.config, .{});
    defer root.deinit();
    const is_laya = root.value == .object and root.value.object.contains("laya");
    const config: @FieldType(Model, "config") = if (is_laya) .{ .laya = try laya.parseConfig(a, parsed.value.config, parsed.value.precision) } else switch (try boundary.detectArchitecture(a, parsed.value.config)) {
        .boundary => .{ .boundary = try boundary.parseConfig(a, parsed.value.config, parsed.value.encoder_config) },
        .span => if (try boundary.declaresSpanArchitecture(a, parsed.value.config)) .{ .decide = try decide.parseConfig(a, parsed.value.config, parsed.value.encoder_config) } else .{ .span = try legacy.parseConfig(a, parsed.value.config, parsed.value.encoder_config) },
        else => return error.UnsupportedGlinerArchitecture,
    };
    model.compute = WasmCompute.init(a);
    // Browser cache shares the model's host budget and dies with the model.
    // F32 entries keep CPU decisions stable across cold and cached requests.
    model.laya_cache = LayaCache.init(a, 64 * 1024 * 1024);
    model.laya_cache.precision = .f32;
    model.encoder_gpu = model.compute.use_gpu;
    // Boundary heads stay on CPU; an encoder scope temporarily enables GPU.
    if (config == .boundary) model.compute.use_gpu = false;
    if (config == .laya) model.compute.resident_dense_gpu = true;
    model.metadata = parsed;
    model.config = config;
    model.tokenizer = null;
    model.tokenizer_digest = null;
    model.descriptors = .empty;
    model.identity = null;
    model.ready = false;
    model.handle = next_handle;
    next_handle +%= 1;
    if (next_handle == 0) next_handle = 1;
    active = model;
    return model.handle;
}
pub fn tokenizer(handle: u32, json: []const u8) !void {
    const model = try get(handle);
    if (model.ready or model.tokenizer != null) return error.InvalidLoadState;
    if (json.len > 32 * 1024 * 1024) return error.TokenizerLimitExceeded;
    model.tokenizer = try HfTokenizer.loadFromBytesWithOptions(model.budget.allocator(), json, .{ .strict_unigram_normalizer = true });
    model.tokenizer_digest = bundle.Digest.of(json);
}

const Weight = struct { name: []const u8, shape: []const i64, kind: u32 };
pub fn weight(handle: u32, metadata: []const u8, bytes: []const u8) !void {
    const model = try get(handle);
    if (model.ready or metadata.len > 8192 or bytes.len > 512 * 1024 * 1024) return error.InvalidLoadState;
    const a = model.budget.allocator();
    const parsed = try std.json.parseFromSlice(Weight, a, metadata, .{});
    defer parsed.deinit();
    const w = parsed.value;
    const kind: types.KnownTensorType = switch (w.kind) {
        0 => .F32,
        1 => .F16,
        2 => .Q4_0,
        8 => .Q8_0,
        12 => .Q4_K,
        30 => .BF16,
        else => return error.UnsupportedTensorType,
    };
    const name = try a.dupe(u8, w.name);
    errdefer a.free(name);
    const shape = try a.dupe(i64, w.shape);
    errdefer a.free(shape);
    try model.descriptors.ensureUnusedCapacity(a, 1);
    if (model.config == .boundary) {
        var found = false;
        for (policy.specs(model.config.boundary.backbone)) |spec| {
            if (!std.mem.eql(u8, spec.name, name)) continue;
            found = true;
            if (!std.mem.eql(i64, spec.shape, shape)) return error.InvalidGlinerBoundaryTensorShape;
            if (kind != try policy.targetType(model.config.boundary.backbone, try boundaryPrecision(model), spec)) return error.InvalidGlinerBoundaryTensorPrecision;
            break;
        }
        if (!found) return error.UnexpectedGlinerBoundaryTensor;
    }
    // Inventories use the checkpoint namespace; DeBERTa's compute contract
    // uses names relative to the outer encoder module for both families.
    const stripped = if (std.mem.startsWith(u8, name, "encoder.")) name[8..] else name;
    const prefixed = if (model.config == .laya) try std.fmt.allocPrint(a, "model.{s}", .{stripped}) else null;
    defer if (prefixed) |value| a.free(value);
    const runtime_name = prefixed orelse stripped;
    if (model.config == .laya and kind != .F32 and kind != .F16 and kind != .BF16) return error.UnsupportedLayaArtifact;
    if (model.config != .laya and kind == .BF16) return error.UnsupportedTensorType;
    if (model.config == .span) try legacy.validateWeight(runtime_name, shape);
    if (model.config == .decide) try decide.validateWeight(runtime_name, shape);
    if (model.config == .laya) {
        try laya.registerWeight(a, &model.compute, model.config.laya, name, runtime_name, shape, bytes, kind);
    } else try model.compute.registerShapedWeight(runtime_name, shape, bytes, kind);
    model.descriptors.appendAssumeCapacity(.{ .name = name, .shape = shape, .encoding = .{ .gguf = .{ .known = kind } }, .byte_len = bytes.len, .quantized = kind != .F32 and kind != .F16 and kind != .BF16 });
}
pub fn finalize(handle: u32, digest: []const u8, size: u64) !void {
    const model = try get(handle);
    if (model.ready or model.tokenizer == null or digest.len != 64 or size == 0) return error.InvalidLoadState;
    for (digest) |c| if (!std.ascii.isHex(c)) return error.InvalidDigest;
    if (model.config == .boundary) {
        _ = try policy.validate(model.budget.allocator(), model.config.boundary.backbone, try boundaryPrecision(model), model.descriptors.items, null);
        model.identity = .{ .backbone = model.config.boundary.backbone, .precision = try boundaryPrecision(model), .weight = .{ .sha256 = digest[0..64].*, .size_bytes = size }, .sidecars = .{
            bundle.Digest.of(model.metadata.value.config), bundle.Digest.of(model.metadata.value.encoder_config), model.tokenizer_digest.?, bundle.Digest.of(model.metadata.value.tokenizer_config),
        } };
    } else if (model.config == .laya) {
        try laya.validateWeights(model.budget.allocator(), model.config.laya, model.descriptors.items);
    } else if (model.config == .decide) {
        try decide.validateWeights(&model.compute);
    } else {
        try legacy.validateWeights(&model.compute, model.config.span);
    }
    model.ready = true;
}

const limits: wire.Limits = .{ .max_request_bytes = 512 * 1024, .max_inputs = 1, .max_text_bytes_per_input = 256 * 1024, .max_total_text_bytes = 256 * 1024, .max_total_schema_bytes = 64 * 1024 };

fn boundaryPrecision(model: *Model) !policy.Precision {
    return std.meta.stringToEnum(policy.Precision, model.metadata.value.precision) orelse error.UnsupportedTensorType;
}

fn enterEncoder(raw: *anyopaque) void {
    const model: *Model = @ptrCast(@alignCast(raw));
    model.compute.use_gpu = model.encoder_gpu;
}
fn leaveEncoder(raw: *anyopaque) void {
    const model: *Model = @ptrCast(@alignCast(raw));
    model.compute.use_gpu = false;
}
pub fn run(handle: u32, json: []const u8, validate_only: bool) !void {
    clearResult();
    last_error = "";
    const model = try get(handle);
    if (!model.ready) return error.ModelNotReady;
    if (json.len > limits.max_request_bytes) return error.ExtractionRequestLimitExceeded;
    const a = model.budget.allocator();
    if (model.config == .laya) {
        const bytes = try laya.run(a, &model.compute, &model.laya_cache, model.config.laya, model.tokenizer.?.tokenizer(), json, validate_only);
        defer a.free(bytes);
        result = try ctx.allocator.dupe(u8, bytes);
        return;
    }
    if (model.config == .span) {
        const bytes = try legacy.run(a, &model.compute, model.config.span, model.tokenizer.?.tokenizer(), json, validate_only);
        defer a.free(bytes);
        result = try ctx.allocator.dupe(u8, bytes);
        return;
    }
    var validator = regex.Context.init(a, .{});
    defer validator.deinit();
    var request = try wire.parseJson(a, json, .{ .limits = limits, .compiler = .{ .regex_context = &validator, .validate_regex_fn = regex.Context.validateCompile } });
    defer request.deinit();
    if (model.config == .decide) {
        const seq = model.config.decide.max_position_embeddings;
        const options: span_v2.Options = .{
            .max_response_bytes = 4 * 1024 * 1024,
            .processor = .{ .max_batch_items = 1, .max_text_bytes = 256 * 1024, .max_sequence_tokens = seq, .max_batch_tokens = seq },
            .max_prompt_tokens = seq,
            .max_sequences_per_item = 8,
            .max_request_tokens = 8 * seq,
        };
        var plan = try span_v2.plan(a, model.tokenizer.?.tokenizer(), &request, options);
        defer plan.deinit(a);
        if (validate_only) {
            result = try std.json.Stringify.valueAlloc(ctx.allocator, .{ .valid = true, .encoded_tokens = plan.prompt_tokens }, .{});
        } else {
            var cb = model.compute.computeBackend();
            const bytes = try span_v2.executePlanned(&cb, a, .{ .deberta = model.config.decide }, &request, &plan, options);
            defer a.free(bytes);
            result = try ctx.allocator.dupe(u8, bytes);
        }
        return;
    }
    for (request.items) |item| if (item.options.long_document.mode == .window and item.options.long_document.max_windows > 32) return error.ExtractionRequestLimitExceeded;
    var options: executor.Options = .{
        .max_response_bytes = 4 * 1024 * 1024,
        .identity = model.identity,
        .processor = .{ .max_batch_items = 1, .max_text_bytes = 256 * 1024, .max_sequence_tokens = 2048, .max_batch_tokens = 2048 },
        .engine = .{ .limits = .{ .max_batch = 1, .max_sequence_tokens = 2048, .max_batch_tokens = 2048 } },
        .long_document = .{ .max_request_windows = 32, .max_total_encoded_tokens = 32 * 2048 },
    };
    options.pipeline.regex_context = &validator;
    if (model.encoder_gpu) options.engine.scope = .{ .ptr = model, .enter = enterEncoder, .leave = leaveEncoder };
    options.pipeline.validate_value_fn = regex.Context.validateValue;
    try executor.preflight(&request, options);
    if (validate_only) {
        const tokens = try executor.browserGeometry(a, &model.config.boundary, model.tokenizer.?.tokenizer(), &request, options);
        result = try std.json.Stringify.valueAlloc(ctx.allocator, .{ .valid = true, .encoded_tokens = tokens }, .{});
    } else {
        var cb = model.compute.computeBackend();
        const bytes = try executor.executeBrowser(&cb, a, &model.config.boundary, model.tokenizer.?.tokenizer(), &request, options);
        defer a.free(bytes);
        result = try ctx.allocator.dupe(u8, bytes);
    }
}
pub fn hashBegin() void {
    hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hash_size = 0;
}
pub fn hashUpdate(bytes: []const u8) void {
    hasher.update(bytes);
    hash_size += bytes.len;
}
pub fn hashEnd() []const u8 {
    digest_buffer = std.fmt.bytesToHex(hasher.finalResult(), .lower);
    return &digest_buffer;
}
