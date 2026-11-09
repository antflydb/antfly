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

//! Supplied-feature composition under explicit model, reader and request
//! leases. Authentication and node-wide queueing precede this boundary.
const std = @import("std");
const platform = @import("antfly_platform");
const manager_mod = @import("model_manager.zig");
const selection = @import("../registry/model_selection.zig");
const manifest_mod = @import("../models/manifest.zig");
const qualified = @import("../models/memorilla_qualification.zig");
const encoder = @import("../pipelines/memorilla_encoder.zig");
const memory_pipeline = @import("../pipelines/memorilla.zig");
const decode = @import("../pipelines/memorilla_generation.zig");
const generation = @import("../pipelines/generation.zig");
const factory = @import("../architectures/session_factory.zig");
const gpt_model = @import("../models/gpt.zig");
const memory = @import("../runtime/root.zig").tier.memory;
const execution = @import("../ops/execution_scope.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const file = @import("../util/c_file.zig");
const PhysicalBudgetScope = if (@import("build_options").enable_cuda) @import("../ops/cuda/cuda_compute.zig").CudaCompute.MemorillaDeviceBudgetScope else struct {
    fn deinit(_: *@This()) void {}
};
const A = std.mem.Allocator;
pub const Profile = platform.inference_settings.MemorillaProfile;
pub const Paths = struct { encoder: selection.ResolvedModel, decoder: selection.ResolvedModel };
pub const Stage = enum { prepared, encoder_loaded, question_encoded, decoder_loaded, reader_loaded, decoded };

/// Warm preflight borrows the already admitted tokenizer without loading a
/// backend on cold or invalid requests. The pin outlives every use of the
/// tokenizer and chat template, including source-only decoder preparation.
pub const GenerationAssets = union(enum) {
    loaded: manager_mod.ModelHandle,
    owned: struct { manifest: manifest_mod.ModelManifest, assets: manager_mod.ModelManager.TokenizerAssets },

    pub fn tokenizer(self: *GenerationAssets) @import("inference_tokenizer").Tokenizer {
        return switch (self.*) {
            .loaded => |*handle| handle.get().getTokenizer(),
            .owned => |*entry| entry.assets.tokenizer(),
        };
    }

    pub fn chatTemplate(self: *GenerationAssets) ?*generation.ChatTemplate {
        return switch (self.*) {
            .loaded => |*handle| handle.get().chat_tmpl,
            .owned => |*entry| entry.assets.chat_tmpl,
        };
    }

    pub fn manifest(self: *GenerationAssets) *const manifest_mod.ModelManifest {
        return switch (self.*) {
            .loaded => |*handle| &handle.get().manifest,
            .owned => |*entry| &entry.manifest,
        };
    }

    pub fn decoderConfig(self: *GenerationAssets, a: A, directory: []const u8) !gpt_model.Config {
        var config = switch (self.*) {
            .loaded => |*handle| try factory.requireMemorillaDecoderF32(handle.get().session),
            .owned => blk: {
                const bytes = try file.readFileMax(a, self.manifest().config_path orelse return error.UnsupportedMemorillaArtifact, 4096);
                defer a.free(bytes);
                break :blk try gpt_model.parseConfig(a, bytes);
            },
        };
        // Preserve generation-config overlays even when the structural model
        // config is borrowed. The loaded generation fence covers this file.
        const path = try std.fs.path.join(a, &.{ directory, "generation_config.json" });
        defer a.free(path);
        const bytes = try file.readFileMax(a, path, 4096);
        defer a.free(bytes);
        try gpt_model.applyGenerationConfigJson(a, &config, bytes);
        return config;
    }

    pub fn deinit(self: *GenerationAssets) void {
        switch (self.*) {
            .loaded => |*handle| handle.release(),
            .owned => |*entry| {
                entry.assets.deinit();
                entry.manifest.deinit();
            },
        }
        self.* = undefined;
    }
};

pub fn acquireGenerationAssets(a: A, io: std.Io, manager: *manager_mod.ModelManager, selected: selection.ResolvedModel, expected: qualified.Profile, control: Control) !GenerationAssets {
    try control.check();
    if (try manager.acquireCachedFromDirWithControl(selected.path, control)) |cached| {
        var handle = cached;
        errdefer handle.release();
        const model = handle.get();
        try qualified.requireRecipe(expected, model.manifest);
        // Cold construction authenticated every consumed metadata path and
        // byte. Reuse that immutable manifest only while its complete current
        // artifact generation still matches and the owning model is pinned.
        try model.verifyMemorillaIdentityWithControl(control);
        if (model.hf_tok == null and model.sp_tok == null) return error.UnsupportedMemorillaArtifact;
        try control.check();
        return .{ .loaded = handle };
    }
    var manifest = try manifest_mod.loadFromDir(a, selected.path);
    errdefer manifest.deinit();
    try qualified.requireRecipe(expected, manifest);
    try qualified.verifySelectedPaths(a, io, selected.path, manifest);
    if (!std.meta.eql(manifest.managed_publication, selected.publication)) return error.ModelArtifactsChanging;
    var assets = try manager.loadGenerationTokenizerAssets(selected.path, &manifest);
    errdefer assets.deinit();
    try control.check();
    return .{ .owned = .{ .manifest = manifest, .assets = assets } };
}

/// Same backend/publication policy as normal loading, without constructing an
/// unused cold-load flight key when the exact resident variant already exists.
pub fn acquireResidentModel(manager: *manager_mod.ModelManager, directory: []const u8, control: Control) !manager_mod.ModelHandle {
    try control.check();
    return (try manager.acquireCachedFromDirWithControl(directory, control)) orelse
        try manager.acquireFromDirWithControl(directory, control);
}

/// Request limits come from operator policy. Diagnostic callbacks/forced
/// tokens are internal and are never decoded from the HTTP request body.
pub const Options = struct {
    request_limits: platform.resource_policy.InferenceRequestLimits = .{},
    context: ?*anyopaque = null,
    question_features: ?*const fn (?*anyopaque, *const encoder.Features) anyerror!void = null,
    prepared_text: ?*const fn (?*anyopaque, *const decode.PreparedText) anyerror!void = null,
    reader_physical_resources: ?*const fn (?*anyopaque, decode.PhysicalResources) anyerror!void = null,
    stage: ?*const fn (?*anyopaque, Stage, *manager_mod.ModelManager) anyerror!void = null,
    trace: ?decode.Trace = null,
    teacher_forced_ids: ?[]const i64 = null,
    ignore_eos: bool = false,
};

pub const OwnedResult = struct {
    generation: generation.GenerationResult,
    encoder_execution_identity: [64]u8,
    decoder_execution_identity: [64]u8,
    module_sha256: [64]u8,
    document_count: usize,
    question_tokens: usize,
    // Keep the buffered response admitted through serialization/publication.
    response_lease: memory.AdmissionLease,

    pub fn deinit(self: *OwnedResult) void {
        self.generation.deinit();
        self.response_lease.release();
        self.* = undefined;
    }
};

pub fn validateInputs(profile: Profile, question: []const u8, documents: []const []const f32, identity: []const u8, max_new_tokens: usize) !void {
    try (platform.inference_settings.Memorilla{ .enabled = true, .profiles = &.{profile} }).validate();
    if (question.len > 8192) return error.MemorillaTextLimitExceeded;
    _ = try qualified.canonicalQuestion(question);
    if (!std.mem.eql(u8, identity, &qualified.executionIdentity(.qwen3_encoder_f32_v1))) return error.IncompatibleMemorillaProducer;
    if (max_new_tokens == 0 or max_new_tokens > profile.max_new_tokens) return error.MemorillaGenerationLimitExceeded;
    if (documents.len == 0) return error.EmptyMemorillaCollection;
    if (documents.len > profile.max_documents) return error.MemorillaCollectionBudgetExceeded;
    for (documents) |row| {
        if (row.len != qualified.width) return error.InvalidMemorillaFeatureWidth;
        var norm: f64 = 0;
        for (row) |value| {
            _ = try encoder.storedValue(value);
            norm += @as(f64, value) * value;
        }
        // FP16 stored vectors and BF16 inputs have a small norm drift. This
        // check rejects arbitrary/rescaled vectors without renormalizing them.
        if (@abs(@sqrt(norm) - 1) > 0.01) return error.UnnormalizedMemorillaFeature;
    }
}

pub fn validateRequestLimits(limits: platform.resource_policy.InferenceRequestLimits, document_count: usize, max_new_tokens: usize) !void {
    try limits.validate();
    try limits.validateItemCount(document_count);
    if (limits.max_output_tokens_per_item) |maximum| if (max_new_tokens > maximum) return error.InferenceOutputTokensExceeded;
}

pub fn validateInputTokenLimit(limits: platform.resource_policy.InferenceRequestLimits, actual_tokens: usize) !void {
    if (limits.max_input_tokens_per_item) |maximum| if (actual_tokens > maximum) return error.InferenceInputTokensExceeded;
}

fn requireLimits(limits: memory.Limits) !void {
    if (limits.host_limit_bytes == 0 or limits.backend_limit_bytes == 0 or limits.combined_limit_bytes == 0 or
        limits.kv_limit_bytes == 0 or limits.scratch_limit_bytes == 0) return error.MemorillaFiniteBudgetRequired;
}

/// Per-backend policy is a subset of its already-held global admission. The
/// reader and decoder cannot each borrow the combined request scratch cap.
pub fn requestBudgetLimits(operator: memory.Limits, held: memory.AdmissionAmounts) !memory.Limits {
    try requireLimits(operator);
    const host = held.hostTotalBytes();
    const backend = held.backendTotalBytes();
    const combined = try std.math.add(usize, host, backend);
    if (host > operator.host_limit_bytes or backend > operator.backend_limit_bytes or combined > operator.combined_limit_bytes or
        held.kvTotalBytes() > operator.kv_limit_bytes or held.scratchTotalBytes() > operator.scratch_limit_bytes)
        return error.ResourceLimitExceeded;
    return .{ .host_limit_bytes = @max(host, 1), .backend_limit_bytes = @max(backend, 1), .combined_limit_bytes = @max(combined, 1), .kv_limit_bytes = @max(held.kvTotalBytes(), 1), .scratch_limit_bytes = @max(held.scratchTotalBytes(), 1) };
}

fn notify(options: Options, stage: Stage, manager: *manager_mod.ModelManager) !void {
    if (options.stage) |callback| try callback(options.context, stage, manager);
}

pub fn generate(a: A, io: std.Io, manager: *manager_mod.ModelManager, profile: Profile, paths: Paths, question: []const u8, documents: []const []const f32, identity: []const u8, max_new_tokens: usize, limits: memory.Limits, control: Control) !OwnedResult {
    return generateWithOptions(a, io, manager, profile, paths, question, documents, identity, max_new_tokens, limits, control, .{});
}

pub fn generateWithRequestLimits(a: A, io: std.Io, manager: *manager_mod.ModelManager, profile: Profile, paths: Paths, question: []const u8, documents: []const []const f32, identity: []const u8, max_new_tokens: usize, limits: memory.Limits, request_limits: platform.resource_policy.InferenceRequestLimits, control: Control) !OwnedResult {
    return generateWithOptions(a, io, manager, profile, paths, question, documents, identity, max_new_tokens, limits, control, .{ .request_limits = request_limits });
}

pub fn generateWithOptions(a: A, io: std.Io, manager: *manager_mod.ModelManager, profile: Profile, paths: Paths, question: []const u8, documents: []const []const f32, identity: []const u8, max_new_tokens: usize, limits: memory.Limits, control: Control, options: Options) !OwnedResult {
    var result = try generateImpl(a, io, manager, profile, paths, question, documents, identity, max_new_tokens, limits, control, options);
    errdefer result.deinit();
    // All model/request GPU retirement in generateImpl has completed before
    // the final live cancellation/authority check and response handoff.
    try control.check();
    return result;
}

fn generateImpl(a: A, io: std.Io, manager: *manager_mod.ModelManager, profile: Profile, paths: Paths, question: []const u8, documents: []const []const f32, identity: []const u8, max_new_tokens: usize, limits: memory.Limits, control: Control, options: Options) !OwnedResult {
    try control.check();
    try validateInputs(profile, question, documents, identity, max_new_tokens);
    try validateRequestLimits(options.request_limits, documents.len, max_new_tokens);
    try requireLimits(limits);
    try qualified.requireCudaEnvironment();
    if (paths.encoder.publication == null or paths.decoder.publication == null) return error.MemorillaManagedProducerRequired;
    try paths.encoder.verify(a, io);
    try paths.decoder.verify(a, io);
    const encoder_control = paths.encoder.bind(control);
    const decoder_control = paths.decoder.bind(control);
    const canonical = try qualified.canonicalQuestion(question);

    // Covers bounded metadata, canonical prompt and supplied feature copies;
    // tokenizer parsing/cache has its own managed admission. Transfer this
    // lease to the final response, which outlives all request GPU allocations.
    var response_lease = try manager.acquireRunResourceAmounts(.cpu, limits, .{ .host_scratch_bytes = 16 * 1024 * 1024 });
    var transfer_response = false;
    defer if (!transfer_response) response_lease.release();
    const backing = platform.allocator.reclaimingScratch(a);
    const preflight_question_tokens = try preflightQuestion(backing, io, manager, paths.encoder, canonical, encoder_control);
    try validateInputTokenLimit(options.request_limits, preflight_question_tokens);
    var assets = try acquireGenerationAssets(backing, io, manager, paths.decoder, .qwen3_decoder_f32_kv_f32_v1, decoder_control);
    defer assets.deinit();
    const config = try assets.decoderConfig(backing, paths.decoder.path);
    const bounded = decode.Limits{ .max_new_tokens = profile.max_new_tokens };
    var text = try decode.prepareText(backing, assets.tokenizer(), assets.chatTemplate() orelse return error.MemorillaChatTemplateRequired, config, canonical, .{}, bounded, decoder_control);
    defer text.deinit();
    try validateInputTokenLimit(options.request_limits, text.input_ids.len);
    if (options.prepared_text) |callback| try callback(options.context, &text);
    const plan = try decode.ResourcePlan.create(config, .cuda, text.input_ids.len, text.rendered.len, 16, max_new_tokens, bounded);
    // Reader execution has separate request admission. Cover activation/cache
    // retirement plus its uploaded inputs and returned sixteen memory rows.
    const reader_request = memory.AdmissionAmounts{ .host_scratch_bytes = 16 * 1024 * 1024, .backend_scratch_bytes = 128 * 1024 * 1024 + documents.len * qualified.width * 16 * @sizeOf(f32) };
    const decoder_request = try memory.AdmissionAmounts.fromEstimate(plan.decoder).merge(.fromEstimate(plan.host));
    const request_amounts = try decoder_request.merge(reader_request);
    var request_lease = try manager.acquireRunResourceAmounts(.gpu, limits, request_amounts);
    defer request_lease.release();
    var decoder_budget = memory.RunBudget.init(try requestBudgetLimits(limits, decoder_request));
    var reader_budget = memory.RunBudget.init(try requestBudgetLimits(limits, reader_request));
    var reservation = try plan.reserve(&decoder_budget);
    defer reservation.deinit();
    const reader_estimate = memory.Estimate{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .backend, .scratch_bytes = reader_request.backend_scratch_bytes, .scratch_tier = .backend };
    try reader_budget.reserveEstimate(reader_estimate);
    defer reader_budget.releaseEstimate(reader_estimate);
    const reader_host_estimate = memory.Estimate{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_bytes = reader_request.host_scratch_bytes, .scratch_tier = .host };
    try reader_budget.reserveEstimate(reader_host_estimate);
    defer reader_budget.releaseEstimate(reader_host_estimate);
    try notify(options, .prepared, manager);

    var encoder_handle = try acquireResidentModel(manager, paths.encoder.path, encoder_control);
    defer encoder_handle.release();
    const encoder_model = encoder_handle.get();
    try encoder_model.verifyMemorillaIdentityWithControl(encoder_control);
    try notify(options, .encoder_loaded, manager);
    var features = try encoder.encodeQuestion(backing, &encoder_handle, canonical, limits, encoder_control);
    defer features.deinit();
    if (!std.mem.eql(u8, features.question_text, canonical)) return error.NonCanonicalMemorillaQuestion;
    try encoder.requireProducer(features.producer, paths.encoder.publication.?);
    if (options.question_features) |callback| try callback(options.context, &features);
    try notify(options, .question_encoded, manager);
    try control.check();
    try paths.encoder.verify(a, io);
    try paths.decoder.verify(a, io);

    var decoder_handle = try acquireResidentModel(manager, paths.decoder.path, decoder_control);
    defer decoder_handle.release();
    const decoder_model = decoder_handle.get();
    try decoder_model.verifyMemorillaIdentityWithControl(decoder_control);
    try notify(options, .decoder_loaded, manager);
    // Nested guards use independent watchdog tokens. Arm before constructing
    // the reader and retain protection through its final physical destruction.
    var native_guard = try control.enterUninterruptible(.uninterruptible);
    defer native_guard.deinit();
    // The decoder handle pins the cached reader and its persistent admission.
    // Release this borrow after all request tensors, budgets and scopes drain.
    var reader_handle = try decoder_model.memorilla_reader_cache.acquire(decoder_model.allocator, io, manager, profile.memory_module_path, limits, control);
    defer reader_handle.deinit();
    var reader_request_clean = false;
    defer if (!reader_request_clean) reader_handle.invalidate();
    const reader = reader_handle.owner();
    try reader.bundle.config.validate(true);
    try notify(options, .reader_loaded, manager);
    const module_sha256 = reader.bundle.weights_sha256;
    const flat = try backing.alloc(f32, documents.len * qualified.width);
    defer backing.free(flat);
    for (documents, 0..) |row, i| {
        for (row, flat[i * qualified.width ..][0..qualified.width]) |value, *out| out.* = (try encoder.storedValue(value)).memory;
    }
    const mask = try backing.alloc(bool, documents.len);
    defer backing.free(mask);
    @memset(mask, false);

    var reader_backend = try reader.computeBackendWithBudget(&reader_budget);
    reader_backend.execution_control = control;
    reader_backend.execution_budget = &reader_budget;
    var reader_scope = try execution.Scope.acquire(control, .{ .mutex = &reader.execution_mutex });
    defer reader_scope.deinit();
    var reader_physical: PhysicalBudgetScope = if (comptime @import("build_options").enable_cuda)
        try reader.compute.beginMemorillaDeviceBudget(reader_request.backendTotalBytes())
    else
        return error.CudaNotEnabled;
    defer reader_physical.deinit();
    try reader_scope.bind(reader_backend);
    // All CT defers below precede reader cleanup and admission refunds.
    const output = blk: {
        defer {
            reader.releaseRequestResources() catch @panic("Memorilla reader request cleanup failed; worker restart required");
            reader_request_clean = true;
        }
        const docs = try reader_backend.fromFloat32Shape(flat, &.{ @intCast(documents.len), @intCast(qualified.width) });
        defer reader_backend.free(docs);
        const query = try reader_backend.fromFloat32Shape(features.question, &.{ 1, @intCast(qualified.width) });
        defer reader_backend.free(query);
        try encoder_model.verifyMemorillaIdentityWithControl(encoder_control);
        try decoder_model.verifyMemorillaIdentityWithControl(decoder_control);
        try factory.requireMemorillaCudaExecution(&reader_backend);
        const memories = try memory_pipeline.forward(&reader_backend, backing, reader.bundle.config, .{ .documents = docs, .question = query, .padding_mask = mask, .batch = 1, .num_documents = documents.len, .max_document_rows = profile.max_documents }, null);
        defer reader_backend.free(memories);
        try factory.requireMemorillaCudaExecution(&reader_backend);
        try control.check();
        const decoder_backend = try factory.getCoordinatedCudaComputeBackend(decoder_model.session, &decoder_budget, @intFromPtr(&reservation));
        defer decoder_backend.deinit();
        var decoder = try decode.Decoder.fromQualifiedSession(decoder_model.session, &decoder_backend, assets.tokenizer(), decoder_model.nativeGenerationMutex());
        decoder.execution_lane = &decoder_model.native_reader_execution_lane;
        break :blk try decode.generate(a, decoder, &text, .{ .backend = &reader_backend, .tensor = memories }, .{ .max_new_tokens = max_new_tokens, .teacher_forced_ids = options.teacher_forced_ids, .trace = options.trace, .ignore_eos = options.ignore_eos }, &reservation, bounded, decoder_control);
    };
    var result = output;
    errdefer result.deinit();
    if (comptime @import("build_options").enable_cuda) {
        if (options.reader_physical_resources) |record| {
            const snapshot = reader.compute.ctx.device_allocations.snapshot();
            try record(options.context, .{ .baseline = reader_physical.baseline, .held_transient_bytes = reader_request.backendTotalBytes(), .limit = snapshot.limit, .live = snapshot.live, .lifetime_peak = snapshot.peak });
        }
    }
    try encoder_model.verifyMemorillaIdentityWithControl(encoder_control);
    try decoder_model.verifyMemorillaIdentityWithControl(decoder_control);
    try paths.encoder.verify(a, io);
    try paths.decoder.verify(a, io);
    try reader_handle.verify(control);
    try control.check();
    try notify(options, .decoded, manager);
    try control.check();
    if (features.question_tokens != preflight_question_tokens) return error.InvalidMemorillaTokens;
    transfer_response = true;
    return .{ .generation = result, .encoder_execution_identity = qualified.executionIdentity(.qwen3_encoder_f32_v1), .decoder_execution_identity = qualified.executionIdentity(.qwen3_decoder_f32_kv_f32_v1), .module_sha256 = module_sha256, .document_count = documents.len, .question_tokens = features.question_tokens, .response_lease = response_lease };
}

fn preflightQuestion(a: A, io: std.Io, manager: *manager_mod.ModelManager, selected: selection.ResolvedModel, canonical: []const u8, control: Control) !usize {
    try control.check();
    var assets = try acquireGenerationAssets(a, io, manager, selected, .qwen3_encoder_f32_v1, control);
    defer assets.deinit();
    var tokens = try assets.tokenizer().encodeForModel(a, canonical, qualified.max_text_tokens + 1);
    defer tokens.deinit();
    if (tokens.ids.len != tokens.attention_mask.len) return error.InvalidMemorillaTokens;
    var active: usize = 0;
    var last: i32 = -1;
    for (tokens.ids, tokens.attention_mask) |id, used| {
        if (used != 0 and used != 1) return error.InvalidMemorillaTokens;
        if (used == 1) {
            active += 1;
            last = id;
        }
    }
    if (active > qualified.max_text_tokens) return error.MemorillaTextLimitExceeded;
    if (active == 0 or last != 151643) return error.InvalidMemorillaTokens;
    try selected.verify(a, io);
    try control.check();
    return active;
}

test "Memorilla supplied feature validation rejects invalid identity geometry norm and bounds before models" {
    const p = Profile{ .name = "stage3", .encoder = "encoder", .decoder = "decoder", .memory_module_path = "reader" };
    var values: [qualified.width]f32 = @splat(0);
    values[0] = 1;
    const rows = [_][]const f32{&values};
    const identity = qualified.executionIdentity(.qwen3_encoder_f32_v1);
    try validateInputs(p, "\xc2\xa0question\xc2\xa0", &rows, &identity, 64);
    try std.testing.expectError(error.EmptyMemorillaCollection, validateInputs(p, "q", &.{}, &identity, 1));
    try std.testing.expectError(error.IncompatibleMemorillaProducer, validateInputs(p, "q", &rows, "other", 1));
    try std.testing.expectError(error.InvalidMemorillaFeatureWidth, validateInputs(p, "q", &.{values[0..2]}, &identity, 1));
    try std.testing.expectError(error.MemorillaGenerationLimitExceeded, validateInputs(p, "q", &rows, &identity, 129));
    values[0] = 2;
    try std.testing.expectError(error.UnnormalizedMemorillaFeature, validateInputs(p, "q", &rows, &identity, 1));
    values[0] = std.math.nan(f32);
    try std.testing.expectError(error.NonFiniteMemorillaFeature, validateInputs(p, "q", &rows, &identity, 1));
}

test "Memorilla service cancellation precedes component resolution and model loading" {
    try std.testing.expectError(error.Timeout, generate(std.testing.allocator, std.testing.io, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, .{ .deadline_ns = 0 }));
}

test "Memorilla service backend budgets cannot share the global request scratch ceiling" {
    const limits = memory.Limits{ .host_limit_bytes = 1000, .backend_limit_bytes = 1000, .combined_limit_bytes = 2000, .kv_limit_bytes = 500, .scratch_limit_bytes = 1500 };
    const decoder = try requestBudgetLimits(limits, .{ .host_scratch_bytes = 100, .backend_scratch_bytes = 200, .backend_kv_bytes = 50 });
    const reader = try requestBudgetLimits(limits, .{ .host_scratch_bytes = 25, .backend_scratch_bytes = 75 });
    try std.testing.expectEqual(@as(usize, 250), decoder.backend_limit_bytes);
    try std.testing.expectEqual(@as(usize, 75), reader.backend_limit_bytes);
    try std.testing.expectEqual(@as(usize, 350), decoder.combined_limit_bytes);
    try std.testing.expectEqual(@as(usize, 100), reader.combined_limit_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, requestBudgetLimits(limits, .{ .backend_scratch_bytes = 1001 }));
}

test "Memorilla service applies operator item input and output ceilings" {
    const limits = platform.resource_policy.InferenceRequestLimits{ .max_items_per_request = 2, .max_input_tokens_per_item = 32, .max_output_tokens_per_item = 4 };
    try validateRequestLimits(limits, 2, 4);
    try std.testing.expectError(error.InferenceBatchTooLarge, validateRequestLimits(limits, 3, 4));
    try std.testing.expectError(error.InferenceOutputTokensExceeded, validateRequestLimits(limits, 2, 5));
    try validateInputTokenLimit(limits, 32);
    try std.testing.expectError(error.InferenceInputTokensExceeded, validateInputTokenLimit(limits, 33));
}
