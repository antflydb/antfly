// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Internal, bounded plain-text execution on the qualified F32 decoder.
//! Callers assemble explicit source/verification prompts; this service does
//! not select fallback policy, certify facts, or expose ordinary generation.
const std = @import("std");
const platform = @import("antfly_platform");
const manager_mod = @import("model_manager.zig");
const selection = @import("../registry/model_selection.zig");
const manifest_mod = @import("../models/manifest.zig");
const qualified = @import("../models/memorilla_qualification.zig");
const decode = @import("../pipelines/memorilla_generation.zig");
const generation = @import("../pipelines/generation.zig");
const factory = @import("../architectures/session_factory.zig");
const gpt_model = @import("../models/gpt.zig");
const memory = @import("../runtime/root.zig").tier.memory;
const common = @import("memorilla_service.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const file = @import("../util/c_file.zig");
const tokenizer_mod = @import("inference_tokenizer");
const grounding = @import("../pipelines/memorilla_grounding.zig");
const A = std.mem.Allocator;
const RequestLimits = platform.resource_policy.InferenceRequestLimits;

pub const Options = struct {
    context: ?*anyopaque = null,
    prepared_text: ?*const fn (?*anyopaque, *const decode.PreparedPlainText) anyerror!void = null,
    trace: ?decode.Trace = null,
    teacher_forced_ids: ?[]const i64 = null,
    ignore_eos: bool = false,
    mode: decode.PlainMode = .plain,
    final_token_cap: usize = 48,
};

/// Text is decoded from this exact raw-token suffix; IDs are never synthesized.
pub const FinalSpan = struct { text: []const u8, token_start: usize, token_end: usize };
pub const OwnedResult = struct {
    generation: generation.GenerationResult,
    decoder_execution_identity: [64]u8,
    response_lease: memory.AdmissionLease,
    final_span: ?FinalSpan = null,
    thinking: ?decode.ThinkingMetadata = null,

    pub fn finalTokenIds(self: *const OwnedResult) ?[]const i32 {
        const span = self.final_span orelse return null;
        return (self.generation.token_ids orelse return null)[span.token_start..span.token_end];
    }

    pub fn deinit(self: *OwnedResult) void {
        if (self.final_span) |span| self.generation.allocator.free(span.text);
        self.generation.deinit();
        self.response_lease.release();
        self.* = undefined;
    }
};

pub fn validateInputs(system: []const u8, user: []const u8, max_new_tokens: usize, request_limits: RequestLimits) !void {
    try common.validateRequestLimits(request_limits, 1, max_new_tokens);
    if (max_new_tokens == 0 or max_new_tokens > 128) return error.MemorillaGenerationLimitExceeded;
    const bytes = std.math.add(usize, system.len, user.len) catch return error.MemorillaResourceOverflow;
    if (user.len == 0 or bytes > 128 * 1024) return error.MemorillaPromptBudgetExceeded;
    _ = std.unicode.Utf8View.init(system) catch return error.InvalidMemorillaPromptRecipe;
    _ = std.unicode.Utf8View.init(user) catch return error.InvalidMemorillaPromptRecipe;
    if (std.mem.indexOfScalar(u8, system, 0) != null or std.mem.indexOfScalar(u8, user, 0) != null)
        return error.InvalidMemorillaPromptRecipe;
}

const bounded = decode.Limits{ .max_new_tokens = 128 };
const PreparedRequest = struct {
    config: gpt_model.Config,
    assets: manager_mod.ModelManager.TokenizerAssets,
    text: decode.PreparedPlainText,
    fn deinit(self: *PreparedRequest) void {
        self.text.deinit();
        self.assets.deinit();
    }
};

fn prepareRequest(a: A, io: std.Io, manager: *manager_mod.ModelManager, selected: selection.ResolvedModel, system: []const u8, user: []const u8, max_new_tokens: usize, request_limits: RequestLimits, bound_control: Control, mode: decode.PlainMode) !PreparedRequest {
    try bound_control.check();
    try validateInputs(system, user, max_new_tokens, request_limits);
    if (selected.publication == null) return error.MemorillaManagedProducerRequired;
    try selected.verify(a, io);
    const backing = platform.allocator.reclaimingScratch(a);
    var manifest = try manifest_mod.loadFromDir(backing, selected.path);
    defer manifest.deinit();
    try qualified.requireRecipe(.qwen3_decoder_f32_kv_f32_v1, manifest);
    try qualified.verifySelectedPaths(backing, io, selected.path, manifest);
    if (!std.meta.eql(manifest.managed_publication, selected.publication)) return error.ModelArtifactsChanging;
    const config_bytes = try file.readFileMax(backing, manifest.config_path orelse return error.UnsupportedMemorillaArtifact, 4096);
    defer backing.free(config_bytes);
    var config = try gpt_model.parseConfig(backing, config_bytes);
    const generation_path = try std.fs.path.join(backing, &.{ selected.path, "generation_config.json" });
    defer backing.free(generation_path);
    const generation_bytes = try file.readFileMax(backing, generation_path, 4096);
    defer backing.free(generation_bytes);
    try gpt_model.applyGenerationConfigJson(backing, &config, generation_bytes);
    var assets = try manager.loadGenerationTokenizerAssets(selected.path, &manifest);
    errdefer assets.deinit();
    var text = try decode.preparePlainTextWithMode(backing, assets.tokenizer(), assets.chat_tmpl orelse return error.MemorillaChatTemplateRequired, config, system, user, bounded, bound_control, mode);
    errdefer text.deinit();
    try common.validateInputTokenLimit(request_limits, text.input_ids.len);
    try selected.verify(a, io);
    try bound_control.check();
    return .{ .config = config, .assets = assets, .text = text };
}

pub fn preflight(a: A, io: std.Io, manager: *manager_mod.ModelManager, selected: selection.ResolvedModel, system: []const u8, user: []const u8, max_new_tokens: usize, limits: memory.Limits, request_limits: RequestLimits, control: Control) !void {
    try control.check();
    try validateInputs(system, user, max_new_tokens, request_limits);
    _ = try common.requestBudgetLimits(limits, .{ .host_scratch_bytes = 16 * 1024 * 1024 });
    var lease = try manager.acquireRunResourceAmounts(.cpu, limits, .{ .host_scratch_bytes = 16 * 1024 * 1024 });
    defer lease.release();
    var prepared = try prepareRequest(a, io, manager, selected, system, user, max_new_tokens, request_limits, selected.bind(control), .plain);
    defer prepared.deinit();
    const plan = try decode.ResourcePlan.createPlain(prepared.config, .cuda, prepared.text.input_ids.len, prepared.text.rendered.len, max_new_tokens, bounded);
    const amounts = try memory.AdmissionAmounts.fromEstimate(plan.decoder).merge(.fromEstimate(plan.host));
    _ = try common.requestBudgetLimits(limits, amounts);
    try control.check();
}

pub fn generate(a: A, io: std.Io, manager: *manager_mod.ModelManager, selected: selection.ResolvedModel, system: []const u8, user: []const u8, max_new_tokens: usize, limits: memory.Limits, request_limits: RequestLimits, control: Control) !OwnedResult {
    return generateWithOptions(a, io, manager, selected, system, user, max_new_tokens, limits, request_limits, control, .{});
}

pub fn generateWithOptions(a: A, io: std.Io, manager: *manager_mod.ModelManager, selected: selection.ResolvedModel, system: []const u8, user: []const u8, max_new_tokens: usize, limits: memory.Limits, request_limits: RequestLimits, control: Control, options: Options) !OwnedResult {
    var result = try generateImpl(a, io, manager, selected, system, user, max_new_tokens, limits, request_limits, control, options);
    errdefer result.deinit();
    // All request device state has physically retired before publication.
    try control.check();
    return result;
}

fn generateImpl(a: A, io: std.Io, manager: *manager_mod.ModelManager, selected: selection.ResolvedModel, system: []const u8, user: []const u8, max_new_tokens: usize, limits: memory.Limits, request_limits: RequestLimits, control: Control, options: Options) !OwnedResult {
    try control.check();
    try validateInputs(system, user, max_new_tokens, request_limits);
    if (options.mode == .bounded_thinking and (options.final_token_cap == 0 or options.final_token_cap > 48 or options.teacher_forced_ids != null or options.ignore_eos))
        return error.InvalidMemorillaForcedTokens;
    if (limits.host_limit_bytes == 0 or limits.backend_limit_bytes == 0 or limits.combined_limit_bytes == 0 or
        limits.kv_limit_bytes == 0 or limits.scratch_limit_bytes == 0) return error.MemorillaFiniteBudgetRequired;
    try qualified.requireCudaEnvironment();
    if (selected.publication == null) return error.MemorillaManagedProducerRequired;
    try selected.verify(a, io);
    const bound_control = selected.bind(control);
    // Covers bounded prompt copies/metadata and result ownership through HTTP
    // serialization. Tokenizer owners retain their existing managed admission.
    var response_lease = try manager.acquireRunResourceAmounts(.cpu, limits, .{ .host_scratch_bytes = 16 * 1024 * 1024 });
    var transferred = false;
    defer if (!transferred) response_lease.release();
    var prepared = try prepareRequest(a, io, manager, selected, system, user, max_new_tokens, request_limits, bound_control, options.mode);
    defer prepared.deinit();
    const config = prepared.config;
    const text = &prepared.text;
    if (options.prepared_text) |callback| try callback(options.context, text);
    const plan = try decode.ResourcePlan.createPlain(config, .cuda, text.input_ids.len, text.rendered.len, max_new_tokens, bounded);
    const request_amounts = try memory.AdmissionAmounts.fromEstimate(plan.decoder).merge(.fromEstimate(plan.host));
    var request_lease = try manager.acquireRunResourceAmounts(.gpu, limits, request_amounts);
    defer request_lease.release();
    var budget = memory.RunBudget.init(try common.requestBudgetLimits(limits, request_amounts));
    var reservation = try plan.reserve(&budget);
    defer reservation.deinit();
    try control.check();
    try selected.verify(a, io);
    var handle = try manager.acquireFromDirWithControl(selected.path, bound_control);
    defer handle.release();
    const model = handle.get();
    try model.verifyMemorillaIdentityWithControl(bound_control);
    var guard = try control.enterUninterruptible(.uninterruptible);
    defer guard.deinit();
    const backend = try factory.getCoordinatedCudaComputeBackend(model.session, &budget, @intFromPtr(&reservation));
    defer backend.deinit();
    var decoder = try decode.Decoder.fromQualifiedSession(model.session, &backend, prepared.assets.tokenizer(), model.nativeGenerationMutex());
    decoder.execution_lane = &model.native_reader_execution_lane;
    var thinking: decode.ThinkingMetadata = .{};
    var result = try decode.generatePlain(a, decoder, text, .{
        .max_new_tokens = max_new_tokens,
        .teacher_forced_ids = options.teacher_forced_ids,
        .trace = options.trace,
        .ignore_eos = options.ignore_eos,
        .mode = options.mode,
        .thinking_metadata = if (options.mode == .bounded_thinking) &thinking else null,
    }, &reservation, bounded, bound_control);
    errdefer result.deinit();
    try model.verifyMemorillaIdentityWithControl(bound_control);
    try selected.verify(a, io);
    try control.check();
    const final_span = if (options.mode == .bounded_thinking)
        try parseThinkingFinal(result.allocator, prepared.assets.tokenizer(), config, result, options.final_token_cap, bound_control)
    else
        null;
    errdefer if (final_span) |span| result.allocator.free(span.text);
    try control.check();
    transferred = true;
    return .{ .generation = result, .decoder_execution_identity = qualified.executionIdentity(.qwen3_decoder_f32_kv_f32_v1), .response_lease = response_lease, .final_span = final_span, .thinking = if (options.mode == .bounded_thinking) thinking else null };
}

fn parseThinkingFinal(a: A, tokenizer: tokenizer_mod.Tokenizer, config: gpt_model.Config, raw: generation.GenerationResult, final_cap: usize, control: Control) !?FinalSpan {
    try control.check();
    const ids = raw.token_ids orelse return null;
    if (!std.mem.eql(u8, raw.finish_reason, "stop") or ids.len == 0 or ids.len > 128 or raw.tokens_used != ids.len or final_cap == 0 or ids[ids.len - 1] < 0 or
        !config.isEosToken(@intCast(ids[ids.len - 1]))) return null;
    const specials = try tokenizer.allSpecialTokenIds(a);
    defer a.free(specials);
    var opening: ?usize = null;
    var closing: ?usize = null;
    for (ids, 0..) |id, i| {
        try control.check();
        if (id < 0 or id >= config.vocab_size or id >= tokenizer.vocabSize()) return null;
        if (id == decode.think_open_id) {
            if (opening != null or closing != null) return null;
            opening = i;
        } else if (id == decode.think_close_id) {
            if (opening == null or closing != null) return null;
            closing = i;
        } else if (config.isEosToken(@intCast(id))) {
            if (i + 1 != ids.len) return null;
        } else if (std.mem.indexOfScalar(u32, specials, @intCast(id)) != null) return null;
    }
    const start_open = opening orelse return null;
    const start = (closing orelse return null) + 1;
    if (start >= ids.len or ids.len - start > final_cap) return null;
    const raw_bytes = try tokenizer.decode(a, ids);
    defer a.free(raw_bytes);
    if (!std.unicode.utf8ValidateSlice(raw_bytes) or std.mem.indexOfScalar(u8, raw_bytes, 0) != null or !std.mem.eql(u8, raw_bytes, raw.text)) return null;
    if (start_open > 0) {
        const prefix = try tokenizer.decode(a, ids[0..start_open]);
        defer a.free(prefix);
        if (!grounding.isBlank(prefix)) return null;
    }
    const text = try tokenizer.decode(a, ids[start..]);
    errdefer a.free(text);
    if (text.len > grounding.max_candidate_bytes or !std.unicode.utf8ValidateSlice(text) or std.mem.indexOfScalar(u8, text, 0) != null or grounding.isBlank(text)) {
        a.free(text);
        return null;
    }
    try control.check();
    return .{ .text = text, .token_start = start, .token_end = ids.len };
}

test "Memorilla source decoder cancellation precedes artifact IO and model load" {
    try std.testing.expectError(error.Timeout, generate(std.testing.allocator, std.testing.io, undefined, undefined, undefined, undefined, undefined, undefined, .{}, .{ .deadline_ns = 0 }));
    try std.testing.expectError(error.Timeout, preflight(std.testing.allocator, std.testing.io, undefined, undefined, undefined, undefined, undefined, undefined, .{}, .{ .deadline_ns = 0 }));
}

test "Memorilla source decoder rejects invalid prompts and operator ceilings" {
    try validateInputs("system", "user", 8, .{ .max_output_tokens_per_item = 8 });
    try std.testing.expectError(error.InferenceOutputTokensExceeded, validateInputs("s", "u", 8, .{ .max_output_tokens_per_item = 7 }));
    try std.testing.expectError(error.MemorillaGenerationLimitExceeded, validateInputs("s", "u", 129, .{}));
    try std.testing.expectError(error.MemorillaPromptBudgetExceeded, validateInputs("s", "", 8, .{}));
    try std.testing.expectError(error.InvalidMemorillaPromptRecipe, validateInputs("s", "\xff", 8, .{}));
    try std.testing.expectError(error.InvalidMemorillaPromptRecipe, validateInputs("s", "u\x00", 8, .{}));
}

const SpanTestTokenizer = struct {
    var sentinel: u8 = 0;
    fn encode(_: *anyopaque, a: A, text: []const u8) ![]i32 {
        if (std.mem.eql(u8, text, "<think>")) return a.dupe(i32, &.{decode.think_open_id});
        if (std.mem.eql(u8, text, "</think>")) return a.dupe(i32, &.{decode.think_close_id});
        const ids = try a.alloc(i32, text.len);
        for (text, ids) |byte, *id| id.* = byte;
        return ids;
    }
    fn append(ptr: *anyopaque, a: A, text: []const u8, ids: *std.ArrayListUnmanaged(i32)) !void {
        const values = try encode(ptr, a, text);
        defer a.free(values);
        try ids.appendSlice(a, values);
    }
    fn model(_: *anyopaque, _: A, _: []const u8, _: usize) !tokenizer_mod.EncodeResult {
        return error.NotUsed;
    }
    fn gen(_: *anyopaque, _: A, _: []const u8, _: usize, _: bool) !tokenizer_mod.EncodeResult {
        return error.NotUsed;
    }
    fn decodeBytes(_: *anyopaque, a: A, ids: []const i32) ![]u8 {
        var bytes: std.ArrayListUnmanaged(u8) = .empty;
        errdefer bytes.deinit(a);
        for (ids) |id| {
            if (id >= 151643) continue;
            if (id == 256) try bytes.append(a, 0xff) else if (id == 257) try bytes.append(a, 0) else try bytes.append(a, @intCast(id));
        }
        return bytes.toOwnedSlice(a);
    }
    fn special(_: *anyopaque) tokenizer_mod.SpecialTokens {
        return .{};
    }
    fn all(_: *anyopaque, a: A) ![]u32 {
        return a.dupe(u32, &.{ 151643, 151644, 151645, 151667, 151668 });
    }
    fn size(_: *anyopaque) usize {
        return 151936;
    }
    fn deinit(_: *anyopaque) void {}
    fn handle() tokenizer_mod.Tokenizer {
        return .{ .ptr = &sentinel, .vtable = &.{ .encode = encode, .encodeInto = append, .encodeForModel = model, .encodeGeneration = gen, .decode = decodeBytes, .specialTokens = special, .allSpecialTokenIds = all, .vocabSize = size, .deinit = deinit } };
    }
};

const span_test_config = gpt_model.Config{ .family = .qwen3, .vocab_size = 151936, .eos_token_id = 151645 };
fn spanTest(a: A, ids: []i32, finish: []const u8, cap: usize) !?FinalSpan {
    const text = try SpanTestTokenizer.handle().decode(a, ids);
    defer a.free(text);
    return parseThinkingFinal(a, SpanTestTokenizer.handle(), span_test_config, .{ .allocator = a, .text = text, .token_ids = ids, .tokens_used = ids.len, .finish_reason = finish }, cap, .{});
}

test "Memorilla thinking suffix preserves actual whitespace token slice and EOS" {
    const a = std.testing.allocator;
    var ids = [_]i32{ 151667, 'r', 151668, ' ', 'A', 151645 };
    const span = (try spanTest(a, &ids, "stop", 3)).?;
    defer a.free(span.text);
    try std.testing.expectEqualStrings(" A", span.text);
    try std.testing.expectEqual(@as(usize, 3), span.token_start);
    try std.testing.expectEqualSlices(i32, &.{ ' ', 'A', 151645 }, ids[span.token_start..span.token_end]);
    try std.testing.expect((try spanTest(a, &ids, "stop", 2)) == null);
    try std.testing.expect((try spanTest(a, &ids, "length", 3)) == null);
}

test "Memorilla thinking suffix rejects missing reopened hidden controls and corrupt bytes" {
    const a = std.testing.allocator;
    var missing = [_]i32{ 151667, 'r', 'A', 151645 };
    var reopened = [_]i32{ 151667, 151668, 151667, 151668, 'A', 151645 };
    var hidden = [_]i32{ 151667, 'r', 151644, 151668, 'A', 151645 };
    var corrupt = [_]i32{ 151667, 256, 151668, 'A', 151645 };
    var nul = [_]i32{ 151667, 'r', 151668, 257, 151645 };
    var early_eos = [_]i32{ 151667, 151645, 151668, 'A', 151645 };
    inline for (.{ &missing, &reopened, &hidden, &corrupt, &nul, &early_eos }) |ids|
        try std.testing.expect((try spanTest(a, ids, "stop", 48)) == null);
    try std.testing.expectError(error.Timeout, parseThinkingFinal(a, undefined, undefined, undefined, 8, .{ .deadline_ns = 0 }));
}

fn spanAllocationTest(a: A) !void {
    var ids = [_]i32{ 151667, 'r', 151668, 'A', 151645 };
    const span = (try spanTest(a, &ids, "stop", 8)).?;
    defer a.free(span.text);
}

test "Memorilla thinking suffix allocation failures release all temporary buffers" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, spanAllocationTest, .{});
}

test "Memorilla thinking preparation uses true template without a prefilled opener" {
    const a = std.testing.allocator;
    var template = try generation.ChatTemplate.init(a, "{% for m in messages %}{{ '<|im_start|>' + m.role + '\\n' + m.content + '<|im_end|>\\n' }}{% endfor %}{{ '<|im_start|>assistant\\n' }}{% if enable_thinking is false %}{{ '<think>\\n\\n</think>\\n\\n' }}{% endif %}", "", "", "", "");
    defer template.deinit();
    var config = span_test_config;
    config.norm_type = .rms_norm;
    config.position_encoding = .rope;
    config.activation = .silu;
    var plain = try decode.preparePlainText(a, SpanTestTokenizer.handle(), &template, config, "s", "u", .{}, .{});
    defer plain.deinit();
    var thinking = try decode.preparePlainTextWithMode(a, SpanTestTokenizer.handle(), &template, config, "s", "u", .{}, .{}, .bounded_thinking);
    defer thinking.deinit();
    try std.testing.expectEqual(.plain, plain.mode);
    try std.testing.expectEqual(.bounded_thinking, thinking.mode);
    try std.testing.expect(std.mem.endsWith(u8, plain.rendered, "<think>\n\n</think>\n\n"));
    try std.testing.expect(std.mem.endsWith(u8, thinking.rendered, "<|im_start|>assistant\n"));
}
