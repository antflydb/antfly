// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded memory candidate + independent source answer -> source verification.
//! Text hashes identify caller-supplied sources, not verified text/vector alignment.
const std = @import("std");
const platform = @import("antfly_platform");
const common = @import("memorilla_service.zig");
const source = @import("source_decoder_service.zig");
const artifact_fence = @import("memorilla_artifact_fence.zig");
const grounding = @import("../pipelines/memorilla_grounding.zig");
const generation = @import("../pipelines/generation.zig");
const qualified = @import("../models/memorilla_qualification.zig");
const memory = @import("../runtime/root.zig").tier.memory;
const manager_mod = @import("model_manager.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const A = std.mem.Allocator;
const RequestLimits = platform.resource_policy.InferenceRequestLimits;
pub const response_bytes: usize = 16 * 1024 * 1024;
pub const AnswerPath = enum { memory_verified, source_fallback_verified, abstained };
pub const Usage = struct {
    total_prompt_tokens: usize = 0,
    total_completion_tokens: usize = 0,
    verification_calls: usize = 0,
    /// Final selection/provenance, not whether source generation ran: an
    /// independent source answer is always generated before any publication.
    source_fallback_used: bool = false,

    fn add(self: *@This(), result: generation.GenerationResult) !void {
        self.total_prompt_tokens = try std.math.add(usize, self.total_prompt_tokens, result.prompt_tokens);
        self.total_completion_tokens = try std.math.add(usize, self.total_completion_tokens, result.tokens_used);
    }
};

pub const OwnedResult = struct {
    allocator: A,
    generation: generation.GenerationResult,
    encoder_execution_identity: [64]u8,
    decoder_execution_identity: [64]u8,
    module_sha256: [64]u8,
    document_count: usize,
    question_tokens: usize,
    document_text_sha256s: [][]const u8,
    answer_path: AnswerPath,
    usage: Usage,
    response_lease: memory.AdmissionLease,

    pub fn memoryTokens(self: *const @This()) usize {
        return if (self.answer_path == .memory_verified) 16 else 0;
    }

    pub fn deinit(self: *@This()) void {
        self.generation.deinit();
        grounding.freeHashes(self.allocator, self.document_text_sha256s);
        self.response_lease.release();
        self.* = undefined;
    }
};

pub const Phase = enum { verification, source_fallback };
pub const PhaseResult = struct {
    generation: generation.GenerationResult,
    decoder_execution_identity: [64]u8,
    response_lease: memory.AdmissionLease,

    pub fn deinit(self: *@This()) void {
        self.generation.deinit();
        self.response_lease.release();
        self.* = undefined;
    }
};

/// Internal synchronous executor seam. Each callback returns a fully retired
/// model request and transfers only its buffered generation/response lease.
pub const Executor = struct {
    ptr: *anyopaque,
    admit_response: *const fn (*anyopaque) anyerror!memory.AdmissionLease,
    preflight: *const fn (*anyopaque, A, []const u8, []const u8, usize, Control) anyerror!void,
    capture_authority: *const fn (*anyopaque, Control) anyerror!void,
    memory_candidate: *const fn (*anyopaque, Control) anyerror!common.OwnedResult,
    source_generate: *const fn (*anyopaque, A, Phase, []const u8, []const u8, usize, Control) anyerror!PhaseResult,
    final_authority: *const fn (*anyopaque, Control) anyerror!void,
};

const Metadata = struct {
    encoder: [64]u8,
    decoder: [64]u8,
    module: [64]u8,
    question_tokens: usize,
};

pub fn verifierTokenLimit(profile: common.Profile, limits: RequestLimits) !usize {
    try limits.validate();
    var cap = @min(grounding.verifier_max_tokens, profile.max_new_tokens);
    if (limits.max_output_tokens_per_item) |maximum| cap = @min(cap, maximum);
    if (cap == 0) return error.InferenceOutputTokensExceeded;
    return cap;
}

fn eligible(candidate: generation.GenerationResult) bool {
    return std.mem.eql(u8, candidate.finish_reason, "stop") and
        candidate.text.len <= grounding.max_candidate_bytes and
        std.unicode.utf8ValidateSlice(candidate.text) and
        std.mem.indexOfScalar(u8, candidate.text, 0) == null and
        !grounding.isBlank(candidate.text) and
        !grounding.isAbstention(candidate.text);
}

fn cloneGeneration(a: A, original: generation.GenerationResult) !generation.GenerationResult {
    const text = try a.dupe(u8, original.text);
    errdefer a.free(text);
    const ids = if (original.token_ids) |values| try a.dupe(i32, values) else null;
    errdefer if (ids) |values| a.free(values);
    const reasoning = if (original.reasoning_text) |values| try a.dupe(u8, values) else null;
    return .{ .allocator = a, .text = text, .reasoning_text = reasoning, .token_ids = ids, .prompt_tokens = original.prompt_tokens, .tokens_used = original.tokens_used, .finish_reason = "stop", .cached_prompt_tokens = original.cached_prompt_tokens, .timing_ms = original.timing_ms };
}

fn abstention(a: A) !generation.GenerationResult {
    const text = try a.dupe(u8, "UNKNOWN");
    errdefer a.free(text);
    const ids = try a.alloc(i32, 0);
    return .{ .allocator = a, .text = text, .token_ids = ids, .tokens_used = 0, .prompt_tokens = 0, .finish_reason = "abstained" };
}

fn verify(a: A, executor: Executor, question: []const u8, texts: []const []const u8, candidate: generation.GenerationResult, cap: usize, decoder_identity: [64]u8, control: Control, usage: *Usage) !bool {
    try control.check();
    const user = try grounding.verificationUser(a, question, texts, candidate.text);
    defer a.free(user);
    usage.verification_calls += 1;
    var verdict = try executor.source_generate(executor.ptr, a, .verification, grounding.verification_system_prompt, user, cap, control);
    defer verdict.deinit();
    try usage.add(verdict.generation);
    if (!std.mem.eql(u8, &verdict.decoder_execution_identity, &decoder_identity)) return error.UnsupportedMemorillaProfile;
    try control.check();
    return grounding.isSupported(verdict.generation.text, verdict.generation.finish_reason);
}

pub fn generateWithExecutor(a: A, profile: common.Profile, question: []const u8, documents: []const []const f32, source_texts: []const []const u8, identity: []const u8, max_new_tokens: usize, request_limits: RequestLimits, control: Control, executor: Executor) !OwnedResult {
    var result = try run(a, profile, question, documents, source_texts, identity, max_new_tokens, request_limits, control, executor);
    errdefer result.deinit();
    // run's phase results/prompts are retired before publication is allowed.
    try executor.final_authority(executor.ptr, control);
    try control.check();
    return result;
}

fn run(a: A, profile: common.Profile, question: []const u8, documents: []const []const f32, source_texts: []const []const u8, identity: []const u8, max_new_tokens: usize, request_limits: RequestLimits, control: Control, executor: Executor) !OwnedResult {
    try control.check();
    try common.validateInputs(profile, question, documents, identity, max_new_tokens);
    try common.validateRequestLimits(request_limits, documents.len, max_new_tokens);
    try grounding.validateSources(source_texts, documents.len);
    const canonical = try qualified.canonicalQuestion(question);
    const verifier_cap = try verifierTokenLimit(profile, request_limits);
    var response = try executor.admit_response(executor.ptr);
    var transferred = false;
    defer if (!transferred) response.release();
    const hashes = try grounding.hashes(a, source_texts);
    errdefer grounding.freeHashes(a, hashes);
    const fallback_user = try grounding.answerUser(a, canonical, source_texts);
    defer a.free(fallback_user);
    // Fail bounds/identity/publication/tokenizer checks before memory GPU work.
    try executor.preflight(executor.ptr, a, grounding.fallback_system_prompt, fallback_user, max_new_tokens, control);
    try control.check();
    try executor.capture_authority(executor.ptr, control);
    var usage: Usage = .{};
    var candidate: ?generation.GenerationResult = null;
    defer if (candidate) |*value| value.deinit();
    const metadata = blk: {
        var original = try executor.memory_candidate(executor.ptr, control);
        defer original.deinit();
        try usage.add(original.generation);
        if (original.document_count != documents.len) return error.InvalidMemorillaReservation;
        if (!std.mem.eql(u8, &original.encoder_execution_identity, &qualified.executionIdentity(.qwen3_encoder_f32_v1)) or
            !std.mem.eql(u8, &original.decoder_execution_identity, &qualified.executionIdentity(.qwen3_decoder_f32_kv_f32_v1)))
            return error.UnsupportedMemorillaProfile;
        if (eligible(original.generation)) candidate = try cloneGeneration(a, original.generation);
        break :blk Metadata{ .encoder = original.encoder_execution_identity, .decoder = original.decoder_execution_identity, .module = original.module_sha256, .question_tokens = original.question_tokens };
    };
    try control.check();
    // Generate without exposing the memory candidate, even when memory looks
    // plausible. A verifier's SUPPORTED label cannot by itself promote it.
    var source_answer: ?generation.GenerationResult = null;
    defer if (source_answer) |*value| value.deinit();
    {
        var independent = try executor.source_generate(executor.ptr, a, .source_fallback, grounding.fallback_system_prompt, fallback_user, max_new_tokens, control);
        defer independent.deinit();
        try usage.add(independent.generation);
        if (!std.mem.eql(u8, &independent.decoder_execution_identity, &metadata.decoder)) return error.UnsupportedMemorillaProfile;
        if (eligible(independent.generation)) source_answer = try cloneGeneration(a, independent.generation);
    }
    try control.check();
    const agrees = if (candidate) |memory_answer|
        if (source_answer) |source_value| grounding.answersAgree(memory_answer.text, source_value.text) else false
    else
        false;
    if (!agrees) {
        if (candidate) |*value| value.deinit();
        candidate = null;
    }
    var path: AnswerPath = .abstained;
    const source_supported = if (source_answer) |value|
        try verify(a, executor, canonical, source_texts, value, verifier_cap, metadata.decoder, control, &usage)
    else
        false;
    if (source_supported) {
        if (agrees) {
            path = .memory_verified;
        } else {
            candidate = source_answer;
            source_answer = null;
            path = .source_fallback_verified;
        }
    } else {
        if (candidate) |*value| value.deinit();
        candidate = null;
    }
    usage.source_fallback_used = path != .memory_verified;
    if (candidate == null) candidate = try abstention(a);
    try control.check();
    const result = OwnedResult{ .allocator = a, .generation = candidate.?, .encoder_execution_identity = metadata.encoder, .decoder_execution_identity = metadata.decoder, .module_sha256 = metadata.module, .document_count = documents.len, .question_tokens = metadata.question_tokens, .document_text_sha256s = hashes, .answer_path = path, .usage = usage, .response_lease = response };
    candidate = null;
    transferred = true;
    return result;
}

const Context = struct {
    a: A,
    io: std.Io,
    manager: *manager_mod.ModelManager,
    profile: common.Profile,
    paths: common.Paths,
    question: []const u8,
    documents: []const []const f32,
    identity: []const u8,
    max_new_tokens: usize,
    limits: memory.Limits,
    request_limits: RequestLimits,
    artifact_snapshot: ?artifact_fence.Snapshot = null,

    fn admit(raw: *anyopaque) !memory.AdmissionLease {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.manager.acquireRunResourceAmounts(.cpu, self.limits, .{ .host_scratch_bytes = response_bytes });
    }
    fn preflight(raw: *anyopaque, a: A, system: []const u8, user: []const u8, cap: usize, control: Control) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return source.preflight(a, self.io, self.manager, self.paths.decoder, system, user, cap, self.limits, self.request_limits, control);
    }
    fn memoryCandidate(raw: *anyopaque, control: Control) !common.OwnedResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return common.generateWithRequestLimits(self.a, self.io, self.manager, self.profile, self.paths, self.question, self.documents, self.identity, self.max_new_tokens, self.limits, self.request_limits, control);
    }
    fn captureAuthority(raw: *anyopaque, control: Control) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.artifact_snapshot = try artifact_fence.capture(self.a, self.io, self.paths.encoder, self.paths.decoder, control);
    }
    fn sourceGenerate(raw: *anyopaque, a: A, _: Phase, system: []const u8, user: []const u8, cap: usize, control: Control) !PhaseResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const value = try source.generate(a, self.io, self.manager, self.paths.decoder, system, user, cap, self.limits, self.request_limits, control);
        return .{ .generation = value.generation, .decoder_execution_identity = value.decoder_execution_identity, .response_lease = value.response_lease };
    }
    fn finalAuthority(raw: *anyopaque, control: Control) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try control.check();
        const expected = self.artifact_snapshot orelse return error.InvalidMemorillaReservation;
        try artifact_fence.verify(self.a, self.io, self.paths.encoder, self.paths.decoder, expected, control);
        try control.check();
    }
};

pub fn generate(a: A, io: std.Io, manager: *manager_mod.ModelManager, profile: common.Profile, paths: common.Paths, question: []const u8, documents: []const []const f32, source_texts: []const []const u8, identity: []const u8, max_new_tokens: usize, limits: memory.Limits, request_limits: RequestLimits, control: Control) !OwnedResult {
    var context = Context{ .a = a, .io = io, .manager = manager, .profile = profile, .paths = paths, .question = question, .documents = documents, .identity = identity, .max_new_tokens = max_new_tokens, .limits = limits, .request_limits = request_limits };
    return generateWithExecutor(a, profile, question, documents, source_texts, identity, max_new_tokens, request_limits, control, .{ .ptr = &context, .admit_response = Context.admit, .preflight = Context.preflight, .capture_authority = Context.captureAuthority, .memory_candidate = Context.memoryCandidate, .source_generate = Context.sourceGenerate, .final_authority = Context.finalAuthority });
}

const TestStep = struct {
    phase: Phase,
    text: []const u8,
    finish: []const u8 = "stop",
    prompt: usize = 20,
    tokens: usize = 2,
    failure: ?anyerror = null,
    cancel_after: bool = false,
    identity_valid: bool = true,
};
const TestEvent = enum { preflight, capture_authority, memory, verification, fallback, final_authority };
const TestScript = struct {
    a: A,
    controller: memory.AdmissionController = .{},
    steps: []const TestStep = &.{},
    cursor: usize = 0,
    events: [8]TestEvent = undefined,
    event_count: usize = 0,
    memory_text: []const u8 = "Alice",
    source_text: []const u8 = "Alice owns the badge.",
    expected_verified_answer: ?[]const u8 = null,
    memory_finish: []const u8 = "stop",
    memory_failure: ?anyerror = null,
    memory_identity_valid: bool = true,
    cancel_after_memory: bool = false,
    cancelled: bool = false,
    preflight_failure: ?anyerror = null,
    capture_failure: ?anyerror = null,
    final_failure: ?anyerror = null,
    expected_verifier_cap: usize = 8,
    expected_answer_cap: usize = 16,

    const profile = common.Profile{ .name = "test", .encoder = "encoder", .decoder = "decoder", .memory_module_path = "memory", .max_documents = 64, .max_new_tokens = 32 };
    const limits = memory.Limits{ .host_limit_bytes = 64 * 1024 * 1024, .backend_limit_bytes = 64 * 1024 * 1024, .combined_limit_bytes = 128 * 1024 * 1024 };

    fn event(self: *@This(), value: TestEvent) void {
        self.events[self.event_count] = value;
        self.event_count += 1;
    }
    fn onlyResponseHeld(self: *@This()) !void {
        try std.testing.expectEqual(response_bytes, self.controller.admitted.host_scratch_bytes);
        try std.testing.expectEqual(@as(usize, 0), self.controller.admitted.backendTotalBytes());
    }
    fn lease(self: *@This(), bytes: usize) !memory.AdmissionLease {
        return self.controller.tryAcquire(.cpu, limits, .{ .host_scratch_bytes = bytes }, false);
    }
    fn gen(self: *@This(), text: []const u8, finish: []const u8, prompt: usize, tokens: usize) !generation.GenerationResult {
        const owned = try self.a.dupe(u8, text);
        errdefer self.a.free(owned);
        const ids = try self.a.alloc(i32, tokens);
        @memset(ids, 42);
        return .{ .allocator = self.a, .text = owned, .token_ids = ids, .prompt_tokens = prompt, .tokens_used = tokens, .finish_reason = finish };
    }
    fn admit(raw: *anyopaque) !memory.AdmissionLease {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.lease(response_bytes);
    }
    fn preflight(raw: *anyopaque, _: A, system: []const u8, user: []const u8, cap: usize, control: Control) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.event(.preflight);
        try self.onlyResponseHeld();
        try control.check();
        try std.testing.expectEqualStrings(grounding.fallback_system_prompt, system);
        try std.testing.expect(std.mem.indexOf(u8, user, "candidate_answer") == null);
        try std.testing.expectEqual(self.expected_answer_cap, cap);
        if (self.preflight_failure) |failure| return failure;
    }
    fn memoryCandidate(raw: *anyopaque, control: Control) !common.OwnedResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.event(.memory);
        try self.onlyResponseHeld();
        try control.check();
        if (self.memory_failure) |failure| return failure;
        var held = try self.lease(64);
        errdefer held.release();
        const value = try self.gen(self.memory_text, self.memory_finish, 10, 2);
        if (self.cancel_after_memory) self.cancelled = true;
        return .{ .generation = value, .response_lease = held, .encoder_execution_identity = qualified.executionIdentity(.qwen3_encoder_f32_v1), .decoder_execution_identity = if (self.memory_identity_valid) qualified.executionIdentity(.qwen3_decoder_f32_kv_f32_v1) else @splat('0'), .module_sha256 = @splat('a'), .document_count = 1, .question_tokens = 5 };
    }
    fn captureAuthority(raw: *anyopaque, control: Control) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.event(.capture_authority);
        try self.onlyResponseHeld();
        try control.check();
        if (self.capture_failure) |failure| return failure;
    }
    fn sourceGenerate(raw: *anyopaque, _: A, phase: Phase, system: []const u8, user: []const u8, cap: usize, control: Control) !PhaseResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.event(if (phase == .verification) .verification else .fallback);
        try self.onlyResponseHeld(); // Prior result and lease MUST already be gone.
        try control.check();
        if (self.cursor >= self.steps.len) return error.UnexpectedVerifiedPhase;
        const step = self.steps[self.cursor];
        self.cursor += 1;
        try std.testing.expectEqual(step.phase, phase);
        try std.testing.expectEqual(if (phase == .verification) self.expected_verifier_cap else self.expected_answer_cap, cap);
        try std.testing.expectEqualStrings(if (phase == .verification) grounding.verification_system_prompt else grounding.fallback_system_prompt, system);
        try std.testing.expect((std.mem.indexOf(u8, user, "candidate_answer") != null) == (phase == .verification));
        if (phase == .source_fallback) {
            // Unknown fields are rejected: independent source generation gets
            // only the question and documents, never a memory proposal.
            var parsed = try std.json.parseFromSlice(struct { question: []const u8, documents: []const []const u8 }, self.a, user, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings(self.source_text, parsed.value.documents[0]);
        } else if (self.expected_verified_answer) |expected| {
            var parsed = try std.json.parseFromSlice(struct { question: []const u8, documents: []const []const u8, candidate_answer: []const u8 }, self.a, user, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings(expected, parsed.value.candidate_answer);
        }
        if (step.failure) |failure| return failure;
        var held = try self.lease(64);
        errdefer held.release();
        const value = try self.gen(step.text, step.finish, step.prompt, step.tokens);
        if (step.cancel_after) self.cancelled = true;
        return .{ .generation = value, .response_lease = held, .decoder_execution_identity = if (step.identity_valid) qualified.executionIdentity(.qwen3_decoder_f32_kv_f32_v1) else @splat('0') };
    }
    fn finalAuthority(raw: *anyopaque, control: Control) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.event(.final_authority);
        try self.onlyResponseHeld();
        try control.check();
        if (self.final_failure) |failure| return failure;
    }
    fn check(raw: ?*anyopaque) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (self.cancelled) return error.Cancelled;
    }
    fn executor(self: *@This()) Executor {
        return .{ .ptr = self, .admit_response = admit, .preflight = preflight, .capture_authority = captureAuthority, .memory_candidate = memoryCandidate, .source_generate = sourceGenerate, .final_authority = finalAuthority };
    }
    fn execute(self: *@This()) !OwnedResult {
        var vector: [qualified.width]f32 = @splat(0);
        vector[0] = 1;
        const identity = qualified.executionIdentity(.qwen3_encoder_f32_v1);
        return generateWithExecutor(self.a, profile, "Who owns the badge?", &.{&vector}, &.{self.source_text}, &identity, self.expected_answer_cap, .{ .max_output_tokens_per_item = 32 }, .{ .ptr = self, .check_fn = check }, self.executor());
    }
    fn expectReleased(self: *@This()) !void {
        try std.testing.expectEqual(@as(usize, 0), self.controller.admitted.hostTotalBytes());
    }
};

test "Memorilla verified memory approval retires intermediates preserves provenance and admits final response" {
    var mock = TestScript{ .a = std.testing.allocator, .expected_verified_answer = "Alice", .steps = &.{ .{ .phase = .source_fallback, .text = "Alice", .prompt = 20 }, .{ .phase = .verification, .text = "SUPPORTED", .prompt = 30 } } };
    var result = try mock.execute();
    try std.testing.expectEqual(AnswerPath.memory_verified, result.answer_path);
    try std.testing.expectEqual(@as(usize, 16), result.memoryTokens());
    try std.testing.expectEqualStrings("Alice", result.generation.text);
    try std.testing.expectEqual(@as(usize, 60), result.usage.total_prompt_tokens);
    try std.testing.expectEqual(@as(usize, 6), result.usage.total_completion_tokens);
    try std.testing.expectEqual(@as(usize, 1), result.usage.verification_calls);
    try std.testing.expect(!result.usage.source_fallback_used);
    const expected = try grounding.hashes(std.testing.allocator, &.{"Alice owns the badge."});
    defer grounding.freeHashes(std.testing.allocator, expected);
    try std.testing.expectEqualStrings(expected[0], result.document_text_sha256s[0]);
    try mock.onlyResponseHeld();
    try std.testing.expectEqualSlices(TestEvent, &.{ .preflight, .capture_authority, .memory, .fallback, .verification, .final_authority }, mock.events[0..mock.event_count]);
    result.deinit();
    try mock.expectReleased();
}

test "Memorilla verified unsupported memory uses independently verified source fallback with cumulative usage" {
    var mock = TestScript{ .a = std.testing.allocator, .memory_text = "Bob", .steps = &.{
        .{ .phase = .source_fallback, .text = "Alice", .prompt = 30, .tokens = 3 },
        .{ .phase = .verification, .text = "SUPPORTED", .prompt = 40 },
    } };
    var result = try mock.execute();
    try std.testing.expectEqual(AnswerPath.source_fallback_verified, result.answer_path);
    try std.testing.expectEqual(@as(usize, 0), result.memoryTokens());
    try std.testing.expectEqualStrings("Alice", result.generation.text);
    try std.testing.expectEqual(@as(usize, 80), result.usage.total_prompt_tokens);
    try std.testing.expectEqual(@as(usize, 7), result.usage.total_completion_tokens);
    try std.testing.expectEqual(@as(usize, 1), result.usage.verification_calls);
    try std.testing.expect(result.usage.source_fallback_used);
    result.deinit();
    try mock.expectReleased();
    // Observed DEV failure: a station was incorrectly judged SUPPORTED as an
    // owner. The memory answer is never sent for approval in this flow.
    var false_approval = TestScript{ .a = std.testing.allocator, .memory_text = "Neral", .source_text = "At Neral station the copper badge belongs to Tovik.", .expected_verified_answer = "Tovik", .steps = &.{
        .{ .phase = .source_fallback, .text = "Tovik" },
        .{ .phase = .verification, .text = "SUPPORTED" },
    } };
    var corrected = try false_approval.execute();
    try std.testing.expectEqualStrings("Tovik", corrected.generation.text);
    try std.testing.expectEqual(AnswerPath.source_fallback_verified, corrected.answer_path);
    try std.testing.expectEqual(@as(usize, 1), corrected.usage.verification_calls);
    corrected.deinit();
    try false_approval.expectReleased();
}

test "Memorilla verified UNKNOWN or truncated memory still attempts sources without verifying invalid candidate" {
    for ([_]struct { text: []const u8, finish: []const u8 }{
        .{ .text = "UNKNOWN", .finish = "stop" },              .{ .text = "Alice", .finish = "length" },
        .{ .text = "", .finish = "stop" },                     .{ .text = "\xff", .finish = "stop" },
        .{ .text = "\xc2\xa0\xe2\x80\x80", .finish = "stop" },
    }) |candidate| {
        var mock = TestScript{ .a = std.testing.allocator, .memory_text = candidate.text, .memory_finish = candidate.finish, .steps = &.{ .{ .phase = .source_fallback, .text = "Alice" }, .{ .phase = .verification, .text = "SUPPORTED" } } };
        var result = try mock.execute();
        try std.testing.expectEqual(AnswerPath.source_fallback_verified, result.answer_path);
        try std.testing.expectEqual(@as(usize, 1), result.usage.verification_calls);
        result.deinit();
        try mock.expectReleased();
    }
}

test "Memorilla verified unapproved fallback abstains without inventing generated token IDs" {
    for ([_]struct { fallback: []const u8, finish: []const u8, label: ?[]const u8, label_finish: []const u8 }{
        .{ .fallback = "UNKNOWN", .finish = "stop", .label = null, .label_finish = "stop" },
        .{ .fallback = "Alice", .finish = "length", .label = null, .label_finish = "stop" },
        .{ .fallback = "Alice", .finish = "stop", .label = "UNSUPPORTED", .label_finish = "stop" },
        .{ .fallback = "Alice", .finish = "stop", .label = "SUPPORTED because", .label_finish = "stop" },
        .{ .fallback = "Alice", .finish = "stop", .label = "SUPPORTED", .label_finish = "length" },
    }) |entry| {
        var steps = [_]TestStep{ .{ .phase = .source_fallback, .text = entry.fallback, .finish = entry.finish }, .{ .phase = .verification, .text = entry.label orelse "", .finish = entry.label_finish } };
        var mock = TestScript{ .a = std.testing.allocator, .memory_text = "Alice", .steps = steps[0..if (entry.label != null) @as(usize, 2) else 1] };
        var result = try mock.execute();
        try std.testing.expectEqual(AnswerPath.abstained, result.answer_path);
        try std.testing.expectEqualStrings("UNKNOWN", result.generation.text);
        try std.testing.expectEqualStrings("abstained", result.generation.finish_reason);
        try std.testing.expectEqual(@as(usize, 0), result.generation.token_ids.?.len);
        try std.testing.expectEqual(@as(usize, 0), result.generation.tokens_used);
        try std.testing.expectEqual(@as(usize, 0), result.generation.prompt_tokens);
        try std.testing.expectEqual(@as(usize, 0), result.memoryTokens());
        result.deinit();
        try mock.expectReleased();
    }
}

test "Memorilla verified preflight and phase errors never become silent fallback" {
    {
        var mock = TestScript{ .a = std.testing.allocator, .preflight_failure = error.InferenceInputTokensExceeded };
        try std.testing.expectError(error.InferenceInputTokensExceeded, mock.execute());
        try std.testing.expectEqualSlices(TestEvent, &.{.preflight}, mock.events[0..mock.event_count]);
        try mock.expectReleased();
    }
    {
        var mock = TestScript{ .a = std.testing.allocator, .capture_failure = error.ModelArtifactsChanging };
        try std.testing.expectError(error.ModelArtifactsChanging, mock.execute());
        try std.testing.expectEqualSlices(TestEvent, &.{ .preflight, .capture_authority }, mock.events[0..mock.event_count]);
        try std.testing.expectEqual(@as(usize, 0), mock.cursor);
        try mock.expectReleased();
    }
    for ([_]anyerror{ error.Cancelled, error.ResourceTemporarilyUnavailable, error.ModelArtifactsChanging }) |failure| {
        var mock = TestScript{ .a = std.testing.allocator, .memory_failure = failure };
        try std.testing.expectError(failure, mock.execute());
        try std.testing.expectEqual(@as(usize, 0), mock.cursor);
        try mock.expectReleased();
        var verify_mock = TestScript{ .a = std.testing.allocator, .steps = &.{ .{ .phase = .source_fallback, .text = "Alice" }, .{ .phase = .verification, .text = "", .failure = failure } } };
        try std.testing.expectError(failure, verify_mock.execute());
        try std.testing.expectEqual(@as(usize, 2), verify_mock.cursor);
        try verify_mock.expectReleased();
    }
}

test "Memorilla verified late cancellation publication and decoder identity failures retire all responses" {
    var memory_cancel = TestScript{ .a = std.testing.allocator, .cancel_after_memory = true };
    try std.testing.expectError(error.Cancelled, memory_cancel.execute());
    try memory_cancel.expectReleased();
    var verify_cancel = TestScript{ .a = std.testing.allocator, .steps = &.{ .{ .phase = .source_fallback, .text = "Alice" }, .{ .phase = .verification, .text = "SUPPORTED", .cancel_after = true } } };
    try std.testing.expectError(error.Cancelled, verify_cancel.execute());
    try verify_cancel.expectReleased();
    var drift = TestScript{ .a = std.testing.allocator, .final_failure = error.ModelArtifactsChanging, .steps = &.{ .{ .phase = .source_fallback, .text = "Alice" }, .{ .phase = .verification, .text = "SUPPORTED" } } };
    try std.testing.expectError(error.ModelArtifactsChanging, drift.execute());
    try drift.expectReleased();
    var identity = TestScript{ .a = std.testing.allocator, .steps = &.{ .{ .phase = .source_fallback, .text = "Alice" }, .{ .phase = .verification, .text = "SUPPORTED", .identity_valid = false } } };
    try std.testing.expectError(error.UnsupportedMemorillaProfile, identity.execute());
    try identity.expectReleased();
    var memory_identity = TestScript{ .a = std.testing.allocator, .memory_identity_valid = false };
    try std.testing.expectError(error.UnsupportedMemorillaProfile, memory_identity.execute());
    try memory_identity.expectReleased();
}

test "Memorilla verified verifier cap is narrowed by profile and operator without widening" {
    try std.testing.expectEqual(@as(usize, 8), try verifierTokenLimit(TestScript.profile, .{}));
    var profile = TestScript.profile;
    profile.max_new_tokens = 3;
    try std.testing.expectEqual(@as(usize, 3), try verifierTokenLimit(profile, .{}));
    try std.testing.expectEqual(@as(usize, 2), try verifierTokenLimit(profile, .{ .max_output_tokens_per_item = 2 }));
    try std.testing.expectError(error.InvalidInferenceConfig, verifierTokenLimit(profile, .{ .max_output_tokens_per_item = 0 }));
}

test "Memorilla verified oversized memory candidate skips verification and failed fallback cannot publish" {
    const large: [grounding.max_candidate_bytes + 1]u8 = @splat('x');
    var oversized = TestScript{ .a = std.testing.allocator, .memory_text = &large, .steps = &.{.{ .phase = .source_fallback, .text = "UNKNOWN" }} };
    var result = try oversized.execute();
    try std.testing.expectEqual(AnswerPath.abstained, result.answer_path);
    try std.testing.expectEqual(@as(usize, 0), result.usage.verification_calls);
    result.deinit();
    try oversized.expectReleased();
    var unavailable = TestScript{ .a = std.testing.allocator, .memory_text = "UNKNOWN", .steps = &.{.{ .phase = .source_fallback, .text = "", .failure = error.ResourceTemporarilyUnavailable }} };
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, unavailable.execute());
    try unavailable.expectReleased();
    var identity = TestScript{ .a = std.testing.allocator, .memory_text = "UNKNOWN", .steps = &.{.{ .phase = .source_fallback, .text = "Alice", .identity_valid = false }} };
    try std.testing.expectError(error.UnsupportedMemorillaProfile, identity.execute());
    try identity.expectReleased();
}

test "Memorilla verified source alignment and output bounds reject before admission or models" {
    var vector: [qualified.width]f32 = @splat(0);
    vector[0] = 1;
    const identity = qualified.executionIdentity(.qwen3_encoder_f32_v1);
    var mock = TestScript{ .a = std.testing.allocator };
    try std.testing.expectError(error.InvalidMemorillaSourceCount, generateWithExecutor(mock.a, TestScript.profile, "q", &.{&vector}, &.{}, &identity, 1, .{}, .{}, mock.executor()));
    try std.testing.expectError(error.InvalidMemorillaSourceText, generateWithExecutor(mock.a, TestScript.profile, "q", &.{&vector}, &.{"\xc2\xa0\xe2\x80\x80"}, &identity, 1, .{}, .{}, mock.executor()));
    try std.testing.expectError(error.InferenceOutputTokensExceeded, generateWithExecutor(mock.a, TestScript.profile, "q", &.{&vector}, &.{"Alice owns the badge."}, &identity, 2, .{ .max_output_tokens_per_item = 1 }, .{}, mock.executor()));
    try std.testing.expectEqual(@as(usize, 0), mock.event_count);
    try mock.expectReleased();
}

fn testAllocationUnwind(a: A) !void {
    var mock = TestScript{ .a = a, .memory_text = "Wrong", .steps = &.{
        .{ .phase = .source_fallback, .text = "Alice" },
        .{ .phase = .verification, .text = "SUPPORTED" },
    } };
    defer mock.controller.deinit();
    defer std.debug.assert(mock.controller.admitted.hostTotalBytes() == 0);
    var result = try mock.execute();
    result.deinit();
}

test "Memorilla verified every allocation failure unwinds buffered generations hashes prompts and admission" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testAllocationUnwind, .{});
}
