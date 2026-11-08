// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Trusted local DEV diagnostic, not a public route or production prompt override.
//! Every row starts fresh F32 KV; source generation never receives a candidate.
const std = @import("std");
const inference = @import("inference_internal");
const service = inference.server.source_decoder_service;
const decode = inference.pipelines.memorilla_generation;
const grounding = inference.pipelines.memorilla_grounding;
const qualified = inference.models.memorilla_qualification;
const memory = inference.runtime.tier.memory;
const Control = inference.execution_control.InferenceExecutionControl;
const A = std.mem.Allocator;
const Case = struct {
    id: []const u8,
    question: []const u8,
    documents: []const []const u8,
    answer_system: ?[]const u8 = null,
    verification_system: ?[]const u8 = null,
    /// Explicit local judge input, not a gold verdict or a source answer.
    judge_candidate: ?[]const u8 = null,
};
const Job = struct {
    schema: []const u8,
    scope: []const u8,
    answer_system: []const u8,
    verification_system: []const u8,
    max_new_tokens: usize = 48,
    cases: []const Case,
    mode: decode.PlainMode = .plain,
};
const Args = struct { decoder: []const u8 = "", input: []const u8 = "", budget_gib: usize = 96 };
fn parse(args: []const []const u8) !Args {
    var out = Args{};
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const key = args[i];
        i += 1;
        if (i == args.len) return error.InvalidArguments;
        if (std.mem.eql(u8, key, "--decoder")) out.decoder = args[i] else if (std.mem.eql(u8, key, "--input")) out.input = args[i] else if (std.mem.eql(u8, key, "--budget-gib")) out.budget_gib = try std.fmt.parseInt(usize, args[i], 10) else return error.InvalidArguments;
    }
    if (out.decoder.len == 0 or out.input.len == 0 or out.budget_gib == 0 or out.budget_gib > 112) return error.InvalidArguments;
    return out;
}
fn emit(a: A, io: std.Io, value: anytype) !void {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    var buffer: [4096]u8 = undefined;
    var writer = std.Io.File.stdout().writerStreaming(io, &buffer);
    try writer.interface.writeAll(bytes);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

const Sampling = struct {
    const Step = struct { position: usize, argmax_id: i64, chosen_id: i64, forced_format: bool };
    steps: [128]Step = undefined,
    count: usize = 0,
    allocator: A,
    input_ids: []const i64 = &.{},
    rendered: ?[]const u8 = null,
    physical: ?decode.PhysicalResources = null,
    fn prepared(raw: ?*anyopaque, text: *const decode.PreparedPlainText) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.input_ids = try self.allocator.dupe(i64, text.input_ids);
        self.rendered = try self.allocator.dupe(u8, text.rendered);
    }
    fn resources(raw: *anyopaque, value: decode.PhysicalResources) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.physical = value;
    }
    fn deinit(self: *@This()) void {
        self.allocator.free(self.input_ids);
        if (self.rendered) |text| self.allocator.free(text);
    }
    fn record(raw: *anyopaque, position: usize, predicted: i64, chosen: i64, forced: bool) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (position != self.count or self.count == self.steps.len) return error.InvalidSourceDecoderFixture;
        self.steps[self.count] = .{ .position = position, .argmax_id = predicted, .chosen_id = chosen, .forced_format = forced };
        self.count += 1;
    }
};

fn thinkingCase(a: A, io: std.Io, models: *inference.server.model_manager.ModelManager, selected: inference.model_selection.ResolvedModel, limits: memory.Limits, control: Control, job: Job, case: Case) !void {
    const judge = case.judge_candidate != null;
    const system = if (judge) case.verification_system orelse job.verification_system else case.answer_system orelse job.answer_system;
    const user = if (case.judge_candidate) |candidate|
        try grounding.verificationUser(a, case.question, case.documents, candidate)
    else
        try grounding.answerUser(a, case.question, case.documents);
    defer a.free(user);
    var sampling: Sampling = .{ .allocator = a };
    defer sampling.deinit();
    var result = try service.generateWithOptions(a, io, models, selected, system, user, 128, limits, .{}, control, .{
        .mode = .bounded_thinking,
        .final_token_cap = if (judge) 8 else @min(job.max_new_tokens, 48),
        .context = &sampling,
        .prepared_text = Sampling.prepared,
        .trace = .{ .context = &sampling, .sampled = Sampling.record, .physical_resources = Sampling.resources },
    });
    defer result.deinit();
    try emit(a, io, .{
        .event = "thinking_case",
        .scope = "mechanism diagnostic only; raw trajectories and strict format checks, no publication or qualification",
        .id = case.id,
        .phase = if (judge) "judge" else "source",
        .judge_candidate = case.judge_candidate,
        .question = case.question,
        .documents = case.documents,
        .system = system,
        .user = user,
        .input_ids = sampling.input_ids,
        .rendered_prompt = sampling.rendered,
        .physical_resources = sampling.physical,
        .raw = .{ .text = result.generation.text, .token_ids = result.generation.token_ids, .finish_reason = result.generation.finish_reason, .prompt_tokens = result.generation.prompt_tokens, .completion_tokens = result.generation.tokens_used },
        .thinking = result.thinking,
        .final_span_valid = result.final_span != null,
        .final_text = if (result.final_span) |span| span.text else null,
        .final_suffix_token_ids = result.finalTokenIds(),
        .final_token_start = if (result.final_span) |span| @as(?usize, span.token_start) else null,
        .strict_supported = if (judge) if (result.final_span) |span| grounding.isSupported(span.text, result.generation.finish_reason) else false else @as(?bool, null),
        .steps = sampling.steps[0..sampling.count],
    });
}

pub fn main(init: std.process.Init) !void {
    const a = std.heap.c_allocator;
    const args = try init.minimal.args.toSlice(a);
    defer a.free(args);
    const opts = try parse(args);
    const cap = try std.math.mul(usize, opts.budget_gib, 1024 * 1024 * 1024);
    const limits = memory.Limits{ .host_limit_bytes = cap, .backend_limit_bytes = cap, .combined_limit_bytes = cap, .kv_limit_bytes = 2 * 1024 * 1024 * 1024, .scratch_limit_bytes = 8 * 1024 * 1024 * 1024 };
    const path = try std.Io.Dir.cwd().realPathFileAlloc(init.io, opts.decoder, a);
    const selected = try inference.model_selection.fromDirectory(a, init.io, path);
    defer a.free(selected.path);
    var sessions = inference.backends.SessionManager.initWithIo(a, init.io);
    sessions.preferred_backends = &.{.cuda};
    sessions.required_backend = .cuda;
    sessions.native_call_policy = .in_process;
    var models = inference.server.model_manager.ModelManager.init(a, sessions);
    defer models.deinit();
    models.configureServingPolicy(.{});
    models.configureAdmissionLimits(limits);
    models.configureProcessMemoryLimit(cap, .explicit);
    models.configureModelCache(600_000, 2);
    try models.ensureResourceOwnerReady();
    var fixture_lease = try models.acquireRunResourceAmounts(.cpu, limits, .{ .host_scratch_bytes = 32 * 1024 * 1024 });
    defer fixture_lease.release();
    const bytes = try inference.util.c_file.readFileMax(a, opts.input, 8 * 1024 * 1024);
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(Job, a, bytes, .{});
    defer parsed.deinit();
    const job = parsed.value;
    if (!std.mem.eql(u8, job.schema, "antfly.memorilla.source-qa-probe.v1") or !std.mem.eql(u8, job.scope, "development-only") or
        job.cases.len == 0 or job.cases.len > 128 or job.max_new_tokens == 0 or job.max_new_tokens > 128 or
        job.answer_system.len > 16 * 1024 or job.verification_system.len > 16 * 1024) return error.InvalidArguments;
    // Parse schema intentionally has no gold answer/support/annotation fields.
    const control = Control{ .io = init.io, .native_call_policy = .in_process };
    for (job.cases) |case| {
        if (case.id.len == 0 or case.id.len > 128 or case.question.len > 8192) return error.InvalidArguments;
        if ((case.answer_system orelse job.answer_system).len > 16 * 1024 or
            (case.verification_system orelse job.verification_system).len > 16 * 1024) return error.InvalidArguments;
        try grounding.validateSources(case.documents, case.documents.len);
        const canonical = try qualified.canonicalQuestion(case.question);
        if (!std.mem.eql(u8, canonical, case.question)) return error.NonCanonicalMemorillaQuestion;
        if (case.judge_candidate) |candidate| {
            if (job.mode != .bounded_thinking or candidate.len == 0 or candidate.len > grounding.max_candidate_bytes or
                !std.unicode.utf8ValidateSlice(candidate) or std.mem.indexOfScalar(u8, candidate, 0) != null) return error.InvalidArguments;
        }
    }
    const identity = qualified.executionIdentity(.qwen3_decoder_f32_kv_f32_v1);
    const job_hash = inference.models.memorilla.digest(bytes);
    try emit(a, init.io, .{ .event = "protocol", .scope = job.scope, .job_sha256 = job_hash[0..], .cases = job.cases.len, .decoder_execution_identity = identity[0..], .memory_rows = @as(usize, 0), .answer_system = job.answer_system, .verification_system = job.verification_system, .max_new_tokens = job.max_new_tokens, .verifier_max_tokens = grounding.verifier_max_tokens, .verifier_scope = "diagnostic also probes UNKNOWN/truncated answers; would_publish still applies production eligibility" });
    for (job.cases) |case| {
        if (job.mode == .bounded_thinking) {
            try thinkingCase(a, init.io, &models, selected, limits, control, job, case);
            continue;
        }
        const answer_system = case.answer_system orelse job.answer_system;
        const verification_system = case.verification_system orelse job.verification_system;
        const user = try grounding.answerUser(a, case.question, case.documents);
        defer a.free(user);
        const output = blk: {
            var answer = try service.generate(a, init.io, &models, selected, answer_system, user, job.max_new_tokens, limits, .{}, control);
            defer answer.deinit();
            const text = try a.dupe(u8, answer.generation.text);
            errdefer a.free(text);
            const ids = try a.dupe(i32, answer.generation.token_ids orelse return error.InvalidSourceDecoderFixture);
            break :blk .{ .text = text, .token_ids = ids, .finish_reason = answer.generation.finish_reason, .prompt_tokens = answer.generation.prompt_tokens, .completion_tokens = answer.generation.tokens_used };
        };
        defer a.free(output.text);
        defer a.free(output.token_ids);
        const verify_user = try grounding.verificationUser(a, case.question, case.documents, output.text);
        defer a.free(verify_user);
        // Always print the raw verdict, even when production would skip an
        // UNKNOWN/truncated answer, so generation and verification failures
        // cannot be confused from aggregate token counts.
        var verdict = try service.generate(a, init.io, &models, selected, verification_system, verify_user, grounding.verifier_max_tokens, limits, .{}, control);
        defer verdict.deinit();
        const eligible = std.mem.eql(u8, output.finish_reason, "stop") and output.text.len <= grounding.max_candidate_bytes and
            std.unicode.utf8ValidateSlice(output.text) and std.mem.indexOfScalar(u8, output.text, 0) == null and
            !grounding.isBlank(output.text) and !grounding.isAbstention(output.text);
        const supported = grounding.isSupported(verdict.generation.text, verdict.generation.finish_reason);
        try emit(a, init.io, .{ .event = "case", .id = case.id, .question = case.question, .documents = case.documents, .answer_system = answer_system, .verification_system = verification_system, .answer_user = user, .answer = output, .verification_user = verify_user, .verdict_text = verdict.generation.text, .verdict_token_ids = verdict.generation.token_ids, .verdict_finish_reason = verdict.generation.finish_reason, .verdict_prompt_tokens = verdict.generation.prompt_tokens, .source_answer_eligible = eligible, .strict_supported = supported, .would_publish = eligible and supported });
    }
    try emit(a, init.io, .{ .event = "complete", .completed = true, .scope = job.scope, .cases = job.cases.len, .memory_rows = @as(usize, 0), .admitted = models.currentAdmissionAmounts() });
}
