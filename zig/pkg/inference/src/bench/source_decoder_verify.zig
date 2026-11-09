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

//! Qualified source decoder: independent CPU oracle, fresh F32 KV,
//! cancellation, A/B/A isolation and admission/physical-allocation proofs.
const std = @import("std");
const inference = @import("inference_internal");
const service = inference.server.source_decoder_service;
const manager_mod = inference.server.model_manager;
const qualified = inference.models.memorilla_qualification;
const grounding = inference.pipelines.memorilla_grounding;
const decode = inference.pipelines.memorilla_generation;
const memory = inference.runtime.tier.memory;
const Control = inference.execution_control.InferenceExecutionControl;
const CB = inference.ops.ComputeBackend;
const CT = inference.ops.CT;
const safe = inference.models.safetensors;
const Tensor = inference.backends.Tensor;
const file = inference.util.c_file;
const A = std.mem.Allocator;

const question = "Who owns the copper access badge at Neral station? Give only the name.";
const documents = [_][]const u8{
    "At Neral station, the copper access badge belongs to Tovik.",
    "At Neral station, the silver access badge belongs to Daskel.",
    "At Orven station, the copper access badge belongs to Zorun.",
};
const Args = struct { decoder: []const u8 = "", fixture: []const u8 = "", budget_gib: usize = 96 };
fn parse(args: []const []const u8) !Args {
    var result = Args{};
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const key = args[i];
        i += 1;
        if (i == args.len) return error.InvalidArguments;
        if (std.mem.eql(u8, key, "--decoder")) result.decoder = args[i] else if (std.mem.eql(u8, key, "--fixture")) result.fixture = args[i] else if (std.mem.eql(u8, key, "--budget-gib")) result.budget_gib = try std.fmt.parseInt(usize, args[i], 10) else return error.InvalidArguments;
    }
    if (result.decoder.len == 0 or result.fixture.len == 0 or result.budget_gib == 0 or result.budget_gib > 112) return error.InvalidArguments;
    return result;
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
fn member(value: std.json.Value, key: []const u8) !std.json.Value {
    if (value != .object) return error.InvalidSourceDecoderFixture;
    return value.object.get(key) orelse error.InvalidSourceDecoderFixture;
}
fn textEqual(value: std.json.Value, expected: []const u8) !void {
    if (value != .string or !std.mem.eql(u8, value.string, expected)) return error.InvalidSourceDecoderFixture;
}
fn verifyModel(a: A, model: std.json.Value) !void {
    const lock = try std.json.parseFromSlice(std.json.Value, a, qualified.lock_bytes, .{});
    defer lock.deinit();
    const expected = try member(try member(lock.value, "models"), "decoder");
    for ([_][]const u8{ "model", "revision" }) |key| try textEqual(try member(model, key), (try member(expected, key)).string);
    const files = try member(model, "files");
    const pinned = try member(expected, "files");
    if (files != .array or pinned != .array or files.array.items.len != pinned.array.items.len) return error.InvalidSourceDecoderFixture;
    for (pinned.array.items) |entry| {
        const path = (try member(entry, "path")).string;
        var matches: usize = 0;
        for (files.array.items) |candidate| {
            const candidate_path = try member(candidate, "path");
            if (candidate_path != .string or !std.mem.eql(u8, path, candidate_path.string)) continue;
            matches += 1;
            var fields = entry.object.iterator();
            while (fields.next()) |field| {
                const found = try member(candidate, field.key_ptr.*);
                switch (field.value_ptr.*) {
                    .string => |expected_text| try textEqual(found, expected_text),
                    .integer => |expected_number| if (found != .integer or found.integer != expected_number) return error.InvalidSourceDecoderFixture,
                    else => return error.InvalidSourceDecoderFixture,
                }
            }
        }
        if (matches != 1) return error.InvalidSourceDecoderFixture;
    }
}

const Oracle = struct {
    reader: safe.MMapReader,
    embeddings: Tensor,
    ids: Tensor,
    prefill: Tensor,
    logits: Tensor,
    tokens: Tensor,
    digest: [64]u8,

    fn open(a: A, path: []const u8, user: []const u8) !Oracle {
        var reader = try safe.MMapReader.openFileAbsoluteLimited(a, path, 256 * 1024 * 1024, 2 * 1024 * 1024);
        errdefer reader.deinit();
        try safe.validateReader(a, &reader);
        const receipt_path = try std.fs.path.join(a, &.{ std.fs.path.dirname(path) orelse ".", "reference.json" });
        defer a.free(receipt_path);
        const bytes = try file.readFileMax(a, receipt_path, 4 * 1024 * 1024);
        defer a.free(bytes);
        const receipt = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
        defer receipt.deinit();
        try textEqual(try member(receipt.value, "schema"), "antfly.memorilla.source-decoder-reference.v1");
        try textEqual(try member(receipt.value, "precision"), "BF16-source-weights-widened-F32; F32-activations-and-KV");
        try textEqual(try member(receipt.value, "system_prompt"), grounding.fallback_system_prompt);
        try textEqual(try member(receipt.value, "user_prompt"), user);
        try verifyModel(a, try member(receipt.value, "model"));
        const digest = inference.models.memorilla.digest(reader.file_bytes);
        const declared = try member(try member(receipt.value, "files"), "reference.safetensors");
        try textEqual(try member(declared, "sha256"), &digest);
        const size = try member(declared, "size_bytes");
        if (size != .integer or size.integer != reader.file_bytes.len) return error.InvalidSourceDecoderFixture;
        var embeddings = try reader.readTensor("input_embeddings");
        errdefer embeddings.deinit();
        var ids = try reader.readTensor("input_ids");
        errdefer ids.deinit();
        var prefill = try reader.readTensor("prefill_logits");
        errdefer prefill.deinit();
        var logits = try reader.readTensor("decode_logits");
        errdefer logits.deinit();
        var tokens = try reader.readTensor("decode_input_ids");
        errdefer tokens.deinit();
        if (embeddings.dtype != .f32 or ids.dtype != .i64 or prefill.dtype != .f32 or logits.dtype != .f32 or tokens.dtype != .i64 or
            ids.shape.len != 1 or ids.elementCount() == 0 or ids.elementCount() > 1024 or
            embeddings.shape.len != 2 or embeddings.shape[0] != ids.elementCount() or embeddings.shape[1] != 4096 or
            prefill.shape.len != 1 or prefill.elementCount() != 151936 or tokens.shape.len != 1 or tokens.elementCount() != 3 or
            logits.shape.len != 2 or logits.shape[0] != 3 or logits.shape[1] != 151936) return error.InvalidSourceDecoderFixture;
        return .{ .reader = reader, .embeddings = embeddings, .ids = ids, .prefill = prefill, .logits = logits, .tokens = tokens, .digest = digest };
    }
    fn deinit(self: *@This()) void {
        self.embeddings.deinit();
        self.ids.deinit();
        self.prefill.deinit();
        self.logits.deinit();
        self.tokens.deinit();
        self.reader.deinit();
    }
};
const Comparison = struct { elements: usize, mismatches: usize = 0, max_absolute_error: f64 = 0 };
fn compare(actual: []const f32, expected: []const f32, tolerance: f64) !Comparison {
    if (actual.len == 0 or actual.len != expected.len) return error.InvalidSourceDecoderFixture;
    var result = Comparison{ .elements = actual.len };
    for (actual, expected) |x, y| {
        if (!std.math.isFinite(x) or !std.math.isFinite(y)) return error.NonFiniteComparison;
        const difference = @abs(@as(f64, x) - y);
        result.max_absolute_error = @max(result.max_absolute_error, difference);
        result.mismatches += @intFromBool(difference > tolerance + tolerance * @abs(@as(f64, y)));
    }
    return result;
}
const Context = struct {
    a: A,
    io: std.Io,
    oracle: *const Oracle,
    label: []const u8,
    validate: bool = true,
    cancel_after_prefill: bool = false,
    cancelled: bool = false,
    ids_match: bool = false,
    embeddings: ?Comparison = null,
    logits: [4]?Comparison = @splat(null),
    digests: [4]?[64]u8 = @splat(null),
    physical_seen: bool = false,

    fn check(raw: ?*anyopaque) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (self.cancelled) return error.Cancelled;
    }
    fn prepared(raw: ?*anyopaque, text: *const decode.PreparedPlainText) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (self.validate) {
            self.ids_match = std.mem.eql(i64, text.input_ids, self.oracle.ids.asInt64());
            if (!self.ids_match) return error.SourceDecoderPromptMismatch;
        }
        try emit(self.a, self.io, .{ .event = "framing", .run = self.label, .prompt_tokens = text.input_ids.len, .input_ids = text.input_ids, .oracle_ids_match = self.ids_match, .memory_rows = @as(usize, 0) });
    }
    fn input(raw: *anyopaque, cb: *const CB, tensor: CT) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (!self.validate) return;
        const values = try cb.toFloat32(tensor, self.a);
        defer self.a.free(values);
        self.embeddings = try compare(values, self.oracle.embeddings.asFloat32(), 0);
        if (!std.mem.eql(u8, std.mem.sliceAsBytes(values), std.mem.sliceAsBytes(self.oracle.embeddings.asFloat32())))
            return error.SourceDecoderEmbeddingBitsMismatch;
    }
    fn logit(raw: *anyopaque, step: usize, values: []const f32) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (step >= 4) return error.InvalidTraceStep;
        self.digests[step] = inference.models.memorilla.digest(std.mem.sliceAsBytes(values));
        if (self.validate) {
            const expected = if (step == 0) self.oracle.prefill.asFloat32() else self.oracle.logits.asFloat32()[(step - 1) * 151936 ..][0..151936];
            self.logits[step] = try compare(values, expected, 0.001);
        }
        if (step == 0 and self.cancel_after_prefill) self.cancelled = true;
    }
    fn physical(raw: *anyopaque, value: decode.PhysicalResources) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.physical_seen = true;
        const within_plan = value.limit <= try std.math.add(usize, value.baseline, value.held_transient_bytes) and value.live <= value.limit;
        try emit(self.a, self.io, .{ .event = "physical_cuda", .run = self.label, .resources = value, .within_admitted_plan = within_plan, .peak_scope = "lifetime high-water, not reset or equated with each request peak" });
        if (!within_plan) return error.MemorillaPhysicalAdmissionExceeded;
    }
    fn passed(self: *const @This()) bool {
        if (!self.ids_match or self.embeddings == null or self.embeddings.?.mismatches != 0 or !self.physical_seen) return false;
        for (self.logits) |item| if (item == null or item.?.mismatches != 0) return false;
        return true;
    }
};

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
    const user = try grounding.answerUser(a, question, &documents);
    defer a.free(user);
    const other_user = try grounding.answerUser(a, "Who owns the silver access badge at Neral station? Give only the name.", &documents);
    defer a.free(other_user);
    var oracle = try Oracle.open(a, opts.fixture, user);
    defer oracle.deinit();
    var sessions = inference.backends.SessionManager.initWithIo(a, init.io);
    sessions.preferred_backends = &.{.cuda};
    sessions.required_backend = .cuda;
    sessions.native_call_policy = .in_process;
    var models = manager_mod.ModelManager.init(a, sessions);
    defer models.deinit();
    models.configureServingPolicy(.{});
    models.configureAdmissionLimits(limits);
    models.configureProcessMemoryLimit(cap, .explicit);
    models.configureModelCache(600_000, 2);
    try models.ensureResourceOwnerReady();
    var context = Context{ .a = a, .io = init.io, .oracle = &oracle, .label = "cold_a" };
    const control = Control{ .io = init.io, .native_call_policy = .in_process, .ptr = &context, .check_fn = Context.check };
    const options = service.Options{ .context = &context, .prepared_text = Context.prepared, .trace = .{ .context = &context, .input_embeddings = Context.input, .logits = Context.logit, .physical_resources = Context.physical }, .teacher_forced_ids = oracle.tokens.asInt64(), .ignore_eos = true };
    try service.preflight(a, init.io, &models, selected, grounding.fallback_system_prompt, user, 3, limits, .{}, control);
    if (models.currentAdmissionAmounts().backend_weight_bytes != 0) return error.SourcePreflightLoadedModel;
    var first = try service.generateWithOptions(a, init.io, &models, selected, grounding.fallback_system_prompt, user, 3, limits, .{}, control, options);
    const baseline = context.digests;
    var passed = context.passed();
    const cold_result = .{ .embeddings = context.embeddings, .logits = context.logits };
    first.deinit();
    const retained = models.currentAdmissionAmounts();
    if (retained.backend_weight_bytes != qualified.decoder_weight_bytes) return error.MemorillaModelWeightAdmissionMissing;
    try emit(a, init.io, .{ .event = "case", .run = context.label, .passed = passed, .comparison = cold_result, .retained = retained });
    context = .{ .a = a, .io = init.io, .oracle = &oracle, .label = "cancel_after_prefill", .cancel_after_prefill = true };
    var cancelled = false;
    if (service.generateWithOptions(a, init.io, &models, selected, grounding.fallback_system_prompt, user, 3, limits, .{}, control, options)) |value| {
        var unexpected = value;
        unexpected.deinit();
    } else |err| {
        if (err != error.Cancelled) return err;
        cancelled = true;
    }
    const cancel_refund = std.meta.eql(retained, models.currentAdmissionAmounts());
    passed = passed and cancelled and cancel_refund;
    try emit(a, init.io, .{ .event = "case", .run = context.label, .cancelled = cancelled, .admission_refunded = cancel_refund });
    for ([_][]const u8{ "a_after_cancel", "b_different_prompt", "a_after_b" }) |label| {
        const is_b = std.mem.eql(u8, label, "b_different_prompt");
        context = .{ .a = a, .io = init.io, .oracle = &oracle, .label = label, .validate = !is_b };
        var request_options = options;
        if (is_b) request_options.teacher_forced_ids = null;
        var result = try service.generateWithOptions(a, init.io, &models, selected, grounding.fallback_system_prompt, if (is_b) other_user else user, 3, limits, .{}, control, request_options);
        const isolated = is_b or std.meta.eql(baseline, context.digests);
        const numerical = is_b or context.passed();
        const count = if (result.generation.token_ids) |ids| ids.len else 0;
        result.deinit();
        const refunded = std.meta.eql(retained, models.currentAdmissionAmounts());
        const case_passed = isolated and numerical and refunded and context.physical_seen and count == 3;
        passed = passed and case_passed;
        try emit(a, init.io, .{ .event = "case", .run = label, .passed = case_passed, .compared_to_oracle = !is_b, .bitwise_a_isolated = isolated, .admission_refunded = refunded, .logits = context.logits, .generated_tokens = count });
    }
    const decoder_identity = qualified.executionIdentity(.qwen3_decoder_f32_kv_f32_v1);
    try emit(a, init.io, .{ .event = "complete", .passed = passed, .scope = "actual-managed-source-decoder-service", .oracle_sha256 = oracle.digest[0..], .decoder_execution_identity = decoder_identity[0..], .memory_rows = @as(usize, 0), .frozen_logit_atol = 0.001, .frozen_logit_rtol = 0.001, .retained = models.currentAdmissionAmounts() });
    if (!passed) return error.SourceDecoderQualificationFailed;
}
