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

//! Offline full-generation qualification, including encoded media read and decode.
const std = @import("std");
const inference = @import("inference_internal");
const factory = inference.architectures.session_factory;
const generation = inference.pipelines.generation;
const runtime = inference.runtime;
const a = std.heap.c_allocator;
const Case = struct { id: []const u8, text: []const u8, kind: enum { text, image, audio, video } = .text, path: ?[]const u8 = null, max_tokens: i32, logits_path: ?[]const u8 = null, ple_input_path: ?[]const u8 = null, expected_prompt_tokens: ?usize = null };
const Suite = struct { warmups: usize = 2, repeats: usize = 3, context_tokens: u32 = 8192, legacy_ple_normalization: bool = false, kv_dtype: enum { f16, f32 } = .f16, cases: []const Case };
const Row = struct { id: []const u8, repeat: usize, ms: f64, prompt_tokens: usize, completion_tokens: usize, tokens: []i32, text: []const u8, phases: ?generation.GenerationTimingMs, gemma4_projector_residual_calls: usize, gemma4_audio_resident_layout_calls: usize, gemma4_projector_f16_calls: usize, gemma4_small_ffn_calls: usize, gemma4_small_ple_calls: usize };
fn residualCalls(session: anytype) usize {
    const active = session orelse return 0;
    return if (comptime factory.CudaRuntimeStats == void) 0 else if (factory.getCudaRuntimeStats(active)) |stats| stats.gemma4_projector_residual_calls else 0;
}
fn audioLayoutCalls(session: anytype) usize {
    const active = session orelse return 0;
    return if (comptime factory.CudaRuntimeStats == void) 0 else if (factory.getCudaRuntimeStats(active)) |stats| stats.gemma4_audio_resident_layout_calls else 0;
}
fn projectorF16Calls(session: anytype) usize {
    const active = session orelse return 0;
    return if (comptime factory.CudaRuntimeStats == void) 0 else if (factory.getCudaRuntimeStats(active)) |stats| stats.gemma4_projector_f16_calls else 0;
}
fn smallFfnCalls(session: anytype) usize {
    const active = session orelse return 0;
    return if (comptime factory.CudaRuntimeStats == void) 0 else if (factory.getCudaRuntimeStats(active)) |stats| stats.gemma4_small_ffn_calls else 0;
}
fn smallPleCalls(session: anytype) usize {
    const active = session orelse return 0;
    return if (comptime factory.CudaRuntimeStats == void) 0 else if (factory.getCudaRuntimeStats(active)) |stats| stats.gemma4_small_ple_calls else 0;
}
fn now() u64 {
    return inference.platform.time.monotonicNs();
}
const ops = inference.ops;
// Compile-time instrumentation for this executable's eager and prepared PLE paths.
const PleProbe = struct {
    io: std.Io,
    input_path: ?[]const u8,
    captured: bool = false,
    calls: usize = 0,
    legacy: bool,
};
threadlocal var active_ple_probe: ?*PleProbe = null;

pub fn qualifyGemmaPleProjection(cb: *const ops.ComputeBackend, hidden: ops.CT, rows: usize, config: inference.architectures.gpt.Config) !f32 {
    const probe = active_ple_probe orelse return error.MissingPleProbe;
    probe.calls += 1;
    if (probe.input_path) |path| {
        if (!probe.captured) {
            const values = try cb.toFloat32(hidden, a);
            defer a.free(values);
            if (rows == 0 or values.len != try std.math.mul(usize, rows, config.hidden_size)) return error.InvalidPleCapture;
            try std.Io.Dir.cwd().writeFile(probe.io, .{ .sub_path = path, .data = std.mem.sliceAsBytes(values) });
            probe.captured = true;
        }
    }
    return if (probe.legacy) config.norm_eps else config.pleProjectionNormEps();
}
const Runner = struct {
    pipeline: *generation.NativeGenerationPipeline,
    manager: *runtime.kv.manager.KvManager,
    storage: *runtime.kv.storage_runtime.KvStorageRuntime,
    pool: runtime.kv.block.KvPoolId,
    io: std.Io,
    base: []const u8,
    kv_dtype: []const u8,
    legacy_ple: bool,
    ple_calls: usize = 0,
    fn run(self: *@This(), case: Case, repeat: usize) !Row {
        var state = generation.NativeDecodeState.initPaged(a, self.manager, self.pool, null);
        state.kv_storage = self.storage;
        defer state.deinit();
        self.pipeline.decode_state = &state;
        defer self.pipeline.decode_state = null;
        const input_path = if (case.ple_input_path) |p| try std.fs.path.join(a, &.{ self.base, p }) else null;
        defer if (input_path) |p| a.free(p);
        var probe = PleProbe{ .io = self.io, .input_path = input_path, .legacy = self.legacy_ple };
        const previous = active_ple_probe;
        active_ple_probe = &probe;
        defer active_ple_probe = previous;
        const logits = if (case.logits_path != null) try a.alloc(f32, self.pipeline.gpt_config.vocab_size) else null;
        defer if (logits) |row| a.free(row);
        self.pipeline.prefill_logits_out = logits;
        defer self.pipeline.prefill_logits_out = null;
        if (logits != null and case.max_tokens != 1) return error.LogitCaptureNeedsOneToken;
        const residual_before = residualCalls(self.pipeline.session);
        const audio_layout_before = audioLayoutCalls(self.pipeline.session);
        const f16_before = projectorF16Calls(self.pipeline.session);
        const small_ffn_before = smallFfnCalls(self.pipeline.session);
        const small_ple_before = smallPleCalls(self.pipeline.session);
        const started = now();
        // Serving resets the model-scoped arena/graph epoch at this boundary.
        try self.pipeline.cb.beginRequest();
        const path = if (case.path) |p| try std.fs.path.join(a, &.{ self.base, p }) else null;
        defer if (path) |p| a.free(p);
        const data = if (path) |p| try std.Io.Dir.cwd().readFileAlloc(self.io, p, a, .limited(64 * 1024 * 1024)) else null;
        defer if (data) |bytes| a.free(bytes);
        const media = [_][]const u8{data orelse &.{}};
        const parts: [2]generation.Message.ContentPart = .{
            if (case.kind == .audio) .{ .audio = 0 } else .{ .image = 0 },
            .{ .text = case.text },
        };
        const message = generation.Message{ .role = "user", .content = case.text, .content_parts = if (case.kind == .image or case.kind == .audio) &parts else null, .image_bytes = if (case.kind == .image) &media else null, .audio_bytes = if (case.kind == .audio) &media else null, .video_bytes = if (case.kind == .video) data else null };
        var result = try self.pipeline.generate(&.{message}, .{ .max_tokens = case.max_tokens, .temperature = 0, .top_k = 0, .top_p = 0, .repetition_penalty = 1, .cache_dtype = self.kv_dtype, .prompt_cache_enabled = false, .ignore_eos = true, .enable_thinking = false });
        defer result.deinit();
        const finished = now();
        if (probe.calls == 0) return error.PleControlNotExercised;
        self.ple_calls += probe.calls;
        if (input_path != null and !probe.captured) return error.MissingPleInputCapture;
        if (result.tokens_used != case.max_tokens) return error.IncompleteGeneration;
        if (case.expected_prompt_tokens) |expected| if (result.prompt_tokens != expected) {
            std.debug.print("prompt mismatch {s}: expected={} actual={}\n", .{ case.id, expected, result.prompt_tokens });
            return error.PreparedTokenMismatch;
        };
        const tokens = result.token_ids orelse return error.MissingTokenIds;
        if (tokens.len != case.max_tokens) return error.IncompleteTokenReadback;
        if (case.logits_path) |p| {
            const values = logits.?;
            const sampled = std.math.cast(usize, tokens[0]) orelse return error.CapturedArgmaxMismatch;
            if (std.mem.indexOfMax(f32, values) != sampled) return error.CapturedArgmaxMismatch;
            for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteLogit;
            const destination = try std.fs.path.join(a, &.{ self.base, p });
            defer a.free(destination);
            try std.Io.Dir.cwd().writeFile(self.io, .{ .sub_path = destination, .data = std.mem.sliceAsBytes(values) });
        }
        const saved = try a.dupe(i32, tokens);
        errdefer a.free(saved);
        return .{ .id = case.id, .repeat = repeat, .ms = @as(f64, @floatFromInt(finished - started)) / 1e6, .prompt_tokens = result.prompt_tokens, .completion_tokens = result.tokens_used, .tokens = saved, .text = try a.dupe(u8, result.text), .phases = result.timing_ms, .gemma4_projector_residual_calls = residualCalls(self.pipeline.session) - residual_before, .gemma4_audio_resident_layout_calls = audioLayoutCalls(self.pipeline.session) - audio_layout_before, .gemma4_projector_f16_calls = projectorF16Calls(self.pipeline.session) - f16_before, .gemma4_small_ffn_calls = smallFfnCalls(self.pipeline.session) - small_ffn_before, .gemma4_small_ple_calls = smallPleCalls(self.pipeline.session) - small_ple_before };
    }
};
pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(a);
    defer a.free(args);
    if (args.len != 7) return error.ExpectedModelTokenizerTemplateProjectorSuiteOutput;
    const raw = try std.Io.Dir.cwd().readFileAlloc(init.io, args[5], a, .limited(16 * 1024 * 1024));
    defer a.free(raw);
    const parsed = try std.json.parseFromSlice(Suite, a, raw, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const suite = parsed.value;
    if (suite.repeats == 0 or suite.repeats > 100 or suite.warmups > 100 or suite.context_tokens == 0 or suite.context_tokens > 8192 or suite.cases.len == 0) return error.InvalidSuite;
    for (suite.cases) |case| if (case.max_tokens <= 0 or case.max_tokens > 8192 or (case.kind != .text and case.path == null)) return error.InvalidCase;
    const validation = suite.cases[0].logits_path != null;
    for (suite.cases) |case| if ((case.logits_path != null) != validation) return error.MixedValidationSuite;
    for (suite.cases) |case| if (case.ple_input_path != null and case.logits_path == null) return error.PleCaptureNeedsValidation;
    const session = try factory.createGemma4CudaQualificationSession(a, args[1]);
    defer session.close();
    factory.attachIo(session, init.io);
    var cb = try factory.getComputeBackend(session, a);
    defer cb.deinit();
    if (cb.kind() != .cuda) return error.RequiredCuda;
    var cfg = factory.getGptConfig(session) orelse return error.WrongArchitecture;
    if (!cfg.hasPle() or cfg.hidden_size == 0) return error.WrongArchitecture;
    cfg.max_position_embeddings = @min(cfg.max_position_embeddings, suite.context_tokens);
    const tok_raw = try std.Io.Dir.cwd().readFileAlloc(init.io, args[2], a, .limited(64 * 1024 * 1024));
    defer a.free(tok_raw);
    var hf = try inference.hf_tokenizer.HfTokenizer.loadFromBytesWithOptions(a, tok_raw, .{ .strict_unigram_normalizer = true });
    const tok = hf.tokenizer();
    defer tok.deinitTokenizer();
    const template_raw = try std.Io.Dir.cwd().readFileAlloc(init.io, args[3], a, .limited(1024 * 1024));
    defer a.free(template_raw);
    var template = try generation.ChatTemplate.init(a, template_raw, "<bos>", "<eos>", "<unk>", "<pad>");
    defer template.deinit();
    const projector = try inference.architectures.gemma4_projector.ProjectorStore.open(a, args[4]);
    defer projector.close();
    var manager = runtime.kv.manager.KvManager.init(a);
    defer manager.deinit();
    const kv_dtype: runtime.kv.pool.KvDType = if (suite.kv_dtype == .f32) .f32 else .f16;
    const pool_config = generation.kvPoolConfig(.cuda, kv_dtype, cfg, false);
    const pool = try manager.addPool(generation.requestKvManagerPoolConfig(pool_config));
    var storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(a, pool_config);
    defer storage.deinit();
    try cb.provisionKvDeviceWriteHook(&storage);
    var pipeline = generation.NativeGenerationPipeline{ .allocator = a, .io = init.io, .cb = cb, .session = session, .gpt_config = cfg, .kv_dtype = kv_dtype, .tokenizer = tok, .add_bos_token = true, .bos_token = "<bos>", .chat_template = &template, .gguf_projector_path = args[4], .projector_store = projector, .return_raw_token_ids = true };
    defer pipeline.deinit();
    var runner = Runner{ .pipeline = &pipeline, .manager = &manager, .storage = &storage, .pool = pool, .io = init.io, .base = std.fs.path.dirname(args[5]) orelse ".", .kv_dtype = @tagName(suite.kv_dtype), .legacy_ple = suite.legacy_ple_normalization };
    // Prime every requested modality before measuring so tower weights do not
    // become a text-only resident-inventory advantage for the native arm.
    for (suite.cases) |case| {
        std.log.info("priming {s}", .{case.id});
        const row = try runner.run(case, 0);
        a.free(row.tokens);
        a.free(row.text);
    }
    var rows: std.ArrayList(Row) = .empty;
    defer {
        for (rows.items) |row| {
            a.free(row.tokens);
            a.free(row.text);
        }
        rows.deinit(a);
    }
    try rows.ensureTotalCapacity(a, try std.math.mul(usize, suite.cases.len, suite.repeats));
    for (suite.cases) |case| for (0..suite.warmups + suite.repeats) |rep| {
        const row = try runner.run(case, rep);
        std.debug.print("native-gemma4 {s} rep={} ms={d:.3} prompt={} output={}\n", .{ case.id, rep, row.ms, row.prompt_tokens, row.completion_tokens });
        if (rep < suite.warmups) {
            a.free(row.tokens);
            a.free(row.text);
        } else rows.appendAssumeCapacity(row);
    };
    const output = try std.json.Stringify.valueAlloc(a, .{ .backend = "native CUDA", .weights = "QAT Q4_0 GGUF", .kv_dtype = @tagName(suite.kv_dtype), .context_tokens = suite.context_tokens, .legacy_ple_normalization = suite.legacy_ple_normalization, .ple_control_calls = runner.ple_calls, .validation_logits = validation, .scope = "encoded file read, preprocessing, towers, prompt construction, prefill, fixed-budget decode and synchronized token readback; excludes HTTP, loading and post-readback cleanup", .rows = rows.items, .cuda = factory.getCudaRuntimeStats(session) }, .{ .whitespace = .indent_2 });
    defer a.free(output);
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = args[6], .data = output });
}
