// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Offline full-generation qualification, including encoded media read and decode.
const std = @import("std");
const inference = @import("inference_internal");
const factory = inference.architectures.session_factory;
const generation = inference.pipelines.generation;
const runtime = inference.runtime;
const a = std.heap.c_allocator;
const Case = struct { id: []const u8, text: []const u8, kind: enum { text, image, audio, video } = .text, path: ?[]const u8 = null, max_tokens: i32, logits_path: ?[]const u8 = null, expected_prompt_tokens: ?usize = null };
const Suite = struct { warmups: usize = 2, repeats: usize = 3, context_tokens: u32 = 8192, cases: []const Case };
const Row = struct { id: []const u8, repeat: usize, ms: f64, prompt_tokens: usize, completion_tokens: usize, tokens: []i32, text: []const u8, phases: ?generation.GenerationTimingMs };
fn now() u64 {
    return inference.platform.time.monotonicNs();
}
const ops = inference.ops;
extern "c" fn setenv([*:0]const u8, [*:0]const u8, c_int) c_int;
const Capture = struct {
    values: ?[]f32 = null,
    reads: usize = 0,
    var active: ?*Capture = null;
    var original: *const fn (*anyopaque, ops.CT, std.mem.Allocator) anyerror![]f32 = undefined;
    fn read(ctx: *anyopaque, tensor: ops.CT, allocator: std.mem.Allocator) ![]f32 {
        const values = try original(ctx, tensor, allocator);
        errdefer allocator.free(values);
        if (active) |self| if (values.len == 262144) {
            self.reads += 1;
            if (self.values == null) self.values = try a.dupe(f32, values);
        };
        return values;
    }
};
const Runner = struct {
    pipeline: *generation.NativeGenerationPipeline,
    manager: *runtime.kv.manager.KvManager,
    storage: *runtime.kv.storage_runtime.KvStorageRuntime,
    pool: runtime.kv.block.KvPoolId,
    io: std.Io,
    base: []const u8,
    fn run(self: *@This(), case: Case, repeat: usize) !Row {
        var state = generation.NativeDecodeState.initPaged(a, self.manager, self.pool, null);
        state.kv_storage = self.storage;
        defer state.deinit();
        self.pipeline.decode_state = &state;
        defer self.pipeline.decode_state = null;
        const original_vtable = self.pipeline.cb.vtable;
        var capture_vtable = original_vtable.*;
        var capture = Capture{};
        defer if (capture.values) |values| a.free(values);
        if (case.logits_path != null) {
            if (case.max_tokens != 1) return error.LogitCaptureNeedsOneToken;
            Capture.original = original_vtable.toFloat32;
            Capture.active = &capture;
            capture_vtable.toFloat32 = Capture.read;
            self.pipeline.cb.vtable = &capture_vtable;
        }
        defer {
            self.pipeline.cb.vtable = original_vtable;
            Capture.active = null;
        }
        const started = now();
        // Serving resets the model-scoped arena/graph epoch at this boundary.
        try self.pipeline.cb.beginRequest();
        const path = if (case.path) |p| try std.fs.path.join(a, &.{ self.base, p }) else null;
        defer if (path) |p| a.free(p);
        const data = if (path) |p| try std.Io.Dir.cwd().readFileAlloc(self.io, p, a, .limited(64 * 1024 * 1024)) else null;
        defer if (data) |bytes| a.free(bytes);
        const media = [_][]const u8{data orelse &.{}};
        const message = generation.Message{ .role = "user", .content = case.text, .image_bytes = if (case.kind == .image) &media else null, .audio_bytes = if (case.kind == .audio) &media else null, .video_bytes = if (case.kind == .video) data else null };
        var result = try self.pipeline.generate(&.{message}, .{ .max_tokens = case.max_tokens, .temperature = 0, .top_k = 0, .top_p = 0, .repetition_penalty = 1, .cache_dtype = "f16", .prompt_cache_enabled = false, .ignore_eos = true, .enable_thinking = false });
        defer result.deinit();
        const finished = now();
        if (case.logits_path) |p| {
            const values = capture.values orelse return error.MissingLogitCapture;
            if (capture.reads != 1 or values.len != self.pipeline.gpt_config.vocab_size) return error.InvalidLogitCapture;
            // The intercepted lm_head readback precedes the architecture's CPU softcap.
            inference.architectures.gpt.applyFinalLogitSoftcapInPlace(self.pipeline.gpt_config, values);
            if (std.mem.indexOfMax(f32, values) != @as(usize, @intCast(result.token_ids.?[0]))) return error.CapturedArgmaxMismatch;
            for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteLogit;
            const destination = try std.fs.path.join(a, &.{ self.base, p });
            defer a.free(destination);
            try std.Io.Dir.cwd().writeFile(self.io, .{ .sub_path = destination, .data = std.mem.sliceAsBytes(values) });
        }
        if (result.tokens_used != case.max_tokens) return error.IncompleteGeneration;
        if (case.expected_prompt_tokens) |expected| if (result.prompt_tokens != expected) {
            std.debug.print("prompt mismatch {s}: expected={} actual={}\n", .{ case.id, expected, result.prompt_tokens });
            return error.PreparedTokenMismatch;
        };
        const tokens = result.token_ids orelse return error.MissingTokenIds;
        if (tokens.len != case.max_tokens) return error.IncompleteTokenReadback;
        const saved = try a.dupe(i32, tokens);
        errdefer a.free(saved);
        return .{ .id = case.id, .repeat = repeat, .ms = @as(f64, @floatFromInt(finished - started)) / 1e6, .prompt_tokens = result.prompt_tokens, .completion_tokens = result.tokens_used, .tokens = saved, .text = try a.dupe(u8, result.text), .phases = result.timing_ms };
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
    if (suite.repeats == 0 or suite.repeats > 100 or suite.warmups > 100 or suite.context_tokens > 8192 or suite.cases.len == 0) return error.InvalidSuite;
    for (suite.cases) |case| if (case.max_tokens <= 0 or case.max_tokens > 8192 or (case.kind != .text and case.path == null)) return error.InvalidCase;
    const validation = suite.cases[0].logits_path != null;
    for (suite.cases) |case| if ((case.logits_path != null) != validation) return error.MixedValidationSuite;
    if (validation) {
        // Validation alone requests complete host logits. Timed runs retain defaults.
        if (setenv("ANTFLY_INFERENCE_CUDA_PREFILL_GREEDY_TOKEN", "0", 1) != 0 or
            setenv("ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN", "0", 1) != 0 or
            setenv("ANTFLY_INFERENCE_CUDA_REPLAY_LAST_LOGITS", "0", 1) != 0) return error.SetEnv;
    }

    const session = try factory.createCudaSession(a, args[1]);
    defer session.close();
    factory.attachIo(session, init.io);
    var cb = try factory.getComputeBackend(session, a);
    defer cb.deinit();
    if (cb.kind() != .cuda) return error.RequiredCuda;
    var cfg = factory.getGptConfig(session) orelse return error.WrongArchitecture;
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
    const pool_config = generation.kvPoolConfig(.cuda, .f16, cfg, false);
    const pool = try manager.addPool(generation.requestKvManagerPoolConfig(pool_config));
    var storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(a, pool_config);
    defer storage.deinit();
    try cb.provisionKvDeviceWriteHook(&storage);
    var pipeline = generation.NativeGenerationPipeline{ .allocator = a, .io = init.io, .cb = cb, .session = session, .gpt_config = cfg, .kv_dtype = .f16, .tokenizer = tok, .add_bos_token = true, .bos_token = "<bos>", .chat_template = &template, .gguf_projector_path = args[4], .projector_store = projector, .return_raw_token_ids = true };
    defer pipeline.deinit();
    var runner = Runner{ .pipeline = &pipeline, .manager = &manager, .storage = &storage, .pool = pool, .io = init.io, .base = std.fs.path.dirname(args[5]) orelse "." };
    // Prime every requested modality before measuring so tower weights do not
    // become a text-only resident-inventory advantage for the native arm.
    for (suite.cases) |case| {
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
    for (suite.cases) |case| for (0..suite.warmups + suite.repeats) |rep| {
        const row = try runner.run(case, rep);
        std.debug.print("native-gemma4 {s} rep={} ms={d:.3} prompt={} output={}\n", .{ case.id, rep, row.ms, row.prompt_tokens, row.completion_tokens });
        if (rep < suite.warmups) {
            a.free(row.tokens);
            a.free(row.text);
        } else try rows.append(a, row);
    };
    const output = try std.json.Stringify.valueAlloc(a, .{ .backend = "native CUDA", .weights = "QAT Q4_0 GGUF", .kv_dtype = "f16", .context_tokens = suite.context_tokens, .validation_logits = for (suite.cases) |case| {
        if (case.logits_path != null) break true;
    } else false, .scope = "encoded file read, preprocessing, towers, prompt construction, prefill, fixed-budget decode and synchronized token readback; excludes HTTP, loading and post-readback cleanup", .rows = rows.items, .cuda = factory.getCudaRuntimeStats(session) }, .{ .whitespace = .indent_2 });
    defer a.free(output);
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = args[6], .data = output });
}
