// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Managed text embedding latency and cross-backend query qualification.
//! This offline child may be terminated by its owning campaign process.
const std = @import("std");
const internal = @import("inference_internal");
const Case = struct {
    name: []const u8,
    text: []const u8,
    task: []const u8 = "RETRIEVAL_QUERY",
    dimensions: usize = 768,
    warmups: ?usize = null,
    iterations: ?usize = null,
};
const Suite = struct { cases: []const Case };
const Measurement = struct {
    name: []const u8,
    task: []const u8,
    dimensions: usize,
    input_tokens: usize,
    warmup_seconds: []f64,
    samples_seconds: []f64,
    p50_seconds: f64,
    p95_seconds: f64,
    vector: []f32,
};
fn now() u64 {
    return internal.platform.time.monotonicNs();
}
fn seconds(start: u64) f64 {
    return @as(f64, @floatFromInt(now() - start)) / std.time.ns_per_s;
}
pub fn main(init: std.process.Init) !void {
    const begun = now();
    const a = std.heap.c_allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const args = try init.minimal.args.toSlice(out);
    var model: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    var suite: ?[]const u8 = null;
    var selected: ?[]const u8 = null;
    var backend: internal.backends.BackendType = .native;
    var warmups: usize = 2;
    var iterations: usize = 20;
    var i: usize = 1;
    while (i < args.len) : (i += 2) {
        if (i + 1 >= args.len) return error.MissingArgument;
        const k = args[i];
        const v = args[i + 1];
        if (std.mem.eql(u8, k, "--model")) model = v else if (std.mem.eql(u8, k, "--output")) output = v else if (std.mem.eql(u8, k, "--suite")) suite = v else if (std.mem.eql(u8, k, "--case")) selected = v else if (std.mem.eql(u8, k, "--warmups")) warmups = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--iterations")) iterations = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--backend")) {
            if (std.mem.eql(u8, v, "native")) backend = .native else if (std.mem.eql(u8, v, "cuda")) backend = .cuda else return error.InvalidBackend;
        } else return error.UnknownArgument;
    }
    if (model == null or output == null or suite == null or iterations == 0 or iterations > 100 or warmups > 100) return error.InvalidBenchmark;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, suite.?, out, .limited(4 * 1024 * 1024));
    const parsed = try std.json.parseFromSlice(Suite, out, bytes, .{ .ignore_unknown_fields = true });
    if (parsed.value.cases.len == 0 or parsed.value.cases.len > 256) return error.InvalidSuite;
    var sessions = internal.backends.SessionManager.initWithIo(a, init.io);
    sessions.preferred_backends = if (backend == .native) &.{.native} else &.{.cuda};
    sessions.required_backend = backend;
    sessions.native_call_policy = .in_process;
    var manager = internal.server.model_manager.ModelManager.init(a, sessions);
    defer manager.deinit();
    const control = internal.execution_control.InferenceExecutionControl{ .io = init.io, .native_call_policy = .in_process };
    const load_begun = now();
    var handle = try manager.acquireFromDirWithPreferredBackendsAndControl(model.?, sessions.preferred_backends, false, control);
    defer handle.release();
    const loaded = handle.get();
    if (loaded.session.backend() != backend) return error.UnexpectedBackend;
    const load_seconds = seconds(load_begun);
    var lease = loaded.acquireEmbeddingAssetLease(false);
    defer lease.release();
    const assets_begun = now();
    try loaded.ensureEmbeddingAssets(true, false, false);
    try loaded.verifyEmbeddingIdentityWithControl(control);
    const assets_seconds = seconds(assets_begun);
    std.debug.print("query_bench backend={s} managed_load_seconds={d:.6} assets_seconds={d:.6}\n", .{ @tagName(backend), load_seconds, assets_seconds });
    var measurements = std.ArrayList(Measurement).empty;
    var first_vector_seconds: ?f64 = null;
    for (parsed.value.cases, 0..) |case, index| {
        if (selected) |name| if (!std.mem.eql(u8, name, case.name)) continue;
        for (parsed.value.cases[0..index]) |previous| if (std.mem.eql(u8, previous.name, case.name)) return error.DuplicateCase;
        const count = case.iterations orelse iterations;
        const warms = case.warmups orelse warmups;
        if (count == 0 or count > 100 or warms > 100 or case.dimensions != 768) return error.InvalidCase;
        const prefix = if (std.mem.eql(u8, case.task, "RETRIEVAL_QUERY")) loaded.manifest.embedding_profile.query.prefix else if (std.mem.eql(u8, case.task, "RETRIEVAL_DOCUMENT")) loaded.manifest.embedding_profile.document.prefix else return error.InvalidTask;
        const samples = try out.alloc(f64, count);
        const warm_samples = try out.alloc(f64, warms);
        var saved: []f32 = &.{};
        var tokens: usize = 0;
        for (0..warms + count) |iteration| {
            var pipeline = loaded.embeddingPipeline(a);
            pipeline.execution_control = control;
            pipeline.config.text_prefix = prefix;
            pipeline.config.embedding_gemma2_dimensions = case.dimensions;
            const started = now();
            const vectors = try pipeline.embed(&.{case.text});
            const elapsed = seconds(started);
            defer {
                for (vectors) |v| a.free(v);
                a.free(vectors);
            }
            if (vectors.len != 1 or vectors[0].len != case.dimensions) return error.InvalidOutput;
            var norm: f64 = 0;
            for (vectors[0]) |v| {
                if (!std.math.isFinite(v)) return error.NonfiniteOutput;
                norm += @as(f64, v) * v;
            }
            if (@abs(@sqrt(norm) - 1) > 1e-4) return error.UnnormalizedOutput;
            if (first_vector_seconds == null) {
                first_vector_seconds = seconds(begun);
                std.debug.print("query_bench backend={s} first_vector_seconds={d:.6} first_inference_seconds={d:.6}\n", .{ @tagName(backend), first_vector_seconds.?, elapsed });
            }
            if (iteration < warms) warm_samples[iteration] = elapsed else samples[iteration - warms] = elapsed;
            if (iteration + 1 == warms + count) saved = try out.dupe(f32, vectors[0]);
        }
        // The native singleton fast path does not populate last_input_tokens.
        // Count the canonical rendered sequence separately, outside timing.
        const rendered = try std.fmt.allocPrint(out, "{s}{s}", .{ prefix, case.text });
        const tokenizer = loaded.getTokenizer();
        var encoded = try tokenizer.encodeForModel(a, rendered, loaded.manifest.maxTextSequenceLength() + 1);
        defer encoded.deinit();
        for (encoded.attention_mask) |value| tokens += @intFromBool(value != 0);
        const ordered = try out.dupe(f64, samples);
        std.mem.sort(f64, ordered, {}, std.sort.asc(f64));
        try measurements.append(out, .{ .name = case.name, .task = case.task, .dimensions = case.dimensions, .input_tokens = tokens, .warmup_seconds = warm_samples, .samples_seconds = samples, .p50_seconds = ordered[(count - 1) / 2], .p95_seconds = ordered[(count * 95 + 99) / 100 - 1], .vector = saved });
        std.debug.print("query_bench backend={s} case={s} tokens={d} p50_seconds={d:.6}\n", .{ @tagName(backend), case.name, tokens, ordered[(count - 1) / 2] });
    }
    if (measurements.items.len == 0) return error.NoCases;
    try loaded.verifyEmbeddingIdentityWithControl(control);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const suite_hash = std.fmt.bytesToHex(digest, .lower);
    const json = try std.json.Stringify.valueAlloc(out, .{ .version = 1, .backend = @tagName(backend), .model_identity = loaded.embedding_identity.?[0..], .suite_sha256 = suite_hash[0..], .managed_load_seconds = load_seconds, .assets_seconds = assets_seconds, .first_vector_seconds = first_vector_seconds.?, .timing_scope = "managed singleton text embedding: tokenization, encoder, pooling, normalization and completed output; excludes HTTP and model loading", .cases = measurements.items }, .{ .whitespace = .indent_2 });
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = output.?, .data = json });
}
