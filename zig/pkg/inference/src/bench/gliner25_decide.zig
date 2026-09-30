// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Loaded-model request latency for the pinned Decide multi-question case.
const std = @import("std");
const inference = @import("inference_internal");
const factory = inference.architectures.session_factory;
const gliner = inference.pipelines.gliner;
const manifest_mod = inference.models.manifest;
const hf = inference.hf_tokenizer;
const c_file = inference.util.c_file;

pub fn main(init: std.process.Init) !void {
    const a = std.heap.c_allocator;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    var model_dir: ?[]const u8 = null;
    var backend: ?[]const u8 = null;
    var warmup: usize = 3;
    var reps: usize = 20;
    while (args.next()) |arg| {
        const value = args.next() orelse return error.MissingArgument;
        if (std.mem.eql(u8, arg, "--model-dir")) model_dir = value else if (std.mem.eql(u8, arg, "--backend")) backend = value else if (std.mem.eql(u8, arg, "--warmup")) warmup = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, arg, "--reps")) reps = try std.fmt.parseInt(usize, value, 10) else return error.InvalidArgument;
    }
    if (warmup > 100 or reps < 3 or reps > 1000) return error.InvalidArgument;
    const path = model_dir orelse return error.MissingArgument;
    const selected = backend orelse return error.MissingArgument;
    var manifest = try manifest_mod.loadFromDir(a, path);
    defer manifest.deinit();
    if (manifest.gliner_classification_head != .label_marker_mlp) return error.UnsupportedGlinerDecisionHead;
    const tokenizer_bytes = try c_file.readFile(a, manifest.tokenizer_json_path orelse return error.NoTokenizerFound);
    defer a.free(tokenizer_bytes);
    const tokenizer = try hf.HfTokenizer.loadFromBytesWithOptions(a, tokenizer_bytes, .{ .strict_unigram_normalizer = true });
    const tok = tokenizer.tokenizer();
    defer tok.deinitTokenizer();
    const session = if (std.mem.eql(u8, selected, "cuda"))
        try factory.createCudaSession(a, path)
    else if (std.mem.eql(u8, selected, "native"))
        try factory.createNativeSession(a, path)
    else
        return error.InvalidBackend;
    defer session.close();
    var pipeline = gliner.GlinerPipeline{
        .allocator = a,
        .session = session,
        .tok = tok,
        .config = .{
            .model_type = manifest.gliner_model_type,
            .classification_head = .label_marker_mlp,
            .max_length = manifest.max_position_embeddings,
            .token_p = manifest.gliner_token_p,
            .token_l = manifest.gliner_token_l,
            .token_sep_struct = manifest.gliner_token_sep_struct,
            .token_sep_text = manifest.gliner_token_sep_text,
        },
    };
    const intent = [_]gliner.DecisionLabel{ .{ .name = "refund" }, .{ .name = "technical_support" }, .{ .name = "sales" } };
    const urgency = [_]gliner.DecisionLabel{ .{ .name = "low" }, .{ .name = "medium" }, .{ .name = "high" } };
    const tasks = [_]gliner.DecisionTask{
        .{ .name = "intent", .labels = &intent },
        .{ .name = "urgency", .labels = &urgency },
    };
    const request = gliner.DecisionRequest{
        .text = "Please refund the duplicate charge. I do not need technical help.",
        .tasks = &tasks,
    };
    const samples = try a.alloc(f64, reps);
    defer a.free(samples);
    for (0..warmup + reps) |i| {
        const start = inference.platform.time.monotonicNs();
        const result = try pipeline.decideBatch(&.{request});
        const elapsed_ns = inference.platform.time.monotonicNs() - start;
        if (result.len != 1 or result[0].tasks.len != 2 or
            result[0].tasks[0].selections[0].label_index != 0 or
            result[0].tasks[1].selections[0].label_index != 0)
            return error.DecisionBenchmarkWinnerMismatch;
        for (result) |*row| row.deinit(a);
        a.free(result);
        if (i >= warmup) samples[i - warmup] = @as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_ms;
    }
    std.mem.sort(f64, samples, {}, struct {
        fn less(_: void, left: f64, right: f64) bool {
            return left < right;
        }
    }.less);
    const median = if (reps % 2 == 0) (samples[reps / 2 - 1] + samples[reps / 2]) / 2 else samples[reps / 2];
    const p95 = samples[@min(reps - 1, (reps * 95 + 99) / 100 - 1)];
    const output = try std.json.Stringify.valueAlloc(a, .{
        .implementation = "antfly_gliner25_decide",
        .backend = selected,
        .warmup = warmup,
        .reps = reps,
        .median_ms = median,
        .p95_ms = p95,
        .min_ms = samples[0],
        .max_ms = samples[reps - 1],
        .samples_ms_sorted = samples,
    }, .{});
    defer a.free(output);
    try std.Io.File.stdout().writeStreamingAll(init.io, output);
    try std.Io.File.stdout().writeStreamingAll(init.io, "\n");
}
