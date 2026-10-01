// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Diagnostic and regression coverage for the in-process "managed_direct"
//! dense-embedding entry point (used by the Lite/dogfood in-process worker
//! bridge, `host.linkedInferenceInvokeProvider` -> `Node.embedDenseTextsDirect*`
//! in antfly/src/standalone/inference_host.zig) versus the HTTP embeddings
//! handler, for the SAME warm session and batch.
//!
//! Context: the enrichment concurrency handoff
//! (lite-enrichment-concurrency-handoff.md) measured the in-process drain at
//! roughly half the direct-call-baseline per-item throughput at the same
//! batch size and asked this package to check whether the managed_direct
//! entry point itself carries extra overhead versus the HTTP handler. This
//! test found none: both call the identical acquire/broker/execute path
//! (runLoadedEmbeddingRuntimeWithRecovery -> tryEmbedTextsViaBroker ->
//! embedDenseTextsOnLoadedModel) for a warm session, and measured latency
//! parity (ratio ~1.0) for a 28-item batch against a real pulled Qwen3
//! embedding checkpoint. The extraction equivalent (extractDirect vs the
//! HTTP extract route) shares one function, extractWithAdmission, that
//! differs only by an admission-accounting enum, so there is no separate
//! code path there to diverge either. The remaining ~0.4-0.9s/batch gap in
//! the handoff is therefore not inside these two entry points; it most
//! likely comes from the worker subprocess IPC boundary (zig/pkg/antfly,
//! outside this package) or from real Metal/GPU scheduling contention
//! between the two concurrently running streams (dense embedding and
//! extraction) under the two-stream execution model, neither of which a
//! single-model, single-caller, in-process comparison like this one can
//! reproduce. This test remains as a regression guard: if a future change
//! introduces real per-entry-point overhead, it will fail.
//!
//! Gated on ANTFLY_QWEN3_EMBED_MODEL_DIR so it only runs with a real pulled
//! Qwen3 embedding checkpoint.
const std = @import("std");
const httpx = @import("httpx");
const platform = @import("antfly_platform");
const server = @import("server.zig");
const Node = server.Node;

fn now() u64 {
    return platform.time.monotonicNs();
}

const Body = struct { model: []const u8, input: []const []const u8 };

fn httpEmbedOnce(a: std.mem.Allocator, node: *Node, model_path: []const u8, inputs: []const []const u8) !void {
    const body = try std.json.Stringify.valueAlloc(a, Body{ .model = model_path, .input = inputs }, .{});
    defer a.free(body);
    var request = try httpx.Request.init(a, .POST, "/ai/v1/embeddings");
    defer request.deinit();
    try request.setJson(body);
    var ctx = httpx.Context.init(a, std.testing.io, &request);
    defer ctx.deinit();
    var response = try node.createEmbedding(&ctx);
    defer response.deinit();
    if (response.status.code != 200) {
        std.debug.print("embed HTTP call failed: status={d} body={s}\n", .{ response.status.code, response.body orelse "" });
        return error.UnexpectedEmbedStatus;
    }
}

test "embedding managed_direct path matches HTTP path per-batch latency for a warm model" {
    const directory = platform.env.getenv("ANTFLY_QWEN3_EMBED_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var path_buffer: [std.Io.Dir.max_path_bytes]u8 = undefined;
    const path_len = if (std.fs.path.isAbsolute(directory))
        try std.Io.Dir.realPathFileAbsolute(std.testing.io, directory, &path_buffer)
    else
        try std.Io.Dir.cwd().realPathFile(std.testing.io, directory, &path_buffer);
    const model_path = path_buffer[0..path_len];
    const models_root = std.fs.path.dirname(model_path) orelse return error.InvalidModelPath;
    // The HTTP-facing resolver requires a relative identifier within
    // models_dir (validateRequestModelIdentifier); the managed_direct entry
    // point is documented for callers that already resolved an absolute,
    // authorized directory, so it takes model_path directly.
    const http_model_name = std.fs.path.basename(model_path);

    var texts_buf: [28][]const u8 = undefined;
    for (&texts_buf, 0..) |*slot, i| {
        slot.* = try std.fmt.allocPrint(a, "Antfly qualification benchmark input item number {d} for warm-model batch timing comparison.", .{i});
    }
    defer for (texts_buf) |text| a.free(text);

    var node = try Node.init(a, .{
        .models_dir = models_root,
        .max_concurrent_requests = 4,
        .max_loaded_models = 0,
        .generation_budget_overrides = .{ .host_limit_bytes = 4 * 1024 * 1024 * 1024, .scratch_limit_bytes = 512 * 1024 * 1024 },
    });
    defer node.deinit();
    try node.attachIo(std.testing.io);

    // Warm the session on both entry points before timing anything: model
    // load, tokenizer materialization, and first-call setup must not be
    // charged to either path's steady-state per-batch cost.
    try httpEmbedOnce(a, &node, http_model_name, &texts_buf);
    {
        const warm = try node.embedDenseTextsDirectWithExecutionControl(a, std.testing.io, .{}, model_path, &texts_buf);
        for (warm) |vector| a.free(vector);
        a.free(warm);
    }

    const iterations = 8;
    var http_total: u64 = 0;
    for (0..iterations) |_| {
        const started = now();
        try httpEmbedOnce(a, &node, http_model_name, &texts_buf);
        http_total += now() -| started;
    }
    var direct_total: u64 = 0;
    for (0..iterations) |_| {
        const started = now();
        const result = try node.embedDenseTextsDirectWithExecutionControl(a, std.testing.io, .{}, model_path, &texts_buf);
        direct_total += now() -| started;
        for (result) |vector| a.free(vector);
        a.free(result);
    }

    const http_avg_ms = @as(f64, @floatFromInt(http_total)) / @as(f64, iterations) / std.time.ns_per_ms;
    const direct_avg_ms = @as(f64, @floatFromInt(direct_total)) / @as(f64, iterations) / std.time.ns_per_ms;
    std.debug.print(
        "embed managed_direct vs http, warm {d}-item batch over {d} iterations: http={d:.2}ms/batch direct={d:.2}ms/batch ratio={d:.3}\n",
        .{ texts_buf.len, iterations, http_avg_ms, direct_avg_ms, direct_avg_ms / http_avg_ms },
    );

    // The two paths execute against the identical warm session, tokenizer,
    // and backend; embedDenseTextsDirectWithExecutionControl must not carry
    // meaningfully more overhead per batch than the HTTP handler for the
    // same request. This is the regression guard for the enrichment
    // concurrency handoff's measured ~2x gap.
    try std.testing.expect(direct_avg_ms <= http_avg_ms * 1.25 + 5.0);
}
