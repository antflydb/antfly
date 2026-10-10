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

//! Loaded-model request latency for the pinned Decide multi-question case.
const std = @import("std");
const builtin = @import("builtin");
const linalg = @import("inference_linalg");
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
    var cases_path: ?[]const u8 = null;
    var case_index: usize = 0;
    var all_cases = false;
    var worker = false;
    var precision: factory.GlinerCudaPrecision = .fp32;
    while (args.next()) |arg| {
        const value = args.next() orelse return error.MissingArgument;
        if (std.mem.eql(u8, arg, "--model-dir")) model_dir = value else if (std.mem.eql(u8, arg, "--backend")) backend = value else if (std.mem.eql(u8, arg, "--warmup")) warmup = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, arg, "--reps")) reps = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, arg, "--cases")) cases_path = value else if (std.mem.eql(u8, arg, "--case")) case_index = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, arg, "--worker")) worker = (try std.fmt.parseInt(u8, value, 10)) == 1 else if (std.mem.eql(u8, arg, "--all-cases")) all_cases = (try std.fmt.parseInt(u8, value, 10)) == 1 else if (std.mem.eql(u8, arg, "--precision")) precision = std.meta.stringToEnum(factory.GlinerCudaPrecision, value) orelse return error.InvalidArgument else return error.InvalidArgument;
    }
    if (warmup > 100 or reps < 3 or reps > 1000) return error.InvalidArgument;
    const path = model_dir orelse return error.MissingArgument;
    const selected = backend orelse return error.MissingArgument;
    if (!std.mem.eql(u8, selected, "cuda") and precision != .auto and precision != .fp32) return error.InvalidArgument;
    var manifest = try manifest_mod.loadFromDir(a, path);
    defer manifest.deinit();
    if (manifest.gliner_classification_head != .label_marker_mlp) return error.UnsupportedGlinerDecisionHead;
    const tokenizer_bytes = try c_file.readFile(a, manifest.tokenizer_json_path orelse return error.NoTokenizerFound);
    defer a.free(tokenizer_bytes);
    const tokenizer = try hf.HfTokenizer.loadFromBytesWithOptions(a, tokenizer_bytes, .{ .strict_unigram_normalizer = true });
    const tok = tokenizer.tokenizer();
    defer tok.deinitTokenizer();
    const session = if (std.mem.eql(u8, selected, "cuda"))
        try factory.createGlinerCudaSessionWithPrecision(a, path, precision)
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
    if (worker) {
        if (cases_path == null or !std.mem.eql(u8, selected, "cuda")) return error.InvalidArgument;
        const ready = try std.json.Stringify.valueAlloc(a, .{ .event = "ready", .backend = selected, .precision = if (precision == .auto) "fp32" else @tagName(precision), .build_mode = @tagName(builtin.mode), .max_position_embeddings = manifest.max_position_embeddings, .timing_boundary = "public_batch_api_loaded_model", .qualification = false }, .{});
        defer a.free(ready);
        try std.Io.File.stdout().writeStreamingAll(init.io, ready);
        try std.Io.File.stdout().writeStreamingAll(init.io, "\n");
        var buffer: [4096]u8 = undefined;
        var stdin = std.Io.File.stdin().readerStreaming(init.io, &buffer);
        const Command = struct { request_id: u32, op: enum { validate, run, stop }, case_index: usize = 0 };
        var commands: usize = 0;
        while (try stdin.interface.takeDelimiter('\n')) |line| {
            if (line.len > 2048 or commands >= 65536) return error.BenchmarkCommandLimitExceeded;
            commands += 1;
            const command = try std.json.parseFromSlice(Command, a, line, .{});
            defer command.deinit();
            if (command.value.op == .stop) return;
            try runCase(init.io, &pipeline, selected, precision, 0, 1, cases_path, command.value.case_index, command.value.request_id);
        }
        return error.BenchmarkProtocolEndedWithoutStop;
    }
    if (all_cases) {
        const bytes = try c_file.readFile(a, cases_path orelse return error.MissingArgument);
        defer a.free(bytes);
        const parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
        defer parsed.deinit();
        const cases = parsed.value.object.get("cases") orelse return error.InvalidFixture;
        if (cases != .array or cases.array.items.len == 0) return error.InvalidFixture;
        for (0..cases.array.items.len) |index| try runCase(init.io, &pipeline, selected, precision, warmup, reps, cases_path, index, null);
    } else try runCase(init.io, &pipeline, selected, precision, warmup, reps, cases_path, case_index, null);
}

fn runCase(io: std.Io, pipeline: *gliner.GlinerPipeline, selected: []const u8, precision: factory.GlinerCudaPrecision, warmup: usize, reps: usize, cases_path: ?[]const u8, case_index: usize, request_id: ?u32) !void {
    const a = std.heap.c_allocator;
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
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const requests: []const gliner.DecisionRequest = if (cases_path) |file|
        try loadCase(scratch, file, case_index)
    else
        &.{request};
    var prepared = try pipeline.prepareDecisionBatch(requests);
    defer prepared.deinit();
    const token_rows = try scratch.alloc([]const i64, prepared.rows.len);
    for (prepared.rows, token_rows) |row, *dest| dest.* = row.input_ids;
    var captured: ?[]gliner.DecisionResult = null;
    defer if (captured) |results| {
        for (results) |*row| row.deinit(a);
        a.free(results);
    };
    const samples = try a.alloc(f64, reps);
    defer a.free(samples);
    const stats_before = factory.getCudaRuntimeStats(pipeline.session);
    for (0..warmup + reps) |i| {
        const start = inference.platform.time.monotonicNs();
        const result = try pipeline.decideBatch(requests);
        const elapsed_ns = inference.platform.time.monotonicNs() - start;
        if (cases_path == null and (result.len != 1 or result[0].tasks.len != 2 or
            result[0].tasks[0].selections[0].label_index != 0 or
            result[0].tasks[1].selections[0].label_index != 0))
            return error.DecisionBenchmarkWinnerMismatch;
        if (captured == null) {
            captured = result;
        } else {
            for (result) |*row| row.deinit(a);
            a.free(result);
        }
        if (i >= warmup) samples[i - warmup] = @as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_ms;
    }
    const CudaEvidence = struct { kernel_launches: usize, h2d_bytes: usize, d2h_bytes: usize, downloads: usize };
    const cuda_transfers: ?CudaEvidence = if (comptime factory.CudaRuntimeStats != void) blk: {
        const before = stats_before orelse break :blk null;
        factory.drainCudaProfile(pipeline.session);
        const delta = factory.cudaStatsDelta(factory.getCudaRuntimeStats(pipeline.session).?, before);
        if (factory.cudaOpProfileLoggingEnabled()) {
            var buffer: [768]u8 = undefined;
            std.debug.print("{s}\n", .{factory.formatCudaPrefillProfileLine(&buffer, delta)});
        }
        break :blk .{ .kernel_launches = delta.kernel_launches, .h2d_bytes = delta.h2d_bytes, .d2h_bytes = delta.d2h_bytes, .downloads = delta.to_float32_calls };
    } else null;
    std.mem.sort(f64, samples, {}, struct {
        fn less(_: void, left: f64, right: f64) bool {
            return left < right;
        }
    }.less);
    const median = if (reps % 2 == 0) (samples[reps / 2 - 1] + samples[reps / 2]) / 2 else samples[reps / 2];
    const p95 = samples[@min(reps - 1, (reps * 95 + 99) / 100 - 1)];
    const output = try std.json.Stringify.valueAlloc(a, .{
        .implementation = "antfly_gliner25_decide",
        .request_id = request_id,
        .duration_ns = @as(u64, @intFromFloat(median * std.time.ns_per_ms)),
        .backend = selected,
        .precision = if (precision == .auto) "fp32" else @tagName(precision),
        .case_index = case_index,
        .batch_size = requests.len,
        .prepared_input_ids = token_rows,
        .decisions = captured.?,
        .cuda_transfers = cuda_transfers,
        .build_mode = @tagName(builtin.mode),
        .zig_version = builtin.zig_version_string,
        .x86_kernel = if (linalg.x86.enabled) @tagName(linalg.x86.selected()) else null,
        .effective_cpu_threads = linalg.pool.cachedCpuCount(),
        .warmup = warmup,
        .reps = reps,
        .median_ms = median,
        .p95_ms = p95,
        .min_ms = samples[0],
        .max_ms = samples[reps - 1],
        .samples_ms_sorted = samples,
    }, .{});
    defer a.free(output);
    try std.Io.File.stdout().writeStreamingAll(io, output);
    try std.Io.File.stdout().writeStreamingAll(io, "\n");
}

fn loadCase(a: std.mem.Allocator, path: []const u8, index: usize) ![]const gliner.DecisionRequest {
    const Fixture = struct { format_version: u32, cases: []const struct { id: []const u8, texts: []const []const u8, tasks: std.json.Value } };
    const bytes = try c_file.readFile(a, path);
    const parsed = try std.json.parseFromSlice(Fixture, a, bytes, .{ .allocate = .alloc_always });
    if (parsed.value.format_version != 1 or index >= parsed.value.cases.len) return error.InvalidFixture;
    const case = parsed.value.cases[index];
    if (case.tasks != .object or case.texts.len == 0) return error.InvalidFixture;
    const tasks = try a.alloc(gliner.DecisionTask, case.tasks.object.count());
    var iter = case.tasks.object.iterator();
    var i: usize = 0;
    while (iter.next()) |entry| : (i += 1) {
        const config = entry.value_ptr.*;
        const labels_value = if (config == .object) config.object.get("labels") orelse return error.InvalidFixture else config;
        const labels = try a.alloc(gliner.DecisionLabel, switch (labels_value) {
            .object => labels_value.object.count(),
            .array => labels_value.array.items.len,
            else => return error.InvalidFixture,
        });
        if (labels_value == .array) {
            for (labels_value.array.items, labels) |label, *dest| {
                if (label != .string) return error.InvalidFixture;
                dest.* = .{ .name = label.string };
            }
        } else {
            for (labels_value.object.keys(), labels_value.object.values(), labels) |key, value, *dest| {
                if (value != .string) return error.InvalidFixture;
                dest.* = .{ .name = key, .description = value.string };
            }
        }
        tasks[i] = .{ .name = entry.key_ptr.*, .labels = labels };
        if (config == .object) {
            if (config.object.get("prompt")) |v| tasks[i].prompt = v.string;
            if (config.object.get("multi_label")) |v| tasks[i].multi_label = v.bool;
            if (config.object.get("cls_threshold")) |v| tasks[i].threshold = switch (v) {
                .float => @floatCast(v.float),
                .integer => @floatFromInt(v.integer),
                else => return error.InvalidFixture,
            };
        }
    }
    const requests = try a.alloc(gliner.DecisionRequest, case.texts.len);
    for (case.texts, requests) |text, *dest| dest.* = .{ .text = text, .tasks = tasks };
    return requests;
}
