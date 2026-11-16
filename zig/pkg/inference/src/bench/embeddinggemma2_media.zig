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

//! Complete raw image/audio requests through the production media pipeline.
const std = @import("std");
const internal = @import("inference_internal");
const factory = internal.architectures.session_factory;
const pipeline = internal.pipelines.embedding_gemma2;
const Part = struct { kind: enum { image, audio, text }, path: ?[]const u8 = null, text: ?[]const u8 = null };
const Case = struct { name: []const u8, parts: []const Part };
const Suite = struct { cases: []const Case };
const Measurement = struct { name: []const u8, input_tokens: []usize, samples_seconds: []f64, vectors: [][]f32, stages: []pipeline.MediaTimings };
fn now() u64 {
    return internal.platform.time.monotonicNs();
}
pub fn main(init: std.process.Init) !void {
    var lifetime = internal.platform.inference_process_supervisor.WorkerLifetime{};
    defer lifetime.deinit(init.io);
    if (try internal.platform.inference_process_supervisor.runIfNeeded(init, 1, &lifetime)) return;
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const args = try init.minimal.args.toSlice(a);
    var model: ?[]const u8 = null;
    var suite: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    var iterations: usize = 8;
    var warmups: usize = 3;
    var selected: ?[]const u8 = null;
    var i: usize = 1;
    while (i < args.len) : (i += 2) {
        if (i + 1 >= args.len) return error.MissingArgument;
        const k = args[i];
        const v = args[i + 1];
        if (std.mem.eql(u8, k, "--model")) model = v else if (std.mem.eql(u8, k, "--suite")) suite = v else if (std.mem.eql(u8, k, "--output")) output = v else if (std.mem.eql(u8, k, "--case")) selected = v else if (std.mem.eql(u8, k, "--iterations")) iterations = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--warmups")) warmups = try std.fmt.parseInt(usize, v, 10) else return error.UnknownArgument;
    }
    if (iterations == 0 or iterations > 100 or warmups > 100) return error.InvalidIterations;
    const model_path = model orelse return error.MissingModel;
    const raw = try std.Io.Dir.cwd().readFileAlloc(init.io, suite orelse return error.MissingSuite, a, .limited(16 * 1024 * 1024));
    const parsed = try std.json.parseFromSlice(Suite, a, raw, .{ .ignore_unknown_fields = true });
    const watchdog = try internal.HardCancellationWatchdog.create(std.heap.c_allocator);
    defer watchdog.destroy();
    try watchdog.start(init.io);
    const control = internal.execution_control.InferenceExecutionControl{ .io = init.io, .hard_cancellation = watchdog.boundary() };
    const session = try factory.createCudaSession(a, model_path);
    defer session.close();
    factory.attachIo(session, init.io);
    const tok_path = try std.fs.path.join(a, &.{ model_path, "tokenizer.json" });
    const tok_bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, tok_path, a, .limited(64 * 1024 * 1024));
    const tok = try internal.hf_tokenizer.HfTokenizer.loadFromBytes(a, tok_bytes);
    defer tok.deinitSelf();
    var results = std.ArrayList(Measurement).empty;
    for (parsed.value.cases) |case| {
        if (selected) |name| {
            if (!std.mem.eql(u8, name, case.name)) continue;
        }
        const samples = try a.alloc(f64, iterations);
        const vectors = try a.alloc([]f32, iterations);
        const tokens = try a.alloc(usize, iterations);
        const stages = try a.alloc(pipeline.MediaTimings, iterations);
        for (0..warmups + iterations) |rep| {
            var request = std.heap.ArenaAllocator.init(std.heap.c_allocator);
            defer request.deinit();
            const ra = request.allocator();
            const started = now();
            const parts = try ra.alloc(pipeline.Part, case.parts.len);
            for (case.parts, parts) |p, *dst| {
                if (p.kind == .text) {
                    dst.* = .{ .text = p.text orelse return error.MissingText };
                    continue;
                }
                const path = try std.fs.path.resolve(ra, &.{ std.fs.path.dirname(suite.?) orelse ".", p.path orelse return error.MissingPath });
                const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, path, ra, .limited(64 * 1024 * 1024));
                dst.* = if (p.kind == .image) .{ .image = bytes } else .{ .audio = .{ .bytes = bytes } };
            }
            var timings = pipeline.MediaTimings{};
            const result = try pipeline.embed(ra, session, tok.tokenizer(), .{ .content = parts }, .{ .timings = &timings }, null, control);
            const elapsed = now() - started;
            if (result.vector.len != 768) return error.InvalidVector;
            var norm: f64 = 0;
            for (result.vector) |x| {
                if (!std.math.isFinite(x)) return error.NonFiniteVector;
                norm += @as(f64, x) * x;
            }
            if (@abs(norm - 1) > 0.001) return error.InvalidVectorNorm;
            if (rep >= warmups) {
                const j = rep - warmups;
                samples[j] = @as(f64, @floatFromInt(elapsed)) / std.time.ns_per_s;
                vectors[j] = try a.dupe(f32, result.vector);
                tokens[j] = result.input_tokens;
                stages[j] = timings;
            }
            std.debug.print("media {s} rep={d} tokens={d} ms={d}\n", .{ case.name, rep, result.input_tokens, @as(f64, @floatFromInt(elapsed)) / 1e6 });
        }
        try results.append(a, .{ .name = case.name, .input_tokens = tokens, .samples_seconds = samples, .vectors = vectors, .stages = stages });
    }
    if (results.items.len == 0) return error.NoCases;
    const report = try std.json.Stringify.valueAlloc(a, .{ .scope = "local encoded file read, decode, preprocessing, tokenization, media tower, shared encoder, pooling and synchronized vector readback; excludes process/model startup and request cleanup", .backend = "cuda", .execution_policy = factory.getEmbeddingGemma2CudaPolicy(session), .warmups = warmups, .iterations = iterations, .stage_scope = "enqueue/host wall observations; asynchronous stage times are not additive device timings", .cases = results.items }, .{ .whitespace = .indent_2 });
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = output orelse return error.MissingOutput, .data = report });
}
