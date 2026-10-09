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

//! Offline artifact comparison. Never grants public serving qualification.
const std = @import("std");
const inference = @import("inference_internal");
const factory = inference.architectures.session_factory;
const processor = inference.pipelines.gliner_boundary_processor;
const schema = inference.pipelines.extraction_schema;
const executor = inference.gliner_span_v2_executor;
const Case = struct {
    id: []const u8,
    text: []const u8,
    schema_json: []const u8,
    input_ids: []const i64,
    logits: []const []const f64,
};
const Report = struct { id: []const u8, tokens: usize, median_ms: f64, p95_ms: f64, core_median_ms: f64, max_logit_error: f64, logits: [][]f64, samples_ms: []f64, core_samples_ms: []f64 };

pub fn main(init: std.process.Init) !void {
    const a = inference.platform.allocator.processAllocator(std.heap.smp_allocator);
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    var path: ?[]const u8 = null;
    var capture: ?[]const u8 = null;
    var backend: []const u8 = "native";
    var warmups: usize = 3;
    var reps: usize = 20;
    var tolerance: f64 = 0.05;
    while (args.next()) |key| {
        const value = args.next() orelse return error.MissingArgument;
        if (std.mem.eql(u8, key, "--model-dir")) path = value else if (std.mem.eql(u8, key, "--capture")) capture = value else if (std.mem.eql(u8, key, "--backend")) backend = value else if (std.mem.eql(u8, key, "--warmups")) warmups = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, key, "--reps")) reps = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, key, "--tolerance")) tolerance = try std.fmt.parseFloat(f64, value) else return error.InvalidArgument;
    }
    if (reps < 3 or reps > 1000 or warmups > 100 or !std.math.isFinite(tolerance) or tolerance <= 0) return error.InvalidArgument;
    const bytes = try inference.util.c_file.readFile(a, capture orelse return error.MissingArgument);
    defer a.free(bytes);
    const cases = try std.json.parseFromSlice([]const Case, a, bytes, .{});
    defer cases.deinit();
    const directory = path orelse return error.MissingArgument;
    const start_load = inference.platform.time.monotonicNs();
    const session = if (std.mem.eql(u8, backend, "metal")) try factory.createMetalSession(a, directory) else if (std.mem.eql(u8, backend, "native")) try factory.createNativeSession(a, directory) else if (std.mem.eql(u8, backend, "cuda")) try factory.createCudaSession(a, directory) else return error.InvalidBackend;
    defer session.close();
    if (std.mem.eql(u8, backend, "metal") and session.backend() != .metal) return error.InvalidBackend;
    if (std.mem.eql(u8, backend, "cuda") and session.backend() != .cuda) return error.InvalidBackend;
    const config = try factory.getGlinerSpanConfig(session);
    if (config != .modern_bert) return error.InvalidModel;
    const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tok_bytes = try inference.util.c_file.readFile(a, tokenizer_path);
    defer a.free(tok_bytes);
    const tok = try inference.hf_tokenizer.HfTokenizer.loadFromBytes(a, tok_bytes);
    defer tok.tokenizer().deinitTokenizer();
    const load_ms = @as(f64, @floatFromInt(inference.platform.time.monotonicNs() - start_load)) / std.time.ns_per_ms;
    const watchdog = if (std.mem.eql(u8, backend, "metal") or std.mem.eql(u8, backend, "cuda")) try inference.HardCancellationWatchdog.create(a) else null;
    defer if (watchdog) |owner| owner.destroy();
    if (watchdog) |owner| try owner.start(init.io);
    var report_arena = std.heap.ArenaAllocator.init(a);
    defer report_arena.deinit();
    const out = report_arena.allocator();
    const reports = try out.alloc(Report, cases.value.len);
    for (cases.value, reports) |case, *report| {
        const samples = try out.alloc(f64, reps);
        const core_samples = try out.alloc(f64, reps);
        var max_error: f64 = 0;
        var last_rows: [][]f64 = &.{};
        for (0..1 + warmups + reps) |iteration| {
            var request_arena = std.heap.ArenaAllocator.init(a);
            defer request_arena.deinit();
            const ra = request_arena.allocator();
            const control = inference.InferenceExecutionControl{
                .hard_cancellation = if (watchdog) |owner| owner.boundary() else null,
                .deadline_ns = inference.platform.time.monotonicNs() + 120 * std.time.ns_per_s,
            };
            const start = inference.platform.time.monotonicNs();
            var compiled = try schema.compile(ra, case.schema_json, .{});
            defer compiled.deinit();
            var prepared = try processor.prepare(ra, tok.tokenizer(), &.{.{ .text = case.text, .schema = &compiled }}, .{ .max_sequence_tokens = 7999, .max_batch_tokens = 7999 });
            defer prepared.deinit();
            if (prepared.samples.len != 1) return error.InvalidCase;
            const counts = try ra.alloc(usize, compiled.schema.classifications.len);
            for (compiled.schema.classifications, counts) |classification, *count| count.* = classification.task.labels.len;
            const core_start = inference.platform.time.monotonicNs();
            var managed = try factory.getManagedComputeBackend(session, ra, null, control);
            defer managed.deinit();
            const rows = try executor.classificationLogits(&managed.backend, ra, .{ .modern_bert = config.modern_bert }, prepared.samples[0], counts);
            const end = inference.platform.time.monotonicNs();
            const elapsed = @as(f64, @floatFromInt(end - start)) / std.time.ns_per_ms;
            const core_elapsed = @as(f64, @floatFromInt(end - core_start)) / std.time.ns_per_ms;
            if (!std.mem.eql(i64, prepared.input_ids, case.input_ids) or rows.len != case.logits.len) return error.PreprocessingMismatch;
            for (rows, case.logits) |got, expected| {
                if (got.len != expected.len) return error.LogitShapeMismatch;
                for (got, expected) |actual, want| {
                    if (!std.math.isFinite(actual) or !std.math.isFinite(want)) return error.NonFiniteLogit;
                    const delta = @abs(actual - want);
                    max_error = @max(max_error, delta);
                    if (delta > tolerance) {
                        std.debug.print("{s}: logit error {d} exceeds {d}\n", .{ case.id, delta, tolerance });
                        return error.LogitMismatch;
                    }
                }
            }
            if (iteration > warmups) {
                samples[iteration - warmups - 1] = elapsed;
                core_samples[iteration - warmups - 1] = core_elapsed;
            }
            if (iteration == warmups + reps) {
                last_rows = try out.alloc([]f64, rows.len);
                for (rows, last_rows) |row, *saved| saved.* = try out.dupe(f64, row);
            }
        }
        const sorted = try out.dupe(f64, samples);
        std.mem.sort(f64, sorted, {}, struct {
            fn less(_: void, x: f64, y: f64) bool {
                return x < y;
            }
        }.less);
        const core_sorted = try out.dupe(f64, core_samples);
        std.mem.sort(f64, core_sorted, {}, struct {
            fn less(_: void, x: f64, y: f64) bool {
                return x < y;
            }
        }.less);
        report.* = .{ .id = case.id, .tokens = case.input_ids.len, .median_ms = if (reps % 2 == 0) (sorted[reps / 2 - 1] + sorted[reps / 2]) / 2 else sorted[reps / 2], .p95_ms = sorted[@min(reps - 1, (reps * 95 + 99) / 100 - 1)], .core_median_ms = if (reps % 2 == 0) (core_sorted[reps / 2 - 1] + core_sorted[reps / 2]) / 2 else core_sorted[reps / 2], .max_logit_error = max_error, .logits = last_rows, .samples_ms = samples, .core_samples_ms = core_samples };
        std.debug.print("{s} {s}: {d:.3} ms, error {d:.6}\n", .{ backend, case.id, report.median_ms, max_error });
    }
    const result = try std.json.Stringify.valueAlloc(a, .{ .scope = "offline loaded pipeline: schema compile, tokenization, encoder, classifier and completed readback; excludes HTTP, model load, and output presentation", .core_scope = "prepared encoder, classifier and completed CPU readback; excludes schema compilation and tokenization", .build_mode = @tagName(@import("builtin").mode), .backend = backend, .model_dir = directory, .load_ms = load_ms, .warmups = warmups, .reps = reps, .validation_preflights = 1, .tolerance = tolerance, .reports = reports, .cuda_stats = factory.getCudaRuntimeStats(session) }, .{});
    defer a.free(result);
    try std.Io.File.stdout().writeStreamingAll(init.io, result);
    try std.Io.File.stdout().writeStreamingAll(init.io, "\n");
}
