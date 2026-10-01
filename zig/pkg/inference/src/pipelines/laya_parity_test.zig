// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const platform = @import("antfly_platform");
const c_file = @import("../util/c_file.zig");
const factory = @import("../architectures/session_factory.zig");
const hf = @import("inference_hf_tokenizer");
const pipeline = @import("laya.zig");
const test_support = @import("../util/laya_test_support.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const Reference = struct {
    intermediates: ?struct { encoder: []const f32, head_hidden: []const f32, marker_scores: []const f32, action_features: []const f32 } = null,
    states: []const []const u8,
    sequences: []const struct { ids: []const i64, markers: []const i64, qtype: i64 },
    logits: []const []const f32,
    action_logits: []const []const f32,
};

test "laya forward preprocessing and batching match the PyTorch reference" {
    const root = try test_support.fixture("ANTFLY_LAYA_REFERENCE");
    const a = std.testing.allocator;
    const model_path = try std.fmt.allocPrint(a, "{s}/model", .{root});
    defer a.free(model_path);
    const reference_path = try std.fmt.allocPrint(a, "{s}/reference.json", .{root});
    defer a.free(reference_path);
    const bytes = try c_file.readFile(a, reference_path);
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(Reference, a, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const ref = parsed.value;
    var session = try test_support.createSession(a, model_path);
    std.debug.print("Laya parity backend: {s}\n", .{@tagName(session.backend())});
    defer session.close();
    const config = factory.getLayaConfig(session) orelse return error.TestUnexpectedResult;
    const tokenizer_bytes = try c_file.readFileFromDir(a, model_path, "tokenizer.json");
    defer a.free(tokenizer_bytes);
    const tokenizer = try hf.HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    const tok = tokenizer.tokenizer();
    defer tok.deinitTokenizer();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const questions = [_]pipeline.Question{
        .{ .name = "tool", .kind = .choice, .instruction = "which tool is needed?", .labels = &.{ "search", "fetch", "none" }, .descriptions = &.{ "", "", "" } },
        .{ .name = "urgency", .kind = .score, .instruction = "urgency?", .labels = &.{ "low", "medium", "high" }, .descriptions = &.{ "", "", "" } },
        .{ .name = "needed", .kind = .noul, .instruction = "is search needed?", .labels = &.{ "false", "true" }, .descriptions = &.{ "", "" } },
    };
    var tasks: [3]pipeline.Task = undefined;
    var seq: usize = 0;
    for (&tasks, questions, ref.states, ref.sequences) |*task, question, text, expected| {
        task.* = .{ .text = text, .question = question };
        const prepared = try pipeline.prepare(alloc, tok, config, task.*);
        try std.testing.expectEqualSlices(i64, expected.ids, prepared.ids);
        try std.testing.expectEqualSlices(i64, expected.markers, prepared.markers);
        seq = @max(seq, expected.ids.len);
    }
    const ids = try alloc.alloc(i64, 3 * seq);
    @memset(ids, 0);
    const masks = try alloc.alloc(i64, ids.len);
    @memset(masks, 0);
    const positions = try alloc.alloc(i64, 9);
    @memset(positions, -1);
    for (ref.sequences, 0..) |row, i| {
        @memcpy(ids[i * seq ..][0..row.ids.len], row.ids);
        @memset(masks[i * seq ..][0..row.ids.len], 1);
        @memcpy(positions[i * 3 ..][0..row.markers.len], row.markers);
    }
    var inputs: [4]Tensor = .{
        try Tensor.initInt64(alloc, "input_ids", &.{ 3, @intCast(seq) }, ids),
        try Tensor.initInt64(alloc, "attention_mask", &.{ 3, @intCast(seq) }, masks),
        try Tensor.initInt64(alloc, "qtype", &.{ 3, 1 }, &.{ 0, 1, 2 }),
        try Tensor.initInt64(alloc, "marker_pos", &.{ 3, 3 }, positions),
    };
    defer for (&inputs) |*input| input.deinit();
    const transfer_before = factory.getCudaRuntimeStats(session);
    const outputs = try session.run(&inputs, alloc);
    try test_support.expectReadbacks(session, transfer_before, 3 * (3 + 2) * @sizeOf(f32));
    defer {
        for (outputs) |*output| output.deinit();
        alloc.free(outputs);
    }
    try std.testing.expectEqual(@as(usize, 2), outputs.len);
    if (ref.intermediates) |expected| {
        const modern = @import("../architectures/modern_bert.zig");
        const cfg_bytes = try c_file.readFileFromDir(a, model_path, "config.json");
        defer a.free(cfg_bytes);
        const encoder_config = try modern.parseConfig(a, cfg_bytes);
        const cb = try factory.getComputeBackend(session, a);
        defer cb.deinit();
        const encoded = try modern.forwardCT(&cb, a, encoder_config, ids, masks, 3, seq);
        defer cb.free(encoded);
        const encoder_host = try cb.toFloat32(encoded, a);
        defer a.free(encoder_host);
        const hidden = try @import("../architectures/laya_head.zig").transform(&cb, a, config, encoded, masks, &.{ 0, 1, 2 }, 3, seq, encoder_config.hidden_size);
        defer cb.free(hidden);
        const head_host = try cb.toFloat32(hidden, a);
        defer a.free(head_host);
        try std.testing.expectEqual(expected.encoder.len, encoder_host.len);
        try std.testing.expectEqual(expected.head_hidden.len, head_host.len);
        var encoder_error: f32 = 0;
        var head_error: f32 = 0;
        for (encoder_host, head_host, 0..) |encoder_value, head_value, i| {
            if (masks[i / encoder_config.hidden_size] == 0) continue;
            try std.testing.expect(std.math.isFinite(encoder_value) and std.math.isFinite(head_value));
            encoder_error = @max(encoder_error, @abs(expected.encoder[i] - encoder_value));
            head_error = @max(head_error, @abs(expected.head_hidden[i] - head_value));
        }
        std.debug.print("Laya intermediates encoder_max_error={d:.7} head_max_error={d:.7}\n", .{ encoder_error, head_error });
        try std.testing.expect(encoder_error <= 2e-4 and head_error <= 2e-4);
        if (session.backend() == .cuda) {
            const scores = try cb.fromFloat32Shape(expected.marker_scores, &.{ 9, 1 });
            defer cb.free(scores);
            const features = (try cb.layaActionFeatures(&.{ .hidden = hidden, .logits = scores, .markers = positions, .batch = 3, .sequence = seq, .options = 3, .hidden_size = encoder_config.hidden_size })).?;
            defer cb.free(features);
            const host_features = try cb.toFloat32(features, a);
            defer a.free(host_features);
            try std.testing.expectEqual(expected.action_features.len, host_features.len);
            for (expected.action_features, host_features) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-4);
        }
    }

    for (ref.logits, 0..) |row, i| for (row, 0..) |value, j| try std.testing.expectApproxEqAbs(value, outputs[0].asFloat32()[i * 3 + j], 2e-4);
    for (ref.action_logits, 0..) |row, i| for (row, 0..) |value, j| try std.testing.expectApproxEqAbs(value, outputs[1].asFloat32()[i * 2 + j], 2e-4);
    try std.testing.expectError(error.InferenceInputTokensExceeded, pipeline.executeWithTokenLimit(alloc, session, tok, config, &tasks, null, 1));
    const result = try pipeline.execute(alloc, session, tok, config, &tasks, null);
    for (result.decisions, questions, ref.logits, ref.action_logits) |actual, q, logits, acts| {
        const expected = try pipeline.decode(alloc, config, q, logits[0..q.labels.len], acts);
        try std.testing.expectEqualStrings(expected.label, actual.label);
        for (expected.probabilities, actual.probabilities) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-4);
    }
    const resident_before = factory.layaResidentStats(session);
    // Different sequence lengths, question types, option counts, and row orders
    // must not couple independent decisions through padding or batch scheduling.
    for ([_]usize{ 1, 2, 3, 8, 16, 32, 128, 512 }) |batch_size| {
        var batch_arena = std.heap.ArenaAllocator.init(a);
        defer batch_arena.deinit();
        const batch_alloc = batch_arena.allocator();
        const batch_tasks = try batch_alloc.alloc(pipeline.Task, batch_size);
        for (batch_tasks, 0..) |*task, i| task.* = tasks[(batch_size - i - 1) % tasks.len];
        const batched = try pipeline.executeWithScratch(batch_alloc, a, session, tok, config, batch_tasks, null, null);
        for (batched.decisions, 0..) |actual, i| {
            const expected = result.decisions[(batch_size - i - 1) % tasks.len];
            try std.testing.expectEqualStrings(expected.label, actual.label);
            for (expected.probabilities, actual.probabilities) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-4);
            try std.testing.expectApproxEqAbs(expected.confidence, actual.confidence, 2e-4);
            try std.testing.expectApproxEqAbs(expected.act_probability, actual.act_probability, 2e-4);
        }
    }
    const Cancel = struct {
        fn check(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, pipeline.executeWithScratch(alloc, a, session, tok, config, &tasks, .{ .check_fn = Cancel.check }, null));
    // A cancelled request must not poison the next use of the same session.
    _ = try pipeline.executeWithScratch(alloc, a, session, tok, config, tasks[0..1], null, null);
    // Repeated known tokens ensure this is token overflow, not one UNK token.
    const long_text = "hello " ** 150;
    try std.testing.expectError(error.ExtractionTextLimitExceeded, pipeline.prepare(alloc, tok, config, .{ .text = long_text, .question = questions[0] }));
    if (resident_before) |before| {
        const after = factory.layaResidentStats(session).?;
        try std.testing.expectEqual(before.weight_upload_bytes, after.weight_upload_bytes);
        try std.testing.expectEqual(before.weight_upload_calls, after.weight_upload_calls);
        try std.testing.expectEqual(before.model_bytes, after.model_bytes);
        try std.testing.expectEqual(@as(u64, 0), after.intermediate_readbacks);
        try std.testing.expectEqual(@as(u64, 0), after.activation_host_accesses);
        try std.testing.expectEqual(@as(u64, 0), after.host_fallbacks);
        try std.testing.expectEqual(@as(u64, 0), after.cached_activation_bytes);
        try std.testing.expect(after.requests > before.requests);
        try std.testing.expectEqual(after.input_upload_bytes, after.physical_upload_bytes);
        try std.testing.expectEqual(after.output_readback_bytes, after.physical_download_bytes);
        try std.testing.expectEqual(after.requests, after.physical_download_calls);
        // Allocation failures at different encoder/head depths must unwind
        // submitted frames and leave the same owner reusable without uploads.
        for ([_]usize{ 0, 1, 2, 4, 8, 16, 32, 64, 96, 128, 256 }) |fail_index| {
            const live_before = @import("../backends/metal_tensor.zig").memoryStatsSnapshot().device_owned_live_bytes;
            var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fail_index });
            const fa = failing.allocator();
            if (session.vtable.runLayaDecisions.?(session.ptr, &inputs, fa, null)) |maybe_outputs| {
                const completed = maybe_outputs orelse return error.TestUnexpectedResult;
                for (completed) |*output| output.deinit();
                fa.free(completed);
            } else |err| try std.testing.expectEqual(error.OutOfMemory, err);
            try std.testing.expectEqual(live_before, @import("../backends/metal_tensor.zig").memoryStatsSnapshot().device_owned_live_bytes);
            _ = try pipeline.execute(alloc, session, tok, config, &tasks, null);
            try std.testing.expectEqual(before.weight_upload_bytes, factory.layaResidentStats(session).?.weight_upload_bytes);
        }
        // Independent model owners must survive failed cold preparation and
        // repeated unload without sharing slots or retaining partial uploads.
        for ([_]usize{ 16, 32, 64 }) |fail_index| {
            var failing = std.testing.FailingAllocator.init(a, .{});
            const fa = failing.allocator();
            var peer = try factory.createMetalSession(fa, model_path);
            defer peer.close();
            // Resident weights use the model allocator, not request scratch.
            // Arm it only after loading, then require preparation to fail
            // without publishing a partial owner.
            failing.fail_index = failing.alloc_index + fail_index;
            try std.testing.expectError(error.OutOfMemory, peer.vtable.runLayaDecisions.?(peer.ptr, &inputs, a, null));
            try std.testing.expect(failing.has_induced_failure);
            try std.testing.expect(factory.layaResidentStats(peer) == null);
            failing.fail_index = std.math.maxInt(usize);
            const retried = try pipeline.execute(alloc, peer, tok, config, &tasks, null);
            for (result.decisions, retried.decisions) |want, got| {
                for (want.probabilities, got.probabilities) |p, q| try std.testing.expectApproxEqAbs(p, q, 2e-4);
            }
            try std.testing.expectEqual(before.weight_upload_bytes, factory.layaResidentStats(session).?.weight_upload_bytes);
        }
        const CancellationProbe = struct {
            checks: usize = 0,
            cancel_at: usize,
            fn check(ptr: ?*anyopaque) anyerror!void {
                const probe: *@This() = @ptrCast(@alignCast(ptr.?));
                probe.checks += 1;
                if (probe.checks == probe.cancel_at) return error.Cancelled;
            }
        };
        // Invoke the architecture callback under a test-owned synchronous GPU
        // lifetime; cancel both an active frame and completed-frame boundaries.
        // Public controlled APIs retain process-isolation policy.
        for ([_]usize{ 3, 4, 5 }) |cancel_at| {
            var probe = CancellationProbe{ .cancel_at = cancel_at };
            const live_before = @import("../backends/metal_tensor.zig").memoryStatsSnapshot().device_owned_live_bytes;
            try std.testing.expectError(error.Cancelled, session.vtable.runLayaDecisions.?(session.ptr, &inputs, a, .{ .ptr = &probe, .check_fn = CancellationProbe.check }));
            try std.testing.expectEqual(live_before, @import("../backends/metal_tensor.zig").memoryStatsSnapshot().device_owned_live_bytes);
            _ = try pipeline.execute(alloc, session, tok, config, &tasks, null);
        }
        std.debug.print("Laya resident model_bytes={d} requests={d} input_bytes={d} output_bytes={d} host_accesses={d}\n", .{ after.model_bytes, after.requests, after.input_upload_bytes, after.output_readback_bytes, after.activation_host_accesses });
    }
}

const QualificationRow = struct {
    dataset: []const u8,
    target: usize,
    task: pipeline.Task,
    ids: []const i64,
    markers: []const i64,
    probabilities: []const f32,
    act_probability: f32,
};

test "laya released checkpoint accuracy parity batching and performance" {
    const root = try test_support.fixture("ANTFLY_LAYA_QUALIFICATION");
    const a = std.testing.allocator;
    const model_path = try std.fmt.allocPrint(a, "{s}/model", .{root});
    defer a.free(model_path);
    const reference_path = try std.fmt.allocPrint(a, "{s}/qualification.json", .{root});
    defer a.free(reference_path);
    const bytes = try c_file.readFile(a, reference_path);
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(struct { rows: []const QualificationRow }, a, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const rows = parsed.value.rows;
    try std.testing.expect(rows.len >= 16);
    const began = platform.time.monotonicNs();
    var session = try test_support.createSession(a, model_path);
    defer session.close();
    std.debug.print("Laya qualification backend={s} load_ms={d:.2}\n", .{ @tagName(session.backend()), @as(f64, @floatFromInt(platform.time.monotonicNs() - began)) / 1e6 });
    const config = factory.getLayaConfig(session) orelse return error.TestUnexpectedResult;
    const tokenizer_bytes = try c_file.readFileFromDir(a, model_path, "tokenizer.json");
    defer a.free(tokenizer_bytes);
    const tokenizer = try hf.HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    const tok = tokenizer.tokenizer();
    defer tok.deinitTokenizer();
    if (platform.env.getenvBoolDefault("ANTFLY_LAYA_PERFORMANCE_ONLY", false)) {
        try benchmarkMatched(a, session, tok, config, rows);
        return;
    }
    var max_probability_error: f32 = 0;
    var max_action_error: f32 = 0;
    var disagreements: usize = 0;
    var correct = [_]usize{0} ** 3;
    var totals = [_]usize{0} ** 3;
    var ordinal_error: f64 = 0;
    var start: usize = 0;
    while (start < rows.len) : (start += 8) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const alloc = arena.allocator();
        const chunk = rows[start..@min(start + 8, rows.len)];
        const tasks = try alloc.alloc(pipeline.Task, chunk.len);
        for (chunk, tasks) |row, *task| {
            task.* = row.task;
            const prepared = try pipeline.prepare(alloc, tok, config, task.*);
            try std.testing.expectEqualSlices(i64, row.ids, prepared.ids);
            try std.testing.expectEqualSlices(i64, row.markers, prepared.markers);
        }
        const result = try pipeline.executeWithScratch(alloc, a, session, tok, config, tasks, null, null);
        for (chunk, result.decisions) |row, decision| {
            const actual = argmax(decision.probabilities);
            disagreements += @intFromBool(actual != argmax(row.probabilities));
            const kind = @intFromEnum(row.task.question.kind);
            correct[kind] += @intFromBool(actual == row.target);
            totals[kind] += 1;
            if (decision.expected_value) |value| ordinal_error += @abs(value - @as(f64, @floatFromInt(row.target)));
            for (row.probabilities, decision.probabilities) |want, got| max_probability_error = @max(max_probability_error, @abs(want - got));
            max_action_error = @max(max_action_error, @abs(row.act_probability - decision.act_probability));
        }
        std.debug.print("Laya evaluated {d}/{d}, max_probability_error={d:.7}\n", .{ start + chunk.len, rows.len, max_probability_error });
    }
    std.debug.print("Laya accuracy choice={d}/{d} score={d}/{d} boolean={d}/{d}; ordinal_mae={d:.6}; disagreements={d}; max_probability_error={d:.7}; max_action_error={d:.7}\n", .{ correct[0], totals[0], correct[1], totals[1], correct[2], totals[2], ordinal_error / @as(f64, @floatFromInt(totals[1])), disagreements, max_probability_error, max_action_error });
    try std.testing.expectEqual(@as(usize, 0), disagreements);
    try std.testing.expect(max_probability_error <= 5e-5);
    try std.testing.expect(max_action_error <= 5e-5);

    if (session.backend() == .cuda) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const tasks = try arena.allocator().alloc(pipeline.Task, 512);
        for (tasks, 0..) |*task, i| task.* = rows[(511 - i) % rows.len].task;
        const result = try pipeline.executeWithScratch(arena.allocator(), a, session, tok, config, tasks, null, null);
        for (result.decisions, 0..) |actual, i| {
            const expected = rows[(511 - i) % rows.len];
            try std.testing.expectEqual(argmax(expected.probabilities), argmax(actual.probabilities));
            for (expected.probabilities, actual.probabilities) |want, got| try std.testing.expectApproxEqAbs(want, got, 5e-5);
            try std.testing.expectApproxEqAbs(expected.act_probability, actual.act_probability, 5e-5);
        }
        std.debug.print("Laya released stress tasks=512 parity=passed\n", .{});
    }
    const comparison_only = platform.env.getenvBoolDefault("ANTFLY_LAYA_COMPARISON_ONLY", false);
    // Fixed-input scaling separates batch effects from changing padded lengths.
    for ([_]usize{ 1, 2, 4, 8, 16, 32, 64, 128 }) |batch_size| {
        if (comparison_only and batch_size != 1 and batch_size != 8) continue;
        var timings: [30]u64 = undefined;
        var batch_error: f32 = 0;
        for (0..timings.len + 5) |iteration| {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const alloc = arena.allocator();
            const tasks = try alloc.alloc(pipeline.Task, batch_size);
            @memset(tasks, rows[0].task);
            const before = platform.time.monotonicNs();
            const result = try pipeline.executeWithScratch(alloc, a, session, tok, config, tasks, null, null);
            const ns = platform.time.monotonicNs() - before;
            if (iteration >= 5) timings[iteration - 5] = ns;
            for (result.decisions) |actual| {
                try std.testing.expectEqual(argmax(rows[0].probabilities), argmax(actual.probabilities));
                for (rows[0].probabilities, actual.probabilities) |want, got| batch_error = @max(batch_error, @abs(want - got));
            }
        }
        std.mem.sort(u64, &timings, {}, std.sort.asc(u64));
        const p50_ms = @as(f64, @floatFromInt(timings[timings.len / 2])) / 1e6;
        const p95_ms = @as(f64, @floatFromInt(timings[(timings.len * 95 + 99) / 100 - 1])) / 1e6;
        std.debug.print("Laya performance profile=fixed backend={s} tokens={d} batch={d} p50_ms={d:.3} p95_ms={d:.3} questions_per_second={d:.2} max_batch_error={d:.7}\n", .{ @tagName(session.backend()), rows[0].ids.len, batch_size, p50_ms, p95_ms, @as(f64, @floatFromInt(batch_size)) * 1000 / p50_ms, batch_error });
        try std.testing.expect(batch_error <= 5e-5);
    }

    // Compare heterogeneous batches to the same independent PyTorch outputs.
    // Reverse rows as well, to catch hidden row-order and padding dependencies.
    for ([_]usize{ 1, 2, 4, 8, 16, 32, 64, 128 }) |batch_size| {
        if (comparison_only and batch_size != 1 and batch_size != 8) continue;
        var timings: [30]u64 = undefined;
        var batch_error: f32 = 0;
        for (0..timings.len + 5) |iteration| {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const alloc = arena.allocator();
            const tasks = try alloc.alloc(pipeline.Task, batch_size);
            for (tasks, 0..) |*task, i| task.* = rows[(batch_size - i - 1) % rows.len].task;
            const before = platform.time.monotonicNs();
            const result = try pipeline.executeWithScratch(alloc, a, session, tok, config, tasks, null, null);
            const ns = platform.time.monotonicNs() - before;
            if (iteration >= 5) timings[iteration - 5] = ns;
            for (result.decisions, 0..) |actual, i| {
                const expected = rows[(batch_size - i - 1) % rows.len];
                try std.testing.expectEqual(argmax(expected.probabilities), argmax(actual.probabilities));
                for (expected.probabilities, actual.probabilities) |want, got| batch_error = @max(batch_error, @abs(want - got));
                try std.testing.expectApproxEqAbs(expected.act_probability, actual.act_probability, 5e-5);
            }
        }
        std.mem.sort(u64, &timings, {}, std.sort.asc(u64));
        const p50_ms = @as(f64, @floatFromInt(timings[timings.len / 2])) / 1e6;
        const p95_ms = @as(f64, @floatFromInt(timings[(timings.len * 95 + 99) / 100 - 1])) / 1e6;
        std.debug.print("Laya performance profile=mixed backend={s} batch={d} p50_ms={d:.3} p95_ms={d:.3} questions_per_second={d:.2} max_batch_error={d:.7}\n", .{ @tagName(session.backend()), batch_size, p50_ms, p95_ms, @as(f64, @floatFromInt(batch_size)) * 1000 / p50_ms, batch_error });
        try std.testing.expect(batch_error <= 5e-5);
    }
}

fn argmax(values: []const f32) usize {
    var index: usize = 0;
    for (values, 0..) |value, i| if (value > values[index]) {
        index = i;
    };
    return index;
}

/// Dedicated benchmark mode; the qualification harness explicitly clears it.
fn benchmarkMatched(a: std.mem.Allocator, session: @import("../backends/session.zig").Session, tok: @import("inference_tokenizer").Tokenizer, cfg: @import("../models/laya.zig").Config, rows: []const QualificationRow) !void {
    std.debug.print("Laya benchmark build mode={s} cpu={s} artifacts={s}\n", .{ @tagName(@import("builtin").mode), @import("builtin").cpu.model.name, @import("build_options").cuda_artifacts });
    const samples = platform.env.getenvUsize("ANTFLY_LAYA_BENCH_SAMPLES") orelse 100;
    const warmups = platform.env.getenvUsize("ANTFLY_LAYA_BENCH_WARMUPS") orelse 10;
    if (samples < 2 or samples > 1000 or warmups > 100) return error.InvalidBenchmarkSamples;
    for ([_]bool{ false, true }) |mixed| {
        for ([_]usize{ 1, 8 }) |batch| {
            var prepared_arena = std.heap.ArenaAllocator.init(a);
            defer prepared_arena.deinit();
            const pa = prepared_arena.allocator();
            const tasks = try pa.alloc(pipeline.Task, batch);
            const seqs = try pa.alloc(pipeline.Sequence, batch);
            var seq: usize = 0;
            var count: usize = 0;
            var useful: usize = 0;
            for (tasks, seqs, 0..) |*task, *prepared, i| {
                task.* = rows[if (mixed) batch - 1 - i else 0].task;
                prepared.* = try pipeline.prepare(pa, tok, cfg, task.*);
                seq = @max(seq, prepared.ids.len);
                count = @max(count, prepared.markers.len);
                useful += prepared.ids.len;
            }
            const ids = try pa.alloc(i64, batch * seq);
            const mask = try pa.alloc(i64, batch * seq);
            const kinds = try pa.alloc(i64, batch);
            const markers = try pa.alloc(i64, batch * count);
            @memset(ids, tok.specialTokens().pad_id);
            @memset(mask, 0);
            @memset(markers, -1);
            for (seqs, tasks, 0..) |prepared, task, i| {
                @memcpy(ids[i * seq ..][0..prepared.ids.len], prepared.ids);
                @memset(mask[i * seq ..][0..prepared.ids.len], 1);
                @memcpy(markers[i * count ..][0..prepared.markers.len], prepared.markers);
                kinds[i] = @intFromEnum(task.question.kind);
            }
            var inputs = [_]Tensor{
                try Tensor.initInt64(pa, "input_ids", &.{ @intCast(batch), @intCast(seq) }, ids),
                try Tensor.initInt64(pa, "attention_mask", &.{ @intCast(batch), @intCast(seq) }, mask),
                try Tensor.initInt64(pa, "qtype", &.{ @intCast(batch), 1 }, kinds),
                try Tensor.initInt64(pa, "marker_pos", &.{ @intCast(batch), @intCast(count) }, markers),
            };
            defer for (&inputs) |*input| input.deinit();
            for ([_]bool{ false, true }) |prepared_only| {
                const timings = try pa.alloc(u64, samples);
                var chunks: usize = 1;
                var padded: usize = batch * seq;
                const before_stats = factory.getCudaRuntimeStats(session);
                for (0..warmups + samples) |iteration| {
                    var arena = std.heap.ArenaAllocator.init(a);
                    defer arena.deinit();
                    const alloc = arena.allocator();
                    const began = platform.time.monotonicNs();
                    const result = if (prepared_only) blk: {
                        const outputs = try session.run(&inputs, a);
                        const elapsed = platform.time.monotonicNs() - began;
                        if (iteration >= warmups) timings[iteration - warmups] = elapsed;
                        defer {
                            for (outputs) |*output| output.deinit();
                            a.free(outputs);
                        }
                        const decisions = try alloc.alloc(pipeline.Decision, batch);
                        for (tasks, decisions, 0..) |task, *decision, i|
                            decision.* = try pipeline.decode(alloc, cfg, task.question, outputs[0].asFloat32()[i * count ..][0..task.question.labels.len], outputs[1].asFloat32()[i * cfg.n_act ..][0..cfg.n_act]);
                        break :blk pipeline.Result{ .decisions = decisions, .prompt_tokens = useful };
                    } else blk: {
                        const result = try pipeline.executeWithScratch(alloc, a, session, tok, cfg, tasks, null, null);
                        const elapsed = platform.time.monotonicNs() - began;
                        if (iteration >= warmups) timings[iteration - warmups] = elapsed;
                        chunks = result.execution_chunks;
                        padded = result.padded_tokens;
                        break :blk result;
                    };
                    for (result.decisions, 0..) |decision, i| {
                        const expected = rows[if (mixed) batch - 1 - i else 0];
                        try std.testing.expectEqual(argmax(expected.probabilities), argmax(decision.probabilities));
                        for (expected.probabilities, decision.probabilities) |want, got| try std.testing.expectApproxEqAbs(want, got, 5e-5);
                        try std.testing.expectApproxEqAbs(expected.act_probability, decision.act_probability, 5e-5);
                    }
                }
                std.mem.sort(u64, timings, {}, std.sort.asc(u64));
                var attention: usize = 0;
                var fusion: usize = 0;
                if (comptime @import("build_options").enable_cuda) {
                    if (before_stats) |before| {
                        const delta = factory.cudaStatsDelta(factory.getCudaRuntimeStats(session).?, before);
                        attention = delta.laya_warp_attention;
                        fusion = delta.laya_packed_geglu;
                    }
                }
                std.debug.print("Laya benchmark {{\"profile\":\"{s}\",\"scope\":\"{s}\",\"batch\":{d},\"samples\":{d},\"warmups\":{d},\"p50_ms\":{d:.6},\"p95_ms\":{d:.6},\"useful_tokens\":{d},\"padded_tokens\":{d},\"chunks\":{d},\"warp_attention\":{d},\"packed_geglu\":{d}}}\n", .{
                    if (mixed) "mixed" else "fixed",                     if (prepared_only) "prepared" else "pipeline",                         batch,  samples, warmups,
                    @as(f64, @floatFromInt(timings[samples / 2])) / 1e6, @as(f64, @floatFromInt(timings[(samples * 95 + 99) / 100 - 1])) / 1e6, useful, padded,  chunks,
                    attention,                                           fusion,
                });
            }
        }
    }
}
