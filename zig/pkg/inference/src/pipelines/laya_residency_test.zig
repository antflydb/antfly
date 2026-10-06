// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const platform = @import("antfly_platform");
const factory = @import("../architectures/session_factory.zig");
const c_file = @import("../util/c_file.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const pipeline = @import("laya.zig");

test "laya resident Metal released checkpoint parity and warm latency" {
    const root = platform.env.getenv("ANTFLY_LAYA_MODEL") orelse return error.SkipZigTest;
    const reference = platform.env.getenv("ANTFLY_LAYA_ORACLE") orelse return error.SkipZigTest;
    const a = if (platform.env.getenv("ANTFLY_LAYA_BENCH") != null) std.heap.c_allocator else std.testing.allocator;
    const bytes = try c_file.readFile(a, reference);
    defer a.free(bytes);
    const Row = struct { ids: []const i64, markers: []const i64, probabilities: []const f32, act_probability: f32 };
    const parsed = try std.json.parseFromSlice([]const Row, a, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 3), parsed.value.len);
    var session = try factory.createMetalSession(a, root);
    defer session.close();
    try std.testing.expectEqual(.metal, session.backend());
    const cfg = factory.getLayaConfig(session) orelse return error.TestUnexpectedResult;
    const questions = [_]pipeline.Question{
        .{ .name = "tool", .kind = .choice, .instruction = "", .labels = &.{ "search", "fetch", "none" }, .descriptions = &.{ "", "", "" } },
        .{ .name = "urgency", .kind = .score, .instruction = "", .labels = &.{ "low", "medium", "high" }, .descriptions = &.{ "", "", "" } },
        .{ .name = "search_needed", .kind = .noul, .instruction = "", .labels = &.{ "false", "true" }, .descriptions = &.{ "", "" } },
    };
    for (parsed.value, questions, 0..) |row, question, kind| {
        var times: [5]u64 = undefined;
        var max_error: f32 = 0;
        for (0..times.len + 1) |iteration| {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const alloc = arena.allocator();
            const mask = try alloc.alloc(i64, row.ids.len);
            @memset(mask, 1);
            var inputs = [_]Tensor{
                try Tensor.initInt64(alloc, "input_ids", &.{ 1, @intCast(row.ids.len) }, row.ids),
                try Tensor.initInt64(alloc, "attention_mask", &.{ 1, @intCast(row.ids.len) }, mask),
                try Tensor.initInt64(alloc, "qtype", &.{ 1, 1 }, &.{@intCast(kind)}),
                try Tensor.initInt64(alloc, "marker_pos", &.{ 1, @intCast(row.markers.len) }, row.markers),
            };
            defer for (&inputs) |*input| input.deinit();
            const start = platform.time.monotonicNs();
            const profile = iteration == 2 and platform.env.getenv("ANTFLY_LAYA_PROFILE") != null;
            if (profile) _ = try factory.beginMetalWorkloadProfile(session, .encoder);
            // Scratch is a freeing allocator, not the request arena.
            const outputs = try session.run(&inputs, a);
            defer {
                for (outputs) |*output| output.deinit();
                a.free(outputs);
            }
            const decision = try pipeline.decode(alloc, cfg, question, outputs[0].asFloat32(), outputs[1].asFloat32());
            const elapsed = platform.time.monotonicNs() - start;
            if (profile) {
                var report = (try factory.endMetalWorkloadProfile(session, a, false)).?;
                defer report.deinit();
                std.debug.print("Laya profile frame_gpu_ms={d:.3} frames={d} signatures={d}\n", .{ @as(f64, @floatFromInt(report.snapshot.whole_frame_gpu_nanos)) / 1e6, report.snapshot.profiled_frame_count, report.snapshot.entry_count });
            }
            if (iteration > 0) times[iteration - 1] = elapsed;
            for (decision.probabilities, row.probabilities) |actual, expected| max_error = @max(max_error, @abs(actual - expected));
            try std.testing.expectApproxEqAbs(row.act_probability, decision.act_probability.?, 5e-5);
            try std.testing.expect(max_error < 5e-5);
        }
        std.mem.sort(u64, &times, {}, std.sort.asc(u64));
        std.debug.print("Laya resident backend=metal kind={s} tokens={d} warm_p50_ms={d:.3} max_probability_error={d:.8}\n", .{ @tagName(question.kind), row.ids.len, @as(f64, @floatFromInt(times[times.len / 2])) / 1e6, max_error });
    }
}
