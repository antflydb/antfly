// Copyright 2026 Antfly, Inc.
// Licensed under the Apache License, Version 2.0.

const std = @import("std");

const ReplayResult = extern struct {
    warmup_iterations: u64 = 0,
    measure_iterations: u64 = 0,
    dispatches_per_iteration: u64 = 0,
    baseline_gpu_nanos: u64 = 0,
    candidate_gpu_nanos: u64 = 0,
    weight_bytes: u64 = 0,
    max_abs_error: f32 = 0,
    max_rel_error: f32 = 0,
    within_tolerance: u32 = 0,
    baseline_top1: u32 = 0,
    candidate_top1: u32 = 0,
    reserved: u32 = 0,
};

extern fn termite_metal_a4b_lm_head_replay(
    warmup_iterations: usize,
    measure_iterations: usize,
    result: *ReplayResult,
) c_int;

fn parsePositive(value: []const u8, name: []const u8) !usize {
    const parsed = std.fmt.parseUnsigned(usize, value, 10) catch {
        std.debug.print("invalid {s}: {s}\n", .{ name, value });
        return error.InvalidArgument;
    };
    if (parsed == 0 or parsed > 10_000) return error.InvalidArgument;
    return parsed;
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.arena.allocator();
    const args = try init.minimal.args.toSlice(allocator);
    var warmups: usize = 3;
    var iterations: usize = 20;
    var index: usize = 1;
    while (index < args.len) : (index += 1) {
        if (std.mem.eql(u8, args[index], "--warmups")) {
            index += 1;
            if (index >= args.len) return error.MissingArgument;
            warmups = try parsePositive(args[index], "--warmups");
        } else if (std.mem.eql(u8, args[index], "--iterations")) {
            index += 1;
            if (index >= args.len) return error.MissingArgument;
            iterations = try parsePositive(args[index], "--iterations");
        } else {
            std.debug.print("unknown argument: {s}\n", .{args[index]});
            return error.InvalidArgument;
        }
    }

    var result = ReplayResult{};
    const rc = termite_metal_a4b_lm_head_replay(warmups, iterations, &result);
    if (rc != 0) {
        std.debug.print("A4B Metal Q6_K LM-head replay failed rc={d}\n", .{rc});
        return error.MetalReplayFailed;
    }
    if (result.within_tolerance != 1 or result.baseline_top1 != result.candidate_top1 or
        result.measure_iterations == 0 or result.dispatches_per_iteration == 0)
    {
        return error.InvalidReplayResult;
    }

    const samples: f64 = @floatFromInt(result.measure_iterations * result.dispatches_per_iteration);
    const baseline_us = @as(f64, @floatFromInt(result.baseline_gpu_nanos)) / samples / 1_000.0;
    const candidate_us = @as(f64, @floatFromInt(result.candidate_gpu_nanos)) / samples / 1_000.0;
    const speedup = baseline_us / candidate_us;
    const projected_full_vocab_us = candidate_us * 4.0;
    const projected_saved_ms = (baseline_us - candidate_us) * 4.0 / 1_000.0;

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;
    try out.print(
        "{{\"schema\":\"antfly.gemma4_a4b_lm_head_replay.v1\",\"geometry\":{{\"in_dim\":2816,\"out_dim\":65536,\"row_blocks\":11,\"full_vocab\":262144}},\"warmups\":{d},\"iterations\":{d},\"within_tolerance\":true,\"top1_equal\":true,\"top1\":{d},\"max_abs_error\":{d:.9},\"max_rel_error\":{d:.9},\"weight_bytes\":{d},\"baseline_gpu_us\":{d:.3},\"candidate_gpu_us\":{d:.3},\"speedup\":{d:.6},\"projected_full_vocab_gpu_us\":{d:.3},\"projected_saved_ms_per_token\":{d:.3}}}\n",
        .{
            result.warmup_iterations,
            result.measure_iterations,
            result.baseline_top1,
            result.max_abs_error,
            result.max_rel_error,
            result.weight_bytes,
            baseline_us,
            candidate_us,
            speedup,
            projected_full_vocab_us,
            projected_saved_ms,
        },
    );
    try out.flush();
}

test "replay ABI remains stable" {
    try std.testing.expectEqual(@as(usize, 72), @sizeOf(ReplayResult));
}
