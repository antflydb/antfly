// Copyright 2026 Antfly, Inc.
// Licensed under the Apache License, Version 2.0.

const std = @import("std");

const ReplayResult = extern struct {
    warmup_iterations: u64 = 0,
    measure_iterations: u64 = 0,
    dispatches_per_iteration: u64 = 0,
    case_count: u64 = 0,
    baseline_gpu_nanos: u64 = 0,
    candidate_gpu_nanos: u64 = 0,
    exact_ids: u32 = 0,
    exact_weights: u32 = 0,
    exact_match: u32 = 0,
    reserved: u32 = 0,
};

extern fn termite_metal_a4b_route_select_replay(
    warmup_iterations: usize,
    measure_iterations: usize,
    result: *ReplayResult,
) c_int;

fn parsePositive(value: []const u8, name: []const u8) !usize {
    const parsed = std.fmt.parseUnsigned(usize, value, 10) catch {
        std.debug.print("invalid {s}: {s}\n", .{ name, value });
        return error.InvalidArgument;
    };
    if (parsed == 0 or parsed > 10_000) {
        std.debug.print("{s} must be between 1 and 10000\n", .{name});
        return error.InvalidArgument;
    }
    return parsed;
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.arena.allocator();
    const args = try init.minimal.args.toSlice(allocator);
    var warmups: usize = 3;
    var iterations: usize = 30;
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
    const rc = termite_metal_a4b_route_select_replay(warmups, iterations, &result);
    if (rc != 0) {
        std.debug.print("A4B Metal route-select replay failed rc={d}\n", .{rc});
        return error.MetalReplayFailed;
    }
    if (result.exact_match != 1 or result.exact_ids != 1 or
        result.exact_weights != 1 or result.measure_iterations == 0 or
        result.dispatches_per_iteration == 0)
    {
        return error.InvalidReplayResult;
    }

    const samples: f64 = @floatFromInt(result.measure_iterations * result.dispatches_per_iteration);
    const baseline_us = @as(f64, @floatFromInt(result.baseline_gpu_nanos)) / samples / 1_000.0;
    const candidate_us = @as(f64, @floatFromInt(result.candidate_gpu_nanos)) / samples / 1_000.0;
    const speedup = baseline_us / candidate_us;
    const saved_ms_per_token = (baseline_us - candidate_us) * 30.0 / 1_000.0;

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;
    try out.print(
        "{{\"schema\":\"antfly.gemma4_a4b_route_select_replay.v1\",\"geometry\":{{\"experts\":128,\"top_k\":8,\"layers\":30}},\"warmups\":{d},\"iterations\":{d},\"dispatches_per_iteration\":{d},\"cases\":{d},\"exact_ids\":true,\"exact_weights\":true,\"baseline_gpu_us_per_layer\":{d:.3},\"candidate_gpu_us_per_layer\":{d:.3},\"speedup\":{d:.6},\"saved_ms_per_token\":{d:.3}}}\n",
        .{
            result.warmup_iterations,
            result.measure_iterations,
            result.dispatches_per_iteration,
            result.case_count,
            baseline_us,
            candidate_us,
            speedup,
            saved_ms_per_token,
        },
    );
    try out.flush();
}

test "replay ABI remains stable" {
    try std.testing.expectEqual(@as(usize, 64), @sizeOf(ReplayResult));
}
