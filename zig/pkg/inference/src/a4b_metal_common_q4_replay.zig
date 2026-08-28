// Copyright 2026 Antfly, Inc.
// Licensed under the Apache License, Version 2.0.

const std = @import("std");

const ReplayResult = extern struct {
    warmup_iterations: u64 = 0,
    measure_iterations: u64 = 0,
    gate_up_baseline_gpu_nanos: u64 = 0,
    gate_up_packed_gpu_nanos: u64 = 0,
    down_baseline_gpu_nanos: u64 = 0,
    down_packed_gpu_nanos: u64 = 0,
    gate_up_weight_bytes: u64 = 0,
    down_weight_bytes: u64 = 0,
    gate_up_max_abs_error: f32 = 0,
    down_max_abs_error: f32 = 0,
    exact_match: u32 = 0,
    reserved: u32 = 0,
};

extern fn termite_metal_a4b_common_q4_replay(
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
    var warmups: usize = 5;
    var iterations: usize = 50;
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
    const rc = termite_metal_a4b_common_q4_replay(warmups, iterations, &result);
    if (rc != 0) {
        std.debug.print("A4B common Q4_0 packed-layout replay failed rc={d}\n", .{rc});
        return error.MetalReplayFailed;
    }
    if (result.exact_match != 1 or result.measure_iterations == 0 or
        result.gate_up_packed_gpu_nanos == 0 or result.down_packed_gpu_nanos == 0)
    {
        return error.InvalidReplayResult;
    }

    const count: f64 = @floatFromInt(result.measure_iterations);
    const gate_up_baseline_us = @as(f64, @floatFromInt(result.gate_up_baseline_gpu_nanos)) / count / 1_000.0;
    const gate_up_packed_us = @as(f64, @floatFromInt(result.gate_up_packed_gpu_nanos)) / count / 1_000.0;
    const down_baseline_us = @as(f64, @floatFromInt(result.down_baseline_gpu_nanos)) / count / 1_000.0;
    const down_packed_us = @as(f64, @floatFromInt(result.down_packed_gpu_nanos)) / count / 1_000.0;
    const gate_up_speedup = gate_up_baseline_us / gate_up_packed_us;
    const down_speedup = down_baseline_us / down_packed_us;
    const baseline_total_us = gate_up_baseline_us + down_baseline_us;
    const packed_total_us = gate_up_packed_us + down_packed_us;
    const combined_speedup = baseline_total_us / packed_total_us;
    const promotion_threshold: f64 = 1.15;

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;
    try out.print(
        "{{\"schema\":\"antfly.gemma4_a4b_common_q4_replay.v1\",\"geometry\":{{\"rows\":1,\"hidden\":2816,\"shared_intermediate\":2112,\"tile_rows\":4}},\"warmups\":{d},\"iterations\":{d},\"exact_match\":true,\"gate_up_max_abs_error\":{d:.9},\"down_max_abs_error\":{d:.9},\"gate_up_weight_bytes\":{d},\"down_weight_bytes\":{d},\"gate_up_baseline_gpu_us\":{d:.3},\"gate_up_packed_gpu_us\":{d:.3},\"gate_up_speedup\":{d:.6},\"down_baseline_gpu_us\":{d:.3},\"down_packed_gpu_us\":{d:.3},\"down_speedup\":{d:.6},\"combined_baseline_gpu_us\":{d:.3},\"combined_packed_gpu_us\":{d:.3},\"combined_speedup\":{d:.6},\"promotion_threshold\":{d:.2},\"promote\":{s}}}\n",
        .{
            result.warmup_iterations,
            result.measure_iterations,
            result.gate_up_max_abs_error,
            result.down_max_abs_error,
            result.gate_up_weight_bytes,
            result.down_weight_bytes,
            gate_up_baseline_us,
            gate_up_packed_us,
            gate_up_speedup,
            down_baseline_us,
            down_packed_us,
            down_speedup,
            baseline_total_us,
            packed_total_us,
            combined_speedup,
            promotion_threshold,
            if (combined_speedup >= promotion_threshold) "true" else "false",
        },
    );
    try out.flush();
}

test "common replay ABI remains stable" {
    try std.testing.expectEqual(@as(usize, 80), @sizeOf(ReplayResult));
}
