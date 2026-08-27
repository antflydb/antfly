// Copyright 2026 Antfly, Inc.
// Licensed under the Apache License, Version 2.0.

const std = @import("std");

const ReplayResult = extern struct {
    warmup_iterations: u64 = 0,
    measure_iterations: u64 = 0,
    gate_generic_gpu_nanos: u64 = 0,
    gate_specialized_gpu_nanos: u64 = 0,
    down_generic_gpu_nanos: u64 = 0,
    down_specialized_gpu_nanos: u64 = 0,
    down_nq8_gpu_nanos: u64 = 0,
    gate_activation_baseline_gpu_nanos: u64 = 0,
    gate_activation_fused_gpu_nanos: u64 = 0,
    down_reduce_baseline_gpu_nanos: u64 = 0,
    down_reduce_fused_gpu_nanos: u64 = 0,
    gate_weight_bytes: u64 = 0,
    down_weight_bytes: u64 = 0,
    gate_max_abs_error: f32 = 0,
    down_max_abs_error: f32 = 0,
    down_nq8_max_abs_error: f32 = 0,
    down_nq8_max_rel_error: f32 = 0,
    gate_activation_max_abs_error: f32 = 0,
    down_reduce_max_abs_error: f32 = 0,
    exact_match: u32 = 0,
    down_nq8_within_tolerance: u32 = 0,
};

extern fn termite_metal_a4b_q4_0_id_replay(
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
    const rc = termite_metal_a4b_q4_0_id_replay(warmups, iterations, &result);
    if (rc != 0) {
        std.debug.print("A4B exact-shape Metal MUL_MV_ID replay failed rc={d}\n", .{rc});
        return error.MetalReplayFailed;
    }
    if (result.exact_match != 1 or result.down_nq8_within_tolerance != 1 or result.measure_iterations == 0) {
        return error.InvalidReplayResult;
    }

    const count: f64 = @floatFromInt(result.measure_iterations);
    const gate_generic_us = @as(f64, @floatFromInt(result.gate_generic_gpu_nanos)) / count / 1_000.0;
    const gate_specialized_us = @as(f64, @floatFromInt(result.gate_specialized_gpu_nanos)) / count / 1_000.0;
    const down_generic_us = @as(f64, @floatFromInt(result.down_generic_gpu_nanos)) / count / 1_000.0;
    const down_specialized_us = @as(f64, @floatFromInt(result.down_specialized_gpu_nanos)) / count / 1_000.0;
    const down_nq8_us = @as(f64, @floatFromInt(result.down_nq8_gpu_nanos)) / count / 1_000.0;
    const gate_activation_baseline_us = @as(f64, @floatFromInt(result.gate_activation_baseline_gpu_nanos)) / count / 1_000.0;
    const gate_activation_fused_us = @as(f64, @floatFromInt(result.gate_activation_fused_gpu_nanos)) / count / 1_000.0;
    const down_reduce_baseline_us = @as(f64, @floatFromInt(result.down_reduce_baseline_gpu_nanos)) / count / 1_000.0;
    const down_reduce_fused_us = @as(f64, @floatFromInt(result.down_reduce_fused_gpu_nanos)) / count / 1_000.0;
    const gate_speedup = gate_generic_us / gate_specialized_us;
    const down_speedup = down_generic_us / down_specialized_us;
    const down_nq8_speedup = down_specialized_us / down_nq8_us;
    const gate_activation_speedup = gate_activation_baseline_us / gate_activation_fused_us;
    const down_reduce_speedup = down_reduce_baseline_us / down_reduce_fused_us;

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;
    try out.print(
        "{{\"schema\":\"antfly.gemma4_a4b_metal_id_replay.v4\",\"geometry\":{{\"experts\":128,\"top_k\":8,\"hidden\":2816,\"intermediate\":704,\"gate_up_out\":1408}},\"warmups\":{d},\"iterations\":{d},\"exact_match\":true,\"down_nq8_within_tolerance\":true,\"gate_max_abs_error\":{d:.9},\"down_max_abs_error\":{d:.9},\"down_nq8_max_abs_error\":{d:.9},\"down_nq8_max_rel_error\":{d:.9},\"gate_activation_max_abs_error\":{d:.9},\"down_reduce_max_abs_error\":{d:.9},\"gate_weight_bytes\":{d},\"down_weight_bytes\":{d},\"gate_generic_gpu_us\":{d:.3},\"gate_specialized_gpu_us\":{d:.3},\"gate_speedup\":{d:.6},\"down_generic_gpu_us\":{d:.3},\"down_specialized_gpu_us\":{d:.3},\"down_speedup\":{d:.6},\"down_nq8_gpu_us\":{d:.3},\"down_nq8_speedup\":{d:.6},\"gate_activation_baseline_gpu_us\":{d:.3},\"gate_activation_fused_gpu_us\":{d:.3},\"gate_activation_speedup\":{d:.6},\"down_reduce_baseline_gpu_us\":{d:.3},\"down_reduce_fused_gpu_us\":{d:.3},\"down_reduce_speedup\":{d:.6}}}\n",
        .{
            result.warmup_iterations,
            result.measure_iterations,
            result.gate_max_abs_error,
            result.down_max_abs_error,
            result.down_nq8_max_abs_error,
            result.down_nq8_max_rel_error,
            result.gate_activation_max_abs_error,
            result.down_reduce_max_abs_error,
            result.gate_weight_bytes,
            result.down_weight_bytes,
            gate_generic_us,
            gate_specialized_us,
            gate_speedup,
            down_generic_us,
            down_specialized_us,
            down_speedup,
            down_nq8_us,
            down_nq8_speedup,
            gate_activation_baseline_us,
            gate_activation_fused_us,
            gate_activation_speedup,
            down_reduce_baseline_us,
            down_reduce_fused_us,
            down_reduce_speedup,
        },
    );
    try out.flush();
}

test "replay ABI remains stable" {
    try std.testing.expectEqual(@as(usize, 136), @sizeOf(ReplayResult));
}
