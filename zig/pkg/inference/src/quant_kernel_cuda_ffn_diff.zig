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

//! Raw CUDA differential harness for the exact F32 E2B FFN candidates.
//!
//! This bypasses graph routing deliberately. It loads the checked-in CUDA
//! artifact and launches the production F32 pair-activation and down kernels
//! beside their generated counterparts with the identical launch contracts.
//! That makes a bitwise result evidence about the kernel implementations,
//! rather than about graph capture, sampling, or model state.

const std = @import("std");
const build_options = @import("build_options");
const cuda_artifact = @import("ops/cuda/artifact.zig");
const cuda_buffer = @import("ops/cuda/buffer.zig");
const cuda_context = @import("ops/cuda/context.zig");
const cuda_driver = @import("ops/cuda/driver.zig");
const cuda_renderer = @import("graph/quant_kernel_cuda_renderer.zig");

const hidden_dim: usize = 1536;
const q4_0_values_per_block: usize = 32;
const q4_0_block_bytes: usize = 18;
const q4_0_tile_cols: usize = 4;
const pair_threads: u32 = 128;
const down_threads: u32 = 256;

// DecoderRuntimeActivationKind.gelu_new. Keep this local so this raw harness
// only depends on the CUDA artifact and renderer-owned plans.
const gelu_new_activation: u32 = 1;

pub const Pattern = enum {
    random,
    cancellation,

    fn label(self: Pattern) []const u8 {
        return switch (self) {
            .random => "random",
            .cancellation => "cancellation",
        };
    }
};

const PatternSelection = enum {
    all,
    random,
    cancellation,

    fn count(self: PatternSelection) usize {
        return if (self == .all) 2 else 1;
    }

    fn at(self: PatternSelection, index: usize) Pattern {
        return switch (self) {
            .all => switch (index) {
                0 => .random,
                1 => .cancellation,
                else => unreachable,
            },
            .random => .random,
            .cancellation => .cancellation,
        };
    }
};

pub const Config = struct {
    /// `null` runs both production E2B intermediate widths.
    inner_dim: ?usize = null,
    pattern: PatternSelection = .all,
    seed: u64 = 0x6a09_e667_f3bc_c909,
    json: bool = false,
    help: bool = false,
};

pub const BitwiseDiff = struct {
    element_count: usize = 0,
    mismatch_count: usize = 0,
    first_mismatch_index: ?usize = null,
    first_reference_bits: ?u32 = null,
    first_candidate_bits: ?u32 = null,
};

pub const CaseResult = struct {
    inner_dim: usize,
    pattern: Pattern,
    pair: BitwiseDiff,
    down: BitwiseDiff,
    chain: BitwiseDiff,
};

const Variant = struct {
    inner_dim: usize,
    pair_plan: cuda_renderer.RenderPlan,
    down_plan: cuda_renderer.RenderPlan,
};

const Functions = struct {
    pair_baseline: cuda_driver.CUfunction,
    pair_candidate: cuda_driver.CUfunction,
    down_baseline: cuda_driver.CUfunction,
    down_candidate: cuda_driver.CUfunction,
};

const Module = struct {
    module: cuda_driver.CUmodule = null,

    fn load(ctx: *cuda_context.CudaContext) !Module {
        try ctx.makeCurrent();
        var module: cuda_driver.CUmodule = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, cuda_artifact.image.ptr, 0, null, null));
        errdefer {
            if (module != null) _ = ctx.driver.fns.cuModuleUnload(module);
        }
        return .{ .module = module };
    }

    fn deinit(self: *Module, ctx: *cuda_context.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuModuleUnload(self.module);
            self.module = null;
        }
    }

    fn functionsFor(self: *const Module, ctx: *cuda_context.CudaContext, variant: Variant) !Functions {
        return .{
            .pair_baseline = try loadFunction(ctx, self.module, variant.pair_plan.production_baseline),
            .pair_candidate = try loadFunction(ctx, self.module, variant.pair_plan.kernel_id),
            .down_baseline = try loadFunction(ctx, self.module, variant.down_plan.production_baseline),
            .down_candidate = try loadFunction(ctx, self.module, variant.down_plan.kernel_id),
        };
    }
};

fn variantFor(inner_dim: usize) !Variant {
    const pair_kind: cuda_renderer.KernelKind = switch (inner_dim) {
        6144 => .q4_0_pair_activation_f32_e2b_6144_exact,
        12288 => .q4_0_pair_activation_f32_e2b_12288_exact,
        else => return error.InvalidInnerDim,
    };
    const down_kind: cuda_renderer.KernelKind = switch (inner_dim) {
        6144 => .q4_0_down_f32_e2b_6144_exact,
        12288 => .q4_0_down_f32_e2b_12288_exact,
        else => return error.InvalidInnerDim,
    };
    const variant = Variant{
        .inner_dim = inner_dim,
        .pair_plan = cuda_renderer.planFor(pair_kind),
        .down_plan = cuda_renderer.planFor(down_kind),
    };
    try validateVariant(variant);
    return variant;
}

fn validateVariant(variant: Variant) !void {
    try variant.pair_plan.validate();
    try variant.down_plan.validate();
    if (variant.pair_plan.production_enabled or variant.down_plan.production_enabled) return error.InvalidPromotionState;
    if (!std.mem.eql(u8, variant.pair_plan.production_baseline, "termite_linear_q4_0_pair_activation_f32_tile4_w4")) return error.InvalidPairBaseline;
    if (!std.mem.eql(u8, variant.down_plan.production_baseline, "termite_linear_q4_0_f32_tile4")) return error.InvalidDownBaseline;
    if (variant.pair_plan.launch.threads_per_block != @as(u16, pair_threads) or variant.down_plan.launch.threads_per_block != @as(u16, down_threads)) return error.InvalidLaunchTopology;
    if (variant.pair_plan.launch.input_dim.fixed != @as(u32, hidden_dim) or variant.pair_plan.launch.output_dim.fixed != @as(u32, @intCast(variant.inner_dim))) return error.InvalidPairShape;
    if (variant.down_plan.launch.input_dim.fixed != @as(u32, @intCast(variant.inner_dim)) or variant.down_plan.launch.output_dim.fixed != @as(u32, hidden_dim)) return error.InvalidDownShape;
}

fn loadFunction(ctx: *cuda_context.CudaContext, module: cuda_driver.CUmodule, name: []const u8) !cuda_driver.CUfunction {
    var name_buffer: [128]u8 = undefined;
    const name_z = try std.fmt.bufPrintZ(&name_buffer, "{s}", .{name});
    var function: cuda_driver.CUfunction = null;
    try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&function, module, name_z));
    return function orelse error.CudaKernelUnavailable;
}

fn checkedMul(a: usize, b: usize) !usize {
    const result = @mulWithOverflow(a, b);
    if (result[1] != 0) return error.InvalidArgument;
    return result[0];
}

fn toU32(value: usize) !u32 {
    if (value > std.math.maxInt(u32)) return error.InvalidArgument;
    return @intCast(value);
}

fn parsePattern(value: []const u8) !PatternSelection {
    if (std.mem.eql(u8, value, "all")) return .all;
    if (std.mem.eql(u8, value, "random")) return .random;
    if (std.mem.eql(u8, value, "cancellation")) return .cancellation;
    return error.InvalidPattern;
}

pub fn parseConfig(args: []const []const u8) !Config {
    var cfg = Config{};
    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--inner-dim")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            if (std.mem.eql(u8, args[index], "all")) {
                cfg.inner_dim = null;
            } else {
                cfg.inner_dim = try std.fmt.parseInt(usize, args[index], 10);
            }
        } else if (std.mem.eql(u8, arg, "--pattern")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.pattern = try parsePattern(args[index]);
        } else if (std.mem.eql(u8, arg, "--seed")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.seed = try std.fmt.parseInt(u64, args[index], 0);
        } else if (std.mem.eql(u8, arg, "--json")) {
            cfg.json = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            cfg.help = true;
        } else {
            return error.UnknownArgument;
        }
    }
    try validateConfig(cfg);
    return cfg;
}

fn validateConfig(cfg: Config) !void {
    if (cfg.help) return;
    if (cfg.inner_dim) |inner_dim| _ = try variantFor(inner_dim);
}

fn nextU32(state: *u64) u32 {
    var value = state.*;
    value ^= value << 7;
    value ^= value >> 9;
    value ^= value << 8;
    state.* = value;
    return @truncate(value);
}

fn nextSignedUnit(state: *u64) f32 {
    const raw: f32 = @floatFromInt(nextU32(state) & 0xffff);
    return raw / 32767.5 - 1.0;
}

fn fillHidden(dst: []f32, pattern: Pattern, seed: u64) void {
    var state = seed ^ (@as(u64, @intFromEnum(pattern)) *% 0x9e37_79b9_7f4a_7c15);
    for (dst, 0..) |*value, index| {
        value.* = switch (pattern) {
            .random => nextSignedUnit(&state) * 0.5,
            .cancellation => blk: {
                const sign: f32 = if ((index & 1) == 0) 1.0 else -1.0;
                const jitter: f32 = @as(f32, @floatFromInt(nextU32(&state) % 17)) * 0.0009765625;
                break :blk sign * (0.03125 + jitter);
            },
        };
    }
}

fn fillDownInput(dst: []f32, pattern: Pattern, seed: u64) void {
    var state = seed ^ 0xd1b5_4a32_d192_ed03 ^ (@as(u64, @intFromEnum(pattern)) *% 0x94d0_49bb_1331_11eb);
    for (dst, 0..) |*value, index| {
        value.* = switch (pattern) {
            .random => nextSignedUnit(&state) * 0.75,
            .cancellation => blk: {
                const sign: f32 = if (((index / 7) & 1) == 0) 1.0 else -1.0;
                const jitter: f32 = @as(f32, @floatFromInt(nextU32(&state) % 23)) * 0.00048828125;
                break :blk sign * (0.0625 + jitter);
            },
        };
    }
}

fn fillQ4_0Weights(dst: []u8, out_dim: usize, in_dim: usize, seed: u64) void {
    const row_blocks = in_dim / q4_0_values_per_block;
    std.debug.assert(dst.len == out_dim * row_blocks * q4_0_block_bytes);
    var state = seed;
    for (0..out_dim) |col| {
        for (0..row_blocks) |block| {
            const offset = (col * row_blocks + block) * q4_0_block_bytes;
            const row = dst[offset..][0..q4_0_block_bytes];
            const scale_index = nextU32(&state) % 13;
            const scale = 0.0078125 + @as(f32, @floatFromInt(scale_index)) * 0.00390625;
            const scale_bits: u16 = @bitCast(@as(f16, @floatCast(scale)));
            row[0] = @truncate(scale_bits);
            row[1] = @truncate(scale_bits >> 8);
            for (0..q4_0_values_per_block / 2) |index| {
                // Q4_0 stores lanes 0..15 in the low nibble and 16..31 in
                // the high nibble. Values 1..15 represent signed -7..7.
                const low: u8 = @intCast((nextU32(&state) % 15) + 1);
                const high: u8 = @intCast((nextU32(&state) % 15) + 1);
                row[2 + index] = low | (high << 4);
            }
        }
    }
}

pub fn compareOutputs(reference: []const f32, candidate: []const f32) !BitwiseDiff {
    if (reference.len != candidate.len) return error.LengthMismatch;
    var diff = BitwiseDiff{ .element_count = reference.len };
    for (reference, candidate, 0..) |expected, actual, index| {
        const expected_bits: u32 = @bitCast(expected);
        const actual_bits: u32 = @bitCast(actual);
        if (expected_bits == actual_bits) continue;
        diff.mismatch_count += 1;
        if (diff.first_mismatch_index == null) {
            diff.first_mismatch_index = index;
            diff.first_reference_bits = expected_bits;
            diff.first_candidate_bits = actual_bits;
        }
    }
    return diff;
}

fn launch(ctx: *cuda_context.CudaContext, function: cuda_driver.CUfunction, grid_x: usize, block_x: u32, params: []?*anyopaque) !void {
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
        function,
        try toU32(grid_x),
        1,
        1,
        block_x,
        1,
        1,
        0,
        ctx.stream,
        params.ptr,
        null,
    ));
    ctx.noteKernelLaunch();
}

fn launchPair(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    dst: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    gate_weight: cuda_buffer.DeviceBuffer,
    up_weight: cuda_buffer.DeviceBuffer,
    inner_dim: usize,
) !void {
    var dst_ptr = dst.ptr;
    var input_ptr = input.ptr;
    var gate_weight_ptr = gate_weight.ptr;
    var up_weight_ptr = up_weight.ptr;
    var rows: u32 = 1;
    var in_dim: u32 = hidden_dim;
    var out_dim = try toU32(inner_dim);
    var activation: u32 = gelu_new_activation;
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&gate_weight_ptr),
        @ptrCast(&up_weight_ptr),
        @ptrCast(&rows),
        @ptrCast(&in_dim),
        @ptrCast(&out_dim),
        @ptrCast(&activation),
    };
    try launch(ctx, function, (inner_dim + q4_0_tile_cols - 1) / q4_0_tile_cols, pair_threads, params[0..]);
}

fn launchDown(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    dst: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    inner_dim: usize,
) !void {
    var dst_ptr = dst.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var rows: u32 = 1;
    var in_dim = try toU32(inner_dim);
    var out_dim: u32 = hidden_dim;
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&rows),
        @ptrCast(&in_dim),
        @ptrCast(&out_dim),
    };
    try launch(ctx, function, hidden_dim / q4_0_tile_cols, down_threads, params[0..]);
}

fn validateArtifactForDevice(ctx: *const cuda_context.CudaContext) !void {
    if (cuda_artifact.is_sm89 and (ctx.info.compute_major != 8 or ctx.info.compute_minor != 9)) {
        return error.CudaComputeCapabilityMismatch;
    }
}

fn runCase(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    module: *const Module,
    cfg: Config,
    variant: Variant,
    pattern: Pattern,
) !CaseResult {
    const pair_row_blocks = hidden_dim / q4_0_values_per_block;
    const down_row_blocks = variant.inner_dim / q4_0_values_per_block;
    const pair_weight_bytes = try checkedMul(try checkedMul(variant.inner_dim, pair_row_blocks), q4_0_block_bytes);
    const down_weight_bytes = try checkedMul(try checkedMul(hidden_dim, down_row_blocks), q4_0_block_bytes);
    const functions = try module.functionsFor(ctx, variant);

    const host_hidden = try allocator.alloc(f32, hidden_dim);
    defer allocator.free(host_hidden);
    const host_down_input = try allocator.alloc(f32, variant.inner_dim);
    defer allocator.free(host_down_input);
    const host_gate_weight = try allocator.alloc(u8, pair_weight_bytes);
    defer allocator.free(host_gate_weight);
    const host_up_weight = try allocator.alloc(u8, pair_weight_bytes);
    defer allocator.free(host_up_weight);
    const host_down_weight = try allocator.alloc(u8, down_weight_bytes);
    defer allocator.free(host_down_weight);
    const host_pair_baseline = try allocator.alloc(f32, variant.inner_dim);
    defer allocator.free(host_pair_baseline);
    const host_pair_candidate = try allocator.alloc(f32, variant.inner_dim);
    defer allocator.free(host_pair_candidate);
    const host_down_baseline = try allocator.alloc(f32, hidden_dim);
    defer allocator.free(host_down_baseline);
    const host_down_candidate = try allocator.alloc(f32, hidden_dim);
    defer allocator.free(host_down_candidate);
    const host_chain_baseline = try allocator.alloc(f32, hidden_dim);
    defer allocator.free(host_chain_baseline);
    const host_chain_candidate = try allocator.alloc(f32, hidden_dim);
    defer allocator.free(host_chain_candidate);

    fillHidden(host_hidden, pattern, cfg.seed);
    fillDownInput(host_down_input, pattern, cfg.seed);
    fillQ4_0Weights(host_gate_weight, variant.inner_dim, hidden_dim, cfg.seed ^ 0x38b3_4a93_5a9b_6b1d);
    fillQ4_0Weights(host_up_weight, variant.inner_dim, hidden_dim, cfg.seed ^ 0x7f4a_7c15_9e37_79b9);
    fillQ4_0Weights(host_down_weight, hidden_dim, variant.inner_dim, cfg.seed ^ 0xbf58_476d_1ce4_e5b9);

    var d_hidden = try cuda_buffer.DeviceBuffer.alloc(ctx, hidden_dim * @sizeOf(f32));
    defer d_hidden.free(ctx);
    var d_down_input = try cuda_buffer.DeviceBuffer.alloc(ctx, variant.inner_dim * @sizeOf(f32));
    defer d_down_input.free(ctx);
    var d_gate_weight = try cuda_buffer.DeviceBuffer.alloc(ctx, pair_weight_bytes);
    defer d_gate_weight.free(ctx);
    var d_up_weight = try cuda_buffer.DeviceBuffer.alloc(ctx, pair_weight_bytes);
    defer d_up_weight.free(ctx);
    var d_down_weight = try cuda_buffer.DeviceBuffer.alloc(ctx, down_weight_bytes);
    defer d_down_weight.free(ctx);
    var d_pair_baseline = try cuda_buffer.DeviceBuffer.alloc(ctx, variant.inner_dim * @sizeOf(f32));
    defer d_pair_baseline.free(ctx);
    var d_pair_candidate = try cuda_buffer.DeviceBuffer.alloc(ctx, variant.inner_dim * @sizeOf(f32));
    defer d_pair_candidate.free(ctx);
    var d_down_baseline = try cuda_buffer.DeviceBuffer.alloc(ctx, hidden_dim * @sizeOf(f32));
    defer d_down_baseline.free(ctx);
    var d_down_candidate = try cuda_buffer.DeviceBuffer.alloc(ctx, hidden_dim * @sizeOf(f32));
    defer d_down_candidate.free(ctx);
    var d_chain_baseline = try cuda_buffer.DeviceBuffer.alloc(ctx, hidden_dim * @sizeOf(f32));
    defer d_chain_baseline.free(ctx);
    var d_chain_candidate = try cuda_buffer.DeviceBuffer.alloc(ctx, hidden_dim * @sizeOf(f32));
    defer d_chain_candidate.free(ctx);

    try d_hidden.copyFromHost(ctx, std.mem.sliceAsBytes(host_hidden));
    try d_down_input.copyFromHost(ctx, std.mem.sliceAsBytes(host_down_input));
    try d_gate_weight.copyFromHost(ctx, host_gate_weight);
    try d_up_weight.copyFromHost(ctx, host_up_weight);
    try d_down_weight.copyFromHost(ctx, host_down_weight);

    // First compare the pair output directly, then compare down against an
    // independent F32 activation buffer and finally compare the full chain.
    try launchPair(ctx, functions.pair_baseline, d_pair_baseline, d_hidden, d_gate_weight, d_up_weight, variant.inner_dim);
    try launchPair(ctx, functions.pair_candidate, d_pair_candidate, d_hidden, d_gate_weight, d_up_weight, variant.inner_dim);
    try launchDown(ctx, functions.down_baseline, d_down_baseline, d_down_input, d_down_weight, variant.inner_dim);
    try launchDown(ctx, functions.down_candidate, d_down_candidate, d_down_input, d_down_weight, variant.inner_dim);
    try launchDown(ctx, functions.down_baseline, d_chain_baseline, d_pair_baseline, d_down_weight, variant.inner_dim);
    try launchDown(ctx, functions.down_candidate, d_chain_candidate, d_pair_candidate, d_down_weight, variant.inner_dim);

    try d_pair_baseline.copyToHost(ctx, std.mem.sliceAsBytes(host_pair_baseline));
    try d_pair_candidate.copyToHost(ctx, std.mem.sliceAsBytes(host_pair_candidate));
    try d_down_baseline.copyToHost(ctx, std.mem.sliceAsBytes(host_down_baseline));
    try d_down_candidate.copyToHost(ctx, std.mem.sliceAsBytes(host_down_candidate));
    try d_chain_baseline.copyToHost(ctx, std.mem.sliceAsBytes(host_chain_baseline));
    try d_chain_candidate.copyToHost(ctx, std.mem.sliceAsBytes(host_chain_candidate));
    try ctx.synchronize();

    return .{
        .inner_dim = variant.inner_dim,
        .pattern = pattern,
        .pair = try compareOutputs(host_pair_baseline, host_pair_candidate),
        .down = try compareOutputs(host_down_baseline, host_down_candidate),
        .chain = try compareOutputs(host_chain_baseline, host_chain_candidate),
    };
}

fn resultPasses(result: CaseResult) bool {
    return result.pair.mismatch_count == 0 and result.down.mismatch_count == 0 and result.chain.mismatch_count == 0;
}

fn allResultsPass(results: []const CaseResult) bool {
    for (results) |result| if (!resultPasses(result)) return false;
    return true;
}

fn writeDiffHuman(writer: anytype, label: []const u8, diff: BitwiseDiff) !void {
    try writer.print("  {s}: elements={d} bitwise_mismatches={d}", .{ label, diff.element_count, diff.mismatch_count });
    if (diff.first_mismatch_index) |index| {
        try writer.print(" first_mismatch={d} reference_bits=0x{x:0>8} candidate_bits=0x{x:0>8}", .{
            index,
            diff.first_reference_bits.?,
            diff.first_candidate_bits.?,
        });
    }
    try writer.print("\n", .{});
}

fn writeResultHuman(writer: anytype, result: CaseResult) !void {
    try writer.print("inner_dim={d} pattern={s} status={s}\n", .{ result.inner_dim, result.pattern.label(), if (resultPasses(result)) "pass" else "fail" });
    try writeDiffHuman(writer, "pair_activation_f32_tile4_w4", result.pair);
    try writeDiffHuman(writer, "down_f32_tile4", result.down);
    try writeDiffHuman(writer, "pair_then_down", result.chain);
}

fn writeDiffJson(writer: anytype, diff: BitwiseDiff) !void {
    try writer.print("{{\"elements\":{d},\"bitwise_mismatches\":{d},\"first_mismatch_index\":", .{ diff.element_count, diff.mismatch_count });
    if (diff.first_mismatch_index) |index| {
        try writer.print("{d},\"reference_bits\":{d},\"candidate_bits\":{d}}}", .{ index, diff.first_reference_bits.?, diff.first_candidate_bits.? });
    } else {
        try writer.writeAll("null,\"reference_bits\":null,\"candidate_bits\":null}");
    }
}

fn writeResultsJson(writer: anytype, results: []const CaseResult, ctx: *const cuda_context.CudaContext) !void {
    try writer.print("{{\"schema\":\"antfly.cuda_ffn_diff.v1\",\"device\":\"{s}\",\"compute_capability\":\"{d}.{d}\",\"pass\":{s},\"results\":[", .{
        ctx.info.nameSlice(),
        ctx.info.compute_major,
        ctx.info.compute_minor,
        if (allResultsPass(results)) "true" else "false",
    });
    for (results, 0..) |result, index| {
        if (index != 0) try writer.writeAll(",");
        try writer.print("{{\"inner_dim\":{d},\"pattern\":\"{s}\",\"pass\":{s},\"pair\":", .{ result.inner_dim, result.pattern.label(), if (resultPasses(result)) "true" else "false" });
        try writeDiffJson(writer, result.pair);
        try writer.writeAll(",\"down\":");
        try writeDiffJson(writer, result.down);
        try writer.writeAll(",\"chain\":");
        try writeDiffJson(writer, result.chain);
        try writer.writeAll("}");
    }
    try writer.writeAll("]}\n");
}

fn writeUsage(writer: anytype) !void {
    try writer.writeAll(
        \\Usage: zig build quant-kernel-cuda-ffn-diff -Dcuda=true -Dcuda-artifacts=sm89 -- [options]
        \\
        \\Launch the production F32 E2B FFN pair/down kernels and their generated
        \\exact candidates from the same checked-in CUDA artifact. Every result is
        \\a strict F32 bit-pattern comparison.
        \\
        \\Options:
        \\  --inner-dim 6144|12288|all  Default: all
        \\  --pattern random|cancellation|all
        \\  --seed N                     Decimal or 0x-prefixed deterministic seed
        \\  --json                       Emit antfly.cuda_ffn_diff.v1 JSON
        \\
    );
}

fn parseConfigFromInit(init: std.process.Init) !Config {
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_iter.next();
    var args: [16][]const u8 = undefined;
    var len: usize = 0;
    while (args_iter.next()) |arg| {
        if (len == args.len) return error.TooManyArguments;
        args[len] = arg;
        len += 1;
    }
    return parseConfig(args[0..len]);
}

pub fn main(init: std.process.Init) !void {
    const cfg = try parseConfigFromInit(init);
    var stdout_buffer: [16384]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const stdout = &stdout_writer.interface;
    if (cfg.help) {
        try writeUsage(stdout);
        try stdout.flush();
        return;
    }
    if (!build_options.enable_cuda) {
        try stdout.writeAll("CUDA is disabled; rebuild with -Dcuda=true.\n");
        try stdout.flush();
        return error.CudaDisabled;
    }

    var ctx = try cuda_context.CudaContext.initDefault();
    defer ctx.deinit();
    try validateArtifactForDevice(&ctx);
    var module = try Module.load(&ctx);
    defer module.deinit(&ctx);

    const inner_dim_count: usize = if (cfg.inner_dim == null) 2 else 1;
    var results: [4]CaseResult = undefined;
    var result_len: usize = 0;
    for (0..inner_dim_count) |inner_index| {
        const inner_dim: usize = cfg.inner_dim orelse if (inner_index == 0) 6144 else 12288;
        const variant = try variantFor(inner_dim);
        for (0..cfg.pattern.count()) |pattern_index| {
            results[result_len] = try runCase(std.heap.c_allocator, &ctx, &module, cfg, variant, cfg.pattern.at(pattern_index));
            result_len += 1;
        }
    }
    const output = results[0..result_len];
    if (cfg.json) {
        try writeResultsJson(stdout, output, &ctx);
    } else {
        try stdout.print("CUDA raw F32 E2B FFN differential: device={s} cc={d}.{d} artifact={s}\n", .{
            ctx.info.nameSlice(),
            ctx.info.compute_major,
            ctx.info.compute_minor,
            cuda_artifact.target,
        });
        for (output) |result| try writeResultHuman(stdout, result);
    }
    try stdout.flush();
    if (!allResultsPass(output)) return error.FfnDifferentialExceeded;
}

test "cuda F32 E2B FFN diff parses the strict raw case surface" {
    const cfg = try parseConfig(&.{ "--inner-dim", "6144", "--pattern", "cancellation", "--seed", "0x42", "--json" });
    try std.testing.expectEqual(@as(?usize, 6144), cfg.inner_dim);
    try std.testing.expectEqual(PatternSelection.cancellation, cfg.pattern);
    try std.testing.expectEqual(@as(u64, 0x42), cfg.seed);
    try std.testing.expect(cfg.json);
    try std.testing.expectError(error.InvalidInnerDim, parseConfig(&.{ "--inner-dim", "8192" }));
    try std.testing.expectError(error.InvalidPattern, parseConfig(&.{ "--pattern", "near-tie" }));
}

test "cuda F32 E2B FFN diff uses renderer-owned exact F32 contracts" {
    inline for ([_]usize{ 6144, 12288 }) |inner_dim| {
        const variant = try variantFor(inner_dim);
        try std.testing.expectEqual(@as(u16, pair_threads), variant.pair_plan.launch.threads_per_block);
        try std.testing.expectEqual(@as(u16, down_threads), variant.down_plan.launch.threads_per_block);
        try std.testing.expectEqual(@as(u16, hidden_dim), variant.pair_plan.launch.input_dim.fixed);
        try std.testing.expectEqual(@as(u16, inner_dim), variant.pair_plan.launch.output_dim.fixed);
        try std.testing.expectEqual(@as(u16, inner_dim), variant.down_plan.launch.input_dim.fixed);
        try std.testing.expectEqual(@as(u16, hidden_dim), variant.down_plan.launch.output_dim.fixed);
    }
}

test "cuda F32 E2B FFN diff reports signed-zero bit drift" {
    const reference = [_]f32{ 0.0, 1.0, -2.0 };
    const candidate = [_]f32{ -0.0, 1.0, -2.0 };
    const diff = try compareOutputs(&reference, &candidate);
    try std.testing.expectEqual(@as(usize, 1), diff.mismatch_count);
    try std.testing.expectEqual(@as(usize, 0), diff.first_mismatch_index.?);
    try std.testing.expectEqual(@as(u32, 0), diff.first_reference_bits.?);
    try std.testing.expectEqual(@as(u32, 0x8000_0000), diff.first_candidate_bits.?);
}
