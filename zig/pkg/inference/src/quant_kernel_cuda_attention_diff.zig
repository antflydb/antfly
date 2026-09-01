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

//! Raw CUDA decode-attention differential harness.
//!
//! This deliberately bypasses `KernelModule` routing and graph replay. It
//! loads the checked-in CUDA artifact, launches the handwritten fast kernel
//! and each generated schedule directly, then compares their output buffers.
//! That keeps a generated schedule investigation separate from model sampling
//! drift, graph state, and runtime gate selection.

const std = @import("std");
const build_options = @import("build_options");
const cuda_artifact = @import("ops/cuda/artifact.zig");
const cuda_buffer = @import("ops/cuda/buffer.zig");
const cuda_context = @import("ops/cuda/context.zig");
const cuda_driver = @import("ops/cuda/driver.zig");
const cuda_renderer = @import("graph/quant_kernel_cuda_renderer.zig");

pub const Pattern = enum {
    random,
    near_tie,
    cancellation,

    fn label(self: Pattern) []const u8 {
        return switch (self) {
            .random => "random",
            .near_tie => "near-tie",
            .cancellation => "cancellation",
        };
    }
};

const PatternSelection = enum {
    all,
    random,
    near_tie,
    cancellation,

    fn count(self: PatternSelection) usize {
        return if (self == .all) 3 else 1;
    }

    fn at(self: PatternSelection, index: usize) Pattern {
        return switch (self) {
            .all => switch (index) {
                0 => .random,
                1 => .near_tie,
                2 => .cancellation,
                else => unreachable,
            },
            .random => .random,
            .near_tie => .near_tie,
            .cancellation => .cancellation,
        };
    }
};

const SplitSelection = enum {
    all,
    split2,
    split4,
    split8,

    fn count(self: SplitSelection) usize {
        return if (self == .all) cuda_renderer.generated_attention_split_variants.len else 1;
    }

    fn at(self: SplitSelection, index: usize) cuda_renderer.AttentionSplitVariant {
        return switch (self) {
            .all => cuda_renderer.generated_attention_split_variants[index],
            .split2 => .split2,
            .split4 => .split4,
            .split8 => .split8,
        };
    }
};

pub const Config = struct {
    /// `null` runs both generated head dimensions.
    head_dim: ?u16 = null,
    kv_len: usize = 1024,
    /// `null` exposes the final KV element, matching normal decode.
    query_position: ?usize = null,
    /// `0` disables the causal sliding window, matching normal decode.
    sliding_window: u32 = 0,
    num_heads: usize = 16,
    num_kv_heads: usize = 1,
    pattern: PatternSelection = .all,
    split_selection: SplitSelection = .all,
    seed: u64 = 0x8f3d_5a71_c24e_119b,
    json: bool = false,
    max_abs: ?f32 = null,
    max_ulp: ?u64 = null,
    require_bitwise: bool = false,
    help: bool = false,
};

pub const DiffStats = struct {
    element_count: usize = 0,
    bitwise_mismatch_count: usize = 0,
    non_finite_mismatch_count: usize = 0,
    max_abs: f32 = 0,
    max_rel: f32 = 0,
    max_ulp: u64 = 0,
    first_mismatch_index: ?usize = null,
};

pub const CaseResult = struct {
    head_dim: u16,
    kv_len: usize,
    query_position: usize,
    sliding_window: u32,
    num_heads: usize,
    num_kv_heads: usize,
    pattern: Pattern,
    split_count: u8,
    serial: DiffStats,
    split: DiffStats,
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

    fn functionsForPlan(self: *const Module, ctx: *cuda_context.CudaContext, plan: cuda_renderer.AttentionRenderPlan) !AttentionFunctions {
        return .{
            .baseline_fast = try loadFunction(ctx, self.module, plan.production_baseline),
            .serial = try loadFunction(ctx, self.module, plan.serial_kernel_id),
            .stage1 = try loadFunction(ctx, self.module, plan.kernel_id),
            .stage2 = try loadFunction(ctx, self.module, plan.reduction_kernel_id),
        };
    }
};

const AttentionFunctions = struct {
    baseline_fast: cuda_driver.CUfunction,
    serial: cuda_driver.CUfunction,
    stage1: cuda_driver.CUfunction,
    stage2: cuda_driver.CUfunction,
};

fn loadFunction(ctx: *cuda_context.CudaContext, module: cuda_driver.CUmodule, name: []const u8) !cuda_driver.CUfunction {
    var name_buffer: [128]u8 = undefined;
    const name_z = try std.fmt.bufPrintSentinel(&name_buffer, "{s}", .{name}, 0);
    var function: cuda_driver.CUfunction = null;
    try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&function, module, name_z));
    return function orelse error.CudaKernelUnavailable;
}

fn checkedMul(a: usize, b: usize) !usize {
    const result = @mulWithOverflow(a, b);
    if (result[1] != 0) return error.InvalidArgument;
    return result[0];
}

fn parsePattern(value: []const u8) !PatternSelection {
    if (std.mem.eql(u8, value, "all")) return .all;
    if (std.mem.eql(u8, value, "random")) return .random;
    if (std.mem.eql(u8, value, "near-tie")) return .near_tie;
    if (std.mem.eql(u8, value, "cancellation")) return .cancellation;
    return error.InvalidPattern;
}

fn parseSplitSelection(value: []const u8) !SplitSelection {
    if (std.mem.eql(u8, value, "all")) return .all;
    if (std.mem.eql(u8, value, "2")) return .split2;
    if (std.mem.eql(u8, value, "4")) return .split4;
    if (std.mem.eql(u8, value, "8")) return .split8;
    return error.InvalidSplitCount;
}

pub fn parseConfig(args: []const []const u8) !Config {
    var cfg = Config{};
    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--head-dim")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            if (std.mem.eql(u8, args[index], "all")) {
                cfg.head_dim = null;
            } else {
                cfg.head_dim = try std.fmt.parseInt(u16, args[index], 10);
            }
        } else if (std.mem.eql(u8, arg, "--kv-len")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.kv_len = try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--query-position")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.query_position = try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--sliding-window")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.sliding_window = try std.fmt.parseInt(u32, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--heads")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.num_heads = try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--kv-heads")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.num_kv_heads = try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--pattern")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.pattern = try parsePattern(args[index]);
        } else if (std.mem.eql(u8, arg, "--split-count")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.split_selection = try parseSplitSelection(args[index]);
        } else if (std.mem.eql(u8, arg, "--seed")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.seed = try std.fmt.parseInt(u64, args[index], 0);
        } else if (std.mem.eql(u8, arg, "--max-abs")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.max_abs = try std.fmt.parseFloat(f32, args[index]);
        } else if (std.mem.eql(u8, arg, "--max-ulp")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.max_ulp = try std.fmt.parseInt(u64, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--require-bitwise")) {
            cfg.require_bitwise = true;
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
    if (cfg.head_dim) |head_dim| {
        if (head_dim != 256 and head_dim != 512) return error.InvalidHeadDim;
    }
    if (cfg.kv_len == 0 or cfg.kv_len > std.math.maxInt(u32)) return error.InvalidKvLength;
    if (cfg.query_position) |query_position| {
        if (query_position >= cfg.kv_len) return error.InvalidQueryPosition;
    }
    if (cfg.num_heads == 0 or cfg.num_heads > cuda_renderer.generated_attention_workspace_max_query_heads) return error.InvalidHeadCount;
    if (cfg.num_kv_heads == 0 or cfg.num_heads % cfg.num_kv_heads != 0) return error.InvalidKvHeadCount;
    if (cfg.num_heads / cfg.num_kv_heads > cuda_renderer.generated_attention_query_heads_per_kv_head) return error.InvalidGqaRatio;
    if (cfg.max_abs) |max_abs| if (!std.math.isFinite(max_abs) or max_abs < 0) return error.InvalidThreshold;
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

fn fillInputs(q: []f32, k: []f32, v: []f32, head_dim: usize, kv_len: usize, num_heads: usize, num_kv_heads: usize, pattern: Pattern, seed: u64) void {
    var state = seed ^ (@as(u64, @backingInt(pattern)) *% 0x9e37_79b9_7f4a_7c15);
    switch (pattern) {
        .random => {
            for (q) |*value| value.* = nextSignedUnit(&state) * 0.125;
            for (k) |*value| value.* = nextSignedUnit(&state) * 0.125;
            for (v) |*value| value.* = nextSignedUnit(&state) * 0.75;
        },
        .near_tie => {
            for (0..num_heads) |head| {
                for (0..head_dim) |d| {
                    const sign: f32 = if (((head + d) & 1) == 0) 1.0 else -1.0;
                    q[head * head_dim + d] = sign * (0.03125 + @as(f32, @floatFromInt((head + d) % 5)) * 0.00003125);
                }
            }
            for (0..kv_len) |token| {
                for (0..num_kv_heads) |kv_head| {
                    for (0..head_dim) |d| {
                        const sign: f32 = if (((kv_head + d) & 1) == 0) 1.0 else -1.0;
                        const perturb = @as(f32, @floatFromInt((token * 17 + d * 3) % 11)) * 0.00000025;
                        k[(token * num_kv_heads + kv_head) * head_dim + d] = sign * (0.03125 + perturb);
                        const parity: f32 = if (((token + d) & 1) == 0) 1.0 else -1.0;
                        v[(token * num_kv_heads + kv_head) * head_dim + d] = parity * (0.25 + @as(f32, @floatFromInt(token % 29)) * 0.0005);
                    }
                }
            }
        },
        .cancellation => {
            for (0..num_heads) |head| {
                for (0..head_dim) |d| {
                    const sign: f32 = if ((d & 1) == 0) 1.0 else -1.0;
                    const perturb = @as(f32, @floatFromInt((head * 13 + d) % 7)) * 0.00001;
                    q[head * head_dim + d] = sign * (0.125 + perturb);
                }
            }
            for (0..kv_len) |token| {
                for (0..num_kv_heads) |kv_head| {
                    for (0..head_dim) |d| {
                        const sign: f32 = if ((d & 1) == 0) 1.0 else -1.0;
                        const token_sign: f32 = if ((token & 1) == 0) 1.0 else -1.0;
                        const perturb = @as(f32, @floatFromInt((token * 19 + kv_head * 7 + d) % 13)) * 0.00001;
                        k[(token * num_kv_heads + kv_head) * head_dim + d] = sign * (token_sign * 0.125 + perturb);
                        v[(token * num_kv_heads + kv_head) * head_dim + d] = token_sign * (0.5 + @as(f32, @floatFromInt(d % 17)) * 0.0001);
                    }
                }
            }
        },
    }
}

fn orderedF32Bits(value: f32) u32 {
    const bits: u32 = @bitCast(value);
    if ((bits & 0x7fff_ffff) == 0) return 0x8000_0000;
    return if ((bits & 0x8000_0000) != 0) ~bits else bits | 0x8000_0000;
}

fn ulpDistance(a: f32, b: f32) u64 {
    const ordered_a: i64 = @intCast(orderedF32Bits(a));
    const ordered_b: i64 = @intCast(orderedF32Bits(b));
    return @intCast(if (ordered_a >= ordered_b) ordered_a - ordered_b else ordered_b - ordered_a);
}

pub fn compareOutputs(reference: []const f32, candidate: []const f32) !DiffStats {
    if (reference.len != candidate.len) return error.LengthMismatch;
    var stats = DiffStats{ .element_count = reference.len };
    for (reference, candidate, 0..) |expected, actual, index| {
        const expected_bits: u32 = @bitCast(expected);
        const actual_bits: u32 = @bitCast(actual);
        if (expected_bits != actual_bits) {
            stats.bitwise_mismatch_count += 1;
            if (stats.first_mismatch_index == null) stats.first_mismatch_index = index;
        }
        if (!std.math.isFinite(expected) or !std.math.isFinite(actual)) {
            if (expected_bits != actual_bits) stats.non_finite_mismatch_count += 1;
            continue;
        }
        const abs_error = @abs(expected - actual);
        stats.max_abs = @max(stats.max_abs, abs_error);
        const relative_error = abs_error / @max(@abs(expected), 1e-30);
        stats.max_rel = @max(stats.max_rel, relative_error);
        stats.max_ulp = @max(stats.max_ulp, ulpDistance(expected, actual));
    }
    return stats;
}

fn launch(ctx: *cuda_context.CudaContext, function: cuda_driver.CUfunction, grid: u32, block: u32, dynamic_shared_bytes: u32, params: []?*anyopaque) !void {
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
        function,
        grid,
        1,
        1,
        block,
        1,
        1,
        dynamic_shared_bytes,
        ctx.stream,
        params.ptr,
        null,
    ));
    ctx.noteKernelLaunch();
}

const LaunchArgs = struct {
    batch: u32 = 1,
    q_seq_len: u32 = 1,
    kv_seq_len: u32,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
    query_position_offset: u32,
    kv_position_offset: u32 = 0,
    sliding_window: u32 = 0,
    total_sequence_len: u32,
    mask_len: u32 = 0,
    bias_mode: u32 = 0,
};

fn attentionPlanFor(head_dim: u16, split_variant: cuda_renderer.AttentionSplitVariant) cuda_renderer.AttentionRenderPlan {
    const kind: cuda_renderer.AttentionKernelKind = switch (split_variant) {
        .split2 => if (head_dim == 256) .gqa_decode_split2_kv_hd256_f32 else .gqa_decode_split2_kv_hd512_f32,
        .split4 => if (head_dim == 256) .gqa_decode_split4_kv_hd256_f32 else .gqa_decode_split4_kv_hd512_f32,
        .split8 => if (head_dim == 256) .gqa_decode_split_kv_hd256_f32 else .gqa_decode_split_kv_hd512_f32,
        .score_prework => unreachable,
    };
    return cuda_renderer.attentionPlanFor(kind);
}

fn launchBaseline(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    dst: cuda_buffer.DeviceBuffer,
    q: cuda_buffer.DeviceBuffer,
    k: cuda_buffer.DeviceBuffer,
    v: cuda_buffer.DeviceBuffer,
    decode_scalars: cuda_buffer.DeviceBuffer,
    args: LaunchArgs,
) !void {
    var dst_ptr = dst.ptr;
    var q_ptr = q.ptr;
    var k_ptr = k.ptr;
    var v_ptr = v.ptr;
    var null_ptr: cuda_driver.CUdeviceptr = 0;
    var batch = args.batch;
    var q_seq_len = args.q_seq_len;
    var kv_seq_len = args.kv_seq_len;
    var num_heads = args.num_heads;
    var num_kv_heads = args.num_kv_heads;
    var head_dim = args.head_dim;
    var query_position_offset = args.query_position_offset;
    var kv_position_offset = args.kv_position_offset;
    var sliding_window = args.sliding_window;
    var total_sequence_len = args.total_sequence_len;
    var mask_len = args.mask_len;
    var bias_mode = args.bias_mode;
    var decode_scalars_ptr = decode_scalars.ptr;
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),               @ptrCast(&q_ptr),              @ptrCast(&k_ptr),          @ptrCast(&v_ptr),              @ptrCast(&null_ptr),     @ptrCast(&null_ptr),
        @ptrCast(&batch),                 @ptrCast(&q_seq_len),          @ptrCast(&kv_seq_len),     @ptrCast(&num_heads),          @ptrCast(&num_kv_heads), @ptrCast(&head_dim),
        @ptrCast(&query_position_offset), @ptrCast(&kv_position_offset), @ptrCast(&sliding_window), @ptrCast(&total_sequence_len), @ptrCast(&mask_len),     @ptrCast(&bias_mode),
        @ptrCast(&decode_scalars_ptr),
    };
    try launch(ctx, function, args.num_heads, args.head_dim, 0, params[0..]);
}

fn launchGeneratedSerial(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    dst: cuda_buffer.DeviceBuffer,
    q: cuda_buffer.DeviceBuffer,
    k: cuda_buffer.DeviceBuffer,
    v: cuda_buffer.DeviceBuffer,
    decode_scalars: cuda_buffer.DeviceBuffer,
    args: LaunchArgs,
    split_kv_min_tokens: u32,
    plan: cuda_renderer.AttentionRenderPlan,
) !void {
    var dst_ptr = dst.ptr;
    var q_ptr = q.ptr;
    var k_ptr = k.ptr;
    var v_ptr = v.ptr;
    var null_ptr: cuda_driver.CUdeviceptr = 0;
    var batch = args.batch;
    var q_seq_len = args.q_seq_len;
    var kv_seq_len = args.kv_seq_len;
    var num_heads = args.num_heads;
    var num_kv_heads = args.num_kv_heads;
    var head_dim = args.head_dim;
    var query_position_offset = args.query_position_offset;
    var kv_position_offset = args.kv_position_offset;
    var sliding_window = args.sliding_window;
    var total_sequence_len = args.total_sequence_len;
    var mask_len = args.mask_len;
    var bias_mode = args.bias_mode;
    var threshold = split_kv_min_tokens;
    var decode_scalars_ptr = decode_scalars.ptr;
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),               @ptrCast(&q_ptr),              @ptrCast(&k_ptr),          @ptrCast(&v_ptr),              @ptrCast(&null_ptr),     @ptrCast(&null_ptr),
        @ptrCast(&batch),                 @ptrCast(&q_seq_len),          @ptrCast(&kv_seq_len),     @ptrCast(&num_heads),          @ptrCast(&num_kv_heads), @ptrCast(&head_dim),
        @ptrCast(&query_position_offset), @ptrCast(&kv_position_offset), @ptrCast(&sliding_window), @ptrCast(&total_sequence_len), @ptrCast(&mask_len),     @ptrCast(&bias_mode),
        @ptrCast(&threshold),             @ptrCast(&decode_scalars_ptr),
    };
    try launch(ctx, function, args.num_heads, args.head_dim, plan.serial_launch.dynamic_shared_memory_bytes, params[0..]);
}

fn launchGeneratedSplit(
    ctx: *cuda_context.CudaContext,
    stage1: cuda_driver.CUfunction,
    stage2: cuda_driver.CUfunction,
    dst: cuda_buffer.DeviceBuffer,
    workspace: cuda_buffer.DeviceBuffer,
    q: cuda_buffer.DeviceBuffer,
    k: cuda_buffer.DeviceBuffer,
    v: cuda_buffer.DeviceBuffer,
    decode_scalars: cuda_buffer.DeviceBuffer,
    args: LaunchArgs,
    split_kv_min_tokens: u32,
    plan: cuda_renderer.AttentionRenderPlan,
    workspace_layout: cuda_renderer.AttentionWorkspaceLayout,
) !void {
    if (workspace.len < workspace_layout.total_bytes) return error.InvalidWorkspace;

    var dst_ptr = dst.ptr;
    var partial_values_ptr = workspace.ptr + workspace_layout.partial_values_offset;
    var partial_max_ptr = workspace.ptr + workspace_layout.partial_max_offset;
    var partial_denom_ptr = workspace.ptr + workspace_layout.partial_denom_offset;
    var q_ptr = q.ptr;
    var k_ptr = k.ptr;
    var v_ptr = v.ptr;
    var null_ptr: cuda_driver.CUdeviceptr = 0;
    var batch = args.batch;
    var q_seq_len = args.q_seq_len;
    var kv_seq_len = args.kv_seq_len;
    var num_heads = args.num_heads;
    var num_kv_heads = args.num_kv_heads;
    var head_dim = args.head_dim;
    var query_position_offset = args.query_position_offset;
    var kv_position_offset = args.kv_position_offset;
    var sliding_window = args.sliding_window;
    var total_sequence_len = args.total_sequence_len;
    var mask_len = args.mask_len;
    var bias_mode = args.bias_mode;
    var threshold = split_kv_min_tokens;
    var decode_scalars_ptr = decode_scalars.ptr;
    var stage1_params = [_]?*anyopaque{
        @ptrCast(&partial_values_ptr), @ptrCast(&partial_max_ptr), @ptrCast(&partial_denom_ptr),     @ptrCast(&q_ptr),              @ptrCast(&k_ptr),          @ptrCast(&v_ptr),
        @ptrCast(&null_ptr),           @ptrCast(&null_ptr),        @ptrCast(&batch),                 @ptrCast(&q_seq_len),          @ptrCast(&kv_seq_len),     @ptrCast(&num_heads),
        @ptrCast(&num_kv_heads),       @ptrCast(&head_dim),        @ptrCast(&query_position_offset), @ptrCast(&kv_position_offset), @ptrCast(&sliding_window), @ptrCast(&total_sequence_len),
        @ptrCast(&mask_len),           @ptrCast(&bias_mode),       @ptrCast(&threshold),             @ptrCast(&decode_scalars_ptr),
    };
    var stage2_params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),   @ptrCast(&partial_values_ptr), @ptrCast(&partial_max_ptr), @ptrCast(&partial_denom_ptr), @ptrCast(&batch),
        @ptrCast(&num_heads), @ptrCast(&head_dim),           @ptrCast(&kv_seq_len),      @ptrCast(&threshold),         @ptrCast(&decode_scalars_ptr),
    };
    const stage1_grid = try checkedMul(@as(usize, args.num_heads), @as(usize, plan.lowering.kv_splits));
    try launch(ctx, stage1, @intCast(stage1_grid), args.head_dim, plan.launch.dynamic_shared_memory_bytes, stage1_params[0..]);
    try launch(ctx, stage2, args.num_heads, args.head_dim, plan.reduction_launch.dynamic_shared_memory_bytes, stage2_params[0..]);
}

fn runCase(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    module: *const Module,
    cfg: Config,
    head_dim: u16,
    pattern: Pattern,
    split_variant: cuda_renderer.AttentionSplitVariant,
) !CaseResult {
    const dim: usize = head_dim;
    const q_len = try checkedMul(cfg.num_heads, dim);
    const kv_width = try checkedMul(cfg.num_kv_heads, dim);
    const kv_len = try checkedMul(cfg.kv_len, kv_width);
    const plan = attentionPlanFor(head_dim, split_variant);
    const workspace_layout = cuda_renderer.generatedAttentionWorkspaceLayoutFor(plan.lowering.kv_splits) orelse return error.InvalidWorkspace;
    const functions = try module.functionsForPlan(ctx, plan);

    const host_q = try allocator.alloc(f32, q_len);
    defer allocator.free(host_q);
    const host_k = try allocator.alloc(f32, kv_len);
    defer allocator.free(host_k);
    const host_v = try allocator.alloc(f32, kv_len);
    defer allocator.free(host_v);
    const host_baseline = try allocator.alloc(f32, q_len);
    defer allocator.free(host_baseline);
    const host_serial = try allocator.alloc(f32, q_len);
    defer allocator.free(host_serial);
    const host_split = try allocator.alloc(f32, q_len);
    defer allocator.free(host_split);
    fillInputs(host_q, host_k, host_v, dim, cfg.kv_len, cfg.num_heads, cfg.num_kv_heads, pattern, cfg.seed);

    var d_q = try cuda_buffer.DeviceBuffer.alloc(ctx, q_len * @sizeOf(f32));
    defer d_q.free(ctx);
    var d_k = try cuda_buffer.DeviceBuffer.alloc(ctx, kv_len * @sizeOf(f32));
    defer d_k.free(ctx);
    var d_v = try cuda_buffer.DeviceBuffer.alloc(ctx, kv_len * @sizeOf(f32));
    defer d_v.free(ctx);
    var d_baseline = try cuda_buffer.DeviceBuffer.alloc(ctx, q_len * @sizeOf(f32));
    defer d_baseline.free(ctx);
    var d_serial = try cuda_buffer.DeviceBuffer.alloc(ctx, q_len * @sizeOf(f32));
    defer d_serial.free(ctx);
    var d_split = try cuda_buffer.DeviceBuffer.alloc(ctx, q_len * @sizeOf(f32));
    defer d_split.free(ctx);
    var d_workspace = try cuda_buffer.DeviceBuffer.alloc(ctx, workspace_layout.total_bytes);
    defer d_workspace.free(ctx);
    var d_scalars = try cuda_buffer.DeviceBuffer.alloc(ctx, 5 * @sizeOf(u32));
    defer d_scalars.free(ctx);

    const scalar_value: u32 = @intCast(cfg.kv_len);
    const query_position: u32 = @intCast(cfg.query_position orelse cfg.kv_len - 1);
    const decode_scalars = [_]u32{ query_position, query_position, scalar_value, scalar_value, 0 };
    try d_q.copyFromHost(ctx, std.mem.sliceAsBytes(host_q));
    try d_k.copyFromHost(ctx, std.mem.sliceAsBytes(host_k));
    try d_v.copyFromHost(ctx, std.mem.sliceAsBytes(host_v));
    try d_scalars.copyFromHost(ctx, std.mem.sliceAsBytes(&decode_scalars));

    const args = LaunchArgs{
        .kv_seq_len = scalar_value,
        .num_heads = @intCast(cfg.num_heads),
        .num_kv_heads = @intCast(cfg.num_kv_heads),
        .head_dim = head_dim,
        .query_position_offset = query_position,
        .sliding_window = cfg.sliding_window,
        .total_sequence_len = scalar_value,
    };
    try launchBaseline(ctx, functions.baseline_fast, d_baseline, d_q, d_k, d_v, d_scalars, args);
    try d_baseline.copyToHost(ctx, std.mem.sliceAsBytes(host_baseline));
    try ctx.synchronize();

    // Force the serial kernel even for long sequence lengths.
    try launchGeneratedSerial(ctx, functions.serial, d_serial, d_q, d_k, d_v, d_scalars, args, std.math.maxInt(u32), plan);
    try d_serial.copyToHost(ctx, std.mem.sliceAsBytes(host_serial));
    try ctx.synchronize();

    // Force the composite split path for every requested length.
    try launchGeneratedSplit(ctx, functions.stage1, functions.stage2, d_split, d_workspace, d_q, d_k, d_v, d_scalars, args, 1, plan, workspace_layout);
    try d_split.copyToHost(ctx, std.mem.sliceAsBytes(host_split));
    try ctx.synchronize();

    return .{
        .head_dim = head_dim,
        .kv_len = cfg.kv_len,
        .query_position = query_position,
        .sliding_window = cfg.sliding_window,
        .num_heads = cfg.num_heads,
        .num_kv_heads = cfg.num_kv_heads,
        .pattern = pattern,
        .split_count = plan.lowering.kv_splits,
        .serial = try compareOutputs(host_baseline, host_serial),
        .split = try compareOutputs(host_baseline, host_split),
    };
}

fn meetsThresholds(stats: DiffStats, cfg: Config) bool {
    if (stats.non_finite_mismatch_count != 0) return false;
    if (cfg.require_bitwise and stats.bitwise_mismatch_count != 0) return false;
    if (cfg.max_abs) |max_abs| if (stats.max_abs > max_abs) return false;
    if (cfg.max_ulp) |max_ulp| if (stats.max_ulp > max_ulp) return false;
    return true;
}

fn resultPasses(result: CaseResult, cfg: Config) bool {
    return meetsThresholds(result.serial, cfg) and meetsThresholds(result.split, cfg);
}

fn writeStatsHuman(writer: anytype, label: []const u8, stats: DiffStats) !void {
    try writer.print(
        "  {s}: elements={d} bitwise_mismatches={d} nonfinite_mismatches={d} max_abs={e:.7} max_rel={e:.7} max_ulp={d}",
        .{ label, stats.element_count, stats.bitwise_mismatch_count, stats.non_finite_mismatch_count, stats.max_abs, stats.max_rel, stats.max_ulp },
    );
    if (stats.first_mismatch_index) |index| try writer.print(" first_mismatch={d}", .{index});
    try writer.print("\n", .{});
}

fn writeResultHuman(writer: anytype, result: CaseResult, cfg: Config) !void {
    try writer.print(
        "head_dim={d} kv_len={d} query_position={d} sliding_window={d} heads={d} kv_heads={d} split_count={d} pattern={s} status={s}\n",
        .{ result.head_dim, result.kv_len, result.query_position, result.sliding_window, result.num_heads, result.num_kv_heads, result.split_count, result.pattern.label(), if (resultPasses(result, cfg)) "pass" else "fail" },
    );
    try writeStatsHuman(writer, "generated_serial_vs_fast", result.serial);
    try writeStatsHuman(writer, "generated_split_vs_fast", result.split);
}

fn writeStatsJson(writer: anytype, stats: DiffStats) !void {
    try writer.print(
        "{{\"elements\":{d},\"bitwise_mismatches\":{d},\"nonfinite_mismatches\":{d},\"max_abs\":{e:.9},\"max_rel\":{e:.9},\"max_ulp\":{d},\"first_mismatch_index\":",
        .{ stats.element_count, stats.bitwise_mismatch_count, stats.non_finite_mismatch_count, stats.max_abs, stats.max_rel, stats.max_ulp },
    );
    if (stats.first_mismatch_index) |index| {
        try writer.print("{d}}}", .{index});
    } else {
        try writer.print("null}}", .{});
    }
}

fn writeResultsJson(writer: anytype, results: []const CaseResult, cfg: Config, device_name: []const u8) !void {
    try writer.print("{{\"schema\":\"antfly.cuda_attention_diff.v2\",\"device\":\"{s}\",\"pass\":{s},\"results\":[", .{ device_name, if (allResultsPass(results, cfg)) "true" else "false" });
    for (results, 0..) |result, index| {
        if (index != 0) try writer.print(",", .{});
        try writer.print("{{\"head_dim\":{d},\"kv_len\":{d},\"query_position\":{d},\"sliding_window\":{d},\"num_heads\":{d},\"num_kv_heads\":{d},\"split_count\":{d},\"pattern\":\"{s}\",\"pass\":{s},\"serial\":", .{
            result.head_dim,
            result.kv_len,
            result.query_position,
            result.sliding_window,
            result.num_heads,
            result.num_kv_heads,
            result.split_count,
            result.pattern.label(),
            if (resultPasses(result, cfg)) "true" else "false",
        });
        try writeStatsJson(writer, result.serial);
        try writer.print(",\"split\":", .{});
        try writeStatsJson(writer, result.split);
        try writer.print("}}", .{});
    }
    try writer.print("]}}\n", .{});
}

fn allResultsPass(results: []const CaseResult, cfg: Config) bool {
    for (results) |result| if (!resultPasses(result, cfg)) return false;
    return true;
}

fn writeUsage(writer: anytype) !void {
    try writer.writeAll(
        \\Usage: zig build quant-kernel-cuda-attention-diff -Dcuda=true -Dcuda-artifacts=sm89 -- [options]
        \\  --head-dim 256|512|all   Default: all
        \\  --kv-len N                Default: 1024
        \\  --query-position N        Zero-based visible-key limit; default: kv_len - 1
        \\  --sliding-window N        Causal visible-key window; 0 disables it
        \\  --heads N --kv-heads N    Default: 16 and 1 (maximum supported GQA ratio)
        \\  --pattern all|random|near-tie|cancellation
        \\  --split-count 2|4|8|all  Default: all
        \\  --seed N                  Decimal or 0x-prefixed deterministic seed
        \\  --max-abs X --max-ulp N   Optional failure thresholds
        \\  --require-bitwise         Fail on any output-bit mismatch
        \\  --json                    Emit antfly.cuda_attention_diff.v2 JSON
        \\
    );
}

fn parseConfigFromInit(init: std.process.Init) !Config {
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_iter.next();
    var args: [32][]const u8 = undefined;
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
    var stdout_buffer: [8192]u8 = undefined;
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
    var module = try Module.load(&ctx);
    defer module.deinit(&ctx);

    const head_dim_count: usize = if (cfg.head_dim == null) 2 else 1;
    var results: [2 * 3 * cuda_renderer.generated_attention_split_variants.len]CaseResult = undefined;
    var result_len: usize = 0;
    for (0..head_dim_count) |head_index| {
        const head_dim: u16 = cfg.head_dim orelse if (head_index == 0) 256 else 512;
        for (0..cfg.pattern.count()) |pattern_index| {
            for (0..cfg.split_selection.count()) |split_index| {
                results[result_len] = try runCase(
                    std.heap.c_allocator,
                    &ctx,
                    &module,
                    cfg,
                    head_dim,
                    cfg.pattern.at(pattern_index),
                    cfg.split_selection.at(split_index),
                );
                result_len += 1;
            }
        }
    }
    const output = results[0..result_len];
    if (cfg.json) {
        try writeResultsJson(stdout, output, cfg, ctx.info.nameSlice());
    } else {
        try stdout.print("CUDA raw attention differential: device={s} cc={d}.{d} baseline=termite_gqa_attention_decode_scalars_fast_f32\n", .{
            ctx.info.nameSlice(),
            ctx.info.compute_major,
            ctx.info.compute_minor,
        });
        for (output) |result| try writeResultHuman(stdout, result, cfg);
    }
    try stdout.flush();
    if (!allResultsPass(output, cfg)) return error.AttentionDifferentialExceeded;
}

test "cuda attention diff parses its supported raw case surface" {
    const cfg = try parseConfig(&.{ "--head-dim", "256", "--kv-len", "512", "--query-position", "511", "--sliding-window", "128", "--heads", "8", "--kv-heads", "1", "--pattern", "near-tie", "--split-count", "4", "--seed", "0x42", "--max-abs", "0.0001", "--max-ulp", "64", "--require-bitwise", "--json" });
    try std.testing.expectEqual(@as(?u16, 256), cfg.head_dim);
    try std.testing.expectEqual(@as(usize, 512), cfg.kv_len);
    try std.testing.expectEqual(@as(?usize, 511), cfg.query_position);
    try std.testing.expectEqual(@as(u32, 128), cfg.sliding_window);
    try std.testing.expectEqual(@as(usize, 8), cfg.num_heads);
    try std.testing.expectEqual(PatternSelection.near_tie, cfg.pattern);
    try std.testing.expectEqual(SplitSelection.split4, cfg.split_selection);
    try std.testing.expect(cfg.require_bitwise);
    try std.testing.expect(cfg.json);
}

test "cuda attention diff rejects invalid generated attention topology" {
    try std.testing.expectError(error.InvalidGqaRatio, parseConfig(&.{ "--heads", "32", "--kv-heads", "1" }));
    try std.testing.expectError(error.InvalidHeadDim, parseConfig(&.{ "--head-dim", "128" }));
    try std.testing.expectError(error.InvalidQueryPosition, parseConfig(&.{ "--kv-len", "512", "--query-position", "512" }));
    try std.testing.expectError(error.InvalidPattern, parseConfig(&.{ "--pattern", "unknown" }));
    try std.testing.expectError(error.InvalidSplitCount, parseConfig(&.{ "--split-count", "3" }));
}

test "cuda attention diff reports bitwise, numerical, and ULP differences" {
    const reference = [_]f32{ 0.0, 1.0, -2.0, 4.0 };
    const candidate = [_]f32{ -0.0, 1.0000001, -2.0, 3.5 };
    const stats = try compareOutputs(&reference, &candidate);
    try std.testing.expectEqual(@as(usize, 3), stats.bitwise_mismatch_count);
    try std.testing.expectEqual(@as(usize, 0), stats.first_mismatch_index.?);
    try std.testing.expect(stats.max_abs >= 0.5);
    try std.testing.expect(stats.max_ulp > 0);
}

test "cuda attention diff uses renderer-owned split plans and workspace layouts" {
    inline for ([_]u16{ 256, 512 }) |head_dim| {
        inline for (cuda_renderer.generated_attention_split_variants) |split_variant| {
            const plan = attentionPlanFor(head_dim, split_variant);
            try std.testing.expectEqual(split_variant.kvSplits(), plan.lowering.kv_splits);
            try std.testing.expect(std.mem.containsAtLeast(u8, plan.source_id, 1, split_variant.sourceTag()));
            try std.testing.expect(plan.serial_kernel_id.len > 0);
            try std.testing.expect(plan.kernel_id.len > 0);
            try std.testing.expect(plan.reduction_kernel_id.len > 0);
            const workspace = cuda_renderer.generatedAttentionWorkspaceLayoutFor(plan.lowering.kv_splits) orelse return error.MissingWorkspaceLayout;
            try std.testing.expect(workspace.total_bytes > workspace.partial_denom_offset);
        }
    }
}

test "cuda attention diff leaves reporting ungated until a threshold is requested" {
    const stats = DiffStats{
        .element_count = 1,
        .bitwise_mismatch_count = 1,
        .max_abs = 0.0001,
        .max_ulp = 8,
    };
    try std.testing.expect(meetsThresholds(stats, .{}));
    try std.testing.expect(!meetsThresholds(stats, .{ .max_abs = 0.00001 }));
    try std.testing.expect(!meetsThresholds(stats, .{ .max_ulp = 7 }));
    try std.testing.expect(!meetsThresholds(stats, .{ .require_bitwise = true }));
}
