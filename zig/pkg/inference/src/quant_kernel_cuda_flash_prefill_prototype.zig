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

//! Standalone differential and timing harness for the default-off SM89 F16
//! paged-GQA flash-prefill prototype.
//!
//! The harness loads an explicitly supplied candidate cubin and the checked-in
//! canonical CUDA cubin as separate modules; it never replaces or dispatches
//! through runtime selection. Each case compares the production-ABI WMMA
//! candidate against either the canonical warp-prefill kernel or qualified
//! Flash-v1 symbols, checks an
//! independent scalar reference against a CPU oracle, audits output guards and
//! input immutability, and optionally measures alternating baseline/candidate
//! timing pairs.

const std = @import("std");
const cuda_buffer = @import("ops/cuda/buffer.zig");
const cuda_context = @import("ops/cuda/context.zig");
const cuda_driver = @import("ops/cuda/driver.zig");

const heads: usize = 8;
const kv_heads: usize = 1;
const page_size: usize = 16;
const canonical_query_tile: usize = 16;
const canonical_key_tile: usize = 16;
const threads: u32 = 256;
const guard_bytes: usize = 256;
const canary_byte: u8 = 0xa5;
const output_poison_bits: u32 = 0x7fc0_d1ff;
const f16_poison_bits: u16 = 0x7e00;
const max_cubin_bytes: usize = 64 * 1024 * 1024;
const default_max_abs: f64 = 5e-3;
const default_max_rms_normalized: f64 = 1e-3;
const default_cpu_sample_max_abs: f64 = 5e-4;
const default_max_timing_cv: f64 = 0.10;
const max_timing_pairs: usize = 32;
const production_abi_arg_count: usize = 28;

const flash_hd256 = "antfly_gqa_attention_prefill_flash_f16_sm89_hd256_swa512_f32_prototype";
const flash_hd512 = "antfly_gqa_attention_prefill_flash_f16_sm89_hd512_global_f32_prototype";
const reference_hd256 = "antfly_gqa_attention_prefill_reference_f16_hd256_swa512_f32_prototype";
const reference_hd512 = "antfly_gqa_attention_prefill_reference_f16_hd512_global_f32_prototype";
const canonical_warp = "termite_gqa_attention_prefill_tiled_f16_warp_f32";
const canonical_flash_hd256 = "antfly_gqa_attention_prefill_flash_sm89_hd256_swa512_f32_v1";
const canonical_flash_hd512 = "antfly_gqa_attention_prefill_flash_sm89_hd512_global_f32_v1";

const BaselineRoute = enum {
    warp,
    flash,
    prototype_flash,

    fn label(self: BaselineRoute) []const u8 {
        return switch (self) {
            .warp => "canonical-warp",
            .flash => "qualified-flash-v1",
            .prototype_flash => "standalone-flash-v1",
        };
    }
};

const PageLayout = enum {
    identity,
    explicit_reversed,
    explicit_permuted,

    fn label(self: PageLayout) []const u8 {
        return switch (self) {
            .identity => "identity-null",
            .explicit_reversed => "explicit-reversed",
            .explicit_permuted => "explicit-permuted",
        };
    }
};

const LayoutSelection = enum {
    all,
    identity,
    explicit_reversed,
    explicit_permuted,

    fn count(self: LayoutSelection) usize {
        return if (self == .all) 3 else 1;
    }

    fn at(self: LayoutSelection, index: usize) PageLayout {
        return switch (self) {
            .all => switch (index) {
                0 => .identity,
                1 => .explicit_reversed,
                else => .explicit_permuted,
            },
            .identity => .identity,
            .explicit_reversed => .explicit_reversed,
            .explicit_permuted => .explicit_permuted,
        };
    }
};

const Pattern = enum {
    random,
    near_tie,
    cancellation,

    fn label(self: Pattern) []const u8 {
        return @tagName(self);
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
                else => .cancellation,
            },
            .random => .random,
            .near_tie => .near_tie,
            .cancellation => .cancellation,
        };
    }
};

const Config = struct {
    candidate_cubin_path: ?[]const u8 = null,
    baseline_cubin_path: ?[]const u8 = null,
    head_dim: ?u16 = null,
    q_len: ?u16 = null,
    prefix: ?u32 = null,
    baseline_route: BaselineRoute = .warp,
    candidate_query_tile: u16 = 16,
    candidate_key_tile: u16 = 16,
    candidate_head_group: u8 = 1,
    candidate_gqa2_concurrent_layout: bool = false,
    layout: LayoutSelection = .all,
    pattern: PatternSelection = .all,
    seed: u64 = 0x6a09_e667_f3bc_c909,
    repeats: usize = 3,
    iterations: usize = 0,
    timing_pairs: usize = 5,
    max_abs: f64 = default_max_abs,
    max_rms_normalized: f64 = default_max_rms_normalized,
    cpu_sample_max_abs: f64 = default_cpu_sample_max_abs,
    max_timing_cv: f64 = default_max_timing_cv,
    require_bitwise_candidate: bool = false,
    json_out: ?[]const u8 = null,
    help: bool = false,

    fn headDimCount(self: Config) usize {
        return if (self.head_dim == null) 2 else 1;
    }

    fn headDimAt(self: Config, index: usize) u16 {
        return self.head_dim orelse if (index == 0) 256 else 512;
    }

    fn qLenCount(self: Config) usize {
        return if (self.q_len == null) 2 else 1;
    }

    fn qLenAt(self: Config, index: usize) u16 {
        return self.q_len orelse if (index == 0) 512 else 3;
    }

    fn prefixCount(self: Config, q_len: u16) usize {
        if (self.prefix != null) return 1;
        return if (q_len == 512) 4 else 1;
    }

    fn prefixAt(self: Config, q_len: u16, index: usize) u32 {
        return self.prefix orelse if (q_len == 512)
            ([_]u32{ 0, 512, 1024, 1536 })[index]
        else
            2048;
    }
};

const CaseSpec = struct {
    head_dim: u16,
    q_len: u16,
    prefix: u32,
    sliding_window: u32,
    layout: PageLayout,
    pattern: Pattern,
    seed: u64,

    fn kvLen(self: CaseSpec) usize {
        return @as(usize, self.prefix) + self.q_len;
    }
};

const VisibleRange = struct {
    begin: usize,
    end: usize,
};

const DiffStats = struct {
    element_count: usize = 0,
    bitwise_mismatches: usize = 0,
    non_finite: usize = 0,
    max_abs: f64 = 0,
    rms_error: f64 = 0,
    rms_normalized: f64 = 0,
};

const TimingStats = struct {
    pairs: usize = 0,
    iterations: usize = 0,
    baseline_sum_us: f64 = 0,
    candidate_sum_us: f64 = 0,
    baseline_sum_squared: f64 = 0,
    candidate_sum_squared: f64 = 0,

    fn baselineMean(self: TimingStats) f64 {
        return if (self.pairs == 0) 0 else self.baseline_sum_us / @as(f64, @floatFromInt(self.pairs));
    }

    fn candidateMean(self: TimingStats) f64 {
        return if (self.pairs == 0) 0 else self.candidate_sum_us / @as(f64, @floatFromInt(self.pairs));
    }

    fn cv(sum: f64, sum_squared: f64, count: usize) f64 {
        if (count < 2 or sum <= 0) return 0;
        const n: f64 = @floatFromInt(count);
        const mean = sum / n;
        const variance = @max(@as(f64, 0), sum_squared / n - mean * mean);
        return @sqrt(variance) / mean;
    }

    fn baselineCv(self: TimingStats) f64 {
        return cv(self.baseline_sum_us, self.baseline_sum_squared, self.pairs);
    }

    fn candidateCv(self: TimingStats) f64 {
        return cv(self.candidate_sum_us, self.candidate_sum_squared, self.pairs);
    }
};

const CaseResult = struct {
    spec: CaseSpec,
    candidate_baseline_diff: DiffStats,
    reference_baseline_diff: DiffStats,
    cpu_sample_max_abs: f64,
    cpu_sample_non_finite: usize,
    candidate_unwritten: usize,
    baseline_unwritten: usize,
    reference_unwritten: usize,
    candidate_canary_mismatches: usize,
    baseline_canary_mismatches: usize,
    reference_canary_mismatches: usize,
    readonly_mismatches: usize,
    candidate_determinism_mismatches: usize,
    baseline_determinism_mismatches: usize,
    timing: TimingStats,

    fn passes(self: CaseResult, cfg: Config) bool {
        return (!cfg.require_bitwise_candidate or self.candidate_baseline_diff.bitwise_mismatches == 0) and
            self.candidate_baseline_diff.non_finite == 0 and
            self.candidate_baseline_diff.max_abs <= cfg.max_abs and
            self.candidate_baseline_diff.rms_normalized <= cfg.max_rms_normalized and
            self.reference_baseline_diff.non_finite == 0 and
            self.reference_baseline_diff.max_abs <= cfg.max_abs and
            self.reference_baseline_diff.rms_normalized <= cfg.max_rms_normalized and
            self.cpu_sample_non_finite == 0 and
            self.cpu_sample_max_abs <= cfg.cpu_sample_max_abs and
            self.candidate_unwritten == 0 and
            self.baseline_unwritten == 0 and
            self.reference_unwritten == 0 and
            self.candidate_canary_mismatches == 0 and
            self.baseline_canary_mismatches == 0 and
            self.reference_canary_mismatches == 0 and
            self.readonly_mismatches == 0 and
            self.candidate_determinism_mismatches == 0 and
            self.baseline_determinism_mismatches == 0 and
            (self.timing.pairs == 0 or
                (self.timing.baselineCv() <= cfg.max_timing_cv and
                    self.timing.candidateCv() <= cfg.max_timing_cv));
    }
};

const Functions = struct {
    flash256: cuda_driver.CUfunction,
    flash512: cuda_driver.CUfunction,
    reference256: cuda_driver.CUfunction,
    reference512: cuda_driver.CUfunction,

    fn candidate(self: Functions, head_dim: u16) cuda_driver.CUfunction {
        return if (head_dim == 256) self.flash256 else self.flash512;
    }

    fn reference(self: Functions, head_dim: u16) cuda_driver.CUfunction {
        return if (head_dim == 256) self.reference256 else self.reference512;
    }
};

const BaselineFunctions = struct {
    warp: cuda_driver.CUfunction,
    flash256: cuda_driver.CUfunction,
    flash512: cuda_driver.CUfunction,

    fn selected(self: BaselineFunctions, route: BaselineRoute, head_dim: u16) cuda_driver.CUfunction {
        return switch (route) {
            .warp => self.warp,
            .flash, .prototype_flash => if (head_dim == 256) self.flash256 else self.flash512,
        };
    }
};

const Module = struct {
    module: cuda_driver.CUmodule = null,

    fn load(ctx: *cuda_context.CudaContext, image: []const u8) !Module {
        if (image.len == 0) return error.EmptyArtifact;
        try ctx.makeCurrent();
        var module: cuda_driver.CUmodule = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, image.ptr, 0, null, null));
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

    fn functions(self: Module, ctx: *cuda_context.CudaContext) !Functions {
        return .{
            .flash256 = try loadFunction(ctx, self.module, flash_hd256),
            .flash512 = try loadFunction(ctx, self.module, flash_hd512),
            .reference256 = try loadFunction(ctx, self.module, reference_hd256),
            .reference512 = try loadFunction(ctx, self.module, reference_hd512),
        };
    }

    fn baselineFunctions(self: Module, ctx: *cuda_context.CudaContext, route: BaselineRoute) !BaselineFunctions {
        return switch (route) {
            .warp => .{
                .warp = try loadFunction(ctx, self.module, canonical_warp),
                .flash256 = null,
                .flash512 = null,
            },
            .flash => .{
                .warp = null,
                .flash256 = try loadFunction(ctx, self.module, canonical_flash_hd256),
                .flash512 = try loadFunction(ctx, self.module, canonical_flash_hd512),
            },
            .prototype_flash => .{
                .warp = null,
                .flash256 = try loadFunction(ctx, self.module, flash_hd256),
                .flash512 = try loadFunction(ctx, self.module, flash_hd512),
            },
        };
    }
};

const LaunchBuffers = struct {
    output: cuda_buffer.DeviceBuffer,
    q: cuda_buffer.DeviceBuffer,
    k: cuda_buffer.DeviceBuffer,
    v: cuda_buffer.DeviceBuffer,
    block_table: cuda_buffer.DeviceBuffer,
};

fn checkedAdd(a: usize, b: usize) !usize {
    const result = @addWithOverflow(a, b);
    if (result[1] != 0) return error.InvalidArgument;
    return result[0];
}

fn checkedMul(a: usize, b: usize) !usize {
    const result = @mulWithOverflow(a, b);
    if (result[1] != 0) return error.InvalidArgument;
    return result[0];
}

fn ceilDiv(value: usize, divisor: usize) usize {
    return value / divisor + @intFromBool(value % divisor != 0);
}

fn toU32(value: usize) !u32 {
    if (value > std.math.maxInt(u32)) return error.InvalidArgument;
    return @intCast(value);
}

fn flashSharedBytes(head_dim: usize, query_rows: usize, key_rows: usize, head_group: usize) !u32 {
    if (head_dim != 256 and head_dim != 512) return error.InvalidHeadDim;
    if ((query_rows != 16 and query_rows != 32 and query_rows != 64) or
        (key_rows != 16 and key_rows != 32 and key_rows != 64)) return error.InvalidTile;
    if (head_group != 1 and head_group != 2 and head_group != 4) return error.InvalidHeadGroup;
    const grouped_query_rows = try checkedMul(head_group, query_rows);
    var bytes = try checkedMul(try checkedAdd(grouped_query_rows, key_rows), try checkedMul(head_dim, @sizeOf(f16)));
    // Eight warp-private F32 score/reweight tiles plus one reduced score tile.
    bytes = try checkedAdd(bytes, 8 * page_size * page_size * @sizeOf(f32));
    bytes = try checkedAdd(bytes, page_size * page_size * @sizeOf(f32));
    bytes = try checkedAdd(bytes, try checkedMul(grouped_query_rows, key_rows * @sizeOf(f16)));
    bytes = try checkedAdd(bytes, key_rows * @sizeOf(u32));
    bytes = try checkedAdd(bytes, 2 * query_rows * @sizeOf(u32));
    bytes = try checkedAdd(bytes, 2 * grouped_query_rows * @sizeOf(f32));
    bytes = try checkedAdd(bytes, 2 * grouped_query_rows * (key_rows / page_size) * @sizeOf(f32));
    bytes = try checkedAdd(bytes, @sizeOf(u32));
    return try toU32(bytes);
}

fn baselineSharedBytes(head_dim: usize, route: BaselineRoute) !u32 {
    return switch (route) {
        .warp => 0,
        .flash, .prototype_flash => flashSharedBytes(head_dim, canonical_query_tile, canonical_key_tile, 1),
    };
}

fn candidateSharedBytes(cfg: Config, head_dim: usize) !u32 {
    if (cfg.candidate_gqa2_concurrent_layout) {
        if (cfg.candidate_query_tile != 16 or cfg.candidate_key_tile != 16 or
            cfg.candidate_head_group != 2) return error.InvalidGqa2ConcurrentLayout;
        if (head_dim != 256 and head_dim != 512) return error.InvalidHeadDim;
        var bytes = try checkedMul(2 * canonical_query_tile, try checkedMul(head_dim, @sizeOf(f16)));
        bytes = try checkedAdd(bytes, 8 * canonical_query_tile * canonical_key_tile * @sizeOf(f32));
        bytes = try checkedAdd(bytes, 2 * canonical_query_tile * canonical_key_tile * @sizeOf(f32));
        bytes = try checkedAdd(bytes, 2 * canonical_query_tile * canonical_key_tile * @sizeOf(f16));
        bytes = try checkedAdd(bytes, 3 * canonical_query_tile * @sizeOf(u32));
        bytes = try checkedAdd(bytes, 4 * 2 * canonical_query_tile * @sizeOf(f32));
        bytes = try checkedAdd(bytes, @sizeOf(u32));
        return try toU32(bytes);
    }
    return flashSharedBytes(
        head_dim,
        cfg.candidate_query_tile,
        cfg.candidate_key_tile,
        cfg.candidate_head_group,
    );
}

fn parseBaselineRoute(value: []const u8) !BaselineRoute {
    if (std.mem.eql(u8, value, "warp")) return .warp;
    if (std.mem.eql(u8, value, "flash")) return .flash;
    if (std.mem.eql(u8, value, "prototype-flash")) return .prototype_flash;
    return error.InvalidBaselineRoute;
}

fn parseLayout(value: []const u8) !LayoutSelection {
    if (std.mem.eql(u8, value, "all")) return .all;
    if (std.mem.eql(u8, value, "identity")) return .identity;
    if (std.mem.eql(u8, value, "explicit-reversed")) return .explicit_reversed;
    if (std.mem.eql(u8, value, "explicit-permuted")) return .explicit_permuted;
    return error.InvalidLayout;
}

fn parsePattern(value: []const u8) !PatternSelection {
    if (std.mem.eql(u8, value, "all")) return .all;
    if (std.mem.eql(u8, value, "random")) return .random;
    if (std.mem.eql(u8, value, "near-tie")) return .near_tie;
    if (std.mem.eql(u8, value, "cancellation")) return .cancellation;
    return error.InvalidPattern;
}

fn parseConfig(args: []const []const u8) !Config {
    var cfg = Config{};
    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--candidate-cubin") or std.mem.eql(u8, arg, "--cubin")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.candidate_cubin_path = args[index];
        } else if (std.mem.eql(u8, arg, "--baseline-cubin")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.baseline_cubin_path = args[index];
        } else if (std.mem.eql(u8, arg, "--head-dim")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.head_dim = if (std.mem.eql(u8, args[index], "all")) null else try std.fmt.parseInt(u16, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--q-len")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.q_len = if (std.mem.eql(u8, args[index], "all")) null else try std.fmt.parseInt(u16, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--prefix")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.prefix = if (std.mem.eql(u8, args[index], "all")) null else try std.fmt.parseInt(u32, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--baseline-route")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.baseline_route = try parseBaselineRoute(args[index]);
        } else if (std.mem.eql(u8, arg, "--candidate-query-tile")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.candidate_query_tile = try std.fmt.parseInt(u16, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--candidate-key-tile")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.candidate_key_tile = try std.fmt.parseInt(u16, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--candidate-head-group")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.candidate_head_group = try std.fmt.parseInt(u8, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--candidate-gqa2-concurrent-layout")) {
            cfg.candidate_gqa2_concurrent_layout = true;
        } else if (std.mem.eql(u8, arg, "--layout")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.layout = try parseLayout(args[index]);
        } else if (std.mem.eql(u8, arg, "--pattern")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.pattern = try parsePattern(args[index]);
        } else if (std.mem.eql(u8, arg, "--seed")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.seed = try std.fmt.parseInt(u64, args[index], 0);
        } else if (std.mem.eql(u8, arg, "--repeats")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.repeats = try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--iterations")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.iterations = try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--timing-pairs")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.timing_pairs = try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--max-abs")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.max_abs = try std.fmt.parseFloat(f64, args[index]);
        } else if (std.mem.eql(u8, arg, "--max-rms-normalized")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.max_rms_normalized = try std.fmt.parseFloat(f64, args[index]);
        } else if (std.mem.eql(u8, arg, "--cpu-sample-max-abs")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.cpu_sample_max_abs = try std.fmt.parseFloat(f64, args[index]);
        } else if (std.mem.eql(u8, arg, "--max-timing-cv")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.max_timing_cv = try std.fmt.parseFloat(f64, args[index]);
        } else if (std.mem.eql(u8, arg, "--require-bitwise-candidate")) {
            cfg.require_bitwise_candidate = true;
        } else if (std.mem.eql(u8, arg, "--json-out")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.json_out = args[index];
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
    if (cfg.candidate_cubin_path == null or cfg.candidate_cubin_path.?.len == 0) return error.MissingCandidateCubin;
    if (cfg.baseline_cubin_path == null or cfg.baseline_cubin_path.?.len == 0) return error.MissingBaselineCubin;
    if (cfg.head_dim) |value| if (value != 256 and value != 512) return error.InvalidHeadDim;
    _ = flashSharedBytes(256, cfg.candidate_query_tile, cfg.candidate_key_tile, cfg.candidate_head_group) catch |err| switch (err) {
        error.InvalidTile => return error.InvalidTile,
        error.InvalidHeadGroup => return error.InvalidHeadGroup,
        else => return err,
    };
    _ = try candidateSharedBytes(cfg, 256);
    if (cfg.q_len) |value| if (value != 512 and value != 3) return error.InvalidQueryLength;
    if (cfg.prefix) |value| {
        if (cfg.q_len == null) return error.AmbiguousPrefix;
        if (cfg.q_len == 3 and value != 2048) return error.InvalidPrefix;
        if (cfg.q_len == 512 and value != 0 and value != 512 and value != 1024 and value != 1536) return error.InvalidPrefix;
    }
    if (cfg.repeats < 2 or cfg.repeats > 100) return error.InvalidRepeats;
    if (cfg.iterations > 1_000_000) return error.InvalidIterations;
    if (cfg.timing_pairs == 0 or cfg.timing_pairs > max_timing_pairs) return error.InvalidTimingPairs;
    if (!std.math.isFinite(cfg.max_abs) or cfg.max_abs < 0) return error.InvalidMaxAbs;
    if (!std.math.isFinite(cfg.max_rms_normalized) or cfg.max_rms_normalized < 0) return error.InvalidMaxRmsNormalized;
    if (!std.math.isFinite(cfg.cpu_sample_max_abs) or cfg.cpu_sample_max_abs < 0) return error.InvalidCpuSampleMaxAbs;
    if (!std.math.isFinite(cfg.max_timing_cv) or cfg.max_timing_cv < 0) return error.InvalidMaxTimingCv;
}

fn parseConfigFromInit(init: std.process.Init) !Config {
    var iterator = std.process.Args.Iterator.init(init.minimal.args);
    _ = iterator.next();
    var args: [64][]const u8 = undefined;
    var len: usize = 0;
    while (iterator.next()) |arg| {
        if (len == args.len) return error.TooManyArguments;
        args[len] = arg;
        len += 1;
    }
    return parseConfig(args[0..len]);
}

fn caseCount(cfg: Config) !usize {
    var shapes: usize = 0;
    for (0..cfg.qLenCount()) |q_index| {
        shapes = try checkedAdd(shapes, cfg.prefixCount(cfg.qLenAt(q_index)));
    }
    var count = try checkedMul(cfg.headDimCount(), shapes);
    count = try checkedMul(count, cfg.layout.count());
    return checkedMul(count, cfg.pattern.count());
}

fn fillCaseSpecs(cfg: Config, specs: []CaseSpec) !void {
    if (specs.len != try caseCount(cfg)) return error.LengthMismatch;
    var cursor: usize = 0;
    for (0..cfg.headDimCount()) |head_index| {
        const head_dim = cfg.headDimAt(head_index);
        for (0..cfg.qLenCount()) |q_index| {
            const q_len = cfg.qLenAt(q_index);
            for (0..cfg.prefixCount(q_len)) |prefix_index| {
                for (0..cfg.layout.count()) |layout_index| {
                    for (0..cfg.pattern.count()) |pattern_index| {
                        specs[cursor] = .{
                            .head_dim = head_dim,
                            .q_len = q_len,
                            .prefix = cfg.prefixAt(q_len, prefix_index),
                            .sliding_window = if (head_dim == 256) 512 else 0,
                            .layout = cfg.layout.at(layout_index),
                            .pattern = cfg.pattern.at(pattern_index),
                            .seed = cfg.seed ^ (@as(u64, cursor) *% 0x9e37_79b9_7f4a_7c15),
                        };
                        cursor += 1;
                    }
                }
            }
        }
    }
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

fn fillInputs(q: []f32, k: []f16, v: []f16, spec: CaseSpec) void {
    var state = spec.seed;
    switch (spec.pattern) {
        .random => {
            for (q) |*value| value.* = @floatCast(nextSignedUnit(&state) * 0.125);
            for (k) |*value| value.* = @floatCast(nextSignedUnit(&state) * 0.125);
            for (v) |*value| value.* = @floatCast(nextSignedUnit(&state) * 0.75);
        },
        .near_tie => {
            const head_dim: usize = spec.head_dim;
            for (0..spec.q_len) |query| for (0..heads) |head| for (0..head_dim) |dimension| {
                const sign: f32 = if (((query + head + dimension) & 1) == 0) 1 else -1;
                const perturb: f32 = @floatFromInt((query * 11 + head * 7 + dimension) % 5);
                q[(query * heads + head) * head_dim + dimension] =
                    @floatCast(sign * (0.03125 + perturb * 0.00003125));
            };
            for (0..spec.kvLen()) |token| for (0..head_dim) |dimension| {
                const sign: f32 = if ((dimension & 1) == 0) 1 else -1;
                const perturb: f32 = @floatFromInt((token * 17 + dimension * 3) % 11);
                k[token * head_dim + dimension] =
                    @floatCast(sign * (0.03125 + perturb * 0.00000025));
                const value_sign: f32 = if (((token + dimension) & 1) == 0) 1 else -1;
                v[token * head_dim + dimension] =
                    @floatCast(value_sign * (0.25 + @as(f32, @floatFromInt(token % 29)) * 0.0005));
            };
        },
        .cancellation => {
            const head_dim: usize = spec.head_dim;
            for (0..spec.q_len) |query| for (0..heads) |head| for (0..head_dim) |dimension| {
                const sign: f32 = if ((dimension & 1) == 0) 1 else -1;
                const perturb: f32 = @floatFromInt((query * 5 + head * 13 + dimension) % 7);
                q[(query * heads + head) * head_dim + dimension] =
                    @floatCast(sign * (0.125 + perturb * 0.00001));
            };
            for (0..spec.kvLen()) |token| for (0..head_dim) |dimension| {
                const dimension_sign: f32 = if ((dimension & 1) == 0) 1 else -1;
                const token_sign: f32 = if ((token & 1) == 0) 1 else -1;
                const perturb: f32 = @floatFromInt((token * 19 + dimension) % 13);
                k[token * head_dim + dimension] =
                    @floatCast(dimension_sign * (token_sign * 0.125 + perturb * 0.00001));
                v[token * head_dim + dimension] =
                    @floatCast(token_sign * (0.5 + @as(f32, @floatFromInt(dimension % 17)) * 0.0001));
            };
        },
    }
}

fn fillBlockTable(table: []u32, layout: PageLayout) void {
    for (table, 0..) |*entry, index| entry.* = @intCast(index);
    if (layout == .explicit_reversed) std.mem.reverse(u32, table);
    if (layout == .explicit_permuted and table.len > 1) {
        // A one-page rotation is a deterministic non-involutive bijection for
        // every production prefix in the qualification matrix. Keeping the
        // permutation independent of payload data catches accidental logical
        // addressing while making failures exactly reproducible.
        for (table, 0..) |*entry, index| entry.* = @intCast((index + 1) % table.len);
    }
}

fn physicalToken(logical: usize, table: []const u32) !usize {
    if (table.len == 0) return logical;
    const logical_block = logical / page_size;
    if (logical_block >= table.len) return error.InvalidPageTable;
    return checkedAdd(
        try checkedMul(@as(usize, table[logical_block]), page_size),
        logical % page_size,
    );
}

fn packPaged(dst: []f16, logical: []const f16, table: []const u32, head_dim: usize) !void {
    @memset(dst, @as(f16, @bitCast(f16_poison_bits)));
    if (logical.len % head_dim != 0) return error.LengthMismatch;
    const logical_tokens = logical.len / head_dim;
    for (0..logical_tokens) |token| {
        const physical = try physicalToken(token, table);
        const dst_begin = try checkedMul(physical, head_dim);
        const src_begin = token * head_dim;
        if (dst_begin + head_dim > dst.len) return error.InvalidPageTable;
        @memcpy(dst[dst_begin .. dst_begin + head_dim], logical[src_begin .. src_begin + head_dim]);
    }
}

fn visibleRange(query_pos: usize, kv_len: usize, kv_position_offset: usize, sliding_window: usize) VisibleRange {
    if (query_pos < kv_position_offset) return .{ .begin = 0, .end = 0 };
    const visible = query_pos - kv_position_offset + 1;
    const end = @min(visible, kv_len);
    var begin: usize = 0;
    if (sliding_window != 0) {
        const window_start_abs = if (query_pos + 1 > sliding_window)
            query_pos + 1 - sliding_window
        else
            0;
        if (window_start_abs > kv_position_offset) begin = @min(window_start_abs - kv_position_offset, end);
    }
    return .{ .begin = begin, .end = end };
}

fn cpuReferenceColumns(
    q: []const f32,
    k: []const f16,
    v: []const f16,
    spec: CaseSpec,
    query_index: usize,
    head: usize,
    columns: [3]usize,
) [3]f32 {
    const head_dim: usize = spec.head_dim;
    const query_pos = @as(usize, spec.prefix) + query_index;
    const range = visibleRange(query_pos, spec.kvLen(), 0, spec.sliding_window);
    const q_offset = (query_index * heads + head) * head_dim;
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
    var maximum = -std.math.inf(f32);
    var denominator: f32 = 0;
    var accumulators = [_]f32{ 0, 0, 0 };
    for (range.begin..range.end) |token| {
        var dot: f32 = 0;
        const kv_offset = token * head_dim;
        for (0..head_dim) |dimension| {
            dot += @as(f32, q[q_offset + dimension]) * @as(f32, k[kv_offset + dimension]);
        }
        const score = dot * scale;
        const next_max = @max(maximum, score);
        const alpha: f32 = if (denominator > 0) @exp(maximum - next_max) else 0;
        const beta: f32 = @exp(score - next_max);
        denominator = denominator * alpha + beta;
        maximum = next_max;
        for (columns, 0..) |column, index| {
            accumulators[index] = accumulators[index] * alpha +
                beta * @as(f32, v[kv_offset + column]);
        }
    }
    if (denominator > 0) {
        for (&accumulators) |*value| value.* /= denominator;
    }
    return accumulators;
}

fn cpuSampleDiff(reference: []const f32, q: []const f32, k: []const f16, v: []const f16, spec: CaseSpec) struct { max_abs: f64, non_finite: usize } {
    const head_dim: usize = spec.head_dim;
    const query_indices = [_]usize{ 0, @as(usize, spec.q_len) - 1 };
    const sampled_heads = [_]usize{ 0, heads - 1 };
    const columns = [_]usize{ 0, head_dim / 3, head_dim - 1 };
    var max_abs: f64 = 0;
    var non_finite: usize = 0;
    for (query_indices) |query_index| for (sampled_heads) |head| {
        const expected = cpuReferenceColumns(q, k, v, spec, query_index, head, columns);
        for (columns, 0..) |column, index| {
            const output_index = (query_index * heads + head) * head_dim + column;
            const actual = reference[output_index];
            if (!std.math.isFinite(expected[index]) or !std.math.isFinite(actual)) {
                non_finite += 1;
            } else {
                max_abs = @max(max_abs, @abs(@as(f64, expected[index]) - @as(f64, actual)));
            }
        }
    };
    return .{ .max_abs = max_abs, .non_finite = non_finite };
}

fn compareOutputs(reference: []const f32, candidate: []const f32) !DiffStats {
    if (reference.len != candidate.len) return error.LengthMismatch;
    var result = DiffStats{ .element_count = reference.len };
    var error_squared: f64 = 0;
    var reference_squared: f64 = 0;
    for (reference, candidate) |expected, actual| {
        if (@as(u32, @bitCast(expected)) != @as(u32, @bitCast(actual))) result.bitwise_mismatches += 1;
        if (!std.math.isFinite(expected) or !std.math.isFinite(actual)) {
            result.non_finite += 1;
            continue;
        }
        const expected64: f64 = expected;
        const actual64: f64 = actual;
        const difference = @abs(expected64 - actual64);
        result.max_abs = @max(result.max_abs, difference);
        error_squared += difference * difference;
        reference_squared += expected64 * expected64;
    }
    if (reference.len != 0) {
        const count: f64 = @floatFromInt(reference.len);
        result.rms_error = @sqrt(error_squared / count);
        const reference_rms = @sqrt(reference_squared / count);
        result.rms_normalized = result.rms_error / @max(reference_rms, 1e-6);
    }
    return result;
}

fn fillOutputImage(image: []u8, payload_bytes: usize) !void {
    if (image.len != try checkedAdd(payload_bytes, 2 * guard_bytes)) return error.LengthMismatch;
    @memset(image, canary_byte);
    var offset = guard_bytes;
    while (offset < guard_bytes + payload_bytes) : (offset += 4) {
        image[offset] = @truncate(output_poison_bits);
        image[offset + 1] = @truncate(output_poison_bits >> 8);
        image[offset + 2] = @truncate(output_poison_bits >> 16);
        image[offset + 3] = @truncate(output_poison_bits >> 24);
    }
}

fn readU32Le(bytes: []const u8, offset: usize) u32 {
    return @as(u32, bytes[offset]) |
        (@as(u32, bytes[offset + 1]) << 8) |
        (@as(u32, bytes[offset + 2]) << 16) |
        (@as(u32, bytes[offset + 3]) << 24);
}

fn copyOutputValues(dst: []f32, image: []const u8) !void {
    if (image.len != dst.len * @sizeOf(f32) + 2 * guard_bytes) return error.LengthMismatch;
    for (dst, 0..) |*value, index| {
        value.* = @bitCast(readU32Le(image, guard_bytes + index * 4));
    }
}

fn countUnwritten(values: []const f32) usize {
    var count: usize = 0;
    for (values) |value| if (@as(u32, @bitCast(value)) == output_poison_bits) {
        count += 1;
    };
    return count;
}

fn countCanaryMismatches(image: []const u8, payload_bytes: usize) usize {
    var count: usize = 0;
    for (image[0..guard_bytes]) |byte| if (byte != canary_byte) {
        count += 1;
    };
    for (image[guard_bytes + payload_bytes ..]) |byte| if (byte != canary_byte) {
        count += 1;
    };
    return count;
}

fn countByteMismatches(a: []const u8, b: []const u8) !usize {
    if (a.len != b.len) return error.LengthMismatch;
    var count: usize = 0;
    for (a, b) |lhs, rhs| if (lhs != rhs) {
        count += 1;
    };
    return count;
}

fn countBitwiseMismatches(a: []const f32, b: []const f32) !usize {
    if (a.len != b.len) return error.LengthMismatch;
    var count: usize = 0;
    for (a, b) |lhs, rhs| if (@as(u32, @bitCast(lhs)) != @as(u32, @bitCast(rhs))) {
        count += 1;
    };
    return count;
}

fn artifactSha256Hex(image: []const u8) [64]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(image, &digest, .{});
    const alphabet = "0123456789abcdef";
    var hex: [64]u8 = undefined;
    for (digest, 0..) |byte, index| {
        hex[index * 2] = alphabet[byte >> 4];
        hex[index * 2 + 1] = alphabet[byte & 0x0f];
    }
    return hex;
}

fn readFileAllocAtPath(io: std.Io, path: []const u8, allocator: std.mem.Allocator) ![]u8 {
    if (!std.fs.path.isAbsolute(path)) {
        return std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_cubin_bytes));
    }
    const parent = std.fs.path.dirname(path) orelse return error.BadPathName;
    const base = std.fs.path.basename(path);
    var directory = try std.Io.Dir.openDirAbsolute(io, parent, .{});
    defer directory.close(io);
    return directory.readFileAlloc(io, base, allocator, .limited(max_cubin_bytes));
}

fn loadFunction(ctx: *cuda_context.CudaContext, module: cuda_driver.CUmodule, name: []const u8) !cuda_driver.CUfunction {
    var name_buffer: [160]u8 = undefined;
    const name_z = try std.fmt.bufPrintZ(&name_buffer, "{s}", .{name});
    var function: cuda_driver.CUfunction = null;
    try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&function, module, name_z));
    return function orelse error.CudaKernelUnavailable;
}

const LaunchKind = enum {
    candidate,
    canonical_warp_baseline,
    canonical_flash_baseline,
    scalar_reference,
};

fn launchAttention(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    kind: LaunchKind,
    cfg: Config,
    spec: CaseSpec,
    buffers: LaunchBuffers,
    block_count_value: u32,
    physical_capacity: u32,
) !void {
    var dst_ptr = buffers.output.ptr + guard_bytes;
    var q_ptr = buffers.q.ptr;
    var k_ptr = buffers.k.ptr;
    var v_ptr = buffers.v.ptr;
    var table_ptr = buffers.block_table.ptr;
    var mask_ptr: cuda_driver.CUdeviceptr = 0;
    var bias_ptr: cuda_driver.CUdeviceptr = 0;
    var batch: u32 = 1;
    var q_seq_len: u32 = spec.q_len;
    var kv_seq_len = try toU32(spec.kvLen());
    var num_heads: u32 = heads;
    var num_kv_heads: u32 = kv_heads;
    var head_dim: u32 = spec.head_dim;
    var query_position_offset = spec.prefix;
    var kv_position_offset: u32 = 0;
    var sliding_window = spec.sliding_window;
    var total_sequence_len = try toU32(spec.kvLen());
    var mask_len: u32 = 0;
    var bias_mode: u32 = 0;
    var key_row_bytes = try toU32(try checkedMul(spec.head_dim, @sizeOf(f16)));
    var base_key_row_bytes = key_row_bytes;
    var value_row_bytes = key_row_bytes;
    var block_count = block_count_value;
    var page_size_tokens: u32 = page_size;
    var format: u32 = 2;
    var value_format: u32 = 2;
    var physical_token_capacity = physical_capacity;
    var decode_scalars_ptr: cuda_driver.CUdeviceptr = 0;
    var params: [production_abi_arg_count]?*anyopaque = .{
        @ptrCast(&dst_ptr),                 @ptrCast(&q_ptr),
        @ptrCast(&k_ptr),                   @ptrCast(&v_ptr),
        @ptrCast(&table_ptr),               @ptrCast(&mask_ptr),
        @ptrCast(&bias_ptr),                @ptrCast(&batch),
        @ptrCast(&q_seq_len),               @ptrCast(&kv_seq_len),
        @ptrCast(&num_heads),               @ptrCast(&num_kv_heads),
        @ptrCast(&head_dim),                @ptrCast(&query_position_offset),
        @ptrCast(&kv_position_offset),      @ptrCast(&sliding_window),
        @ptrCast(&total_sequence_len),      @ptrCast(&mask_len),
        @ptrCast(&bias_mode),               @ptrCast(&key_row_bytes),
        @ptrCast(&base_key_row_bytes),      @ptrCast(&value_row_bytes),
        @ptrCast(&block_count),             @ptrCast(&page_size_tokens),
        @ptrCast(&format),                  @ptrCast(&value_format),
        @ptrCast(&physical_token_capacity), @ptrCast(&decode_scalars_ptr),
    };
    try ctx.makeCurrent();
    const tiled = kind != .scalar_reference;
    const tile_rows: usize = switch (kind) {
        .candidate => cfg.candidate_query_tile,
        .canonical_warp_baseline, .canonical_flash_baseline => canonical_query_tile,
        .scalar_reference => 1,
    };
    const grid_x: u32 = switch (kind) {
        .candidate => @intCast(heads / cfg.candidate_head_group),
        .canonical_warp_baseline, .canonical_flash_baseline, .scalar_reference => heads,
    };
    const grid_y: u32 = if (tiled) try toU32(ceilDiv(spec.q_len, tile_rows)) else spec.q_len;
    const block_x: u32 = if (tiled) threads else spec.head_dim;
    const dynamic_shared: u32 = switch (kind) {
        .candidate => try candidateSharedBytes(cfg, spec.head_dim),
        .canonical_flash_baseline => try baselineSharedBytes(spec.head_dim, .flash),
        .canonical_warp_baseline, .scalar_reference => 0,
    };
    if (dynamic_shared > 48 * 1024) {
        const set_attribute = ctx.driver.fns.cuFuncSetAttribute orelse return error.CudaDynamicSharedMemoryOptInUnavailable;
        try ctx.driver.check(set_attribute(function, 8, @intCast(dynamic_shared)));
    }
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
        function,
        grid_x,
        grid_y,
        1,
        block_x,
        1,
        1,
        dynamic_shared,
        ctx.stream,
        &params,
        null,
    ));
    ctx.noteKernelLaunch();
}

fn timeLaunches(
    ctx: *cuda_context.CudaContext,
    iterations: usize,
    function: cuda_driver.CUfunction,
    kind: LaunchKind,
    cfg: Config,
    spec: CaseSpec,
    buffers: LaunchBuffers,
    block_count_value: u32,
    physical_capacity: u32,
) !f64 {
    const pair = try ctx.beginProfileEventPair();
    for (0..iterations) |_| {
        try launchAttention(ctx, function, kind, cfg, spec, buffers, block_count_value, physical_capacity);
    }
    const total_us = try ctx.endProfileEventPairUs(pair);
    return @as(f64, @floatFromInt(total_us)) / @as(f64, @floatFromInt(iterations));
}

fn runTimings(
    ctx: *cuda_context.CudaContext,
    cfg: Config,
    candidate_functions: Functions,
    baseline_functions: BaselineFunctions,
    spec: CaseSpec,
    baseline_buffers: LaunchBuffers,
    candidate_buffers: LaunchBuffers,
    block_count_value: u32,
    physical_capacity: u32,
) !TimingStats {
    if (cfg.iterations == 0) return .{};
    const baseline_function = baseline_functions.selected(cfg.baseline_route, spec.head_dim);
    const baseline_kind: LaunchKind = switch (cfg.baseline_route) {
        .flash, .prototype_flash => .canonical_flash_baseline,
        .warp => .canonical_warp_baseline,
    };
    var stats = TimingStats{ .pairs = cfg.timing_pairs, .iterations = cfg.iterations };
    for (0..cfg.timing_pairs) |pair_index| {
        var baseline_us: f64 = 0;
        var candidate_us: f64 = 0;
        if ((pair_index & 1) == 0) {
            baseline_us = try timeLaunches(ctx, cfg.iterations, baseline_function, baseline_kind, cfg, spec, baseline_buffers, block_count_value, physical_capacity);
            candidate_us = try timeLaunches(ctx, cfg.iterations, candidate_functions.candidate(spec.head_dim), .candidate, cfg, spec, candidate_buffers, block_count_value, physical_capacity);
        } else {
            candidate_us = try timeLaunches(ctx, cfg.iterations, candidate_functions.candidate(spec.head_dim), .candidate, cfg, spec, candidate_buffers, block_count_value, physical_capacity);
            baseline_us = try timeLaunches(ctx, cfg.iterations, baseline_function, baseline_kind, cfg, spec, baseline_buffers, block_count_value, physical_capacity);
        }
        stats.baseline_sum_us += baseline_us;
        stats.candidate_sum_us += candidate_us;
        stats.baseline_sum_squared += baseline_us * baseline_us;
        stats.candidate_sum_squared += candidate_us * candidate_us;
    }
    return stats;
}

fn runCase(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    cfg: Config,
    candidate_functions: Functions,
    baseline_functions: BaselineFunctions,
    spec: CaseSpec,
) !CaseResult {
    const head_dim: usize = spec.head_dim;
    const q_count = try checkedMul(try checkedMul(spec.q_len, heads), head_dim);
    const kv_count = try checkedMul(spec.kvLen(), head_dim);
    const mapped_blocks = ceilDiv(spec.kvLen(), page_size);
    const physical_capacity = try checkedMul(mapped_blocks, page_size);
    const physical_count = try checkedMul(physical_capacity, head_dim);
    const output_bytes = try checkedMul(q_count, @sizeOf(f32));
    const output_image_bytes = try checkedAdd(output_bytes, 2 * guard_bytes);

    const host_q = try allocator.alloc(f32, q_count);
    defer allocator.free(host_q);
    const logical_k = try allocator.alloc(f16, kv_count);
    defer allocator.free(logical_k);
    const logical_v = try allocator.alloc(f16, kv_count);
    defer allocator.free(logical_v);
    fillInputs(host_q, logical_k, logical_v, spec);

    const table_entries = if (spec.layout == .identity) 0 else mapped_blocks;
    const block_table = try allocator.alloc(u32, table_entries);
    defer allocator.free(block_table);
    fillBlockTable(block_table, spec.layout);
    const host_k = try allocator.alloc(f16, physical_count);
    defer allocator.free(host_k);
    const host_v = try allocator.alloc(f16, physical_count);
    defer allocator.free(host_v);
    try packPaged(host_k, logical_k, block_table, head_dim);
    try packPaged(host_v, logical_v, block_table, head_dim);

    const reference_image = try allocator.alloc(u8, output_image_bytes);
    defer allocator.free(reference_image);
    const baseline_image = try allocator.alloc(u8, output_image_bytes);
    defer allocator.free(baseline_image);
    const candidate_image = try allocator.alloc(u8, output_image_bytes);
    defer allocator.free(candidate_image);
    try fillOutputImage(reference_image, output_bytes);
    try fillOutputImage(baseline_image, output_bytes);
    try fillOutputImage(candidate_image, output_bytes);
    const reference_values = try allocator.alloc(f32, q_count);
    defer allocator.free(reference_values);
    const baseline_values = try allocator.alloc(f32, q_count);
    defer allocator.free(baseline_values);
    const candidate_values = try allocator.alloc(f32, q_count);
    defer allocator.free(candidate_values);
    const repeat_values = try allocator.alloc(f32, q_count);
    defer allocator.free(repeat_values);

    var d_q = try cuda_buffer.DeviceBuffer.alloc(ctx, std.mem.sliceAsBytes(host_q).len);
    defer d_q.free(ctx);
    var d_k = try cuda_buffer.DeviceBuffer.alloc(ctx, std.mem.sliceAsBytes(host_k).len);
    defer d_k.free(ctx);
    var d_v = try cuda_buffer.DeviceBuffer.alloc(ctx, std.mem.sliceAsBytes(host_v).len);
    defer d_v.free(ctx);
    var d_table = try cuda_buffer.DeviceBuffer.alloc(ctx, std.mem.sliceAsBytes(block_table).len);
    defer d_table.free(ctx);
    var d_reference_output = try cuda_buffer.DeviceBuffer.alloc(ctx, output_image_bytes);
    defer d_reference_output.free(ctx);
    var d_baseline_output = try cuda_buffer.DeviceBuffer.alloc(ctx, output_image_bytes);
    defer d_baseline_output.free(ctx);
    var d_candidate_output = try cuda_buffer.DeviceBuffer.alloc(ctx, output_image_bytes);
    defer d_candidate_output.free(ctx);

    try d_q.copyFromHost(ctx, std.mem.sliceAsBytes(host_q));
    try d_k.copyFromHost(ctx, std.mem.sliceAsBytes(host_k));
    try d_v.copyFromHost(ctx, std.mem.sliceAsBytes(host_v));
    if (block_table.len != 0) try d_table.copyFromHost(ctx, std.mem.sliceAsBytes(block_table));
    try d_reference_output.copyFromHost(ctx, reference_image);
    try d_baseline_output.copyFromHost(ctx, baseline_image);
    try d_candidate_output.copyFromHost(ctx, candidate_image);

    const reference_buffers = LaunchBuffers{
        .output = d_reference_output,
        .q = d_q,
        .k = d_k,
        .v = d_v,
        .block_table = d_table,
    };
    const baseline_buffers = LaunchBuffers{
        .output = d_baseline_output,
        .q = d_q,
        .k = d_k,
        .v = d_v,
        .block_table = d_table,
    };
    const candidate_buffers = LaunchBuffers{
        .output = d_candidate_output,
        .q = d_q,
        .k = d_k,
        .v = d_v,
        .block_table = d_table,
    };
    const block_count_value = try toU32(block_table.len);
    const physical_capacity_u32 = try toU32(physical_capacity);
    const baseline_function = baseline_functions.selected(cfg.baseline_route, spec.head_dim);
    const baseline_kind: LaunchKind = switch (cfg.baseline_route) {
        .flash, .prototype_flash => .canonical_flash_baseline,
        .warp => .canonical_warp_baseline,
    };
    try launchAttention(ctx, candidate_functions.reference(spec.head_dim), .scalar_reference, cfg, spec, reference_buffers, block_count_value, physical_capacity_u32);
    try launchAttention(ctx, baseline_function, baseline_kind, cfg, spec, baseline_buffers, block_count_value, physical_capacity_u32);
    try launchAttention(ctx, candidate_functions.candidate(spec.head_dim), .candidate, cfg, spec, candidate_buffers, block_count_value, physical_capacity_u32);
    try d_reference_output.copyToHost(ctx, reference_image);
    try d_baseline_output.copyToHost(ctx, baseline_image);
    try d_candidate_output.copyToHost(ctx, candidate_image);
    try ctx.synchronize();
    try copyOutputValues(reference_values, reference_image);
    try copyOutputValues(baseline_values, baseline_image);
    try copyOutputValues(candidate_values, candidate_image);

    var candidate_determinism_mismatches: usize = 0;
    var baseline_determinism_mismatches: usize = 0;
    for (1..cfg.repeats) |_| {
        try launchAttention(ctx, candidate_functions.candidate(spec.head_dim), .candidate, cfg, spec, candidate_buffers, block_count_value, physical_capacity_u32);
        try d_candidate_output.copyToHost(ctx, candidate_image);
        try ctx.synchronize();
        try copyOutputValues(repeat_values, candidate_image);
        candidate_determinism_mismatches += try countBitwiseMismatches(candidate_values, repeat_values);

        try launchAttention(ctx, baseline_function, baseline_kind, cfg, spec, baseline_buffers, block_count_value, physical_capacity_u32);
        try d_baseline_output.copyToHost(ctx, baseline_image);
        try ctx.synchronize();
        try copyOutputValues(repeat_values, baseline_image);
        baseline_determinism_mismatches += try countBitwiseMismatches(baseline_values, repeat_values);
    }

    const timing = try runTimings(
        ctx,
        cfg,
        candidate_functions,
        baseline_functions,
        spec,
        baseline_buffers,
        candidate_buffers,
        block_count_value,
        physical_capacity_u32,
    );

    const q_after = try allocator.alloc(u8, std.mem.sliceAsBytes(host_q).len);
    defer allocator.free(q_after);
    const k_after = try allocator.alloc(u8, std.mem.sliceAsBytes(host_k).len);
    defer allocator.free(k_after);
    const v_after = try allocator.alloc(u8, std.mem.sliceAsBytes(host_v).len);
    defer allocator.free(v_after);
    const table_after = try allocator.alloc(u8, std.mem.sliceAsBytes(block_table).len);
    defer allocator.free(table_after);
    try d_q.copyToHost(ctx, q_after);
    try d_k.copyToHost(ctx, k_after);
    try d_v.copyToHost(ctx, v_after);
    if (block_table.len != 0) try d_table.copyToHost(ctx, table_after);
    // Re-read timed destinations so guard auditing covers every measured
    // launch, not just the initial differential launch.
    try d_baseline_output.copyToHost(ctx, baseline_image);
    try d_candidate_output.copyToHost(ctx, candidate_image);
    try ctx.synchronize();
    var readonly_mismatches = try countByteMismatches(std.mem.sliceAsBytes(host_q), q_after);
    readonly_mismatches += try countByteMismatches(std.mem.sliceAsBytes(host_k), k_after);
    readonly_mismatches += try countByteMismatches(std.mem.sliceAsBytes(host_v), v_after);
    if (block_table.len != 0) {
        readonly_mismatches += try countByteMismatches(std.mem.sliceAsBytes(block_table), table_after);
    }

    const cpu_diff = cpuSampleDiff(reference_values, host_q, logical_k, logical_v, spec);
    const candidate_baseline_diff = try compareOutputs(baseline_values, candidate_values);
    const reference_baseline_diff = try compareOutputs(reference_values, baseline_values);
    return .{
        .spec = spec,
        .candidate_baseline_diff = candidate_baseline_diff,
        .reference_baseline_diff = reference_baseline_diff,
        .cpu_sample_max_abs = cpu_diff.max_abs,
        .cpu_sample_non_finite = cpu_diff.non_finite,
        .candidate_unwritten = countUnwritten(candidate_values),
        .baseline_unwritten = countUnwritten(baseline_values),
        .reference_unwritten = countUnwritten(reference_values),
        .candidate_canary_mismatches = countCanaryMismatches(candidate_image, output_bytes),
        .baseline_canary_mismatches = countCanaryMismatches(baseline_image, output_bytes),
        .reference_canary_mismatches = countCanaryMismatches(reference_image, output_bytes),
        .readonly_mismatches = readonly_mismatches,
        .candidate_determinism_mismatches = candidate_determinism_mismatches,
        .baseline_determinism_mismatches = baseline_determinism_mismatches,
        .timing = timing,
    };
}

fn writeResult(writer: *std.Io.Writer, result: CaseResult, cfg: Config) !void {
    const spec = result.spec;
    try writer.print(
        "head_dim={d} policy={s} q_len={d} prefix={d} kv_len={d} layout={s} pattern={s} status={s}\n",
        .{
            spec.head_dim,
            if (spec.sliding_window == 0) "global" else "swa512",
            spec.q_len,
            spec.prefix,
            spec.kvLen(),
            spec.layout.label(),
            spec.pattern.label(),
            if (result.passes(cfg)) "pass" else "fail",
        },
    );
    try writer.print(
        "  route: abi=production-28 q=f32 kv=f16 page_size=16 baseline={s} candidate_q_tile={d} candidate_k_tile={d} candidate_head_group={d} candidate_dynamic_shared_bytes={d}\n",
        .{ cfg.baseline_route.label(), cfg.candidate_query_tile, cfg.candidate_key_tile, cfg.candidate_head_group, try candidateSharedBytes(cfg, spec.head_dim) },
    );
    try writer.print(
        "  candidate_vs_canonical: elements={d} bitwise_mismatches={d} nonfinite={d} max_abs={e:.9} max_abs_gate={e:.9} rms={e:.9} normalized_rms={e:.9} normalized_rms_gate={e:.9}\n",
        .{
            result.candidate_baseline_diff.element_count,
            result.candidate_baseline_diff.bitwise_mismatches,
            result.candidate_baseline_diff.non_finite,
            result.candidate_baseline_diff.max_abs,
            cfg.max_abs,
            result.candidate_baseline_diff.rms_error,
            result.candidate_baseline_diff.rms_normalized,
            cfg.max_rms_normalized,
        },
    );
    try writer.print(
        "  scalar_reference_vs_canonical: elements={d} bitwise_mismatches={d} nonfinite={d} max_abs={e:.9} rms={e:.9} normalized_rms={e:.9}\n",
        .{
            result.reference_baseline_diff.element_count,
            result.reference_baseline_diff.bitwise_mismatches,
            result.reference_baseline_diff.non_finite,
            result.reference_baseline_diff.max_abs,
            result.reference_baseline_diff.rms_error,
            result.reference_baseline_diff.rms_normalized,
        },
    );
    try writer.print(
        "  cpu_oracle: sampled_rows=2 sampled_heads=2 sampled_columns=3 max_abs={e:.9} gate={e:.9} nonfinite={d}\n",
        .{ result.cpu_sample_max_abs, cfg.cpu_sample_max_abs, result.cpu_sample_non_finite },
    );
    try writer.print(
        "  integrity: candidate_unwritten={d} canonical_unwritten={d} reference_unwritten={d} candidate_canary={d} canonical_canary={d} reference_canary={d} readonly_mismatches={d} candidate_determinism={d} canonical_determinism={d}\n",
        .{
            result.candidate_unwritten,
            result.baseline_unwritten,
            result.reference_unwritten,
            result.candidate_canary_mismatches,
            result.baseline_canary_mismatches,
            result.reference_canary_mismatches,
            result.readonly_mismatches,
            result.candidate_determinism_mismatches,
            result.baseline_determinism_mismatches,
        },
    );
    if (result.timing.pairs != 0) {
        const baseline_us = result.timing.baselineMean();
        const candidate_us = result.timing.candidateMean();
        try writer.print(
            "  timing: baseline={s} order=alternating-AB-BA pairs={d} iterations={d} baseline_us={d:.3} candidate_us={d:.3} speedup={d:.4}x baseline_cv={d:.5} candidate_cv={d:.5} max_cv={d:.5}\n",
            .{
                cfg.baseline_route.label(),
                result.timing.pairs,
                result.timing.iterations,
                baseline_us,
                candidate_us,
                if (candidate_us > 0) baseline_us / candidate_us else 0,
                result.timing.baselineCv(),
                result.timing.candidateCv(),
                cfg.max_timing_cv,
            },
        );
    }
}

fn writeResultsJson(
    writer: *std.Io.Writer,
    results: []const CaseResult,
    cfg: Config,
    candidate_artifact_sha: []const u8,
    baseline_artifact_sha: []const u8,
    compute_major: i32,
    compute_minor: i32,
) !void {
    var pass = true;
    for (results) |result| pass = pass and result.passes(cfg);
    try writer.print(
        "{{\"schema\":\"antfly.cuda_flash_prefill_prototype.v4\",\"runtime_integrated\":false,\"architecture\":\"sm_89\",\"compute_capability\":\"{d}.{d}\",\"artifacts\":{{\"candidate_sha256\":\"{s}\",\"baseline_sha256\":\"{s}\"}},\"candidate\":\"wmma-f16-kv-f32-q\",\"baseline\":\"{s}\",\"candidate_query_tile\":{d},\"candidate_key_tile\":{d},\"candidate_head_group\":{d},\"scalar_reference\":\"standalone-f16-kv-f32-q\",\"pass\":{s},",
        .{ compute_major, compute_minor, candidate_artifact_sha, baseline_artifact_sha, cfg.baseline_route.label(), cfg.candidate_query_tile, cfg.candidate_key_tile, cfg.candidate_head_group, if (pass) "true" else "false" },
    );
    try writer.print(
        "\"gates\":{{\"require_bitwise_candidate\":{s},\"max_abs\":{e:.12},\"max_rms_normalized\":{e:.12},\"cpu_sample_max_abs\":{e:.12},\"max_timing_cv\":{e:.12}}},\"results\":[",
        .{ if (cfg.require_bitwise_candidate) "true" else "false", cfg.max_abs, cfg.max_rms_normalized, cfg.cpu_sample_max_abs, cfg.max_timing_cv },
    );
    for (results, 0..) |result, index| {
        if (index != 0) try writer.writeByte(',');
        const spec = result.spec;
        const baseline_us = result.timing.baselineMean();
        const candidate_us = result.timing.candidateMean();
        try writer.print(
            "{{\"head_dim\":{d},\"policy\":\"{s}\",\"q_len\":{d},\"prefix\":{d},\"kv_len\":{d},\"layout\":\"{s}\",\"pattern\":\"{s}\",\"seed\":{d},\"pass\":{s},",
            .{
                spec.head_dim,
                if (spec.sliding_window == 0) "global" else "swa512",
                spec.q_len,
                spec.prefix,
                spec.kvLen(),
                spec.layout.label(),
                spec.pattern.label(),
                spec.seed,
                if (result.passes(cfg)) "true" else "false",
            },
        );
        try writer.print(
            "\"route\":{{\"production_abi_args\":28,\"q_format\":\"f32\",\"kv_format\":\"f16\",\"page_size_tokens\":16,\"baseline\":\"{s}\",\"candidate_query_tile\":{d},\"candidate_key_tile\":{d},\"candidate_head_group\":{d},\"candidate_dynamic_shared_bytes\":{d}}},",
            .{ cfg.baseline_route.label(), cfg.candidate_query_tile, cfg.candidate_key_tile, cfg.candidate_head_group, try candidateSharedBytes(cfg, spec.head_dim) },
        );
        try writer.print(
            "\"candidate_vs_canonical\":{{\"elements\":{d},\"bitwise_mismatches\":{d},\"nonfinite\":{d},\"max_abs\":{e:.12},\"rms_error\":{e:.12},\"rms_normalized\":{e:.12}}},",
            .{
                result.candidate_baseline_diff.element_count,
                result.candidate_baseline_diff.bitwise_mismatches,
                result.candidate_baseline_diff.non_finite,
                result.candidate_baseline_diff.max_abs,
                result.candidate_baseline_diff.rms_error,
                result.candidate_baseline_diff.rms_normalized,
            },
        );
        try writer.print(
            "\"scalar_reference_vs_canonical\":{{\"elements\":{d},\"bitwise_mismatches\":{d},\"nonfinite\":{d},\"max_abs\":{e:.12},\"rms_error\":{e:.12},\"rms_normalized\":{e:.12}}},",
            .{
                result.reference_baseline_diff.element_count,
                result.reference_baseline_diff.bitwise_mismatches,
                result.reference_baseline_diff.non_finite,
                result.reference_baseline_diff.max_abs,
                result.reference_baseline_diff.rms_error,
                result.reference_baseline_diff.rms_normalized,
            },
        );
        try writer.print(
            "\"cpu_oracle\":{{\"samples\":12,\"max_abs\":{e:.12},\"nonfinite\":{d}}},",
            .{ result.cpu_sample_max_abs, result.cpu_sample_non_finite },
        );
        try writer.print(
            "\"integrity\":{{\"candidate_unwritten\":{d},\"canonical_unwritten\":{d},\"reference_unwritten\":{d},\"candidate_canary_mismatches\":{d},\"canonical_canary_mismatches\":{d},\"reference_canary_mismatches\":{d},\"readonly_mismatches\":{d},\"candidate_determinism_mismatches\":{d},\"canonical_determinism_mismatches\":{d}}},",
            .{
                result.candidate_unwritten,
                result.baseline_unwritten,
                result.reference_unwritten,
                result.candidate_canary_mismatches,
                result.baseline_canary_mismatches,
                result.reference_canary_mismatches,
                result.readonly_mismatches,
                result.candidate_determinism_mismatches,
                result.baseline_determinism_mismatches,
            },
        );
        try writer.print(
            "\"timing\":{{\"order\":\"alternating-AB-BA\",\"pairs\":{d},\"iterations\":{d},\"baseline_mean_us\":{d:.6},\"candidate_mean_us\":{d:.6},\"speedup\":{d:.6},\"baseline_cv\":{d:.9},\"candidate_cv\":{d:.9}}}}}",
            .{
                result.timing.pairs,
                result.timing.iterations,
                baseline_us,
                candidate_us,
                if (candidate_us > 0) baseline_us / candidate_us else 0,
                result.timing.baselineCv(),
                result.timing.candidateCv(),
            },
        );
    }
    try writer.writeAll("]}\n");
}

fn writeUsage(writer: *std.Io.Writer) !void {
    try writer.writeAll(
        \\Usage: antfly-cuda-flash-prefill-prototype --candidate-cubin PATH --baseline-cubin PATH [options]
        \\
        \\This is a raw, default-off qualification harness. It does not exercise runtime dispatch.
        \\
        \\Options:
        \\  --candidate-cubin PATH          Standalone WMMA prototype artifact
        \\  --baseline-cubin PATH           Checked-in canonical CUDA cubin
        \\  --head-dim all|256|512          Default: all
        \\  --q-len all|512|3               Default: all
        \\  --prefix all|0|512|1024|1536|2048
        \\                                  q512 defaults to 0/512/1024/1536; q3 to 2048
        \\  --baseline-route warp|flash|prototype-flash
        \\                                  Default: warp; prototype-flash compares two standalone v1 artifacts
        \\  --candidate-query-tile 16|32|64 Candidate launch/output rows per CTA
        \\  --candidate-key-tile 16|32|64   Candidate shared-memory key/value tile
        \\  --candidate-head-group 1|2|4    Query heads sharing each K/V tile
        \\  --candidate-gqa2-concurrent-layout
        \\                                  q16/k16/g2 shared layout (standalone v3)
        \\  --layout all|identity|explicit-reversed|explicit-permuted
        \\  --pattern all|random|near-tie|cancellation
        \\  --seed N                        Deterministic input seed
        \\  --repeats N                     Candidate determinism launches; default: 3
        \\  --iterations N                  Launches per timing sample; default: 0
        \\  --timing-pairs N                Alternating AB/BA pairs; default: 5
        \\  --max-abs X                     GPU differential gate; default: 5e-3
        \\  --max-rms-normalized X          GPU differential gate; default: 1e-3
        \\  --cpu-sample-max-abs X          Scalar-reference/CPU gate; default: 5e-4
        \\  --max-timing-cv X               Timing stability gate; default: 0.10
        \\  --require-bitwise-candidate     Require zero candidate/canonical bit mismatches
        \\  --json-out PATH                 Write versioned machine-readable evidence
        \\  -h, --help
        \\
    );
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

    const allocator = std.heap.c_allocator;
    const candidate_image = try readFileAllocAtPath(init.io, cfg.candidate_cubin_path.?, allocator);
    defer allocator.free(candidate_image);
    const baseline_image = try readFileAllocAtPath(init.io, cfg.baseline_cubin_path.?, allocator);
    defer allocator.free(baseline_image);
    const candidate_artifact_sha = artifactSha256Hex(candidate_image);
    const baseline_artifact_sha = artifactSha256Hex(baseline_image);
    const count = try caseCount(cfg);
    const specs = try allocator.alloc(CaseSpec, count);
    defer allocator.free(specs);
    try fillCaseSpecs(cfg, specs);
    const results = try allocator.alloc(CaseResult, count);
    defer allocator.free(results);

    var ctx = try cuda_context.CudaContext.initDefault();
    defer ctx.deinit();
    if (ctx.info.compute_major != 8 or ctx.info.compute_minor != 9) return error.UnsupportedCudaArchitecture;
    var candidate_module = try Module.load(&ctx, candidate_image);
    defer candidate_module.deinit(&ctx);
    var baseline_module = try Module.load(&ctx, baseline_image);
    defer baseline_module.deinit(&ctx);
    const candidate_functions = try candidate_module.functions(&ctx);
    const baseline_functions = try baseline_module.baselineFunctions(&ctx, cfg.baseline_route);
    for (specs, 0..) |spec, index| {
        results[index] = try runCase(allocator, &ctx, cfg, candidate_functions, baseline_functions, spec);
    }

    try stdout.print(
        "CUDA flash-prefill qualification: device={s} cc={d}.{d} candidate_sha256={s} baseline_sha256={s} cases={d} candidate=wmma-f16-kv-f32-q baseline={s} candidate_q_tile={d} candidate_k_tile={d} candidate_head_group={d} runtime_integrated=false\n",
        .{ ctx.info.nameSlice(), ctx.info.compute_major, ctx.info.compute_minor, &candidate_artifact_sha, &baseline_artifact_sha, results.len, cfg.baseline_route.label(), cfg.candidate_query_tile, cfg.candidate_key_tile, cfg.candidate_head_group },
    );
    var pass = true;
    for (results) |result| {
        try writeResult(stdout, result, cfg);
        pass = pass and result.passes(cfg);
    }
    if (cfg.json_out) |path| {
        var file = if (std.fs.path.isAbsolute(path))
            try std.Io.Dir.createFileAbsolute(init.io, path, .{ .truncate = true })
        else
            try std.Io.Dir.cwd().createFile(init.io, path, .{ .truncate = true });
        defer file.close(init.io);
        var file_buffer: [8192]u8 = undefined;
        var file_writer = file.writer(init.io, &file_buffer);
        try writeResultsJson(
            &file_writer.interface,
            results,
            cfg,
            &candidate_artifact_sha,
            &baseline_artifact_sha,
            ctx.info.compute_major,
            ctx.info.compute_minor,
        );
        try file_writer.interface.flush();
        try stdout.print("machine-readable evidence: {s}\n", .{path});
    }
    try stdout.flush();
    if (!pass) return error.FlashPrefillPrototypeDifferentialExceeded;
}

test "prototype config is fail-closed and covers only locked production shapes" {
    const cfg = try parseConfig(&.{
        "--candidate-cubin",                  "/tmp/prototype.cubin",
        "--baseline-cubin",                   "/tmp/canonical.cubin",
        "--head-dim",                         "512",
        "--q-len",                            "3",
        "--prefix",                           "2048",
        "--baseline-route",                   "flash",
        "--candidate-query-tile",             "16",
        "--candidate-key-tile",               "16",
        "--candidate-head-group",             "2",
        "--candidate-gqa2-concurrent-layout", "--layout",
        "explicit-permuted",                  "--pattern",
        "near-tie",                           "--repeats",
        "4",                                  "--iterations",
        "10",                                 "--timing-pairs",
        "7",                                  "--require-bitwise-candidate",
        "--json-out",                         "/tmp/prototype.json",
    });
    try std.testing.expectEqual(@as(?u16, 512), cfg.head_dim);
    try std.testing.expectEqual(@as(?u16, 3), cfg.q_len);
    try std.testing.expectEqual(@as(?u32, 2048), cfg.prefix);
    try std.testing.expectEqual(BaselineRoute.flash, cfg.baseline_route);
    try std.testing.expectEqual(@as(u16, 16), cfg.candidate_query_tile);
    try std.testing.expectEqual(@as(u16, 16), cfg.candidate_key_tile);
    try std.testing.expectEqual(@as(u8, 2), cfg.candidate_head_group);
    try std.testing.expect(cfg.candidate_gqa2_concurrent_layout);
    try std.testing.expectEqual(LayoutSelection.explicit_permuted, cfg.layout);
    try std.testing.expectEqual(PatternSelection.near_tie, cfg.pattern);
    try std.testing.expectEqual(@as(usize, 4), cfg.repeats);
    try std.testing.expectEqual(@as(usize, 10), cfg.iterations);
    try std.testing.expectEqual(@as(usize, 7), cfg.timing_pairs);
    try std.testing.expect(cfg.require_bitwise_candidate);
    try std.testing.expectEqualStrings("/tmp/prototype.json", cfg.json_out.?);
    try std.testing.expectEqual(BaselineRoute.prototype_flash, try parseBaselineRoute("prototype-flash"));
    try std.testing.expectEqualStrings("standalone-flash-v1", BaselineRoute.prototype_flash.label());

    try std.testing.expectError(error.MissingCandidateCubin, parseConfig(&.{}));
    try std.testing.expectError(error.MissingBaselineCubin, parseConfig(&.{ "--candidate-cubin", "x" }));
    try std.testing.expectError(error.InvalidHeadDim, parseConfig(&.{ "--candidate-cubin", "x", "--baseline-cubin", "y", "--head-dim", "128" }));
    try std.testing.expectError(error.InvalidQueryLength, parseConfig(&.{ "--candidate-cubin", "x", "--baseline-cubin", "y", "--q-len", "16" }));
    try std.testing.expectError(error.AmbiguousPrefix, parseConfig(&.{ "--candidate-cubin", "x", "--baseline-cubin", "y", "--prefix", "2048" }));
    try std.testing.expectError(error.InvalidPrefix, parseConfig(&.{ "--candidate-cubin", "x", "--baseline-cubin", "y", "--q-len", "3", "--prefix", "1536" }));
    try std.testing.expectError(error.InvalidLayout, parseConfig(&.{ "--candidate-cubin", "x", "--baseline-cubin", "y", "--layout", "fixed" }));
    try std.testing.expectError(error.InvalidBaselineRoute, parseConfig(&.{ "--candidate-cubin", "x", "--baseline-cubin", "y", "--baseline-route", "scalar" }));
    try std.testing.expectError(error.InvalidTile, parseConfig(&.{ "--candidate-cubin", "x", "--baseline-cubin", "y", "--candidate-query-tile", "48" }));
    try std.testing.expectError(error.InvalidHeadGroup, parseConfig(&.{ "--candidate-cubin", "x", "--baseline-cubin", "y", "--candidate-head-group", "8" }));
    try std.testing.expectError(error.InvalidGqa2ConcurrentLayout, parseConfig(&.{ "--candidate-cubin", "x", "--baseline-cubin", "y", "--candidate-head-group", "2", "--candidate-query-tile", "32", "--candidate-gqa2-concurrent-layout" }));
    try std.testing.expectError(error.InvalidRepeats, parseConfig(&.{ "--candidate-cubin", "x", "--baseline-cubin", "y", "--repeats", "1" }));
}

test "default matrix covers production prefixes, policies, layouts, and stress patterns" {
    var cfg = Config{ .candidate_cubin_path = "unused", .baseline_cubin_path = "unused" };
    try std.testing.expectEqual(@as(usize, 90), try caseCount(cfg));
    var specs: [90]CaseSpec = undefined;
    try fillCaseSpecs(cfg, &specs);
    var hd256_q512: usize = 0;
    var hd256_q3: usize = 0;
    var hd512_q512: usize = 0;
    var hd512_q3: usize = 0;
    var q512_prefix_counts = [_]usize{0} ** 4;
    for (specs) |spec| {
        if (spec.head_dim == 256 and spec.q_len == 512) hd256_q512 += 1;
        if (spec.head_dim == 256 and spec.q_len == 3) hd256_q3 += 1;
        if (spec.head_dim == 512 and spec.q_len == 512) hd512_q512 += 1;
        if (spec.head_dim == 512 and spec.q_len == 3) hd512_q3 += 1;
        try std.testing.expectEqual(@as(u32, if (spec.head_dim == 256) 512 else 0), spec.sliding_window);
        if (spec.q_len == 512) {
            const prefix_index: usize = switch (spec.prefix) {
                0 => 0,
                512 => 1,
                1024 => 2,
                1536 => 3,
                else => return error.TestUnexpectedResult,
            };
            q512_prefix_counts[prefix_index] += 1;
        } else {
            try std.testing.expectEqual(@as(u32, 2048), spec.prefix);
        }
    }
    try std.testing.expectEqual(@as(usize, 36), hd256_q512);
    try std.testing.expectEqual(@as(usize, 9), hd256_q3);
    try std.testing.expectEqual(@as(usize, 36), hd512_q512);
    try std.testing.expectEqual(@as(usize, 9), hd512_q3);
    try std.testing.expectEqualSlices(usize, &.{ 18, 18, 18, 18 }, &q512_prefix_counts);
    cfg.pattern = .random;
    try std.testing.expectEqual(@as(usize, 30), try caseCount(cfg));
}

test "shared-memory ABI mirrors the CUDA candidate layout" {
    try std.testing.expectEqual(@as(u32, 26_564), try flashSharedBytes(256, 16, 16, 1));
    try std.testing.expectEqual(@as(u32, 42_948), try flashSharedBytes(512, 16, 16, 1));
    try std.testing.expectEqual(@as(u32, 45_188), try flashSharedBytes(256, 32, 32, 1));
    try std.testing.expectEqual(@as(u32, 77_956), try flashSharedBytes(512, 32, 32, 1));
    try std.testing.expectEqual(@as(u32, 53_828), try flashSharedBytes(256, 64, 16, 1));
    try std.testing.expectEqual(@as(u32, 94_788), try flashSharedBytes(512, 64, 16, 1));
    try std.testing.expectEqual(@as(u32, 53_252), try flashSharedBytes(256, 16, 64, 1));
    try std.testing.expectEqual(@as(u32, 94_212), try flashSharedBytes(512, 16, 64, 1));
    try std.testing.expectEqual(@as(u32, 151_812), try flashSharedBytes(512, 64, 64, 1));
    try std.testing.expectEqual(@as(u32, 35_524), try flashSharedBytes(256, 16, 16, 2));
    try std.testing.expectEqual(@as(u32, 60_100), try flashSharedBytes(512, 16, 16, 2));
    try std.testing.expectEqual(@as(u32, 53_444), try flashSharedBytes(256, 16, 16, 4));
    try std.testing.expectEqual(@as(u32, 94_404), try flashSharedBytes(512, 16, 16, 4));
    const gqa2_cfg = Config{
        .candidate_query_tile = 16,
        .candidate_key_tile = 16,
        .candidate_head_group = 2,
        .candidate_gqa2_concurrent_layout = true,
    };
    try std.testing.expectEqual(@as(u32, 28_356), try candidateSharedBytes(gqa2_cfg, 256));
    try std.testing.expectEqual(@as(u32, 44_740), try candidateSharedBytes(gqa2_cfg, 512));
    try std.testing.expect((try candidateSharedBytes(gqa2_cfg, 512)) <= 50_688);
    try std.testing.expect((try flashSharedBytes(512, 32, 32, 1)) < 100 * 1024);
    try std.testing.expectError(error.InvalidHeadDim, flashSharedBytes(128, 16, 16, 1));
    try std.testing.expectError(error.InvalidTile, flashSharedBytes(256, 48, 16, 1));
    try std.testing.expectError(error.InvalidHeadGroup, flashSharedBytes(256, 16, 16, 8));
}

test "causal and SWA ranges cover production chunk boundaries" {
    try std.testing.expectEqual(VisibleRange{ .begin = 1025, .end = 1537 }, visibleRange(1536, 2048, 0, 512));
    try std.testing.expectEqual(VisibleRange{ .begin = 1536, .end = 2048 }, visibleRange(2047, 2048, 0, 512));
    try std.testing.expectEqual(VisibleRange{ .begin = 0, .end = 1537 }, visibleRange(1536, 2048, 0, 0));
    try std.testing.expectEqual(VisibleRange{ .begin = 1537, .end = 2049 }, visibleRange(2048, 2051, 0, 512));
    try std.testing.expectEqual(VisibleRange{ .begin = 0, .end = 2051 }, visibleRange(2050, 2051, 0, 0));
}

test "identity, reversed, and permuted page packing preserve logical rows and poison slack" {
    const logical = [_]f16{ 1, 2, 3, 4, 5, 6, 7, 8 };
    var identity_dst = [_]f16{0} ** 12;
    try packPaged(&identity_dst, &logical, &.{}, 2);
    try std.testing.expectEqualSlices(f16, &logical, identity_dst[0..logical.len]);
    try std.testing.expectEqual(f16_poison_bits, @as(u16, @bitCast(identity_dst[logical.len])));

    var table = [_]u32{ 0, 0 };
    fillBlockTable(&table, .explicit_reversed);
    try std.testing.expectEqualSlices(u32, &.{ 1, 0 }, &table);
    var paged_dst = [_]f16{0} ** (page_size * 2 * 2);
    try packPaged(&paged_dst, &logical, &table, 2);
    for (0..4) |token| {
        const physical = try physicalToken(token, &table);
        try std.testing.expectEqualSlices(
            f16,
            logical[token * 2 .. token * 2 + 2],
            paged_dst[physical * 2 .. physical * 2 + 2],
        );
    }

    var permuted_table = [_]u32{ 0, 0, 0, 0 };
    fillBlockTable(&permuted_table, .explicit_permuted);
    try std.testing.expectEqualSlices(u32, &.{ 1, 2, 3, 0 }, &permuted_table);
    var seen = [_]bool{false} ** permuted_table.len;
    for (permuted_table) |physical_page| {
        try std.testing.expect(physical_page < seen.len);
        try std.testing.expect(!seen[physical_page]);
        seen[physical_page] = true;
    }
}

test "CUDA sources contain real WMMA kernels and no production fallback call" {
    const wrapper = @embedFile("ops/cuda/prototypes/gqa_flash_prefill_f16_sm89.cu");
    const v2 = @embedFile("ops/cuda/prototypes/gqa_flash_prefill_v2_sm89.cu");
    const v3 = @embedFile("ops/cuda/prototypes/gqa_flash_prefill_v3_gqa2_sm89.cu");
    const template = @embedFile("graph/templates/cuda_gqa_flash_prefill_f16_sm89.cuh");
    try std.testing.expect(std.mem.indexOf(u8, wrapper, flash_hd256) != null);
    try std.testing.expect(std.mem.indexOf(u8, wrapper, flash_hd512) != null);
    try std.testing.expect(std.mem.indexOf(u8, wrapper, reference_hd256) != null);
    try std.testing.expect(std.mem.indexOf(u8, wrapper, reference_hd512) != null);
    try std.testing.expect(std.mem.count(u8, template, "wmma::mma_sync") >= 2);
    try std.testing.expect(std.mem.count(u8, v2, "wmma::mma_sync") >= 2);
    try std.testing.expect(std.mem.count(u8, v3, "wmma::mma_sync") >= 3);
    try std.testing.expect(std.mem.indexOf(u8, v2, "ANTFLY_FLASH_V2_HEAD_GROUP") != null);
    try std.testing.expect(std.mem.indexOf(u8, v2, "first_head = blockIdx.x * kHeadGroup") != null);
    try std.testing.expect(std.mem.indexOf(u8, v3, "kHeadsPerCta = 2u") != null);
    try std.testing.expect(std.mem.indexOf(u8, v3, "kWarpsPerHead = 4u") != null);
    try std.testing.expect(std.mem.indexOf(u8, template, "host-owned") != null);
    try std.testing.expect(std.mem.indexOf(u8, template, "termite_gqa_attention_prefill_turboquant_fast_f32") == null);
    try std.testing.expect(std.mem.indexOf(u8, v2, "termite_gqa_attention_prefill_turboquant_fast_f32") == null);
    try std.testing.expect(std.mem.indexOf(u8, v3, "termite_gqa_attention_prefill_turboquant_fast_f32") == null);
}

test "prototype entry points expose the production F32-Q ABI without status arguments" {
    const source = @embedFile("graph/templates/cuda_gqa_flash_prefill_f16_sm89.cuh");
    try std.testing.expect(std.mem.indexOf(u8, source, "const float* q") != null);
    try std.testing.expect(std.mem.indexOf(u8, source, "unsigned total_sequence_len") != null);
    try std.testing.expect(std.mem.indexOf(u8, source, "unsigned value_format") != null);
    try std.testing.expect(std.mem.indexOf(u8, source, "const unsigned* decode_scalars") != null);
    try std.testing.expect(std.mem.indexOf(u8, source, "unsigned* status") == null);
}

test "machine-readable evidence is valid versioned JSON" {
    const cfg = Config{ .candidate_cubin_path = "unused", .baseline_cubin_path = "unused" };
    const result = CaseResult{
        .spec = .{
            .head_dim = 256,
            .q_len = 3,
            .prefix = 2048,
            .sliding_window = 512,
            .layout = .identity,
            .pattern = .random,
            .seed = 42,
        },
        .candidate_baseline_diff = .{},
        .reference_baseline_diff = .{},
        .cpu_sample_max_abs = 0,
        .cpu_sample_non_finite = 0,
        .candidate_unwritten = 0,
        .baseline_unwritten = 0,
        .reference_unwritten = 0,
        .candidate_canary_mismatches = 0,
        .baseline_canary_mismatches = 0,
        .reference_canary_mismatches = 0,
        .readonly_mismatches = 0,
        .candidate_determinism_mismatches = 0,
        .baseline_determinism_mismatches = 0,
        .timing = .{},
    };
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    try writeResultsJson(&output.writer, &.{result}, cfg, "prototype-sha", "canonical-sha", 8, 9);
    var parsed = try std.json.parseFromSlice(
        std.json.Value,
        std.testing.allocator,
        output.writer.buffered(),
        .{},
    );
    defer parsed.deinit();
    const root = parsed.value.object;
    try std.testing.expectEqualStrings(
        "antfly.cuda_flash_prefill_prototype.v4",
        root.get("schema").?.string,
    );
    try std.testing.expect(!root.get("runtime_integrated").?.bool);
    try std.testing.expect(root.get("pass").?.bool);
    try std.testing.expectEqual(@as(usize, 1), root.get("results").?.array.items.len);
}
