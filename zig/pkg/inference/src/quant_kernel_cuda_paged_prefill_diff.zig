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

//! Raw paged-KV CUDA prefill-attention differential and timing harness.
//!
//! This deliberately bypasses runtime routing. The embedded production
//! prefill kernel and both embedded F16-page candidates receive identical
//! buffers and launch arguments. The default qualification matrix is a
//! bounded, deterministic pairwise sweep that covers every supported axis;
//! `--matrix cartesian` is available for exhaustive investigations.

const std = @import("std");
const build_options = @import("build_options");
const cuda_artifact = @import("ops/cuda/artifact.zig");
const cuda_buffer = @import("ops/cuda/buffer.zig");
const cuda_context = @import("ops/cuda/context.zig");
const cuda_driver = @import("ops/cuda/driver.zig");

const baseline_kernel_name = "termite_gqa_attention_prefill_turboquant_fast_f32";
const exact_kernel_name = "termite_gqa_attention_prefill_tiled_f16_exact_f32";
const warp_kernel_name = "termite_gqa_attention_prefill_tiled_f16_warp_f32";

const num_heads: usize = 8;
const num_kv_heads: usize = 1;
const page_size_default: usize = 16;
const local_window_default: u32 = 128;
const tile_queries: usize = 16;
const guard_bytes: usize = 256;
const canary_byte: u8 = 0xa5;
const output_poison_bits: u32 = 0x7fc0_d1ff;
const f16_poison_bits: u16 = 0x7e00;
const max_timing_pairs: usize = 32;
// The warp route may change F32 accumulation order, but it must remain tightly
// bounded. Absolute error protects near-zero cases; normalized RMS protects the
// full output distribution. Exact promotion additionally uses --require-bitwise.
const default_max_abs: f64 = 5e-4;
const default_max_rms_normalized: f64 = 2e-4;
const rms_reference_floor: f64 = 1e-6;
const default_max_timing_cv: f64 = 0.10;

const supported_q_lengths = [_]usize{ 2, 15, 16, 17, 31, 32, 33, 128, 512 };
const supported_prefixes = [_]usize{ 0, 511, 512, 2003 };
const supported_page_sizes = [_]usize{ 1, 3, 16, 256 };
const device_audit_kinds = [_]DeviceAuditKind{
    .null_nonzero,
    .nonnull_zero,
    .zero_page_size,
    .undersized_capacity,
    .nonaligned_capacity,
    .too_few_blocks,
    .huge_mapped_entry,
};

const Candidate = enum {
    exact,
    warp,

    fn label(self: Candidate) []const u8 {
        return switch (self) {
            .exact => "exact",
            .warp => "warp",
        };
    }

    fn kernelName(self: Candidate) []const u8 {
        return switch (self) {
            .exact => exact_kernel_name,
            .warp => warp_kernel_name,
        };
    }

    fn threads(self: Candidate, head_dim: u16) u32 {
        return switch (self) {
            // The exact candidate preserves the baseline block-wide F32
            // reduction tree. The warp candidate assigns two queries/warp.
            .exact => head_dim,
            .warp => 256,
        };
    }
};

const CandidateSelection = enum {
    all,
    exact,
    warp,

    fn count(self: CandidateSelection) usize {
        return if (self == .all) 2 else 1;
    }

    fn at(self: CandidateSelection, index: usize) Candidate {
        return switch (self) {
            .all => if (index == 0) .exact else .warp,
            .exact => .exact,
            .warp => .warp,
        };
    }
};

const Pattern = enum {
    random,
    near_tie,
    cancellation,
    signed_zero,
    subnormal,

    fn label(self: Pattern) []const u8 {
        return switch (self) {
            .random => "random",
            .near_tie => "near-tie",
            .cancellation => "cancellation",
            .signed_zero => "signed-zero",
            .subnormal => "subnormal",
        };
    }
};

const PatternSelection = enum {
    all,
    random,
    near_tie,
    cancellation,
    signed_zero,
    subnormal,

    fn count(self: PatternSelection) usize {
        return if (self == .all) 5 else 1;
    }

    fn at(self: PatternSelection, index: usize) Pattern {
        return switch (self) {
            .all => switch (index) {
                0 => .random,
                1 => .near_tie,
                2 => .cancellation,
                3 => .signed_zero,
                4 => .subnormal,
                else => unreachable,
            },
            .random => .random,
            .near_tie => .near_tie,
            .cancellation => .cancellation,
            .signed_zero => .signed_zero,
            .subnormal => .subnormal,
        };
    }
};

const PageOrder = enum {
    identity,
    fixed,
    reversed,
    permuted,

    fn label(self: PageOrder) []const u8 {
        return switch (self) {
            .identity => "identity",
            .fixed => "fixed",
            .reversed => "reversed",
            .permuted => "permuted",
        };
    }

    fn usesExplicitBlockTable(self: PageOrder) bool {
        return self != .identity;
    }
};

const PageOrderSelection = enum {
    all,
    identity,
    fixed,
    reversed,
    permuted,

    fn count(self: PageOrderSelection) usize {
        return if (self == .all) 4 else 1;
    }

    fn at(self: PageOrderSelection, index: usize) PageOrder {
        return switch (self) {
            .all => switch (index) {
                0 => .identity,
                1 => .fixed,
                2 => .reversed,
                3 => .permuted,
                else => unreachable,
            },
            .identity => .identity,
            .fixed => .fixed,
            .reversed => .reversed,
            .permuted => .permuted,
        };
    }
};

const PageTableContract = struct {
    entry_count: usize,
    launch_block_count: usize,
    block_table_null: bool,
};

fn pageTableContract(order: PageOrder, logical_blocks: usize) PageTableContract {
    if (!order.usesExplicitBlockTable()) return .{
        .entry_count = 0,
        .launch_block_count = 0,
        .block_table_null = true,
    };
    return .{
        .entry_count = logical_blocks,
        .launch_block_count = logical_blocks,
        .block_table_null = false,
    };
}

const CapacityMode = enum {
    minimal,
    extra,

    fn label(self: CapacityMode) []const u8 {
        return switch (self) {
            .minimal => "minimal",
            .extra => "extra",
        };
    }
};

const CapacitySelection = enum {
    all,
    minimal,
    extra,

    fn count(self: CapacitySelection) usize {
        return if (self == .all) 2 else 1;
    }

    fn at(self: CapacitySelection, index: usize) CapacityMode {
        return switch (self) {
            .all => if (index == 0) .minimal else .extra,
            .minimal => .minimal,
            .extra => .extra,
        };
    }
};

const PageGeometry = struct {
    page_size: usize,
    capacity_mode: CapacityMode,
};

const Matrix = enum {
    pairwise,
    cartesian,
};

const Config = struct {
    candidate: CandidateSelection = .all,
    head_dim: ?u16 = null,
    q_len: ?usize = null,
    prefix: ?usize = null,
    page_order: PageOrderSelection = .all,
    pattern: PatternSelection = .all,
    /// `null` covers both global attention and `local_window`.
    window: ?u32 = null,
    local_window: u32 = local_window_default,
    /// `null` covers `supported_page_sizes`.
    page_size: ?usize = null,
    capacity: CapacitySelection = .all,
    matrix: Matrix = .pairwise,
    seed: u64 = 0xe703_7ed1_a0b4_28db,
    repeats: usize = 3,
    iterations: usize = 0,
    timing_pairs: usize = 5,
    max_abs: f64 = default_max_abs,
    max_rms_normalized: f64 = default_max_rms_normalized,
    max_timing_cv: f64 = default_max_timing_cv,
    require_bitwise: bool = false,
    json: bool = false,
    json_out: ?[]const u8 = null,
    help: bool = false,

    fn headDimCount(self: Config) usize {
        return if (self.head_dim == null) 2 else 1;
    }

    fn headDimAt(self: Config, index: usize) u16 {
        return self.head_dim orelse if (index == 0) 256 else 512;
    }

    fn qLenCount(self: Config) usize {
        return if (self.q_len == null) supported_q_lengths.len else 1;
    }

    fn qLenAt(self: Config, index: usize) usize {
        return self.q_len orelse supported_q_lengths[index];
    }

    fn prefixCount(self: Config) usize {
        return if (self.prefix == null) supported_prefixes.len else 1;
    }

    fn prefixAt(self: Config, index: usize) usize {
        return self.prefix orelse supported_prefixes[index];
    }

    fn windowCount(self: Config) usize {
        return if (self.window == null) 2 else 1;
    }

    fn windowAt(self: Config, index: usize) u32 {
        return self.window orelse if (index == 0) 0 else self.local_window;
    }

    fn pageSizeCount(self: Config) usize {
        return if (self.page_size == null) supported_page_sizes.len else 1;
    }

    fn pageSizeAt(self: Config, index: usize) usize {
        return self.page_size orelse supported_page_sizes[index];
    }

    fn geometryCount(self: Config) usize {
        return self.pageSizeCount() * self.capacity.count();
    }

    fn geometryAt(self: Config, index: usize) PageGeometry {
        const capacity_count = self.capacity.count();
        return .{
            .page_size = self.pageSizeAt(index / capacity_count),
            .capacity_mode = self.capacity.at(index % capacity_count),
        };
    }
};

const CaseSpec = struct {
    candidate: Candidate,
    head_dim: u16,
    q_len: usize,
    prefix: usize,
    sliding_window: u32,
    page_size: usize,
    capacity_mode: CapacityMode,
    page_order: PageOrder,
    pattern: Pattern,
};

const DiffStats = struct {
    element_count: usize = 0,
    bitwise_mismatch_count: usize = 0,
    non_finite_count: usize = 0,
    max_abs: f64 = 0,
    max_rel: f64 = 0,
    max_ulp: u64 = 0,
    rms_error: f64 = 0,
    rms_normalized_error: f64 = 0,
    first_mismatch_index: ?usize = null,
    first_reference_bits: ?u32 = null,
    first_candidate_bits: ?u32 = null,
};

const IntegrityStats = struct {
    baseline_unwritten: usize = 0,
    candidate_unwritten: usize = 0,
    baseline_canary_mismatches: usize = 0,
    candidate_canary_mismatches: usize = 0,
    readonly_mismatches: usize = 0,
    baseline_determinism_mismatches: usize = 0,
    candidate_determinism_mismatches: usize = 0,

    fn passes(self: IntegrityStats) bool {
        return self.baseline_unwritten == 0 and
            self.candidate_unwritten == 0 and
            self.baseline_canary_mismatches == 0 and
            self.candidate_canary_mismatches == 0 and
            self.readonly_mismatches == 0 and
            self.baseline_determinism_mismatches == 0 and
            self.candidate_determinism_mismatches == 0;
    }
};

const TimingStats = struct {
    pairs: usize = 0,
    iterations: usize = 0,
    baseline_total_us: u64 = 0,
    candidate_total_us: u64 = 0,
    baseline_min_pair_us: u64 = 0,
    baseline_max_pair_us: u64 = 0,
    candidate_min_pair_us: u64 = 0,
    candidate_max_pair_us: u64 = 0,
    baseline_pair_sum_squared: f64 = 0,
    candidate_pair_sum_squared: f64 = 0,

    fn baselineMeanLaunchUs(self: TimingStats) f64 {
        const launches = self.pairs * self.iterations;
        return if (launches == 0) 0 else @as(f64, @floatFromInt(self.baseline_total_us)) / @as(f64, @floatFromInt(launches));
    }

    fn candidateMeanLaunchUs(self: TimingStats) f64 {
        const launches = self.pairs * self.iterations;
        return if (launches == 0) 0 else @as(f64, @floatFromInt(self.candidate_total_us)) / @as(f64, @floatFromInt(launches));
    }

    fn coefficientOfVariation(total_us: u64, sum_squared: f64, pairs: usize) f64 {
        if (pairs < 2 or total_us == 0) return 0;
        const count: f64 = @floatFromInt(pairs);
        const mean = @as(f64, @floatFromInt(total_us)) / count;
        const variance = @max(0.0, sum_squared / count - mean * mean);
        return @sqrt(variance) / mean;
    }

    fn baselineCv(self: TimingStats) f64 {
        return coefficientOfVariation(self.baseline_total_us, self.baseline_pair_sum_squared, self.pairs);
    }

    fn candidateCv(self: TimingStats) f64 {
        return coefficientOfVariation(self.candidate_total_us, self.candidate_pair_sum_squared, self.pairs);
    }

    fn passes(self: TimingStats, max_cv: f64) bool {
        return self.pairs == 0 or (self.baselineCv() <= max_cv and self.candidateCv() <= max_cv);
    }
};

const CaseResult = struct {
    spec: CaseSpec,
    kv_len: usize,
    page_size: usize,
    mapped_blocks: usize,
    launch_block_count: usize,
    block_table_null: bool,
    physical_capacity: usize,
    poisoned_unused_rows: usize,
    diff: DiffStats,
    integrity: IntegrityStats,
    timing: TimingStats,

    fn passes(self: CaseResult, cfg: Config) bool {
        return self.integrity.passes() and
            self.diff.non_finite_count == 0 and
            numericalGatePass(self.diff, cfg) and
            self.timing.passes(cfg.max_timing_cv);
    }
};

const MetamorphicResult = struct {
    candidate: Candidate,
    head_dim: u16,
    q_len: usize,
    prefix: usize,
    kv_len: usize,
    page_size: usize,
    mapped_blocks: usize,
    seed: u64,
    diff: DiffStats,
    identity_unwritten: usize,
    explicit_unwritten: usize,
    identity_canary_mismatches: usize,
    explicit_canary_mismatches: usize,
    readonly_mismatches: usize,
    identity_determinism_mismatches: usize,
    explicit_determinism_mismatches: usize,

    fn passes(self: MetamorphicResult) bool {
        return self.diff.bitwise_mismatch_count == 0 and
            self.diff.non_finite_count == 0 and
            self.identity_unwritten == 0 and
            self.explicit_unwritten == 0 and
            self.identity_canary_mismatches == 0 and
            self.explicit_canary_mismatches == 0 and
            self.readonly_mismatches == 0 and
            self.identity_determinism_mismatches == 0 and
            self.explicit_determinism_mismatches == 0;
    }
};

const DeviceAuditExpectation = enum {
    no_write,
    bounded_zero,
};

const DeviceAuditKind = enum {
    null_nonzero,
    nonnull_zero,
    zero_page_size,
    undersized_capacity,
    nonaligned_capacity,
    too_few_blocks,
    huge_mapped_entry,

    fn label(self: DeviceAuditKind) []const u8 {
        return switch (self) {
            .null_nonzero => "null-nonzero",
            .nonnull_zero => "nonnull-zero",
            .zero_page_size => "zero-page-size",
            .undersized_capacity => "undersized-capacity",
            .nonaligned_capacity => "nonaligned-capacity",
            .too_few_blocks => "too-few-blocks",
            .huge_mapped_entry => "huge-mapped-entry",
        };
    }

    fn category(self: DeviceAuditKind) []const u8 {
        return if (self == .huge_mapped_entry) "bounded-data" else "launch-contract";
    }
};

const DeviceAuditPlan = struct {
    kind: DeviceAuditKind,
    q_len: usize,
    prefix: usize,
    table_present: bool,
    block_count: u32,
    page_size: u32,
    physical_capacity: u32,
    first_mapped_block: u32,
    expectation: DeviceAuditExpectation,

    fn kvLen(self: DeviceAuditPlan) usize {
        return self.q_len + self.prefix;
    }
};

const DeviceAuditResult = struct {
    candidate: Candidate,
    head_dim: u16,
    plan: DeviceAuditPlan,
    element_count: usize,
    unwritten: usize,
    non_finite_count: usize,
    nonzero_count: usize,
    canary_mismatches: usize,
    readonly_mismatches: usize,
    determinism_mismatches: usize,
    output_image_mismatches: usize,

    fn passes(self: DeviceAuditResult) bool {
        if (self.canary_mismatches != 0 or
            self.readonly_mismatches != 0 or
            self.determinism_mismatches != 0) return false;
        return switch (self.plan.expectation) {
            .no_write => self.unwritten == self.element_count and
                self.output_image_mismatches == 0,
            .bounded_zero => self.unwritten == 0 and
                self.non_finite_count == 0 and
                self.nonzero_count == 0,
        };
    }
};

fn numericalGatePass(diff: DiffStats, cfg: Config) bool {
    return diff.max_abs <= cfg.max_abs and
        diff.rms_normalized_error <= cfg.max_rms_normalized and
        (!cfg.require_bitwise or diff.bitwise_mismatch_count == 0);
}

fn deviceAuditPlan(kind: DeviceAuditKind) DeviceAuditPlan {
    var plan = DeviceAuditPlan{
        .kind = kind,
        .q_len = 2,
        .prefix = 15,
        .table_present = false,
        .block_count = 0,
        .page_size = 16,
        .physical_capacity = 32,
        .first_mapped_block = 0,
        .expectation = .no_write,
    };
    switch (kind) {
        .null_nonzero => plan.block_count = 2,
        .nonnull_zero => plan.table_present = true,
        .zero_page_size => plan.page_size = 0,
        .undersized_capacity => plan.physical_capacity = 16,
        .nonaligned_capacity => plan.physical_capacity = 31,
        .too_few_blocks => {
            plan.table_present = true;
            plan.block_count = 1;
        },
        .huge_mapped_entry => {
            plan.prefix = 0;
            plan.table_present = true;
            plan.block_count = 1;
            plan.physical_capacity = 16;
            // Multiplication by 16 wraps to row zero in u32. The device
            // translator must use wide checked arithmetic and reject it.
            plan.first_mapped_block = 0x1000_0000;
            plan.expectation = .bounded_zero;
        },
    }
    return plan;
}

fn pageLayoutContractValid(plan: DeviceAuditPlan) bool {
    const kv_len = plan.kvLen();
    if (kv_len == 0 or plan.page_size == 0) return false;
    const count_present = plan.block_count != 0;
    if (plan.table_present != count_present) return false;
    if (plan.physical_capacity == 0 or plan.physical_capacity % plan.page_size != 0) return false;
    if (!plan.table_present) return plan.physical_capacity >= kv_len;
    const required_blocks = ceilDiv(kv_len, plan.page_size);
    return plan.physical_capacity >= plan.page_size and plan.block_count >= required_blocks;
}

fn mappedBlockFits(entry: u32, page_size: u32, physical_capacity: u32) bool {
    if (page_size == 0) return false;
    const base = @as(u64, entry) * @as(u64, page_size);
    return base < physical_capacity;
}

const LaunchArgs = struct {
    q_seq_len: u32,
    kv_seq_len: u32,
    head_dim: u32,
    query_position_offset: u32,
    sliding_window: u32,
    total_sequence_len: u32,
    key_row_bytes: u32,
    value_row_bytes: u32,
    block_count: u32,
    page_size_tokens: u32,
    physical_token_capacity: u32,
};

const Functions = struct {
    baseline: cuda_driver.CUfunction,
    exact: cuda_driver.CUfunction,
    warp: cuda_driver.CUfunction,

    fn candidate(self: Functions, kind: Candidate) cuda_driver.CUfunction {
        return switch (kind) {
            .exact => self.exact,
            .warp => self.warp,
        };
    }
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

    fn functions(self: Module, ctx: *cuda_context.CudaContext) !Functions {
        return .{
            .baseline = try loadFunction(ctx, self.module, baseline_kernel_name),
            .exact = try loadFunction(ctx, self.module, exact_kernel_name),
            .warp = try loadFunction(ctx, self.module, warp_kernel_name),
        };
    }
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

fn physicalCapacity(kv_len: usize, page_size: usize, mode: CapacityMode) !usize {
    const mapped_blocks = ceilDiv(kv_len, page_size);
    const capacity_blocks = try checkedAdd(mapped_blocks, @intFromBool(mode == .extra));
    return checkedMul(capacity_blocks, page_size);
}

fn toU32(value: usize) !u32 {
    if (value > std.math.maxInt(u32)) return error.InvalidArgument;
    return @intCast(value);
}

fn artifactSha256Hex() [64]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(cuda_artifact.image[0..], &digest, .{});
    const alphabet = "0123456789abcdef";
    var hex: [64]u8 = undefined;
    for (digest, 0..) |byte, index| {
        hex[index * 2] = alphabet[byte >> 4];
        hex[index * 2 + 1] = alphabet[byte & 0x0f];
    }
    return hex;
}

fn contains(comptime T: type, haystack: []const T, needle: T) bool {
    for (haystack) |value| if (value == needle) return true;
    return false;
}

fn parseCandidate(value: []const u8) !CandidateSelection {
    if (std.mem.eql(u8, value, "all")) return .all;
    if (std.mem.eql(u8, value, "exact")) return .exact;
    if (std.mem.eql(u8, value, "warp")) return .warp;
    return error.InvalidCandidate;
}

fn parsePattern(value: []const u8) !PatternSelection {
    if (std.mem.eql(u8, value, "all")) return .all;
    if (std.mem.eql(u8, value, "random")) return .random;
    if (std.mem.eql(u8, value, "near-tie")) return .near_tie;
    if (std.mem.eql(u8, value, "cancellation")) return .cancellation;
    if (std.mem.eql(u8, value, "signed-zero")) return .signed_zero;
    if (std.mem.eql(u8, value, "subnormal")) return .subnormal;
    return error.InvalidPattern;
}

fn parsePageOrder(value: []const u8) !PageOrderSelection {
    if (std.mem.eql(u8, value, "all")) return .all;
    if (std.mem.eql(u8, value, "identity")) return .identity;
    if (std.mem.eql(u8, value, "fixed")) return .fixed;
    if (std.mem.eql(u8, value, "reversed")) return .reversed;
    if (std.mem.eql(u8, value, "permuted")) return .permuted;
    return error.InvalidPageOrder;
}

fn parseCapacity(value: []const u8) !CapacitySelection {
    if (std.mem.eql(u8, value, "all")) return .all;
    if (std.mem.eql(u8, value, "minimal")) return .minimal;
    if (std.mem.eql(u8, value, "extra")) return .extra;
    return error.InvalidCapacity;
}

fn parseMatrix(value: []const u8) !Matrix {
    if (std.mem.eql(u8, value, "pairwise")) return .pairwise;
    if (std.mem.eql(u8, value, "cartesian")) return .cartesian;
    return error.InvalidMatrix;
}

fn parseConfig(args: []const []const u8) !Config {
    var cfg = Config{};
    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--candidate")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.candidate = try parseCandidate(args[index]);
        } else if (std.mem.eql(u8, arg, "--head-dim")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.head_dim = if (std.mem.eql(u8, args[index], "all")) null else try std.fmt.parseInt(u16, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--q-len")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.q_len = if (std.mem.eql(u8, args[index], "all")) null else try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--prefix")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.prefix = if (std.mem.eql(u8, args[index], "all")) null else try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--window")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            if (std.mem.eql(u8, args[index], "all")) {
                cfg.window = null;
            } else if (std.mem.eql(u8, args[index], "global")) {
                cfg.window = 0;
            } else {
                cfg.window = try std.fmt.parseInt(u32, args[index], 10);
            }
        } else if (std.mem.eql(u8, arg, "--local-window")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.local_window = try std.fmt.parseInt(u32, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--page-size")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.page_size = if (std.mem.eql(u8, args[index], "all")) null else try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--capacity")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.capacity = try parseCapacity(args[index]);
        } else if (std.mem.eql(u8, arg, "--page-order")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.page_order = try parsePageOrder(args[index]);
        } else if (std.mem.eql(u8, arg, "--pattern")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.pattern = try parsePattern(args[index]);
        } else if (std.mem.eql(u8, arg, "--matrix")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.matrix = try parseMatrix(args[index]);
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
        } else if (std.mem.eql(u8, arg, "--max-timing-cv")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.max_timing_cv = try std.fmt.parseFloat(f64, args[index]);
        } else if (std.mem.eql(u8, arg, "--require-bitwise")) {
            cfg.require_bitwise = true;
        } else if (std.mem.eql(u8, arg, "--json")) {
            cfg.json = true;
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
    if (cfg.head_dim) |value| if (value != 256 and value != 512) return error.InvalidHeadDim;
    if (cfg.q_len) |value| if (!contains(usize, &supported_q_lengths, value)) return error.InvalidQueryLength;
    if (cfg.prefix) |value| if (!contains(usize, &supported_prefixes, value)) return error.InvalidPrefix;
    if (cfg.window) |value| if (value != 0 and value < 2) return error.InvalidWindow;
    if (cfg.local_window < 2) return error.InvalidWindow;
    if (cfg.page_size) |value| if (value == 0 or value > 4096) return error.InvalidPageSize;
    if (cfg.repeats < 2 or cfg.repeats > 100) return error.InvalidRepeats;
    if (cfg.iterations > 1_000_000) return error.InvalidIterations;
    if (cfg.timing_pairs == 0 or cfg.timing_pairs > max_timing_pairs) return error.InvalidTimingPairs;
    if (!std.math.isFinite(cfg.max_abs) or cfg.max_abs < 0) return error.InvalidMaxAbs;
    if (!std.math.isFinite(cfg.max_rms_normalized) or cfg.max_rms_normalized < 0) return error.InvalidMaxRmsNormalized;
    if (!std.math.isFinite(cfg.max_timing_cv) or cfg.max_timing_cv < 0) return error.InvalidMaxTimingCv;
}

fn pairwiseSpan(cfg: Config) usize {
    return @max(
        cfg.qLenCount(),
        @max(cfg.prefixCount(), @max(cfg.windowCount(), @max(cfg.geometryCount(), @max(cfg.page_order.count(), cfg.pattern.count())))),
    );
}

fn caseCount(cfg: Config) !usize {
    var count = try checkedMul(cfg.candidate.count(), cfg.headDimCount());
    switch (cfg.matrix) {
        .pairwise => count = try checkedMul(count, pairwiseSpan(cfg)),
        .cartesian => {
            count = try checkedMul(count, cfg.qLenCount());
            count = try checkedMul(count, cfg.prefixCount());
            count = try checkedMul(count, cfg.windowCount());
            count = try checkedMul(count, cfg.geometryCount());
            count = try checkedMul(count, cfg.page_order.count());
            count = try checkedMul(count, cfg.pattern.count());
        },
    }
    return count;
}

fn fillCaseSpecs(cfg: Config, out: []CaseSpec) !void {
    if (out.len != try caseCount(cfg)) return error.LengthMismatch;
    var cursor: usize = 0;
    for (0..cfg.candidate.count()) |candidate_index| {
        const candidate = cfg.candidate.at(candidate_index);
        for (0..cfg.headDimCount()) |head_index| {
            const head_dim = cfg.headDimAt(head_index);
            switch (cfg.matrix) {
                .pairwise => for (0..pairwiseSpan(cfg)) |index| {
                    const geometry = cfg.geometryAt(index % cfg.geometryCount());
                    out[cursor] = .{
                        .candidate = candidate,
                        .head_dim = head_dim,
                        .q_len = cfg.qLenAt(index % cfg.qLenCount()),
                        .prefix = cfg.prefixAt(index % cfg.prefixCount()),
                        .sliding_window = cfg.windowAt(index % cfg.windowCount()),
                        .page_size = geometry.page_size,
                        .capacity_mode = geometry.capacity_mode,
                        .page_order = cfg.page_order.at(index % cfg.page_order.count()),
                        .pattern = cfg.pattern.at(index % cfg.pattern.count()),
                    };
                    cursor += 1;
                },
                .cartesian => for (0..cfg.qLenCount()) |q_index| {
                    for (0..cfg.prefixCount()) |prefix_index| {
                        for (0..cfg.windowCount()) |window_index| {
                            for (0..cfg.geometryCount()) |geometry_index| {
                                const geometry = cfg.geometryAt(geometry_index);
                                for (0..cfg.page_order.count()) |order_index| {
                                    for (0..cfg.pattern.count()) |pattern_index| {
                                        out[cursor] = .{
                                            .candidate = candidate,
                                            .head_dim = head_dim,
                                            .q_len = cfg.qLenAt(q_index),
                                            .prefix = cfg.prefixAt(prefix_index),
                                            .sliding_window = cfg.windowAt(window_index),
                                            .page_size = geometry.page_size,
                                            .capacity_mode = geometry.capacity_mode,
                                            .page_order = cfg.page_order.at(order_index),
                                            .pattern = cfg.pattern.at(pattern_index),
                                        };
                                        cursor += 1;
                                    }
                                }
                            }
                        }
                    }
                },
            }
        }
    }
    std.debug.assert(cursor == out.len);
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

fn negativeZero() f32 {
    return @bitCast(@as(u32, 0x8000_0000));
}

fn f16Subnormal(negative: bool) f32 {
    const bits: u16 = if (negative) 0x8001 else 0x0001;
    return @floatCast(@as(f16, @bitCast(bits)));
}

fn fillInputs(
    q: []f32,
    k: []f32,
    v: []f32,
    q_len: usize,
    kv_len: usize,
    head_dim: usize,
    pattern: Pattern,
    seed: u64,
) void {
    var state = seed ^ (@as(u64, @intFromEnum(pattern)) *% 0x9e37_79b9_7f4a_7c15);
    switch (pattern) {
        .random => {
            for (q) |*value| value.* = nextSignedUnit(&state) * 0.125;
            for (k) |*value| value.* = nextSignedUnit(&state) * 0.125;
            for (v) |*value| value.* = nextSignedUnit(&state) * 0.75;
        },
        .near_tie => {
            for (0..q_len) |qi| for (0..num_heads) |head| for (0..head_dim) |d| {
                const sign: f32 = if (((qi + head + d) & 1) == 0) 1.0 else -1.0;
                const perturb: f32 = @floatFromInt((qi * 11 + head + d) % 5);
                q[(qi * num_heads + head) * head_dim + d] = sign * (0.03125 + perturb * 0.00003125);
            };
            for (0..kv_len) |token| for (0..head_dim) |d| {
                const sign: f32 = if ((d & 1) == 0) 1.0 else -1.0;
                const perturb: f32 = @floatFromInt((token * 17 + d * 3) % 11);
                k[token * head_dim + d] = sign * (0.03125 + perturb * 0.00000025);
                const parity: f32 = if (((token + d) & 1) == 0) 1.0 else -1.0;
                v[token * head_dim + d] = parity * (0.25 + @as(f32, @floatFromInt(token % 29)) * 0.0005);
            };
        },
        .cancellation => {
            for (0..q_len) |qi| for (0..num_heads) |head| for (0..head_dim) |d| {
                const sign: f32 = if ((d & 1) == 0) 1.0 else -1.0;
                const perturb: f32 = @floatFromInt((qi * 5 + head * 13 + d) % 7);
                q[(qi * num_heads + head) * head_dim + d] = sign * (0.125 + perturb * 0.00001);
            };
            for (0..kv_len) |token| for (0..head_dim) |d| {
                const sign: f32 = if ((d & 1) == 0) 1.0 else -1.0;
                const token_sign: f32 = if ((token & 1) == 0) 1.0 else -1.0;
                const perturb: f32 = @floatFromInt((token * 19 + d) % 13);
                k[token * head_dim + d] = sign * (token_sign * 0.125 + perturb * 0.00001);
                v[token * head_dim + d] = token_sign * (0.5 + @as(f32, @floatFromInt(d % 17)) * 0.0001);
            };
        },
        .signed_zero => {
            for (q, 0..) |*value, index| value.* = switch (index % 4) {
                0 => 0.0,
                1 => negativeZero(),
                2 => 0.03125,
                else => -0.03125,
            };
            for (k, 0..) |*value, index| value.* = switch (index % 4) {
                0 => negativeZero(),
                1 => 0.0,
                2 => -0.0625,
                else => 0.0625,
            };
            for (v, 0..) |*value, index| value.* = switch (index % 4) {
                0 => 0.0,
                1 => negativeZero(),
                2 => 0.25,
                else => -0.25,
            };
        },
        .subnormal => {
            for (q, 0..) |*value, index| value.* = if (index % 17 == 0)
                (if ((index & 1) == 0) 1.0 else -1.0)
            else
                f16Subnormal((index & 1) != 0);
            for (k, 0..) |*value, index| value.* = if (index % 19 == 0)
                (if ((index & 1) == 0) 0.5 else -0.5)
            else
                f16Subnormal((index & 1) != 0);
            for (v, 0..) |*value, index| value.* = if (index % 23 == 0)
                (if ((index & 1) == 0) 0.125 else -0.125)
            else
                f16Subnormal((index & 1) != 0);
        },
    }
}

fn physicalToken(logical_token: usize, block_table: []const u32, page_size: usize) usize {
    if (block_table.len == 0) return logical_token;
    const logical_block = logical_token / page_size;
    return @as(usize, block_table[logical_block]) * page_size + logical_token % page_size;
}

fn fillBlockTable(block_table: []u32, order: PageOrder, seed: u64) void {
    for (block_table, 0..) |*physical_block, logical_block| physical_block.* = @intCast(logical_block);
    switch (order) {
        .identity, .fixed => {},
        .reversed => std.mem.reverse(u32, block_table),
        .permuted => {
            var state = seed ^ 0xd1b5_4a32_d192_ed03;
            var remaining = block_table.len;
            while (remaining > 1) {
                const swap_index = @as(usize, nextU32(&state)) % remaining;
                remaining -= 1;
                std.mem.swap(u32, &block_table[remaining], &block_table[swap_index]);
            }
        },
    }
}

fn writeU16Le(dst: []u8, offset: usize, bits: u16) void {
    dst[offset] = @truncate(bits);
    dst[offset + 1] = @truncate(bits >> 8);
}

fn writeU32Le(dst: []u8, offset: usize, bits: u32) void {
    for (0..4) |byte_index| dst[offset + byte_index] = @truncate(bits >> @intCast(byte_index * 8));
}

fn packPagedF16(dst: []u8, logical: []const f32, block_table: []const u32, page_size: usize, row_width: usize) void {
    var index: usize = 0;
    while (index + 1 < dst.len) : (index += 2) writeU16Le(dst, index, f16_poison_bits);
    const row_bytes = row_width * @sizeOf(f16);
    const logical_tokens = logical.len / row_width;
    for (0..logical_tokens) |token| {
        const physical = physicalToken(token, block_table, page_size);
        const row = dst[physical * row_bytes ..][0..row_bytes];
        for (logical[token * row_width ..][0..row_width], 0..) |value, value_index| {
            writeU16Le(row, value_index * 2, @bitCast(@as(f16, @floatCast(value))));
        }
    }
}

fn guardedSize(payload_bytes: usize) !usize {
    return checkedAdd(try checkedAdd(guard_bytes, payload_bytes), guard_bytes);
}

fn allocGuardedImage(allocator: std.mem.Allocator, payload_bytes: usize, payload_byte: u8) ![]u8 {
    const image = try allocator.alloc(u8, try guardedSize(payload_bytes));
    @memset(image, canary_byte);
    @memset(payloadSlice(image, payload_bytes), payload_byte);
    return image;
}

fn payloadSlice(image: []u8, payload_bytes: usize) []u8 {
    return image[guard_bytes..][0..payload_bytes];
}

fn constPayloadSlice(image: []const u8, payload_bytes: usize) []const u8 {
    return image[guard_bytes..][0..payload_bytes];
}

fn devicePayloadView(allocation: cuda_buffer.DeviceBuffer, payload_bytes: usize) cuda_buffer.DeviceBuffer {
    return .{ .ptr = allocation.ptr + guard_bytes, .len = payload_bytes };
}

fn countByteMismatches(expected: []const u8, actual: []const u8) !usize {
    if (expected.len != actual.len) return error.LengthMismatch;
    var mismatches: usize = 0;
    for (expected, actual) |a, b| mismatches += @intFromBool(a != b);
    return mismatches;
}

fn countOutputCanaryMismatches(image: []const u8, payload_bytes: usize) usize {
    var mismatches: usize = 0;
    for (image[0..guard_bytes]) |value| mismatches += @intFromBool(value != canary_byte);
    for (image[guard_bytes + payload_bytes ..]) |value| mismatches += @intFromBool(value != canary_byte);
    return mismatches;
}

fn fillOutputPoison(image: []u8, payload_bytes: usize) void {
    const payload = payloadSlice(image, payload_bytes);
    var offset: usize = 0;
    while (offset + 3 < payload.len) : (offset += 4) writeU32Le(payload, offset, output_poison_bits);
}

fn copyPayloadToF32(dst: []f32, image: []const u8) !void {
    const bytes = std.mem.sliceAsBytes(dst);
    if (image.len != try guardedSize(bytes.len)) return error.LengthMismatch;
    @memcpy(bytes, constPayloadSlice(image, bytes.len));
}

fn countPoison(values: []const f32) usize {
    var count: usize = 0;
    for (values) |value| count += @intFromBool(@as(u32, @bitCast(value)) == output_poison_bits);
    return count;
}

fn countNonFinite(values: []const f32) usize {
    var count: usize = 0;
    for (values) |value| count += @intFromBool(!std.math.isFinite(value));
    return count;
}

fn countNonZero(values: []const f32) usize {
    var count: usize = 0;
    for (values) |value| count += @intFromBool((@as(u32, @bitCast(value)) & 0x7fff_ffff) != 0);
    return count;
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

fn compareOutputs(reference: []const f32, candidate: []const f32) !DiffStats {
    if (reference.len != candidate.len) return error.LengthMismatch;
    var stats = DiffStats{ .element_count = reference.len };
    var sum_error_squared: f64 = 0;
    var sum_reference_squared: f64 = 0;
    for (reference, candidate, 0..) |expected, actual, index| {
        const expected_bits: u32 = @bitCast(expected);
        const actual_bits: u32 = @bitCast(actual);
        if (expected_bits != actual_bits) {
            stats.bitwise_mismatch_count += 1;
            if (stats.first_mismatch_index == null) {
                stats.first_mismatch_index = index;
                stats.first_reference_bits = expected_bits;
                stats.first_candidate_bits = actual_bits;
            }
        }
        if (!std.math.isFinite(expected) or !std.math.isFinite(actual)) {
            stats.non_finite_count += 1;
            continue;
        }
        const expected64: f64 = expected;
        const actual64: f64 = actual;
        const abs_error = @abs(expected64 - actual64);
        stats.max_abs = @max(stats.max_abs, abs_error);
        stats.max_rel = @max(stats.max_rel, abs_error / @max(@abs(expected64), 1e-30));
        stats.max_ulp = @max(stats.max_ulp, ulpDistance(expected, actual));
        sum_error_squared += abs_error * abs_error;
        sum_reference_squared += expected64 * expected64;
    }
    if (reference.len != 0) {
        const count64: f64 = @floatFromInt(reference.len);
        stats.rms_error = @sqrt(sum_error_squared / count64);
        const reference_rms = @sqrt(sum_reference_squared / count64);
        stats.rms_normalized_error = stats.rms_error / @max(reference_rms, rms_reference_floor);
    }
    return stats;
}

fn countBitwiseMismatches(reference: []const f32, candidate: []const f32) !usize {
    return (try compareOutputs(reference, candidate)).bitwise_mismatch_count;
}

fn loadFunction(ctx: *cuda_context.CudaContext, module: cuda_driver.CUmodule, name: []const u8) !cuda_driver.CUfunction {
    var name_buffer: [160]u8 = undefined;
    const name_z = try std.fmt.bufPrintZ(&name_buffer, "{s}", .{name});
    var function: cuda_driver.CUfunction = null;
    try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&function, module, name_z));
    return function orelse error.CudaKernelUnavailable;
}

fn launchAttention(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    candidate: ?Candidate,
    dst: cuda_buffer.DeviceBuffer,
    q: cuda_buffer.DeviceBuffer,
    k: cuda_buffer.DeviceBuffer,
    v: cuda_buffer.DeviceBuffer,
    block_table: cuda_buffer.DeviceBuffer,
    args: LaunchArgs,
) !void {
    var dst_ptr = dst.ptr;
    var q_ptr = q.ptr;
    var k_ptr = k.ptr;
    var v_ptr = v.ptr;
    var block_table_ptr = block_table.ptr;
    var null_ptr: cuda_driver.CUdeviceptr = 0;
    var batch: u32 = 1;
    var q_seq_len = args.q_seq_len;
    var kv_seq_len = args.kv_seq_len;
    var heads: u32 = num_heads;
    var kv_heads: u32 = num_kv_heads;
    var head_dim = args.head_dim;
    var query_position_offset = args.query_position_offset;
    var kv_position_offset: u32 = 0;
    var sliding_window = args.sliding_window;
    var total_sequence_len = args.total_sequence_len;
    var mask_len: u32 = 0;
    var bias_mode: u32 = 0;
    var key_row_bytes = args.key_row_bytes;
    var base_key_row_bytes = args.key_row_bytes;
    var value_row_bytes = args.value_row_bytes;
    var block_count = args.block_count;
    var page_size_tokens = args.page_size_tokens;
    var format: u32 = 2;
    var value_format: u32 = 2;
    var physical_token_capacity = args.physical_token_capacity;
    var decode_scalars_ptr: cuda_driver.CUdeviceptr = 0;
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),            @ptrCast(&q_ptr),                 @ptrCast(&k_ptr),                   @ptrCast(&v_ptr),
        @ptrCast(&block_table_ptr),    @ptrCast(&null_ptr),              @ptrCast(&null_ptr),                @ptrCast(&batch),
        @ptrCast(&q_seq_len),          @ptrCast(&kv_seq_len),            @ptrCast(&heads),                   @ptrCast(&kv_heads),
        @ptrCast(&head_dim),           @ptrCast(&query_position_offset), @ptrCast(&kv_position_offset),      @ptrCast(&sliding_window),
        @ptrCast(&total_sequence_len), @ptrCast(&mask_len),              @ptrCast(&bias_mode),               @ptrCast(&key_row_bytes),
        @ptrCast(&base_key_row_bytes), @ptrCast(&value_row_bytes),       @ptrCast(&block_count),             @ptrCast(&page_size_tokens),
        @ptrCast(&format),             @ptrCast(&value_format),          @ptrCast(&physical_token_capacity), @ptrCast(&decode_scalars_ptr),
    };
    const block: u32 = if (candidate) |kind| kind.threads(@intCast(args.head_dim)) else args.head_dim;
    const grid_x: u32 = if (candidate == null) try toU32(try checkedMul(@as(usize, args.q_seq_len), num_heads)) else num_heads;
    const grid_y: u32 = if (candidate == null) 1 else try toU32(ceilDiv(args.q_seq_len, tile_queries));
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
        function,
        grid_x,
        grid_y,
        1,
        block,
        1,
        1,
        0,
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
    candidate: ?Candidate,
    dst: cuda_buffer.DeviceBuffer,
    q: cuda_buffer.DeviceBuffer,
    k: cuda_buffer.DeviceBuffer,
    v: cuda_buffer.DeviceBuffer,
    block_table: cuda_buffer.DeviceBuffer,
    args: LaunchArgs,
) !u64 {
    const pair = try ctx.beginProfileEventPair();
    for (0..iterations) |_| try launchAttention(ctx, function, candidate, dst, q, k, v, block_table, args);
    return ctx.endProfileEventPairUs(pair);
}

fn runTimings(
    ctx: *cuda_context.CudaContext,
    cfg: Config,
    spec: CaseSpec,
    functions: Functions,
    d_baseline: cuda_buffer.DeviceBuffer,
    d_candidate: cuda_buffer.DeviceBuffer,
    d_q: cuda_buffer.DeviceBuffer,
    d_k: cuda_buffer.DeviceBuffer,
    d_v: cuda_buffer.DeviceBuffer,
    d_table: cuda_buffer.DeviceBuffer,
    args: LaunchArgs,
) !TimingStats {
    if (cfg.iterations == 0) return .{};
    var stats = TimingStats{ .pairs = cfg.timing_pairs, .iterations = cfg.iterations };
    for (0..cfg.timing_pairs) |pair_index| {
        var baseline_us: u64 = undefined;
        var candidate_us: u64 = undefined;
        if ((pair_index & 1) == 0) {
            baseline_us = try timeLaunches(ctx, cfg.iterations, functions.baseline, null, d_baseline, d_q, d_k, d_v, d_table, args);
            candidate_us = try timeLaunches(ctx, cfg.iterations, functions.candidate(spec.candidate), spec.candidate, d_candidate, d_q, d_k, d_v, d_table, args);
        } else {
            candidate_us = try timeLaunches(ctx, cfg.iterations, functions.candidate(spec.candidate), spec.candidate, d_candidate, d_q, d_k, d_v, d_table, args);
            baseline_us = try timeLaunches(ctx, cfg.iterations, functions.baseline, null, d_baseline, d_q, d_k, d_v, d_table, args);
        }
        stats.baseline_total_us += baseline_us;
        stats.candidate_total_us += candidate_us;
        const baseline_f64: f64 = @floatFromInt(baseline_us);
        const candidate_f64: f64 = @floatFromInt(candidate_us);
        stats.baseline_pair_sum_squared += baseline_f64 * baseline_f64;
        stats.candidate_pair_sum_squared += candidate_f64 * candidate_f64;
        if (pair_index == 0) {
            stats.baseline_min_pair_us = baseline_us;
            stats.baseline_max_pair_us = baseline_us;
            stats.candidate_min_pair_us = candidate_us;
            stats.candidate_max_pair_us = candidate_us;
        } else {
            stats.baseline_min_pair_us = @min(stats.baseline_min_pair_us, baseline_us);
            stats.baseline_max_pair_us = @max(stats.baseline_max_pair_us, baseline_us);
            stats.candidate_min_pair_us = @min(stats.candidate_min_pair_us, candidate_us);
            stats.candidate_max_pair_us = @max(stats.candidate_max_pair_us, candidate_us);
        }
    }
    return stats;
}

fn deviceImage(ctx: *cuda_context.CudaContext, image: []const u8) !cuda_buffer.DeviceBuffer {
    var device = try cuda_buffer.DeviceBuffer.alloc(ctx, image.len);
    errdefer device.free(ctx);
    try device.copyFromHost(ctx, image);
    return device;
}

fn readDeviceImage(allocator: std.mem.Allocator, ctx: *cuda_context.CudaContext, device: cuda_buffer.DeviceBuffer) ![]u8 {
    const image = try allocator.alloc(u8, device.len);
    errdefer allocator.free(image);
    try device.copyToHost(ctx, image);
    try ctx.synchronize();
    return image;
}

fn verifyDeviceUnchanged(allocator: std.mem.Allocator, ctx: *cuda_context.CudaContext, device: cuda_buffer.DeviceBuffer, expected: []const u8) !usize {
    const actual = try readDeviceImage(allocator, ctx, device);
    defer allocator.free(actual);
    return countByteMismatches(expected, actual);
}

fn caseSeed(cfg: Config, spec: CaseSpec) u64 {
    var seed = cfg.seed;
    seed ^= @as(u64, spec.head_dim) << 41;
    seed ^= @as(u64, @intCast(spec.q_len)) *% 0x9e37_79b9;
    seed ^= @as(u64, @intCast(spec.prefix)) *% 0x85eb_ca6b;
    seed ^= @as(u64, @intFromEnum(spec.page_order)) << 17;
    seed ^= @as(u64, @intFromEnum(spec.pattern)) << 23;
    seed ^= @as(u64, spec.sliding_window) << 29;
    seed ^= @as(u64, @intCast(spec.page_size)) *% 0x27d4_eb2f;
    seed ^= @as(u64, @intFromEnum(spec.capacity_mode)) << 37;
    return seed;
}

fn runCase(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    cfg: Config,
    functions: Functions,
    spec: CaseSpec,
) !CaseResult {
    const dim: usize = spec.head_dim;
    const kv_len = try checkedAdd(spec.prefix, spec.q_len);
    const q_count = try checkedMul(try checkedMul(spec.q_len, num_heads), dim);
    const kv_width = try checkedMul(num_kv_heads, dim);
    const kv_count = try checkedMul(kv_len, kv_width);
    const output_bytes = try checkedMul(q_count, @sizeOf(f32));
    const q_bytes = output_bytes;
    const row_bytes = try checkedMul(kv_width, @sizeOf(f16));
    const mapped_blocks = ceilDiv(kv_len, spec.page_size);
    const table_contract = pageTableContract(spec.page_order, mapped_blocks);
    // Every allocation contains complete pages. Extra mode adds one entirely
    // unmapped poison page; minimal mode retains only final-page padding.
    const physical_capacity = try physicalCapacity(kv_len, spec.page_size, spec.capacity_mode);
    const kv_bytes = try checkedMul(physical_capacity, row_bytes);
    const table_bytes = try checkedMul(table_contract.entry_count, @sizeOf(u32));

    const host_q = try allocator.alloc(f32, q_count);
    defer allocator.free(host_q);
    const logical_k = try allocator.alloc(f32, kv_count);
    defer allocator.free(logical_k);
    const logical_v = try allocator.alloc(f32, kv_count);
    defer allocator.free(logical_v);
    const block_table = try allocator.alloc(u32, table_contract.entry_count);
    defer allocator.free(block_table);
    const host_baseline = try allocator.alloc(f32, q_count);
    defer allocator.free(host_baseline);
    const host_candidate = try allocator.alloc(f32, q_count);
    defer allocator.free(host_candidate);
    const host_repeat = try allocator.alloc(f32, q_count);
    defer allocator.free(host_repeat);

    const seed = caseSeed(cfg, spec);
    fillInputs(host_q, logical_k, logical_v, spec.q_len, kv_len, dim, spec.pattern, seed);
    fillBlockTable(block_table, spec.page_order, seed);

    const q_image = try allocGuardedImage(allocator, q_bytes, 0);
    defer allocator.free(q_image);
    @memcpy(payloadSlice(q_image, q_bytes), std.mem.sliceAsBytes(host_q));
    const k_image = try allocGuardedImage(allocator, kv_bytes, 0);
    defer allocator.free(k_image);
    packPagedF16(payloadSlice(k_image, kv_bytes), logical_k, block_table, spec.page_size, kv_width);
    const v_image = try allocGuardedImage(allocator, kv_bytes, 0);
    defer allocator.free(v_image);
    packPagedF16(payloadSlice(v_image, kv_bytes), logical_v, block_table, spec.page_size, kv_width);
    const table_image = if (table_contract.block_table_null)
        try allocator.alloc(u8, 0)
    else
        try allocGuardedImage(allocator, table_bytes, 0);
    defer allocator.free(table_image);
    if (!table_contract.block_table_null) {
        @memcpy(payloadSlice(table_image, table_bytes), std.mem.sliceAsBytes(block_table));
    }
    const baseline_image = try allocGuardedImage(allocator, output_bytes, 0);
    defer allocator.free(baseline_image);
    fillOutputPoison(baseline_image, output_bytes);
    const candidate_image = try allocGuardedImage(allocator, output_bytes, 0);
    defer allocator.free(candidate_image);
    fillOutputPoison(candidate_image, output_bytes);

    var d_q_all = try deviceImage(ctx, q_image);
    defer d_q_all.free(ctx);
    var d_k_all = try deviceImage(ctx, k_image);
    defer d_k_all.free(ctx);
    var d_v_all = try deviceImage(ctx, v_image);
    defer d_v_all.free(ctx);
    var d_table_all: cuda_buffer.DeviceBuffer = if (table_contract.block_table_null)
        .{}
    else
        try deviceImage(ctx, table_image);
    defer d_table_all.free(ctx);
    var d_baseline_all = try deviceImage(ctx, baseline_image);
    defer d_baseline_all.free(ctx);
    var d_candidate_all = try deviceImage(ctx, candidate_image);
    defer d_candidate_all.free(ctx);

    const d_q = devicePayloadView(d_q_all, q_bytes);
    const d_k = devicePayloadView(d_k_all, kv_bytes);
    const d_v = devicePayloadView(d_v_all, kv_bytes);
    const d_table: cuda_buffer.DeviceBuffer = if (table_contract.block_table_null)
        .{}
    else
        devicePayloadView(d_table_all, table_bytes);
    std.debug.assert((d_table.ptr == 0) == table_contract.block_table_null);
    const d_baseline = devicePayloadView(d_baseline_all, output_bytes);
    const d_candidate = devicePayloadView(d_candidate_all, output_bytes);
    const args = LaunchArgs{
        .q_seq_len = try toU32(spec.q_len),
        .kv_seq_len = try toU32(kv_len),
        .head_dim = spec.head_dim,
        .query_position_offset = try toU32(spec.prefix),
        .sliding_window = spec.sliding_window,
        .total_sequence_len = try toU32(kv_len),
        .key_row_bytes = try toU32(row_bytes),
        .value_row_bytes = try toU32(row_bytes),
        .block_count = try toU32(table_contract.launch_block_count),
        .page_size_tokens = try toU32(spec.page_size),
        .physical_token_capacity = try toU32(physical_capacity),
    };

    try launchAttention(ctx, functions.baseline, null, d_baseline, d_q, d_k, d_v, d_table, args);
    try launchAttention(ctx, functions.candidate(spec.candidate), spec.candidate, d_candidate, d_q, d_k, d_v, d_table, args);
    const baseline_readback = try readDeviceImage(allocator, ctx, d_baseline_all);
    defer allocator.free(baseline_readback);
    const candidate_readback = try readDeviceImage(allocator, ctx, d_candidate_all);
    defer allocator.free(candidate_readback);
    try copyPayloadToF32(host_baseline, baseline_readback);
    try copyPayloadToF32(host_candidate, candidate_readback);

    var integrity = IntegrityStats{
        .baseline_unwritten = countPoison(host_baseline),
        .candidate_unwritten = countPoison(host_candidate),
        .baseline_canary_mismatches = countOutputCanaryMismatches(baseline_readback, output_bytes),
        .candidate_canary_mismatches = countOutputCanaryMismatches(candidate_readback, output_bytes),
    };
    for (1..cfg.repeats) |_| {
        try launchAttention(ctx, functions.baseline, null, d_baseline, d_q, d_k, d_v, d_table, args);
        const baseline_repeat_image = try readDeviceImage(allocator, ctx, d_baseline_all);
        defer allocator.free(baseline_repeat_image);
        try copyPayloadToF32(host_repeat, baseline_repeat_image);
        integrity.baseline_determinism_mismatches += try countBitwiseMismatches(host_baseline, host_repeat);

        try launchAttention(ctx, functions.candidate(spec.candidate), spec.candidate, d_candidate, d_q, d_k, d_v, d_table, args);
        const candidate_repeat_image = try readDeviceImage(allocator, ctx, d_candidate_all);
        defer allocator.free(candidate_repeat_image);
        try copyPayloadToF32(host_repeat, candidate_repeat_image);
        integrity.candidate_determinism_mismatches += try countBitwiseMismatches(host_candidate, host_repeat);
    }

    const timing = try runTimings(ctx, cfg, spec, functions, d_baseline, d_candidate, d_q, d_k, d_v, d_table, args);
    integrity.readonly_mismatches += try verifyDeviceUnchanged(allocator, ctx, d_q_all, q_image);
    integrity.readonly_mismatches += try verifyDeviceUnchanged(allocator, ctx, d_k_all, k_image);
    integrity.readonly_mismatches += try verifyDeviceUnchanged(allocator, ctx, d_v_all, v_image);
    if (!table_contract.block_table_null) {
        integrity.readonly_mismatches += try verifyDeviceUnchanged(allocator, ctx, d_table_all, table_image);
    }
    const baseline_final = try readDeviceImage(allocator, ctx, d_baseline_all);
    defer allocator.free(baseline_final);
    const candidate_final = try readDeviceImage(allocator, ctx, d_candidate_all);
    defer allocator.free(candidate_final);
    integrity.baseline_canary_mismatches += countOutputCanaryMismatches(baseline_final, output_bytes);
    integrity.candidate_canary_mismatches += countOutputCanaryMismatches(candidate_final, output_bytes);

    return .{
        .spec = spec,
        .kv_len = kv_len,
        .page_size = spec.page_size,
        .mapped_blocks = mapped_blocks,
        .launch_block_count = table_contract.launch_block_count,
        .block_table_null = table_contract.block_table_null,
        .physical_capacity = physical_capacity,
        .poisoned_unused_rows = physical_capacity - kv_len,
        .diff = try compareOutputs(host_baseline, host_candidate),
        .integrity = integrity,
        .timing = timing,
    };
}

fn runMetamorphicCase(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    cfg: Config,
    functions: Functions,
    candidate: Candidate,
    head_dim: u16,
) !MetamorphicResult {
    const q_len: usize = 33;
    const prefix: usize = 511;
    const kv_len = q_len + prefix;
    const page_size: usize = page_size_default;
    const mapped_blocks = ceilDiv(kv_len, page_size);
    const physical_capacity = try checkedMul(mapped_blocks, page_size);
    const dim: usize = head_dim;
    const q_count = try checkedMul(try checkedMul(q_len, num_heads), dim);
    const kv_width = try checkedMul(num_kv_heads, dim);
    const kv_count = try checkedMul(kv_len, kv_width);
    const output_bytes = try checkedMul(q_count, @sizeOf(f32));
    const row_bytes = try checkedMul(kv_width, @sizeOf(f16));
    const kv_bytes = try checkedMul(physical_capacity, row_bytes);
    const table_bytes = try checkedMul(mapped_blocks, @sizeOf(u32));
    const seed = cfg.seed ^ (@as(u64, head_dim) << 41) ^ (@as(u64, @intFromEnum(candidate)) << 13) ^ 0x6d65_7461_7061_6765;

    const host_q = try allocator.alloc(f32, q_count);
    defer allocator.free(host_q);
    const logical_k = try allocator.alloc(f32, kv_count);
    defer allocator.free(logical_k);
    const logical_v = try allocator.alloc(f32, kv_count);
    defer allocator.free(logical_v);
    const block_table = try allocator.alloc(u32, mapped_blocks);
    defer allocator.free(block_table);
    const host_identity = try allocator.alloc(f32, q_count);
    defer allocator.free(host_identity);
    const host_explicit = try allocator.alloc(f32, q_count);
    defer allocator.free(host_explicit);
    const host_repeat = try allocator.alloc(f32, q_count);
    defer allocator.free(host_repeat);

    fillInputs(host_q, logical_k, logical_v, q_len, kv_len, dim, .random, seed);
    fillBlockTable(block_table, .fixed, seed);
    const no_table = [_]u32{};

    const q_image = try allocGuardedImage(allocator, output_bytes, 0);
    defer allocator.free(q_image);
    @memcpy(payloadSlice(q_image, output_bytes), std.mem.sliceAsBytes(host_q));
    const k_image = try allocGuardedImage(allocator, kv_bytes, 0);
    defer allocator.free(k_image);
    packPagedF16(payloadSlice(k_image, kv_bytes), logical_k, &no_table, page_size, kv_width);
    const v_image = try allocGuardedImage(allocator, kv_bytes, 0);
    defer allocator.free(v_image);
    packPagedF16(payloadSlice(v_image, kv_bytes), logical_v, &no_table, page_size, kv_width);
    const table_image = try allocGuardedImage(allocator, table_bytes, 0);
    defer allocator.free(table_image);
    @memcpy(payloadSlice(table_image, table_bytes), std.mem.sliceAsBytes(block_table));
    const identity_image = try allocGuardedImage(allocator, output_bytes, 0);
    defer allocator.free(identity_image);
    fillOutputPoison(identity_image, output_bytes);
    const explicit_image = try allocGuardedImage(allocator, output_bytes, 0);
    defer allocator.free(explicit_image);
    fillOutputPoison(explicit_image, output_bytes);

    var d_q_all = try deviceImage(ctx, q_image);
    defer d_q_all.free(ctx);
    var d_k_all = try deviceImage(ctx, k_image);
    defer d_k_all.free(ctx);
    var d_v_all = try deviceImage(ctx, v_image);
    defer d_v_all.free(ctx);
    var d_table_all = try deviceImage(ctx, table_image);
    defer d_table_all.free(ctx);
    var d_identity_all = try deviceImage(ctx, identity_image);
    defer d_identity_all.free(ctx);
    var d_explicit_all = try deviceImage(ctx, explicit_image);
    defer d_explicit_all.free(ctx);

    const d_q = devicePayloadView(d_q_all, output_bytes);
    const d_k = devicePayloadView(d_k_all, kv_bytes);
    const d_v = devicePayloadView(d_v_all, kv_bytes);
    const d_table = devicePayloadView(d_table_all, table_bytes);
    const d_identity = devicePayloadView(d_identity_all, output_bytes);
    const d_explicit = devicePayloadView(d_explicit_all, output_bytes);
    const identity_args = LaunchArgs{
        .q_seq_len = try toU32(q_len),
        .kv_seq_len = try toU32(kv_len),
        .head_dim = head_dim,
        .query_position_offset = try toU32(prefix),
        .sliding_window = 0,
        .total_sequence_len = try toU32(kv_len),
        .key_row_bytes = try toU32(row_bytes),
        .value_row_bytes = try toU32(row_bytes),
        .block_count = 0,
        .page_size_tokens = try toU32(page_size),
        .physical_token_capacity = try toU32(physical_capacity),
    };
    var explicit_args = identity_args;
    explicit_args.block_count = try toU32(mapped_blocks);
    const function = functions.candidate(candidate);

    try launchAttention(ctx, function, candidate, d_identity, d_q, d_k, d_v, .{}, identity_args);
    try launchAttention(ctx, function, candidate, d_explicit, d_q, d_k, d_v, d_table, explicit_args);
    const identity_readback = try readDeviceImage(allocator, ctx, d_identity_all);
    defer allocator.free(identity_readback);
    const explicit_readback = try readDeviceImage(allocator, ctx, d_explicit_all);
    defer allocator.free(explicit_readback);
    try copyPayloadToF32(host_identity, identity_readback);
    try copyPayloadToF32(host_explicit, explicit_readback);

    var identity_determinism_mismatches: usize = 0;
    var explicit_determinism_mismatches: usize = 0;
    for (1..cfg.repeats) |_| {
        try launchAttention(ctx, function, candidate, d_identity, d_q, d_k, d_v, .{}, identity_args);
        const repeat_identity = try readDeviceImage(allocator, ctx, d_identity_all);
        defer allocator.free(repeat_identity);
        try copyPayloadToF32(host_repeat, repeat_identity);
        identity_determinism_mismatches += try countBitwiseMismatches(host_identity, host_repeat);

        try launchAttention(ctx, function, candidate, d_explicit, d_q, d_k, d_v, d_table, explicit_args);
        const repeat_explicit = try readDeviceImage(allocator, ctx, d_explicit_all);
        defer allocator.free(repeat_explicit);
        try copyPayloadToF32(host_repeat, repeat_explicit);
        explicit_determinism_mismatches += try countBitwiseMismatches(host_explicit, host_repeat);
    }

    var readonly_mismatches: usize = 0;
    readonly_mismatches += try verifyDeviceUnchanged(allocator, ctx, d_q_all, q_image);
    readonly_mismatches += try verifyDeviceUnchanged(allocator, ctx, d_k_all, k_image);
    readonly_mismatches += try verifyDeviceUnchanged(allocator, ctx, d_v_all, v_image);
    readonly_mismatches += try verifyDeviceUnchanged(allocator, ctx, d_table_all, table_image);
    const identity_final = try readDeviceImage(allocator, ctx, d_identity_all);
    defer allocator.free(identity_final);
    const explicit_final = try readDeviceImage(allocator, ctx, d_explicit_all);
    defer allocator.free(explicit_final);

    return .{
        .candidate = candidate,
        .head_dim = head_dim,
        .q_len = q_len,
        .prefix = prefix,
        .kv_len = kv_len,
        .page_size = page_size,
        .mapped_blocks = mapped_blocks,
        .seed = seed,
        .diff = try compareOutputs(host_identity, host_explicit),
        .identity_unwritten = countPoison(host_identity),
        .explicit_unwritten = countPoison(host_explicit),
        .identity_canary_mismatches = countOutputCanaryMismatches(identity_readback, output_bytes) + countOutputCanaryMismatches(identity_final, output_bytes),
        .explicit_canary_mismatches = countOutputCanaryMismatches(explicit_readback, output_bytes) + countOutputCanaryMismatches(explicit_final, output_bytes),
        .readonly_mismatches = readonly_mismatches,
        .identity_determinism_mismatches = identity_determinism_mismatches,
        .explicit_determinism_mismatches = explicit_determinism_mismatches,
    };
}

fn runDeviceAuditCase(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    cfg: Config,
    functions: Functions,
    candidate: Candidate,
    head_dim: u16,
    kind: DeviceAuditKind,
) !DeviceAuditResult {
    const plan = deviceAuditPlan(kind);
    const kv_len = plan.kvLen();
    const host_page_size: usize = page_size_default;
    // The backing allocations remain valid and fully initialized even when
    // the declared launch geometry is intentionally malformed.
    const host_physical_capacity: usize = 32;
    const dim: usize = head_dim;
    const q_count = try checkedMul(try checkedMul(plan.q_len, num_heads), dim);
    const kv_width = try checkedMul(num_kv_heads, dim);
    const kv_count = try checkedMul(kv_len, kv_width);
    const output_bytes = try checkedMul(q_count, @sizeOf(f32));
    const row_bytes = try checkedMul(kv_width, @sizeOf(f16));
    const kv_bytes = try checkedMul(host_physical_capacity, row_bytes);
    const table_entries = ceilDiv(kv_len, host_page_size);
    const table_bytes = try checkedMul(table_entries, @sizeOf(u32));
    const seed = cfg.seed ^ (@as(u64, head_dim) << 41) ^ (@as(u64, @intFromEnum(candidate)) << 13) ^ (@as(u64, @intFromEnum(kind)) << 19) ^ 0x6175_6469_745f_6b76;

    const host_q = try allocator.alloc(f32, q_count);
    defer allocator.free(host_q);
    const logical_k = try allocator.alloc(f32, kv_count);
    defer allocator.free(logical_k);
    const logical_v = try allocator.alloc(f32, kv_count);
    defer allocator.free(logical_v);
    const block_table = try allocator.alloc(u32, table_entries);
    defer allocator.free(block_table);
    const host_output = try allocator.alloc(f32, q_count);
    defer allocator.free(host_output);
    const host_repeat = try allocator.alloc(f32, q_count);
    defer allocator.free(host_repeat);

    fillInputs(host_q, logical_k, logical_v, plan.q_len, kv_len, dim, .random, seed);
    fillBlockTable(block_table, .fixed, seed);
    block_table[0] = plan.first_mapped_block;
    const no_table = [_]u32{};

    const q_image = try allocGuardedImage(allocator, output_bytes, 0);
    defer allocator.free(q_image);
    @memcpy(payloadSlice(q_image, output_bytes), std.mem.sliceAsBytes(host_q));
    const k_image = try allocGuardedImage(allocator, kv_bytes, 0);
    defer allocator.free(k_image);
    packPagedF16(payloadSlice(k_image, kv_bytes), logical_k, &no_table, host_page_size, kv_width);
    const v_image = try allocGuardedImage(allocator, kv_bytes, 0);
    defer allocator.free(v_image);
    packPagedF16(payloadSlice(v_image, kv_bytes), logical_v, &no_table, host_page_size, kv_width);
    const table_image = try allocGuardedImage(allocator, table_bytes, 0);
    defer allocator.free(table_image);
    @memcpy(payloadSlice(table_image, table_bytes), std.mem.sliceAsBytes(block_table));
    const output_image = try allocGuardedImage(allocator, output_bytes, 0);
    defer allocator.free(output_image);
    fillOutputPoison(output_image, output_bytes);

    var d_q_all = try deviceImage(ctx, q_image);
    defer d_q_all.free(ctx);
    var d_k_all = try deviceImage(ctx, k_image);
    defer d_k_all.free(ctx);
    var d_v_all = try deviceImage(ctx, v_image);
    defer d_v_all.free(ctx);
    var d_table_all = try deviceImage(ctx, table_image);
    defer d_table_all.free(ctx);
    var d_output_all = try deviceImage(ctx, output_image);
    defer d_output_all.free(ctx);

    const d_q = devicePayloadView(d_q_all, output_bytes);
    const d_k = devicePayloadView(d_k_all, kv_bytes);
    const d_v = devicePayloadView(d_v_all, kv_bytes);
    const d_table = devicePayloadView(d_table_all, table_bytes);
    const launch_table: cuda_buffer.DeviceBuffer = if (plan.table_present) d_table else .{};
    const d_output = devicePayloadView(d_output_all, output_bytes);
    const args = LaunchArgs{
        .q_seq_len = try toU32(plan.q_len),
        .kv_seq_len = try toU32(kv_len),
        .head_dim = head_dim,
        .query_position_offset = try toU32(plan.prefix),
        .sliding_window = 0,
        .total_sequence_len = try toU32(kv_len),
        .key_row_bytes = try toU32(row_bytes),
        .value_row_bytes = try toU32(row_bytes),
        .block_count = plan.block_count,
        .page_size_tokens = plan.page_size,
        .physical_token_capacity = plan.physical_capacity,
    };
    const function = functions.candidate(candidate);

    try launchAttention(ctx, function, candidate, d_output, d_q, d_k, d_v, launch_table, args);
    const first_readback = try readDeviceImage(allocator, ctx, d_output_all);
    defer allocator.free(first_readback);
    try copyPayloadToF32(host_output, first_readback);
    var determinism_mismatches: usize = 0;
    for (1..cfg.repeats) |_| {
        try launchAttention(ctx, function, candidate, d_output, d_q, d_k, d_v, launch_table, args);
        const repeat_readback = try readDeviceImage(allocator, ctx, d_output_all);
        defer allocator.free(repeat_readback);
        try copyPayloadToF32(host_repeat, repeat_readback);
        determinism_mismatches += try countBitwiseMismatches(host_output, host_repeat);
    }

    var readonly_mismatches: usize = 0;
    readonly_mismatches += try verifyDeviceUnchanged(allocator, ctx, d_q_all, q_image);
    readonly_mismatches += try verifyDeviceUnchanged(allocator, ctx, d_k_all, k_image);
    readonly_mismatches += try verifyDeviceUnchanged(allocator, ctx, d_v_all, v_image);
    readonly_mismatches += try verifyDeviceUnchanged(allocator, ctx, d_table_all, table_image);
    const final_readback = try readDeviceImage(allocator, ctx, d_output_all);
    defer allocator.free(final_readback);

    return .{
        .candidate = candidate,
        .head_dim = head_dim,
        .plan = plan,
        .element_count = q_count,
        .unwritten = countPoison(host_output),
        .non_finite_count = countNonFinite(host_output),
        .nonzero_count = countNonZero(host_output),
        .canary_mismatches = countOutputCanaryMismatches(first_readback, output_bytes) + countOutputCanaryMismatches(final_readback, output_bytes),
        .readonly_mismatches = readonly_mismatches,
        .determinism_mismatches = determinism_mismatches,
        .output_image_mismatches = try countByteMismatches(output_image, first_readback),
    };
}

fn writeResultHuman(writer: anytype, result: CaseResult, cfg: Config, artifact_sha256: []const u8) !void {
    const spec = result.spec;
    try writer.print(
        "candidate={s} head_dim={d} q_len={d} prefix={d} kv_len={d} window={d} page_size={d} capacity_mode={s} mapped_blocks={d} launch_block_count={d} block_table_null={s} physical_capacity={d} page_order={s} pattern={s} artifact_sha256={s} status={s}\n",
        .{ spec.candidate.label(), spec.head_dim, spec.q_len, spec.prefix, result.kv_len, spec.sliding_window, result.page_size, spec.capacity_mode.label(), result.mapped_blocks, result.launch_block_count, if (result.block_table_null) "true" else "false", result.physical_capacity, spec.page_order.label(), spec.pattern.label(), artifact_sha256, if (result.passes(cfg)) "pass" else "fail" },
    );
    try writer.print(
        "  diff: elements={d} bitwise_mismatches={d} nonfinite={d} max_abs={e:.9} max_rel={e:.9} max_ulp={d} rms={e:.9} rms_normalized={e:.9}",
        .{ result.diff.element_count, result.diff.bitwise_mismatch_count, result.diff.non_finite_count, result.diff.max_abs, result.diff.max_rel, result.diff.max_ulp, result.diff.rms_error, result.diff.rms_normalized_error },
    );
    if (result.diff.first_mismatch_index) |index| try writer.print(
        " first_mismatch={d} reference_bits=0x{x:0>8} candidate_bits=0x{x:0>8}",
        .{ index, result.diff.first_reference_bits.?, result.diff.first_candidate_bits.? },
    );
    try writer.writeByte('\n');
    try writer.print(
        "  integrity: poisoned_unused_rows={d} baseline_unwritten={d} candidate_unwritten={d} baseline_canary={d} candidate_canary={d} readonly={d} baseline_nondeterministic={d} candidate_nondeterministic={d}\n",
        .{ result.poisoned_unused_rows, result.integrity.baseline_unwritten, result.integrity.candidate_unwritten, result.integrity.baseline_canary_mismatches, result.integrity.candidate_canary_mismatches, result.integrity.readonly_mismatches, result.integrity.baseline_determinism_mismatches, result.integrity.candidate_determinism_mismatches },
    );
    if (result.timing.iterations != 0) {
        const baseline_us = result.timing.baselineMeanLaunchUs();
        const candidate_us = result.timing.candidateMeanLaunchUs();
        try writer.print(
            "  paired_timing: order=alternating-AB-BA pairs={d} iterations={d} baseline_us={d:.3} candidate_us={d:.3} speedup={d:.4}x baseline_cv={d:.5} candidate_cv={d:.5} max_cv={d:.5} baseline_pair_range_us=[{d},{d}] candidate_pair_range_us=[{d},{d}]\n",
            .{ result.timing.pairs, result.timing.iterations, baseline_us, candidate_us, if (candidate_us > 0) baseline_us / candidate_us else 0, result.timing.baselineCv(), result.timing.candidateCv(), cfg.max_timing_cv, result.timing.baseline_min_pair_us, result.timing.baseline_max_pair_us, result.timing.candidate_min_pair_us, result.timing.candidate_max_pair_us },
        );
    }
}

fn writeMetamorphicHuman(writer: anytype, result: MetamorphicResult, artifact_sha256: []const u8) !void {
    try writer.print(
        "metamorphic=identity-null-vs-explicit-fixed candidate={s} head_dim={d} q_len={d} prefix={d} kv_len={d} page_size={d} mapped_blocks={d} shared_seed=0x{x} artifact_sha256={s} status={s}\n",
        .{ result.candidate.label(), result.head_dim, result.q_len, result.prefix, result.kv_len, result.page_size, result.mapped_blocks, result.seed, artifact_sha256, if (result.passes()) "pass" else "fail" },
    );
    try writer.print(
        "  bitwise_mismatches={d} nonfinite={d} identity_unwritten={d} explicit_unwritten={d} identity_canary={d} explicit_canary={d} readonly={d} identity_nondeterministic={d} explicit_nondeterministic={d}\n",
        .{ result.diff.bitwise_mismatch_count, result.diff.non_finite_count, result.identity_unwritten, result.explicit_unwritten, result.identity_canary_mismatches, result.explicit_canary_mismatches, result.readonly_mismatches, result.identity_determinism_mismatches, result.explicit_determinism_mismatches },
    );
}

fn writeDeviceAuditHuman(writer: anytype, result: DeviceAuditResult, artifact_sha256: []const u8) !void {
    const plan = result.plan;
    try writer.print(
        "device_audit={s} category={s} expectation={s} candidate={s} head_dim={d} q_len={d} prefix={d} kv_len={d} block_table_null={s} block_count={d} page_size={d} physical_capacity={d} first_mapped_block={d} layout_contract_valid={s} artifact_sha256={s} status={s}\n",
        .{ plan.kind.label(), plan.kind.category(), @tagName(plan.expectation), result.candidate.label(), result.head_dim, plan.q_len, plan.prefix, plan.kvLen(), if (plan.table_present) "false" else "true", plan.block_count, plan.page_size, plan.physical_capacity, plan.first_mapped_block, if (pageLayoutContractValid(plan)) "true" else "false", artifact_sha256, if (result.passes()) "pass" else "fail" },
    );
    try writer.print(
        "  elements={d} unwritten={d} nonfinite={d} nonzero={d} canary={d} readonly={d} nondeterministic={d} output_image_mismatches={d}\n",
        .{ result.element_count, result.unwritten, result.non_finite_count, result.nonzero_count, result.canary_mismatches, result.readonly_mismatches, result.determinism_mismatches, result.output_image_mismatches },
    );
}

fn qualificationPass(results: []const CaseResult, metamorphic: []const MetamorphicResult, audits: []const DeviceAuditResult, cfg: Config) bool {
    for (results) |result| if (!result.passes(cfg)) return false;
    for (metamorphic) |result| if (!result.passes()) return false;
    for (audits) |result| if (!result.passes()) return false;
    return true;
}

fn writeResultsJson(writer: anytype, results: []const CaseResult, metamorphic: []const MetamorphicResult, audits: []const DeviceAuditResult, cfg: Config, device_name: []const u8, artifact_sha256: []const u8) !void {
    const pass = qualificationPass(results, metamorphic, audits, cfg);
    try writer.print(
        "{{\"schema\":\"antfly.cuda_paged_prefill_diff.v1\",\"device\":\"{s}\",\"artifact\":{{\"mode\":\"{s}\",\"target\":\"{s}\",\"format\":\"{s}\",\"sha256\":\"{s}\"}},\"baseline\":\"{s}\",\"matrix\":\"{s}\",\"numerical_gate\":{{\"max_abs\":{e:.12},\"max_rms_normalized\":{e:.12},\"rms_reference_floor\":{e:.12},\"require_bitwise\":{s}}},\"timing_gate\":{{\"order\":\"alternating-AB-BA\",\"max_cv\":{e:.12}}},\"pass\":{s},\"results\":[",
        .{ device_name, cuda_artifact.mode, cuda_artifact.target, cuda_artifact.format, artifact_sha256, baseline_kernel_name, @tagName(cfg.matrix), cfg.max_abs, cfg.max_rms_normalized, rms_reference_floor, if (cfg.require_bitwise) "true" else "false", cfg.max_timing_cv, if (pass) "true" else "false" },
    );
    for (results, 0..) |result, index| {
        if (index != 0) try writer.writeByte(',');
        const spec = result.spec;
        try writer.print(
            "{{\"candidate\":\"{s}\",\"kernel\":\"{s}\",\"artifact_sha256\":\"{s}\",\"head_dim\":{d},\"q_len\":{d},\"prefix\":{d},\"kv_len\":{d},\"sliding_window\":{d},\"page_size\":{d},\"capacity_mode\":\"{s}\",\"mapped_blocks\":{d},\"launch_block_count\":{d},\"block_table_null\":{s},\"physical_capacity\":{d},\"poisoned_unused_rows\":{d},\"page_order\":\"{s}\",\"pattern\":\"{s}\",\"pass\":{s},",
            .{
                spec.candidate.label(),
                spec.candidate.kernelName(),
                artifact_sha256,
                spec.head_dim,
                spec.q_len,
                spec.prefix,
                result.kv_len,
                spec.sliding_window,
                result.page_size,
                spec.capacity_mode.label(),
                result.mapped_blocks,
                result.launch_block_count,
                if (result.block_table_null) "true" else "false",
                result.physical_capacity,
                result.poisoned_unused_rows,
                spec.page_order.label(),
                spec.pattern.label(),
                if (result.passes(cfg)) "true" else "false",
            },
        );
        try writer.print(
            "\"diff\":{{\"elements\":{d},\"bitwise_mismatches\":{d},\"nonfinite\":{d},\"max_abs\":{e:.12},\"max_rel\":{e:.12},\"max_ulp\":{d},\"rms\":{e:.12},\"rms_normalized\":{e:.12}}},\"integrity\":{{\"baseline_unwritten\":{d},\"candidate_unwritten\":{d},\"baseline_canary_mismatches\":{d},\"candidate_canary_mismatches\":{d},\"readonly_mismatches\":{d},\"baseline_determinism_mismatches\":{d},\"candidate_determinism_mismatches\":{d}}},",
            .{
                result.diff.element_count,
                result.diff.bitwise_mismatch_count,
                result.diff.non_finite_count,
                result.diff.max_abs,
                result.diff.max_rel,
                result.diff.max_ulp,
                result.diff.rms_error,
                result.diff.rms_normalized_error,
                result.integrity.baseline_unwritten,
                result.integrity.candidate_unwritten,
                result.integrity.baseline_canary_mismatches,
                result.integrity.candidate_canary_mismatches,
                result.integrity.readonly_mismatches,
                result.integrity.baseline_determinism_mismatches,
                result.integrity.candidate_determinism_mismatches,
            },
        );
        const baseline_us = result.timing.baselineMeanLaunchUs();
        const candidate_us = result.timing.candidateMeanLaunchUs();
        try writer.print(
            "\"timing\":{{\"order\":\"alternating-AB-BA\",\"pairs\":{d},\"iterations\":{d},\"baseline_total_us\":{d},\"candidate_total_us\":{d},\"baseline_mean_launch_us\":{d:.6},\"candidate_mean_launch_us\":{d:.6},\"speedup\":{d:.6},\"baseline_cv\":{d:.9},\"candidate_cv\":{d:.9},\"max_cv\":{d:.9},\"cv_pass\":{s}}}}}",
            .{ result.timing.pairs, result.timing.iterations, result.timing.baseline_total_us, result.timing.candidate_total_us, baseline_us, candidate_us, if (candidate_us > 0) baseline_us / candidate_us else 0, result.timing.baselineCv(), result.timing.candidateCv(), cfg.max_timing_cv, if (result.timing.passes(cfg.max_timing_cv)) "true" else "false" },
        );
    }
    try writer.writeAll("],\"metamorphic_results\":[");
    for (metamorphic, 0..) |result, index| {
        if (index != 0) try writer.writeByte(',');
        try writer.print(
            "{{\"candidate\":\"{s}\",\"kernel\":\"{s}\",\"artifact_sha256\":\"{s}\",\"head_dim\":{d},\"q_len\":{d},\"prefix\":{d},\"kv_len\":{d},\"page_size\":{d},\"mapped_blocks\":{d},\"shared_seed\":{d},\"identity\":{{\"block_table_null\":true,\"block_count\":0}},\"explicit\":{{\"block_table_null\":false,\"block_count\":{d}}},\"pass\":{s},\"diff\":{{\"elements\":{d},\"bitwise_mismatches\":{d},\"nonfinite\":{d},\"max_abs\":{e:.12},\"max_ulp\":{d}}},\"integrity\":{{\"identity_unwritten\":{d},\"explicit_unwritten\":{d},\"identity_canary_mismatches\":{d},\"explicit_canary_mismatches\":{d},\"readonly_mismatches\":{d},\"identity_determinism_mismatches\":{d},\"explicit_determinism_mismatches\":{d}}}}}",
            .{ result.candidate.label(), result.candidate.kernelName(), artifact_sha256, result.head_dim, result.q_len, result.prefix, result.kv_len, result.page_size, result.mapped_blocks, result.seed, result.mapped_blocks, if (result.passes()) "true" else "false", result.diff.element_count, result.diff.bitwise_mismatch_count, result.diff.non_finite_count, result.diff.max_abs, result.diff.max_ulp, result.identity_unwritten, result.explicit_unwritten, result.identity_canary_mismatches, result.explicit_canary_mismatches, result.readonly_mismatches, result.identity_determinism_mismatches, result.explicit_determinism_mismatches },
        );
    }
    try writer.writeAll("],\"device_audit_results\":[");
    for (audits, 0..) |result, index| {
        if (index != 0) try writer.writeByte(',');
        const plan = result.plan;
        try writer.print(
            "{{\"kind\":\"{s}\",\"category\":\"{s}\",\"expectation\":\"{s}\",\"candidate\":\"{s}\",\"kernel\":\"{s}\",\"artifact_sha256\":\"{s}\",\"head_dim\":{d},\"q_len\":{d},\"prefix\":{d},\"kv_len\":{d},\"block_table_null\":{s},\"block_count\":{d},\"page_size\":{d},\"physical_capacity\":{d},\"first_mapped_block\":{d},\"layout_contract_valid\":{s},\"pass\":{s},\"evidence\":{{\"elements\":{d},\"unwritten\":{d},\"nonfinite\":{d},\"nonzero\":{d},\"canary_mismatches\":{d},\"readonly_mismatches\":{d},\"determinism_mismatches\":{d},\"output_image_mismatches\":{d}}}}}",
            .{ plan.kind.label(), plan.kind.category(), @tagName(plan.expectation), result.candidate.label(), result.candidate.kernelName(), artifact_sha256, result.head_dim, plan.q_len, plan.prefix, plan.kvLen(), if (plan.table_present) "false" else "true", plan.block_count, plan.page_size, plan.physical_capacity, plan.first_mapped_block, if (pageLayoutContractValid(plan)) "true" else "false", if (result.passes()) "true" else "false", result.element_count, result.unwritten, result.non_finite_count, result.nonzero_count, result.canary_mismatches, result.readonly_mismatches, result.determinism_mismatches, result.output_image_mismatches },
        );
    }
    try writer.writeAll("]}\n");
}

fn writeUsage(writer: anytype) !void {
    try writer.writeAll(
        \\Usage: zig build quant-kernel-cuda-paged-prefill-diff -Dcuda=true -Dcuda-artifacts=sm89 -- [options]
        \\  --candidate exact|warp|all       Default: all
        \\  --head-dim 256|512|all          Default: all
        \\  --q-len 2|15|16|17|31|32|33|128|512|all
        \\  --prefix 0|511|512|2003|all     Prefix tokens before this chunk
        \\  --window global|N|all           Default: all (global plus 128)
        \\  --local-window N                Local size used by --window all
        \\  --page-size N|all               Default: all (1, 3, 16, 256)
        \\  --capacity minimal|extra|all    Complete-page capacity; default: all
        \\  --page-order identity|fixed|reversed|permuted|all
        \\  --pattern random|near-tie|cancellation|signed-zero|subnormal|all
        \\  --matrix pairwise|cartesian     Default: pairwise bounded coverage
        \\  --seed N                        Decimal or 0x-prefixed deterministic seed
        \\  --repeats N                     Determinism launches; default: 3
        \\  --iterations N                  Launches per timing sample; default: 0
        \\  --timing-pairs N                Alternating AB/BA pairs; default: 5
        \\  --max-abs X                     Numeric gate; default: 5e-4
        \\  --max-rms-normalized X          Numeric gate; default: 2e-4
        \\  --max-timing-cv X               AB/BA pair CV gate; default: 0.10
        \\  --require-bitwise               Fail on any output-bit mismatch
        \\  --json                          Emit antfly.cuda_paged_prefill_diff.v1 JSON
        \\  --json-out PATH                 Persist the same JSON evidence to PATH
        \\Exact qualification: --candidate exact --require-bitwise
        \\Warp qualification:  --candidate warp (bounded by both numeric gates)
        \\
    );
}

fn parseConfigFromInit(init: std.process.Init) !Config {
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_iter.next();
    var args: [64][]const u8 = undefined;
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
    if (!build_options.enable_cuda) return error.CudaDisabled;

    const allocator = std.heap.c_allocator;
    const spec_count = try caseCount(cfg);
    const specs = try allocator.alloc(CaseSpec, spec_count);
    defer allocator.free(specs);
    try fillCaseSpecs(cfg, specs);
    const results = try allocator.alloc(CaseResult, spec_count);
    defer allocator.free(results);
    const metamorphic_count = try checkedMul(cfg.candidate.count(), cfg.headDimCount());
    const metamorphic = try allocator.alloc(MetamorphicResult, metamorphic_count);
    defer allocator.free(metamorphic);
    const audit_count = try checkedMul(metamorphic_count, device_audit_kinds.len);
    const audits = try allocator.alloc(DeviceAuditResult, audit_count);
    defer allocator.free(audits);
    const artifact_sha256 = artifactSha256Hex();

    var ctx = try cuda_context.CudaContext.initDefault();
    defer ctx.deinit();
    var module = try Module.load(&ctx);
    defer module.deinit(&ctx);
    const functions = try module.functions(&ctx);
    for (specs, 0..) |spec, index| results[index] = try runCase(allocator, &ctx, cfg, functions, spec);
    var metamorphic_index: usize = 0;
    var audit_index: usize = 0;
    for (0..cfg.candidate.count()) |candidate_index| {
        const candidate = cfg.candidate.at(candidate_index);
        for (0..cfg.headDimCount()) |head_index| {
            const head_dim = cfg.headDimAt(head_index);
            metamorphic[metamorphic_index] = try runMetamorphicCase(allocator, &ctx, cfg, functions, candidate, head_dim);
            metamorphic_index += 1;
            for (device_audit_kinds) |kind| {
                audits[audit_index] = try runDeviceAuditCase(allocator, &ctx, cfg, functions, candidate, head_dim, kind);
                audit_index += 1;
            }
        }
    }
    std.debug.assert(metamorphic_index == metamorphic.len);
    std.debug.assert(audit_index == audits.len);

    if (cfg.json_out) |path| {
        var file = if (std.fs.path.isAbsolute(path))
            try std.Io.Dir.createFileAbsolute(init.io, path, .{ .truncate = true })
        else
            try std.Io.Dir.cwd().createFile(init.io, path, .{ .truncate = true });
        defer file.close(init.io);
        var file_buffer: [8192]u8 = undefined;
        var file_writer = file.writer(init.io, &file_buffer);
        try writeResultsJson(&file_writer.interface, results, metamorphic, audits, cfg, ctx.info.nameSlice(), &artifact_sha256);
        try file_writer.interface.flush();
    }

    if (cfg.json) {
        try writeResultsJson(stdout, results, metamorphic, audits, cfg, ctx.info.nameSlice(), &artifact_sha256);
    } else if (cfg.json_out) |path| {
        const pass = qualificationPass(results, metamorphic, audits, cfg);
        try stdout.print("CUDA paged prefill evidence: path={s} artifact_sha256={s} differential_cases={d} metamorphic_cases={d} device_audit_cases={d} pass={s}\n", .{ path, &artifact_sha256, results.len, metamorphic.len, audits.len, if (pass) "true" else "false" });
    } else {
        try stdout.print(
            "CUDA paged prefill differential: device={s} cc={d}.{d} artifact_target={s} artifact_sha256={s} baseline={s} candidates={s} matrix={s} differential_cases={d} metamorphic_cases={d} device_audit_cases={d} heads=8 kv_heads=1 storage=f16 max_abs={e:.6} max_rms_normalized={e:.6} rms_reference_floor={e:.6} max_timing_cv={d:.5} require_bitwise={s}\n",
            .{ ctx.info.nameSlice(), ctx.info.compute_major, ctx.info.compute_minor, cuda_artifact.target, &artifact_sha256, baseline_kernel_name, @tagName(cfg.candidate), @tagName(cfg.matrix), results.len, metamorphic.len, audits.len, cfg.max_abs, cfg.max_rms_normalized, rms_reference_floor, cfg.max_timing_cv, if (cfg.require_bitwise) "true" else "false" },
        );
        for (results) |result| try writeResultHuman(stdout, result, cfg, &artifact_sha256);
        for (metamorphic) |result| try writeMetamorphicHuman(stdout, result, &artifact_sha256);
        for (audits) |result| try writeDeviceAuditHuman(stdout, result, &artifact_sha256);
    }
    try stdout.flush();
    if (!qualificationPass(results, metamorphic, audits, cfg)) return error.PagedPrefillDifferentialExceeded;
}

test "paged prefill diff parses its complete qualification surface" {
    const cfg = try parseConfig(&.{
        "--candidate",       "warp",      "--head-dim",           "512",               "--q-len",         "33",
        "--prefix",          "2003",      "--window",             "256",               "--local-window",  "192",
        "--page-size",       "32",        "--capacity",           "extra",             "--page-order",    "permuted",
        "--pattern",         "subnormal", "--matrix",             "cartesian",         "--seed",          "0x42",
        "--repeats",         "4",         "--iterations",         "100",               "--timing-pairs",  "7",
        "--max-abs",         "0.001",     "--max-rms-normalized", "0.0003",            "--max-timing-cv", "0.05",
        "--require-bitwise", "--json",    "--json-out",           "/tmp/prefill.json",
    });
    try std.testing.expectEqual(CandidateSelection.warp, cfg.candidate);
    try std.testing.expectEqual(@as(?u16, 512), cfg.head_dim);
    try std.testing.expectEqual(@as(?usize, 33), cfg.q_len);
    try std.testing.expectEqual(@as(?usize, 2003), cfg.prefix);
    try std.testing.expectEqual(@as(?u32, 256), cfg.window);
    try std.testing.expectEqual(@as(u32, 192), cfg.local_window);
    try std.testing.expectEqual(@as(?usize, 32), cfg.page_size);
    try std.testing.expectEqual(CapacitySelection.extra, cfg.capacity);
    try std.testing.expectEqual(PageOrderSelection.permuted, cfg.page_order);
    try std.testing.expectEqual(PatternSelection.subnormal, cfg.pattern);
    try std.testing.expectEqual(Matrix.cartesian, cfg.matrix);
    try std.testing.expectEqual(@as(usize, 4), cfg.repeats);
    try std.testing.expectEqual(@as(usize, 100), cfg.iterations);
    try std.testing.expectEqual(@as(usize, 7), cfg.timing_pairs);
    try std.testing.expectApproxEqAbs(@as(f64, 0.001), cfg.max_abs, 1e-15);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0003), cfg.max_rms_normalized, 1e-15);
    try std.testing.expectApproxEqAbs(@as(f64, 0.05), cfg.max_timing_cv, 1e-15);
    try std.testing.expect(cfg.require_bitwise);
    try std.testing.expect(cfg.json);
    try std.testing.expectEqualStrings("/tmp/prefill.json", cfg.json_out.?);
}

test "paged prefill diff rejects unsupported shapes and malformed controls" {
    try std.testing.expectError(error.InvalidCandidate, parseConfig(&.{ "--candidate", "mma" }));
    try std.testing.expectError(error.InvalidHeadDim, parseConfig(&.{ "--head-dim", "128" }));
    try std.testing.expectError(error.InvalidQueryLength, parseConfig(&.{ "--q-len", "14" }));
    try std.testing.expectError(error.InvalidPrefix, parseConfig(&.{ "--prefix", "2000" }));
    try std.testing.expectError(error.InvalidWindow, parseConfig(&.{ "--window", "1" }));
    try std.testing.expectError(error.InvalidPageSize, parseConfig(&.{ "--page-size", "0" }));
    try std.testing.expectError(error.InvalidCapacity, parseConfig(&.{ "--capacity", "tight" }));
    try std.testing.expectError(error.InvalidPageOrder, parseConfig(&.{ "--page-order", "random" }));
    try std.testing.expectError(error.InvalidPattern, parseConfig(&.{ "--pattern", "zeros" }));
    try std.testing.expectError(error.InvalidMatrix, parseConfig(&.{ "--matrix", "huge" }));
    try std.testing.expectError(error.InvalidRepeats, parseConfig(&.{ "--repeats", "1" }));
    try std.testing.expectError(error.InvalidTimingPairs, parseConfig(&.{ "--timing-pairs", "0" }));
    try std.testing.expectError(error.InvalidMaxAbs, parseConfig(&.{ "--max-abs", "-0.1" }));
    try std.testing.expectError(error.InvalidMaxRmsNormalized, parseConfig(&.{ "--max-rms-normalized", "nan" }));
    try std.testing.expectError(error.InvalidMaxTimingCv, parseConfig(&.{ "--max-timing-cv", "-0.1" }));
}

test "page-order selection and config include the production identity contract" {
    const expected = [_]PageOrder{ .identity, .fixed, .reversed, .permuted };
    try std.testing.expectEqual(expected.len, PageOrderSelection.all.count());
    for (expected, 0..) |order, index| {
        try std.testing.expectEqual(order, PageOrderSelection.all.at(index));
    }

    const cfg = try parseConfig(&.{ "--page-order", "identity" });
    try std.testing.expectEqual(PageOrderSelection.identity, cfg.page_order);
    try std.testing.expectEqual(@as(usize, 1), cfg.page_order.count());
    try std.testing.expectEqual(PageOrder.identity, cfg.page_order.at(0));
}

test "page-table contract distinguishes null identity from every explicit map" {
    const identity = pageTableContract(.identity, 7);
    try std.testing.expectEqual(@as(usize, 0), identity.entry_count);
    try std.testing.expectEqual(@as(usize, 0), identity.launch_block_count);
    try std.testing.expect(identity.block_table_null);

    for ([_]PageOrder{ .fixed, .reversed, .permuted }) |order| {
        const explicit = pageTableContract(order, 7);
        try std.testing.expectEqual(@as(usize, 7), explicit.entry_count);
        try std.testing.expectEqual(@as(usize, 7), explicit.launch_block_count);
        try std.testing.expect(!explicit.block_table_null);
    }
}

test "page geometry selection covers representative sizes and both capacities" {
    const cfg = try parseConfig(&.{});
    try std.testing.expectEqual(@as(usize, 8), cfg.geometryCount());
    var cursor: usize = 0;
    for (supported_page_sizes) |page_size| for ([_]CapacityMode{ .minimal, .extra }) |capacity_mode| {
        const geometry = cfg.geometryAt(cursor);
        try std.testing.expectEqual(page_size, geometry.page_size);
        try std.testing.expectEqual(capacity_mode, geometry.capacity_mode);
        cursor += 1;
    };

    const filtered = try parseConfig(&.{ "--page-size", "3", "--capacity", "minimal" });
    try std.testing.expectEqual(@as(usize, 1), filtered.geometryCount());
    try std.testing.expectEqual(PageGeometry{ .page_size = 3, .capacity_mode = .minimal }, filtered.geometryAt(0));

    try std.testing.expectEqual(@as(usize, 17), try physicalCapacity(17, 1, .minimal));
    try std.testing.expectEqual(@as(usize, 18), try physicalCapacity(17, 1, .extra));
    try std.testing.expectEqual(@as(usize, 18), try physicalCapacity(17, 3, .minimal));
    try std.testing.expectEqual(@as(usize, 21), try physicalCapacity(17, 3, .extra));
    try std.testing.expectEqual(@as(usize, 32), try physicalCapacity(17, 16, .minimal));
    try std.testing.expectEqual(@as(usize, 48), try physicalCapacity(17, 16, .extra));
    try std.testing.expectEqual(@as(usize, 256), try physicalCapacity(17, 256, .minimal));
    try std.testing.expectEqual(@as(usize, 512), try physicalCapacity(17, 256, .extra));
}

test "device audit truth table separates malformed launches from bounded data" {
    try std.testing.expectEqual(@as(usize, 7), device_audit_kinds.len);
    for (device_audit_kinds[0..6]) |kind| {
        const plan = deviceAuditPlan(kind);
        try std.testing.expectEqual(DeviceAuditExpectation.no_write, plan.expectation);
        try std.testing.expect(!pageLayoutContractValid(plan));
    }

    const huge = deviceAuditPlan(.huge_mapped_entry);
    try std.testing.expectEqual(DeviceAuditExpectation.bounded_zero, huge.expectation);
    try std.testing.expect(pageLayoutContractValid(huge));
    try std.testing.expect(!mappedBlockFits(huge.first_mapped_block, huge.page_size, huge.physical_capacity));
    // This value deliberately aliases row zero under unchecked u32 multiply.
    try std.testing.expectEqual(@as(u32, 0), huge.first_mapped_block *% huge.page_size);
}

test "pairwise layout covers every required axis for each candidate and head dim" {
    const cfg = try parseConfig(&.{});
    try std.testing.expectEqual(@as(usize, 36), try caseCount(cfg));
    var specs: [36]CaseSpec = undefined;
    try fillCaseSpecs(cfg, &specs);
    for ([_]Candidate{ .exact, .warp }) |candidate| for ([_]u16{ 256, 512 }) |head_dim| {
        var q_seen = [_]bool{false} ** supported_q_lengths.len;
        var prefix_seen = [_]bool{false} ** supported_prefixes.len;
        var order_seen = [_]bool{false} ** 4;
        var pattern_seen = [_]bool{false} ** 5;
        var window_seen = [_]bool{false} ** 2;
        var geometry_seen = [_][2]bool{[_]bool{false} ** 2} ** supported_page_sizes.len;
        for (specs) |spec| {
            if (spec.candidate != candidate or spec.head_dim != head_dim) continue;
            for (supported_q_lengths, 0..) |value, index| {
                if (spec.q_len == value) q_seen[index] = true;
            }
            for (supported_prefixes, 0..) |value, index| {
                if (spec.prefix == value) prefix_seen[index] = true;
            }
            order_seen[@intFromEnum(spec.page_order)] = true;
            pattern_seen[@intFromEnum(spec.pattern)] = true;
            window_seen[@intFromBool(spec.sliding_window != 0)] = true;
            for (supported_page_sizes, 0..) |page_size, page_index| {
                if (spec.page_size == page_size) geometry_seen[page_index][@intFromEnum(spec.capacity_mode)] = true;
            }
        }
        for (q_seen) |seen| try std.testing.expect(seen);
        for (prefix_seen) |seen| try std.testing.expect(seen);
        for (order_seen) |seen| try std.testing.expect(seen);
        for (pattern_seen) |seen| try std.testing.expect(seen);
        for (window_seen) |seen| try std.testing.expect(seen);
        for (geometry_seen) |capacity_seen| for (capacity_seen) |seen| try std.testing.expect(seen);
    };
}

test "cartesian layout count is exact and launch contracts are explicit" {
    const cfg = try parseConfig(&.{ "--matrix", "cartesian" });
    try std.testing.expectEqual(@as(usize, 46080), try caseCount(cfg));
    try std.testing.expectEqual(@as(u32, 256), Candidate.exact.threads(256));
    try std.testing.expectEqual(@as(u32, 512), Candidate.exact.threads(512));
    try std.testing.expectEqual(@as(u32, 256), Candidate.warp.threads(256));
    try std.testing.expectEqual(@as(u32, 256), Candidate.warp.threads(512));
    try std.testing.expectEqual(@as(usize, 1), ceilDiv(16, tile_queries));
    try std.testing.expectEqual(@as(usize, 2), ceilDiv(17, tile_queries));
}

test "candidate symbol constants stay aligned with CUDA runtime loading" {
    const runtime_source = @embedFile("ops/cuda/kernels.zig");
    try std.testing.expect(std.mem.indexOf(u8, runtime_source, exact_kernel_name) != null);
    try std.testing.expect(std.mem.indexOf(u8, runtime_source, warp_kernel_name) != null);
    try std.testing.expect(!std.mem.containsAtLeast(u8, exact_kernel_name, 1, "turboquant"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, warp_kernel_name, 1, "turboquant"));
}

test "embedded artifact SHA-256 is stable lowercase provenance" {
    const first = artifactSha256Hex();
    const second = artifactSha256Hex();
    try std.testing.expectEqualSlices(u8, &first, &second);
    for (first) |byte| try std.testing.expect((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'));
}

test "paged F16 layout permutes pages and leaves unmapped rows poisoned" {
    const no_table = [_]u32{};
    try std.testing.expectEqual(@as(usize, 5), physicalToken(5, &no_table, 4));

    var fixed = [_]u32{ 0, 0, 0 };
    fillBlockTable(&fixed, .fixed, 0);
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 2 }, &fixed);

    var table = [_]u32{ 0, 0, 0 };
    fillBlockTable(&table, .reversed, 0);
    try std.testing.expectEqualSlices(u32, &.{ 2, 1, 0 }, &table);
    try std.testing.expectEqual(@as(usize, 9), physicalToken(1, &table, 4));
    try std.testing.expectEqual(@as(usize, 5), physicalToken(5, &table, 4));

    const logical = [_]f32{ 1.0, -2.0, 0.5, -0.5 };
    var packed_bytes = [_]u8{0} ** 16;
    packPagedF16(&packed_bytes, &logical, &no_table, 1, 4);
    const first_bits: u16 = @bitCast(@as(f16, 1.0));
    try std.testing.expectEqual(@as(u8, @truncate(first_bits)), packed_bytes[0]);
    try std.testing.expectEqual(@as(u8, @truncate(first_bits >> 8)), packed_bytes[1]);
    // The second physical row is unmapped and remains an F16 quiet NaN.
    try std.testing.expectEqual(@as(u8, 0x00), packed_bytes[8]);
    try std.testing.expectEqual(@as(u8, 0x7e), packed_bytes[9]);

    var permuted = [_]u32{ 0, 0, 0, 0, 0, 0, 0 };
    fillBlockTable(&permuted, .permuted, 0x42);
    var seen = [_]bool{false} ** permuted.len;
    for (permuted) |physical| {
        try std.testing.expect(physical < permuted.len);
        try std.testing.expect(!seen[physical]);
        seen[physical] = true;
    }
}

test "diff metrics preserve signed-zero evidence and report normalized RMS" {
    const reference = [_]f32{ 0.0, 1.0, -2.0, 4.0 };
    const candidate = [_]f32{ negativeZero(), 1.0000001, -2.0, 3.5 };
    const stats = try compareOutputs(&reference, &candidate);
    try std.testing.expectEqual(@as(usize, 3), stats.bitwise_mismatch_count);
    try std.testing.expectEqual(@as(usize, 0), stats.first_mismatch_index.?);
    try std.testing.expectEqual(@as(u64, 0), ulpDistance(0.0, negativeZero()));
    try std.testing.expect(stats.max_abs >= 0.5);
    try std.testing.expect(stats.max_ulp > 0);
    try std.testing.expect(stats.rms_error > 0);
    try std.testing.expect(stats.rms_normalized_error > 0);
}

test "numerical gates fail closed and bitwise qualification is additive" {
    var cfg = Config{};
    var diff = DiffStats{
        .max_abs = default_max_abs * 0.5,
        .rms_normalized_error = default_max_rms_normalized * 0.5,
        .bitwise_mismatch_count = 7,
    };
    // The bounded warp-style gate tolerates non-bitwise output only while both
    // quantitative limits hold.
    try std.testing.expect(numericalGatePass(diff, cfg));
    diff.max_abs = default_max_abs * 2.0;
    try std.testing.expect(!numericalGatePass(diff, cfg));
    diff.max_abs = default_max_abs * 0.5;
    diff.rms_normalized_error = default_max_rms_normalized * 2.0;
    try std.testing.expect(!numericalGatePass(diff, cfg));
    diff.rms_normalized_error = default_max_rms_normalized * 0.5;
    cfg.require_bitwise = true;
    try std.testing.expect(!numericalGatePass(diff, cfg));
    diff.bitwise_mismatch_count = 0;
    try std.testing.expect(numericalGatePass(diff, cfg));
}

test "AB BA timing rejects unstable coefficient of variation" {
    var stable = TimingStats{
        .pairs = 4,
        .iterations = 10,
        .baseline_total_us = 400,
        .candidate_total_us = 200,
        .baseline_pair_sum_squared = 40_000,
        .candidate_pair_sum_squared = 10_000,
    };
    try std.testing.expectApproxEqAbs(@as(f64, 0), stable.baselineCv(), 1e-15);
    try std.testing.expect(stable.passes(default_max_timing_cv));
    // Pair samples [50, 50, 50, 250] have CV > 0.8.
    stable.baseline_pair_sum_squared = 70_000;
    try std.testing.expect(stable.baselineCv() > 0.8);
    try std.testing.expect(!stable.passes(default_max_timing_cv));
}

test "guard and poison checks distinguish unwritten payload from overruns" {
    const allocator = std.testing.allocator;
    const image = try allocGuardedImage(allocator, 8, 0);
    defer allocator.free(image);
    fillOutputPoison(image, 8);
    var values: [2]f32 = undefined;
    try copyPayloadToF32(&values, image);
    try std.testing.expectEqual(@as(usize, 2), countPoison(&values));
    try std.testing.expectEqual(@as(usize, 0), countOutputCanaryMismatches(image, 8));
    image[0] = 0;
    image[guard_bytes + 8] = 0;
    try std.testing.expectEqual(@as(usize, 2), countOutputCanaryMismatches(image, 8));
}
