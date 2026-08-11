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

//! Raw paged-KV CUDA decode-attention differential harness.
//!
//! The production bundle supplies the handwritten TurboQuant decode kernel.
//! Independently compiled generated cubins supply the score-prework candidate.
//! Keeping the candidate outside the runtime bundle prevents a parity or
//! performance experiment from changing production dispatch.

const std = @import("std");
const build_options = @import("build_options");
const cuda_artifact = @import("ops/cuda/artifact.zig");
const cuda_buffer = @import("ops/cuda/buffer.zig");
const cuda_context = @import("ops/cuda/context.zig");
const cuda_driver = @import("ops/cuda/driver.zig");
const cuda_renderer = @import("graph/quant_kernel_cuda_renderer.zig");

const Pattern = enum {
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

const KeyFormat = enum(u32) {
    polar4 = 0,
    f16 = 2,

    fn label(self: KeyFormat) []const u8 {
        return switch (self) {
            .polar4 => "polar4",
            .f16 => "f16",
        };
    }
};

const KeyFormatSelection = enum {
    all,
    polar4,
    f16,

    fn count(self: KeyFormatSelection) usize {
        return if (self == .all) 2 else 1;
    }

    fn at(self: KeyFormatSelection, index: usize) KeyFormat {
        return switch (self) {
            .all => if (index == 0) .polar4 else .f16,
            .polar4 => .polar4,
            .f16 => .f16,
        };
    }
};

const ValueFormat = enum(u32) {
    f32 = 0,
    f16 = 2,

    fn label(self: ValueFormat) []const u8 {
        return switch (self) {
            .f32 => "f32",
            .f16 => "f16",
        };
    }
};

const ValueFormatSelection = enum {
    all,
    f32,
    f16,

    fn count(self: ValueFormatSelection) usize {
        return if (self == .all) 2 else 1;
    }

    fn at(self: ValueFormatSelection, index: usize) ValueFormat {
        return switch (self) {
            .all => if (index == 0) .f32 else .f16,
            .f32 => .f32,
            .f16 => .f16,
        };
    }
};

const PageOrder = enum {
    identity_null,
    fixed,
    reversed,
    permuted,

    fn label(self: PageOrder) []const u8 {
        return switch (self) {
            .identity_null => "identity-null",
            .fixed => "fixed",
            .reversed => "reversed",
            .permuted => "permuted",
        };
    }
};

const PageOrderSelection = enum {
    all,
    identity_null,
    fixed,
    reversed,
    permuted,

    fn count(self: PageOrderSelection) usize {
        return if (self == .all) 4 else 1;
    }

    fn at(self: PageOrderSelection, index: usize) PageOrder {
        return switch (self) {
            .all => switch (index) {
                0 => .identity_null,
                1 => .fixed,
                2 => .reversed,
                3 => .permuted,
                else => unreachable,
            },
            .identity_null => .identity_null,
            .fixed => .fixed,
            .reversed => .reversed,
            .permuted => .permuted,
        };
    }
};

const Config = struct {
    head_dim: ?u16 = null,
    kv_len: usize = 1024,
    query_position: ?usize = null,
    sliding_window: u32 = 0,
    page_size: usize = 16,
    num_heads: usize = 8,
    num_kv_heads: usize = 1,
    pattern: PatternSelection = .all,
    key_format: KeyFormatSelection = .all,
    value_format: ValueFormatSelection = .all,
    page_order: PageOrderSelection = .all,
    seed: u64 = 0x579a_e418_6d2c_3f01,
    iterations: usize = 0,
    candidate_hd256: ?[]const u8 = null,
    candidate_hd512: ?[]const u8 = null,
    json: bool = false,
    help: bool = false,
};

const DiffStats = struct {
    element_count: usize = 0,
    bitwise_mismatch_count: usize = 0,
    non_finite_mismatch_count: usize = 0,
    max_abs: f32 = 0,
    max_rel: f32 = 0,
    max_ulp: u64 = 0,
    first_mismatch_index: ?usize = null,
};

const OutputIntegrity = struct {
    guard_mutation_count: usize = 0,
    poison_element_count: usize = 0,

    fn passes(self: OutputIntegrity) bool {
        return self.guard_mutation_count == 0 and self.poison_element_count == 0;
    }
};

const InputIntegrity = struct {
    q_mutation_count: usize = 0,
    k_mutation_count: usize = 0,
    v_mutation_count: usize = 0,
    table_mutation_count: usize = 0,
    scalar_mutation_count: usize = 0,
    score_mutation_count: usize = 0,

    fn passes(self: InputIntegrity) bool {
        return self.q_mutation_count == 0 and
            self.k_mutation_count == 0 and
            self.v_mutation_count == 0 and
            self.table_mutation_count == 0 and
            self.scalar_mutation_count == 0 and
            self.score_mutation_count == 0;
    }
};

const ConsumerPairTiming = struct {
    pair_count: usize = 0,
    serial_total_us: u64 = 0,
    tiled64_total_us: u64 = 0,
    serial_cv: f64 = 0,
    tiled64_cv: f64 = 0,

    fn maxCv(self: ConsumerPairTiming) f64 {
        return @max(self.serial_cv, self.tiled64_cv);
    }

    fn stable(self: ConsumerPairTiming) bool {
        return self.pair_count < 3 or self.maxCv() <= timing_cv_limit;
    }
};

const timing_pair_limit: usize = 10;
const timing_cv_limit: f64 = 0.10;
const output_guard_bytes: usize = 256;
const output_prefix_canary: u8 = 0xa5;
const output_suffix_canary: u8 = 0x5a;
const output_poison_bits: u32 = 0x7fc0_0bad;

const CaseResult = struct {
    head_dim: u16,
    kv_len: usize,
    query_position: usize,
    sliding_window: u32,
    page_size: usize,
    block_count: usize,
    num_heads: usize,
    num_kv_heads: usize,
    pattern: Pattern,
    key_format: KeyFormat,
    value_format: ValueFormat,
    page_order: PageOrder,
    serial_vs_production: DiffStats,
    tiled64_vs_production: ?DiffStats,
    tiled64_vs_serial: ?DiffStats,
    tiled64_repeat: ?DiffStats,
    baseline_output_integrity: OutputIntegrity,
    serial_output_integrity: OutputIntegrity,
    tiled64_output_integrity: ?OutputIntegrity,
    input_integrity: InputIntegrity,
    baseline_total_us: u64 = 0,
    score_total_us: u64 = 0,
    serial_consume_total_us: u64 = 0,
    tiled64_consume_total_us: u64 = 0,
    consumer_pair_timing: ConsumerPairTiming = .{},
    iterations: usize = 0,

    fn passes(self: CaseResult) bool {
        if (self.serial_vs_production.bitwise_mismatch_count != 0 or
            self.serial_vs_production.non_finite_mismatch_count != 0)
        {
            return false;
        }
        if (self.tiled64_vs_production) |diff| {
            if (diff.bitwise_mismatch_count != 0 or diff.non_finite_mismatch_count != 0) return false;
        }
        if (self.tiled64_vs_serial) |diff| {
            if (diff.bitwise_mismatch_count != 0 or diff.non_finite_mismatch_count != 0) return false;
        }
        if (self.tiled64_repeat) |diff| {
            if (diff.bitwise_mismatch_count != 0 or diff.non_finite_mismatch_count != 0) return false;
        }
        if (!self.baseline_output_integrity.passes() or
            !self.serial_output_integrity.passes() or
            !self.input_integrity.passes() or
            !self.consumer_pair_timing.stable())
        {
            return false;
        }
        if (self.tiled64_output_integrity) |integrity| return integrity.passes();
        return true;
    }
};

const Module = struct {
    module: cuda_driver.CUmodule = null,

    fn loadEmbedded(ctx: *cuda_context.CudaContext) !Module {
        return loadImage(ctx, cuda_artifact.image.ptr);
    }

    fn loadFile(ctx: *cuda_context.CudaContext, io: std.Io, allocator: std.mem.Allocator, path: []const u8) !Module {
        const image = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(64 * 1024 * 1024));
        defer allocator.free(image);
        return loadImage(ctx, image.ptr);
    }

    fn loadImage(ctx: *cuda_context.CudaContext, image: *const anyopaque) !Module {
        try ctx.makeCurrent();
        var module: cuda_driver.CUmodule = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, image, 0, null, null));
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
};

const CandidateFunctions = struct {
    score: cuda_driver.CUfunction,
    serial_consume: cuda_driver.CUfunction,
    tiled64_consume: cuda_driver.CUfunction,
};

const CandidateProvenance = struct {
    hd256_sha256: ?[std.crypto.hash.sha2.Sha256.digest_length * 2]u8 = null,
    hd512_sha256: ?[std.crypto.hash.sha2.Sha256.digest_length * 2]u8 = null,
};

const CandidateConsumer = enum {
    serial,
    tiled64,
};

const LaunchArgs = struct {
    kv_seq_len: u32,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
    query_position_offset: u32,
    sliding_window: u32,
    total_sequence_len: u32,
    key_row_bytes: u32,
    value_row_bytes: u32,
    block_count: u32,
    page_size_tokens: u32,
    format: u32,
    value_format: u32,
    physical_token_capacity: u32,
    score_capacity: u32,
};

fn checkedMul(a: usize, b: usize) !usize {
    const result = @mulWithOverflow(a, b);
    if (result[1] != 0) return error.InvalidArgument;
    return result[0];
}

fn checkedAdd(a: usize, b: usize) !usize {
    const result = @addWithOverflow(a, b);
    if (result[1] != 0) return error.InvalidArgument;
    return result[0];
}

fn ceilDiv(value: usize, divisor: usize) usize {
    return value / divisor + @intFromBool(value % divisor != 0);
}

fn outputImageSize(payload_bytes: usize) !usize {
    return checkedAdd(try checkedMul(output_guard_bytes, 2), payload_bytes);
}

fn initializeOutputImage(image: []u8, payload_bytes: usize) !void {
    if (image.len != try outputImageSize(payload_bytes) or payload_bytes % @sizeOf(u32) != 0) {
        return error.InvalidArgument;
    }
    @memset(image[0..output_guard_bytes], output_prefix_canary);
    const payload = image[output_guard_bytes..][0..payload_bytes];
    var index: usize = 0;
    while (index < payload.len) : (index += @sizeOf(u32)) {
        payload[index] = @truncate(output_poison_bits);
        payload[index + 1] = @truncate(output_poison_bits >> 8);
        payload[index + 2] = @truncate(output_poison_bits >> 16);
        payload[index + 3] = @truncate(output_poison_bits >> 24);
    }
    @memset(image[output_guard_bytes + payload_bytes ..], output_suffix_canary);
}

fn outputPayloadBuffer(image: cuda_buffer.DeviceBuffer, payload_bytes: usize) !cuda_buffer.DeviceBuffer {
    if (image.len != try outputImageSize(payload_bytes)) return error.InvalidArgument;
    return .{ .ptr = image.ptr + output_guard_bytes, .len = payload_bytes };
}

fn inspectOutputImage(image: []const u8, payload_bytes: usize) !OutputIntegrity {
    if (image.len != try outputImageSize(payload_bytes) or payload_bytes % @sizeOf(u32) != 0) {
        return error.InvalidArgument;
    }
    var integrity = OutputIntegrity{};
    for (image[0..output_guard_bytes]) |value| {
        integrity.guard_mutation_count += @intFromBool(value != output_prefix_canary);
    }
    const payload = image[output_guard_bytes..][0..payload_bytes];
    var index: usize = 0;
    while (index < payload.len) : (index += @sizeOf(u32)) {
        const poisoned = payload[index] == @as(u8, @truncate(output_poison_bits)) and
            payload[index + 1] == @as(u8, @truncate(output_poison_bits >> 8)) and
            payload[index + 2] == @as(u8, @truncate(output_poison_bits >> 16)) and
            payload[index + 3] == @as(u8, @truncate(output_poison_bits >> 24));
        integrity.poison_element_count += @intFromBool(poisoned);
    }
    for (image[output_guard_bytes + payload_bytes ..]) |value| {
        integrity.guard_mutation_count += @intFromBool(value != output_suffix_canary);
    }
    return integrity;
}

fn byteMutationCount(before: []const u8, after: []const u8) !usize {
    if (before.len != after.len) return error.LengthMismatch;
    var count: usize = 0;
    for (before, after) |expected, actual| count += @intFromBool(expected != actual);
    return count;
}

fn parsePattern(value: []const u8) !PatternSelection {
    if (std.mem.eql(u8, value, "all")) return .all;
    if (std.mem.eql(u8, value, "random")) return .random;
    if (std.mem.eql(u8, value, "near-tie")) return .near_tie;
    if (std.mem.eql(u8, value, "cancellation")) return .cancellation;
    return error.InvalidPattern;
}

fn parseKeyFormat(value: []const u8) !KeyFormatSelection {
    if (std.mem.eql(u8, value, "all")) return .all;
    if (std.mem.eql(u8, value, "polar4")) return .polar4;
    if (std.mem.eql(u8, value, "f16")) return .f16;
    return error.InvalidKeyFormat;
}

fn parseValueFormat(value: []const u8) !ValueFormatSelection {
    if (std.mem.eql(u8, value, "all")) return .all;
    if (std.mem.eql(u8, value, "f32")) return .f32;
    if (std.mem.eql(u8, value, "f16")) return .f16;
    return error.InvalidValueFormat;
}

fn parsePageOrder(value: []const u8) !PageOrderSelection {
    if (std.mem.eql(u8, value, "all")) return .all;
    if (std.mem.eql(u8, value, "identity-null")) return .identity_null;
    if (std.mem.eql(u8, value, "fixed")) return .fixed;
    if (std.mem.eql(u8, value, "reversed")) return .reversed;
    if (std.mem.eql(u8, value, "permuted")) return .permuted;
    return error.InvalidPageOrder;
}

fn parseConfig(args: []const []const u8) !Config {
    var cfg = Config{};
    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--head-dim")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.head_dim = if (std.mem.eql(u8, args[index], "all")) null else try std.fmt.parseInt(u16, args[index], 10);
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
        } else if (std.mem.eql(u8, arg, "--page-size")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.page_size = try std.fmt.parseInt(usize, args[index], 10);
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
        } else if (std.mem.eql(u8, arg, "--key-format")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.key_format = try parseKeyFormat(args[index]);
        } else if (std.mem.eql(u8, arg, "--value-format")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.value_format = try parseValueFormat(args[index]);
        } else if (std.mem.eql(u8, arg, "--page-order")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.page_order = try parsePageOrder(args[index]);
        } else if (std.mem.eql(u8, arg, "--seed")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.seed = try std.fmt.parseInt(u64, args[index], 0);
        } else if (std.mem.eql(u8, arg, "--iterations")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.iterations = try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--candidate-hd256")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.candidate_hd256 = args[index];
        } else if (std.mem.eql(u8, arg, "--candidate-hd512")) {
            index += 1;
            if (index >= args.len) return error.MissingValue;
            cfg.candidate_hd512 = args[index];
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
    if (cfg.head_dim) |head_dim| if (head_dim != 256 and head_dim != 512) return error.InvalidHeadDim;
    if (cfg.kv_len == 0 or cfg.kv_len > cuda_renderer.generated_attention_score_prework_max_kv_tokens) return error.InvalidKvLength;
    if (cfg.query_position) |position| if (position >= cfg.kv_len) return error.InvalidQueryPosition;
    if (cfg.page_size == 0 or cfg.page_size > cfg.kv_len) return error.InvalidPageSize;
    if (cfg.num_heads == 0 or cfg.num_heads > cuda_renderer.generated_attention_workspace_max_query_heads) return error.InvalidHeadCount;
    if (cfg.num_kv_heads == 0 or cfg.num_heads % cfg.num_kv_heads != 0) return error.InvalidKvHeadCount;
    if (cfg.num_heads / cfg.num_kv_heads > cuda_renderer.generated_attention_query_heads_per_kv_head) return error.InvalidGqaRatio;
    if (cfg.iterations > 1_000_000) return error.InvalidIterations;
    if ((cfg.head_dim == null or cfg.head_dim.? == 256) and cfg.candidate_hd256 == null) return error.MissingHd256Candidate;
    if ((cfg.head_dim == null or cfg.head_dim.? == 512) and cfg.candidate_hd512 == null) return error.MissingHd512Candidate;
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
    var state = seed ^ (@as(u64, @intFromEnum(pattern)) *% 0x9e37_79b9_7f4a_7c15);
    switch (pattern) {
        .random => {
            for (q) |*value| value.* = nextSignedUnit(&state) * 0.125;
            for (k) |*value| value.* = nextSignedUnit(&state) * 0.125;
            for (v) |*value| value.* = nextSignedUnit(&state) * 0.75;
        },
        .near_tie => {
            for (0..num_heads) |head| for (0..head_dim) |d| {
                const sign: f32 = if (((head + d) & 1) == 0) 1.0 else -1.0;
                q[head * head_dim + d] = sign * (0.03125 + @as(f32, @floatFromInt((head + d) % 5)) * 0.00003125);
            };
            for (0..kv_len) |token| for (0..num_kv_heads) |kv_head| for (0..head_dim) |d| {
                const sign: f32 = if (((kv_head + d) & 1) == 0) 1.0 else -1.0;
                const perturb = @as(f32, @floatFromInt((token * 17 + d * 3) % 11)) * 0.00000025;
                const index = (token * num_kv_heads + kv_head) * head_dim + d;
                k[index] = sign * (0.03125 + perturb);
                const parity: f32 = if (((token + d) & 1) == 0) 1.0 else -1.0;
                v[index] = parity * (0.25 + @as(f32, @floatFromInt(token % 29)) * 0.0005);
            };
        },
        .cancellation => {
            for (0..num_heads) |head| for (0..head_dim) |d| {
                const sign: f32 = if ((d & 1) == 0) 1.0 else -1.0;
                const perturb = @as(f32, @floatFromInt((head * 13 + d) % 7)) * 0.00001;
                q[head * head_dim + d] = sign * (0.125 + perturb);
            };
            for (0..kv_len) |token| for (0..num_kv_heads) |kv_head| for (0..head_dim) |d| {
                const sign: f32 = if ((d & 1) == 0) 1.0 else -1.0;
                const token_sign: f32 = if ((token & 1) == 0) 1.0 else -1.0;
                const perturb = @as(f32, @floatFromInt((token * 19 + kv_head * 7 + d) % 13)) * 0.00001;
                const index = (token * num_kv_heads + kv_head) * head_dim + d;
                k[index] = sign * (token_sign * 0.125 + perturb);
                v[index] = token_sign * (0.5 + @as(f32, @floatFromInt(d % 17)) * 0.0001);
            };
        },
    }
}

fn encodePolar4(value: f32) u8 {
    const clipped = std.math.clamp(value, -1.0, 1.0);
    const scaled = @round((clipped + 1.0) * 7.5);
    return @intFromFloat(std.math.clamp(scaled, 0.0, 15.0));
}

fn physicalToken(logical_token: usize, block_table: []const u32, page_size: usize) usize {
    const logical_block = logical_token / page_size;
    return @as(usize, block_table[logical_block]) * page_size + logical_token % page_size;
}

fn fillBlockTable(block_table: []u32, order: PageOrder, seed: u64) void {
    for (block_table, 0..) |*physical_block, logical_block| physical_block.* = @intCast(logical_block);
    switch (order) {
        .identity_null, .fixed => {},
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

fn packPagedKeys(dst: []u8, logical: []const f32, block_table: []const u32, page_size: usize, kv_width: usize, row_bytes: usize, format: KeyFormat) void {
    switch (format) {
        .polar4 => @memset(dst, 0xff),
        .f16 => {
            var index: usize = 0;
            while (index + 1 < dst.len) : (index += 2) {
                dst[index] = 0x00;
                dst[index + 1] = 0x7e;
            }
        },
    }
    const logical_tokens = logical.len / kv_width;
    for (0..logical_tokens) |token| {
        const physical = physicalToken(token, block_table, page_size);
        const row = dst[physical * row_bytes ..][0..row_bytes];
        const values = logical[token * kv_width ..][0..kv_width];
        switch (format) {
            .polar4 => for (0..row_bytes) |byte_index| {
                const value_index = byte_index * 2;
                const lo = encodePolar4(values[value_index]);
                const hi = if (value_index + 1 < values.len) encodePolar4(values[value_index + 1]) else 0;
                row[byte_index] = lo | (hi << 4);
            },
            .f16 => for (values, 0..) |value, value_index| {
                const half: f16 = @floatCast(value);
                const bits: u16 = @bitCast(half);
                row[value_index * 2] = @truncate(bits);
                row[value_index * 2 + 1] = @truncate(bits >> 8);
            },
        }
    }
}

fn packPagedValues(dst: []u8, logical: []const f32, block_table: []const u32, page_size: usize, kv_width: usize, format: ValueFormat) void {
    const row_bytes = kv_width * @as(usize, switch (format) {
        .f32 => @sizeOf(f32),
        .f16 => @sizeOf(f16),
    });
    const poison_bits: u32 = if (format == .f32) 0x7fc0_0000 else 0x0000_7e00;
    const element_bytes: usize = if (format == .f32) 4 else 2;
    var poison_index: usize = 0;
    while (poison_index + element_bytes <= dst.len) : (poison_index += element_bytes) {
        for (0..element_bytes) |byte_index| dst[poison_index + byte_index] = @truncate(poison_bits >> @intCast(byte_index * 8));
    }
    const logical_tokens = logical.len / kv_width;
    for (0..logical_tokens) |token| {
        const physical = physicalToken(token, block_table, page_size);
        const row = dst[physical * row_bytes ..][0..row_bytes];
        for (logical[token * kv_width ..][0..kv_width], 0..) |value, value_index| switch (format) {
            .f32 => {
                const bits: u32 = @bitCast(value);
                for (0..4) |byte_index| row[value_index * 4 + byte_index] = @truncate(bits >> @intCast(byte_index * 8));
            },
            .f16 => {
                const bits: u16 = @bitCast(@as(f16, @floatCast(value)));
                row[value_index * 2] = @truncate(bits);
                row[value_index * 2 + 1] = @truncate(bits >> 8);
            },
        };
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

fn compareOutputs(reference: []const f32, candidate: []const f32) !DiffStats {
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
        stats.max_rel = @max(stats.max_rel, abs_error / @max(@abs(expected), 1e-30));
        stats.max_ulp = @max(stats.max_ulp, ulpDistance(expected, actual));
    }
    return stats;
}

fn loadFunction(ctx: *cuda_context.CudaContext, module: cuda_driver.CUmodule, name: []const u8) !cuda_driver.CUfunction {
    var name_buffer: [160]u8 = undefined;
    const name_z = try std.fmt.bufPrintZ(&name_buffer, "{s}", .{name});
    var function: cuda_driver.CUfunction = null;
    try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&function, module, name_z));
    return function orelse error.CudaKernelUnavailable;
}

fn sha256File(
    io: std.Io,
    allocator: std.mem.Allocator,
    path: []const u8,
) ![std.crypto.hash.sha2.Sha256.digest_length * 2]u8 {
    const image = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(64 * 1024 * 1024));
    defer allocator.free(image);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(image, &digest, .{});
    return std.fmt.bytesToHex(digest, .lower);
}

fn candidatePlan(head_dim: u16) cuda_renderer.AttentionRenderPlan {
    return cuda_renderer.attentionPlanFor(if (head_dim == 256)
        .gqa_decode_score_prework_hd256_f32
    else
        .gqa_decode_score_prework_hd512_f32);
}

fn launch(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    grid_x: u32,
    grid_y: u32,
    block: u32,
    params: []?*anyopaque,
) !void {
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(function, grid_x, grid_y, 1, block, 1, 1, 0, ctx.stream, params.ptr, null));
    ctx.noteKernelLaunch();
}

fn launchBaseline(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    dst: cuda_buffer.DeviceBuffer,
    q: cuda_buffer.DeviceBuffer,
    k: cuda_buffer.DeviceBuffer,
    v: cuda_buffer.DeviceBuffer,
    block_table: cuda_buffer.DeviceBuffer,
    decode_scalars: cuda_buffer.DeviceBuffer,
    args: LaunchArgs,
) !void {
    var dst_ptr = dst.ptr;
    var q_ptr = q.ptr;
    var k_ptr = k.ptr;
    var v_ptr = v.ptr;
    var block_table_ptr = block_table.ptr;
    var null_ptr: cuda_driver.CUdeviceptr = 0;
    var batch: u32 = 1;
    var q_seq_len: u32 = 1;
    var kv_seq_len = args.kv_seq_len;
    var num_heads = args.num_heads;
    var num_kv_heads = args.num_kv_heads;
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
    var format = args.format;
    var value_format = args.value_format;
    var physical_token_capacity = args.physical_token_capacity;
    var decode_scalars_ptr = decode_scalars.ptr;
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),            @ptrCast(&q_ptr),          @ptrCast(&k_ptr),              @ptrCast(&v_ptr),     @ptrCast(&block_table_ptr), @ptrCast(&null_ptr),                @ptrCast(&null_ptr),
        @ptrCast(&batch),              @ptrCast(&q_seq_len),      @ptrCast(&kv_seq_len),         @ptrCast(&num_heads), @ptrCast(&num_kv_heads),    @ptrCast(&head_dim),                @ptrCast(&query_position_offset),
        @ptrCast(&kv_position_offset), @ptrCast(&sliding_window), @ptrCast(&total_sequence_len), @ptrCast(&mask_len),  @ptrCast(&bias_mode),       @ptrCast(&key_row_bytes),           @ptrCast(&base_key_row_bytes),
        @ptrCast(&value_row_bytes),    @ptrCast(&block_count),    @ptrCast(&page_size_tokens),   @ptrCast(&format),    @ptrCast(&value_format),    @ptrCast(&physical_token_capacity), @ptrCast(&decode_scalars_ptr),
    };
    try launch(ctx, function, args.num_heads, 1, args.head_dim, params[0..]);
}

fn launchCandidateScore(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    scores: cuda_buffer.DeviceBuffer,
    q: cuda_buffer.DeviceBuffer,
    k: cuda_buffer.DeviceBuffer,
    block_table: cuda_buffer.DeviceBuffer,
    decode_scalars: cuda_buffer.DeviceBuffer,
    args: LaunchArgs,
) !void {
    var scores_ptr = scores.ptr;
    var q_ptr = q.ptr;
    var k_ptr = k.ptr;
    var block_table_ptr = block_table.ptr;
    var batch: u32 = 1;
    var q_seq_len: u32 = 1;
    var kv_seq_len = args.kv_seq_len;
    var num_heads = args.num_heads;
    var num_kv_heads = args.num_kv_heads;
    var head_dim = args.head_dim;
    var query_position_offset = args.query_position_offset;
    var kv_position_offset: u32 = 0;
    var sliding_window = args.sliding_window;
    var total_sequence_len = args.total_sequence_len;
    var key_row_bytes = args.key_row_bytes;
    var base_key_row_bytes = args.key_row_bytes;
    var block_count = args.block_count;
    var page_size_tokens = args.page_size_tokens;
    var format = args.format;
    var physical_token_capacity = args.physical_token_capacity;
    var score_capacity = args.score_capacity;
    const chunk_count_value: u32 = cuda_renderer.generated_attention_score_prework_chunks;
    var chunk_count = chunk_count_value;
    var chunk_size: u32 = @intCast(ceilDiv(args.score_capacity, chunk_count_value));
    var decode_scalars_ptr = decode_scalars.ptr;
    var score_params = [_]?*anyopaque{
        @ptrCast(&scores_ptr),     @ptrCast(&q_ptr),              @ptrCast(&k_ptr),                 @ptrCast(&block_table_ptr),
        @ptrCast(&batch),          @ptrCast(&q_seq_len),          @ptrCast(&kv_seq_len),            @ptrCast(&num_heads),
        @ptrCast(&num_kv_heads),   @ptrCast(&head_dim),           @ptrCast(&query_position_offset), @ptrCast(&kv_position_offset),
        @ptrCast(&sliding_window), @ptrCast(&total_sequence_len), @ptrCast(&key_row_bytes),         @ptrCast(&base_key_row_bytes),
        @ptrCast(&block_count),    @ptrCast(&page_size_tokens),   @ptrCast(&format),                @ptrCast(&physical_token_capacity),
        @ptrCast(&score_capacity), @ptrCast(&chunk_size),         @ptrCast(&chunk_count),           @ptrCast(&decode_scalars_ptr),
    };
    const score_grid = try checkedMul(args.num_heads, chunk_count_value);
    try launch(ctx, function, @intCast(score_grid), 1, args.head_dim, score_params[0..]);
}

fn launchCandidateConsumer(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    consumer: CandidateConsumer,
    dst: cuda_buffer.DeviceBuffer,
    scores: cuda_buffer.DeviceBuffer,
    v: cuda_buffer.DeviceBuffer,
    block_table: cuda_buffer.DeviceBuffer,
    decode_scalars: cuda_buffer.DeviceBuffer,
    args: LaunchArgs,
) !void {
    var dst_ptr = dst.ptr;
    var scores_ptr = scores.ptr;
    var v_ptr = v.ptr;
    var block_table_ptr = block_table.ptr;
    var batch: u32 = 1;
    var q_seq_len: u32 = 1;
    var kv_seq_len = args.kv_seq_len;
    var num_heads = args.num_heads;
    var num_kv_heads = args.num_kv_heads;
    var head_dim = args.head_dim;
    var query_position_offset = args.query_position_offset;
    var kv_position_offset: u32 = 0;
    var sliding_window = args.sliding_window;
    var total_sequence_len = args.total_sequence_len;
    var value_row_bytes = args.value_row_bytes;
    var block_count = args.block_count;
    var page_size_tokens = args.page_size_tokens;
    var value_format = args.value_format;
    var physical_token_capacity = args.physical_token_capacity;
    var score_capacity = args.score_capacity;
    var decode_scalars_ptr = decode_scalars.ptr;
    var consume_params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),            @ptrCast(&scores_ptr),         @ptrCast(&v_ptr),                   @ptrCast(&block_table_ptr),
        @ptrCast(&batch),              @ptrCast(&q_seq_len),          @ptrCast(&kv_seq_len),              @ptrCast(&num_heads),
        @ptrCast(&num_kv_heads),       @ptrCast(&head_dim),           @ptrCast(&query_position_offset),   @ptrCast(&kv_position_offset),
        @ptrCast(&sliding_window),     @ptrCast(&total_sequence_len), @ptrCast(&value_row_bytes),         @ptrCast(&block_count),
        @ptrCast(&page_size_tokens),   @ptrCast(&value_format),       @ptrCast(&physical_token_capacity), @ptrCast(&score_capacity),
        @ptrCast(&decode_scalars_ptr),
    };
    const grid_y: u32 = switch (consumer) {
        .serial => 1,
        .tiled64 => args.head_dim / cuda_renderer.generated_attention_score_prework_tiled64_tile_size,
    };
    const block: u32 = switch (consumer) {
        .serial => args.head_dim,
        .tiled64 => cuda_renderer.generated_attention_score_prework_tiled64_tile_size,
    };
    try launch(ctx, function, args.num_heads, grid_y, block, consume_params[0..]);
}

fn timeBaseline(ctx: *cuda_context.CudaContext, iterations: usize, function: cuda_driver.CUfunction, dst: cuda_buffer.DeviceBuffer, q: cuda_buffer.DeviceBuffer, k: cuda_buffer.DeviceBuffer, v: cuda_buffer.DeviceBuffer, table: cuda_buffer.DeviceBuffer, scalars: cuda_buffer.DeviceBuffer, args: LaunchArgs) !u64 {
    if (iterations == 0) return 0;
    const pair = try ctx.beginProfileEventPair();
    for (0..iterations) |_| try launchBaseline(ctx, function, dst, q, k, v, table, scalars, args);
    return ctx.endProfileEventPairUs(pair);
}

fn timeCandidateScore(ctx: *cuda_context.CudaContext, iterations: usize, function: cuda_driver.CUfunction, scores: cuda_buffer.DeviceBuffer, q: cuda_buffer.DeviceBuffer, k: cuda_buffer.DeviceBuffer, table: cuda_buffer.DeviceBuffer, scalars: cuda_buffer.DeviceBuffer, args: LaunchArgs) !u64 {
    if (iterations == 0) return 0;
    const pair = try ctx.beginProfileEventPair();
    for (0..iterations) |_| try launchCandidateScore(ctx, function, scores, q, k, table, scalars, args);
    return ctx.endProfileEventPairUs(pair);
}

fn timeCandidateConsumer(ctx: *cuda_context.CudaContext, iterations: usize, function: cuda_driver.CUfunction, consumer: CandidateConsumer, dst: cuda_buffer.DeviceBuffer, scores: cuda_buffer.DeviceBuffer, v: cuda_buffer.DeviceBuffer, table: cuda_buffer.DeviceBuffer, scalars: cuda_buffer.DeviceBuffer, args: LaunchArgs) !u64 {
    if (iterations == 0) return 0;
    const pair = try ctx.beginProfileEventPair();
    for (0..iterations) |_| try launchCandidateConsumer(ctx, function, consumer, dst, scores, v, table, scalars, args);
    return ctx.endProfileEventPairUs(pair);
}

fn coefficientOfVariation(samples: []const f64) f64 {
    if (samples.len < 2) return 0;
    var mean: f64 = 0;
    for (samples) |sample| mean += sample;
    mean /= @floatFromInt(samples.len);
    if (mean <= 0) return 0;
    var variance: f64 = 0;
    for (samples) |sample| {
        const delta = sample - mean;
        variance += delta * delta;
    }
    variance /= @floatFromInt(samples.len);
    return @sqrt(variance) / mean;
}

fn timeCandidateConsumersPaired(
    ctx: *cuda_context.CudaContext,
    iterations: usize,
    functions: CandidateFunctions,
    serial_dst: cuda_buffer.DeviceBuffer,
    tiled64_dst: cuda_buffer.DeviceBuffer,
    scores: cuda_buffer.DeviceBuffer,
    v: cuda_buffer.DeviceBuffer,
    table: cuda_buffer.DeviceBuffer,
    scalars: cuda_buffer.DeviceBuffer,
    args: LaunchArgs,
) !ConsumerPairTiming {
    if (iterations == 0) return .{};
    const pair_count = @min(iterations, timing_pair_limit);
    var serial_samples: [timing_pair_limit]f64 = undefined;
    var tiled64_samples: [timing_pair_limit]f64 = undefined;
    var timing = ConsumerPairTiming{ .pair_count = pair_count };
    const launches_per_pair = iterations / pair_count;
    const extra_launches = iterations % pair_count;
    for (0..pair_count) |pair_index| {
        const launches = launches_per_pair + @intFromBool(pair_index < extra_launches);
        var serial_us: u64 = 0;
        var tiled64_us: u64 = 0;
        if ((pair_index & 1) == 0) {
            serial_us = try timeCandidateConsumer(ctx, launches, functions.serial_consume, .serial, serial_dst, scores, v, table, scalars, args);
            tiled64_us = try timeCandidateConsumer(ctx, launches, functions.tiled64_consume, .tiled64, tiled64_dst, scores, v, table, scalars, args);
        } else {
            tiled64_us = try timeCandidateConsumer(ctx, launches, functions.tiled64_consume, .tiled64, tiled64_dst, scores, v, table, scalars, args);
            serial_us = try timeCandidateConsumer(ctx, launches, functions.serial_consume, .serial, serial_dst, scores, v, table, scalars, args);
        }
        timing.serial_total_us += serial_us;
        timing.tiled64_total_us += tiled64_us;
        serial_samples[pair_index] = @as(f64, @floatFromInt(serial_us)) / @as(f64, @floatFromInt(launches));
        tiled64_samples[pair_index] = @as(f64, @floatFromInt(tiled64_us)) / @as(f64, @floatFromInt(launches));
    }
    timing.serial_cv = coefficientOfVariation(serial_samples[0..pair_count]);
    timing.tiled64_cv = coefficientOfVariation(tiled64_samples[0..pair_count]);
    return timing;
}

fn runCase(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    baseline_function: cuda_driver.CUfunction,
    candidate_functions: CandidateFunctions,
    cfg: Config,
    head_dim: u16,
    pattern: Pattern,
    key_format: KeyFormat,
    value_format: ValueFormat,
    page_order: PageOrder,
) !CaseResult {
    const dim: usize = head_dim;
    const q_count = try checkedMul(cfg.num_heads, dim);
    const kv_width = try checkedMul(cfg.num_kv_heads, dim);
    const logical_kv_count = try checkedMul(cfg.kv_len, kv_width);
    const block_count = ceilDiv(cfg.kv_len, cfg.page_size);
    const physical_capacity = try checkedMul(block_count, cfg.page_size);
    const key_row_bytes = switch (key_format) {
        .polar4 => ceilDiv(kv_width, 2),
        .f16 => try checkedMul(kv_width, 2),
    };
    const value_row_bytes = try checkedMul(kv_width, @as(usize, switch (value_format) {
        .f32 => @sizeOf(f32),
        .f16 => @sizeOf(f16),
    }));
    const key_bytes = try checkedMul(physical_capacity, key_row_bytes);
    const value_bytes = try checkedMul(physical_capacity, value_row_bytes);
    const score_capacity: usize = if (cfg.sliding_window == 0)
        cuda_renderer.generated_attention_score_prework_max_kv_tokens
    else
        @min(cfg.sliding_window, cuda_renderer.generated_attention_score_prework_max_kv_tokens);
    const score_count = try checkedMul(cfg.num_heads, score_capacity);
    const output_payload_bytes = try checkedMul(q_count, @sizeOf(f32));
    const output_image_bytes = try outputImageSize(output_payload_bytes);

    const host_q = try allocator.alloc(f32, q_count);
    defer allocator.free(host_q);
    const logical_k = try allocator.alloc(f32, logical_kv_count);
    defer allocator.free(logical_k);
    const logical_v = try allocator.alloc(f32, logical_kv_count);
    defer allocator.free(logical_v);
    const paged_k = try allocator.alloc(u8, key_bytes);
    defer allocator.free(paged_k);
    const paged_v = try allocator.alloc(u8, value_bytes);
    defer allocator.free(paged_v);
    const block_table = try allocator.alloc(u32, block_count);
    defer allocator.free(block_table);
    const host_baseline = try allocator.alloc(f32, q_count);
    defer allocator.free(host_baseline);
    const host_serial = try allocator.alloc(f32, q_count);
    defer allocator.free(host_serial);
    const host_tiled64 = try allocator.alloc(f32, q_count);
    defer allocator.free(host_tiled64);
    const host_tiled64_repeat = try allocator.alloc(f32, q_count);
    defer allocator.free(host_tiled64_repeat);
    const output_initial_image = try allocator.alloc(u8, output_image_bytes);
    defer allocator.free(output_initial_image);
    try initializeOutputImage(output_initial_image, output_payload_bytes);
    const baseline_output_image = try allocator.alloc(u8, output_image_bytes);
    defer allocator.free(baseline_output_image);
    const serial_output_image = try allocator.alloc(u8, output_image_bytes);
    defer allocator.free(serial_output_image);
    const tiled64_output_image = try allocator.alloc(u8, output_image_bytes);
    defer allocator.free(tiled64_output_image);
    const poisoned_scores = try allocator.alloc(u32, score_count);
    defer allocator.free(poisoned_scores);
    @memset(poisoned_scores, 0x7fc0_0000);
    const scores_before_consume = try allocator.alloc(u32, score_count);
    defer allocator.free(scores_before_consume);
    const scores_after_consume = try allocator.alloc(u32, score_count);
    defer allocator.free(scores_after_consume);
    const q_after = try allocator.alloc(u8, host_q.len * @sizeOf(f32));
    defer allocator.free(q_after);
    const k_after = try allocator.alloc(u8, paged_k.len);
    defer allocator.free(k_after);
    const v_after = try allocator.alloc(u8, paged_v.len);
    defer allocator.free(v_after);
    const table_after = try allocator.alloc(u8, block_table.len * @sizeOf(u32));
    defer allocator.free(table_after);
    var scalars_after: [5 * @sizeOf(u32)]u8 = undefined;

    fillInputs(host_q, logical_k, logical_v, dim, cfg.kv_len, cfg.num_heads, cfg.num_kv_heads, pattern, cfg.seed);
    fillBlockTable(block_table, page_order, cfg.seed);
    packPagedKeys(paged_k, logical_k, block_table, cfg.page_size, kv_width, key_row_bytes, key_format);
    packPagedValues(paged_v, logical_v, block_table, cfg.page_size, kv_width, value_format);

    var d_q = try cuda_buffer.DeviceBuffer.alloc(ctx, host_q.len * @sizeOf(f32));
    defer d_q.free(ctx);
    var d_k = try cuda_buffer.DeviceBuffer.alloc(ctx, paged_k.len);
    defer d_k.free(ctx);
    var d_v = try cuda_buffer.DeviceBuffer.alloc(ctx, paged_v.len);
    defer d_v.free(ctx);
    var d_table = try cuda_buffer.DeviceBuffer.alloc(ctx, block_table.len * @sizeOf(u32));
    defer d_table.free(ctx);
    var d_scalars = try cuda_buffer.DeviceBuffer.alloc(ctx, 5 * @sizeOf(u32));
    defer d_scalars.free(ctx);
    var d_baseline_image = try cuda_buffer.DeviceBuffer.alloc(ctx, output_image_bytes);
    defer d_baseline_image.free(ctx);
    const d_baseline = try outputPayloadBuffer(d_baseline_image, output_payload_bytes);
    var d_serial_image = try cuda_buffer.DeviceBuffer.alloc(ctx, output_image_bytes);
    defer d_serial_image.free(ctx);
    const d_serial = try outputPayloadBuffer(d_serial_image, output_payload_bytes);
    var d_tiled64_image = try cuda_buffer.DeviceBuffer.alloc(ctx, output_image_bytes);
    defer d_tiled64_image.free(ctx);
    const d_tiled64 = try outputPayloadBuffer(d_tiled64_image, output_payload_bytes);
    var d_scores = try cuda_buffer.DeviceBuffer.alloc(ctx, score_count * @sizeOf(f32));
    defer d_scores.free(ctx);

    const query_position: u32 = @intCast(cfg.query_position orelse cfg.kv_len - 1);
    const kv_len_u32: u32 = @intCast(cfg.kv_len);
    const decode_scalars = [_]u32{ query_position, query_position, kv_len_u32, kv_len_u32, 0 };
    try d_q.copyFromHost(ctx, std.mem.sliceAsBytes(host_q));
    try d_k.copyFromHost(ctx, paged_k);
    try d_v.copyFromHost(ctx, paged_v);
    try d_table.copyFromHost(ctx, std.mem.sliceAsBytes(block_table));
    try d_scalars.copyFromHost(ctx, std.mem.sliceAsBytes(&decode_scalars));
    try d_scores.copyFromHost(ctx, std.mem.sliceAsBytes(poisoned_scores));
    try d_baseline_image.copyFromHost(ctx, output_initial_image);
    try d_serial_image.copyFromHost(ctx, output_initial_image);
    try d_tiled64_image.copyFromHost(ctx, output_initial_image);

    const identity_null_table = page_order == .identity_null;
    const launch_table: cuda_buffer.DeviceBuffer = if (identity_null_table) .{} else d_table;
    const args = LaunchArgs{
        .kv_seq_len = kv_len_u32,
        .num_heads = @intCast(cfg.num_heads),
        .num_kv_heads = @intCast(cfg.num_kv_heads),
        .head_dim = head_dim,
        .query_position_offset = query_position,
        .sliding_window = cfg.sliding_window,
        .total_sequence_len = kv_len_u32,
        .key_row_bytes = @intCast(key_row_bytes),
        .value_row_bytes = @intCast(value_row_bytes),
        .block_count = if (identity_null_table) 0 else @intCast(block_count),
        .page_size_tokens = @intCast(cfg.page_size),
        .format = @intFromEnum(key_format),
        .value_format = @intFromEnum(value_format),
        .physical_token_capacity = @intCast(physical_capacity),
        .score_capacity = @intCast(score_capacity),
    };

    const tiled64_max_kv_tokens = cuda_renderer.generatedAttentionScorePreworkTiled64MaxKvTokens(head_dim) orelse
        return error.UnsupportedHeadDimension;
    const tiled64_tested = score_capacity <= tiled64_max_kv_tokens;

    try launchBaseline(ctx, baseline_function, d_baseline, d_q, d_k, d_v, launch_table, d_scalars, args);
    try launchCandidateScore(ctx, candidate_functions.score, d_scores, d_q, d_k, launch_table, d_scalars, args);
    try d_scores.copyToHost(ctx, std.mem.sliceAsBytes(scores_before_consume));
    try launchCandidateConsumer(ctx, candidate_functions.serial_consume, .serial, d_serial, d_scores, d_v, launch_table, d_scalars, args);
    if (tiled64_tested) {
        try launchCandidateConsumer(ctx, candidate_functions.tiled64_consume, .tiled64, d_tiled64, d_scores, d_v, launch_table, d_scalars, args);
    }
    try d_baseline.copyToHost(ctx, std.mem.sliceAsBytes(host_baseline));
    try d_serial.copyToHost(ctx, std.mem.sliceAsBytes(host_serial));
    if (tiled64_tested) try d_tiled64.copyToHost(ctx, std.mem.sliceAsBytes(host_tiled64));

    if (tiled64_tested) {
        try launchCandidateConsumer(ctx, candidate_functions.tiled64_consume, .tiled64, d_tiled64, d_scores, d_v, launch_table, d_scalars, args);
        try d_tiled64.copyToHost(ctx, std.mem.sliceAsBytes(host_tiled64_repeat));
    }
    try d_scores.copyToHost(ctx, std.mem.sliceAsBytes(scores_after_consume));
    try d_q.copyToHost(ctx, q_after);
    try d_k.copyToHost(ctx, k_after);
    try d_v.copyToHost(ctx, v_after);
    try d_table.copyToHost(ctx, table_after);
    try d_scalars.copyToHost(ctx, &scalars_after);
    try d_baseline_image.copyToHost(ctx, baseline_output_image);
    try d_serial_image.copyToHost(ctx, serial_output_image);
    if (tiled64_tested) try d_tiled64_image.copyToHost(ctx, tiled64_output_image);
    try ctx.synchronize();

    for (0..3) |_| {
        try launchBaseline(ctx, baseline_function, d_baseline, d_q, d_k, d_v, launch_table, d_scalars, args);
        try launchCandidateScore(ctx, candidate_functions.score, d_scores, d_q, d_k, launch_table, d_scalars, args);
        try launchCandidateConsumer(ctx, candidate_functions.serial_consume, .serial, d_serial, d_scores, d_v, launch_table, d_scalars, args);
        if (tiled64_tested) {
            try launchCandidateConsumer(ctx, candidate_functions.tiled64_consume, .tiled64, d_tiled64, d_scores, d_v, launch_table, d_scalars, args);
        }
    }
    try ctx.synchronize();
    const baseline_total_us = try timeBaseline(ctx, cfg.iterations, baseline_function, d_baseline, d_q, d_k, d_v, launch_table, d_scalars, args);
    const score_total_us = try timeCandidateScore(ctx, cfg.iterations, candidate_functions.score, d_scores, d_q, d_k, launch_table, d_scalars, args);
    const consumer_pair_timing = if (tiled64_tested)
        try timeCandidateConsumersPaired(ctx, cfg.iterations, candidate_functions, d_serial, d_tiled64, d_scores, d_v, launch_table, d_scalars, args)
    else
        ConsumerPairTiming{
            .serial_total_us = try timeCandidateConsumer(ctx, cfg.iterations, candidate_functions.serial_consume, .serial, d_serial, d_scores, d_v, launch_table, d_scalars, args),
        };

    var score_mutation_count: usize = 0;
    for (scores_before_consume, scores_after_consume) |before, after| {
        score_mutation_count += @intFromBool(before != after);
    }

    return .{
        .head_dim = head_dim,
        .kv_len = cfg.kv_len,
        .query_position = query_position,
        .sliding_window = cfg.sliding_window,
        .page_size = cfg.page_size,
        .block_count = @intCast(args.block_count),
        .num_heads = cfg.num_heads,
        .num_kv_heads = cfg.num_kv_heads,
        .pattern = pattern,
        .key_format = key_format,
        .value_format = value_format,
        .page_order = page_order,
        .serial_vs_production = try compareOutputs(host_baseline, host_serial),
        .tiled64_vs_production = if (tiled64_tested) try compareOutputs(host_baseline, host_tiled64) else null,
        .tiled64_vs_serial = if (tiled64_tested) try compareOutputs(host_serial, host_tiled64) else null,
        .tiled64_repeat = if (tiled64_tested) try compareOutputs(host_tiled64, host_tiled64_repeat) else null,
        .baseline_output_integrity = try inspectOutputImage(baseline_output_image, output_payload_bytes),
        .serial_output_integrity = try inspectOutputImage(serial_output_image, output_payload_bytes),
        .tiled64_output_integrity = if (tiled64_tested) try inspectOutputImage(tiled64_output_image, output_payload_bytes) else null,
        .input_integrity = .{
            .q_mutation_count = try byteMutationCount(std.mem.sliceAsBytes(host_q), q_after),
            .k_mutation_count = try byteMutationCount(paged_k, k_after),
            .v_mutation_count = try byteMutationCount(paged_v, v_after),
            .table_mutation_count = try byteMutationCount(std.mem.sliceAsBytes(block_table), table_after),
            .scalar_mutation_count = try byteMutationCount(std.mem.sliceAsBytes(&decode_scalars), &scalars_after),
            .score_mutation_count = score_mutation_count,
        },
        .baseline_total_us = baseline_total_us,
        .score_total_us = score_total_us,
        .serial_consume_total_us = consumer_pair_timing.serial_total_us,
        .tiled64_consume_total_us = consumer_pair_timing.tiled64_total_us,
        .consumer_pair_timing = consumer_pair_timing,
        .iterations = cfg.iterations,
    };
}

fn writeDiffHuman(writer: anytype, label: []const u8, diff: DiffStats) !void {
    try writer.print("  {s}: elements={d} bitwise_mismatches={d} nonfinite_mismatches={d} max_abs={e:.7} max_rel={e:.7} max_ulp={d}", .{
        label,
        diff.element_count,
        diff.bitwise_mismatch_count,
        diff.non_finite_mismatch_count,
        diff.max_abs,
        diff.max_rel,
        diff.max_ulp,
    });
    if (diff.first_mismatch_index) |index| try writer.print(" first_mismatch={d}", .{index});
    try writer.writeByte('\n');
}

fn writeResultHuman(writer: anytype, result: CaseResult) !void {
    try writer.print(
        "head_dim={d} kv_len={d} query_position={d} sliding_window={d} page_size={d} blocks={d} heads={d} kv_heads={d} key_format={s} value_format={s} page_order={s} pattern={s} status={s}\n",
        .{ result.head_dim, result.kv_len, result.query_position, result.sliding_window, result.page_size, result.block_count, result.num_heads, result.num_kv_heads, result.key_format.label(), result.value_format.label(), result.page_order.label(), result.pattern.label(), if (result.passes()) "pass" else "fail" },
    );
    try writeDiffHuman(writer, "serial_vs_production", result.serial_vs_production);
    if (result.tiled64_vs_production) |diff| {
        try writeDiffHuman(writer, "tiled64_vs_production", diff);
        try writeDiffHuman(writer, "tiled64_vs_serial", result.tiled64_vs_serial.?);
        try writeDiffHuman(writer, "tiled64_repeat", result.tiled64_repeat.?);
    } else {
        try writer.writeAll("  tiled64: not_tested (score capacity exceeds the specialized consumer cap)\n");
    }
    try writer.print(
        "  output_integrity: baseline_guards={d} baseline_poison={d} serial_guards={d} serial_poison={d}",
        .{
            result.baseline_output_integrity.guard_mutation_count,
            result.baseline_output_integrity.poison_element_count,
            result.serial_output_integrity.guard_mutation_count,
            result.serial_output_integrity.poison_element_count,
        },
    );
    if (result.tiled64_output_integrity) |integrity| {
        try writer.print(" tiled64_guards={d} tiled64_poison={d}", .{
            integrity.guard_mutation_count,
            integrity.poison_element_count,
        });
    }
    try writer.writeByte('\n');
    try writer.print(
        "  input_mutations: q={d} k={d} v={d} table={d} scalars={d} scores={d}\n",
        .{
            result.input_integrity.q_mutation_count,
            result.input_integrity.k_mutation_count,
            result.input_integrity.v_mutation_count,
            result.input_integrity.table_mutation_count,
            result.input_integrity.scalar_mutation_count,
            result.input_integrity.score_mutation_count,
        },
    );
    if (result.iterations != 0) {
        const baseline_us = @as(f64, @floatFromInt(result.baseline_total_us)) / @as(f64, @floatFromInt(result.iterations));
        const score_us = @as(f64, @floatFromInt(result.score_total_us)) / @as(f64, @floatFromInt(result.iterations));
        const serial_consume_us = @as(f64, @floatFromInt(result.serial_consume_total_us)) / @as(f64, @floatFromInt(result.iterations));
        const serial_pipeline_us = score_us + serial_consume_us;
        try writer.print("  raw_launch_timing: iterations={d} baseline_us={d:.3} score_us={d:.3} serial_consume_us={d:.3} serial_pipeline_us={d:.3} serial_speedup={d:.4}x", .{
            result.iterations,
            baseline_us,
            score_us,
            serial_consume_us,
            serial_pipeline_us,
            if (serial_pipeline_us > 0) baseline_us / serial_pipeline_us else 0,
        });
        if (result.tiled64_vs_production != null) {
            const tiled64_consume_us = @as(f64, @floatFromInt(result.tiled64_consume_total_us)) / @as(f64, @floatFromInt(result.iterations));
            const tiled64_pipeline_us = score_us + tiled64_consume_us;
            try writer.print(" tiled64_consume_us={d:.3} tiled64_pipeline_us={d:.3} tiled64_speedup={d:.4}x", .{
                tiled64_consume_us,
                tiled64_pipeline_us,
                if (tiled64_pipeline_us > 0) baseline_us / tiled64_pipeline_us else 0,
            });
        }
        try writer.writeByte('\n');
        if (result.consumer_pair_timing.pair_count != 0) {
            try writer.print(
                "  consumer_timing_pairs: count={d} order=alternating_serial_first tiled64_first serial_cv={d:.5} tiled64_cv={d:.5} max_cv={d:.5} cv_limit={d:.5} stable={s}\n",
                .{
                    result.consumer_pair_timing.pair_count,
                    result.consumer_pair_timing.serial_cv,
                    result.consumer_pair_timing.tiled64_cv,
                    result.consumer_pair_timing.maxCv(),
                    timing_cv_limit,
                    if (result.consumer_pair_timing.stable()) "true" else "false",
                },
            );
        }
    }
}

fn writeResultsJson(
    writer: anytype,
    results: []const CaseResult,
    device_name: []const u8,
    compute_major: i32,
    compute_minor: i32,
    provenance: CandidateProvenance,
) !void {
    var pass = true;
    for (results) |result| if (!result.passes()) {
        pass = false;
        break;
    };
    try writer.print(
        "{{\"schema\":\"antfly.cuda_paged_attention_diff.v4\",\"device\":\"{s}\",\"compute_capability\":\"{d}.{d}\",\"candidate_hd256_kernel_id\":\"{s}\",\"candidate_hd512_kernel_id\":\"{s}\",\"candidate_hd256_sha256\":",
        .{
            device_name,
            compute_major,
            compute_minor,
            candidatePlan(256).tiled64_kernel_id.?,
            candidatePlan(512).tiled64_kernel_id.?,
        },
    );
    if (provenance.hd256_sha256) |digest| {
        try writer.print("\"{s}\"", .{digest});
    } else {
        try writer.writeAll("null");
    }
    try writer.writeAll(",\"candidate_hd512_sha256\":");
    if (provenance.hd512_sha256) |digest| {
        try writer.print("\"{s}\"", .{digest});
    } else {
        try writer.writeAll("null");
    }
    try writer.print(",\"pass\":{s},\"results\":[", .{if (pass) "true" else "false"});
    for (results, 0..) |result, index| {
        if (index != 0) try writer.writeByte(',');
        try writer.print(
            "{{\"head_dim\":{d},\"kv_len\":{d},\"query_position\":{d},\"sliding_window\":{d},\"page_size\":{d},\"block_count\":{d},\"num_heads\":{d},\"num_kv_heads\":{d},\"key_format\":\"{s}\",\"value_format\":\"{s}\",\"page_order\":\"{s}\",\"pattern\":\"{s}\",\"pass\":{s},\"serial_bitwise_mismatches\":{d},\"serial_nonfinite_mismatches\":{d},\"serial_max_abs\":{e:.9},\"serial_max_rel\":{e:.9},\"serial_max_ulp\":{d},\"tiled64_tested\":{s}",
            .{ result.head_dim, result.kv_len, result.query_position, result.sliding_window, result.page_size, result.block_count, result.num_heads, result.num_kv_heads, result.key_format.label(), result.value_format.label(), result.page_order.label(), result.pattern.label(), if (result.passes()) "true" else "false", result.serial_vs_production.bitwise_mismatch_count, result.serial_vs_production.non_finite_mismatch_count, result.serial_vs_production.max_abs, result.serial_vs_production.max_rel, result.serial_vs_production.max_ulp, if (result.tiled64_vs_production != null) "true" else "false" },
        );
        try writer.print(
            ",\"baseline_output_guard_mutations\":{d},\"baseline_output_poison_elements\":{d},\"serial_output_guard_mutations\":{d},\"serial_output_poison_elements\":{d},\"q_mutations\":{d},\"k_mutations\":{d},\"v_mutations\":{d},\"table_mutations\":{d},\"scalar_mutations\":{d},\"score_mutations\":{d},\"iterations\":{d},\"baseline_total_us\":{d},\"score_total_us\":{d},\"serial_consume_total_us\":{d},\"tiled64_consume_total_us\":{d},\"consumer_timing_pair_count\":{d},\"consumer_timing_order\":\"alternating_serial_tiled\",\"serial_consume_cv\":{d:.9},\"tiled64_consume_cv\":{d:.9},\"consumer_timing_max_cv\":{d:.9},\"consumer_timing_cv_limit\":{d:.9},\"consumer_timing_stable\":{s}",
            .{ result.baseline_output_integrity.guard_mutation_count, result.baseline_output_integrity.poison_element_count, result.serial_output_integrity.guard_mutation_count, result.serial_output_integrity.poison_element_count, result.input_integrity.q_mutation_count, result.input_integrity.k_mutation_count, result.input_integrity.v_mutation_count, result.input_integrity.table_mutation_count, result.input_integrity.scalar_mutation_count, result.input_integrity.score_mutation_count, result.iterations, result.baseline_total_us, result.score_total_us, result.serial_consume_total_us, result.tiled64_consume_total_us, result.consumer_pair_timing.pair_count, result.consumer_pair_timing.serial_cv, result.consumer_pair_timing.tiled64_cv, result.consumer_pair_timing.maxCv(), timing_cv_limit, if (result.consumer_pair_timing.stable()) "true" else "false" },
        );
        if (result.tiled64_vs_production) |diff| {
            try writer.print(",\"tiled64_bitwise_mismatches\":{d},\"tiled64_nonfinite_mismatches\":{d},\"tiled64_max_abs\":{e:.9},\"tiled64_max_rel\":{e:.9},\"tiled64_max_ulp\":{d},\"tiled64_vs_serial_bitwise_mismatches\":{d},\"tiled64_repeat_bitwise_mismatches\":{d},\"tiled64_output_guard_mutations\":{d},\"tiled64_output_poison_elements\":{d}", .{
                diff.bitwise_mismatch_count,
                diff.non_finite_mismatch_count,
                diff.max_abs,
                diff.max_rel,
                diff.max_ulp,
                result.tiled64_vs_serial.?.bitwise_mismatch_count,
                result.tiled64_repeat.?.bitwise_mismatch_count,
                result.tiled64_output_integrity.?.guard_mutation_count,
                result.tiled64_output_integrity.?.poison_element_count,
            });
        }
        try writer.writeByte('}');
    }
    try writer.writeAll("]}\n");
}

fn writeUsage(writer: anytype) !void {
    try writer.writeAll(
        \\Usage: zig build quant-kernel-cuda-paged-attention-diff -Dcuda=true -Dcuda-artifacts=sm89 -- [options]
        \\  --candidate-hd256 PATH    Generated HD256 score-prework cubin
        \\  --candidate-hd512 PATH    Generated HD512 score-prework cubin
        \\  --head-dim 256|512|all    Default: all
        \\  --kv-len N                 Default: 1024; maximum: 4096
        \\  --query-position N         Default: kv_len - 1
        \\  --sliding-window N         Default: 0 (disabled)
        \\  --page-size N              Default: 16
        \\  --page-order all|identity-null|fixed|reversed|permuted
        \\  --heads N --kv-heads N     Default: 8 and 1 (qualified Gemma 4 tiled64 GQA)
        \\  --pattern all|random|near-tie|cancellation
        \\  --key-format all|polar4|f16
        \\  --value-format all|f32|f16
        \\  --seed N                   Decimal or 0x-prefixed deterministic seed
        \\  --iterations N             Raw launch timings; consumers use alternating pairs
        \\  --json                     Emit antfly.cuda_paged_attention_diff.v4 JSON
        \\
    );
}

fn parseConfigFromInit(init: std.process.Init) !Config {
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_iter.next();
    var args: [40][]const u8 = undefined;
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

    var ctx = try cuda_context.CudaContext.initDefault();
    defer ctx.deinit();
    var baseline_module = try Module.loadEmbedded(&ctx);
    defer baseline_module.deinit(&ctx);
    const baseline_function = try loadFunction(&ctx, baseline_module.module, "termite_gqa_attention_decode_turboquant_fast_f32");
    const provenance = CandidateProvenance{
        .hd256_sha256 = if (cfg.candidate_hd256) |path| try sha256File(init.io, std.heap.c_allocator, path) else null,
        .hd512_sha256 = if (cfg.candidate_hd512) |path| try sha256File(init.io, std.heap.c_allocator, path) else null,
    };

    const max_results = 2 * 3 * 2 * 2 * 4;
    var results: [max_results]CaseResult = undefined;
    var result_len: usize = 0;
    const head_dim_count: usize = if (cfg.head_dim == null) 2 else 1;
    for (0..head_dim_count) |head_index| {
        const head_dim: u16 = cfg.head_dim orelse if (head_index == 0) 256 else 512;
        const path = if (head_dim == 256) cfg.candidate_hd256.? else cfg.candidate_hd512.?;
        var candidate_module = try Module.loadFile(&ctx, init.io, std.heap.c_allocator, path);
        defer candidate_module.deinit(&ctx);
        const plan = candidatePlan(head_dim);
        const functions = CandidateFunctions{
            .score = try loadFunction(&ctx, candidate_module.module, plan.kernel_id),
            .serial_consume = try loadFunction(&ctx, candidate_module.module, plan.reduction_kernel_id),
            .tiled64_consume = try loadFunction(&ctx, candidate_module.module, plan.tiled64_kernel_id orelse return error.MissingTiled64KernelPlan),
        };
        for (0..cfg.pattern.count()) |pattern_index| {
            for (0..cfg.key_format.count()) |format_index| {
                for (0..cfg.value_format.count()) |value_format_index| {
                    for (0..cfg.page_order.count()) |page_order_index| {
                        results[result_len] = try runCase(
                            std.heap.c_allocator,
                            &ctx,
                            baseline_function,
                            functions,
                            cfg,
                            head_dim,
                            cfg.pattern.at(pattern_index),
                            cfg.key_format.at(format_index),
                            cfg.value_format.at(value_format_index),
                            cfg.page_order.at(page_order_index),
                        );
                        result_len += 1;
                    }
                }
            }
        }
    }

    const output = results[0..result_len];
    if (cfg.json) {
        try writeResultsJson(stdout, output, ctx.info.nameSlice(), ctx.info.compute_major, ctx.info.compute_minor, provenance);
    } else {
        try stdout.print("CUDA paged attention differential: device={s} cc={d}.{d} baseline=termite_gqa_attention_decode_turboquant_fast_f32 candidate=generated_score_prework unused_storage=nan_poison hd256_kernel={s} hd512_kernel={s}", .{
            ctx.info.nameSlice(),
            ctx.info.compute_major,
            ctx.info.compute_minor,
            candidatePlan(256).tiled64_kernel_id.?,
            candidatePlan(512).tiled64_kernel_id.?,
        });
        if (provenance.hd256_sha256) |digest| try stdout.print(" hd256_sha256={s}", .{digest});
        if (provenance.hd512_sha256) |digest| try stdout.print(" hd512_sha256={s}", .{digest});
        try stdout.writeByte('\n');
        for (output) |result| try writeResultHuman(stdout, result);
    }
    try stdout.flush();
    for (output) |result| if (!result.passes()) return error.PagedAttentionDifferentialExceeded;
}

test "paged attention diff parses the candidate and paged case surface" {
    const cfg = try parseConfig(&.{
        "--candidate-hd256", "hd256.cubin", "--head-dim",   "256", "--kv-len",       "512", "--query-position", "511",
        "--sliding-window",  "128",         "--page-size",  "32",  "--heads",        "8",   "--kv-heads",       "1",
        "--pattern",         "near-tie",    "--key-format", "f16", "--value-format", "f16", "--page-order",     "permuted",
        "--seed",            "0x42",        "--iterations", "100", "--json",
    });
    try std.testing.expectEqual(@as(?u16, 256), cfg.head_dim);
    try std.testing.expectEqual(@as(usize, 512), cfg.kv_len);
    try std.testing.expectEqual(@as(?usize, 511), cfg.query_position);
    try std.testing.expectEqual(@as(u32, 128), cfg.sliding_window);
    try std.testing.expectEqual(@as(usize, 32), cfg.page_size);
    try std.testing.expectEqual(PatternSelection.near_tie, cfg.pattern);
    try std.testing.expectEqual(KeyFormatSelection.f16, cfg.key_format);
    try std.testing.expectEqual(ValueFormatSelection.f16, cfg.value_format);
    try std.testing.expectEqual(PageOrderSelection.permuted, cfg.page_order);
    try std.testing.expectEqual(@as(usize, 100), cfg.iterations);
    try std.testing.expect(cfg.json);
}

test "paged attention diff rejects invalid topology and missing cubins" {
    try std.testing.expectError(error.MissingHd256Candidate, parseConfig(&.{ "--head-dim", "256" }));
    try std.testing.expectError(error.MissingHd512Candidate, parseConfig(&.{ "--head-dim", "512", "--candidate-hd256", "unused" }));
    try std.testing.expectError(error.InvalidKvLength, parseConfig(&.{ "--head-dim", "256", "--candidate-hd256", "x", "--kv-len", "4097" }));
    try std.testing.expectError(error.InvalidPageSize, parseConfig(&.{ "--head-dim", "256", "--candidate-hd256", "x", "--kv-len", "16", "--page-size", "32" }));
    try std.testing.expectError(error.InvalidGqaRatio, parseConfig(&.{ "--head-dim", "256", "--candidate-hd256", "x", "--heads", "32", "--kv-heads", "1" }));
    try std.testing.expectError(error.InvalidKeyFormat, parseConfig(&.{ "--head-dim", "256", "--candidate-hd256", "x", "--key-format", "packed3" }));
    try std.testing.expectError(error.InvalidValueFormat, parseConfig(&.{ "--head-dim", "256", "--candidate-hd256", "x", "--value-format", "int8" }));
    try std.testing.expectError(error.InvalidPageOrder, parseConfig(&.{ "--head-dim", "256", "--candidate-hd256", "x", "--page-order", "random" }));
}

test "paged attention diff reverses physical pages and packs both key formats" {
    var table = [_]u32{ 0, 0, 0 };
    fillBlockTable(&table, .reversed, 0);
    try std.testing.expectEqualSlices(u32, &.{ 2, 1, 0 }, &table);
    try std.testing.expectEqual(@as(usize, 9), physicalToken(1, &table, 4));
    try std.testing.expectEqual(@as(usize, 5), physicalToken(5, &table, 4));

    const logical = [_]f32{ -1.0, 1.0, 0.0, 0.5 };
    var polar = [_]u8{0} ** 2;
    const identity = [_]u32{0};
    packPagedKeys(&polar, &logical, &identity, 1, 4, 2, .polar4);
    try std.testing.expectEqual(@as(u8, 0xf0), polar[0]);
    try std.testing.expectEqual(@as(u8, 0xb8), polar[1]);

    var half = [_]u8{0} ** 8;
    packPagedKeys(&half, &logical, &identity, 1, 4, 8, .f16);
    const first_bits: u16 = @bitCast(@as(f16, -1.0));
    try std.testing.expectEqual(@as(u8, @truncate(first_bits)), half[0]);
    try std.testing.expectEqual(@as(u8, @truncate(first_bits >> 8)), half[1]);

    var permuted = [_]u32{ 0, 0, 0, 0, 0, 0, 0 };
    fillBlockTable(&permuted, .permuted, 0x42);
    var seen = [_]bool{false} ** permuted.len;
    for (permuted) |physical| {
        try std.testing.expect(physical < permuted.len);
        try std.testing.expect(!seen[physical]);
        seen[physical] = true;
    }
}

test "paged attention diff includes the production identity-null page ABI" {
    try std.testing.expectEqual(@as(usize, 4), PageOrderSelection.all.count());
    try std.testing.expectEqual(PageOrder.identity_null, PageOrderSelection.all.at(0));
    try std.testing.expectEqual(PageOrderSelection.identity_null, try parsePageOrder("identity-null"));
    var table = [_]u32{ 7, 7, 7 };
    fillBlockTable(&table, .identity_null, 0x42);
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 2 }, &table);
}

test "paged attention diff detects output guard poison and readonly mutations" {
    const payload_bytes = 2 * @sizeOf(f32);
    var image: [output_guard_bytes * 2 + payload_bytes]u8 = undefined;
    try initializeOutputImage(&image, payload_bytes);
    const pristine = try inspectOutputImage(&image, payload_bytes);
    try std.testing.expectEqual(@as(usize, 0), pristine.guard_mutation_count);
    try std.testing.expectEqual(@as(usize, 2), pristine.poison_element_count);

    image[0] ^= 1;
    image[output_guard_bytes] = 0;
    image[output_guard_bytes + 1] = 0;
    image[output_guard_bytes + 2] = 0;
    image[output_guard_bytes + 3] = 0;
    const changed = try inspectOutputImage(&image, payload_bytes);
    try std.testing.expectEqual(@as(usize, 1), changed.guard_mutation_count);
    try std.testing.expectEqual(@as(usize, 1), changed.poison_element_count);
    try std.testing.expectEqual(@as(usize, 1), try byteMutationCount(&.{ 1, 2, 3 }, &.{ 1, 9, 3 }));
}

test "paged attention diff timing CV is deterministic and bounded" {
    try std.testing.expectEqual(@as(f64, 0), coefficientOfVariation(&.{ 3.0, 3.0, 3.0 }));
    const variable = coefficientOfVariation(&.{ 1.0, 2.0, 3.0 });
    try std.testing.expect(variable > 0);
    const stable = ConsumerPairTiming{ .pair_count = 3, .serial_cv = 0.01, .tiled64_cv = 0.02 };
    try std.testing.expect(stable.stable());
    const unstable = ConsumerPairTiming{ .pair_count = 3, .serial_cv = timing_cv_limit + 0.01 };
    try std.testing.expect(!unstable.stable());
}

test "paged attention diff packs F16 values and poisons unused rows" {
    const logical = [_]f32{ 1.0, -2.0 };
    const table = [_]u32{0};
    var packed_values = [_]u8{0} ** 8;
    packPagedValues(&packed_values, &logical, &table, 2, 2, .f16);
    const first_bits: u16 = @bitCast(@as(f16, 1.0));
    try std.testing.expectEqual(@as(u8, @truncate(first_bits)), packed_values[0]);
    try std.testing.expectEqual(@as(u8, @truncate(first_bits >> 8)), packed_values[1]);
    // Second physical token is padding and remains an F16 quiet NaN poison.
    try std.testing.expectEqual(@as(u8, 0x00), packed_values[4]);
    try std.testing.expectEqual(@as(u8, 0x7e), packed_values[5]);
}

test "paged attention diff reports exact output differences" {
    const reference = [_]f32{ 0.0, 1.0, -2.0, 4.0 };
    const candidate = [_]f32{ -0.0, 1.0000001, -2.0, 3.5 };
    const stats = try compareOutputs(&reference, &candidate);
    try std.testing.expectEqual(@as(usize, 3), stats.bitwise_mismatch_count);
    try std.testing.expectEqual(@as(usize, 0), stats.first_mismatch_index.?);
    try std.testing.expect(stats.max_abs >= 0.5);
    try std.testing.expect(stats.max_ulp > 0);
}

test "paged attention diff JSON writer emits the complete v4 schema" {
    const result = CaseResult{
        .head_dim = 256,
        .kv_len = 512,
        .query_position = 511,
        .sliding_window = 512,
        .page_size = 16,
        .block_count = 0,
        .num_heads = 8,
        .num_kv_heads = 1,
        .pattern = .random,
        .key_format = .f16,
        .value_format = .f16,
        .page_order = .identity_null,
        .serial_vs_production = .{ .element_count = 2048 },
        .tiled64_vs_production = .{ .element_count = 2048 },
        .tiled64_vs_serial = .{ .element_count = 2048 },
        .tiled64_repeat = .{ .element_count = 2048 },
        .baseline_output_integrity = .{},
        .serial_output_integrity = .{},
        .tiled64_output_integrity = .{},
        .input_integrity = .{},
        .iterations = 100,
        .consumer_pair_timing = .{ .pair_count = 10 },
    };
    var storage: [8192]u8 = undefined;
    var writer = std.Io.Writer.fixed(&storage);
    try writeResultsJson(&writer, &.{result}, "test-device", 8, 9, .{});
    const encoded = writer.buffered();
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    const root = parsed.value.object;
    try std.testing.expectEqualStrings("antfly.cuda_paged_attention_diff.v4", root.get("schema").?.string);
    try std.testing.expectEqualStrings("8.9", root.get("compute_capability").?.string);
    try std.testing.expect(root.get("candidate_hd256_sha256").? == .null);
    const first = root.get("results").?.array.items[0].object;
    try std.testing.expectEqualStrings("alternating_serial_tiled", first.get("consumer_timing_order").?.string);
    try std.testing.expect(first.get("tiled64_output_guard_mutations") != null);
    try std.testing.expect(first.get("score_mutations") != null);
}
