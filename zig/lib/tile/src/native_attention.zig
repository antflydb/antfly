// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Online attention arithmetic shared across native schedules. Call sites retain
//! their maximum reduction, empty-tile guards and probability storage rounding.
const f = @import("fexpr.zig");
const math = @import("native_math.zig");
pub const Online = struct {
    pub const correction = f.operand("old_max").minus(f.operand("new_max")).expOf();
    pub const probability = f.operand("score").minus(f.operand("new_max")).expOf();
    pub const mass = f.operand("old_sum").times(f.operand("correction")).plus(f.operand("tile_sum"));
    pub inline fn updateMass(old_sum: f32, correction_: f32, tile_sum: f32) f32 {
        return mass.evaluate(math.ScalarMath, .{ .value = old_sum, .old_sum = old_sum, .correction = correction_, .tile_sum = tile_sum });
    }
};

pub const CausalWindow = struct {
    pub fn contains(query: u32, key: u32, window: u32) bool {
        return key <= query and (window == 0 or query - key < window);
    }
    pub fn native(comptime query: []const u8, comptime key: []const u8, comptime window: []const u8) []const u8 {
        return key ++ " <= " ++ query ++ " && (" ++ window ++ " == 0u || (" ++ query ++ " - " ++ key ++ ") < " ++ window ++ ")";
    }
};

pub fn cudaOnlineRow64() []const u8 {
    return @import("source_template.zig").render(
        \\#define ANTFLY_CUDA_ATTENTION_ONLINE_ROW64(Exp, ProbabilityStore) \
        \\    for (unsigned int shift = 16u; shift; shift >>= 1u) \
        \\        next_max = fmaxf(next_max, __shfl_down_sync(0xffffffffu, next_max, shift)); \
        \\    next_max = fmaxf(maximum[row], __shfl_sync(0xffffffffu, next_max, 0u)); \
        \\    const float a = isfinite(maximum[row]) ? {{correction}} : 0.0f; \
        \\    float sum = 0.0f; \
        \\    _Pragma("unroll") \
        \\    for (unsigned int j = 0u; j < 2u; ++j) { \
        \\        const float p = isfinite(next_max) ? {{probability}} : 0.0f; \
        \\        ProbabilityStore \
        \\        sum += p; \
        \\    } \
        \\    for (unsigned int shift = 16u; shift; shift >>= 1u) \
        \\        sum += __shfl_down_sync(0xffffffffu, sum, shift); \
        \\    if (lane == 0u) { \
        \\        alpha[row] = a; \
        \\        denominator[row] = {{mass}}; \
        \\        maximum[row] = next_max; \
        \\    }
        \\
    , .{
        .correction = Online.correction.renderNative(.{ .exp = "Exp" }, .{ .old_max = "maximum[row]", .new_max = "next_max" }, null),
        .probability = Online.probability.renderNative(.{ .exp = "Exp" }, .{ .score = "values[j]", .new_max = "next_max" }, null),
        .mass = Online.mass.renderNative(.{}, .{ .old_sum = "denominator[row]", .correction = "a", .tile_sum = "sum" }, null),
    });
}

/// Qualified relative-attention schedule: each warp owns a query and retains
/// two head coordinates. Launch admission and device guards share this owner.
pub const RelativeRow64 = struct {
    pub const threads = 256;
    pub const queries_per_group = threads / 32;
    pub const head_dim = 64;
    pub const max_sequence = 512;
    pub fn supports(batch: usize, sequence: usize, heads: usize, width: usize) bool {
        return batch != 0 and sequence != 0 and sequence <= max_sequence and heads != 0 and width == head_dim;
    }
    pub fn blocks(batch: usize, sequence: usize, heads: usize) error{InvalidLayout}!usize {
        const std = @import("std");
        if (!supports(batch, sequence, heads, head_dim)) return error.InvalidLayout;
        const groups = sequence / queries_per_group + @intFromBool(sequence % queries_per_group != 0);
        const rows = std.math.mul(usize, batch, heads) catch return error.InvalidLayout;
        return std.math.mul(usize, rows, groups) catch error.InvalidLayout;
    }
    pub const header = @import("std").fmt.comptimePrint(
        "#define ANTFLY_RELATIVE_ROW64_THREADS {d}u\n#define ANTFLY_RELATIVE_ROW64_QUERIES {d}u\n#define ANTFLY_RELATIVE_ROW64_HEAD_DIM {d}u\n#define ANTFLY_RELATIVE_ROW64_MAX_SEQUENCE {d}u\n",
        .{ threads, queries_per_group, head_dim, max_sequence },
    );
};
