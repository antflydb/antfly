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
    /// Native U32 phases. Callers retain zero initialization, empty/query
    /// guards and replay admission. Arguments are side-effect-free names or
    /// lvalues; the native wrapping add/subtract association is intentional.
    pub fn cudaSource() []const u8 {
        return
        \\#ifndef ANTFLY_CAUSAL_KEY_WINDOW_PHASES
        \\#define ANTFLY_CAUSAL_KEY_WINDOW_PHASES
        \\#define ANTFLY_CAUSAL_KEY_END(TempType, Query, Count, Offset, End) \
        \\    TempType visible = Query - Offset + 1u; \
        \\    End = visible < Count ? visible : Count;
        \\#define ANTFLY_CAUSAL_WINDOW_START(TempType, Query, Window, Offset, Begin, End) \
        \\    TempType window_start_abs = (Query + 1u > Window) ? (Query + 1u - Window) : 0u; \
        \\    if (window_start_abs > Offset) { \
        \\        Begin = window_start_abs - Offset; \
        \\        if (Begin > End) Begin = End; \
        \\    }
        \\#define ANTFLY_CAUSAL_KEY_WINDOW(TempType, Query, Count, Offset, Window, Begin, End) \
        \\    ANTFLY_CAUSAL_KEY_END(TempType, Query, Count, Offset, End) \
        \\    if (Window != 0u) { \
        \\        ANTFLY_CAUSAL_WINDOW_START(TempType, Query, Window, Offset, Begin, End) \
        \\    }
        \\#endif
        \\
        ;
    }
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

/// Device scalar replay preserves the native load order. Serial consumers do
/// not acquire total_sequence_len, which may be unused/stale on that schedule.
pub const DecodeReplay = struct {
    pub fn cudaSource() []const u8 {
        return
        \\#ifndef ANTFLY_DECODE_REPLAY_PHASES
        \\#define ANTFLY_DECODE_REPLAY_PHASES
        \\#define ANTFLY_DECODE_REPLAY_KV(Scalars) \
        \\    kv_position_offset = Scalars[4]; \
        \\    query_position_offset = Scalars[1]; \
        \\    kv_seq_len = Scalars[2];
        \\#define ANTFLY_DECODE_REPLAY_TOTAL(Scalars) \
        \\    ANTFLY_DECODE_REPLAY_KV(Scalars) \
        \\    total_sequence_len = Scalars[3];
        \\#endif
        \\
        ;
    }
};
