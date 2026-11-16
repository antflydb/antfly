// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Ordered sampling phases; callers retain their candidate admission, tie,
//! nonfinite, temperature and random-stream policies.
const render = @import("source_template.zig").render;

pub const TopK = struct {
    count: []const u8,
    value: []const u8,
    id: []const u8,
    pub fn insert(comptime p: TopK) []const u8 {
        return render(
            \\if ({{value}} <= top_vals[{{count}} - 1u]) continue;
            \\uint insert_at = {{count}} - 1u;
            \\while (insert_at > 0u && {{value}} > top_vals[insert_at - 1u]) {
            \\    top_vals[insert_at] = top_vals[insert_at - 1u];
            \\    top_idx[insert_at] = top_idx[insert_at - 1u];
            \\    insert_at -= 1u;
            \\}
            \\top_vals[insert_at] = {{value}};
            \\top_idx[insert_at] = {{id}};
            \\
        , .{ .count = p.count, .value = p.value, .id = p.id });
    }
};

/// Fixed integer avalanche and F32 uniform with legacy endpoint rounding. Wrapping U32
/// multiplication, high 24 bits, and the half-unit offset are ABI semantics. The top value can round to 1.0;
/// retaining that boundary preserves the existing random stream.
pub fn uniform(comptime id: []const u8, comptime seed: []const u8) []const u8 {
    return render(
        \\uint h = ({{id}} ^ {{seed}}) * 0x9E3779B9u;
        \\h ^= h >> 16; h *= 0x85EBCA6Bu; h ^= h >> 13; h *= 0xC2B2AE35u; h ^= h >> 16;
        \\float u = (float(h >> 8) + 0.5f) * (1.0f / 16777216.0f);
        \\
    , .{ .id = id, .seed = seed });
}
