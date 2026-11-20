// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

/// Ranked candidate insertion: value descending, then lowest unsigned ID.
/// In contrast to legacy TopK, NaNs are rejected by the positive predicate.
/// Expansion stays in the caller loop to preserve array scalarization/liveness.
pub fn rankedInsert(comptime better: []const u8) []const u8 {
    return render(
        \\if (!{{better}}(value, token_id, best_values[top_k - 1u], best_ids[top_k - 1u])) continue;
        \\uint at = top_k - 1u; while (at > 0u && {{better}}(value, token_id, best_values[at - 1u], best_ids[at - 1u])) { best_values[at] = best_values[at - 1u]; best_ids[at] = best_ids[at - 1u]; --at; }
        \\best_values[at] = value; best_ids[at] = token_id;
    , .{ .better = better });
}

/// Positive F32 probability keys, four bits per pass. The guarded mask avoids
/// shifting a U32 by 32 on the first pass. Histogram accumulation stays native.
pub const radix4_geometry = "uint shift = 28u - state->pass * 4u; uint prefix_mask = state->pass == 0u ? 0u : (0xffffffffu << (32u - state->pass * 4u));";
