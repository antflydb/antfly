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

//! Shared logical coverage for causal paged attention with query-relative logits.
const std = @import("std");

pub const Query = struct {
    rows: u32,
    start: u32,
    q_heads: u32,
    kv_heads: u32,
    window: u32 = 0,
    relative_extent: u32,
    score_scale: f32,

    pub const Coverage = struct {
        row_heads: u32,
        live_keys: u32,
    };

    pub fn coverage(query: Query, visible_start: usize, visible_end: usize) !Coverage {
        if (query.rows == 0 or query.q_heads == 0 or query.q_heads > 64 or query.kv_heads == 0 or
            query.q_heads % query.kv_heads != 0 or !std.math.isFinite(query.score_scale) or query.score_scale <= 0)
            return error.InvalidAttentionGeometry;
        const end = try std.math.add(u32, query.start, query.rows);
        const row_heads = try std.math.mul(u32, query.rows, query.q_heads);
        if (visible_start > query.start or end > visible_end or visible_end > (1 << 24) or row_heads > 65535)
            return error.InvalidAttentionGeometry;
        const keys = end - @as(u32, @intCast(visible_start));
        return .{ .row_heads = row_heads, .live_keys = if (query.window == 0) keys else @min(keys, query.window) };
    }
};
