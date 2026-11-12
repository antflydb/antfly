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

//! GLiDE encoder backbone (ModernBERT). Every layer is bidirectional over the
//! row's real tokens; local layers use the +-64 token window. Right-padded rows
//! take the padding-free row-segment attention path on Metal.
//!
//! GLiDE-only Metal execution (ModernBERT `Config.glide`; other callers keep
//! their routes):
//!   - row-segment attention on the F16-staged per-row flash kernel,
//!   - BF16 linears mirrored as F16 slots on the F32-input TensorOps GEMM,
//!   - the last layer runs queries, output projection and MLP for the readout
//!     rows only (`modern_bert.forwardReadoutCT`).
//! `ANTFLY_GLIDE_DISABLE_{ROW_FLASH,F16_LINEARS,TAIL_PRUNE}` restore the
//! generic route for each, for A/B qualification.

const std = @import("std");
const platform = @import("antfly_platform");
const ops = @import("../ops/ops.zig");
const modern_bert = @import("modern_bert.zig");

/// Final-norm hidden states at `readout_index[b]` for every row: [batch * hidden] f32.
pub fn readoutHidden(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    config: modern_bert.Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    readout_index: []const i64,
) ![]f32 {
    if (readout_index.len != batch) return error.InvalidInputShape;
    if (seq_len > @as(usize, config.max_position_embeddings)) return error.InvalidInputShape;
    var execution = config;
    execution.glide = .{
        .f16_row_attention = !platform.env.getenvBool("ANTFLY_GLIDE_DISABLE_ROW_FLASH"),
        .f16_linears = !platform.env.getenvBool("ANTFLY_GLIDE_DISABLE_F16_LINEARS"),
        // A whole forward below ~4k padded tokens takes well under 1.5 s on
        // M-series GPUs; per-two-layer cancellation fences cost ~7 ms (+18%)
        // on a 118-token prompt. Longer forwards keep the mid-forward checks.
        .single_frame_below_tokens = 4096,
    };
    if (platform.env.getenvBool("ANTFLY_GLIDE_DISABLE_TAIL_PRUNE")) {
        const hidden = try modern_bert.forwardCT(cb, allocator, execution, input_ids, attention_mask, batch, seq_len);
        defer cb.free(hidden);
        return gatherRows(cb, allocator, hidden, config.hidden_size, seq_len, readout_index);
    }
    const rows = try modern_bert.forwardReadoutCT(cb, allocator, execution, input_ids, attention_mask, batch, seq_len, readout_index);
    defer cb.free(rows);
    return cb.toFloat32(rows, allocator);
}

fn gatherRows(cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, hidden: ops.CT, hidden_size: usize, seq_len: usize, readout_index: []const i64) ![]f32 {
    const batch = readout_index.len;
    const row_ids = try allocator.alloc(u32, batch);
    defer allocator.free(row_ids);
    const fallback_ids = try allocator.alloc(i64, batch);
    defer allocator.free(fallback_ids);
    for (readout_index, row_ids, fallback_ids, 0..) |index, *row_id, *fallback, row| {
        const global = row * seq_len + @as(usize, @intCast(index));
        row_id.* = std.math.cast(u32, global) orelse return error.InvalidInputShape;
        fallback.* = @intCast(global);
    }
    const gathered = if (try cb.takeRows(hidden, row_ids, batch, hidden_size)) |value|
        value
    else
        try cb.embeddingLookup(hidden, fallback_ids, batch, hidden_size);
    defer cb.free(gathered);
    return cb.toFloat32(gathered, allocator);
}
