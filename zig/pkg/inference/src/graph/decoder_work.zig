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

//! Borrowed physical decoder work shared by task adapters and step executors.
//! The coordinator retains each record until publication sets ready. Callers
//! retain its sequence state, prefix tensor and positions for that whole borrow.
const std = @import("std");
const CT = @import("../ops/ops.zig").CT;
const State = @import("decode_state_runtime.zig").NativeDecodeState;
const StepItem = @import("../runtime/scheduler/native_generate.zig").StepItem;

/// Resident projected prefix with axis-major three-axis rotary positions.
pub const Prefix = struct {
    token_count: usize,
    positions: []const u32,
    embeddings: ?CT,
    intermediate_layer_count: usize = 0,
};

pub const Decode = struct {
    allocator: std.mem.Allocator,
    decode_state: *State,
    token_id: i64,
    seq_len: usize,
    logits: ?[]f32 = null,
    wants_greedy: bool = false,
    greedy_token: ?u32 = null,
    /// Immutable while queued/claimed. The task updates this before requeue.
    remaining_tokens: usize = 1,
    /// Opaque task publication association; execution never dereferences it.
    continuation_context: ?*anyopaque = null,
    failure: ?anyerror = null,
    ready: std.atomic.Value(bool) = .init(false),
};

pub const Prefill = struct {
    prepared_prefix: ?Prefix = null,
    allocator: std.mem.Allocator,
    decode_state: *State,
    token_ids: []const i64,
    seq_len: usize,
    query_seq_len: usize,
    wants_last_logits: bool,
    wants_greedy: bool = false,
    greedy_token: ?u32 = null,
    allow_shared_kv_prefill: bool = false,
    logits: ?[]f32 = null,
    failure: ?anyerror = null,
    ready: std.atomic.Value(bool) = .init(false),
};

/// Validate the full request before reserving any peer KV or recurrent state.
pub fn preparedPrefixStart(work: *const Prefill, item: StepItem) !usize {
    const prefix = work.prepared_prefix orelse return error.InvalidPreparedPrompt;
    const full = prefix.token_count;
    const positions = std.math.mul(usize, full, 3) catch return error.InvalidPreparedPrompt;
    if (item.phase != .prefill or work.query_seq_len == 0 or work.query_seq_len > work.seq_len or
        work.seq_len > full or work.seq_len != item.total_sequence_len or
        work.query_seq_len != item.query_sequence_len or prefix.positions.len != positions or
        prefix.intermediate_layer_count != 0 or prefix.embeddings == null or
        work.wants_last_logits != (work.seq_len == full)) return error.InvalidPreparedPrompt;
    const start = work.seq_len - work.query_seq_len;
    if (work.decode_state.total_tokens != start) return error.InvalidPreparedPrompt;
    return start;
}

pub fn copyPrefixPositions(work: *const Prefill, start: usize, output: []u32, total: usize, offset: usize) void {
    const prefix = work.prepared_prefix.?;
    const full = prefix.token_count;
    const query = work.query_seq_len;
    std.debug.assert(output.len == 3 * total and offset <= total and query <= total - offset);
    std.debug.assert(start <= full and query <= full - start);
    for (0..3) |axis| @memcpy(output[axis * total + offset ..][0..query], prefix.positions[axis * full + start ..][0..query]);
}
