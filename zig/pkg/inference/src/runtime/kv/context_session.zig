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

//! Request-owned context execution contract. All device references are borrowed
//! until the synchronous call returns. A session never aliases another request.
const std = @import("std");

pub const DeviceRef = struct {
    handle: *anyopaque,
    offset: usize = 0,
    bytes: usize,
};
pub const Attention = struct {
    consumer_layer: u32,
    owner_layer: u32,
    query_start: u32,
    query_rows: u32,
    query_heads: u32,
    kv_heads: u32,
    head_dim: u32,
    sliding_window: u32,
    skip_kv_write: bool,
    scale: f32,
    logit_softcap: f32 = 0,
};
pub const Frame = struct {
    /// Backend-owned runtime with an active frame. The hook may only encode;
    /// the backend remains the sole submit/wait/cancel owner.
    runtime: *anyopaque,
};
pub const Metrics = struct {
    logical_tokens: u32 = 0,
    retained_rows: u32 = 0,
    live_bytes: usize = 0,
    staging_bytes: usize = 0,
    peak_bytes: usize = 0,
    query_bytes: usize = 0,
    compacted_blocks: u32 = 0,
    fit_fallbacks: u32 = 0,
    exact_tail_fallbacks: u32 = 0,
    compaction_nanoseconds: u64 = 0,
    rejected_transactions: u64 = 0,
    epoch: u64 = 1,
    status_readback_bytes: u64 = 0,
    attention_status_readback_bytes: u64 = 0,
    kv_readback_bytes: u64 = 0,
    /// Native v2 reads bounded derived P/Y matrices for its FP64 host solve.
    /// This is separate from source KV and validation-status readback.
    derived_attention_readback_bytes: u64 = 0,
    /// Includes replay attention and append status; excludes diagnostic KV reads.
    replay_status_readback_bytes: u64 = 0,
    /// Device-only gathering of fragmented original blocks, never whole-cache copies.
    source_gather_bytes: u64 = 0,
};
pub const Hook = struct {
    ctx: *anyopaque,
    /// Monotonic identity, separate from per-publication epochs.
    identity: u64,
    plan: @import("../context_policy.zig").Plan,
    vtable: *const VTable,
    pub const VTable = struct {
        next_chunk: *const fn (*anyopaque, u64, u32, u32) anyerror!u32,
        attention: *const fn (*anyopaque, u64, Attention, DeviceRef, DeviceRef, DeviceRef, DeviceRef) anyerror!void,
        attention_framed: ?*const fn (*anyopaque, u64, Frame, Attention, DeviceRef, DeviceRef, DeviceRef, DeviceRef) anyerror!void = null,
    };
    pub fn nextChunk(self: Hook, start: u32, requested: u32) !u32 {
        return self.vtable.next_chunk(self.ctx, self.identity, start, requested);
    }
    pub fn attention(self: Hook, request: Attention, q: DeviceRef, k: DeviceRef, v: DeviceRef, out: DeviceRef) !void {
        return self.vtable.attention(self.ctx, self.identity, request, q, k, v, out);
    }
    pub fn supportsFramedAttention(self: Hook) bool {
        return self.vtable.attention_framed != null;
    }
    pub fn attentionFramed(self: Hook, frame: Frame, request: Attention, q: DeviceRef, k: DeviceRef, v: DeviceRef, out: DeviceRef) !void {
        const dispatch = self.vtable.attention_framed orelse return error.ContextFramedExecutionUnsupported;
        return dispatch(self.ctx, self.identity, frame, request, q, k, v, out);
    }
};
