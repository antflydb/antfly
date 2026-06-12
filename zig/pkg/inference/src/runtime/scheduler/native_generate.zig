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

const std = @import("std");

pub const RequestId = u64;

pub const Phase = enum {
    waiting,
    prefill,
    decode,
};

pub const Admission = struct {
    requested_units: usize,
    prompt_bytes: usize,
    max_tokens: i32,
};

pub const Lease = struct {
    request_id: RequestId,
    reserved_units: usize,
    prompt_bytes: usize,
    max_tokens: i32,
    prefill_chunk_size: usize,
    active_requests_snapshot: usize,
};

pub const Policy = struct {
    /// Default item cap for a unified step (prefill chunks + decode tokens packed
    /// into one fused forward pass).
    max_step_items: usize = 16,
    /// Default query-token cap for a unified step. Decode items contribute 1
    /// token; prefill items contribute their `query_sequence_len`.
    max_step_query_tokens: usize = 512,

    pub fn defaultStepBudget(self: Policy) StepBudget {
        return .{
            .max_items = @max(self.max_step_items, 1),
            .max_query_tokens = @max(self.max_step_query_tokens, 1),
        };
    }
};

/// Admission budget for a single unified step. A step packs whatever
/// pending prefill chunks and decode tokens fit, capped by these limits.
///
/// Step-level admission replaces the older epoch-turn `max_batch_lead_wait_turns`
/// deferral heuristic: instead of waiting for a minimum batch size to materialize,
/// we always emit a step containing at least the leader and pack the rest of the
/// queue up to the budget.
pub const StepBudget = struct {
    /// Maximum total items (prefill + decode) packed into one step.
    max_items: usize,
    /// Maximum query tokens fused into one forward pass. Decode items contribute
    /// 1 token each; prefill items contribute their `query_sequence_len`.
    max_query_tokens: usize,
    /// Optional KV block budget. When set, items whose
    /// `kv_blocks_estimate` (configured via `notePendingKvBlocks`) would exceed
    /// the running total are skipped. The leader is always admitted regardless
    /// of this budget; caller-side admission must gate at acquire time when
    /// pool headroom for the leader itself is uncertain.
    max_kv_blocks: ?usize = null,
};

pub const Stats = struct {
    turn_yields_total: u64 = 0,

    // Unified-step counters. A "step" is a single fused forward pass that may
    // contain any mix of prefill chunks and decode tokens.
    step_batches_total: u64 = 0,
    step_prefill_items_total: u64 = 0,
    step_decode_items_total: u64 = 0,
    step_query_tokens_total: u64 = 0,
    step_singleton_batches_total: u64 = 0,
    step_kv_block_skips_total: u64 = 0,
};

const Entry = struct {
    id: RequestId,
    requested_units: usize,
    prompt_bytes: usize,
    max_tokens: i32,
    phase: Phase = .waiting,
    prompt_tokens_processed: usize = 0,
    total_prompt_tokens: usize = 0,
    generated_tokens: usize = 0,
};

const PendingDecode = struct {
    request_id: RequestId,
    work_ptr: *anyopaque,
    total_sequence_len: usize,
    kv_sequence_len: usize,
    kv_position_offset: usize,
    kv_blocks_estimate: usize = 0,
    exclusive_step: bool = false,
};

const PendingPrefill = struct {
    request_id: RequestId,
    work_ptr: *anyopaque,
    total_sequence_len: usize,
    query_sequence_len: usize,
    kv_sequence_len: usize,
    kv_position_offset: usize,
    kv_blocks_estimate: usize = 0,
    exclusive_step: bool = false,
};

pub const StepItem = struct {
    work_ptr: *anyopaque,
    phase: Phase,
    query_sequence_len: usize,
    total_sequence_len: usize,
    kv_sequence_len: usize,
    kv_position_offset: usize,
};

pub const Snapshot = struct {
    waiting_requests: usize,
    prefill_requests: usize,
    decode_requests: usize,
    active_units: usize,
};

pub const NativeGenerateCoordinator = struct {
    allocator: std.mem.Allocator,
    policy: Policy = .{},
    stats: Stats = .{},
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    next_request_id: RequestId = 1,
    active_units: usize = 0,
    pending_prefill: std.ArrayListUnmanaged(PendingPrefill) = .empty,
    pending_decode: std.ArrayListUnmanaged(PendingDecode) = .empty,
    in_turn: ?RequestId = null,
    in_turn_phase: ?Phase = null,
    last_prefill_granted: ?RequestId = null,
    last_decode_granted: ?RequestId = null,
    consecutive_decode_turns: usize = 0,
    max_decode_streak_before_prefill: usize = 4,
    base_prefill_chunk_size: usize = 256,
    min_prefill_chunk_size: usize = 32,

    pub fn init(allocator: std.mem.Allocator) NativeGenerateCoordinator {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *NativeGenerateCoordinator) void {
        self.entries.deinit(self.allocator);
        self.pending_prefill.deinit(self.allocator);
        self.pending_decode.deinit(self.allocator);
    }

    pub fn acquire(self: *NativeGenerateCoordinator, admission: Admission) !Lease {
        const request_id = self.next_request_id;
        self.next_request_id += 1;
        const reserved_units = @max(admission.requested_units, 1);
        try self.entries.append(self.allocator, .{
            .id = request_id,
            .requested_units = reserved_units,
            .prompt_bytes = admission.prompt_bytes,
            .max_tokens = admission.max_tokens,
            .phase = .waiting,
        });
        self.active_units += reserved_units;

        return .{
            .request_id = request_id,
            .reserved_units = reserved_units,
            .prompt_bytes = admission.prompt_bytes,
            .max_tokens = admission.max_tokens,
            .prefill_chunk_size = self.recommendPrefillChunkFor(request_id),
            .active_requests_snapshot = self.entries.items.len,
        };
    }

    pub fn release(self: *NativeGenerateCoordinator, lease: Lease) void {
        const idx = self.indexOf(lease.request_id) orelse return;
        const entry = self.entries.items[idx];
        self.removePendingPrefillByRequest(lease.request_id);
        self.removePendingDecodeByRequest(lease.request_id);
        if (self.in_turn == lease.request_id) {
            self.in_turn = null;
            self.in_turn_phase = null;
        }
        if (self.active_units > entry.requested_units) {
            self.active_units -= entry.requested_units;
        } else {
            self.active_units = 0;
        }
        _ = self.entries.swapRemove(idx);
    }

    pub fn enqueuePrefillWork(
        self: *NativeGenerateCoordinator,
        lease: Lease,
        work_ptr: *anyopaque,
        total_sequence_len: usize,
        query_sequence_len: usize,
        kv_sequence_len: usize,
        kv_position_offset: usize,
    ) !void {
        if (self.findPendingPrefill(work_ptr) != null) return;
        try self.pending_prefill.append(self.allocator, .{
            .request_id = lease.request_id,
            .work_ptr = work_ptr,
            .total_sequence_len = total_sequence_len,
            .query_sequence_len = query_sequence_len,
            .kv_sequence_len = kv_sequence_len,
            .kv_position_offset = kv_position_offset,
        });
    }

    pub fn cancelPrefillWork(self: *NativeGenerateCoordinator, work_ptr: *anyopaque) void {
        self.removePendingPrefill(work_ptr);
    }

    pub fn enqueueDecodeWork(
        self: *NativeGenerateCoordinator,
        lease: Lease,
        work_ptr: *anyopaque,
        total_sequence_len: usize,
        kv_sequence_len: usize,
        kv_position_offset: usize,
    ) !void {
        if (self.findPendingDecode(work_ptr) != null) return;
        try self.pending_decode.append(self.allocator, .{
            .request_id = lease.request_id,
            .work_ptr = work_ptr,
            .total_sequence_len = total_sequence_len,
            .kv_sequence_len = kv_sequence_len,
            .kv_position_offset = kv_position_offset,
        });
    }

    pub fn cancelDecodeWork(self: *NativeGenerateCoordinator, work_ptr: *anyopaque) void {
        self.removePendingDecode(work_ptr);
    }

    /// Default per-step admission budget derived from the configured policy.
    pub fn defaultStepBudget(self: *const NativeGenerateCoordinator) StepBudget {
        return self.policy.defaultStepBudget();
    }

    /// Record an estimated KV-block cost for an already-enqueued pending item.
    /// Used by `claimStep` when the caller supplies `budget.max_kv_blocks` so
    /// that admission tracks the KV pool's free-block headroom.
    pub fn notePendingKvBlocks(
        self: *NativeGenerateCoordinator,
        work_ptr: *anyopaque,
        phase: Phase,
        blocks: usize,
    ) void {
        switch (phase) {
            .decode => {
                if (self.findPendingDecode(work_ptr)) |idx| {
                    self.pending_decode.items[idx].kv_blocks_estimate = blocks;
                }
            },
            .prefill => {
                if (self.findPendingPrefill(work_ptr)) |idx| {
                    self.pending_prefill.items[idx].kv_blocks_estimate = blocks;
                }
            },
            .waiting => {},
        }
    }

    /// Read back the recorded KV-block estimate for a pending item. Returns
    /// null when the work is not enqueued in the requested phase. Useful for
    /// tests and observability — production admission consults the field
    /// internally during `claimStep`.
    pub fn pendingKvBlocksEstimate(
        self: *const NativeGenerateCoordinator,
        work_ptr: *anyopaque,
        phase: Phase,
    ) ?usize {
        switch (phase) {
            .decode => {
                const idx = self.findPendingDecode(work_ptr) orelse return null;
                return self.pending_decode.items[idx].kv_blocks_estimate;
            },
            .prefill => {
                const idx = self.findPendingPrefill(work_ptr) orelse return null;
                return self.pending_prefill.items[idx].kv_blocks_estimate;
            },
            .waiting => return null,
        }
    }

    /// Mark an already-enqueued pending item as requiring a singleton scheduler
    /// step. The item still participates in turn-taking and budget accounting,
    /// but `claimStep` will not batch it with any peer work.
    pub fn notePendingExclusiveStep(
        self: *NativeGenerateCoordinator,
        work_ptr: *anyopaque,
        phase: Phase,
        exclusive: bool,
    ) void {
        switch (phase) {
            .decode => {
                if (self.findPendingDecode(work_ptr)) |idx| {
                    self.pending_decode.items[idx].exclusive_step = exclusive;
                }
            },
            .prefill => {
                if (self.findPendingPrefill(work_ptr)) |idx| {
                    self.pending_prefill.items[idx].exclusive_step = exclusive;
                }
            },
            .waiting => {},
        }
    }

    pub fn pendingRequiresExclusiveStep(
        self: *const NativeGenerateCoordinator,
        work_ptr: *anyopaque,
        phase: Phase,
    ) ?bool {
        switch (phase) {
            .decode => {
                const idx = self.findPendingDecode(work_ptr) orelse return null;
                return self.pending_decode.items[idx].exclusive_step;
            },
            .prefill => {
                const idx = self.findPendingPrefill(work_ptr) orelse return null;
                return self.pending_prefill.items[idx].exclusive_step;
            },
            .waiting => return null,
        }
    }

    /// Unified step claim: replaces the prefill/decode/mixed claim trio with a
    /// single entry point that always packs whatever pending work fits the
    /// budget. The leader is always included; additional items are packed
    /// decode-first (cheap, latency-sensitive), then prefill chunks.
    ///
    /// Per-step admission is the headroom budget itself — there is no
    /// minimum-batch-size deferral. A leader-only step is a valid outcome when
    /// no other compatible work is queued.
    pub fn claimStep(
        self: *NativeGenerateCoordinator,
        allocator: std.mem.Allocator,
        lease: *Lease,
        leader_work_ptr: *anyopaque,
        leader_phase: Phase,
        budget: StepBudget,
        out: *std.ArrayListUnmanaged(StepItem),
    ) !bool {
        if (self.in_turn) |owner| {
            if (owner != lease.request_id) return false;
        } else {
            const selected = self.pickNextTurn() orelse return false;
            if (selected != lease.request_id) return false;
        }

        const max_items = @max(budget.max_items, 1);
        const max_query_tokens = @max(budget.max_query_tokens, 1);

        out.clearRetainingCapacity();

        var query_tokens_total: usize = 0;
        var kv_blocks_total: usize = 0;
        var leader_exclusive = false;

        switch (leader_phase) {
            .decode => {
                const leader_idx = self.findPendingDecode(leader_work_ptr) orelse return false;
                const pending = self.pending_decode.items[leader_idx];
                try out.append(allocator, .{
                    .work_ptr = pending.work_ptr,
                    .phase = .decode,
                    .query_sequence_len = 1,
                    .total_sequence_len = pending.total_sequence_len,
                    .kv_sequence_len = pending.kv_sequence_len,
                    .kv_position_offset = pending.kv_position_offset,
                });
                query_tokens_total = 1;
                kv_blocks_total = pending.kv_blocks_estimate;
                leader_exclusive = pending.exclusive_step;
            },
            .prefill => {
                const leader_idx = self.findPendingPrefill(leader_work_ptr) orelse return false;
                const pending = self.pending_prefill.items[leader_idx];
                try out.append(allocator, .{
                    .work_ptr = pending.work_ptr,
                    .phase = .prefill,
                    .query_sequence_len = pending.query_sequence_len,
                    .total_sequence_len = pending.total_sequence_len,
                    .kv_sequence_len = pending.kv_sequence_len,
                    .kv_position_offset = pending.kv_position_offset,
                });
                query_tokens_total = pending.query_sequence_len;
                kv_blocks_total = pending.kv_blocks_estimate;
                leader_exclusive = pending.exclusive_step;
            },
            .waiting => return false,
        }

        if (leader_exclusive) {
            self.in_turn = lease.request_id;
            self.in_turn_phase = leader_phase;
            return true;
        }

        // Pack additional decode items first — they're single-token and most
        // benefit from coalescing into the same forward pass.
        for (self.pending_decode.items) |pending| {
            if (out.items.len >= max_items) break;
            if (pending.work_ptr == leader_work_ptr) continue;
            if (pending.exclusive_step) continue;
            const next_query = query_tokens_total + 1;
            if (next_query > max_query_tokens) break;
            if (budget.max_kv_blocks) |limit| {
                const next_kv = kv_blocks_total + pending.kv_blocks_estimate;
                if (next_kv > limit) {
                    self.stats.step_kv_block_skips_total += 1;
                    continue;
                }
                kv_blocks_total = next_kv;
            }
            try out.append(allocator, .{
                .work_ptr = pending.work_ptr,
                .phase = .decode,
                .query_sequence_len = 1,
                .total_sequence_len = pending.total_sequence_len,
                .kv_sequence_len = pending.kv_sequence_len,
                .kv_position_offset = pending.kv_position_offset,
            });
            query_tokens_total = next_query;
        }

        // Then fill remaining slack with prefill chunks. We probe each pending
        // prefill against the remaining query-token budget rather than break
        // on the first oversize chunk, so a smaller chunk later in the queue
        // can still be admitted.
        for (self.pending_prefill.items) |pending| {
            if (out.items.len >= max_items) break;
            if (pending.work_ptr == leader_work_ptr) continue;
            if (pending.exclusive_step) continue;
            const next_query = query_tokens_total + pending.query_sequence_len;
            if (next_query > max_query_tokens) continue;
            if (budget.max_kv_blocks) |limit| {
                const next_kv = kv_blocks_total + pending.kv_blocks_estimate;
                if (next_kv > limit) {
                    self.stats.step_kv_block_skips_total += 1;
                    continue;
                }
                kv_blocks_total = next_kv;
            }
            try out.append(allocator, .{
                .work_ptr = pending.work_ptr,
                .phase = .prefill,
                .query_sequence_len = pending.query_sequence_len,
                .total_sequence_len = pending.total_sequence_len,
                .kv_sequence_len = pending.kv_sequence_len,
                .kv_position_offset = pending.kv_position_offset,
            });
            query_tokens_total = next_query;
        }

        self.in_turn = lease.request_id;
        // The in-turn phase tracks the leader so finishTurn fairness reflects
        // the request that was woken to drive this step.
        self.in_turn_phase = leader_phase;
        return true;
    }

    /// Mark a unified step complete. Removes pending entries, updates step_*
    /// stats, and releases the turn. Streak fairness is biased toward decode
    /// when any decode item executed (so prefill chunks served alongside
    /// decode don't double-charge the prefill rotation).
    pub fn completeStep(
        self: *NativeGenerateCoordinator,
        lease: *Lease,
        items: []const StepItem,
    ) void {
        var prefill_count: u64 = 0;
        var decode_count: u64 = 0;
        var query_tokens: u64 = 0;
        for (items) |item| {
            switch (item.phase) {
                .prefill => {
                    self.removePendingPrefill(item.work_ptr);
                    prefill_count += 1;
                    query_tokens += @intCast(item.query_sequence_len);
                },
                .decode => {
                    self.removePendingDecode(item.work_ptr);
                    decode_count += 1;
                    query_tokens += 1;
                },
                .waiting => {},
            }
        }
        self.stats.step_batches_total += 1;
        self.stats.step_prefill_items_total += prefill_count;
        self.stats.step_decode_items_total += decode_count;
        self.stats.step_query_tokens_total += query_tokens;
        if (prefill_count + decode_count <= 1) {
            self.stats.step_singleton_batches_total += 1;
        }
        const phase: Phase = if (decode_count > 0) .decode else .prefill;
        self.finishTurn(lease, phase);
    }

    pub fn awaitTurn(self: *NativeGenerateCoordinator, lease: *Lease, phase: Phase, io: std.Io) void {
        while (!self.tryAcquireTurn(lease, phase)) {
            self.stats.turn_yields_total += 1;
            io.sleep(std.Io.Duration.fromMilliseconds(0), .awake) catch return;
        }
    }

    pub fn tryAcquireTurn(self: *NativeGenerateCoordinator, lease: *Lease, phase: Phase) bool {
        if (self.in_turn) |owner| {
            return owner == lease.request_id and self.in_turn_phase == phase;
        }

        const selected = self.pickNextTurn() orelse return true;
        if (selected != lease.request_id) return false;

        self.in_turn = lease.request_id;
        self.in_turn_phase = phase;
        lease.prefill_chunk_size = self.recommendPrefillChunkFor(lease.request_id);
        return true;
    }

    pub fn finishTurn(self: *NativeGenerateCoordinator, lease: *Lease, phase: Phase) void {
        if (self.in_turn != lease.request_id) return;
        self.in_turn = null;
        self.in_turn_phase = null;
        switch (phase) {
            .decode => {
                self.last_decode_granted = lease.request_id;
                self.consecutive_decode_turns += 1;
            },
            .prefill => {
                self.last_prefill_granted = lease.request_id;
                self.consecutive_decode_turns = 0;
            },
            .waiting => {},
        }
    }

    pub fn notePrefillProgress(self: *NativeGenerateCoordinator, lease: *Lease, processed_tokens: usize, total_prompt_tokens: usize) void {
        const idx = self.indexOf(lease.request_id) orelse return;
        var entry = &self.entries.items[idx];
        entry.phase = .prefill;
        entry.prompt_tokens_processed = processed_tokens;
        entry.total_prompt_tokens = total_prompt_tokens;
        lease.prefill_chunk_size = self.recommendPrefillChunkFor(lease.request_id);
    }

    pub fn beginDecode(self: *NativeGenerateCoordinator, lease: *Lease, total_prompt_tokens: usize) void {
        const idx = self.indexOf(lease.request_id) orelse return;
        var entry = &self.entries.items[idx];
        entry.phase = .decode;
        entry.prompt_tokens_processed = total_prompt_tokens;
        entry.total_prompt_tokens = total_prompt_tokens;
        lease.prefill_chunk_size = self.recommendPrefillChunkFor(lease.request_id);
    }

    pub fn noteDecodeProgress(self: *NativeGenerateCoordinator, lease: *Lease, generated_tokens: usize) void {
        const idx = self.indexOf(lease.request_id) orelse return;
        var entry = &self.entries.items[idx];
        entry.phase = .decode;
        entry.generated_tokens = generated_tokens;
    }

    pub fn snapshot(self: *const NativeGenerateCoordinator) Snapshot {
        var waiting_requests: usize = 0;
        var prefill_requests: usize = 0;
        var decode_requests: usize = 0;
        for (self.entries.items) |entry| {
            switch (entry.phase) {
                .waiting => waiting_requests += 1,
                .prefill => prefill_requests += 1,
                .decode => decode_requests += 1,
            }
        }
        return .{
            .waiting_requests = waiting_requests,
            .prefill_requests = prefill_requests,
            .decode_requests = decode_requests,
            .active_units = self.active_units,
        };
    }

    pub fn schedulerStats(self: *const NativeGenerateCoordinator) Stats {
        return self.stats;
    }

    fn recommendPrefillChunkFor(self: *const NativeGenerateCoordinator, request_id: RequestId) usize {
        const idx = self.indexOf(request_id) orelse return self.base_prefill_chunk_size;
        const entry = self.entries.items[idx];
        const state = self.snapshot();

        const prompt_bytes = @max(entry.prompt_bytes, 1);
        const prompt_token_estimate = @max(prompt_bytes / 4, 1);

        var target = self.base_prefill_chunk_size;
        if (state.decode_requests > 0) {
            target = self.min_prefill_chunk_size;
        } else if (state.prefill_requests >= 3 or state.active_units >= 12) {
            target = self.min_prefill_chunk_size;
        } else if (state.prefill_requests >= 2 or state.active_units >= 8) {
            target = 64;
        } else if (state.waiting_requests > 1 or state.active_units >= 4) {
            target = 128;
        }

        if (entry.max_tokens <= 64 and target > 128) {
            target = 128;
        }

        target = @min(target, prompt_token_estimate);
        if (target <= self.min_prefill_chunk_size) return self.min_prefill_chunk_size;
        return alignDown(target, self.min_prefill_chunk_size);
    }

    fn indexOf(self: *const NativeGenerateCoordinator, request_id: RequestId) ?usize {
        for (self.entries.items, 0..) |entry, idx| {
            if (entry.id == request_id) return idx;
        }
        return null;
    }

    fn pickNextTurn(self: *const NativeGenerateCoordinator) ?RequestId {
        const prefill_candidate = self.pickPendingPrefillRoundRobin(self.last_prefill_granted) orelse self.pickPhaseRoundRobin(.prefill, self.last_prefill_granted);
        const decode_candidate = self.pickPendingDecodeRoundRobin(self.last_decode_granted);

        if (decode_candidate) |decode_id| {
            if (prefill_candidate != null and self.consecutive_decode_turns >= self.max_decode_streak_before_prefill) {
                return prefill_candidate;
            }
            return decode_id;
        }
        if (prefill_candidate) |prefill_id| return prefill_id;
        return null;
    }

    fn pickPhaseRoundRobin(self: *const NativeGenerateCoordinator, phase: Phase, last_granted: ?RequestId) ?RequestId {
        if (self.entries.items.len == 0) return null;

        var start_idx: usize = 0;
        if (last_granted) |last_id| {
            if (self.indexOf(last_id)) |idx| {
                start_idx = (idx + 1) % self.entries.items.len;
            }
        }

        for (0..self.entries.items.len) |offset| {
            const idx = (start_idx + offset) % self.entries.items.len;
            const entry = self.entries.items[idx];
            if (entry.phase == phase) return entry.id;
        }
        return null;
    }

    fn pickPendingDecodeRoundRobin(self: *const NativeGenerateCoordinator, last_granted: ?RequestId) ?RequestId {
        if (self.pending_decode.items.len == 0) return null;

        var start_idx: usize = 0;
        if (last_granted) |last_id| {
            if (self.findPendingDecodeByRequest(last_id)) |idx| {
                start_idx = (idx + 1) % self.pending_decode.items.len;
            }
        }

        for (0..self.pending_decode.items.len) |offset| {
            const idx = (start_idx + offset) % self.pending_decode.items.len;
            return self.pending_decode.items[idx].request_id;
        }
        return null;
    }

    fn pickPendingPrefillRoundRobin(self: *const NativeGenerateCoordinator, last_granted: ?RequestId) ?RequestId {
        if (self.pending_prefill.items.len == 0) return null;

        var start_idx: usize = 0;
        if (last_granted) |last_id| {
            if (self.findPendingPrefillByRequest(last_id)) |idx| {
                start_idx = (idx + 1) % self.pending_prefill.items.len;
            }
        }

        for (0..self.pending_prefill.items.len) |offset| {
            const idx = (start_idx + offset) % self.pending_prefill.items.len;
            return self.pending_prefill.items[idx].request_id;
        }
        return null;
    }

    fn findPendingPrefill(self: *const NativeGenerateCoordinator, work_ptr: *anyopaque) ?usize {
        for (self.pending_prefill.items, 0..) |pending, idx| {
            if (pending.work_ptr == work_ptr) return idx;
        }
        return null;
    }

    fn findPendingPrefillByRequest(self: *const NativeGenerateCoordinator, request_id: RequestId) ?usize {
        for (self.pending_prefill.items, 0..) |pending, idx| {
            if (pending.request_id == request_id) return idx;
        }
        return null;
    }

    fn removePendingPrefill(self: *NativeGenerateCoordinator, work_ptr: *anyopaque) void {
        if (self.findPendingPrefill(work_ptr)) |idx| {
            _ = self.pending_prefill.swapRemove(idx);
        }
    }

    fn removePendingPrefillByRequest(self: *NativeGenerateCoordinator, request_id: RequestId) void {
        var idx: usize = 0;
        while (idx < self.pending_prefill.items.len) {
            if (self.pending_prefill.items[idx].request_id == request_id) {
                _ = self.pending_prefill.swapRemove(idx);
            } else {
                idx += 1;
            }
        }
    }

    fn findPendingDecode(self: *const NativeGenerateCoordinator, work_ptr: *anyopaque) ?usize {
        for (self.pending_decode.items, 0..) |pending, idx| {
            if (pending.work_ptr == work_ptr) return idx;
        }
        return null;
    }

    fn findPendingDecodeByRequest(self: *const NativeGenerateCoordinator, request_id: RequestId) ?usize {
        for (self.pending_decode.items, 0..) |pending, idx| {
            if (pending.request_id == request_id) return idx;
        }
        return null;
    }

    fn removePendingDecode(self: *NativeGenerateCoordinator, work_ptr: *anyopaque) void {
        if (self.findPendingDecode(work_ptr)) |idx| {
            _ = self.pending_decode.swapRemove(idx);
        }
    }

    fn removePendingDecodeByRequest(self: *NativeGenerateCoordinator, request_id: RequestId) void {
        var idx: usize = 0;
        while (idx < self.pending_decode.items.len) {
            if (self.pending_decode.items[idx].request_id == request_id) {
                _ = self.pending_decode.swapRemove(idx);
            } else {
                idx += 1;
            }
        }
    }

    fn alignDown(value: usize, alignment: usize) usize {
        if (alignment <= 1) return value;
        return value - (value % alignment);
    }
};

pub fn aggregateStats(models: anytype) struct { snapshot: Snapshot, stats: Stats } {
    var snapshot = Snapshot{
        .waiting_requests = 0,
        .prefill_requests = 0,
        .decode_requests = 0,
        .active_units = 0,
    };
    var stats = Stats{};

    var it = models.iterator();
    while (it.next()) |entry| {
        const coordinator = entry.value_ptr.*.native_generate_coordinator orelse continue;
        const s = coordinator.snapshot();
        snapshot.waiting_requests += s.waiting_requests;
        snapshot.prefill_requests += s.prefill_requests;
        snapshot.decode_requests += s.decode_requests;
        snapshot.active_units += s.active_units;

        const st = coordinator.schedulerStats();
        stats.turn_yields_total += st.turn_yields_total;
        stats.step_batches_total += st.step_batches_total;
        stats.step_prefill_items_total += st.step_prefill_items_total;
        stats.step_decode_items_total += st.step_decode_items_total;
        stats.step_query_tokens_total += st.step_query_tokens_total;
        stats.step_singleton_batches_total += st.step_singleton_batches_total;
        stats.step_kv_block_skips_total += st.step_kv_block_skips_total;
    }

    return .{ .snapshot = snapshot, .stats = stats };
}

test "native generate coordinator tracks waiting prefill and decode phases" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var first = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 4096,
        .max_tokens = 256,
    });
    defer coordinator.release(first);

    const second = try coordinator.acquire(.{
        .requested_units = 4,
        .prompt_bytes = 4096,
        .max_tokens = 256,
    });
    defer coordinator.release(second);

    var snapshot = coordinator.snapshot();
    try std.testing.expectEqual(@as(usize, 2), snapshot.waiting_requests);
    try std.testing.expectEqual(@as(usize, 0), snapshot.prefill_requests);

    coordinator.notePrefillProgress(&first, 128, 1024);
    snapshot = coordinator.snapshot();
    try std.testing.expectEqual(@as(usize, 1), snapshot.waiting_requests);
    try std.testing.expectEqual(@as(usize, 1), snapshot.prefill_requests);
    try std.testing.expectEqual(@as(usize, 128), coordinator.recommendPrefillChunkFor(second.request_id));

    coordinator.beginDecode(&first, 1024);
    snapshot = coordinator.snapshot();
    try std.testing.expectEqual(@as(usize, 1), snapshot.decode_requests);
    try std.testing.expectEqual(@as(usize, 32), coordinator.recommendPrefillChunkFor(second.request_id));
}

test "native generate coordinator caps chunk by prompt size" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 96,
        .max_tokens = 16,
    });
    defer coordinator.release(lease);

    coordinator.notePrefillProgress(&lease, 0, 24);
    try std.testing.expectEqual(@as(usize, 32), lease.prefill_chunk_size);
}

test "native generate coordinator round-robins turns and prioritizes decode" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var first = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 4096,
        .max_tokens = 256,
    });
    defer coordinator.release(first);
    var second = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 4096,
        .max_tokens = 256,
    });
    defer coordinator.release(second);

    coordinator.notePrefillProgress(&first, 64, 512);
    coordinator.notePrefillProgress(&second, 64, 512);

    try std.testing.expect(coordinator.tryAcquireTurn(&first, .prefill));
    try std.testing.expect(!coordinator.tryAcquireTurn(&second, .prefill));
    coordinator.finishTurn(&first, .prefill);

    try std.testing.expect(coordinator.tryAcquireTurn(&second, .prefill));
    coordinator.finishTurn(&second, .prefill);

    coordinator.beginDecode(&first, 512);
    try coordinator.enqueueDecodeWork(first, @ptrFromInt(1), 512, 512, 0);
    try std.testing.expect(coordinator.tryAcquireTurn(&first, .decode));
    try std.testing.expect(!coordinator.tryAcquireTurn(&second, .prefill));
}

test "claimStep packs decode leader with extra decode and prefill work" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var decode_lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 128,
        .max_tokens = 16,
    });
    defer coordinator.release(decode_lease);
    var prefill_lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 128,
        .max_tokens = 16,
    });
    defer coordinator.release(prefill_lease);

    var decode_a: u8 = 1;
    var decode_b: u8 = 2;
    var prefill_w: u8 = 3;

    coordinator.beginDecode(&decode_lease, 8);
    coordinator.notePrefillProgress(&prefill_lease, 0, 8);
    try coordinator.enqueueDecodeWork(decode_lease, @ptrCast(&decode_a), 9, 9, 0);
    try coordinator.enqueueDecodeWork(decode_lease, @ptrCast(&decode_b), 9, 9, 0);
    try coordinator.enqueuePrefillWork(prefill_lease, @ptrCast(&prefill_w), 10, 4, 6, 0);

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);

    const claimed = try coordinator.claimStep(
        allocator,
        &decode_lease,
        @ptrCast(&decode_a),
        .decode,
        coordinator.defaultStepBudget(),
        &step,
    );
    try std.testing.expect(claimed);
    try std.testing.expectEqual(@as(usize, 3), step.items.len);
    try std.testing.expectEqual(@as(*anyopaque, @ptrCast(&decode_a)), step.items[0].work_ptr);

    var saw_other_decode = false;
    var saw_prefill = false;
    for (step.items[1..]) |item| {
        if (item.phase == .decode and item.work_ptr == @as(*anyopaque, @ptrCast(&decode_b))) saw_other_decode = true;
        if (item.phase == .prefill and item.work_ptr == @as(*anyopaque, @ptrCast(&prefill_w))) saw_prefill = true;
    }
    try std.testing.expect(saw_other_decode);
    try std.testing.expect(saw_prefill);
}

test "claimStep keeps exclusive leader as singleton" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 128,
        .max_tokens = 16,
    });
    defer coordinator.release(lease);

    var exclusive_work: u8 = 1;
    var peer_work: u8 = 2;

    coordinator.beginDecode(&lease, 8);
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&exclusive_work), 9, 9, 0);
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&peer_work), 9, 9, 0);
    coordinator.notePendingExclusiveStep(@ptrCast(&exclusive_work), .decode, true);

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);

    const claimed = try coordinator.claimStep(
        allocator,
        &lease,
        @ptrCast(&exclusive_work),
        .decode,
        coordinator.defaultStepBudget(),
        &step,
    );
    try std.testing.expect(claimed);
    try std.testing.expectEqual(@as(usize, 1), step.items.len);
    try std.testing.expectEqual(@as(*anyopaque, @ptrCast(&exclusive_work)), step.items[0].work_ptr);
}

test "claimStep skips exclusive peer work" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 128,
        .max_tokens = 16,
    });
    defer coordinator.release(lease);

    var leader_work: u8 = 1;
    var exclusive_peer: u8 = 2;
    var normal_peer: u8 = 3;

    coordinator.beginDecode(&lease, 8);
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&leader_work), 9, 9, 0);
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&exclusive_peer), 9, 9, 0);
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&normal_peer), 9, 9, 0);
    coordinator.notePendingExclusiveStep(@ptrCast(&exclusive_peer), .decode, true);

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);

    const claimed = try coordinator.claimStep(
        allocator,
        &lease,
        @ptrCast(&leader_work),
        .decode,
        coordinator.defaultStepBudget(),
        &step,
    );
    try std.testing.expect(claimed);
    try std.testing.expectEqual(@as(usize, 2), step.items.len);
    try std.testing.expectEqual(@as(*anyopaque, @ptrCast(&leader_work)), step.items[0].work_ptr);
    try std.testing.expectEqual(@as(*anyopaque, @ptrCast(&normal_peer)), step.items[1].work_ptr);
}

test "claimStep emits leader-only step when no other work is queued" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var decode_lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 64,
        .max_tokens = 8,
    });
    defer coordinator.release(decode_lease);

    var work: u8 = 1;
    coordinator.beginDecode(&decode_lease, 4);
    try coordinator.enqueueDecodeWork(decode_lease, @ptrCast(&work), 5, 5, 0);

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);

    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &decode_lease,
        @ptrCast(&work),
        .decode,
        coordinator.defaultStepBudget(),
        &step,
    ));
    try std.testing.expectEqual(@as(usize, 1), step.items.len);
    try std.testing.expectEqual(@as(*anyopaque, @ptrCast(&work)), step.items[0].work_ptr);
}

test "claimStep with prefill leader and no pending decodes still admits the leader" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var prefill_lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 256,
        .max_tokens = 8,
    });
    defer coordinator.release(prefill_lease);

    var work: u8 = 1;
    coordinator.notePrefillProgress(&prefill_lease, 0, 16);
    try coordinator.enqueuePrefillWork(prefill_lease, @ptrCast(&work), 16, 8, 8, 0);

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);

    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &prefill_lease,
        @ptrCast(&work),
        .prefill,
        coordinator.defaultStepBudget(),
        &step,
    ));
    try std.testing.expectEqual(@as(usize, 1), step.items.len);
    try std.testing.expectEqual(Phase.prefill, step.items[0].phase);
    try std.testing.expectEqual(@as(usize, 8), step.items[0].query_sequence_len);
}

test "claimStep respects max_items budget" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var lease_a = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(lease_a);
    var lease_b = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(lease_b);
    var lease_c = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(lease_c);

    var w_a: u8 = 1;
    var w_b: u8 = 2;
    var w_c: u8 = 3;

    coordinator.beginDecode(&lease_a, 4);
    coordinator.beginDecode(&lease_b, 4);
    coordinator.beginDecode(&lease_c, 4);
    try coordinator.enqueueDecodeWork(lease_a, @ptrCast(&w_a), 5, 5, 0);
    try coordinator.enqueueDecodeWork(lease_b, @ptrCast(&w_b), 5, 5, 0);
    try coordinator.enqueueDecodeWork(lease_c, @ptrCast(&w_c), 5, 5, 0);

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);

    const budget = StepBudget{ .max_items = 2, .max_query_tokens = 1024 };
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &lease_a,
        @ptrCast(&w_a),
        .decode,
        budget,
        &step,
    ));
    try std.testing.expectEqual(@as(usize, 2), step.items.len);
}

test "claimStep respects max_query_tokens budget for prefill chunks" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var leader_lease = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(leader_lease);
    var big_lease = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 1024, .max_tokens = 8 });
    defer coordinator.release(big_lease);
    var small_lease = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(small_lease);

    var leader_w: u8 = 1;
    var big_w: u8 = 2;
    var small_w: u8 = 3;

    coordinator.beginDecode(&leader_lease, 4);
    coordinator.notePrefillProgress(&big_lease, 0, 256);
    coordinator.notePrefillProgress(&small_lease, 0, 16);
    try coordinator.enqueueDecodeWork(leader_lease, @ptrCast(&leader_w), 5, 5, 0);
    try coordinator.enqueuePrefillWork(big_lease, @ptrCast(&big_w), 256, 256, 0, 0);
    try coordinator.enqueuePrefillWork(small_lease, @ptrCast(&small_w), 16, 16, 0, 0);

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);

    // Budget allows 32 query tokens: leader (1) + small prefill (16) fits;
    // the 256-chunk prefill must be skipped, but the smaller chunk still admits.
    const budget = StepBudget{ .max_items = 8, .max_query_tokens = 32 };
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &leader_lease,
        @ptrCast(&leader_w),
        .decode,
        budget,
        &step,
    ));

    var saw_small = false;
    var saw_big = false;
    for (step.items) |item| {
        if (item.work_ptr == @as(*anyopaque, @ptrCast(&small_w))) saw_small = true;
        if (item.work_ptr == @as(*anyopaque, @ptrCast(&big_w))) saw_big = true;
    }
    try std.testing.expect(saw_small);
    try std.testing.expect(!saw_big);
}

test "claimStep skips items whose KV-block estimate exceeds budget" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var lease_a = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(lease_a);
    var lease_b = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(lease_b);
    var lease_c = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(lease_c);

    var w_a: u8 = 1;
    var w_b: u8 = 2;
    var w_c: u8 = 3;

    coordinator.beginDecode(&lease_a, 4);
    coordinator.beginDecode(&lease_b, 4);
    coordinator.beginDecode(&lease_c, 4);
    try coordinator.enqueueDecodeWork(lease_a, @ptrCast(&w_a), 5, 5, 0);
    try coordinator.enqueueDecodeWork(lease_b, @ptrCast(&w_b), 5, 5, 0);
    try coordinator.enqueueDecodeWork(lease_c, @ptrCast(&w_c), 5, 5, 0);

    coordinator.notePendingKvBlocks(@ptrCast(&w_a), .decode, 1);
    coordinator.notePendingKvBlocks(@ptrCast(&w_b), .decode, 4);
    coordinator.notePendingKvBlocks(@ptrCast(&w_c), .decode, 1);

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);

    // Budget = 3 blocks. Leader uses 1 → 1 remaining for the rest.
    // Adding w_b (4) overflows and is skipped; w_c (1) fits.
    const budget = StepBudget{ .max_items = 8, .max_query_tokens = 1024, .max_kv_blocks = 3 };
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &lease_a,
        @ptrCast(&w_a),
        .decode,
        budget,
        &step,
    ));

    var saw_b = false;
    var saw_c = false;
    for (step.items) |item| {
        if (item.work_ptr == @as(*anyopaque, @ptrCast(&w_b))) saw_b = true;
        if (item.work_ptr == @as(*anyopaque, @ptrCast(&w_c))) saw_c = true;
    }
    try std.testing.expect(!saw_b);
    try std.testing.expect(saw_c);
    try std.testing.expectEqual(@as(u64, 1), coordinator.stats.step_kv_block_skips_total);
}

test "claimStep refuses to take a turn that belongs to another request" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var first = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(first);
    var second = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(second);

    var w1: u8 = 1;
    var w2: u8 = 2;
    coordinator.beginDecode(&first, 4);
    coordinator.beginDecode(&second, 4);
    try coordinator.enqueueDecodeWork(first, @ptrCast(&w1), 5, 5, 0);
    try coordinator.enqueueDecodeWork(second, @ptrCast(&w2), 5, 5, 0);

    // First request wins the round-robin; second must back off.
    coordinator.in_turn = first.request_id;
    coordinator.in_turn_phase = .decode;

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);

    try std.testing.expect(!(try coordinator.claimStep(
        allocator,
        &second,
        @ptrCast(&w2),
        .decode,
        coordinator.defaultStepBudget(),
        &step,
    )));
    try std.testing.expectEqual(@as(usize, 0), step.items.len);
}

test "completeStep updates step stats and removes pending work" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var decode_lease = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(decode_lease);
    var prefill_lease = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 128, .max_tokens = 8 });
    defer coordinator.release(prefill_lease);

    var dec_w: u8 = 1;
    var pre_w: u8 = 2;
    coordinator.beginDecode(&decode_lease, 4);
    coordinator.notePrefillProgress(&prefill_lease, 0, 8);
    try coordinator.enqueueDecodeWork(decode_lease, @ptrCast(&dec_w), 5, 5, 0);
    try coordinator.enqueuePrefillWork(prefill_lease, @ptrCast(&pre_w), 8, 4, 4, 0);

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &decode_lease,
        @ptrCast(&dec_w),
        .decode,
        coordinator.defaultStepBudget(),
        &step,
    ));
    try std.testing.expectEqual(@as(usize, 2), step.items.len);

    coordinator.completeStep(&decode_lease, step.items);

    try std.testing.expectEqual(@as(u64, 1), coordinator.stats.step_batches_total);
    try std.testing.expectEqual(@as(u64, 1), coordinator.stats.step_decode_items_total);
    try std.testing.expectEqual(@as(u64, 1), coordinator.stats.step_prefill_items_total);
    try std.testing.expectEqual(@as(u64, 5), coordinator.stats.step_query_tokens_total);
    try std.testing.expectEqual(@as(u64, 0), coordinator.stats.step_singleton_batches_total);
    try std.testing.expectEqual(@as(?usize, null), coordinator.findPendingDecode(@ptrCast(&dec_w)));
    try std.testing.expectEqual(@as(?usize, null), coordinator.findPendingPrefill(@ptrCast(&pre_w)));
    try std.testing.expectEqual(@as(?RequestId, null), coordinator.in_turn);
}

test "completeStep records singleton steps" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var lease = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(lease);

    var work: u8 = 1;
    coordinator.beginDecode(&lease, 4);
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&work), 5, 5, 0);

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &lease,
        @ptrCast(&work),
        .decode,
        coordinator.defaultStepBudget(),
        &step,
    ));
    coordinator.completeStep(&lease, step.items);

    try std.testing.expectEqual(@as(u64, 1), coordinator.stats.step_singleton_batches_total);
    try std.testing.expectEqual(@as(u64, 1), coordinator.stats.step_batches_total);
    try std.testing.expectEqual(@as(u64, 1), coordinator.stats.step_decode_items_total);
}

// --- Workload simulation tests ---

const SimWorkSlot = struct {
    lease_idx: u32,
    item_idx: u32,
    phase: Phase,
};

test "scheduler drains a multi-lease workload without leaking pending work" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    const n_leases: usize = 4;
    const items_per_lease: usize = 3;
    const total_items: usize = n_leases * items_per_lease;

    var leases: [n_leases]Lease = undefined;
    var leases_acquired: usize = 0;
    defer for (leases[0..leases_acquired]) |l| coordinator.release(l);

    var slots: [n_leases * items_per_lease]SimWorkSlot = undefined;
    for (0..n_leases) |li| {
        leases[li] = try coordinator.acquire(.{
            .requested_units = 1,
            .prompt_bytes = 256,
            .max_tokens = 32,
        });
        leases_acquired += 1;

        for (0..items_per_lease) |wi| {
            const slot = &slots[li * items_per_lease + wi];
            slot.* = .{ .lease_idx = @intCast(li), .item_idx = @intCast(wi), .phase = undefined };
            // Alternate decode/prefill so every step has packing potential.
            if ((li + wi) % 2 == 0) {
                slot.phase = .decode;
                coordinator.beginDecode(&leases[li], 8);
                try coordinator.enqueueDecodeWork(leases[li], @ptrCast(slot), 9, 9, 0);
            } else {
                slot.phase = .prefill;
                coordinator.notePrefillProgress(&leases[li], 0, 8);
                try coordinator.enqueuePrefillWork(leases[li], @ptrCast(slot), 12, 4, 8, 0);
            }
        }
    }

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);

    var seen = [_]bool{false} ** total_items;
    var steps_taken: usize = 0;
    const safety_iter_cap: usize = total_items * 8;
    var safety_iter: usize = 0;

    while (coordinator.pending_decode.items.len + coordinator.pending_prefill.items.len > 0) {
        safety_iter += 1;
        try std.testing.expect(safety_iter <= safety_iter_cap);

        var made_progress = false;
        for (0..n_leases) |li| {
            // Pick the first still-pending work for this lease.
            var leader_ptr: ?*anyopaque = null;
            var leader_phase: Phase = .waiting;
            for (0..items_per_lease) |wi| {
                const slot = &slots[li * items_per_lease + wi];
                const slot_ptr: *anyopaque = @ptrCast(slot);
                if (coordinator.findPendingDecode(slot_ptr) != null) {
                    leader_ptr = slot_ptr;
                    leader_phase = .decode;
                    break;
                }
                if (coordinator.findPendingPrefill(slot_ptr) != null) {
                    leader_ptr = slot_ptr;
                    leader_phase = .prefill;
                    break;
                }
            }
            if (leader_ptr == null) continue;

            const claimed = try coordinator.claimStep(
                allocator,
                &leases[li],
                leader_ptr.?,
                leader_phase,
                coordinator.defaultStepBudget(),
                &step,
            );
            if (!claimed) continue;
            try std.testing.expect(step.items.len >= 1);

            // Each item must map back to a known slot, must not be seen twice,
            // and the leader must always be at index 0.
            try std.testing.expectEqual(leader_ptr.?, step.items[0].work_ptr);
            for (step.items) |item| {
                const idx = (@intFromPtr(item.work_ptr) - @intFromPtr(&slots[0])) / @sizeOf(SimWorkSlot);
                try std.testing.expect(idx < total_items);
                try std.testing.expect(!seen[idx]);
                try std.testing.expectEqual(slots[idx].phase, item.phase);
                seen[idx] = true;
            }

            coordinator.completeStep(&leases[li], step.items);
            steps_taken += 1;
            made_progress = true;
        }
        try std.testing.expect(made_progress);
    }

    try std.testing.expectEqual(@as(usize, 0), coordinator.pending_decode.items.len);
    try std.testing.expectEqual(@as(usize, 0), coordinator.pending_prefill.items.len);
    for (seen) |s| try std.testing.expect(s);

    try std.testing.expectEqual(
        @as(u64, total_items),
        coordinator.stats.step_prefill_items_total + coordinator.stats.step_decode_items_total,
    );
    // Average items-per-step should be > 1 once mixed work is present —
    // otherwise the unified step API isn't actually batching.
    try std.testing.expect(coordinator.stats.step_batches_total > 0);
    try std.testing.expect(coordinator.stats.step_batches_total <= total_items);
    const avg_density_x10 = (@as(u64, total_items) * 10) / coordinator.stats.step_batches_total;
    try std.testing.expect(avg_density_x10 >= 15);
}

test "scheduler honors KV-block budget under contention" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var lease_a = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(lease_a);
    var lease_b = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(lease_b);
    var lease_c = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(lease_c);

    var w_a: u8 = 1;
    var w_b: u8 = 2;
    var w_c: u8 = 3;

    coordinator.beginDecode(&lease_a, 4);
    coordinator.beginDecode(&lease_b, 4);
    coordinator.beginDecode(&lease_c, 4);
    try coordinator.enqueueDecodeWork(lease_a, @ptrCast(&w_a), 5, 5, 0);
    try coordinator.enqueueDecodeWork(lease_b, @ptrCast(&w_b), 5, 5, 0);
    try coordinator.enqueueDecodeWork(lease_c, @ptrCast(&w_c), 5, 5, 0);

    coordinator.notePendingKvBlocks(@ptrCast(&w_a), .decode, 3);
    coordinator.notePendingKvBlocks(@ptrCast(&w_b), .decode, 3);
    coordinator.notePendingKvBlocks(@ptrCast(&w_c), .decode, 3);

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);

    // Pool headroom = 5 blocks. Leader (w_a, 3) admits and consumes 3. Adding
    // w_b (3) would exceed 5; same for w_c. Result: leader-only step, both
    // skips counted.
    const budget = StepBudget{
        .max_items = 8,
        .max_query_tokens = 1024,
        .max_kv_blocks = 5,
    };
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &lease_a,
        @ptrCast(&w_a),
        .decode,
        budget,
        &step,
    ));
    try std.testing.expectEqual(@as(usize, 1), step.items.len);
    try std.testing.expectEqual(@as(u64, 2), coordinator.stats.step_kv_block_skips_total);
}

test "scheduler step batches drain decode-only workload with high density" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    const n_leases: usize = 3;
    const rounds: usize = 5;

    var leases: [n_leases]Lease = undefined;
    for (0..n_leases) |li| {
        leases[li] = try coordinator.acquire(.{
            .requested_units = 1,
            .prompt_bytes = 64,
            .max_tokens = 32,
        });
        coordinator.beginDecode(&leases[li], 4);
    }
    defer for (leases) |l| coordinator.release(l);

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);

    var works_storage: [n_leases * rounds]SimWorkSlot = undefined;

    var round: usize = 0;
    while (round < rounds) : (round += 1) {
        for (0..n_leases) |li| {
            const slot = &works_storage[round * n_leases + li];
            slot.* = .{ .lease_idx = @intCast(li), .item_idx = @intCast(round), .phase = .decode };
            try coordinator.enqueueDecodeWork(leases[li], @ptrCast(slot), 5 + round, 5 + round, 0);
        }

        // Drain this round's pending decodes. Each lease attempts a claim;
        // packing means the first claimer typically sweeps everyone's work.
        var iter: usize = 0;
        const cap: usize = n_leases * 4;
        while (coordinator.pending_decode.items.len > 0) : (iter += 1) {
            try std.testing.expect(iter <= cap);
            var made_progress = false;
            for (0..n_leases) |li| {
                const slot = &works_storage[round * n_leases + li];
                const slot_ptr: *anyopaque = @ptrCast(slot);
                if (coordinator.findPendingDecode(slot_ptr) == null) continue;
                if (try coordinator.claimStep(
                    allocator,
                    &leases[li],
                    slot_ptr,
                    .decode,
                    coordinator.defaultStepBudget(),
                    &step,
                )) {
                    coordinator.completeStep(&leases[li], step.items);
                    made_progress = true;
                }
            }
            try std.testing.expect(made_progress);
        }
    }

    // Total decode items processed should equal what we enqueued, and
    // step packing should keep batches sparse — at most one step per round
    // when packing works (usually exactly one per round under round-robin
    // turn rotation).
    try std.testing.expectEqual(@as(u64, n_leases * rounds), coordinator.stats.step_decode_items_total);
    try std.testing.expect(coordinator.stats.step_batches_total <= rounds * 2);
}
