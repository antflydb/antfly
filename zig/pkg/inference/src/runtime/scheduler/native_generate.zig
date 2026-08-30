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
const platform = @import("antfly_platform");

fn spinLock(mutex: *std.atomic.Mutex) void {
    platform.sync.lockYielding(mutex);
}

pub const RequestId = u64;

pub const Phase = enum {
    waiting,
    prefill,
    decode,
};

/// Fairness state captured when prefill memory is admitted. A plan is never
/// resized after this snapshot: later arrivals wait for scheduler turns rather
/// than silently changing the memory geometry underneath the running request.
pub const PrefillFairness = enum(u8) {
    sole_idle,
    shared,
};

/// Weight-route intent carried with every admitted chunk. Execution currently
/// treats this as telemetry; keeping the intent in the immutable plan lets a
/// qualified backend route a pathological tiny tail to its quantized matvec
/// path without reconstructing chunk geometry at dispatch time.
pub const PrefillWeightRouteHint = enum(u8) {
    default,
    prefer_quantized,
};

pub const PrefillChunk = struct {
    rows: usize,
    weight_route_hint: PrefillWeightRouteHint = .default,
};

/// Production retains the measured fixed-ceiling schedule. Balanced geometry
/// remains an explicit candidate so it can be benchmarked and promoted without
/// changing the admission/execution contract.
pub const PrefillChunkSelection = enum(u8) {
    fixed_ceiling,
    balanced_candidate,
};

pub const PrefillChunkPlan = struct {
    pub const fingerprint_version: u64 = 1;
    pub const tiny_tail_rows: usize = 31;

    chunks: []const PrefillChunk = &.{},
    prompt_tokens: usize = 0,
    max_chunk_rows: usize = 0,
    max_scratch_bytes: usize = 0,
    fairness: PrefillFairness = .shared,
    selection: PrefillChunkSelection = .fixed_ceiling,
    /// A prefill plan must not rely on synchronizing the device merely to
    /// reclaim deferred temporaries. This remains zero until the runtime has a
    /// separately qualified non-zero envelope.
    forced_drain_budget_bytes: usize = 0,
    fingerprint: u64 = 0,

    pub fn init(
        allocator: std.mem.Allocator,
        prompt_tokens: usize,
        max_chunk_rows: usize,
        max_scratch_bytes: usize,
        fairness: PrefillFairness,
        selection: PrefillChunkSelection,
    ) !PrefillChunkPlan {
        if (prompt_tokens == 0 or max_chunk_rows == 0) return error.InvalidPrefillChunkPlan;
        const chunk_count = std.math.divCeil(usize, prompt_tokens, max_chunk_rows) catch
            return error.InvalidPrefillChunkPlan;
        const chunks = try allocator.alloc(PrefillChunk, chunk_count);
        errdefer allocator.free(chunks);

        switch (selection) {
            .fixed_ceiling => {
                var remaining = prompt_tokens;
                for (chunks, 0..) |*chunk, idx| {
                    const rows = @min(remaining, max_chunk_rows);
                    chunk.* = .{
                        .rows = rows,
                        .weight_route_hint = if (idx + 1 == chunks.len and
                            chunks.len > 1 and rows <= tiny_tail_rows)
                            .prefer_quantized
                        else
                            .default,
                    };
                    remaining -= rows;
                }
                std.debug.assert(remaining == 0);
            },
            .balanced_candidate => {
                const base_rows = prompt_tokens / chunk_count;
                const wider_chunks = prompt_tokens % chunk_count;
                for (chunks, 0..) |*chunk, idx| {
                    chunk.* = .{ .rows = base_rows + @intFromBool(idx < wider_chunks) };
                }
            },
        }

        var plan = PrefillChunkPlan{
            .chunks = chunks,
            .prompt_tokens = prompt_tokens,
            .max_chunk_rows = max_chunk_rows,
            .max_scratch_bytes = max_scratch_bytes,
            .fairness = fairness,
            .selection = selection,
        };
        plan.fingerprint = plan.computeFingerprint();
        try plan.validate();
        return plan;
    }

    pub fn deinit(self: *PrefillChunkPlan, allocator: std.mem.Allocator) void {
        if (self.chunks.len != 0) allocator.free(self.chunks);
        self.* = .{};
    }

    pub fn isAdmitted(self: PrefillChunkPlan) bool {
        return self.chunks.len != 0 and self.fingerprint != 0;
    }

    pub fn validate(self: PrefillChunkPlan) !void {
        if (!self.isAdmitted() or self.prompt_tokens == 0 or self.max_chunk_rows == 0)
            return error.InvalidPrefillChunkPlan;
        if (self.forced_drain_budget_bytes != 0) return error.InvalidPrefillChunkPlan;
        var total_rows: usize = 0;
        for (self.chunks) |chunk| {
            if (chunk.rows == 0 or chunk.rows > self.max_chunk_rows)
                return error.InvalidPrefillChunkPlan;
            total_rows = std.math.add(usize, total_rows, chunk.rows) catch
                return error.InvalidPrefillChunkPlan;
        }
        if (total_rows != self.prompt_tokens or self.computeFingerprint() != self.fingerprint)
            return error.InvalidPrefillChunkPlan;
    }

    pub fn tail(self: PrefillChunkPlan) PrefillChunk {
        return self.chunks[self.chunks.len - 1];
    }

    fn hashU64(hash: *u64, value: u64) void {
        var remaining = value;
        for (0..@sizeOf(u64)) |_| {
            hash.* = (hash.* ^ @as(u8, @truncate(remaining))) *% 1099511628211;
            remaining >>= 8;
        }
    }

    fn computeFingerprint(self: PrefillChunkPlan) u64 {
        var hash: u64 = 14695981039346656037;
        hashU64(&hash, fingerprint_version);
        hashU64(&hash, self.prompt_tokens);
        hashU64(&hash, self.max_chunk_rows);
        hashU64(&hash, self.max_scratch_bytes);
        hashU64(&hash, @backingInt(self.fairness));
        hashU64(&hash, @backingInt(self.selection));
        hashU64(&hash, self.forced_drain_budget_bytes);
        hashU64(&hash, self.chunks.len);
        for (self.chunks) |chunk| {
            hashU64(&hash, chunk.rows);
            hashU64(&hash, @backingInt(chunk.weight_route_hint));
        }
        return hash;
    }
};

pub const Admission = struct {
    requested_units: usize,
    prompt_bytes: usize,
    /// Exact encoded prompt length when already available. Zero preserves the
    /// legacy byte-based estimate for callers that have not tokenized yet.
    prompt_tokens: usize = 0,
    /// Optional request/model-specific ceiling. Zero leaves the scheduler
    /// policy as the only idle-prefill limit.
    prefill_chunk_limit: usize = 0,
    max_tokens: i32,
};

pub const Lease = struct {
    request_id: RequestId,
    reserved_units: usize,
    prompt_bytes: usize,
    max_tokens: i32,
    prefill_chunk_size: usize,
    active_requests_snapshot: usize,
    /// Borrowed immutable view. The coordinator owns the chunk storage until
    /// `release`; standalone callers may leave this empty and use the legacy
    /// scalar fallback.
    prefill_chunk_plan: PrefillChunkPlan = .{},
};

pub const Policy = struct {
    /// Default item cap for a unified step (prefill chunks + decode tokens packed
    /// into one fused forward pass).
    max_step_items: usize = 16,
    /// Default query-token cap for a unified step. Decode items contribute 1
    /// token; prefill items contribute their `query_sequence_len`.
    max_step_query_tokens: usize = 512,
    /// Backends opt in after heterogeneous position/KV batching passes their
    /// response-equivalence gate.
    allow_mixed_sequence_lengths: bool = false,
    /// Optional rollout cap on the active request set. Above this count the
    /// coordinator preserves fairness but emits singleton forward steps.
    max_active_requests_for_batching: ?usize = null,
    /// Maximum time a singleton decode may wait for an already-admitted peer.
    /// The wait is skipped when this is the only active request.
    max_decode_wait_us: u32 = 1_000,
    /// Largest prefill issued for a sole request on an otherwise idle model.
    /// Admission budgets scratch at this size before generation starts. The
    /// ceiling remains configurable up to 2048. Model-specific performance
    /// limits belong on the admission so unrelated backends retain this cap.
    max_idle_prefill_chunk_size: usize = 2048,

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
    decode_coalesce_waits_total: u64 = 0,
    decode_coalesce_wait_us_total: u64 = 0,
    step_batch_size_2_total: u64 = 0,
    step_batch_size_3_4_total: u64 = 0,
    step_batch_size_5_8_total: u64 = 0,
    step_batch_size_9_16_total: u64 = 0,
};

const Entry = struct {
    id: RequestId,
    requested_units: usize,
    prompt_bytes: usize,
    prompt_tokens: usize,
    prefill_chunk_limit: usize,
    prefill_chunk_plan: PrefillChunkPlan = .{},
    phase: Phase = .waiting,
    prompt_tokens_processed: usize = 0,
    total_prompt_tokens: usize = 0,
    generated_tokens: usize = 0,
    /// Total sequence length of the next decode work this request is expected
    /// to enqueue. This bridges the gap after a scheduled forward completes
    /// and before its owner samples the result and enqueues the next step.
    /// A null value means decode work is already pending or the next geometry
    /// is not known yet.
    next_decode_work_total_sequence_len: ?usize = null,
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

fn decodeWorkCompatible(
    pending: PendingDecode,
    total_sequence_len: usize,
    kv_sequence_len: usize,
    kv_position_offset: usize,
) bool {
    return pending.total_sequence_len == total_sequence_len and
        pending.kv_sequence_len == kv_sequence_len and
        pending.kv_position_offset == kv_position_offset;
}

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

pub const PendingWorkOptions = struct {
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
    mutex: std.atomic.Mutex = .unlocked,
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

    fn lockCoordinator(self: *const NativeGenerateCoordinator) void {
        spinLock(&@constCast(self).mutex);
    }

    fn unlockCoordinator(self: *const NativeGenerateCoordinator) void {
        @constCast(self).mutex.unlock();
    }

    pub fn setPolicy(self: *NativeGenerateCoordinator, policy: Policy) void {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        self.policy = policy;
    }

    pub fn deinit(self: *NativeGenerateCoordinator) void {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        for (self.entries.items) |*entry| {
            entry.prefill_chunk_plan.deinit(self.allocator);
        }
        self.entries.deinit(self.allocator);
        self.pending_prefill.deinit(self.allocator);
        self.pending_decode.deinit(self.allocator);
    }

    pub fn acquire(self: *NativeGenerateCoordinator, admission: Admission) !Lease {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        const request_id = self.next_request_id;
        self.next_request_id += 1;
        const reserved_units = @max(admission.requested_units, 1);
        try self.entries.append(self.allocator, .{
            .id = request_id,
            .requested_units = reserved_units,
            .prompt_bytes = admission.prompt_bytes,
            .prompt_tokens = admission.prompt_tokens,
            .prefill_chunk_limit = admission.prefill_chunk_limit,
            .phase = .waiting,
        });
        self.active_units += reserved_units;

        return .{
            .request_id = request_id,
            .reserved_units = reserved_units,
            .prompt_bytes = admission.prompt_bytes,
            .max_tokens = admission.max_tokens,
            .prefill_chunk_size = self.recommendPrefillChunkForUnlocked(request_id),
            .active_requests_snapshot = self.entries.items.len,
        };
    }

    pub fn release(self: *NativeGenerateCoordinator, lease: Lease) void {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        const idx = self.indexOf(lease.request_id) orelse return;
        const entry = &self.entries.items[idx];
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
        entry.prefill_chunk_plan.deinit(self.allocator);
        _ = self.entries.swapRemove(idx);
    }

    /// Finalize the request's immutable prefill geometry after memory
    /// reservation. The coordinator owns the chunk storage through `release`;
    /// the lease and generation config receive borrowed views only.
    pub fn admitPrefillChunkPlan(
        self: *NativeGenerateCoordinator,
        lease: *Lease,
        prompt_tokens: usize,
        max_chunk_rows: usize,
        max_scratch_bytes: usize,
        selection: PrefillChunkSelection,
    ) !void {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        const idx = self.indexOf(lease.request_id) orelse return error.InvalidPrefillChunkPlan;
        const entry = &self.entries.items[idx];
        if (entry.prefill_chunk_plan.isAdmitted() or lease.prefill_chunk_plan.isAdmitted())
            return error.PrefillChunkPlanAlreadyAdmitted;
        if (entry.prompt_tokens != 0 and entry.prompt_tokens != prompt_tokens)
            return error.InvalidPrefillChunkPlan;

        const state = self.snapshotUnlocked();
        const fairness: PrefillFairness = if (self.entries.items.len == 1 and state.decode_requests == 0)
            .sole_idle
        else
            .shared;
        var plan = try PrefillChunkPlan.init(
            self.allocator,
            prompt_tokens,
            max_chunk_rows,
            max_scratch_bytes,
            fairness,
            selection,
        );
        errdefer plan.deinit(self.allocator);
        entry.prefill_chunk_plan = plan;
        lease.prefill_chunk_plan = plan;
        lease.prefill_chunk_size = max_chunk_rows;
    }

    pub fn enqueuePrefillWork(
        self: *NativeGenerateCoordinator,
        lease: Lease,
        work_ptr: *anyopaque,
        total_sequence_len: usize,
        query_sequence_len: usize,
        kv_sequence_len: usize,
        kv_position_offset: usize,
        options: PendingWorkOptions,
    ) !void {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        if (self.findPendingPrefill(work_ptr) != null) return;
        try self.pending_prefill.append(self.allocator, .{
            .request_id = lease.request_id,
            .work_ptr = work_ptr,
            .total_sequence_len = total_sequence_len,
            .query_sequence_len = query_sequence_len,
            .kv_sequence_len = kv_sequence_len,
            .kv_position_offset = kv_position_offset,
            .kv_blocks_estimate = options.kv_blocks_estimate,
            .exclusive_step = options.exclusive_step,
        });
    }

    pub fn cancelPrefillWork(self: *NativeGenerateCoordinator, work_ptr: *anyopaque) void {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        self.removePendingPrefill(work_ptr);
    }

    pub fn enqueueDecodeWork(
        self: *NativeGenerateCoordinator,
        lease: Lease,
        work_ptr: *anyopaque,
        total_sequence_len: usize,
        kv_sequence_len: usize,
        kv_position_offset: usize,
        options: PendingWorkOptions,
    ) !void {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        if (self.findPendingDecode(work_ptr) != null) return;
        try self.pending_decode.append(self.allocator, .{
            .request_id = lease.request_id,
            .work_ptr = work_ptr,
            .total_sequence_len = total_sequence_len,
            .kv_sequence_len = kv_sequence_len,
            .kv_position_offset = kv_position_offset,
            .kv_blocks_estimate = options.kv_blocks_estimate,
            .exclusive_step = options.exclusive_step,
        });
        if (self.indexOf(lease.request_id)) |entry_idx| {
            self.entries.items[entry_idx].next_decode_work_total_sequence_len = null;
        }
    }

    pub fn cancelDecodeWork(self: *NativeGenerateCoordinator, work_ptr: *anyopaque) void {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        self.removePendingDecode(work_ptr);
    }

    /// Cancel work only when no claimed step can still reference its pointer.
    /// A claimed step keeps `in_turn` set until it removes its pending items;
    /// publication happens immediately afterward, so callers must keep waiting
    /// when this returns false.
    pub fn cancelPendingWorkIfIdle(
        self: *NativeGenerateCoordinator,
        work_ptr: *anyopaque,
        phase: Phase,
    ) bool {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        if (self.in_turn != null) return false;
        return switch (phase) {
            .decode => if (self.findPendingDecode(work_ptr)) |idx| blk: {
                _ = self.pending_decode.swapRemove(idx);
                break :blk true;
            } else false,
            .prefill => if (self.findPendingPrefill(work_ptr)) |idx| blk: {
                _ = self.pending_prefill.swapRemove(idx);
                break :blk true;
            } else false,
            .waiting => false,
        };
    }

    /// Default per-step admission budget derived from the configured policy.
    pub fn defaultStepBudget(self: *const NativeGenerateCoordinator) StepBudget {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        var budget = self.policy.defaultStepBudget();
        if (self.policy.max_active_requests_for_batching) |limit| {
            if (self.entries.items.len > limit) budget.max_items = 1;
        }
        return budget;
    }

    /// Return the configured coalescing delay only when waiting can form a
    /// useful decode batch. A sole request never pays the latency tax.
    pub fn decodeCoalesceDelayUs(self: *const NativeGenerateCoordinator, lease: Lease) u32 {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        if (self.policy.max_decode_wait_us == 0) return 0;
        if (self.policy.max_step_items <= 1) return 0;
        if (self.policy.max_active_requests_for_batching) |limit| {
            if (self.entries.items.len > limit) return 0;
        }
        if (self.in_turn != null or self.pending_decode.items.len != 1) return 0;
        const leader = self.pending_decode.items[0];
        if (leader.request_id != lease.request_id) return 0;
        if (!self.hasCompatibleDecodePeerUnlocked(
            lease,
            leader.total_sequence_len,
            leader.kv_sequence_len,
            leader.kv_position_offset,
        )) return 0;
        return self.policy.max_decode_wait_us;
    }

    pub fn shouldBatchCurrentRequests(self: *const NativeGenerateCoordinator) bool {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        if (self.entries.items.len <= 1 or self.policy.max_step_items <= 1) return false;
        if (self.policy.max_active_requests_for_batching) |limit| {
            if (self.entries.items.len > limit) return false;
        }
        return true;
    }

    /// Whether concurrent requests must rendezvous through scheduler turns.
    /// This remains true above the batching rollout cap: the step budget then
    /// becomes singleton, but direct lock racing would lose round-robin fairness.
    pub fn shouldCoordinateCurrentRequests(self: *const NativeGenerateCoordinator) bool {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        return self.entries.items.len > 1;
    }

    pub fn shouldBatchDecode(
        self: *const NativeGenerateCoordinator,
        lease: Lease,
        total_sequence_len: usize,
        kv_sequence_len: usize,
        kv_position_offset: usize,
    ) bool {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        if (self.policy.max_step_items <= 1) return false;
        if (self.policy.max_active_requests_for_batching) |limit| {
            if (self.entries.items.len > limit) return false;
        }
        return self.hasCompatibleDecodePeerUnlocked(
            lease,
            total_sequence_len,
            kv_sequence_len,
            kv_position_offset,
        );
    }

    fn hasCompatibleDecodePeerUnlocked(
        self: *const NativeGenerateCoordinator,
        lease: Lease,
        total_sequence_len: usize,
        kv_sequence_len: usize,
        kv_position_offset: usize,
    ) bool {
        const current_idx = self.indexOf(lease.request_id) orelse return false;
        if (self.entries.items[current_idx].phase != .decode) return false;

        for (self.entries.items) |entry| {
            if (entry.id == lease.request_id or entry.phase != .decode) continue;
            if (self.findPendingDecodeByRequest(entry.id)) |pending_idx| {
                if (decodeWorkCompatible(
                    self.pending_decode.items[pending_idx],
                    total_sequence_len,
                    kv_sequence_len,
                    kv_position_offset,
                )) return true;
                continue;
            }
            // Do not infer readiness from total_prompt_tokens +
            // generated_tokens alone. That value stays at the just-executed
            // input position while the owner is sampling its result, so it
            // can describe work that will never be enqueued again. The
            // explicit next-work position is advanced when a step completes
            // and cleared when that work is actually enqueued.
            if (entry.next_decode_work_total_sequence_len == total_sequence_len) return true;
        }
        return false;
    }

    pub fn noteDecodeCoalesceWait(self: *NativeGenerateCoordinator, delay_us: u32) void {
        if (delay_us == 0) return;
        self.lockCoordinator();
        defer self.unlockCoordinator();
        self.stats.decode_coalesce_waits_total += 1;
        self.stats.decode_coalesce_wait_us_total += delay_us;
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
        self.lockCoordinator();
        defer self.unlockCoordinator();
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
        self: *NativeGenerateCoordinator,
        work_ptr: *anyopaque,
        phase: Phase,
    ) ?usize {
        self.lockCoordinator();
        defer self.unlockCoordinator();
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
        self.lockCoordinator();
        defer self.unlockCoordinator();
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
        self: *NativeGenerateCoordinator,
        work_ptr: *anyopaque,
        phase: Phase,
    ) ?bool {
        self.lockCoordinator();
        defer self.unlockCoordinator();
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
        self.lockCoordinator();
        defer self.unlockCoordinator();
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
        var leader_total_sequence_len: usize = 0;
        var leader_kv_sequence_len: usize = 0;
        var leader_kv_position_offset: usize = 0;
        var decode_geometry_initialized = leader_phase == .decode;

        switch (leader_phase) {
            .decode => {
                const leader_idx = self.findPendingDecode(leader_work_ptr) orelse return false;
                const pending = self.pending_decode.items[leader_idx];
                if (pending.request_id != lease.request_id) return false;
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
                leader_total_sequence_len = pending.total_sequence_len;
                leader_kv_sequence_len = pending.kv_sequence_len;
                leader_kv_position_offset = pending.kv_position_offset;
            },
            .prefill => {
                const leader_idx = self.findPendingPrefill(leader_work_ptr) orelse return false;
                const pending = self.pending_prefill.items[leader_idx];
                if (pending.request_id != lease.request_id) return false;
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
            if (!self.policy.allow_mixed_sequence_lengths and
                decode_geometry_initialized and
                !decodeWorkCompatible(
                    pending,
                    leader_total_sequence_len,
                    leader_kv_sequence_len,
                    leader_kv_position_offset,
                ))
            {
                continue;
            }
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
            if (!self.policy.allow_mixed_sequence_lengths and !decode_geometry_initialized) {
                leader_total_sequence_len = pending.total_sequence_len;
                leader_kv_sequence_len = pending.kv_sequence_len;
                leader_kv_position_offset = pending.kv_position_offset;
                decode_geometry_initialized = true;
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
    /// stats, and releases the turn. Fairness follows the claimed leader phase;
    /// peer work packed into the same forward does not own the turn rotation.
    pub fn completeStep(
        self: *NativeGenerateCoordinator,
        lease: *Lease,
        items: []const StepItem,
    ) void {
        self.lockCoordinator();
        defer self.unlockCoordinator();
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
                    if (self.findPendingDecode(item.work_ptr)) |pending_idx| {
                        const request_id = self.pending_decode.items[pending_idx].request_id;
                        if (self.indexOf(request_id)) |entry_idx| {
                            self.entries.items[entry_idx].next_decode_work_total_sequence_len =
                                std.math.add(usize, item.total_sequence_len, 1) catch null;
                        }
                    }
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
        switch (items.len) {
            0, 1 => {},
            2 => self.stats.step_batch_size_2_total += 1,
            3...4 => self.stats.step_batch_size_3_4_total += 1,
            5...8 => self.stats.step_batch_size_5_8_total += 1,
            else => self.stats.step_batch_size_9_16_total += 1,
        }
        if (prefill_count + decode_count <= 1) {
            self.stats.step_singleton_batches_total += 1;
        }
        const phase = self.in_turn_phase orelse if (decode_count > 0) Phase.decode else Phase.prefill;
        self.finishTurnUnlocked(lease, phase);
    }

    pub fn awaitTurn(self: *NativeGenerateCoordinator, lease: *Lease, phase: Phase, io: std.Io) !void {
        while (!self.tryAcquireTurn(lease, phase)) {
            self.noteTurnYield();
            try io.sleep(std.Io.Duration.fromMilliseconds(0), .awake);
        }
    }

    fn noteTurnYield(self: *NativeGenerateCoordinator) void {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        self.stats.turn_yields_total += 1;
    }

    pub fn tryAcquireTurn(self: *NativeGenerateCoordinator, lease: *Lease, phase: Phase) bool {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        return self.tryAcquireTurnUnlocked(lease, phase);
    }

    fn tryAcquireTurnUnlocked(self: *NativeGenerateCoordinator, lease: *Lease, phase: Phase) bool {
        if (self.in_turn) |owner| {
            return owner == lease.request_id and self.in_turn_phase == phase;
        }

        if (self.indexOf(lease.request_id) == null) return false;
        if (self.pickNextTurn()) |selected| {
            if (selected != lease.request_id) return false;
        }

        self.in_turn = lease.request_id;
        self.in_turn_phase = phase;
        lease.prefill_chunk_size = self.recommendPrefillChunkForUnlocked(lease.request_id);
        return true;
    }

    pub fn finishTurn(self: *NativeGenerateCoordinator, lease: *Lease, phase: Phase) void {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        self.finishTurnUnlocked(lease, phase);
    }

    fn finishTurnUnlocked(self: *NativeGenerateCoordinator, lease: *Lease, phase: Phase) void {
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
        self.lockCoordinator();
        defer self.unlockCoordinator();
        const idx = self.indexOf(lease.request_id) orelse return;
        var entry = &self.entries.items[idx];
        entry.phase = .prefill;
        entry.prompt_tokens_processed = processed_tokens;
        entry.total_prompt_tokens = total_prompt_tokens;
        if (!lease.prefill_chunk_plan.isAdmitted()) {
            lease.prefill_chunk_size = self.recommendPrefillChunkForUnlocked(lease.request_id);
        }
    }

    pub fn beginDecode(self: *NativeGenerateCoordinator, lease: *Lease, total_prompt_tokens: usize) void {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        const idx = self.indexOf(lease.request_id) orelse return;
        var entry = &self.entries.items[idx];
        entry.phase = .decode;
        entry.prompt_tokens_processed = total_prompt_tokens;
        entry.total_prompt_tokens = total_prompt_tokens;
        entry.next_decode_work_total_sequence_len = null;
        if (!lease.prefill_chunk_plan.isAdmitted()) {
            lease.prefill_chunk_size = self.recommendPrefillChunkForUnlocked(lease.request_id);
        }
    }

    pub fn noteDecodeProgress(self: *NativeGenerateCoordinator, lease: *Lease, generated_tokens: usize) void {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        const idx = self.indexOf(lease.request_id) orelse return;
        var entry = &self.entries.items[idx];
        entry.phase = .decode;
        entry.generated_tokens = generated_tokens;
        entry.next_decode_work_total_sequence_len = std.math.add(
            usize,
            entry.total_prompt_tokens,
            generated_tokens,
        ) catch null;
    }

    pub fn snapshot(self: *const NativeGenerateCoordinator) Snapshot {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        return self.snapshotUnlocked();
    }

    fn snapshotUnlocked(self: *const NativeGenerateCoordinator) Snapshot {
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
        self.lockCoordinator();
        defer self.unlockCoordinator();
        return self.stats;
    }

    pub fn recommendPrefillChunkFor(self: *const NativeGenerateCoordinator, request_id: RequestId) usize {
        self.lockCoordinator();
        defer self.unlockCoordinator();
        return self.recommendPrefillChunkForUnlocked(request_id);
    }

    fn recommendPrefillChunkForUnlocked(self: *const NativeGenerateCoordinator, request_id: RequestId) usize {
        const idx = self.indexOf(request_id) orelse return self.base_prefill_chunk_size;
        const entry = self.entries.items[idx];
        const state = self.snapshotUnlocked();

        const prompt_token_estimate = if (entry.prompt_tokens > 0)
            entry.prompt_tokens
        else
            @max(@max(entry.prompt_bytes, 1) / 4, 1);

        var target = self.base_prefill_chunk_size;
        if (self.entries.items.len == 1 and state.decode_requests == 0) {
            // Sole request on an idle scheduler: no fairness pressure, so run
            // the prompt in as few passes as possible. Chunked prefill costs
            // several full per-layer passes and starves large-GEMM
            // efficiency. Still capped: activation scratch scales with chunk
            // rows, so an unbounded chunk turns very long prompts into a
            // single allocation spike that the memory budget was never sized
            // for.
            const configured_limit = @max(self.policy.max_idle_prefill_chunk_size, 1);
            const request_limit = if (entry.prefill_chunk_limit > 0)
                @min(configured_limit, entry.prefill_chunk_limit)
            else
                configured_limit;
            return @min(prompt_token_estimate, @max(request_limit, 1));
        } else if (state.decode_requests > 0) {
            target = self.min_prefill_chunk_size;
        } else if (state.prefill_requests >= 3 or state.active_units >= 12) {
            target = self.min_prefill_chunk_size;
        } else if (state.prefill_requests >= 2 or state.active_units >= 8) {
            target = 64;
        } else if (state.waiting_requests > 1 or state.active_units >= 4) {
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
        return pickPendingRequestRoundRobin(self.pending_decode.items, last_granted);
    }

    fn pickPendingPrefillRoundRobin(self: *const NativeGenerateCoordinator, last_granted: ?RequestId) ?RequestId {
        return pickPendingRequestRoundRobin(self.pending_prefill.items, last_granted);
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

/// Request IDs are allocated monotonically and never reused. Like `acquire`'s
/// increment, this ordering assumes a coordinator does not survive u64 wrap.
/// Scanning by ID gives stable acquisition-order round robin without rescanning
/// the active-entry array under the lock.
fn pickPendingRequestRoundRobin(pending: anytype, last_granted: ?RequestId) ?RequestId {
    var first: ?RequestId = null;
    var next: ?RequestId = null;
    for (pending) |item| {
        if (first == null or item.request_id < first.?) first = item.request_id;
        if (last_granted) |last_id| {
            if (item.request_id > last_id and (next == null or item.request_id < next.?)) {
                next = item.request_id;
            }
        }
    }
    return next orelse first;
}

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
        stats.decode_coalesce_waits_total += st.decode_coalesce_waits_total;
        stats.decode_coalesce_wait_us_total += st.decode_coalesce_wait_us_total;
        stats.step_batch_size_2_total += st.step_batch_size_2_total;
        stats.step_batch_size_3_4_total += st.step_batch_size_3_4_total;
        stats.step_batch_size_5_8_total += st.step_batch_size_5_8_total;
        stats.step_batch_size_9_16_total += st.step_batch_size_9_16_total;
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

test "native generate coordinator uses the exact sole prompt size" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 96,
        .prompt_tokens = 24,
        .max_tokens = 16,
    });
    defer coordinator.release(lease);

    coordinator.notePrefillProgress(&lease, 0, 24);
    try std.testing.expectEqual(@as(usize, 24), lease.prefill_chunk_size);
}

test "native generate coordinator does not cap short-output long prompts at 128" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();
    coordinator.setPolicy(.{ .max_idle_prefill_chunk_size = 2048 });

    const lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 8_000,
        .prompt_tokens = 2_003,
        .max_tokens = 64,
    });
    defer coordinator.release(lease);

    try std.testing.expectEqual(@as(usize, 2_003), lease.prefill_chunk_size);
}

test "native generate coordinator honors the configured idle ceiling" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();
    coordinator.setPolicy(.{ .max_idle_prefill_chunk_size = 64 });

    const lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 8_000,
        .prompt_tokens = 2_003,
        .max_tokens = 64,
    });
    defer coordinator.release(lease);

    try std.testing.expectEqual(@as(usize, 64), lease.prefill_chunk_size);
}

test "native generate coordinator honors a per-request prefill ceiling" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    const lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 8_000,
        .prompt_tokens = 2_003,
        .prefill_chunk_limit = 128,
        .max_tokens = 64,
    });
    defer coordinator.release(lease);

    try std.testing.expectEqual(@as(usize, 128), lease.prefill_chunk_size);
}

test "prefill chunk plan retains measured fixed schedule and marks tiny tail" {
    const allocator = std.testing.allocator;
    var plan = try PrefillChunkPlan.init(
        allocator,
        2051,
        512,
        900 * 1024 * 1024,
        .sole_idle,
        .fixed_ceiling,
    );
    defer plan.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 5), plan.chunks.len);
    try std.testing.expectEqualSlices(
        usize,
        &.{ 512, 512, 512, 512, 3 },
        &.{
            plan.chunks[0].rows,
            plan.chunks[1].rows,
            plan.chunks[2].rows,
            plan.chunks[3].rows,
            plan.chunks[4].rows,
        },
    );
    try std.testing.expectEqual(PrefillWeightRouteHint.prefer_quantized, plan.tail().weight_route_hint);
    try std.testing.expectEqual(@as(usize, 0), plan.forced_drain_budget_bytes);
    try std.testing.expect(plan.fingerprint != 0);
    try plan.validate();
}

test "balanced prefill candidate avoids boundary micro tails" {
    const allocator = std.testing.allocator;
    const boundaries = [_]usize{ 511, 512, 513, 1023, 1024, 1025, 2047, 2048, 2049, 2051 };
    for (boundaries) |prompt_tokens| {
        var plan = try PrefillChunkPlan.init(
            allocator,
            prompt_tokens,
            512,
            1,
            .sole_idle,
            .balanced_candidate,
        );
        defer plan.deinit(allocator);
        var total: usize = 0;
        var min_rows: usize = std.math.maxInt(usize);
        var max_rows: usize = 0;
        for (plan.chunks) |chunk| {
            total += chunk.rows;
            min_rows = @min(min_rows, chunk.rows);
            max_rows = @max(max_rows, chunk.rows);
            try std.testing.expectEqual(PrefillWeightRouteHint.default, chunk.weight_route_hint);
        }
        try std.testing.expectEqual(prompt_tokens, total);
        try std.testing.expect(max_rows - min_rows <= 1);
        try plan.validate();
    }

    var canonical = try PrefillChunkPlan.init(
        allocator,
        2051,
        512,
        1,
        .sole_idle,
        .balanced_candidate,
    );
    defer canonical.deinit(allocator);
    try std.testing.expectEqualSlices(
        usize,
        &.{ 411, 410, 410, 410, 410 },
        &.{
            canonical.chunks[0].rows,
            canonical.chunks[1].rows,
            canonical.chunks[2].rows,
            canonical.chunks[3].rows,
            canonical.chunks[4].rows,
        },
    );
}

test "admitted prefill plan remains immutable when a peer arrives" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();
    coordinator.setPolicy(.{ .max_idle_prefill_chunk_size = 512 });

    var sole = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 8_000,
        .prompt_tokens = 2051,
        .max_tokens = 300,
    });
    defer coordinator.release(sole);
    try coordinator.admitPrefillChunkPlan(&sole, 2051, 512, 900 * 1024 * 1024, .fixed_ceiling);
    const admitted_fingerprint = sole.prefill_chunk_plan.fingerprint;

    const peer = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 64,
        .prompt_tokens = 16,
        .max_tokens = 16,
    });
    defer coordinator.release(peer);
    coordinator.notePrefillProgress(&sole, 512, 2051);

    try std.testing.expectEqual(@as(usize, 512), sole.prefill_chunk_size);
    try std.testing.expectEqual(admitted_fingerprint, sole.prefill_chunk_plan.fingerprint);
    try std.testing.expectEqual(@as(usize, 3), sole.prefill_chunk_plan.tail().rows);
}

test "native generate coordinator shrinks a sole long request after a peer arrives" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var sole = try coordinator.acquire(.{
        .requested_units = 18,
        .prompt_bytes = 15_434,
        .max_tokens = 16,
    });
    defer coordinator.release(sole);
    try std.testing.expectEqual(@as(usize, 2048), sole.prefill_chunk_size);

    const peer = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 64,
        .max_tokens = 16,
    });
    defer coordinator.release(peer);

    coordinator.notePrefillProgress(&sole, 0, 2048);
    try std.testing.expectEqual(@as(usize, 32), sole.prefill_chunk_size);
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
    try coordinator.enqueueDecodeWork(first, @ptrFromInt(1), 512, 512, 0, .{});
    try std.testing.expect(coordinator.tryAcquireTurn(&first, .decode));
    try std.testing.expect(!coordinator.tryAcquireTurn(&second, .prefill));
}

test "direct turns without pending work still hold ownership" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var first = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(first);
    var second = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(second);
    coordinator.beginDecode(&first, 4);
    coordinator.beginDecode(&second, 4);

    try std.testing.expect(coordinator.tryAcquireTurn(&first, .decode));
    try std.testing.expectEqual(first.request_id, coordinator.in_turn.?);
    try std.testing.expect(!coordinator.tryAcquireTurn(&second, .decode));
    coordinator.finishTurn(&first, .decode);

    try std.testing.expect(coordinator.tryAcquireTurn(&second, .decode));
    try std.testing.expectEqual(second.request_id, coordinator.in_turn.?);
    coordinator.finishTurn(&second, .decode);
}

test "pending round robin preserves request order after release swap-removes an entry" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var first = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(first);
    var second = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    var third = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(third);
    var fourth = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(fourth);

    var work: [4]u8 = undefined;
    const leases = [_]*Lease{ &first, &second, &third, &fourth };
    for (leases, 0..) |lease, idx| {
        coordinator.beginDecode(lease, 4);
        try coordinator.enqueueDecodeWork(lease.*, @ptrCast(&work[idx]), 5, 5, 0, .{});
    }

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &first,
        @ptrCast(&work[0]),
        .decode,
        .{ .max_items = 1, .max_query_tokens = 1 },
        &step,
    ));
    coordinator.completeStep(&first, step.items);

    // Releasing the second entry swaps the fourth into its array slot. Turn
    // order must remain 1, 3, 4 rather than following that storage order.
    coordinator.release(second);
    try std.testing.expectEqual(fourth.request_id, coordinator.entries.items[1].id);
    try std.testing.expectEqual(third.request_id, coordinator.pickPendingDecodeRoundRobin(first.request_id).?);
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &third,
        @ptrCast(&work[2]),
        .decode,
        .{ .max_items = 1, .max_query_tokens = 1 },
        &step,
    ));
    coordinator.completeStep(&third, step.items);
    try std.testing.expectEqual(fourth.request_id, coordinator.pickPendingDecodeRoundRobin(third.request_id).?);
}

test "awaitTurn propagates cancellation without stealing the active turn" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var first = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(first);
    var second = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(second);
    var first_work: u8 = 1;
    var second_work: u8 = 2;
    coordinator.beginDecode(&first, 4);
    coordinator.beginDecode(&second, 4);
    try coordinator.enqueueDecodeWork(first, @ptrCast(&first_work), 5, 5, 0, .{});
    try coordinator.enqueueDecodeWork(second, @ptrCast(&second_work), 5, 5, 0, .{});
    try std.testing.expect(coordinator.tryAcquireTurn(&first, .decode));

    const Waiter = struct {
        fn run(target: *NativeGenerateCoordinator, lease: *Lease, io: std.Io) anyerror!void {
            try target.awaitTurn(lease, .decode, io);
        }
    };
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var future = try io.concurrent(Waiter.run, .{ &coordinator, &second, io });
    try std.testing.expectError(error.Canceled, future.cancel(io));
    try std.testing.expectEqual(first.request_id, coordinator.in_turn.?);
}

test "deferred turn cleanup releases ownership after post-acquire failure" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var first = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(first);
    var second = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(second);
    var first_work: u8 = 1;
    var second_work: u8 = 2;
    coordinator.beginDecode(&first, 4);
    coordinator.beginDecode(&second, 4);
    try coordinator.enqueueDecodeWork(first, @ptrCast(&first_work), 5, 5, 0, .{});
    try coordinator.enqueueDecodeWork(second, @ptrCast(&second_work), 5, 5, 0, .{});

    const FailingStep = struct {
        fn run(target: *NativeGenerateCoordinator, lease: *Lease, io: std.Io) !void {
            try target.awaitTurn(lease, .decode, io);
            defer target.finishTurn(lease, .decode);
            return error.InjectedFailure;
        }
    };
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    try std.testing.expectError(error.InjectedFailure, FailingStep.run(&coordinator, &first, io_impl.io()));
    try std.testing.expectEqual(@as(?RequestId, null), coordinator.in_turn);

    coordinator.cancelDecodeWork(@ptrCast(&first_work));
    try std.testing.expect(coordinator.tryAcquireTurn(&second, .decode));
}

test "idle cancellation removes pending work but never a claimed pointer" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var first = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(first);
    var second = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(second);
    var first_work: u8 = 1;
    var second_work: u8 = 2;
    coordinator.beginDecode(&first, 4);
    coordinator.beginDecode(&second, 4);
    try coordinator.enqueueDecodeWork(first, @ptrCast(&first_work), 5, 5, 0, .{});
    try coordinator.enqueueDecodeWork(second, @ptrCast(&second_work), 5, 5, 0, .{});

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &first,
        @ptrCast(&first_work),
        .decode,
        .{ .max_items = 2, .max_query_tokens = 2 },
        &step,
    ));
    try std.testing.expectEqual(@as(usize, 2), step.items.len);
    try std.testing.expect(!coordinator.cancelPendingWorkIfIdle(@ptrCast(&second_work), .decode));

    coordinator.completeStep(&first, step.items);
    try std.testing.expect(!coordinator.cancelPendingWorkIfIdle(@ptrCast(&second_work), .decode));

    try coordinator.enqueueDecodeWork(second, @ptrCast(&second_work), 6, 6, 0, .{});
    try std.testing.expect(coordinator.cancelPendingWorkIfIdle(@ptrCast(&second_work), .decode));
    try std.testing.expectEqual(@as(usize, 0), coordinator.pending_decode.items.len);
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
    try coordinator.enqueueDecodeWork(decode_lease, @ptrCast(&decode_a), 9, 9, 0, .{});
    try coordinator.enqueueDecodeWork(decode_lease, @ptrCast(&decode_b), 9, 9, 0, .{});
    try coordinator.enqueuePrefillWork(prefill_lease, @ptrCast(&prefill_w), 10, 4, 6, 0, .{});

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
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&exclusive_work), 9, 9, 0, .{});
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&peer_work), 9, 9, 0, .{});
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
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&leader_work), 9, 9, 0, .{});
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&exclusive_peer), 9, 9, 0, .{});
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&normal_peer), 9, 9, 0, .{});
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
    try coordinator.enqueueDecodeWork(decode_lease, @ptrCast(&work), 5, 5, 0, .{});

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
    try coordinator.enqueuePrefillWork(prefill_lease, @ptrCast(&work), 16, 8, 8, 0, .{});

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
    try coordinator.enqueueDecodeWork(lease_a, @ptrCast(&w_a), 5, 5, 0, .{});
    try coordinator.enqueueDecodeWork(lease_b, @ptrCast(&w_b), 5, 5, 0, .{});
    try coordinator.enqueueDecodeWork(lease_c, @ptrCast(&w_c), 5, 5, 0, .{});

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
    try coordinator.enqueueDecodeWork(leader_lease, @ptrCast(&leader_w), 5, 5, 0, .{});
    try coordinator.enqueuePrefillWork(big_lease, @ptrCast(&big_w), 256, 256, 0, 0, .{});
    try coordinator.enqueuePrefillWork(small_lease, @ptrCast(&small_w), 16, 16, 0, 0, .{});

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
    try coordinator.enqueueDecodeWork(lease_a, @ptrCast(&w_a), 5, 5, 0, .{});
    try coordinator.enqueueDecodeWork(lease_b, @ptrCast(&w_b), 5, 5, 0, .{});
    try coordinator.enqueueDecodeWork(lease_c, @ptrCast(&w_c), 5, 5, 0, .{});

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

test "enqueue work stores batching metadata atomically" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var leader = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(leader);
    var exclusive = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(exclusive);
    var over_budget = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(over_budget);
    var admitted = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(admitted);

    var leader_w: u8 = 1;
    var exclusive_w: u8 = 2;
    var over_budget_w: u8 = 3;
    var admitted_w: u8 = 4;

    coordinator.beginDecode(&leader, 4);
    coordinator.beginDecode(&exclusive, 4);
    coordinator.beginDecode(&over_budget, 4);
    coordinator.beginDecode(&admitted, 4);
    try coordinator.enqueueDecodeWork(leader, @ptrCast(&leader_w), 5, 5, 0, .{ .kv_blocks_estimate = 1 });
    try coordinator.enqueueDecodeWork(exclusive, @ptrCast(&exclusive_w), 5, 5, 0, .{ .exclusive_step = true });
    try coordinator.enqueueDecodeWork(over_budget, @ptrCast(&over_budget_w), 5, 5, 0, .{ .kv_blocks_estimate = 4 });
    try coordinator.enqueueDecodeWork(admitted, @ptrCast(&admitted_w), 5, 5, 0, .{ .kv_blocks_estimate = 2 });

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &leader,
        @ptrCast(&leader_w),
        .decode,
        .{ .max_items = 4, .max_query_tokens = 8, .max_kv_blocks = 4 },
        &step,
    ));

    try std.testing.expectEqual(@as(usize, 2), step.items.len);
    try std.testing.expectEqual(@as(*anyopaque, @ptrCast(&leader_w)), step.items[0].work_ptr);
    try std.testing.expectEqual(@as(*anyopaque, @ptrCast(&admitted_w)), step.items[1].work_ptr);
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
    try coordinator.enqueueDecodeWork(first, @ptrCast(&w1), 5, 5, 0, .{});
    try coordinator.enqueueDecodeWork(second, @ptrCast(&w2), 5, 5, 0, .{});

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

test "claimStep rejects a peer pointer presented by the selected lease" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var first = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(first);
    var second = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(second);
    coordinator.beginDecode(&first, 4);
    coordinator.beginDecode(&second, 4);

    var first_work: u8 = 1;
    var second_work: u8 = 2;
    try coordinator.enqueueDecodeWork(first, @ptrCast(&first_work), 5, 5, 0, .{});
    try coordinator.enqueueDecodeWork(second, @ptrCast(&second_work), 5, 5, 0, .{});

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);
    try std.testing.expect(!(try coordinator.claimStep(
        allocator,
        &first,
        @ptrCast(&second_work),
        .decode,
        coordinator.defaultStepBudget(),
        &step,
    )));
    try std.testing.expectEqual(@as(usize, 0), step.items.len);
    try std.testing.expectEqual(@as(?RequestId, null), coordinator.in_turn);
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
    try coordinator.enqueueDecodeWork(decode_lease, @ptrCast(&dec_w), 5, 5, 0, .{});
    try coordinator.enqueuePrefillWork(prefill_lease, @ptrCast(&pre_w), 8, 4, 4, 0, .{});

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

test "prefill leader packs compatible decode peers by default" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();
    coordinator.policy.max_step_items = 3;

    var prefill = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 128, .max_tokens = 8 });
    defer coordinator.release(prefill);
    var first_decode = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(first_decode);
    var second_decode = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(second_decode);

    coordinator.notePrefillProgress(&prefill, 0, 8);
    coordinator.beginDecode(&first_decode, 4);
    coordinator.beginDecode(&second_decode, 4);
    coordinator.consecutive_decode_turns = coordinator.max_decode_streak_before_prefill;

    var prefill_work: u8 = 1;
    var first_decode_work: u8 = 2;
    var second_decode_work: u8 = 3;
    try coordinator.enqueuePrefillWork(prefill, @ptrCast(&prefill_work), 8, 4, 4, 0, .{});
    try coordinator.enqueueDecodeWork(first_decode, @ptrCast(&first_decode_work), 5, 5, 0, .{});
    try coordinator.enqueueDecodeWork(second_decode, @ptrCast(&second_decode_work), 5, 5, 0, .{});

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &prefill,
        @ptrCast(&prefill_work),
        .prefill,
        coordinator.defaultStepBudget(),
        &step,
    ));
    try std.testing.expectEqual(@as(usize, 3), step.items.len);
    try std.testing.expectEqual(Phase.prefill, step.items[0].phase);
    try std.testing.expectEqual(Phase.decode, step.items[1].phase);
    try std.testing.expectEqual(Phase.decode, step.items[2].phase);
}

test "mixed step fairness follows the prefill leader" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();
    coordinator.policy.max_step_items = 2;
    coordinator.policy.allow_mixed_sequence_lengths = true;

    var first_prefill = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 128, .max_tokens = 8 });
    defer coordinator.release(first_prefill);
    var second_prefill = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 128, .max_tokens = 8 });
    defer coordinator.release(second_prefill);
    var decode = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(decode);

    coordinator.notePrefillProgress(&first_prefill, 0, 8);
    coordinator.notePrefillProgress(&second_prefill, 0, 8);
    coordinator.beginDecode(&decode, 4);
    coordinator.consecutive_decode_turns = coordinator.max_decode_streak_before_prefill;

    var first_work: u8 = 1;
    var second_work: u8 = 2;
    var decode_work: u8 = 3;
    try coordinator.enqueuePrefillWork(first_prefill, @ptrCast(&first_work), 8, 4, 4, 0, .{});
    try coordinator.enqueuePrefillWork(second_prefill, @ptrCast(&second_work), 8, 4, 4, 0, .{});
    try coordinator.enqueueDecodeWork(decode, @ptrCast(&decode_work), 5, 5, 0, .{});

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &first_prefill,
        @ptrCast(&first_work),
        .prefill,
        coordinator.defaultStepBudget(),
        &step,
    ));
    try std.testing.expectEqual(@as(usize, 2), step.items.len);
    try std.testing.expectEqual(Phase.prefill, step.items[0].phase);
    try std.testing.expectEqual(Phase.decode, step.items[1].phase);

    coordinator.completeStep(&first_prefill, step.items);
    try std.testing.expectEqual(first_prefill.request_id, coordinator.last_prefill_granted.?);
    try std.testing.expectEqual(@as(usize, 0), coordinator.consecutive_decode_turns);
    try std.testing.expectEqual(second_prefill.request_id, coordinator.pickNextTurn().?);
}

test "completeStep records singleton steps" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var lease = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(lease);

    var work: u8 = 1;
    coordinator.beginDecode(&lease, 4);
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&work), 5, 5, 0, .{});

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
                try coordinator.enqueueDecodeWork(leases[li], @ptrCast(slot), 9, 9, 0, .{});
            } else {
                slot.phase = .prefill;
                coordinator.notePrefillProgress(&leases[li], 0, 8);
                try coordinator.enqueuePrefillWork(leases[li], @ptrCast(slot), 12, 4, 8, 0, .{});
            }
        }
    }

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);

    var seen = @as([total_items]bool, @splat(false));
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
    try coordinator.enqueueDecodeWork(lease_a, @ptrCast(&w_a), 5, 5, 0, .{});
    try coordinator.enqueueDecodeWork(lease_b, @ptrCast(&w_b), 5, 5, 0, .{});
    try coordinator.enqueueDecodeWork(lease_c, @ptrCast(&w_c), 5, 5, 0, .{});

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
            try coordinator.enqueueDecodeWork(leases[li], @ptrCast(slot), 5 + round, 5 + round, 0, .{});
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

test "decode coalescing skips a sole request and records a two-item batch" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();
    coordinator.policy.max_decode_wait_us = 1_000;

    var first = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 32, .max_tokens = 8 });
    defer coordinator.release(first);
    coordinator.beginDecode(&first, 4);
    coordinator.noteDecodeProgress(&first, 1);
    var first_work: u8 = 1;
    try coordinator.enqueueDecodeWork(first, @ptrCast(&first_work), 5, 5, 0, .{});
    try std.testing.expectEqual(@as(u32, 0), coordinator.decodeCoalesceDelayUs(first));

    var second = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 32, .max_tokens = 8 });
    defer coordinator.release(second);
    try std.testing.expect(coordinator.shouldBatchCurrentRequests());
    try std.testing.expect(!coordinator.shouldBatchDecode(first, 5, 5, 0));
    coordinator.beginDecode(&second, 4);
    try std.testing.expect(!coordinator.shouldBatchDecode(first, 5, 5, 0));
    try std.testing.expectEqual(@as(u32, 0), coordinator.decodeCoalesceDelayUs(first));
    coordinator.noteDecodeProgress(&second, 1);
    try std.testing.expect(coordinator.shouldBatchDecode(first, 5, 5, 0));
    try std.testing.expectEqual(@as(u32, 1_000), coordinator.decodeCoalesceDelayUs(first));
    coordinator.noteDecodeCoalesceWait(1_000);

    var second_work: u8 = 2;
    try coordinator.enqueueDecodeWork(second, @ptrCast(&second_work), 5, 5, 0, .{});
    try std.testing.expectEqual(@as(u32, 0), coordinator.decodeCoalesceDelayUs(first));

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &first,
        @ptrCast(&first_work),
        .decode,
        coordinator.defaultStepBudget(),
        &step,
    ));
    try std.testing.expectEqual(@as(usize, 2), step.items.len);
    coordinator.completeStep(&first, step.items);
    try std.testing.expectEqual(@as(u64, 1), coordinator.stats.decode_coalesce_waits_total);
    try std.testing.expectEqual(@as(u64, 1_000), coordinator.stats.decode_coalesce_wait_us_total);
    try std.testing.expectEqual(@as(u64, 1), coordinator.stats.step_batch_size_2_total);
}

test "completed decode advertises next work instead of stale progress" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();
    coordinator.policy.max_decode_wait_us = 1_000;

    var first = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 32, .max_tokens = 8 });
    defer coordinator.release(first);
    var second = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 32, .max_tokens = 8 });
    defer coordinator.release(second);
    coordinator.beginDecode(&first, 4);
    coordinator.beginDecode(&second, 4);
    coordinator.noteDecodeProgress(&first, 1);
    coordinator.noteDecodeProgress(&second, 1);

    var second_work: u8 = 1;
    try coordinator.enqueueDecodeWork(second, @ptrCast(&second_work), 5, 5, 0, .{});
    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &second,
        @ptrCast(&second_work),
        .decode,
        coordinator.defaultStepBudget(),
        &step,
    ));
    coordinator.completeStep(&second, step.items);

    // The second entry still reports one generated token until its owner
    // samples the completed logits. Its next work is nevertheless position
    // six, so a position-five leader must not pay a stale coalescing wait.
    var first_work: u8 = 2;
    try coordinator.enqueueDecodeWork(first, @ptrCast(&first_work), 5, 5, 0, .{});
    try std.testing.expect(!coordinator.shouldBatchDecode(first, 5, 5, 0));
    try std.testing.expectEqual(@as(u32, 0), coordinator.decodeCoalesceDelayUs(first));
    coordinator.cancelDecodeWork(@ptrCast(&first_work));
}

test "C2 scheduler routing survives decode progress skew" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();
    coordinator.policy.max_step_items = 2;
    coordinator.policy.max_active_requests_for_batching = 2;
    coordinator.policy.max_decode_wait_us = 1_000;

    var first = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 32, .max_tokens = 8 });
    defer coordinator.release(first);
    var second = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 32, .max_tokens = 8 });
    defer coordinator.release(second);
    coordinator.beginDecode(&first, 4);
    coordinator.beginDecode(&second, 4);
    coordinator.noteDecodeProgress(&first, 1);

    // A compatibility-only routing decision would send the lagging request
    // direct here. C2 routing must keep it on the scheduler so the requests
    // can rendezvous after the lagging step catches up.
    try std.testing.expect(coordinator.shouldBatchCurrentRequests());
    try std.testing.expect(!coordinator.shouldBatchDecode(second, 4, 4, 0));

    var lagging_work: u8 = 1;
    try coordinator.enqueueDecodeWork(second, @ptrCast(&lagging_work), 4, 4, 0, .{});
    try std.testing.expectEqual(@as(u32, 0), coordinator.decodeCoalesceDelayUs(second));

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &second,
        @ptrCast(&lagging_work),
        .decode,
        coordinator.defaultStepBudget(),
        &step,
    ));
    try std.testing.expectEqual(@as(usize, 1), step.items.len);
    coordinator.completeStep(&second, step.items);
    coordinator.noteDecodeProgress(&second, 1);

    // Once aligned, the first pending request waits and the second request is
    // guaranteed to take the same scheduler route, producing a row-two step.
    var first_work: u8 = 2;
    var second_work: u8 = 3;
    try coordinator.enqueueDecodeWork(first, @ptrCast(&first_work), 5, 5, 0, .{});
    try std.testing.expectEqual(@as(u32, 1_000), coordinator.decodeCoalesceDelayUs(first));
    try coordinator.enqueueDecodeWork(second, @ptrCast(&second_work), 5, 5, 0, .{});
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &first,
        @ptrCast(&first_work),
        .decode,
        coordinator.defaultStepBudget(),
        &step,
    ));
    try std.testing.expectEqual(@as(usize, 2), step.items.len);
    coordinator.completeStep(&first, step.items);
    try std.testing.expectEqual(@as(u64, 1), coordinator.stats.step_batch_size_2_total);
    try std.testing.expectEqual(@as(u64, 0), coordinator.stats.decode_coalesce_waits_total);
}

test "decode batching ignores incompatible pending peers" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();
    coordinator.policy.max_decode_wait_us = 1_000;

    var first = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 32, .max_tokens = 8 });
    defer coordinator.release(first);
    coordinator.beginDecode(&first, 4);
    coordinator.noteDecodeProgress(&first, 1);
    var first_work: u8 = 1;
    try coordinator.enqueueDecodeWork(first, @ptrCast(&first_work), 5, 5, 0, .{});

    var second = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 64, .max_tokens = 8 });
    defer coordinator.release(second);
    coordinator.beginDecode(&second, 8);
    coordinator.noteDecodeProgress(&second, 1);
    var second_work: u8 = 2;
    try coordinator.enqueueDecodeWork(second, @ptrCast(&second_work), 9, 9, 0, .{});

    try std.testing.expect(coordinator.shouldBatchCurrentRequests());
    try std.testing.expect(!coordinator.shouldBatchDecode(first, 5, 5, 0));
    try std.testing.expectEqual(@as(u32, 0), coordinator.decodeCoalesceDelayUs(first));

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &first,
        @ptrCast(&first_work),
        .decode,
        coordinator.defaultStepBudget(),
        &step,
    ));
    try std.testing.expectEqual(@as(usize, 1), step.items.len);
    coordinator.completeStep(&first, step.items);
}

test "coordinator serializes concurrent acquire and release" {
    const Worker = struct {
        fn run(coordinator: *NativeGenerateCoordinator) void {
            for (0..128) |_| {
                const lease = coordinator.acquire(.{
                    .requested_units = 1,
                    .prompt_bytes = 32,
                    .max_tokens = 8,
                }) catch @panic("coordinator acquire failed");
                coordinator.release(lease);
            }
        }
    };

    var coordinator = NativeGenerateCoordinator.init(std.testing.allocator);
    defer coordinator.deinit();
    var threads: [4]std.Thread = undefined;
    for (&threads) |*thread| {
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{&coordinator});
    }
    for (&threads) |*thread| thread.join();

    const state = coordinator.snapshot();
    try std.testing.expectEqual(@as(usize, 0), state.active_units);
    try std.testing.expectEqual(@as(usize, 0), state.waiting_requests);
    try std.testing.expectEqual(@as(RequestId, 513), coordinator.next_request_id);
}

test "active request rollout cap forces singleton budgets" {
    var coordinator = NativeGenerateCoordinator.init(std.testing.allocator);
    defer coordinator.deinit();
    coordinator.setPolicy(.{
        .max_step_items = 8,
        .max_active_requests_for_batching = 2,
    });

    const first = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(first);
    const second = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(second);
    try std.testing.expectEqual(@as(usize, 8), coordinator.defaultStepBudget().max_items);

    const third = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(third);
    try std.testing.expectEqual(@as(usize, 1), coordinator.defaultStepBudget().max_items);
    try std.testing.expect(!coordinator.shouldBatchCurrentRequests());
    try std.testing.expect(coordinator.shouldCoordinateCurrentRequests());
}

test "rollout cap disables pointless decode coalescing" {
    var coordinator = NativeGenerateCoordinator.init(std.testing.allocator);
    defer coordinator.deinit();
    coordinator.setPolicy(.{
        .max_step_items = 2,
        .max_active_requests_for_batching = 2,
        .max_decode_wait_us = 1_000,
    });

    var first = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(first);
    var second = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(second);
    const third = try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 });
    defer coordinator.release(third);
    coordinator.beginDecode(&first, 4);
    coordinator.beginDecode(&second, 4);
    coordinator.noteDecodeProgress(&first, 1);
    coordinator.noteDecodeProgress(&second, 1);

    var work: u8 = 1;
    try coordinator.enqueueDecodeWork(first, @ptrCast(&work), 5, 5, 0, .{});
    try std.testing.expectEqual(@as(u32, 0), coordinator.decodeCoalesceDelayUs(first));
}

test "rollout cap singleton turns rotate across stable request order" {
    const allocator = std.testing.allocator;
    var coordinator = NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();
    coordinator.setPolicy(.{
        .max_step_items = 8,
        .max_active_requests_for_batching = 2,
    });

    var leases = [_]Lease{
        try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 }),
        try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 }),
        try coordinator.acquire(.{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 }),
    };
    defer for (leases) |lease| coordinator.release(lease);
    for (&leases) |*lease| coordinator.beginDecode(lease, 4);

    var work: [6]u8 = undefined;
    for (0..3) |idx| {
        work[idx] = @intCast(idx);
        try coordinator.enqueueDecodeWork(leases[idx], @ptrCast(&work[idx]), 5, 5, 0, .{});
    }

    var step = std.ArrayListUnmanaged(StepItem).empty;
    defer step.deinit(allocator);
    for (0..6) |turn| {
        const lease_idx = turn % leases.len;
        try std.testing.expect(try coordinator.claimStep(
            allocator,
            &leases[lease_idx],
            @ptrCast(&work[turn]),
            .decode,
            coordinator.defaultStepBudget(),
            &step,
        ));
        try std.testing.expectEqual(@as(usize, 1), step.items.len);
        try std.testing.expectEqual(@as(*anyopaque, @ptrCast(&work[turn])), step.items[0].work_ptr);
        coordinator.completeStep(&leases[lease_idx], step.items);

        if (turn < leases.len) {
            work[turn + leases.len] = @intCast(turn + leases.len);
            try coordinator.enqueueDecodeWork(
                leases[lease_idx],
                @ptrCast(&work[turn + leases.len]),
                6,
                6,
                0,
                .{},
            );
        }
    }
}
