// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const change_journal_mod = @import("change_journal.zig");
const catch_up_policy = @import("catch_up_policy.zig");
const replay_source_mod = @import("replay_source.zig");
const derived_types = @import("derived_types.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
const db_types = @import("../types.zig");
const batcher = @import("../batcher.zig");
const internal_keys = @import("../../internal_keys.zig");
const docstore_mod = @import("../../docstore.zig");
const mem_backend_mod = @import("../../mem_backend.zig");
const resource_manager_mod = @import("../../resource_manager.zig");
const platform_time = @import("antfly_platform").time;

pub const ApplyFn = batcher.ApplyFn;
pub const PersistProgressFn = *const fn (ctx: *anyopaque, index_name: []const u8, sequence: u64) anyerror!void;
pub const ProgressFn = *const fn (ctx: *anyopaque, index_name: []const u8, progress: CatchUpProgress) anyerror!void;
pub const BeginWindowFn = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef) anyerror!void;
pub const FinishWindowFn = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, success: bool) anyerror!void;
pub const BeginCatchUpFn = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef) anyerror!void;
pub const FinishCatchUpFn = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, success: bool) anyerror!void;

pub const CatchUpStats = struct {
    scanned_entries: usize = 0,
    applied_entries: usize = 0,
    replay_scan_batches: usize = 0,
    replay_hint_filter_skips: usize = 0,
    last_sequence: u64 = 0,
    last_applied_sequence: u64 = 0,
    window_collect_ns: u64 = 0,
    apply_ns: u64 = 0,

    pub fn appliedSequenceAdvance(self: @This(), from_sequence: u64) ?u64 {
        if (self.last_applied_sequence <= from_sequence) return null;
        return self.last_applied_sequence;
    }

    pub fn shouldTryTargetAdvance(self: @This(), from_sequence: u64, target_sequence: u64) bool {
        return self.applied_entries == 0 and target_sequence > from_sequence;
    }
};

test "CatchUpStats target advance covers scanned zero-applied tail" {
    const applied: u64 = 260;
    const target_sequence: u64 = 273;

    try std.testing.expect((CatchUpStats{
        .scanned_entries = 13,
        .applied_entries = 0,
        .last_sequence = applied,
    }).shouldTryTargetAdvance(applied, target_sequence));
    try std.testing.expect((CatchUpStats{
        .scanned_entries = 0,
        .applied_entries = 0,
        .last_sequence = applied,
    }).shouldTryTargetAdvance(applied, target_sequence));
    try std.testing.expect(!(CatchUpStats{
        .scanned_entries = 13,
        .applied_entries = 1,
        .last_sequence = target_sequence,
    }).shouldTryTargetAdvance(applied, target_sequence));
    try std.testing.expect(!(CatchUpStats{
        .scanned_entries = 13,
        .applied_entries = 0,
        .last_sequence = applied,
    }).shouldTryTargetAdvance(target_sequence, target_sequence));
    try std.testing.expectEqual(@as(?u64, null), (CatchUpStats{
        .scanned_entries = 13,
        .applied_entries = 0,
        .last_sequence = target_sequence,
    }).appliedSequenceAdvance(target_sequence));
    try std.testing.expectEqual(@as(?u64, null), (CatchUpStats{
        .scanned_entries = 13,
        .applied_entries = 0,
        .last_sequence = target_sequence,
    }).appliedSequenceAdvance(applied));
    try std.testing.expectEqual(target_sequence, (CatchUpStats{
        .scanned_entries = 13,
        .applied_entries = 1,
        .last_sequence = target_sequence,
        .last_applied_sequence = target_sequence,
    }).appliedSequenceAdvance(applied).?);
}

pub const CatchUpProgress = struct {
    sequence: u64 = 0,
    scanned_entries: u64 = 0,
    applied_entries: u64 = 0,
    replay_scan_batches: u64 = 0,
    replay_hint_filter_skips: u64 = 0,
};

pub const CatchUpOptions = struct {
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    progress_ctx: ?*anyopaque = null,
    progress_fn: ?ProgressFn = null,
    persist_ctx: ?*anyopaque = null,
    persist_progress_fn: ?PersistProgressFn = null,
    window_ctx: ?*anyopaque = null,
    begin_window_fn: ?BeginWindowFn = null,
    finish_window_fn: ?FinishWindowFn = null,
    catch_up_ctx: ?*anyopaque = null,
    begin_catch_up_fn: ?BeginCatchUpFn = null,
    finish_catch_up_fn: ?FinishCatchUpFn = null,
    max_records_per_window: usize = catch_up_max_records_per_window_default,
    max_chunk_bytes: u64 = catch_up_max_chunk_bytes_default,
    max_items_per_window: usize = 0,
    max_windows_per_call: usize = 0,
    /// Cooperative publication quantum; expires only between complete chunks.
    max_call_ns: u64 = 0,
    max_call_bytes: u64 = 0,
    estimated_dense_vector_bytes: u64 = 0,
    target_sequence: u64 = 0,
    /// Optional absolute monotonic deadline. Collection stops before opening a
    /// new apply window once this deadline is reached. Callers must still size
    /// windows so an individual backend apply fits within their service SLO.
    deadline_ns: ?u64 = null,
};

pub const catch_up_max_records_per_window_default: usize = 2048;
pub const catch_up_max_chunk_bytes_default: u64 = 16 * 1024 * 1024;
pub const dense_replay_estimated_vector_bytes_default: u64 = 384 * @sizeOf(f32);

pub fn targetHintForManagedIndex(index_ref: index_manager_mod.ManagedIndexRef) change_journal_mod.TargetHint {
    return switch (index_ref.kind) {
        .full_text => .full_text,
        .dense_vector => .dense_vector,
        .sparse_vector => .sparse_vector,
        .graph => .graph,
        .algebraic => .algebraic,
    };
}

fn logCatchUpError(
    index_ref: index_manager_mod.ManagedIndexRef,
    phase: []const u8,
    sequence: u64,
    scanned_entries: usize,
    applied_entries: usize,
    err: anyerror,
) void {
    if (catch_up_policy.isRecoverableAdmissionError(err)) return;
    if (err == error.ReplayDocumentNotVisible) return;
    if (err == error.ArtifactRepairRequired) return;
    if (err == error.CatchUpDeadlineExceeded) return;
    if (index_ref.kind == .dense_vector and err == error.NotFound) return;
    std.log.err(
        "derived catch_up failed index={s} kind={s} phase={s} sequence={} scanned_entries={} applied_entries={} err={s}",
        .{ index_ref.name, @tagName(index_ref.kind), phase, sequence, scanned_entries, applied_entries, @errorName(err) },
    );
}

pub fn catchUpIndex(
    alloc: Allocator,
    replay_source: replay_source_mod.Source,
    index_ref: index_manager_mod.ManagedIndexRef,
    from_sequence: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    apply_ctx: *anyopaque,
    apply_fn: ApplyFn,
    persist_ctx: ?*anyopaque,
    persist_progress_fn: ?PersistProgressFn,
) !CatchUpStats {
    return try catchUpIndexWithOptions(alloc, replay_source, index_ref, from_sequence, apply_ctx, apply_fn, .{
        .resource_manager = resource_manager,
        .persist_ctx = persist_ctx,
        .persist_progress_fn = persist_progress_fn,
    });
}

pub fn catchUpIndexWithOptions(
    alloc: Allocator,
    replay_source: replay_source_mod.Source,
    index_ref: index_manager_mod.ManagedIndexRef,
    from_sequence: u64,
    apply_ctx: *anyopaque,
    apply_fn: ApplyFn,
    options: CatchUpOptions,
) !CatchUpStats {
    return try catchUpIndexFromReplaySource(alloc, replay_source, index_ref, from_sequence, apply_ctx, apply_fn, options);
}

fn catchUpIndexFromReplaySource(
    alloc: Allocator,
    replay_source: replay_source_mod.Source,
    index_ref: index_manager_mod.ManagedIndexRef,
    from_sequence: u64,
    apply_ctx: *anyopaque,
    apply_fn: ApplyFn,
    options: CatchUpOptions,
) !CatchUpStats {
    const hint = targetHintForManagedIndex(index_ref);
    var stats = CatchUpStats{};
    const next_sequence = from_sequence;
    var catch_up_open = false;
    errdefer if (catch_up_open) {
        if (options.finish_catch_up_fn) |finish_catch_up| {
            finish_catch_up(options.catch_up_ctx.?, index_ref, false) catch {};
        }
    };
    if (options.begin_catch_up_fn) |begin_catch_up| {
        begin_catch_up(options.catch_up_ctx.?, index_ref) catch |err| {
            logCatchUpError(index_ref, "begin_catch_up", next_sequence, stats.scanned_entries, stats.applied_entries, err);
            return err;
        };
        catch_up_open = true;
    }
    var replay_cursor = replay_source.openMatchingCursor(alloc, from_sequence, hint) catch |err| {
        logCatchUpError(index_ref, "replay_source_open", next_sequence, stats.scanned_entries, stats.applied_entries, err);
        return err;
    };
    defer replay_cursor.deinit(alloc);
    stats = catchUpIndexFromMatchingCursor(alloc, &replay_cursor, index_ref, apply_ctx, apply_fn, options) catch |err| {
        logCatchUpError(index_ref, "replay_source_iterate", next_sequence, stats.scanned_entries, stats.applied_entries, err);
        return err;
    };
    if (catch_up_open) {
        if (options.finish_catch_up_fn) |finish_catch_up| {
            finish_catch_up(options.catch_up_ctx.?, index_ref, true) catch |err| {
                logCatchUpError(index_ref, "finish_catch_up", stats.last_sequence, stats.scanned_entries, stats.applied_entries, err);
                return err;
            };
        }
    }
    return stats;
}

pub fn catchUpIndexFromMatchingCursor(
    alloc: Allocator,
    replay_cursor: *replay_source_mod.MatchingCursor,
    index_ref: index_manager_mod.ManagedIndexRef,
    apply_ctx: *anyopaque,
    apply_fn: ApplyFn,
    options: CatchUpOptions,
) !CatchUpStats {
    var stats = CatchUpStats{};
    var completed_windows: usize = 0;
    const call_started_ns = monotonicTimeNs();
    var call_bytes: u64 = 0;
    // Scratch belongs to this catch-up call, rather than one replay window.
    // Its allocator charges retained capacity to the replay memory budget.
    var scratch_budget: ?resource_manager_mod.BudgetedAllocator = if (options.resource_manager) |manager|
        resource_manager_mod.BudgetedAllocator.init(manager, .derived_replay_window, alloc, 1)
    else
        null;
    defer if (scratch_budget) |*budget| budget.deinit();
    const scratch_alloc = if (scratch_budget) |*budget| budget.allocator() else alloc;
    var decode_scratch: change_journal_mod.BorrowedBinaryRecordScratch = .{};
    defer decode_scratch.deinit(scratch_alloc);
    while (true) {
        // Oversized records may need more scratch once, but cannot establish
        // a large retained high-water mark for subsequent windows.
        defer {
            const retained_limit = if (options.resource_manager) |manager|
                @min(@as(u64, 64 * 1024), manager.availableAdmissionBytes(.derived_replay_window) / 8)
            else
                64 * 1024;
            decode_scratch.trimRetainedCapacity(scratch_alloc, @intCast(retained_limit));
            if (scratch_budget) |*budget| _ = budget.releaseUnusedCredit();
        }
        if (options.deadline_ns) |deadline| {
            if (monotonicTimeNs() >= deadline) return error.CatchUpDeadlineExceeded;
        }
        var builder = ReplayChunkBuilder.init(alloc, index_ref, options.resource_manager, options.max_chunk_bytes);
        if (options.max_call_bytes != 0)
            builder.max_chunk_bytes = @min(builder.max_chunk_bytes, options.max_call_bytes -| call_bytes);
        builder.max_items = options.max_items_per_window;
        builder.estimated_dense_vector_bytes = options.estimated_dense_vector_bytes;
        builder.target_sequence = options.target_sequence;
        builder.decode_scratch = &decode_scratch;
        builder.scratch_alloc = scratch_alloc;
        builder.scratch_budget = if (scratch_budget) |*budget| budget else null;
        defer builder.deinit();

        const collect_started_ns = monotonicTimeNs();
        const had_retained_scratch = decode_scratch.retainedCapacityBytes() != 0;
        var retried_fresh = false;
        var collection_limit = options.max_records_per_window;
        var retry_stats = replay_source_mod.MatchingRecordStats{};
        const chunk_stats = collect: while (true) {
            const collected = replay_cursor.forEachNext(collection_limit, &builder, replayChunkConsumeRecord) catch |err| {
                // An empty window gets one retry after reclaiming cached
                // scratch and key-block slack. The failed record has not
                // advanced the cursor; a fresh exact-sized retry is bounded.
                if (err == error.ResourceBudgetExceeded and
                    (had_retained_scratch or builder.hasSpareKeyCapacity() or builder.had_spare_metadata) and
                    !retried_fresh and !builder.has_accepted_work)
                {
                    // No-op records already advanced the cursor. Keep the
                    // remaining record quota rather than restarting the window.
                    retry_stats.add(replay_cursor.last_scan_stats);
                    if (collection_limit != 0) collection_limit -= builder.accepted_records;
                    builder.resetEmptyWindow();
                    builder.minimal_key_blocks = true;
                    decode_scratch.trimRetainedCapacity(scratch_alloc, 0);
                    if (scratch_budget) |*budget| _ = budget.releaseUnusedCredit();
                    retried_fresh = true;
                    continue :collect;
                }
                return err;
            };
            retry_stats.add(collected);
            break :collect retry_stats;
        };
        stats.window_collect_ns += monotonicTimeNs() - collect_started_ns;
        if (chunk_stats.last_sequence == 0) {
            break;
        }
        // Never begin a storage mutation after the caller's deadline. The
        // replay cursor is disposable; a retry reopens it from the last
        // durably persisted applied sequence.
        if (options.deadline_ns) |deadline| {
            if (monotonicTimeNs() >= deadline) return error.CatchUpDeadlineExceeded;
        }
        stats.scanned_entries += if (chunk_stats.scanned_entries != 0) chunk_stats.scanned_entries else chunk_stats.matched_entries;
        stats.replay_scan_batches += chunk_stats.scan_batches;
        stats.replay_hint_filter_skips += chunk_stats.hint_filter_skips;
        stats.last_sequence = chunk_stats.last_sequence;
        if (options.progress_fn) |progress| {
            try progress(options.progress_ctx.?, index_ref.name, .{
                .sequence = chunk_stats.last_sequence,
                .scanned_entries = @intCast(stats.scanned_entries),
                .applied_entries = @intCast(stats.applied_entries),
                .replay_scan_batches = @intCast(stats.replay_scan_batches),
                .replay_hint_filter_skips = @intCast(stats.replay_hint_filter_skips),
            });
        }

        const collected_bytes = builder.tracked_bytes;
        const batch = try builder.finishBorrowed(chunk_stats.last_sequence);
        var window_open = false;
        errdefer if (window_open) {
            if (options.finish_window_fn) |finish_window| {
                finish_window(options.window_ctx.?, index_ref, false) catch {};
            }
        };
        if (options.begin_window_fn) |begin_window| {
            begin_window(options.window_ctx.?, index_ref) catch |err| {
                logCatchUpError(index_ref, "begin_window", chunk_stats.last_sequence, stats.scanned_entries, stats.applied_entries, err);
                return err;
            };
            window_open = true;
        }

        const apply_started_ns = monotonicTimeNs();
        const applied = applyBatchBounded(apply_ctx, apply_fn, batch, index_ref, options.max_items_per_window) catch |err| {
            stats.apply_ns += monotonicTimeNs() - apply_started_ns;
            logCatchUpError(index_ref, "journal_apply", chunk_stats.last_sequence, stats.scanned_entries, stats.applied_entries, err);
            return err;
        };
        if (applied) {
            stats.apply_ns += monotonicTimeNs() - apply_started_ns;
            stats.applied_entries += 1;
            stats.last_applied_sequence = chunk_stats.last_sequence;
        } else {
            stats.apply_ns += monotonicTimeNs() - apply_started_ns;
        }
        if (options.progress_fn) |progress| {
            try progress(options.progress_ctx.?, index_ref.name, .{
                .sequence = chunk_stats.last_sequence,
                .scanned_entries = @intCast(stats.scanned_entries),
                .applied_entries = @intCast(stats.applied_entries),
                .replay_scan_batches = @intCast(stats.replay_scan_batches),
                .replay_hint_filter_skips = @intCast(stats.replay_hint_filter_skips),
            });
        }
        if (options.finish_window_fn) |finish_window| {
            finish_window(options.window_ctx.?, index_ref, true) catch |err| {
                logCatchUpError(index_ref, "finish_window", chunk_stats.last_sequence, stats.scanned_entries, stats.applied_entries, err);
                return err;
            };
            window_open = false;
        }
        if (options.persist_progress_fn) |persist| {
            if (applied) {
                persist(options.persist_ctx.?, index_ref.name, chunk_stats.last_sequence) catch |err| {
                    logCatchUpError(index_ref, "persist_progress", chunk_stats.last_sequence, stats.scanned_entries, stats.applied_entries, err);
                    return err;
                };
            }
        }
        completed_windows += 1;
        call_bytes +|= collected_bytes;
        // Oversized single records retain the existing one-record progress
        // exception, but cannot multiply it across coalesced chunks.
        if (options.max_call_bytes != 0 and call_bytes >= options.max_call_bytes) break;
        if (options.max_call_ns != 0) {
            if (monotonicTimeNs() - call_started_ns >= options.max_call_ns) break;
            if (options.resource_manager) |manager| if (manager.shouldDeferOptionalMaintenanceForForegroundTraffic()) break;
        }
        if (options.deadline_ns) |deadline| {
            if (monotonicTimeNs() >= deadline) break;
        }
        if (options.max_windows_per_call > 0 and completed_windows >= options.max_windows_per_call) break;
    }
    return stats;
}

fn applyBatchBounded(
    apply_ctx: *anyopaque,
    apply_fn: ApplyFn,
    batch: derived_types.DerivedBatch,
    index_ref: index_manager_mod.ManagedIndexRef,
    max_items: usize,
) !bool {
    if (index_ref.kind != .full_text or max_items == 0) return try apply_fn(apply_ctx, batch, index_ref);
    if (batch.changed_artifact_keys.len != 0 or
        batch.graph_doc_clears.len != 0 or
        batch.dense_embeddings.len != 0 or
        batch.sparse_embeddings.len != 0 or
        batch.generated_enrichment_refs.len != 0 or
        batch.graph_writes.len != 0 or
        batch.graph_deletes.len != 0)
    {
        return try apply_fn(apply_ctx, batch, index_ref);
    }

    const total_items = batch.deleted_keys.len +| batch.overwritten_doc_keys.len +| batch.documents.len;
    if (total_items <= max_items) return try apply_fn(apply_ctx, batch, index_ref);

    var applied_any = false;
    var start: usize = 0;
    while (start < batch.deleted_keys.len) {
        const end = @min(start + max_items, batch.deleted_keys.len);
        applied_any = (try apply_fn(apply_ctx, .{
            .sequence = batch.sequence,
            .deleted_keys = batch.deleted_keys[start..end],
        }, index_ref)) or applied_any;
        start = end;
    }

    start = 0;
    while (start < batch.overwritten_doc_keys.len) {
        const end = @min(start + max_items, batch.overwritten_doc_keys.len);
        applied_any = (try apply_fn(apply_ctx, .{
            .sequence = batch.sequence,
            .overwritten_doc_keys = batch.overwritten_doc_keys[start..end],
        }, index_ref)) or applied_any;
        start = end;
    }

    start = 0;
    while (start < batch.documents.len) {
        const end = @min(start + max_items, batch.documents.len);
        applied_any = (try apply_fn(apply_ctx, .{
            .sequence = batch.sequence,
            .documents = batch.documents[start..end],
        }, index_ref)) or applied_any;
        start = end;
    }
    return applied_any;
}

fn monotonicTimeNs() u64 {
    return platform_time.monotonicNs();
}

const ReplayChunkBuilder = struct {
    const KeyBlock = struct { next: ?*KeyBlock, allocation_len: usize, used: usize };
    alloc: Allocator,
    index_ref: index_manager_mod.ManagedIndexRef,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    decode_scratch: *change_journal_mod.BorrowedBinaryRecordScratch = undefined,
    scratch_alloc: Allocator = undefined,
    scratch_budget: ?*resource_manager_mod.BudgetedAllocator = null,
    // Each block includes its header and is admitted at its full allocation
    // size before allocation. Keys live through apply and are freed together.
    key_blocks: ?*KeyBlock = null,
    has_accepted_work: bool = false,
    accepted_records: usize = 0,
    had_spare_metadata: bool = false,
    minimal_key_blocks: bool = false,
    documents: []derived_types.DerivedDocument = &.{},
    target: [1]derived_types.DerivedTargetRef = undefined,
    max_chunk_bytes: u64,
    changed_doc_keys: std.ArrayListUnmanaged([]const u8) = .empty,
    deleted_doc_keys: std.ArrayListUnmanaged([]const u8) = .empty,
    overwritten_doc_keys: std.ArrayListUnmanaged([]const u8) = .empty,
    changed_artifact_keys: std.ArrayListUnmanaged([]const u8) = .empty,
    seen_changed_docs: std.StringHashMapUnmanaged(void) = .empty,
    seen_deleted_docs: std.StringHashMapUnmanaged(void) = .empty,
    seen_overwritten_docs: std.StringHashMapUnmanaged(void) = .empty,
    seen_changed_artifacts: std.StringHashMapUnmanaged(void) = .empty,
    tracked_bytes: u64 = 0,
    item_count: usize = 0,
    max_items: usize = 0,
    estimated_dense_vector_bytes: u64 = 0,
    target_sequence: u64 = 0,

    fn init(
        alloc: Allocator,
        index_ref: index_manager_mod.ManagedIndexRef,
        resource_manager: ?*resource_manager_mod.ResourceManager,
        max_chunk_bytes: u64,
    ) @This() {
        return .{
            .alloc = alloc,
            .index_ref = index_ref,
            .resource_manager = resource_manager,
            .max_chunk_bytes = max_chunk_bytes,
        };
    }

    pub fn deinit(self: *@This()) void {
        if (self.documents.len > 0) self.alloc.free(self.documents);
        self.changed_doc_keys.deinit(self.alloc);
        self.deleted_doc_keys.deinit(self.alloc);
        self.overwritten_doc_keys.deinit(self.alloc);
        self.changed_artifact_keys.deinit(self.alloc);
        self.seen_changed_docs.deinit(self.alloc);
        self.seen_deleted_docs.deinit(self.alloc);
        self.seen_overwritten_docs.deinit(self.alloc);
        self.seen_changed_artifacts.deinit(self.alloc);
        while (self.key_blocks) |block| {
            const next = block.next;
            const bytes = @as([*]align(@alignOf(KeyBlock)) u8, @ptrCast(block))[0..block.allocation_len];
            self.alloc.free(bytes);
            self.key_blocks = next;
        }
        if (self.resource_manager) |manager|
            manager.adjustUsage(.derived_replay_window, &self.tracked_bytes, 0) catch {};
        self.* = undefined;
    }

    fn resetEmptyWindow(self: *@This()) void {
        var fresh = ReplayChunkBuilder.init(self.alloc, self.index_ref, self.resource_manager, self.max_chunk_bytes);
        fresh.decode_scratch = self.decode_scratch;
        fresh.scratch_alloc = self.scratch_alloc;
        fresh.scratch_budget = self.scratch_budget;
        fresh.max_items = self.max_items;
        fresh.estimated_dense_vector_bytes = self.estimated_dense_vector_bytes;
        fresh.target_sequence = self.target_sequence;
        self.deinit();
        self.* = fresh;
    }

    fn hasSpareKeyCapacity(self: *const @This()) bool {
        var next = self.key_blocks;
        while (next) |block| : (next = block.next) {
            if (block.used < block.allocation_len - @sizeOf(KeyBlock)) return true;
        }
        return false;
    }

    fn preferredKeyBlockBytes(self: *const @This(), headroom: u64) usize {
        if (self.minimal_key_blocks) return 0;
        var preferred: u64 = 4096;
        if (self.max_chunk_bytes != 0) preferred = @min(preferred, self.max_chunk_bytes / 8);
        preferred = @min(preferred, headroom / 8);
        return @intCast(preferred);
    }

    fn hasOutput(self: *const @This()) bool {
        return self.changed_doc_keys.items.len != 0 or self.deleted_doc_keys.items.len != 0 or
            self.overwritten_doc_keys.items.len != 0 or self.changed_artifact_keys.items.len != 0;
    }

    fn allocateKeyBlock(self: *@This(), bytes: usize) !void {
        const previous = self.tracked_bytes;
        try self.observeTrackedBytes(previous +| bytes);
        errdefer self.observeTrackedBytes(previous) catch {};
        const memory = try self.alloc.alignedAlloc(u8, .of(KeyBlock), bytes);
        const block: *KeyBlock = @ptrCast(memory.ptr);
        block.* = .{ .next = self.key_blocks, .allocation_len = bytes, .used = 0 };
        self.key_blocks = block;
    }

    fn compactRecordKeys(self: *@This(), starts: [4]usize) !void {
        const lists = .{ &self.changed_doc_keys, &self.deleted_doc_keys, &self.overwritten_doc_keys, &self.changed_artifact_keys };
        const maps = .{ &self.seen_changed_docs, &self.seen_deleted_docs, &self.seen_overwritten_docs, &self.seen_changed_artifacts };
        var bytes: usize = 0;
        inline for (lists, starts) |list, start| {
            for (list.items[start..]) |key| bytes = try std.math.add(usize, bytes, key.len);
        }
        if (bytes == 0) return;
        try self.allocateKeyBlock(try std.math.add(usize, @sizeOf(KeyBlock), bytes));
        inline for (lists, maps, starts) |list, seen, start| {
            for (list.items[start..]) |*key| {
                const entry = seen.getEntry(key.*).?;
                const owned = try self.copyKey(key.*);
                entry.key_ptr.* = owned;
                key.* = owned;
            }
        }
    }

    fn copyKey(self: *@This(), value: []const u8) ![]const u8 {
        if (self.key_blocks == null or self.key_blocks.?.allocation_len - @sizeOf(KeyBlock) - self.key_blocks.?.used < value.len) {
            const minimum = std.math.add(usize, @sizeOf(KeyBlock), value.len) catch return error.OutOfMemory;
            const headroom = if (self.resource_manager) |manager| manager.availableAdmissionBytes(.derived_replay_window) else std.math.maxInt(u64);
            const bytes = @max(minimum, self.preferredKeyBlockBytes(headroom));
            try self.allocateKeyBlock(bytes);
        }
        const block = self.key_blocks.?;
        const start = @sizeOf(KeyBlock) + block.used;
        const owned = @as([*]u8, @ptrCast(block))[start..][0..value.len];
        @memcpy(owned, value);
        block.used += value.len;
        return owned;
    }

    fn appendUniqueKey(
        self: *@This(),
        list: *std.ArrayListUnmanaged([]const u8),
        seen: *std.StringHashMapUnmanaged(void),
        value: []const u8,
    ) !void {
        if (value.len == 0) return;
        const prior_list_capacity = list.capacity;
        const prior_seen_capacity = seen.capacity();
        // Default StringHashMap load limit is public. Avoid getOrPut's
        // pre-probe growth for duplicates only when the map has no room.
        const load_limit = @as(u64, prior_seen_capacity) * std.hash_map.default_max_load_percentage / 100;
        if (seen.count() >= load_limit and seen.contains(value)) return;
        const entry = try seen.getOrPut(self.alloc, value);
        if (entry.found_existing) return;
        errdefer _ = seen.remove(value);
        // Replace the temporary borrowed key only after ownership succeeds.
        // Compact records transfer these keys together before the scan returns.
        const owned = if (self.minimal_key_blocks) value else try self.copyKey(value);
        entry.key_ptr.* = owned;
        try list.append(self.alloc, owned);
        errdefer list.items.len -= 1;
        var next_tracked_bytes = self.tracked_bytes;
        // Reserve descriptors with their keys, before the window is published.
        // finishBorrowed consumes this credit without another admission step.
        if (list == &self.changed_doc_keys)
            next_tracked_bytes +|= @sizeOf(derived_types.DerivedDocument);
        next_tracked_bytes +|= @as(u64, @intCast(list.capacity - prior_list_capacity)) * @sizeOf([]const u8);
        next_tracked_bytes +|= @as(u64, @intCast(seen.capacity() - prior_seen_capacity)) * (@sizeOf([]const u8) + @sizeOf(void));
        try self.observeTrackedBytes(next_tracked_bytes);
    }

    fn artifactMatches(key: []const u8, kind: ?db_types.IndexKind) bool {
        return if (kind) |selected| if (selected == .graph)
            internal_keys.isGraphEdgeArtifactKey(key) or internal_keys.isAssetArtifactKey(key) or
                internal_keys.isChunkArtifactRecordKey(key) or internal_keys.isResolutionArtifactKey(key)
        else
            internal_keys.isEmbeddingArtifactKey(key) or internal_keys.isDerivedEmbeddingArtifactKey(key) else true;
    }

    fn reserveKeyMetadata(self: *@This(), list: *std.ArrayListUnmanaged([]const u8), seen: *std.StringHashMapUnmanaged(void), keys: []const []const u8, kind: ?db_types.IndexKind) !void {
        // Reserve once at the first bulk record of a window, rather than
        // repeating capacity planning and map probes for subsequent records. Compact admission stays incremental and deduplication-aware.
        // The bounded hint avoids large reservations for duplicate-heavy records.
        if (self.minimal_key_blocks or list.items.len != 0 or seen.count() != 0 or keys.len < 16 or keys.len > 128) return;
        var new_count: usize = 0;
        for (keys) |key| {
            if (key.len != 0 and artifactMatches(key, kind)) new_count += 1;
        }
        if (new_count < 16) return;
        const desired_list = std.ArrayListUnmanaged([]const u8).growCapacity(try std.math.add(usize, list.items.len, new_count));
        const desired_map = try std.math.add(usize, seen.count(), new_count);
        const prior_list = list.capacity;
        const prior_map = seen.capacity();
        const list_upper = @max(prior_list, desired_list);
        const map_upper = @max(@as(u64, prior_map), @max(@as(u64, 8), @as(u64, desired_map) *| 4));
        const upper = @as(u64, list_upper - prior_list) *| @sizeOf([]const u8) +|
            (map_upper - prior_map) *| @sizeOf([]const u8);
        if (upper == 0) return;
        if (self.resource_manager) |manager| {
            if (upper > manager.availableAdmissionBytes(.derived_replay_window)) return;
        }
        const previous = self.tracked_bytes;
        self.observeTrackedBytes(previous +| upper) catch |err| {
            // An optional optimization cannot turn concurrent pressure into a
            // rejection of a record that incremental admission can still fit.
            if (err == error.ResourceBudgetExceeded) return;
            return err;
        };
        self.had_spare_metadata = true;
        errdefer self.observeTrackedBytes(previous +|
            @as(u64, list.capacity - prior_list) * @sizeOf([]const u8) +|
            @as(u64, seen.capacity() - prior_map) * @sizeOf([]const u8)) catch {};
        try list.ensureTotalCapacityPrecise(self.alloc, desired_list);
        try seen.ensureTotalCapacity(self.alloc, std.math.cast(u32, desired_map) orelse return error.Overflow);
        try self.observeTrackedBytes(previous +|
            @as(u64, list.capacity - prior_list) * @sizeOf([]const u8) +|
            @as(u64, seen.capacity() - prior_map) * @sizeOf([]const u8));
    }

    fn appendKeys(self: *@This(), list: *std.ArrayListUnmanaged([]const u8), seen: *std.StringHashMapUnmanaged(void), keys: []const []const u8, comptime kind: ?db_types.IndexKind) !void {
        if (keys.len >= 16 and keys.len <= 128) try self.reserveKeyMetadata(list, seen, keys, kind);
        for (keys) |key| if (artifactMatches(key, kind)) try self.appendUniqueKey(list, seen, key);
    }

    fn appendRecord(self: *@This(), record: change_journal_mod.Record) !void {
        const starts: [4]usize = if (self.minimal_key_blocks)
            .{ self.changed_doc_keys.items.len, self.deleted_doc_keys.items.len, self.overwritten_doc_keys.items.len, self.changed_artifact_keys.items.len }
        else
            undefined;
        self.item_count +|= recordItemCountForIndex(record, self.index_ref.kind);
        switch (self.index_ref.kind) {
            .full_text, .algebraic => {
                try self.appendKeys(&self.changed_doc_keys, &self.seen_changed_docs, record.changed_doc_keys, null);
                try self.appendKeys(&self.deleted_doc_keys, &self.seen_deleted_docs, record.deleted_doc_keys, null);
                try self.appendKeys(&self.overwritten_doc_keys, &self.seen_overwritten_docs, record.overwritten_doc_keys, null);
            },
            .dense_vector, .sparse_vector => {
                try self.appendKeys(&self.changed_doc_keys, &self.seen_changed_docs, record.changed_doc_keys, null);
                try self.appendKeys(&self.deleted_doc_keys, &self.seen_deleted_docs, record.deleted_doc_keys, null);
                try self.appendKeys(&self.overwritten_doc_keys, &self.seen_overwritten_docs, record.overwritten_doc_keys, null);
                try self.appendKeys(&self.changed_artifact_keys, &self.seen_changed_artifacts, record.changed_artifact_keys, .dense_vector);
            },
            .graph => {
                try self.appendKeys(&self.deleted_doc_keys, &self.seen_deleted_docs, record.deleted_doc_keys, null);
                try self.appendKeys(&self.changed_artifact_keys, &self.seen_changed_artifacts, record.changed_artifact_keys, .graph);
            },
        }
        if (self.minimal_key_blocks) try self.compactRecordKeys(starts);
        if (self.index_ref.kind == .dense_vector) {
            const vector_bytes = @as(u64, @intCast(countEmbeddingArtifactKeys(record.changed_artifact_keys))) * self.estimatedDenseVectorBytes();
            try self.observeTrackedBytes(self.tracked_bytes +| vector_bytes);
        }
    }

    fn wouldOverflowWithRecord(self: *@This(), record: change_journal_mod.Record) bool {
        if (self.tracked_bytes == 0 and self.item_count == 0) return false;
        if (self.max_chunk_bytes > 0 and self.tracked_bytes + recordEstimatedBytesForIndex(record, self.index_ref.kind, self.estimatedDenseVectorBytes()) > self.max_chunk_bytes) return true;
        if (self.max_items > 0 and self.item_count + recordItemCountForIndex(record, self.index_ref.kind) > self.max_items) return true;
        if (self.resource_manager) |manager| {
            // The hard limit also covers shared decode scratch and other replay
            // workers. Size the next record against available admission, rather
            // than discovering descriptor/key growth at publication time.
            const available = manager.availableAdmissionBytes(.derived_replay_window);
            if (self.recordAdmissionBytes(record, available) > available) return true;
        }
        return false;
    }

    const KeyAdmission = struct { remaining: usize, preferred: usize, compact: bool, compact_bytes: u64 = 0 };

    fn keyAdmissionBytes(
        list: *const std.ArrayListUnmanaged([]const u8),
        seen: *const std.StringHashMapUnmanaged(void),
        keys: []const []const u8,
        descriptor_bytes: u64,
        artifact_kind: ?db_types.IndexKind,
        key_space: *KeyAdmission,
    ) u64 {
        var count: u64 = 0;
        var bytes: u64 = 0;
        for (keys) |key| {
            if (key.len == 0 or seen.contains(key)) continue;
            if (!artifactMatches(key, artifact_kind)) continue;
            count +|= 1;
            bytes +|= descriptor_bytes;
            if (key_space.compact) {
                key_space.compact_bytes +|= key.len;
                continue;
            }
            if (key_space.remaining < key.len) {
                const allocation = @max(@as(u64, key_space.preferred), @as(u64, key.len) +| @sizeOf(KeyBlock));
                bytes +|= allocation;
                key_space.remaining = std.math.cast(usize, allocation - @sizeOf(KeyBlock)) orelse return std.math.maxInt(u64);
            }
            key_space.remaining -= key.len;
        }
        if (count == 0) return 0;
        // Duplicate keys within this record may overestimate growth; they never
        // under-admit it. Use a conservative map bound without depending on
        // the standard library's private load-factor/capacity implementation.
        const next_len = std.math.cast(usize, @as(u64, @intCast(list.items.len)) +| count) orelse return std.math.maxInt(u64);
        const list_capacity = if (next_len > list.capacity) std.ArrayListUnmanaged([]const u8).growCapacity(next_len) else list.capacity;
        bytes +|= @as(u64, @intCast(list_capacity - list.capacity)) *| @sizeOf([]const u8);
        const map_capacity = @max(@as(u64, seen.capacity()), @max(@as(u64, 8), (@as(u64, seen.count()) +| count) *| 4));
        bytes +|= (map_capacity - seen.capacity()) *| @sizeOf([]const u8);
        return bytes;
    }

    fn recordAdmissionBytes(self: *const @This(), record: change_journal_mod.Record, headroom: u64) u64 {
        var key_space = KeyAdmission{
            .remaining = if (self.key_blocks) |block| block.allocation_len - @sizeOf(KeyBlock) - block.used else 0,
            .preferred = self.preferredKeyBlockBytes(headroom),
            .compact = self.minimal_key_blocks,
        };
        var bytes: u64 = 0;
        if (self.index_ref.kind != .graph) {
            bytes +|= keyAdmissionBytes(&self.changed_doc_keys, &self.seen_changed_docs, record.changed_doc_keys, @sizeOf(derived_types.DerivedDocument), null, &key_space);
        }
        bytes +|= keyAdmissionBytes(&self.deleted_doc_keys, &self.seen_deleted_docs, record.deleted_doc_keys, 0, null, &key_space);
        if (self.index_ref.kind != .graph) {
            bytes +|= keyAdmissionBytes(&self.overwritten_doc_keys, &self.seen_overwritten_docs, record.overwritten_doc_keys, 0, null, &key_space);
        }
        if (self.index_ref.kind == .graph or self.index_ref.kind == .dense_vector or self.index_ref.kind == .sparse_vector)
            bytes +|= keyAdmissionBytes(&self.changed_artifact_keys, &self.seen_changed_artifacts, record.changed_artifact_keys, 0, self.index_ref.kind, &key_space);
        if (self.index_ref.kind == .dense_vector)
            bytes +|= @as(u64, @intCast(countEmbeddingArtifactKeys(record.changed_artifact_keys))) *| self.estimatedDenseVectorBytes();
        if (key_space.compact_bytes != 0) bytes +|= key_space.compact_bytes +| @sizeOf(KeyBlock);
        return bytes;
    }

    fn estimatedDenseVectorBytes(self: *const @This()) u64 {
        if (self.estimated_dense_vector_bytes > 0) return self.estimated_dense_vector_bytes;
        return dense_replay_estimated_vector_bytes_default;
    }

    fn observeTrackedBytes(self: *@This(), next_tracked_bytes: u64) !void {
        if (self.resource_manager) |manager| {
            try manager.adjustUsage(.derived_replay_window, &self.tracked_bytes, next_tracked_bytes);
            return;
        }
        self.tracked_bytes = next_tracked_bytes;
    }

    fn releaseDeduplication(self: *@This()) !void {
        const bytes = (@as(u64, self.seen_changed_docs.capacity()) + self.seen_deleted_docs.capacity() +
            self.seen_overwritten_docs.capacity() + self.seen_changed_artifacts.capacity()) * @sizeOf([]const u8);
        self.seen_changed_docs.deinit(self.alloc);
        self.seen_deleted_docs.deinit(self.alloc);
        self.seen_overwritten_docs.deinit(self.alloc);
        self.seen_changed_artifacts.deinit(self.alloc);
        self.seen_changed_docs = .empty;
        self.seen_deleted_docs = .empty;
        self.seen_overwritten_docs = .empty;
        self.seen_changed_artifacts = .empty;
        try self.observeTrackedBytes(self.tracked_bytes - bytes);
    }

    /// Borrows keys, target metadata, and document descriptors from this
    /// builder. Apply callbacks must finish using them before returning (or
    /// clone them), just as with the previous per-window owned batch.
    /// Only builder.deinit owns cleanup; never call deinitDerivedBatch here.
    fn finishBorrowed(self: *@This(), sequence: u64) !derived_types.DerivedBatch {
        const count = self.changed_doc_keys.items.len;
        // Deduplication is complete. Release maps and admission before the
        // descriptors are allocated, avoiding overlap with publication/apply.
        try self.releaseDeduplication();
        self.documents = try self.alloc.alloc(derived_types.DerivedDocument, count);
        const targets: []const derived_types.DerivedTargetRef = switch (self.index_ref.kind) {
            .full_text, .algebraic => blk: {
                self.target[0] = .{
                    .kind = if (self.index_ref.kind == .full_text) .full_text else .algebraic,
                    .index_name = self.index_ref.name,
                };
                break :blk &self.target;
            },
            else => &.{},
        };
        for (self.changed_doc_keys.items, self.documents) |key, *doc| {
            doc.* = .{ .key = key, .action = .upsert, .targets = targets };
        }
        // Keys now live in document descriptors; the temporary pointer list
        // has no consumer. Release both its storage and its admitted capacity.
        const key_list_bytes = @as(u64, self.changed_doc_keys.capacity) * @sizeOf([]const u8);
        self.changed_doc_keys.deinit(self.alloc);
        self.changed_doc_keys = .empty;
        try self.observeTrackedBytes(self.tracked_bytes - key_list_bytes);
        return .{
            .sequence = sequence,
            .documents = self.documents,
            .deleted_keys = self.deleted_doc_keys.items,
            .overwritten_doc_keys = self.overwritten_doc_keys.items,
            .changed_artifact_keys = self.changed_artifact_keys.items,
        };
    }
};

fn replayChunkConsumeRecord(ctx: *anyopaque, sequence: u64, payload: []const u8) !void {
    const builder: *ReplayChunkBuilder = @ptrCast(@alignCast(ctx));
    if (builder.target_sequence != 0 and sequence > builder.target_sequence) return replay_source_mod.StopReplayChunk.StopReplayChunk;
    // A full window does not need to allocate descriptors for a lookahead
    // record. StopReplayChunk leaves that record at the cursor for the retry.
    if (builder.max_items != 0 and builder.item_count >= builder.max_items)
        return replay_source_mod.StopReplayChunk.StopReplayChunk;
    if (change_journal_mod.looksLikeBinaryRecord(payload)) {
        const denial_generation = if (builder.scratch_budget) |budget| budget.denialGeneration() else 0;
        const record = change_journal_mod.decodeBinaryRecordBorrowedScratchSelected(builder.scratch_alloc, payload, builder.decode_scratch, .{
            .changed_doc_keys = builder.index_ref.kind != .graph,
            .overwritten_doc_keys = builder.index_ref.kind != .graph,
            .changed_artifact_keys = builder.index_ref.kind != .full_text and builder.index_ref.kind != .algebraic,
        }) catch |err| {
            if (err == error.OutOfMemory) {
                if (builder.scratch_budget) |budget| {
                    if (budget.denialGeneration() != denial_generation) {
                        // Partial lookahead scratch is disposable. Release it
                        // before applying the already collected window; its
                        // keys are owned independently by the builder.
                        builder.decode_scratch.trimRetainedCapacity(builder.scratch_alloc, 0);
                        _ = budget.releaseUnusedCredit();
                        if (builder.item_count != 0) return replay_source_mod.StopReplayChunk.StopReplayChunk;
                        return error.ResourceBudgetExceeded;
                    }
                }
            }
            return err;
        };
        // Scratch and the builder share the slice. Amortized spare credit
        // must not crowd out keys/descriptors, especially when only the
        // aggregate budget is configured. Retained buffers stay charged.
        if (builder.scratch_budget) |budget| _ = budget.releaseUnusedCredit();
        if (builder.wouldOverflowWithRecord(record)) return replay_source_mod.StopReplayChunk.StopReplayChunk;
        try builder.appendRecord(record);
        if (!builder.has_accepted_work) builder.has_accepted_work = builder.hasOutput();
        builder.accepted_records += 1;
        return;
    }

    var record = try change_journal_mod.decodeRecord(builder.alloc, payload);
    defer record.deinit();
    if (builder.wouldOverflowWithRecord(record.record)) return replay_source_mod.StopReplayChunk.StopReplayChunk;
    try builder.appendRecord(record.record);
    if (!builder.has_accepted_work) builder.has_accepted_work = builder.hasOutput();
    builder.accepted_records += 1;
}

fn recordEstimatedBytesForIndex(record: change_journal_mod.Record, kind: db_types.IndexKind, estimated_dense_vector_bytes: u64) u64 {
    var total = estimatedStringListBytes(record.changed_doc_keys) +
        estimatedStringListBytes(record.deleted_doc_keys) +
        estimatedStringListBytes(record.overwritten_doc_keys) +
        estimatedStringListBytes(record.changed_artifact_keys);
    if (kind != .graph)
        total +|= @as(u64, @intCast(record.changed_doc_keys.len)) *| @sizeOf(derived_types.DerivedDocument);
    if (kind == .dense_vector) {
        total +|= @as(u64, @intCast(countEmbeddingArtifactKeys(record.changed_artifact_keys))) * estimated_dense_vector_bytes;
    }
    return total;
}

fn recordItemCountForIndex(record: change_journal_mod.Record, kind: db_types.IndexKind) usize {
    return switch (kind) {
        .full_text, .algebraic => record.changed_doc_keys.len + record.deleted_doc_keys.len + record.overwritten_doc_keys.len,
        .dense_vector, .sparse_vector => record.changed_doc_keys.len + record.deleted_doc_keys.len + record.overwritten_doc_keys.len + countEmbeddingArtifactKeys(record.changed_artifact_keys),
        .graph => record.deleted_doc_keys.len + countGraphArtifactKeys(record.changed_artifact_keys),
    };
}

fn countEmbeddingArtifactKeys(keys: []const []const u8) usize {
    var count: usize = 0;
    for (keys) |key| {
        if (internal_keys.isEmbeddingArtifactKey(key) or internal_keys.isDerivedEmbeddingArtifactKey(key)) count += 1;
    }
    return count;
}

fn countGraphArtifactKeys(keys: []const []const u8) usize {
    var count: usize = 0;
    for (keys) |key| {
        if (internal_keys.isGraphEdgeArtifactKey(key) or
            internal_keys.isAssetArtifactKey(key) or
            internal_keys.isChunkArtifactRecordKey(key) or
            internal_keys.isResolutionArtifactKey(key))
        {
            count += 1;
        }
    }
    return count;
}

fn estimatedStringListBytes(values: []const []const u8) u64 {
    var total: u64 = @as(u64, @intCast(values.len)) * @sizeOf([]const u8);
    for (values) |value| total +|= value.len;
    return total;
}

const TestApplyCapture = struct {
    alloc: Allocator,
    call_count: usize = 0,
    applied_documents: usize = 0,
    applied_deleted_keys: usize = 0,
    applied_overwritten_doc_keys: usize = 0,
    applied_changed_artifact_keys: usize = 0,
    applied_dense_embeddings: usize = 0,
    applied_sparse_embeddings: usize = 0,
    applied_graph_doc_clears: usize = 0,
    applied_graph_writes: usize = 0,
    applied_graph_deletes: usize = 0,
    last_sequence: u64 = 0,
    sequences: std.ArrayListUnmanaged(u64) = .empty,
    last_batch: ?derived_types.DerivedBatch = null,

    pub fn deinit(self: *TestApplyCapture) void {
        if (self.last_batch) |*batch| derived_types.deinitDerivedBatch(self.alloc, batch);
        self.sequences.deinit(self.alloc);
        self.* = undefined;
    }
};

const TestWindowHooks = struct {
    begin_calls: usize = 0,
    finish_calls: usize = 0,
    successful_finishes: usize = 0,
};

const TestCatchUpHooks = struct {
    begin_calls: usize = 0,
    finish_calls: usize = 0,
    successful_finishes: usize = 0,
};

const TestPersistOrderHooks = struct {
    alloc: Allocator,
    order: std.ArrayListUnmanaged(u8) = .empty,

    pub fn deinit(self: *@This()) void {
        self.order.deinit(self.alloc);
        self.* = undefined;
    }
};

fn testApplyCapture(ctx: *anyopaque, batch: derived_types.DerivedBatch, _: index_manager_mod.ManagedIndexRef) !bool {
    const capture: *TestApplyCapture = @ptrCast(@alignCast(ctx));
    if (capture.last_batch) |*existing| derived_types.deinitDerivedBatch(capture.alloc, existing);
    capture.last_batch = try derived_types.cloneBatch(capture.alloc, batch);
    capture.call_count += 1;
    capture.applied_documents += batch.documents.len;
    capture.applied_deleted_keys += batch.deleted_keys.len;
    capture.applied_overwritten_doc_keys += batch.overwritten_doc_keys.len;
    capture.applied_changed_artifact_keys += batch.changed_artifact_keys.len;
    capture.applied_dense_embeddings += batch.dense_embeddings.len;
    capture.applied_sparse_embeddings += batch.sparse_embeddings.len;
    capture.applied_graph_doc_clears += batch.graph_doc_clears.len;
    capture.applied_graph_writes += batch.graph_writes.len;
    capture.applied_graph_deletes += batch.graph_deletes.len;
    capture.last_sequence = batch.sequence;
    try capture.sequences.append(capture.alloc, batch.sequence);
    return batch.documents.len > 0 or
        batch.deleted_keys.len > 0 or
        batch.overwritten_doc_keys.len > 0 or
        batch.changed_artifact_keys.len > 0 or
        batch.dense_embeddings.len > 0 or
        batch.sparse_embeddings.len > 0 or
        batch.graph_doc_clears.len > 0 or
        batch.graph_writes.len > 0 or
        batch.graph_deletes.len > 0;
}

fn testBeginWindowHook(ctx: *anyopaque, _: index_manager_mod.ManagedIndexRef) !void {
    const hooks: *TestWindowHooks = @ptrCast(@alignCast(ctx));
    hooks.begin_calls += 1;
}

fn testFinishWindowHook(ctx: *anyopaque, _: index_manager_mod.ManagedIndexRef, success: bool) !void {
    const hooks: *TestWindowHooks = @ptrCast(@alignCast(ctx));
    hooks.finish_calls += 1;
    if (success) hooks.successful_finishes += 1;
}

fn testBeginCatchUpHook(ctx: *anyopaque, _: index_manager_mod.ManagedIndexRef) !void {
    const hooks: *TestCatchUpHooks = @ptrCast(@alignCast(ctx));
    hooks.begin_calls += 1;
}

fn testFinishCatchUpHook(ctx: *anyopaque, _: index_manager_mod.ManagedIndexRef, success: bool) !void {
    const hooks: *TestCatchUpHooks = @ptrCast(@alignCast(ctx));
    hooks.finish_calls += 1;
    if (success) hooks.successful_finishes += 1;
}

fn testFinishWindowOrderHook(ctx: *anyopaque, _: index_manager_mod.ManagedIndexRef, success: bool) !void {
    const hooks: *TestPersistOrderHooks = @ptrCast(@alignCast(ctx));
    try hooks.order.append(hooks.alloc, if (success) 'f' else 'F');
}

fn testPersistProgressOrderHook(ctx: *anyopaque, _: []const u8, _: u64) !void {
    const hooks: *TestPersistOrderHooks = @ptrCast(@alignCast(ctx));
    try hooks.order.append(hooks.alloc, 'p');
}

fn appendChangeJournalRecord(log: *change_journal_mod.Journal, alloc: Allocator, record: change_journal_mod.Record) !void {
    const payload = try change_journal_mod.encodeRecord(alloc, record);
    defer alloc.free(payload);
    _ = try log.appendOpaque(payload);
}

fn appendReplayStreamRecord(
    store: *docstore_mod.DocStore,
    alloc: Allocator,
    sequence: u64,
    record: change_journal_mod.Record,
) !void {
    const payload = try change_journal_mod.encodeRecord(alloc, record);
    defer alloc.free(payload);
    try store.appendReplayOpaque(alloc, sequence, payload);
}

fn testInMemoryJournalOpenOptions() change_journal_mod.OpenOptions {
    return .{
        .backend = .lsm_memory,
        .lsm_options = .{
            .flush_threshold = 512,
            .compact_threshold_runs = 256,
            .wal_enabled = false,
            .obsolete_retention_ns = 0,
        },
    };
}

test "catchUpIndex batches dense replay records before applying" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-batched-log", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeSentinel(u8, path, 0);
    defer alloc.free(path_z);
    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-batched-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    const artifact_a = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(artifact_a);
    const artifact_b = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:b", "dv_v1");
    defer alloc.free(artifact_b);
    const artifact_c = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:c", "dv_v1");
    defer alloc.free(artifact_c);

    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .changed_artifact_keys = &.{artifact_a},
        .target_hints = &.{.dense_vector},
    });
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:b"},
        .changed_artifact_keys = &.{artifact_b},
        .target_hints = &.{.dense_vector},
    });
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 3,
        .changed_doc_keys = &.{"doc:c"},
        .changed_artifact_keys = &.{artifact_c},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();

    const stats = try catchUpIndex(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        .{ .name = "dv_v1", .kind = .dense_vector },
        0,
        null,
        &capture,
        testApplyCapture,
        null,
        null,
    );

    try std.testing.expectEqual(@as(usize, 3), stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 1), stats.applied_entries);
    try std.testing.expectEqual(@as(u64, 3), stats.last_sequence);
    try std.testing.expectEqual(@as(usize, 1), capture.call_count);
    try std.testing.expectEqual(@as(usize, 3), capture.applied_changed_artifact_keys);
    try std.testing.expectEqual(@as(usize, 3), capture.applied_documents);
    try std.testing.expectEqual(@as(u64, 3), capture.last_sequence);
}

test "catchUpIndex batches replay-stream records and respects from_sequence" {
    const alloc = std.testing.allocator;

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const artifact_a = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(artifact_a);
    const artifact_b = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:b", "dv_v1");
    defer alloc.free(artifact_b);
    const artifact_c = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:c", "dv_v1");
    defer alloc.free(artifact_c);

    try appendReplayStreamRecord(&store, alloc, 1, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .changed_artifact_keys = &.{artifact_a},
        .target_hints = &.{.dense_vector},
    });
    try appendReplayStreamRecord(&store, alloc, 2, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:b"},
        .changed_artifact_keys = &.{artifact_b},
        .target_hints = &.{.dense_vector},
    });
    try appendReplayStreamRecord(&store, alloc, 3, .{
        .sequence = 3,
        .changed_doc_keys = &.{"doc:c"},
        .changed_artifact_keys = &.{artifact_c},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();

    const stats = try catchUpIndex(
        alloc,
        replay_source_mod.Source.fromPrimaryStore(&store, null, null),
        .{ .name = "dv_v1", .kind = .dense_vector },
        1,
        null,
        &capture,
        testApplyCapture,
        null,
        null,
    );

    try std.testing.expectEqual(@as(usize, 2), stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 1), stats.applied_entries);
    try std.testing.expectEqual(@as(u64, 3), stats.last_sequence);
    try std.testing.expectEqual(@as(usize, 1), capture.call_count);
    try std.testing.expectEqual(@as(usize, 2), capture.applied_documents);
    try std.testing.expectEqual(@as(usize, 2), capture.applied_changed_artifact_keys);
    try std.testing.expectEqual(@as(u64, 3), capture.last_sequence);
    const last = capture.last_batch.?;
    try std.testing.expectEqual(@as(usize, 2), last.documents.len);
    try std.testing.expectEqualStrings("doc:b", last.documents[0].key);
    try std.testing.expectEqualStrings("doc:c", last.documents[1].key);
}

test "catchUpIndex window hooks fire once per replay window" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-window-hooks-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:b"},
        .target_hints = &.{.dense_vector},
    });
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 3,
        .changed_doc_keys = &.{"doc:c"},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    var hooks = TestWindowHooks{};

    const stats = try catchUpIndexWithOptions(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        .{ .name = "dv_v1", .kind = .dense_vector },
        0,
        &capture,
        testApplyCapture,
        .{
            .window_ctx = &hooks,
            .begin_window_fn = testBeginWindowHook,
            .finish_window_fn = testFinishWindowHook,
            .max_records_per_window = 2,
        },
    );

    try std.testing.expectEqual(@as(usize, 3), stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 2), capture.call_count);
    try std.testing.expectEqual(@as(usize, 2), hooks.begin_calls);
    try std.testing.expectEqual(@as(usize, 2), hooks.finish_calls);
    try std.testing.expectEqual(@as(usize, 2), hooks.successful_finishes);
}

test "catchUpIndex refuses to open an apply window after its deadline" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-expired-deadline-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);
    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    try std.testing.expectError(error.CatchUpDeadlineExceeded, catchUpIndexWithOptions(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        .{ .name = "dv_v1", .kind = .dense_vector },
        0,
        &capture,
        testApplyCapture,
        .{ .deadline_ns = 1 },
    ));
    try std.testing.expectEqual(@as(usize, 0), capture.call_count);
}

test "catchUpIndex can stop after bounded replay windows" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-window-limit-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:b"},
        .target_hints = &.{.dense_vector},
    });
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 3,
        .changed_doc_keys = &.{"doc:c"},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    var hooks = TestWindowHooks{};

    const stats = try catchUpIndexWithOptions(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        .{ .name = "dv_v1", .kind = .dense_vector },
        0,
        &capture,
        testApplyCapture,
        .{
            .window_ctx = &hooks,
            .begin_window_fn = testBeginWindowHook,
            .finish_window_fn = testFinishWindowHook,
            .max_records_per_window = 2,
            .max_windows_per_call = 1,
        },
    );

    try std.testing.expectEqual(@as(usize, 2), stats.scanned_entries);
    try std.testing.expectEqual(@as(u64, 2), stats.last_sequence);
    try std.testing.expectEqual(@as(usize, 1), capture.call_count);
    try std.testing.expectEqual(@as(usize, 1), hooks.begin_calls);
    try std.testing.expectEqual(@as(usize, 1), hooks.finish_calls);
    try std.testing.expectEqual(@as(usize, 1), hooks.successful_finishes);
}

test "coalesced replay byte and time quanta stop only after a complete record" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(alloc, ".zig-cache/tmp/{s}/coalesced-quanta", .{tmp.sub_path}, 0);
    defer alloc.free(path);
    var journal = try change_journal_mod.Journal.open(path, testInMemoryJournalOpenOptions());
    defer journal.close();
    for (1..4) |sequence| try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = sequence,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });
    for ([_]CatchUpOptions{
        .{ .max_records_per_window = 1, .max_call_bytes = 1 },
        .{ .max_records_per_window = 1, .max_call_ns = 1 },
    }) |options| {
        var capture = TestApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        var sequence: u64 = 0;
        for (1..4) |expected| {
            const stats = try catchUpIndexWithOptions(alloc, replay_source_mod.Source.fromJournal(&journal), .{ .name = "dv_v1", .kind = .dense_vector }, sequence, &capture, testApplyCapture, options);
            try std.testing.expectEqual(@as(u64, expected), stats.last_sequence);
            try std.testing.expectEqual(@as(usize, 1), stats.scanned_entries);
            sequence = stats.last_sequence;
        }
    }
}

test "catchUpIndex catch-up hooks fire once per replay run" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-catch-up-hooks-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:b"},
        .target_hints = &.{.dense_vector},
    });
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 3,
        .changed_doc_keys = &.{"doc:c"},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    var hooks = TestCatchUpHooks{};

    const stats = try catchUpIndexWithOptions(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        .{ .name = "dv_v1", .kind = .dense_vector },
        0,
        &capture,
        testApplyCapture,
        .{
            .catch_up_ctx = &hooks,
            .begin_catch_up_fn = testBeginCatchUpHook,
            .finish_catch_up_fn = testFinishCatchUpHook,
            .max_records_per_window = 2,
        },
    );

    try std.testing.expectEqual(@as(usize, 3), stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 2), capture.call_count);
    try std.testing.expectEqual(@as(usize, 1), hooks.begin_calls);
    try std.testing.expectEqual(@as(usize, 1), hooks.finish_calls);
    try std.testing.expectEqual(@as(usize, 1), hooks.successful_finishes);
}

test "catchUpIndex persists replay progress after finishing replay window" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-persist-order-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    var hooks = TestPersistOrderHooks{ .alloc = alloc };
    defer hooks.deinit();

    const stats = try catchUpIndexWithOptions(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        .{ .name = "dv_v1", .kind = .dense_vector },
        0,
        &capture,
        testApplyCapture,
        .{
            .window_ctx = &hooks,
            .finish_window_fn = testFinishWindowOrderHook,
            .persist_ctx = &hooks,
            .persist_progress_fn = testPersistProgressOrderHook,
        },
    );

    try std.testing.expectEqual(@as(usize, 1), stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 1), stats.applied_entries);
    try std.testing.expectEqualStrings("fp", hooks.order.items);
}

test "catchUpIndex removes pending chunk dense vectors by parent document" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-dense-parent-log", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeSentinel(u8, path, 0);
    defer alloc.free(path_z);
    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-dense-parent-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    const chunk_key = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 2);
    defer alloc.free(chunk_key);
    const artifact_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "dv_v1");
    defer alloc.free(artifact_key);

    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 1,
        .overwritten_doc_keys = &.{"doc:a"},
        .changed_doc_keys = &.{"doc:a"},
        .changed_artifact_keys = &.{artifact_key},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();

    const stats = try catchUpIndex(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        .{ .name = "dv_v1", .kind = .dense_vector },
        0,
        null,
        &capture,
        testApplyCapture,
        null,
        null,
    );

    try std.testing.expectEqual(@as(usize, 1), stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 1), stats.applied_entries);
    try std.testing.expectEqual(@as(usize, 1), capture.call_count);
    try std.testing.expectEqual(@as(usize, 1), capture.applied_overwritten_doc_keys);
    try std.testing.expectEqual(@as(usize, 1), capture.applied_changed_artifact_keys);
    const last = capture.last_batch.?;
    try std.testing.expectEqualStrings("doc:a", last.overwritten_doc_keys[0]);
    try std.testing.expectEqualStrings(artifact_key, last.changed_artifact_keys[0]);
}

test "catchUpIndex chunks large replay windows" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-chunked-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    const record_count = 4;
    var i: usize = 0;
    while (i < record_count) : (i += 1) {
        const doc_key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
        defer alloc.free(doc_key);
        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, doc_key, "dv_v1");
        defer alloc.free(artifact_key);
        try appendChangeJournalRecord(&journal, alloc, .{
            .sequence = @intCast(i + 1),
            .changed_doc_keys = &.{doc_key},
            .changed_artifact_keys = &.{artifact_key},
            .target_hints = &.{.dense_vector},
        });
    }

    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();

    const stats = try catchUpIndexWithOptions(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        .{ .name = "dv_v1", .kind = .dense_vector },
        0,
        &capture,
        testApplyCapture,
        .{ .max_records_per_window = 3 },
    );

    try std.testing.expectEqual(record_count, stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 2), stats.applied_entries);
    try std.testing.expectEqual(@as(usize, 2), capture.call_count);
    try std.testing.expectEqual(record_count, capture.applied_documents);
    try std.testing.expectEqual(record_count, capture.applied_changed_artifact_keys);
}

test "catchUpIndex chunks replay by byte budget" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-byte-chunked-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    const artifact_a = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(artifact_a);
    const artifact_b = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:b", "dv_v1");
    defer alloc.free(artifact_b);

    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .changed_artifact_keys = &.{artifact_a},
        .target_hints = &.{.dense_vector},
    });
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:b"},
        .changed_artifact_keys = &.{artifact_b},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();

    const stats = try catchUpIndexWithOptions(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        .{ .name = "dv_v1", .kind = .dense_vector },
        0,
        &capture,
        testApplyCapture,
        .{ .max_chunk_bytes = 1 },
    );

    try std.testing.expectEqual(@as(usize, 2), stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 2), stats.applied_entries);
    try std.testing.expectEqual(@as(usize, 2), capture.call_count);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2 }, capture.sequences.items);
}

test "catchUpIndex chunks dense replay by item budget" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-item-chunked-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    const artifact_a = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(artifact_a);
    const artifact_b = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:b", "dv_v1");
    defer alloc.free(artifact_b);

    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .changed_artifact_keys = &.{artifact_a},
        .target_hints = &.{.dense_vector},
    });
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:b"},
        .changed_artifact_keys = &.{artifact_b},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();

    const stats = try catchUpIndexWithOptions(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        .{ .name = "dv_v1", .kind = .dense_vector },
        0,
        &capture,
        testApplyCapture,
        .{ .max_items_per_window = 2 },
    );

    try std.testing.expectEqual(@as(usize, 2), stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 2), stats.applied_entries);
    try std.testing.expectEqual(@as(usize, 2), capture.call_count);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2 }, capture.sequences.items);
}

test "catchUpIndex subchunks one oversized full text record before advancing its sequence" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-full-text-item-chunked-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{ "doc:a", "doc:b", "doc:c", "doc:d", "doc:e" },
        .target_hints = &.{.full_text},
    });

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.derived_replay_window)] = .{ .hard_limit_bytes = 2048 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);
    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    const stats = try catchUpIndexWithOptions(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        .{ .name = "text", .kind = .full_text },
        0,
        &capture,
        testApplyCapture,
        .{ .max_items_per_window = 2, .max_chunk_bytes = 512, .resource_manager = &manager },
    );

    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expect(manager.snapshot().memory.peak_bytes <= 2048);
    try std.testing.expectEqual(@as(usize, 1), stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 1), stats.applied_entries);
    try std.testing.expectEqual(@as(usize, 3), capture.call_count);
    try std.testing.expectEqual(@as(usize, 5), capture.applied_documents);
    try std.testing.expectEqualSlices(u64, &.{ 1, 1, 1 }, capture.sequences.items);
}

test "catchUpIndex chunks dense replay by estimated vector byte budget" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-dense-vector-byte-chunked-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    const artifact_a = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(artifact_a);
    const artifact_b = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:b", "dv_v1");
    defer alloc.free(artifact_b);

    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .changed_artifact_keys = &.{artifact_a},
        .target_hints = &.{.dense_vector},
    });
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:b"},
        .changed_artifact_keys = &.{artifact_b},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();

    const estimated_vector_bytes = 1024 * 1024;
    const stats = try catchUpIndexWithOptions(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        .{ .name = "dv_v1", .kind = .dense_vector },
        0,
        &capture,
        testApplyCapture,
        .{
            .max_chunk_bytes = estimated_vector_bytes + 4096,
            .estimated_dense_vector_bytes = estimated_vector_bytes,
        },
    );

    try std.testing.expectEqual(@as(usize, 2), stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 2), stats.applied_entries);
    try std.testing.expectEqual(@as(usize, 2), capture.call_count);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2 }, capture.sequences.items);
}

test "catchUpIndex batches full-text replay records before applying" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-unbatched-log", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeSentinel(u8, path, 0);
    defer alloc.free(path_z);
    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-unbatched-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .changed_artifact_keys = &.{"not-used-by-full-text"},
        .target_hints = &.{.full_text},
    });
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:b"},
        .target_hints = &.{.full_text},
    });
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 3,
        .changed_doc_keys = &.{"doc:c"},
        .target_hints = &.{.full_text},
    });

    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();

    const stats = try catchUpIndex(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        .{ .name = "ft_v1", .kind = .full_text },
        0,
        null,
        &capture,
        testApplyCapture,
        null,
        null,
    );

    try std.testing.expectEqual(@as(usize, 3), stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 1), stats.applied_entries);
    try std.testing.expectEqual(@as(u64, 3), stats.last_sequence);
    try std.testing.expectEqual(@as(usize, 1), capture.call_count);
    try std.testing.expectEqual(@as(usize, 3), capture.applied_documents);
    try std.testing.expectEqual(@as(usize, 0), capture.applied_changed_artifact_keys);
    try std.testing.expectEqual(@as(u64, 3), capture.last_sequence);
}

test "catchUpIndex batches sparse replay records before applying" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-sparse-batched-log", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeSentinel(u8, path, 0);
    defer alloc.free(path_z);
    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-sparse-batched-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    const artifact_a = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "sv_v1");
    defer alloc.free(artifact_a);
    const artifact_b = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:b", "sv_v1");
    defer alloc.free(artifact_b);

    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .changed_artifact_keys = &.{artifact_a},
        .target_hints = &.{.sparse_vector},
    });
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:b"},
        .changed_artifact_keys = &.{artifact_b},
        .target_hints = &.{.sparse_vector},
    });

    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();

    const stats = try catchUpIndex(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        .{ .name = "sv_v1", .kind = .sparse_vector },
        0,
        null,
        &capture,
        testApplyCapture,
        null,
        null,
    );

    try std.testing.expectEqual(@as(usize, 2), stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 1), stats.applied_entries);
    try std.testing.expectEqual(@as(usize, 1), capture.call_count);
    try std.testing.expectEqual(@as(usize, 2), capture.applied_changed_artifact_keys);
    try std.testing.expectEqual(@as(usize, 2), capture.applied_documents);
}

test "catchUpIndex batches graph artifact journal records before applying" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-graph-journal-log", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeSentinel(u8, path, 0);
    defer alloc.free(path_z);
    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-graph-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "graph_v1", "links", "doc:b");
    defer alloc.free(artifact_key);
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 7,
        .changed_doc_keys = &.{"doc:a"},
        .changed_artifact_keys = &.{artifact_key},
        .target_hints = &.{.graph},
    });

    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();

    const stats = try catchUpIndex(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        .{ .name = "graph_v1", .kind = .graph },
        0,
        null,
        &capture,
        testApplyCapture,
        null,
        null,
    );

    try std.testing.expectEqual(@as(usize, 1), stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 1), stats.applied_entries);
    try std.testing.expectEqual(@as(u64, 1), stats.last_sequence);
    try std.testing.expectEqual(@as(usize, 1), capture.call_count);
    try std.testing.expectEqual(@as(usize, 1), capture.applied_changed_artifact_keys);
    try std.testing.expectEqual(@as(usize, 0), capture.applied_graph_writes);
    try std.testing.expectEqualStrings(artifact_key, capture.last_batch.?.changed_artifact_keys[0]);
}

test "catchUpIndex batches resolution artifact graph journal records before applying" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-resolution-graph-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    const artifact_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(artifact_key);
    try appendChangeJournalRecord(&journal, alloc, .{
        .sequence = 7,
        .changed_artifact_keys = &.{artifact_key},
        .target_hints = &.{.graph},
    });

    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();

    const stats = try catchUpIndex(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        .{ .name = "graph_v1", .kind = .graph },
        0,
        null,
        &capture,
        testApplyCapture,
        null,
        null,
    );

    try std.testing.expectEqual(@as(usize, 1), stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 1), stats.applied_entries);
    try std.testing.expectEqual(@as(usize, 1), capture.applied_changed_artifact_keys);
    try std.testing.expectEqualStrings(artifact_key, capture.last_batch.?.changed_artifact_keys[0]);
}

test "replay batch borrows shared text and algebraic targets and clones own their data" {
    const alloc = std.testing.allocator;
    for ([_]db_types.IndexKind{ .full_text, .algebraic, .dense_vector, .sparse_vector, .graph }) |kind| {
        var builder = ReplayChunkBuilder.init(alloc, .{ .name = "projection", .kind = kind }, null, 0);
        var clone: derived_types.DerivedBatch = undefined;
        {
            defer builder.deinit();
            try builder.appendRecord(.{ .changed_doc_keys = &.{ "a", "b", "c" }, .deleted_doc_keys = &.{"deleted"}, .overwritten_doc_keys = &.{"overwritten"} });
            const batch = try builder.finishBorrowed(1);
            const document_count: usize = if (kind == .graph) 0 else 3;
            try std.testing.expectEqual(document_count, batch.documents.len);
            if (kind == .full_text or kind == .algebraic) {
                try std.testing.expectEqual(batch.documents[0].targets.ptr, batch.documents[1].targets.ptr);
                try std.testing.expectEqual(builder.index_ref.name.ptr, batch.documents[0].targets[0].index_name.ptr);
                try std.testing.expectEqual(if (kind == .full_text) derived_types.DerivedTarget.full_text else derived_types.DerivedTarget.algebraic, batch.documents[0].targets[0].kind);
            } else {
                for (batch.documents) |doc| try std.testing.expectEqual(@as(usize, 0), doc.targets.len);
            }
            clone = try derived_types.cloneBatch(alloc, batch);
        }
        defer derived_types.deinitDerivedBatch(alloc, &clone);
        if (kind != .graph) try std.testing.expectEqualStrings("c", clone.documents[2].key);
        if (kind == .full_text or kind == .algebraic) try std.testing.expectEqualStrings("projection", clone.documents[2].targets[0].index_name);
        try std.testing.expectEqualStrings("deleted", clone.deleted_keys[0]);
        try std.testing.expectEqual(@as(usize, if (kind == .graph) 0 else 1), clone.overwritten_doc_keys.len);
    }
}

fn replayBatchAllocationFailure(alloc: Allocator, kind: db_types.IndexKind, compact: bool) !void {
    var builder = ReplayChunkBuilder.init(alloc, .{ .name = "projection", .kind = kind }, null, 0);
    defer builder.deinit();
    builder.minimal_key_blocks = compact;
    try builder.appendRecord(.{ .changed_doc_keys = &.{ "a", "b", "c" }, .deleted_doc_keys = &.{"deleted"}, .overwritten_doc_keys = &.{"overwritten"}, .changed_artifact_keys = &.{ "\x01doc\x00\x00\x20embedding\x00\x00index\x00\x00", "\x01doc\x00\x00\x20graph\x00\x00index\x00\x00\x32edge\x00\x00target\x00\x00" } });
    _ = try builder.finishBorrowed(1);
}

test "replay batch cleans up every allocation failure before borrowed publication" {
    for ([_]db_types.IndexKind{ .full_text, .algebraic, .dense_vector, .sparse_vector, .graph }) |kind| {
        for ([_]bool{ false, true }) |compact| try std.testing.checkAllAllocationFailures(std.testing.allocator, replayBatchAllocationFailure, .{ kind, compact });
    }
}

test "replay batch budget denial rolls back key ownership" {
    const alloc = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.derived_replay_window)] = .{ .soft_limit_bytes = 1, .hard_limit_bytes = 1 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);
    {
        var builder = ReplayChunkBuilder.init(alloc, .{ .name = "text", .kind = .full_text }, &manager, 0);
        defer builder.deinit();
        try std.testing.expectError(error.ResourceBudgetExceeded, builder.appendRecord(.{ .changed_doc_keys = &.{"a"} }));
        try std.testing.expectEqual(@as(usize, 0), builder.changed_doc_keys.items.len);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "catchUpIndex accounts shared decoding scratch and releases it after apply failure" {
    const alloc = std.testing.allocator;
    var log = try change_journal_mod.Journal.open("budgeted-replay-scratch", testInMemoryJournalOpenOptions());
    defer log.close();
    try appendChangeJournalRecord(&log, alloc, .{ .sequence = 1, .changed_doc_keys = &.{"a"}, .target_hints = &.{.full_text} });
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(alloc);
    const Consumer = struct {
        fn apply(ctx: *anyopaque, _: derived_types.DerivedBatch, _: index_manager_mod.ManagedIndexRef) !bool {
            const resources: *resource_manager_mod.ResourceManager = @ptrCast(@alignCast(ctx));
            try std.testing.expect(resources.sliceStats(.derived_replay_window).used_bytes > 0);
            return error.InjectedApplyFailure;
        }
    };
    @import("../../../test_error_logs.zig").expectErrorLogs(2);
    try std.testing.expectError(error.InjectedApplyFailure, catchUpIndexWithOptions(alloc, replay_source_mod.Source.fromJournal(&log), .{ .name = "text", .kind = .full_text }, 0, &manager, Consumer.apply, .{ .resource_manager = &manager }));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
}

test "catchUpIndex admits document descriptors during collection and progresses under hard limits" {
    const alloc = std.testing.allocator;
    for ([_]db_types.IndexKind{ .full_text, .algebraic, .dense_vector, .sparse_vector }) |kind| {
        for ([_]bool{ false, true }) |aggregate_limited| {
            var budgets = resource_manager_mod.Options.defaultBudgets();
            budgets[@backingInt(resource_manager_mod.Slice.derived_replay_window)] = .{ .soft_limit_bytes = 1000, .hard_limit_bytes = if (aggregate_limited) 0 else 1000 };
            var manager = resource_manager_mod.ResourceManager.init(.{
                .budgets = budgets,
                .memory_budget = .{ .hard_limit_bytes = if (aggregate_limited) 1000 else 0 },
            });
            defer manager.deinit(alloc);
            var log = try change_journal_mod.Journal.open("descriptor-window-admission", testInMemoryJournalOpenOptions());
            defer log.close();
            for ([_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h", "i", "j" }, 1..) |key, sequence|
                try appendChangeJournalRecord(&log, alloc, .{ .sequence = sequence, .changed_doc_keys = &.{key}, .target_hints = &.{targetHintForManagedIndex(.{ .name = "index", .kind = kind })} });
            var capture = TestApplyCapture{ .alloc = alloc };
            defer capture.deinit();
            const stats = try catchUpIndexWithOptions(alloc, replay_source_mod.Source.fromJournal(&log), .{ .name = "index", .kind = kind }, 0, &capture, testApplyCapture, .{ .resource_manager = &manager, .max_chunk_bytes = 1000 });
            try std.testing.expectEqual(@as(usize, 10), capture.applied_documents);
            try std.testing.expectEqual(@as(u64, 10), stats.last_applied_sequence);
            try std.testing.expect(capture.call_count > 1);
            const snapshot = manager.snapshot();
            try std.testing.expect(snapshot.memory.peak_bytes <= 1000);
            try std.testing.expectEqual(@as(u64, 0), snapshot.memory.used_bytes);
            try std.testing.expectEqual(@as(u64, 0), snapshot.memory.accounting_errors);
        }
    }
}

test "catchUpIndex yields collected window on lookahead admission and preserves the cursor" {
    const alloc = std.testing.allocator;
    // Zero exercises admission denial; one exercises the pre-decode item bound.
    for ([_]usize{ 0, 1 }) |max_items| {
        var log = try change_journal_mod.Journal.open("lookahead-window-admission", testInMemoryJournalOpenOptions());
        defer log.close();
        try appendChangeJournalRecord(&log, alloc, .{ .sequence = 1, .changed_doc_keys = &.{"a"}, .target_hints = &.{.full_text} });
        var keys: [200][]const u8 = undefined;
        var names: [200][8]u8 = undefined;
        for (&keys, &names, 0..) |*key, *name, i| key.* = try std.fmt.bufPrint(name, "key{d}", .{i});
        try appendChangeJournalRecord(&log, alloc, .{ .sequence = 2, .changed_doc_keys = &keys, .target_hints = &.{.full_text} });
        var budgets = resource_manager_mod.Options.defaultBudgets();
        budgets[@backingInt(resource_manager_mod.Slice.derived_replay_window)] = .{ .soft_limit_bytes = 2000, .hard_limit_bytes = 2000 };
        var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
        defer manager.deinit(alloc);
        var capture = TestApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        var cursor = try replay_source_mod.Source.fromJournal(&log).openMatchingCursor(alloc, 0, .full_text);
        defer cursor.deinit(alloc);
        const index: index_manager_mod.ManagedIndexRef = .{ .name = "text", .kind = .full_text };
        const first = try catchUpIndexFromMatchingCursor(alloc, &cursor, index, &capture, testApplyCapture, .{ .resource_manager = &manager, .max_items_per_window = max_items, .max_windows_per_call = 1 });
        try std.testing.expectEqual(@as(u64, 1), first.last_applied_sequence);
        try std.testing.expectEqual(@as(usize, 1), capture.applied_documents);
        const snapshot = manager.snapshot();
        try std.testing.expectEqual(@as(u64, 0), snapshot.memory.used_bytes);
        try std.testing.expectEqual(@as(u64, 0), snapshot.memory.accounting_errors);
        if (max_items != 0) try std.testing.expectEqual(@as(u64, 0), snapshot.slices[@backingInt(resource_manager_mod.Slice.derived_replay_window)].hard_limit_rejections);
        // A record that cannot fit even in an empty window remains a hard
        // admission error, without bypassing the limit or consuming its cursor.
        try std.testing.expectError(error.ResourceBudgetExceeded, catchUpIndexFromMatchingCursor(alloc, &cursor, index, &capture, testApplyCapture, .{ .resource_manager = &manager }));
        try std.testing.expectEqual(@as(usize, 1), capture.applied_documents);
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
        // With enough space, the same cursor must replay the deferred record
        // exactly once. Its sequence cannot be consumed by failed lookahead.
        const second = try catchUpIndexFromMatchingCursor(alloc, &cursor, index, &capture, testApplyCapture, .{});
        try std.testing.expectEqual(@as(u64, 2), second.last_applied_sequence);
        try std.testing.expectEqual(@as(usize, 201), capture.applied_documents);
        const end = try catchUpIndexFromMatchingCursor(alloc, &cursor, index, &capture, testApplyCapture, .{});
        try std.testing.expectEqual(@as(usize, 0), end.applied_entries);
    }
}

test "replay batch descriptor admission is deduplicated and finish needs no new credit" {
    const alloc = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.derived_replay_window)] = .{ .hard_limit_bytes = 1000 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);
    var builder = ReplayChunkBuilder.init(alloc, .{ .name = "text", .kind = .full_text }, &manager, 1000);
    defer builder.deinit();
    try builder.appendRecord(.{ .changed_doc_keys = &.{ "a", "b" } });
    const admitted = builder.tracked_bytes;
    try builder.appendRecord(.{ .changed_doc_keys = &.{ "a", "b", "a" } });
    try std.testing.expectEqual(admitted, builder.tracked_bytes);
    const spare = manager.availableAdmissionBytes(.derived_replay_window);
    var other = try manager.reserve(.derived_replay_window, spare);
    defer other.release();
    const released = @as(u64, builder.changed_doc_keys.capacity + builder.seen_changed_docs.capacity()) * @sizeOf([]const u8);
    const batch = try builder.finishBorrowed(1);
    try std.testing.expectEqual(@as(usize, 2), batch.documents.len);
    try std.testing.expectEqual(admitted - released, builder.tracked_bytes);
}

test "catchUpIndex reclaims changing record shapes under a tight hard budget" {
    const alloc = std.testing.allocator;
    var log = try change_journal_mod.Journal.open("fresh-review-retained-scratch", testInMemoryJournalOpenOptions());
    defer log.close();
    var deleted: [20][]const u8 = undefined;
    var names: [20][8]u8 = undefined;
    for (&deleted, &names, 0..) |*key, *name, i| key.* = try std.fmt.bufPrint(name, "d{d:0>3}", .{i});
    try appendChangeJournalRecord(&log, alloc, .{ .sequence = 1, .deleted_doc_keys = &deleted, .target_hints = &.{.full_text} });
    try appendChangeJournalRecord(&log, alloc, .{ .sequence = 2, .changed_doc_keys = &.{ "u0", "u1", "u2", "u3", "u4", "u5", "u6", "u7" }, .target_hints = &.{.full_text} });
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.derived_replay_window)] = .{ .hard_limit_bytes = 2000 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);
    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    var cursor = try replay_source_mod.Source.fromJournal(&log).openMatchingCursor(alloc, 0, .full_text);
    defer cursor.deinit(alloc);
    const index: index_manager_mod.ManagedIndexRef = .{ .name = "text", .kind = .full_text };
    const retry = try catchUpIndexFromMatchingCursor(alloc, &cursor, index, &capture, testApplyCapture, .{ .resource_manager = &manager, .max_records_per_window = 1 });
    try std.testing.expectEqual(@as(usize, 20), capture.applied_deleted_keys);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(usize, 2), retry.scanned_entries);
    try std.testing.expectEqual(@as(usize, 2), retry.replay_scan_batches);
    try std.testing.expectEqual(@as(u64, 2), retry.last_applied_sequence);
    try std.testing.expectEqual(@as(usize, 8), capture.applied_documents);
}

test "replay key blocks admit their header and spare capacity and roll back failed allocations" {
    const alloc = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.derived_replay_window)] = .{ .hard_limit_bytes = 64 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);
    var builder = ReplayChunkBuilder.init(alloc, .{ .name = "text", .kind = .full_text }, &manager, 0);
    defer builder.deinit();
    const first = try builder.copyKey("abcd");
    const second = try builder.copyKey("efgh");
    try std.testing.expectEqual(@as(u64, 2 * (@sizeOf(ReplayChunkBuilder.KeyBlock) + 4)), builder.tracked_bytes);
    try std.testing.expectEqual(builder.tracked_bytes, manager.snapshot().memory.used_bytes);
    try std.testing.expectError(error.ResourceBudgetExceeded, builder.copyKey("ijkl"));
    try std.testing.expectEqualStrings("abcd", first);
    try std.testing.expectEqualStrings("efgh", second);
    try std.testing.expectEqual(builder.tracked_bytes, manager.snapshot().memory.used_bytes);
    // A backing allocator failure releases the reservation taken before allocation.
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    var failed = ReplayChunkBuilder.init(failing.allocator(), .{ .name = "text", .kind = .full_text }, &manager, 0);
    defer failed.deinit();
    // Free the successful blocks to leave admission available for the injected failure.
    builder.deinit();
    builder = ReplayChunkBuilder.init(alloc, .{ .name = "text", .kind = .full_text }, &manager, 0);
    try std.testing.expectError(error.OutOfMemory, failed.copyKey("abcd"));
    try std.testing.expectEqual(@as(u64, 0), failed.tracked_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "catchUpIndex drops key block slack before rejecting an otherwise fitting record" {
    const alloc = std.testing.allocator;
    var journal = try change_journal_mod.Journal.open("compact-key-admission", testInMemoryJournalOpenOptions());
    defer journal.close();
    try appendChangeJournalRecord(&journal, alloc, .{ .sequence = 1, .changed_doc_keys = &.{"a"}, .target_hints = &.{.full_text} });
    var budgets = resource_manager_mod.Options.defaultBudgets();
    // A one-key scratch list, output list, dedup map, descriptor and exact key
    // block fit. The preferred block's unused bytes do not fit.
    budgets[@backingInt(resource_manager_mod.Slice.derived_replay_window)] = .{ .hard_limit_bytes = 580 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);
    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    const stats = try catchUpIndexWithOptions(alloc, replay_source_mod.Source.fromJournal(&journal), .{ .name = "text", .kind = .full_text }, 0, &capture, testApplyCapture, .{ .resource_manager = &manager });
    try std.testing.expectEqual(@as(u64, 1), stats.last_applied_sequence);
    try std.testing.expectEqual(@as(usize, 1), capture.applied_documents);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expect(manager.snapshot().memory.peak_bytes <= 580);
}

test "catchUpIndex no-op records do not disable compact key admission" {
    const alloc = std.testing.allocator;
    var journal = try change_journal_mod.Journal.open("review-noop-admission", testInMemoryJournalOpenOptions());
    defer journal.close();
    try appendChangeJournalRecord(&journal, alloc, .{ .sequence = 1, .changed_artifact_keys = &.{"ignored-by-text"}, .target_hints = &.{.full_text} });
    try appendChangeJournalRecord(&journal, alloc, .{ .sequence = 2, .changed_doc_keys = &.{"a"}, .target_hints = &.{.full_text} });
    try appendChangeJournalRecord(&journal, alloc, .{ .sequence = 3, .target_hints = &.{.full_text} });
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.derived_replay_window)] = .{ .hard_limit_bytes = 580 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);
    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    var cursor = try replay_source_mod.Source.fromJournal(&journal).openMatchingCursor(alloc, 0, .full_text);
    defer cursor.deinit(alloc);
    const index: index_manager_mod.ManagedIndexRef = .{ .name = "text", .kind = .full_text };
    const retry = try catchUpIndexFromMatchingCursor(alloc, &cursor, index, &capture, testApplyCapture, .{ .resource_manager = &manager, .max_records_per_window = 2, .max_windows_per_call = 1 });
    try std.testing.expectEqual(@as(usize, 2), retry.scanned_entries);
    try std.testing.expectEqual(@as(usize, 2), retry.replay_scan_batches);
    try std.testing.expectEqual(@as(u64, 2), retry.last_applied_sequence);
    try std.testing.expectEqual(@as(usize, 1), capture.applied_documents);
}

test "catchUpIndex compact fallback shares one key block per record" {
    const alloc = std.testing.allocator;
    var journal = try change_journal_mod.Journal.open("review-header-admission", testInMemoryJournalOpenOptions());
    defer journal.close();
    const record: change_journal_mod.Record = .{ .sequence = 1, .changed_doc_keys = &.{ "a", "b" }, .target_hints = &.{.full_text} };
    try appendChangeJournalRecord(&journal, alloc, record);
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.derived_replay_window)] = .{ .hard_limit_bytes = 750 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);
    var capture = TestApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    const index: index_manager_mod.ManagedIndexRef = .{ .name = "text", .kind = .full_text };
    const stats = try catchUpIndexWithOptions(alloc, replay_source_mod.Source.fromJournal(&journal), index, 0, &capture, testApplyCapture, .{ .resource_manager = &manager });
    try std.testing.expectEqual(@as(u64, 1), stats.last_applied_sequence);
    try std.testing.expectEqual(@as(usize, 2), capture.applied_documents);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expect(manager.snapshot().memory.peak_bytes <= 750);
}

test "replay metadata hint cannot reject a duplicate-heavy record under a hard budget" {
    const alloc = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.derived_replay_window)] = .{ .hard_limit_bytes = 600 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);
    var builder = ReplayChunkBuilder.init(alloc, .{ .name = "text", .kind = .full_text }, &manager, 0);
    defer builder.deinit();
    const repeated = @as([128][]const u8, @splat("a"));
    try builder.appendRecord(.{ .changed_doc_keys = &repeated });
    const batch = try builder.finishBorrowed(1);
    try std.testing.expectEqual(@as(usize, 1), batch.documents.len);
    try std.testing.expect(manager.snapshot().memory.peak_bytes <= 600);
}

fn replayMetadataAllocationFailure(alloc: Allocator) !void {
    var builder = ReplayChunkBuilder.init(alloc, .{ .name = "text", .kind = .full_text }, null, 0);
    defer builder.deinit();
    var names: [32][8]u8 = undefined;
    var keys: [32][]const u8 = undefined;
    for (&names, &keys, 0..) |*name, *key, i| key.* = try std.fmt.bufPrint(name, "doc-{d}", .{i});
    try builder.appendRecord(.{ .changed_doc_keys = &keys });
    _ = try builder.finishBorrowed(1);
}

test "replay metadata initial reservation releases every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, replayMetadataAllocationFailure, .{});
}

test "replay publication releases temporary metadata storage and admission before apply" {
    const Counter = @import("../../../allocation_bench_support.zig").Counter;
    var counter: Counter = .{};
    var manager = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 750 } });
    defer manager.deinit(std.testing.allocator);
    var builder = ReplayChunkBuilder.init(counter.allocator(), .{ .name = "text", .kind = .full_text }, &manager, 0);
    defer builder.deinit();
    builder.minimal_key_blocks = true;
    try builder.appendRecord(.{ .changed_doc_keys = &.{ "a", "b" } });
    const batch = try builder.finishBorrowed(1);
    try std.testing.expectEqual(@as(usize, 2), batch.documents.len);
    try std.testing.expectEqualStrings("a", batch.documents[0].key);
    try std.testing.expectEqualStrings("b", batch.documents[1].key);
    try std.testing.expectEqual(@as(u64, counter.live), builder.tracked_bytes);
    try std.testing.expectEqual(@as(usize, 0), builder.changed_doc_keys.capacity);
    var apply_bytes: u64 = 0;
    try manager.adjustUsage(.document_extraction_working_set, &apply_bytes, 400);
    defer manager.adjustUsage(.document_extraction_working_set, &apply_bytes, 0) catch unreachable;
    try std.testing.expect(counter.live + 400 <= 750);
    try std.testing.expect(counter.peak <= 750);
}

test "replay batcher duplicate metadata at its load limit does not grow" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var builder = ReplayChunkBuilder.init(failing.allocator(), .{ .name = "text", .kind = .full_text }, null, 0);
    defer builder.deinit();
    try builder.seen_changed_docs.ensureTotalCapacity(builder.alloc, 1);
    const capacity = builder.seen_changed_docs.capacity();
    const limit = @as(usize, capacity) * std.hash_map.default_max_load_percentage / 100;
    var names: [16][16]u8 = undefined;
    try std.testing.expect(limit <= names.len);
    for (0..limit) |i| try builder.appendRecord(.{ .changed_doc_keys = &.{try std.fmt.bufPrint(&names[i], "key-{d}", .{i})} });
    const allocations = failing.alloc_index;
    for (0..256) |_| try builder.appendRecord(.{ .changed_doc_keys = &.{"key-0"} });
    try std.testing.expectEqual(capacity, builder.seen_changed_docs.capacity());
    try std.testing.expectEqual(allocations, failing.alloc_index);
}
