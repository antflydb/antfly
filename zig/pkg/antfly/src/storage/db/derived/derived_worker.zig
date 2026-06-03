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
const replay_source_mod = @import("replay_source.zig");
const derived_types = @import("derived_types.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
const db_types = @import("../types.zig");
const batcher = @import("../batcher.zig");
const internal_keys = @import("../../internal_keys.zig");
const docstore_mod = @import("../../docstore.zig");
const mem_backend_mod = @import("../../mem_backend.zig");
const resource_manager_mod = @import("../../resource_manager.zig");
const platform_time = @import("../../../platform/time.zig");

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
    window_collect_ns: u64 = 0,
    apply_ns: u64 = 0,
};

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
    estimated_dense_vector_bytes: u64 = 0,
    target_sequence: u64 = 0,
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
    if (err == error.WriterLocked) return;
    if (err == error.ReplayDocumentNotVisible) return;
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
    while (true) {
        var builder = ReplayChunkBuilder.init(alloc, index_ref, options.resource_manager, options.max_chunk_bytes);
        builder.max_items = options.max_items_per_window;
        builder.estimated_dense_vector_bytes = options.estimated_dense_vector_bytes;
        builder.target_sequence = options.target_sequence;
        defer builder.deinit();

        const collect_started_ns = monotonicTimeNs();
        const chunk_stats = replay_cursor.forEachNext(
            options.max_records_per_window,
            &builder,
            replayChunkConsumeRecord,
        ) catch |err| return err;
        stats.window_collect_ns += monotonicTimeNs() - collect_started_ns;
        if (chunk_stats.last_sequence == 0) {
            break;
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

        var batch = try builder.finish(chunk_stats.last_sequence);
        var window_open = false;
        errdefer if (window_open) {
            if (options.finish_window_fn) |finish_window| {
                finish_window(options.window_ctx.?, index_ref, false) catch {};
            }
        };
        if (options.begin_window_fn) |begin_window| {
            begin_window(options.window_ctx.?, index_ref) catch |err| {
                logCatchUpError(index_ref, "begin_window", chunk_stats.last_sequence, stats.scanned_entries, stats.applied_entries, err);
                derived_types.deinitDerivedBatch(alloc, &batch);
                return err;
            };
            window_open = true;
        }

        const apply_started_ns = monotonicTimeNs();
        if (apply_fn(apply_ctx, batch, index_ref) catch |err| {
            stats.apply_ns += monotonicTimeNs() - apply_started_ns;
            logCatchUpError(index_ref, "journal_apply", chunk_stats.last_sequence, stats.scanned_entries, stats.applied_entries, err);
            derived_types.deinitDerivedBatch(alloc, &batch);
            return err;
        }) {
            stats.apply_ns += monotonicTimeNs() - apply_started_ns;
            stats.applied_entries += 1;
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
                derived_types.deinitDerivedBatch(alloc, &batch);
                return err;
            };
            window_open = false;
        }
        if (options.persist_progress_fn) |persist| {
            persist(options.persist_ctx.?, index_ref.name, chunk_stats.last_sequence) catch |err| {
                logCatchUpError(index_ref, "persist_progress", chunk_stats.last_sequence, stats.scanned_entries, stats.applied_entries, err);
                derived_types.deinitDerivedBatch(alloc, &batch);
                return err;
            };
        }
        derived_types.deinitDerivedBatch(alloc, &batch);
        completed_windows += 1;
        if (options.max_windows_per_call > 0 and completed_windows >= options.max_windows_per_call) break;
    }
    return stats;
}

fn monotonicTimeNs() u64 {
    return platform_time.monotonicNs();
}

const ReplayChunkBuilder = struct {
    alloc: Allocator,
    index_ref: index_manager_mod.ManagedIndexRef,
    resource_manager: ?*resource_manager_mod.ResourceManager,
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

    fn deinit(self: *@This()) void {
        for (self.changed_doc_keys.items) |key| self.alloc.free(key);
        self.changed_doc_keys.deinit(self.alloc);
        for (self.deleted_doc_keys.items) |key| self.alloc.free(key);
        self.deleted_doc_keys.deinit(self.alloc);
        for (self.overwritten_doc_keys.items) |key| self.alloc.free(key);
        self.overwritten_doc_keys.deinit(self.alloc);
        for (self.changed_artifact_keys.items) |key| self.alloc.free(key);
        self.changed_artifact_keys.deinit(self.alloc);
        self.seen_changed_docs.deinit(self.alloc);
        self.seen_deleted_docs.deinit(self.alloc);
        self.seen_overwritten_docs.deinit(self.alloc);
        self.seen_changed_artifacts.deinit(self.alloc);
        if (self.resource_manager) |manager| manager.releaseBytes(.derived_replay_window, self.tracked_bytes);
        self.* = undefined;
    }

    fn appendUniqueKey(
        self: *@This(),
        list: *std.ArrayListUnmanaged([]const u8),
        seen: *std.StringHashMapUnmanaged(void),
        value: []const u8,
    ) !void {
        if (value.len == 0) return;
        if (seen.contains(value)) return;
        const owned = try self.alloc.dupe(u8, value);
        errdefer self.alloc.free(owned);
        const prior_list_capacity = list.capacity;
        const prior_seen_capacity = seen.capacity();
        try seen.put(self.alloc, owned, {});
        errdefer _ = seen.remove(owned);
        try list.append(self.alloc, owned);
        var next_tracked_bytes = self.tracked_bytes;
        next_tracked_bytes +|= owned.len;
        next_tracked_bytes +|= @as(u64, @intCast(list.capacity - prior_list_capacity)) * @sizeOf([]const u8);
        next_tracked_bytes +|= @as(u64, @intCast(seen.capacity() - prior_seen_capacity)) * (@sizeOf([]const u8) + @sizeOf(void));
        try self.observeTrackedBytes(next_tracked_bytes);
    }

    fn appendRecord(self: *@This(), record: change_journal_mod.Record) !void {
        self.item_count +|= recordItemCountForIndex(record, self.index_ref.kind);
        switch (self.index_ref.kind) {
            .full_text, .algebraic => {
                for (record.changed_doc_keys) |key| try self.appendUniqueKey(&self.changed_doc_keys, &self.seen_changed_docs, key);
                for (record.deleted_doc_keys) |key| try self.appendUniqueKey(&self.deleted_doc_keys, &self.seen_deleted_docs, key);
                for (record.overwritten_doc_keys) |key| try self.appendUniqueKey(&self.overwritten_doc_keys, &self.seen_overwritten_docs, key);
            },
            .dense_vector, .sparse_vector => {
                for (record.changed_doc_keys) |key| try self.appendUniqueKey(&self.changed_doc_keys, &self.seen_changed_docs, key);
                for (record.deleted_doc_keys) |key| try self.appendUniqueKey(&self.deleted_doc_keys, &self.seen_deleted_docs, key);
                for (record.overwritten_doc_keys) |key| try self.appendUniqueKey(&self.overwritten_doc_keys, &self.seen_overwritten_docs, key);
                for (record.changed_artifact_keys) |key| {
                    if (!internal_keys.isEmbeddingArtifactKey(key) and !internal_keys.isDerivedEmbeddingArtifactKey(key)) continue;
                    try self.appendUniqueKey(&self.changed_artifact_keys, &self.seen_changed_artifacts, key);
                }
            },
            .graph => {
                for (record.deleted_doc_keys) |key| try self.appendUniqueKey(&self.deleted_doc_keys, &self.seen_deleted_docs, key);
                for (record.changed_artifact_keys) |key| {
                    if (!internal_keys.isGraphEdgeArtifactKey(key) and !internal_keys.isAssetArtifactKey(key)) continue;
                    try self.appendUniqueKey(&self.changed_artifact_keys, &self.seen_changed_artifacts, key);
                }
            },
        }
        if (self.index_ref.kind == .dense_vector) {
            const vector_bytes = @as(u64, @intCast(countEmbeddingArtifactKeys(record.changed_artifact_keys))) * self.estimatedDenseVectorBytes();
            try self.observeTrackedBytes(self.tracked_bytes +| vector_bytes);
        }
    }

    fn wouldOverflowWithRecord(self: *@This(), record: change_journal_mod.Record) bool {
        if (self.tracked_bytes == 0 and self.item_count == 0) return false;
        if (self.max_chunk_bytes > 0 and self.tracked_bytes + recordEstimatedBytesForIndex(record, self.index_ref.kind, self.estimatedDenseVectorBytes()) > self.max_chunk_bytes) return true;
        if (self.max_items > 0 and self.item_count + recordItemCountForIndex(record, self.index_ref.kind) > self.max_items) return true;
        return false;
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

    fn finish(self: *@This(), sequence: u64) !derived_types.DerivedBatch {
        const changed_doc_keys = try self.changed_doc_keys.toOwnedSlice(self.alloc);
        errdefer {
            for (changed_doc_keys) |key| self.alloc.free(key);
            if (changed_doc_keys.len > 0) self.alloc.free(changed_doc_keys);
        }

        var documents = try self.alloc.alloc(derived_types.DerivedDocument, changed_doc_keys.len);
        var initialized_docs: usize = 0;
        errdefer {
            var tmp = derived_types.DerivedBatch{ .documents = documents[0..initialized_docs] };
            derived_types.deinitDerivedBatch(self.alloc, &tmp);
        }
        for (changed_doc_keys, 0..) |key, i| {
            const targets: []const derived_types.DerivedTargetRef = switch (self.index_ref.kind) {
                .full_text, .algebraic => blk: {
                    const refs = try self.alloc.alloc(derived_types.DerivedTargetRef, 1);
                    refs[0] = .{
                        .kind = if (self.index_ref.kind == .full_text) .full_text else .algebraic,
                        .index_name = try self.alloc.dupe(u8, self.index_ref.name),
                    };
                    break :blk refs;
                },
                else => &.{},
            };
            documents[i] = .{
                .key = key,
                .action = .upsert,
                .targets = targets,
            };
            initialized_docs += 1;
        }
        if (changed_doc_keys.len > 0) self.alloc.free(changed_doc_keys);

        const deleted_keys = try self.deleted_doc_keys.toOwnedSlice(self.alloc);
        errdefer {
            for (deleted_keys) |key| self.alloc.free(key);
            if (deleted_keys.len > 0) self.alloc.free(deleted_keys);
        }
        const overwritten_doc_keys = try self.overwritten_doc_keys.toOwnedSlice(self.alloc);
        errdefer {
            for (overwritten_doc_keys) |key| self.alloc.free(key);
            if (overwritten_doc_keys.len > 0) self.alloc.free(overwritten_doc_keys);
        }
        const changed_artifact_keys = try self.changed_artifact_keys.toOwnedSlice(self.alloc);
        errdefer {
            for (changed_artifact_keys) |key| self.alloc.free(key);
            if (changed_artifact_keys.len > 0) self.alloc.free(changed_artifact_keys);
        }

        const batch: derived_types.DerivedBatch = .{
            .sequence = sequence,
            .documents = documents,
            .deleted_keys = deleted_keys,
            .overwritten_doc_keys = overwritten_doc_keys,
            .changed_artifact_keys = changed_artifact_keys,
        };

        self.seen_changed_docs.deinit(self.alloc);
        self.seen_deleted_docs.deinit(self.alloc);
        self.seen_overwritten_docs.deinit(self.alloc);
        self.seen_changed_artifacts.deinit(self.alloc);
        self.seen_changed_docs = .empty;
        self.seen_deleted_docs = .empty;
        self.seen_overwritten_docs = .empty;
        self.seen_changed_artifacts = .empty;
        return batch;
    }
};

fn replayChunkConsumeRecord(ctx: *anyopaque, sequence: u64, payload: []const u8) !void {
    const builder: *ReplayChunkBuilder = @ptrCast(@alignCast(ctx));
    if (builder.target_sequence != 0 and sequence > builder.target_sequence) return replay_source_mod.StopReplayChunk.StopReplayChunk;
    if (change_journal_mod.looksLikeBinaryRecord(payload)) {
        var record = try change_journal_mod.decodeBinaryRecordBorrowed(builder.alloc, payload);
        defer record.deinit();
        if (builder.wouldOverflowWithRecord(record.record)) return replay_source_mod.StopReplayChunk.StopReplayChunk;
        try builder.appendRecord(record.record);
        return;
    }

    var record = try change_journal_mod.decodeRecord(builder.alloc, payload);
    defer record.deinit();
    if (builder.wouldOverflowWithRecord(record.record)) return replay_source_mod.StopReplayChunk.StopReplayChunk;
    try builder.appendRecord(record.record);
}

fn recordEstimatedBytesForIndex(record: change_journal_mod.Record, kind: db_types.IndexKind, estimated_dense_vector_bytes: u64) u64 {
    var total = estimatedStringListBytes(record.changed_doc_keys) +
        estimatedStringListBytes(record.deleted_doc_keys) +
        estimatedStringListBytes(record.overwritten_doc_keys) +
        estimatedStringListBytes(record.changed_artifact_keys);
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
        if (internal_keys.isGraphEdgeArtifactKey(key) or internal_keys.isAssetArtifactKey(key)) count += 1;
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

    fn deinit(self: *TestApplyCapture) void {
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

    fn deinit(self: *@This()) void {
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
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-batched-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
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
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
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

test "catchUpIndex can stop after bounded replay windows" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-window-limit-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
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

test "catchUpIndex catch-up hooks fire once per replay run" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-catch-up-hooks-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
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
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
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
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-dense-parent-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
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
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    var i: usize = 0;
    while (i < catch_up_max_records_per_window_default + 1) : (i += 1) {
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

    try std.testing.expectEqual(catch_up_max_records_per_window_default + 1, stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 2), stats.applied_entries);
    try std.testing.expectEqual(@as(usize, 2), capture.call_count);
    try std.testing.expectEqual(catch_up_max_records_per_window_default + 1, capture.applied_documents);
    try std.testing.expectEqual(catch_up_max_records_per_window_default + 1, capture.applied_changed_artifact_keys);
}

test "catchUpIndex chunks replay by byte budget" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-byte-chunked-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
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
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
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

test "catchUpIndex chunks dense replay by estimated vector byte budget" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-dense-vector-byte-chunked-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
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
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-unbatched-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
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
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-sparse-batched-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
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
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/derived-graph-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
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
