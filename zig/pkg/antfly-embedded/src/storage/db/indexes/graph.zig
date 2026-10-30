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

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const db_mod = @import("../db.zig");
const DB = db_mod.DB;
const Indexes = @import("mod.zig").Indexes;

/// Graph index state owned by `DB.indexes`.
pub const GraphIndexes = struct {
    replacement: Replacement = .{},

    /// Background replacement: builds the new generation in a shadow catalog, catches it up with the live tail, then cuts over.
    pub const Replacement = struct {
        mutex: std.Io.Mutex = .init,
        manager: ?*db_mod.index_manager_mod.IndexManager = null,
        catalog_generation: u64 = 0,
        progress: ?db_mod.GraphReplacementProgress = null,
    };

    fn dbPtr(self: *GraphIndexes) *DB {
        const indexes: *Indexes = @alignCast(@fieldParentPtr("graph", self));
        return indexes.dbPtr();
    }

    pub fn replacementCutoverBudgetNs(activation_attempts: u32) u64 {
        return (if (activation_attempts >= 8) @as(u64, 1000) else 100) * std.time.ns_per_ms;
    }

    pub fn hasPendingReplacements(self: *GraphIndexes) bool {
        const db = self.dbPtr();
        const jobs = db.core.index_manager.graphReplacementBuildsAlloc(db.core.store, db.alloc) catch return true;
        defer jobs.deinit();
        for (jobs.value) |job| if (job.phase != .failed) return true;
        return false;
    }

    pub fn replacementStatusAlloc(self: *GraphIndexes, alloc: Allocator) !std.json.Parsed([]db_mod.index_manager_mod.GraphReplacementBuild) {
        const db = self.dbPtr();
        return db.core.index_manager.graphReplacementBuildsAlloc(db.core.store, alloc);
    }

    /// Caller owns the returned names and slice. Include upstream artifact
    /// producers so diagnostics and source replay describe the same dependency.
    pub fn replacementProducerNamesAlloc(self: *GraphIndexes, a: Allocator, job: db_mod.index_manager_mod.GraphReplacementBuild) ![][]u8 {
        const db = self.dbPtr();
        const cfg = job.config orelse return a.alloc([]u8, 0);
        db.core.index_manager.catalog_mutex.lockShared();
        defer db.core.index_manager.catalog_mutex.unlockShared();
        var names: std.ArrayList([]const u8) = .empty;
        defer names.deinit(a);
        var graph = try db_mod.index_manager_mod.parseGraphConfig(a, cfg.config_json);
        defer graph.deinit(a);
        for (graph.artifact_sources) |source| try names.append(a, source.artifact_name);
        for (db.core.index_manager.resolvers.items) |resolver| {
            if (std.mem.eql(u8, resolver.replacement_index, cfg.name)) try names.append(a, resolver.source_artifact);
        }
        return db.core.index_manager.artifactProducerNamesForArtifactsAlloc(a, names.items);
    }

    pub fn catchUpReplacement(self: *GraphIndexes, private: *db_mod.index_manager_mod.IndexManager, name: []const u8, from: u64, target: u64, limit: usize, deadline: ?u64) !u64 {
        const db = self.dbPtr();
        if (target <= from) return from;
        var arena = std.heap.ArenaAllocator.init(db.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const Collect = struct {
            database: *DB,
            alloc: Allocator,
            target: u64,
            deadline: ?u64,
            applied: u64,
            documents: std.StringHashMapUnmanaged(void) = .empty,
            artifacts: std.StringHashMapUnmanaged(void) = .empty,

            fn add(state: *@This(), set: *std.StringHashMapUnmanaged(void), keys: []const []const u8) !void {
                for (keys) |key| {
                    const entry = try set.getOrPut(state.alloc, key);
                    if (!entry.found_existing) entry.key_ptr.* = try state.alloc.dupe(u8, key);
                }
            }

            fn record(state: *@This(), sequence: u64, payload: []const u8) anyerror!void {
                if (sequence > state.target) return error.StopReplayChunk;
                if (state.deadline) |end| if (state.database.indexes.graph.replacementWorkNow() >= end) return error.StopReplayChunk;
                var decoded = try db_mod.change_journal_mod.decodeRecord(state.alloc, payload);
                defer decoded.deinit();
                try state.add(&state.documents, decoded.record.changed_doc_keys);
                try state.add(&state.documents, decoded.record.deleted_doc_keys);
                try state.add(&state.documents, decoded.record.overwritten_doc_keys);
                try state.add(&state.artifacts, decoded.record.changed_artifact_keys);
                state.applied = sequence;
            }
        };
        const collect_now = self.replacementWorkNow();
        const collection_deadline = if (deadline) |end| collect_now +| (end -| collect_now) / 4 else null;
        var collected = Collect{ .database = db, .alloc = a, .target = target, .deadline = collection_deadline, .applied = from };
        _ = db.core.store.forEachReplayLaneFrom(db_mod.internal_keys.replay_all_kind, from +| 1, limit, &collected, Collect.record) catch |err| switch (err) {
            error.StopReplayChunk => {},
            else => return err,
        };
        // A bounded cutover must not repeatedly rebuild a prefix of a large
        // window without acknowledging it. Leave such windows to the ordinal
        // cursor path before spending the work budget. Activation, including
        // escalation, always has a finite deadline; more than 32 unique sources
        // therefore use resumable per-entry batches rather than one coalesced
        // pass. Unfenced construction may coalesce larger windows.
        if (deadline != null and collected.documents.count() + collected.artifacts.count() > 32) return from;
        // A hot source can occur hundreds of times in a replay window. Build
        // its current projection once, then acknowledge the whole window only
        // after every collected source is durable. Interrupted rounds are safe
        // to repeat without advancing their journal watermark.
        var ctx = self.replacementContext(private);
        defer ctx.deinit(db.alloc);
        var source_read = try db.core.store.beginReadTxn();
        defer source_read.abort();
        var documents: std.ArrayList([]const u8) = .empty;
        var document_keys = collected.documents.keyIterator();
        while (document_keys.next()) |key| try documents.append(a, key.*);
        var offset: usize = 0;
        while (offset < documents.items.len) {
            if (deadline) |end| if (self.replacementWorkNow() >= end) return from;
            const end = @min(offset + 32, documents.items.len);
            try db_mod.rebuildGraphReplacementDocumentsWithRead(&ctx, name, documents.items[offset..end], &source_read);
            offset = end;
        }
        var keys: std.ArrayList([]const u8) = .empty;
        var artifacts = collected.artifacts.keyIterator();
        while (artifacts.next()) |key| {
            const owner_key = try db_mod.internal_keys.decodeDocumentComponentAlloc(a, key.*);
            if (owner_key != null and collected.documents.contains(owner_key.?)) continue;
            try keys.append(a, key.*);
        }
        if (keys.items.len != 0) {
            if (deadline) |end| if (self.replacementWorkNow() >= end) return from;
            const changed = try db_mod.materializeGraphSourceArtifactsForIndex(a, ctx.store, private, keys.items, name, .{ .require_resolution_contract = true });
            try db_mod.applyDerivedBatchToIndexContextProfiled(&ctx, .{ .changed_artifact_keys = changed }, .{ .name = name, .kind = .graph }, null, false, null);
        }
        return collected.applied;
    }

    /// The final write pause can yield within one large journal batch. The
    /// cursor indexes immutable source keys in its next retained entry, and is
    /// published only after the corresponding shadow projection checkpoint.
    pub fn catchUpReplacementTail(self: *GraphIndexes, private: *db_mod.index_manager_mod.IndexManager, name: []const u8, from: u64, target: u64, source_cursor: *usize, deadline: u64) !u64 {
        const db = self.dbPtr();
        var arena = std.heap.ArenaAllocator.init(db.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const First = struct {
            alloc: Allocator,
            target: u64,
            sequence: u64 = 0,
            payload: ?[]const u8 = null,
            fn record(state: *@This(), sequence: u64, payload: []const u8) anyerror!void {
                if (sequence > state.target) return error.StopReplayChunk;
                state.sequence = sequence;
                state.payload = try state.alloc.dupe(u8, payload);
            }
        };
        var first = First{ .alloc = a, .target = target };
        _ = db.core.store.forEachReplayLaneFrom(db_mod.internal_keys.replay_all_kind, from +| 1, 1, &first, First.record) catch |err| switch (err) {
            error.StopReplayChunk => {},
            else => return err,
        };
        const payload = first.payload orelse return from;
        var decoded = try db_mod.change_journal_mod.decodeRecord(a, payload);
        defer decoded.deinit();
        var documents: std.StringArrayHashMapUnmanaged(void) = .empty;
        for ([_][]const []const u8{ decoded.record.changed_doc_keys, decoded.record.deleted_doc_keys, decoded.record.overwritten_doc_keys }) |keys| {
            for (keys) |key| try documents.put(a, key, {});
        }
        var artifacts: std.StringArrayHashMapUnmanaged(void) = .empty;
        for (decoded.record.changed_artifact_keys) |key| {
            const owner_key = try db_mod.internal_keys.decodeDocumentComponentAlloc(a, key);
            if (owner_key != null and documents.contains(owner_key.?)) continue;
            try artifacts.put(a, key, {});
        }
        const total = documents.count() + artifacts.count();
        if (source_cursor.* > total) return error.InvalidReplacementReplayCursor;
        var ctx = self.replacementContext(private);
        defer ctx.deinit(db.alloc);
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        while (source_cursor.* < total) {
            if (self.replacementWorkNow() >= deadline) return from;
            const ordinal = source_cursor.*;
            if (ordinal < documents.count()) {
                const end = @min(ordinal + 32, documents.count());
                try db_mod.rebuildGraphReplacementDocumentsWithRead(&ctx, name, documents.keys()[ordinal..end], &read);
                source_cursor.* = end;
            } else {
                const start = ordinal - documents.count();
                const end = @min(start + 32, artifacts.count());
                const changed = try db_mod.materializeGraphSourceArtifactsForIndex(a, ctx.store, private, artifacts.keys()[start..end], name, .{ .require_resolution_contract = true });
                try db_mod.applyDerivedBatchToIndexContextProfiled(&ctx, .{ .changed_artifact_keys = changed }, .{ .name = name, .kind = .graph }, null, false, null);
                source_cursor.* = documents.count() + end;
            }
            if (builtin.is_test) if (DB.test_graph_replacement_tail_batch) |hook| hook(db, source_cursor.*);
        }
        source_cursor.* = 0;
        return first.sequence;
    }

    fn replacementContext(self: *GraphIndexes, manager: *db_mod.index_manager_mod.IndexManager) db_mod.AsyncContext {
        const db = self.dbPtr();
        return .{
            .alloc = db.alloc,
            .io = db.backend_runtime.io(),
            .store = db.core.store,
            .relational_base_rows = db_mod.relationalColumns(db) != null,
            .index_manager = manager,
            .apply_mutex = db.async_context.apply_mutex,
            .dense_bulk_session_scope = .external,
            .require_graph_resolution_contract = true,
        };
    }

    /// One fair maintenance quantum. Opening a shadow reuses its durable graph;
    /// the saved source cursor and replay watermark never restart the corpus.
    pub fn advanceReplacement(self: *GraphIndexes) !bool {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("advanceGraphReplacement", .{db});
        const pending = try self.advanceReplacementUntil(null);
        if (pending) try db.core.index_manager.checkpointIo().sleep(.fromMilliseconds(1), .awake);
        return pending;
    }

    pub fn replacementWorkNow(_: *GraphIndexes) u64 {
        if (builtin.is_test) if (DB.test_graph_replacement_work_now_ns) |now| return now;
        return db_mod.monotonicTimeNs();
    }

    pub fn advanceReplacementUntil(self: *GraphIndexes, deadline: ?u64) !bool {
        return self.advanceReplacementForIdentityUntil(deadline, null, .manual);
    }

    pub fn advanceReplacementForIdentityUntil(self: *GraphIndexes, deadline: ?u64, identity: ?DB.GraphReplacementIdentity, drive: DB.GraphReplacementDrive) !bool {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return false;
        if (deadline) |end| if (self.replacementWorkNow() >= end) return self.hasPendingReplacements();
        if (!self.replacement.mutex.tryLock()) return self.hasPendingReplacements();
        defer self.replacement.mutex.unlock(db.core.index_manager.checkpointIo());
        const jobs = try self.replacementStatusAlloc(db.alloc);
        defer jobs.deinit();
        for (jobs.value) |original| {
            if (identity) |wanted| {
                if (!std.mem.eql(u8, original.name, wanted.name) or original.generation != wanted.generation) continue;
            }
            if (original.phase == .failed) continue;
            var saved = original;
            const retry_now = db.replacementRetryNow();
            if (saved.retry_after_ns > retry_now +| DB.replacement_max_backoff_ns) {
                // Clock correction is local maintenance, not a replicated
                // command. Leave queued jobs untouched when writes are denied
                // or source authority requires ordered application.
                if (!try db.canRunLocalGraphEndpointCleanup()) continue;
                // Persist the correction once. Taking min(now + limit) only
                // in memory on every poll would slide the deadline forever.
                var owner = db.acquireReplicationMutationShared();
                defer if (owner) |*lease| lease.release();
                try db.enforceReplicationWriteGate();
                try db.lockApplyForPortableRuntime();
                defer db.core.unlockApply();
                saved.retry_after_ns = retry_now +| DB.replacement_max_backoff_ns;
                try db.core.index_manager.saveGraphReplacementBuild(db.core.store, saved);
            }
            if (saved.retry_after_ns > retry_now) continue;
            // The native cutover shares the tail's finite escalation policy.
            // Select its budget from this already-loaded durable reservation.
            const work_deadline = if (drive == .cutover)
                self.replacementWorkNow() +| replacementCutoverBudgetNs(saved.activation_attempts)
            else
                deadline;
            return self.advanceReplacementOnce(work_deadline, saved, drive == .detached) catch |err| {
                if (err == error.Canceled or err == error.Cancelled or err == error.EnrichmentWaitCanceled) {
                    // A stale detached snapshot yields without failing the job
                    // or consuming its retry budget. Interrupting this owner
                    // is also not cancelling the durable
                    // job. Discard volatile work and preserve its checkpoint.
                    self.clearReplacementManager();
                    return err;
                }
                if (db.core.index_manager.get(saved.name) == null) {
                    if (err == error.Busy or err == error.IntegrityTopologyBusy) return true;
                    var owner = db.acquireReplicationMutationShared();
                    defer if (owner) |*lease| lease.release();
                    try db.enforceReplicationWriteGate();
                    var activity = try db.acquireResolverCatalogActivity(true);
                    defer activity.deinit();
                    try db.lockApplyForPortableRuntime();
                    defer db.core.unlockApply();
                    // Activation can fail after a newer checkpoint committed.
                    // Account against that durable progress, never overwrite it
                    // with the scheduler's pre-quantum copy.
                    const durable = try self.replacementStatusAlloc(db.alloc);
                    defer durable.deinit();
                    var job = for (durable.value) |current| {
                        if (std.mem.eql(u8, current.name, saved.name)) break current;
                    } else return self.hasPendingReplacements();
                    job.last_error = @errorName(err);
                    if (DB.replacementRetryable(err) and job.retry_count < 4 and job.total_retry_count < DB.replacement_total_retry_limit) {
                        // Persist wall-clock backoff across owner changes/restarts.
                        // Discard volatile projection state and resume the last
                        // acknowledged checkpoint, including partial tail work.
                        job.retry_after_ns = db.replacementRetryNow() +| (@as(u64, 50) << @intCast(job.retry_count * 2)) * std.time.ns_per_ms;
                        job.retry_count += 1;
                        job.total_retry_count += 1;
                        try db.core.index_manager.saveGraphReplacementBuild(db.core.store, job);
                    } else {
                        job.phase = .failed;
                        job.retry_after_ns = 0;
                        try db.core.index_manager.failGraphReplacement(db.core.store, job);
                    }
                    if (db.resolution_runtime) |runtime| runtime.notifyBackfill();
                }
                self.clearReplacementManager();
                return self.hasPendingReplacements();
            };
        }
        // Sleep only after releasing replacement ownership: cancellation,
        // deletion and other ready jobs must not queue behind retry backoff.
        return self.hasPendingReplacements();
    }

    fn clearReplacementProgress(self: *GraphIndexes) void {
        const db = self.dbPtr();
        if (self.replacement.progress) |progress| if (progress.job.resume_key_b64) |key| db.alloc.free(key);
        self.replacement.progress = null;
    }

    pub fn clearReplacementManager(self: *GraphIndexes) void {
        const db = self.dbPtr();
        self.clearReplacementProgress();
        if (self.replacement.manager) |manager| {
            // Only checkpointed shadow state is reusable. Never flush an
            // abandoned or retired generation over a newer connection view.
            manager.deinitImmutableSnapshot();
            db.alloc.destroy(manager);
            self.replacement.manager = null;
        }
    }

    pub fn cancelReplacement(self: *GraphIndexes, name: []const u8) !bool {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        try self.replacement.mutex.lock(db.core.index_manager.checkpointIo());
        defer self.replacement.mutex.unlock(db.core.index_manager.checkpointIo());
        var owner = db.acquireReplicationMutationShared();
        defer if (owner) |*lease| lease.release();
        try db.enforceReplicationWriteGate();

        var activity = try db.acquireResolverCatalogActivity(true);
        defer activity.deinit();
        const cancelled = blk: {
            try db.lockApplyForPortableRuntime();
            defer db.core.unlockApply();
            db.core.repair_replay_mutex.lockUncancelable(db.core.index_manager.checkpointIo());
            defer db.core.repair_replay_mutex.unlock(db.core.index_manager.checkpointIo());
            if (self.replacement.manager) |manager| if (manager.get(name) != null) self.clearReplacementManager();
            break :blk try db.core.index_manager.cancelGraphReplacement(db.core.store, name);
        };
        // Inline maintenance lanes may execute cleanup synchronously. Never
        // submit while retaining apply/replay admission needed by that cleanup.
        if (cancelled) {
            db.scheduleGeneratedArtifactCleanup();
            if (db.resolution_runtime) |runtime| runtime.notifyBackfill();
        }
        return cancelled;
    }

    /// Construct the synchronous driver even when optional runtimes were not
    /// opened. Serialize publication with producer reconfiguration, outside
    /// apply/snapshot admission so joining another owner cannot deadlock.
    pub fn ensureReplacementEnrichmentRuntime(self: *GraphIndexes) !void {
        const db = self.dbPtr();
        const created = blk: {
            db_mod.lockAtomicWithBackoff(&db.async_context.enrichment_lifecycle_mutex);
            defer db.async_context.enrichment_lifecycle_mutex.unlock();
            if (db.enrichment_runtime != null) break :blk false;
            var config: db_mod.enrichment_runtime_mod.Config = .{ .enable_without_producers = true };
            try db.initOptionalEnrichmentRuntime(&config);
            break :blk true;
        };
        if (created) try db.restartEnrichmentAfterStructuralMutation("graph replacement replay", "");
    }

    pub fn advanceReplacementOnce(self: *GraphIndexes, deadline: ?u64, saved: db_mod.index_manager_mod.GraphReplacementBuild, bounded_replay: bool) !bool {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return false;
        var owner = db.acquireReplicationMutationShared();
        defer if (owner) |*lease| lease.release();
        try db.enforceReplicationWriteGate();
        if (!saved.producer_replay_complete and db.core.index_manager.minimum_reader_version.load(.acquire) < 3) {
            // Upgrade legacy reservations before appending any producer work.
            // A crash must never leave new prerequisites open to epoch-2 writers.
            try db.lockApplyForPortableRuntime();
            defer db.core.unlockApply();
            try db.core.index_manager.saveGraphReplacementBuild(db.core.store, saved);
        }
        if (saved.producer_replay_complete and saved.producer_replay_target > 0) {
            db_mod.releaseReplicationMutationShared(&owner);
            try self.ensureReplacementEnrichmentRuntime();
            owner = db.acquireReplicationMutationShared();
            try db.enforceReplicationWriteGate();
        }
        var build_snapshot_lease = try db.acquireSnapshotReplayMutation();
        var snapshot_held = true;
        defer if (snapshot_held) build_snapshot_lease.release();
        var job = saved;
        const cfg = job.config orelse return error.InvalidIndexConfig;
        if (!job.producer_replay_complete) {
            // Reuse the managed-admission source replay owner: one bounded
            // primary page per quantum, with a durable cursor after its append.
            // A crash can repeat a page but cannot skip source documents.
            // The append owns its own role/snapshot admission. Release ours
            // first so a waiting snapshot cannot deadlock a nested reader.
            build_snapshot_lease.release();
            snapshot_held = false;
            db_mod.releaseReplicationMutationShared(&owner);
            var arena = std.heap.ArenaAllocator.init(db.alloc);
            defer arena.deinit();
            const a = arena.allocator();
            const producer_resume = if (job.producer_replay_resume_b64) |encoded| blk: {
                const bytes = try a.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(encoded));
                try std.base64.standard.Decoder.decode(bytes, encoded);
                break :blk bytes;
            } else null;
            const producers = try self.replacementProducerNamesAlloc(a, job);
            var page = if (producers.len == 0)
                DB.GeneratedEnrichmentReplayPageResult{ .complete = true }
            else blk: {
                try self.ensureReplacementEnrichmentRuntime();
                break :blk try db.appendGeneratedEnrichmentsFromStoredDocsPage(a, producers, true, producer_resume, 32, .{
                    .deadline_ns = @min(deadline orelse std.math.maxInt(u64), self.replacementWorkNow() +| 100 * std.time.ns_per_ms),
                });
            };
            defer page.deinit(a);
            job.producer_replay_complete = page.complete;
            job.producer_replay_target = @max(job.producer_replay_target, page.target_sequence);
            job.producer_replay_resume_b64 = if (page.next_resume_key) |key| blk: {
                const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(key.len));
                break :blk std.base64.standard.Encoder.encode(encoded, key);
            } else null;
            owner = db.acquireReplicationMutationShared();
            try db.enforceReplicationWriteGate();
            build_snapshot_lease = try db.acquireSnapshotReplayMutation();
            snapshot_held = true;
            try db.core.index_manager.saveGraphReplacementBuild(db.core.store, job);
            return true;
        }
        if (job.producer_replay_target > 0) {
            const runtime = db.enrichment_runtime orelse return error.EnrichmentRuntimeUnavailable;
            runtime.notifySequence(job.producer_replay_target);
            if (runtime.stats().applied_sequence < job.producer_replay_target) return true;
        }
        const catalog_generation = db.core.index_manager.write_plan_generation.load(.acquire);
        if (self.replacement.manager) |manager| if (manager.get(job.name) == null or self.replacement.catalog_generation != catalog_generation) self.clearReplacementManager();
        if (self.replacement.manager == null) {
            const manager = try db.alloc.create(db_mod.index_manager_mod.IndexManager);
            errdefer db.alloc.destroy(manager);
            manager.* = try db_mod.index_manager_mod.IndexManager.initWithOptions(db.core.index_manager.alloc, db.core.index_manager.base_path, db.index_backends);
            errdefer manager.deinit();
            // Repeated bounded pages revisit the same checkpoint runs. Use
            // the backend's existing bounded cache rather than publishing a
            // fresh native immutable-source lease on every block read.
            manager.graph_reverse_lsm_options.local_block_cache_enabled = true;
            // This unpublished manager has no independent compactor. Let
            // ordinary bounded write-pressure maintenance prevent checkpoint
            // runs accumulating without limit during a long build.
            manager.graph_reverse_lsm_options.foreground_soft_compaction = true;
            manager.source_payload_store = db.core.index_manager.source_payload_store;
            manager.setIo(db.backend_runtime.io());
            try manager.registerReplacementIndex(db.core.store, cfg);
            self.replacement.manager = manager;
            self.replacement.catalog_generation = catalog_generation;
        }
        const private = self.replacement.manager.?;
        if (self.replacement.progress) |progress| {
            job.resume_key_b64 = progress.job.resume_key_b64;
            job.scanned = progress.job.scanned;
            job.applied_sequence = progress.job.applied_sequence;
            job.tail_source_cursor = progress.job.tail_source_cursor;
            job.phase = progress.job.phase;
        }
        var ctx = self.replacementContext(private);
        defer ctx.deinit(db.alloc);
        var arena = std.heap.ArenaAllocator.init(db.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        if (job.phase == .building) {
            if (builtin.is_test) if (DB.test_graph_replacement_before_build) |hook| try hook(db);
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            var cursor = try read.openCursor();
            defer cursor.close();
            const source_lower = try db_mod.documentRangeLowerAlloc(a, "");
            const source_upper = (try db_mod.documentRangeUpperAlloc(a, "")).?;
            const build_resume = if (job.resume_key_b64) |encoded| blk: {
                const size = try std.base64.standard.Decoder.calcSizeForSlice(encoded);
                const bytes = try a.alloc(u8, size);
                try std.base64.standard.Decoder.decode(bytes, encoded);
                break :blk bytes;
            } else source_lower;
            var row = try cursor.seekAtOrAfter(if (std.mem.lessThan(u8, build_resume, source_lower)) source_lower else build_resume);
            if (job.resume_key_b64 != null and row != null and std.mem.eql(u8, row.?.key, build_resume)) row = try cursor.next();
            var documents: std.ArrayList([]const u8) = .empty;
            var artifacts: std.ArrayList([]const u8) = .empty;
            var count: usize = 0;
            var visited: usize = 0;
            var source_bytes: usize = 0;
            // Detached pages amortize reopening and durably checkpointing the
            // private native graph. Keep the 1 MiB/100 ms scan caps for large
            // rows; foreground waits retain their smaller document quantum.
            const document_limit: usize = if (bounded_replay) 256 else 32;
            const build_deadline = @min(deadline orelse std.math.maxInt(u64), db_mod.monotonicTimeNs() +| 100 * std.time.ns_per_ms);
            while (row) |kv| : (row = try cursor.next()) {
                if (!std.mem.lessThan(u8, kv.key, source_upper)) {
                    row = null;
                    break;
                }
                if (count == document_limit or visited == 4096) break;
                if (visited != 0 and source_bytes +| kv.value.len > 1024 * 1024) break;
                // Setup can consume a short caller quantum. Admit one
                // bounded scan unit so repeated waits cannot starve the job.
                if (visited != 0 and db_mod.monotonicTimeNs() >= build_deadline) break;
                visited += 1;
                source_bytes +|= kv.value.len;
                if (db_mod.internal_keys.isStoredDocumentRowKey(kv.key)) {
                    const key = (try db_mod.internal_keys.decodeStoredDocumentRowKeyAlloc(a, kv.key)).?;
                    count += 1;
                    try documents.append(a, key);
                } else if (db_mod.internal_keys.isAssetArtifactKey(kv.key) or db_mod.internal_keys.isChunkArtifactRecordKey(kv.key) or db_mod.internal_keys.isResolutionArtifactKey(kv.key)) {
                    const owner_key = (try db_mod.internal_keys.decodeDocumentComponentAlloc(a, kv.key)).?;
                    const primary = if (db_mod.relationalColumns(db) != null)
                        try db_mod.internal_keys.relationalRowKeyAlloc(a, owner_key)
                    else
                        try db_mod.internal_keys.documentKeyAlloc(a, owner_key);
                    const primary_value = read.get(primary) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                    if (primary_value == null) {
                        count += 1;
                        try artifacts.append(a, try a.dupe(u8, kv.key));
                    }
                }
                const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(kv.key.len));
                job.resume_key_b64 = std.base64.standard.Encoder.encode(encoded, kv.key);
                job.scanned += 1;
            }
            if (documents.items.len > 0) try db_mod.rebuildGraphReplacementDocumentsWithRead(&ctx, cfg.name, documents.items, &read);
            if (artifacts.items.len > 0) {
                const changed = try db_mod.materializeGraphSourceArtifactsForIndex(a, ctx.store, private, artifacts.items, cfg.name, .{ .require_resolution_contract = true });
                try db_mod.applyDerivedBatchToIndexContextProfiled(&ctx, .{ .changed_artifact_keys = changed }, .{ .name = cfg.name, .kind = .graph }, null, false, null);
            }
            if (row == null) job.phase = .catching_up;
        }
        const target = db.core.nextDerivedSequence();
        const before_catchup = job.applied_sequence;
        // Construction setup may exhaust a short caller deadline. Background
        // replay gets its own finite quantum after the base page, so a moving
        // journal tip cannot monopolize construction or discard its progress.
        const catchup_deadline = if (bounded_replay) deadline orelse (self.replacementWorkNow() +| 100 * std.time.ns_per_ms) else deadline;
        job.applied_sequence = try self.catchUpReplacement(private, cfg.name, job.applied_sequence, target, 1024, catchup_deadline);
        if (job.applied_sequence > before_catchup) job.tail_source_cursor = 0;
        // Yielding is not a durability boundary. Keep volatile progress with
        // the resident shadow, retaining the journal at the last durable cut.
        // Checkpoint the first quantum, at most once per second thereafter,
        // and always before activation. A crash repeats only unacknowledged
        // work, never an entire completed build.
        const now = db_mod.monotonicTimeNs();
        const previous_checkpoint = if (self.replacement.progress) |progress| progress.checkpoint_ns else 0;
        const checkpoint = previous_checkpoint == 0 or now -| previous_checkpoint >= std.time.ns_per_s or job.phase == .catching_up;
        var progress_job = job;
        progress_job.name = "";
        progress_job.old = "";
        progress_job.config = null;
        progress_job.last_error = null;
        progress_job.resume_key_b64 = if (job.resume_key_b64) |key| try db.alloc.dupe(u8, key) else null;
        var progress_transferred = false;
        errdefer if (!progress_transferred) {
            if (progress_job.resume_key_b64) |key| db.alloc.free(key);
        };
        if (checkpoint) {
            if (builtin.is_test) if (DB.test_graph_replacement_before_checkpoint) |hook| hook(db);
            try private.checkpointLsmWalForManagedIndex(.{ .name = cfg.name, .kind = .graph });
            try private.syncIndexByName(cfg.name, true);
            if (job.scanned > saved.scanned or job.applied_sequence > saved.applied_sequence or job.tail_source_cursor > saved.tail_source_cursor or job.phase != saved.phase) {
                job.retry_count = 0;
                job.retry_after_ns = 0;
                job.last_error = null;
                progress_job.retry_count = 0;
                progress_job.retry_after_ns = 0;
            }
            try db.core.index_manager.saveGraphReplacementBuild(db.core.store, job);
        }
        self.clearReplacementProgress();
        self.replacement.progress = .{ .job = progress_job, .checkpoint_ns = if (checkpoint) now else previous_checkpoint };
        progress_transferred = true;
        if (job.phase != .catching_up or bounded_replay) return true;
        // Activation acquires its own structural/role admission. Release the
        // construction leases first so a waiting snapshot/role fence cannot
        // deadlock a nested acquisition by this owner.
        build_snapshot_lease.release();
        snapshot_held = false;
        db_mod.releaseReplicationMutationShared(&owner);
        if (builtin.is_test) if (DB.test_graph_replacement_before_activation) |hook| try hook(db);
        var prepared = DB.PreparedGraphReplacement{ .manager = private, .sequence = job.applied_sequence, .deadline_ns = deadline, .tail_source_cursor = job.tail_source_cursor, .activation_attempts = job.activation_attempts };
        defer if (prepared.entry) |*entry| private.destroyDetachedReplacementIndex(entry);
        _ = db.addIndexForReconciliation(cfg, .ordinary, null, job.old, &prepared) catch |err| switch (err) {
            error.Busy, error.IntegrityTopologyBusy => {
                // The bounded fence may have advanced the durable intra-entry
                // cursor; refresh from that cut on the next maintenance turn.
                self.clearReplacementProgress();
                return true;
            },
            else => return err,
        };
        return self.hasPendingReplacements();
    }

    fn replacementGenerationCommitted(self: *GraphIndexes, name: []const u8, generation: u64) !bool {
        const db = self.dbPtr();
        const actual = db.indexGeneration(name) orelse return false;
        if (actual != generation) return error.Cancelled;
        return true;
    }

    fn driveManualReplacementReplay(self: *GraphIndexes, producer_target: u64, deadline: u64) !void {
        const db = self.dbPtr();
        // A wait observes accepted work even when this owner cannot choose
        // local effects. Leave denied/ordered work queued, as the replacement
        // scheduler does, rather than turning the observation into a write.
        if (db_mod.monotonicTimeNs() >= deadline or !try db.canRunLocalGraphEndpointCleanup()) return;
        try db.enforceReplicationWriteGate();
        if (db.enrichment_runtime) |runtime| {
            if (!db.local_execution.optional_runtime_workers_enabled and !runtime.isStarted() and producer_target > runtime.stats().applied_sequence) {
                // The enrichment guard uses its own real-time clock; the API
                // wait deadline is monotonic. Convert the remaining duration.
                const remaining = deadline -| db_mod.monotonicTimeNs();
                const provider_deadline = runtime.deadline_clock.nowRealtimeNs() +| remaining;
                runtime.catchUpUntilWithVisibilityDeadline(producer_target, .none, provider_deadline) catch |err| switch (err) {
                    error.EnrichmentWaitTimeout, error.EnrichmentRetryInProgress => return,
                    else => return err,
                };
            }
        }
        if (db_mod.monotonicTimeNs() >= deadline) return;
        if (db.hasConfiguredResolvers()) {
            if (db.resolution_runtime == null) try db.initResolutionRuntime();
            const resolution = db.resolution_runtime.?;
            resolution.notifySequence(db.core.nextDerivedSequence());
            if ((!db.local_execution.optional_runtime_workers_enabled or !db.resolver_workers_enabled) and !resolution.worker_started.load(.acquire)) try resolution.catchUpOnce(deadline);
        }
    }

    /// Wait for the generation returned by registration, including across
    /// caller-managed wait quanta. Reusing its name never completes this wait.
    pub fn waitReplacement(self: *GraphIndexes, name: []const u8, generation: u64, timeout_ms: u64) !void {
        const db = self.dbPtr();
        if (generation == 0) return error.Cancelled;
        const deadline = db_mod.monotonicTimeNs() +| timeout_ms *| std.time.ns_per_ms;
        while (true) {
            // Publication appends the new runtime entry before committing the
            // catalog. Its exclusive catalog lease spans commit and retirement;
            // a shared observation cannot acknowledge that provisional entry.
            if (try self.replacementGenerationCommitted(name, generation)) return;
            if (builtin.is_test) if (DB.test_graph_replacement_wait_observation) |hook| try hook(db);
            const jobs = try self.replacementStatusAlloc(db.alloc);
            defer jobs.deinit();
            const pending = for (jobs.value) |job| {
                if (std.mem.eql(u8, job.name, name)) break job;
            } else {
                // The swap may have removed the marker after the first
                // catalog observation. Missing work alone is not cancellation.
                if (try self.replacementGenerationCommitted(name, generation)) return;
                return error.Cancelled;
            };
            if (pending.generation != generation) return error.Cancelled;
            if (pending.phase == .failed) return error.IndexReplacementFailed;
            if (db_mod.monotonicTimeNs() >= deadline) {
                if (try self.replacementGenerationCommitted(name, generation)) return;
                return error.Busy;
            }
            // A named wait advances its own reservation, avoiding unrelated
            // corpus builds. Recheck its generation under scheduler ownership
            // so cancellation/name reuse cannot redirect this quantum.
            try self.driveManualReplacementReplay(pending.producer_replay_target, deadline);
            _ = try self.advanceReplacementForIdentityUntil(deadline, .{ .name = name, .generation = generation }, .manual);
            // The control IO also exists for manual/inline runtimes. Never
            // spin when there is no independent background IO runtime.
            try db.core.index_manager.checkpointIo().sleep(.fromMilliseconds(1), .awake);
        }
    }

    /// Called only by a Lite owner holding its live coordinator and path
    /// publication fence. Producer replay runs in the separate detached turn.
    pub fn finishBackgroundReplacement(self: *GraphIndexes, name: []const u8, generation: u64) !bool {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("finishBackgroundGraphReplacement", .{ db, name, generation });
        return self.advanceReplacementForIdentityUntil(null, .{ .name = name, .generation = generation }, .cutover);
    }
};
