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
    cleanup_owner: @import("../graph_cleanup_owner.zig").Owner = .{},
    metrics: Metrics = .{},
    replacement: Replacement = .{},

    /// Graph metric materialization: the optional runtime and idle-time maintenance settings.
    pub const Metrics = struct {
        owner: @import("../local_runtime_owner.zig").RuntimeOwner(db_mod.graph_metric_runtime_mod.GraphMetricRuntime) = .{},
        runtime: ?*db_mod.graph_metric_runtime_mod.GraphMetricRuntime = null,
        idle_maintenance: db_mod.OpenOptions.GraphMetricIdleMaintenanceMode = .auto,
        idle_planned_options: db_mod.index_manager_mod.IndexManager.GraphMetricPlannedMaintenanceOptions = .{},
        idle_auto_options: db_mod.index_manager_mod.IndexManager.GraphMetricPlannedAutoIdleOptions = .{},
        idle_degree_canary_options: db_mod.index_manager_mod.IndexManager.GraphMetricDegreeCanaryOptions = .{},
    };

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
                if (!try db.indexes.graph.canRunLocalEndpointCleanup()) continue;
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
        if (db_mod.monotonicTimeNs() >= deadline or !try db.indexes.graph.canRunLocalEndpointCleanup()) return;
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

    pub fn initMetricRuntime(self: *GraphIndexes, cfg: db_mod.graph_metric_runtime_mod.Config) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("initOptionalGraphMetricRuntime", .{ db, cfg });
        if (comptime builtin.os.tag == .freestanding) return;
        if (!db.start_index_workers) return;
        // Manual backends with only filesystem authority cannot own a
        // background worker. Their caller drives maintenance explicitly.
        if (db.backend_runtime.io() == null) return;
        const resources = db.core.asyncResources();
        self.metrics.owner = try @TypeOf(self.metrics.owner).create(db.runtime_alloc, .{
            db.runtime_alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            db.backend_runtime,
            cfg.forDbOwner(),
        });
        const runtime = self.metrics.owner.runtime.?;
        self.metrics.runtime = runtime;
        resources.index_manager.graph_metric_notify = .{ .ptr = runtime, .notify = db_mod.graph_metric_runtime_mod.GraphMetricRuntime.notifyOpaque };
    }

    pub fn drainStandaloneEndpointCleanup(self: *GraphIndexes) !bool {
        return self.drainStandaloneEndpointCleanupWithOptions(.{}, std.math.maxInt(usize));
    }

    fn drainStandaloneEndpointCleanupWithOptions(self: *GraphIndexes, opts: db_mod.BatchExecutionOptions, max_pages: usize) !bool {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return false;
        // Most batches create no endpoint jobs. Avoid authority probes and
        // maintenance admission altogether on their completion path.
        if (!try db.core.store.hasGraphEndpointCleanup()) return false;
        if (!try self.canRunLocalEndpointCleanup()) return false;
        var pages: usize = 0;
        while (pages < max_pages and try db.core.store.hasGraphEndpointCleanup()) {
            if (!try self.canRunLocalEndpointCleanup()) break;
            // The primary mutation is already durable. Cancellation can leave
            // queued maintenance for the resident owner, but must never interrupt
            // a page halfway through its atomic commit. Explicit drains use .none.
            if (opts.visibility_cancellation.isCancelled()) return error.EnrichmentWaitCanceled;
            try db.batchInternal(.{ .graph_endpoint_cleanup = true, .sync_level = .write }, null, .{
                .document_child_range_dispatcher = opts.document_child_range_dispatcher,
                .committed_batch_effects_observer = opts.committed_batch_effects_observer,
            });
            pages += 1;
        }
        return pages != 0;
    }

    /// Shared completion for ordinary, profiled, callback and transaction
    /// batches, after the serialized primary apply section has finished.
    /// Replicated apply must only execute its exact command; its owner drives
    /// cleanup independently through subsequent ordered Raft entries.
    pub fn finishBatchEndpointCleanup(self: *GraphIndexes, req: db_mod.types.BatchRequest, opts: db_mod.BatchExecutionOptions) !void {
        const db = self.dbPtr();
        if (req.graph_endpoint_cleanup or !opts.wait_for_sync_level or
            opts.ordered_apply_receipt != null or opts.replication_applied_lsn_marker != null or
            opts.native_initial_child_entry != null or opts.restore_staging != null or
            req.restore_staging_scope != null or req.restore_staging != null) return;
        // Weak sync levels pay for at most one bounded maintenance page.
        // Durable jobs continue through the resident scheduler; full_index
        // and explicit maintenance retain complete cleanup guarantees.
        const max_pages: usize = if (req.sync_level == .full_index) std.math.maxInt(usize) else 1;
        const drained = try self.drainStandaloneEndpointCleanupWithOptions(opts, max_pages);
        // A resident worker can finish before the foreground queue probe.
        // Deletes/transforms must still include its newly committed replay cut.
        if (req.sync_level == .full_index and (drained or req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0))
            try db.waitForCurrentSyncLevelWithCancellation(.full_index, opts.visibility_cancellation);
    }

    /// Local maintenance follows the same captured write admission as user writes.
    /// A retained standby/fenced generation may open and replay exact commands,
    /// but cannot choose cleanup effects or mutate its local directory on its own.
    fn endpointCleanupWriteAuthority(self: *GraphIndexes) !bool {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return false;
        return if (db.local_execution.replication_write_gate) |gate| gate.allowsBackgroundWrite() else true;
    }

    fn canRunLocalEndpointCleanup(self: *GraphIndexes) !bool {
        const db = self.dbPtr();
        if (!try self.endpointCleanupWriteAuthority()) return false;
        return !try db.maintenanceRequiresOrderedApply();
    }

    pub fn startEndpointCleanupWorker(self: *GraphIndexes) void {
        const db = self.dbPtr();
        if (comptime builtin.os.tag == .freestanding) return;
        if (!db.stable_address or !db.local_execution.optional_runtime_workers_enabled or db.open_mode != .writer) return;
        if (!(self.canRunLocalEndpointCleanup() catch false)) return;
        self.cleanup_owner.start(self.cleanupPort());
    }

    fn cleanupPort(self: *GraphIndexes) @import("../graph_cleanup_owner.zig").Owner.Port {
        const db = self.dbPtr();
        return .{ .ptr = db, .runtime = db.backend_runtime, .run = cleanupPage };
    }

    fn cleanupPage(ptr: *anyopaque) !bool {
        const db: *DB = @ptrCast(@alignCast(ptr));
        return db.indexes.graph.runStandaloneEndpointCleanupStep();
    }

    pub fn stopEndpointCleanupWorker(self: *GraphIndexes) void {
        const db = self.dbPtr();
        self.cleanup_owner.stop(db.backend_runtime.io());
    }

    fn endpointCleanupWorkerStep(self: *GraphIndexes) ?u64 {
        return self.cleanup_owner.step(self.cleanupPort());
    }

    /// Every enqueue path is serviced independently of subsequent foreground
    /// writes. Each scheduler turn commits at most one bounded page.
    pub fn runStandaloneEndpointCleanupStep(self: *GraphIndexes) !bool {
        const db = self.dbPtr();
        if (!try self.canRunLocalEndpointCleanup()) return false;
        if (!try db.core.store.hasGraphEndpointCleanup()) {
            // A ready directory needs no work. Idle owners must not contend
            // with user mutations for the apply lock every scheduler turn.
            if (try db.core.store.graphIncomingDirectoryReady()) return false;
            try db.lockApplyForPortableRuntime();
            defer db.core.unlockApply();
            if (!try self.canRunLocalEndpointCleanup()) return false;
            if (db.core.index_manager.hasGraphIndexes() and !try db.core.store.graphIncomingDirectoryReady())
                return !try db.core.store.backfillGraphIncomingDirectoryPage();
            return false;
        }
        try db.batchInternal(.{ .graph_endpoint_cleanup = true, .sync_level = .write }, null, .{});
        return true;
    }

    /// The leader selects exact identities before proposal. Followers apply
    /// these afterimages rather than consulting their local directory progress.
    pub fn prepareEndpointCleanupBatch(self: *GraphIndexes, alloc: Allocator) !?DB.GraphEndpointCleanupBatch {
        const db = self.dbPtr();
        if (!try self.endpointCleanupWriteAuthority()) return null;
        if (!try db.core.store.hasGraphEndpointCleanup() and try db.core.store.graphIncomingDirectoryReady()) return null;
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        if (!try self.endpointCleanupWriteAuthority()) return null;
        if (!try db.core.store.hasGraphEndpointCleanup()) {
            // Build local directory coverage ahead of future endpoint deletes,
            // without proposing empty Raft maintenance entries.
            if (db.core.index_manager.hasGraphIndexes() and !try db.core.store.graphIncomingDirectoryReady())
                _ = try db.core.store.backfillGraphIncomingDirectoryPage();
            return null;
        }
        var page = (try db.core.store.prepareGraphEndpointCleanupPage(alloc)) orelse return null;
        errdefer page.deinit();
        const replay_writes = try db_mod.graphCleanupReplayWritesAlloc(alloc, page);
        errdefer alloc.free(replay_writes);
        return .{ .page = page, .graph_deletes = try db_mod.graphEndpointCleanupDeletesAlloc(alloc, page), .replay_writes = replay_writes };
    }

    pub fn requireEndpointCleanupReady(self: *GraphIndexes) !void {
        const db = self.dbPtr();
        if (!try db.core.store.graphEndpointCleanupBlocksReads()) return;
        return error.StorageBusy;
    }

    /// Endpoint deletion is a document mutation, including TTL and deletes
    /// prepared by another transaction. Reuse the durable shared read guards
    /// so it cannot enqueue cleanup after an inline relationship votes prepared.
    /// The guards survive restart and are released atomically with resolution.
    /// Fact-owned projections deliberately have an independent lifecycle.
    pub fn prepareEndpointDependenciesLocked(
        self: *GraphIndexes,
        alloc: Allocator,
        scratch: Allocator,
        txn_id: db_mod.types.TxnId,
        intents: []const db_mod.transactions_mod.WriteIntent,
        schema_view: ?db_mod.schema_registry_mod.SchemaView,
        predicates: *std.ArrayListUnmanaged(db_mod.transactions_mod.VersionPredicate),
    ) !void {
        const db = self.dbPtr();
        if (!db.core.hasGraphIndexes()) return;
        var manager = try db.core.initTxnManager();
        defer manager.deinit();
        var targets = std.StringHashMapUnmanaged(void).empty;
        defer targets.deinit(scratch);
        var read = try db.core.store.beginProbeTxn();
        defer read.abort();
        const owning_table: ?[]const u8 = db.local_execution.row_policy_table_name orelse (read.get(db_mod.internal_keys.graph_owning_table_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        });
        var rows = std.heap.ArenaAllocator.init(alloc);
        defer rows.deinit();
        const row_alloc = rows.allocator();
        for (intents) |intent| {
            if (db_mod.isMetadataKey(intent.key)) continue;
            const value = intent.value orelse continue;
            defer _ = rows.reset(.retain_capacity);
            var prepared: ?db_mod.mapper.PreparedRelationalWrite = if (intent.prepared_row != null) prepared_row: {
                const view = schema_view orelse return error.PreparedGenerationChanged;
                break :prepared_row try db_mod.mapper.PreparedRelationalWrite.initFromIntent(row_alloc, intent.key, value, view.validator(), view.tableSchema().*, view.physicalLayout(), intent.prepared_row);
            } else null;
            defer if (prepared) |*row| row.deinit(row_alloc);
            var parsed: ?std.json.Parsed(std.json.Value) = if (prepared == null)
                try std.json.parseFromSlice(std.json.Value, row_alloc, value, .{ .parse_numbers = false })
            else
                null;
            defer if (parsed) |*root| root.deinit();
            if (prepared) |*row| {
                // AROW columns are lazy; materialize only fields consumed by
                // graph mappings before planning durable endpoint guards.
                for (db.core.graphIndexes()) |entry| {
                    for (entry.edge_type_configs) |edge_cfg| {
                        if (edge_cfg.field_name) |name| try row.requireLogicalField(name);
                    }
                }
            }
            const root = if (prepared) |*row| row.parsedValue() else parsed.?.value;
            var extracted = if (prepared) |*row| row.takeExtracted() else try db_mod.mapper.extractWriteFromParsed(row_alloc, intent.key, value, root);
            defer extracted.deinit(row_alloc);
            try db_mod.augmentExtractedWriteWithGraphFieldEdgesParsed(db, row_alloc, intent.key, root, &extracted);
            for (extracted.graph_writes) |edge| {
                if (edge.owner_document.len != 0 and !std.mem.eql(u8, edge.owner_document, edge.source)) continue;
                if (edge.owner.len != 0 and !std.mem.eql(u8, edge.owner, edge.source)) continue;
                var table_scratch = db_mod.graph_metadata_tables.Scratch.init(row_alloc, null);
                defer table_scratch.deinit();
                const source_table = try table_scratch.table(edge.metadata_json, "source_table");
                const target_table = try table_scratch.table(edge.metadata_json, "target_table");
                if (!db_mod.graph_metadata_tables.inlineEndpointsAreLocal(source_table, target_table, owning_table)) continue;
                if (targets.contains(edge.target)) continue;
                const target = try scratch.dupe(u8, edge.target);
                // Request preparation owns all predicate bytes until prepare.
                const job = try db_mod.internal_keys.graphEndpointCleanupKeyAlloc(scratch, target);
                _ = read.get(job) catch |err| switch (err) {
                    error.NotFound => {
                        if (!try manager.hasSharedReadDependency(txn_id, target)) {
                            if (predicates.items.len >= db_mod.transactions_mod.max_read_guards_per_transaction) return error.TransactionTooLarge;
                            const version = (try db_mod.ttl_mod.readTimestamp(db.core.store, alloc, target)) orelse 0;
                            try predicates.append(alloc, .{ .key = target, .expected_version = version });
                        }
                        try targets.put(scratch, target, {});
                        continue;
                    },
                    else => return err,
                };
                return error.IntegrityTopologyBusy;
            }
        }
    }

    pub fn runMetricMaintenanceForIdle(self: *GraphIndexes) !usize {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        while (true) {
            db_mod.lockApply(db);
            if (!db.core.index_manager.graphRetirementAdmissionOpen()) {
                db.core.unlockApply();
                return error.IntegrityTopologyBusy;
            }
            const more = db.core.index_manager.runGraphOwnershipCleanupStep() catch |err| {
                db.core.unlockApply();
                return err;
            };
            db.core.unlockApply();
            if (!more) break;
        }
        // Planned maintenance uses the same catalog pins and transaction
        // fences as background workers. Never hold the ingest lock while
        // draining graph computation; graph writes may supersede a build.
        switch (self.metrics.idle_maintenance) {
            .auto => return self.runMetricPlannedAutoMaintenanceForIdle(),
            .planned => return self.drainMetricPlannedIdle(),
            else => {},
        }
        db_mod.lockApply(db);
        defer db.core.unlockApply();
        try self.requireEndpointCleanupReady();
        if (!db.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
        return switch (self.metrics.idle_maintenance) {
            .legacy => try db.core.index_manager.runGraphMetricMaintenance(),
            .planned, .auto => unreachable,
            .degree_canary => try self.runMetricDegreeCanaryMaintenanceForIdleLocked(),
        };
    }

    fn metricPlannedProgress(result: db_mod.index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult) usize {
        return result.planning_steps + result.builds_started + result.pages_completed + result.phases_advanced + result.published;
    }

    fn drainMetricPlannedIdle(self: *GraphIndexes) !usize {
        const db = self.dbPtr();
        const result = try db.core.index_manager.runGraphMetricPlannedMaintenance(self.metrics.idle_planned_options);
        if (result.budget_exhausted) return error.RunUntilIdleDidNotConverge;
        return metricPlannedProgress(result);
    }

    fn runMetricPlannedAutoMaintenanceForIdle(self: *GraphIndexes) !usize {
        const db = self.dbPtr();
        const result = try db.core.index_manager.runGraphMetricPlannedAutoMaintenance(
            self.metrics.idle_planned_options,
            self.metrics.idle_auto_options,
        );
        if (result.budget_exhausted) return error.RunUntilIdleDidNotConverge;
        const progressed = metricPlannedProgress(result);
        const after = try db.core.index_manager.graphMetricPlannedAutoIdleDecision(self.metrics.idle_auto_options);
        if (!after.shouldRunPlanned() and after.ineligible_queued != 0) {
            // Admission caps must not silently select unlimited local compute.
            return error.RunUntilIdleDidNotConverge;
        }
        return progressed;
    }

    fn runMetricDegreeCanaryMaintenanceForIdleLocked(self: *GraphIndexes) !usize {
        const db = self.dbPtr();
        const decision = try db.core.index_manager.graphMetricDegreeCanaryDecision(self.metrics.idle_degree_canary_options);
        if (decision.shouldRunPlanned()) return try self.drainMetricPlannedIdle();
        if (decision.active_degree_builds != 0 or decision.blocked_active_non_degree != 0) {
            return error.RunUntilIdleDidNotConverge;
        }
        return try db.core.index_manager.runGraphMetricMaintenance();
    }

    pub fn runMetricPlannedMaintenanceForIdle(
        self: *GraphIndexes,
        options: db_mod.index_manager_mod.IndexManager.GraphMetricPlannedMaintenanceOptions,
    ) !db_mod.index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        db_mod.lockApply(db);
        defer db.core.unlockApply();
        try self.requireEndpointCleanupReady();
        if (!db.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
        return try db.core.index_manager.runGraphMetricPlannedMaintenance(options);
    }

    pub fn runMetricServiceMaintenanceJsonAlloc(self: *GraphIndexes, alloc: Allocator, body: []const u8) ![]u8 {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("runGraphMetricServiceMaintenanceJsonAlloc", .{ db, alloc, body });
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        var parsed = std.json.parseFromSlice(DB.GraphMetricServiceMaintenanceRequest, alloc, if (body.len == 0) "{}" else body, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return error.InvalidGraphMetricRuntimeConfig;
        defer parsed.deinit();

        var manual_clock = db_mod.platform_clock.ManualClock{};
        if (parsed.value.now_ms) |now_ms| manual_clock.setRealtimeNs(now_ms *| std.time.ns_per_ms);
        const resources = db.core.asyncResources();
        var runtime = try db_mod.graph_metric_runtime_mod.GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            db.backend_runtime,
            .{
                .start_background_loop = false,
                .role = parsed.value.role,
                .runtime_id = parsed.value.runtime_id,
                .lease_owned = parsed.value.lease_owned,
                .owner_id = parsed.value.owner_id,
                .lease_ttl_ms = parsed.value.lease_ttl_ms,
                .coordinator_start_background_builds = parsed.value.start_background_builds,
                .planned_options = .{
                    .worker_id = parsed.value.worker_id orelse "",
                    .worker_ids = parsed.value.worker_ids orelse &.{},
                    .max_rounds = parsed.value.max_rounds,
                    .max_metrics_per_round = parsed.value.max_metrics_per_round,
                    .max_pages_per_round = parsed.value.max_pages_per_round,
                },
                .clock = if (parsed.value.now_ms != null) manual_clock.clock() else db_mod.platform_clock.Clock.real(),
            },
        );
        var preserve_lease = false;
        defer if (preserve_lease) runtime.deinitPreserveLease() else runtime.deinit();

        if (parsed.value.action == .release) {
            const released = try runtime.ownership.releaseHeldLease();
            var current_lease = try runtime.ownership.loadLease(alloc);
            defer if (current_lease) |*lease| db_mod.lease_mod.deinitRecord(alloc, lease);
            var runtime_stats = runtime.stats();
            runtime_stats.shutdown = true;
            return try std.json.Stringify.valueAlloc(alloc, .{
                .released = released,
                .lease_owner_id_hash = if (current_lease) |lease| db_mod.graph_metric_runtime_mod.identityHash(lease.owner_id) else 0,
                .lease_expires_at_ms = if (current_lease) |lease| lease.expires_at_ms else 0,
                .stats = runtime_stats,
            }, .{ .emit_null_optional_fields = false });
        }

        const result: db_mod.index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult = if (parsed.value.action == .tick) try runtime.runOnceDetailed() else .{};
        const runtime_stats = runtime.stats();
        preserve_lease = parsed.value.preserve_lease_after_tick and parsed.value.lease_owned and parsed.value.action == .tick and runtime_stats.has_lease;
        return try std.json.Stringify.valueAlloc(alloc, .{
            .result = result,
            .stats = runtime_stats,
        }, .{ .emit_null_optional_fields = false });
    }

    pub fn refreshMetric(self: *GraphIndexes, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !db_mod.types.GraphMetricStatus {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("refreshGraphMetric", .{ db, alloc, index_name, metric_name });
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        db_mod.lockApply(db);
        defer db.core.unlockApply();
        try self.requireEndpointCleanupReady();
        if (!db.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
        const entry = db.core.graphIndex(index_name) orelse return error.IndexNotFound;
        try entry.index.enableGraphMetric(metric_name);
        var status = try entry.index.runGraphMetric(metric_name);
        defer status.deinit(entry.index.alloc);
        const cloned = try DB.cloneGraphMetricStatusFromGraph(alloc, status);
        if (self.metrics.runtime) |runtime| runtime.notify();
        return cloned;
    }

    pub fn rebuildMetric(self: *GraphIndexes, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !db_mod.types.GraphMetricStatus {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("rebuildGraphMetric", .{ db, alloc, index_name, metric_name });
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        db_mod.lockApply(db);
        defer db.core.unlockApply();
        try self.requireEndpointCleanupReady();
        if (!db.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
        const entry = db.core.graphIndex(index_name) orelse return error.IndexNotFound;
        try entry.index.enableGraphMetric(metric_name);
        var status = try entry.index.runGraphMetric(metric_name);
        defer status.deinit(entry.index.alloc);
        const cloned = try DB.cloneGraphMetricStatusFromGraph(alloc, status);
        if (self.metrics.runtime) |runtime| runtime.notify();
        return cloned;
    }

    /// Durably enqueue metric work and return immediately. Public control-plane
    /// actions use this path so request latency is independent of graph size;
    /// the bounded maintenance runtime performs and checkpoints the build.
    pub fn scheduleMetricBuild(
        self: *GraphIndexes,
        alloc: Allocator,
        index_name: []const u8,
        metric_name: []const u8,
        force: bool,
    ) !db_mod.types.GraphMetricStatus {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("scheduleGraphMetricBuild", .{ db, alloc, index_name, metric_name, force });
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        const owned_status = blk: {
            db_mod.lockApply(db);
            defer db.core.unlockApply();
            try self.requireEndpointCleanupReady();
            if (!db.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
            const entry = db.core.graphIndex(index_name) orelse return error.IndexNotFound;
            var current = try entry.index.graphMetricStatus(metric_name);
            defer current.deinit(entry.index.alloc);
            if (current.state == .disabled) {
                try entry.index.enableGraphMetric(metric_name);
                current.deinit(entry.index.alloc);
                current = try entry.index.graphMetricStatus(metric_name);
            }
            if (!force and current.state == .fresh) {
                break :blk try DB.cloneGraphMetricStatusFromGraph(alloc, current);
            }
            const target_generation = @max(current.edge_generation, current.target_edge_generation);
            // Force means "build another immutable score epoch", not
            // "unpublish first". Repeated requests remain idempotent while a
            // build is active, and readers keep the verified prior epoch until
            // the new pointer is atomically published.
            var scheduled = entry.index.queueGraphMetricBuild(metric_name, target_generation) catch |err| switch (err) {
                // A newer edge snapshot may be queued while the prior bounded
                // build is still active. Treat repeated control-plane actions
                // as accepted and expose the active/queued generations in the
                // returned status instead of turning a safe retry into a 500.
                error.GraphMetricBuildAlreadyRunning => break :blk try DB.cloneGraphMetricStatusFromGraph(alloc, current),
                else => return err,
            };
            defer scheduled.deinit(db.core.index_manager.alloc);
            break :blk try DB.cloneGraphMetricStatusFromGraph(alloc, scheduled);
        };
        if (self.metrics.runtime) |runtime| runtime.notify();
        return owned_status;
    }

    pub fn deleteMetricMaterialization(self: *GraphIndexes, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !db_mod.types.GraphMetricStatus {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        db_mod.lockApply(db);
        defer db.core.unlockApply();
        try self.requireEndpointCleanupReady();
        if (!db.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
        const entry = db.core.graphIndex(index_name) orelse return error.IndexNotFound;
        try entry.index.deleteGraphMetricMaterialization(metric_name);
        var status = try entry.index.graphMetricStatus(metric_name);
        defer status.deinit(entry.index.alloc);
        const cloned = try DB.cloneGraphMetricStatusFromGraph(alloc, status);
        if (self.metrics.runtime) |runtime| runtime.notify();
        return cloned;
    }

    pub fn pauseMetricMaintenance(self: *GraphIndexes, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !db_mod.types.GraphMetricStatus {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("pauseGraphMetricMaintenance", .{ db, alloc, index_name, metric_name });
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        db_mod.lockApply(db);
        defer db.core.unlockApply();
        try self.requireEndpointCleanupReady();
        if (!db.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
        const entry = db.core.graphIndex(index_name) orelse return error.IndexNotFound;
        var status = try entry.index.pauseGraphMetricMaintenance(metric_name);
        defer status.deinit(entry.index.alloc);
        return try DB.cloneGraphMetricStatusFromGraph(alloc, status);
    }

    pub fn resumeMetricMaintenance(self: *GraphIndexes, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !db_mod.types.GraphMetricStatus {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("resumeGraphMetricMaintenance", .{ db, alloc, index_name, metric_name });
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        db_mod.lockApply(db);
        defer db.core.unlockApply();
        try self.requireEndpointCleanupReady();
        if (!db.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
        const entry = db.core.graphIndex(index_name) orelse return error.IndexNotFound;
        var status = try entry.index.resumeGraphMetricMaintenance(metric_name);
        defer status.deinit(entry.index.alloc);
        return try DB.cloneGraphMetricStatusFromGraph(alloc, status);
    }

    pub fn ensureMetricPlannedBuild(
        self: *GraphIndexes,
        alloc: Allocator,
        index_name: []const u8,
        metric_name: []const u8,
        target_generation: u64,
    ) !db_mod.types.GraphMetricStatus {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("ensureGraphMetricPlannedBuild", .{ db, alloc, index_name, metric_name, target_generation });
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        db_mod.lockApply(db);
        defer db.core.unlockApply();
        try self.requireEndpointCleanupReady();
        if (!db.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
        var status = try db.core.index_manager.ensureGraphMetricPlannedBuild(index_name, metric_name, target_generation);
        defer status.deinit(db.core.index_manager.alloc);
        return try DB.cloneGraphMetricStatusFromGraph(alloc, status);
    }

    pub fn runMetricPlannedWorkerPageStep(self: *GraphIndexes, index_name: []const u8, metric_name: []const u8, worker_id: []const u8) !db_mod.graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        db_mod.lockApply(db);
        defer db.core.unlockApply();
        try self.requireEndpointCleanupReady();
        if (!db.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
        return try db.core.index_manager.runGraphMetricPlannedWorkerPageStep(index_name, metric_name, worker_id);
    }

    pub fn runMetricPlannedWorkerPageStepAt(self: *GraphIndexes, index_name: []const u8, metric_name: []const u8, worker_id: []const u8, now_ms: u64) !db_mod.graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("runGraphMetricPlannedWorkerPageStepAt", .{ db, index_name, metric_name, worker_id, now_ms });
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        db_mod.lockApply(db);
        defer db.core.unlockApply();
        try self.requireEndpointCleanupReady();
        if (!db.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
        return try db.core.index_manager.runGraphMetricPlannedWorkerPageStepAt(index_name, metric_name, worker_id, now_ms);
    }

    fn runMetricPlannedCoordinatorStep(self: *GraphIndexes, index_name: []const u8, metric_name: []const u8) !db_mod.graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        db_mod.lockApply(db);
        defer db.core.unlockApply();
        try self.requireEndpointCleanupReady();
        if (!db.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
        return try db.core.index_manager.runGraphMetricPlannedCoordinatorStep(index_name, metric_name);
    }

    pub fn runMetricPlannedCoordinatorStepAt(self: *GraphIndexes, index_name: []const u8, metric_name: []const u8, now_ms: u64) !db_mod.graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("runGraphMetricPlannedCoordinatorStepAt", .{ db, index_name, metric_name, now_ms });
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        db_mod.lockApply(db);
        defer db.core.unlockApply();
        try self.requireEndpointCleanupReady();
        if (!db.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
        return try db.core.index_manager.runGraphMetricPlannedCoordinatorStepAt(index_name, metric_name, now_ms);
    }

    pub fn failMetricPlannedBuild(self: *GraphIndexes, alloc: Allocator, index_name: []const u8, metric_name: []const u8, err: anyerror) !db_mod.types.GraphMetricStatus {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("failGraphMetricPlannedBuild", .{ db, alloc, index_name, metric_name, @errorName(err) });
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        db_mod.lockApply(db);
        defer db.core.unlockApply();
        try self.requireEndpointCleanupReady();
        if (!db.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
        var status = try db.core.index_manager.failGraphMetricPlannedBuild(index_name, metric_name, err);
        defer status.deinit(db.core.index_manager.alloc);
        return try DB.cloneGraphMetricStatusFromGraph(alloc, status);
    }

    pub fn runMetricPlannedDrain(self: *GraphIndexes, alloc: Allocator, index_name: []const u8, metric_name: []const u8, target_generation: u64, options: db_mod.graph_mod.GraphIndex.GraphMetricPlannedDrainOptions) !db_mod.types.GraphMetricStatus {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("runGraphMetricPlannedDrain", .{ db, alloc, index_name, metric_name, target_generation, options });
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        db_mod.lockApply(db);
        defer db.core.unlockApply();
        try self.requireEndpointCleanupReady();
        if (!db.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
        var status = try db.core.index_manager.runGraphMetricPlannedDrain(index_name, metric_name, target_generation, options);
        defer status.deinit(db.core.index_manager.alloc);
        return try DB.cloneGraphMetricStatusFromGraph(alloc, status);
    }

    pub fn runMetricPlannedCoordinatorSweep(self: *GraphIndexes, options: db_mod.index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepOptions) !db_mod.index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        // Planned graph work is generation-fenced and storage-transactional.
        // IndexManager pins catalog lifetime without blocking foreground apply.
        return try db.core.index_manager.runGraphMetricPlannedCoordinatorSweep(options);
    }

    pub fn runMetricPlannedWorkerSweep(self: *GraphIndexes, options: db_mod.index_manager_mod.IndexManager.GraphMetricPlannedWorkerSweepOptions) !db_mod.index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        return try db.core.index_manager.runGraphMetricPlannedWorkerSweep(options);
    }

    pub fn populateMetricStatusStats(alloc: Allocator, item: *db_mod.types.DBIndexStats, graph_index: *db_mod.graph_mod.GraphIndex) !void {
        if (graph_index.metric_configs.len == 0) return;
        const statuses = try alloc.alloc(db_mod.types.GraphMetricStatus, graph_index.metric_configs.len);
        var initialized: usize = 0;
        errdefer {
            for (statuses[0..initialized]) |*status| status.deinit(alloc);
            alloc.free(statuses);
        }
        for (graph_index.metric_configs, 0..) |cfg, i| {
            var status = try graph_index.graphMetricStatus(cfg.name);
            defer status.deinit(graph_index.alloc);
            statuses[i] = try DB.cloneGraphMetricStatusFromGraph(alloc, status);
            initialized += 1;
        }
        item.graph_metric_status = statuses;
    }

    pub fn metricRuntimeStats(self: *GraphIndexes) db_mod.types.GraphMetricRuntimeStats {
        const runtime = self.metrics.runtime orelse return .{};
        const runtime_snapshot = runtime.stats();
        const total = runtime_snapshot.total_result;
        const last = runtime_snapshot.last_result;
        return .{
            .enabled = runtime_snapshot.enabled,
            .role = switch (runtime_snapshot.role) {
                .combined => .combined,
                .coordinator => .coordinator,
                .worker => .worker,
                .worker_pool => .worker_pool,
            },
            .runtime_id_hash = runtime_snapshot.runtime_id_hash,
            .owner_id_hash = runtime_snapshot.owner_id_hash,
            .lease_key_hash = runtime_snapshot.lease_key_hash,
            .worker_id_hash = runtime_snapshot.worker_id_hash,
            .worker_count = @intCast(runtime_snapshot.worker_count),
            .lease_owned = runtime_snapshot.lease_owned,
            .has_lease = runtime_snapshot.has_lease,
            .acquisition_count = runtime_snapshot.acquisition_count,
            .takeover_count = runtime_snapshot.takeover_count,
            .lease_acquire_failures = runtime_snapshot.lease_acquire_failures,
            .lost_leases = runtime_snapshot.lost_leases,
            .last_acquired_ms = runtime_snapshot.last_acquired_ms,
            .lease_expires_at_ms = runtime_snapshot.lease_expires_at_ms,
            .lease_renew_after_ms = runtime_snapshot.lease_renew_after_ms,
            .renewal_count = runtime_snapshot.renewal_count,
            .started = runtime_snapshot.started,
            .shutdown = runtime_snapshot.shutdown,
            .notified = runtime_snapshot.notified,
            .ticks_started = runtime_snapshot.ticks_started,
            .ticks_completed = runtime_snapshot.ticks_completed,
            .durable_progress_ticks = runtime_snapshot.durable_progress_ticks,
            .idle_ticks = runtime_snapshot.idle_ticks,
            .error_ticks = runtime_snapshot.error_ticks,
            .last_error_name = if (runtime_snapshot.last_error_name) |name| db_mod.types.RuntimeErrorName.init(name) else null,
            .total_metrics_scanned = @intCast(total.metrics_scanned),
            .total_active_builds = @intCast(total.active_builds),
            .total_builds_started = @intCast(total.builds_started),
            .total_worker_steps = @intCast(total.worker_steps),
            .total_coordinator_steps = @intCast(total.coordinator_steps),
            .total_retired_input_records = @intCast(total.retired_input_records),
            .total_pages_claimed = @intCast(total.pages_claimed),
            .total_pages_completed = @intCast(total.pages_completed),
            .total_phases_advanced = @intCast(total.phases_advanced),
            .total_published = @intCast(total.published),
            .total_failed_builds = @intCast(total.failed_builds),
            .last_metrics_scanned = @intCast(last.metrics_scanned),
            .last_active_builds = @intCast(last.active_builds),
            .last_builds_started = @intCast(last.builds_started),
            .last_worker_steps = @intCast(last.worker_steps),
            .last_coordinator_steps = @intCast(last.coordinator_steps),
            .last_retired_input_records = @intCast(last.retired_input_records),
            .last_pages_claimed = @intCast(last.pages_claimed),
            .last_pages_completed = @intCast(last.pages_completed),
            .last_phases_advanced = @intCast(last.phases_advanced),
            .last_published = @intCast(last.published),
            .last_failed_builds = @intCast(last.failed_builds),
            .last_budget_exhausted = last.budget_exhausted,
        };
    }
};
