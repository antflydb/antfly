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
                    var activity = try db.enrichment.acquireResolverCatalogActivity(true);
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
                    if (db.enrichment.resolution.runtime) |runtime| runtime.notifyBackfill();
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

        var activity = try db.enrichment.acquireResolverCatalogActivity(true);
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
            if (db.enrichment.resolution.runtime) |runtime| runtime.notifyBackfill();
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
            if (db.enrichment.runtime != null) break :blk false;
            var config: db_mod.enrichment_runtime_mod.Config = .{ .enable_without_producers = true };
            try db.enrichment.initRuntime(&config);
            break :blk true;
        };
        if (created) try db.enrichment.restartAfterStructuralMutation("graph replacement replay", "");
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
                break :blk try db.enrichment.appendGeneratedEnrichmentsFromStoredDocsPage(a, producers, true, producer_resume, 32, .{
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
            const runtime = db.enrichment.runtime orelse return error.EnrichmentRuntimeUnavailable;
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
        if (db.enrichment.runtime) |runtime| {
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
            if (db.enrichment.resolution.runtime == null) try db.enrichment.initResolutionRuntime();
            const resolution = db.enrichment.resolution.runtime.?;
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

    pub fn runMetricPlannedCoordinatorStep(self: *GraphIndexes, index_name: []const u8, metric_name: []const u8) !db_mod.graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
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

test "db graph endpoint cleanup pages are replicated bounded and restartable" {
    var checked_allocator = try @import("antfly_platform").allocator.CheckedTestAllocator.init();
    defer checked_allocator.deinit();
    const alloc = checked_allocator.allocator();
    var directory = try db_mod.TestDirectory.init("endpoint-cleanup-pages");
    defer directory.cleanup();
    var follower_directory = try db_mod.TestDirectory.init("endpoint-cleanup-follower");
    defer follower_directory.cleanup();
    var follower = try DB.open(alloc, std.mem.span(follower_directory.path().ptr), .{});
    defer follower.close();
    try follower.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    {
        var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const writes = try scratch.alloc(db_mod.types.GraphEdgeWrite, 600);
        for (writes, 0..) |*write, i| write.* = .{ .index_name = "g", .source = try std.fmt.allocPrint(scratch, "source:{d:0>4}", .{i}), .target = "hub", .edge_type = "R" };
        try db_mod.graphTestApplyOrdered(&db, .{ .graph_writes = writes, .sync_level = .full_index }, .{ .term = 1, .index = 1 });
        try db_mod.graphTestApplyOrdered(&follower, .{ .graph_writes = writes, .sync_level = .write }, .{ .term = 1, .index = 1 });
        try db_mod.graphTestApplyOrdered(&follower, .{ .deletes = &.{"hub"}, .sync_level = .write }, .{ .term = 1, .index = 2 });
        // Local directory readiness differs across replicas. The follower must
        // still apply the leader-selected identities, without replanning.
        try follower.core.store.invalidateGraphDirectories();
        try db_mod.graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"}, .sync_level = .write }, .{ .term = 1, .index = 2 });
        try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
        var archive = std.ArrayListUnmanaged(u8).empty;
        defer archive.deinit(alloc);
        try std.testing.expectError(error.StorageBusy, db_mod.portable_backup.exportPortable(alloc, db.core.store, &archive));
        var reply = (try db.lookup(alloc, "", .{ .relational_topology_json = "{\"mode\":\"graph_endpoint_cleanup\"}" })).?;
        defer reply.deinit(alloc);
        var plan = try std.json.parseFromSlice(db_mod.types.GraphEndpointCleanupStatus, alloc, reply.json, .{});
        defer plan.deinit();
        try std.testing.expect(plan.value.pending);
        try std.testing.expectEqual(@as(usize, 256), plan.value.graph_deletes.len);
        try std.testing.expectEqual(@as(usize, 0), plan.value.deletes.len);
        try std.testing.expectError(error.StorageBusy, db.getEdges(alloc, "g", "source:0000", "", .out));
        try std.testing.expectError(error.IntegrityTopologyBusy, db_mod.graphTestApplyOrdered(&db, .{ .graph_writes = &.{.{ .index_name = "g", .source = "new", .target = "hub", .edge_type = "R" }}, .sync_level = .write }, .{ .term = 1, .index = 3 }));
        var first_page = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
        defer first_page.deinit();
        try db_mod.graphTestApplyOrdered(&db, first_page.request(), .{ .term = 1, .index = 3 });
        try db_mod.graphTestApplyOrdered(&follower, first_page.request(), .{ .term = 1, .index = 3 });
        const remaining = try db.core.store.scanPrefix(alloc, db_mod.internal_keys.graph_incoming_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, remaining);
        try std.testing.expectEqual(@as(usize, 344), remaining.len);
        // An already-applied Raft entry cannot consume a second page.
        try db_mod.graphTestApplyOrdered(&db, first_page.request(), .{ .term = 1, .index = 3 });
        const unchanged = try db.core.store.scanPrefix(alloc, db_mod.internal_keys.graph_incoming_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, unchanged);
        try std.testing.expectEqual(remaining.len, unchanged.len);
    }
    {
        var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
        defer db.close();
        try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
        var second_page = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
        defer second_page.deinit();
        try db_mod.graphTestApplyOrdered(&db, second_page.request(), .{ .term = 2, .index = 4 });
        try db_mod.graphTestApplyOrdered(&follower, second_page.request(), .{ .term = 2, .index = 4 });
        try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
        var final_page = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
        defer final_page.deinit();
        try db_mod.graphTestApplyOrdered(&db, final_page.request(), .{ .term = 2, .index = 5 });
        try db_mod.graphTestApplyOrdered(&follower, final_page.request(), .{ .term = 2, .index = 5 });
        try std.testing.expect(!try follower.core.store.hasGraphEndpointCleanup());
        try follower.runUntilIdle();
        _ = try follower.rebuildGraphDerivedState();
        const follower_edges = try follower.getEdges(alloc, "g", "hub", "", .in);
        defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, follower_edges);
        try std.testing.expectEqual(@as(usize, 0), follower_edges.len);
        try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
        try db.runUntilIdle();
        _ = try db.rebuildGraphDerivedState();
        const edges = try db.getEdges(alloc, "g", "hub", "", .in);
        defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 0), edges.len);
        try db_mod.graphTestApplyOrdered(&db, .{ .graph_writes = &.{.{ .index_name = "g", .source = "new", .target = "hub", .edge_type = "R" }}, .sync_level = .full_index }, .{ .term = 2, .index = 6 });
        // Raft apply deliberately returns before derived visibility; the
        // public writer waits for its requested sync level separately.
        try db.runUntilIdle();
        const revived = try db.getEdges(alloc, "g", "hub", "", .in);
        defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, revived);
        try std.testing.expectEqual(@as(usize, 1), revived.len);
    }
}

test "db graph endpoint cleanup pages finish on constrained tables" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("constrained-cleanup");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db_mod.graphTestApplyOrdered(&db, .{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "hub", .edge_type = "R" }} }, .{ .term = 1, .index = 1 });
    // Seed the durable state produced by a coordinated document deletion.
    const job = try db_mod.internal_keys.graphEndpointCleanupKeyAlloc(alloc, "hub");
    defer alloc.free(job);
    try db.core.store.put(job, "hub");
    var cleanup = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
    defer cleanup.deinit();
    try std.testing.expectEqual(@as(usize, 1), cleanup.page.deletes.len);
    try db_mod.graphTestApplyOrdered(&db, cleanup.request(), .{ .term = 1, .index = 2 });
    try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
    try db.runUntilIdle();
    const edges = try db.getEdges(alloc, "g", "a", "", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);
    try std.testing.expectError(error.InvalidBatchRequest, db_mod.graphTestApplyOrdered(&db, .{ .graph_endpoint_cleanup = true, .graph_endpoint_cleanup_planned = true, .deletes = &.{"document"} }, .{ .term = 1, .index = 3 }));
    try std.testing.expectError(error.ForeignKeyCoordinationRequired, db_mod.graphTestApplyOrdered(&db, .{ .deletes = &.{"document"} }, .{ .term = 1, .index = 3 }));
}

test "db graph endpoint cleanup pages batch unrelated deletes without blocking graph reads" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("empty-cleanup-pages");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db_mod.graphTestApplyOrdered(&db, .{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R" }}, .sync_level = .full_index }, .{ .term = 1, .index = 1 });
    try db.runUntilIdle();
    try db.core.store.ensureGraphIncomingDirectory();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const deletes = try scratch.alloc([]const u8, 600);
    for (deletes, 0..) |*key, i| key.* = try std.fmt.allocPrint(scratch, "isolated:{d}", .{i});
    try db_mod.graphTestApplyOrdered(&db, .{ .deletes = deletes }, .{ .term = 1, .index = 2 });
    try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
    try std.testing.expect(!try db.core.store.graphEndpointCleanupBlocksReads());
    const edges = try db.getEdges(alloc, "g", "a", "", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    for ([_]usize{ 256, 256, 88 }, 0..) |size, i| {
        var page = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 0), page.graph_deletes.len);
        try std.testing.expectEqual(size, page.page.deletes.len);
        try db_mod.graphTestApplyOrdered(&db, page.request(), .{ .term = 1, .index = 3 + i });
    }
    try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
}

test "db graph endpoint cleanup pages respect raft ownership before first applied marker" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("raft-cleanup-authority");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    const job = try db_mod.internal_keys.graphEndpointCleanupKeyAlloc(alloc, "hub");
    defer alloc.free(job);
    try db.core.store.put(job, "hub");
    try std.testing.expect(!try db.indexes.graph.runStandaloneEndpointCleanupStep());
    try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
}

test "db graph endpoint cleanup pages ordinary writes do not drain directory migration" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |graph_index| {
        var directory = try db_mod.TestDirectory.init("write-migration-bounded");
        defer directory.cleanup();
        var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        if (graph_index) try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        // Seed unrelated historical rows directly: the regression exercises
        // the next DB write, not extraction/replay for the fixture corpus.
        const writes = try scratch.alloc(db_mod.docstore_mod.KVPair, 600);
        for (writes, 0..) |*write, i| {
            const key = try std.fmt.allocPrint(scratch, "doc:{d}", .{i});
            write.* = .{ .key = try db_mod.encodeStoreLookupKeyAlloc(&db, scratch, key), .value = "{}" };
        }
        try db.core.store.putBatch(writes, &.{});
        // Existing pre-migration stores lack a ready marker.
        try db.core.store.delete(db_mod.internal_keys.graph_incoming_ready_key);
        try db.batch(.{ .writes = &.{.{ .key = "new", .value = "{}" }}, .sync_level = .write });
        try std.testing.expect(!try db.core.store.graphIncomingDirectoryReady());
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, db_mod.internal_keys.graph_incoming_cursor_key));
        if (graph_index) {
            try std.testing.expect(try db.indexes.graph.runStandaloneEndpointCleanupStep());
            try std.testing.expect(!try db.core.store.graphIncomingDirectoryReady());
            const cursor = try db.core.store.get(alloc, db_mod.internal_keys.graph_incoming_cursor_key);
            alloc.free(cursor);
            // Another ordinary write cannot consume the remaining pages.
            try db.batch(.{ .writes = &.{.{ .key = "other", .value = "{}" }}, .sync_level = .write });
            try std.testing.expect(!try db.core.store.graphIncomingDirectoryReady());
            while (try db.indexes.graph.runStandaloneEndpointCleanupStep()) {}
            try std.testing.expect(try db.core.store.graphIncomingDirectoryReady());
        }
    }
}

test "db graph endpoint cleanup pages incomplete directory preserves owner retirement lifecycle" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("retirement-owner-fallback");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"v\":1}" }}, .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R" }}, .sync_level = .full_index });
    try db.batch(.{ .graph_deletes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R" }}, .sync_level = .full_index });
    const artifact = try db_mod.internal_keys.graphRelationshipArtifactKeyAlloc(alloc, "a", "g", "R", "b", "a", "");
    defer alloc.free(artifact);
    try std.testing.expect(try db.core.store.graphRelationshipRetired(artifact));
    try db.core.store.delete(db_mod.internal_keys.graph_incoming_ready_key);
    // A semantic no-op must retain the retirement even without a summary.
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"v\":1}" }}, .sync_level = .write });
    try std.testing.expect(try db.core.store.graphRelationshipRetired(artifact));
    // Only this owner's changed lifecycle removes its marker.
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"v\":2}" }}, .sync_level = .write });
    try std.testing.expect(!try db.core.store.graphRelationshipRetired(artifact));
    // Weak-sync completion may finish a small directory migration while the
    // bounded owner job remains pending. It does not enumerate owner history.
    try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
    while (try db.indexes.graph.runStandaloneEndpointCleanupStep()) {}
    try std.testing.expect(try db.core.store.graphIncomingDirectoryReady());
}

test "db graph endpoint cleanup pages progress through row policy phases without user admission" {
    const alloc = std.testing.allocator;
    for ([_]db_mod.table_catalog_mod.RowPolicyPhase{ .preparing, .active }) |phase| {
        for (0..3) |mode| {
            var directory = try db_mod.TestDirectory.init("policy-cleanup-admission");
            defer directory.cleanup();
            var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
            defer db.close();
            try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
            const edge: db_mod.types.GraphEdgeWrite = .{ .index_name = "g", .source = "a", .target = "hub", .edge_type = "R" };
            if (mode == 1)
                try db_mod.graphTestApplyOrdered(&db, .{ .graph_writes = &.{edge} }, .{ .term = 1, .index = 1 })
            else
                try db.batch(.{ .graph_writes = &.{edge}, .sync_level = .full_index });
            const job = try db_mod.internal_keys.graphEndpointCleanupKeyAlloc(alloc, "hub");
            defer alloc.free(job);
            try db.core.store.put(job, "hub");
            db.local_execution.row_policy_gate.phase.store(@backingInt(phase), .release);
            defer db.local_execution.row_policy_gate.phase.store(@backingInt(db_mod.table_catalog_mod.RowPolicyPhase.disabled), .release);
            // Maintenance must not acquire a user admission lease, including
            // during preparing. Invalid mixed commands never gain exemption.
            try std.testing.expectError(error.InvalidBatchRequest, db.batch(.{ .graph_endpoint_cleanup = true, .writes = &.{.{ .key = "user", .value = "{}" }} }));
            try std.testing.expectError(error.RowPolicyAuthenticationRequired, db.batch(.{ .writes = &.{.{ .key = "user", .value = "{}" }} }));
            try std.testing.expectError(error.RowPolicyAuthenticationRequired, db_mod.graphTestApplyOrdered(&db, .{ .graph_writes = &.{edge} }, .{ .term = 1, .index = 2 }));
            switch (mode) {
                0 => try std.testing.expect(try db.indexes.graph.runStandaloneEndpointCleanupStep()),
                1 => {
                    var page = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
                    defer page.deinit();
                    try db_mod.graphTestApplyOrdered(&db, page.request(), .{ .term = 1, .index = 2 });
                },
                2 => try db.batchInternal(.{ .graph_endpoint_cleanup = true, .sync_level = .write }, null, .{ .replication_applied_lsn_marker = 1, .bypass_replication_write_gate = true }),
                else => unreachable,
            }
            try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
            // Authentication failures and cleanup cannot create a user row.
            try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "user"));
        }
    }
}

test "db graph endpoint cleanup pages weak sync has one page budget and full index drains" {
    const alloc = std.testing.allocator;
    const Capture = struct {
        calls: usize = 0,
        fn observe(ptr: *anyopaque, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }
        fn dispatch(_: *anyopaque, _: Allocator, _: db_mod.DocumentArtifactChildRangeDispatch) !void {}
    };
    for ([_]db_mod.types.SyncLevel{ .propose, .write, .full_text, .enrichments, .full_index }) |level| {
        var directory = try db_mod.TestDirectory.init("cleanup-foreground-budget");
        defer directory.cleanup();
        var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        try db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{}" }}, .sync_level = .write });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const jobs = try scratch.alloc(db_mod.docstore_mod.KVPair, 600);
        for (jobs, 0..) |*job, i| {
            const endpoint = try std.fmt.allocPrint(scratch, "isolated:{d}", .{i});
            job.* = .{ .key = try db_mod.internal_keys.graphEndpointCleanupKeyAlloc(scratch, endpoint), .value = endpoint };
        }
        try db.core.store.putBatch(jobs, &.{});
        var capture = Capture{};
        try db.batchWithDocumentArtifactChildRangeDispatcherAndCommittedEffectsObserver(.{ .writes = &.{.{ .key = "unrelated", .value = "{}" }}, .sync_level = level }, .{ .ptr = &capture, .select_destination = db_mod.testChildRangeDestination, .apply = Capture.dispatch }, .{ .ptr = &capture, .apply = Capture.observe });
        const pending = try db.core.store.scanPrefix(alloc, db_mod.internal_keys.graph_endpoint_cleanup_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, pending);
        if (level == .full_index) {
            try std.testing.expectEqual(@as(usize, 0), pending.len);
            try std.testing.expectEqual(@as(usize, 4), capture.calls);
        } else {
            try std.testing.expectEqual(@as(usize, 344), pending.len);
            try std.testing.expectEqual(@as(usize, 2), capture.calls);
            // Each scheduler turn remains bounded and eventually clears jobs.
            try std.testing.expect(try db.indexes.graph.runStandaloneEndpointCleanupStep());
            try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
            try std.testing.expect(try db.indexes.graph.runStandaloneEndpointCleanupStep());
            try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
        }
    }
}

test "db graph endpoint cleanup pages cancellation stops between commits" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("review-cleanup-cancel");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{}" }}, .sync_level = .write });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const jobs = try scratch.alloc(db_mod.docstore_mod.KVPair, 257);
    for (jobs, 0..) |*job, i| {
        const endpoint = try std.fmt.allocPrint(scratch, "isolated:{d}", .{i});
        job.* = .{ .key = try db_mod.internal_keys.graphEndpointCleanupKeyAlloc(scratch, endpoint), .value = endpoint };
    }
    try db.core.store.putBatch(jobs, &.{});
    const Capture = struct {
        calls: usize = 0,
        canceled: std.atomic.Value(bool) = .init(false),
        fn observe(ptr: *anyopaque, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 2) self.canceled.store(true, .release);
        }
    };
    var capture = Capture{};
    try std.testing.expectError(error.EnrichmentWaitCanceled, db.batchInternal(.{ .writes = &.{.{ .key = "unrelated", .value = "{}" }}, .sync_level = .full_index }, null, .{ .committed_batch_effects_observer = .{ .ptr = &capture, .apply = Capture.observe }, .visibility_cancellation = .fromAtomic(&capture.canceled) }));
    try std.testing.expectEqual(@as(usize, 2), capture.calls);
    try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
    const committed_key = try db_mod.encodeStoreLookupKeyAlloc(&db, alloc, "unrelated");
    defer alloc.free(committed_key);
    const committed = try db.core.store.get(alloc, committed_key);
    alloc.free(committed);
    // Cancellation preserves the primary commit and the unfinished durable job.
    try std.testing.expect(try db.indexes.graph.runStandaloneEndpointCleanupStep());
    try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
}

test "db graph endpoint cleanup pages stale plan preserves revived relationship" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("review-stale-graph-plan");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    const write = db_mod.types.GraphEdgeWrite{ .index_name = "g", .source = "a", .target = "hub", .edge_type = "R", .edge_id = "one" };
    try db_mod.graphTestApplyOrdered(&db, .{ .graph_writes = &.{write}, .sync_level = .write }, .{ .term = 1, .index = 1 });
    try db_mod.graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"}, .sync_level = .write }, .{ .term = 1, .index = 2 });
    var plan = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
    defer plan.deinit();
    try db_mod.graphTestApplyOrdered(&db, plan.request(), .{ .term = 1, .index = 3 });
    try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
    try db_mod.graphTestApplyOrdered(&db, .{ .graph_writes = &.{write}, .sync_level = .write }, .{ .term = 1, .index = 4 });
    try db.runUntilIdle();
    {
        const edges = try db.getEdges(alloc, "g", "a", "", .out);
        defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
    }
    // The same selected page arrives as a distinct later ordered command,
    // rather than redelivery of an already-applied Raft entry.
    try db_mod.graphTestApplyOrdered(&db, plan.request(), .{ .term = 1, .index = 5 });
    try db.runUntilIdle();
    const edges = try db.getEdges(alloc, "g", "a", "", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
}

test "db graph endpoint cleanup pages fence a later deletion of the same endpoint" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("cleanup-job-aba");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    const edge = db_mod.types.GraphEdgeWrite{ .index_name = "g", .source = "a", .target = "hub", .edge_type = "R" };
    try db_mod.graphTestApplyOrdered(&db, .{ .graph_writes = &.{edge} }, .{ .term = 1, .index = 1 });
    try db_mod.graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"} }, .{ .term = 1, .index = 2 });
    var old = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
    defer old.deinit();
    try db_mod.graphTestApplyOrdered(&db, old.request(), .{ .term = 1, .index = 3 });
    try db_mod.graphTestApplyOrdered(&db, .{ .graph_writes = &.{edge} }, .{ .term = 1, .index = 4 });
    try db_mod.graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"} }, .{ .term = 1, .index = 5 });
    var fresh = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
    defer fresh.deinit();
    try std.testing.expect(fresh.page.guards[0].generation > old.page.guards[0].generation);
    try db_mod.graphTestApplyOrdered(&db, old.request(), .{ .term = 1, .index = 6 });
    try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
    var unchanged = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
    defer unchanged.deinit();
    try std.testing.expectEqual(fresh.page.guards[0].generation, unchanged.page.guards[0].generation);
    try std.testing.expectEqual(@as(usize, 1), unchanged.graph_deletes.len);
    try db_mod.graphTestApplyOrdered(&db, fresh.request(), .{ .term = 1, .index = 7 });
    try db.runUntilIdle();
    try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
    const edges = try db.getEdges(alloc, "g", "a", "", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);
}

test "db graph endpoint cleanup pages filter stale and live jobs independently" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("cleanup-mixed-generations");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db_mod.graphTestApplyOrdered(&db, .{ .graph_writes = &.{
        .{ .index_name = "g", .source = "a", .target = "hub-a", .edge_type = "R", .edge_id = "one" },
        .{ .index_name = "g", .source = "b", .target = "hub-b", .edge_type = "R", .edge_id = "two" },
    } }, .{ .term = 1, .index = 1 });
    try db_mod.graphTestApplyOrdered(&db, .{ .deletes = &.{ "hub-a", "hub-b" } }, .{ .term = 1, .index = 2 });
    var plan = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 2), plan.page.guards.len);
    try std.testing.expectEqual(@as(usize, 2), plan.request().graph_deletes.len);
    const job = try db_mod.internal_keys.graphEndpointCleanupKeyAlloc(alloc, "hub-a");
    defer alloc.free(job);
    const guard = if (std.mem.eql(u8, plan.page.guards[0].endpoint, "hub-a")) plan.page.guards[0] else plan.page.guards[1];
    const edges = plan.request().graph_deletes;
    const edge = if (std.mem.eql(u8, edges[0].target, "hub-a")) edges[0] else edges[1];
    try db_mod.graphTestApplyOrdered(&db, .{ .graph_endpoint_cleanup = true, .graph_endpoint_cleanup_planned = true, .graph_endpoint_cleanup_guards = &.{guard}, .graph_deletes = &.{edge}, .deletes = &.{job} }, .{ .term = 1, .index = 3 });
    try db_mod.graphTestApplyOrdered(&db, .{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "hub-a", .edge_type = "R", .edge_id = "one" }} }, .{ .term = 1, .index = 4 });
    try db_mod.graphTestApplyOrdered(&db, plan.request(), .{ .term = 1, .index = 5 });
    try std.testing.expect(try db.orderedMutationAlreadyApplied(.{ .term = 1, .index = 5 }));
    try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
    try db.runUntilIdle();
    const revived = try db.getEdges(alloc, "g", "a", "", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, revived);
    const retired = try db.getEdges(alloc, "g", "b", "", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, retired);
    try std.testing.expectEqual(@as(usize, 1), revived.len);
    try std.testing.expectEqual(@as(usize, 0), retired.len);
}

test "db graph endpoint cleanup pages transaction dependencies survive reopen" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("graph-prepared-endpoint");
    defer directory.cleanup();
    var txn_id: db_mod.types.TxnId = undefined;
    {
        var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}" });
        txn_id = try db.beginTransaction(21_000);
        try db.writeTransaction(txn_id, .{ .writes = &.{.{ .key = "b", .value = "{\"links\":[\"field-target\"],\"_edges\":{\"g\":{\"R\":[{\"target\":\"hub\"}]}}}" }} });
        try std.testing.expectError(error.IntentConflict, db_mod.graphTestApplyOrdered(&db, .{ .deletes = &.{ "hub", "field-target" } }, .{ .term = 1, .index = 1 }));
        try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
        try std.testing.expectError(error.SchemaInUse, db.deleteIndex("g"));
        try std.testing.expectError(error.SchemaInUse, db.addIndex(.{ .name = "another", .kind = .graph, .config_json = "{}" }));
    }
    {
        var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        try std.testing.expectError(error.IntentConflict, db_mod.graphTestApplyOrdered(&db, .{ .deletes = &.{ "hub", "field-target" } }, .{ .term = 1, .index = 1 }));
        const deleting = try db.beginTransaction(22_000);
        try std.testing.expectError(error.IntentConflict, db.writeTransaction(deleting, .{ .deletes = &.{ "hub", "field-target" } }));
        try db.abortTransaction(deleting, 22_001);
        try db.commitTransaction(txn_id, 21_001);
        try db_mod.graphTestApplyOrdered(&db, .{ .deletes = &.{ "hub", "field-target" } }, .{ .term = 1, .index = 1 });
        while (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)) |selected| {
            var plan = selected;
            defer plan.deinit();
            try db.batch(plan.request());
        }
        try db.runUntilIdle();
        const edges = try db.getEdges(alloc, "g", "b", "", .out);
        defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 0), edges.len);
    }
}

test "db graph endpoint cleanup pages transaction admission rejects active cleanup and abort releases guards" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("graph-prepare-cleanup-admission");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    const value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"hub\"}]}}}";
    const aborted = try db.beginTransaction(1);
    try db.writeIntents(aborted, &.{.{ .key = "a", .value = value }}, &.{});
    try std.testing.expectError(error.IntentConflict, db_mod.graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"} }, .{ .term = 1, .index = 1 }));
    try db.abortTransaction(aborted, 2);
    try db_mod.graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"} }, .{ .term = 1, .index = 1 });
    const rejected = try db.beginTransaction(3);
    try std.testing.expectError(error.IntegrityTopologyBusy, db.writeIntents(rejected, &.{.{ .key = "b", .value = value }}, &.{}));
    try std.testing.expectError(error.IntegrityTopologyBusy, db.writeTransaction(rejected, .{ .writes = &.{.{ .key = "b", .value = value }} }));
    try db.abortTransaction(rejected, 4);
    var plan = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
    defer plan.deinit();
    try db_mod.graphTestApplyOrdered(&db, plan.request(), .{ .term = 1, .index = 2 });
    const accepted = try db.beginTransaction(5);
    try db.writeIntents(accepted, &.{.{ .key = "c", .value = value }}, &.{});
    try db.commitTransaction(accepted, 6);
}

test "db graph qualified cleanup preserves foreign identities across rebuild and reopen" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("qualified-cleanup-lifecycle");
    defer directory.cleanup();
    for (0..2) |iteration| {
        var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        db.local_execution.row_policy_table_name = "facts";
        if (iteration == 0) {
            try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
            try db.batch(.{ .graph_writes = &.{
                .{ .index_name = "g", .source = "a", .target = "shared", .edge_type = "R", .edge_id = "local" },
                .{ .index_name = "g", .source = "a", .target = "shared", .edge_type = "R", .edge_id = "foreign-target", .metadata_json = "{\"target_table\":\"people\"}" },
                .{ .index_name = "g", .source = "a", .target = "shared", .edge_type = "R", .edge_id = "foreign-source", .owner_document = "a", .metadata_json = "{\"source_table\":\"people\"}" },
            }, .sync_level = .full_index });
            try db.core.store.ensureGraphIncomingDirectory();
            try db_mod.graphTestApplyOrdered(&db, .{ .deletes = &.{"shared"} }, .{ .term = 1, .index = 1 });
            var old = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
            defer old.deinit();
            try std.testing.expectEqual(@as(usize, 1), old.graph_deletes.len);
            // Same storage identity, new foreign destination. The selected
            // page must check its current routing before publishing retirement.
            try db_mod.graphTestApplyOrdered(&db, .{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "shared", .edge_type = "R", .edge_id = "local", .metadata_json = "{\"target_table\":\"people\"}" }} }, .{ .term = 1, .index = 2 });
            try db_mod.graphTestApplyOrdered(&db, old.request(), .{ .term = 1, .index = 3 });
            try db.runUntilIdle();
        }
        _ = try db.rebuildGraphDerivedState();
        const edges = try db.getEdges(alloc, "g", "a", "", .out);
        defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 3), edges.len);
    }
}

test "db graph owner revival bounds foreground history and fences stale pages and newer retirements" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("owner-revival-lifecycle");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const contract = @import("../../graph_cleanup_contract.zig");
    const history = try a.alloc(db_mod.docstore_mod.KVPair, 601);
    for (history, 0..) |*row, i| {
        const artifact = try db_mod.internal_keys.graphRelationshipArtifactKeyAlloc(a, "a", "g", if (i == 600) "R" else "OLD", if (i == 600) "b" else "target", "a", if (i == 600) "" else try std.fmt.allocPrint(a, "{d:0>4}", .{i}));
        row.* = .{ .key = try db_mod.internal_keys.graphRetirementKeyAlloc(a, artifact), .value = "1" };
    }
    try db.core.store.putBatch(history, &.{});
    try db_mod.graphTestApplyOrdered(&db, .{ .writes = &.{.{ .key = "a", .value = "{\"v\":1,\"_edges\":{\"g\":{\"R\":[{\"target\":\"b\"}]}}}" }} }, .{ .term = 1, .index = 1 });
    const prefix = try db_mod.internal_keys.graphRetirementPrefixAlloc(a, "a");
    const retained = try db.core.store.scanPrefix(alloc, prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, retained);
    // The explicitly reauthored inline relationship clears its own marker.
    // Every unrelated historical marker remains for bounded maintenance.
    try std.testing.expectEqual(@as(usize, 600), retained.len);
    try std.testing.expect(try db.core.store.graphEndpointCleanupBlocksReads());
    var stale = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
    defer stale.deinit();
    try std.testing.expect(stale.page.inspected <= 255);
    try db_mod.graphTestApplyOrdered(&db, .{ .writes = &.{.{ .key = "a", .value = "{\"v\":2,\"_edges\":{\"g\":{\"R\":[{\"target\":\"b\"}]}}}" }} }, .{ .term = 1, .index = 2 });
    const job_key = try contract.ownerJobKeyAlloc(a, "a");
    const current_job = try db.core.store.get(alloc, job_key);
    defer alloc.free(current_job);
    try db_mod.graphTestApplyOrdered(&db, stale.request(), .{ .term = 1, .index = 3 });
    const after_stale = try db.core.store.get(alloc, job_key);
    defer alloc.free(after_stale);
    try std.testing.expectEqualSlices(u8, current_job, after_stale);
    var fresh = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
    defer fresh.deinit();
    const newer = contract.retirementValue(fresh.page.guards[0].generation + 1);
    const retained_key = fresh.page.deletes[0];
    try db.core.store.put(retained_key, &newer);
    try db_mod.graphTestApplyOrdered(&db, fresh.request(), .{ .term = 1, .index = 4 });
    var index: u64 = 5;
    var pages: usize = 1;
    while (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)) |selected| {
        var page = selected;
        defer page.deinit();
        try std.testing.expect(page.page.inspected <= 255);
        try std.testing.expect(page.page.bytes <= 256 * 1024);
        try db_mod.graphTestApplyOrdered(&db, page.request(), .{ .term = 1, .index = index });
        index += 1;
        pages += 1;
    }
    try std.testing.expect(pages >= 4);
    const marker = try db.core.store.get(alloc, retained_key);
    defer alloc.free(marker);
    try std.testing.expectEqualSlices(u8, &newer, marker);
    try db.runUntilIdle();
    const edges = try db.getEdges(alloc, "g", "a", "", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("b", edges[0].target);
}

test "db graph owner revival retained input replay cannot overwrite a newer input" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("owner-replay-input-race");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json =
        \\{"source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
    });
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"relations\":{\"relations\":[{\"type\":\"R\",\"target\":{\"document_id\":\"b\"}}]}}" }}, .sync_level = .full_index });
    const contract = @import("../../graph_cleanup_contract.zig");
    const job = try contract.ownerJobKeyAlloc(alloc, "a");
    defer alloc.free(job);
    const value = try contract.encodeOwnerJobAlloc(alloc, .{ .owner = "a", .generation = 10, .phase = .inputs });
    defer alloc.free(value);
    try db.core.store.put(job, value);
    var plan = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
    defer plan.deinit();
    const input_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "a", "asset", "relations_v1");
    defer alloc.free(input_key);
    const old = try db.core.store.get(alloc, input_key);
    defer alloc.free(old);
    const newer = try std.mem.concat(alloc, u8, &.{ old, " " });
    defer alloc.free(newer);
    try db.core.store.put(input_key, newer);
    try db_mod.graphTestApplyOrdered(&db, plan.request(), .{ .term = 1, .index = 1 });
    const actual = try db.core.store.get(alloc, input_key);
    defer alloc.free(actual);
    try std.testing.expectEqualSlices(u8, newer, actual);
    var sequence: u64 = 2;
    while (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)) |selected| {
        var page = selected;
        defer page.deinit();
        try db_mod.graphTestApplyOrdered(&db, page.request(), .{ .term = 1, .index = sequence });
        sequence += 1;
    }
    try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
}

test "db graph cleanup lookup carries and applies owner replay afterimages" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("fresh-owner-wire");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    const contract = @import("../../graph_cleanup_contract.zig");
    const key = try contract.ownerJobKeyAlloc(alloc, "owner");
    defer alloc.free(key);
    const value = try contract.encodeOwnerJobAlloc(alloc, .{ .owner = "owner", .generation = 7, .phase = .inputs });
    defer alloc.free(value);
    try db.core.store.put(key, value);
    var page = (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)).?;
    defer page.deinit();
    try std.testing.expect(page.request().merge_artifacts.len > 0);
    var response = (try db.lookupRelationalTopology(alloc, "{\"mode\":\"graph_endpoint_cleanup\"}", .{})).?;
    defer response.deinit(alloc);
    var parsed = try std.json.parseFromSlice(db_mod.types.GraphEndpointCleanupStatus, alloc, response.json, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(page.replay_writes.len, parsed.value.merge_artifacts.len);
    const req = parsed.value.request();
    try db_mod.types.validateGraphEndpointCleanupCommand(req);
    for (page.replay_writes, req.merge_artifacts) |expected, actual| {
        try std.testing.expectEqualSlices(u8, expected.key, actual.key);
        try std.testing.expectEqualSlices(u8, expected.value, actual.value);
    }
    try db.batch(req);
    const advanced = try db.core.store.get(alloc, key);
    defer alloc.free(advanced);
    try std.testing.expectEqual(contract.OwnerPhase.chunks, (try contract.decodeOwnerJob(key, advanced)).phase);
    // Duplicate ordered maintenance pages cannot roll back a newer checkpoint.
    try db.batch(req);
    const replayed = try db.core.store.get(alloc, key);
    defer alloc.free(replayed);
    try std.testing.expectEqualSlices(u8, advanced, replayed);
}

test "db graph endpoint cleanup transaction guards respect physical endpoint tables" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("qualified-transaction-guards");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    db.local_execution.row_policy_table_name = "facts";
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    const foreign = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"hub\",\"metadata\":{\"target_table\":\"people\"}}]}}}";
    const first = try db.beginTransaction(1);
    try db.writeTransaction(first, .{ .writes = &.{.{ .key = "a", .value = foreign }} });
    // A foreign dependency must not lock the same document key in this table.
    try db_mod.graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"} }, .{ .term = 1, .index = 1 });
    try db.abortTransaction(first, 2);
    // Subsequent admission also works from the durable table identity when
    // the caller has no execution-time table override.
    db.local_execution.row_policy_table_name = null;
    for ([_][]const u8{
        "{\"target_table\":\"people\"}",
        "{\"source_table\":\"people\"}",
        "{\"source_table\":\"facts\",\"target_table\":\"people\"}",
        "{\"source_table\":\"people\",\"target_table\":\"facts\"}",
    }) |metadata| {
        const value = try std.fmt.allocPrint(alloc, "{{\"_edges\":{{\"g\":{{\"R\":[{{\"target\":\"hub\",\"metadata\":{s}}}]}}}}}}", .{metadata});
        defer alloc.free(value);
        for (0..2) |entrypoint| {
            const txn_id = try db.beginTransaction(3);
            if (entrypoint == 0)
                try db.writeIntents(txn_id, &.{.{ .key = "a", .value = value }}, &.{})
            else
                try db.writeTransaction(txn_id, .{ .writes = &.{.{ .key = "a", .value = value }} });
            try db.abortTransaction(txn_id, 4);
        }
    }
    const local = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"hub\",\"metadata\":{\"source_table\":\"facts\",\"target_table\":\"facts\"}}]}}}";
    const rejected = try db.beginTransaction(5);
    try std.testing.expectError(error.IntegrityTopologyBusy, db.writeTransaction(rejected, .{ .writes = &.{.{ .key = "a", .value = local }} }));
    try std.testing.expectError(error.IntegrityTopologyBusy, db.writeIntents(rejected, &.{.{ .key = "a", .value = local }}, &.{}));
    try db.abortTransaction(rejected, 6);
    while (try db.indexes.graph.prepareEndpointCleanupBatch(alloc)) |selected| {
        var plan = selected;
        defer plan.deinit();
        try db.batch(plan.request());
    }
    const accepted = try db.beginTransaction(7);
    try db.writeTransaction(accepted, .{ .writes = &.{.{ .key = "a", .value = local }} });
    try std.testing.expectError(error.IntentConflict, db_mod.graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"} }, .{ .term = 1, .index = 2 }));
    try db.abortTransaction(accepted, 8);
}

test "capi resolver background replacement source pages remain byte bounded" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("replacement-source-byte-bound");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false });
    defer db.close();
    try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}" });
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const tmp = scratch.allocator();
    var padding: [40 * 1024]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(0x6249);
    const random = prng.random();
    const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";
    for (&padding) |*byte| byte.* = alphabet[random.int(u6)];
    const value = try std.fmt.allocPrint(tmp, "{{\"links\":[\"before\"],\"next_links\":[\"after\"],\"padding\":\"{s}\"}}", .{padding});
    var rows: [64]db_mod.types.BatchWrite = undefined;
    for (&rows, 0..) |*row, i| row.* = .{ .key = try std.fmt.allocPrint(tmp, "source:{d:0>4}", .{i}), .value = value };
    try db.batch(.{ .writes = &rows });
    try db.runUntilIdle();
    // These rows fit the document-count quantum but exceed its byte budget.
    // Prove the fixture's physical input size, including stored encodings.
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var bytes: usize = 0;
        for (rows) |row| bytes += (try read.get(try db_mod.internal_keys.documentKeyAlloc(tmp, row.key))).len;
        try std.testing.expect(bytes > 1024 * 1024);
    }
    _ = try db.replaceGraphIndex("old", .{ .name = "new", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"next_links\"}]}" });
    var observed = false;
    for (0..8) |_| {
        _ = try db.runBackgroundTurnWithCancellation(.none, true);
        const jobs = try db.indexes.graph.replacementStatusAlloc(a);
        defer jobs.deinit();
        try std.testing.expectEqual(@as(usize, 1), jobs.value.len);
        if (jobs.value[0].scanned != 0) {
            try std.testing.expectEqual(.building, jobs.value[0].phase);
            try std.testing.expect(jobs.value[0].resume_key_b64 != null);
            try std.testing.expect(db.core.index_manager.get("old") != null);
            try std.testing.expect(db.core.index_manager.get("new") == null);
            observed = true;
            break;
        }
    }
    try std.testing.expect(observed);
    try db.runUntilIdle();
    const edges = try db.core.index_manager.graphIndex("new").?.index.getEdges(a, "source:0063", "R", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(a, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("after", edges[0].target);
}

test "capi resolver replacement resumes checkpoints while writes continue" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("replacement-resume");
    defer directory.cleanup();
    const options: db_mod.OpenOptions = .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false };
    var scanned: u64 = 0;
    var generation: u64 = 0;
    const old_cfg: db_mod.types.IndexConfig = .{ .name = "old", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}" };
    const new_cfg: db_mod.types.IndexConfig = .{ .name = "new", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"next_links\"}]}" };
    {
        var db = try DB.open(a, directory.path(), options);
        defer db.close();
        try db.addIndex(old_cfg);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var rows: std.ArrayList(db_mod.types.BatchWrite) = .empty;
        for (0..300) |i| try rows.append(arena.allocator(), .{ .key = try std.fmt.allocPrint(arena.allocator(), "source:{d:0>4}", .{i}), .value = "{\"links\":[\"before\"],\"next_links\":[\"after\"]}" });
        try db.batch(.{ .writes = rows.items });
        try db.runUntilIdle();
        generation = try db.replaceGraphIndex("old", new_cfg);
        try std.testing.expect(db.core.index_manager.get("new") == null);
        const old_edges = try db.core.index_manager.graphIndex("old").?.index.getEdges(a, "source:0000", "R", .out);
        defer db_mod.graph_mod.GraphIndex.freeEdges(a, old_edges);
        try std.testing.expectEqual(@as(usize, 1), old_edges.len);
        try std.testing.expectEqualStrings("before", old_edges[0].target);
        try std.testing.expectEqual(generation, try db.replaceGraphIndex("old", new_cfg)); // registration is idempotent
        try std.testing.expect(try db.indexes.graph.advanceReplacement());
        const progress = try db.indexes.graph.replacementStatusAlloc(a);
        defer progress.deinit();
        scanned = progress.value[0].scanned;
        try std.testing.expect(scanned > 0);
        try std.testing.expect(progress.value[0].resume_key_b64 != null);
        try std.testing.expectError(error.Busy, db.indexes.graph.waitReplacement("new", generation, 0));
    }
    {
        var db = try DB.open(a, directory.path(), options);
        defer db.close();
        const progress = try db.indexes.graph.replacementStatusAlloc(a);
        defer progress.deinit();
        try std.testing.expectEqual(scanned, progress.value[0].scanned);
        try std.testing.expect(db.core.index_manager.get("old") != null);
        var turns: usize = 0;
        while (db.core.index_manager.get("new") == null) : (turns += 1) {
            if (turns > 1000) return error.TestUnexpectedResult;
            // Every build round races a new field-backed primary revision.
            try db.batch(.{ .writes = &.{.{ .key = "source:0000", .value = "{\"links\":[\"before\"],\"next_links\":[\"latest\"]}" }} });
            _ = try db.indexes.graph.advanceReplacement();
        }
        try std.testing.expect(db.core.index_manager.get("old") == null);
        const edges = try db.core.index_manager.graphIndex("new").?.index.getEdges(a, "source:0000", "R", .out);
        defer db_mod.graph_mod.GraphIndex.freeEdges(a, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
        try std.testing.expectEqualStrings("latest", edges[0].target);
        for ([_][]const u8{ "source:0001", "source:0299" }) |key| {
            const retained = try db.core.index_manager.graphIndex("new").?.index.getEdges(a, key, "R", .out);
            defer db_mod.graph_mod.GraphIndex.freeEdges(a, retained);
            try std.testing.expectEqual(@as(usize, 1), retained.len);
            try std.testing.expectEqualStrings("after", retained[0].target);
        }
        try db.indexes.graph.waitReplacement("new", generation, 0);
        try std.testing.expect(!db.indexes.graph.hasPendingReplacements());
    }
}

test "capi resolver replacement fenced tail advances gaps over 1024" {
    var checked_allocator = try @import("antfly_platform").allocator.CheckedTestAllocator.init();
    defer checked_allocator.deinit();
    const a = checked_allocator.allocator();
    var directory = try db_mod.TestDirectory.init("replacement-tail");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false });
    defer db.close();
    const config = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
    try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = config });
    _ = try db.replaceGraphIndex("old", .{ .name = "new", .kind = .graph, .config_json = config });
    const jobs = try db.indexes.graph.replacementStatusAlloc(a);
    defer jobs.deinit();
    const floor = jobs.value[0].applied_sequence;
    for (0..1100) |revision| {
        const value = try std.fmt.allocPrint(a, "{{\"links\":[\"target\"],\"revision\":{d}}}", .{revision});
        defer a.free(value);
        try db.batch(.{ .writes = &.{.{ .key = "hot", .value = value }} });
    }
    try std.testing.expect(db.core.nextDerivedSequence() - floor > 1024);
    var private = try db_mod.index_manager_mod.IndexManager.initWithOptions(a, db.core.index_manager.base_path, db.index_backends);
    defer private.deinit();
    private.source_payload_store = db.core.index_manager.source_payload_store;
    private.setIo(db.backend_runtime.io());
    const cfg = jobs.value[0].config.?;
    try private.registerReplacementIndex(db.core.store, cfg);
    var prepared = DB.PreparedGraphReplacement{ .manager = &private, .sequence = floor };
    defer if (prepared.entry) |*entry| private.destroyDetachedReplacementIndex(entry);
    // A loaded machine may spend an entire window acquiring/reading IO.
    // Require eventual progress over bounded attempts, not the first 100 ms.
    for (0..100) |_| {
        db.core.lockApply();
        defer db.core.unlockApply();
        prepared.finish(&db, cfg) catch |err| if (err != error.Busy) return err;
        if (prepared.sequence > floor or prepared.tail_source_cursor > 0) break;
    }
    // The former >1024 shortcut never consumed even one entry.
    try std.testing.expect(prepared.sequence > floor or prepared.tail_source_cursor > 0);
    if (prepared.sequence < db.core.nextDerivedSequence()) {
        const progress = try db.indexes.graph.replacementStatusAlloc(a);
        defer progress.deinit();
        try std.testing.expectEqual(prepared.sequence, progress.value[0].applied_sequence);
        try std.testing.expectEqual(prepared.tail_source_cursor, progress.value[0].tail_source_cursor);
        try std.testing.expect(progress.value[0].activation_attempts > 0);
        // Escalated fences retain a finite budget and resume durable progress
        // across admissions rather than holding writers for an unbounded tail.
        prepared.activation_attempts = 8;
        db.core.lockApply();
        defer db.core.unlockApply();
        for (0..100) |_| {
            prepared.finish(&db, cfg) catch |err| {
                if (err == error.Busy) continue;
                return err;
            };
            break;
        }
        try std.testing.expectEqual(db.core.nextDerivedSequence(), prepared.sequence);
        try std.testing.expect(prepared.entry != null);
    }
}

test "capi resolver replacement tail resumes inside a large journal batch" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("replacement-batch-tail");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false });
    defer db.close();
    const config = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
    try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = config });
    _ = try db.replaceGraphIndex("old", .{ .name = "new", .kind = .graph, .config_json = config });
    const jobs = try db.indexes.graph.replacementStatusAlloc(a);
    defer jobs.deinit();
    const floor = jobs.value[0].applied_sequence;
    var rows: std.ArrayList(db_mod.types.BatchWrite) = .empty;
    defer rows.deinit(a);
    for (0..100) |i| try rows.append(a, .{ .key = try std.fmt.allocPrint(a, "doc:{d}", .{i}), .value = "{\"links\":[\"target\"]}" });
    defer for (rows.items) |row| a.free(row.key);
    try db.batch(.{ .writes = rows.items });
    var private = try db_mod.index_manager_mod.IndexManager.initWithOptions(a, db.core.index_manager.base_path, db.index_backends);
    defer private.deinit();
    private.source_payload_store = db.core.index_manager.source_payload_store;
    private.setIo(db.backend_runtime.io());
    try private.registerReplacementIndex(db.core.store, jobs.value[0].config.?);
    // The first source was checkpointed in a previous bounded pause. A zero
    // budget must preserve that intra-entry cursor rather than restarting it.
    var cursor: usize = 1;
    try std.testing.expectEqual(floor, try db.indexes.graph.catchUpReplacementTail(&private, "new", floor, db.core.nextDerivedSequence(), &cursor, db_mod.monotonicTimeNs()));
    try std.testing.expectEqual(@as(usize, 1), cursor);
    // Replaying from the beginning is idempotent, as on a background resume.
    cursor = 0;
    const target = db.core.nextDerivedSequence();
    const batches_before = DB.test_graph_replacement_document_batches;
    try std.testing.expectEqual(target, try db.indexes.graph.catchUpReplacementTail(&private, "new", floor, target, &cursor, std.math.maxInt(u64)));
    try std.testing.expectEqual(@as(usize, 4), DB.test_graph_replacement_document_batches - batches_before);
    try std.testing.expectEqual(@as(usize, 0), cursor);
    const edges = try private.graphIndex("new").?.index.getEdges(a, "doc:99", "R", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(a, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
}

test "capi resolver replacement retries transient failures with durable bounded backoff" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("replacement-retries");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false });
    defer db.close();
    const config = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
    try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = config });
    const generation = try db.replaceGraphIndex("old", .{ .name = "new", .kind = .graph, .config_json = config });
    const Failure = struct {
        fn call(_: *DB) anyerror!void {
            return error.NoSpaceLeft;
        }
    };
    DB.test_graph_replacement_before_build = Failure.call;
    defer DB.test_graph_replacement_before_build = null;
    for (0..5) |attempt| {
        _ = try db.indexes.graph.advanceReplacement();
        const jobs = try db.indexes.graph.replacementStatusAlloc(a);
        defer jobs.deinit();
        const job = jobs.value[0];
        try std.testing.expectEqual(@as(u32, @intCast(@min(attempt + 1, 4))), job.retry_count);
        try std.testing.expectEqualStrings("NoSpaceLeft", job.last_error.?);
        if (attempt < 4) {
            try std.testing.expect(job.phase != .failed);
            try std.testing.expect(job.retry_after_ns > db.replacementRetryNow());
            // A waiting job does not consume its budget before the deadline.
            _ = try db.indexes.graph.advanceReplacement();
            var next = job;
            next.retry_after_ns = 0;
            try db.core.index_manager.saveGraphReplacementBuild(db.core.store, next);
        } else {
            try std.testing.expectEqual(.failed, job.phase);
            try std.testing.expectError(error.IndexReplacementFailed, db.indexes.graph.waitReplacement("new", generation, 100));
            try std.testing.expectEqual(@as(u64, 999), try db.core.index_manager.clampReplayForGraphReplacementBuilds(db.core.store, 999));
        }
    }
    try std.testing.expect(try db.indexes.graph.cancelReplacement("new"));
    try std.testing.expectError(error.Cancelled, db.indexes.graph.waitReplacement("new", generation, 100));
}

test "capi resolver rejected serving deletion preserves its pending shadow" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("replacement-delete-atomic");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false });
    defer db.close();
    const config = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
    try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = config });
    _ = try db.replaceGraphIndex("old", .{ .name = "new", .kind = .graph, .config_json = config });
    const Failure = struct {
        fn call(_: *DB) anyerror!void {
            return error.InjectedDeleteFailure;
        }
    };
    DB.test_index_delete_after_worker_paused = Failure.call;
    defer DB.test_index_delete_after_worker_paused = null;
    try std.testing.expectError(error.InjectedDeleteFailure, db.deleteIndex("old"));
    try std.testing.expect(db.core.index_manager.get("old") != null);
    try std.testing.expect(db.indexes.graph.hasPendingReplacements());
    DB.test_index_delete_after_worker_paused = null;
    try std.testing.expect(try db.deleteIndex("old"));
    try std.testing.expect(db.core.index_manager.get("old") == null);
    try std.testing.expect(!db.indexes.graph.hasPendingReplacements());
    try db.runUntilIdle();
}

test "capi resolver replacement retry budget resets only after durable progress" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("replacement-retry-progress");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false });
    defer db.close();
    const config = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
    try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = config });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var rows: std.ArrayList(db_mod.types.BatchWrite) = .empty;
    for (0..100) |i| try rows.append(arena.allocator(), .{ .key = try std.fmt.allocPrint(arena.allocator(), "source:{d:0>4}", .{i}), .value = "{\"links\":[\"target\"]}" });
    try db.batch(.{ .writes = rows.items });
    _ = try db.replaceGraphIndex("old", .{ .name = "new", .kind = .graph, .config_json = config });
    {
        const jobs = try db.indexes.graph.replacementStatusAlloc(a);
        defer jobs.deinit();
        var job = jobs.value[0];
        job.retry_count = 4;
        job.total_retry_count = DB.replacement_total_retry_limit - 1;
        job.last_error = "NoSpaceLeft";
        try db.core.index_manager.saveGraphReplacementBuild(db.core.store, job);
    }
    _ = try db.indexes.graph.advanceReplacement();
    {
        const jobs = try db.indexes.graph.replacementStatusAlloc(a);
        defer jobs.deinit();
        try std.testing.expect(jobs.value[0].scanned > 0);
        try std.testing.expectEqual(@as(u32, 0), jobs.value[0].retry_count);
        try std.testing.expectEqual(DB.replacement_total_retry_limit - 1, jobs.value[0].total_retry_count);
        try std.testing.expect(jobs.value[0].last_error == null);
    }
    const Yield = struct {
        fn call(_: *DB) anyerror!void {
            return error.EnrichmentWaitCanceled;
        }
    };
    DB.test_graph_replacement_before_build = Yield.call;
    defer DB.test_graph_replacement_before_build = null;
    for (0..8) |_| {
        try std.testing.expectError(error.EnrichmentWaitCanceled, db.indexes.graph.advanceReplacement());
        const yielded = try db.indexes.graph.replacementStatusAlloc(a);
        defer yielded.deinit();
        try std.testing.expect(yielded.value[0].phase != .failed);
        try std.testing.expectEqual(@as(u32, 0), yielded.value[0].retry_count);
        try std.testing.expectEqual(DB.replacement_total_retry_limit - 1, yielded.value[0].total_retry_count);
        try std.testing.expect(yielded.value[0].last_error == null);
    }
    DB.test_graph_replacement_before_build = null;
    const Failure = struct {
        fn call(_: *DB) anyerror!void {
            return error.NoSpaceLeft;
        }
    };
    DB.test_graph_replacement_before_build = Failure.call;
    defer DB.test_graph_replacement_before_build = null;
    _ = try db.indexes.graph.advanceReplacement();
    const jobs = try db.indexes.graph.replacementStatusAlloc(a);
    defer jobs.deinit();
    try std.testing.expectEqual(@as(u32, 1), jobs.value[0].retry_count);
    try std.testing.expect(jobs.value[0].phase != .failed);
    try std.testing.expectEqual(DB.replacement_total_retry_limit, jobs.value[0].total_retry_count);
    var ready = jobs.value[0];
    ready.retry_after_ns = 0;
    try db.core.index_manager.saveGraphReplacementBuild(db.core.store, ready);
    _ = try db.indexes.graph.advanceReplacement();
    const exhausted = try db.indexes.graph.replacementStatusAlloc(a);
    defer exhausted.deinit();
    try std.testing.expectEqual(.failed, exhausted.value[0].phase);
    try std.testing.expectEqual(@as(u32, 1), exhausted.value[0].retry_count);
    try std.testing.expectEqual(DB.replacement_total_retry_limit, exhausted.value[0].total_retry_count);
    try std.testing.expect(!DB.replacementRetryable(error.Unexpected));
    try std.testing.expect(!DB.replacementRetryable(error.InputOutput));
    try std.testing.expect(!DB.replacementRetryable(error.ReadFailed));
}

test "capi resolver replacement wait cannot acknowledge provisional catalog entries" {
    if (builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("replacement-wait-publication");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false });
    defer db.close();
    const Check = struct {
        var database: *DB = undefined;
        var generation: u64 = 0;
        var thread: ?std.Thread = null;
        var done: std.atomic.Value(bool) = .init(false);
        var early: bool = false;
        var failed: bool = false;
        var missing_waiter: bool = false;
        fn wait() void {
            database.indexes.graph.waitReplacement("new", generation, 0) catch {
                failed = true;
            };
            done.store(true, .release);
        }
        fn boundary(phase: db_mod.index_manager_mod.ReplacementSwapBoundary) void {
            if (phase == .before_commit) {
                thread = std.Thread.spawn(.{}, wait, .{}) catch @panic("spawn failed");
                const deadline = db_mod.monotonicTimeNs() + 2 * std.time.ns_per_s;
                while (!done.load(.acquire) and database.core.index_manager.catalog_mutex.shared_waiters.load(.acquire) == 0 and db_mod.monotonicTimeNs() < deadline)
                    database.core.index_manager.checkpointIo().sleep(.fromMilliseconds(1), .awake) catch @panic("sleep failed");
                missing_waiter = !done.load(.acquire) and database.core.index_manager.catalog_mutex.shared_waiters.load(.acquire) == 0;
            }
            early = early or done.load(.acquire);
        }
    };
    Check.database = &db;
    Check.thread = null;
    Check.done.store(false, .release);
    Check.early = false;
    Check.failed = false;
    Check.missing_waiter = false;
    const config = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
    try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = config });
    Check.generation = try db.replaceGraphIndex("old", .{ .name = "new", .kind = .graph, .config_json = config });
    db_mod.index_manager_mod.test_replacement_swap_boundary = Check.boundary;
    defer db_mod.index_manager_mod.test_replacement_swap_boundary = null;
    _ = try db.indexes.graph.advanceReplacement();
    const thread = Check.thread orelse return error.TestUnexpectedResult;
    thread.join();
    try std.testing.expect(Check.done.load(.acquire));
    try std.testing.expect(!Check.failed);
    try std.testing.expect(!Check.early);
    try std.testing.expect(!Check.missing_waiter);
}

test "capi resolver replacement crash boundaries recover atomic ownership" {
    if (comptime builtin.os.tag != .linux and builtin.os.tag != .macos) return error.SkipZigTest;
    var checked_allocator = try @import("antfly_platform").allocator.CheckedTestAllocator.init();
    defer checked_allocator.deinit();
    const a = checked_allocator.allocator();
    const io = std.testing.io;
    const options: db_mod.OpenOptions = .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false };
    if (std.c.getenv("ANTFLY_REPLACEMENT_CRASH_PATH")) |path| {
        // An exec'd child has fresh allocator/IO state. Kill in the actual
        // production boundaries: no close, unwind or final checkpoint occurs.
        var db = try DB.open(a, std.mem.span(path), options);
        const Crash = struct {
            fn kill() noreturn {
                _ = std.c.kill(std.c.getpid(), .KILL);
                @panic("SIGKILL failed");
            }
            fn beforeCheckpoint(database: *DB) void {
                const jobs = database.indexes.graph.replacementStatusAlloc(std.heap.page_allocator) catch @panic("status failed");
                defer jobs.deinit();
                // Leave one acknowledged checkpoint, then die with newer
                // projected work that has not published its source cursor.
                if (jobs.value[0].scanned > 0) kill();
            }
            fn addTail(database: *DB) anyerror!void {
                DB.test_graph_replacement_before_activation = null;
                var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                defer arena.deinit();
                var rows: std.ArrayList(db_mod.types.BatchWrite) = .empty;
                for (0..100) |i| try rows.append(arena.allocator(), .{ .key = try std.fmt.allocPrint(arena.allocator(), "source:{d:0>4}", .{i}), .value = "{\"links\":[\"before\"],\"next_links\":[\"tail_latest\"]}" });
                try database.batch(.{ .writes = rows.items });
            }
            fn tail(_: *DB, cursor: usize) void {
                if (cursor > 0) kill();
            }
            fn swap(phase: db_mod.index_manager_mod.ReplacementSwapBoundary) void {
                const stage = std.mem.span(std.c.getenv("ANTFLY_REPLACEMENT_CRASH_STAGE").?);
                if (std.mem.eql(u8, stage, @tagName(phase))) kill();
            }
        };
        const stage = std.mem.span(std.c.getenv("ANTFLY_REPLACEMENT_CRASH_STAGE").?);
        if (std.mem.eql(u8, stage, "checkpoint")) {
            DB.test_graph_replacement_before_checkpoint = Crash.beforeCheckpoint;
        } else if (std.mem.eql(u8, stage, "activation_tail")) {
            DB.test_graph_replacement_before_activation = Crash.addTail;
            DB.test_graph_replacement_tail_batch = Crash.tail;
        } else db_mod.index_manager_mod.test_replacement_swap_boundary = Crash.swap;
        for (0..1000) |_| _ = try db.indexes.graph.advanceReplacement();
        return error.TestUnexpectedResult; // boundary must have killed us
    }
    const executable = try std.process.executablePathAlloc(io, a);
    defer a.free(executable);
    for ([_][]const u8{ "checkpoint", "activation_tail", "before_commit", "after_commit" }) |stage| {
        var directory = try db_mod.TestDirectory.init("replacement-crash-boundary");
        defer directory.cleanup();
        var generation: u64 = 0;
        {
            var db = try DB.open(a, directory.path(), options);
            defer db.close();
            try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}" });
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var rows: std.ArrayList(db_mod.types.BatchWrite) = .empty;
            for (0..100) |i| try rows.append(arena.allocator(), .{ .key = try std.fmt.allocPrint(arena.allocator(), "source:{d:0>4}", .{i}), .value = "{\"links\":[\"before\"],\"next_links\":[\"after\"]}" });
            try db.batch(.{ .writes = rows.items });
            try db.runUntilIdle();
            generation = try db.replaceGraphIndex("old", .{ .name = "new", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"next_links\"}]}" });
        }
        var env = std.process.Environ.Map.init(a);
        defer env.deinit();
        try env.put("TZ", "UTC0");
        if (checked_allocator.traces) try env.put("ANTFLY_TEST_ALLOCATOR_TRACES", "1");
        try env.put("ANTFLY_REPLACEMENT_CRASH_PATH", directory.path());
        try env.put("ANTFLY_REPLACEMENT_CRASH_STAGE", stage);
        var child = try std.process.spawn(io, .{ .argv = &.{ executable, "--test-filter", "capi resolver replacement crash boundaries recover atomic ownership" }, .environ_map = &env });
        const term = try child.wait(io);
        try std.testing.expectEqual(std.process.Child.Term{ .signal = .KILL }, term);
        var db = try DB.open(a, directory.path(), options);
        defer db.close();
        const swapped = std.mem.eql(u8, stage, "after_commit");
        try std.testing.expectEqual(swapped, db.core.index_manager.get("new") != null);
        try std.testing.expectEqual(!swapped, db.core.index_manager.get("old") != null);
        if (!swapped) {
            const jobs = try db.indexes.graph.replacementStatusAlloc(a);
            defer jobs.deinit();
            try std.testing.expectEqual(@as(usize, 1), jobs.value.len);
            if (std.mem.eql(u8, stage, "checkpoint")) {
                try std.testing.expect(jobs.value[0].scanned > 0);
                try std.testing.expect(jobs.value[0].scanned < 100);
            }
        }
        try db.runUntilIdle();
        try db.indexes.graph.waitReplacement("new", generation, 0);
        for ([_][]const u8{ "source:0000", "source:0099" }) |key| {
            const edges = try db.core.index_manager.graphIndex("new").?.index.getEdges(a, key, "R", .out);
            defer db_mod.graph_mod.GraphIndex.freeEdges(a, edges);
            try std.testing.expectEqual(@as(usize, 1), edges.len);
            try std.testing.expectEqualStrings(if (std.mem.eql(u8, stage, "activation_tail")) "tail_latest" else "after", edges[0].target);
        }
    }
}

test "capi resolver pending replacement fences old writers without a permanent plain epoch" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("replacement-writer-fence");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false });
    defer db.close();
    const resolver_catalog = @import("../catalog/resolver_catalog.zig");
    const config = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
    try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = config });
    _ = try db.replaceGraphIndex("old", .{ .name = "new", .kind = .graph, .config_json = config });
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.UnsupportedDatabaseReaderVersion, resolver_catalog.readMinimumReaderVersion(&read, 1));
        const raw = try read.get("\x00\x00__metadata__:resolvers");
        // Older releases predate the epoch key and parse a strict array.
        try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice([]resolver_catalog.ResolverConfig, a, raw, .{}));
        try std.testing.expectEqual(@as(u32, 2), db.core.index_manager.minimum_reader_version.load(.acquire));
    }
    try db.runUntilIdle();
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u32, 1), try resolver_catalog.readMinimumReaderVersion(&read, 1));
        try std.testing.expectEqual(@as(u32, 1), db.core.index_manager.minimum_reader_version.load(.acquire));
    }
    _ = try db.replaceGraphIndex("new", .{ .name = "later", .kind = .graph, .config_json = config });
    try std.testing.expect(try db.indexes.graph.cancelReplacement("later"));
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u32, 1), try resolver_catalog.readMinimumReaderVersion(&read, 1));
    }
}

test "capi resolver replacement reader fence survives multiple jobs and resolver epoch upgrades" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("replacement-fence-multiple");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false });
    defer db.close();
    const resolver_catalog = @import("../catalog/resolver_catalog.zig");
    const config = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
    try db.addIndex(.{ .name = "old1", .kind = .graph, .config_json = config });
    try db.addIndex(.{ .name = "old2", .kind = .graph, .config_json = config });
    _ = try db.replaceGraphIndex("old1", .{ .name = "new1", .kind = .graph, .config_json = config });
    _ = try db.replaceGraphIndex("old2", .{ .name = "new2", .kind = .graph, .config_json = config });
    try std.testing.expect(try db.indexes.graph.cancelReplacement("new1"));
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.UnsupportedDatabaseReaderVersion, resolver_catalog.readMinimumReaderVersion(&read, 1));
    }
    // An authoritative feature upgrade during a temporary fence must survive
    // the last shadow's removal. Resolver serialization introduces v2 fields.
    try db.core.addResolver(.{ .name = "concepts", .table = "default", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "entity/{{ uslug _entity.text }}", .promote = false });
    try std.testing.expect(try db.indexes.graph.cancelReplacement("new2"));
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.UnsupportedDatabaseReaderVersion, resolver_catalog.readMinimumReaderVersion(&read, 1));
    }
}

test "capi resolver replacement wait rechecks publication when a job disappears" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("replacement-wait-gap");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false });
    defer db.close();
    const config = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
    try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = config });
    const generation = try db.replaceGraphIndex("old", .{ .name = "new", .kind = .graph, .config_json = config });
    const Gap = struct {
        fn swap(database: *DB) anyerror!void {
            DB.test_graph_replacement_wait_observation = null;
            try database.runUntilIdle();
        }
        fn cancel(database: *DB) anyerror!void {
            DB.test_graph_replacement_wait_observation = null;
            if (!try database.indexes.graph.cancelReplacement("later")) return error.TestUnexpectedResult;
        }
    };
    DB.test_graph_replacement_wait_observation = Gap.swap;
    defer DB.test_graph_replacement_wait_observation = null;
    try db.indexes.graph.waitReplacement("new", generation, 0);
    try std.testing.expect(db.indexGeneration("new") != null);
    const later_generation = try db.replaceGraphIndex("new", .{ .name = "later", .kind = .graph, .config_json = config });
    DB.test_graph_replacement_wait_observation = Gap.cancel;
    try std.testing.expectError(error.Cancelled, db.indexes.graph.waitReplacement("later", later_generation, 0));
    try std.testing.expect(db.indexGeneration("new") != null);
}

test "capi resolver replacement backward wall clock step clamps durable backoff once" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("replacement-clock-step");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false });
    defer db.close();
    const config = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
    try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = config });
    const generation = try db.replaceGraphIndex("old", .{ .name = "new", .kind = .graph, .config_json = config });
    DB.test_graph_replacement_retry_now_ns = 50 * std.time.ns_per_s;
    defer DB.test_graph_replacement_retry_now_ns = null;
    {
        const jobs = try db.indexes.graph.replacementStatusAlloc(a);
        defer jobs.deinit();
        var job = jobs.value[0];
        job.retry_count = 1;
        job.total_retry_count = 1;
        job.retry_after_ns = db.replacementRetryNow() + 9 * std.time.ns_per_hour;
        try db.core.index_manager.saveGraphReplacementBuild(db.core.store, job);
    }
    _ = try db.indexes.graph.advanceReplacement();
    const expected = db.replacementRetryNow() + DB.replacement_max_backoff_ns;
    {
        const jobs = try db.indexes.graph.replacementStatusAlloc(a);
        defer jobs.deinit();
        try std.testing.expectEqual(expected, jobs.value[0].retry_after_ns);
        try std.testing.expectEqual(@as(u32, 1), jobs.value[0].total_retry_count);
    }
    DB.test_graph_replacement_retry_now_ns.? += std.time.ns_per_s;
    _ = try db.indexes.graph.advanceReplacement();
    {
        const jobs = try db.indexes.graph.replacementStatusAlloc(a);
        defer jobs.deinit();
        try std.testing.expectEqual(expected, jobs.value[0].retry_after_ns);
    }
    DB.test_graph_replacement_retry_now_ns = expected;
    try db.runUntilIdle();
    try db.indexes.graph.waitReplacement("new", generation, 0);
}

test "capi resolver replacement interrupted ownership preserves accepted work" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("replacement-interruption");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false });
    defer db.close();
    const config = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
    try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = config });
    const generation = try db.replaceGraphIndex("old", .{ .name = "new", .kind = .graph, .config_json = config });
    const Cancel = struct {
        fn wait(_: ?*anyopaque, _: *const u32, _: u32, _: std.Io.Timeout) std.Io.Cancelable!void {
            return error.Canceled;
        }
        fn build(_: *DB) anyerror!void {
            return error.Canceled;
        }
    };
    {
        const original = db.core.index_manager.io;
        const io = db.core.index_manager.checkpointIo();
        var vtable = io.vtable.*;
        vtable.futexWait = Cancel.wait;
        db.core.index_manager.io = .{ .userdata = io.userdata, .vtable = &vtable };
        defer db.core.index_manager.io = original;
        try std.testing.expect(db.indexes.graph.replacement.mutex.tryLock());
        defer db.indexes.graph.replacement.mutex.unlock(io);
        try std.testing.expectError(error.Canceled, db.indexes.graph.cancelReplacement("new"));
    }
    DB.test_graph_replacement_before_build = Cancel.build;
    defer DB.test_graph_replacement_before_build = null;
    try std.testing.expectError(error.Canceled, db.indexes.graph.advanceReplacement());
    {
        const jobs = try db.indexes.graph.replacementStatusAlloc(a);
        defer jobs.deinit();
        try std.testing.expectEqual(@as(usize, 1), jobs.value.len);
        try std.testing.expectEqual(.building, jobs.value[0].phase);
        try std.testing.expectEqual(@as(u32, 0), jobs.value[0].total_retry_count);
        try std.testing.expect(jobs.value[0].last_error == null);
    }
    DB.test_graph_replacement_before_build = null;
    try db.runUntilIdle();
    try db.indexes.graph.waitReplacement("new", generation, 0);
}

test "capi resolver escalated deadline bounds tail work and releases queued writers" {
    if (builtin.single_threaded) return error.SkipZigTest;
    // Keep allocation checking, without stack-capture work dominating a
    // latency regression's source batches in Debug builds.
    var allocator: std.heap.DebugAllocator(.{ .stack_trace_frames = 0 }) = .init;
    defer std.testing.expectEqual(.ok, allocator.deinit()) catch @panic("leak");
    const a = allocator.allocator();
    var directory = try db_mod.TestDirectory.init("replacement-bounded-writer");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false });
    defer db.close();
    const config = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
    try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = config });
    _ = try db.replaceGraphIndex("old", .{ .name = "new", .kind = .graph, .config_json = config });
    const jobs = try db.indexes.graph.replacementStatusAlloc(a);
    defer jobs.deinit();
    var job = jobs.value[0];
    job.phase = .catching_up; // initial corpus is empty and already built
    job.activation_attempts = 8;
    try db.core.index_manager.saveGraphReplacementBuild(db.core.store, job);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var rows: std.ArrayList(db_mod.types.BatchWrite) = .empty;
    for (0..128) |i| try rows.append(arena.allocator(), .{ .key = try std.fmt.allocPrint(arena.allocator(), "source:{d:0>4}", .{i}), .value = "{\"links\":[]}" });
    const Check = struct {
        var database: *DB = undefined;
        var writes: []const db_mod.types.BatchWrite = &.{};
        var started: std.Io.Event = .unset;
        var done: std.Io.Event = .unset;
        var thread: ?std.Thread = null;
        var failed: bool = false;
        var batches: usize = 0;
        var late_batch: bool = false;
        var started_ns: u64 = 0;
        var finished_ns: u64 = 0;
        fn writer() void {
            const io = database.core.index_manager.checkpointIo();
            started_ns = db_mod.monotonicTimeNs();
            started.set(io);
            database.batch(.{ .writes = &.{.{ .key = "writer", .value = "{\"links\":[]}" }} }) catch {
                failed = true;
            };
            finished_ns = db_mod.monotonicTimeNs();
            done.set(io);
        }
        fn activate(db_to_activate: *DB) anyerror!void {
            // Introduce the tail after unlocked catch-up, immediately before
            // the real addIndexForReconciliation activation acquires apply.
            try db_to_activate.batch(.{ .writes = writes });
        }
        fn batch(_: *DB, _: usize) void {
            const io = database.core.index_manager.checkpointIo();
            if (thread == null) {
                thread = std.Thread.spawn(.{}, writer, .{}) catch @panic("spawn");
                started.waitUncancelable(io);
            }
            if (done.isSet()) @panic("writer escaped production activation gate");
            late_batch = late_batch or DB.test_graph_replacement_work_now_ns.? >= 11 * std.time.ns_per_s;
            batches += 1;
            DB.test_graph_replacement_work_now_ns.? += 400 * std.time.ns_per_ms;
        }
    };
    Check.database = &db;
    Check.writes = rows.items;
    Check.started = .unset;
    Check.done = .unset;
    Check.thread = null;
    Check.failed = false;
    Check.batches = 0;
    Check.late_batch = false;
    DB.test_graph_replacement_work_now_ns = 10 * std.time.ns_per_s;
    defer DB.test_graph_replacement_work_now_ns = null;
    DB.test_graph_replacement_before_activation = Check.activate;
    defer DB.test_graph_replacement_before_activation = null;
    DB.test_graph_replacement_tail_batch = Check.batch;
    defer DB.test_graph_replacement_tail_batch = null;
    defer if (Check.thread) |worker| worker.join();
    try std.testing.expect(try db.indexes.graph.finishBackgroundReplacement("new", job.generation));
    const io = db.core.index_manager.checkpointIo();
    try Check.done.waitTimeout(io, .{ .duration = .{ .raw = .fromMilliseconds(1500), .clock = .awake } });
    Check.thread.?.join();
    Check.thread = null;
    try std.testing.expect(!Check.failed);
    try std.testing.expect(Check.finished_ns - Check.started_ns < 1500 * std.time.ns_per_ms);
    // A controlled work clock proves the actual escalation uses a 1 s budget,
    // with one in-flight 400 ms batch permitted to overrun. No test-owned apply
    // lock determines either the writer's release or the activation deadline.
    try std.testing.expectEqual(@as(usize, 3), Check.batches);
    try std.testing.expect(!Check.late_batch);
    try std.testing.expectEqual(@as(u64, 11_200 * std.time.ns_per_ms), DB.test_graph_replacement_work_now_ns.?);
    const progress = try db.indexes.graph.replacementStatusAlloc(a);
    defer progress.deinit();
    try std.testing.expectEqual(@as(usize, 96), progress.value[0].tail_source_cursor);
    try std.testing.expectEqual(job.applied_sequence, progress.value[0].applied_sequence);
    try std.testing.expect(db.indexGeneration("old") != null);
    try std.testing.expect(db.indexGeneration("new") == null);
}

test "capi resolver replacement clock clamp skips denied and ordered writes" {
    const a = std.testing.allocator;
    for (0..3) |mode| {
        var directory = try db_mod.TestDirectory.init("replacement-clamp-gate");
        defer directory.cleanup();
        var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false });
        defer db.close();
        const config = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
        try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = config });
        const generation = try db.replaceGraphIndex("old", .{ .name = "new", .kind = .graph, .config_json = config });
        DB.test_graph_replacement_retry_now_ns = 50 * std.time.ns_per_s;
        defer DB.test_graph_replacement_retry_now_ns = null;
        const future = db.replacementRetryNow() + 9 * std.time.ns_per_hour;
        {
            const jobs = try db.indexes.graph.replacementStatusAlloc(a);
            defer jobs.deinit();
            var job = jobs.value[0];
            job.retry_after_ns = future;
            try db.core.index_manager.saveGraphReplacementBuild(db.core.store, job);
        }
        const Gate = struct {
            fn deny(_: *const anyopaque) anyerror!void {
                return error.NotLeader;
            }
        };
        if (mode < 2) {
            db.local_execution.replication_write_gate = .{ .borrowed = .{ .ptr = &mode, .check_fn = Gate.deny, .allows_background_work = mode == 0 } };
        } else {
            try db_mod.graphTestApplyOrdered(&db, .{ .writes = &.{.{ .key = "replicated", .value = "{}" }} }, .{ .term = 1, .index = 1 });
        }
        try std.testing.expect(try db.indexes.graph.advanceReplacement());
        try std.testing.expectError(error.Busy, db.indexes.graph.waitReplacement("new", generation, 2));
        const jobs = try db.indexes.graph.replacementStatusAlloc(a);
        defer jobs.deinit();
        try std.testing.expectEqual(future, jobs.value[0].retry_after_ns);
        try std.testing.expectEqual(@as(u32, 0), jobs.value[0].total_retry_count);
    }
}

test "capi resolver replacement waits reject reused serving and reserved generations" {
    const a = std.testing.allocator;
    for (0..3) |mode| {
        var directory = try db_mod.TestDirectory.init("replacement-wait-generation");
        defer directory.cleanup();
        var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false });
        defer db.close();
        const config = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
        const different = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"other_links\"}]}";
        try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = config });
        const cfg: db_mod.types.IndexConfig = .{ .name = "new", .kind = .graph, .config_json = config };
        const generation = try db.replaceGraphIndex("old", cfg);
        try std.testing.expectEqual(generation, try db.replaceGraphIndex("old", cfg));
        try std.testing.expectError(error.Busy, db.indexes.graph.waitReplacement("new", generation, 0));
        try std.testing.expect(try db.indexes.graph.cancelReplacement("new"));
        try db.runUntilIdle();
        if (mode == 0) {
            try db.addIndex(.{ .name = "new", .kind = .graph, .config_json = different });
            try std.testing.expect(db.indexGeneration("new").? != generation);
            try std.testing.expectError(error.Cancelled, db.indexes.graph.waitReplacement("new", generation, 1000));
            try std.testing.expect(db.indexGeneration("old") != null);
        } else {
            const next_generation = try db.replaceGraphIndex("old", .{ .name = "new", .kind = .graph, .config_json = if (mode == 1) config else different });
            try std.testing.expect(next_generation != generation);
            try std.testing.expectError(error.Cancelled, db.indexes.graph.waitReplacement("new", generation, 1000));
            // An old waiter must neither follow nor advance the new job.
            const jobs = try db.indexes.graph.replacementStatusAlloc(a);
            defer jobs.deinit();
            try std.testing.expectEqual(.building, jobs.value[0].phase);
            try std.testing.expectEqual(next_generation, jobs.value[0].generation);
            try db.runUntilIdle();
            try db.indexes.graph.waitReplacement("new", next_generation, 0);
            try std.testing.expectError(error.Cancelled, db.indexes.graph.waitReplacement("new", generation, 0));
        }
    }
}

test "capi resolver replacement wait rechecks generation in the publication gap" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("replacement-wait-reused-gap");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false });
    defer db.close();
    const config = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
    try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = config });
    const generation = try db.replaceGraphIndex("old", .{ .name = "new", .kind = .graph, .config_json = config });
    const Gap = struct {
        fn reuse(database: *DB) anyerror!void {
            DB.test_graph_replacement_wait_observation = null;
            try std.testing.expect(try database.indexes.graph.cancelReplacement("new"));
            try database.runUntilIdle();
            try database.addIndex(.{ .name = "new", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"other_links\"}]}" });
        }
    };
    DB.test_graph_replacement_wait_observation = Gap.reuse;
    defer DB.test_graph_replacement_wait_observation = null;
    try std.testing.expectError(error.Cancelled, db.indexes.graph.waitReplacement("new", generation, 0));
    try std.testing.expect(db.indexGeneration("old") != null);
    try std.testing.expect(db.indexGeneration("new").? != generation);
}

test "inline resolver replacement named wait bypasses earlier queued producer prerequisites" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("review-multi-replacement");
    defer directory.cleanup();
    var runtime = try db_mod.background_runtime_mod.BackendRuntimeHandle.init(a, .{ .backend = .manual, .borrowed_io = .{ .general = std.testing.io } });
    defer runtime.deinit();
    var db = try DB.open(a, directory.path(), .{ .backend_runtime = runtime.ptr(), .executor = .{ .backend = .manual }, .start_optional_runtimes = false, .start_optional_runtime_workers = false, .start_independent_maintenance = false });
    defer db.close();
    try db.addIndex(.{ .name = "old_a", .kind = .graph, .config_json = "{}" });
    try db.addIndex(.{ .name = "old_b", .kind = .graph, .config_json = "{}" });
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"relations\":[{\"type\":\"R\",\"target\":\"doc:b\"}]}" }} });
    const config_a = "{\"source\":{\"artifact\":\"relations_a\",\"format\":\"extraction_relation\"},\"artifact\":{\"name\":\"relations_a\",\"kind\":\"asset\",\"source\":{\"type\":\"field\",\"value\":\"relations\"},\"content_type\":\"application/json\"}}";
    const config_b = "{\"source\":{\"artifact\":\"relations_b\",\"format\":\"extraction_relation\"},\"artifact\":{\"name\":\"relations_b\",\"kind\":\"asset\",\"source\":{\"type\":\"field\",\"value\":\"relations\"},\"content_type\":\"application/json\"}}";
    const generation_a = try db.replaceGraphIndex("old_a", .{ .name = "new_a", .kind = .graph, .config_json = config_a });
    const generation_b = try db.replaceGraphIndex("old_b", .{ .name = "new_b", .kind = .graph, .config_json = config_b });
    // Waiting only for the later name must finish without first building
    // an unrelated replacement; no worker or separate drain is available.
    try db.indexes.graph.waitReplacement("new_b", generation_b, 30_000);
    try std.testing.expect(db.indexGeneration("new_a") == null);
    try std.testing.expect(db.indexGeneration("old_a") != null);
    try db.indexes.graph.waitReplacement("new_a", generation_a, 30_000);
    try db.indexes.graph.waitReplacement("new_b", generation_b, 30_000);
}

test "inline resolver replacement named scheduler rejects a reused reservation generation" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("replacement-named-scheduler-generation");
    defer directory.cleanup();
    var host = try db_mod.background_runtime_mod.BackendRuntimeHandle.init(a, .{ .backend = .manual, .borrowed_io = .{ .general = std.testing.io } });
    defer host.deinit();
    var db = try DB.open(a, directory.path(), .{ .backend_runtime = host.ptr(), .executor = .{ .backend = .manual }, .start_optional_runtimes = false, .start_independent_maintenance = false });
    defer db.close();
    try db.addIndex(.{ .name = "old", .kind = .graph, .config_json = "{}" });
    const cfg: db_mod.types.IndexConfig = .{ .name = "new", .kind = .graph, .config_json = "{}" };
    const previous = try db.replaceGraphIndex("old", cfg);
    try std.testing.expect(try db.indexes.graph.cancelReplacement("new"));
    try db.runUntilIdle();
    const current = try db.replaceGraphIndex("old", cfg);
    try std.testing.expect(current != previous);
    // Model reuse between a waiter's observation and scheduler admission.
    try std.testing.expect(try db.indexes.graph.advanceReplacementForIdentityUntil(null, .{ .name = "new", .generation = previous }, .manual));
    try std.testing.expect(db.indexGeneration("new") == null);
    const jobs = try db.indexes.graph.replacementStatusAlloc(a);
    defer jobs.deinit();
    try std.testing.expectEqual(current, jobs.value[0].generation);
    try std.testing.expectEqual(@as(u64, 0), jobs.value[0].scanned);
    try db.indexes.graph.waitReplacement("new", current, 30_000);
}
