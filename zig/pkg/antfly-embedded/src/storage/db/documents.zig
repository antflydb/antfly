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
const db_mod = @import("db.zig");
const DB = db_mod.DB;

/// Document state owned by `DB`: authority backfill and repair, group source
/// maintenance, and the bulk ingest session.
pub const Documents = struct {
    authority: Authority = .{},
    group_sources: GroupSources = .{},
    bulk_ingest: BulkIngest = .{},

    /// Document authority backfill and repair: pages legacy documents into the authority index and attaches distributed authority snapshots.
    pub const Authority = struct {
        backfill_active: std.atomic.Value(bool) = .init(true),
        backfill_requested: std.atomic.Value(bool) = .init(false),
        backfill_mutex: std.atomic.Mutex = .unlocked,
        backfill_restart: bool = false,
        repairs: @import("../document_mutation_revision.zig").RepairQueue = .{},
        repairs_yield_sweep: bool = false,
    };

    /// Group source maintenance: the local worker owner, or state borrowed from the native connection owner.
    pub const GroupSources = struct {
        owner: @import("group_source_owner.zig").Owner = .{},
        /// Borrowed from the native connection owner; stable across DB refreshes.
        /// The owner serializes every producer slice and outlives this wrapper.
        borrowed_state: ?*DB.GroupSourceMaintenanceState = null,
    };

    /// The active bulk ingest session and its document identity scratch.
    pub const BulkIngest = struct {
        session: @import("bulk_ingest_session.zig").State = .{},
        identity: @import("bulk_ingest_session.zig").IdentityScratch = .{},
    };

    fn dbPtr(self: *Documents) *DB {
        return @alignCast(@fieldParentPtr("documents", self));
    }

    /// Local derived-cache metadata, not a user mutation or replication fact.
    /// Leadership and root-image gates cover the short publication transaction;
    /// hashing and scanning happen beforehand under a disposable read snapshot.
    pub fn advanceAuthorityBackfillPage(self: *Documents) !bool {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("advanceDocumentAuthorityBackfillPage", .{db});
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return true;
        if (!db_mod.replicationWriteGateAllowsBackgroundWork(db.local_execution.replication_write_gate)) return false;
        if (!self.authority.backfill_mutex.tryLock()) return false;
        defer self.authority.backfill_mutex.unlock();
        if (self.authority.repairs.pending.load(.acquire) and !self.authority.repairs_yield_sweep) {
            var page = self.authority.repairs.take() orelse return false;
            defer page.deinit(db.alloc);
            errdefer if (!page.retry(&self.authority.repairs, db.alloc)) self.authority.backfill_requested.store(true, .release);
            var repair_ctx = db.batchContext();
            var repair_mutation = db_mod.acquireReplicationMutationSharedContext(&repair_ctx);
            defer if (repair_mutation) |*lease| lease.release();
            try db_mod.enforceReplicationWriteGateOptional(repair_ctx.replication_write_gate);
            var repair_replay = try db_mod.acquireSnapshotReplayContext(&repair_ctx);
            defer if (repair_replay) |*lease| lease.release();
            try db_mod.lockApplyForPortableRuntimeContext(&repair_ctx);
            defer repair_ctx.apply_mutex.unlockExclusive();
            var writer = try db.core.store.beginWriteTxn();
            errdefer writer.abort();
            _ = try page.stage(&writer);
            try writer.commit();
            self.authority.repairs_yield_sweep = self.authority.backfill_active.load(.acquire) or self.authority.backfill_requested.load(.acquire);
            return !self.authority.repairs_yield_sweep and !self.authority.repairs.pending.load(.acquire);
        }
        self.authority.repairs_yield_sweep = false;
        const restart = if (!self.authority.backfill_active.load(.acquire)) self.authority.backfill_requested.swap(false, .acq_rel) else false;
        if (!self.authority.backfill_active.load(.acquire) and !restart) return !self.authority.repairs.pending.load(.acquire);
        self.authority.backfill_active.store(true, .release);
        self.authority.backfill_restart = self.authority.backfill_restart or restart;
        var ctx = db.batchContext();
        var replication_mutation = db_mod.acquireReplicationMutationSharedContext(&ctx);
        defer if (replication_mutation) |*lease| lease.release();
        try db_mod.enforceReplicationWriteGateOptional(ctx.replication_write_gate);
        var snapshot_replay = try db_mod.acquireSnapshotReplayContext(&ctx);
        defer if (snapshot_replay) |*lease| lease.release();
        var budget = if (db.core.index_manager.resource_manager) |manager|
            db_mod.resource_manager_mod.BudgetedAllocator.init(manager, .relational_preparation_working_set, db.alloc, 1)
        else
            null;
        defer if (budget) |*value| value.deinit();
        const scratch = if (budget) |*value| value.allocator() else db.alloc;
        var prepared = (@import("../document_mutation_revision.zig").BackfillPage.prepare(scratch, db.core.store, self.authority.backfill_restart) catch |err| {
            if (err == error.OutOfMemory) if (budget) |*value| if (value.denied()) return error.ResourceBudgetExceeded;
            return err;
        }) orelse {
            self.authority.backfill_active.store(false, .release);
            return !self.authority.repairs.pending.load(.acquire);
        };
        defer prepared.deinit();
        try db_mod.lockApplyForPortableRuntimeContext(&ctx);
        defer ctx.apply_mutex.unlockExclusive();
        var writer = try db.core.store.beginWriteTxn();
        errdefer writer.abort();
        _ = try prepared.stage(&writer);
        try writer.commit();
        self.authority.backfill_restart = false;
        self.authority.backfill_active.store(!prepared.complete, .release);
        return prepared.complete and !self.authority.repairs.pending.load(.acquire);
    }

    pub fn attachDistributedAuthority(self: *Documents, alloc: Allocator, req: db_mod.types.SearchRequest, result: *db_mod.types.SearchResult) !void {
        const db = self.dbPtr();
        if (req.document_authority_mode == .none) return;
        if (req.document_authority_group == 0 or req.include_stored or req.return_mode != .parent or req.highlight != null or req.graph_queries.len != 0 or req.aggregations_json.len != 0) return error.InvalidQueryRequest;
        var schema_view = db.core.acquireSchemaView();
        defer if (schema_view) |*view| view.release();
        try self.attachDistributedAuthorityAtSnapshot(alloc, req, result, null, schema_view);
    }

    pub fn attachDistributedAuthorityAtSnapshot(self: *Documents, alloc: Allocator, req: db_mod.types.SearchRequest, result: *db_mod.types.SearchResult, pinned: ?*db_mod.docstore_mod.DocStore.Txn, schema_view: ?db_mod.schema_registry_mod.SchemaView) !void {
        const db = self.dbPtr();
        if (req.document_authority_mode == .none) return;
        if (req.document_output_allocator != null and req.document_authority_mode != .hydrate) return error.InvalidQueryRequest;
        const scalar = @import("../../api/field_projection.zig");
        var scalar_arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(alloc));
        defer scalar_arena.deinit();
        const native_json = if (schema_view) |view| view.tableSchema().storage_mode != .relational and view.tableSchema().external_base_source == null else true;
        const scalar_eligible = native_json and scalar.hydrationEligible(req);
        const borrowed_scalar = if (scalar_eligible) (if (req.field_projection_plan) |shared| shared.get(req.field_projection_json) else null) else null;
        var owned_scalar: ?scalar.Plan = if (scalar_eligible and borrowed_scalar == null) try scalar.decode(scalar_arena.allocator(), req.field_projection_json) else null;
        defer if (owned_scalar) |*plan| plan.deinit();
        const scalar_plan: ?*const scalar.Plan = borrowed_scalar orelse (if (owned_scalar) |*plan| plan else null);
        var scalar_public_plan = if (scalar_plan != null) try @import("document_query.zig").PublicPlan.init(scalar_arena.allocator(), .{ .fields = req.fields, .include_all_fields = req.include_all_fields }) else null;
        defer if (scalar_public_plan) |*plan| plan.deinit();
        const scalar_operands = if (scalar_plan) |plan| try @import("field_projection_source.zig").OperandPlans.init(scalar_arena.allocator(), plan) else null;
        var scalar_authorities: @import("../conditional_source.zig").Window = .{};
        var scalar_batch = @import("field_projection_source.zig").Batch.init(@import("antfly_platform").allocator.reclaimingScratch(alloc));
        defer scalar_batch.deinit();
        var scalar_metadata_rows: usize = 0;
        var scalar_certificate_rows: usize = 0;
        var scalar_fallback_rows: usize = 0;
        const lookup_opts: db_mod.types.LookupOptions = .{ .output_exact = req.document_output_exact, .fields = if (req.defer_stored_projection) &.{} else req.fields, .include_all_fields = req.defer_stored_projection or req.include_all_fields };
        var plans = try db_mod.LookupProjectionPlans.init(alloc, lookup_opts);
        defer plans.deinit();
        var read: ?db_mod.docstore_mod.DocStore.Txn = if (pinned) |captured_read| try captured_read.forkRead() else try db.core.store.beginReadTxn();
        defer if (read) |*owned| owned.abort();
        var metadata_scratch = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(alloc));
        defer metadata_scratch.deinit();
        var projection_scratch = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(alloc));
        defer projection_scratch.deinit();
        var primary_window = db_mod.DocumentPrimaryReadWindow.init(alloc);
        defer primary_window.deinit();
        var physical: [64][]const u8 = undefined;
        var ids: [64][]const u8 = undefined;
        var contents: [64]?@import("../document_mutation_revision.zig").Content = undefined;
        var versions: [64]u64 = undefined;
        var mutation_revisions: [64]u64 = undefined;
        const body_allocator = req.document_output_allocator orelse alloc;
        if (req.document_output_allocator != null) result.stored_data_allocator = body_allocator;
        var eager_bytes: usize = 0;
        var primary_batch_end: usize = 0;
        var primary_values: [64]?[]const u8 = undefined;
        for (result.hits, 0..) |*hit, index| {
            if (req.cancellation) |token| try token.check();
            if (req.execution_deadline_ns) |deadline| if (db_mod.platform_time.monotonicNs() >= deadline) return error.Timeout;
            // Apply-shared fences ordinary reads; captured dense reads fork
            // the original visibility cut. Retire page scratch every 64 hits.
            if (index != 0 and index % 64 == 0) {
                primary_window.retire();
                read.?.abort();
                read = null;
                read = if (pinned) |captured_read| try captured_read.forkRead() else try db.core.store.beginReadTxn();
            }
            if (index % 64 == 0) {
                scalar_batch.reset();
                if (!metadata_scratch.reset(.{ .retain_with_limit = 64 * 1024 })) _ = metadata_scratch.reset(.free_all);
                const m = metadata_scratch.allocator();
                const count = @min(@as(usize, 64), result.hits.len - index);
                for (result.hits[index..][0..count], 0..) |candidate, row| {
                    ids[row] = candidate.id;
                    physical[row] = try db_mod.encodeStoreLookupKeyWithPinnedSchemaAlloc(db, m, candidate.id, schema_view);
                }
                var scope = try read.?.openReadScope(@import("antfly_platform").allocator.reclaimingScratch(alloc));
                defer scope.close();
                try @import("../document_mutation_revision.zig").loadWindowWithRevisions(&scope, m, physical[0..count], ids[0..count], contents[0..count], versions[0..count], mutation_revisions[0..count]);
                if (scalar_plan != null and db.core.store.valuesAreAuthenticated()) scalar_authorities = try @import("../conditional_source.zig").Window.prepare(m, &scope, physical[0..count], ids[0..count]);
            }
            if (req.cancellation) |token| try token.check();
            if (req.execution_deadline_ns) |deadline| if (db_mod.platform_time.monotonicNs() >= deadline) return error.Timeout;
            const store_key = physical[index % 64];
            const cached = contents[index % 64];
            var raw_primary: ?[]const u8 = null;
            defer {
                // Prefetched spans stay live until their last consumer retires.
                if (index >= primary_batch_end or index + 1 == primary_batch_end)
                    primary_window.finishRow(cached == null);
            }
            var digest: [32]u8 = undefined;
            const primary_bytes = if (cached) |content| cached: {
                digest = content.digest;
                break :cached content.bytes;
            } else legacy: {
                const raw = primary_window.get(&read.?, store_key, null) catch |err| return if (err == error.NotFound) error.StorageReadTemporarilyUnavailable else err;
                raw_primary = raw;
                std.crypto.hash.sha2.Sha256.hash(raw, &digest, .{});
                const revision = mutation_revisions[index % 64];
                if (!self.authority.repairs.enqueue(db.alloc, store_key, .{ .revision = revision, .bytes = raw.len, .kind = store_key[store_key.len - 1], .digest = digest }))
                    self.authority.backfill_requested.store(true, .release);
                break :legacy raw.len;
            };
            const version = versions[index % 64];
            const authority: db_mod.types.document_authority.Authority = .{ .group = req.document_authority_group, .version = version, .digest = digest };
            // Small pages stay in one round trip; large candidates travel
            // as proofs until their global window has been selected.
            const eager = req.document_authority_mode == .candidates and primary_bytes <= 8 * 1024 and primary_bytes <= 64 * 1024 -| eager_bytes;
            if (scalar_plan == null and raw_primary == null and (req.document_authority_mode == .hydrate or eager)) {
                if (index >= primary_batch_end) {
                    const offset = index % 64;
                    const count = @min(@as(usize, 64) - offset, result.hits.len - index);
                    const byte_limit = if (req.document_authority_mode == .hydrate) db_mod.DocumentPrimaryReadWindow.max_bytes else 64 * 1024 -| eager_bytes;
                    var batch_count: usize = 0;
                    var bytes: usize = 0;
                    while (batch_count < count) : (batch_count += 1) {
                        const next = contents[offset + batch_count] orelse break;
                        if (req.document_authority_mode == .candidates and next.bytes > 8 * 1024) break;
                        if (next.bytes > byte_limit -| bytes) break;
                        bytes += @intCast(next.bytes);
                    }
                    if (batch_count > 1) {
                        try primary_window.getMany(&read.?, physical[offset..][0..batch_count], primary_values[offset..][0..batch_count]);
                        primary_batch_end = index + batch_count;
                    }
                }
                raw_primary = if (index < primary_batch_end)
                    primary_values[index % 64] orelse return error.StorageReadTemporarilyUnavailable
                else
                    primary_window.get(&read.?, store_key, primary_bytes) catch |err| return if (err == error.NotFound) error.StorageReadTemporarilyUnavailable else err;
            }
            if (!projection_scratch.reset(.{ .retain_with_limit = 64 * 1024 })) _ = projection_scratch.reset(.free_all);
            var projected: ?db_mod.types.LookupResult = if (scalar_plan) |plan| lazy: {
                const a = projection_scratch.allocator();
                var row: @import("field_projection_source.zig").Row = .{ .a = a, .txn = &read.?, .physical = store_key, .id = hit.id, .req = req, .authority = scalar_authorities.rows[index % 64], .full_bytes = raw_primary, .public_plan = if (scalar_public_plan) |*public| public else null, .operands = if (scalar_operands) |*operands| operands else null };
                defer row.deinit();
                if (raw_primary == null) if (scalar_operands) |*operands| {
                    const count = @min(@as(usize, 64), result.hits.len - index + index % 64);
                    if (try scalar_batch.at(&read.?, index % 64, physical[0..count], ids[0..count], scalar_authorities.rows[0..count], req, operands)) |prepared| row.attach(prepared);
                };
                const previous = if (hit.computed_json) |bytes| try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{}) else null;
                const encoded = try plan.evaluateEncoded(a, alloc, row.source(), previous, scalar.requestLimits(&req));
                errdefer alloc.free(encoded);
                const receipt = if (previous == null) try plan.ownValidatedReceipt(alloc, encoded) else null;
                errdefer if (receipt) |owned| alloc.destroy(owned);
                const source = try row.base(body_allocator);
                if (hit.computed_json) |bytes| alloc.free(bytes);
                if (hit.computed_receipt) |owned| alloc.destroy(owned);
                hit.computed_json = encoded;
                hit.computed_receipt = receipt;
                if (row.metadata_hits != 0) scalar_metadata_rows += 1 else scalar_fallback_rows += 1;
                if (row.certificate_hits != 0) scalar_certificate_rows += 1;
                if (scalar_authorities.rows[index % 64].repair) self.authority.backfill_requested.store(true, .release);
                break :lazy .{ .json = source };
            } else if (req.document_authority_mode == .hydrate or eager)
                try db.lookupPrimarySnapshotWithPreparedProjection(projection_scratch.allocator(), body_allocator, hit.id, lookup_opts, &read.?, schema_view, raw_primary, version, &plans)
            else
                null;
            if (req.document_authority_mode == .candidates) if (projected) |*document| {
                // Charge actual projected JSON, including native rows whose
                // compact storage bytes can expand during serialization.
                if (document.json.len > 8 * 1024 or document.json.len > 64 * 1024 -| eager_bytes) {
                    document.deinit(body_allocator);
                    projected = null;
                } else eager_bytes += document.json.len;
            };
            defer if (projected) |*document| document.deinit(body_allocator);
            if (req.document_authority_mode == .hydrate and projected == null) return error.StorageReadTemporarilyUnavailable;
            const owned_authority = try alloc.create(db_mod.types.document_authority.Authority);
            owned_authority.* = authority;
            errdefer alloc.destroy(owned_authority);
            // Projection already has final body ownership. No arena copy.
            const document_bytes = if (projected) |document| document.json else null;
            projected = null;
            if (hit.stored_data) |bytes| alloc.free(bytes);
            if (hit.source_value) |*value| db_mod.types.deinitJsonValue(alloc, value);
            if (hit.column_source) |source| source.deinit();
            hit.source_value = null;
            hit.column_source = null;
            if (hit.document_authority) |proof| alloc.destroy(proof);
            hit.document_authority = owned_authority;
            hit.stored_data = document_bytes;
            if (scalar_plan != null) hit.finalized_source = .{ .bytes = document_bytes.? };
        }
        result.document_authority_version = 1;
        if (scalar_plan != null and db_mod.benchQueryProfileEnabled()) std.log.info("antfly_bench_projection metadata_rows={d} fallback_rows={d} certificate_rows={d}", .{ scalar_metadata_rows, scalar_fallback_rows, scalar_certificate_rows });
    }

    pub fn beginDenseAutoBulkIngest(self: *Documents) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("beginDenseAutoBulkIngestSession", .{db});
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        try db.enforceReplicationWriteGate();
        try db.enforcePortableRuntimeGate();
        try db_mod.beginExternalDenseBulkSessionTrackedWait(db.async_context, db.backend_runtime.io());
        errdefer db_mod.finishExternalDenseBulkSessionTrackedBestEffort(db.async_context);
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        const resources = db.core.batchExecutionResources();
        try resources.index_manager.beginDenseBulkIngestSessions();
        self.bulk_ingest.session.begin();
    }

    pub fn beginPrimaryStoreAutoBulkIngest(self: *Documents) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("beginPrimaryStoreAutoBulkIngestSession", .{db});
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        try db.enforceReplicationWriteGate();
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        const resources = db.core.batchExecutionResources();
        try resources.store.beginBulkIngestSession();
        errdefer resources.store.abortBulkIngestSession();
        try self.configureBulkIngestIdentityAllNewLocked();
        errdefer db.clearBulkIngestIdentityAllNewLocked();
        self.bulk_ingest.session.begin();
    }

    pub fn finishPrimaryStoreAutoBulkIngest(self: *Documents, options: db_mod.backend_types.BulkIngestFinishOptions) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("finishPrimaryStoreAutoBulkIngestSessionWithOptions", .{ db, options });
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        try db.enforceReplicationWriteGate();
        {
            try db.lockApplyForPortableRuntime();
            defer db.core.unlockApply();
            const resources = db.core.batchExecutionResources();
            resources.store.finishBulkIngestSessionWithOptions(options) catch |err| {
                self.bulk_ingest.session.finish();
                db.clearBulkIngestIdentityAllNewLocked();
                return err;
            };
            self.bulk_ingest.session.finish();
            db.clearBulkIngestIdentityAllNewLocked();
        }
        db_mod.flushDeferredExternalBulkExecutorNotificationOrTarget(
            db.async_context,
            db.executor,
            db.core.nextDerivedSequence(),
        );
        DB.notifyQueryVisibilityHook(db.async_context, .publish);
    }

    pub fn rollPrimaryStoreAutoBulkIngest(self: *Documents, options: db_mod.backend_types.BulkIngestFinishOptions) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("rollPrimaryStoreAutoBulkIngestSessionWithOptions", .{ db, options });
        try self.finishPrimaryStoreAutoBulkIngest(options);
        try self.beginPrimaryStoreAutoBulkIngest();
    }

    pub fn finishDenseAutoBulkIngest(self: *Documents, options: db_mod.backend_types.BulkIngestFinishOptions) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("finishDenseAutoBulkIngestSessionWithOptions", .{ db, options });
        try self.finishDenseAutoBulkIngestInternal(options, true);
    }

    pub fn rollDenseAutoBulkIngest(self: *Documents, options: db_mod.backend_types.BulkIngestFinishOptions) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("rollDenseAutoBulkIngestSessionWithOptions", .{ db, options });
        try self.finishDenseAutoBulkIngestInternal(options, true);
        try self.beginDenseAutoBulkIngest();
    }

    pub fn finishDenseAutoBulkIngestInternal(self: *Documents, options: db_mod.backend_types.BulkIngestFinishOptions, notify_executor: bool) !void {
        const db = self.dbPtr();
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        try db.enforceReplicationWriteGate();
        var external_session_tracked = true;
        defer if (external_session_tracked) db_mod.finishExternalDenseBulkSessionTrackedBestEffort(db.async_context);
        {
            try db.lockApplyForPortableRuntime();
            defer db.core.unlockApply();
            const resources = db.core.batchExecutionResources();
            resources.store.flushBufferedWritesWithOptions(options) catch |err| {
                self.bulk_ingest.session.finish();
                return err;
            };
            resources.index_manager.finishDenseBulkIngestSessionsWithOptions(options) catch |err| {
                self.bulk_ingest.session.finish();
                db.clearBulkIngestIdentityAllNewLocked();
                return err;
            };
            self.bulk_ingest.session.finish();
            db.clearBulkIngestIdentityAllNewLocked();
        }
        external_session_tracked = false;
        _ = try db_mod.finishExternalDenseBulkSessionTrackedAndFinalize(db.async_context);
        if (notify_executor) {
            db_mod.flushDeferredExternalBulkExecutorNotificationOrTarget(
                db.async_context,
                db.executor,
                db.core.nextDerivedSequence(),
            );
        }
        DB.notifyQueryVisibilityHook(db.async_context, .publish);
    }

    pub fn abortDenseAutoBulkIngest(self: *Documents) void {
        const db = self.dbPtr();
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        db_mod.lockApply(db);
        {
            defer db.core.unlockApply();
            const resources = db.core.batchExecutionResources();
            self.bulk_ingest.session.finish();
            resources.index_manager.abortDenseBulkIngestSessions();
        }
        db_mod.finishExternalDenseBulkSessionTrackedBestEffort(db.async_context);
        db_mod.flushDeferredExternalBulkExecutorNotification(db.async_context, db.executor);
    }

    pub fn abortPrimaryStoreAutoBulkIngest(self: *Documents) void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("abortPrimaryStoreAutoBulkIngestSession", .{db}) catch @panic("incompatible storage owner");
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        db_mod.lockApply(db);
        defer db.core.unlockApply();
        const resources = db.core.batchExecutionResources();
        self.bulk_ingest.session.finish();
        db.clearBulkIngestIdentityAllNewLocked();
        resources.store.abortBulkIngestSession();
    }

    pub fn configureBulkIngestIdentityAllNewLocked(self: *Documents) !void {
        const db = self.dbPtr();
        db.clearBulkIngestIdentityAllNewLocked();
        if (!try db.primaryUserNamespaceIsEmptyLocked()) return;
        if (try db_mod.doc_identity.loadAllNewTrustedStateForNamespace(db.core.store, db.core.identity_namespace)) |state| {
            self.bulk_ingest.identity.trusted = state;
            db.core.identity_visibility.summary = state.visibility_summary;
            db.clearLiveDocSetCache();
            db.clearNonVisibleDocSetCache();
            self.bulk_ingest.identity.enabled = true;
        }
    }

    pub fn runLocalGroupSourceSlice(ptr: *anyopaque) !bool {
        const db: *DB = @ptrCast(@alignCast(ptr));
        if (!db.stable_address or db.closed or db.open_mode != .writer or !db.local_execution.optional_runtime_workers_enabled or db.core.index_manager.minimum_reader_version.load(.acquire) < 8) return false;
        return db.documents.runGroupSourceSlice();
    }

    /// One bounded producer/control slice, without inference. Native Lite calls
    /// this on the live coordinator under its short path lease, then replays
    /// downstream providers in its separate detached turn.
    pub fn runGroupSourceMaintenanceSlice(self: *Documents, state: *DB.GroupSourceMaintenanceState) !bool {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("runGroupSourceMaintenanceSlice", .{ db, state });
        const before = state.submitted;
        _ = try self.runGroupSourceSliceWithState(state);
        // Only a committed control mutation makes detached provider replay
        // eligible. Visiting an idle sibling must not reopen provider state.
        return state.submitted != before;
    }

    pub fn runGroupSourceSlice(self: *Documents) !bool {
        return self.runGroupSourceSliceWithState(self.group_sources.borrowed_state orelse &self.group_sources.owner.state);
    }

    fn runGroupSourceSliceWithState(self: *Documents, state: *DB.GroupSourceMaintenanceState) !bool {
        const db = self.dbPtr();
        if (!state.slice_mutex.tryLock()) return false;
        defer state.slice_mutex.unlock();
        // The resident scheduler runs independently of foreground index DDL.
        // A slice borrows the current producer through batchContext as well
        // as the final wake. Keep that entire bounded, inference-free turn
        // inside the replacement fence so no borrowed runtime can be freed.
        // Yield during reconfiguration rather than blocking the scheduler;
        // durable group queues retain every pending command for the next turn.
        if (!db.enrichment_owner.replacement_mutex.tryLock()) return false;
        defer db.enrichment_owner.replacement_mutex.unlock();
        return self.runGroupSourceSliceLocked(state);
    }

    pub fn runGroupSourceSliceLocked(self: *Documents, state: *DB.GroupSourceMaintenanceState) !bool {
        const db = self.dbPtr();
        // A table-wide coordinator must drive partial owners. The native
        // worker can certify membership only for an unpartitioned Lite DB.
        if (db.core.byteRange().start.len != 0 or db.core.byteRange().end.len != 0) return false;
        if (db.open_mode != .writer or db.core.index_manager.minimum_reader_version.load(.acquire) < 8 or self.group_sources.owner.stopping.load(.acquire)) return false;
        if (try db.maintenanceRequiresOrderedApply()) return false;
        try db_mod.enforceReplicationWriteGateOptional(db.local_execution.replication_write_gate);
        var budget = if (db.core.index_manager.resource_manager) |manager| db_mod.resource_manager_mod.BudgetedAllocator.init(manager, .shard_transition_working_set, db.runtime_alloc, 1) else null;
        defer if (budget) |*value| value.deinit();
        var arena = std.heap.ArenaAllocator.init(if (budget) |*value| value.allocator() else db.runtime_alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const work = @import("group_source_worker.zig");
        const raw = (try self.lookupGroupSource(a, "{\"action\":\"definitions\"}")) orelse return false;
        const definitions = try std.json.parseFromSliceLeaky([]work.Definition, a, raw.json, .{});
        if (definitions.len == 0) return false;
        const definition = definitions[state.next_definition % definitions.len];
        state.next_definition +%= 1;
        const namespace = definition.namespace;
        if (state.worker == null) {
            const identity = try std.json.Stringify.valueAlloc(a, .{ db.root_incarnation, db_mod.platform_time.realtimeNs(), namespace }, .{});
            state.worker = .{ .allocator = state.allocator orelse db.runtime_alloc, .identity = @import("group_sources.zig").digest(identity) };
        }
        var context = DB.LocalGroupSourceContext{ .db = db, .state = state };
        const ports = work.Ports{ .ptr = &context, .query_fn = localGroupSourceQuery, .submit_fn = localGroupSourceSubmit, .fan_in_fn = localGroupSourceFanIn, .settled_fn = localGroupSourceSettled };
        const before = state.submitted;
        try state.worker.?.sourceStep(a, ports, definition);
        try state.worker.?.groupStep(a, ports, definition, @intCast(@divFloor(db_mod.platform_time.realtimeNs(), std.time.ns_per_ms)));
        const changed = state.submitted != before;
        state.idle_definitions = if (changed) 0 else state.idle_definitions +| 1;
        // A no-op GC/idle definition is not proof that its siblings are idle.
        // Complete a fair round before allowing a manual or connection owner
        // to sleep. Progress resets the round so downstream receipts can settle.
        return changed or state.idle_definitions < definitions.len;
    }

    fn localGroupSourceQuery(ptr: *anyopaque, a: Allocator, route: []const u8, request: []const u8) ![]const u8 {
        const context: *DB.LocalGroupSourceContext = @ptrCast(@alignCast(ptr));
        const db = context.db;
        if (!db.core.byteRange().contains(route)) return error.KeyOutOfRange;
        const result = (try db.documents.lookupGroupSource(a, request)) orelse return error.NotFound;
        return result.json;
    }

    fn localGroupSourceSubmit(ptr: *anyopaque, command: @import("group_source_contract.zig").Command) !void {
        const context: *DB.LocalGroupSourceContext = @ptrCast(@alignCast(ptr));
        const db = context.db;
        if (try db.maintenanceRequiresOrderedApply()) return error.ArtifactCatalogDrift;
        try db.batchInternal(.{ .group_source = command, .sync_level = .write }, null, .{});
        context.state.submitted +%= 1;
        // Group publication appends private producer rows. Independent resident
        // owners need the same downstream wake as an enrichments-level wait;
        // connection-owned Lite observes the durable sequence on its next turn.
        if (db.enrichment_runtime) |runtime| runtime.notifySequence(db.core.nextDerivedSequence());
    }

    fn localGroupSourceFanIn(ptr: *anyopaque, a: Allocator, definition: @import("group_source_worker.zig").Definition, partition: @import("group_source_index.zig").Partition, ordinal: i64) !@import("group_source_worker.zig").FanIn {
        const context: *DB.LocalGroupSourceContext = @ptrCast(@alignCast(ptr));
        const db = context.db;
        // A partial local range cannot certify a table-wide fan-in. The
        // cluster adapter must enumerate all authoritative table owners.
        if (db.core.byteRange().start.len != 0 or db.core.byteRange().end.len != 0) return error.GroupedSourceTopologyUnavailable;
        const contract = @import("group_source_contract.zig");
        const reads = @import("group_source_reads.zig");
        var query = contract.Query{ .action = .partition, .artifact = definition.artifact, .definition = definition.definition, .group = partition.group, .segment = partition.segment, .cell = partition.cell, .ordinal = ordinal, .max_bytes = contract.max_partition_bytes };
        var members: std.ArrayList(@import("group_sources.zig").Member) = .empty;
        var revision: ?u64 = null;
        var bytes: usize = 0;
        var earlier_pending = false;
        while (true) {
            const request = try std.json.Stringify.valueAlloc(a, query, .{});
            const response = try localGroupSourceQuery(ptr, a, definition.route.value, request);
            const page = try std.json.parseFromSliceLeaky(reads.SourcePage, a, response, .{ .allocate = .alloc_always });
            earlier_pending = earlier_pending or page.earlier_pending;
            if (revision) |expected| {
                if (expected != page.revision) return error.EnrichmentSourceChanged;
            } else revision = page.revision;
            for (page.members) |member| {
                bytes += member.text.len + member.document.len;
                if (members.items.len == 65536 or bytes > contract.max_partition_bytes) return error.GroupedSourcePartitionTooLarge;
                try members.append(a, member);
            }
            if (page.done) break;
            if (page.cursor.len == 0 or std.mem.eql(u8, query.cursor, page.cursor)) return error.GroupedSourceCorrupt;
            query.cursor = page.cursor;
        }
        const sources = try a.alloc(contract.SourceCut, 1);
        sources[0] = .{ .owner = definition.source_owner, .revision = revision orelse 0 };
        return .{ .earlier_pending = earlier_pending, .members = try members.toOwnedSlice(a), .sources = sources, .topology_epoch = 0 };
    }

    fn localGroupSourceSettled(ptr: *anyopaque, sequence: u64, units: []const []const u8) !bool {
        const context: *DB.LocalGroupSourceContext = @ptrCast(@alignCast(ptr));
        return context.db.documents.groupSourceSettled(sequence, units);
    }

    pub fn groupSourceSettled(self: *Documents, sequence: u64, units: []const []const u8) !bool {
        const db = self.dbPtr();
        var arena = std.heap.ArenaAllocator.init(db.runtime_alloc);
        defer arena.deinit();
        var overlay = @import("group_source_mutations.zig").Overlay{ .completion_root = db.root_incarnation, .allocator = arena.allocator(), .reader = @import("group_source_mutations.zig").Reader.fromStore(db.core.store) };
        if (try @import("artifact_publication.zig").authority(&overlay) != null) {
            for (units) |unit| if (!try overlay.sourceSettled(unit)) return false;
            return true;
        }
        // A native standalone owner has no Raft publication authority. Its
        // ordinary committed replay cursor and per-producer repair receipts
        // provide completion; replicated owners always require durable proofs.
        if (try db.maintenanceRequiresOrderedApply()) return false;
        if (sequence == 0) return true;
        const runtime = db.enrichment_runtime orelse return false;
        const status = runtime.stats();
        if (status.applied_sequence < sequence or status.worker_failed) return false;
        const check = runtime.failure_pending_fn orelse return status.fatal_error_count == 0;
        var plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer plan.release();
        for (units) |unit| for (plan.plan().generated_templates) |template| {
            if (!db_mod.enrichment_types.appliesToDocument(template, unit)) continue;
            const failure = db_mod.enrichment_runtime_mod.FailureIdentity{ .kind = template.kind, .artifact_name = template.artifact_name, .source_artifact_name = template.upstream_artifact_name, .doc_key = unit, .sequence = sequence };
            if (try check(runtime.failure_ctx orelse return false, failure, template.index_name)) return false;
        };
        return true;
    }

    pub fn lookupGroupSource(self: *Documents, a: Allocator, raw: []const u8) !?db_mod.types.LookupResult {
        const db = self.dbPtr();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const owned = arena.allocator();
        const contract = @import("group_source_contract.zig");
        const query = try std.json.parseFromSliceLeaky(contract.Query, owned, raw, .{ .allocate = .alloc_always });
        try query.validate();
        var plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer plan.release();
        const empty_groups = @import("group_source_mutations.zig").Plan{ .arena = undefined, .entries = &.{} };
        const group_plan = plan.plan().group_sources orelse empty_groups;
        if (query.action == .definitions) {
            if (db.core.index_manager.minimum_reader_version.load(.acquire) < 8) return .{ .json = try a.dupe(u8, "[]") };
            var overlay = @import("group_source_mutations.zig").Overlay{ .completion_root = db.root_incarnation, .allocator = owned, .reader = @import("group_source_mutations.zig").Reader.fromStore(db.core.store) };
            const table = try @import("group_source_mutations.zig").tableIdentity(&overlay, db.core.identity_namespace.table_id, db.root_incarnation);
            var namespace: [24]u8 = undefined;
            db_mod.doc_identity.encodeNamespace(&namespace, db.core.identity_namespace);
            var source_namespace = namespace;
            if (std.mem.allEqual(u8, &source_namespace, 0)) std.mem.writeInt(u128, source_namespace[8..24], db.root_incarnation, .big);
            const definitions = try owned.alloc(@import("group_source_worker.zig").Definition, group_plan.entries.len + 1);
            definitions[group_plan.entries.len] = .{ .artifact = "__group_source_gc", .definition = .{ .table_incarnation = table, .configuration = @splat(1) }, .source = .{ .group_by = "_", .order_by = "_", .field = "_" }, .namespace = namespace, .source_owner = .{ .namespace = source_namespace, .incarnation = @import("group_source_owner_identity.zig").fromNamespace(db.core.identity_namespace, db.root_incarnation) }, .route = .{ .value = db.core.byteRange().start }, .retirement = true };
            for (group_plan.entries, definitions[0..group_plan.entries.len]) |entry, *definition| definition.* = .{ .artifact = entry.name, .definition = .{ .table_incarnation = table, .configuration = entry.configuration }, .source = entry.source, .namespace = namespace, .source_owner = .{ .namespace = source_namespace, .incarnation = @import("group_source_owner_identity.zig").fromNamespace(db.core.identity_namespace, db.root_incarnation) }, .route = .{ .value = db.core.byteRange().start }, .horizon = entry.horizon };
            return .{ .json = try std.json.Stringify.valueAlloc(a, definitions, .{}) };
        }
        var matched = false;
        for (group_plan.entries) |entry| if (std.mem.eql(u8, entry.name, query.artifact) and std.mem.eql(u8, &entry.configuration, &query.definition.configuration)) {
            matched = true;
            break;
        };
        if (!matched and query.action != .retirement and query.action != .retirement_page and query.action != .activation) return error.InvalidGroupedSourceQuery;
        var overlay = @import("group_source_mutations.zig").Overlay{ .completion_root = db.root_incarnation, .allocator = owned, .reader = @import("group_source_mutations.zig").Reader.fromStore(db.core.store) };
        if (query.definition.table_incarnation != try @import("group_source_mutations.zig").tableIdentity(&overlay, db.core.identity_namespace.table_id, db.root_incarnation)) return error.InvalidGroupedSourceQuery;
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const baseline = @import("group_source_baseline.zig");
        const reads = @import("group_source_reads.zig");
        const state = @import("group_source_state.zig");
        const group = state.Group{ .definition = query.definition, .key = query.group };
        var namespace: [24]u8 = undefined;
        db_mod.doc_identity.encodeNamespace(&namespace, db.core.identity_namespace);
        if (std.mem.allEqual(u8, &namespace, 0)) std.mem.writeInt(u128, namespace[8..24], db.root_incarnation, .big);
        const owner = state.Owner{ .namespace = namespace, .incarnation = @import("group_source_owner_identity.zig").fromNamespace(db.core.identity_namespace, db.root_incarnation) };
        var command_namespace: [24]u8 = undefined;
        db_mod.doc_identity.encodeNamespace(&command_namespace, db.core.identity_namespace);
        const output = switch (query.action) {
            .definitions => unreachable,
            .activation => blk: {
                if (!try db.maintenanceRequiresOrderedApply()) break :blk try a.dupe(u8, "true");
                const active = try @import("artifact_publication.zig").authority(&read);
                const catalogs = try @import("artifact_inventory.zig").catalogs(&read);
                const inventory = @import("artifact_inventory.zig");
                const status = try inventory.status(owned, &read, command_namespace);
                const ready = if (active) |value| status.ready and status.ordered != null and value.epoch == status.ordered.?.epoch and std.mem.eql(u8, &value.catalog_digest, &catalogs.digest()) else false;
                break :blk try std.json.Stringify.valueAlloc(a, ready, .{});
            },
            .retirement => try std.json.Stringify.valueAlloc(a, try @import("group_source_retirement.zig").load(owned, &read, baseline.retirementGeneration(baseline.sourceGeneration(db.core.index_manager.activeSchemaVersion() orelse 0, command_namespace, db.core.byteRange().start, db.core.byteRange().end, db.root_incarnation), try db.core.index_manager.artifact_semantic_cache.get(owned, try @import("artifact_inventory.zig").catalogs(&read)))), .{}),
            .retirement_page => try std.json.Stringify.valueAlloc(a, try @import("group_source_retirement.zig").preparePage(owned, &read, baseline.retirementGeneration(baseline.sourceGeneration(db.core.index_manager.activeSchemaVersion() orelse 0, command_namespace, db.core.byteRange().start, db.core.byteRange().end, db.root_incarnation), try db.core.index_manager.artifact_semantic_cache.get(owned, try @import("artifact_inventory.zig").catalogs(&read)))), .{}),
            .backfill_page => try std.json.Stringify.valueAlloc(a, try baseline.preparePage(owned, &read, query.definition, baseline.sourceGeneration(db.core.index_manager.activeSchemaVersion() orelse 0, command_namespace, db.core.byteRange().start, db.core.byteRange().end, db.root_incarnation)), .{}),
            .readiness => blk: {
                const completion = @import("artifact_completion_progress.zig");
                const statuses = try owned.alloc(completion.GroupReadiness, query.documents.len);
                for (query.documents, statuses) |document, *status| {
                    if (!db.core.byteRange().contains(document)) return error.KeyOutOfRange;
                    status.* = try completion.diagnoseGroup(owned, &read, db.root_incarnation, document, plan.plan());
                }
                const Replay = struct {
                    target: u64,
                    applied: u64,
                    worker_started: bool,
                    worker_failed: bool,
                    lease_owned: bool,
                    retrying: bool,
                    consecutive_retries: u32,
                    fatal_errors: u64,
                    last_error: []const u8,
                    asset_backend: bool,
                };
                const replay: ?Replay = if (db.enrichment_runtime) |runtime| status: {
                    const replay_stats = runtime.stats();
                    break :status .{ .target = replay_stats.target_sequence, .applied = replay_stats.applied_sequence, .worker_started = replay_stats.worker_started, .worker_failed = replay_stats.worker_failed, .lease_owned = replay_stats.lease_owned, .retrying = replay_stats.retrying, .consecutive_retries = replay_stats.consecutive_retry_count, .fatal_errors = replay_stats.fatal_error_count, .last_error = try owned.dupe(u8, replay_stats.last_error_code.slice()), .asset_backend = runtime.config.asset_producer != null };
                } else null;
                break :blk try std.json.Stringify.valueAlloc(a, .{ .units = statuses, .replay = replay }, .{});
            },
            .settled => blk: {
                const authority = try @import("artifact_publication.zig").authority(&read);
                var complete = query.documents.len == 0 or authority != null;
                if (authority) |active| {
                    const progress = try @import("artifact_producer_obligations.zig").load(&read);
                    if (progress) |value| try value.requireAuthority(active);
                    complete = progress != null and progress.?.baseline_complete;
                }
                if (authority == null and !try db.maintenanceRequiresOrderedApply()) complete = try self.groupSourceSettled(query.through_sequence, query.documents);
                for (query.documents) |document| {
                    if (!db.core.byteRange().contains(document)) return error.KeyOutOfRange;
                    if (authority != null and !try @import("artifact_completion_progress.zig").groupReady(owned, &read, db.root_incarnation, document)) {
                        complete = false;
                        break;
                    }
                }
                break :blk try std.json.Stringify.valueAlloc(a, complete, .{});
            },
            .backfill => try std.json.Stringify.valueAlloc(a, try baseline.load(owned, &read, query.definition, baseline.sourceGeneration(db.core.index_manager.activeSchemaVersion() orelse 0, command_namespace, db.core.byteRange().start, db.core.byteRange().end, db.root_incarnation)), .{}),
            .partition => blk: {
                try baseline.requireReady(owned, &read, query.definition, baseline.sourceGeneration(db.core.index_manager.activeSchemaVersion() orelse 0, command_namespace, db.core.byteRange().start, db.core.byteRange().end, db.root_incarnation));
                break :blk try std.json.Stringify.valueAlloc(a, try reads.partition(owned, &read, query, owner, .{ .start = db.core.byteRange().start, .end = db.core.byteRange().end }), .{});
            },
            .outbox => try std.json.Stringify.valueAlloc(a, try reads.directory(owned, &read, @import("group_source_queue.zig").source_prefix, query.cursor, query.limit, .{ .start = db.core.byteRange().start, .end = db.core.byteRange().end }), .{}),
            .pending => if (query.group.len == 0)
                try std.json.Stringify.valueAlloc(a, try reads.directory(owned, &read, @import("group_source_queue.zig").group_prefix, query.cursor, query.limit, .{ .start = db.core.byteRange().start, .end = db.core.byteRange().end }), .{})
            else
                try std.json.Stringify.valueAlloc(a, try state.nextPending(owned, &read, group), .{}),
            .status => try std.json.Stringify.valueAlloc(a, .{
                .owner = owner,
                .namespace = command_namespace,
                .event_status = if (query.event) |event| try @import("group_source_propagation.zig").status(owned, &read, query.source_owner.?, event) else null,
                .lease = if (query.group.len > 0) try state.currentLease(owned, &read, group) else null,
                .pending = if (query.segment.len > 0) try state.pendingFor(owned, &read, group, .{ .group = query.group, .segment = query.segment, .cell = query.cell }) else null,
                .snapshot = if (query.segment.len > 0) try state.readSnapshot(owned, &read, group, .{ .group = query.group, .segment = query.segment, .cell = query.cell }) else null,
            }, .{}),
            .prior => return error.InvalidGroupedSourceQuery,
        };
        return .{ .json = output };
    }

    pub fn waitForGroupSourcesThrough(self: *Documents, through: u64, wait: db_mod.derived_executor_mod.VisibilityWait) !u64 {
        const db = self.dbPtr();
        const io = db.backend_runtime.controlIo() orelse db.backend_runtime.io() orelse return error.BackendRuntimeIoUnavailable;
        var producer_sequence = through;
        while (true) {
            try wait.check();
            if (db.async_context.background_closing.load(.acquire)) return error.Canceled;
            const ready = blk: {
                var arena = std.heap.ArenaAllocator.init(db.runtime_alloc);
                defer arena.deinit();
                const a = arena.allocator();
                var pinned = try db.core.index_manager.acquireWritePlanSnapshot();
                defer pinned.release();
                const plan = pinned.plan().group_sources orelse break :blk true;
                var overlay = @import("group_source_mutations.zig").Overlay{ .completion_root = db.root_incarnation, .allocator = a, .reader = @import("group_source_mutations.zig").Reader.fromStore(db.core.store) };
                const table = try @import("group_source_mutations.zig").tableIdentity(&overlay, db.core.identity_namespace.table_id, db.root_incarnation);
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                const baseline = @import("group_source_baseline.zig");
                var namespace: [24]u8 = undefined;
                db_mod.doc_identity.encodeNamespace(&namespace, db.core.identity_namespace);
                const generation = baseline.sourceGeneration(db.core.index_manager.activeSchemaVersion() orelse 0, namespace, db.core.byteRange().start, db.core.byteRange().end, db.root_incarnation);
                var settled = true;
                for (plan.entries) |entry| {
                    const definition = @import("group_source_index.zig").Definition{ .table_incarnation = table, .configuration = entry.configuration };
                    if ((try baseline.load(a, &read, definition, generation)).phase != .ready) settled = false;
                    if (try @import("group_source_debt.zig").earliest(a, &read, definition, through)) |event| {
                        settled = false;
                        var source_namespace = namespace;
                        if (std.mem.allEqual(u8, &source_namespace, 0)) std.mem.writeInt(u128, source_namespace[8..24], db.root_incarnation, .big);
                        const owner = @import("group_source_state.zig").Owner{ .namespace = source_namespace, .incarnation = @import("group_source_owner_identity.zig").fromNamespace(db.core.identity_namespace, db.root_incarnation) };
                        producer_sequence = @max(producer_sequence, try @import("group_source_visibility.zig").producerSequence(a, &read, owner, event));
                    }
                }
                break :blk settled;
            };
            if (ready) return producer_sequence;
            _ = try self.runGroupSourceSlice();
            if (db.local_execution.optional_runtime_workers_enabled) {
                // Wake the owner without waiting for a global cursor that
                // may be blocked on an independent, later request. The
                // durable source debt above is the completion authority.
                if (db.enrichment_runtime) |runtime| runtime.notifySequence(producer_sequence);
            } else try db.runEnrichmentUntilWithVisibilityDeadline(producer_sequence, wait.cancellation, wait.deadline_ns.?);
            try wait.check();
            try io.sleep(.fromMilliseconds(5), .awake);
        }
    }
};
