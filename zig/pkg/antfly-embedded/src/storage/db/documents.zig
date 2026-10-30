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

/// Document state owned by `DB.documents`.
pub const Documents = struct {
    authority: Authority = .{},

    /// Document authority backfill and repair: pages legacy documents into the authority index and attaches distributed authority snapshots.
    pub const Authority = struct {
        backfill_active: std.atomic.Value(bool) = .init(true),
        backfill_requested: std.atomic.Value(bool) = .init(false),
        backfill_mutex: std.atomic.Mutex = .unlocked,
        backfill_restart: bool = false,
        repairs: @import("../document_mutation_revision.zig").RepairQueue = .{},
        repairs_yield_sweep: bool = false,
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
};
