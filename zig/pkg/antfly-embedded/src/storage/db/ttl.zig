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

/// TTL state owned by `DB`: the cleanup context and runtime that expire documents.
pub const Ttl = struct {
    owner: @import("local_runtime_owner.zig").Bundle(db_mod.TtlCleanupContext, db_mod.ttl_runtime_mod.TtlRuntime) = .{},
    cleanup_context: ?*db_mod.TtlCleanupContext = null,
    runtime: ?*db_mod.ttl_runtime_mod.TtlRuntime = null,

    fn dbPtr(self: *Ttl) *DB {
        return @alignCast(@fieldParentPtr("ttl", self));
    }

    fn constructRuntime(alloc: Allocator, ttl_ctx: *db_mod.TtlCleanupContext, db: *DB, cfg: db_mod.ttl_runtime_mod.Config) !db_mod.ttl_runtime_mod.TtlRuntime {
        return try db_mod.ttl_runtime_mod.TtlRuntime.init(
            alloc,
            db.core.batchExecutionResources().store,
            ttl_ctx,
            db_mod.deleteExpiredDocumentsFromCandidates,
            &db.async_context.text_merge_deferred,
            db.backend_runtime,
            cfg,
        );
    }

    pub fn initRuntime(self: *Ttl, cfg: db_mod.ttl_runtime_mod.Config) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("initRuntime", .{ db, cfg });
        const batch_resources = db.core.batchExecutionResources();
        var context_value: db_mod.TtlCleanupContext = .{
            .batch = .{
                .alloc = db.runtime_alloc,
                .io = db.backend_runtime.io(),
                .store = batch_resources.store,
                .applied_sequence_checkpoint_path = batch_resources.applied_sequence_checkpoint_path,
                .shard_manager = batch_resources.shard_manager,
                .change_journal = batch_resources.change_journal,
                .replay_source = batch_resources.replay_source,
                .index_manager = batch_resources.index_manager,
                .apply_mutex = batch_resources.apply_mutex,
                .portable_runtime_activation_pending = &db.async_context.portable_runtime_activation_pending,
                .snapshot_admission = batch_resources.snapshot_admission,
                .snapshot_replay_admission = batch_resources.snapshot_replay_admission,
                .log_mutex = batch_resources.log_mutex,
                .identity_namespace = batch_resources.identity_namespace,
                .executor = db.executor,
                .enrichment_runtime = db.enrichment.runtime,
                .resolution_runtime = db.enrichment.resolution.runtime,
                .promotion_runtime = db.enrichment.promotion.runtime,
                .relational_base_rows = db_mod.relationalColumns(db) != null,
                .table_catalog = &db.core.table_catalog,
                .replication_async_effect_mirror = db.local_execution.replication_async_effect_mirror,
                .replication_async_batch_mirror = db.local_execution.replication_async_batch_mirror,
                .replication_async_metadata_mirror = db.local_execution.replication_async_metadata_mirror,
                .replication_write_gate = db.local_execution.replication_write_gate,
            },
            .grace_period_ns = cfg.grace_period_ns,
            .clock = cfg.clock,
            .schema_registry = db.core.schema_registry,
        };
        context_value.batch.identity_visibility = &db.core.identity_visibility;
        self.owner = try @TypeOf(self.owner).create(db.runtime_alloc, context_value, .{ db, cfg }, constructRuntime);
        const ttl_ctx = self.owner.context.?;
        const runtime = self.owner.runtime.?;
        runtime.setGraphExpireFn(db_mod.expireGraphTtlCandidates);
        self.cleanup_context = ttl_ctx;
        self.runtime = runtime;
    }

    /// The context must outlive the managed DB. This only swaps the callback;
    /// teardown drains TTL workers before releasing the owning service.
    pub fn setCoordinated(self: *Ttl, port: ?db_mod.coordinated_ttl.Port) void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("setCoordinated", .{ db, port }) catch @panic("incompatible storage owner");
        const context = self.cleanup_context orelse return;
        const io = db.backend_runtime.io() orelse std.Options.debug_io;
        context.hook_mutex.lockUncancelable(io);
        defer context.hook_mutex.unlock(io);
        context.coordinated_port = port;
    }

    pub fn visibleResolvedDocSetNoLockAlloc(
        self: *Ttl,
        alloc: Allocator,
        set: *const db_mod.doc_set.ResolvedDocSet,
        generation: ?u64,
        duration_ns: u64,
    ) !db_mod.doc_set.ResolvedDocSet {
        const db = self.dbPtr();
        if (set.* == .none) return .none;
        if (duration_ns == 0) return try db_mod.doc_set.cloneAlloc(alloc, set);
        if (set.* == .all) return .all;

        const doc_ids = (try db.docIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, set, generation)) orelse
            return .none;
        defer db_mod.freeConstDocIds(alloc, doc_ids);

        const timestamps = try db_mod.loadDocumentTimestampsMany(db, alloc, doc_ids);
        defer alloc.free(timestamps);

        const expiry_now = db_mod.currentTimeNs();
        var kept = std.ArrayListUnmanaged([]const u8).empty;
        defer kept.deinit(alloc);
        for (doc_ids, timestamps) |doc_id, ts| {
            if (ts != 0 and db_mod.ttl_mod.isExpired(ts, duration_ns, expiry_now)) continue;
            try kept.append(alloc, doc_id);
        }
        return try db.resolveDocSetForIdsNoLockAtGenerationAlloc(alloc, kept.items, generation);
    }
};
