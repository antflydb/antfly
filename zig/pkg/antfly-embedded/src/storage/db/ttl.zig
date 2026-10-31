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

test "relational index mutations follow primary batches transactions deletes and reopen" {
    const alloc = std.testing.allocator;
    const native = @import("../relational_index.zig");
    for ([_]db_mod.PrimaryBackend{.{ .lsm = .{ .flush_threshold = 2 } }}) |backend| {
        var path_tmp = try db_mod.TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        defer db_mod.cleanupTempDir(path);
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]db_mod.schema_mod.RelationalColumn{
            .{ .name = "tenant", .path = "tenant", .column_type = .integer },
            .{ .name = "id", .path = "id", .column_type = .integer },
        };
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var view = db.core.acquireSchemaView().?;
        defer view.release();
        var definition = try db.core.relational_indexes.prepare(view, &.{.{
            .name = "tenant_id",
            .owner_kind = .table,
            .owner_name = native.relational_table_index_owner_name,
            .access_method = .ordered_tuple,
            .keys = &.{ .{ .column = "tenant" }, .{ .column = "id", .direction = .desc } },
        }});
        defer definition.deinit();
        {
            db.core.lockApply();
            defer db.core.unlockApply();
            _ = try db.core.relational_indexes.commit(&definition, &.{});
        }
        const first = "{\"tenant\":1,\"id\":9007199254740993}";
        const second = "{\"tenant\":2,\"id\":9007199254740994}";
        const third = "{\"tenant\":3,\"id\":7}";
        try db.batch(.{ .writes = &.{.{ .key = "row\x00:1", .value = first }} });
        try db_mod.expectTestRelationalIndexRow(&db, "row\x00:1", first, true);
        try db.batch(.{ .writes = &.{ .{ .key = "row\x00:1", .value = second }, .{ .key = "row\x00:1", .value = third } }, .deletes = &.{"row\x00:1"} });
        try db_mod.expectTestRelationalIndexRow(&db, "row\x00:1", first, false);
        try db_mod.expectTestRelationalIndexRow(&db, "row\x00:1", second, false);
        try db_mod.expectTestRelationalIndexRow(&db, "row\x00:1", third, false);
        try std.testing.expect((try db.get(alloc, "row\x00:1")) == null);
        try db.batch(.{ .writes = &.{ .{ .key = "row\x00:1", .value = second }, .{ .key = "row\x00:1", .value = third } } });
        try db_mod.expectTestRelationalIndexRow(&db, "row\x00:1", second, false);
        try db_mod.expectTestRelationalIndexRow(&db, "row\x00:1", third, true);
        const txn = try db.beginTransaction(100);
        try db.writeIntents(txn, &.{.{ .key = "row\x00:1", .value = second }}, &.{});
        try std.testing.expectError(error.SchemaInUse, db.setSchema(.{ .version = 2, .storage_mode = .relational, .relational_columns = &columns }));
        try db.commitTransaction(txn, 200);
        try db.commitTransaction(txn, 200); // Idempotent replay cannot leave old keys.
        try db_mod.expectTestRelationalIndexRow(&db, "row\x00:1", third, false);
        try db_mod.expectTestRelationalIndexRow(&db, "row\x00:1", second, true);
        {
            var archive = std.ArrayList(u8).empty;
            defer archive.deinit(alloc);
            try db_mod.portable_backup.exportPortable(alloc, db.core.store, &archive);
            for ([_]bool{ false, true }) |unpublished| {
                var restore_tmp = try db_mod.TestDirectory.init("db");
                defer restore_tmp.cleanup();
                const restore_path = restore_tmp.path().ptr;
                defer db_mod.cleanupTempDir(restore_path);
                var restored = try DB.open(alloc, std.mem.span(restore_path), .{ .start_optional_runtimes = false, .primary_backend = backend });
                defer restored.close();
                if (unpublished)
                    try restored.importPortableIntoUnpublishedEmpty(alloc, archive.items, db_mod.doc_identity.default_namespace)
                else
                    try restored.importPortableIntoEmpty(alloc, archive.items, db_mod.doc_identity.default_namespace);
                try db_mod.expectTestRelationalIndexRow(&restored, "row\x00:1", second, true);
                try db_mod.expectTestRelationalIndexRow(&restored, "row\x00:1", first, false);
                try db_mod.expectTestRelationalIndexRow(&restored, "row\x00:1", third, false);
            }
        }
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        try db_mod.expectTestRelationalIndexRow(&db, "row\x00:1", second, true);
        try db.batch(.{ .deletes = &.{"row\x00:1"} });
        try db_mod.expectTestRelationalIndexRow(&db, "row\x00:1", second, false);
        try db.batch(.{ .deletes = &.{"row\x00:1"} });
        try db.batch(.{ .writes = &.{ .{ .key = "a\x00", .value = first }, .{ .key = "z\x00", .value = third } } });
        {
            const page = try db.mergeDocumentsPage(alloc, db.getRange(), "a\x00");
            defer {
                for (page) |item| {
                    alloc.free(item.key);
                    alloc.free(item.value);
                }
                alloc.free(page);
            }
            try std.testing.expectEqual(@as(usize, 1), page.len);
            try std.testing.expectEqualStrings("z\x00", page[0].key);
            try std.testing.expectEqualStrings(third, page[0].value);
        }
        var split_tmp = try db_mod.TestDirectory.init("db");
        defer split_tmp.cleanup();
        const split_path = split_tmp.path().ptr;
        defer db_mod.cleanupTempDir(split_path);
        try db.split(db.getRange(), "m", "", std.mem.span(split_path), true);
        {
            var right = try DB.open(alloc, std.mem.span(split_path), .{ .start_optional_runtimes = false, .primary_backend = backend });
            defer right.close();
            try db_mod.expectTestRelationalIndexRow(&right, "z\x00", third, true);
            try db_mod.expectTestRelationalIndexRow(&right, "a\x00", first, false);
        }
        try db.finalizeSplit(.{ .start = "", .end = "m" });
        try db_mod.expectTestRelationalIndexRow(&db, "a\x00", first, true);
        try db_mod.expectTestRelationalIndexRow(&db, "z\x00", third, false);
        // Expiration uses its own stable runtime context. Cover both the
        // retired schema generation and newly written current-generation keys.
        const expired_at = db_mod.currentTimeNs() - 3 * std.time.ns_per_s;
        try db.batch(.{ .writes = &.{.{ .key = "a\x00", .value = first }}, .timestamp_ns = expired_at });
        try db.setSchema(.{ .version = 2, .storage_mode = .relational, .relational_columns = &columns, .ttl_duration_ns = std.time.ns_per_s });
        try db.batch(.{ .writes = &.{.{ .key = "b\x00", .value = second }}, .timestamp_ns = expired_at });
        try db_mod.initStoppedTtlRuntimeForTest(&db, .{ .enabled = false, .grace_period_ns = 0 });
        var expired_a = [_]u8{ 'a', 0 };
        var expired_b = [_]u8{ 'b', 0 };
        try std.testing.expectEqual(@as(u32, 2), try db_mod.deleteExpiredDocumentsFromCandidates(db.ttl.cleanup_context.?, &.{
            .{ .key = &expired_a, .timestamp_ns = expired_at },
            .{ .key = &expired_b, .timestamp_ns = expired_at },
        }));
        const remaining_index_keys = try db.core.store.scanPrefix(alloc, db_mod.relational_index_records.forward_namespace);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, remaining_index_keys);
        try std.testing.expectEqual(@as(usize, 0), remaining_index_keys.len);
        try std.testing.expectEqual(@as(u64, 0), db.core.table_catalog.row_count);
        const persisted_catalog = (try db_mod.table_catalog_mod.load(alloc, db.core.store)).?;
        try std.testing.expectEqual(@as(u64, 0), persisted_catalog.row_count);
        try db.batch(.{ .writes = &.{.{ .key = "b\x00", .value = second }} });
        try std.testing.expectEqual(@as(u64, 1), db.core.table_catalog.row_count);
        try db.batch(.{ .deletes = &.{"b\x00"} });
        try std.testing.expectEqual(@as(u64, 0), db.core.table_catalog.row_count);
    }
}

test "db relational ttl cleanup preserves physical mode across reopen" {
    const alloc = std.testing.allocator;
    const ttl_duration_ns: u64 = std.time.ns_per_s;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.setSchema(.{
            .version = 1,
            .storage_mode = .relational,
            .ttl_duration_ns = ttl_duration_ns,
            .relational_columns = &.{.{
                .name = "title",
                .path = "title",
                .column_type = .string,
                .required = true,
            }},
        });
        try db.batch(.{
            .writes = &.{.{ .key = "row:expired", .value = "{\"title\":\"expired\"}" }},
            .timestamp_ns = db_mod.currentTimeNs() - 2 * ttl_duration_ns,
        });
        // Read visibility must expire the packed row before background GC.
        try std.testing.expect((try db.get(alloc, "row:expired")) == null);
        var scan = try db.scan(alloc, "", "", .{ .include_documents = true });
        defer scan.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), scan.documents.len);
        const stored_key = try db_mod.internal_keys.relationalRowKeyAlloc(alloc, "row:expired");
        defer alloc.free(stored_key);
        const stored_row = try db.core.store.get(alloc, stored_key);
        defer alloc.free(stored_row);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer reopened.close();
    const ttl_cfg: db_mod.ttl_runtime_mod.Config = .{
        .enabled = true,
        .interval_ms = 10,
        .batch_size = 8,
        .grace_period_ns = 0,
    };
    try db_mod.initStoppedTtlRuntimeForTest(&reopened, ttl_cfg);
    try std.testing.expect(reopened.ttl.cleanup_context.?.batch.relational_base_rows);
    try reopened.ttl.runtime.?.runOnce();

    try std.testing.expect((try reopened.get(alloc, "row:expired")) == null);
    const row_key = try db_mod.internal_keys.relationalRowKeyAlloc(alloc, "row:expired");
    defer alloc.free(row_key);
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, row_key));
    const stats = try reopened.diagnosticStats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.live_ordinals);
}

test "db graph ttl document and relational cleanup retire all graph deadlines" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |relational| {
        var tmp = try db_mod.TestDirectory.init("db-graph-ttl-document-gc");
        defer tmp.cleanup();
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.setSchema(.{
            .version = 1,
            .storage_mode = if (relational) .relational else .document,
            .ttl_duration_ns = std.time.ns_per_s,
            .relational_columns = if (relational) &.{.{ .name = "relations", .path = "relations", .column_type = .json, .is_json = true, .json_kind = .any, .allows_null = true }} else &.{},
        });
        const key = try db_mod.GraphPrimaryPublicationTest.seed(&db, true);
        defer alloc.free(key);
        _ = try db_mod.applyDerivedBatchToIndexAsync(db.async_context, .{ .changed_artifact_keys = &.{key} }, .{ .name = "g", .kind = .graph }, .{});
        try db.addIndex(.{ .name = "direct", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" });
        try db.batch(.{ .graph_writes = &.{.{ .index_name = "direct", .source = "doc:a", .target = "doc:c", .edge_type = "links" }}, .sync_level = .full_index });
        const before = try db.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, before);
        try std.testing.expectEqual(@as(usize, 2), before.len);
        const timestamp = (try db_mod.ttl_mod.readTimestamp(db.core.store, alloc, "doc:a")).?;
        var clock = db_mod.platform_clock.ManualClock{};
        clock.setRealtimeNs(timestamp + 2 * std.time.ns_per_s);
        try db_mod.initStoppedTtlRuntimeForTest(&db, .{ .enabled = true, .grace_period_ns = 0, .clock = clock.clock() });
        try db.ttl.runtime.?.runOnce();
        try std.testing.expectEqual(@as(u64, 1), db.ttl.runtime.?.stats().deleted_docs);
        const after = try db.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, after);
        try std.testing.expectEqual(@as(usize, 0), after.len);
        try std.testing.expect((try db.get(alloc, "doc:a")) == null);
        db.close();
        db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
        const reopened = try db.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, reopened);
        try std.testing.expectEqual(@as(usize, 0), reopened.len);
    }
}

test "db graph ttl shared worker conditionally expires direct edge artifact" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db-graph-direct-ttl-worker");
    defer path_tmp.cleanup();
    var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.addIndex(.{ .name = "direct_graph", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" });
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"title":"keep","_edges":{"direct_graph":{"links":[{"target":"doc:b","weight":2}]}}}
    }}, .sync_level = .full_index });
    const due_rows = try db.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, due_rows);
    try std.testing.expectEqual(@as(usize, 1), due_rows.len);
    const due = (try db_mod.graph_edge_ttl_expiration.decodeDue(due_rows[0].value)).direct;
    try std.testing.expectEqual(due.deadline_ns, try db_mod.graph_edge_ttl_expiration.deadlineFromKey(due_rows[0].key));
    var scanned_rows: usize = 0;
    var expired_read = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = .{
            .query_type = .neighbors,
            .index_name = "direct_graph",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .direction = .out, .edge_types = &.{"links"} },
        } }},
        .graph_ttl_now_ns = due.deadline_ns + 1,
        .graph_physical_scan_observation = &scanned_rows,
    });
    defer expired_read.deinit();
    try std.testing.expectEqual(@as(usize, 0), expired_read.graph_results[0].nodes.len);
    // The canonical row and its private direct contributor both count.
    try std.testing.expectEqual(@as(usize, 2), scanned_rows);
    var manual_clock = db_mod.platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(due.deadline_ns + 1);
    try db_mod.initStoppedTtlRuntimeForTest(&db, .{ .enabled = true, .grace_period_ns = 0, .clock = manual_clock.clock() });
    try db.ttl.runtime.?.runOnce();
    try std.testing.expectEqual(@as(u64, 1), db.ttl.runtime.?.stats().expired_graph_artifacts);
    const due_after = try db.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, due_after);
    try std.testing.expectEqual(@as(usize, 0), due_after.len);
    try db.core.store.put(due_rows[0].key, due_rows[0].value);
    try db.ttl.runtime.?.runOnce();
    const pruned = try db.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, pruned);
    try std.testing.expectEqual(@as(usize, 0), pruned.len);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, due.artifact_key));
    const edges = try db.getEdges(alloc, "direct_graph", "doc:a", "links", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);
    const doc = (try db.get(alloc, "doc:a")) orelse return error.TestExpectedDocument;
    defer alloc.free(doc);
}

test "db graph ttl migration cold writer fences legacy expiry without changing readonly files" {
    const alloc = std.testing.allocator;
    const resolver_catalog = @import("catalog/resolver_catalog.zig");
    const migration = @import("maintenance/graph_ttl_migration.zig");
    var path_tmp = try db_mod.TestDirectory.init("db-graph-ttl-legacy-namespace");
    defer path_tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const resolution = try db_mod.internal_keys.resolutionArtifactKeyAlloc(scratch, "doc:a", "resolver");
    var promotion: std.ArrayList(u8) = .empty;
    try promotion.appendSlice(scratch, &db_mod.internal_keys.promoted_keys_state_prefix);
    try db_mod.internal_keys.appendEncodedComponent(&promotion, scratch, resolution);
    var deadline: u64 = 0;
    {
        var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" });
        try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"_edges\":{\"g\":{\"links\":[{\"target\":\"doc:b\"}]}}}" }}, .sync_level = .full_index });
        const rows = try db.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, rows);
        try std.testing.expectEqual(@as(usize, 1), rows.len);
        deadline = try db_mod.graph_edge_ttl_expiration.deadlineFromKey(rows[0].key);
        const legacy = try scratch.dupe(u8, rows[0].key);
        @memcpy(legacy[0..db_mod.internal_keys.legacy_graph_edge_expiration_index_prefix.len], &db_mod.internal_keys.legacy_graph_edge_expiration_index_prefix);
        // Reconstruct the old writer's durable representation, including the
        // mixed family and pre-migration reader fence. No worker is running.
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(legacy, rows[0].value);
        try txn.delete(rows[0].key);
        try txn.put(promotion.items, "promotion state");
        try txn.put(resolver_catalog.minimum_reader_key, "3");
        try txn.delete("\x00\x00__metadata__:resolvers");
        try txn.delete(migration.progress_key);
        try txn.commit();
    }
    {
        var db = try DB.open(alloc, path_tmp.path(), .{ .open_mode = .query_readonly, .start_optional_runtimes = false });
        defer db.close();
        const floor = try db.core.store.get(alloc, resolver_catalog.minimum_reader_key);
        defer alloc.free(floor);
        try std.testing.expectEqualStrings("3", floor);
        const rows = try db.core.store.scanPrefix(alloc, &db_mod.internal_keys.legacy_graph_edge_expiration_index_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, rows);
        try std.testing.expectEqual(@as(usize, 2), rows.len);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, migration.progress_key));
    }
    {
        var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectError(error.UnsupportedDatabaseReaderVersion, resolver_catalog.readMinimumReaderVersion(&read, 3));
        }
        var clock = db_mod.platform_clock.ManualClock{};
        clock.setRealtimeNs(deadline + 1);
        try db_mod.initStoppedTtlRuntimeForTest(&db, .{ .enabled = true, .grace_period_ns = 0, .clock = clock.clock() });
        try db.ttl.runtime.?.runOnce();
        try std.testing.expectEqual(@as(u64, 1), db.ttl.runtime.?.stats().expired_graph_artifacts);
        try std.testing.expectEqual(@as(u64, 0), db.ttl.runtime.?.stats().error_count);
        const preserved = try db.core.store.get(alloc, promotion.items);
        defer alloc.free(preserved);
        try std.testing.expectEqualStrings("promotion state", preserved);
        const edges = try db.getEdges(alloc, "g", "doc:a", "links", .out);
        defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 0), edges.len);
    }
}

test "db ttl cleanup reclaims expired documents through normal delete semantics" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const ttl_cfg: db_mod.ttl_runtime_mod.Config = .{
        .enabled = true,
        .interval_ms = 10,
        .batch_size = 8,
        .grace_period_ns = 0,
    };
    var backend_runtime = try db_mod.background_runtime_mod.BackendRuntimeHandle.init(alloc, .{});
    defer backend_runtime.deinit();
    const opened = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtimes = false,
        .ttl_cleanup = ttl_cfg,
        .executor = .{ .backend = .manual },
        .backend_runtime = backend_runtime.ptr(),
    });
    // Managed serving adopts the returned wrapper into a stable cache entry.
    var db = opened;
    defer db.close();
    try db_mod.initStoppedTtlRuntimeForTest(&db, ttl_cfg);
    const NoopVisibilityHook = struct {
        fn onChange(
            _: *anyopaque,
            _: db_mod.QueryVisibilityEvent,
        ) void {}
    };
    db.setQueryVisibilityHook(.{
        .ptr = &db,
        .on_change = NoopVisibilityHook.onChange,
    });
    defer db.setQueryVisibilityHook(null);

    try db.setSchema(.{
        .version = 1,
        .default_type = "_default",
        .ttl_duration_ns = 1_000_000_000,
    });
    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    const now_ns = db_mod.currentTimeNs();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:expired", .value = "{\"title\":\"gone\",\"body\":\"common token\"}" }},
        .timestamp_ns = now_ns - 2_000_000_000,
    });

    try db.ttl.runtime.?.runOnce();
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(u64, 0), try db.getTimestamp(alloc, "doc:expired"));
    {
        const stats = try db.runtimeStatusStatsConsistent(alloc);
        defer db_mod.types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 0), stats.source_doc_count);
        try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.live_ordinals);
    }
    {
        const stats = try db.diagnosticStats(alloc);
        defer db_mod.types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.state_rows);
        try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.live_ordinals);
        try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.tombstone_ordinals);
        try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.primary_docs_missing_ordinals);
        try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.primary_docs_missing_identity_state);
        try std.testing.expect(!stats.doc_identity.rebuild_required);
    }

    try db.setSchema(.{
        .version = 2,
        .default_type = "_default",
        .ttl_duration_ns = 0,
    });

    var text = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "common" } },
        .limit = 10,
    });
    defer text.deinit();
    try std.testing.expectEqual(@as(u32, 0), text.total_hits);
}

test "db stats expose ttl cleanup activity" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const ttl_cfg: db_mod.ttl_runtime_mod.Config = .{
        .enabled = true,
        .interval_ms = 10,
        .batch_size = 8,
        .grace_period_ns = 0,
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtimes = false,
        .ttl_cleanup = ttl_cfg,
    });
    defer db.close();
    try db_mod.initStoppedTtlRuntimeForTest(&db, ttl_cfg);

    try db.setSchema(.{
        .version = 1,
        .default_type = "_default",
        .ttl_duration_ns = 1_000_000_000,
    });

    const now_ns = db_mod.currentTimeNs();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:expired", .value = "{\"title\":\"gone\"}" }},
        .timestamp_ns = now_ns - 2_000_000_000,
    });

    try db.ttl.runtime.?.runOnce();
    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.ttl_cleanup.enabled);
    try std.testing.expect(stats.ttl_cleanup.runs > 0);
    try std.testing.expect(stats.ttl_cleanup.scanned_timestamps > 0);
    try std.testing.expect(stats.ttl_cleanup.deleted_docs > 0);
    try std.testing.expect(stats.ttl_cleanup.last_run_ns > 0);
}

test "db ttl cleanup can run under lease ownership" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const ttl_cfg: db_mod.ttl_runtime_mod.Config = .{
        .enabled = true,
        .lease_owned = true,
        .owner_id = "ttl-owner-a",
        .lease_ttl_ms = 250,
        .interval_ms = 10,
        .batch_size = 8,
        .grace_period_ns = 0,
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtimes = false,
        .ttl_cleanup = ttl_cfg,
    });
    defer db.close();
    try db_mod.initStoppedTtlRuntimeForTest(&db, ttl_cfg);

    try db.setSchema(.{
        .version = 1,
        .default_type = "_default",
        .ttl_duration_ns = 1_000_000_000,
    });

    const now_ns = db_mod.currentTimeNs();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:expired", .value = "{\"title\":\"gone\"}" }},
        .timestamp_ns = now_ns - 2_000_000_000,
    });

    try db.ttl.runtime.?.runOnce();
    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);

    try std.testing.expect(stats.ttl_cleanup.enabled);
    try std.testing.expect(stats.ttl_cleanup.lease_owned);
    try std.testing.expect(stats.ttl_cleanup.has_lease);
    try std.testing.expect(stats.ttl_cleanup.deleted_docs > 0);
}

test "db ttl cleanup can run under lease ownership with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const primary_backend: db_mod.PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    const ttl_cfg: db_mod.ttl_runtime_mod.Config = .{
        .enabled = true,
        .lease_owned = true,
        .owner_id = "ttl-owner-a",
        .lease_ttl_ms = 250,
        .interval_ms = 10,
        .batch_size = 8,
        .grace_period_ns = 0,
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = primary_backend,
        .start_optional_runtimes = false,
        .ttl_cleanup = ttl_cfg,
    });
    defer db.close();
    try db_mod.initStoppedTtlRuntimeForTest(&db, ttl_cfg);

    try db.setSchema(.{
        .version = 1,
        .default_type = "_default",
        .ttl_duration_ns = 1_000_000_000,
    });

    const now_ns = db_mod.currentTimeNs();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:expired", .value = "{\"title\":\"gone\"}" }},
        .timestamp_ns = now_ns - 2_000_000_000,
    });

    try db.ttl.runtime.?.runOnce();
    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);

    try std.testing.expect(stats.ttl_cleanup.enabled);
    try std.testing.expect(stats.ttl_cleanup.lease_owned);
    try std.testing.expect(stats.ttl_cleanup.has_lease);
    try std.testing.expect(stats.ttl_cleanup.deleted_docs > 0);
}

test "db ttl cleanup can run with manual clock" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var clock = db_mod.platform_clock.ManualClock{
        .now_realtime_ns = 10 * std.time.ns_per_s,
    };

    const ttl_cfg: db_mod.ttl_runtime_mod.Config = .{
        .enabled = true,
        .interval_ms = 10,
        .batch_size = 8,
        .grace_period_ns = 0,
        .clock = clock.clock(),
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtimes = false,
        .ttl_cleanup = ttl_cfg,
    });
    defer db.close();
    try db_mod.initStoppedTtlRuntimeForTest(&db, ttl_cfg);

    try db.setSchema(.{
        .version = 1,
        .default_type = "_default",
        .ttl_duration_ns = std.time.ns_per_s,
    });

    const now_ns = clock.clock().nowRealtimeNs();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:expired_manual", .value = "{\"title\":\"gone\"}" }},
        .timestamp_ns = now_ns - 2 * std.time.ns_per_s,
    });

    try db.ttl.runtime.?.runOnce();
    try std.testing.expectEqual(@as(u64, 0), try db.getTimestamp(alloc, "doc:expired_manual"));

    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.ttl_cleanup.runs > 0);
    try std.testing.expect(stats.ttl_cleanup.deleted_docs > 0);
}

test "db ttl cleanup background worker starts and deletes with manual clock" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var clock = db_mod.platform_clock.ManualClock{
        .now_realtime_ns = 10 * std.time.ns_per_s,
    };

    const ttl_cfg: db_mod.ttl_runtime_mod.Config = .{
        .enabled = true,
        .interval_ms = 1_000,
        .batch_size = 8,
        .grace_period_ns = 0,
        .clock = clock.clock(),
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .ttl_cleanup = ttl_cfg,
    });
    defer db.close();
    try std.testing.expect(db.ttl.runtime != null);

    try db.setSchema(.{
        .version = 1,
        .default_type = "_default",
        .ttl_duration_ns = std.time.ns_per_s,
    });

    const now_ns = clock.clock().nowRealtimeNs();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:expired_worker", .value = "{\"title\":\"gone\"}" }},
        .timestamp_ns = now_ns - 2 * std.time.ns_per_s,
    });

    try db_mod.waitForRawDelete(alloc, &db, "doc:expired_worker", 200);

    var stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    var attempts: usize = 0;
    while (stats.ttl_cleanup.deleted_docs == 0 and attempts < 200) : (attempts += 1) {
        db_mod.sleepPollInterval();
        db_mod.types.freeDBStats(alloc, stats);
        stats = try db.stats(alloc);
    }

    try std.testing.expect(stats.ttl_cleanup.enabled);
    try std.testing.expect(stats.ttl_cleanup.runs > 0);
    try std.testing.expect(stats.ttl_cleanup.deleted_docs > 0);
}
