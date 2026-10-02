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

//! Server command, transaction recovery and ordered replay integration tests.
const engine = @import("db/db.zig");
const server_test_adapter = @import("server_db_adapter.zig");
const server_recovery = @import("server_transaction_recovery.zig");
const transaction_runtime_mod = server_recovery;
const Allocator = engine.test_support.Allocator;
const CountingDenseEmbedder = engine.test_support.CountingDenseEmbedder;
const DB = engine.test_support.DB;
const Io = engine.test_support.Io;
const OpenOptions = engine.test_support.OpenOptions;
const OrderedApplyReceipt = engine.test_support.OrderedApplyReceipt;
const TestDirectory = engine.test_support.TestDirectory;
const TestTransactionRecoveryResolver = struct {
    fn resolve(_: *anyopaque, _: transactions_mod.TxnId, _: []const u8, _: transactions_mod.TxnStatus, _: u64) !void {}
};
const TtlCleanupContext = engine.test_support.TtlCleanupContext;
const TxnResolverRecorder = struct {
    mutex: std.atomic.Mutex = .unlocked,
    calls: u32 = 0,

    fn resolve(ctx_ptr: *anyopaque, txn_id: transactions_mod.TxnId, participant: []const u8, status: transactions_mod.TxnStatus, commit_version: u64) anyerror!void {
        _ = txn_id;
        _ = status;
        _ = commit_version;
        if (!std.mem.eql(u8, participant, "remote")) return error.UnexpectedParticipant;
        const self: *TxnResolverRecorder = @ptrCast(@alignCast(ctx_ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.calls += 1;
    }
};
const activeSplitShadow = engine.test_support.activeSplitShadow;
const apply_state = engine.test_support.apply_state;
const background_runtime_mod = engine.test_support.background_runtime_mod;
const cleanupTempDir = engine.test_support.cleanupTempDir;
const doc_identity = engine.test_support.doc_identity;
const docstore_mod = engine.test_support.docstore_mod;
const documentRangeLowerAlloc = engine.test_support.documentRangeLowerAlloc;
const embedder_mod = engine.test_support.embedder_mod;
const encodeStoreLookupKeyAlloc = engine.test_support.encodeStoreLookupKeyAlloc;
const enrichment_artifact_codec = engine.test_support.enrichment_artifact_codec;
const expectMergeArtifactSearches = engine.test_support.expectMergeArtifactSearches;
const expireGraphTtlCandidateContext = engine.test_support.expireGraphTtlCandidateContext;
const graph_edge_ttl_expiration = engine.test_support.graph_edge_ttl_expiration;
const graph_mod = engine.test_support.graph_mod;
const index_repair_state = engine.test_support.index_repair_state;
const internal_keys = engine.test_support.internal_keys;
const loadDerivedCoverageCounters = engine.test_support.loadDerivedCoverageCounters;
const lockAtomic = engine.test_support.lockAtomic;
const mapper = engine.test_support.mapper;
const merge_state_mod = engine.test_support.merge_state_mod;
const orderedApplyReceiptWrite = engine.test_support.orderedApplyReceiptWrite;
const ordered_apply_receipt_value_len = engine.test_support.ordered_apply_receipt_value_len;
const platform_clock = engine.test_support.platform_clock;
const portable_backup = engine.test_support.portable_backup;
const range_cardinality = engine.test_support.range_cardinality;
const relational_columns = engine.test_support.relational_columns;
const relational_store = engine.test_support.relational_store;
const replication_effects_mod = engine.test_support.replication_effects_mod;
const replication_record_mod = engine.test_support.replication_record_mod;
const resource_manager_mod = engine.test_support.resource_manager_mod;
const schema_mod = engine.test_support.schema_mod;
const shard_mod = engine.test_support.shard_mod;
const sleepPollInterval = engine.test_support.sleepPollInterval;
const std = engine.test_support.std;
const table_catalog_mod = engine.test_support.table_catalog_mod;
const transactions_mod = engine.test_support.transactions_mod;
const tryFinalizePrimarySplitFast = engine.test_support.tryFinalizePrimarySplitFast;
const types = engine.test_support.types;

test "graph ownership cleanup runs on borrowed VoprIo before replicated merge" {
    const vopr = @import("vopr");
    const alloc = std.testing.allocator;
    var runtime_io = try vopr.vopr_io.VoprIo.init(.{
        .seed = 704,
        .file_allocator = alloc,
        // Full DB graph apply crosses the LSM and debug allocator on this
        // fiber. Match the production-shaped DB/DataServer VOPR campaigns,
        // rather than the generic scheduler's 1 MiB task stack.
        .tasks = .{ .stack_size = 8 * 1024 * 1024 },
    });
    defer runtime_io.deinit();
    runtime_io.monotonic_ns = 200 * std.time.ns_per_day;
    var backend = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{
        .backend = .manual,
        .borrowed_io = .{ .general = runtime_io.io() },
    });
    var owners_closed = false;
    defer if (!owners_closed) backend.deinit();
    var db = try DB.open(alloc, "/graph-maintenance-vopr", .{
        .backend_runtime = backend.ptr(),
        .executor = .{ .backend = .manual },
        .primary_backend = .{ .mem = .{} },
        .physical_root_mode = .external_backend,
        .index_backends = .{ .graph_reverse_backend = .lsm, .graph_lsm_storage = backend.ptr().storage() },
        .start_optional_runtimes = false,
    });
    defer if (!owners_closed) db.close();
    const Run = struct {
        fn run(database: *DB, owner: *background_runtime_mod.BackendRuntimeHandle, io: std.Io, closed: *bool) !void {
            defer {
                database.close();
                owner.deinit();
                closed.* = true;
            }
            try database.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
            try engine.test_support.saveAllLiveIndexStatusSnapshots(engine.test_support.dbPointer(&database), database.alloc);
            const initial_status = (try engine.test_support.loadIndexStatusSnapshot(engine.test_support.dbPointer(&database), database.alloc, "g")) orelse return error.GraphStatusSnapshotMissing;
            try std.testing.expectEqual(@as(u64, 200 * std.time.ns_per_day), initial_status.updated_at_ns);
            try database.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "z", .target = "a", .edge_type = "link", .weight = 1 }}, .sync_level = .full_index });
            try server_test_adapter.applyOrdered(&database, .{ .split_transition = .{ .kind = .finalize, .transition_id = 1, .attempt_epoch = 1, .destination_group_id = 2, .split_key = "m" } }, .{ .term = 1, .index = 1 });
            const merge = types.BatchRequest{ .merge_checkpoint = .{
                .kind = .accept,
                .transition_id = 10,
                .donor_group_id = 2,
                .receiver_group_id = 1,
                .receiver_base_start = "",
                .receiver_base_end = "m",
                .merged_start = "",
                .merged_end = "",
            } };
            try std.testing.expectError(error.RaftApplyWriterUnavailable, server_test_adapter.applyOrdered(&database, merge, .{ .term = 1, .index = 2 }));
            try std.testing.expectEqual(@as(u64, 1), (try database.orderedApplyReceipt()).?.index);
            database.startResidentBackgroundWorkersIfNeeded();
            if (database.artifact_repair_metadata_future == null) return error.GraphMaintenanceWorkerMissing;
            const graph = &database.core.index_manager.graphIndex("g").?.index;
            for (0..100) |_| {
                if (!graph.ownershipTransitionPending()) break;
                try io.sleep(.fromMilliseconds(100), .awake);
            }
            if (graph.ownershipTransitionPending()) return error.GraphOwnershipCleanupTimedOut;
            try server_test_adapter.applyOrdered(&database, merge, .{ .term = 1, .index = 2 });
            try std.testing.expectEqual(@as(u64, 2), (try database.orderedApplyReceipt()).?.index);
            try std.testing.expectEqualStrings("", database.getRange().end);
            const retired = try database.getEdges(database.alloc, "g", "a", "link", .in);
            defer graph_mod.GraphIndex.freeEdges(database.alloc, retired);
            try std.testing.expectEqual(@as(usize, 0), retired.len);
        }
    };
    var future = runtime_io.io().async(Run.run, .{ &db, &backend, runtime_io.io(), &owners_closed });
    const scheduler = runtime_io.scheduler();
    var enabled: vopr.transition.List = .{};
    defer enabled.deinit(alloc);
    var events: vopr.event.Sink = .{};
    defer events.deinit(alloc);
    for (0..10_000) |_| {
        if (scheduler.quiescent()) break;
        enabled.items.clearRetainingCapacity();
        try scheduler.enumerateReady(&enabled, alloc);
        try enabled.canonicalize();
        try std.testing.expect(enabled.items.items.len != 0);
        // This is a bounded liveness check: run ready work before advancing
        // time. Canonical ID order alone may repeatedly pick the observer's
        // timer while starving the maintenance worker it is waiting for.
        var selected = enabled.items.items[0];
        for (enabled.items.items) |candidate| {
            if (!std.mem.eql(u8, candidate.name, "vopr-io.time_advance")) {
                selected = candidate;
                break;
            }
        }
        try scheduler.executeReady(selected.id, &events, alloc);
    }
    try std.testing.expect(scheduler.quiescent());
    try future.await(runtime_io.io());
    try std.testing.expect(owners_closed);
    try runtime_io.ensureNoCapabilityViolation();
}

test "db transaction recovery enabled requires backend runtime io" {
    const alloc = std.testing.allocator;

    var runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{
        .backend = .manual,
        .filesystem_io = std.testing.io,
    });
    defer runtime.deinit();

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer cleanupTempDir(path);

    var resolver_ctx: u8 = 0;
    const server_recovery_config_1: server_recovery.Config = .{
        .enabled = true,
        .resolver_ctx = &resolver_ctx,
        .resolve_participant_fn = TestTransactionRecoveryResolver.resolve,
    };
    try std.testing.expectError(error.MissingBackendRuntimeIo, DB.open(alloc, std.mem.span(path), .{
        .backend_runtime = runtime.ptr(),
        .executor = .{ .backend = .manual },
        .transaction_recovery = server_recovery.borrowedConfig(&server_recovery_config_1),
    }));
}

test "relational replicated admission is identical across local memory envelopes" {
    const alloc = std.testing.allocator;
    for ([_]u64{ 256 * 1024, 4 * 1024 * 1024 }) |capacity| {
        var options = resource_manager_mod.Options{ .identity_allocator = alloc };
        options.budgets[@intFromEnum(resource_manager_mod.Slice.relational_preparation_working_set)] = .{ .hard_limit_bytes = capacity };
        var resources = resource_manager_mod.ResourceManager.init(options);
        defer resources.deinit(alloc);
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        defer cleanupTempDir(path);
        var db = try DB.open(alloc, std.mem.span(path), .{ .resource_manager = &resources, .start_optional_runtimes = false });
        defer db.close();
        const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        db.core.table_catalog.transaction_admission_bytes = 12 * 1024;
        const catalog = db.core.table_catalog.encode();
        try db.core.store.putBatch(&.{.{ .key = table_catalog_mod.key, .value = &catalog }}, &.{});
        const txn = try db.beginTransaction(100);
        try db.writeReplicatedTransactionAtRaftEntry(txn, .{ .writes = &.{.{ .key = "a", .value = "{\"n\":1}" }} }, .{ .term = 1, .index = 1 });
        try std.testing.expectError(error.TransactionTooLarge, db.writeReplicatedTransactionAtRaftEntry(txn, .{ .writes = &.{.{ .key = "b", .value = "{\"n\":2}" }} }, .{ .term = 1, .index = 2 }));
        try std.testing.expectEqual(@as(u64, 1), (try db.orderedApplyReceipt()).?.index);
        var intents = try db.core.collectTransactionIntentBatch(alloc, txn);
        defer intents.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), intents.writes.len);
        // The logical row is stored once, as AROW. Only API-only special
        // fields remain in the sidecar; ordinary JSON is not duplicated.
        try std.testing.expectEqualStrings("{}", intents.writes[0].value);
        try std.testing.expect(intents.prepared_rows[0] != null);
        try db.commitTransaction(txn, 200);
        const row = (try db.get(alloc, "a")).?;
        defer alloc.free(row);
        try std.testing.expectEqualStrings("{\"n\":1}", row);
        try std.testing.expectEqual(@as(u64, 0), resources.sliceStats(.relational_preparation_working_set).used_bytes);
    }
}

test "db relational one-shot recovery resolves orphaned intents into packed rows" {
    const alloc = std.testing.allocator;
    var resources = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = alloc });
    defer resources.deinit(alloc);

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .resource_manager = &resources, .start_optional_runtimes = false });
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"amount":{"type":"numeric"}},"required":["title"],"additionalProperties":false}}}}
    ;
    try db.setSchemaJson(alloc, schema_json);

    const commit_ts: u64 = 2_000;
    const txn_id = try db.beginTransaction(1_000);
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "row:one_shot_recovered", .value = "{\"title\":\"one shot\",\"amount\":21.5}" }},
    });
    // A metadata refresh before recovery must not reinterpret the durable
    // prepare vote under a different physical type or public validator.
    try db.setSchemaJson(alloc,
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"amount":{"type":"string"}},"required":["title"],"additionalProperties":false}}}}
    );

    const record_key = blk: {
        const prefix = "\x00\x00__txn_records__:";
        var key: [prefix.len + @sizeOf(transactions_mod.TxnId)]u8 = undefined;
        @memcpy(key[0..prefix.len], prefix);
        @memcpy(key[prefix.len..], &txn_id);
        break :blk key;
    };
    var record_value: [33]u8 = undefined;
    record_value[0] = @intFromEnum(transactions_mod.TxnStatus.committed);
    std.mem.writeInt(u64, record_value[1..9], 1_000, .little);
    std.mem.writeInt(u64, record_value[9..17], commit_ts, .little);
    std.mem.writeInt(u64, record_value[17..25], 1_000, .little);
    std.mem.writeInt(u64, record_value[25..33], commit_ts, .little);
    try db.core.store.put(&record_key, &record_value);

    var recorder = TxnResolverRecorder{};
    const capacity = resources.sliceStats(.relational_preparation_working_set).hard_limit_bytes;
    var competing = try resources.reserve(.relational_preparation_working_set, capacity);
    defer competing.release();
    try std.testing.expectError(error.ResourceBudgetExceeded, server_recovery.runDbRecoveryOnce(&db, .{
        .enabled = true,
        .cutoff_ns = 1,
        .resolver_ctx = &recorder,
        .resolve_participant_fn = TxnResolverRecorder.resolve,
    }));
    try std.testing.expect(try db.core.transactionHasIntents(txn_id));
    competing.release();
    const recovery = try server_recovery.runDbRecoveryOnce(&db, .{
        .enabled = true,
        .cutoff_ns = 1,
        .resolver_ctx = &recorder,
        .resolve_participant_fn = TxnResolverRecorder.resolve,
    });
    try std.testing.expect(recovery.resolved_finalized >= 1);
    try std.testing.expectEqual(@as(u64, 0), resources.sliceStats(.relational_preparation_working_set).used_bytes);

    const raw = (try db.get(alloc, "row:one_shot_recovered")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"one shot\",\"amount\":21.5}", raw);
    try std.testing.expectEqual(commit_ts, try db.getTimestamp(alloc, "row:one_shot_recovered"));

    const relational_key = try relational_store.keyAlloc(alloc, "row:one_shot_recovered");
    defer alloc.free(relational_key);
    const raw_row = try db.core.store.get(alloc, relational_key);
    defer alloc.free(raw_row);
    try std.testing.expect(mapper.isRelationalRowValue(raw_row));
    try std.testing.expectEqual(@as(u32, 1), try mapper.relationalRowSchemaVersion(raw_row));
    try std.testing.expectEqual(commit_ts, try relational_store.rowWriteTimestampNs(raw_row));

    const primary_key = try internal_keys.documentKeyAlloc(alloc, "row:one_shot_recovered");
    defer alloc.free(primary_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, primary_key));
}

test "db identity namespace reassignment refreshes transaction recovery hook context" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer cleanupTempDir(path);

    const old_namespace = doc_identity.Namespace{ .table_id = 28, .shard_id = 2801, .range_id = 28001 };
    const new_namespace = doc_identity.Namespace{ .table_id = 28, .shard_id = 2802, .range_id = 28002 };
    var recorder = TxnResolverRecorder{};
    const server_recovery_config_2: server_recovery.Config = .{
        .enabled = true,
        .interval_ms = 60_000,
        .resolver_ctx = &recorder,
        .resolve_participant_fn = TxnResolverRecorder.resolve,
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .identity_namespace = old_namespace,
        .transaction_recovery = server_recovery.borrowedConfig(&server_recovery_config_2),
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
    });

    const identity_ctx = db.transaction_recovery_identity_context orelse return error.TestExpectedEqual;
    const local_ctx = db.transaction_recovery_local_context orelse return error.TestExpectedEqual;
    try std.testing.expect(identity_ctx.identity_namespace.eql(old_namespace));
    try std.testing.expect(server_recovery.test_support.runtimeConfig(db.transaction_runtime.?).resolution_extra_hooks.build != null);
    try std.testing.expectEqual(
        @intFromPtr(identity_ctx),
        @intFromPtr(server_recovery.test_support.runtimeConfig(db.transaction_runtime.?).resolution_extra_hooks.ctx.?),
    );
    try std.testing.expectEqual(
        @intFromPtr(local_ctx),
        @intFromPtr(server_recovery.test_support.runtimeConfig(db.transaction_runtime.?).local_resolution_ctx.?),
    );
    try std.testing.expect(local_ctx.execution != null);
    try std.testing.expect(local_ctx.execution.?.core == db.core);
    try std.testing.expect(local_ctx.execution.?.local_execution == db.local_execution);

    try db.reassignIdentityNamespaceForInternalTransition(new_namespace);
    try std.testing.expect(db.core.identity_namespace.eql(new_namespace));
    try std.testing.expect(identity_ctx.identity_namespace.eql(new_namespace));

    var txn = try db.core.store.beginProbeTxn();
    defer txn.abort();
    const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a")).?;
    const state = (try doc_identity.lookupStateTxn(&txn, ordinal)).?;
    try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(new_namespace, "doc:a"), state.canonical_doc_id);
}

test "db replicated merge artifacts preserve graph ttl dense sparse projections across replay and reopen" {
    const alloc = std.testing.allocator;
    var donor_path_tmp = try TestDirectory.init("db");
    defer donor_path_tmp.cleanup();
    const donor_path = donor_path_tmp.path().ptr;
    var donor = try DB.open(alloc, std.mem.span(donor_path), .{});
    defer donor.close();
    const configs = [_]types.IndexConfig{
        .{ .name = "dv_v1", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}" },
        .{ .name = "sp_v1", .kind = .sparse_vector, .config_json = "{\"field\":\"sparse\"}" },
        .{ .name = "gr_v1", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"7d\"}}" },
    };
    for (configs) |config| try donor.addIndex(config);
    try donor.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"embedding\":[1,0,0],\"sparse\":{\"indices\":[7],\"values\":[1]},\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:b\"}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .full_index,
    });
    const donor_due = try donor.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, donor_due);
    try std.testing.expectEqual(@as(usize, 1), donor_due.len);
    const rows = try donor.mergeArtifactsPage(alloc, .{ .start = "", .end = "" }, null);
    defer {
        for (rows) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        alloc.free(rows);
    }
    try std.testing.expectEqual(@as(usize, 3), rows.len);
    const end_page = try donor.mergeArtifactsPage(alloc, .{ .start = "", .end = "" }, rows[rows.len - 1].key);
    defer alloc.free(end_page);
    try std.testing.expectEqual(@as(usize, 0), end_page.len);
    const primary = (try donor.get(alloc, "doc:a")).?;
    defer alloc.free(primary);
    // Exercise both a live indexed receiver and a replica that restarts with
    // artifact replay still pending.
    for ([_]bool{ true, false }) |start_workers| {
        var receiver_path_tmp = try TestDirectory.init("db");
        defer receiver_path_tmp.cleanup();
        const receiver_path = receiver_path_tmp.path().ptr;
        {
            var receiver = try DB.open(alloc, std.mem.span(receiver_path), .{ .start_index_workers = start_workers });
            defer receiver.close();
            for (configs) |config| try receiver.addIndex(config);
            try receiver.updateRange(.{ .start = "doc:m", .end = "" });
            try receiver.batch(.{ .merge_checkpoint = .{
                .kind = .accept,
                .transition_id = 1,
                .donor_group_id = 2,
                .receiver_group_id = 3,
                .receiver_base_start = "doc:m",
                .receiver_base_end = "",
                .merged_start = "",
                .merged_end = "",
            } });
            try receiver.batch(.{
                .writes = &.{ .{ .key = "doc:a", .value = primary }, .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" } },
                .sync_level = .full_index,
            });
            const req: types.BatchRequest = .{
                .merge_replication = .{ .transition_id = 1, .donor_group_id = 2, .receiver_group_id = 3, .identity_namespace = receiver.core.identity_namespace },
                .merge_artifacts = rows,
            };
            const identity: OrderedApplyReceipt = .{ .term = 1, .index = 10 };
            try server_test_adapter.applyOrdered(&receiver, req, identity);
            try server_test_adapter.applyOrdered(&receiver, req, identity);
            if (start_workers) {
                try receiver.runUntilIdle();
                try expectMergeArtifactSearches(alloc, &receiver);
            }
        }
        var reopened = try DB.open(alloc, std.mem.span(receiver_path), .{});
        defer reopened.close();
        try reopened.runUntilIdle();
        try expectMergeArtifactSearches(alloc, &reopened);
        const receiver_due = try reopened.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, receiver_due);
        try std.testing.expectEqual(@as(usize, 1), receiver_due.len);
    }
}

test "db replicated merge imports graph source asset and rebuilds ttl contender" {
    const alloc = std.testing.allocator;
    var donor_tmp = try TestDirectory.init("db-graph-source-merge-donor");
    defer donor_tmp.cleanup();
    var receiver_tmp = try TestDirectory.init("db-graph-source-merge-receiver");
    defer receiver_tmp.cleanup();
    var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
    defer donor.close();
    var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
    defer receiver.close();
    const enrichment: types.EnrichmentConfig = .{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" };
    const index: types.IndexConfig = .{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
    };
    try donor.addEnrichment(enrichment);
    try donor.addIndex(index);
    try receiver.addEnrichment(enrichment);
    try receiver.addIndex(index);
    try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"title":"owner","relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2}}
    }}, .sync_level = .enrichments });
    try donor.runUntilIdle();
    const donor_due = try donor.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, donor_due);
    try std.testing.expectEqual(@as(usize, 1), donor_due.len);
    const donor_candidate = (try graph_edge_ttl_expiration.decodeDue(donor_due[0].value)).source;
    const donor_deadline = donor_candidate.deadline_ns;
    const rows = try donor.mergeArtifactsPage(alloc, .{ .start = "", .end = "" }, null);
    defer {
        for (rows) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        alloc.free(rows);
    }
    var saw_asset = false;
    for (rows) |row| if (internal_keys.isAssetArtifactKey(row.key)) {
        saw_asset = true;
    };
    try std.testing.expect(saw_asset);
    const primary = (try donor.get(alloc, "doc:a")) orelse return error.TestExpectedDocument;
    defer alloc.free(primary);
    try receiver.updateRange(.{ .start = "doc:m", .end = "" });
    try receiver.batch(.{ .merge_checkpoint = .{
        .kind = .accept,
        .transition_id = 1,
        .donor_group_id = 2,
        .receiver_group_id = 3,
        .receiver_base_start = "doc:m",
        .receiver_base_end = "",
        .merged_start = "",
        .merged_end = "",
    } });
    try receiver.batch(.{ .writes = &.{.{ .key = "doc:a", .value = primary }}, .sync_level = .full_index });
    try server_test_adapter.applyOrdered(&receiver, .{
        .merge_replication = .{ .transition_id = 1, .donor_group_id = 2, .receiver_group_id = 3, .identity_namespace = receiver.core.identity_namespace },
        .merge_artifacts = rows,
    }, .{ .term = 1, .index = 10 });
    try receiver.runUntilIdle();
    const due = try receiver.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due);
    try std.testing.expectEqual(@as(usize, 1), due.len);
    const contender = (try graph_edge_ttl_expiration.decodeDue(due[0].value)).source;
    try std.testing.expectEqual(donor_deadline, contender.deadline_ns);
    try std.testing.expectEqual(receiver.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation, contender.generation);
}

test "db replicated merge keeps expired graph source suppressed until asset changes" {
    const alloc = std.testing.allocator;
    var donor_tmp = try TestDirectory.init("db-graph-tombstone-merge-donor");
    defer donor_tmp.cleanup();
    var receiver_tmp = try TestDirectory.init("db-graph-tombstone-merge-receiver");
    defer receiver_tmp.cleanup();
    var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
    defer donor.close();
    var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
    defer receiver.close();
    const enrichment: types.EnrichmentConfig = .{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" };
    const index: types.IndexConfig = .{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
    };
    try donor.addEnrichment(enrichment);
    try donor.addIndex(index);
    try receiver.addEnrichment(enrichment);
    try receiver.addIndex(index);
    const original =
        \\{"title":"owner","relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2}}
    ;
    try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value = original }}, .sync_level = .enrichments });
    try donor.runUntilIdle();
    const due_rows = try donor.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due_rows);
    try std.testing.expectEqual(@as(usize, 1), due_rows.len);
    const candidate = (try graph_edge_ttl_expiration.decodeDue(due_rows[0].value)).source;
    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(candidate.deadline_ns + 1);
    var ttl_ctx = TtlCleanupContext{ .batch = engine.test_support.batchContext(engine.test_support.dbPointer(&donor)), .grace_period_ns = 0, .clock = manual_clock.clock() };
    try std.testing.expect(try expireGraphTtlCandidateContext(&ttl_ctx, candidate));
    const donor_tombstone = try internal_keys.graphEdgeTtlTombstoneKeyAlloc(alloc, candidate.edge_key, candidate.index_name, candidate.generation, candidate.state_key);
    defer alloc.free(donor_tombstone);
    const tombstone_value = try donor.core.store.get(alloc, donor_tombstone);
    defer alloc.free(tombstone_value);
    const rows = try donor.mergeArtifactsPage(alloc, .{ .start = "", .end = "" }, null);
    defer {
        for (rows) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        alloc.free(rows);
    }
    var saw_tombstone = false;
    for (rows) |row| if (internal_keys.isGraphEdgeTtlTombstoneKey(row.key)) {
        saw_tombstone = true;
    };
    try std.testing.expect(saw_tombstone);
    const primary = (try donor.get(alloc, "doc:a")) orelse return error.TestExpectedDocument;
    defer alloc.free(primary);
    try receiver.updateRange(.{ .start = "doc:m", .end = "" });
    try receiver.batch(.{ .merge_checkpoint = .{
        .kind = .accept,
        .transition_id = 1,
        .donor_group_id = 2,
        .receiver_group_id = 3,
        .receiver_base_start = "doc:m",
        .receiver_base_end = "",
        .merged_start = "",
        .merged_end = "",
    } });
    try receiver.batch(.{ .writes = &.{.{ .key = "doc:a", .value = primary }}, .sync_level = .enrichments });
    try server_test_adapter.applyOrdered(&receiver, .{
        .merge_replication = .{ .transition_id = 1, .donor_group_id = 2, .receiver_group_id = 3, .identity_namespace = receiver.core.identity_namespace },
        .merge_artifacts = rows,
    }, .{ .term = 1, .index = 10 });
    try receiver.runUntilIdle();
    const rebound_tombstone = try internal_keys.rebindGraphEdgeTtlStateKeyGenerationAlloc(alloc, donor_tombstone, receiver.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation);
    defer alloc.free(rebound_tombstone);
    const rebound_raw = try receiver.core.store.get(alloc, rebound_tombstone);
    defer alloc.free(rebound_raw);
    try std.testing.expectEqualSlices(u8, tombstone_value, rebound_raw);
    const receiver_due = try receiver.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, receiver_due);
    try std.testing.expectEqual(@as(usize, 0), receiver_due.len);
    const expired_edges = try receiver.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, expired_edges);
    try std.testing.expectEqual(@as(usize, 0), expired_edges.len);
    try receiver.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"title":"owner","relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2},"note":1}
    }}, .sync_level = .enrichments });
    try receiver.runUntilIdle();
    try std.testing.expectError(error.NotFound, receiver.core.store.get(alloc, candidate.edge_key));
    try receiver.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"title":"owner","relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":4},"note":1}
    }}, .sync_level = .enrichments });
    try receiver.runUntilIdle();
    try std.testing.expectError(error.NotFound, receiver.core.store.get(alloc, rebound_tombstone));
    const revived = try receiver.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, revived);
    try std.testing.expectEqual(@as(usize, 1), revived.len);
    try std.testing.expectEqual(@as(f64, 4), revived[0].weight);
}

test "db repair activation restarts unjournaled source races without penalizing replayed writes" {
    const alloc = std.testing.allocator;
    const publication = @import("db/artifact_publication.zig");
    for ([_]bool{ false, true }) |unjournaled| {
        var directory = try TestDirectory.init("repair-source-gap");
        defer directory.cleanup();
        var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{
            .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
            .online_source_authority = .raft,
            .primary_backend = .{ .lsm = .{} },
            .index_backends = .{ .text_main_backend = .lsm },
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try server_test_adapter.applyOrdered(&db, .{ .writes = &.{ .{ .key = "doc", .value = "{\"title\":\"alpha\"}" }, .{ .key = "historic", .value = "{\"title\":\"historical\"}" } }, .sync_level = .write }, .{ .term = 1, .index = 1 });
        const repair_id = (try db.admitManagedIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" })).?;
        var catalog = try db.artifactInventoryCommand(alloc);
        defer catalog.catalogs.deinit(alloc);
        catalog.binding.effect_protocol = 15;
        try server_test_adapter.applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 2 });
        var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
        activation.publication_digest = activation.digest();
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = activation }, .{ .term = 1, .index = 3 });
        for (0..8) |_| {
            const step = try db.advanceIndexRepairIntent(alloc, repair_id, .{ .max_activation_pause_ms = 1 });
            try std.testing.expect(!step.repaired and !step.terminal);
            if (step.busy) break;
        } else return error.TestExpectedYield;
        var pending = try engine.test_support.loadIndexRepairEntryById(engine.test_support.dbPointer(&db), alloc, repair_id);
        defer pending.deinit(alloc);
        try std.testing.expect(pending.intent.build_source_guard.?.boundary != null);
        if (unjournaled) {
            const primary = try internal_keys.documentKeyAlloc(alloc, "doc");
            defer alloc.free(primary);
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            var marker: [16]u8 = undefined;
            std.mem.writeInt(u64, marker[0..8], 1, .little);
            std.mem.writeInt(u64, marker[8..16], 4, .little);
            try txn.put(&internal_keys.raft_document_applied_entry_key, &marker);
            try txn.put(primary, "{\"title\":\"beta\"}");
            try txn.commit();
        } else try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{\"title\":\"beta\"}" }}, .sync_level = .write }, .{ .term = 1, .index = 4 });
        if (unjournaled) {
            const stale = try db.advanceIndexRepairIntent(alloc, repair_id, .{ .max_activation_pause_ms = 5_000 });
            try std.testing.expect(!stale.repaired and !stale.terminal);
            var reset = try engine.test_support.loadIndexRepairEntryById(engine.test_support.dbPointer(&db), alloc, repair_id);
            defer reset.deinit(alloc);
            try std.testing.expect(reset.intent.candidate_relative_path == null);
            try std.testing.expectEqual(@as(u32, 0), reset.intent.failure_streak);
            try std.testing.expectEqual(@as(u64, 0), reset.intent.next_retry_at_ms);
        }
        const resumed = try db.advanceIndexRepairIntent(alloc, repair_id, .{ .max_activation_pause_ms = 5_000 });
        try std.testing.expect(resumed.repaired);
        if (!unjournaled) try std.testing.expectEqual(@as(u64, 0), resumed.documents_reprocessed);
        var result = try db.search(alloc, .{ .index_name = "text", .query = .{ .match = .{ .field = "_all", .text = "beta" } }, .limit = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        const seal = (try db.core.index_manager.textIndexEntry("text").?.persistent.loadProjectionSeal(alloc)).?;
        try std.testing.expect(seal.baseline != null);
        try std.testing.expectEqual(@as(u64, if (unjournaled) 1 else 0), seal.baseline.?.gap_epoch);
        // Enroll the pre-activation row through the real ordered baseline,
        // then close both native and projection requirements without rewriting
        // the historic document or borrowing the newer global journal tip.
        var baseline_page = (try @import("db/artifact_producer_baseline.zig").prepareRaft(alloc, db.core.store)).?;
        defer baseline_page.deinit();
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = baseline_page.command }, .{ .term = 1, .index = 5 });
        var plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer plan.release();
        for (0..16) |_| {
            if (try @import("db/artifact_native_stream.zig").advance(alloc, db.core.store, db.root_incarnation, "historic", plan.plan()) == .closed) break;
        } else return error.TestExpectedHistoricNativeClosure;
        const completion = @import("db/artifact_completion_progress.zig");
        try completion.refreshProjections(alloc, db.core.store, db.core.index_manager, db.core.applied_sequence_checkpoint_path, db.root_incarnation, "historic", plan.plan(), .{ .time_budget_ns = null });
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            const node = for (plan.plan().completion_plan.?.nodes) |*value| {
                if (value.kind == .index_projection) break value;
            } else return error.TestExpectedProjectionNode;
            var adopted = try @import("db/artifact_projection_certificate.zig").prepareClosure(alloc, &read, db.root_incarnation, "doc", node);
            defer adopted.deinit();
            try std.testing.expectEqual(unjournaled, adopted.baseline != null);
            try adopted.requireCurrent(&read, db.root_incarnation);
        }
        var prepared = blk: {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expect(try publication.materializationState(&read, catalog.namespace, "historic") == null);
            break :blk (try completion.discover(alloc, &read, db.root_incarnation, "historic", plan.plan(), .{ .time_budget_ns = null })).?;
        };
        defer prepared.deinit();
        try std.testing.expect(prepared.atEnd());
        {
            var txn = try db.core.store.beginWriteTxn();
            defer txn.abort();
            try @import("db/artifact_source_gap.zig").record(&txn);
            try std.testing.expectError(error.EnrichmentSourceChanged, prepared.stageCurrent(&txn, db.root_incarnation));
        }
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = try prepared.command() }, .{ .term = 1, .index = 6 });
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(try @import("db/artifact_producer_obligations.zig").lookupWork(alloc, &read, (try publication.authority(&read)).?, "historic") == null);
    }
}

test "db repair activation automatically schedules bounded historical adoption and preserves pause across restart" {
    const alloc = std.testing.allocator;
    const publication = @import("db/artifact_publication.zig");
    var directory = try TestDirectory.init("automatic-artifact-adoption");
    defer directory.cleanup();
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .index_backends = .{ .text_main_backend = .lsm }, .start_index_workers = false, .start_optional_runtimes = false };
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), options);
    defer db.close();
    try db.addIndex(.{ .name = "first", .kind = .full_text, .config_json = "{}", .coverage_generation = 7 });
    try db.addIndex(.{ .name = "second", .kind = .full_text, .config_json = "{}", .coverage_generation = 9 });
    try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "historic", .value = "{\"title\":\"historical\"}" }}, .sync_level = .full_index }, .{ .term = 1, .index = 1 });
    try db.runUntilIdle();
    var catalog = try db.artifactInventoryCommand(alloc);
    defer catalog.catalogs.deinit(alloc);
    // Simulate an imported physical generation retaining its source root's
    // valid seal. It is queryable, but that evidence cannot certify this owner.
    // The second index has no seal, exercising both adoption admission paths.
    {
        const entry = db.core.index_manager.textIndexEntry("first").?;
        try entry.persistent.publishProjectionSeal(.{
            .root = if (db.root_incarnation == 1) 2 else 1,
            .namespace = catalog.namespace,
            .generation = entry.config.coverage_generation,
            .config_hash = types.indexConfigHash(entry.config),
            .applied_sequence = try db.core.loadAppliedSequence(alloc, "first"),
        });
    }
    catalog.binding.effect_protocol = 15;
    try server_test_adapter.applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 2 });
    var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    activation.publication_digest = activation.digest();
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = activation }, .{ .term = 1, .index = 3 });
    var baseline = (try @import("db/artifact_producer_baseline.zig").prepareRaft(alloc, db.core.store)).?;
    defer baseline.deinit();
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = baseline.command }, .{ .term = 1, .index = 4 });
    {
        var plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer plan.release();
        {
            var busy = (try apply_state.tryAcquireProjectionSnapshot(alloc, db.core.index_manager.checkpointIo(), db.core.store, db.core.applied_sequence_checkpoint_path)).?;
            defer busy.deinit();
            try engine.test_support.refreshArtifactProjections(engine.test_support.dbPointer(&db), alloc, "historic", plan.plan(), .{ .time_budget_ns = null });
            try std.testing.expect(try engine.test_support.indexRepairIdForIndex(engine.test_support.dbPointer(&db), alloc, "first") == null);
            try std.testing.expect(try engine.test_support.indexRepairIdForIndex(engine.test_support.dbPointer(&db), alloc, "second") == null);
        }
        try engine.test_support.refreshArtifactProjections(engine.test_support.dbPointer(&db), alloc, "historic", plan.plan(), .{ .time_budget_ns = null });
        {
            var state = try db.loadIndexRepairState(alloc);
            defer state.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 1), state.entries.items.len);
        }
        try engine.test_support.refreshArtifactProjections(engine.test_support.dbPointer(&db), alloc, "historic", plan.plan(), .{ .time_budget_ns = null });
        var before = try db.loadIndexRepairState(alloc);
        defer before.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 2), before.entries.items.len);
        for (before.entries.items) |entry| {
            try std.testing.expectEqual(index_repair_state.Trigger.artifact_baseline_adoption, entry.intent.trigger);
            try std.testing.expectEqual(@as(u64, 0), entry.intent.operator_job_id);
            try std.testing.expect(!engine.test_support.indexRepairIntentBlocksService(entry.intent));
        }
        try engine.test_support.refreshArtifactProjections(engine.test_support.dbPointer(&db), alloc, "historic", plan.plan(), .{ .time_budget_ns = null });
        var after = try db.loadIndexRepairState(alloc);
        defer after.deinit(alloc);
        try std.testing.expectEqual(before.control_revision, after.control_revision);
    }
    const first = (try engine.test_support.indexRepairIdForIndex(engine.test_support.dbPointer(&db), alloc, "first")).?;
    const second = (try engine.test_support.indexRepairIdForIndex(engine.test_support.dbPointer(&db), alloc, "second")).?;
    {
        var result = try db.search(alloc, .{ .index_name = "first", .query = .{ .match = .{ .field = "_all", .text = "historical" } }, .limit = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        var pause = try db.repairArtifactIssuesWithRequest(alloc, .{ .target = .index, .index_name = "first", .control = .pause_automatic });
        defer pause.deinit(alloc);
        try std.testing.expectEqual(@as(u64, 1), pause.controls_applied);
    }
    db.close();
    db = try DB.open(alloc, std.mem.span(directory.path().ptr), options);
    {
        var plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer plan.release();
        try engine.test_support.refreshArtifactProjections(engine.test_support.dbPointer(&db), alloc, "historic", plan.plan(), .{ .time_budget_ns = null });
        var paused = try engine.test_support.loadIndexRepairEntryById(engine.test_support.dbPointer(&db), alloc, first);
        defer paused.deinit(alloc);
        try std.testing.expectEqual(index_repair_state.Automation.paused, paused.intent.automation);
    }
    try std.testing.expect((try db.advanceIndexRepairIntent(alloc, second, .{ .max_activation_pause_ms = 5_000 })).repaired);
    try std.testing.expect(try db.resumeAutomaticIndexRepair(alloc, "first", first));
    try std.testing.expect((try db.advanceIndexRepairIntent(alloc, first, .{ .max_activation_pause_ms = 5_000 })).repaired);
    var plan = try db.core.index_manager.acquireWritePlanSnapshot();
    defer plan.release();
    for (0..16) |_| {
        if (try @import("db/artifact_native_stream.zig").advance(alloc, db.core.store, db.root_incarnation, "historic", plan.plan()) == .closed) break;
    } else return error.TestExpectedNativeClosure;
    try engine.test_support.refreshArtifactProjections(engine.test_support.dbPointer(&db), alloc, "historic", plan.plan(), .{ .time_budget_ns = null });
    var complete = blk: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        break :blk (try @import("db/artifact_completion_progress.zig").discover(alloc, &read, db.root_incarnation, "historic", plan.plan(), .{ .time_budget_ns = null })).?;
    };
    defer complete.deinit();
    try std.testing.expect(complete.atEnd());
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = try complete.command() }, .{ .term = 1, .index = 5 });
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqual(@as(u64, 0), (try @import("db/artifact_producer_obligations.zig").load(&read)).?.pending_documents);
}

test "db replicated transaction commits each raft receipt atomically" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const txn_id: transactions_mod.TxnId = .{0x5a} ** 16;
    const participant = "table:receipts:group:7";
    const begin_entry: OrderedApplyReceipt = .{ .term = 3, .index = 11 };
    const prepare_entry: OrderedApplyReceipt = .{ .term = 3, .index = 12 };
    const resolve_entry: OrderedApplyReceipt = .{ .term = 3, .index = 13 };

    _ = try db.beginReplicatedTransactionAtRaftEntry(
        txn_id,
        12_000,
        12_000,
        &.{participant},
        false,
        false,
        begin_entry,
    );
    try std.testing.expectEqualDeep(begin_entry, (try db.orderedApplyReceipt()).?);

    // An exact begin replay is fenced before it can alter the existing record.
    _ = try db.beginReplicatedTransactionAtRaftEntry(
        txn_id,
        99_000,
        99_000,
        &.{participant},
        false,
        false,
        begin_entry,
    );
    try std.testing.expectEqual(transactions_mod.TxnStatus.pending, try db.getTransactionStatus(txn_id));

    try db.writeReplicatedTransactionAtRaftEntry(txn_id, .{
        .writes = &.{.{ .key = "doc:receipt", .value = "{\"title\":\"transaction\"}" }},
    }, prepare_entry);
    try std.testing.expectEqualDeep(prepare_entry, (try db.orderedApplyReceipt()).?);

    try db.resolveReplicatedTransactionAtRaftEntry(
        txn_id,
        .committed,
        15_000,
        .write,
        .none,
        resolve_entry,
        participant,
    );
    try std.testing.expectEqualDeep(resolve_entry, (try db.orderedApplyReceipt()).?);
    const unresolved = try db.getUnresolvedTransactionParticipants(alloc, txn_id);
    defer transactions_mod.freeParticipantList(alloc, unresolved);
    try std.testing.expectEqual(@as(usize, 0), unresolved.len);

    // Replaying the terminal entry after a newer write must not replay the
    // transaction's document batch. This also exercises the terminal
    // completion path used after a crash between old non-atomic versions.
    try db.batch(.{
        .writes = &.{.{ .key = "doc:receipt", .value = "{\"title\":\"newer\"}" }},
        .timestamp_ns = 16_000,
    });
    try db.resolveReplicatedTransactionAtRaftEntry(
        txn_id,
        .committed,
        15_000,
        .write,
        .none,
        resolve_entry,
        participant,
    );
    const raw = (try db.get(alloc, "doc:receipt")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"newer\"}", raw);
    try std.testing.expectEqual(@as(u64, 16_000), try db.getTimestamp(alloc, "doc:receipt"));
}

test "storage.hot_standby merge proof adoption certifies receiver-local absent output" {
    const alloc = std.testing.allocator;
    const publication = @import("db/artifact_publication.zig");
    const provenance = @import("db/artifact_producer_provenance.zig");
    const proof_batch = @import("db/source_proof_batch.zig");
    const inventory = @import("db/artifact_inventory.zig");
    const pages = @import("db/merge_page_contract.zig");
    const source_catalog = @import("db/merge_artifact_catalog.zig");
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer cleanupTempDir(path);
    const receiver_namespace: @import("db/doc_identity_namespace.zig").Namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 };
    const donor_namespace: @import("db/doc_identity_namespace.zig").Namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 };
    var db = try DB.open(alloc, std.mem.span(path), .{ .identity_namespace = receiver_namespace, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    const keys = @import("internal_keys.zig");
    const row_key = try keys.documentKeyAlloc(alloc, "doc");
    defer alloc.free(row_key);
    const ttl_key = try keys.ttlKeyAlloc(alloc, "doc");
    defer alloc.free(ttl_key);
    const output_key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "output");
    defer alloc.free(output_key);
    const index_prefix = "AIDX\x02\x00\x00\x00\x01\x00\x00\x00\x05\x00\x00\x00index\x00\x02\x00\x00\x00{}";
    const donor_catalog: inventory.Catalogs = .{ .indexes = index_prefix ++ "\x01\x00\x00\x00\x00\x00\x00\x00" };
    const receiver_catalog: inventory.Catalogs = .{ .indexes = index_prefix ++ "\x09\x00\x00\x00\x00\x00\x00\x00" };
    const donor_binding: inventory.Binding = .{ .epoch = 1, .digest = donor_catalog.digest(), .semantic_digest = try donor_catalog.semanticDigest(alloc), .effect_protocol = 15 };
    const receiver_binding: inventory.Binding = .{ .epoch = 2, .digest = receiver_catalog.digest(), .semantic_digest = try receiver_catalog.semanticDigest(alloc), .effect_protocol = 15 };
    var receiver_bytes: publication.Namespace = undefined;
    var donor_bytes: publication.Namespace = undefined;
    doc_identity.encodeNamespace(&receiver_bytes, receiver_namespace);
    doc_identity.encodeNamespace(&donor_bytes, donor_namespace);
    const progress: pages.Progress = .{
        .version = 2,
        .transition_id = 42,
        .donor_group_id = 2,
        .receiver_group_id = 3,
        .receiver_namespace = receiver_namespace,
        .attempt = .{ .donor_term = 3, .sequence = 1 },
        .source = .{ .namespace = donor_namespace, .pin_digest = @splat(9), .applied_index = 5, .retention = .{ .epoch = 1, .after_sequence = 0 }, .artifact_catalog = donor_binding, .provenance_required = true },
        .provenance_pending = true,
    };
    const progress_raw = try pages.encode(alloc, progress);
    defer alloc.free(progress_raw);
    const source_raw = (try source_catalog.encode(alloc, .{ .kind = @as(enum { begin_copy, accept }, .begin_copy), .page_source = @as(?pages.Source, progress.source), .page_receiver_namespace = @as(?@TypeOf(receiver_namespace), receiver_namespace), .page_source_catalogs = @as(?inventory.Catalogs, donor_catalog) }, progress)).?;
    defer alloc.free(source_raw);
    var state_bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer state_bytes.deinit(alloc);
    try merge_state_mod.encode(&state_bytes, alloc, .{ .transition_id = progress.transition_id, .donor_group_id = progress.donor_group_id, .receiver_group_id = progress.receiver_group_id, .phase = .accepting, .receiver_base_range = .{ .start = "dop", .end = "" }, .merged_range = .{ .start = "doc", .end = "" }, .copy_attempt = progress.attempt });
    const ordered_raw = try std.json.Stringify.valueAlloc(alloc, inventory.Ordered{ .command = .{ .namespace = receiver_bytes, .previous = donor_binding, .binding = receiver_binding, .catalogs = receiver_catalog }, .applied_index = 4 }, .{ .emit_strings_as_arrays = true });
    defer alloc.free(ordered_raw);
    const row = "{}";
    var row_digest: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(row, &row_digest, .{});
    const source = publication.Source{ .document_key = "doc", .content_digest = row_digest, .timestamp = 7, .input_position = null };
    const effect = provenance.Effect{ .family = .base_vector, .key = output_key, .source_index = 0, .value_digest = null, .value_bytes = 0 };
    var donor_proof: provenance.Proof = .{ .namespace = donor_bytes, .authority_epoch = donor_binding.epoch, .catalog_digest = donor_binding.digest, .producer_kind = .index, .producer_name = "index", .producer_generation = 1, .producer_artifact_name = "output", .publication_digest = @splat(4), .input_digest = undefined, .sources = (&source)[0..1], .artifact_sources = &.{}, .effects = (&effect)[0..1] };
    donor_proof.input_digest = donor_proof.inputCommand().inputDigest();
    const proof_raw = try provenance.encodeAlloc(alloc, donor_proof);
    defer alloc.free(proof_raw);
    const imported = try proof_batch.encodeValueAlloc(alloc, &.{1}, proof_raw);
    defer alloc.free(imported);
    const record_digest = proof_batch.recordDigest(proof_raw[proof_raw.len - 32 ..][0..32].*, &.{1});
    const imported_key = proof_batch.mergeKey(donor_bytes, progress.source.pin_digest, donor_proof.publication_digest);
    const witness_key = proof_batch.witnessKey(donor_bytes, progress.source.pin_digest, donor_proof.publication_digest);
    var ttl: [8]u8 = undefined;
    std.mem.writeInt(u64, &ttl, 7, .little);
    var authority_raw: [100]u8 = undefined;
    @memcpy(authority_raw[0..4], "APA1");
    @memcpy(authority_raw[4..28], &receiver_bytes);
    std.mem.writeInt(u64, authority_raw[28..36], receiver_binding.epoch, .little);
    @memcpy(authority_raw[36..68], &receiver_binding.digest);
    std.crypto.hash.Blake3.hash(authority_raw[0..68], authority_raw[68..100], .{});
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(row_key, row);
        try txn.put(ttl_key, &ttl);
        try txn.put(inventory.index_key, receiver_catalog.indexes);
        try inventory.refresh(&txn);
        try txn.put(inventory.ordered_key, ordered_raw);
        try txn.put(publication.authority_key, &authority_raw);
        try txn.put(merge_state_mod.key, state_bytes.items);
        try txn.put(pages.key, progress_raw);
        try txn.put(source_catalog.key, source_raw);
        try txn.put(&imported_key, imported);
        try txn.put(&witness_key, &record_digest);
        try txn.commit();
    }
    const command: @import("db/merge_proof_adoption.zig").Command = .{ .transition_id = progress.transition_id, .attempt = progress.attempt, .source_pin = progress.source.pin_digest, .proof_digest = donor_proof.publication_digest, .record_digest = record_digest };
    try server_test_adapter.applyOrdered(&db, .{ .merge_proof_adoption = command }, .{ .term = 4, .index = 7 });
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqualDeep(publication.Position{ .raft = .{ .term = 4, .index = 7 } }, (try publication.artifactRevision(&read, receiver_bytes, output_key)).?);
    }
    try std.testing.expectEqual(@as(u64, 7), (try db.orderedApplyReceipt()).?.index);
    var wrong = command;
    wrong.record_digest = @splat(8);
    try server_test_adapter.applyOrdered(&db, .{ .merge_proof_adoption = wrong }, .{ .term = 4, .index = 8 });
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqualDeep(publication.Position{ .raft = .{ .term = 4, .index = 7 } }, (try publication.artifactRevision(&read, receiver_bytes, output_key)).?);
    try std.testing.expectEqual(@as(u64, 8), (try db.orderedApplyReceipt()).?.index);
}

test "storage.hot_standby stale merge proof adoption advances only its ordered watermark" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer cleanupTempDir(path);
    const command: @import("db/merge_proof_adoption.zig").Command = .{
        .transition_id = 42,
        .attempt = .{ .donor_term = 3, .sequence = 1 },
        .source_pin = @splat(1),
        .proof_digest = @splat(2),
        .record_digest = @splat(3),
    };
    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();
        const before = db.core.nextDerivedSequence();
        try server_test_adapter.applyOrdered(&db, .{ .merge_proof_adoption = command }, .{ .term = 2, .index = 7 });
        try std.testing.expectEqual(@as(u64, 7), (try db.orderedApplyReceipt()).?.index);
        try std.testing.expectEqual(before, db.core.nextDerivedSequence());
    }
    var reopened = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer reopened.close();
    try server_test_adapter.applyOrdered(&reopened, .{ .merge_proof_adoption = command }, .{ .term = 2, .index = 7 });
    try std.testing.expectEqual(@as(u64, 7), (try reopened.orderedApplyReceipt()).?.index);
}

test "db raced replicated transaction completion persists receipt and participant acknowledgement" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const txn_id: transactions_mod.TxnId = .{0x6b} ** 16;
    const participant = "table:receipts:group:8";
    _ = try db.beginReplicatedTransactionAtRaftEntry(
        txn_id,
        20_000,
        20_000,
        &.{participant},
        true,
        true,
        .{ .term = 4, .index = 21 },
    );
    try db.writeReplicatedTransactionAtRaftEntry(txn_id, .{
        .writes = &.{.{ .key = "doc:raced-receipt", .value = "{\"title\":\"transaction\"}" }},
    }, .{ .term = 4, .index = 22 });

    var collected = try db.core.collectTransactionIntentBatch(alloc, txn_id);
    defer collected.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), collected.writes.len);

    // Model the production race: collection happened outside the apply fence,
    // then recovery resolved the intents before the replicated entry acquired
    // the lock. Resolution deliberately preserves intent_revision.
    try db.resolveTransactionIntents(txn_id, .committed, 25_000);
    const unresolved_before = try db.getUnresolvedTransactionParticipants(alloc, txn_id);
    defer transactions_mod.freeParticipantList(alloc, unresolved_before);
    try std.testing.expectEqual(@as(usize, 1), unresolved_before.len);

    const resolve_entry: OrderedApplyReceipt = .{ .term = 4, .index = 23 };
    try engine.test_support.batchInternal(engine.test_support.dbPointer(&db), .{
        .writes = &.{.{ .key = "doc:raced-receipt", .value = "{\"title\":\"transaction\"}" }},
        .timestamp_ns = 25_000,
        .sync_level = .write,
    }, null, .{
        .bypass_replication_write_gate = true,
        .raft_applied_entry_marker = resolve_entry,
        .transaction_resolution = .{
            .txn_id = txn_id,
            .status = .committed,
            .commit_version = 25_000,
            .expected_intent_revision = collected.revision,
            .intent_keys = &.{"doc:raced-receipt"},
            .resolved_participant = participant,
        },
    });

    try std.testing.expectEqualDeep(resolve_entry, (try db.orderedApplyReceipt()).?);
    const unresolved_after = try db.getUnresolvedTransactionParticipants(alloc, txn_id);
    defer transactions_mod.freeParticipantList(alloc, unresolved_after);
    try std.testing.expectEqual(@as(usize, 0), unresolved_after.len);
}

test "online direct vector uncertified source cannot authorize unknown effects or chunks" {
    const alloc = std.testing.allocator;
    const pages = @import("db/merge_page_contract.zig");
    var directory = try TestDirectory.init("online-vector-mode-fence");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .identity_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} } });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    var catalog = try db.artifactInventoryCommand(alloc);
    defer catalog.catalogs.deinit(alloc);
    try server_test_adapter.applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 1 });
    const key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "a", "unknown");
    defer alloc.free(key);
    const value = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &.{ 1, 2 });
    defer alloc.free(value);
    var request: types.BatchRequest = .{
        .merge_replication = .{ .transition_id = 1, .donor_group_id = 2, .receiver_group_id = 3, .identity_namespace = db.core.identity_namespace, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } },
        .merge_page = .{ .source = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .pin_digest = @splat(1), .applied_index = 2, .retention = .{ .epoch = 1, .after_sequence = 0 }, .artifact_catalog = catalog.binding }, .sequence = 1, .phase = .tail, .exhausted = false, .digest = @splat(0), .tail = .{ .fragment = .{ .sequence = 1, .offset = 0, .total_effects = 1, .frame_digest = @splat(2) } }, .artifact_effects = &.{.{ .key = key, .value = value }} },
    };
    // Legacy REF3 sources do not certify base-artifact capture, regardless of
    // receiver catalog. Ordered protocol-14 sources do, even without an active
    // vector projection, because historical base values remain user data.
    request.merge_page.?.source.artifact_catalog = null;
    request.merge_page.?.digest = pages.commandDigest(request);
    try std.testing.expectError(error.InvalidMergePage, server_test_adapter.applyOrdered(&db, request, .{ .term = 1, .index = 2 }));
    request.merge_page.?.source.artifact_catalog = catalog.binding;
    request.merge_page.?.digest = pages.commandDigest(request);
    const chunks = try pages.RowChunks(types.BatchRequest).init(request);
    var chunked = try chunks.requestAt(0);
    chunked.merge_page.?.source.artifact_catalog = null;
    chunked.merge_page.?.digest = pages.commandDigest(chunked);
    try std.testing.expectError(error.InvalidMergePage, server_test_adapter.applyOrdered(&db, chunked, .{ .term = 1, .index = 2 }));
    try std.testing.expectEqual(@as(u64, 1), (try db.orderedApplyReceipt()).?.index);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, key));
}

test "online direct vector Raft retention captures exact artifacts and rejects unmarked writes after reopen" {
    const alloc = std.testing.allocator;
    const retention = @import("retained_effects.zig");
    var directory = try TestDirectory.init("online-vector-retention");
    defer directory.cleanup();
    const options: OpenOptions = .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} } };
    var db = try DB.open(alloc, directory.path(), options);
    defer db.close();
    try db.addIndex(.{ .name = "dense", .kind = .dense_vector, .config_json = "{\"field\":\"v\",\"dims\":2}" });
    try db.addIndex(.{ .name = "sparse", .kind = .sparse_vector, .config_json = "{\"field\":\"s\"}" });
    var namespace: retention.Namespace = undefined;
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        doc_identity.encodeNamespace(&namespace, db.core.identity_namespace);
        try txn.put(&internal_keys.identity_namespace_key, &namespace);
        _ = try retention.admitWithDirectVectors(&txn, namespace, 1, @splat(9), retention.default_limit, true);
        try txn.commit();
    }
    const request: types.BatchRequest = .{ .writes = &.{.{ .key = "row", .value = "{\"v\":[1,2],\"s\":{\"indices\":[1,3],\"values\":[2,4]}}" }}, .sync_level = .full_index };
    var invalid_intents = [_]transactions_mod.WriteIntent{.{ .key = "invalid", .value = "{\"v\":[1e999,2]}", .retained_artifact_bytes = 1, .retained_artifact_keys = 1 }};
    try std.testing.expectError(error.InvalidBatchRequest, engine.test_support.prepareOnlineVectorIntentBoundsLocked(engine.test_support.dbPointer(&db), alloc, &invalid_intents, null));
    var bounded_intents = [_]transactions_mod.WriteIntent{.{ .key = "row", .value = request.writes[0].value, .retained_artifact_bytes = 0, .retained_artifact_keys = 0 }};
    try engine.test_support.prepareOnlineVectorIntentBoundsLocked(engine.test_support.dbPointer(&db), alloc, &bounded_intents, null);
    try std.testing.expectEqual(@as(u64, 2), bounded_intents[0].retained_artifact_keys);
    try std.testing.expect(bounded_intents[0].retained_artifact_bytes > request.writes[0].value.len);
    try std.testing.expectError(error.RetainedEffectsFenceMismatch, db.batch(request));
    try server_test_adapter.applyOrdered(&db, request, .{ .term = 1, .index = 1 });
    try server_test_adapter.applyOrdered(&db, request, .{ .term = 1, .index = 1 });
    db.close();
    db = try DB.open(alloc, directory.path(), options);
    {
        var txn = try db.core.store.beginReadTxn();
        defer txn.abort();
        try std.testing.expect((try retention.load(&txn)).?.direct_vectors);
        var frame = (try retention.read(&txn, namespace, 1, @splat(9), 0)).?;
        var vectors: usize = 0;
        while (try frame.next()) |effect| if (effect.isVector()) {
            vectors += 1;
            try @import("db/online_vector_artifacts.zig").validate(effect.key, effect.value);
            try std.testing.expectEqualSlices(u8, try txn.get(effect.key), effect.value.?);
        };
        try std.testing.expectEqual(@as(usize, 2), vectors);
        try std.testing.expectEqual(@as(u64, 1), (try retention.load(&txn)).?.latest);
    }
    try server_test_adapter.applyOrdered(&db, .{ .deletes = &.{"row"}, .sync_level = .full_index }, .{ .term = 1, .index = 2 });
    var txn = try db.core.store.beginReadTxn();
    defer txn.abort();
    var frame = (try retention.read(&txn, namespace, 1, @splat(9), 1)).?;
    var tombstones: usize = 0;
    while (try frame.next()) |effect| if (effect.isVector()) {
        try std.testing.expect(effect.value == null);
        tombstones += 1;
    };
    try std.testing.expectEqual(@as(usize, 2), tombstones);
}

test "db participant recovery callbacks run outside the apply lock" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const txn_id = try db.beginTransactionWithParticipants(1_000, &.{"remote"});
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:reentrant-recovery", .value = "{\"title\":\"value\"}" }},
    });
    try db.resolveTransactionIntents(txn_id, .committed, 2_000);

    const ReentrantResolver = struct {
        db: *DB,
        calls: usize = 0,

        fn resolve(ctx: *anyopaque, resolved_txn_id: transactions_mod.TxnId, participant: []const u8, _: transactions_mod.TxnStatus, _: u64) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try std.testing.expectEqualStrings("remote", participant);
            // This acquires the DB apply lock. It would deadlock if recovery
            // still held that lock while invoking participant callbacks.
            try self.db.markTransactionParticipantResolved(resolved_txn_id, participant);
            self.calls += 1;
        }
    };
    var resolver = ReentrantResolver{ .db = &db };
    const stats = try server_recovery.runDbRecoveryOnce(&db, .{
        .enabled = true,
        .lease_owned = true,
        .resolver_ctx = &resolver,
        .resolve_participant_fn = ReentrantResolver.resolve,
        .cutoff_ns = 1,
    });

    try std.testing.expectEqual(@as(usize, 1), resolver.calls);
    try std.testing.expectEqual(@as(u64, 1), stats.notification_successes);
    try std.testing.expectError(transactions_mod.TxnError.TxnNotFound, db.getTransactionStatus(txn_id));
}

test "db transaction recovery runtime resolves participants and unblocks cleanup" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer cleanupTempDir(path);

    var recorder = TxnResolverRecorder{};
    const txn_id = blk: {
        var setup_db = try DB.open(alloc, std.mem.span(path), .{});
        defer setup_db.close();

        const txn_id = try setup_db.beginTransactionWithParticipants(1_000, &.{ "local", "remote" });
        try setup_db.writeTransaction(txn_id, .{
            .writes = &.{.{ .key = "doc:participant_runtime", .value = "{\"title\":\"value\"}" }},
        });
        try setup_db.resolveTransactionIntents(txn_id, .committed, 2_000);
        try setup_db.markTransactionParticipantResolved(txn_id, "local");
        break :blk txn_id;
    };

    const server_recovery_config_3: server_recovery.Config = .{
        .enabled = true,
        .interval_ms = 10,
        .cutoff_ns = 1,
        .resolver_ctx = &recorder,
        .resolve_participant_fn = TxnResolverRecorder.resolve,
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .transaction_recovery = server_recovery.borrowedConfig(&server_recovery_config_3),
    });
    defer db.close();
    try std.testing.expect(db.transaction_recovery_identity_context != null);
    try std.testing.expect(server_recovery.test_support.runtimeConfig(db.transaction_runtime.?).resolution_extra_hooks.build != null);

    var cleared = false;
    var attempts: usize = 0;
    while (attempts < 500) : (attempts += 1) {
        const status = db.getTransactionStatus(txn_id);
        if (status) |_| {} else |err| {
            if (err == transactions_mod.TxnError.TxnNotFound) {
                cleared = true;
                break;
            }
            return err;
        }
        sleepPollInterval();
    }
    if (!cleared) return error.TransactionRecoveryCleanupTimeout;

    var stats = try db.stats(alloc);
    var stats_ready = stats.transaction_recovery.runs > 0 and
        stats.transaction_recovery.notification_attempts > 0 and
        stats.transaction_recovery.notification_successes > 0 and
        stats.transaction_recovery.cleaned_records > 0;
    var resolver_called = false;
    lockAtomic(&recorder.mutex);
    resolver_called = recorder.calls > 0;
    recorder.mutex.unlock();

    attempts = 0;
    while ((!stats_ready or !resolver_called) and attempts < 500) : (attempts += 1) {
        types.freeDBStats(alloc, stats);
        sleepPollInterval();
        stats = try db.stats(alloc);
        stats_ready = stats.transaction_recovery.runs > 0 and
            stats.transaction_recovery.notification_attempts > 0 and
            stats.transaction_recovery.notification_successes > 0 and
            stats.transaction_recovery.cleaned_records > 0;
        lockAtomic(&recorder.mutex);
        resolver_called = recorder.calls > 0;
        recorder.mutex.unlock();
    }
    defer types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.transaction_recovery.enabled);
    if (!stats_ready) return error.TransactionRecoveryStatsTimeout;
    try std.testing.expectError(transactions_mod.TxnError.TxnNotFound, db.getTransactionStatus(txn_id));
    if (!resolver_called) return error.TransactionRecoveryResolverTimeout;
}

test "db transaction recovery runtime rebuilds all derived effects for committed orphaned intents" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer cleanupTempDir(path);

    const txn_id = blk: {
        var setup_db = try DB.open(alloc, std.mem.span(path), .{});
        defer setup_db.close();
        try setup_db.addIndex(.{ .name = "ft_recovered_txn", .kind = .full_text, .config_json = "{}" });

        const txn_id = try setup_db.beginTransaction(1_000);
        try setup_db.writeTransaction(txn_id, .{
            .writes = &.{.{ .key = "doc:recovered_orphan", .value = "{\"title\":\"recovered\"}" }},
        });

        const record_key = blk_key: {
            const prefix = "\x00\x00__txn_records__:";
            var key: [prefix.len + @sizeOf(transactions_mod.TxnId)]u8 = undefined;
            @memcpy(key[0..prefix.len], prefix);
            @memcpy(key[prefix.len..], &txn_id);
            break :blk_key key;
        };
        var record_value: [33]u8 = undefined;
        record_value[0] = @intFromEnum(transactions_mod.TxnStatus.committed);
        std.mem.writeInt(u64, record_value[1..9], 1_000, .little);
        std.mem.writeInt(u64, record_value[9..17], 2_000, .little);
        std.mem.writeInt(u64, record_value[17..25], 1_000, .little);
        std.mem.writeInt(u64, record_value[25..33], 2_000, .little);
        try setup_db.core.store.put(record_key[0..], record_value[0..]);
        break :blk txn_id;
    };

    var recorder = TxnResolverRecorder{};
    const server_recovery_config_4: server_recovery.Config = .{
        .enabled = true,
        .interval_ms = 10,
        .cutoff_ns = 1,
        .resolver_ctx = &recorder,
        .resolve_participant_fn = TxnResolverRecorder.resolve,
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .transaction_recovery = server_recovery.borrowedConfig(&server_recovery_config_4),
    });
    defer db.close();

    // Do not call a DB wrapper method while waiting. Recovery must be able to
    // resolve the orphan from its stable heap owner without a caller first
    // publishing the address of this by-value handle.
    const recovered_store_key = try encodeStoreLookupKeyAlloc(&db, alloc, "doc:recovered_orphan");
    defer alloc.free(recovered_store_key);
    var recovered_without_api_call = false;
    var attempts: usize = 0;
    while (attempts < 500) : (attempts += 1) {
        const raw = try db.core.getStoreValue(alloc, recovered_store_key);
        if (raw) |value| {
            alloc.free(value);
            recovered_without_api_call = true;
            break;
        }
        sleepPollInterval();
    }
    if (!recovered_without_api_call) return error.TransactionRecoveryLocalResolutionTimeout;

    var cleaned = false;
    attempts = 0;
    while (attempts < 500) : (attempts += 1) {
        const status = db.core.getTransactionStatus(txn_id);
        if (status) |_| {} else |err| {
            if (err == transactions_mod.TxnError.TxnNotFound) {
                cleaned = true;
                break;
            }
            return err;
        }
        sleepPollInterval();
    }
    if (!cleaned) return error.TransactionRecoveryCleanupTimeout;

    const raw = (try db.get(alloc, "doc:recovered_orphan")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"recovered\"}", raw);
    try std.testing.expectEqual(@as(?u64, 1), try range_cardinality.load(alloc, db.core.store));

    const stats = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.state_rows);
    try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.live_ordinals);
    try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.primary_docs_missing_ordinals);
    try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.primary_docs_missing_identity_state);

    try db.waitForCurrentSyncLevel(.full_index);
    var result = try db.search(alloc, .{
        .index_name = "ft_recovered_txn",
        .query = .{ .match = .{ .field = "_all", .text = "recovered" } },
        .limit = 10,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:recovered_orphan", result.hits[0].id);
}

test "db transaction recovery shares serving visibility and invalidates query caches" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var recorder = TxnResolverRecorder{};
    const server_recovery_config_5: server_recovery.Config = .{
        .enabled = true,
        .resolver_ctx = &recorder,
        .resolve_participant_fn = TxnResolverRecorder.resolve,
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .transaction_recovery = server_recovery.borrowedConfig(&server_recovery_config_5),
    });
    defer db.close();
    try engine.test_support.prepareTransactionRecoveryOwner(engine.test_support.dbPointer(&db));
    const recovery = db.transaction_recovery_local_context.?.execution.?;
    try std.testing.expect(&db.core.identity_visibility == &recovery.core.identity_visibility);
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }} });
    try std.testing.expect(try engine.test_support.allDocsVisibleSummaryFast(engine.test_support.dbPointer(&db), null));
    const generation = try engine.test_support.currentIdentityReadGeneration(engine.test_support.dbPointer(&db));
    var live = try engine.test_support.broadLiveDocSetCachedAlloc(engine.test_support.dbPointer(&db), alloc, generation);
    defer live.deinit(alloc);
    var hidden = (try engine.test_support.nonVisibleDocSetCachedAlloc(engine.test_support.dbPointer(&db), alloc, generation)).?;
    defer hidden.deinit(alloc);
    try std.testing.expect(db.core.identity_visibility.live_generation != null);
    try std.testing.expect(db.core.identity_visibility.nonvisible_generation != null);

    const txn_id = try db.beginTransaction(1_000);
    try db.writeTransaction(txn_id, .{ .deletes = &.{"doc:a"} });
    const config = server_recovery.test_support.runtimeConfig(db.transaction_runtime.?);
    try config.resolve_local_fn.?(config.local_resolution_ctx.?, txn_id, .committed, 2_000);
    const stored = try db.get(alloc, "doc:a");
    if (stored) |value| alloc.free(value);
    try std.testing.expect(stored == null);
    const durable = (try doc_identity.visibilitySummaryFromStore(db.core.store)).?;
    try std.testing.expectEqual(@as(u64, 0), durable.live_ordinals);
    try std.testing.expectEqualDeep(durable, db.core.identity_visibility.summary.?);
    try std.testing.expect(!try engine.test_support.allDocsVisibleSummaryFast(engine.test_support.dbPointer(&db), null));
    try std.testing.expect(db.core.identity_visibility.live_generation == null);
    try std.testing.expect(db.core.identity_visibility.nonvisible_generation == null);

    // Serving writes also replace the recovery view, rather than leaving a
    // second cached summary that can later mask the next recovered mutation.
    try db.batch(.{ .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }} });
    try std.testing.expectEqual(@as(u64, 1), recovery.core.identity_visibility.summary.?.live_ordinals);
}

test "db transaction recovery borrows replacement enrichment only during resolution" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var recorder = TxnResolverRecorder{};
    var initial = embedder_mod.DeterministicDenseEmbedder{};
    const server_recovery_config_6: server_recovery.Config = .{
        .enabled = true,
        .resolver_ctx = &recorder,
        .resolve_participant_fn = TxnResolverRecorder.resolve,
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{ .dense_embedder = initial.interface() },
        .transaction_recovery = server_recovery.borrowedConfig(&server_recovery_config_6),
    });
    defer db.close();
    try db.addIndex(.{ .name = "ft_recovery", .kind = .full_text, .config_json = "{}" });
    try engine.test_support.prepareTransactionRecoveryOwner(engine.test_support.dbPointer(&db));
    const recovery = db.transaction_recovery_local_context.?.execution.?;
    try std.testing.expectEqual(db.enrichment_runtime, recovery.async_context.enrichment_runtime);
    const original = db.enrichment_runtime.?;
    var replacement = embedder_mod.DeterministicDenseEmbedder{};
    try db.reconfigureEnrichmentRuntimePaused(.{ .dense_embedder = replacement.interface() });
    try std.testing.expect(db.enrichment_runtime.? != original);
    try std.testing.expectEqual(db.enrichment_runtime, recovery.async_context.enrichment_runtime);
    const config = server_recovery.test_support.runtimeConfig(db.transaction_runtime.?);
    const txn_id = try db.beginTransaction(1_000);
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"recovered\"}" }},
    });
    try config.resolve_local_fn.?(config.local_resolution_ctx.?, txn_id, .committed, 2_000);
    try std.testing.expectEqual(db.enrichment_runtime, recovery.async_context.enrichment_runtime);
    const sequence = db.core.nextDerivedSequence();
    try std.testing.expect(sequence > 0);
    try std.testing.expectEqual(sequence, db.enrichment_runtime.?.stats().target_sequence);
    try db.waitForCurrentSyncLevel(.full_text);
    var result = try db.search(alloc, .{
        .index_name = "ft_recovery",
        .query = .{ .match = .{ .field = "_all", .text = "recovered" } },
        .limit = 10,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);

    // Removing the producer must also leave no dangling pointer behind.
    try db.reconfigureEnrichmentRuntimePaused(.{});
    try std.testing.expect(db.enrichment_runtime == null);
    const deleted = try db.beginTransaction(3_000);
    try db.writeTransaction(deleted, .{ .deletes = &.{"doc:a"} });
    try config.resolve_local_fn.?(config.local_resolution_ctx.?, deleted, .committed, 4_000);
    try std.testing.expectEqual(db.enrichment_runtime, recovery.async_context.enrichment_runtime);
}

test "db transaction recovery provider guard survives failed enrichment replacement" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var recorder = TxnResolverRecorder{};
    var initial = embedder_mod.DeterministicDenseEmbedder{};
    const server_recovery_config_7: server_recovery.Config = .{
        .enabled = true,
        .resolver_ctx = &recorder,
        .resolve_participant_fn = TxnResolverRecorder.resolve,
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{ .dense_embedder = initial.interface() },
        .transaction_recovery = server_recovery.borrowedConfig(&server_recovery_config_7),
    });
    defer db.close();
    try engine.test_support.prepareTransactionRecoveryOwner(engine.test_support.dbPointer(&db));
    const original = db.enrichment_runtime.?;
    const recovery = db.transaction_recovery_local_context.?;
    const Fault = struct {
        fn afterDetached(target: *DB) !void {
            const context = target.transaction_recovery_local_context.?;
            if (context.provider_mutex.tryLock()) {
                context.provider_mutex.unlock(target.backend_runtime.io().?);
                return error.TestMissingRecoveryProviderGuard;
            }
            return error.TestReplacementInterrupted;
        }
    };
    engine.test_support.enrichmentReconfigureHook.* = Fault.afterDetached;
    defer engine.test_support.enrichmentReconfigureHook.* = null;
    var replacement = embedder_mod.DeterministicDenseEmbedder{};
    try std.testing.expectError(error.TestReplacementInterrupted, db.reconfigureEnrichmentRuntimePaused(.{
        .dense_embedder = replacement.interface(),
    }));
    try std.testing.expectEqual(original, db.enrichment_runtime.?);
    try std.testing.expect(recovery.provider_mutex.tryLock());
    recovery.provider_mutex.unlock(db.backend_runtime.io().?);
    try std.testing.expectEqual(db.enrichment_runtime, recovery.execution.?.async_context.enrichment_runtime);
    const config = server_recovery.test_support.runtimeConfig(db.transaction_runtime.?);
    const txn_id = try db.beginTransaction(1_000);
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"after failure\"}" }},
    });
    try config.resolve_local_fn.?(config.local_resolution_ctx.?, txn_id, .committed, 2_000);
    try std.testing.expectEqual(db.enrichment_runtime, recovery.execution.?.async_context.enrichment_runtime);
    try std.testing.expect(recovery.provider_mutex.tryLock());
    recovery.provider_mutex.unlock(db.backend_runtime.io().?);
}

test "db transaction recovery borrowed execution observes split shadow lifetime" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var recorder = TxnResolverRecorder{};
    const server_recovery_config_8: server_recovery.Config = .{
        .enabled = true,
        .resolver_ctx = &recorder,
        .resolve_participant_fn = TxnResolverRecorder.resolve,
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .transaction_recovery = server_recovery.borrowedConfig(&server_recovery_config_8),
    });
    defer db.close();
    try engine.test_support.prepareTransactionRecoveryOwner(engine.test_support.dbPointer(&db));
    const recovery_ctx = db.transaction_recovery_local_context orelse
        return error.TransactionRecoveryOwnerUnbound;
    try std.testing.expect(recovery_ctx.execution != null);
    var recovery_execution = engine.test_support.transactionRecoveryExecution(&db);
    try std.testing.expect(activeSplitShadow(&recovery_execution) == null);

    try db.addIndex(.{
        .name = "ft_split_recovery",
        .kind = .full_text,
        .config_json = "{\"field\":\"title\"}",
    });
    try db.createShadowIndexManager("doc:m", "");
    try std.testing.expect(recovery_ctx.split_shadow != null);
    try std.testing.expect(recovery_ctx.split_shadow.?.manager == db.shadow.?.manager);
    try std.testing.expect(activeSplitShadow(&recovery_execution) == db.shadow.?);
    // Ticket counters and synchronization must have one address even though
    // the execution view was captured before the split began.
    try std.testing.expect(&activeSplitShadow(&recovery_execution).?.next_ticket == &db.shadow.?.next_ticket);

    try db.closeShadowIndexManager();
    try std.testing.expect(recovery_ctx.split_shadow == null);
    try std.testing.expect(activeSplitShadow(&recovery_execution) == null);
}

test "db merge receiver fences stale copies and retains retired transitions across reopen" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    const first: types.MergeReplicationCheckpoint = .{
        .kind = .accept,
        .transition_id = 100,
        .donor_group_id = 101,
        .receiver_group_id = 102,
        .receiver_base_start = "m",
        .receiver_base_end = "z",
        .merged_start = "a",
        .merged_end = "z",
    };
    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();
        try db.updateRange(.{ .start = "m", .end = "z" });
        const copy: types.MergeReplicationContext = .{
            .transition_id = 100,
            .donor_group_id = 101,
            .receiver_group_id = 102,
            .identity_namespace = db.core.identity_namespace,
        };
        const payload: types.BatchRequest = .{ .merge_replication = copy, .writes = &.{.{ .key = "b", .value = "{}" }} };
        try server_test_adapter.applyOrdered(&db, payload, .{ .term = 1, .index = 1 });
        try std.testing.expect((try db.get(alloc, "b")) == null);
        try server_test_adapter.applyOrdered(&db, .{ .merge_checkpoint = first }, .{ .term = 1, .index = 2 });
        try server_test_adapter.applyOrdered(&db, payload, .{ .term = 1, .index = 3 });
        var terminal = first;
        terminal.kind = .bootstrap_complete;
        terminal.bootstrap_applied_index = 3;
        try server_test_adapter.applyOrdered(&db, .{ .merge_checkpoint = terminal }, .{ .term = 1, .index = 4 });
        terminal.kind = .finalize;
        try server_test_adapter.applyOrdered(&db, .{ .merge_checkpoint = terminal }, .{ .term = 1, .index = 5 });
        try db.batch(.{ .writes = &.{.{ .key = "b", .value = "{\"public\":true}" }} });
        const before = db.core.nextDerivedSequence();
        try server_test_adapter.applyOrdered(&db, payload, .{ .term = 2, .index = 6 });
        try server_test_adapter.applyOrdered(&db, .{ .merge_replication = copy, .deletes = &.{"b"} }, .{ .term = 2, .index = 7 });
        // A stale artifact must be ignored before its payload is decoded.
        const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "b", "graph", "links", "c");
        defer alloc.free(artifact_key);
        try server_test_adapter.applyOrdered(&db, .{ .merge_replication = copy, .merge_artifacts = &.{.{ .key = artifact_key, .value = "invalid stale artifact" }} }, .{ .term = 2, .index = 8 });
        try std.testing.expect((try db.core.getStoreValue(alloc, artifact_key)) == null);
        try std.testing.expectEqual(before, db.core.nextDerivedSequence());
        try std.testing.expectEqual(@as(u64, 8), (try db.orderedApplyReceipt()).?.index);
        const value = (try db.get(alloc, "b")).?;
        defer alloc.free(value);
        try std.testing.expectEqualStrings("{\"public\":true}", value);
        var second = first;
        second.transition_id = 200;
        second.donor_group_id = 201;
        second.receiver_base_start = "a";
        second.merged_start = "";
        try server_test_adapter.applyOrdered(&db, .{ .merge_checkpoint = second }, .{ .term = 2, .index = 9 });
        try server_test_adapter.applyOrdered(&db, payload, .{ .term = 2, .index = 10 });
        second.kind = .bootstrap_complete;
        second.bootstrap_applied_index = 10;
        try server_test_adapter.applyOrdered(&db, .{ .merge_checkpoint = second }, .{ .term = 2, .index = 11 });
        second.kind = .finalize;
        try server_test_adapter.applyOrdered(&db, .{ .merge_checkpoint = second }, .{ .term = 2, .index = 12 });
    }
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();
    try server_test_adapter.applyOrdered(&db, .{ .merge_checkpoint = first }, .{ .term = 3, .index = 13 });
    const raw = (try db.core.getStoreValue(alloc, merge_state_mod.key)).?;
    defer alloc.free(raw);
    var state = try merge_state_mod.decodeAlloc(alloc, raw);
    defer state.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 200), state.transition_id);
    try std.testing.expectEqual(merge_state_mod.Phase.finalized, state.phase);
    try std.testing.expectEqualSlices(u64, &.{100}, state.retired_transition_ids);
    try std.testing.expectEqualStrings("", db.getRange().start);
    const value = (try db.get(alloc, "b")).?;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("{\"public\":true}", value);
}

test "db merge copy attempts fence delayed leaders before finalize across reopen" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var checkpoint: types.MergeReplicationCheckpoint = .{
        .kind = .accept,
        .transition_id = 100,
        .donor_group_id = 101,
        .receiver_group_id = 102,
        .receiver_base_start = "m",
        .receiver_base_end = "z",
        .merged_start = "a",
        .merged_end = "z",
    };
    const Apply = struct {
        fn command(db: *DB, index: *u64, req: types.BatchRequest) !void {
            index.* += 1;
            try server_test_adapter.applyOrdered(&db, req, .{ .term = 7, .index = index.* });
        }
    };
    var index: u64 = 0;
    var old_copy: types.MergeReplicationContext = undefined;
    var old_begin = checkpoint;
    old_begin.kind = .begin_copy;
    old_begin.copy_attempt = .{ .donor_term = 1, .sequence = 100 };
    var new_begin = old_begin;
    // Term must dominate sequence, even if the successor just restarted.
    new_begin.copy_attempt = .{ .donor_term = 2, .sequence = 1 };
    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();
        try db.updateRange(.{ .start = "m", .end = "z" });
        old_copy = .{
            .transition_id = 100,
            .donor_group_id = 101,
            .receiver_group_id = 102,
            .identity_namespace = db.core.identity_namespace,
            .copy_attempt = old_begin.copy_attempt,
        };
        try Apply.command(&db, &index, .{ .merge_checkpoint = checkpoint });
        try Apply.command(&db, &index, .{ .merge_checkpoint = old_begin });
        try Apply.command(&db, &index, .{ .merge_replication = old_copy, .writes = &.{.{ .key = "b", .value = "{}" }} });
        try Apply.command(&db, &index, .{ .merge_checkpoint = new_begin });
        var new_copy = old_copy;
        new_copy.copy_attempt = new_begin.copy_attempt;
        try Apply.command(&db, &index, .{ .merge_replication = new_copy, .writes = &.{.{ .key = "b", .value = "{\"new\":true}" }} });
        const count_before = (try range_cardinality.loadOrProveEmpty(alloc, db.core.store)).?;
        const cardinality_key = @import("db/merge_cardinality.zig").key;
        const cardinality_before = (try db.core.getStoreValue(alloc, cardinality_key)).?;
        defer alloc.free(cardinality_before);
        // A delayed begin cannot take ownership back. Nor can its completion
        // or finalize certify B's still-incomplete copy.
        try Apply.command(&db, &index, .{ .merge_checkpoint = old_begin });
        var stale = old_begin;
        stale.kind = .bootstrap_complete;
        stale.bootstrap_applied_index = 900;
        try Apply.command(&db, &index, .{ .merge_checkpoint = stale });
        stale.kind = .finalize;
        try Apply.command(&db, &index, .{ .merge_checkpoint = stale });
        stale.kind = .rollback;
        stale.bootstrap_applied_index = 0;
        try Apply.command(&db, &index, .{ .merge_checkpoint = stale });
        try Apply.command(&db, &index, .{ .merge_replication = old_copy, .deletes = &.{"b"} });
        try std.testing.expectEqual(count_before, (try range_cardinality.loadOrProveEmpty(alloc, db.core.store)).?);
        const cardinality_after = (try db.core.getStoreValue(alloc, cardinality_key)).?;
        defer alloc.free(cardinality_after);
        try std.testing.expectEqualSlices(u8, cardinality_before, cardinality_after);
        const raw = (try db.core.getStoreValue(alloc, merge_state_mod.key)).?;
        defer alloc.free(raw);
        var state = try merge_state_mod.decodeAlloc(alloc, raw);
        defer state.deinit(alloc);
        try std.testing.expectEqual(merge_state_mod.Phase.accepting, state.phase);
        try std.testing.expect(!state.bootstrap_complete);
        try std.testing.expectEqual(std.math.Order.eq, state.copy_attempt.order(new_begin.copy_attempt));
    }
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();
    checkpoint = new_begin;
    checkpoint.kind = .bootstrap_complete;
    checkpoint.bootstrap_applied_index = 20;
    try Apply.command(&db, &index, .{ .merge_checkpoint = checkpoint });
    const before = db.core.nextDerivedSequence();
    // This is the reported window: B completed bootstrap, but has not yet
    // finalized, when A's delayed clear and artifact page are delivered.
    try Apply.command(&db, &index, .{ .merge_replication = old_copy, .deletes = &.{"b"} });
    const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "b", "graph", "links", "c");
    defer alloc.free(artifact_key);
    try Apply.command(&db, &index, .{ .merge_replication = old_copy, .merge_artifacts = &.{.{ .key = artifact_key, .value = "invalid stale artifact" }} });
    // Completion also closes the winning attempt against duplicate packets.
    var completed_copy = old_copy;
    completed_copy.copy_attempt = checkpoint.copy_attempt;
    try Apply.command(&db, &index, .{ .merge_replication = completed_copy, .deletes = &.{"b"} });
    try std.testing.expectEqual(before, db.core.nextDerivedSequence());
    try std.testing.expectEqual(index, (try db.orderedApplyReceipt()).?.index);
    try std.testing.expect((try db.core.getStoreValue(alloc, artifact_key)) == null);
    checkpoint.kind = .finalize;
    try Apply.command(&db, &index, .{ .merge_checkpoint = checkpoint });
    const value = (try db.get(alloc, "b")).?;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("{\"new\":true}", value);
    const raw = (try db.core.getStoreValue(alloc, merge_state_mod.key)).?;
    defer alloc.free(raw);
    var state = try merge_state_mod.decodeAlloc(alloc, raw);
    defer state.deinit(alloc);
    try std.testing.expectEqual(merge_state_mod.Phase.finalized, state.phase);
    try std.testing.expectEqual(@as(u64, 20), state.bootstrap_applied_index);
}

test "db replicated merge checkpoints keep rolled back receivers live across delayed controls and reopen" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var checkpoint: types.MergeReplicationCheckpoint = .{
        .kind = .accept,
        .transition_id = 50,
        .donor_group_id = 51,
        .receiver_group_id = 52,
        .receiver_base_start = "m",
        .receiver_base_end = "z",
        .merged_start = "a",
        .merged_end = "z",
    };
    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();
        try db.updateRange(.{ .start = "m", .end = "z" });
        try server_test_adapter.applyOrdered(&db, .{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 1 });
        checkpoint.kind = .rollback;
        try server_test_adapter.applyOrdered(&db, .{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 2 });
    }
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();
    for ([_]types.MergeReplicationCheckpoint.Kind{ .accept, .begin_copy, .bootstrap_complete, .finalize, .rollback }, 3..) |kind, index| {
        checkpoint.kind = kind;
        checkpoint.copy_attempt = .{ .donor_term = 2, .sequence = 1 };
        checkpoint.bootstrap_applied_index = if (kind == .bootstrap_complete or kind == .finalize) 100 else 0;
        try server_test_adapter.applyOrdered(&db, .{ .merge_checkpoint = checkpoint }, .{ .term = 2, .index = index });
        try std.testing.expectEqualStrings("m", db.getRange().start);
    }
    try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "n", .value = "{\"live\":true}" }} }, .{ .term = 2, .index = 8 });
    const raw = (try db.core.getStoreValue(alloc, merge_state_mod.key)).?;
    defer alloc.free(raw);
    var state = try merge_state_mod.decodeAlloc(alloc, raw);
    defer state.deinit(alloc);
    try std.testing.expectEqual(merge_state_mod.Phase.rolled_back, state.phase);
    try std.testing.expect(!state.bootstrap_complete);
    try std.testing.expectEqual(@as(u64, 0), state.bootstrap_applied_index);
    try std.testing.expectEqual(@as(u64, 8), (try db.orderedApplyReceipt()).?.index);
    const value = (try db.get(alloc, "n")).?;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("{\"live\":true}", value);
}

test "db terminal merge controls preserve a subsequent split across reopen" {
    const alloc = std.testing.allocator;
    for ([_]types.MergeReplicationCheckpoint.Kind{ .finalize, .rollback }) |terminal| {
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        var checkpoint: types.MergeReplicationCheckpoint = .{
            .kind = .accept,
            .transition_id = 60,
            .donor_group_id = 61,
            .receiver_group_id = 62,
            .receiver_base_start = "m",
            .receiver_base_end = "z",
            .merged_start = "a",
            .merged_end = "z",
        };
        const expected_start = if (terminal == .finalize) "a" else "m";
        {
            var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
            defer db.close();
            try db.updateRange(.{ .start = "m", .end = "z" });
            try server_test_adapter.applyOrdered(&db, .{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 1 });
            checkpoint.kind = .bootstrap_complete;
            checkpoint.bootstrap_applied_index = 1;
            try server_test_adapter.applyOrdered(&db, .{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 2 });
            checkpoint.kind = terminal;
            checkpoint.bootstrap_applied_index = if (terminal == .finalize) 1 else 0;
            try server_test_adapter.applyOrdered(&db, .{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 3 });
            // Exercise the production split-start mutation, including its
            // persisted range, without replacing the terminal merge receipt.
            try db.core.prepareSplit("t");
            try db.core.completeSplitTransition(63, "t");
            checkpoint.kind = .accept;
            checkpoint.bootstrap_applied_index = 0;
            try server_test_adapter.applyOrdered(&db, .{ .merge_checkpoint = checkpoint }, .{ .term = 2, .index = 4 });
            try std.testing.expectEqualStrings(expected_start, db.getRange().start);
            try std.testing.expectEqualStrings("t", db.getRange().end);
        }
        {
            var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
            defer db.close();
            for ([_]types.MergeReplicationCheckpoint.Kind{ .accept, .begin_copy, .bootstrap_complete, .finalize, .rollback }, 5..) |kind, index| {
                checkpoint.kind = kind;
                checkpoint.copy_attempt = .{ .donor_term = 2, .sequence = 1 };
                checkpoint.bootstrap_applied_index = if (kind == .bootstrap_complete or kind == .finalize) 100 else 0;
                try server_test_adapter.applyOrdered(&db, .{ .merge_checkpoint = checkpoint }, .{ .term = 2, .index = index });
                try std.testing.expectEqualStrings(expected_start, db.getRange().start);
                try std.testing.expectEqualStrings("t", db.getRange().end);
            }
            var conflicting = checkpoint;
            conflicting.donor_group_id = 99;
            try std.testing.expectError(error.ConflictingMergeTransition, server_test_adapter.applyOrdered(
                &db,
                .{ .merge_checkpoint = conflicting },
                .{ .term = 2, .index = 10 },
            ));
            conflicting = checkpoint;
            conflicting.merged_end = "zz";
            try std.testing.expectError(error.ConflictingMergeTransition, server_test_adapter.applyOrdered(
                &db,
                .{ .merge_checkpoint = conflicting },
                .{ .term = 2, .index = 10 },
            ));
            try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "n", .value = "{\"live\":true}" }} }, .{ .term = 2, .index = 10 });
            const raw = (try db.core.getStoreValue(alloc, merge_state_mod.key)).?;
            defer alloc.free(raw);
            var state = try merge_state_mod.decodeAlloc(alloc, raw);
            defer state.deinit(alloc);
            try std.testing.expectEqual(if (terminal == .finalize) merge_state_mod.Phase.finalized else .rolled_back, state.phase);
            try std.testing.expectEqual(terminal == .finalize, state.bootstrap_complete);
            try std.testing.expectEqual(@as(u64, if (terminal == .finalize) 1 else 0), state.bootstrap_applied_index);
            try std.testing.expectEqualStrings("z", state.receiver_base_range.end);
            try std.testing.expectEqualStrings("z", state.merged_range.?.end);
            try std.testing.expectEqual(@as(u64, 10), (try db.orderedApplyReceipt()).?.index);
            try std.testing.expectEqual(shard_mod.SplitPhase.splitting, db.core.splitState().?.phase);
        }
        var reopened = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer reopened.close();
        try std.testing.expectEqualStrings(expected_start, reopened.getRange().start);
        try std.testing.expectEqualStrings("t", reopened.getRange().end);
        const value = (try reopened.get(alloc, "n")).?;
        defer alloc.free(value);
        try std.testing.expectEqualStrings("{\"live\":true}", value);
        try std.testing.expectEqual(@as(u64, 10), (try reopened.orderedApplyReceipt()).?.index);
    }
}

test "db physical lsm split retains parent merge receipts and clears child receipts across reopen" {
    const alloc = std.testing.allocator;
    const options: OpenOptions = .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .start_index_workers = false,
    };
    for ([_]types.MergeReplicationCheckpoint.Kind{ .finalize, .rollback }) |terminal| {
        for ([_]bool{ false, true }) |old_key_layout| {
            var parent_path_tmp = try TestDirectory.init("db");
            defer parent_path_tmp.cleanup();
            const parent_path = parent_path_tmp.path().ptr;
            var child_path_tmp = try TestDirectory.init("db");
            defer child_path_tmp.cleanup();
            const child_path = child_path_tmp.path().ptr;
            var checkpoint: types.MergeReplicationCheckpoint = .{
                .kind = .accept,
                .transition_id = 70,
                .donor_group_id = 71,
                .receiver_group_id = 72,
                .receiver_base_start = "a",
                .receiver_base_end = "m",
                .merged_start = "a",
                .merged_end = "z",
            };
            const split_key = if (terminal == .finalize) "m" else "g";
            const right_key = if (terminal == .finalize) "t" else "j";
            {
                var parent = try DB.open(alloc, std.mem.span(parent_path), options);
                defer parent.close();
                try parent.updateRange(.{ .start = "a", .end = "m" });
                try server_test_adapter.applyOrdered(&parent, .{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 1 });
                checkpoint.kind = .rollback;
                try server_test_adapter.applyOrdered(&parent, .{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 2 });
                checkpoint.kind = .accept;
                checkpoint.transition_id = 80;
                checkpoint.donor_group_id = 81;
                try server_test_adapter.applyOrdered(&parent, .{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 3 });
                checkpoint.kind = .begin_copy;
                checkpoint.copy_attempt = .{ .donor_term = 1, .sequence = 1 };
                try server_test_adapter.applyOrdered(&parent, .{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 4 });
                checkpoint.kind = .bootstrap_complete;
                checkpoint.bootstrap_applied_index = 4;
                try server_test_adapter.applyOrdered(&parent, .{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 5 });
                checkpoint.kind = terminal;
                checkpoint.bootstrap_applied_index = if (terminal == .finalize) 4 else 0;
                try server_test_adapter.applyOrdered(&parent, .{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 6 });
                try server_test_adapter.applyOrdered(&parent, .{ .writes = &.{
                    .{ .key = "b", .value = "{\"side\":\"parent\"}" },
                    .{ .key = right_key, .value = "{\"side\":\"child\"}" },
                } }, .{ .term = 1, .index = 7 });
                const before = (try parent.core.getStoreValue(alloc, merge_state_mod.key)).?;
                defer alloc.free(before);
                // Production records predating the protected metadata key
                // must be promoted before a physical split can discard them.
                if (old_key_layout and !std.mem.eql(u8, merge_state_mod.key, "raftmerge:state")) {
                    try parent.core.store.putBatch(&.{.{ .key = "raftmerge:state", .value = before }}, &.{merge_state_mod.key});
                }
                try parent.split(parent.getRange(), split_key, "", std.mem.span(child_path), true);
                const prepared_receipt = (try parent.core.getStoreValue(alloc, merge_state_mod.key)) orelse return error.MissingMergeReceiptAfterPrepare;
                defer alloc.free(prepared_receipt);
                try std.testing.expectEqualSlices(u8, before, prepared_receipt);
                // Inspect the destructive rewrite boundary itself: receipt
                // preservation must not rely on a later restoration write.
                const split_lower = try documentRangeLowerAlloc(alloc, split_key);
                defer alloc.free(split_lower);
                _ = try tryFinalizePrimarySplitFast(&parent, split_lower);
                const rewritten_receipt = (try parent.core.getStoreValue(alloc, merge_state_mod.key)) orelse return error.MissingMergeReceiptAfterPhysicalRewrite;
                defer alloc.free(rewritten_receipt);
                try std.testing.expectEqualSlices(u8, before, rewritten_receipt);
                try parent.finalizeSplit(.{ .start = "a", .end = split_key });
                const after = (try parent.core.getStoreValue(alloc, merge_state_mod.key)) orelse return error.MissingMergeReceiptAfterSplit;
                defer alloc.free(after);
                try std.testing.expectEqualSlices(u8, before, after);
                try std.testing.expect((try parent.get(alloc, right_key)) == null);
            }
            {
                var parent = try DB.open(alloc, std.mem.span(parent_path), options);
                defer parent.close();
                for ([_]types.MergeReplicationCheckpoint.Kind{ .accept, .begin_copy, .bootstrap_complete, .finalize, .rollback }, 8..) |kind, index| {
                    checkpoint.kind = kind;
                    checkpoint.copy_attempt = .{ .donor_term = 2, .sequence = 1 };
                    checkpoint.bootstrap_applied_index = if (kind == .bootstrap_complete or kind == .finalize) 100 else 0;
                    try server_test_adapter.applyOrdered(&parent, .{ .merge_checkpoint = checkpoint }, .{ .term = 2, .index = index });
                    try std.testing.expectEqualStrings("a", parent.getRange().start);
                    try std.testing.expectEqualStrings(split_key, parent.getRange().end);
                }
                var retired = checkpoint;
                retired.kind = .accept;
                retired.transition_id = 70;
                retired.donor_group_id = 71;
                retired.bootstrap_applied_index = 0;
                try server_test_adapter.applyOrdered(&parent, .{ .merge_checkpoint = retired }, .{ .term = 2, .index = 13 });
                try server_test_adapter.applyOrdered(&parent, .{ .writes = &.{.{ .key = "b", .value = "{\"live\":true}" }} }, .{ .term = 2, .index = 14 });
                const raw = (try parent.core.getStoreValue(alloc, merge_state_mod.key)).?;
                defer alloc.free(raw);
                var receipt = try merge_state_mod.decodeAlloc(alloc, raw);
                defer receipt.deinit(alloc);
                try std.testing.expectEqual(if (terminal == .finalize) merge_state_mod.Phase.finalized else .rolled_back, receipt.phase);
                try std.testing.expectEqual(@as(u64, 80), receipt.transition_id);
                try std.testing.expectEqualSlices(u64, &.{70}, receipt.retired_transition_ids);
                try std.testing.expectEqual(std.math.Order.eq, receipt.copy_attempt.order(.{ .donor_term = 1, .sequence = 1 }));
                try std.testing.expectEqual(@as(u64, if (terminal == .finalize) 4 else 0), receipt.bootstrap_applied_index);
                try std.testing.expectEqual(@as(u64, 14), (try parent.orderedApplyReceipt()).?.index);
            }
            {
                var child = try DB.open(alloc, std.mem.span(child_path), options);
                defer child.close();
                try std.testing.expect((try child.core.getStoreValue(alloc, merge_state_mod.key)) == null);
                try std.testing.expect((try child.core.getStoreValue(alloc, "raftmerge:state")) == null);
                const value = (try child.get(alloc, right_key)).?;
                defer alloc.free(value);
                try std.testing.expectEqualStrings("{\"side\":\"child\"}", value);
                var fresh = checkpoint;
                fresh.kind = .accept;
                fresh.transition_id = 90;
                fresh.donor_group_id = 91;
                fresh.receiver_group_id = 92;
                fresh.receiver_base_start = split_key;
                fresh.receiver_base_end = if (terminal == .finalize) "z" else "m";
                fresh.merged_start = "a";
                fresh.merged_end = fresh.receiver_base_end;
                fresh.bootstrap_applied_index = 0;
                fresh.copy_attempt = .{};
                try server_test_adapter.applyOrdered(&child, .{ .merge_checkpoint = fresh }, .{ .term = 1, .index = 1 });
                try std.testing.expectEqual(@as(u64, 1), (try child.orderedApplyReceipt()).?.index);
            }
            var reopened = try DB.open(alloc, std.mem.span(parent_path), options);
            defer reopened.close();
            try std.testing.expectEqualStrings(split_key, reopened.getRange().end);
            const value = (try reopened.get(alloc, "b")).?;
            defer alloc.free(value);
            try std.testing.expectEqualStrings("{\"live\":true}", value);
        }
    }
}

test "db scoped native restore imports cached artifacts before rows across restart" {
    try testScopedNativeArtifactRestore(false, false, false);
    try testScopedNativeArtifactRestore(true, false, false);
}

test "db scoped restore projects regenerated target vectors instead of archived values" {
    try testScopedNativeArtifactRestore(false, true, false);
}

test "db scoped restore rejects unverified generated vectors without a producer runtime" {
    try testScopedNativeArtifactRestore(false, false, true);
}

fn testScopedNativeArtifactRestore(standby: bool, replace_generated: bool, unverified_generated: bool) !void {
    const alloc = std.testing.allocator;
    const staging = @import("db/restore_staging.zig");
    var source_tmp = try TestDirectory.init("native-artifact-source");
    defer source_tmp.cleanup();
    var target_tmp = try TestDirectory.init("native-artifact-target");
    defer target_tmp.cleanup();
    const source_path = std.mem.span(source_tmp.path().ptr);
    const target_path = std.mem.span(target_tmp.path().ptr);
    var counting = CountingDenseEmbedder{};
    var preserved_indexes: []types.IndexConfig = &.{};
    defer types.freeIndexConfigs(alloc, preserved_indexes);
    var source_graph_generation: u64 = 0;
    const source_options: OpenOptions = .{
        .identity_namespace = .{ .table_id = 101, .shard_id = 102, .range_id = 102 },
        .primary_backend = .{ .lsm = .{} },
        .start_index_workers = false,
        .enrichment = .{ .owner_id = "artifact-source", .dense_embedder = counting.interface() },
    };
    const index: types.IndexConfig = .{
        .name = "semantic",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"chunks\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"vectors\"}}",
    };
    const whole_index: types.IndexConfig = .{
        .name = "whole_document",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"whole_vector\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"whole_vector\"}}",
    };
    const explicit_index: types.IndexConfig = .{ .name = "explicit", .kind = .dense_vector, .config_json = "{\"field\":\"explicit_vector\",\"dims\":3}" };
    const graph_index: types.IndexConfig = .{ .name = "links", .kind = .graph, .config_json = "{}" };
    const text_index: types.IndexConfig = .{ .name = "full_text", .kind = .full_text, .config_json = "{}" };
    {
        var source = try DB.open(alloc, source_path, source_options);
        defer source.close();
        try source.addIndex(index);
        try source.addIndex(whole_index);
        try source.addIndex(explicit_index);
        try source.addIndex(graph_index);
        try source.addIndex(text_index);
        try source.batch(.{
            .writes = &.{
                .{ .key = "doc", .value = "{\"body\":\"abcdefghijklmno\",\"_embeddings\":{\"explicit\":[1,0,0]}}" },
                .{ .key = "other", .value = "{\"body\":\"abcdefghijklmno\"}" },
            },
            .graph_writes = &.{.{ .index_name = "links", .source = "doc", .target = "other", .edge_type = "related", .weight = 1.0 }},
            .sync_level = .full_index,
        });
        try source.runUntilIdle();
        if (unverified_generated) {
            // Portable archives carry logical vector values, not native
            // producer fingerprints. They cannot satisfy managed coverage.
            const value = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &.{ 1, 0, 0 });
            defer alloc.free(value);
            for ([_][]const u8{ "doc", "other" }) |key| {
                const artifact = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, key, "whole_vector");
                defer alloc.free(artifact);
                try source.core.store.put(artifact, value);
            }
        }
        // Native manifests retain each committed index incarnation, not its
        // original zero-generation creation request.
        preserved_indexes = try source.listIndexes(alloc);
        source_graph_generation = source.core.index_manager.graphIndex("links").?.config.coverage_generation;
    }
    const source_calls = counting.calls;
    try std.testing.expect(source_calls > 0);
    var read_options = source_options;
    read_options.open_mode = .query_readonly;
    read_options.primary_only_readonly = true;
    var source = try DB.open(alloc, source_path, read_options);
    defer source.close();
    const target_options: OpenOptions = .{
        .identity_namespace = .{ .table_id = 201, .shard_id = 202, .range_id = 202 },
        .primary_backend = .{ .lsm = .{} },
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .enrichment = .{ .owner_id = "artifact-target", .dense_embedder = counting.interface() },
    };
    var target = try DB.open(alloc, target_path, target_options);
    defer target.close();
    for (preserved_indexes) |config| try target.addIndex(config);
    try std.testing.expectEqual(source_graph_generation, target.core.index_manager.graphIndex("links").?.config.coverage_generation);
    // Real hidden owners persist their namespace during lifecycle admission,
    // before any source rows or generated artifacts arrive.
    try doc_identity.writeNamespaceToStore(target.core.store, target_options.identity_namespace.?);
    const schema = try schema_mod.serializeSchema(alloc, target.core.schema orelse .{});
    defer alloc.free(schema);
    const scope: staging.Scope = .{
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .source_artifact_digest = @splat(3),
        .source_namespace = source_options.identity_namespace.?,
        .target_namespace = target_options.identity_namespace.?,
        .target_schema_digest = staging.digest(schema),
        .preserve_artifacts = true,
    };
    var invalid_count: [8]u8 = undefined;
    std.mem.writeInt(u64, &invalid_count, 1, .little);
    try target.core.store.putBatch(&.{.{ .key = &internal_keys.range_document_count_key, .value = &invalid_count }}, &.{});
    try std.testing.expectError(error.RestoreStagingTargetNotEmpty, target.beginRestoreStaging(alloc, scope));
    try std.testing.expect(try doc_identity.visibilitySummaryFromStore(target.core.store) == null);
    try std.testing.expectError(error.NotFound, target.core.store.get(alloc, staging.key));
    try target.core.store.putBatch(&.{}, &.{&internal_keys.range_document_count_key});
    try target.beginRestoreStaging(alloc, scope);
    try std.testing.expectEqual(@as(u64, 0), (try doc_identity.visibilitySummaryFromStore(target.core.store)).?.live_ordinals);
    try std.testing.expectEqual(@as(?u64, 0), try range_cardinality.load(alloc, target.core.store));
    var index_number: u64 = 1;
    var artifact_pages: usize = 0;
    var replaced = false;
    while (true) : (index_number += 1) {
        if (replace_generated and !replaced) {
            var progress = (try target.restoreStagingStatus(alloc)).?;
            defer progress.deinit();
            if (progress.value.rows_complete) {
                // Model a successful producer completion after logical import
                // and before the final replay of archived artifact keys.
                const key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "whole_vector");
                defer alloc.free(key);
                const before = try target.core.store.get(alloc, key);
                defer alloc.free(before);
                const value = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, try enrichment_artifact_codec.sourceHash(before), &.{ 0, 1, 0 });
                defer alloc.free(value);
                try target.core.store.put(key, value);
                replaced = true;
            }
        }
        var page = try target.prepareRestoreStagingPage(alloc, scope, &source, if (standby) 1 else 128, .none);
        defer page.deinit();
        const batch = page.batch orelse break;
        if (standby) {
            const payload = try replication_effects_mod.encodeBatchMutationRequestAlloc(alloc, batch);
            defer alloc.free(payload);
            const record: replication_record_mod.RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = index_number, .previous_lsn = index_number - 1, .payload = payload };
            try @import("db/replication_ingress.zig").applyRecord(&target, record);
            try @import("db/replication_ingress.zig").applyRecord(&target, record);
        } else {
            try server_test_adapter.applyOrdered(&target, batch, .{ .term = 1, .index = index_number });
            // Lost acknowledgements replay without mutating the next page.
            try server_test_adapter.applyOrdered(&target, batch, .{ .term = 1, .index = index_number });
        }
        if (batch.restore_staging.?.import_page.artifact_page) {
            artifact_pages += 1;
            try std.testing.expectEqual(@as(u64, 0), target.core.table_catalog.row_count);
            if (artifact_pages == 1) {
                target.close();
                target = try DB.open(alloc, target_path, target_options);
            }
        }
        if (page.phase == .imported) break;
        try std.testing.expect(index_number < 100);
    }
    try std.testing.expect(artifact_pages >= 1);
    try std.testing.expectEqual(source_calls, counting.calls);
    try std.testing.expectEqual(@as(u64, 6), target.core.index_manager.denseIndex("semantic").?.index.stats().active_count);
    try std.testing.expectEqual(@as(u64, if (unverified_generated) 0 else 2), target.core.index_manager.denseIndex("whole_document").?.index.stats().active_count);
    try std.testing.expectEqual(@as(u64, 1), target.core.index_manager.denseIndex("explicit").?.index.stats().active_count);
    const stats = try target.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(?u64, 2), try range_cardinality.load(alloc, target.core.store));
    try std.testing.expectEqual(@as(u64, 2), stats.source_doc_count);
    try std.testing.expectEqual(@as(u64, 2), stats.doc_identity.live_ordinals);
    var covered: usize = 0;
    for (stats.indexes) |item| {
        if (!std.mem.eql(u8, item.name, "semantic") and !std.mem.eql(u8, item.name, "whole_document")) continue;
        covered += 1;
        if (unverified_generated and std.mem.eql(u8, item.name, "whole_document")) {
            try std.testing.expectEqual(@as(u64, 0), item.coverage_produced_count);
            try std.testing.expectEqual(@as(u64, 0), item.coverage_skipped_count);
            try std.testing.expectEqual(@as(u64, 0), item.coverage_terminal_failed_count);
            try std.testing.expect(stats.source_doc_count > item.coverage_produced_count + item.coverage_skipped_count + item.coverage_terminal_failed_count);
            try std.testing.expectEqual(@as(u64, 0), item.publication_target_count);
            continue;
        }
        try std.testing.expect(item.coverage_summary_ready);
        try std.testing.expectEqual(@as(u64, 2), item.coverage_produced_count);
        try std.testing.expectEqual(@as(u64, 0), item.coverage_skipped_count);
        try std.testing.expectEqual(@as(u64, 0), item.coverage_terminal_failed_count);
        try std.testing.expect(item.publication_target_ready);
        try std.testing.expectEqual(@as(u64, if (std.mem.eql(u8, item.name, "semantic")) 6 else 2), item.publication_target_count);
    }
    try std.testing.expectEqual(@as(usize, 2), covered);
    if (unverified_generated) {
        const key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "whole_vector");
        defer alloc.free(key);
        try std.testing.expectError(error.NotFound, target.core.store.get(alloc, key));
        return;
    }
    if (replace_generated) {
        try std.testing.expect(replaced);
        const key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "whole_vector");
        defer alloc.free(key);
        const value = try target.core.store.get(alloc, key);
        defer alloc.free(value);
        const vector = try enrichment_artifact_codec.decodeDenseEmbeddingAlloc(alloc, value);
        defer alloc.free(vector);
        try std.testing.expectEqualSlices(f32, &.{ 0, 1, 0 }, vector);
    }
    try std.testing.expect(target.core.artifact_cleanup_maybe.load(.acquire));
    try std.testing.expect(target.core.identity_namespace.eql(target_options.identity_namespace.?));
    var quanta: usize = 0;
    while (!try target.prepareRestoreStagingIndexesStep(alloc, scope.digest())) : (quanta += 1) {
        try std.testing.expect(quanta < 1000);
    }
    _ = try target.finishRestoreStaging(alloc, scope.digest(), .validated);
    _ = try target.finishRestoreStaging(alloc, scope.digest(), .published);
    target.close();
    target = try DB.open(alloc, target_path, target_options);
    const reopened_stats = try target.stats(alloc);
    defer types.freeDBStats(alloc, reopened_stats);
    try std.testing.expectEqual(@as(?u64, 2), try range_cardinality.load(alloc, target.core.store));
    try std.testing.expectEqual(@as(u64, 2), reopened_stats.source_doc_count);
    try std.testing.expectEqual(@as(u64, 2), reopened_stats.doc_identity.live_ordinals);
    for (reopened_stats.indexes) |item| {
        if (!std.mem.eql(u8, item.name, "semantic") and !std.mem.eql(u8, item.name, "whole_document")) continue;
        try std.testing.expect(item.coverage_summary_ready);
        try std.testing.expectEqual(reopened_stats.source_doc_count, item.coverage_produced_count);
    }
    var result = try target.search(alloc, .{ .index_name = "explicit", .dense = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc", result.hits[0].id);
    const edges = try target.getEdges(alloc, "links", "other", "related", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc", edges[0].source);
}

test "db ordered artifact inventory producer baseline resumes and includes behind-cursor writes" {
    const alloc = std.testing.allocator;
    const publication = @import("db/artifact_publication.zig");
    const obligations = @import("db/artifact_producer_obligations.zig");
    const baseline = @import("db/artifact_producer_baseline.zig");
    const codec = @import("db/artifact_publication_transport_codec.zig");
    const transport = @import("db/artifact_publication_transport.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/producer-baseline", .{tmp.sub_path});
    defer alloc.free(path);
    const follower_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/producer-baseline-follower", .{tmp.sub_path});
    defer alloc.free(follower_path);
    const Replicate = struct {
        fn apply(owners: [2]*DB, request: types.BatchRequest, position: OrderedApplyReceipt) !void {
            for (owners) |owner| try server_test_adapter.applyOrdered(&owner, request, position);
        }
    };
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        var follower = try DB.open(alloc, follower_path, options);
        defer follower.close();
        const owners = [2]*DB{ &db, &follower };
        try db.setSchemaJson(alloc, "{}");
        try follower.setSchemaJson(alloc, "{}");
        var writes: [129]types.BatchWrite = undefined;
        var names: [129][16]u8 = undefined;
        for (&writes, &names, 0..) |*write, *name, index| write.* = .{ .key = try std.fmt.bufPrint(name, "doc{d:0>4}", .{index}), .value = "{}" };
        try Replicate.apply(owners, .{ .writes = &writes, .timestamp_ns = 100 }, .{ .term = 1, .index = 1 });
        {
            var txn = try follower.core.store.beginWriteTxn();
            errdefer txn.abort();
            for (writes) |write| {
                const key = try internal_keys.sharedPdfConsumerAttemptKeyAlloc(alloc, write.key);
                defer alloc.free(key);
                try txn.put(key, "replica-local pending work");
            }
            try txn.commit();
        }
        var catalog = try db.artifactInventoryCommand(alloc);
        defer catalog.catalogs.deinit(alloc);
        catalog.binding.effect_protocol = 15;
        try Replicate.apply(owners, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 2 });
        var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
        activation.publication_digest = activation.digest();
        try Replicate.apply(owners, .{ .artifact_publication = activation }, .{ .term = 1, .index = 3 });
        // Occupy every producer-upload slot. A baseline control must still
        // enter, commit on both owners, and release its reserved capacity.
        const producer_hashes = [_]transport.Digest{transport.chunkDigest(0, "x")};
        for (0..transport.max_producer_uploads) |i| {
            var digest: transport.Digest = @splat(8);
            digest[0] = @intCast(i + 1);
            try Replicate.apply(owners, .{ .artifact_publication_transport = .{ .action = .begin, .namespace = catalog.namespace, .publication_digest = digest, .command_digest = @splat(9), .encoded_len = 1, .chunk_hashes = &producer_hashes } }, .{ .term = 1, .index = 4 + i });
        }
        // Without a dispatcher, local maintenance must not mutate replicated
        // progress. Discovery is read-only even when it reaches a page end.
        try std.testing.expect(!try db.advanceArtifactProducerBaselinePage());
        const Capture = struct {
            command: ?[]u8 = null,
            refuse: bool = true,
            fn enqueue(ptr: *anyopaque, namespace: publication.Namespace, bytes: []const u8) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (self.refuse) return error.ResourceBudgetExceeded;
                var decoded = try codec.decodeBorrowed(std.testing.allocator, bytes);
                defer decoded.deinit();
                try std.testing.expectEqualDeep(namespace, decoded.command.namespace);
                try std.testing.expectEqual(.baseline, decoded.command.mode);
                self.command = try std.testing.allocator.dupe(u8, bytes);
            }
        };
        var capture: Capture = .{};
        defer if (capture.command) |bytes| alloc.free(bytes);
        db.local_execution.artifact_publication_dispatcher = .{ .ptr = &capture, .enqueue = Capture.enqueue };
        defer db.local_execution.artifact_publication_dispatcher = null;
        try std.testing.expectError(error.ResourceBudgetExceeded, db.advanceArtifactProducerBaselinePage());
        capture.refuse = false;
        // Enqueue owns the serialized page after preparation releases its
        // snapshot/arena. It must still report incomplete until ordered apply.
        try std.testing.expect(!try db.advanceArtifactProducerBaselinePage());
        try std.testing.expect(capture.command != null);
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(@as(u64, 0), (try obligations.load(&read)).?.pending_documents);
        }
        try Replicate.apply(owners, .{ .writes = &.{.{ .key = "000-behind", .value = "{}" }}, .timestamp_ns = 101 }, .{ .term = 1, .index = 10 });
        var decoded = try codec.decodeBorrowed(alloc, capture.command.?);
        defer decoded.deinit();
        const hashes = [_]transport.Digest{transport.chunkDigest(0, capture.command.?)};
        const begin: transport.Request = .{ .action = .begin, .namespace = catalog.namespace, .publication_digest = decoded.command.publication_digest, .command_digest = decoded.command.publication_digest, .encoded_len = @intCast(capture.command.?.len), .chunk_hashes = &hashes, .control = true };
        const root = begin.proposedManifest().root();
        try Replicate.apply(owners, .{ .artifact_publication_transport = begin }, .{ .term = 1, .index = 11 });
        const base64 = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(capture.command.?.len));
        defer alloc.free(base64);
        _ = std.base64.standard.Encoder.encode(base64, capture.command.?);
        try Replicate.apply(owners, .{ .artifact_publication_transport = .{ .action = .chunk, .namespace = catalog.namespace, .publication_digest = decoded.command.publication_digest, .manifest_root = root, .chunk_base64 = base64 } }, .{ .term = 1, .index = 12 });
        const finalize: types.BatchRequest = .{ .artifact_publication_transport = .{ .action = .finalize, .namespace = catalog.namespace, .publication_digest = decoded.command.publication_digest, .manifest_root = root } };
        try Replicate.apply(owners, finalize, .{ .term = 1, .index = 13 });
        try Replicate.apply(owners, finalize, .{ .term = 1, .index = 14 });
        for (owners) |owner| {
            var applied = try owner.core.store.beginReadTxn();
            defer applied.abort();
            try std.testing.expectEqual(@as(u64, 13), (try transport.terminal(&applied, catalog.namespace, decoded.command.publication_digest)).?.decided_index);
            try std.testing.expectError(error.NotFound, applied.get(&transport.manifestKey(catalog.namespace, decoded.command.publication_digest)));
            try std.testing.expectEqual(@as(u64, 1 + decoded.command.baseline.?.row_keys.len), (try obligations.load(&applied)).?.pending_documents);
        }
    }
    var reopened = try DB.open(alloc, path, options);
    defer reopened.close();
    var follower = try DB.open(alloc, follower_path, options);
    defer follower.close();
    const owners = [2]*DB{ &reopened, &follower };
    var complete = false;
    var position: u64 = 15;
    var inserted: u64 = 0;
    for (0..128) |_| {
        var prepared = (try baseline.prepareRaft(alloc, reopened.core.store)) orelse {
            complete = true;
            break;
        };
        defer prepared.deinit();
        // Continuous tail inserts cannot move the saved baseline bound. They
        // are covered by foreground obligation capture, not rediscovery.
        var name: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&name, "zzz-growth-{d}", .{inserted});
        try Replicate.apply(owners, .{ .writes = &.{.{ .key = key, .value = "{}" }}, .timestamp_ns = 102 + inserted }, .{ .term = 2, .index = position });
        position += 1;
        inserted += 1;
        try Replicate.apply(owners, .{ .artifact_publication = prepared.command }, .{ .term = 2, .index = position });
        position += 1;
    }
    try std.testing.expect(complete);
    var read = try reopened.core.store.beginReadTxn();
    defer read.abort();
    const state = (try obligations.load(&read)).?;
    try std.testing.expect(state.baseline_complete);
    try std.testing.expectEqual(@as(u64, 130) + inserted, state.pending_documents);
    var follower_read = try follower.core.store.beginReadTxn();
    defer follower_read.abort();
    try std.testing.expectEqualDeep(state, (try obligations.load(&follower_read)).?);
    try std.testing.expectError(error.ArtifactCatalogDrift, obligations.requireDrained(&read, (try publication.authority(&read)).?));
    {
        const authority = (try publication.authority(&read)).?;
        var after: ?[]u8 = null;
        defer if (after) |value| alloc.free(value);
        var discovered: usize = 0;
        var pages: usize = 0;
        while (true) {
            var page = blk: {
                var snapshot = try reopened.core.store.beginReadTxn();
                defer snapshot.abort();
                break :blk try obligations.scanWork(alloc, &snapshot, authority, if (after) |value| .{ .authority = authority, .document = value } else null);
            };
            defer page.deinit();
            try std.testing.expect(page.items.len <= 128);
            for (page.items) |item| {
                if (after) |previous| try std.testing.expect(std.mem.order(u8, previous, item.document) == .lt);
                try std.testing.expectEqualDeep(try publication.inputRevision(&read, authority.namespace, item.document), item.position);
            }
            discovered += page.items.len;
            pages += 1;
            if (page.at_end) break;
            try std.testing.expect(page.items.len != 0);
            if (after) |value| alloc.free(value);
            after = null;
            after = try alloc.dupe(u8, page.next_document.?);
        }
        try std.testing.expectEqual(state.pending_documents, discovered);
        try std.testing.expect(pages >= 2);
        // This is discovery only: scanning never clears work or grants seal.
        try std.testing.expectEqual(state.pending_documents, (try obligations.load(&read)).?.pending_documents);
    }
    {
        const validation = @import("db/artifact_producer_validation.zig");
        var stale = (try validation.prepareRaft(alloc, reopened.core.store)).?;
        defer stale.deinit();
        try std.testing.expect(stale.command.validation.?.at_end);
        // Mutation after discovery invalidates even an empty validation page
        // identically on both owners. A new leader cannot replay it as proof.
        try Replicate.apply(owners, .{ .writes = &.{.{ .key = "validation-race", .value = "{}" }}, .timestamp_ns = 900 }, .{ .term = 3, .index = position });
        position += 1;
        try Replicate.apply(owners, .{ .artifact_publication = stale.command }, .{ .term = 3, .index = position });
        position += 1;
        for (owners) |owner| {
            var latest = try owner.core.store.beginReadTxn();
            defer latest.abort();
            try std.testing.expect(!(try validation.load(&latest)).?.complete);
        }
        var fresh = (try validation.prepareRaft(alloc, follower.core.store)).?;
        defer fresh.deinit();
        for (0..2) |_| {
            // Lost reply: duplicate ordered completion is idempotent.
            try Replicate.apply(owners, .{ .artifact_publication = fresh.command }, .{ .term = 3, .index = position });
            position += 1;
        }
        for (owners) |owner| {
            var latest = try owner.core.store.beginReadTxn();
            defer latest.abort();
            const authority = (try publication.authority(&latest)).?;
            try validation.requireComplete(&latest, authority);
            // Validating existing provenance cannot stand in for missing
            // required streams: the producer obligations are still pending.
            try std.testing.expectError(error.ArtifactCatalogDrift, obligations.requireDrained(&latest, authority));
        }
    }
    {
        // Authority and obligations are an atomic activation pair. Losing
        // the latter cannot be interpreted as an already-completed baseline.
        var txn = try reopened.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(obligations.key);
        try txn.commit();
    }
    try std.testing.expectError(error.ArtifactCatalogCorrupt, baseline.prepareRaft(alloc, reopened.core.store));
    try std.testing.expectError(error.ArtifactCatalogCorrupt, reopened.advanceArtifactProducerBaselinePage());
}

test "db ordered artifact inventory upload backpressure advances apply without false completion" {
    const alloc = std.testing.allocator;
    const transport = @import("db/artifact_publication_transport.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/publication-upload-admission", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{}" }} }, .{ .term = 1, .index = 1 });
    var namespace: transport.Namespace = undefined;
    doc_identity.encodeNamespace(&namespace, db.core.identity_namespace);
    const hashes = [_]transport.Digest{transport.chunkDigest(0, "x")};
    var begin: transport.Request = .{ .action = .begin, .namespace = namespace, .publication_digest = @splat(1), .command_digest = @splat(2), .encoded_len = 1, .chunk_hashes = &hashes };
    for (0..transport.max_producer_uploads) |ordinal| {
        begin.publication_digest[0] = @intCast(ordinal);
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication_transport = begin }, .{ .term = 1, .index = ordinal + 2 });
    }
    const cut = db.core.store.lastReplaySequence(0);
    begin.publication_digest[0] = 99;
    const root = begin.proposedManifest().root();
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication_transport = begin }, .{ .term = 1, .index = 10 });
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication_transport = .{ .action = .chunk, .namespace = namespace, .publication_digest = begin.publication_digest, .manifest_root = root, .chunk_base64 = "eA==" } }, .{ .term = 1, .index = 11 });
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication_transport = .{ .action = .finalize, .namespace = namespace, .publication_digest = begin.publication_digest, .manifest_root = root } }, .{ .term = 1, .index = 12 });
    try std.testing.expectEqual(@as(u64, 12), (try db.orderedApplyReceipt()).?.index);
    try std.testing.expectEqual(cut, db.core.store.lastReplaySequence(0));
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try transport.terminal(&read, namespace, begin.publication_digest)) == null);
        try std.testing.expectError(error.NotFound, read.get(&transport.manifestKey(namespace, begin.publication_digest)));
        try std.testing.expectError(error.NotFound, read.get(&try transport.chunkKey(namespace, begin.publication_digest, 0)));
    }
    // Reclamation is ordered and bounded to one expired upload per begin;
    // the refused identity is eligible for admission again, not poisoned by
    // a permanent capacity-error receipt.
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication_transport = begin }, .{ .term = 1, .index = transport.max_upload_age_entries + 2 });
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    _ = try transport.decodeManifest(try read.get(&transport.manifestKey(namespace, begin.publication_digest)));
}

test "db ordered artifact inventory graph planning inherits generation head fences through accepted assets" {
    try testGraphGenerationHeadFence(false);
}

test "db ordered artifact inventory accepted graph receipts lose current credit after upstream head replacement" {
    try testGraphGenerationHeadFence(true);
}

fn testGraphGenerationHeadFence(accepted_before_switch: bool) !void {
    const alloc = std.testing.allocator;
    const publication = @import("db/artifact_publication.zig");
    const generations = @import("db/artifact_chunk_generation.zig");
    const chunks = @import("db/artifact_chunk_manifest.zig");
    const planning = @import("db/artifact_graph_planning.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/graph-head-fence", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.addEnrichment(.{ .name = "relations", .kind = .asset, .field = "body", .content_type = "application/json" });
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"sources\":[{\"artifact\":\"relations\"}]}" });
    try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"seed\"}" }}, .timestamp_ns = 100 }, .{ .term = 1, .index = 1 });
    var catalog = try db.artifactInventoryCommand(alloc);
    defer catalog.catalogs.deinit(alloc);
    catalog.binding.effect_protocol = 15;
    try server_test_adapter.applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 2 });
    var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    activation.publication_digest = activation.digest();
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = activation }, .{ .term = 1, .index = 3 });
    const authority: publication.Authority = .{ .namespace = catalog.namespace, .epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest };
    const scope = try chunks.keyAlloc(alloc, "doc", "upstream");
    defer alloc.free(scope);
    var plan = try generations.Plan.init(alloc, scope, try generations.Spec.init(authority, scope, @splat(9), chunks.Builder.init().finish(), 1));
    defer plan.deinit();
    const asset_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc", "asset", "relations");
    defer alloc.free(asset_key);
    var token: @import("db/artifact_producer_context.zig").Token = .{ .arena = std.heap.ArenaAllocator.init(alloc), .namespace = authority.namespace, .epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_kind = .enrichment, .producer_name = "relations", .producer_generation = authority.epoch, .artifact_name = "relations", .source = undefined };
    defer token.deinit();
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        token.source = try publication.capturePrimarySource(token.arena.allocator(), &read, authority.namespace, "doc");
        try token.observe(plan.head_key, null, null);
        try token.observePrecondition(asset_key, null, null);
    }
    const asset_command = try token.command(&.{.{ .family = .document_artifact, .key = asset_key, .value = "{}", .source_index = 0 }});
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = asset_command }, .{ .term = 1, .index = 4 });
    var pinned = try db.core.store.beginReadTxn();
    defer pinned.abort();
    const Check = struct {
        fn run(a: Allocator, read: *@import("backend_erased.zig").ReadTxn, key: []const u8) !void {
            const context = try planning.Context.create(a, read, "doc", "relations", key, "{}");
            defer context.destroy();
        }
    };
    if (!accepted_before_switch) try std.testing.checkAllAllocationFailures(alloc, Check.run, .{ &pinned.read.?, asset_key });
    const context = try planning.Context.create(alloc, &pinned.read.?, "doc", "relations", asset_key, "{}");
    defer context.destroy();
    const inherited = for (context.base.artifact_sources) |guard| {
        if (std.mem.eql(u8, guard.key, plan.head_key)) break true;
    } else false;
    try std.testing.expect(inherited);
    const count_key = try internal_keys.graphEdgeContenderCountKeyAlloc(alloc, "doc", "g");
    defer alloc.free(count_key);
    const count = try @import("db/graph_edge_contender.zig").encodeVisibleCount(db.core.index_manager.coverageGenerationForIndex("g").?, 0);
    try context.publishGraphEffects(&[_]docstore_mod.KVPair{.{ .key = count_key, .value = &count }}, &.{});
    try std.testing.expectEqual(@as(usize, 1), context.commands.items.len);
    const graph_command = context.commands.items[0];
    if (accepted_before_switch) try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = graph_command }, .{ .term = 1, .index = 5 });
    {
        // Inject only a visibility change, under the actual Raft input-capture
        // boundary. No primary/asset bytes or asset receipt are changed.
        const Guard = struct {
            pub fn validate(_: @This(), _: anytype) !void {}
        };
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        _ = try plan.begin(&txn);
        _ = try plan.publish(&txn, null, Guard{});
        var marker_bytes: [ordered_apply_receipt_value_len]u8 = undefined;
        const marker = orderedApplyReceiptWrite(.{ .term = 1, .index = if (accepted_before_switch) 6 else 5 }, &marker_bytes);
        try txn.put(marker.key, marker.value);
        try txn.commit();
    }
    const before = db.core.store.lastReplaySequence(0);
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = graph_command }, .{ .term = 1, .index = if (accepted_before_switch) 7 else 6 });
    try std.testing.expectEqual(before, db.core.store.lastReplaySequence(0));
    var current = try db.core.store.beginReadTxn();
    defer current.abort();
    try std.testing.expectEqual(publication.Rejection.stale_source, (try publication.rejected(&current, graph_command)).?.reason);
    try std.testing.expectEqual(accepted_before_switch, (try publication.readReceipt(&current, graph_command, graph_command.sources[0])) != null);
    if (accepted_before_switch) {
        try std.testing.expectEqualSlices(u8, &count, try current.get(count_key));
        try std.testing.expectError(error.EnrichmentSourceChanged, @import("db/artifact_producer_provenance.zig").readCurrentForSource(alloc, &current, graph_command, graph_command.sources[0]));
    } else try std.testing.expectError(error.NotFound, current.get(count_key));
    try std.testing.expectEqualStrings("{}", try current.get(asset_key));
    try std.testing.expectError(error.EnrichmentSourceChanged, planning.Context.create(alloc, &current.read.?, "doc", "relations", asset_key, "{}"));
}

test "db ordered artifact inventory graph planning inherits selected extraction head instead of stale root" {
    const alloc = std.testing.allocator;
    const publication = @import("db/artifact_publication.zig");
    const provenance = @import("db/artifact_producer_provenance.zig");
    const extraction = @import("db/artifact_extraction_generation.zig");
    const generations = @import("db/artifact_chunk_generation.zig");
    const chunks = @import("db/artifact_chunk_manifest.zig");
    const planning = @import("db/artifact_graph_planning.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/graph-extraction-head", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.addEnrichment(.{ .name = "relations", .kind = .asset, .field = "body", .content_type = "application/json", .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}" });
    try db.addEnrichment(.{ .name = "copy", .kind = .asset, .source_artifact_name = "relations", .content_type = "application/json" });
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"sources\":[{\"artifact\":\"relations\"}]}" });
    try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"seed\"}" }}, .timestamp_ns = 100 }, .{ .term = 1, .index = 1 });
    var catalog = try db.artifactInventoryCommand(alloc);
    defer catalog.catalogs.deinit(alloc);
    catalog.binding.effect_protocol = 15;
    try server_test_adapter.applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 2 });
    var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    activation.publication_digest = activation.digest();
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = activation }, .{ .term = 1, .index = 3 });
    const authority: publication.Authority = .{ .namespace = catalog.namespace, .epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest };
    const scope = try @import("db/artifact_generation_scope.zig").extractionKeyAlloc(alloc, "doc", "relations");
    defer alloc.free(scope);
    const root_entry: extraction.Entry = .{ .name = "root", .value = "{}" };
    const encoded_entry = try extraction.encodeEntry(alloc, root_entry);
    defer alloc.free(encoded_entry);
    var output = chunks.Builder.init();
    try output.append(0, encoded_entry);
    var source: publication.Source = undefined;
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        source = try publication.capturePrimarySource(alloc, &read, catalog.namespace, "doc");
    }
    defer alloc.free(source.document_key);
    var header: publication.Command = .{ .producer_kind = .enrichment, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "relations", .producer_generation = catalog.binding.epoch, .producer_artifact_name = "relations", .sources = (&source)[0..1], .mutations = &.{}, .publication_digest = @splat(0) };
    var plan = try extraction.Plan.init(alloc, scope, try generations.Spec.init(authority, scope, header.inputDigest(), output.finish(), 1));
    defer plan.deinit();
    const head_raw = plan.core.spec.encode();
    const effects = [_]publication.Mutation{.{ .family = .document_artifact, .key = plan.core.head_key, .value = &head_raw, .source_index = 0 }};
    header.mutations = &effects;
    header.publication_digest = header.digest();
    var head_digest: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(&head_raw, &head_digest, .{});
    const proof_effects = [_]provenance.Effect{.{ .family = .document_artifact, .key = plan.core.head_key, .value_digest = head_digest, .value_bytes = head_raw.len, .source_index = 0 }};
    const proof: provenance.Proof = .{ .namespace = header.namespace, .authority_epoch = header.authority_epoch, .catalog_digest = header.catalog_digest, .producer_kind = header.producer_kind, .producer_name = header.producer_name, .producer_generation = header.producer_generation, .producer_artifact_name = header.producer_artifact_name, .publication_digest = header.publication_digest, .input_digest = header.inputDigest(), .sources = header.sources, .artifact_sources = &.{}, .effects = &proof_effects };
    const encoded_proof = try provenance.encodeAlloc(alloc, proof);
    defer alloc.free(encoded_proof);
    const stale_root = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc", "asset", "relations");
    defer alloc.free(stale_root);
    {
        // Receiver-side accepted-generation fixture. Production head writes
        // remain gated on ordered producer admission and finalization.
        const Guard = struct {
            pub fn validate(_: @This(), _: anytype) !void {}
        };
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(stale_root, "{\"stale\":true}");
        _ = try plan.begin(&txn);
        var append = try extraction.PreparedAppend.init(alloc, &plan, try plan.core.load(&txn), &.{root_entry});
        defer append.deinit();
        _ = try append.stage(&plan, &txn);
        _ = try plan.publish(&txn, null, Guard{});
        const position: publication.Position = .{ .raft = .{ .term = 1, .index = 4 } };
        try publication.stageArtifactRevisions(&txn, header, position);
        try provenance.stage(&txn, header, encoded_proof, position);
        var marker_bytes: [ordered_apply_receipt_value_len]u8 = undefined;
        const marker = orderedApplyReceiptWrite(.{ .term = 1, .index = 4 }, &marker_bytes);
        try txn.put(marker.key, marker.value);
        try txn.commit();
    }
    var pinned = try db.core.store.beginReadTxn();
    defer pinned.abort();
    var view = (try extraction.View(@import("docstore.zig").DocStore.Txn).open(alloc, &pinned, scope)).?;
    defer view.deinit();
    try std.testing.expectEqualStrings("{}", (try view.get(alloc, "root")).?);
    try std.testing.expectEqualStrings("{\"stale\":true}", try pinned.get(stale_root));
    const AllocationCheck = struct {
        fn run(a: Allocator, read: *@import("backend_erased.zig").ReadTxn, head: []const u8, value: []const u8) !void {
            const selected_context = try planning.Context.createWithProof(a, read, "doc", "relations", head, value);
            defer selected_context.destroy();
            try std.testing.expectEqualStrings(head, selected_context.base.artifact_sources[0].key);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationCheck.run, .{ &pinned.read.?, plan.core.head_key, @as([]const u8, &head_raw) });
    const context = try planning.Context.createWithProof(alloc, &pinned.read.?, "doc", "relations", plan.core.head_key, &head_raw);
    defer context.destroy();
    try std.testing.expectEqual(@as(usize, 1), context.base.artifact_sources.len);
    try std.testing.expectEqualStrings(plan.core.head_key, context.base.artifact_sources[0].key);
    const copy_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc", "asset", "copy");
    defer alloc.free(copy_key);
    const copy_guard = [_]publication.ArtifactSource{.{ .key = plan.core.head_key, .content_digest = head_digest, .input_position = try publication.artifactRevision(&pinned, catalog.namespace, plan.core.head_key), .source_index = 0 }};
    const copy_effects = [_]publication.Mutation{.{ .family = .document_artifact, .key = copy_key, .value = "{}", .source_index = 0 }};
    var copy_command: publication.Command = .{ .producer_kind = .enrichment, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "copy", .producer_generation = catalog.binding.epoch, .producer_artifact_name = "copy", .sources = (&source)[0..1], .artifact_sources = &copy_guard, .mutations = &copy_effects, .publication_digest = @splat(0) };
    copy_command.publication_digest = copy_command.digest();
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = copy_command }, .{ .term = 1, .index = 5 });
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqualStrings("{}", try read.get(copy_key));
        try std.testing.expect((try publication.readReceipt(&read, copy_command, source)) != null);
        var indexed = try provenance.prepareDocumentReferences(alloc, copy_command);
        defer indexed.deinit();
        try std.testing.expectEqual(@as(usize, 1), indexed.entries.len);
        try std.testing.expectEqualSlices(u8, &copy_command.publication_digest, try read.get(indexed.entries[0].key));
    }
    const count_key = try internal_keys.graphEdgeContenderCountKeyAlloc(alloc, "doc", "g");
    defer alloc.free(count_key);
    const count = try @import("db/graph_edge_contender.zig").encodeVisibleCount(db.core.index_manager.coverageGenerationForIndex("g").?, 0);
    try context.publishGraphEffects(&[_]docstore_mod.KVPair{.{ .key = count_key, .value = &count }}, &.{});
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = context.commands.items[0] }, .{ .term = 1, .index = 6 });
    var current = try db.core.store.beginReadTxn();
    defer current.abort();
    try std.testing.expect((try publication.readReceipt(&current, context.commands.items[0], context.commands.items[0].sources[0])) != null);
    var empty = try extraction.Plan.init(alloc, scope, try generations.Spec.init(authority, scope, header.inputDigest(), chunks.Builder.init().finish(), 2));
    defer empty.deinit();
    {
        const Guard = struct {
            pub fn validate(_: @This(), _: anytype) !void {}
        };
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        _ = try empty.begin(&txn);
        _ = try empty.publish(&txn, plan.core.spec.id(), Guard{});
        var marker_bytes: [ordered_apply_receipt_value_len]u8 = undefined;
        const marker = orderedApplyReceiptWrite(.{ .term = 1, .index = 7 }, &marker_bytes);
        try txn.put(marker.key, marker.value);
        try txn.commit();
    }
    var replaced = try db.core.store.beginReadTxn();
    defer replaced.abort();
    try std.testing.expectError(error.EnrichmentSourceChanged, planning.Context.createWithProof(alloc, &replaced.read.?, "doc", "relations", plan.core.head_key, &head_raw));
    const empty_head_raw = empty.core.spec.encode();
    var empty_digest: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(&empty_head_raw, &empty_digest, .{});
    const empty_guard = [_]publication.ArtifactSource{.{ .key = empty.core.head_key, .content_digest = empty_digest, .input_position = try publication.artifactRevision(&replaced, catalog.namespace, empty.core.head_key), .source_index = 0 }};
    var empty_copy_command = copy_command;
    empty_copy_command.artifact_sources = &empty_guard;
    empty_copy_command.publication_digest = empty_copy_command.digest();
    try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateArtifactSources(alloc, &replaced, copy_command.namespace, copy_command.sources, copy_command.artifact_sources));
    var empty_fence: @import("db/artifact_asset_publication.zig").UpstreamFence = .{ .key = stale_root, .requires_value = true };
    try std.testing.expectError(error.EnrichmentSourceChanged, empty_fence.bind(alloc, &replaced, empty_copy_command));
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = context.commands.items[0] }, .{ .term = 1, .index = 8 });
    var post = try db.core.store.beginReadTxn();
    defer post.abort();
    try std.testing.expectEqual(publication.Rejection.stale_source, (try publication.rejected(&post, context.commands.items[0])).?.reason);
}

test "db ordered artifact inventory accepted upload commits graph coverage provenance and terminal receipt" {
    try testAcceptedArtifactUpload(false);
}

test "db ordered artifact inventory pending coverage upload survives reopen and retries without retransmission" {
    try testAcceptedArtifactUpload(true);
}

fn testAcceptedArtifactUpload(inject_missing_counter: bool) !void {
    const alloc = std.testing.allocator;
    const publication = @import("db/artifact_publication.zig");
    const transport = @import("db/artifact_publication_transport.zig");
    const codec = @import("db/artifact_publication_transport_codec.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/accepted-publication-upload", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    var command: publication.Command = undefined;
    var root: transport.Digest = undefined;
    var count: [20]u8 = undefined;
    const key = try internal_keys.graphEdgeContenderCountKeyAlloc(owned, "doc", "g");
    var committed: u64 = 0;
    var saved_counter: [8]u8 = undefined;
    var counter_key: []const u8 = "";
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, "{}");
        try db.addEnrichment(.{ .name = "relations", .kind = .asset, .field = "body", .content_type = "application/json" });
        try db.addEnrichment(.{ .name = "other_relations", .kind = .asset, .field = "body", .content_type = "application/json" });
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"sources\":[{\"artifact\":\"relations\"},{\"artifact\":\"other_relations\"}]}" });
        try server_test_adapter.applyOrdered(&db, .{ .writes = &.{ .{ .key = "doc", .value = "{}" }, .{ .key = "neighbor", .value = "{}" } }, .timestamp_ns = 100 }, .{ .term = 1, .index = 1 });
        {
            // Replica-local historical markers are deliberately populated,
            // inconsistent and missing counters. Activation must establish a
            // new epoch, not scan or promote this local state into authority.
            const generation = db.core.index_manager.coverageGenerationForIndex("g").?;
            const legacy_marker = try internal_keys.derivedCoverageOutcomeKeyAlloc(owned, "g", generation, "doc");
            const legacy_count = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(owned, "g", generation, "produced");
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            try txn.put(legacy_marker, "produced");
            try txn.put(legacy_count, "not an authoritative count");
            try txn.commit();
        }
        var catalog = try db.artifactInventoryCommand(alloc);
        defer catalog.catalogs.deinit(alloc);
        catalog.binding.effect_protocol = 15;
        try server_test_adapter.applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 2 });
        var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
        activation.publication_digest = activation.digest();
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = activation }, .{ .term = 1, .index = 3 });
        const sources = try owned.alloc(publication.Source, 2);
        const preconditions = try owned.alloc(publication.ArtifactSource, 1);
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            sources[0] = try publication.capturePrimarySource(owned, &read, catalog.namespace, "doc");
            sources[1] = try publication.capturePrimarySource(owned, &read, catalog.namespace, "neighbor");
            const previous = read.get(key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            var digest: ?publication.Digest = null;
            if (previous) |raw| {
                var hash: publication.Digest = undefined;
                std.crypto.hash.sha2.Sha256.hash(raw, &hash, .{});
                digest = hash;
            }
            preconditions[0] = .{ .key = key, .content_digest = digest, .input_position = try publication.artifactRevision(&read, catalog.namespace, key), .source_index = 0 };
        }
        const generation = db.core.index_manager.coverageGenerationForIndex("g").?;
        counter_key = try @import("db/artifact_coverage_epoch.zig").counter(owned, @import("db/artifact_coverage_epoch.zig").forCommand(activation), "g", generation, "terminal_failed");
        count = try @import("db/graph_edge_contender.zig").encodeVisibleCount(generation, 0);
        const mutations = try owned.alloc(publication.Mutation, 1);
        mutations[0] = .{ .family = .graph, .key = key, .value = &count, .source_index = 0 };
        command = .{ .producer_kind = .graph, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "g", .producer_generation = generation, .producer_artifact_name = "relations", .sources = sources, .mutation_preconditions = preconditions, .mutations = mutations, .publication_digest = @splat(0) };
        command.publication_digest = command.digest();
        const encoded = try codec.encodeAlloc(owned, command);
        const hash = transport.chunkDigest(0, encoded);
        const begin: transport.Request = .{ .action = .begin, .namespace = command.namespace, .publication_digest = command.publication_digest, .command_digest = command.digest(), .encoded_len = @intCast(encoded.len), .chunk_hashes = &.{hash} };
        root = begin.proposedManifest().root();
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication_transport = begin }, .{ .term = 1, .index = 4 });
        const base64 = try owned.alloc(u8, std.base64.standard.Encoder.calcSize(encoded.len));
        _ = std.base64.standard.Encoder.encode(base64, encoded);
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication_transport = .{ .action = .chunk, .namespace = command.namespace, .publication_digest = command.publication_digest, .manifest_root = root, .chunk_base64 = base64 } }, .{ .term = 1, .index = 5 });
        if (inject_missing_counter) {
            // Fault injection, not migration: save an actual initialized
            // counter and remove it to exercise temporary baseline refusal.
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            saved_counter = (try txn.get(counter_key))[0..8].*;
            try txn.delete(counter_key);
            try txn.commit();
        }
        const before = db.core.store.lastReplaySequence(0);
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication_transport = .{ .action = .finalize, .namespace = command.namespace, .publication_digest = command.publication_digest, .manifest_root = root } }, .{ .term = 1, .index = 6 });
        if (inject_missing_counter) {
            try std.testing.expectEqual(before, db.core.store.lastReplaySequence(0));
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(publication.Rejection.baseline_pending, (try publication.rejected(&read, command)).?.reason);
            try std.testing.expect((try transport.terminal(&read, command.namespace, command.publication_digest)) == null);
            try std.testing.expect((try publication.readReceipt(&read, command, command.sources[0])) == null);
            _ = try read.get(&transport.manifestKey(command.namespace, command.publication_digest));
            _ = try read.get(&try transport.chunkKey(command.namespace, command.publication_digest, 0));
        }
        committed = db.core.store.lastReplaySequence(0);
    }
    var db = try DB.open(alloc, path, options);
    defer db.close();
    var recovered: ?transport.RecoveryHint = null;
    if (inject_missing_counter) {
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            try txn.put(counter_key, &saved_counter);
            try txn.commit();
        }
        const Capture = struct {
            refused: bool = true,
            hint: ?transport.RecoveryHint = null,
            fn enqueue(ptr: *anyopaque, namespace: [24]u8, bytes: []const u8) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (self.refused) return error.ResourceLimitExceeded;
                self.hint = (try transport.RecoveryHint.decode(bytes)) orelse return error.InvalidBatchRequest;
                try std.testing.expectEqualDeep(namespace, self.hint.?.namespace);
            }
        };
        var capture: Capture = .{};
        db.local_execution.artifact_publication_dispatcher = .{ .ptr = &capture, .enqueue = Capture.enqueue };
        defer db.local_execution.artifact_publication_dispatcher = null;
        try std.testing.expectError(error.ResourceLimitExceeded, db.advanceArtifactUploadRecovery());
        try std.testing.expectEqual(@as(u64, 0), db.artifact_upload_recovery_cursor.load(.acquire));
        capture.refused = false;
        try std.testing.expect(try db.advanceArtifactUploadRecovery());
        recovered = capture.hint.?;
        try std.testing.expectEqual(@as(u64, 4), recovered.?.created_index);
        // A delayed hint for a retired incarnation advances only the Raft
        // watermark. It cannot consume this upload or credit a receipt.
        var stale = recovered.?.request();
        stale.created_index = 2;
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication_transport = stale }, .{ .term = 2, .index = 7 });
        try std.testing.expectEqual(committed, db.core.store.lastReplaySequence(0));
        try std.testing.expect(try db.advanceArtifactUploadRecovery());
    }
    const finalize_index: u64 = if (inject_missing_counter) 8 else 7;
    const finalize: transport.Request = if (recovered) |hint| hint.request() else .{ .action = .finalize, .namespace = command.namespace, .publication_digest = command.publication_digest, .manifest_root = root };
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication_transport = finalize }, .{ .term = 2, .index = finalize_index });
    if (inject_missing_counter) committed = db.core.store.lastReplaySequence(0);
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication_transport = finalize }, .{ .term = 2, .index = finalize_index + 1 });
    try std.testing.expectEqual(committed, db.core.store.lastReplaySequence(0));
    const observed = try loadDerivedCoverageCounters(alloc, db.core.store, "g", command.producer_generation, null, null);
    try std.testing.expectEqual(@as(?u64, 0), observed.produced);
    try std.testing.expectEqual(@as(?u64, 1), observed.skipped);
    try std.testing.expectEqual(@as(?u64, 0), observed.terminal_failed);
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqualSlices(u8, &count, try read.get(key));
    try std.testing.expect((try publication.readReceipt(&read, command, command.sources[0])) != null);
    // A causal dependency is not an output owner. In particular, replay of
    // this accepted upload must not wait for a nonexistent neighbor receipt.
    try std.testing.expect((try publication.readReceipt(&read, command, command.sources[1])) == null);
    try std.testing.expect((try publication.rejected(&read, command)) == null);
    try std.testing.expectEqual(@as(u64, if (inject_missing_counter) 8 else 6), (try transport.terminal(&read, command.namespace, command.publication_digest)).?.decided_index);
    try std.testing.expect((try transport.nextRecovery(&read, command.namespace, 0)) == null);
    var proof = (try @import("db/artifact_producer_provenance.zig").readCurrentForArtifact(alloc, &read, key, &count)).?;
    defer proof.deinit();
    try std.testing.expectEqualDeep(command.publication_digest, proof.proof.publication_digest);
    const provenance = @import("db/artifact_producer_provenance.zig");
    try std.testing.expect((try provenance.readCurrentForSource(alloc, &read, command, command.sources[1])) == null);
    var accepted = (try provenance.readCurrentForSource(alloc, &read, command, command.sources[0])).?;
    defer accepted.deinit();
    try std.testing.expectEqualDeep(command.publication_digest, accepted.receipt.publication_digest);
    {
        // A different stream legitimately republishes the shared visible
        // count. Completion follows its current accepted projection without
        // invalidating the first stream or reinvoking its provider.
        var next = command;
        next.producer_artifact_name = "other_relations";
        var condition = command.mutation_preconditions[0];
        var count_digest: publication.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(&count, &count_digest, .{});
        condition.content_digest = count_digest;
        condition.input_position = try publication.artifactRevision(&read, command.namespace, key);
        next.mutation_preconditions = (&condition)[0..1];
        next.publication_digest = next.digest();
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = next }, .{ .term = 2, .index = finalize_index + 2 });
        var latest = try db.core.store.beginReadTxn();
        defer latest.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, provenance.readCurrentForSource(alloc, &latest, command, command.sources[0]));
        var converged = (try provenance.readConvergedForSource(alloc, &latest, command, command.sources[0])).?;
        defer converged.deinit();
        try std.testing.expectEqualDeep(command.publication_digest, converged.receipt.publication_digest);
        const Verify = struct {
            fn run(a: Allocator, txn: *@TypeOf(latest), selector: publication.Command) !void {
                var result = (try provenance.readConvergedForSource(a, txn, selector, selector.sources[0])) orelse return error.TestExpectedEqual;
                defer result.deinit();
                try std.testing.expectEqualDeep(selector.publication_digest, result.receipt.publication_digest);
            }
        };
        try std.testing.checkAllAllocationFailures(alloc, Verify.run, .{ &latest, command });
    }
    const validation = @import("db/artifact_producer_validation.zig");
    var clean_page = (try validation.prepareRaft(alloc, db.core.store)).?;
    defer clean_page.deinit();
    try std.testing.expectEqual(@as(usize, 0), clean_page.command.validation.?.repair_documents.len);
    {
        // Revision-only invalidation models a same-byte rewrite. The prior
        // receipt and proof remain durable, but may not certify current work.
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try publication.stageArtifactRevisions(&txn, command, .{ .raft = .{ .term = 2, .index = 100 } });
        try validation.invalidate(alloc, &txn, (try publication.authority(&txn)).?);
        try txn.commit();
    }
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = clean_page.command }, .{ .term = 2, .index = finalize_index + 3 });
    {
        var latest = try db.core.store.beginReadTxn();
        defer latest.abort();
        try std.testing.expect(!(try validation.load(&latest)).?.complete);
    }
    // Work is prepared from the current snapshot and installed through the
    // same Raft apply path; no provider work or payload IO occurs under apply.
    var repair_page = (try validation.prepareRaft(alloc, db.core.store)).?;
    defer repair_page.deinit();
    try std.testing.expect(repair_page.command.validation.?.repair_documents.len != 0);
    for (repair_page.command.validation.?.repair_documents) |document| try std.testing.expectEqualStrings("doc", document);
    try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = repair_page.command }, .{ .term = 2, .index = finalize_index + 4 });
    {
        var latest = try db.core.store.beginReadTxn();
        defer latest.abort();
        const obligations = @import("db/artifact_producer_obligations.zig");
        try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&latest)).?.pending_documents);
    }
    var current = try db.core.store.beginReadTxn();
    defer current.abort();
    try std.testing.expectEqualSlices(u8, &count, try current.get(key));
    try std.testing.expectError(error.EnrichmentSourceChanged, provenance.readCurrentForSource(alloc, &current, command, command.sources[0]));
    try std.testing.expectError(error.EnrichmentSourceChanged, provenance.readConvergedForSource(alloc, &current, command, command.sources[0]));
    // The older pinned snapshot remains a coherent accepted view.
    var pinned = (try provenance.readCurrentForSource(alloc, &read, command, command.sources[0])).?;
    defer pinned.deinit();
}

test "db ordered artifact inventory upload resumes across restart and atomically rejects then retires chunks" {
    const alloc = std.testing.allocator;
    const publication = @import("db/artifact_publication.zig");
    const transport = @import("db/artifact_publication_transport.zig");
    const codec = @import("db/artifact_publication_transport_codec.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/publication-upload", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(key);
    const payload = try alloc.alloc(u8, codec.chunk_bytes + 19);
    defer alloc.free(payload);
    @memset(payload, 0xff);
    var command: publication.Command = undefined;
    var encoded: []u8 = &.{};
    defer alloc.free(encoded);
    var hashes: [2]transport.Digest = undefined;
    var begin: transport.Request = undefined;
    var root: transport.Digest = undefined;
    const sources = [_]publication.Source{.{ .document_key = "doc", .content_digest = @splat(4), .timestamp = 100, .input_position = null }};
    const effects = [_]publication.Mutation{.{ .family = .base_vector, .key = key, .value = payload, .source_index = 0 }};
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, "{}");
        try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{}" }}, .timestamp_ns = 100 }, .{ .term = 1, .index = 1 });
        var catalog = try db.artifactInventoryCommand(alloc);
        defer catalog.catalogs.deinit(alloc);
        try server_test_adapter.applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 2 });
        // A stale epoch is a deterministic rejection even when the authored
        // output is too large for one upload chunk. No partial artifact may
        // become visible before or after the final decision.
        command = .{ .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch + 1, .catalog_digest = catalog.binding.digest, .producer_name = "model", .producer_generation = 1, .producer_artifact_name = "model", .sources = &sources, .mutations = &effects, .publication_digest = @splat(0) };
        command.publication_digest = command.digest();
        encoded = try codec.encodeAlloc(alloc, command);
        hashes = .{ transport.chunkDigest(0, encoded[0..codec.chunk_bytes]), transport.chunkDigest(1, encoded[codec.chunk_bytes..]) };
        begin = .{ .action = .begin, .namespace = command.namespace, .publication_digest = command.publication_digest, .command_digest = command.digest(), .encoded_len = @intCast(encoded.len), .chunk_hashes = &hashes };
        root = begin.proposedManifest().root();
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication_transport = begin }, .{ .term = 1, .index = 3 });
        const base64 = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(codec.chunk_bytes));
        defer alloc.free(base64);
        _ = std.base64.standard.Encoder.encode(base64, encoded[0..codec.chunk_bytes]);
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication_transport = .{ .action = .chunk, .namespace = command.namespace, .publication_digest = command.publication_digest, .manifest_root = root, .ordinal = 0, .chunk_base64 = base64 } }, .{ .term = 1, .index = 4 });
    }
    var reopened = try DB.open(alloc, path, options);
    defer reopened.close();
    try server_test_adapter.applyOrdered(&reopened, .{ .artifact_publication_transport = begin }, .{ .term = 2, .index = 5 });
    const tail = encoded[codec.chunk_bytes..];
    const base64 = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(tail.len));
    defer alloc.free(base64);
    _ = std.base64.standard.Encoder.encode(base64, tail);
    try server_test_adapter.applyOrdered(&reopened, .{ .artifact_publication_transport = .{ .action = .chunk, .namespace = command.namespace, .publication_digest = command.publication_digest, .manifest_root = root, .ordinal = 1, .chunk_base64 = base64 } }, .{ .term = 2, .index = 6 });
    const before = reopened.core.store.lastReplaySequence(0);
    const finalize: types.BatchRequest = .{ .artifact_publication_transport = .{ .action = .finalize, .namespace = command.namespace, .publication_digest = command.publication_digest, .manifest_root = root } };
    try server_test_adapter.applyOrdered(&reopened, finalize, .{ .term = 2, .index = 7 });
    // A lost finalize response retries from the terminal record after chunk
    // deletion; it never needs to resurrect the large original command.
    try server_test_adapter.applyOrdered(&reopened, finalize, .{ .term = 2, .index = 8 });
    try std.testing.expectEqual(before, reopened.core.store.lastReplaySequence(0));
    try std.testing.expectEqual(@as(u64, 8), (try reopened.orderedApplyReceipt()).?.index);
    var read = try reopened.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqual(publication.Rejection.stale_catalog, (try publication.rejected(&read, command)).?.reason);
    try std.testing.expectEqual(@as(u64, 7), (try transport.terminal(&read, command.namespace, command.publication_digest)).?.decided_index);
    try std.testing.expectError(error.NotFound, read.get(key));
    try std.testing.expectError(error.NotFound, read.get(&transport.manifestKey(command.namespace, command.publication_digest)));
    for (0..2) |ordinal| try std.testing.expectError(error.NotFound, read.get(&try transport.chunkKey(command.namespace, command.publication_digest, ordinal)));
}

test "db ordered artifact inventory idle upload retirement replays across owners restart and racing chunks" {
    const alloc = std.testing.allocator;
    const transport = @import("db/artifact_publication_transport.zig");
    const publication = @import("db/artifact_publication.zig");
    const codec = @import("db/artifact_publication_transport_codec.zig");
    const obligations = @import("db/artifact_producer_obligations.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const payload = try alloc.alloc(u8, codec.chunk_bytes);
    defer alloc.free(payload);
    @memset(payload, 'x');
    const base64 = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(payload.len));
    defer alloc.free(base64);
    _ = std.base64.standard.Encoder.encode(base64, payload);
    const hashes = [_]transport.Digest{ transport.chunkDigest(0, payload), transport.chunkDigest(1, "tail") };
    var before_chunk: ?transport.RecoveryHint = null;
    var after_chunk: ?transport.RecoveryHint = null;
    for (0..2) |owner| {
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/idle-upload-{d}", .{ tmp.sub_path, owner });
        defer alloc.free(path);
        var namespace: publication.Namespace = undefined;
        doc_identity.encodeNamespace(&namespace, options.identity_namespace.?);
        const begin: transport.Request = .{ .action = .begin, .namespace = namespace, .publication_digest = @splat(3), .command_digest = @splat(4), .encoded_len = codec.chunk_bytes + 4, .chunk_hashes = &hashes };
        {
            var db = try DB.open(alloc, path, options);
            defer db.close();
            try db.setSchemaJson(alloc, "{}");
            try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{}" }}, .timestamp_ns = 100 }, .{ .term = 1, .index = 1 });
            var catalog = try db.artifactInventoryCommand(alloc);
            defer catalog.catalogs.deinit(alloc);
            catalog.binding.effect_protocol = 15;
            try server_test_adapter.applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 2 });
            var activation: publication.Command = .{ .mode = .activate, .namespace = namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
            activation.publication_digest = activation.digest();
            try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = activation }, .{ .term = 1, .index = 3 });
            try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{\"changed\":true}" }}, .timestamp_ns = 101 }, .{ .term = 1, .index = 4 });
            try server_test_adapter.applyOrdered(&db, .{ .artifact_publication_transport = begin }, .{ .term = 1, .index = 5 });
            {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                const inventory = try transport.recoveryInventory(&read, namespace);
                var tracker: transport.RecoveryTracker = .{};
                try std.testing.expect(tracker.observe(inventory, 0, 0) == null);
                const hint = tracker.observe(inventory, transport.RecoveryTracker.idle_ns, 0).?;
                if (before_chunk) |expected| try std.testing.expectEqualDeep(expected, hint) else before_chunk = hint;
            }
            try server_test_adapter.applyOrdered(&db, .{ .artifact_publication_transport = .{ .action = .chunk, .namespace = namespace, .publication_digest = begin.publication_digest, .manifest_root = begin.proposedManifest().root(), .chunk_base64 = base64 } }, .{ .term = 1, .index = 6 });
            try server_test_adapter.applyOrdered(&db, .{ .artifact_publication_transport = before_chunk.?.request() }, .{ .term = 1, .index = 7 });
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            const inventory = try transport.recoveryInventory(&read, namespace);
            try std.testing.expectEqual(@as(usize, 1), inventory.count);
            try std.testing.expect(!inventory.entries[0].complete);
            var tracker: transport.RecoveryTracker = .{};
            _ = tracker.observe(inventory, 0, 0);
            const hint = tracker.observe(inventory, transport.RecoveryTracker.idle_ns, 0).?;
            if (after_chunk) |expected| try std.testing.expectEqualDeep(expected, hint) else after_chunk = hint;
        }
        var reopened = try DB.open(alloc, path, options);
        defer reopened.close();
        const replay = reopened.core.store.lastReplaySequence(0);
        try server_test_adapter.applyOrdered(&reopened, .{ .artifact_publication_transport = after_chunk.?.request() }, .{ .term = 2, .index = 8 });
        try server_test_adapter.applyOrdered(&reopened, .{ .artifact_publication_transport = after_chunk.?.request() }, .{ .term = 2, .index = 9 });
        {
            var read = try reopened.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(@as(usize, 0), (try transport.recoveryInventory(&read, namespace)).count);
            try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
            try std.testing.expect((try transport.terminal(&read, namespace, begin.publication_digest)) == null);
            try std.testing.expectError(error.NotFound, read.get(&try transport.chunkKey(namespace, begin.publication_digest, 0)));
        }
        try server_test_adapter.applyOrdered(&reopened, .{ .artifact_publication_transport = begin }, .{ .term = 2, .index = 10 });
        try server_test_adapter.applyOrdered(&reopened, .{ .artifact_publication_transport = after_chunk.?.request() }, .{ .term = 2, .index = 11 });
        try std.testing.expectEqual(replay, reopened.core.store.lastReplaySequence(0));
        var read = try reopened.core.store.beginReadTxn();
        defer read.abort();
        const inventory = try transport.recoveryInventory(&read, namespace);
        try std.testing.expectEqual(@as(usize, 1), inventory.count);
        try std.testing.expectEqual(@as(u64, 10), inventory.entries[0].hint.created_index);
        try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
    }
}

test "db ordered artifact inventory chunk publication authenticates complete sets and rejects omitted tail retirement" {
    const alloc = std.testing.allocator;
    const publication = @import("db/artifact_publication.zig");
    const chunks = @import("db/artifact_chunk_manifest.zig");
    const Context = @import("db/artifact_producer_context.zig").Token;
    const Harness = struct {
        fn captureVector(a: Allocator, db: *DB, scope: []const u8) !@import("db/artifact_chunk_vector_publication.zig").Input {
            var plan = try db.core.index_manager.acquireWritePlanSnapshot();
            defer plan.release();
            var request = for (plan.plan().generated_templates) |template| {
                if (template.kind == .sparse_embedding and std.mem.eql(u8, template.index_name, "sparse")) break template;
            } else return error.TestUnexpectedResult;
            request.doc_key = "doc";
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            return (try @import("db/artifact_chunk_vector_publication.zig").capture(a, &read, request, plan.plan(), scope)) orelse error.TestUnexpectedResult;
        }
        fn checkCapture(a: Allocator, db: *DB, scope: []const u8) !void {
            var input = try captureVector(a, db, scope);
            defer input.deinit();
        }
        fn vectorToken(db: *DB, scope: []const u8, output: []const u8) !Context {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            const authority = (try publication.authority(&read)).?;
            var token: Context = .{ .arena = std.heap.ArenaAllocator.init(db.alloc), .namespace = authority.namespace, .epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_kind = .index, .producer_name = "sparse", .producer_generation = db.core.index_manager.coverageGenerationForIndex("sparse").?, .artifact_name = "model", .producer_scope_key = scope, .source = undefined };
            errdefer token.deinit();
            token.source = try publication.capturePrimarySource(token.arena.allocator(), &read, authority.namespace, "doc");
            var input = try @import("db/artifact_chunk_generation.zig").captureInput(db.alloc, &read, scope);
            defer input.deinit();
            // Deliberately permit uncertified legacy input in this fixture:
            // the final writer must reject it even if a sender does not.
            if (try @import("db/artifact_producer_provenance.zig").readCurrentForArtifact(db.alloc, &read, input.proofKey(scope), input.proofValue())) |accepted| {
                var proof = accepted;
                defer proof.deinit();
                try token.inheritProof(proof.proof);
            }
            try input.observe(&token, &read, scope);
            try token.observePrecondition(output, null, null);
            return token;
        }
        fn materialize(_: *anyopaque, a: Allocator, _: []const u8, raw: []const u8) ![]u8 {
            return a.dupe(u8, raw);
        }
        fn capture(db: *DB) !Context {
            var plan = try db.core.index_manager.acquireWritePlanSnapshot();
            defer plan.release();
            var request = for (plan.plan().generated_templates) |template| {
                if (template.kind == .chunk_text and std.mem.eql(u8, template.artifact_name, "chunks")) break template;
            } else return error.TestUnexpectedResult;
            request.doc_key = "doc";
            const key = try internal_keys.documentKeyAlloc(db.alloc, "doc");
            defer db.alloc.free(key);
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            const raw = read.get(key) catch |err| if (err == error.NotFound) null else return err;
            return (try @import("db/artifact_asset_publication.zig").capture(db.alloc, &read, request, plan.plan(), key, raw, .{ .ptr = db, .materialize = materialize })) orelse error.TestUnexpectedResult;
        }
        fn prepare(a: Allocator, command: publication.Command, catalogs: @import("db/artifact_inventory.zig").Catalogs) !void {
            var result = try publication.prepareEffects(a, command, catalogs);
            defer result.deinit();
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/ordered-chunk-publication", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const manifest_key = try chunks.keyAlloc(alloc, "doc", "chunks");
    defer alloc.free(manifest_key);
    const zero = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc", "chunks", 0);
    defer alloc.free(zero);
    const one = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc", "chunks", 1);
    defer alloc.free(one);
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, "{}");
        try db.addEnrichment(.{ .name = "chunks", .kind = .chunk, .field = "body", .chunk_size = 20 });
        try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{\"sources\":[{\"artifact\":\"chunks\"}]}" });
        try db.addEnrichment(.{ .name = "model", .kind = .embedding, .field = "body", .source_artifact_name = "chunks" });
        try db.addIndex(.{ .name = "sparse", .kind = .sparse_vector, .config_json = "{\"field\":\"sparse\",\"embedding_name\":\"model\"}" });
        try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"text\"}" }}, .timestamp_ns = 100 }, .{ .term = 1, .index = 1 });
        var old = chunks.Builder.init();
        try old.append(0, "old zero");
        try old.append(1, "old one");
        // A previously reconstructed inventory, before ordered activation.
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            try txn.put(zero, "old zero");
            try txn.put(one, "old one");
            try txn.put(manifest_key, &old.finish().encode());
            try txn.commit();
        }
        var catalog = try db.artifactInventoryCommand(alloc);
        defer catalog.catalogs.deinit(alloc);
        catalog.binding.effect_protocol = 15;
        try server_test_adapter.applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 2 });
        var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
        activation.publication_digest = activation.digest();
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = activation }, .{ .term = 1, .index = 3 });
        var token = try Harness.capture(&db);
        defer token.deinit();
        var rows = try chunks.PreparedRows.init(alloc, "doc", "chunks", "body", &.{.{ .chunk_id = 0, .text = @constCast("new text") }});
        defer rows.deinit();
        const encoded = rows.manifest.?.encode();
        const effects = [_]publication.Mutation{
            rows.mutations[0],
            .{ .family = .document_artifact, .key = manifest_key, .value = &encoded, .source_index = 0 },
            .{ .family = .document_artifact, .key = one, .value = null, .source_index = 0 },
        };
        const incomplete = try token.command(effects[0..2]);
        // The sender's next set is valid, but omitting the old tail cannot
        // pass the actual inventory fence or create an acceptance receipt.
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = incomplete }, .{ .term = 1, .index = 4 });
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqualStrings("old one", try read.get(one));
            try std.testing.expect(!try token.accepted(alloc, &read));
        }
        const command = try token.command(&effects);
        const vector_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, zero, "model");
        defer alloc.free(vector_key);
        const vector = try @import("db/enrichment/artifact_codec.zig").encodeSparseEmbeddingAlloc(alloc, 123, &.{1}, &.{2});
        defer alloc.free(vector);
        try std.testing.expectError(error.ArtifactPublicationPending, Harness.captureVector(alloc, &db, zero));
        var uncertified = try Harness.vectorToken(&db, zero, vector_key);
        defer uncertified.deinit();
        const rejected = try uncertified.command(&.{.{ .family = .derived_vector, .key = vector_key, .value = vector, .source_index = 0 }});
        const before = db.core.store.lastReplaySequence(0);
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = rejected }, .{ .term = 1, .index = 5 });
        try std.testing.expectEqual(before, db.core.store.lastReplaySequence(0));
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(publication.Rejection.baseline_pending, (try publication.rejected(&read, rejected)).?.reason);
            try std.testing.expectError(error.NotFound, read.get(vector_key));
        }
        try std.testing.checkAllAllocationFailures(alloc, Harness.prepare, .{ command, catalog.catalogs });
        var prepared = try publication.prepareEffects(alloc, command, catalog.catalogs);
        defer prepared.deinit();
        try std.testing.expectEqual(@as(usize, 2), prepared.batch.documents.len);
        try std.testing.expectEqual(@as(usize, 1), prepared.coverage.len);
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = command }, .{ .term = 1, .index = 6 });
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expect(try token.accepted(alloc, &read));
            try std.testing.expectEqualStrings(rows.mutations[0].value.?, try read.get(zero));
            try std.testing.expectError(error.NotFound, read.get(one));
            try std.testing.expectEqualDeep(rows.manifest.?, try chunks.Manifest.decode(try read.get(manifest_key)));
        }
        try std.testing.checkAllAllocationFailures(alloc, Harness.checkCapture, .{ &db, zero });
        {
            var absent = try Harness.captureVector(alloc, &db, one);
            defer absent.deinit();
            try std.testing.expect(absent.value == null);
            const other_document = try internal_keys.chunkArtifactKeyAlloc(alloc, "other", "chunks", 0);
            defer alloc.free(other_document);
            try std.testing.expectError(error.InvalidBatchRequest, Harness.captureVector(alloc, &db, other_document));
            const other_producer = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc", "other", 0);
            defer alloc.free(other_producer);
            try std.testing.expectError(error.EnrichmentSourceChanged, Harness.captureVector(alloc, &db, other_producer));
        }
        var certified = try Harness.captureVector(alloc, &db, zero);
        defer certified.deinit();
        try std.testing.expectEqualStrings(rows.mutations[0].value.?, certified.value.?);
        try std.testing.expectEqualStrings(vector_key, certified.output_key);
        const accepted_vector = try certified.token.command(&.{.{ .family = .derived_vector, .key = certified.output_key, .value = vector, .source_index = 0 }});
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = accepted_vector }, .{ .term = 1, .index = 7 });
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqualSlices(u8, vector, try read.get(vector_key));
            try std.testing.expect(try certified.token.accepted(alloc, &read));
        }
        try std.testing.expect((try db.core.index_manager.sparseIndex("sparse").?.index.debugDocNumForDocId(zero)) != null);
    }
    var reopened = try DB.open(alloc, path, options);
    defer reopened.close();
    var token = try Harness.capture(&reopened);
    defer token.deinit();
    var read = try reopened.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expect(try token.accepted(alloc, &read));
    try std.testing.expectError(error.NotFound, read.get(one));
}

test "db ordered artifact inventory asset publication authenticates output and preserves shared text coverage" {
    const alloc = std.testing.allocator;
    const publication = @import("db/artifact_publication.zig");
    const asset = @import("db/artifact_asset_publication.zig");
    const Context = @import("db/artifact_producer_context.zig").Token;
    const Harness = struct {
        fn materialize(_: *anyopaque, a: Allocator, _: []const u8, raw: []const u8) ![]u8 {
            return a.dupe(u8, raw);
        }
        fn capture(db: *DB, name: []const u8) !Context {
            return captureAlloc(db.alloc, db, name);
        }
        fn captureAlloc(a: Allocator, db: *DB, name: []const u8) !Context {
            var plan = try db.core.index_manager.acquireWritePlanSnapshot();
            defer plan.release();
            var request = for (plan.plan().generated_templates) |template| {
                if (std.mem.eql(u8, template.artifact_name, name)) break template;
            } else return error.TestUnexpectedResult;
            request.doc_key = "doc";
            const key = try internal_keys.documentKeyAlloc(db.alloc, "doc");
            defer db.alloc.free(key);
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            return (try asset.capture(a, &read, request, plan.plan(), key, try read.get(key), .{ .ptr = db, .materialize = materialize })) orelse error.TestUnexpectedResult;
        }
        fn checkCapture(a: Allocator, db: *DB) !void {
            var token = try captureAlloc(a, db, "first");
            defer token.deinit();
        }
        fn checkPrepare(a: Allocator, command: publication.Command, catalogs: @import("db/artifact_inventory.zig").Catalogs) !void {
            var prepared = try asset.prepare(a, command, catalogs);
            defer prepared.deinit();
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/asset-ordered-output", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const first_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc", "asset", "first");
    defer alloc.free(first_key);
    const second_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc", "asset", "second");
    defer alloc.free(second_key);
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, "{}");
        try db.addEnrichment(.{ .name = "first", .kind = .asset, .field = "body", .content_type = "text/plain" });
        try db.addEnrichment(.{ .name = "second", .kind = .asset, .field = "body", .content_type = "text/plain" });
        try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{\"sources\":[{\"artifact\":\"first\"},{\"artifact\":\"second\"}]}" });
        try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"text\"}" }}, .timestamp_ns = 100 }, .{ .term = 1, .index = 1 });
        var catalog = try db.artifactInventoryCommand(alloc);
        defer catalog.catalogs.deinit(alloc);
        catalog.binding.effect_protocol = 15;
        try server_test_adapter.applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 2 });
        var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
        activation.publication_digest = activation.digest();
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = activation }, .{ .term = 1, .index = 3 });
        try std.testing.checkAllAllocationFailures(alloc, Harness.checkCapture, .{&db});
        for ([_][]const u8{ "first", "second" }, [_][]const u8{ first_key, second_key }, 0..) |name, key, ordinal| {
            var token = try Harness.capture(&db, name);
            defer token.deinit();
            const command = try token.command(&.{.{ .family = .document_artifact, .key = key, .value = name, .source_index = 0 }});
            var prepared = try asset.prepare(alloc, command, catalog.catalogs);
            defer prepared.deinit();
            if (ordinal == 0) try std.testing.checkAllAllocationFailures(alloc, Harness.checkPrepare, .{ command, catalog.catalogs });
            try std.testing.expectEqual(@as(usize, 1), prepared.batch.documents.len);
            try std.testing.expectEqual(@as(usize, 2), prepared.coverage[0].artifact_keys.len);
            const forged = try token.command(&.{.{ .family = .document_artifact, .key = if (ordinal == 0) second_key else first_key, .value = name, .source_index = 0 }});
            try std.testing.expectError(error.InvalidBatchRequest, asset.prepare(alloc, forged, catalog.catalogs));
            try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = command }, .{ .term = 1, .index = ordinal + 4 });
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expect(try token.accepted(alloc, &read));
            try std.testing.expectEqualStrings(name, try read.get(key));
        }
        try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{}" }}, .timestamp_ns = 101 }, .{ .term = 1, .index = 6 });
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            // Bytes from the old primary remain physically present, but do
            // not establish current authoritative projection availability.
            try std.testing.expect(!try @import("db/artifact_producer_provenance.zig").currentArtifactProduced(alloc, &read, second_key));
        }
        {
            var token = try Harness.capture(&db, "second");
            defer token.deinit();
            const command = try token.command(&.{.{ .family = .document_artifact, .key = second_key, .value = "second", .source_index = 0 }});
            try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = command }, .{ .term = 1, .index = 7 });
        }
        for ([_][]const u8{ "first", "second" }, [_][]const u8{ first_key, second_key }, 0..) |name, key, ordinal| {
            if (ordinal == 1) try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{\"revision\":2}" }}, .timestamp_ns = 102 }, .{ .term = 1, .index = 9 });
            var token = try Harness.capture(&db, name);
            defer token.deinit();
            const command = try token.command(&.{.{ .family = .document_artifact, .key = key, .value = null, .source_index = 0 }});
            try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = command }, .{ .term = 1, .index = ordinal * 2 + 8 });
            const marker = try @import("db/artifact_coverage_epoch.zig").marker(alloc, @import("db/artifact_coverage_epoch.zig").forCommand(command), "text", db.core.index_manager.coverageGenerationForIndex("text").?, "doc");
            defer alloc.free(marker);
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqualStrings(if (ordinal == 0) "produced" else "skipped", try read.get(marker));
        }
    }
    var reopened = try DB.open(alloc, path, options);
    defer reopened.close();
    var token = try Harness.capture(&reopened, "second");
    defer token.deinit();
    var read = try reopened.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expect(try token.accepted(alloc, &read));
    try std.testing.expectError(error.NotFound, read.get(first_key));
    try std.testing.expectError(error.NotFound, read.get(second_key));
}

test "db ordered artifact inventory full text replay publishes physical coverage before sidecar" {
    const alloc = std.testing.allocator;
    const publication = @import("db/artifact_publication.zig");
    const certificates = @import("db/artifact_projection_certificate.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/physical-projection-seal", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .index_backends = .{ .text_main_backend = .lsm }, .start_index_workers = false, .start_optional_runtimes = false };
    var saved: @import("projection_seal.zig").Seal = undefined;
    var requirement: publication.Digest = undefined;
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, "{}");
        try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
        var catalog = try db.artifactInventoryCommand(alloc);
        defer catalog.catalogs.deinit(alloc);
        catalog.binding.effect_protocol = 15;
        try server_test_adapter.applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 1 });
        var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
        activation.publication_digest = activation.digest();
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = activation }, .{ .term = 1, .index = 2 });
        try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"durable projection\"}" }}, .sync_level = .full_index, .timestamp_ns = 100 }, .{ .term = 1, .index = 3 });
        try db.runUntilIdle();
        const entry = db.core.index_manager.textIndexEntry("text").?;
        saved = (try entry.persistent.loadProjectionSeal(alloc)) orelse return error.TestExpectedPhysicalProjectionSeal;
        const checkpoint = try db.core.loadProjectionCheckpoint(alloc, "text");
        try std.testing.expect(saved.applied_sequence > 0);
        try std.testing.expectEqual(db.root_incarnation, saved.root);
        try std.testing.expectEqualSlices(u8, &catalog.namespace, &saved.namespace);
        try std.testing.expectEqual(entry.config.coverage_generation, saved.generation);
        try std.testing.expectEqual(checkpoint.config_hash, saved.config_hash);
        try std.testing.expectEqual(checkpoint.applied_sequence, saved.applied_sequence);
        try std.testing.expectEqual(@as(u32, 1), entry.persistent.snapshot().liveDocCount());
        const Validate = struct {
            fn run(a: Allocator, target: *DB, generation: u64) !void {
                var snapshot = (try apply_state.tryAcquireProjectionSnapshot(a, target.core.index_manager.checkpointIo(), target.core.store, target.core.applied_sequence_checkpoint_path)).?;
                defer snapshot.deinit();
                try std.testing.expect(try target.core.index_manager.tryValidateFullTextProjection(a, &snapshot, "text", generation + 1) == null);
                var guard = (try target.core.index_manager.tryValidateFullTextProjection(a, &snapshot, "text", generation)) orelse return error.TestExpectedPhysicalProjectionGuard;
                defer guard.deinit();
                try std.testing.expect(try target.core.index_manager.tryValidateFullTextProjection(a, &snapshot, "text", generation) == null);
                var read = try target.core.store.beginReadTxn();
                defer read.abort();
                try guard.requireCurrent(&read);
            }
        };
        try std.testing.checkAllAllocationFailures(alloc, Validate.run, .{ &db, saved.generation });
        {
            var plan = try db.core.index_manager.acquireWritePlanSnapshot();
            defer plan.release();
            const node = for (plan.plan().completion_plan.?.nodes) |*candidate| {
                if (candidate.kind == .index_projection and std.mem.eql(u8, candidate.name, "text")) break candidate;
            } else return error.TestExpectedPhysicalProjectionNode;
            requirement = node.id;
            var snapshot = (try apply_state.tryAcquireProjectionSnapshot(alloc, db.core.index_manager.checkpointIo(), db.core.store, db.core.applied_sequence_checkpoint_path)).?;
            defer snapshot.deinit();
            var guard = (try db.core.index_manager.tryValidateFullTextProjection(alloc, &snapshot, "text", saved.generation)).?;
            defer guard.deinit();
            {
                var txn = try db.core.store.beginWriteTxn();
                defer txn.abort();
                try std.testing.expect(try certificates.stageFullText(&txn, &guard, node));
            }
            {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                try std.testing.expect(try certificates.load(&read, &certificates.key("text")) == null);
            }
            {
                var txn = try db.core.store.beginWriteTxn();
                errdefer txn.abort();
                try txn.put(&certificates.key("text"), "damaged derived certificate");
                try std.testing.expectError(error.ArtifactCatalogCorrupt, certificates.load(&txn, &certificates.key("text")));
                try std.testing.expect(try certificates.stageFullText(&txn, &guard, node));
                try std.testing.expect(!try certificates.stageFullText(&txn, &guard, node));
                const certificate = (try certificates.load(&txn, &certificates.key("text"))).?;
                try certificate.requireCurrent(&txn, db.root_incarnation, requirement, saved.applied_sequence);
                try std.testing.expectError(error.ArtifactPublicationPending, certificate.requireCurrent(&txn, db.root_incarnation, requirement, saved.applied_sequence + 1));
                try std.testing.expectError(error.EnrichmentSourceChanged, certificate.requireCurrent(&txn, db.root_incarnation + 1, requirement, saved.applied_sequence));
                var invalid = node.*;
                invalid.id[0] ^= 1;
                try std.testing.expectError(error.ArtifactCatalogCorrupt, certificates.stageFullText(&txn, &guard, &invalid));
                var closure = try certificates.prepareClosure(alloc, &txn, db.root_incarnation, "doc", node);
                defer closure.deinit();
                try closure.requireCurrent(&txn, db.root_incarnation);
                try txn.commit();
            }
        }
        {
            var plan = try db.core.index_manager.acquireWritePlanSnapshot();
            defer plan.release();
            for (0..16) |_| {
                if (try @import("db/artifact_native_stream.zig").advance(alloc, db.core.store, db.root_incarnation, "doc", plan.plan()) == .closed) break;
            } else return error.TestExpectedNativeProjectionClosure;
            const completion = @import("db/artifact_completion_progress.zig");
            {
                var txn = try db.core.store.beginWriteTxn();
                errdefer txn.abort();
                try certificates.remove(&txn, "text");
                try txn.commit();
            }
            {
                var busy = (try apply_state.tryAcquireProjectionSnapshot(alloc, db.core.index_manager.checkpointIo(), db.core.store, db.core.applied_sequence_checkpoint_path)).?;
                defer busy.deinit();
                try completion.refreshProjections(alloc, db.core.store, db.core.index_manager, db.core.applied_sequence_checkpoint_path, db.root_incarnation, "doc", plan.plan(), .{});
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                try std.testing.expect(try certificates.load(&read, &certificates.key("text")) == null);
            }
            try completion.refreshProjections(alloc, db.core.store, db.core.index_manager, db.core.applied_sequence_checkpoint_path, db.root_incarnation, "doc", plan.plan(), .{});
            // Covered pages must not need a sidecar file at all.
            try completion.refreshProjections(alloc, db.core.store, db.core.index_manager, "missing-parent/no-sidecar", db.root_incarnation, "doc", plan.plan(), .{});
            var complete = blk: {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                var result = (try @import("db/artifact_completion_progress.zig").discover(alloc, &read, db.root_incarnation, "doc", plan.plan(), .{ .time_budget_ns = null })).?;
                errdefer result.deinit();
                var scheduled = (try @import("db/artifact_completion_progress.zig").discoverNextControl(alloc, &read, db.root_incarnation, "doc", plan.plan(), .{ .time_budget_ns = null })).?;
                defer scheduled.deinit();
                try std.testing.expect(scheduled == .completion);
                try std.testing.expectEqualDeep(try result.command(), try scheduled.command());
                break :blk result;
            };
            defer complete.deinit();
            try std.testing.expect(complete.atEnd());
            // Ordered apply must reconstruct receiver-local evidence rather
            // than relying on the certificate used by command discovery.
            {
                var txn = try db.core.store.beginWriteTxn();
                errdefer txn.abort();
                try certificates.remove(&txn, "text");
                try txn.commit();
            }
            try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = try complete.command() }, .{ .term = 1, .index = 4 });
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(@as(u64, 0), (try @import("db/artifact_producer_obligations.zig").load(&read)).?.pending_documents);
        }
        {
            var snapshot = (try apply_state.tryAcquireProjectionSnapshot(alloc, db.core.index_manager.checkpointIo(), db.core.store, db.core.applied_sequence_checkpoint_path)).?;
            defer snapshot.deinit();
            var guard = (try db.core.index_manager.tryValidateFullTextProjection(alloc, &snapshot, "text", saved.generation)).?;
            defer guard.deinit();
            var txn = try db.core.store.beginWriteTxn();
            defer txn.abort();
            try txn.put(guard.admission_key, "pending admission");
            try std.testing.expectError(error.EnrichmentSourceChanged, guard.requireCurrent(&txn));
        }
        // A clean sidecar ahead of the index's physical watermark cannot
        // certify completion, even though its config and generation match.
        var ahead = checkpoint;
        ahead.applied_sequence += 1;
        try apply_state.saveProjectionCheckpointWithSidecar(alloc, db.core.index_manager.checkpointIo(), db.core.store, db.core.applied_sequence_checkpoint_path, "text", ahead);
        {
            var snapshot = (try apply_state.tryAcquireProjectionSnapshot(alloc, db.core.index_manager.checkpointIo(), db.core.store, db.core.applied_sequence_checkpoint_path)).?;
            defer snapshot.deinit();
            try std.testing.expect(try db.core.index_manager.tryValidateFullTextProjection(alloc, &snapshot, "text", saved.generation) == null);
        }
        try apply_state.saveProjectionCheckpointWithSidecar(alloc, db.core.index_manager.checkpointIo(), db.core.store, db.core.applied_sequence_checkpoint_path, "text", checkpoint);
    }
    var db = try DB.open(alloc, path, options);
    defer db.close();
    try std.testing.expectEqualDeep(saved, (try db.core.index_manager.textIndexEntry("text").?.persistent.loadProjectionSeal(alloc)).?);
    const before_prune = blk: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const certificate = (try certificates.load(&read, &certificates.key("text"))).?;
        try std.testing.expectError(error.EnrichmentSourceChanged, certificate.requireCurrent(&read, db.root_incarnation, requirement, saved.applied_sequence));
        break :blk try @import("db/artifact_projection_epoch.zig").load(&read);
    };
    try db.core.index_manager.pruneTextSplitRange("a");
    try std.testing.expect(try db.core.index_manager.textIndexEntry("text").?.persistent.loadProjectionSeal(alloc) == null);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(try @import("db/artifact_projection_epoch.zig").load(&read) > before_prune);
    }
    try std.testing.expect(try db.core.index_manager.remove(db.core.store, "text"));
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expect(try certificates.load(&read, &certificates.key("text")) == null);
}

test "db ordered artifact inventory materialization replay cut is owner local atomic and durable" {
    const alloc = std.testing.allocator;
    const publication = @import("db/artifact_publication.zig");
    const source_gap = @import("db/artifact_source_gap.zig");
    var source_guard: source_gap.Guard = undefined;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/materialization-replay-cut", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(artifact_key);
    var namespace: publication.Namespace = undefined;
    var saved: publication.Materialization = undefined;
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, "{}");
        var catalog = try db.artifactInventoryCommand(alloc);
        defer catalog.catalogs.deinit(alloc);
        namespace = catalog.namespace;
        try server_test_adapter.applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 1 });
        var activation: publication.Command = .{ .mode = .activate, .namespace = namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
        activation.publication_digest = activation.digest();
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = activation }, .{ .term = 1, .index = 2 });
        try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"one\"}" }}, .timestamp_ns = 100 }, .{ .term = 1, .index = 3 });
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            saved = (try publication.materializationState(&read, namespace, "doc")).?;
            try std.testing.expectEqual(@as(u64, 3), saved.position.raft.index);
            source_guard = try source_gap.Guard.capture(&read);
            try std.testing.expectEqual(@as(u64, 0), source_guard.gap_epoch);
            try std.testing.expectEqual(db.core.store.lastReplaySequence(0), saved.replay_sequence.?);
        }
        try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "other", .value = "{}" }}, .timestamp_ns = 101 }, .{ .term = 1, .index = 4 });
        try std.testing.expect(db.core.store.lastReplaySequence(0) > saved.replay_sequence.?);
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqualDeep(saved, (try publication.materializationState(&read, namespace, "doc")).?);
            try source_guard.requireCurrent(&read);
        }
        // A physical artifact write without a fresh replay record cannot use
        // another owner's journal progress to claim projection readiness.
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            var marker: [16]u8 = undefined;
            std.mem.writeInt(u64, marker[0..8], 1, .little);
            std.mem.writeInt(u64, marker[8..16], 5, .little);
            try txn.put(&internal_keys.raft_document_applied_entry_key, &marker);
            try txn.put(artifact_key, "unjournaled artifact");
            try txn.commit();
        }
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            const changed = (try publication.materializationState(&read, namespace, "doc")).?;
            try std.testing.expectEqual(@as(u64, 5), changed.position.raft.index);
            try std.testing.expect(changed.replay_sequence == null);
            try std.testing.expectError(error.EnrichmentSourceChanged, source_guard.requireCurrent(&read));
            source_guard = try source_gap.Guard.capture(&read);
            try std.testing.expectEqual(@as(u64, 1), source_guard.gap_epoch);
            try std.testing.expectEqual(@as(u64, 3), (try publication.inputRevision(&read, namespace, "doc")).?.raft.index);
        }
        try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"two\"}" }}, .timestamp_ns = 102 }, .{ .term = 1, .index = 6 });
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            saved = (try publication.materializationState(&read, namespace, "doc")).?;
            try std.testing.expectEqual(@as(u64, 6), saved.position.raft.index);
            try source_guard.requireCurrent(&read);
            try std.testing.expectEqual(db.core.store.lastReplaySequence(0), saved.replay_sequence.?);
        }
        {
            var txn = try db.core.store.beginWriteTxn();
            defer txn.abort();
            try txn.put(artifact_key, "aborted artifact");
        }
    }
    var reopened = try DB.open(alloc, path, options);
    defer reopened.close();
    var read = try reopened.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqualDeep(saved, (try publication.materializationState(&read, namespace, "doc")).?);
    try source_guard.requireCurrent(&read);
}

test "db ordered artifact inventory stale publications commit rejection without artifact or replay progress" {
    const alloc = std.testing.allocator;
    const publication = @import("db/artifact_publication.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/publication-rejection", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(artifact_key);
    const primary_key = try internal_keys.documentKeyAlloc(alloc, "doc");
    defer alloc.free(primary_key);
    const timestamp_key = try internal_keys.ttlKeyAlloc(alloc, "doc");
    defer alloc.free(timestamp_key);
    var source: publication.Source = undefined;
    var stale_source: publication.Command = undefined;
    var stale_catalog: publication.Command = undefined;
    const effects = [_]publication.Mutation{.{ .family = .base_vector, .key = artifact_key, .value = "never materialized", .source_index = 0 }};
    var committed_replay: u64 = 0;
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, "{}");
        try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{\"text\":\"before\"}" }}, .timestamp_ns = 100 }, .{ .term = 1, .index = 1 });
        var catalog = try db.artifactInventoryCommand(alloc);
        defer catalog.catalogs.deinit(alloc);
        try server_test_adapter.applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 2 });
        var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
        activation.publication_digest = activation.digest();
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = activation }, .{ .term = 1, .index = 3 });
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            source = .{ .document_key = "doc", .content_digest = undefined, .timestamp = std.mem.readInt(u64, (try read.get(timestamp_key))[0..8], .little), .input_position = try publication.inputRevision(&read, catalog.namespace, "doc") };
            std.crypto.hash.sha2.Sha256.hash(try read.get(primary_key), &source.content_digest, .{});
        }
        try std.testing.expectEqual(@as(u16, 14), catalog.binding.effect_protocol);
        try std.testing.expect((try @import("db/artifact_producer_baseline.zig").prepareRaft(alloc, db.core.store)) == null);
        try std.testing.expect(try db.advanceArtifactProducerBaselinePage());
        stale_source = .{ .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "model", .producer_generation = 1, .producer_artifact_name = "model", .sources = (&source)[0..1], .mutations = &effects, .publication_digest = @splat(0) };
        stale_source.publication_digest = stale_source.digest();
        // A legitimate input commit wins after provider preparation/proposal.
        try server_test_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{\"text\":\"after\"}" }}, .timestamp_ns = 101 }, .{ .term = 1, .index = 4 });
        committed_replay = db.core.store.lastReplaySequence(0);
        const next_reservation = db.core.store.nextReplaySequence(1);
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = stale_source }, .{ .term = 1, .index = 5 });
        try std.testing.expectEqual(next_reservation, db.core.store.nextReplaySequence(1));
        stale_catalog = stale_source;
        stale_catalog.authority_epoch += 1;
        stale_catalog.publication_digest = stale_catalog.digest();
        try server_test_adapter.applyOrdered(&db, .{ .artifact_publication = stale_catalog }, .{ .term = 1, .index = 6 });
        try std.testing.expectEqual(committed_replay, db.core.store.lastReplaySequence(0));
        try std.testing.expectEqual(@as(u64, 6), (try db.orderedApplyReceipt()).?.index);
    }
    var reopened = try DB.open(alloc, path, options);
    defer reopened.close();
    try server_test_adapter.applyOrdered(&reopened, .{ .artifact_publication = stale_catalog }, .{ .term = 1, .index = 6 });
    try std.testing.expectEqual(committed_replay, reopened.core.store.lastReplaySequence(0));
    var read = try reopened.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(artifact_key));
    const rejected_source = (try publication.rejected(&read, stale_source)).?;
    const rejected_catalog = (try publication.rejected(&read, stale_catalog)).?;
    try std.testing.expectEqual(publication.Rejection.stale_source, rejected_source.reason);
    try std.testing.expectEqual(@as(u64, 5), rejected_source.applied_index);
    try std.testing.expectEqual(publication.Rejection.stale_catalog, rejected_catalog.reason);
    try std.testing.expectEqual(@as(u64, 6), rejected_catalog.applied_index);
    try std.testing.expect((try publication.readReceipt(&read, stale_source, source)) == null);
}

test "db ordered artifact inventory commits receipt and detects catalog drift across reopen" {
    const alloc = std.testing.allocator;
    // Logical restore copies definitions but cannot inherit another owner's
    // ordered epoch or local materialization receipt. Native Raft snapshots
    // instead preserve the complete primary store for this same namespace.
    try std.testing.expect(!portable_backup.isPortableMetadataKey(@import("db/artifact_inventory.zig").ordered_key));
    try std.testing.expect(!portable_backup.isPortableMetadataKey(@import("db/artifact_inventory.zig").local_key));
    try std.testing.expect(!portable_backup.isPortableMetadataKey(@import("db/artifact_reconcile_intent.zig").key));
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/ordered-artifacts", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, "{}");
        var command = try db.artifactInventoryCommand(alloc);
        defer command.catalogs.deinit(alloc);
        try std.testing.expect(!(try db.artifactInventoryStatus()).ready);
        try server_test_adapter.applyOrdered(&db, .{ .artifact_catalog = command }, .{ .term = 1, .index = 1 });
        try std.testing.expect((try db.artifactInventoryStatus()).ready);
        try server_test_adapter.applyOrdered(&db, .{ .artifact_catalog = command }, .{ .term = 1, .index = 1 });
    }
    var reopened = try DB.open(alloc, path, options);
    defer reopened.close();
    try std.testing.expect((try reopened.artifactInventoryStatus()).ready);
    try reopened.addIndex(.{ .name = "new_text", .kind = .full_text, .config_json = "{}" });
    try std.testing.expect(!(try reopened.artifactInventoryStatus()).ready);
    var next = try reopened.artifactInventoryCommand(alloc);
    defer next.catalogs.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), next.binding.epoch);
    try server_test_adapter.applyOrdered(&reopened, .{ .artifact_catalog = next }, .{ .term = 1, .index = 2 });
    try std.testing.expect((try reopened.artifactInventoryStatus()).ready);
    // A stale command must fail its ordered CAS before reconciliation can
    // remove a legitimate newer local index or change its generation.
    var stale = next;
    stale.previous = null;
    stale.binding.epoch = 1;
    stale.catalogs = .{};
    stale.binding.digest = stale.catalogs.digest();
    stale.binding.semantic_digest = try stale.catalogs.semanticDigest(alloc);
    try std.testing.expectError(error.ArtifactCatalogEpochChanged, reopened.reconcileOrderedArtifactCatalogStep(stale, 3));
    try std.testing.expect(reopened.hasIndex("new_text"));
    try std.testing.expect((try reopened.artifactInventoryStatus()).ready);
    try reopened.reassignIdentityNamespaceForInternalTransition(.{ .table_id = 1, .shard_id = 9, .range_id = 9 });
    const rebound = try reopened.artifactInventoryStatus();
    try std.testing.expect(rebound.ordered == null and !rebound.ready);
    try std.testing.expect(reopened.hasIndex("new_text"));
}

test "relational index system online admission defers during index structural mutation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/source-admission-index-guard", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.updateRange(.{ .start = "m", .end = "z" });
    try db.core.store.delete(@import("db/relational_integrity_catalog.zig").key);
    const identity = try db.relationalTopologyIdentity();
    const scope: @import("db/online_source_contract.zig").Scope = .{
        .fence = .{ .admission_epoch = 1, .attempt = 1, .transition_id = 7, .owner_group_id = 2, .peer_group_id = 3, .role = .merge_source, .namespace = db.core.identity_namespace, .catalog_digest = identity.catalog_digest },
        .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 },
        .consumer_epoch = 1,
        .copy_attempt = .{ .donor_term = 2, .sequence = 1 },
    };
    const command: types.BatchRequest = .{ .online_source = .{ .admit = .{ .scope = scope } } };
    try std.testing.expect(db.local_execution.index_structural_mutation_mutex.tryLock());
    var held = true;
    defer if (held) db.local_execution.index_structural_mutation_mutex.unlock();
    try std.testing.expectError(error.StorageBusy, server_test_adapter.applyOrdered(&db, command, .{ .term = 2, .index = 1 }));
    db.local_execution.index_structural_mutation_mutex.unlock();
    held = false;
    try std.testing.expect((try db.orderedApplyReceipt()) == null);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try @import("retained_effects.zig").load(&read)) == null);
        try std.testing.expect((try @import("source_pin_state.zig").load(&read)) == null);
    }
    try server_test_adapter.applyOrdered(&db, command, .{ .term = 2, .index = 1 });
    try std.testing.expectEqual(@as(u64, 1), (try db.orderedApplyReceipt()).?.index);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try @import("retained_effects.zig").load(&read)).?.active());
    }
}

test "db transaction batched acknowledgement migration preserves legacy replay and survives reopen" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db-ack-index");
    defer path_tmp.cleanup();
    const path = std.mem.span(path_tmp.path().ptr);
    defer cleanupTempDir(path_tmp.path().ptr);
    var db = try DB.open(alloc, path, .{ .start_index_workers = false });
    var opened = true;
    defer if (opened) db.close();
    const txn: transactions_mod.TxnId = @splat(53);
    const prefix = "\x00\x00__txn_participant_index_v1__:";
    var index_key: [prefix.len + 16]u8 = undefined;
    @memcpy(index_key[0..prefix.len], prefix);
    @memcpy(index_key[prefix.len..], &txn);
    _ = try db.beginReplicatedTransactionAtRaftEntry(txn, 10000, 10000, &.{ "a", "b", "c" }, false, false, .{ .term = 3, .index = 1 });
    try db.markReplicatedTransactionParticipantResolvedAtRaftEntry(txn, "a", .{ .term = 3, .index = 2 });
    {
        var read = try db.core.store.beginProbeTxn();
        defer read.abort();
        try std.testing.expectError(error.NotFound, read.get(&index_key));
    }
    try std.testing.expectError(error.InvalidParticipant, db.markReplicatedTransactionParticipantsResolvedAtRaftEntry(txn, &.{ "b", "absent" }, .{ .term = 3, .index = 3 }));
    try std.testing.expectEqual(@as(u64, 2), (try db.orderedApplyReceipt()).?.index);
    try db.markReplicatedTransactionParticipantsResolvedAtRaftEntry(txn, &.{"b"}, .{ .term = 3, .index = 3 });
    // Exact replay is fenced before payload admission, preserving the durable
    // marker together with the indexed membership and migrated resolution.
    try db.markReplicatedTransactionParticipantsResolvedAtRaftEntry(txn, &.{"absent"}, .{ .term = 3, .index = 3 });
    db.close();
    opened = false;
    db = try DB.open(alloc, path, .{ .start_index_workers = false });
    opened = true;
    try std.testing.expectEqual(@as(u64, 3), (try db.orderedApplyReceipt()).?.index);
    const pending = try db.getUnresolvedTransactionParticipants(alloc, txn);
    defer transactions_mod.freeParticipantList(alloc, pending);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    try std.testing.expectEqualStrings("c", pending[0]);
    try db.markReplicatedTransactionParticipantResolvedAtRaftEntry(txn, "c", .{ .term = 3, .index = 4 });
    const complete = try db.getUnresolvedTransactionParticipants(alloc, txn);
    defer transactions_mod.freeParticipantList(alloc, complete);
    try std.testing.expectEqual(@as(usize, 0), complete.len);
}

test "db transaction recovery observes admission replacement after execution binding" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("db");
    defer directory.cleanup();
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .transaction_recovery = .{ .enabled = true },
    });
    defer db.close();
    try engine.test_support.prepareTransactionRecoveryOwner(&db);
    const txn_id = try db.beginTransaction(1_000);
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:guarded", .value = "{\"title\":\"recovered\"}" }},
    });
    const Gate = struct {
        blocked: bool = true,
        calls: usize = 0,
        fn check(ptr: *const anyopaque) !void {
            const gate: *@This() = @constCast(@as(*const @This(), @ptrCast(@alignCast(ptr))));
            gate.calls += 1;
            if (gate.blocked) return error.TestRecoveryAdmissionClosed;
        }
    };
    var gate: Gate = .{};
    // Rebinding occurs after recovery has retained its execution capabilities.
    db.local_execution.replication_write_gate = .{ .primary = .{ .ptr = &gate, .check_fn = Gate.check } };
    const context = db.transaction_runtime.?.local.?.config;
    try std.testing.expectError(error.TestRecoveryAdmissionClosed, context.resolve_local_fn.?(context.local_resolution_ctx.?, txn_id, .committed, 2_000));
    try std.testing.expectEqual(@as(usize, 1), gate.calls);
    var intents = try db.core.collectTransactionIntentBatch(alloc, txn_id);
    defer intents.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), intents.writes.len);
    gate.blocked = false;
    // Exercise the normal bound resolver, not a hand-constructed test view.
    try context.resolve_local_fn.?(context.local_resolution_ctx.?, txn_id, .committed, 2_000);
    const value = (try db.get(alloc, "doc:guarded")).?;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("{\"title\":\"recovered\"}", value);
    try std.testing.expect(gate.calls > 1);
}

test "db ordered artifact inventory follower completes initial build under durable admission across reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const leader_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/admission-leader", .{tmp.sub_path});
    defer alloc.free(leader_path);
    const follower_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/admission-follower", .{tmp.sub_path});
    defer alloc.free(follower_path);
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var leader = try DB.open(alloc, leader_path, options);
    defer leader.close();
    try leader.setSchemaJson(alloc, "{}");
    try leader.addIndex(.{ .name = "admitted_text", .kind = .full_text, .config_json = "{}" });
    var command = try leader.artifactInventoryCommand(alloc);
    defer command.catalogs.deinit(alloc);
    var follower = try DB.open(alloc, follower_path, options);
    defer follower.close();
    try follower.setSchemaJson(alloc, "{}");
    try server_test_adapter.applyOrdered(&follower, .{ .writes = &.{.{ .key = "doc", .value = "{\"text\":\"retained follower source\"}" }} }, .{ .term = 1, .index = 1 });
    const owner = try follower.relationalTopologyIdentity();
    const scope: @import("db/online_source_contract.zig").Scope = .{
        .authority = .raft,
        .fence = .{ .role = .merge_source, .transition_id = 7, .attempt = 1, .admission_epoch = owner.next_epoch, .owner_group_id = 2, .peer_group_id = 3, .namespace = owner.namespace, .catalog_digest = owner.catalog_digest },
        .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 },
        .consumer_epoch = 1,
        .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
    };
    const request: types.BatchRequest = .{ .artifact_catalog = command, .online_source = .{ .admit = .{ .scope = scope, .artifact_catalog = command.binding } } };
    var saw_pending = false;
    var completed = false;
    for (0..256) |turn| {
        server_test_adapter.applyOrdered(&follower, request, .{ .term = 1, .index = 2 }) catch |err| {
            if (err != error.ArtifactCatalogDrift) return err;
            saw_pending = true;
            if (turn == 0) {
                follower.close();
                follower = try DB.open(alloc, follower_path, options);
                try std.testing.expectError(error.ArtifactCatalogEpochChanged, follower.advanceOrderedArtifactInitialBuild(&.{}, .{ .token = @splat(0xff) }));
            }
            continue;
        };
        completed = true;
        break;
    }
    try std.testing.expect(saw_pending);
    try std.testing.expect(completed);
    try server_test_adapter.applyOrdered(&follower, request, .{ .term = 1, .index = 2 });
    try std.testing.expect((try follower.artifactInventoryStatus()).ready);
    try std.testing.expectEqual(@as(u64, 2), (try follower.orderedApplyReceipt()).?.index);
    const document = (try follower.get(alloc, "doc")).?;
    defer alloc.free(document);
    try std.testing.expect(std.mem.indexOf(u8, document, "retained follower source") != null);
    var result = try follower.search(alloc, .{ .index_name = "admitted_text", .query = .{ .match = .{ .field = "_all", .text = "retained" } }, .limit = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
}
