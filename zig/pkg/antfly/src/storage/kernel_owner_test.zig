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
const abi = @import("kernel_owner_abi");
const error_identity = @import("kernel_error_identity");
const local_query_client = @import("local_query_client");
const client = @import("kernel_owner_client.zig");
const wal_client = @import("kernel_wal_client.zig");
const data_apply_client = @import("data_raft_apply_client.zig");
const metadata_apply_client = @import("metadata_raft_apply_client.zig");

test "local query identity relay preserves origin and attributes protocol defects to consumer" {
    const failure = error_identity.failureFromError(
        error.InvalidQueryRequest,
        .local_query,
        abi.abi_version,
        @intFromEnum(abi.LocalQueryOperation.parse_internal_request),
    );
    var forwarded: abi.FailureIdentity = .{};
    try local_query_client.acceptProviderFailure(
        failure.status,
        failure,
        .validate_provider_response,
        &forwarded,
    );
    try std.testing.expectEqualDeep(failure, forwarded);

    var malformed = failure;
    malformed.operation = 0;
    var replacement: abi.FailureIdentity = .{};
    try std.testing.expectError(
        error.InvalidBoundaryFailureIdentity,
        local_query_client.acceptProviderFailure(
            malformed.status,
            malformed,
            .validate_provider_response,
            &replacement,
        ),
    );
    try std.testing.expectEqual(abi.Status.invalid_boundary_failure_identity, replacement.status);
    try std.testing.expectEqual(abi.FailureBoundary.storage_owner, replacement.boundary);
    try std.testing.expectEqual(abi.abi_version, replacement.boundary_version);
    try std.testing.expectEqual(
        @intFromEnum(abi.LocalQueryOperation.validate_provider_response),
        replacement.operation,
    );
    try std.testing.expectEqualStrings("InvalidBoundaryFailureIdentity", replacement.errorName());
}

test "HA seed storage-owner boundary preserves exact operational errors" {
    try std.testing.expectError(
        error.InvalidArgument,
        client.haSeedActivate("{"),
    );
    try std.testing.expectError(
        error.InvalidStagingRoot,
        client.haSeedActivate(
            \\{"staging_root":"relative","target_root":"/valid","expected":{"generation":"gen","slot_name":"slot","identity":{"cluster_id":1,"timeline_id":1,"epoch":1}}}
        ),
    );
}

fn cleanup(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
}

const TestWalOptions = struct {
    const Backend = enum { lmdb, lsm, lsm_memory };
    const CommitBackend = enum { sync, worker_thread, async_io, adaptive };
    const Empty = struct {};
    const Hook = struct { ctx: ?*anyopaque = null };

    backend: ?Backend = null,
    storage: ?*anyopaque = null,
    lsm_options: Empty = .{},
    clock: Hook = .{},
    commit_scheduler: Hook = .{},
    artificial_sync_delay_ns: u64 = 0,
    group_commit_window_ns: u64 = 0,
    group_commit_max_requests: usize = 64,
    commit_backend: CommitBackend = .adaptive,
    no_sync: bool = false,
    read_only: bool = false,
    model_commit_backend_completions: bool = false,

    pub fn resolvedBackend(self: @This()) Backend {
        return self.backend orelse .lsm;
    }
};

test "opaque WAL preserves durable operations and exact failure identity" {
    const root = "/tmp/antfly-storage-kernel-wal-owner";
    const path = root ++ "/wal";
    const bootstrap_path = root ++ "/bootstrap";
    const read_only_path = root ++ "/read-only";
    cleanup(root);
    defer cleanup(root);

    const path_z = try std.testing.allocator.dupeZ(u8, path);
    defer std.testing.allocator.free(path_z);
    var wal = try wal_client.WAL.open(path_z.ptr, TestWalOptions{});
    defer wal.close();
    try std.testing.expectEqual(@as(u64, 1), try wal.append("alpha"));
    try std.testing.expectEqual(@as(u64, 2), try wal.append("beta"));
    try std.testing.expectEqual(@as(u64, 2), wal.lastLsn());

    const entries = try wal.iterateFrom(std.testing.allocator, 1);
    defer {
        for (entries) |entry| std.testing.allocator.free(@constCast(entry.data));
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqualStrings("alpha", entries[0].data);
    try std.testing.expectEqualStrings("beta", entries[1].data);

    const second = (try wal.readAt(std.testing.allocator, 2)).?;
    defer std.testing.allocator.free(@constCast(second.data));
    try std.testing.expectEqualStrings("beta", second.data);
    try wal.truncate(1);
    try std.testing.expect((try wal.readAt(std.testing.allocator, 1)) == null);

    try std.testing.expectError(error.WalLsnMismatch, wal.appendAt(9, "must-not-append"));
    try wal.truncateAfter(1);
    try std.testing.expectEqual(@as(u64, 2), try wal.append("gamma"));
    const stats = wal.statsSnapshot();
    try std.testing.expectEqual(@as(u64, 3), stats.append_calls);
    try std.testing.expectEqual(@as(u64, 3), stats.logical_entries);
    try std.testing.expectError(error.Overflow, wal.truncateAfter(std.math.maxInt(u64)));

    const bootstrap_z = try std.testing.allocator.dupeZ(u8, bootstrap_path);
    defer std.testing.allocator.free(bootstrap_z);
    var bootstrap = try wal_client.WAL.open(bootstrap_z.ptr, TestWalOptions{});
    defer bootstrap.close();
    try std.testing.expectEqual(@as(u64, 7), try bootstrap.appendAt(7, "timeline"));

    const read_only_z = try std.testing.allocator.dupeZ(u8, read_only_path);
    defer std.testing.allocator.free(read_only_z);
    {
        var writable = try wal_client.WAL.open(read_only_z.ptr, TestWalOptions{});
        defer writable.close();
        _ = try writable.append("durable");
    }
    var read_only = try wal_client.WAL.open(read_only_z.ptr, TestWalOptions{ .read_only = true });
    defer read_only.close();
    try std.testing.expectError(error.ReadOnly, read_only.append("rejected"));

    try std.testing.expectError(
        error.UnsupportedKernelWalOptions,
        wal_client.WAL.open(path_z.ptr, TestWalOptions{ .backend = .lsm_memory }),
    );
}

test "coarse aggregation ABI preserves results and semantic error identities" {
    const hit_bodies = [_][]const u8{
        "{\"category\":\"alpha\",\"price\":10}",
        "{\"category\":\"alpha\",\"price\":20}",
        "{\"category\":\"beta\",\"price\":30}",
    };
    var hits: [hit_bodies.len]client.AggregationHit = undefined;
    for (hit_bodies, 0..) |body, i| hits[i] = .{ .stored_data = .fromSlice(body) };

    const base = client.AggregationRequest{
        .total_hits = hit_bodies.len,
        .context_json = .fromSlice("{}"),
        .hits = &hits,
        .hit_count = hits.len,
    };
    var response = try client.aggregate(.{
        .total_hits = base.total_hits,
        .aggregations_json = .fromSlice("{\"by_category\":{\"type\":\"terms\",\"field\":\"category\",\"size\":10}}"),
        .context_json = base.context_json,
        .hits = base.hits,
        .hit_count = base.hit_count,
    });
    defer response.deinit();
    try std.testing.expect(std.mem.indexOf(u8, response.bytes(), "by_category") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.bytes(), "alpha") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.bytes(), "beta") != null);

    try std.testing.expectError(error.InvalidAggregation, client.aggregate(.{
        .total_hits = base.total_hits,
        .aggregations_json = .fromSlice("{\"bad\":{\"type\":\"histogram\",\"field\":\"price\",\"interval\":0}}"),
        .context_json = base.context_json,
        .hits = base.hits,
        .hit_count = base.hit_count,
    }));
    try std.testing.expectError(error.UnsupportedAggregation, client.aggregate(.{
        .total_hits = base.total_hits,
        .aggregations_json = .fromSlice("{\"unsupported_without_text_context\":{\"type\":\"significant_terms\",\"field\":\"category\",\"size\":10}}"),
        .context_json = base.context_json,
        .hits = base.hits,
        .hit_count = base.hit_count,
    }));
}

test "opaque storage context owns Lite system namespaces auth and table owners" {
    const root = "/tmp/antfly-storage-kernel-context-lite";
    const lite_path = root ++ "/standalone.aflite";
    const auth_path = root ++ "/auth";
    cleanup(root);
    defer cleanup(root);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try std.Io.Dir.cwd().createDirPath(io_impl.io(), root);

    var context = client.Context{};
    try context.ensureWith(.{
        .storage_kind = .lite,
        .storage_path = .fromSlice(lite_path),
        .auth_storage_path = .fromSlice(auth_path),
    });

    var catalog = try context.systemStore(std.testing.allocator, "system/metadata");
    var write = try catalog.beginWrite();
    try write.put("catalog", "{\"epoch\":2}");
    try write.commit();
    var read = try catalog.beginRead();
    try std.testing.expectEqualStrings("{\"epoch\":2}", try read.get("catalog"));
    read.abort();

    var auth_users = try context.systemStore(std.testing.allocator, "system/auth-users");
    var users = try client.singleNamespaceStore(
        std.testing.allocator,
        &auth_users,
        "usermgr_users",
    );
    var auth_write = try users.beginWrite();
    try auth_write.put(.{ .name = "usermgr_users" }, "userpass:admin", "hash");
    try auth_write.commit();
    var auth_read = try users.beginRead();
    var auth_cursor = try auth_read.openCursor(.{ .name = "usermgr_users" });
    const first = (try auth_cursor.first()).?;
    try std.testing.expectEqualStrings("userpass:admin", first.key);
    try std.testing.expectEqualStrings("hash", first.value);
    auth_cursor.close();
    auth_read.abort();

    var owner = try client.Owner.open(.{
        .context = context.handle,
        .path = .fromSlice("group-7001/table-db"),
        .table_name = .fromSlice("docs"),
        .group_id = 7001,
        .has_identity_namespace = 1,
        .identity_table_id = 7,
        .identity_shard_id = 7001,
        .identity_range_id = 7001,
    });
    var response = try owner.batchJson(
        "docs",
        "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\"}},\"sync_level\":\"full_index\"}",
    );
    try std.testing.expect(std.mem.indexOf(u8, response.bytes(), "\"inserted\":1") != null);
    response.deinit();
    try std.testing.expectEqual(
        abi.Status.busy,
        abi.antfly_storage_context_destroy(context.handle),
    );

    const maintenance_status = context.maintenanceSource().status();
    try std.testing.expectEqualStrings("lite", maintenance_status.engine);
    try std.testing.expect(maintenance_status.maintenance.check);

    owner.deinit();
    users.deinit();
    auth_users.deinit();
    catalog.deinit();
    context.deinit();
}

test "opaque storage owner performs coarse batch and query on one live DB" {
    const path = "/tmp/antfly-storage-kernel-owner-batch-query";
    const backup_root = "/tmp/antfly-storage-kernel-owner-backups";
    cleanup(path);
    cleanup(backup_root);
    defer cleanup(path);
    defer cleanup(backup_root);

    var owner = try client.Owner.open(.{
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .group_id = 7001,
        .lsm_root_generation = 0,
        .has_identity_namespace = 1,
        .identity_table_id = 7,
        .identity_shard_id = 7001,
        .identity_range_id = 7001,
    });
    defer owner.deinit();

    var duplicate_owner: ?*anyopaque = null;
    try std.testing.expectEqual(abi.Status.lsm_root_writer_already_open, abi.antfly_storage_owner_open(&.{
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .has_identity_namespace = 1,
        .identity_table_id = 7,
        .identity_shard_id = 7001,
        .identity_range_id = 7001,
    }, &duplicate_owner));
    try std.testing.expect(duplicate_owner == null);

    const batch_json =
        \\{"inserts":{"doc:a":{"title":"alpha"},"doc:b":{"title":"beta"}},"sync_level":"full_index"}
    ;
    var batch_response = try owner.batchJson("docs", batch_json);
    defer batch_response.deinit();
    try std.testing.expect(std.mem.indexOf(u8, batch_response.bytes(), "\"inserted\":2") != null);

    var status_response = try owner.runtimeStatusJson("docs");
    defer status_response.deinit();
    try std.testing.expect(std.mem.indexOf(u8, status_response.bytes(), "\"source_doc_count\":2") != null);

    const maintenance = try owner.maintenance("docs", .inspect);
    try std.testing.expectEqual(abi.abi_version, maintenance.version);
    try std.testing.expectEqual(@as(u8, 0), maintenance.progressed);
    try std.testing.expectError(error.InvalidArgument, owner.maintenance("articles", .inspect));
    const invalid_maintenance = abi.MaintenanceRequest{
        .action = std.math.maxInt(u32),
        .table_name = .fromSlice("docs"),
    };
    var invalid_maintenance_result: abi.MaintenanceResult = .{};
    try std.testing.expectEqual(
        abi.Status.invalid_argument,
        abi.antfly_storage_owner_maintenance(owner.handle, &invalid_maintenance, &invalid_maintenance_result),
    );

    var replicated_response = try owner.replicatedBatchJson(
        "docs",
        "{\"inserts\":{\"doc:c\":{\"title\":\"gamma\"}},\"sync_level\":\"full_index\"}",
    );
    defer replicated_response.deinit();
    try std.testing.expect(std.mem.indexOf(u8, replicated_response.bytes(), "\"inserted\":1") != null);
    try std.testing.expectError(error.InvalidBatchRequest, owner.batchJson("docs", "{"));
    try std.testing.expectError(error.InvalidBatchRequest, owner.replicatedBatchJson("docs", "{"));
    try owner.waitForSync("docs", .full_index);
    try owner.applyHAReplicationRecord("docs", .{
        .record_kind = 0x0012,
        .payload_codec = 0,
        .cluster_id = 1,
        .shard_id = 7001,
        .table_id = 7,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 1,
        .previous_lsn = 0,
    });

    const txn_id: [16]u8 = .{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    var txn_begin = try owner.replicatedBatchJson(
        "docs",
        "{\"_transaction\":{\"phase\":\"begin\",\"txn_id\":\"000102030405060708090a0b0c0d0e0f\",\"begin_timestamp\":\"42\",\"created_at_ns\":\"43\",\"topology_epoch\":\"7\",\"participants\":[\"table2:00000004:docs:7001\"]},\"sync_level\":\"write\"}",
    );
    defer txn_begin.deinit();
    try std.testing.expectEqual(abi.TxnStatus.pending, try owner.transactionStatus("docs", txn_id));
    var txn_prepare = try owner.replicatedBatchJson(
        "docs",
        "{\"inserts\":{\"doc:txn\":{\"title\":\"transactional\"}},\"_transaction\":{\"phase\":\"prepare\",\"txn_id\":\"000102030405060708090a0b0c0d0e0f\",\"topology_epoch\":\"7\"},\"sync_level\":\"write\"}",
    );
    defer txn_prepare.deinit();
    var txn_resolve = try owner.replicatedBatchJson(
        "docs",
        "{\"_transaction\":{\"phase\":\"resolve\",\"txn_id\":\"000102030405060708090a0b0c0d0e0f\",\"status\":\"committed\",\"commit_version\":\"44\"},\"sync_level\":\"full_index\"}",
    );
    defer txn_resolve.deinit();
    try std.testing.expectEqual(abi.TxnStatus.committed, try owner.transactionStatus("docs", txn_id));

    const query_json =
        \\{"query":{"match_all":{}},"limit":10}
    ;
    try std.testing.expectError(error.InvalidArgument, owner.queryJson("articles", query_json));
    try std.testing.expectError(error.InvalidQueryRequest, owner.queryJson("docs", "{"));

    // The nested distributed -> storage-owner -> local-query path must retain
    // both the semantic status and its originating stage, not merely rethrow a
    // broad failure after the inner provider unwinds.
    var invalid_query_response: abi.OwnedBytes = .{};
    var invalid_query_failure: abi.FailureIdentity = .{};
    const invalid_query_status = abi.antfly_storage_owner_query_json(
        owner.handle,
        &.{
            .table_name = .fromSlice("docs"),
            .request_json = .fromSlice("{"),
        },
        &invalid_query_response,
        &invalid_query_failure,
    );
    try std.testing.expectEqual(abi.Status.invalid_query, invalid_query_status);
    try std.testing.expectEqual(invalid_query_status, invalid_query_failure.status);
    try std.testing.expectEqual(abi.FailureBoundary.local_query, invalid_query_failure.boundary);
    try std.testing.expectEqual(abi.abi_version, invalid_query_failure.boundary_version);
    try std.testing.expectEqual(
        @intFromEnum(abi.LocalQueryOperation.parse_internal_request),
        invalid_query_failure.operation,
    );
    try std.testing.expectEqualStrings("InvalidQueryRequest", invalid_query_failure.errorName());
    try std.testing.expect(invalid_query_failure.error_name_hash != 0);
    try std.testing.expectEqual(@as(usize, 0), invalid_query_response.len);

    var invalid_abi_response: abi.OwnedBytes = .{};
    var invalid_abi_failure: abi.FailureIdentity = .{};
    var invalid_abi_request = abi.LocalQueryRequest{};
    invalid_abi_request.version = abi.abi_version - 1;
    const invalid_abi_status = abi.antfly_local_query_execute(
        &invalid_abi_request,
        &invalid_abi_response,
        &invalid_abi_failure,
    );
    try std.testing.expectEqual(abi.Status.invalid_abi, invalid_abi_status);
    try std.testing.expectEqual(invalid_abi_status, invalid_abi_failure.status);
    try std.testing.expectEqual(abi.FailureBoundary.local_query, invalid_abi_failure.boundary);
    try std.testing.expectEqual(
        @intFromEnum(abi.LocalQueryOperation.validate_request),
        invalid_abi_failure.operation,
    );
    try std.testing.expectEqualStrings("InvalidAbiVersion", invalid_abi_failure.errorName());

    // Every operation family crossing the storage-owner boundary carries the
    // same complete envelope even when it remains in the storage unit.
    var operation_response: abi.OwnedBytes = .{};
    defer abi.antfly_storage_owner_buffer_destroy(&operation_response);
    var operation_failure: abi.FailureIdentity = .{};
    const invalid_algebraic_status = abi.antfly_storage_owner_algebraic_partials_json(
        owner.handle,
        &.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice("{") },
        &operation_response,
        &operation_failure,
    );
    try std.testing.expect(invalid_algebraic_status != .ok);
    try std.testing.expectEqual(invalid_algebraic_status, operation_failure.status);
    try std.testing.expectEqual(abi.FailureBoundary.local_query, operation_failure.boundary);
    try std.testing.expectEqual(abi.abi_version, operation_failure.boundary_version);
    try std.testing.expectEqual(
        @intFromEnum(abi.LocalQueryOperation.algebraic_partials),
        operation_failure.operation,
    );
    try std.testing.expect(operation_failure.error_name_hash != 0);

    const invalid_text_stats_status = abi.antfly_storage_owner_text_stats_json(
        owner.handle,
        &.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice("{") },
        &operation_response,
        &operation_failure,
    );
    try std.testing.expect(invalid_text_stats_status != .ok);
    try std.testing.expectEqual(invalid_text_stats_status, operation_failure.status);
    try std.testing.expectEqual(abi.FailureBoundary.local_query, operation_failure.boundary);
    try std.testing.expectEqual(abi.abi_version, operation_failure.boundary_version);
    try std.testing.expectEqual(
        @intFromEnum(abi.LocalQueryOperation.text_stats),
        operation_failure.operation,
    );
    try std.testing.expect(operation_failure.error_name_hash != 0);

    const invalid_preflight_status = abi.antfly_storage_owner_preflight_json(
        owner.handle,
        &.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice("{") },
        &operation_response,
        &operation_failure,
    );
    try std.testing.expect(invalid_preflight_status != .ok);
    try std.testing.expectEqual(invalid_preflight_status, operation_failure.status);
    try std.testing.expectEqual(abi.FailureBoundary.local_query, operation_failure.boundary);
    try std.testing.expectEqual(abi.abi_version, operation_failure.boundary_version);
    try std.testing.expectEqual(
        @intFromEnum(abi.LocalQueryOperation.preflight),
        operation_failure.operation,
    );
    try std.testing.expect(operation_failure.error_name_hash != 0);

    const invalid_graph_status = abi.antfly_storage_owner_graph_expand_json(
        owner.handle,
        &.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice("{") },
        &operation_response,
        &operation_failure,
    );
    try std.testing.expect(invalid_graph_status != .ok);
    try std.testing.expectEqual(invalid_graph_status, operation_failure.status);
    try std.testing.expectEqual(abi.FailureBoundary.local_query, operation_failure.boundary);
    try std.testing.expectEqual(abi.abi_version, operation_failure.boundary_version);
    try std.testing.expectEqual(
        @intFromEnum(abi.LocalQueryOperation.parse_graph_expand),
        operation_failure.operation,
    );
    try std.testing.expect(operation_failure.error_name_hash != 0);

    const invalid_aggregation_status = abi.antfly_storage_aggregate_json(
        &.{ .context_json = .fromSlice("{"), .aggregations_json = .fromSlice("[]") },
        &operation_response,
        &operation_failure,
    );
    try std.testing.expect(invalid_aggregation_status != .ok);
    try std.testing.expectEqual(abi.FailureBoundary.storage_owner, operation_failure.boundary);
    try std.testing.expectEqual(
        @intFromEnum(abi.LocalQueryOperation.parse_aggregation),
        operation_failure.operation,
    );

    var query_response = try owner.queryJson("docs", query_json);
    defer query_response.deinit();
    try std.testing.expect(std.mem.indexOf(u8, query_response.bytes(), "docs") != null);
    try std.testing.expect(std.mem.indexOf(u8, query_response.bytes(), "doc:a") != null);
    try std.testing.expect(std.mem.indexOf(u8, query_response.bytes(), "doc:b") != null);
    try std.testing.expect(std.mem.indexOf(u8, query_response.bytes(), "doc:txn") != null);

    var reconciled = false;
    for (0..64) |_| {
        const result = try owner.reconcile(
            "docs",
            "",
            "{\"full_text_index_v1\":{\"type\":\"full_text\"}}",
            "full_text_index_v1",
            true,
        );
        try std.testing.expect(result.state != .degraded);
        if (result.state == .complete) {
            reconciled = true;
            break;
        }
    }
    try std.testing.expect(reconciled);
    var text_response = try owner.queryJson(
        "docs",
        "{\"full_text_search\":{\"match\":\"alpha\",\"field\":\"title\"},\"indexes\":[\"full_text_index_v1\"],\"limit\":10}",
    );
    defer text_response.deinit();
    try std.testing.expect(std.mem.indexOf(u8, text_response.bytes(), "doc:a") != null);
    try std.testing.expect(std.mem.indexOf(u8, text_response.bytes(), "doc:b") == null);

    try std.testing.expectError(
        error.InvalidArgument,
        owner.configure("articles", "", "{}"),
    );
    const replacement_indexes_json =
        \\{"dense_idx":{"type":"embeddings","external":true,"dimension":3},
        \\ "full_text_index_v0":{"type":"full_text","enrichments":[{"name":"document_units_v1","kind":"asset","field":"url","content_type":"application/json","producer_json":"{\"type\":\"document_extraction\",\"config\":{}}"}]}}
    ;
    try owner.configure("docs", "", replacement_indexes_json);
    var dense_reconciled = false;
    for (0..64) |_| {
        const result = try owner.reconcile(
            "docs",
            "",
            replacement_indexes_json,
            "dense_idx",
            true,
        );
        try std.testing.expect(result.state != .degraded);
        if (result.state == .complete) {
            dense_reconciled = true;
            break;
        }
    }
    try std.testing.expect(dense_reconciled);
    var indexed_batch = try owner.batchJson(
        "docs",
        "{\"inserts\":{\"doc:artifact\":{\"title\":\"artifact\",\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YQ==\"},\"doc:c\":{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[1,0,0]}},\"doc:d\":{\"title\":\"delta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}},\"sync_level\":\"full_index\"}",
    );
    defer indexed_batch.deinit();
    var text_memory = try owner.textMemoryJson("docs");
    defer text_memory.deinit();
    try std.testing.expect(std.mem.indexOf(u8, text_memory.bytes(), "\"text_indexes\":1") != null);
    var dense_response = try owner.queryJson(
        "docs",
        "{\"embeddings\":{\"dense_idx\":[1,0,0]},\"indexes\":[\"dense_idx\"],\"limit\":2}",
    );
    defer dense_response.deinit();
    const doc_c = std.mem.indexOf(u8, dense_response.bytes(), "doc:c") orelse return error.MissingDenseHit;
    const doc_d = std.mem.indexOf(u8, dense_response.bytes(), "doc:d") orelse return error.MissingDenseHit;
    try std.testing.expect(doc_c < doc_d);

    var reprocessed = try owner.artifactOperationJson(
        "docs",
        .reprocess_document,
        "{\"doc_key\":\"doc:artifact\",\"artifact_name\":\"document_units_v1\"}",
        null,
        null,
        false,
    );
    defer reprocessed.deinit();
    try std.testing.expect(std.mem.indexOf(u8, reprocessed.bytes(), "\"handled\":true") != null);

    var reprocessed_range = try owner.artifactOperationJson(
        "docs",
        .reprocess_document_range,
        "{\"artifact_name\":\"document_units_v1\",\"request\":{\"from_key\":\"doc:artifact\",\"to_key\":\"doc:b\",\"limit\":1}}",
        null,
        null,
        false,
    );
    defer reprocessed_range.deinit();
    try std.testing.expect(std.mem.indexOf(u8, reprocessed_range.bytes(), "\"reprocessed\":1") != null);

    var placement = try owner.artifactOperationJson(
        "docs",
        .update_child_range_placement,
        "{\"doc_key\":\"doc:artifact\",\"artifact_name\":\"document_units_v1\",\"update\":{\"range_id\":\"range:000000\",\"placement\":\"remote\",\"owner_group_id\":7002,\"placement_generation\":3,\"route_status\":\"remote_committed\",\"split_eligible\":true}}",
        null,
        null,
        false,
    );
    defer placement.deinit();
    try std.testing.expect(std.mem.indexOf(u8, placement.bytes(), "\"handled\":true") != null);

    const ChildRangeCapture = struct {
        calls: usize = 0,
        owner_group_id: u64 = 0,
        saw_document: bool = false,
        saw_artifact: bool = false,

        fn dispatch(
            ptr: ?*anyopaque,
            owner_group_id: u64,
            request_json: abi.BorrowedBytes,
        ) callconv(.c) abi.Status {
            const self: *@This() = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
            self.calls += 1;
            self.owner_group_id = owner_group_id;
            self.saw_document = std.mem.indexOf(u8, request_json.slice(), "\"doc_key\":\"doc:artifact\"") != null;
            self.saw_artifact = std.mem.indexOf(u8, request_json.slice(), "\"artifact_name\":\"document_units_v1\"") != null;
            return .ok;
        }
    };
    var child_range_capture = ChildRangeCapture{};
    var routed_batch = try owner.batchJsonWithDocumentChildRangeDispatcher(
        "docs",
        "{\"inserts\":{\"doc:artifact\":{\"title\":\"artifact updated\",\"url\":\"data:text/plain;base64,YmV0YQ==\"}},\"sync_level\":\"full_index\"}",
        &child_range_capture,
        ChildRangeCapture.dispatch,
    );
    defer routed_batch.deinit();
    try std.testing.expectEqual(@as(usize, 1), child_range_capture.calls);
    try std.testing.expectEqual(@as(u64, 7002), child_range_capture.owner_group_id);
    try std.testing.expect(child_range_capture.saw_document);
    try std.testing.expect(child_range_capture.saw_artifact);

    var empty_child_batch = try owner.artifactOperationJson(
        "docs",
        .apply_child_range_batch,
        "{\"doc_key\":\"doc:artifact\",\"artifact_name\":\"document_units_v1\",\"batch\":{}}",
        null,
        null,
        false,
    );
    defer empty_child_batch.deinit();
    try std.testing.expect(std.mem.indexOf(u8, empty_child_batch.bytes(), "\"sequence\":0") != null);

    var corrupted = try owner.artifactOperationJson(
        "docs",
        .corrupt_embedding,
        "{\"doc_key\":\"doc:c\",\"index_name\":\"dense_idx\"}",
        null,
        null,
        false,
    );
    defer corrupted.deinit();
    var repair_issues = try owner.artifactOperationJson(
        "docs",
        .list_repair_issues,
        "{\"artifact_kind\":\"embedding\",\"limit\":10}",
        null,
        null,
        false,
    );
    defer repair_issues.deinit();
    try std.testing.expect(std.mem.indexOf(u8, repair_issues.bytes(), "\"issues\"") != null);

    const Cancel = struct {
        fn requested(_: ?*anyopaque) callconv(.c) u8 {
            return 1;
        }
    };
    try std.testing.expectError(error.Canceled, owner.artifactOperationJson(
        "docs",
        .repair_issues,
        "{\"artifact_kind\":\"embedding\",\"limit\":10}",
        null,
        Cancel.requested,
        false,
    ));
    var repaired = try owner.artifactOperationJson(
        "docs",
        .repair_issues,
        "{\"artifact_kind\":\"embedding\",\"limit\":10}",
        null,
        null,
        false,
    );
    defer repaired.deinit();
    try std.testing.expect(std.mem.indexOf(u8, repaired.bytes(), "\"scanned\":0") != null);

    try std.testing.expectError(error.InvalidArgument, owner.beginBulkIngest("articles"));
    try owner.beginBulkIngest("docs");
    var bulk_batch = try owner.batchJson(
        "docs",
        "{\"inserts\":{\"doc:bulk\":{\"title\":\"bulk\"}},\"sync_level\":\"write\"}",
    );
    defer bulk_batch.deinit();
    try owner.finishBulkIngest(&.{
        .compact = 0,
        .table_name = .fromSlice("docs"),
    });
    var bulk_query = try owner.queryJson(
        "docs",
        "{\"query\":{\"match_all\":{}},\"limit\":10}",
    );
    defer bulk_query.deinit();
    try std.testing.expect(std.mem.indexOf(u8, bulk_query.bytes(), "doc:bulk") != null);

    try owner.beginBulkIngest("docs");
    try owner.abortBulkIngest("docs");
    var post_abort_batch = try owner.batchJson(
        "docs",
        "{\"inserts\":{\"doc:after-abort\":{\"title\":\"ordinary\"}},\"sync_level\":\"full_index\"}",
    );
    defer post_abort_batch.deinit();

    var portable_backup = try owner.backupJson(
        "docs",
        backup_root,
        "portable-owner",
        .portable,
    );
    defer portable_backup.deinit();
    try std.testing.expect(std.mem.indexOf(u8, portable_backup.bytes(), "\"group_id\":7001") != null);
    try std.testing.expect(std.mem.indexOf(u8, portable_backup.bytes(), "portable-owner.afb") != null);
    try std.testing.expect(std.mem.indexOf(u8, portable_backup.bytes(), "\"artifact_sha256\"") != null);

    var native_backup = try owner.backupJson(
        "docs",
        backup_root,
        "native-owner",
        .native,
    );
    defer native_backup.deinit();
    try std.testing.expect(std.mem.indexOf(u8, native_backup.bytes(), "\"group_id\":7001") != null);
    try std.testing.expect(std.mem.indexOf(u8, native_backup.bytes(), "native-owner") != null);
    try std.testing.expectError(
        error.InvalidArgument,
        owner.backupJson("articles", backup_root, "wrong-table", .native),
    );
}

test "opaque storage owner validates ABI and destruction is idempotent" {
    var invalid_context: ?*anyopaque = undefined;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_context_create(&.{ .version = abi.abi_version + 1 }, &invalid_context),
    );
    try std.testing.expect(invalid_context == null);
    try std.testing.expectEqual(abi.Status.ok, abi.antfly_storage_context_destroy(null));

    var owner: ?*anyopaque = undefined;
    var invalid: abi.OpenRequest = .{};
    invalid.version = abi.abi_version + 1;
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_open(&invalid, &owner));
    try std.testing.expect(owner == null);

    var invalid_configure: abi.ConfigureRequest = .{};
    invalid_configure.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_configure(null, &invalid_configure),
    );

    var invalid_reconcile: abi.ReconcileRequest = .{};
    invalid_reconcile.version = abi.abi_version + 1;
    var reconcile_result: abi.ReconcileResult = .{};
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_reconcile(null, &invalid_reconcile, &reconcile_result),
    );
    try std.testing.expectEqual(abi.abi_version, reconcile_result.version);

    var invalid_table: abi.TableRequest = .{};
    invalid_table.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_bulk_begin(null, &invalid_table),
    );
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_bulk_abort(null, &invalid_table),
    );
    var invalid_txn_status: abi.TransactionStatusRequest = .{};
    invalid_txn_status.version = abi.abi_version + 1;
    var txn_status_result: abi.TransactionStatusResult = .{};
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_transaction_status(null, &invalid_txn_status, &txn_status_result),
    );
    try std.testing.expectEqual(abi.abi_version, txn_status_result.version);
    var invalid_bulk_finish: abi.BulkFinishRequest = .{};
    invalid_bulk_finish.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_bulk_finish(null, &invalid_bulk_finish),
    );
    var invalid_maintenance: abi.MaintenanceRequest = .{};
    invalid_maintenance.version = abi.abi_version + 1;
    var maintenance_result: abi.MaintenanceResult = .{};
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_maintenance(null, &invalid_maintenance, &maintenance_result),
    );
    try std.testing.expectEqual(abi.abi_version, maintenance_result.version);
    try std.testing.expectEqual(
        abi.Status.invalid_argument,
        abi.antfly_storage_owner_maintenance(null, &.{}, &maintenance_result),
    );

    var response: abi.OwnedBytes = .{};
    var invalid_batch_operation: abi.BatchJsonOperationRequest = .{
        .table_name = .fromSlice("docs"),
        .request_json = .fromSlice("{}"),
    };
    invalid_batch_operation.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_batch_json(null, &invalid_batch_operation, &response),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    var invalid_operation: abi.JsonOperationRequest = .{
        .table_name = .fromSlice("docs"),
        .request_json = .fromSlice("{}"),
    };
    invalid_operation.version = abi.abi_version + 1;
    var invalid_sync: abi.SyncRequest = .{};
    invalid_sync.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_wait_for_sync(null, &invalid_sync),
    );
    var invalid_ha: abi.HAReplicationRecordRequest = .{};
    invalid_ha.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_apply_ha_replication_record(null, &invalid_ha),
    );
    var invalid_backup: abi.BackupRequest = .{};
    invalid_backup.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_backup_json(null, &invalid_backup, &response),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    const invalid_backup_format = abi.BackupRequest{ .format = std.math.maxInt(u32) };
    try std.testing.expectEqual(
        abi.Status.invalid_argument,
        abi.antfly_storage_owner_backup_json(null, &invalid_backup_format, &response),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    var snapshot: ?*anyopaque = undefined;
    var invalid_snapshot: abi.SnapshotPrepareRequest = .{};
    invalid_snapshot.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_snapshot_prepare(&invalid_snapshot, &snapshot),
    );
    try std.testing.expect(snapshot == null);
    var invalid_restore: abi.RestorePrepareRequest = .{};
    invalid_restore.version = abi.abi_version + 1;
    var restore_result: abi.RestorePrepareResult = .{ .snapshot = @ptrFromInt(1) };
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_restore_prepare(&invalid_restore, &restore_result),
    );
    try std.testing.expectEqual(abi.abi_version, restore_result.version);
    try std.testing.expect(restore_result.snapshot == null);
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_restore_reconcile(&invalid_restore),
    );
    var invalid_restore_bootstrap: abi.RestoreBootstrapRequest = .{};
    invalid_restore_bootstrap.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_restore_apply_bootstrap(&invalid_restore_bootstrap),
    );
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_restore_repair(null, &invalid_restore),
    );
    try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_snapshot_promote(null));
    var snapshot_publish_result: abi.SnapshotPublishResult = .{ .durability_uncertain = 1 };
    try std.testing.expectEqual(
        abi.Status.invalid_argument,
        abi.antfly_storage_snapshot_publish_prepared(null, &snapshot_publish_result),
    );
    try std.testing.expectEqual(@as(u8, 0), snapshot_publish_result.durability_uncertain);
    try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_snapshot_commit(null));
    try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_snapshot_rollback(null));
    abi.antfly_storage_snapshot_destroy(null);
    abi.antfly_storage_snapshot_destroy(null);
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_replicated_batch_json(null, &invalid_operation, &response),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    var query_failure: abi.FailureIdentity = .{};
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_preflight_json(null, &invalid_operation, &response, &query_failure),
    );
    try std.testing.expectEqual(abi.FailureBoundary.storage_owner, query_failure.boundary);
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_text_stats_json(null, &invalid_operation, &response, &query_failure),
    );
    try std.testing.expectEqual(abi.FailureBoundary.storage_owner, query_failure.boundary);
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_algebraic_partials_json(null, &invalid_operation, &response, &query_failure),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    const invalid_controlled = abi.ControlledJsonOperationRequest{ .version = abi.abi_version + 1 };
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_graph_expand_json(null, &invalid_controlled, &response, &query_failure));
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_graph_hydrate_json(null, &invalid_controlled, &response, &query_failure));
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_graph_edges_json(null, &invalid_controlled, &response, &query_failure));
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_document_artifact_manifest_json(null, &invalid_operation, &response));
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_document_artifact_manifests_json(null, &invalid_operation, &response));
    try std.testing.expectEqual(@as(u64, 0), response.len);
    const invalid_artifact_operation = abi.ArtifactOperationRequest{ .version = abi.abi_version + 1 };
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_artifact_operation_json(null, &invalid_artifact_operation, &response),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    const invalid_artifact_tag = abi.ArtifactOperationRequest{ .operation = std.math.maxInt(u32) };
    try std.testing.expectEqual(
        abi.Status.invalid_argument,
        abi.antfly_storage_owner_artifact_operation_json(null, &invalid_artifact_tag, &response),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_runtime_status_json(null, &invalid_operation, &response));
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_restore_state_json(null, &invalid_operation, &response));
    try std.testing.expectEqual(@as(u64, 0), response.len);

    try std.testing.expectError(error.InvalidQueryRequest, client.statusToError(.invalid_query));
    try std.testing.expectError(error.UnsupportedQueryRequest, client.statusToError(.unsupported_query));
    try std.testing.expectError(error.IndexNotFound, client.statusToError(.index_not_found));
    try std.testing.expectError(error.IdentityReadGenerationChanged, client.statusToError(.identity_read_generation_changed));
    try std.testing.expectError(error.Timeout, client.statusToError(.timeout));
    try std.testing.expectError(error.Cancelled, client.statusToError(.cancelled));
    try std.testing.expectError(
        error.DenseRepairBackpressure,
        client.statusToError(.dense_repair_backpressure),
    );

    var empty: abi.OwnedBytes = .{};
    abi.antfly_storage_owner_buffer_destroy(&empty);
    abi.antfly_storage_owner_buffer_destroy(&empty);
}

test "opaque storage owner transaction recovery crosses callback ABI" {
    const path = "/tmp/antfly-storage-kernel-owner-transaction-recovery";
    cleanup(path);
    defer cleanup(path);

    const Capture = struct {
        calls: std.atomic.Value(u32) = .init(0),

        fn resolve(
            ptr: ?*anyopaque,
            txn_id: *const abi.TxnId,
            participant: abi.BorrowedBytes,
            status: abi.TxnStatus,
            commit_version: u64,
        ) callconv(.c) abi.Status {
            const self: *@This() = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
            const expected_txn_id: [16]u8 = @splat(0x2a);
            if (!std.mem.eql(u8, &txn_id.bytes, &expected_txn_id) or
                !std.mem.eql(u8, participant.slice(), "table2:00000006:remote:9002") or
                status != .committed or commit_version != 102)
                return .invalid_argument;
            _ = self.calls.fetchAdd(1, .release);
            return .ok;
        }
    };
    var capture = Capture{};
    var owner = try client.Owner.open(.{
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .group_id = 9001,
        .transaction_recovery = .{
            .enabled = 1,
            .lease_owned = 1,
            .interval_ms = 10,
            .cutoff_ns = 1,
            .callback_ctx = &capture,
            .owner_id = .fromSlice("owner-test"),
            .resolve_participant_fn = Capture.resolve,
        },
    });
    defer owner.deinit();

    const txn_id: [16]u8 = @splat(0x2a);
    var begin = try owner.replicatedBatchJson(
        "docs",
        "{\"_transaction\":{\"phase\":\"begin\",\"txn_id\":\"2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a\",\"begin_timestamp\":\"100\",\"created_at_ns\":\"1\",\"topology_epoch\":\"0\",\"participants\":[\"table2:00000004:docs:9001\",\"table2:00000006:remote:9002\"]},\"sync_level\":\"write\"}",
    );
    begin.deinit();
    var resolve = try owner.replicatedBatchJson(
        "docs",
        "{\"_transaction\":{\"phase\":\"resolve\",\"txn_id\":\"2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a\",\"status\":\"committed\",\"commit_version\":\"102\"},\"sync_level\":\"write\"}",
    );
    resolve.deinit();

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    for (0..500) |_| {
        if (capture.calls.load(.acquire) > 0) break;
        try io_impl.io().sleep(.fromMilliseconds(2), .awake);
    }
    try std.testing.expectEqual(@as(u32, 1), capture.calls.load(.acquire));

    var cleaned = false;
    for (0..500) |_| {
        _ = owner.transactionStatus("docs", txn_id) catch |err| {
            if (err == error.TxnNotFound) {
                cleaned = true;
                break;
            }
            return err;
        };
        try io_impl.io().sleep(.fromMilliseconds(2), .awake);
    }
    try std.testing.expect(cleaned);
}

test "opaque storage context enforces owner lifetime and shares process storage state" {
    const first_path = "/tmp/antfly-storage-kernel-context-first";
    const second_path = "/tmp/antfly-storage-kernel-context-second";
    cleanup(first_path);
    cleanup(second_path);
    defer cleanup(first_path);
    defer cleanup(second_path);

    var context: ?*anyopaque = null;
    try std.testing.expectEqual(abi.Status.ok, abi.antfly_storage_context_create(&.{}, &context));
    try std.testing.expect(context != null);
    var metrics: abi.ContextMetricsResult = undefined;
    try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_context_metrics(null, &metrics));
    try std.testing.expectEqual(abi.Status.ok, abi.antfly_storage_context_metrics(context, &metrics));
    try std.testing.expectEqual(abi.abi_version, metrics.version);
    try std.testing.expectEqual(@as(u64, 0), metrics.lsm_cache_entry_count);

    var first = try client.Owner.open(.{
        .context = context,
        .path = .fromSlice(first_path),
        .table_name = .fromSlice("first"),
    });
    var second = try client.Owner.open(.{
        .context = context,
        .path = .fromSlice(second_path),
        .table_name = .fromSlice("second"),
    });
    try std.testing.expectEqual(abi.Status.busy, abi.antfly_storage_context_destroy(context));

    first.deinit();
    try std.testing.expectEqual(abi.Status.busy, abi.antfly_storage_context_destroy(context));
    second.deinit();
    try std.testing.expectEqual(abi.Status.ok, abi.antfly_storage_context_destroy(context));
}

test "opaque data raft apply owner preserves batch snapshot and placement lifecycle" {
    const source_root = "/tmp/antfly-storage-kernel-data-apply-source";
    const restored_root = "/tmp/antfly-storage-kernel-data-apply-restored";
    const authoritative_root = "/tmp/antfly-storage-kernel-data-apply-authoritative";
    cleanup(source_root);
    cleanup(restored_root);
    cleanup(authoritative_root);
    defer cleanup(source_root);
    defer cleanup(restored_root);
    defer cleanup(authoritative_root);

    var context = client.Context{};
    try context.ensure();
    defer context.deinit();

    var invalid_store: ?*anyopaque = @ptrFromInt(1);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_data_apply_store_open(&.{
        .version = abi.abi_version + 1,
        .root_dir = .fromSlice(source_root),
    }, &invalid_store));
    try std.testing.expect(invalid_store == null);

    var source = try data_apply_client.RaftApplyStore.init(std.testing.allocator, .{
        .root_dir = source_root,
        .context = context.handle,
    });
    defer source.deinit();

    const payload = "opaque-data-apply";
    var encoded: [4 + 8 + 8 + 1 + 4 + payload.len]u8 = undefined;
    var pos: usize = 0;
    std.mem.writeInt(u32, encoded[pos..][0..4], 1, .little);
    pos += 4;
    std.mem.writeInt(u64, encoded[pos..][0..8], 4, .little);
    pos += 8;
    std.mem.writeInt(u64, encoded[pos..][0..8], 9, .little);
    pos += 8;
    encoded[pos] = 0;
    pos += 1;
    std.mem.writeInt(u32, encoded[pos..][0..4], payload.len, .little);
    pos += 4;
    @memcpy(encoded[pos..], payload);
    try source.applyBatch(81, 9, &encoded);
    const latest = (try source.latestBatch(81)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 9), latest.commit_index);
    try std.testing.expectEqual(@as(u64, 9), latest.last_entry_index);
    try std.testing.expectEqual(@as(usize, 1), latest.normal_entry_count);
    const transition_latest = (try source.latestBatchForTransition(81)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(latest, transition_latest);
    var empty_observation = try source.observeSplitControl(std.testing.allocator, 81);
    defer empty_observation.deinit(std.testing.allocator);
    try std.testing.expect(empty_observation.state == null);
    try std.testing.expectEqual(@as(u64, 0), empty_observation.delta_sequence);

    var authoritative = try client.Owner.open(.{
        .context = context.handle,
        .path = .fromSlice(authoritative_root),
        .table_name = .fromSlice("docs"),
        .group_id = 81,
    });
    defer authoritative.deinit();
    var authoritative_batch = try authoritative.batchJson(
        "docs",
        "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\"},\"doc:b\":{\"title\":\"beta\"}},\"sync_level\":\"write\"}",
    );
    authoritative_batch.deinit();
    var reconcile = try source.reconcileAuthoritativeOwner(
        std.testing.allocator,
        authoritative.handle,
        81,
        latest,
        false,
        1,
        1024 * 1024,
    );
    defer reconcile.deinit(std.testing.allocator);
    try std.testing.expect(reconcile == .reconciled);
    var projection_range = try source.currentRange(std.testing.allocator, 81);
    defer projection_range.deinit(std.testing.allocator);
    var projection_page = try source.groupStatePageInRange(
        std.testing.allocator,
        81,
        .{ .start = projection_range.start, .end = projection_range.end },
        null,
        8,
        1024 * 1024,
    );
    defer projection_page.deinit(std.testing.allocator);
    try std.testing.expect(projection_page.entries.len > 0);
    var invalid_projection: abi.OwnedBytes = .{};
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_data_apply_store_projection(
        source.handle,
        &.{ .version = abi.abi_version + 1 },
        &invalid_projection,
    ));
    try std.testing.expectEqual(@as(u64, 0), invalid_projection.len);

    var placement = try source.beginActiveGroupTransition(&.{81});
    placement.commit();
    placement.deinit();
    try source.retainActiveGroups(&.{81});
    try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_data_apply_store_retain_groups(
        source.handle,
        &.{ .group_count = 1 },
    ));
    var aborted = try source.beginActiveGroupTransition(&.{ 81, 82 });
    aborted.abort();
    aborted.deinit();

    var prepared = (try source.prepareSnapshot(81, 9)) orelse return error.TestExpectedEqual;
    defer prepared.deinit();
    var materialized = try prepared.materializeFile(std.testing.allocator);
    defer materialized.deinit(std.testing.allocator);
    const materialized_bytes = try std.Io.Dir.cwd().readFileAlloc(
        std.Options.debug_io,
        materialized.path,
        std.testing.allocator,
        .limited(materialized.size + 1),
    );
    defer std.testing.allocator.free(materialized_bytes);
    try std.testing.expectEqual(materialized.size, @as(u64, @intCast(materialized_bytes.len)));

    var cancelled = (try source.prepareSnapshot(81, 9)) orelse return error.TestExpectedEqual;
    defer cancelled.deinit();
    cancelled.cancel();
    try std.testing.expectError(error.SnapshotBuildCancelled, cancelled.materializeFile(std.testing.allocator));

    const snapshot = try source.buildSnapshot(std.testing.allocator, 81);
    defer std.testing.allocator.free(snapshot);
    try std.testing.expect(snapshot.len > 0);

    var restored = try data_apply_client.RaftApplyStore.init(std.testing.allocator, .{
        .root_dir = restored_root,
        .context = context.handle,
    });
    defer restored.deinit();
    try restored.installSnapshot(81, 9, snapshot);
    const restored_latest = (try restored.latestBatch(81)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(latest.commit_index, restored_latest.commit_index);
    try std.testing.expectEqual(latest.last_entry_index, restored_latest.last_entry_index);

    try std.testing.expectEqual(abi.Status.busy, abi.antfly_storage_context_destroy(context.handle));
}

test "opaque metadata apply owner preserves semantic error identity" {
    const path = "/tmp/antfly-storage-kernel-metadata-errors";
    cleanup(path);
    defer cleanup(path);

    var store = try metadata_apply_client.RaftApplyStore.init(std.testing.allocator, .{
        .root_dir = path,
        .no_sync = true,
    });
    defer store.deinit();
    const snapshots = store.snapshotBuilder();

    try std.testing.expectError(
        error.InvalidMetadataSnapshot,
        snapshots.installSnapshot(std.testing.allocator, 91, 7, "not-a-metadata-snapshot"),
    );

    // The concrete store deliberately preserves the committed watermark even
    // when a batch has no projectable metadata commands. That gives this
    // cross-archive test a real provider-side index mismatch to round-trip.
    try snapshots.applyBatch(.{
        .group_id = 91,
        .commit_index = 7,
        .entries_bytes = "not-projectable-entries",
    });
    const latest = (try store.latestBatch(91)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 7), latest.commit_index);
    try std.testing.expectEqualStrings("not-projectable-entries", latest.entries_bytes);
    try std.testing.expectError(
        error.AppliedSnapshotIndexMismatch,
        snapshots.prepareSnapshot(91, 8),
    );
}
