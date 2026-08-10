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
const client = @import("kernel_owner_client.zig");

fn cleanup(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
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
    try std.testing.expectEqual(abi.Status.busy, abi.antfly_storage_owner_open(&.{
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
    try owner.configure(
        "docs",
        "",
        \\{"dense_idx":{"type":"embeddings","external":true,"dimension":3},
        \\ "full_text_index_v0":{"type":"full_text","enrichments":[{"name":"document_units_v1","kind":"asset","field":"url","content_type":"application/json","producer_json":"{\"type\":\"document_extraction\",\"config\":{}}"}]}}
        ,
    );
    var indexed_batch = try owner.batchJson(
        "docs",
        "{\"inserts\":{\"doc:artifact\":{\"title\":\"artifact\",\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YQ==\"},\"doc:c\":{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[1,0,0]}},\"doc:d\":{\"title\":\"delta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}},\"sync_level\":\"full_index\"}",
    );
    defer indexed_batch.deinit();
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
        "{\"doc_key\":\"doc:artifact\",\"artifact_name\":\"document_units_v1\",\"update\":{\"range_id\":\"range:000000\",\"placement\":\"remote\",\"owner_group_id\":7002,\"placement_generation\":3}}",
        null,
        null,
        false,
    );
    defer placement.deinit();
    try std.testing.expect(std.mem.indexOf(u8, placement.bytes(), "\"handled\":true") != null);

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
    try std.testing.expectError(error.Cancelled, owner.artifactOperationJson(
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
    var invalid_operation: abi.JsonOperationRequest = .{
        .table_name = .fromSlice("docs"),
        .request_json = .fromSlice("{}"),
    };
    invalid_operation.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_batch_json(null, &invalid_operation, &response),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
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
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_preflight_json(null, &invalid_operation, &response),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_text_stats_json(null, &invalid_operation, &response),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_algebraic_partials_json(null, &invalid_operation, &response),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    const invalid_controlled = abi.ControlledJsonOperationRequest{ .version = abi.abi_version + 1 };
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_graph_expand_json(null, &invalid_controlled, &response));
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_graph_hydrate_json(null, &invalid_controlled, &response));
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_graph_edges_json(null, &invalid_controlled, &response));
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

    try std.testing.expectError(error.InvalidQueryRequest, client.statusToError(.invalid_query));
    try std.testing.expectError(error.UnsupportedQueryRequest, client.statusToError(.unsupported_query));
    try std.testing.expectError(error.IndexNotFound, client.statusToError(.index_not_found));
    try std.testing.expectError(error.IdentityReadGenerationChanged, client.statusToError(.identity_read_generation_changed));
    try std.testing.expectError(error.Timeout, client.statusToError(.timeout));
    try std.testing.expectError(error.Cancelled, client.statusToError(.cancelled));

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
