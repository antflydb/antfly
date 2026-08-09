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
    cleanup(path);
    defer cleanup(path);

    var owner = try client.Owner.open(.{
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
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

    const query_json =
        \\{"query":{"match_all":{}},"limit":10}
    ;
    try std.testing.expectError(error.InvalidArgument, owner.queryJson("articles", query_json));

    var query_response = try owner.queryJson("docs", query_json);
    defer query_response.deinit();
    try std.testing.expect(std.mem.indexOf(u8, query_response.bytes(), "docs") != null);
    try std.testing.expect(std.mem.indexOf(u8, query_response.bytes(), "doc:a") != null);
    try std.testing.expect(std.mem.indexOf(u8, query_response.bytes(), "doc:b") != null);
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
    var snapshot: ?*anyopaque = undefined;
    var invalid_snapshot: abi.SnapshotPrepareRequest = .{};
    invalid_snapshot.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_snapshot_prepare(&invalid_snapshot, &snapshot),
    );
    try std.testing.expect(snapshot == null);
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
