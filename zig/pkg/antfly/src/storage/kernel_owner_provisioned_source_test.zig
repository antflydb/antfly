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
const kernel_owner_source = @import("../api/kernel_owner_source.zig");
const backup_contract = @import("../api/backup_contract.zig");
const db_mod = @import("db/mod.zig");
const distributed_graph = @import("../api/distributed_graph.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const query_api = @import("../api/query.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const read_gate = @import("../raft/read_gate.zig");
const table_catalog = @import("../api/table_catalog.zig");
const table_reads = @import("../api/table_reads.zig");
const table_writes = @import("../api/table_writes.zig");
const shard_state_store = @import("../data/storage/shard_state_store.zig");

fn cleanup(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
}

test "provisioned batch lookup scan and query share one opaque live storage owner" {
    const alloc = std.testing.allocator;
    const replica_root = "/tmp/antfly-storage-kernel-provisioned-source";
    cleanup(replica_root);
    defer cleanup(replica_root);

    const Catalog = struct {
        const metadata_incarnation: metadata_api.MetadataClusterIncarnation = "31313131313131313131313131313131".*;

        accept_publication: bool = true,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .validate_publication = validatePublication,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{
                    .metadata_group_id = 1,
                    .metadata_incarnation = metadata_incarnation,
                    .metrics = .{},
                },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "articles",
                    .placement_role = "data",
                    .indexes_json =
                    \\{"dense_idx":{"type":"embeddings","external":true,"dimension":3},
                    \\ "full_text_index_v0":{"type":"full_text","enrichments":[{"name":"document_units_v1","kind":"asset","field":"url","content_type":"application/json","producer_json":"{\"type\":\"document_extraction\",\"config\":{}}"}]},
                    \\ "alg":{"type":"algebraic","version":1,"table":"articles","schema_version":1,
                    \\        "group_fields":[{"name":"category","path":"category","type":"keyword"}],
                    \\        "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
                    \\        "materializations":[{"name":"sum_by_category","op":"sum","group_by":["category"],"measure":"amount"}]},
                    \\ "relations_graph":{"type":"graph","edge_types":[{"name":"mentions"}]}}
                    ,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .range_id = 7001,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn validatePublication(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.accept_publication) return false;
            var snapshot = try adminSnapshot(ptr);
            return contract.matches(&snapshot);
        }
    };

    const LeaseCapture = struct {
        count: usize = 0,
        last_group_id: u64 = 0,

        fn requester(self: *@This()) read_gate.ReadableLeaseRequester {
            return .{
                .ptr = self,
                .vtable = &.{ .request_readable_lease = requestReadableLease },
            };
        }

        fn requestReadableLease(ptr: *anyopaque, group_id: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.count += 1;
            self.last_group_id = group_id;
        }
    };

    const GenerationTracker = struct {
        generation: u64 = table_reads.backend_current_root_generation,
        reservations: usize = 0,

        fn iface(self: *@This()) table_reads.GroupVisibleRootGenerationSource {
            return .{
                .ptr = self,
                .visible_root_generation_for_group = visible,
                .reserve_root_generation_for_group = reserve,
                .finish_root_generation_reservation = finish,
            };
        }

        fn visible(ptr: *anyopaque, _: u64) u64 {
            return (@as(*@This(), @ptrCast(@alignCast(ptr)))).generation;
        }

        fn reserve(ptr: *anyopaque, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reservations += 1;
        }

        fn finish(ptr: *anyopaque, _: u64, advance: bool) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            std.debug.assert(self.reservations > 0);
            if (advance) self.generation +%= 1;
            self.reservations -= 1;
        }
    };

    var catalog = Catalog{};
    var lease_capture = LeaseCapture{};
    var generations = GenerationTracker{};
    var owner_source = kernel_owner_source.ProvisionedKernelOwnerSource.init(
        alloc,
        replica_root,
        catalog.iface(),
        lease_capture.requester(),
    );
    _ = owner_source.withGroupVisibleRootGeneration(generations.iface());
    var owner_source_active = true;
    defer if (owner_source_active) owner_source.deinit();
    var write_source = table_writes.ProvisionedTableWriteSource.init(replica_root, catalog.iface());
    var write_source_active = true;
    defer if (write_source_active) write_source.deinit();
    _ = owner_source.withTransactionRecoverySource(&write_source);
    _ = write_source.withLocalWriteSource(owner_source.writeSource());
    _ = write_source.withStorageSnapshotSource(owner_source.snapshotSource());
    _ = write_source.withGroupVisibleRootGeneration(generations.iface());
    var read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        catalog.iface(),
        lease_capture.requester(),
    );
    _ = read_source.withLocalReadSource(owner_source.readSource());
    _ = read_source.withGroupVisibleRootGeneration(generations.iface());

    try std.testing.expect((try write_source.source().createTable(alloc, "articles", .{})) != null);
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());

    _ = try write_source.source().batch(alloc, "articles", .{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"category\":\"news\",\"amount\":10,\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YQ==\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"category\":\"news\",\"amount\":20,\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
        },
        .graph_writes = &.{.{
            .index_name = "relations_graph",
            .source = "doc:a",
            .target = "doc:b",
            .edge_type = "mentions",
            .weight = 0.75,
        }},
        .timestamp_ns = 4242,
        .sync_level = .full_index,
    });
    const txn_id: db_mod.types.TxnId = .{ 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };
    const txn_participant = "table2:00000008:articles:7001";
    try std.testing.expect((try write_source.source().txnBeginGroupLocal(
        alloc,
        7001,
        "articles",
        txn_id,
        5_000,
        0,
        true,
        &.{txn_participant},
    )) != null);
    try std.testing.expectEqual(
        db_mod.types.TxnStatus.pending,
        (try write_source.source().txnStatusGroupLocal(alloc, 7001, "articles", txn_id)).?,
    );
    try std.testing.expect((try write_source.source().txnPrepareGroupLocal(
        alloc,
        7001,
        "articles",
        txn_id,
        0,
        .{ .writes = &.{.{
            .key = "doc:txn",
            .value = "{\"title\":\"transactional\",\"category\":\"news\",\"amount\":30}",
        }} },
    )) != null);
    try std.testing.expect((try write_source.source().txnResolveGroupLocal(
        alloc,
        7001,
        "articles",
        txn_id,
        .committed,
        5_001,
        0,
        .full_index,
    )) != null);
    try std.testing.expect((try write_source.source().txnAcknowledgeGroupLocal(
        alloc,
        7001,
        "articles",
        txn_id,
        txn_participant,
    )) != null);

    const backup_root = "/tmp/antfly-storage-kernel-provisioned-source-backups";
    cleanup(backup_root);
    defer cleanup(backup_root);
    const backup_formats = [_]struct {
        format: backup_contract.BackupFormat,
        backup_id: []const u8,
    }{
        .{ .format = .portable, .backup_id = "portable-owner" },
        .{ .format = .native, .backup_id = "native-owner" },
    };
    for (backup_formats) |backup| {
        const shards = (try write_source.source().backupTable(alloc, "articles", .{
            .backup_root = backup_root,
            .backup_id = backup.backup_id,
            .format = backup.format,
        })).?;
        defer table_writes.freeStorageKernelBackupShards(alloc, shards);
        try std.testing.expectEqual(@as(usize, 1), shards.len);
        try std.testing.expectEqual(@as(u64, 7001), shards[0].group_id);
        try std.testing.expect(shards[0].artifact_size_bytes > 0);
        try std.testing.expectEqual(@as(usize, 64), shards[0].artifact_sha256.len);
        const artifact_path = try std.fs.path.join(alloc, &.{ backup_root, shards[0].snapshot_path });
        defer alloc.free(artifact_path);
        var backup_io_impl = std.Io.Threaded.init(alloc, .{});
        defer backup_io_impl.deinit();
        _ = try std.Io.Dir.cwd().statFile(backup_io_impl.io(), artifact_path, .{});
    }
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());
    try std.testing.expectEqual(
        db_mod.types.TxnStatus.committed,
        (try write_source.source().txnStatusGroupLocal(alloc, 7001, "articles", txn_id)).?,
    );
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());
    try std.testing.expect((try write_source.source().beginBulkIngest(alloc, "articles")) != null);
    try std.testing.expect((try write_source.source().beginBulkIngest(alloc, "articles")) != null);
    _ = try write_source.source().batch(alloc, "articles", .{
        .writes = &.{.{
            .key = "doc:bulk",
            .value = "{\"title\":\"bulk\",\"category\":\"archive\",\"amount\":5}",
        }},
        .timestamp_ns = 4243,
        .sync_level = .write,
    });
    try std.testing.expect((try write_source.source().finishBulkIngest(
        alloc,
        "articles",
        .{ .compact = false },
    )) != null);
    try std.testing.expect((try write_source.source().finishBulkIngest(
        alloc,
        "articles",
        .{ .compact = false },
    )) != null);
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());
    try std.testing.expect((try write_source.source().beginBulkIngest(alloc, "articles")) != null);
    try std.testing.expectError(
        error.AutoBulkIngestBusy,
        write_source.finishManagedWriterAutoBulkForTransition(7001, "articles"),
    );
    write_source.source().abortBulkIngest("articles");
    try write_source.finishManagedWriterAutoBulkForTransition(7001, "articles");
    try std.testing.expect(!write_source.finishExpiredAutoBulkIngestBestEffort());
    try std.testing.expectEqual(@as(u64, 0), write_source.autoBulkIngestStatsBestEffort().open_entries);
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());
    const structural = (try owner_source.writeSource().reconcileTableGroupLocal(
        7001,
        "articles",
        null,
        false,
    )).?;
    try std.testing.expect(structural.state == .complete);
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());

    var replicated_descriptor = try owner_source.loadDescriptor(alloc, 7001, "articles");
    defer replicated_descriptor.deinit(alloc);
    try owner_source.applyPreparedReplicatedBatchGroupLocal(
        alloc,
        7001,
        "articles",
        replicated_descriptor.view(),
        .{
            .deletes = &.{"doc:missing"},
            .timestamp_ns = 4243,
            .sync_level = .full_index,
        },
    );
    try owner_source.waitForCurrentSyncGroupLocal(7001, "articles", .full_index);
    try owner_source.applyHAReplicationRecordGroupLocal(7001, "articles", .{
        .kind = .checkpoint,
        .cluster_id = 1,
        .shard_id = 7001,
        .table_id = 7,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 0,
        .previous_lsn = 0,
    });

    var lookup = (try read_source.source().lookup(alloc, "articles", "doc:a", .{
        .fields = &.{"title"},
        .include_all_fields = false,
    }, .read_index)).?;
    defer lookup.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 4242), lookup.version);
    try std.testing.expect(std.mem.indexOf(u8, lookup.json, "alpha") != null);
    try std.testing.expect(std.mem.indexOf(u8, lookup.json, "dense_idx") == null);

    var txn_lookup = (try read_source.source().lookup(alloc, "articles", "doc:txn", .{}, .read_index)).?;
    defer txn_lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, txn_lookup.json, "transactional") != null);

    try std.testing.expect((try read_source.source().lookup(
        alloc,
        "articles",
        "doc:missing",
        .{},
        .read_index,
    )) == null);

    var scan = (try read_source.source().scan(alloc, "articles", "doc:a", "doc:z", .{
        .inclusive_from = true,
        .exclusive_to = true,
        .include_documents = true,
        .limit = 10,
        .fields = &.{"title"},
        .include_all_fields = false,
        .filter_query_json = "{\"term\":{\"path\":\"/title\",\"value\":\"alpha\"}}",
    }, .read_index)).?;
    defer scan.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, scan.ndjson, "doc:a") != null);
    try std.testing.expect(std.mem.indexOf(u8, scan.ndjson, "alpha") != null);
    try std.testing.expect(std.mem.indexOf(u8, scan.ndjson, "doc:b") == null);
    try std.testing.expect(std.mem.indexOf(u8, scan.ndjson, "dense_idx") == null);

    var query = try query_api.parsePublicQueryRequest(
        alloc,
        null,
        "articles",
        "{\"query\":{\"match_all\":{}},\"limit\":10}",
    );
    defer query.deinit(alloc);
    var response = (try read_source.source().query(alloc, "articles", query.req, .read_index)).?;
    defer response.deinit(alloc);

    try std.testing.expect(std.mem.indexOf(u8, response.json, "articles") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "doc:a") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "doc:b") != null);

    var dense_query = try query_api.parseQueryRequest(
        alloc,
        null,
        "articles",
        "{\"embeddings\":{\"dense_idx\":[1.0,0.0,0.0]},\"indexes\":[\"dense_idx\"],\"limit\":2}",
    );
    defer dense_query.deinit(alloc);
    var preflight = (try read_source.source().preflightQuery(
        alloc,
        "articles",
        dense_query.req,
        .read_index,
        1000,
    )).?;
    defer preflight.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), preflight.dense_query_count);
    try std.testing.expectEqual(@as(usize, 1), preflight.embedding_indexes.len);
    try std.testing.expectEqualStrings("dense_idx", preflight.embedding_indexes[0].name);
    // Named composed embeddings are not the single-vector worker fast path.
    try std.testing.expectEqual(@as(u32, 0), preflight.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 0), preflight.vector_worker_fallback_count);

    var dense_response = (try read_source.source().query(alloc, "articles", dense_query.req, .read_index)).?;
    defer dense_response.deinit(alloc);
    const first_doc_a = std.mem.indexOf(u8, dense_response.json, "doc:a") orelse return error.MissingDenseHit;
    const second_doc_b = std.mem.indexOf(u8, dense_response.json, "doc:b") orelse return error.MissingDenseHit;
    try std.testing.expect(first_doc_a < second_doc_b);

    const text_stats_body =
        \\{"fields":[{"index_name":"full_text_index_v0","field":"_all","terms":["alpha"]}]}
    ;
    var text_stats_response = (try read_source.source().textStatsGroupLocal(
        alloc,
        7001,
        "articles",
        text_stats_body,
    )).?;
    defer text_stats_response.deinit(alloc);
    var text_stats = try table_reads.parseTextStatsHttpResponse(alloc, text_stats_body, text_stats_response.json);
    defer text_stats.deinit(alloc);
    switch (text_stats) {
        .fields => |fields| {
            try std.testing.expectEqual(@as(usize, 1), fields.fields.len);
            try std.testing.expectEqual(@as(u32, 4), fields.fields[0].global_doc_count);
            try std.testing.expectEqual(@as(u32, 1), fields.fields[0].term_doc_freqs[0].doc_freq);
        },
        .background_fields => return error.UnexpectedBackgroundTextStats,
    }
    try std.testing.expectError(
        error.IdentityReadGenerationChanged,
        read_source.source().textStatsGroupLocal(
            alloc,
            7001,
            "articles",
            "{\"_identity_read_generation\":999999,\"fields\":[{\"index_name\":\"full_text_index_v0\",\"field\":\"_all\",\"terms\":[\"alpha\"]}]}",
        ),
    );

    const algebraic_ir = db_mod.algebraic.ir;
    const sum_expr = algebraic_ir.TensorExpr{
        .fragment = .reduce,
        .input_dims = &.{ .doc, .scalar },
        .output_dims = &.{.bucket},
        .semantic_id = "sum_by_category",
        .layout = .materialized_expr,
        .law_id = .sum,
    };
    var sum_plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, sum_expr)).?;
    defer sum_plan.deinit(alloc);
    const algebraic_body = try table_reads.encodeAlgebraicPartialsRequestWithProgramAtGeneration(
        alloc,
        "alg",
        null,
        &.{sum_plan.access_path},
        &.{sum_expr},
        null,
    );
    defer alloc.free(algebraic_body);
    var algebraic_response = (try read_source.source().algebraicPartialsGroupLocal(
        alloc,
        7001,
        "articles",
        algebraic_body,
    )).?;
    defer algebraic_response.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, algebraic_response.json, "\"partials\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, algebraic_response.json, "sum_by_category") != null);
    try std.testing.expectError(
        error.InvalidQueryRequest,
        read_source.source().algebraicPartialsGroupLocal(alloc, 7001, "articles", "{}"),
    );

    var expand_req = distributed_graph.GraphExpandRequest{
        .name = try alloc.dupe(u8, "mentions"),
        .index_name = try alloc.dupe(u8, "relations_graph"),
        .frontier = try alloc.dupe(distributed_graph.GraphFrontierItem, &.{.{
            .id = 1,
            .key = try alloc.dupe(u8, "doc:a"),
        }}),
        .exclude_nodes = @constCast((&[_]distributed_graph.GraphNodeIdentity{})[0..]),
        .exclude_edges = @constCast((&[_][]u8{})[0..]),
        .params = .{ .max_depth = 1, .max_results = 10 },
    };
    defer expand_req.deinit(alloc);
    var expand_response = (try read_source.source().graphExpandGroupLocal(
        alloc,
        7001,
        "articles",
        expand_req,
        .read_index,
    )).?;
    defer expand_response.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), expand_response.expansions.len);
    try std.testing.expectEqualStrings("doc:a", expand_response.expansions[0].frontier_key);
    try std.testing.expect(expand_response.expansions[0].graph_result.nodes.len > 0);

    var hydrate_req = distributed_graph.GraphHydrateRequest{
        .keys = try alloc.alloc([]u8, 2),
        .incoming_index_name = try alloc.dupe(u8, "relations_graph"),
        .incoming_index_name_owned = true,
    };
    hydrate_req.keys[0] = try alloc.dupe(u8, "doc:a");
    hydrate_req.keys[1] = try alloc.dupe(u8, "doc:b");
    defer hydrate_req.deinit(alloc);
    var hydrate_response = (try read_source.source().graphHydrateGroupLocal(
        alloc,
        7001,
        "articles",
        hydrate_req,
        .read_index,
    )).?;
    defer hydrate_response.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), hydrate_response.hits.len);
    try std.testing.expectEqualSlices(bool, &.{ false, true }, hydrate_response.has_incoming);

    const graph_path = db_mod.algebraic.ir.graphEdgeAccessPath("relations_graph");
    var edges_req = distributed_graph.GraphEdgesRequest{
        .index_name = try alloc.dupe(u8, "relations_graph"),
        .key = try alloc.dupe(u8, "doc:a"),
        .direction = .out,
        .tensor_access_path = .{
            .owner = try alloc.dupe(u8, graph_path.owner),
            .layout = graph_path.layout,
            .fragments = try alloc.dupe(db_mod.algebraic.ir.TensorFragment, graph_path.fragments),
            .output_dims = try alloc.dupe(db_mod.algebraic.ir.Dimension, graph_path.output_dims),
            .law_ids = try alloc.dupe(db_mod.algebraic.law.Id, graph_path.law_ids),
        },
        .tensor_program = try distributed_graph.graphEdgesTensorProgramEnvelopeAlloc(alloc, "relations_graph"),
    };
    defer edges_req.deinit(alloc);
    var edges_response = (try read_source.source().graphEdgesGroupLocal(
        alloc,
        7001,
        "articles",
        edges_req,
        .read_index,
    )).?;
    defer edges_response.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), edges_response.edges.len);
    try std.testing.expectEqualStrings("doc:b", edges_response.edges[0].target);

    var manifest = (try read_source.source().documentArtifactManifest(
        alloc,
        "articles",
        "doc:a",
        "document_units_v1",
        .read_index,
    )) orelse return error.MissingDocumentArtifactManifest;
    defer manifest.deinit(alloc);
    try std.testing.expectEqualStrings("doc:a", manifest.document_id);
    try std.testing.expectEqualStrings("document_units_v1", manifest.artifact_name);
    try std.testing.expectEqualStrings("text", manifest.route_type);
    try std.testing.expectEqual(@as(usize, 1), manifest.unit_count);
    try std.testing.expect(manifest.child_ranges.len > 0);
    try std.testing.expectEqualStrings("unit", manifest.child_ranges[0].range_kind);
    try std.testing.expect(std.mem.indexOf(u8, manifest.manifest_json, "\"range_policy\"") != null);
    try std.testing.expect(manifest.state_json != null);

    var manifests = (try read_source.source().documentArtifactManifests(
        alloc,
        "articles",
        "doc:a",
        .read_index,
    )) orelse return error.MissingDocumentArtifactManifestList;
    defer manifests.deinit(alloc);
    try std.testing.expectEqualStrings("doc:a", manifests.document_id);
    try std.testing.expectEqual(@as(usize, 1), manifests.artifacts.len);
    try std.testing.expectEqualStrings("document_units_v1", manifests.artifacts[0].artifact_name);
    try std.testing.expect(manifests.artifacts[0].child_ranges.len > 0);
    try std.testing.expect(manifests.artifacts[0].state_json != null);
    try std.testing.expect((try read_source.source().documentArtifactManifest(
        alloc,
        "articles",
        "doc:missing",
        "document_units_v1",
        .read_index,
    )) == null);

    try std.testing.expectEqual(@as(?bool, true), try write_source.source().updateDocumentArtifactChildRangePlacement(
        alloc,
        "articles",
        "doc:a",
        "document_units_v1",
        .{
            .range_id = "range:000000",
            .placement = "remote",
            .owner_group_id = 7002,
            .placement_generation = 3,
            .route_status = "remote_committed",
        },
    ));
    var moved_manifest = (try read_source.source().documentArtifactManifest(
        alloc,
        "articles",
        "doc:a",
        "document_units_v1",
        .read_index,
    )) orelse return error.MissingDocumentArtifactManifest;
    defer moved_manifest.deinit(alloc);
    try std.testing.expectEqualStrings("remote", moved_manifest.child_ranges[0].placement);
    try std.testing.expectEqual(@as(?u64, 7002), moved_manifest.child_ranges[0].owner_group_id);

    const generation_before_reprocess = moved_manifest.generation;
    try std.testing.expectEqual(@as(?bool, true), try write_source.source().reprocessDocumentArtifact(
        alloc,
        "articles",
        "doc:a",
        "document_units_v1",
    ));
    var reprocessed_manifest = (try read_source.source().documentArtifactManifest(
        alloc,
        "articles",
        "doc:a",
        "document_units_v1",
        .read_index,
    )) orelse return error.MissingDocumentArtifactManifest;
    defer reprocessed_manifest.deinit(alloc);
    try std.testing.expect(reprocessed_manifest.generation > generation_before_reprocess);

    var range_reprocess = (try write_source.source().reprocessDocumentArtifactRangeGroupLocal(
        alloc,
        7001,
        "articles",
        "document_units_v1",
        .{ .from_key = "doc:a", .to_key = "doc:b", .limit = 1 },
    )) orelse return error.StorageKernelArtifactOperationMissing;
    defer range_reprocess.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), range_reprocess.scanned);
    try std.testing.expectEqual(@as(usize, 1), range_reprocess.reprocessed);

    var repair_issues = (try write_source.source().listArtifactRepairIssuesGroupLocal(
        alloc,
        7001,
        "articles",
        .{ .limit = 10 },
    )) orelse return error.StorageKernelArtifactOperationMissing;
    defer repair_issues.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 10), repair_issues.limit);

    try std.testing.expectEqual(@as(?u64, 0), try write_source.source().applyDocumentArtifactChildRangeBatchGroupLocal(
        alloc,
        7001,
        "articles",
        "doc:a",
        "document_units_v1",
        .{},
    ));
    try std.testing.expect((try write_source.source().corruptEmbeddingArtifact(
        alloc,
        "articles",
        "doc:a",
        "dense_idx",
    )) != null);

    const CancelRepair = struct {
        fn requested(_: *anyopaque) bool {
            return true;
        }
    };
    var cancel_token: u8 = 0;
    try std.testing.expectError(error.Canceled, write_source.source().repairArtifactIssuesGroupLocalControlled(
        alloc,
        7001,
        "articles",
        .{ .limit = 10 },
        .{ .cancel_check = .{ .ptr = &cancel_token, .is_requested = CancelRepair.requested } },
    ));

    var cancellation = std.atomic.Value(bool).init(true);
    edges_req.cancellation = &cancellation;
    try std.testing.expectError(
        error.Cancelled,
        read_source.source().graphEdgesGroupLocal(alloc, 7001, "articles", edges_req, .read_index),
    );
    cancellation.store(false, .release);
    edges_req.execution_deadline_ns = 1;
    try std.testing.expectError(
        error.Timeout,
        read_source.source().graphEdgesGroupLocal(alloc, 7001, "articles", edges_req, .read_index),
    );

    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());
    try std.testing.expectEqual(@as(usize, 17), lease_capture.count);
    try std.testing.expectEqual(@as(u64, 7001), lease_capture.last_group_id);

    var statuses = (try write_source.source().localRuntimeStatuses(alloc, "articles")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 4), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(.live_writer_publish, statuses.items[0].metadata.source);
    try std.testing.expectEqual(.fresh, statuses.items[0].metadata.freshness);
    try std.testing.expect(statuses.items[0].lsm_storage_stats != null);

    try std.testing.expectEqual(@as(usize, 1), owner_source.retireTable("articles"));
    try std.testing.expectEqual(@as(usize, 0), owner_source.ownerCountForTest());
    var reopened_lookup = (try read_source.source().lookup(
        alloc,
        "articles",
        "doc:b",
        .{},
        .read_index,
    )).?;
    defer reopened_lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, reopened_lookup.json, "beta") != null);
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());
    try std.testing.expectEqual(@as(usize, 18), lease_capture.count);

    const accepted_snapshot = try shard_state_store.encodeGroupStateSnapshot(
        alloc,
        .{ .start = "", .end = "" },
        &.{.{
            .key = "doc:snapshot",
            .value = "{\"title\":\"published snapshot\",\"category\":\"archive\",\"amount\":30}",
        }},
        &.{},
    );
    defer alloc.free(accepted_snapshot);
    try write_source.installRaftSnapshotGroupLocal(alloc, 7001, accepted_snapshot);
    try std.testing.expectEqual(@as(usize, 0), owner_source.ownerCountForTest());
    try std.testing.expectEqual(@as(u64, 1), generations.generation);
    try std.testing.expectEqual(@as(usize, 0), generations.reservations);
    try std.testing.expect((try read_source.source().lookup(
        alloc,
        "articles",
        "doc:b",
        .{},
        .read_index,
    )) == null);
    var published_snapshot_lookup = (try read_source.source().lookup(
        alloc,
        "articles",
        "doc:snapshot",
        .{},
        .read_index,
    )).?;
    defer published_snapshot_lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, published_snapshot_lookup.json, "published snapshot") != null);
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());

    const rejected_snapshot = try shard_state_store.encodeGroupStateSnapshot(
        alloc,
        .{ .start = "", .end = "" },
        &.{.{ .key = "doc:rejected", .value = "{\"title\":\"rejected snapshot\"}" }},
        &.{},
    );
    defer alloc.free(rejected_snapshot);
    catalog.accept_publication = false;
    try std.testing.expectError(
        error.CatalogChanged,
        write_source.installRaftSnapshotGroupLocal(alloc, 7001, rejected_snapshot),
    );
    try std.testing.expectEqual(@as(usize, 0), owner_source.ownerCountForTest());
    try std.testing.expectEqual(@as(u64, 1), generations.generation);
    try std.testing.expectEqual(@as(usize, 0), generations.reservations);
    try std.testing.expect((try read_source.source().lookup(
        alloc,
        "articles",
        "doc:rejected",
        .{},
        .read_index,
    )) == null);
    var rolled_back_snapshot_lookup = (try read_source.source().lookup(
        alloc,
        "articles",
        "doc:snapshot",
        .{},
        .read_index,
    )).?;
    defer rolled_back_snapshot_lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, rolled_back_snapshot_lookup.json, "published snapshot") != null);
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());

    const group_path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root});
    defer alloc.free(group_path);
    const open_request: abi.OpenRequest = .{
        .path = .fromSlice(group_path),
        .table_name = .fromSlice("articles"),
        .has_identity_namespace = 1,
        .identity_table_id = 7,
        .identity_shard_id = 7001,
        .identity_range_id = 7001,
    };
    var duplicate: ?*anyopaque = null;
    try std.testing.expectEqual(abi.Status.busy, abi.antfly_storage_owner_open(&open_request, &duplicate));
    try std.testing.expect(duplicate == null);

    try std.testing.expect((try write_source.source().dropTable(alloc, "articles", &.{7001})) != null);
    try std.testing.expectEqual(@as(usize, 0), owner_source.ownerCountForTest());
    var path_io_impl = std.Io.Threaded.init(alloc, .{});
    defer path_io_impl.deinit();
    try std.testing.expectError(
        error.FileNotFound,
        std.Io.Dir.cwd().access(path_io_impl.io(), group_path, .{}),
    );

    write_source.deinit();
    write_source_active = false;
    owner_source.deinit();
    owner_source_active = false;

    var reopened: ?*anyopaque = null;
    try std.testing.expectEqual(abi.Status.ok, abi.antfly_storage_owner_open(&open_request, &reopened));
    try std.testing.expect(reopened != null);
    abi.antfly_storage_owner_close(reopened);
}
