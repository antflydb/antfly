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
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
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

    var lease_capture = LeaseCapture{};
    var owner_source = kernel_owner_source.ProvisionedKernelOwnerSource.init(
        alloc,
        replica_root,
        Catalog.iface(),
        lease_capture.requester(),
    );
    var owner_source_active = true;
    defer if (owner_source_active) owner_source.deinit();
    var write_source = table_writes.ProvisionedTableWriteSource.init(replica_root, Catalog.iface());
    var write_source_active = true;
    defer if (write_source_active) write_source.deinit();
    _ = write_source.withLocalWriteSource(owner_source.writeSource());
    var read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        Catalog.iface(),
        lease_capture.requester(),
    );
    _ = read_source.withLocalReadSource(owner_source.readSource());

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

    var lookup = (try read_source.source().lookup(alloc, "articles", "doc:a", .{
        .fields = &.{"title"},
        .include_all_fields = false,
    }, .read_index)).?;
    defer lookup.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 4242), lookup.version);
    try std.testing.expect(std.mem.indexOf(u8, lookup.json, "alpha") != null);
    try std.testing.expect(std.mem.indexOf(u8, lookup.json, "dense_idx") == null);

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
            try std.testing.expectEqual(@as(u32, 2), fields.fields[0].global_doc_count);
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
    try std.testing.expectEqual(@as(usize, 14), lease_capture.count);
    try std.testing.expectEqual(@as(u64, 7001), lease_capture.last_group_id);

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

    write_source.deinit();
    write_source_active = false;
    owner_source.deinit();
    owner_source_active = false;

    var reopened: ?*anyopaque = null;
    try std.testing.expectEqual(abi.Status.ok, abi.antfly_storage_owner_open(&open_request, &reopened));
    try std.testing.expect(reopened != null);
    abi.antfly_storage_owner_close(reopened);
}
