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

pub const cluster = @import("cluster.zig");
pub const batch = @import("batch.zig");
pub const backups = @import("backups.zig");
pub const query = @import("query.zig");
pub const query_contract = @import("query_contract.zig");
pub const cluster_api_http = @import("cluster_api_http.zig");
pub const public_table_http = @import("public_table_http.zig");
pub const public_embedding_query = @import("public_embedding_query.zig");
pub const public_graph_query = @import("public_graph_query.zig");
pub const public_search_request = @import("public_search_request.zig");
pub const public_query_string = @import("public_query_string.zig");
pub const public_text_query = @import("public_text_query.zig");
pub const query_builder_agent = @import("query_builder_agent.zig");
pub const distributed_txn = @import("distributed_txn.zig");
pub const transactions = @import("transactions.zig");
const e2e = @import("e2e.zig");
const multi_node_e2e = @import("multi_node_e2e.zig");
pub const table_catalog = @import("table_catalog.zig");
pub const table_router = @import("table_router.zig");
pub const tables = @import("tables.zig");
pub const table_contract = @import("table_contract.zig");
pub const indexes = @import("indexes.zig");
pub const http_routes = @import("http_routes.zig");
pub const provisioned_storage = @import("provisioned_storage.zig");
pub const table_reads = @import("table_reads.zig");
pub const table_writes = @import("table_writes.zig");
pub const distributed_candidate_source = @import("distributed_candidate_source.zig");
pub const distributed_entity_sink = @import("distributed_entity_sink.zig");
pub const distributed_join = @import("distributed_join.zig");
pub const distributed_graph = @import("distributed_graph.zig");
pub const http_internal_group_read_routes = @import("http_internal_group_read_routes.zig");
pub const http_internal_group_join_routes = @import("http_internal_group_join_routes.zig");
pub const http_server = @import("http_server.zig");
pub const http_client = @import("http_client.zig");
pub const httpx_handler = @import("httpx_handler.zig");

pub const ClusterHealth = cluster.ClusterHealth;
pub const ClusterStatus = cluster.ClusterStatus;
pub const clusterStatusFromMetadata = cluster.fromMetadataStatus;
pub const BatchResult = batch.BatchResult;
pub const QueryResponse = query.QueryResponse;
pub const TableReadSource = table_reads.TableReadSource;
pub const BoundTableReadSource = table_reads.BoundTableReadSource;
pub const ProvisionedGroupStorage = provisioned_storage.ProvisionedGroupStorage;
pub const ProvisionedTableReadCache = table_reads.ProvisionedTableReadCache;
pub const ProvisionedTableReadSource = table_reads.ProvisionedTableReadSource;
pub const GroupLsmGenerationSource = table_reads.GroupLsmGenerationSource;
pub const HostedProvisionedTableReadSource = table_reads.HostedProvisionedTableReadSource;
pub const DistributedCandidateSource = distributed_candidate_source.DistributedCandidateSource;
pub const DistributedEntitySink = distributed_entity_sink.DistributedEntitySink;
pub const TableWriteSource = table_writes.TableWriteSource;
pub const BoundTableWriteSource = table_writes.BoundTableWriteSource;
pub const ProvisionedTableWriteCache = table_writes.ProvisionedTableWriteCache;
pub const ProvisionedTableWriteSource = table_writes.ProvisionedTableWriteSource;
pub const HostedProvisionedTableWriteSource = table_writes.HostedProvisionedTableWriteSource;
pub const HostedGroupRouter = table_router.HostedGroupRouter;
pub const ApiHttpServer = http_server.ApiHttpServer;
pub const ApiHttpClient = http_client.ApiHttpClient;

test "join inequality: jsonValuesCompare all six operators on integers" {
    const three: std.json.Value = .{ .integer = 3 };
    const five: std.json.Value = .{ .integer = 5 };

    try std.testing.expect(distributed_join.jsonValuesCompare(three, five, .eq) == false);
    try std.testing.expect(distributed_join.jsonValuesCompare(three, three, .eq) == true);

    try std.testing.expect(distributed_join.jsonValuesCompare(three, five, .neq) == true);
    try std.testing.expect(distributed_join.jsonValuesCompare(three, three, .neq) == false);

    try std.testing.expect(distributed_join.jsonValuesCompare(three, five, .lt) == true);
    try std.testing.expect(distributed_join.jsonValuesCompare(five, three, .lt) == false);
    try std.testing.expect(distributed_join.jsonValuesCompare(three, three, .lt) == false);

    try std.testing.expect(distributed_join.jsonValuesCompare(three, five, .lte) == true);
    try std.testing.expect(distributed_join.jsonValuesCompare(three, three, .lte) == true);
    try std.testing.expect(distributed_join.jsonValuesCompare(five, three, .lte) == false);

    try std.testing.expect(distributed_join.jsonValuesCompare(five, three, .gt) == true);
    try std.testing.expect(distributed_join.jsonValuesCompare(three, five, .gt) == false);

    try std.testing.expect(distributed_join.jsonValuesCompare(five, three, .gte) == true);
    try std.testing.expect(distributed_join.jsonValuesCompare(five, five, .gte) == true);
    try std.testing.expect(distributed_join.jsonValuesCompare(three, five, .gte) == false);
}

test "join inequality: cross-type int/float coercion" {
    const int_three: std.json.Value = .{ .integer = 3 };
    const float_three: std.json.Value = .{ .float = 3.0 };
    const float_four: std.json.Value = .{ .float = 4.5 };

    try std.testing.expect(distributed_join.jsonValuesOrdered(int_three, float_three) == 0);
    try std.testing.expect(distributed_join.jsonValuesOrdered(int_three, float_four) < 0);
    try std.testing.expect(distributed_join.jsonValuesOrdered(float_four, int_three) > 0);

    try std.testing.expect(distributed_join.jsonValuesCompare(int_three, float_four, .lt) == true);
    try std.testing.expect(distributed_join.jsonValuesCompare(float_four, int_three, .gt) == true);
}

test "join inequality: string lexicographic ordering" {
    const apple: std.json.Value = .{ .string = "apple" };
    const banana: std.json.Value = .{ .string = "banana" };

    try std.testing.expect(distributed_join.jsonValuesCompare(apple, banana, .lt) == true);
    try std.testing.expect(distributed_join.jsonValuesCompare(banana, apple, .gt) == true);
    try std.testing.expect(distributed_join.jsonValuesCompare(apple, apple, .eq) == true);
    try std.testing.expect(distributed_join.jsonValuesCompare(apple, banana, .gte) == false);
}

test "join inequality: incomparable types return 0" {
    const null_val: std.json.Value = .null;
    const bool_val: std.json.Value = .{ .bool = true };
    const int_val: std.json.Value = .{ .integer = 42 };

    try std.testing.expectEqual(@as(i8, 0), distributed_join.jsonValuesOrdered(null_val, int_val));
    try std.testing.expectEqual(@as(i8, 0), distributed_join.jsonValuesOrdered(bool_val, int_val));
    try std.testing.expectEqual(@as(i8, 0), distributed_join.jsonValuesOrdered(null_val, bool_val));
}

test "api module compiles" {
    _ = cluster;
    _ = batch;
    _ = backups;
    _ = query;
    _ = query_contract;
    _ = cluster_api_http;
    _ = public_table_http;
    _ = public_graph_query;
    _ = public_query_string;
    _ = public_search_request;
    _ = public_text_query;
    _ = query_builder_agent;
    _ = distributed_txn;
    _ = transactions;
    _ = e2e;
    _ = multi_node_e2e;
    _ = table_catalog;
    _ = table_router;
    _ = tables;
    _ = table_contract;
    _ = indexes;
    _ = http_routes;
    _ = provisioned_storage;
    _ = table_reads;
    _ = table_writes;
    _ = distributed_candidate_source;
    _ = distributed_entity_sink;
    _ = distributed_join;
    _ = distributed_graph;
    _ = http_internal_group_read_routes;
    _ = http_internal_group_join_routes;
    _ = http_server;
    _ = http_client;
    _ = httpx_handler;
    _ = ClusterHealth;
    _ = ClusterStatus;
    _ = clusterStatusFromMetadata;
    _ = BatchResult;
    _ = QueryResponse;
    _ = TableReadSource;
    _ = BoundTableReadSource;
    _ = ProvisionedGroupStorage;
    _ = ProvisionedTableReadCache;
    _ = ProvisionedTableReadSource;
    _ = HostedProvisionedTableReadSource;
    _ = TableWriteSource;
    _ = BoundTableWriteSource;
    _ = ProvisionedTableWriteCache;
    _ = ProvisionedTableWriteSource;
    _ = HostedProvisionedTableWriteSource;
    _ = HostedGroupRouter;
    _ = ApiHttpServer;
    _ = ApiHttpClient;
}

test "distributed graph result_ref fail-closed guards are covered" {
    try distributed_graph.testResultRefFailClosedGuards(std.testing.allocator);
}

test "api distributed graph hydrate carries identity generation and clears cross-range ordinals" {
    try distributed_graph.testHydrateIdentityGenerationAndCrossRangeOrdinalBoundary(std.testing.allocator);
}

test "public graph result_ref fail-closed guards are covered" {
    try public_graph_query.testResolveGraphSelectorFailClosedGuard(std.testing.allocator);
}

test "api table reads reject distributed resolved doc filters" {
    const db_mod = @import("../storage/db/mod.zig");

    var sentinel: u8 = 0;
    var req: db_mod.types.SearchRequest = .{
        .resolved_doc_filter = &sentinel,
    };

    try std.testing.expectError(error.UnsupportedQueryRequest, table_reads.testing.rejectResolvedDocFilterForCrossGroup(req, 2));
    try table_reads.testing.rejectResolvedDocFilterForCrossGroup(req, 1);
    try table_reads.testing.rejectResolvedDocFilterForRemoteRoute(req, .local);
    var remote_uri_buf = [_]u8{'h'};
    try std.testing.expectError(error.UnsupportedQueryRequest, table_reads.testing.rejectResolvedDocFilterForRemoteRoute(req, .{ .remote = .{ .node_id = 2, .base_uri = remote_uri_buf[0..] } }));
    req.resolved_doc_filter = null;
    try table_reads.testing.rejectResolvedDocFilterForCrossGroup(req, 2);
    try table_reads.testing.rejectResolvedDocFilterForRemoteRoute(req, .{ .remote = .{ .node_id = 2, .base_uri = remote_uri_buf[0..] } });
}

test "api table reads reject stale doc identity before multigroup fanout" {
    const alloc = std.testing.allocator;
    const metadata_api = @import("../metadata/api.zig");
    const metadata_table_manager = @import("../metadata/table_manager.zig");
    const metadata_reconciler = @import("../metadata/reconciler.zig");
    const metadata_transition_state = @import("../metadata/transition_state.zig");
    const raft_reconciler = @import("../raft/reconciler.zig");

    const FakeCatalog = struct {
        statuses: []const metadata_reconciler.MergedGroupStatus,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(self.statuses),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const healthy_statuses = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7002, .namespace_range_id = 7002, .allocated_ordinals = 1 } },
    };
    var healthy_catalog = FakeCatalog{ .statuses = healthy_statuses[0..] };
    try table_reads.testing.validateDocIdentityReadyForMultiGroupRead(alloc, healthy_catalog.iface(), "docs", 2);
    try table_reads.testing.validateDocIdentityReadyForMultiGroupRead(alloc, healthy_catalog.iface(), "docs", 1);

    const rebuild_required = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
        .{ .group_id = 7002, .doc_identity = .{ .rebuild_required = true } },
    };
    var rebuild_catalog = FakeCatalog{ .statuses = rebuild_required[0..] };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, table_reads.testing.validateDocIdentityReadyForMultiGroupRead(alloc, rebuild_catalog.iface(), "docs", 2));
}

test "api public table query rejects only top-level internal fields" {
    const alloc = std.testing.allocator;

    try std.testing.expect(try public_table_http.testing.hasInternalShardQueryFields(alloc,
        \\{"query":{"match_all":{}},"_identity_read_generation":1}
    ));
    try std.testing.expect(try public_table_http.testing.hasInternalShardQueryFields(alloc,
        \\{"query":{"match_all":{}},"allow_doc_identity_reassignment":true}
    ));
    try std.testing.expect(!try public_table_http.testing.hasInternalShardQueryFields(alloc,
        \\{"full_text_search":{"query":"mentions \"_identity_read_generation\" and \"native_doc_id_constraints\""}}
    ));
    try std.testing.expect(try query_contract.testing.bodyHasInternalShardFields(alloc,
        \\{"query":{"match_all":{}},"native_doc_id_constraints":{"include_doc_ids":["doc:a"]}}
    ));
    try std.testing.expect(!try query_contract.testing.bodyHasInternalShardFields(alloc,
        \\{"full_text_search":{"query":"mentions \"native_doc_id_constraints\""}}
    ));
    try std.testing.expect(!try query_contract.testing.bodyHasPublicDocFilterBindings(alloc,
        \\{"full_text_search":{"query":"mentions \"with\""}}
    ));
    try std.testing.expectError(error.InvalidQueryRequest, query_contract.parseQueryRequest(alloc, null, "docs",
        \\{"with":{"visible":{"match_all":{}}},"identity_read_generation":1,"query":{"match_all":{}}}
    ));
    try std.testing.expectError(error.InvalidQueryRequest, query_contract.parseQueryRequest(alloc, null, "docs",
        \\{"with":{"visible":{"match_all":{}}},"allow_doc_identity_reassignment":true,"query":{"match_all":{}}}
    ));
    try std.testing.expectError(error.InvalidQueryRequest, query_contract.parseQueryRequest(alloc, null, "docs",
        \\{"embeddings":{"dense_idx":"AACAPwAAAEAAAEBA"},"indexes":["dense_idx"],"identity_read_generation":1}
    ));
    try std.testing.expectError(error.InvalidQueryRequest, query_contract.parseQueryRequest(alloc, null, "docs",
        \\{"embeddings":{"dense_idx":"AACAPwAAAEAAAEBA"},"indexes":["dense_idx"],"allow_doc_identity_reassignment":true}
    ));
    try public_graph_query.rejectInternalDocIdentityFields(alloc,
        \\{"graph_searches":{"g":{"type":"neighbors","index_name":"graph","start_nodes":{"keys":["doc:a"]}}}}
    );
    try std.testing.expectError(error.InvalidQueryRequest, public_graph_query.rejectInternalDocIdentityFields(alloc,
        \\{"graph_searches":{"g":{"type":"neighbors","index_name":"graph","start_nodes":{"keys":["doc:a"]}}},"identity_read_generation":1}
    ));
    try std.testing.expectError(error.InvalidQueryRequest, public_graph_query.rejectInternalDocIdentityFields(alloc,
        \\{"graph_searches":{"g":{"type":"neighbors","index_name":"graph","start_nodes":{"keys":["doc:a"]}}},"allow_doc_identity_reassignment":true}
    ));
    try std.testing.expectError(error.InvalidQueryRequest, public_graph_query.rejectInternalDocIdentityFields(alloc,
        \\{"graph_searches":{"g":{"type":"neighbors","index_name":"graph","start_nodes":{"keys":["doc:a"]}}},"native_doc_id_constraints":{"include_doc_ids":["doc:a"]}}
    ));
}

test "api query contract tensor program envelope preserves dictionary identity" {
    const alloc = std.testing.allocator;
    const algebraic = @import("../storage/db/mod.zig").algebraic;
    const dictionary = algebraic.lexical.DictionaryIdentity.analyzedText("docs", "body", "default");
    const input_expr = algebraic.ir.TensorExpr{
        .fragment = .automaton_select,
        .output_dims = &.{.doc},
        .dictionary = dictionary,
    };
    const step = algebraic.ir.TensorProgramStep{
        .expr = .{
            .fragment = .reduce,
            .input_dims = &.{.doc},
            .output_dims = &.{.bucket},
            .law_id = .count,
        },
        .inputs = &.{.{ .input = 0 }},
    };
    const program = algebraic.ir.TensorProgram{
        .inputs = &.{input_expr},
        .steps = &.{step},
        .output = .{ .step = 0 },
        .outputs = &.{ .{ .input = 0 }, .{ .step = 0 } },
    };

    const encoded = try query_contract.encodeAlgebraicTensorProgramEnvelopeAlloc(alloc, program);
    defer alloc.free(encoded);
    var parsed = try query_contract.parseAlgebraicTensorProgramEnvelopeAlloc(alloc, encoded);
    defer parsed.deinit(alloc);
    var view = try parsed.asProgramAlloc(alloc);
    defer view.deinit(alloc);

    const expected_id = try algebraic.ir.tensorProgramIdAlloc(alloc, program);
    defer alloc.free(expected_id);
    try std.testing.expectEqualStrings(expected_id, parsed.program_id);
    try std.testing.expect(view.program.inputs[0].dictionary != null);
    try std.testing.expect(dictionary.eql(view.program.inputs[0].dictionary.?));
    try std.testing.expectEqual(@as(usize, 2), view.program.outputs.len);
    try std.testing.expectEqual(@as(?usize, 0), view.program.outputs[0].input);
    try std.testing.expectEqual(@as(?usize, 0), view.program.outputs[1].step);
}
