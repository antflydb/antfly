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
const metadata_openapi = @import("antfly_metadata_openapi");
const generating = @import("antfly_generating");
const db_mod = @import("../storage/db/mod.zig");
const document_mapper = @import("../storage/db/document_mapper.zig");

pub const cluster = @import("cluster.zig");
pub const batch = @import("batch.zig");
pub const backups = @import("backups.zig");
pub const linear_merge = @import("linear_merge.zig");
pub const query = @import("query.zig");
pub const query_contract = @import("query_contract.zig");
pub const cluster_api_http = @import("cluster_api_http.zig");
pub const retrieval_agent = @import("retrieval_agent.zig");
pub const recursive_agent = @import("recursive_agent.zig");
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
pub const artifact_reprocess_jobs = @import("artifact_reprocess_jobs.zig");
pub const http_internal_group_read_routes = @import("http_internal_group_read_routes.zig");
pub const http_internal_group_join_routes = @import("http_internal_group_join_routes.zig");
pub const http_server = @import("http_server.zig");
pub const http_client = @import("http_client.zig");
pub const httpx_handler = @import("httpx_handler.zig");
pub const connections = @import("connections.zig");

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
pub const GroupVisibleRootGenerationSource = table_reads.GroupVisibleRootGenerationSource;
pub const HAReadGate = table_reads.HAReadGate;
pub const backend_current_root_generation = table_reads.backend_current_root_generation;
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

test "linear merge request parser accepts raw payload value under public request cap" {
    const alloc = std.testing.allocator;
    const payload = try alloc.alloc(u8, 6 * 1024 * 1024);
    defer alloc.free(payload);
    @memset(payload, 'x');

    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{\"records\":{\"doc:a\":{\"raw_payload\":\"");
    try writer.writeAll(payload);
    try writer.writeAll("\"}}}");

    var req = try linear_merge.parseRequest(alloc, out.written());
    defer req.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), req.writes.len);
    try std.testing.expect(std.mem.indexOf(u8, req.writes[0].value, "\"raw_payload\"") != null);
}

test "public batch default schema accepts docsaf doc_type row and rejects reserved _type" {
    const alloc = std.testing.allocator;

    var reserved_type = try batch.parseBatchRequest(alloc,
        \\{"inserts":{"sample.pdf:page:1":{"id":"sample.pdf:page:1","file_path":"sample.pdf","title":"sample.pdf - Page 1","content":"Montessori classroom notes","_type":"pdf_page","metadata":{"page_number":1,"total_pages":14,"source_pdf":"sample.pdf","extraction_method":"text_stream"},"url":"https://example.com/sample.pdf#page=1"}},"sync_level":"write"}
    );
    defer reserved_type.deinit(alloc);

    var parsed_schema = try tables.parseValidatedTableSchema(alloc, tables.default_schema_json);
    defer parsed_schema.deinit(alloc);
    try std.testing.expectError(error.InvalidBatchRequest, tables.validateWritesAgainstTableSchema(alloc, parsed_schema, reserved_type.req.writes));

    var docsaf_row = try batch.parseBatchRequest(alloc,
        \\{"inserts":{"sample.pdf:page:1":{"id":"sample.pdf:page:1","file_path":"sample.pdf","title":"sample.pdf - Page 1","content":"Montessori classroom notes","doc_type":"pdf_page","metadata":{"page_number":1,"total_pages":14,"source_pdf":"sample.pdf","extraction_method":"text_stream"},"url":"https://example.com/sample.pdf#page=1"}},"sync_level":"write"}
    );
    defer docsaf_row.deinit(alloc);

    try tables.validateWritesAgainstTableSchema(alloc, parsed_schema, docsaf_row.req.writes);

    var extracted = try document_mapper.extractWrite(alloc, docsaf_row.writes[0].key, docsaf_row.writes[0].value);
    defer extracted.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), extracted.dense_embeddings.len);
    try std.testing.expectEqual(@as(usize, 0), extracted.sparse_embeddings.len);
    try std.testing.expectEqual(@as(usize, 0), extracted.graph_writes.len);
    try std.testing.expect(extracted.cleaned_value != null);
    try std.testing.expect(std.mem.indexOf(u8, extracted.cleaned_value.?, "\"doc_type\":\"pdf_page\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, extracted.cleaned_value.?, "\"_type\"") == null);
}

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
    _ = retrieval_agent;
    _ = recursive_agent;
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
    _ = connections;
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

test "api recursive execution mode validation fails closed" {
    try std.testing.expect(@hasDecl(metadata_openapi, "AgentExecutionMode"));
    try std.testing.expect(@hasDecl(metadata_openapi, "RecursiveAgentConfig"));
    try std.testing.expect(@hasDecl(metadata_openapi, "ContextObjectKind"));
    try std.testing.expect(@hasDecl(metadata_openapi, "RecursiveTraceArtifact"));
    try std.testing.expect(@hasField(metadata_openapi.RetrievalAgentRequest, "execution_mode"));
    try std.testing.expect(@hasField(metadata_openapi.RetrievalAgentRequest, "recursive"));
    try std.testing.expect(@hasField(metadata_openapi.RetrievalAgentResult, "trace_artifact"));
    try std.testing.expect(@hasField(metadata_openapi.QueryBuilderRequest, "execution_mode"));
    try std.testing.expect(@hasField(metadata_openapi.QueryBuilderRequest, "recursive"));

    const pipeline_request: metadata_openapi.RetrievalAgentRequest = .{
        .query = "find docs",
        .queries = &.{.{
            .table = "docs",
            .semantic_search = "find docs",
        }},
        .execution_mode = .pipeline,
    };
    try std.testing.expectEqual(metadata_openapi.AgentExecutionMode.pipeline, try recursive_agent.validateRetrievalExecutionMode(pipeline_request, 0));
    try std.testing.expectError(error.InvalidRetrievalAgentRequest, recursive_agent.validateRetrievalExecutionMode(pipeline_request, 1));

    const recursive_request: metadata_openapi.RetrievalAgentRequest = .{
        .query = "find docs",
        .queries = &.{.{
            .table = "docs",
            .semantic_search = "find docs",
        }},
        .execution_mode = .recursive,
        .recursive = .{
            .max_depth = 1,
            .max_subcalls = 4,
            .max_concurrency = 2,
            .split_policy = .by_document,
            .merge_policy = .verify,
        },
    };
    try std.testing.expectEqual(metadata_openapi.AgentExecutionMode.recursive, try recursive_agent.validateRetrievalExecutionMode(recursive_request, 0));

    const query_builder_request: metadata_openapi.QueryBuilderRequest = .{
        .intent = "build a query",
        .mode = "join_aggregation",
        .execution_mode = .pipeline,
    };
    try recursive_agent.validateQueryBuilderExecutionMode(query_builder_request);

    const recursive_query_builder_request: metadata_openapi.QueryBuilderRequest = .{
        .intent = "build a query",
        .mode = "join_aggregation",
        .execution_mode = .recursive,
        .recursive = .{
            .max_depth = 1,
            .max_subcalls = 4,
            .max_concurrency = 2,
            .split_policy = .by_query,
            .merge_policy = .verify,
        },
    };
    try recursive_agent.validateQueryBuilderExecutionMode(recursive_query_builder_request);

    const recursive_cfg = try recursive_agent.normalizeConfig(recursive_query_builder_request.recursive);
    try std.testing.expectEqual(@as(usize, 4), recursive_agent.childCountForContextCount(recursive_cfg, 12));
    try std.testing.expectEqual(@as(usize, 2), recursive_agent.scheduledConcurrency(recursive_cfg, 4));
    try std.testing.expect(recursive_agent.contextKindAllowed(recursive_cfg, .document));
    try std.testing.expectEqualStrings("max_subcalls", recursive_agent.incompleteReason(true, 0, false).?);
}

test "api recursive generation runners require timeout support" {
    const RetrievalGeneration = struct {
        fn iface() retrieval_agent.GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(_: *anyopaque, _: std.mem.Allocator, _: []const generating.ChainLink, _: []const generating.ChatMessage) !generating.GenerateResult {
            return error.TestUnexpectedResult;
        }
    };

    const QueryBuilderGeneration = struct {
        fn iface() query_builder_agent.GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(_: *anyopaque, _: std.mem.Allocator, _: []const generating.ChainLink, _: []const generating.ChatMessage) !generating.GenerateResult {
            return error.TestUnexpectedResult;
        }
    };

    try std.testing.expectError(
        error.DeadlineExceeded,
        RetrievalGeneration.iface().executeChainWithTimeoutMs(std.testing.allocator, &.{}, &.{}, 1),
    );
    try std.testing.expectError(
        error.DeadlineExceeded,
        QueryBuilderGeneration.iface().executeChainWithTimeoutMs(std.testing.allocator, &.{}, &.{}, 1),
    );
}

test "api query builder recursive aggregation inference emits typed candidate artifact" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const RuntimeValidator = struct {
        fn iface() query_builder_agent.QueryBuilderRuntimeQueryRequestValidator {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .validate_query_request = validateQueryRequest,
                    .preflight_query_request = preflightQueryRequest,
                },
            };
        }

        fn validateQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            query_request: metadata_openapi.QueryRequest,
        ) !?[]const u8 {
            if (query_request.aggregations == null) return error.TestExpectedEqual;
            return null;
        }

        fn preflightQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            query_request: metadata_openapi.QueryRequest,
            _: u32,
        ) !?db_mod.RuntimePreflightSummary {
            if (query_request.aggregations == null) return error.TestExpectedEqual;
            return .{
                .shard_count = 1,
                .shard_result_window = 10,
                .shard_result_window_total = 10,
                .positive_id_result_upper_bound = 1500,
                .structured_filter_doc_count_estimate = 1500,
                .aggregation_may_scan_full_results = true,
            };
        }
    };

    const result = try query_builder_agent.buildQueryBuilderResponseWithContext(arena, .{
        .table = "orders",
        .intent = "sum amount by status",
        .schema_fields = &.{ "status", "amount", "customer_id" },
        .mode = "aggregation",
        .execution_mode = .recursive,
        .recursive = .{
            .max_depth = 1,
            .max_subcalls = 1,
            .max_concurrency = 1,
            .split_policy = .by_query,
            .merge_policy = .verify,
        },
    }, .{
        .schema_fields = &.{ "status", "amount", "customer_id" },
        .full_text_index_metadata = &.{.{ .name = "orders_text", .fields = &.{ "status", "amount", "customer_id" } }},
        .runtime_query_request_validator = RuntimeValidator.iface(),
    }, null);

    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, result.status.?);
    try std.testing.expectEqualStrings("aggregation", result.specialist.?);
    try std.testing.expect(result.query_request != null);
    try std.testing.expect(result.query_request.?.aggregations != null);

    const candidate_plans = result.plan.?.object.get("candidate_plans").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), candidate_plans.len);
    const candidate_query = candidate_plans[0].object.get("query_request").?.object;
    const aggregations = candidate_query.get("aggregations").?.object;
    const by_status = aggregations.get("by_status").?.object;
    try std.testing.expectEqualStrings("terms", by_status.get("type").?.string);
    try std.testing.expectEqualStrings("status", by_status.get("field").?.string);
    const sub_aggregations = by_status.get("sub_aggregations").?.object;
    const sum_amount = sub_aggregations.get("sum_amount").?.object;
    try std.testing.expectEqualStrings("sum", sum_amount.get("type").?.string);
    try std.testing.expectEqualStrings("amount", sum_amount.get("field").?.string);
    try std.testing.expect(candidate_plans[0].object.get("runtime_preflight_estimated").?.bool);
    try std.testing.expect(candidate_plans[0].object.get("aggregation_may_scan_full_results").?.bool);
    try std.testing.expectEqual(@as(i64, 1500), candidate_plans[0].object.get("aggregation_second_pass_doc_estimate").?.integer);
    try std.testing.expectEqual(@as(i64, 1500), candidate_plans[0].object.get("aggregation_second_pass_doc_upper_bound").?.integer);
    try std.testing.expectEqualStrings("full_result_second_pass_medium", candidate_plans[0].object.get("aggregation_cost_heuristic").?.string);
}

test "api query builder recursive aggregation inference uses field catalog metadata" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const result = try query_builder_agent.buildQueryBuilderResponseWithContext(arena, .{
        .table = "subscriptions",
        .intent = "total revenue by plan",
        .schema_fields = &.{ "plan", "total_cents", "status" },
        .mode = "aggregation",
        .execution_mode = .recursive,
        .recursive = .{
            .max_depth = 1,
            .max_subcalls = 1,
            .max_concurrency = 1,
            .split_policy = .by_query,
            .merge_policy = .verify,
        },
    }, .{
        .schema_fields = &.{ "plan", "total_cents", "status" },
        .doc_count = 12_000,
        .field_metadata = &.{
            .{
                .name = "plan",
                .aliases = &.{ "subscription plan", "tier" },
                .kind = .keyword,
                .groupable = true,
            },
            .{
                .name = "total_cents",
                .aliases = &.{ "revenue", "amount" },
                .kind = .numeric,
                .metric = true,
            },
            .{
                .name = "status",
                .kind = .keyword,
                .groupable = true,
            },
        },
        .full_text_index_metadata = &.{.{ .name = "subscriptions_text", .fields = &.{ "plan", "total_cents", "status" } }},
    }, null);

    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, result.status.?);
    try std.testing.expectEqualStrings("aggregation", result.specialist.?);
    try std.testing.expect(result.query_request != null);
    try std.testing.expect(result.query_request.?.aggregations != null);

    const candidate_plans = result.plan.?.object.get("candidate_plans").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), candidate_plans.len);
    const candidate_query = candidate_plans[0].object.get("query_request").?.object;
    const aggregations = candidate_query.get("aggregations").?.object;
    const by_plan = aggregations.get("by_plan").?.object;
    try std.testing.expectEqualStrings("terms", by_plan.get("type").?.string);
    try std.testing.expectEqualStrings("plan", by_plan.get("field").?.string);
    const sub_aggregations = by_plan.get("sub_aggregations").?.object;
    const sum_total = sub_aggregations.get("sum_total_cents").?.object;
    try std.testing.expectEqualStrings("sum", sum_total.get("type").?.string);
    try std.testing.expectEqualStrings("total_cents", sum_total.get("field").?.string);
    try std.testing.expectEqualStrings("plan", candidate_plans[0].object.get("aggregation_group_field").?.string);
    try std.testing.expectEqualStrings("total_cents", candidate_plans[0].object.get("aggregation_metric_field").?.string);
    try std.testing.expect(candidate_plans[0].object.get("aggregation_field_catalog_backed").?.bool);
    try std.testing.expectEqual(@as(i64, 12_000), candidate_plans[0].object.get("aggregation_table_doc_count").?.integer);
    try std.testing.expectEqual(@as(i64, 10), candidate_plans[0].object.get("aggregation_bucket_size").?.integer);
    try std.testing.expectEqualStrings("catalog_medium", candidate_plans[0].object.get("aggregation_stat_heuristic").?.string);
}

test "api query builder recursive join aggregation inference emits typed candidate artifact" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const result = try query_builder_agent.buildQueryBuilderResponseWithContext(arena, .{
        .table = "orders",
        .intent = "sum amount by customer with customers including tier",
        .schema_fields = &.{ "customer_id", "amount", "status" },
        .mode = "join_aggregation",
        .execution_mode = .recursive,
        .recursive = .{
            .max_depth = 1,
            .max_subcalls = 1,
            .max_concurrency = 1,
            .split_policy = .by_query,
            .merge_policy = .verify,
        },
    }, .{
        .schema_fields = &.{ "customer_id", "amount", "status" },
        .full_text_index_metadata = &.{.{ .name = "orders_text", .fields = &.{ "customer_id", "amount", "status" } }},
    }, null);

    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, result.status.?);
    try std.testing.expectEqualStrings("join_aggregation", result.specialist.?);
    try std.testing.expect(result.query_request != null);
    try std.testing.expect(result.query_request.?.join != null);
    try std.testing.expect(result.query_request.?.aggregations != null);

    const candidate_plans = result.plan.?.object.get("candidate_plans").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), candidate_plans.len);
    const candidate = candidate_plans[0].object;
    try std.testing.expect(candidate.get("preflight_error_count").?.integer > 0);
    const candidate_query = candidate.get("query_request").?.object;
    const join = candidate_query.get("join").?.object;
    try std.testing.expectEqualStrings("customers", join.get("right_table").?.string);
    try std.testing.expectEqualStrings("inner", join.get("join_type").?.string);
    try std.testing.expectEqualStrings("customer_id", join.get("on").?.object.get("left_field").?.string);
    try std.testing.expectEqualStrings("id", join.get("on").?.object.get("right_field").?.string);
    try std.testing.expectEqualStrings("index_lookup", join.get("strategy_hint").?.string);
    try std.testing.expectEqualStrings("tier", join.get("right_fields").?.array.items[0].string);

    const aggregations = candidate_query.get("aggregations").?.object;
    const by_customer = aggregations.get("by_customer_id").?.object;
    try std.testing.expectEqualStrings("terms", by_customer.get("type").?.string);
    try std.testing.expectEqualStrings("customer_id", by_customer.get("field").?.string);
    const sum_amount = by_customer.get("sub_aggregations").?.object.get("sum_amount").?.object;
    try std.testing.expectEqualStrings("sum", sum_amount.get("type").?.string);
    try std.testing.expectEqualStrings("amount", sum_amount.get("field").?.string);
}

test "api query builder recursive join inference uses related table catalog metadata" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const result = try query_builder_agent.buildQueryBuilderResponseWithContext(arena, .{
        .table = "orders",
        .intent = "join orders with accounts including region and plan",
        .schema_fields = &.{ "account_uuid", "amount", "status" },
        .mode = "join",
        .execution_mode = .recursive,
        .recursive = .{
            .max_depth = 1,
            .max_subcalls = 1,
            .max_concurrency = 1,
            .split_policy = .by_query,
            .merge_policy = .verify,
        },
    }, .{
        .schema_fields = &.{ "account_uuid", "amount", "status" },
        .full_text_index_metadata = &.{.{ .name = "orders_text", .fields = &.{ "account_uuid", "amount", "status" } }},
        .related_tables = &.{.{
            .name = "accounts",
            .schema_fields = &.{ "uuid", "region", "plan", "owner_email" },
            .primary_key_fields = &.{"uuid"},
        }},
    }, null);

    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, result.status.?);
    try std.testing.expectEqualStrings("join", result.specialist.?);
    try std.testing.expect(result.query_request != null);
    const join = result.query_request.?.join orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("accounts", join.right_table);
    try std.testing.expectEqualStrings("account_uuid", join.on.left_field);
    try std.testing.expectEqualStrings("uuid", join.on.right_field);
    try std.testing.expect(join.right_fields != null);
    try std.testing.expectEqual(@as(usize, 2), join.right_fields.?.len);
    try std.testing.expectEqualStrings("region", join.right_fields.?[0]);
    try std.testing.expectEqualStrings("plan", join.right_fields.?[1]);

    const candidate_plans = result.plan.?.object.get("candidate_plans").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), candidate_plans.len);
    const candidate_join = candidate_plans[0].object.get("query_request").?.object.get("join").?.object;
    try std.testing.expectEqualStrings("uuid", candidate_join.get("on").?.object.get("right_field").?.string);
}

test "api query builder require executable rejects inferred join until runtime support lands" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"require_executable":true}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    try std.testing.expectError(error.InvalidQueryBuilderRequest, query_builder_agent.buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "orders",
        .intent = "orders with customers",
        .schema_fields = &.{ "customer_id", "amount", "status" },
        .mode = "join",
        .constraints = constraints_tree.value,
    }, .{
        .schema_fields = &.{ "customer_id", "amount", "status" },
        .full_text_index_metadata = &.{.{ .name = "orders_text", .fields = &.{ "customer_id", "amount", "status" } }},
    }, null));
}

test "api query builder require executable accepts runtime validated join" {
    const RuntimeValidator = struct {
        fn iface() query_builder_agent.QueryBuilderRuntimeQueryRequestValidator {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .validate_query_request = validateQueryRequest,
                    .preflight_query_request = preflightQueryRequest,
                    .plan_join_query_request = planJoinQueryRequest,
                },
            };
        }

        fn validateQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            query_request: metadata_openapi.QueryRequest,
        ) !?[]const u8 {
            if (query_request.join == null) return error.TestExpectedEqual;
            if (!std.mem.eql(u8, query_request.join.?.right_table, "customers")) return error.TestExpectedEqual;
            return null;
        }

        fn preflightQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            query_request: metadata_openapi.QueryRequest,
            _: u32,
        ) !?db_mod.RuntimePreflightSummary {
            if (query_request.join == null) return error.TestExpectedEqual;
            return .{
                .remote_shard_count = 1,
                .shard_count = 2,
                .shard_result_window = 64,
                .shard_result_window_total = 64,
                .positive_id_result_upper_bound = 12,
                .structured_filter_doc_count_estimate = 8,
                .stored_projection_doc_upper_bound_total = 12,
            };
        }

        fn planJoinQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            query_request: metadata_openapi.QueryRequest,
            runtime_preflight: db_mod.RuntimePreflightSummary,
        ) !?query_builder_agent.QueryBuilderRuntimeJoinPlannerSummary {
            if (query_request.join == null) return error.TestExpectedEqual;
            try std.testing.expectEqual(@as(?u32, 8), runtime_preflight.result_doc_estimate);
            return .{
                .strategy = "index_lookup",
                .estimated_cost = 8.0,
                .estimated_rows = 8,
                .estimated_memory_bytes = 1024,
                .used_stats = true,
                .shuffle_candidate = false,
                .forced_broadcast_fallback = false,
                .left_sample_row_count = 8,
                .left_sample_source = "runtime_query",
            };
        }
    };

    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"require_executable":true}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try query_builder_agent.buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "orders",
        .intent = "orders with customers",
        .schema_fields = &.{ "customer_id", "amount", "status" },
        .mode = "join",
        .execution_mode = .recursive,
        .recursive = .{
            .max_depth = 1,
            .max_subcalls = 1,
            .max_concurrency = 1,
            .split_policy = .by_query,
            .merge_policy = .verify,
        },
        .constraints = constraints_tree.value,
    }, .{
        .schema_fields = &.{ "customer_id", "amount", "status" },
        .doc_count = 10_000,
        .full_text_index_metadata = &.{.{ .name = "orders_text", .fields = &.{ "customer_id", "amount", "status" } }},
        .related_tables = &.{.{
            .name = "customers",
            .schema_fields = &.{ "id", "tier" },
            .primary_key_fields = &.{"id"},
            .doc_count = 250,
            .shard_count = 1,
            .key_selectivity_heuristic = "primary_key_unique",
        }},
        .runtime_query_request_validator = RuntimeValidator.iface(),
    }, null);

    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, result.status.?);
    try std.testing.expectEqualStrings("join", result.specialist.?);
    try std.testing.expect(result.query_request != null);
    try std.testing.expect(result.query_request.?.join != null);
    const candidate_plans = result.plan.?.object.get("candidate_plans").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), candidate_plans.len);
    try std.testing.expectEqual(@as(i64, 0), candidate_plans[0].object.get("preflight_error_count").?.integer);
    try std.testing.expect(candidate_plans[0].object.get("runtime_preflight_estimated").?.bool);
    try std.testing.expect(candidate_plans[0].object.get("join_runtime_validated").?.bool);
    try std.testing.expectEqual(@as(i64, 4), candidate_plans[0].object.get("preflight_latency_heuristic_score").?.integer);
    try std.testing.expectEqualStrings("medium", candidate_plans[0].object.get("preflight_latency_heuristic").?.string);
    try std.testing.expectEqualStrings("index_lookup", candidate_plans[0].object.get("join_strategy_hint").?.string);
    try std.testing.expectEqual(@as(i64, 10_000), candidate_plans[0].object.get("join_left_doc_count").?.integer);
    try std.testing.expectEqual(@as(i64, 250), candidate_plans[0].object.get("join_right_doc_count").?.integer);
    try std.testing.expectEqual(@as(i64, 1), candidate_plans[0].object.get("join_right_shard_count").?.integer);
    try std.testing.expectEqualStrings("primary_key_unique", candidate_plans[0].object.get("join_right_key_selectivity").?.string);
    try std.testing.expectEqualStrings("id", candidate_plans[0].object.get("join_right_key_field").?.string);
    try std.testing.expect(candidate_plans[0].object.get("join_right_key_unique").?.bool);
    try std.testing.expectEqual(@as(i64, 250), candidate_plans[0].object.get("join_right_key_estimated_distinct").?.integer);
    switch (candidate_plans[0].object.get("join_right_key_avg_rows_per_key").?) {
        .integer => |avg| try std.testing.expectEqual(@as(i64, 1), avg),
        .float => |avg| try std.testing.expectEqual(@as(f64, 1.0), avg),
        else => return error.UnexpectedJoinRightKeyAvgRowsPerKey,
    }
    try std.testing.expectEqualStrings("primary_key_metadata", candidate_plans[0].object.get("join_right_key_stats_source").?.string);
    try std.testing.expectEqualStrings("high", candidate_plans[0].object.get("join_right_key_stats_confidence").?.string);
    try std.testing.expectEqualStrings("not_co_located", candidate_plans[0].object.get("join_shard_colocation").?.string);
    try std.testing.expectEqual(@as(i64, 8), candidate_plans[0].object.get("join_left_result_doc_estimate").?.integer);
    try std.testing.expectEqual(@as(i64, 8), candidate_plans[0].object.get("join_left_result_doc_upper_bound").?.integer);
    try std.testing.expectEqual(@as(i64, 2), candidate_plans[0].object.get("join_runtime_shard_count").?.integer);
    try std.testing.expectEqual(@as(i64, 1), candidate_plans[0].object.get("join_runtime_remote_shard_count").?.integer);
    try std.testing.expectEqualStrings("runtime_validated_remote", candidate_plans[0].object.get("join_runtime_feasibility").?.string);
    try std.testing.expectEqualStrings("index_lookup", candidate_plans[0].object.get("join_planner_strategy").?.string);
    switch (candidate_plans[0].object.get("join_planner_estimated_cost").?) {
        .integer => |cost| try std.testing.expectEqual(@as(i64, 8), cost),
        .float => |cost| try std.testing.expectEqual(@as(f64, 8.0), cost),
        else => return error.UnexpectedJoinPlannerEstimatedCost,
    }
    try std.testing.expectEqual(@as(i64, 8), candidate_plans[0].object.get("join_planner_estimated_rows").?.integer);
    try std.testing.expectEqual(@as(i64, 1024), candidate_plans[0].object.get("join_planner_estimated_memory_bytes").?.integer);
    try std.testing.expect(candidate_plans[0].object.get("join_planner_used_stats").?.bool);
    try std.testing.expect(!candidate_plans[0].object.get("join_planner_shuffle_candidate").?.bool);
    try std.testing.expect(!candidate_plans[0].object.get("join_planner_forced_broadcast_fallback").?.bool);
    try std.testing.expectEqual(@as(i64, 8), candidate_plans[0].object.get("join_planner_left_sample_row_count").?.integer);
    try std.testing.expectEqualStrings("runtime_query", candidate_plans[0].object.get("join_planner_left_sample_source").?.string);
    try std.testing.expectEqualStrings("small_right_index_lookup", candidate_plans[0].object.get("join_cardinality_heuristic").?.string);
    try std.testing.expectEqualStrings("broadcast", candidate_plans[0].object.get("join_recommended_strategy").?.string);
    const strategy_options = candidate_plans[0].object.get("join_strategy_options").?.array.items;
    try std.testing.expectEqual(@as(usize, 3), strategy_options.len);
    try std.testing.expectEqualStrings("index_lookup", strategy_options[0].object.get("strategy").?.string);
    try std.testing.expect(strategy_options[0].object.get("feasible").?.bool);
    try std.testing.expectEqualStrings("right_key_unique", strategy_options[0].object.get("feasibility").?.string);
    try std.testing.expectEqualStrings("broadcast", strategy_options[1].object.get("strategy").?.string);
    try std.testing.expect(strategy_options[1].object.get("recommended").?.bool);
    try std.testing.expect(strategy_options[1].object.get("feasible").?.bool);
    try std.testing.expectEqualStrings("right_side_broadcastable", strategy_options[1].object.get("feasibility").?.string);
    try std.testing.expectEqualStrings("shuffle", strategy_options[2].object.get("strategy").?.string);
    try std.testing.expect(!strategy_options[2].object.get("feasible").?.bool);
    try std.testing.expectEqualStrings("single_shard_shuffle_unnecessary", strategy_options[2].object.get("feasibility").?.string);
}

test "api query builder recursive auto mode reports candidate budget truncation" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const result = try query_builder_agent.buildQueryBuilderResponseWithContext(arena, .{
        .table = "docs",
        .intent = "find raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .execution_mode = .recursive,
        .recursive = .{
            .max_depth = 1,
            .max_subcalls = 1,
            .max_concurrency = 1,
            .split_policy = .by_query,
            .merge_policy = .verify,
        },
    }, .{
        .schema_fields = &.{ "title", "body", "status" },
        .full_text_index_metadata = &.{.{ .name = "docs_text", .fields = &.{ "title", "body", "status" } }},
    }, null);

    try std.testing.expectEqual(metadata_openapi.AgentStatus.incomplete, result.status.?);
    try std.testing.expect(result.incomplete_details != null);
    try std.testing.expectEqualStrings("max_subcalls", result.incomplete_details.?.reason);

    const plan = result.plan.?;
    try std.testing.expectEqualStrings("recursive", plan.object.get("execution_mode").?.string);
    const recursive_plan = plan.object.get("recursive").?.object;
    try std.testing.expectEqual(@as(i64, 1), recursive_plan.get("scheduled_candidate_count").?.integer);
    try std.testing.expect(recursive_plan.get("candidate_mode_count").?.integer >= 2);
    try std.testing.expect(recursive_plan.get("truncated_candidate_count").?.integer >= 1);
    try std.testing.expect(recursive_plan.get("budget_limited").?.bool);
    try std.testing.expectEqual(@as(i64, 1), recursive_plan.get("scheduled_concurrency").?.integer);
    try std.testing.expectEqual(@as(i64, 1), recursive_plan.get("actual_concurrency").?.integer);

    const decomposition = result.steps.?[0];
    try std.testing.expectEqual(metadata_openapi.AgentStepKind.recursive_decomposition, decomposition.kind.?);
    try std.testing.expect(decomposition.details.?.object.get("budget_limited").?.bool);
    try std.testing.expectEqual(@as(i64, 1), decomposition.details.?.object.get("scheduled_candidate_count").?.integer);
}

test "api query builder recursive join nonunique right key emits estimated key stats" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{
        \\  "join": {
        \\    "right_table": "customers",
        \\    "join_type": "inner",
        \\    "on": {
        \\      "left_field": "customer_tier",
        \\      "right_field": "tier"
        \\    }
        \\  }
        \\}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try query_builder_agent.buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "orders",
        .intent = "orders with customers by tier",
        .schema_fields = &.{ "customer_tier", "amount", "status" },
        .mode = "join",
        .execution_mode = .recursive,
        .recursive = .{
            .max_depth = 1,
            .max_subcalls = 1,
            .max_concurrency = 1,
            .split_policy = .by_query,
            .merge_policy = .verify,
        },
        .constraints = constraints_tree.value,
    }, .{
        .schema_fields = &.{ "customer_tier", "amount", "status" },
        .doc_count = 5_000,
        .full_text_index_metadata = &.{.{ .name = "orders_text", .fields = &.{ "customer_tier", "amount", "status" } }},
        .related_tables = &.{.{
            .name = "customers",
            .schema_fields = &.{ "tier", "region" },
            .doc_count = 1_000,
            .shard_count = 2,
        }},
    }, null);

    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, result.status.?);
    const candidate_plans = result.plan.?.object.get("candidate_plans").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), candidate_plans.len);
    try std.testing.expectEqualStrings("tier", candidate_plans[0].object.get("join_right_key_field").?.string);
    try std.testing.expect(!candidate_plans[0].object.get("join_right_key_unique").?.bool);
    try std.testing.expectEqual(@as(i64, 100), candidate_plans[0].object.get("join_right_key_estimated_distinct").?.integer);
    switch (candidate_plans[0].object.get("join_right_key_avg_rows_per_key").?) {
        .integer => |avg| try std.testing.expectEqual(@as(i64, 10), avg),
        .float => |avg| try std.testing.expectEqual(@as(f64, 10.0), avg),
        else => return error.UnexpectedJoinRightKeyAvgRowsPerKey,
    }
    try std.testing.expectEqualStrings("schema_name_heuristic", candidate_plans[0].object.get("join_right_key_stats_source").?.string);
    try std.testing.expectEqualStrings("low", candidate_plans[0].object.get("join_right_key_stats_confidence").?.string);
    try std.testing.expectEqualStrings("medium_selectivity", candidate_plans[0].object.get("join_right_key_selectivity").?.string);
    const strategy_options = candidate_plans[0].object.get("join_strategy_options").?.array.items;
    try std.testing.expectEqualStrings("right_key_selectivity_estimated", strategy_options[0].object.get("feasibility").?.string);
}

test "api retrieval agent recursive mode maps child subcalls and merges" {
    const FakeRunner = struct {
        fn iface() retrieval_agent.QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, _: []const u8) !QueryResponse {
            try std.testing.expectEqualStrings("docs", table_name);
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"status":200,"took":1,"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"alpha body"}},{"_id":"doc:b","_score":0.8,"_source":{"content":"beta body"}}]}}]}
                ),
            };
        }
    };

    const FakeGeneration = struct {
        calls: usize = 0,

        fn iface(self: *@This()) retrieval_agent.GenerationRunner {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute_chain = executeChain,
                    .execute_chain_with_timeout_ms = executeChainWithTimeoutMs,
                    .max_concurrent_chain_calls = maxConcurrentChainCalls,
                },
            };
        }

        fn maxConcurrentChainCalls(_: *anyopaque, _: []const generating.ChainLink) usize {
            return 2;
        }

        fn executeChainWithTimeoutMs(ptr: *anyopaque, alloc: std.mem.Allocator, chain: []const generating.ChainLink, messages: []const generating.ChatMessage, timeout_ms: u64) !generating.GenerateResult {
            try std.testing.expect(timeout_ms > 0);
            return try executeChain(ptr, alloc, chain, messages);
        }

        fn executeChain(ptr: *anyopaque, alloc: std.mem.Allocator, chain: []const generating.ChainLink, messages: []const generating.ChatMessage) !generating.GenerateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(@as(usize, 1), chain.len);
            try std.testing.expectEqualStrings("local-generator", chain[0].generator.model);

            const prompt = messages[1].content.?.text;
            const content = switch (self.calls) {
                1 => blk: {
                    try std.testing.expect(std.mem.indexOf(u8, prompt, "doc:a") != null);
                    break :blk "child summary a cites doc:a";
                },
                2 => blk: {
                    try std.testing.expect(std.mem.indexOf(u8, prompt, "doc:b") != null);
                    break :blk "child summary b cites doc:b";
                },
                3 => blk: {
                    try std.testing.expect(std.mem.indexOf(u8, prompt, "Child 1") != null);
                    try std.testing.expect(std.mem.indexOf(u8, prompt, "child summary a cites doc:a") != null);
                    try std.testing.expect(std.mem.indexOf(u8, prompt, "child summary b cites doc:b") != null);
                    break :blk "merged recursive answer cites doc:a and doc:b";
                },
                else => return error.UnexpectedGenerationCall,
            };

            return .{
                .content = try alloc.dupe(u8, content),
                .allocator = alloc,
            };
        }
    };

    var fake_generation = FakeGeneration{};
    const body =
        \\{"query":"find alpha","stream":false,"execution_mode":"recursive","recursive":{"max_depth":1,"max_subcalls":2,"max_concurrency":1,"split_policy":"by_document","merge_policy":"verify","child_tool_policy":"inherit_narrowed","allowed_context_object_types":["document"]},"generator":{"provider":"antfly","model":"local-generator","api_url":"http://127.0.0.1:8082"},"steps":{"generation":{"enabled":true}},"queries":[{"table":"docs","semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":5}]}
    ;
    const encoded = try retrieval_agent.executeJson(std.testing.allocator, FakeRunner.iface(), fake_generation.iface(), body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(metadata_openapi.RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, parsed.value.status);
    try std.testing.expectEqualStrings("merged recursive answer cites doc:a and doc:b", parsed.value.generation.?);
    try std.testing.expectEqualStrings("local-generator", parsed.value.model.?);
    try std.testing.expectEqual(@as(usize, 3), fake_generation.calls);
    try std.testing.expect(parsed.value.trace_artifact != null);
    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, parsed.value.trace_artifact.?.final_status);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.trace_artifact.?.context_objects.len);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.trace_artifact.?.subcalls.len);
    try std.testing.expectEqual(parsed.value.steps.?.len, parsed.value.trace_artifact.?.steps.len);
    try std.testing.expectEqualStrings("doc:a", parsed.value.trace_artifact.?.context_objects[0].id);
    try std.testing.expectEqualStrings("recursive_subcall_1", parsed.value.trace_artifact.?.subcalls[0].id);
    try std.testing.expectEqual(metadata_openapi.AgentStepStatus.success, parsed.value.trace_artifact.?.subcalls[0].status);
    try std.testing.expectEqualStrings("fixed_tokenizer", parsed.value.trace_artifact.?.subcalls[0].token_count_method.?);

    var saw_decomposition = false;
    var subcall_count: usize = 0;
    var saw_merge = false;
    var saw_validation = false;
    for (parsed.value.steps.?) |step| {
        if (step.kind) |kind| switch (kind) {
            .recursive_decomposition => {
                saw_decomposition = true;
                try std.testing.expectEqual(@as(i64, 1), step.details.?.object.get("scheduled_concurrency").?.integer);
                try std.testing.expectEqual(@as(i64, 1), step.details.?.object.get("actual_concurrency").?.integer);
            },
            .recursive_subcall => {
                subcall_count += 1;
                try std.testing.expectEqualStrings("fixed_tokenizer", step.details.?.object.get("token_count_method").?.string);
                try std.testing.expect(step.details.?.object.get("estimated_context_tokens").?.integer > 0);
            },
            .recursive_merge => saw_merge = true,
            .validation => {
                if (std.mem.eql(u8, step.name, "recursive_merge_verification")) {
                    saw_validation = true;
                    try std.testing.expectEqual(metadata_openapi.AgentStepStatus.success, step.status.?);
                    try std.testing.expect(step.details.?.object.get("passed").?.bool);
                }
            },
            else => {},
        };
    }
    try std.testing.expect(saw_decomposition);
    try std.testing.expectEqual(@as(usize, 2), subcall_count);
    try std.testing.expect(saw_merge);
    try std.testing.expect(saw_validation);
}

test "api retrieval agent recursive mode executes child subcalls with configured concurrency" {
    const FakeRunner = struct {
        fn iface() retrieval_agent.QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, _: []const u8) !QueryResponse {
            try std.testing.expectEqualStrings("docs", table_name);
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"status":200,"took":1,"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"alpha body"}},{"_id":"doc:b","_score":0.9,"_source":{"content":"beta body"}},{"_id":"doc:c","_score":0.8,"_source":{"content":"gamma body"}},{"_id":"doc:d","_score":0.7,"_source":{"content":"delta body"}}]}}]}
                ),
            };
        }
    };

    const FakeGeneration = struct {
        calls: std.atomic.Value(usize) = .init(0),
        active_children: std.atomic.Value(usize) = .init(0),
        max_active_children: std.atomic.Value(usize) = .init(0),

        fn iface(self: *@This()) retrieval_agent.GenerationRunner {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute_chain = executeChain,
                    .execute_chain_with_timeout_ms = executeChainWithTimeoutMs,
                    .max_concurrent_chain_calls = maxConcurrentChainCalls,
                },
            };
        }

        fn maxConcurrentChainCalls(_: *anyopaque, _: []const generating.ChainLink) usize {
            return 2;
        }

        fn executeChainWithTimeoutMs(ptr: *anyopaque, alloc: std.mem.Allocator, chain: []const generating.ChainLink, messages: []const generating.ChatMessage, timeout_ms: u64) !generating.GenerateResult {
            try std.testing.expect(timeout_ms > 0);
            return try executeChain(ptr, alloc, chain, messages);
        }

        fn executeChain(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const generating.ChainLink, messages: []const generating.ChatMessage) !generating.GenerateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.calls.fetchAdd(1, .seq_cst);
            const is_merge = std.mem.indexOf(u8, messages[1].content.?.text, "Child 1") != null;
            if (!is_merge) {
                const active = self.active_children.fetchAdd(1, .seq_cst) + 1;
                updateMax(&self.max_active_children, active);
                sleepMs(100);
                _ = self.active_children.fetchSub(1, .seq_cst);
            }
            return .{
                .content = try alloc.dupe(u8, if (is_merge) "concurrent merged answer" else "child summary"),
                .allocator = alloc,
            };
        }

        fn updateMax(max_active: *std.atomic.Value(usize), value: usize) void {
            var observed = max_active.load(.monotonic);
            while (value > observed) {
                observed = max_active.cmpxchgWeak(observed, value, .monotonic, .monotonic) orelse break;
            }
        }

        fn sleepMs(ms: u64) void {
            var req: std.posix.timespec = .{
                .sec = @intCast(ms / std.time.ms_per_s),
                .nsec = @intCast((ms % std.time.ms_per_s) * std.time.ns_per_ms),
            };
            while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
                .SUCCESS => return,
                .INTR => continue,
                else => return,
            };
        }
    };

    var fake_generation = FakeGeneration{};
    const body =
        \\{"query":"find alpha","stream":false,"execution_mode":"recursive","recursive":{"max_depth":1,"max_subcalls":4,"max_concurrency":2,"split_policy":"by_document","merge_policy":"verify","child_tool_policy":"inherit_narrowed","allowed_context_object_types":["document"]},"generator":{"provider":"antfly","model":"local-generator","api_url":"http://127.0.0.1:8082"},"steps":{"generation":{"enabled":true}},"queries":[{"table":"docs","semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":5}]}
    ;
    const encoded = try retrieval_agent.executeJson(std.testing.allocator, FakeRunner.iface(), fake_generation.iface(), body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(metadata_openapi.RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, parsed.value.status);
    try std.testing.expectEqualStrings("concurrent merged answer", parsed.value.generation.?);
    try std.testing.expectEqual(@as(usize, 5), fake_generation.calls.load(.seq_cst));
    try std.testing.expect(fake_generation.max_active_children.load(.seq_cst) >= 2);

    const decomposition = for (parsed.value.steps.?) |step| {
        if (step.kind != null and step.kind.? == .recursive_decomposition) break step;
    } else return error.MissingRecursiveDecompositionStep;
    try std.testing.expectEqual(@as(i64, 2), decomposition.details.?.object.get("scheduled_concurrency").?.integer);
    try std.testing.expectEqual(@as(i64, 2), decomposition.details.?.object.get("actual_concurrency").?.integer);
}

test "api retrieval agent recursive verify merge fails when child citations are dropped" {
    const FakeRunner = struct {
        fn iface() retrieval_agent.QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, _: []const u8) !QueryResponse {
            try std.testing.expectEqualStrings("docs", table_name);
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"status":200,"took":1,"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"alpha body"}},{"_id":"doc:b","_score":0.8,"_source":{"content":"beta body"}}]}}]}
                ),
            };
        }
    };

    const FakeGeneration = struct {
        calls: usize = 0,

        fn iface(self: *@This()) retrieval_agent.GenerationRunner {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute_chain = executeChain,
                    .execute_chain_with_timeout_ms = executeChainWithTimeoutMs,
                },
            };
        }

        fn executeChainWithTimeoutMs(ptr: *anyopaque, alloc: std.mem.Allocator, chain: []const generating.ChainLink, messages: []const generating.ChatMessage, timeout_ms: u64) !generating.GenerateResult {
            try std.testing.expect(timeout_ms > 0);
            return try executeChain(ptr, alloc, chain, messages);
        }

        fn executeChain(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const generating.ChainLink, messages: []const generating.ChatMessage) !generating.GenerateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const prompt = messages[1].content.?.text;
            const content = switch (self.calls) {
                1 => blk: {
                    try std.testing.expect(std.mem.indexOf(u8, prompt, "doc:a") != null);
                    break :blk "child summary cites doc:a";
                },
                2 => blk: {
                    try std.testing.expect(std.mem.indexOf(u8, prompt, "doc:b") != null);
                    break :blk "child summary cites doc:b";
                },
                3 => "merged answer drops citations",
                else => return error.UnexpectedGenerationCall,
            };
            return .{
                .content = try alloc.dupe(u8, content),
                .allocator = alloc,
            };
        }
    };

    var fake_generation = FakeGeneration{};
    const body =
        \\{"query":"find alpha","stream":false,"execution_mode":"recursive","recursive":{"max_depth":1,"max_subcalls":2,"max_concurrency":1,"split_policy":"by_document","merge_policy":"verify","child_tool_policy":"inherit_narrowed","allowed_context_object_types":["document"]},"generator":{"provider":"antfly","model":"local-generator","api_url":"http://127.0.0.1:8082"},"steps":{"generation":{"enabled":true}},"queries":[{"table":"docs","semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":5}]}
    ;
    const encoded = try retrieval_agent.executeJson(std.testing.allocator, FakeRunner.iface(), fake_generation.iface(), body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(metadata_openapi.RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.failed, parsed.value.status);
    try std.testing.expectEqualStrings("merged answer drops citations", parsed.value.generation.?);
    try std.testing.expectEqual(@as(usize, 3), fake_generation.calls);

    const verification = for (parsed.value.steps.?) |step| {
        if (step.kind != null and step.kind.? == .validation and std.mem.eql(u8, step.name, "recursive_merge_verification")) break step;
    } else return error.MissingRecursiveMergeVerificationStep;
    try std.testing.expectEqual(metadata_openapi.AgentStepStatus.@"error", verification.status.?);
    try std.testing.expect(!verification.details.?.object.get("passed").?.bool);
    try std.testing.expectEqual(@as(usize, 2), verification.details.?.object.get("missing_context_ids").?.array.items.len);

    const merge = for (parsed.value.steps.?) |step| {
        if (step.kind != null and step.kind.? == .recursive_merge) break step;
    } else return error.MissingRecursiveMergeStep;
    try std.testing.expectEqual(@as(i64, 2), merge.details.?.object.get("missing_verified_context_count").?.integer);
    try std.testing.expect(!merge.details.?.object.get("verified").?.bool);
}

test "api retrieval agent recursive mode reports incomplete when max_subcalls truncates context" {
    const FakeRunner = struct {
        fn iface() retrieval_agent.QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, _: []const u8) !QueryResponse {
            try std.testing.expectEqualStrings("docs", table_name);
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"status":200,"took":1,"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"alpha body"}},{"_id":"doc:b","_score":0.8,"_source":{"content":"beta body"}},{"_id":"doc:c","_score":0.7,"_source":{"content":"gamma body"}}]}}]}
                ),
            };
        }
    };

    const FakeGeneration = struct {
        calls: usize = 0,

        fn iface(self: *@This()) retrieval_agent.GenerationRunner {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute_chain = executeChain,
                    .execute_chain_with_timeout_ms = executeChainWithTimeoutMs,
                },
            };
        }

        fn executeChainWithTimeoutMs(ptr: *anyopaque, alloc: std.mem.Allocator, chain: []const generating.ChainLink, messages: []const generating.ChatMessage, timeout_ms: u64) !generating.GenerateResult {
            try std.testing.expect(timeout_ms > 0);
            return try executeChain(ptr, alloc, chain, messages);
        }

        fn executeChain(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const generating.ChainLink, _: []const generating.ChatMessage) !generating.GenerateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const content = if (self.calls == 3) "partial merged answer" else "child summary";
            return .{
                .content = try alloc.dupe(u8, content),
                .allocator = alloc,
            };
        }
    };

    var fake_generation = FakeGeneration{};
    const body =
        \\{"query":"find alpha","stream":false,"execution_mode":"recursive","recursive":{"max_depth":1,"max_subcalls":2,"max_concurrency":1,"split_policy":"by_document","merge_policy":"verify","child_tool_policy":"inherit_narrowed","allowed_context_object_types":["document"]},"generator":{"provider":"antfly","model":"local-generator","api_url":"http://127.0.0.1:8082"},"steps":{"generation":{"enabled":true}},"queries":[{"table":"docs","semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":5}]}
    ;
    const encoded = try retrieval_agent.executeJson(std.testing.allocator, FakeRunner.iface(), fake_generation.iface(), body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(metadata_openapi.RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.incomplete, parsed.value.status);
    try std.testing.expect(parsed.value.incomplete_details != null);
    try std.testing.expectEqualStrings("max_subcalls", parsed.value.incomplete_details.?.reason);
    try std.testing.expectEqualStrings("partial merged answer", parsed.value.generation.?);
    try std.testing.expectEqual(@as(usize, 3), fake_generation.calls);

    const decomposition = for (parsed.value.steps.?) |step| {
        if (step.kind != null and step.kind.? == .recursive_decomposition) break step;
    } else return error.MissingRecursiveDecompositionStep;
    try std.testing.expectEqual(@as(i64, 3), decomposition.details.?.object.get("hit_count").?.integer);
    try std.testing.expectEqual(@as(i64, 2), decomposition.details.?.object.get("child_frame_count").?.integer);
    try std.testing.expectEqual(@as(i64, 1), decomposition.details.?.object.get("truncated_context_count").?.integer);
    try std.testing.expect(decomposition.details.?.object.get("budget_limited").?.bool);
}

test "api retrieval agent recursive mode skips children over context token budget" {
    const FakeRunner = struct {
        fn iface() retrieval_agent.QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, _: []const u8) !QueryResponse {
            try std.testing.expectEqualStrings("docs", table_name);
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"status":200,"took":1,"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"alpha body has several words"}},{"_id":"doc:b","_score":0.8,"_source":{"content":"beta body also has several words"}}]}}]}
                ),
            };
        }
    };

    const FakeGeneration = struct {
        calls: usize = 0,

        fn iface(self: *@This()) retrieval_agent.GenerationRunner {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute_chain = executeChain,
                    .execute_chain_with_timeout_ms = executeChainWithTimeoutMs,
                },
            };
        }

        fn executeChainWithTimeoutMs(ptr: *anyopaque, alloc: std.mem.Allocator, chain: []const generating.ChainLink, messages: []const generating.ChatMessage, timeout_ms: u64) !generating.GenerateResult {
            try std.testing.expect(timeout_ms > 0);
            return try executeChain(ptr, alloc, chain, messages);
        }

        fn executeChain(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const generating.ChainLink, messages: []const generating.ChatMessage) !generating.GenerateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expect(std.mem.indexOf(u8, messages[1].content.?.text, "No child summaries were produced") != null);
            return .{
                .content = try alloc.dupe(u8, "incomplete answer without child context"),
                .allocator = alloc,
            };
        }
    };

    var fake_generation = FakeGeneration{};
    const body =
        \\{"query":"find alpha","stream":false,"execution_mode":"recursive","recursive":{"max_depth":1,"max_subcalls":2,"max_concurrency":1,"max_child_context_tokens":1,"split_policy":"by_document","merge_policy":"verify","child_tool_policy":"inherit_narrowed","allowed_context_object_types":["document"]},"generator":{"provider":"antfly","model":"local-generator","api_url":"http://127.0.0.1:8082"},"steps":{"generation":{"enabled":true}},"queries":[{"table":"docs","semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":5}]}
    ;
    const encoded = try retrieval_agent.executeJson(std.testing.allocator, FakeRunner.iface(), fake_generation.iface(), body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(metadata_openapi.RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.incomplete, parsed.value.status);
    try std.testing.expect(parsed.value.incomplete_details != null);
    try std.testing.expectEqualStrings("max_child_context_tokens", parsed.value.incomplete_details.?.reason);
    try std.testing.expectEqualStrings("incomplete answer without child context", parsed.value.generation.?);
    try std.testing.expectEqual(@as(usize, 1), fake_generation.calls);

    var skipped_subcalls: usize = 0;
    var decomposition_actual_concurrency: ?i64 = null;
    var merge_skipped_count: ?i64 = null;
    for (parsed.value.steps.?) |step| {
        if (step.kind) |kind| switch (kind) {
            .recursive_decomposition => {
                decomposition_actual_concurrency = step.details.?.object.get("actual_concurrency").?.integer;
            },
            .recursive_subcall => {
                if (step.status != null and step.status.? == .skipped) {
                    skipped_subcalls += 1;
                    try std.testing.expectEqualStrings("max_child_context_tokens", step.details.?.object.get("skip_reason").?.string);
                    try std.testing.expectEqualStrings("fixed_tokenizer", step.details.?.object.get("token_count_method").?.string);
                    try std.testing.expect(step.details.?.object.get("estimated_context_tokens").?.integer > 1);
                }
            },
            .recursive_merge => merge_skipped_count = step.details.?.object.get("skipped_child_count").?.integer,
            else => {},
        };
    }
    try std.testing.expectEqual(@as(i64, 0), decomposition_actual_concurrency.?);
    try std.testing.expectEqual(@as(usize, 2), skipped_subcalls);
    try std.testing.expectEqual(@as(i64, 2), merge_skipped_count.?);
}

test "api retrieval agent recursive mode stops scheduling after wall time budget" {
    const FakeRunner = struct {
        fn iface() retrieval_agent.QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, _: []const u8) !QueryResponse {
            try std.testing.expectEqualStrings("docs", table_name);
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"status":200,"took":1,"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"alpha body"}},{"_id":"doc:b","_score":0.8,"_source":{"content":"beta body"}},{"_id":"doc:c","_score":0.7,"_source":{"content":"gamma body"}}]}}]}
                ),
            };
        }
    };

    const FakeGeneration = struct {
        calls: usize = 0,

        fn iface(self: *@This()) retrieval_agent.GenerationRunner {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute_chain = executeChain,
                    .execute_chain_with_timeout_ms = executeChainWithTimeoutMs,
                },
            };
        }

        fn executeChainWithTimeoutMs(ptr: *anyopaque, alloc: std.mem.Allocator, chain: []const generating.ChainLink, messages: []const generating.ChatMessage, timeout_ms: u64) !generating.GenerateResult {
            try std.testing.expect(timeout_ms > 0);
            return try executeChain(ptr, alloc, chain, messages);
        }

        fn executeChain(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const generating.ChainLink, messages: []const generating.ChatMessage) !generating.GenerateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const is_merge = std.mem.indexOf(u8, messages[1].content.?.text, "Child outputs:") != null;
            if (!is_merge) sleepMs(1200);
            return .{
                .content = try alloc.dupe(u8, if (is_merge) "wall-time bounded answer" else "slow child summary"),
                .allocator = alloc,
            };
        }

        fn sleepMs(ms: u64) void {
            var req: std.posix.timespec = .{
                .sec = @intCast(ms / std.time.ms_per_s),
                .nsec = @intCast((ms % std.time.ms_per_s) * std.time.ns_per_ms),
            };
            while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
                .SUCCESS => return,
                .INTR => continue,
                else => return,
            };
        }
    };

    var fake_generation = FakeGeneration{};
    const body =
        \\{"query":"find alpha","stream":false,"execution_mode":"recursive","recursive":{"max_depth":1,"max_subcalls":3,"max_concurrency":1,"max_wall_time_ms":1000,"split_policy":"by_document","merge_policy":"verify","child_tool_policy":"inherit_narrowed","allowed_context_object_types":["document"]},"generator":{"provider":"antfly","model":"local-generator","api_url":"http://127.0.0.1:8082"},"steps":{"generation":{"enabled":true}},"queries":[{"table":"docs","semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":5}]}
    ;
    const encoded = try retrieval_agent.executeJson(std.testing.allocator, FakeRunner.iface(), fake_generation.iface(), body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(metadata_openapi.RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.incomplete, parsed.value.status);
    try std.testing.expect(parsed.value.incomplete_details != null);
    try std.testing.expectEqualStrings("max_wall_time_ms", parsed.value.incomplete_details.?.reason);
    try std.testing.expect(std.mem.startsWith(u8, parsed.value.generation.?, "Recursive execution stopped before merge: max_wall_time_ms"));
    try std.testing.expect(std.mem.indexOf(u8, parsed.value.generation.?, "slow child summary") != null);
    try std.testing.expectEqual(@as(usize, 1), fake_generation.calls);

    var subcall_count: usize = 0;
    var decomposition_actual_concurrency: ?i64 = null;
    const merge = for (parsed.value.steps.?) |step| {
        if (step.kind != null and step.kind.? == .recursive_decomposition) {
            decomposition_actual_concurrency = step.details.?.object.get("actual_concurrency").?.integer;
        }
        if (step.kind != null and step.kind.? == .recursive_subcall) subcall_count += 1;
        if (step.kind != null and step.kind.? == .recursive_merge) break step;
    } else return error.MissingRecursiveMergeStep;
    try std.testing.expectEqual(@as(i64, 1), decomposition_actual_concurrency.?);
    try std.testing.expectEqual(@as(usize, 3), subcall_count);
    try std.testing.expectEqual(metadata_openapi.AgentStepStatus.skipped, merge.status.?);
    try std.testing.expectEqual(@as(i64, 2), merge.details.?.object.get("skipped_child_count").?.integer);
    try std.testing.expect(merge.details.?.object.get("wall_time_exhausted").?.bool);
    try std.testing.expect(merge.details.?.object.get("elapsed_ms").?.integer >= 1);
}

test "api retrieval agent recursive mode refreshes serial child timeout at launch" {
    const FakeRunner = struct {
        fn iface() retrieval_agent.QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, _: []const u8) !QueryResponse {
            try std.testing.expectEqualStrings("docs", table_name);
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"status":200,"took":1,"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"alpha body"}},{"_id":"doc:b","_score":0.8,"_source":{"content":"beta body"}}]}}]}
                ),
            };
        }
    };

    const FakeGeneration = struct {
        calls: usize = 0,
        child_calls: usize = 0,
        first_child_timeout_ms: u64 = 0,

        fn iface(self: *@This()) retrieval_agent.GenerationRunner {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute_chain = executeChain,
                    .execute_chain_with_timeout_ms = executeChainWithTimeoutMs,
                },
            };
        }

        fn executeChainWithTimeoutMs(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const generating.ChainLink, messages: []const generating.ChatMessage, timeout_ms: u64) !generating.GenerateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expect(timeout_ms > 0);
            const is_merge = std.mem.indexOf(u8, messages[1].content.?.text, "Child outputs:") != null;
            if (is_merge) {
                return .{
                    .content = try alloc.dupe(u8, "fresh-timeout merged answer"),
                    .allocator = alloc,
                };
            }

            self.child_calls += 1;
            if (self.child_calls == 1) {
                self.first_child_timeout_ms = timeout_ms;
                sleepMs(150);
            } else if (self.child_calls == 2) {
                try std.testing.expect(timeout_ms < self.first_child_timeout_ms);
            } else {
                return error.UnexpectedGenerationCall;
            }

            return .{
                .content = try alloc.dupe(u8, if (self.child_calls == 1) "first child summary" else "second child summary"),
                .allocator = alloc,
            };
        }

        fn executeChain(ptr: *anyopaque, alloc: std.mem.Allocator, chain: []const generating.ChainLink, messages: []const generating.ChatMessage) !generating.GenerateResult {
            return try executeChainWithTimeoutMs(ptr, alloc, chain, messages, std.time.ms_per_s);
        }

        fn sleepMs(ms: u64) void {
            var req: std.posix.timespec = .{
                .sec = @intCast(ms / std.time.ms_per_s),
                .nsec = @intCast((ms % std.time.ms_per_s) * std.time.ns_per_ms),
            };
            while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
                .SUCCESS => return,
                .INTR => continue,
                else => return,
            };
        }
    };

    var fake_generation = FakeGeneration{};
    const body =
        \\{"query":"find alpha","stream":false,"execution_mode":"recursive","recursive":{"max_depth":1,"max_subcalls":2,"max_concurrency":1,"max_wall_time_ms":1000,"split_policy":"by_document","merge_policy":"verify","child_tool_policy":"inherit_narrowed","allowed_context_object_types":["document"]},"generator":{"provider":"antfly","model":"local-generator","api_url":"http://127.0.0.1:8082"},"steps":{"generation":{"enabled":true}},"queries":[{"table":"docs","semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":5}]}
    ;
    const encoded = try retrieval_agent.executeJson(std.testing.allocator, FakeRunner.iface(), fake_generation.iface(), body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(metadata_openapi.RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, parsed.value.status);
    try std.testing.expectEqualStrings("fresh-timeout merged answer", parsed.value.generation.?);
    try std.testing.expectEqual(@as(usize, 3), fake_generation.calls);
    try std.testing.expectEqual(@as(usize, 2), fake_generation.child_calls);
}

test "artifact enrichment request permits asset full text routing" {
    const config_json = try table_contract.parseArtifactEnrichmentRequest(
        std.testing.allocator,
        "image_caption_v1",
        "{\"kind\":\"asset\",\"field\":\"caption_json\",\"full_text_index\":true}",
    );
    defer std.testing.allocator.free(config_json);

    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"kind\":\"asset\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"full_text_index\":true") != null);
}

test "distributed graph result_ref fail-closed guards are covered" {
    try distributed_graph.testResultRefFailClosedGuards(std.testing.allocator);
}

test "api distributed graph hydrate carries identity generation and clears cross-range ordinals" {
    try distributed_graph.testHydrateIdentityGenerationAndCrossRangeOrdinalBoundary(std.testing.allocator);
}

test "api distributed graph cross-table hydrate clears query scoped filter" {
    try distributed_graph.testCrossTableHydrateClearsQueryScopedFilterAndOrdinals(std.testing.allocator);
}

test "public graph result_ref fail-closed guards are covered" {
    try public_graph_query.testResolveGraphSelectorFailClosedGuard(std.testing.allocator);
}

test "api table reads reject distributed resolved doc filters" {
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
    var hierarchy_query = try query_contract.parseQueryRequest(alloc, null, "docs",
        \\{"full_text_search":{"match":"needle","field":"content"},"hierarchy":{"return_level":"source","include":["unit","chunk"],"max_children_per_parent":2}}
    );
    defer hierarchy_query.deinit(alloc);
    try std.testing.expectEqual(@as(@TypeOf(hierarchy_query.req.return_mode), .parent_with_chunks), hierarchy_query.req.return_mode);
    try std.testing.expectEqual(@as(u32, 2), hierarchy_query.req.max_chunks_per_parent);
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
