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
const group_ids = @import("../common/group_ids.zig");
const raft_engine = @import("raft_engine");
const metadata_mod = @import("../metadata/mod.zig");
const metadata_http_client = @import("../metadata/http_client.zig");
const metadata_http_server = @import("../metadata/http_server.zig");
const metadata_service = @import("../metadata/service.zig");
const data_runtime = @import("../data/runtime.zig");
const raft_host = @import("../raft/host.zig");
const http_server = @import("http_server.zig");
const http_client = @import("http_client.zig");
const backups_api = @import("backups.zig");
const std_http_executor = @import("../raft/transport/std_http_executor.zig");
const std_http_listener = @import("../raft/transport/std_http_listener.zig");
const http_common = @import("../raft/transport/http_common.zig");
const raft_routes = @import("../raft/transport/routes.zig");
const routes = @import("http_routes.zig");
const db_mod = @import("../storage/db/mod.zig");
const internal_keys = @import("../storage/internal_keys.zig");
const table_reads = @import("table_reads.zig");
const table_catalog = @import("table_catalog.zig");
const table_writes = @import("table_writes.zig");
const generating_api_openapi = @import("antfly_generating_api_openapi");
const transactions_api = @import("transactions.zig");
const test_contract_helpers = @import("test_contract_helpers.zig");
const public_test_helpers = @import("../public_test_helpers.zig");
const indexes_openapi = @import("antfly_indexes_openapi");
const metadata_openapi = @import("antfly_metadata_openapi");
const query_openapi = @import("antfly_query_openapi");
const RetrievalAgentResult = metadata_openapi.RetrievalAgentResult;
const AgentStatus = metadata_openapi.AgentStatus;
const RetrievalStrategy = metadata_openapi.RetrievalStrategy;

fn parseJsonBody(comptime T: type, alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(T) {
    return try std.json.parseFromSlice(T, alloc, body, .{});
}

fn parseJsonBodyIgnoreUnknown(comptime T: type, alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(T) {
    return try std.json.parseFromSlice(T, alloc, body, .{ .ignore_unknown_fields = true });
}

fn jsonValueContainsText(value: std.json.Value, needle: []const u8) bool {
    switch (value) {
        .string => |text| return std.mem.indexOf(u8, text, needle) != null,
        .array => |items| {
            for (items.items) |item| {
                if (jsonValueContainsText(item, needle)) return true;
            }
            return false;
        },
        .object => |obj| {
            var it = obj.iterator();
            while (it.next()) |entry| {
                if (std.mem.indexOf(u8, entry.key_ptr.*, needle) != null) return true;
                if (jsonValueContainsText(entry.value_ptr.*, needle)) return true;
            }
            return false;
        },
        else => return false,
    }
}

fn messagesContainText(messages: anytype, needle: []const u8) bool {
    for (messages) |message| {
        if (std.mem.indexOf(u8, message.content, needle) != null) return true;
    }
    return false;
}

fn stringSliceContainsText(values: []const []const u8, needle: []const u8) bool {
    for (values) |value| {
        if (std.mem.indexOf(u8, value, needle) != null) return true;
    }
    return false;
}

const TestEmbeddingRequest = struct {
    model: std.json.Value,
    input: std.json.Value,
};

const TestAntflyChunkRequest = struct {
    input: std.json.Value,
    config: struct {
        model: []const u8,
    },
};

const TestAntflyEmbedRequest = struct {
    model: []const u8,
    input: std.json.Value,
};

const TestAntflyRerankRequest = struct {
    prompts: []const []const u8,
};

const TestAntflyGenerateRequest = struct {
    messages: []const struct {
        content: []const u8,
    },
};

const TestSseEvent = struct {
    event: []const u8,
    data: []const u8,
};

fn parseSseEventsAlloc(alloc: std.mem.Allocator, body: []const u8) ![]TestSseEvent {
    var events = std.ArrayListUnmanaged(TestSseEvent).empty;
    errdefer events.deinit(alloc);

    var frames = std.mem.splitSequence(u8, body, "\n\n");
    while (frames.next()) |frame| {
        if (frame.len == 0) continue;
        var event_name: ?[]const u8 = null;
        var data: ?[]const u8 = null;
        var lines = std.mem.splitScalar(u8, frame, '\n');
        while (lines.next()) |line| {
            if (std.mem.startsWith(u8, line, "event: ")) {
                event_name = line["event: ".len..];
            } else if (std.mem.startsWith(u8, line, "data: ")) {
                data = line["data: ".len..];
            }
        }
        if (event_name != null and data != null) {
            try events.append(alloc, .{
                .event = event_name.?,
                .data = data.?,
            });
        }
    }

    return try events.toOwnedSlice(alloc);
}

const LookupTitle = struct {
    title: []const u8,
};

const UserName = struct {
    name: []const u8,
};

const OrderItem = struct {
    item: []const u8,
};

const RetrievalClarificationProgress = struct {
    id: []const u8,
    kind: []const u8,
    name: []const u8,
    phase: []const u8,
    questions: ?[]metadata_openapi.AgentQuestion = null,
};

const IndexStatusSummary = struct {
    config: struct {
        name: []const u8,
    },
    status: struct {
        backfill_active: ?bool = null,
        doc_count: ?u64 = null,
        node_count: ?u64 = null,
        edge_count: ?u64 = null,
    },
};

fn startMetadataAdminListener(
    alloc: std.mem.Allocator,
    svc: *metadata_service.MetadataService,
    server: *metadata_http_server.MetadataHttpServer,
    listener: *std_http_listener.StdHttpListener,
) ![]u8 {
    server.* = metadata_http_server.MetadataHttpServer.init(
        alloc,
        .{},
        metadata_http_server.AdminSource.fromMetadataService(svc),
    );
    listener.* = std_http_listener.StdHttpListener.init(alloc, .{}, server.executor());
    try listener.start();
    return try listener.baseUri(alloc);
}

const Factory = struct {
    alloc: std.mem.Allocator,
    store: *raft_engine.core.MemoryStorage,

    fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_descriptor = buildDescriptor,
                .free_descriptor = freeDescriptor,
            },
        };
    }

    fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
        errdefer self.alloc.free(peers);
        var bootstrap = try raft_host.catalog.runtimeBootstrapFromRecord(self.alloc, record);
        errdefer raft_host.catalog.freeRuntimeBootstrap(self.alloc, &bootstrap);
        return .{
            .group = .{
                .group_id = record.group_id,
                .local_node_id = record.local_node_id,
                .raft_config = .{
                    .id = record.local_node_id,
                    .group_id = record.group_id,
                    .peers = peers,
                    .election_tick = 5,
                    .heartbeat_tick = 1,
                    .pre_vote = false,
                    .check_quorum = true,
                },
                .storage = self.store.storage(),
            },
            .bootstrap = bootstrap,
        };
    }

    fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        raft_host.catalog.freeRuntimeBootstrap(alloc, &desc.bootstrap);
        self.alloc.free(desc.group.raft_config.peers);
    }
};

const FakeEmbeddingProvider = struct {
    fn executor() http_common.RequestExecutor {
        return .{
            .ptr = undefined,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        try std.testing.expectEqual(http_common.Method.POST, req.method);
        try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));

        var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, alloc, req.body);
        defer parsed_req.deinit();

        const vector = if (jsonValueContainsText(parsed_req.value.input, "alpha concept") or jsonValueContainsText(parsed_req.value.input, "alpha body"))
            "[1,0,0]"
        else if (jsonValueContainsText(parsed_req.value.input, "beta body"))
            "[0,1,0]"
        else
            "[0,0,1]";

        const body = try std.fmt.allocPrint(alloc, "{{\"object\":\"list\",\"data\":[{{\"object\":\"embedding\",\"index\":0,\"embedding\":{s}}}],\"model\":\"test-embed\",\"usage\":{{\"prompt_tokens\":1,\"total_tokens\":1}}}}", .{vector});

        return .{
            .status = 200,
            .content_type = try alloc.dupe(u8, "application/json"),
            .body = body,
        };
    }
};

fn encodeDenseEmbeddingResponse(alloc: std.mem.Allocator, vector: []const f32) ![]u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    errdefer buf.deinit(alloc);

    try buf.appendSlice(alloc, "{\"object\":\"list\",\"data\":[{\"object\":\"embedding\",\"index\":0,\"embedding\":[");
    for (vector, 0..) |value, i| {
        if (i > 0) try buf.append(alloc, ',');
        const num = try std.fmt.allocPrint(alloc, "{d}", .{value});
        defer alloc.free(num);
        try buf.appendSlice(alloc, num);
    }
    try buf.appendSlice(alloc, "]}],\"model\":\"test-embed\",\"usage\":{\"prompt_tokens\":1,\"total_tokens\":1}}");
    return try buf.toOwnedSlice(alloc);
}

fn encodeSparseEmbeddingResponse(
    alloc: std.mem.Allocator,
    indices: []const i32,
    values: []const f32,
) ![]u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    errdefer buf.deinit(alloc);

    try buf.appendSlice(alloc, "{\"object\":\"list\",\"data\":[{\"object\":\"embedding\",\"index\":0,\"embedding\":{\"indices\":[");
    for (indices, 0..) |value, i| {
        if (i > 0) try buf.append(alloc, ',');
        const num = try std.fmt.allocPrint(alloc, "{d}", .{value});
        defer alloc.free(num);
        try buf.appendSlice(alloc, num);
    }
    try buf.appendSlice(alloc, "],\"values\":[");
    for (values, 0..) |value, i| {
        if (i > 0) try buf.append(alloc, ',');
        const num = try std.fmt.allocPrint(alloc, "{d}", .{value});
        defer alloc.free(num);
        try buf.appendSlice(alloc, num);
    }
    try buf.appendSlice(alloc, "]}}],\"model\":\"test-embed\",\"usage\":{\"prompt_tokens\":1,\"total_tokens\":1}}");
    return try buf.toOwnedSlice(alloc);
}

const FakeAntflyProvider = struct {
    fn executor() http_common.RequestExecutor {
        return .{
            .ptr = undefined,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        try std.testing.expectEqual(http_common.Method.POST, req.method);

        if (std.mem.endsWith(u8, req.uri, "/api/chunk")) {
            var parsed_req = try parseJsonBodyIgnoreUnknown(TestAntflyChunkRequest, alloc, req.body);
            defer parsed_req.deinit();
            try std.testing.expectEqualStrings("antfly-chunker-v1", parsed_req.value.config.model);
            const body = if (jsonValueContainsText(parsed_req.value.input, "beta body"))
                try alloc.dupe(u8,
                    \\{"object":"list","data":[
                    \\  {"id":0,"mime_type":"text/plain","text":"beta body","start_char":0,"end_char":9},
                    \\  {"id":1,"mime_type":"text/plain","text":"chunk tail","start_char":10,"end_char":20}
                    \\]}
                )
            else
                try alloc.dupe(u8,
                    \\{"object":"list","data":[
                    \\  {"id":0,"mime_type":"text/plain","text":"alpha body","start_char":0,"end_char":10},
                    \\  {"id":1,"mime_type":"text/plain","text":"chunk tail","start_char":11,"end_char":21}
                    \\]}
                );
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = body,
            };
        }

        if (std.mem.endsWith(u8, req.uri, "/embed") or std.mem.endsWith(u8, req.uri, "/embeddings")) {
            var parsed_req = try parseJsonBodyIgnoreUnknown(TestAntflyEmbedRequest, alloc, req.body);
            defer parsed_req.deinit();
            if (std.mem.eql(u8, parsed_req.value.model, "antfly-sparse-v1")) {
                const body = if (jsonValueContainsText(parsed_req.value.input, "alpha body"))
                    try encodeSparseEmbeddingResponse(alloc, &.{ 7, 42 }, &.{ 1.5, 0.5 })
                else if (jsonValueContainsText(parsed_req.value.input, "beta body"))
                    try encodeSparseEmbeddingResponse(alloc, &.{ 7, 42 }, &.{ 0.25, 1.0 })
                else
                    try encodeSparseEmbeddingResponse(alloc, &.{99}, &.{0.1});
                return .{
                    .status = 200,
                    .content_type = try alloc.dupe(u8, "application/json"),
                    .body = body,
                };
            }

            const vector: [3]f32 = if (jsonValueContainsText(parsed_req.value.input, "alpha concept") or jsonValueContainsText(parsed_req.value.input, "alpha body"))
                .{ 1, 0, 0 }
            else if (jsonValueContainsText(parsed_req.value.input, "image/png") or jsonValueContainsText(parsed_req.value.input, "media"))
                .{ 1, 0, 0 }
            else if (jsonValueContainsText(parsed_req.value.input, "beta body"))
                .{ 0, 1, 0 }
            else
                .{ 0, 0, 1 };
            const body = try encodeDenseEmbeddingResponse(alloc, vector[0..]);
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = body,
            };
        }

        if (std.mem.endsWith(u8, req.uri, "/rerank")) {
            var parsed_req = try parseJsonBodyIgnoreUnknown(TestAntflyRerankRequest, alloc, req.body);
            defer parsed_req.deinit();
            const scores = if (stringSliceContainsText(parsed_req.value.prompts, "alpha body") and stringSliceContainsText(parsed_req.value.prompts, "beta body"))
                "[0.1,0.9]"
            else
                "[0.9]";
            const body = try std.fmt.allocPrint(alloc, "{{\"scores\":{s}}}", .{scores});
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = body,
            };
        }

        if (std.mem.endsWith(u8, req.uri, "/generate")) {
            var parsed_req = try parseJsonBodyIgnoreUnknown(TestAntflyGenerateRequest, alloc, req.body);
            defer parsed_req.deinit();
            const content = if (messagesContainText(parsed_req.value.messages, "doc:a") or messagesContainText(parsed_req.value.messages, "hello retrieval"))
                "Generated answer citing doc:a"
            else
                "Generated answer";
            const body = try std.fmt.allocPrint(alloc, "{{\"choices\":[{{\"message\":{{\"content\":{f}}}}}]}}", .{
                std.json.fmt(content, .{}),
            });
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = body,
            };
        }

        return error.TestUnexpectedResult;
    }
};

test "public api smoke e2e creates table inserts and queries documents" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-full-e2e-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-full-e2e-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = group_ids.main_metadata_group_id,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = group_ids.main_metadata_group_id,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "docs table");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);
    var created_table = try std.json.parseFromSlice(metadata_openapi.Table, std.testing.allocator, created.body, .{});
    defer created_table.deinit();
    try std.testing.expectEqualStrings("docs", created_table.value.name);
    try std.testing.expectEqualStrings("docs table", created_table.value.description.?);
    try std.testing.expectEqual(@as(usize, 1), created_table.value.indexes.map.count());
    try std.testing.expect(created_table.value.indexes.map.get("full_text_index_v0") != null);

    const customers_create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "customers table");
    defer std.testing.allocator.free(customers_create_body);
    var created_customers = try client.createTable(base_uri, "customers", customers_create_body);
    defer created_customers.deinit(std.testing.allocator);

    const addresses_create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "addresses table");
    defer std.testing.allocator.free(addresses_create_body);
    var created_addresses = try client.createTable(base_uri, "addresses", addresses_create_body);
    defer created_addresses.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const projected_ranges = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, projected_ranges);
    try std.testing.expect(projected_ranges.len > 0);
    const group_id = projected_ranges[0].group_id;
    const provisioned_db_path = try metadata_mod.groupDbPathFromReplicaRoot(std.testing.allocator, replica_root, group_id);
    defer std.testing.allocator.free(provisioned_db_path);

    var db = try db_mod.DB.open(std.testing.allocator, provisioned_db_path, .{});
    defer db.close();
    try std.testing.expect(db.core.index_manager.textIndex("full_text_index_v0") != null);

    var table_detail = try client.fetchTable(base_uri, "docs");
    defer table_detail.deinit(std.testing.allocator);
    var parsed_detail = try std.json.parseFromSlice(metadata_openapi.TableStatus, std.testing.allocator, table_detail.body, .{});
    defer parsed_detail.deinit();
    try std.testing.expectEqualStrings("docs", parsed_detail.value.name);
    try std.testing.expect(parsed_detail.value.indexes.map.get("full_text_index_v0") != null);

    var listed_tables = try client.fetchTables(base_uri, null);
    defer listed_tables.deinit(std.testing.allocator);
    var parsed_table_list = try std.json.parseFromSlice([]metadata_openapi.TableStatus, std.testing.allocator, listed_tables.body, .{});
    defer parsed_table_list.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_table_list.value.len);
    try std.testing.expectEqualStrings("docs", parsed_table_list.value[0].name);

    var prefixed_tables = try client.fetchTables(base_uri, "do");
    defer prefixed_tables.deinit(std.testing.allocator);
    var parsed_prefixed_tables = try std.json.parseFromSlice([]metadata_openapi.TableStatus, std.testing.allocator, prefixed_tables.body, .{});
    defer parsed_prefixed_tables.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_prefixed_tables.value.len);

    var missing_prefix_tables = try client.fetchTables(base_uri, "zzz");
    defer missing_prefix_tables.deinit(std.testing.allocator);
    var parsed_missing_prefix_tables = try std.json.parseFromSlice([]metadata_openapi.TableStatus, std.testing.allocator, missing_prefix_tables.body, .{});
    defer parsed_missing_prefix_tables.deinit();
    try std.testing.expectEqual(@as(usize, 0), parsed_missing_prefix_tables.value.len);

    const schema_body = try test_contract_helpers.encodeSchemaUpdateRequest(std.testing.allocator);
    defer std.testing.allocator.free(schema_body);
    var updated_schema = try client.updateTableSchema(base_uri, "docs", schema_body);
    defer updated_schema.deinit(std.testing.allocator);
    var parsed_updated_schema = try std.json.parseFromSlice(metadata_openapi.TableStatus, std.testing.allocator, updated_schema.body, .{});
    defer parsed_updated_schema.deinit();
    try std.testing.expect(parsed_updated_schema.value.schema != null);
    try std.testing.expect(parsed_updated_schema.value.schema.?.document_schemas != null);
    try std.testing.expect(parsed_updated_schema.value.migration != null);
    try std.testing.expectEqualStrings("rebuilding", parsed_updated_schema.value.migration.?.state);
    try std.testing.expectEqual(@as(?i64, 0), parsed_updated_schema.value.migration.?.read_schema.version);

    var table_detail_after_schema = try client.fetchTable(base_uri, "docs");
    defer table_detail_after_schema.deinit(std.testing.allocator);
    var parsed_table_detail_after_schema = try std.json.parseFromSlice(metadata_openapi.TableStatus, std.testing.allocator, table_detail_after_schema.body, .{});
    defer parsed_table_detail_after_schema.deinit();
    try std.testing.expect(parsed_table_detail_after_schema.value.migration != null);
    try std.testing.expectEqualStrings("rebuilding", parsed_table_detail_after_schema.value.migration.?.state);
    try std.testing.expect(parsed_table_detail_after_schema.value.indexes.map.get("full_text_index_v0") != null);
    try std.testing.expect(parsed_table_detail_after_schema.value.indexes.map.get("full_text_index_v1") != null);

    var index_detail = try client.fetchTableIndex(base_uri, "docs", "full_text_index_v0");
    defer index_detail.deinit(std.testing.allocator);
    var parsed_index = try parseJsonBodyIgnoreUnknown(IndexStatusSummary, std.testing.allocator, index_detail.body);
    defer parsed_index.deinit();
    try std.testing.expectEqualStrings("full_text_index_v0", parsed_index.value.config.name);

    var listed_indexes_after_schema = try client.fetchTableIndexes(base_uri, "docs");
    defer listed_indexes_after_schema.deinit(std.testing.allocator);
    var parsed_listed_indexes_after_schema = try parseJsonBodyIgnoreUnknown([]IndexStatusSummary, std.testing.allocator, listed_indexes_after_schema.body);
    defer parsed_listed_indexes_after_schema.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_listed_indexes_after_schema.value.len);
    try std.testing.expectEqualStrings("full_text_index_v0", parsed_listed_indexes_after_schema.value[0].config.name);
    try std.testing.expectEqualStrings("full_text_index_v1", parsed_listed_indexes_after_schema.value[1].config.name);

    const create_index_body = try test_contract_helpers.encodeCreateIndexRequest(std.testing.allocator, "embed_idx");
    defer std.testing.allocator.free(create_index_body);
    var created_index = try client.createTableIndex(base_uri, "docs", "embed_idx", create_index_body);
    defer created_index.deinit(std.testing.allocator);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    var embed_index_detail = try client.fetchTableIndex(base_uri, "docs", "embed_idx");
    defer embed_index_detail.deinit(std.testing.allocator);
    var parsed_embed_index = try parseJsonBodyIgnoreUnknown(IndexStatusSummary, std.testing.allocator, embed_index_detail.body);
    defer parsed_embed_index.deinit();
    try std.testing.expectEqualStrings("embed_idx", parsed_embed_index.value.config.name);

    var listed_indexes = try client.fetchTableIndexes(base_uri, "docs");
    defer listed_indexes.deinit(std.testing.allocator);
    var parsed_index_list = try parseJsonBodyIgnoreUnknown([]IndexStatusSummary, std.testing.allocator, listed_indexes.body);
    defer parsed_index_list.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_index_list.value.len);
    try std.testing.expectEqualStrings("full_text_index_v1", parsed_index_list.value[0].config.name);
    try std.testing.expectEqualStrings("embed_idx", parsed_index_list.value[1].config.name);

    var stable_table_detail = try client.fetchTable(base_uri, "docs");
    defer stable_table_detail.deinit(std.testing.allocator);
    var parsed_stable_table_detail = try std.json.parseFromSlice(metadata_openapi.TableStatus, std.testing.allocator, stable_table_detail.body, .{});
    defer parsed_stable_table_detail.deinit();
    try std.testing.expect(parsed_stable_table_detail.value.migration == null);
    try std.testing.expect(parsed_stable_table_detail.value.indexes.map.get("full_text_index_v0") == null);
    try std.testing.expect(parsed_stable_table_detail.value.indexes.map.get("full_text_index_v1") != null);

    const provisioned_indexes = try db.listIndexes(std.testing.allocator);
    defer db_mod.types.freeIndexConfigs(std.testing.allocator, provisioned_indexes);
    var found_embed = false;
    for (provisioned_indexes) |cfg| {
        if (!std.mem.eql(u8, cfg.name, "embed_idx")) continue;
        found_embed = true;
        try std.testing.expectEqual(db_mod.types.IndexKind.dense_vector, cfg.kind);
    }
    try std.testing.expect(found_embed);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"hello full text world","status":"published","score":10,"created_at":"2026-03-01T00:00:00Z"},"doc:b":{"title":"beta","body":"secondary document","status":"draft","score":3,"created_at":"2026-03-10T00:00:00Z"},"doc:c":{"title":"gamma","body":"hello filtered world","status":"published","score":8,"created_at":"2026-03-20T00:00:00Z"}}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);
    var parsed_batch = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.testing.allocator, batch.body, .{});
    defer parsed_batch.deinit();
    try std.testing.expectEqual(@as(i64, 3), parsed_batch.value.inserted.?);

    const customers_batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"cust:a":{"name":"Alice","tier":"gold","address_id":"addr:a"},"cust:b":{"name":"Bob","tier":"silver","address_id":"addr:b"},"cust:z":{"name":"Zoe","tier":"gold","address_id":"addr:z"}}}
    );
    defer std.testing.allocator.free(customers_batch_body);
    var customers_batch = try client.fetchBatch(base_uri, "customers", customers_batch_body);
    defer customers_batch.deinit(std.testing.allocator);
    var parsed_customers_batch = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.testing.allocator, customers_batch.body, .{});
    defer parsed_customers_batch.deinit();
    try std.testing.expectEqual(@as(i64, 3), parsed_customers_batch.value.inserted.?);

    const addresses_batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"addr:a":{"city":"Austin","state":"TX"},"addr:b":{"city":"Boston","state":"MA"},"addr:z":{"city":"Zurich","state":"ZH"}}}
    );
    defer std.testing.allocator.free(addresses_batch_body);
    var addresses_batch = try client.fetchBatch(base_uri, "addresses", addresses_batch_body);
    defer addresses_batch.deinit(std.testing.allocator);
    var parsed_addresses_batch = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.testing.allocator, addresses_batch.body, .{});
    defer parsed_addresses_batch.deinit();
    try std.testing.expectEqual(@as(i64, 3), parsed_addresses_batch.value.inserted.?);

    const query_body = try test_contract_helpers.encodeMatchQueryRequest(std.testing.allocator, "body", "hello", &.{ "title", "body" }, 5);
    defer std.testing.allocator.free(query_body);
    var query = try client.fetchQuery(base_uri, "docs", query_body);
    defer query.deinit(std.testing.allocator);
    var query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, query.body, .{});
    defer query_responses.deinit();
    try std.testing.expectEqual(@as(usize, 1), query_responses.value.responses.?.len);
    const query_result = query_responses.value.responses.?[0];
    try std.testing.expectEqualStrings("docs", query_result.table.?);
    try std.testing.expectEqual(@as(i64, 1), query_result.hits.?.total.?);
    try std.testing.expectEqualStrings("doc:a", query_result.hits.?.hits.?[0]._id);

    const aggregation_query_body = try std.testing.allocator.dupe(u8,
        \\{"full_text_search":{"query":"body:hello OR body:secondary"},"fields":["title","body","status","score"],"limit":2,"aggregations":{"score_stats":{"type":"stats","field":"score"},"by_status":{"type":"terms","field":"status","size":5}}}
    );
    defer std.testing.allocator.free(aggregation_query_body);
    var aggregation_query = try client.fetchQuery(base_uri, "docs", aggregation_query_body);
    defer aggregation_query.deinit(std.testing.allocator);
    var aggregation_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, aggregation_query.body, .{});
    defer aggregation_responses.deinit();
    try std.testing.expectEqual(@as(usize, 1), aggregation_responses.value.responses.?.len);
    const aggregation_result = aggregation_responses.value.responses.?[0];
    const aggregations = aggregation_result.aggregations.?;
    const score_stats = aggregations.map.get("score_stats").?;
    const by_status = aggregations.map.get("by_status").?;

    try std.testing.expectEqual(@as(i64, 3), score_stats.count.?);
    try std.testing.expectEqual(@as(f32, 21), score_stats.sum.?);
    try std.testing.expectEqual(@as(f32, 7), score_stats.avg.?);
    try std.testing.expectEqual(@as(f32, 3), score_stats.min.?);
    try std.testing.expectEqual(@as(f32, 10), score_stats.max.?);

    const status_buckets = by_status.buckets.?;
    try std.testing.expectEqual(@as(usize, 2), status_buckets.len);
    var saw_published = false;
    var saw_draft = false;
    for (status_buckets) |bucket| {
        const key = bucket.key;
        const doc_count = bucket.doc_count;
        if (std.mem.eql(u8, key, "published")) {
            saw_published = true;
            try std.testing.expectEqual(@as(i64, 2), doc_count);
        } else if (std.mem.eql(u8, key, "draft")) {
            saw_draft = true;
            try std.testing.expectEqual(@as(i64, 1), doc_count);
        } else {
            return error.TestUnexpectedResult;
        }
    }
    try std.testing.expect(saw_published);
    try std.testing.expect(saw_draft);

    const joined_batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"updates":{"doc:a":{"customer_id":"cust:a"},"doc:b":{"customer_id":"cust:b"},"doc:c":{"customer_id":"cust:missing"}}}
    );
    defer std.testing.allocator.free(joined_batch_body);
    var joined_batch = try client.fetchBatch(base_uri, "docs", joined_batch_body);
    defer joined_batch.deinit(std.testing.allocator);
    var parsed_joined_batch = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.testing.allocator, joined_batch.body, .{});
    defer parsed_joined_batch.deinit();
    try std.testing.expectEqual(@as(i64, 3), parsed_joined_batch.value.transformed.?);

    const join_query_body = try std.testing.allocator.dupe(u8,
        \\{"full_text_search":{"query":"body:hello OR body:secondary"},"fields":["title"],"limit":5,"profile":true,"join":{"right_table":"customers","join_type":"inner","on":{"left_field":"customer_id","right_field":"_id","operator":"eq"},"right_fields":["name","tier"]}}
    );
    defer std.testing.allocator.free(join_query_body);
    var join_query = try client.fetchQuery(base_uri, "docs", join_query_body);
    defer join_query.deinit(std.testing.allocator);
    var join_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, join_query.body, .{});
    defer join_responses.deinit();

    try std.testing.expectEqual(@as(usize, 1), join_responses.value.responses.?.len);
    const join_response = join_responses.value.responses.?[0];
    const join_hits = join_response.hits.?.hits.?;
    try std.testing.expectEqual(@as(usize, 2), join_hits.len);
    const base_join_profile = join_response.profile.?.object.get("join").?.object;
    try std.testing.expectEqualStrings("index_lookup", base_join_profile.get("strategy_used").?.string);
    try std.testing.expect(base_join_profile.get("planner_used_stats").?.bool);
    try std.testing.expect(!base_join_profile.get("shuffle_candidate").?.bool);
    try std.testing.expect(!base_join_profile.get("forced_broadcast_fallback").?.bool);

    var saw_alice = false;
    var saw_bob = false;
    for (join_hits) |hit| {
        const source_value = hit._source.?.object;
        try std.testing.expect(source_value.get("customer_id") == null);
        const title = source_value.get("title").?.string;
        const joined_name = source_value.get("customers.name").?.string;
        const joined_tier = source_value.get("customers.tier").?.string;
        if (std.mem.eql(u8, title, "alpha")) {
            saw_alice = true;
            try std.testing.expectEqualStrings("Alice", joined_name);
            try std.testing.expectEqualStrings("gold", joined_tier);
        } else if (std.mem.eql(u8, title, "beta")) {
            saw_bob = true;
            try std.testing.expectEqualStrings("Bob", joined_name);
            try std.testing.expectEqualStrings("silver", joined_tier);
        } else {
            return error.TestUnexpectedResult;
        }
    }
    try std.testing.expect(saw_alice);
    try std.testing.expect(saw_bob);

    const shuffle_join_query_body = try std.testing.allocator.dupe(u8,
        \\{"full_text_search":{"query":"body:hello OR body:secondary"},"fields":["title"],"limit":5,"profile":true,"join":{"right_table":"customers","join_type":"inner","on":{"left_field":"customer_id","right_field":"_id","operator":"eq"},"strategy_hint":"shuffle","right_fields":["name","tier"]}}
    );
    defer std.testing.allocator.free(shuffle_join_query_body);
    var shuffle_join_query = try client.fetchQuery(base_uri, "docs", shuffle_join_query_body);
    defer shuffle_join_query.deinit(std.testing.allocator);
    var shuffle_join_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, shuffle_join_query.body, .{});
    defer shuffle_join_responses.deinit();

    try std.testing.expectEqual(@as(usize, 1), shuffle_join_responses.value.responses.?.len);
    const shuffle_join_profile = shuffle_join_responses.value.responses.?[0].profile.?.object.get("join").?.object;
    try std.testing.expectEqualStrings("shuffle", shuffle_join_profile.get("strategy_used").?.string);
    try std.testing.expectEqual(@as(i64, 2), shuffle_join_profile.get("rows_matched").?.integer);
    try std.testing.expectEqual(@as(i64, 0), shuffle_join_profile.get("rows_unmatched_left").?.integer);
    try std.testing.expect(shuffle_join_profile.get("shuffle_partitions").?.integer > 0);
    try std.testing.expect(!shuffle_join_profile.get("forced_broadcast_fallback").?.bool);

    const filtered_nested_join_query_body = try std.testing.allocator.dupe(u8,
        \\{"full_text_search":{"query":"body:hello OR body:secondary"},"fields":["title"],"limit":5,"profile":true,"join":{"right_table":"customers","join_type":"left","on":{"left_field":"customer_id","right_field":"_id","operator":"eq"},"right_filters":{"filter_query":{"query":"tier:gold"}},"strategy_hint":"broadcast","nested_join":{"right_table":"addresses","join_type":"left","on":{"left_field":"address_id","right_field":"_id","operator":"eq"},"right_fields":["city"]},"right_fields":["name","addresses.city"]}}
    );
    defer std.testing.allocator.free(filtered_nested_join_query_body);
    var filtered_nested_join_query = try client.fetchQuery(base_uri, "docs", filtered_nested_join_query_body);
    defer filtered_nested_join_query.deinit(std.testing.allocator);
    var filtered_nested_join_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, filtered_nested_join_query.body, .{});
    defer filtered_nested_join_responses.deinit();

    try std.testing.expectEqual(@as(usize, 1), filtered_nested_join_responses.value.responses.?.len);
    const filtered_nested_response = filtered_nested_join_responses.value.responses.?[0];
    const filtered_nested_hits = filtered_nested_response.hits.?.hits.?;
    try std.testing.expectEqual(@as(usize, 3), filtered_nested_hits.len);
    const join_profile = filtered_nested_response.profile.?.object.get("join").?.object;
    try std.testing.expectEqualStrings("broadcast", join_profile.get("strategy_used").?.string);
    try std.testing.expectEqual(@as(i64, 1), join_profile.get("rows_matched").?.integer);
    try std.testing.expectEqual(@as(i64, 2), join_profile.get("rows_unmatched_left").?.integer);

    var saw_left_alpha = false;
    var saw_left_beta = false;
    var saw_left_gamma = false;
    for (filtered_nested_hits) |hit| {
        const source_value = hit._source.?.object;
        try std.testing.expect(source_value.get("customer_id") == null);
        const title = source_value.get("title").?.string;
        if (std.mem.eql(u8, title, "alpha")) {
            saw_left_alpha = true;
            try std.testing.expectEqualStrings("Alice", source_value.get("customers.name").?.string);
            try std.testing.expectEqualStrings("Austin", source_value.get("customers.addresses.city").?.string);
        } else if (std.mem.eql(u8, title, "beta")) {
            saw_left_beta = true;
            try std.testing.expect(source_value.get("customers.name") == null);
            try std.testing.expect(source_value.get("customers.addresses.city") == null);
        } else if (std.mem.eql(u8, title, "gamma")) {
            saw_left_gamma = true;
            try std.testing.expect(source_value.get("customers.name") == null);
            try std.testing.expect(source_value.get("customers.addresses.city") == null);
        } else {
            return error.TestUnexpectedResult;
        }
    }
    try std.testing.expect(saw_left_alpha);
    try std.testing.expect(saw_left_beta);
    try std.testing.expect(saw_left_gamma);

    const right_join_query_body = try std.testing.allocator.dupe(u8,
        \\{"full_text_search":{"query":"body:hello OR body:secondary"},"fields":["title"],"limit":10,"profile":true,"join":{"right_table":"customers","join_type":"right","on":{"left_field":"customer_id","right_field":"_id","operator":"eq"},"right_fields":["name","tier"]}}
    );
    defer std.testing.allocator.free(right_join_query_body);
    var right_join_query = try client.fetchQuery(base_uri, "docs", right_join_query_body);
    defer right_join_query.deinit(std.testing.allocator);
    var right_join_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, right_join_query.body, .{});
    defer right_join_responses.deinit();

    try std.testing.expectEqual(@as(usize, 1), right_join_responses.value.responses.?.len);
    const right_join_response = right_join_responses.value.responses.?[0];
    const right_join_hits = right_join_response.hits.?.hits.?;
    try std.testing.expectEqual(@as(usize, 3), right_join_hits.len);
    const right_join_profile = right_join_response.profile.?.object.get("join").?.object;
    try std.testing.expectEqualStrings("broadcast", right_join_profile.get("strategy_used").?.string);
    try std.testing.expectEqual(@as(i64, 2), right_join_profile.get("rows_matched").?.integer);
    try std.testing.expectEqual(@as(i64, 1), right_join_profile.get("rows_unmatched_left").?.integer);
    try std.testing.expectEqual(@as(i64, 1), right_join_profile.get("rows_unmatched_right").?.integer);

    var saw_right_alpha = false;
    var saw_right_beta = false;
    var saw_right_zoe = false;
    for (right_join_hits) |hit| {
        const source_value = hit._source.?.object;
        const joined_name = source_value.get("customers.name").?.string;
        if (std.mem.eql(u8, joined_name, "Alice")) {
            saw_right_alpha = true;
            try std.testing.expectEqualStrings("alpha", source_value.get("title").?.string);
        } else if (std.mem.eql(u8, joined_name, "Bob")) {
            saw_right_beta = true;
            try std.testing.expectEqualStrings("beta", source_value.get("title").?.string);
        } else if (std.mem.eql(u8, joined_name, "Zoe")) {
            saw_right_zoe = true;
            try std.testing.expect(source_value.get("title").? == .null);
            try std.testing.expectEqualStrings("gold", source_value.get("customers.tier").?.string);
        } else {
            return error.TestUnexpectedResult;
        }
    }
    try std.testing.expect(saw_right_alpha);
    try std.testing.expect(saw_right_beta);
    try std.testing.expect(saw_right_zoe);

    const filtered_query_body = try test_contract_helpers.encodeFilteredQueryRequest(
        std.testing.allocator,
        "body",
        "hello",
        "status",
        "published",
        "title",
        "gamma",
        &.{ "title", "body", "status" },
        10,
    );
    defer std.testing.allocator.free(filtered_query_body);
    var filtered_query = try client.fetchQuery(base_uri, "docs", filtered_query_body);
    defer filtered_query.deinit(std.testing.allocator);
    var filtered_query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, filtered_query.body, .{});
    defer filtered_query_responses.deinit();
    const filtered_query_result = filtered_query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 1), filtered_query_result.hits.?.total.?);
    try std.testing.expectEqualStrings("doc:a", filtered_query_result.hits.?.hits.?[0]._id);

    const phrase_query_body = try test_contract_helpers.encodeQueryRequest(std.testing.allocator, query_openapi.MatchPhraseQuery{
        .match_phrase = "full text",
        .field = "body",
    }, &.{ "title", "body" }, 10);
    defer std.testing.allocator.free(phrase_query_body);
    var phrase_query = try client.fetchQuery(base_uri, "docs", phrase_query_body);
    defer phrase_query.deinit(std.testing.allocator);
    var phrase_query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, phrase_query.body, .{});
    defer phrase_query_responses.deinit();
    const phrase_query_result = phrase_query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 1), phrase_query_result.hits.?.total.?);
    try std.testing.expectEqualStrings("doc:a", phrase_query_result.hits.?.hits.?[0]._id);

    const fuzzy_query_body = try test_contract_helpers.encodeQueryRequest(std.testing.allocator, query_openapi.FuzzyQuery{
        .term = "helo",
        .field = "body",
        .fuzziness = .{ .integer = 1 },
    }, &.{ "title", "body" }, 10);
    defer std.testing.allocator.free(fuzzy_query_body);
    var fuzzy_query = try client.fetchQuery(base_uri, "docs", fuzzy_query_body);
    defer fuzzy_query.deinit(std.testing.allocator);
    var fuzzy_query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, fuzzy_query.body, .{});
    defer fuzzy_query_responses.deinit();
    const fuzzy_query_result = fuzzy_query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 2), fuzzy_query_result.hits.?.total.?);
    const fuzzy_hits = fuzzy_query_result.hits.?.hits.?;
    var saw_doc_a = false;
    var saw_doc_c = false;
    for (fuzzy_hits) |hit| {
        if (std.mem.eql(u8, hit._id, "doc:a")) saw_doc_a = true;
        if (std.mem.eql(u8, hit._id, "doc:c")) saw_doc_c = true;
    }
    try std.testing.expect(saw_doc_a);
    try std.testing.expect(saw_doc_c);

    const numeric_range_query_body = try test_contract_helpers.encodeQueryRequest(std.testing.allocator, query_openapi.NumericRangeQuery{
        .field = "score",
        .min = 9,
        .max = 10,
        .inclusive_max = true,
    }, &.{ "title", "score" }, 10);
    defer std.testing.allocator.free(numeric_range_query_body);
    var numeric_range_query = try client.fetchQuery(base_uri, "docs", numeric_range_query_body);
    defer numeric_range_query.deinit(std.testing.allocator);
    var numeric_range_query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, numeric_range_query.body, .{});
    defer numeric_range_query_responses.deinit();
    const numeric_range_query_result = numeric_range_query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 1), numeric_range_query_result.hits.?.total.?);
    try std.testing.expectEqualStrings("doc:a", numeric_range_query_result.hits.?.hits.?[0]._id);

    const prefix_query_body = try test_contract_helpers.encodeQueryRequest(std.testing.allocator, query_openapi.PrefixQuery{
        .prefix = "alp",
        .field = "title",
    }, &.{"title"}, 10);
    defer std.testing.allocator.free(prefix_query_body);
    var prefix_query = try client.fetchQuery(base_uri, "docs", prefix_query_body);
    defer prefix_query.deinit(std.testing.allocator);
    var prefix_query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, prefix_query.body, .{});
    defer prefix_query_responses.deinit();
    const prefix_query_result = prefix_query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 1), prefix_query_result.hits.?.total.?);
    try std.testing.expectEqualStrings("doc:a", prefix_query_result.hits.?.hits.?[0]._id);

    const wildcard_query_body = try test_contract_helpers.encodeQueryRequest(std.testing.allocator, query_openapi.WildcardQuery{
        .wildcard = "*ta",
        .field = "title",
    }, &.{"title"}, 10);
    defer std.testing.allocator.free(wildcard_query_body);
    var wildcard_query = try client.fetchQuery(base_uri, "docs", wildcard_query_body);
    defer wildcard_query.deinit(std.testing.allocator);
    var wildcard_query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, wildcard_query.body, .{});
    defer wildcard_query_responses.deinit();
    const wildcard_query_result = wildcard_query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 1), wildcard_query_result.hits.?.total.?);
    try std.testing.expectEqualStrings("doc:b", wildcard_query_result.hits.?.hits.?[0]._id);

    const regexp_query_body = try test_contract_helpers.encodeQueryRequest(std.testing.allocator, query_openapi.RegexpQuery{
        .regexp = "^g.*a$",
        .field = "title",
    }, &.{"title"}, 10);
    defer std.testing.allocator.free(regexp_query_body);
    var regexp_query = try client.fetchQuery(base_uri, "docs", regexp_query_body);
    defer regexp_query.deinit(std.testing.allocator);
    var regexp_query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, regexp_query.body, .{});
    defer regexp_query_responses.deinit();
    const regexp_query_result = regexp_query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 1), regexp_query_result.hits.?.total.?);
    try std.testing.expectEqualStrings("doc:c", regexp_query_result.hits.?.hits.?[0]._id);

    const term_range_query_body = try test_contract_helpers.encodeQueryRequest(std.testing.allocator, query_openapi.TermRangeQuery{
        .field = "title",
        .min = "alpha",
        .max = "beta",
        .inclusive_max = false,
    }, &.{"title"}, 10);
    defer std.testing.allocator.free(term_range_query_body);
    var term_range_query = try client.fetchQuery(base_uri, "docs", term_range_query_body);
    defer term_range_query.deinit(std.testing.allocator);
    var term_range_query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, term_range_query.body, .{});
    defer term_range_query_responses.deinit();
    const term_range_query_result = term_range_query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 1), term_range_query_result.hits.?.total.?);
    try std.testing.expectEqualStrings("doc:a", term_range_query_result.hits.?.hits.?[0]._id);

    const date_range_query_body = try test_contract_helpers.encodeQueryRequest(std.testing.allocator, query_openapi.DateRangeStringQuery{
        .field = "created_at",
        .start = "2026-03-15T00:00:00Z",
        .end = "2026-03-25T00:00:00Z",
        .inclusive_end = true,
    }, &.{ "title", "created_at" }, 10);
    defer std.testing.allocator.free(date_range_query_body);
    var date_range_query = try client.fetchQuery(base_uri, "docs", date_range_query_body);
    defer date_range_query.deinit(std.testing.allocator);
    var date_range_query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, date_range_query.body, .{});
    defer date_range_query_responses.deinit();
    const date_range_query_result = date_range_query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 1), date_range_query_result.hits.?.total.?);
    try std.testing.expectEqualStrings("doc:c", date_range_query_result.hits.?.hits.?[0]._id);

    const count_profile_query_body = try test_contract_helpers.encodeMatchQueryRequestWithFlags(
        std.testing.allocator,
        "body",
        "hello",
        &.{},
        10,
        true,
        true,
    );
    defer std.testing.allocator.free(count_profile_query_body);
    var count_profile_query = try client.fetchQuery(base_uri, "docs", count_profile_query_body);
    defer count_profile_query.deinit(std.testing.allocator);
    var count_profile_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, count_profile_query.body, .{});
    defer count_profile_responses.deinit();
    const count_profile_result = count_profile_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 2), count_profile_result.hits.?.total.?);
    try std.testing.expectEqual(@as(usize, 0), count_profile_result.hits.?.hits.?.len);
    try std.testing.expect(count_profile_result.profile != null);
    try std.testing.expect(count_profile_result.took >= 0);
    try std.testing.expectEqual(@as(i64, 1), count_profile_result.profile.?.object.get("shards").?.object.get("total").?.integer);
    try std.testing.expectEqual(false, count_profile_result.profile.?.object.get("merge") != null);

    const delete_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"deletes\":[\"doc:a\",\"doc:c\"]}");
    defer std.testing.allocator.free(delete_body);
    var deleted = try client.fetchBatch(base_uri, "docs", delete_body);
    defer deleted.deinit(std.testing.allocator);
    var parsed_deleted = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.testing.allocator, deleted.body, .{});
    defer parsed_deleted.deinit();
    try std.testing.expectEqual(@as(i64, 2), parsed_deleted.value.deleted.?);

    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(base_uri, "docs", "doc:a", null));

    const deleted_query_body = try test_contract_helpers.encodeMatchQueryRequest(std.testing.allocator, "body", "hello", &.{ "title", "body" }, 5);
    defer std.testing.allocator.free(deleted_query_body);
    var deleted_query = try client.fetchQuery(base_uri, "docs", deleted_query_body);
    defer deleted_query.deinit(std.testing.allocator);
    var deleted_query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, deleted_query.body, .{});
    defer deleted_query_responses.deinit();
    const deleted_query_result = deleted_query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 0), deleted_query_result.hits.?.total.?);

    var deleted_index = try client.deleteTableIndex(base_uri, "docs", "embed_idx");
    defer deleted_index.deinit(std.testing.allocator);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchTableIndex(base_uri, "docs", "embed_idx"));

    var listed_indexes_after_delete = try client.fetchTableIndexes(base_uri, "docs");
    defer listed_indexes_after_delete.deinit(std.testing.allocator);
    var parsed_index_list_after_delete = try parseJsonBodyIgnoreUnknown([]IndexStatusSummary, std.testing.allocator, listed_indexes_after_delete.body);
    defer parsed_index_list_after_delete.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_index_list_after_delete.value.len);
    try std.testing.expectEqualStrings("full_text_index_v1", parsed_index_list_after_delete.value[0].config.name);

    const provisioned_indexes_after_delete = try db.listIndexes(std.testing.allocator);
    defer db_mod.types.freeIndexConfigs(std.testing.allocator, provisioned_indexes_after_delete);
    for (provisioned_indexes_after_delete) |cfg| {
        try std.testing.expect(!std.mem.eql(u8, cfg.name, "embed_idx"));
    }

    _ = try client.dropTable(base_uri, "docs");
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchTable(base_uri, "docs"));
    var listed_tables_after_drop = try client.fetchTables(base_uri, null);
    defer listed_tables_after_drop.deinit(std.testing.allocator);
    var parsed_table_list_after_drop = try std.json.parseFromSlice([]metadata_openapi.TableStatus, std.testing.allocator, listed_tables_after_drop.body, .{});
    defer parsed_table_list_after_drop.deinit();
    try std.testing.expectEqual(@as(usize, 0), parsed_table_list_after_drop.value.len);
}

test "public api e2e rebuilds schema-migration full-text index on exact backfill boundary" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-schema-boundary-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-schema-boundary-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 2113,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 2113,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "schema migration boundary docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const num_docs: usize = 1000;
    const batch_size: usize = 200;
    var start: usize = 0;
    while (start < num_docs) : (start += batch_size) {
        const end = @min(start + batch_size, num_docs);
        var body = std.ArrayList(u8).empty;
        defer body.deinit(std.testing.allocator);
        try body.appendSlice(std.testing.allocator, "{\"inserts\":{");
        var j = start;
        while (j < end) : (j += 1) {
            if (j != start) try body.append(std.testing.allocator, ',');
            try body.print(
                std.testing.allocator,
                "\"doc-{d:0>4}\":{{\"title\":\"Document {d}\",\"content\":\"This is the content of document number {d} with some searchable text.\"}}",
                .{ j, j, j },
            );
        }
        try body.appendSlice(std.testing.allocator, "}}");
        var batch = try client.fetchBatch(base_uri, "docs", body.items);
        defer batch.deinit(std.testing.allocator);
    }

    var v0_ready = false;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        try svc.runRound();
        var index = try client.fetchTableIndex(base_uri, "docs", "full_text_index_v0");
        defer index.deinit(std.testing.allocator);
        var parsed_index = try parseJsonBody(IndexStatusSummary, std.testing.allocator, index.body);
        defer parsed_index.deinit();
        if (parsed_index.value.status.backfill_active == false and
            parsed_index.value.status.doc_count == 1000)
        {
            v0_ready = true;
            break;
        }
    }
    try std.testing.expect(v0_ready);

    const schema_body = try test_contract_helpers.encodeSchemaUpdateRequest(std.testing.allocator);
    defer std.testing.allocator.free(schema_body);
    var updated_schema = try client.updateTableSchema(base_uri, "docs", schema_body);
    defer updated_schema.deinit(std.testing.allocator);
    var parsed_updated_schema = try parseJsonBody(metadata_openapi.TableStatus, std.testing.allocator, updated_schema.body);
    defer parsed_updated_schema.deinit();
    try std.testing.expect(parsed_updated_schema.value.migration != null);
    try std.testing.expectEqualStrings("rebuilding", parsed_updated_schema.value.migration.?.state);
    try std.testing.expectEqual(@as(i64, 0), parsed_updated_schema.value.migration.?.read_schema.version);
    try std.testing.expect(parsed_updated_schema.value.indexes.map.get("full_text_index_v1") != null);

    var v1_ready = false;
    rounds = 0;
    while (rounds < 128) : (rounds += 1) {
        try svc.runRound();
        var index = try client.fetchTableIndex(base_uri, "docs", "full_text_index_v1");
        defer index.deinit(std.testing.allocator);
        var parsed_index = try parseJsonBody(IndexStatusSummary, std.testing.allocator, index.body);
        defer parsed_index.deinit();
        if (parsed_index.value.status.backfill_active == false and
            parsed_index.value.status.doc_count == 1000)
        {
            v1_ready = true;
            break;
        }
    }
    try std.testing.expect(v1_ready);

    var old_index_dropped = false;
    rounds = 0;
    while (rounds < 128) : (rounds += 1) {
        try svc.runRound();
        var listed_indexes = try client.fetchTableIndexes(base_uri, "docs");
        defer listed_indexes.deinit(std.testing.allocator);
        var table_detail = try client.fetchTable(base_uri, "docs");
        defer table_detail.deinit(std.testing.allocator);
        var parsed_listed_indexes = try parseJsonBodyIgnoreUnknown([]IndexStatusSummary, std.testing.allocator, listed_indexes.body);
        defer parsed_listed_indexes.deinit();
        var parsed_table_detail = try parseJsonBody(metadata_openapi.TableStatus, std.testing.allocator, table_detail.body);
        defer parsed_table_detail.deinit();
        var saw_v0 = false;
        var saw_v1 = false;
        for (parsed_listed_indexes.value) |index_status| {
            if (std.mem.eql(u8, index_status.config.name, "full_text_index_v0")) saw_v0 = true;
            if (std.mem.eql(u8, index_status.config.name, "full_text_index_v1")) saw_v1 = true;
        }
        if (!saw_v0 and saw_v1 and parsed_table_detail.value.migration == null) {
            old_index_dropped = true;
            break;
        }
    }
    try std.testing.expect(old_index_dropped);

    var lookup = try client.fetchLookup(base_uri, "docs", "doc-0500", null);
    defer lookup.deinit(std.testing.allocator);
    var parsed_lookup = try parseJsonBody(LookupTitle, std.testing.allocator, lookup.body);
    defer parsed_lookup.deinit();
    try std.testing.expectEqualStrings("Document 500", parsed_lookup.value.title);
}

test "public api e2e rejects table backup during active schema migration" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-backup-migration-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-backup-migration-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-backup-migration-out", .{tmp.sub_path});
    defer std.testing.allocator.free(backup_root);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3114,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3114,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "docs table");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const schema_body = try test_contract_helpers.encodeSchemaUpdateRequest(std.testing.allocator);
    defer std.testing.allocator.free(schema_body);
    var updated_schema = try client.updateTableSchema(base_uri, "docs", schema_body);
    defer updated_schema.deinit(std.testing.allocator);
    var parsed_updated_schema = try parseJsonBody(metadata_openapi.TableStatus, std.testing.allocator, updated_schema.body);
    defer parsed_updated_schema.deinit();
    try std.testing.expect(parsed_updated_schema.value.migration != null);
    try std.testing.expectEqualStrings("rebuilding", parsed_updated_schema.value.migration.?.state);

    const backup_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"migration-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(backup_body);
    const backup_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/tables/docs/backup");
    defer std.testing.allocator.free(backup_uri);

    var backup_resp = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = backup_uri,
        .content_type = "application/json",
        .body = backup_body,
    });
    defer backup_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), backup_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, backup_resp.body, "backup does not support active schema migration") != null);
}

test "public api e2e rejects table restore for migration-state backup manifests" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-restore-migration-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-restore-migration-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-restore-migration-out", .{tmp.sub_path});
    defer std.testing.allocator.free(backup_root);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3115,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3115,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "docs table");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const backup_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"restore-migration-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(backup_body);
    var backup_resp = try client.fetchBackupTable(base_uri, "docs", backup_body);
    defer backup_resp.deinit(std.testing.allocator);
    var parsed_backup = try parseJsonBody(metadata_openapi.ClusterBackupResponse, std.testing.allocator, backup_resp.body);
    defer parsed_backup.deinit();
    try std.testing.expectEqualStrings("successful", parsed_backup.value.status);

    var manifest = try backups_api.readManifest(std.testing.allocator, backup_root, "restore-migration-snap");
    defer manifest.deinit(std.testing.allocator);
    std.testing.allocator.free(@constCast(manifest.read_schema_json));
    manifest.read_schema_json = try std.testing.allocator.dupe(u8, "{\"version\":0}");
    try backups_api.writeManifest(std.testing.allocator, backup_root, &manifest);

    _ = try client.dropTable(base_uri, "docs");
    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const restore_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"restore-migration-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(restore_body);
    const restore_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/tables/docs/restore");
    defer std.testing.allocator.free(restore_uri);

    var restore_resp = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = restore_uri,
        .content_type = "application/json",
        .body = restore_body,
    });
    defer restore_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), restore_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, restore_resp.body, "restore does not support active schema migration") != null);
}

test "public api e2e rejects table restore when target already exists" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-restore-exists-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-restore-exists-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-restore-exists-out", .{tmp.sub_path});
    defer std.testing.allocator.free(backup_root);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3116,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3116,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "docs table");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const backup_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"restore-exists-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(backup_body);
    var backup_resp = try client.fetchBackupTable(base_uri, "docs", backup_body);
    defer backup_resp.deinit(std.testing.allocator);
    var parsed_backup = try parseJsonBody(metadata_openapi.ClusterBackupResponse, std.testing.allocator, backup_resp.body);
    defer parsed_backup.deinit();
    try std.testing.expectEqualStrings("successful", parsed_backup.value.status);

    const restore_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"restore-exists-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(restore_body);
    const restore_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/tables/docs/restore");
    defer std.testing.allocator.free(restore_uri);

    var restore_resp = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = restore_uri,
        .content_type = "application/json",
        .body = restore_body,
    });
    defer restore_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), restore_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, restore_resp.body, "restore target already exists") != null);
}

test "public api e2e rejects table restore for mismatched backup manifests" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-restore-mismatch-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-restore-mismatch-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-restore-mismatch-out", .{tmp.sub_path});
    defer std.testing.allocator.free(backup_root);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3117,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3117,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "docs table");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const backup_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"restore-mismatch-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(backup_body);
    var backup_resp = try client.fetchBackupTable(base_uri, "docs", backup_body);
    defer backup_resp.deinit(std.testing.allocator);
    var parsed_backup = try parseJsonBody(metadata_openapi.ClusterBackupResponse, std.testing.allocator, backup_resp.body);
    defer parsed_backup.deinit();
    try std.testing.expectEqualStrings("successful", parsed_backup.value.status);

    var manifest = try backups_api.readManifest(std.testing.allocator, backup_root, "restore-mismatch-snap");
    defer manifest.deinit(std.testing.allocator);
    std.testing.allocator.free(@constCast(manifest.table_name));
    manifest.table_name = try std.testing.allocator.dupe(u8, "other");
    try backups_api.writeManifest(std.testing.allocator, backup_root, &manifest);

    _ = try client.dropTable(base_uri, "docs");
    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const restore_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"restore-mismatch-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(restore_body);
    const restore_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/tables/docs/restore");
    defer std.testing.allocator.free(restore_uri);

    var restore_resp = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = restore_uri,
        .content_type = "application/json",
        .body = restore_body,
    });
    defer restore_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), restore_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, restore_resp.body, "invalid restore request") != null);
}

test "public api e2e validates backup and restore request shapes and locations" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-backup-validate-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-backup-validate-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3118,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3118,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "docs table");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const table_backup_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/tables/docs/backup");
    defer std.testing.allocator.free(table_backup_uri);
    var table_backup_invalid = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = table_backup_uri,
        .content_type = "application/json",
        .body = "{}",
    });
    defer table_backup_invalid.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), table_backup_invalid.status);
    try std.testing.expect(std.mem.indexOf(u8, table_backup_invalid.body, "invalid backup request") != null);

    var table_backup_unsupported = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = table_backup_uri,
        .content_type = "application/json",
        .body = "{\"backup_id\":\"snap\",\"location\":\"ftp://bucket/path\"}",
    });
    defer table_backup_unsupported.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), table_backup_unsupported.status);
    try std.testing.expect(std.mem.indexOf(u8, table_backup_unsupported.body, "unsupported backup location") != null);

    const table_restore_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/tables/docs/restore");
    defer std.testing.allocator.free(table_restore_uri);
    var table_restore_invalid = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = table_restore_uri,
        .content_type = "application/json",
        .body = "{}",
    });
    defer table_restore_invalid.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), table_restore_invalid.status);
    try std.testing.expect(std.mem.indexOf(u8, table_restore_invalid.body, "invalid restore request") != null);

    var table_restore_unsupported = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = table_restore_uri,
        .content_type = "application/json",
        .body = "{\"backup_id\":\"snap\",\"location\":\"ftp://bucket/path\"}",
    });
    defer table_restore_unsupported.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), table_restore_unsupported.status);
    try std.testing.expect(std.mem.indexOf(u8, table_restore_unsupported.body, "unsupported backup location") != null);

    const cluster_backup_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/backup");
    defer std.testing.allocator.free(cluster_backup_uri);
    var cluster_backup_invalid = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = cluster_backup_uri,
        .content_type = "application/json",
        .body = "{}",
    });
    defer cluster_backup_invalid.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), cluster_backup_invalid.status);
    try std.testing.expect(std.mem.indexOf(u8, cluster_backup_invalid.body, "invalid backup request") != null);

    var cluster_backup_unsupported = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = cluster_backup_uri,
        .content_type = "application/json",
        .body = "{\"backup_id\":\"snap\",\"location\":\"ftp://bucket/path\"}",
    });
    defer cluster_backup_unsupported.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), cluster_backup_unsupported.status);
    try std.testing.expect(std.mem.indexOf(u8, cluster_backup_unsupported.body, "unsupported backup location") != null);

    const backups_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/backups");
    defer std.testing.allocator.free(backups_uri);
    var backups_missing = try executor.executor().execute(std.testing.allocator, .{
        .method = .GET,
        .uri = backups_uri,
    });
    defer backups_missing.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), backups_missing.status);
    try std.testing.expect(std.mem.indexOf(u8, backups_missing.body, "missing location") != null);

    const backups_unsupported_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/backups?location=ftp://bucket/path");
    defer std.testing.allocator.free(backups_unsupported_uri);
    var backups_unsupported = try executor.executor().execute(std.testing.allocator, .{
        .method = .GET,
        .uri = backups_unsupported_uri,
    });
    defer backups_unsupported.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), backups_unsupported.status);
    try std.testing.expect(std.mem.indexOf(u8, backups_unsupported.body, "unsupported backup location") != null);

    const cluster_restore_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/restore");
    defer std.testing.allocator.free(cluster_restore_uri);
    var cluster_restore_invalid = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = cluster_restore_uri,
        .content_type = "application/json",
        .body = "{}",
    });
    defer cluster_restore_invalid.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), cluster_restore_invalid.status);
    try std.testing.expect(std.mem.indexOf(u8, cluster_restore_invalid.body, "invalid restore request") != null);

    var cluster_restore_unsupported = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = cluster_restore_uri,
        .content_type = "application/json",
        .body = "{\"backup_id\":\"snap\",\"location\":\"ftp://bucket/path\"}",
    });
    defer cluster_restore_unsupported.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), cluster_restore_unsupported.status);
    try std.testing.expect(std.mem.indexOf(u8, cluster_restore_unsupported.body, "unsupported backup location") != null);

    var cluster_restore_bad_mode = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = cluster_restore_uri,
        .content_type = "application/json",
        .body = "{\"backup_id\":\"snap\",\"location\":\"file:///tmp/backup\",\"restore_mode\":\"bogus\"}",
    });
    defer cluster_restore_bad_mode.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), cluster_restore_bad_mode.status);
    try std.testing.expect(std.mem.indexOf(u8, cluster_restore_bad_mode.body, "invalid restore request") != null);
}

test "public api e2e backs up drops and restores a table" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-backup-restore-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-backup-restore-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-backup-restore-out", .{tmp.sub_path});
    defer std.testing.allocator.free(backup_root);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = group_ids.main_metadata_group_id,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = group_ids.main_metadata_group_id,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "docs table");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const batch_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/tables/docs/batch");
    defer std.testing.allocator.free(batch_uri);
    var batch_resp = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = batch_uri,
        .content_type = "application/json",
        .body = "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\",\"body\":\"restored\"}},\"sync_level\":\"full_text\"}",
    });
    defer batch_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), batch_resp.status);

    const backup_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"roundtrip-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(backup_body);
    var backup_resp = try client.fetchBackupTable(base_uri, "docs", backup_body);
    defer backup_resp.deinit(std.testing.allocator);
    var parsed_backup = try parseJsonBody(metadata_openapi.ClusterBackupResponse, std.testing.allocator, backup_resp.body);
    defer parsed_backup.deinit();
    try std.testing.expectEqualStrings("successful", parsed_backup.value.status);

    _ = try client.dropTable(base_uri, "docs");

    const restore_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"roundtrip-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(restore_body);
    const restore_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/tables/docs/restore");
    defer std.testing.allocator.free(restore_uri);
    var restore_resp = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = restore_uri,
        .content_type = "application/json",
        .body = restore_body,
    });
    defer restore_resp.deinit(std.testing.allocator);
    if (restore_resp.status != 202) {
        std.debug.print("restore status={d} body={s}\n", .{ restore_resp.status, restore_resp.body });
    }
    try std.testing.expectEqual(@as(u16, 202), restore_resp.status);
    var parsed_restore = try parseJsonBody(metadata_openapi.ClusterRestoreResponse, std.testing.allocator, restore_resp.body);
    defer parsed_restore.deinit();
    try std.testing.expectEqualStrings("triggered", parsed_restore.value.status);

    var lookup = try client.fetchLookup(base_uri, "docs", "doc:a", null);
    defer lookup.deinit(std.testing.allocator);
    var parsed_lookup = try parseJsonBody(LookupTitle, std.testing.allocator, lookup.body);
    defer parsed_lookup.deinit();
    try std.testing.expectEqualStrings("alpha", parsed_lookup.value.title);
}

test "public api split e2e backs up drops and restores a table" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-split-backup-restore-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-split-backup-restore-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-split-backup-restore-out", .{tmp.sub_path});
    defer std.testing.allocator.free(backup_root);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = group_ids.main_metadata_group_id,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = group_ids.main_metadata_group_id,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var metadata_admin_server: metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_listener: std_http_listener.StdHttpListener = undefined;
    const metadata_api = try startMetadataAdminListener(
        std.testing.allocator,
        &svc,
        &metadata_admin_server,
        &metadata_admin_listener,
    );
    defer std.testing.allocator.free(metadata_api);
    defer metadata_admin_listener.deinit();
    defer metadata_admin_server.deinit();

    var data_server = try data_runtime.DataServer.initFromMetadataApiUrl(std.testing.allocator, .{
        .replica_root_dir = replica_root,
    }, metadata_api);
    defer data_server.deinit();
    try data_server.start();

    const base_uri = try data_server.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "docs table");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const batch_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/tables/docs/batch");
    defer std.testing.allocator.free(batch_uri);
    var batch_resp = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = batch_uri,
        .content_type = "application/json",
        .body = "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\",\"body\":\"restored\"}},\"sync_level\":\"full_text\"}",
    });
    defer batch_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), batch_resp.status);

    const backup_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"split-roundtrip-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(backup_body);
    var backup_resp = try client.fetchBackupTable(base_uri, "docs", backup_body);
    defer backup_resp.deinit(std.testing.allocator);
    var parsed_backup = try parseJsonBody(metadata_openapi.ClusterBackupResponse, std.testing.allocator, backup_resp.body);
    defer parsed_backup.deinit();
    try std.testing.expectEqualStrings("successful", parsed_backup.value.status);

    _ = try client.dropTable(base_uri, "docs");

    const restore_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"split-roundtrip-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(restore_body);
    const restore_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/tables/docs/restore");
    defer std.testing.allocator.free(restore_uri);
    var restore_resp = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = restore_uri,
        .content_type = "application/json",
        .body = restore_body,
    });
    defer restore_resp.deinit(std.testing.allocator);
    if (restore_resp.status != 202) {
        std.debug.print("split restore status={d} body={s}\n", .{ restore_resp.status, restore_resp.body });
    }
    try std.testing.expectEqual(@as(u16, 202), restore_resp.status);
    var parsed_restore = try parseJsonBody(metadata_openapi.ClusterRestoreResponse, std.testing.allocator, restore_resp.body);
    defer parsed_restore.deinit();
    try std.testing.expectEqualStrings("triggered", parsed_restore.value.status);

    var lookup = try client.fetchLookup(base_uri, "docs", "doc:a", null);
    defer lookup.deinit(std.testing.allocator);
    var parsed_lookup = try parseJsonBody(LookupTitle, std.testing.allocator, lookup.body);
    defer parsed_lookup.deinit();
    try std.testing.expectEqualStrings("alpha", parsed_lookup.value.title);
}

test "public api swarm-like e2e backs up drops and restores a table" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-swarm-like-backup-restore-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-swarm-like-backup-restore-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-swarm-like-backup-restore-out", .{tmp.sub_path});
    defer std.testing.allocator.free(backup_root);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 2116,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{
        .observe_local_replica_root = true,
    });
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 2116,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var metadata_admin_server: metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_listener: std_http_listener.StdHttpListener = undefined;
    const metadata_api = try startMetadataAdminListener(
        std.testing.allocator,
        &svc,
        &metadata_admin_server,
        &metadata_admin_listener,
    );
    defer std.testing.allocator.free(metadata_api);
    defer metadata_admin_listener.deinit();
    defer metadata_admin_server.deinit();

    var data_server = try data_runtime.DataServer.initFromMetadataApiUrl(std.testing.allocator, .{
        .replica_root_dir = replica_root,
        .store_registration = .{
            .node_id = 1,
            .store_id = 1,
            .role = "data",
        },
    }, metadata_api);
    defer data_server.deinit();
    try data_server.start();
    try data_server.registerNodeIfConfigured();
    svc.setLocalGroupStatusProvider(data_server.localGroupStatusProvider());
    defer svc.setLocalGroupStatusProvider(null);

    const base_uri = try data_server.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "docs table");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 12) : (rounds += 1) {
        try data_server.runRound();
        try svc.runRound();
    }

    const batch_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/tables/docs/batch");
    defer std.testing.allocator.free(batch_uri);
    var batch_resp = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = batch_uri,
        .content_type = "application/json",
        .body = "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\",\"body\":\"restored\"}},\"sync_level\":\"full_text\"}",
    });
    defer batch_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), batch_resp.status);

    const backup_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"swarm-like-roundtrip-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(backup_body);
    var backup_resp = try client.fetchBackupTable(base_uri, "docs", backup_body);
    defer backup_resp.deinit(std.testing.allocator);
    var parsed_backup = try parseJsonBody(metadata_openapi.ClusterBackupResponse, std.testing.allocator, backup_resp.body);
    defer parsed_backup.deinit();
    try std.testing.expectEqualStrings("successful", parsed_backup.value.status);

    _ = try client.dropTable(base_uri, "docs");
    rounds = 0;
    while (rounds < 24) : (rounds += 1) {
        try data_server.runRound();
        try svc.runRound();
    }

    const restore_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"swarm-like-roundtrip-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(restore_body);
    const restore_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/tables/docs/restore");
    defer std.testing.allocator.free(restore_uri);
    var restore_resp = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = restore_uri,
        .content_type = "application/json",
        .body = restore_body,
    });
    defer restore_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), restore_resp.status);

    rounds = 0;
    while (rounds < 40) : (rounds += 1) {
        try data_server.runRound();
        try svc.runRound();
        var lookup = client.fetchLookup(base_uri, "docs", "doc:a", null) catch |err| switch (err) {
            error.HttpNotFound => {
                continue;
            },
            else => return err,
        };
        defer lookup.deinit(std.testing.allocator);
        var parsed_lookup = try parseJsonBody(LookupTitle, std.testing.allocator, lookup.body);
        defer parsed_lookup.deinit();
        try std.testing.expectEqualStrings("alpha", parsed_lookup.value.title);
        return;
    }

    return error.TestExpectedEqual;
}

test "split data runtime registers a store with metadata" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const metadata_replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/split-metadata-root", .{tmp.sub_path});
    defer std.testing.allocator.free(metadata_replica_root);
    const data_replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/split-data-root", .{tmp.sub_path});
    defer std.testing.allocator.free(data_replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/split-metadata-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), metadata_replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), data_replica_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), metadata_replica_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), data_replica_root) catch {};

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 2113,
            .replica_root_dir = metadata_replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 2113,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var metadata_admin_server: metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_listener: std_http_listener.StdHttpListener = undefined;
    const metadata_api = try startMetadataAdminListener(
        std.testing.allocator,
        &svc,
        &metadata_admin_server,
        &metadata_admin_listener,
    );
    defer std.testing.allocator.free(metadata_api);
    defer metadata_admin_listener.deinit();
    defer metadata_admin_server.deinit();

    var data_server = try data_runtime.DataServer.initFromMetadataApiUrl(std.testing.allocator, .{
        .replica_root_dir = data_replica_root,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "rack-a",
        },
    }, metadata_api);
    defer data_server.deinit();
    try data_server.start();
    const base_uri = try data_server.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.testing.allocator, executor.executor());

    var snapshot = try metadata_client.fetchSnapshot(metadata_api);
    defer snapshot.deinit();
    try std.testing.expectEqual(@as(usize, 1), snapshot.value.stores.len);
    try std.testing.expectEqual(@as(u64, 19), snapshot.value.stores[0].store_id);
    try std.testing.expectEqual(@as(u64, 9), snapshot.value.stores[0].node_id);
    try std.testing.expectEqualStrings("data", snapshot.value.stores[0].role);
    try std.testing.expectEqualStrings("rack-a", snapshot.value.stores[0].failure_domain);
    try std.testing.expect(snapshot.value.stores[0].live);

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "split docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var control_loop = metadata_mod.MetadataControlLoop.init(std.testing.allocator);
    defer control_loop.deinit();
    _ = try svc.reconcileOnceEnsuringLease(&control_loop);

    var post_reconcile_snapshot = try metadata_client.fetchSnapshot(metadata_api);
    defer post_reconcile_snapshot.deinit();
    try std.testing.expect(post_reconcile_snapshot.value.placement_intents.len > 0);
    const group_id = post_reconcile_snapshot.value.ranges[0].group_id;

    var rounds: usize = 0;
    while (rounds < 4) : (rounds += 1) try data_server.runRound();

    const group_db_path = try metadata_mod.groupDbPathFromReplicaRoot(std.testing.allocator, data_replica_root, group_id);
    defer std.testing.allocator.free(group_db_path);
    _ = try std.Io.Dir.cwd().statFile(io_impl.io(), group_db_path, .{});

    const batch_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/tables/docs/batch");
    defer std.testing.allocator.free(batch_uri);
    var batch_resp = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = batch_uri,
        .content_type = "application/json",
        .body = "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\"}},\"sync_level\":\"full_text\"}",
    });
    defer batch_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), batch_resp.status);

    var lookup = try client.fetchLookup(base_uri, "docs", "doc:a", null);
    defer lookup.deinit(std.testing.allocator);
    var parsed_lookup = try parseJsonBody(LookupTitle, std.testing.allocator, lookup.body);
    defer parsed_lookup.deinit();
    try std.testing.expectEqualStrings("alpha", parsed_lookup.value.title);
}

test "split data runtime serves retrieval agent pipeline queries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const metadata_replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/split-retrieval-metadata-root", .{tmp.sub_path});
    defer std.testing.allocator.free(metadata_replica_root);
    const data_replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/split-retrieval-data-root", .{tmp.sub_path});
    defer std.testing.allocator.free(data_replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/split-retrieval-metadata-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), metadata_replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), data_replica_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), metadata_replica_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), data_replica_root) catch {};

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 2114,
            .replica_root_dir = metadata_replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 2114,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var metadata_admin_server: metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_listener: std_http_listener.StdHttpListener = undefined;
    const metadata_api = try startMetadataAdminListener(
        std.testing.allocator,
        &svc,
        &metadata_admin_server,
        &metadata_admin_listener,
    );
    defer std.testing.allocator.free(metadata_api);
    defer metadata_admin_listener.deinit();
    defer metadata_admin_server.deinit();

    var data_server = try data_runtime.DataServer.initFromMetadataApiUrl(std.testing.allocator, .{
        .replica_root_dir = data_replica_root,
    }, metadata_api);
    defer data_server.deinit();
    try data_server.start();

    const base_uri = try data_server.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "retrieval docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const batch_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/tables/docs/batch");
    defer std.testing.allocator.free(batch_uri);
    var batch_resp = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = batch_uri,
        .content_type = "application/json",
        .body = "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\",\"body\":\"hello retrieval\"},\"doc:b\":{\"title\":\"beta\",\"body\":\"secondary\"}},\"sync_level\":\"full_text\"}",
    });
    defer batch_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), batch_resp.status);

    const retrieval_body =
        \\{"query":"find retrieval docs","stream":false,"queries":[{"table":"docs","full_text_search":{"query":"body:hello"},"limit":5}]}
    ;
    const retrieval_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, routes.Routes.agents_retrieval);
    defer std.testing.allocator.free(retrieval_uri);
    var retrieval_resp = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = retrieval_uri,
        .content_type = "application/json",
        .body = retrieval_body,
    });
    defer retrieval_resp.deinit(std.testing.allocator);
    if (retrieval_resp.status != 200) {
        std.debug.print("split retrieval status={d} body={s}\n", .{ retrieval_resp.status, retrieval_resp.body });
    }
    try std.testing.expectEqual(@as(u16, 200), retrieval_resp.status);
    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, retrieval_resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(AgentStatus.completed, parsed.value.status);
    try std.testing.expectEqualStrings("doc:a", parsed.value.hits[0]._id);
}

test "public api e2e supports managed semantic search and sparse embeddings" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-semantic-sparse-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-semantic-sparse-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3112,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3112,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    var embed_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeEmbeddingProvider.executor());
    defer embed_listener.deinit();
    try embed_listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const embed_base_uri = try embed_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(embed_base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "semantic docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const semantic_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexRequest(
        std.testing.allocator,
        "semantic_idx",
        "body",
        3,
        .{
            .provider = .openai,
            .model = "text-embedding-3-small",
            .url = embed_base_uri,
        },
        null,
    );
    defer std.testing.allocator.free(semantic_index_body);
    var semantic_index_resp = try client.createTableIndex(base_uri, "docs", "semantic_idx", semantic_index_body);
    defer semantic_index_resp.deinit(std.testing.allocator);

    const sparse_index_body = "{\"name\":\"sparse_idx\",\"type\":\"embeddings\",\"sparse\":true,\"external\":true}";
    var sparse_index_resp = try client.createTableIndex(base_uri, "docs", "sparse_idx", sparse_index_body);
    defer sparse_index_resp.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"alpha body","_embeddings":{"sparse_idx":{"7":1.5,"42":0.5}}},
        \\  "doc:b":{"title":"beta","body":"beta body","_embeddings":{"sparse_idx":{"99":2.0}}}
        \\}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    const semantic_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.testing.allocator, "alpha concept", &.{"semantic_idx"}, 5);
    defer std.testing.allocator.free(semantic_query_body);
    var semantic_query = try client.fetchQuery(base_uri, "docs", semantic_query_body);
    defer semantic_query.deinit(std.testing.allocator);
    var parsed_semantic = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, semantic_query.body, .{});
    defer parsed_semantic.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_semantic.value.responses.?[0].hits.?.hits.?[0]._id);

    const sparse_query_body = try test_contract_helpers.encodeSparseEmbeddingsQueryRequest(std.testing.allocator, "sparse_idx", &.{ 7, 42 }, &.{ 1.5, 0.5 }, 5);
    defer std.testing.allocator.free(sparse_query_body);
    var sparse_query = try client.fetchQuery(base_uri, "docs", sparse_query_body);
    defer sparse_query.deinit(std.testing.allocator);
    var parsed_sparse = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, sparse_query.body, .{});
    defer parsed_sparse.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_sparse.value.responses.?[0].hits.?.hits.?[0]._id);

    const unknown_semantic_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.testing.allocator, "alpha concept", &.{"unknown_semantic"}, 5);
    defer std.testing.allocator.free(unknown_semantic_query_body);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchQuery(base_uri, "docs", unknown_semantic_query_body));

    const unknown_sparse_query_body = try test_contract_helpers.encodeSparseEmbeddingsQueryRequest(std.testing.allocator, "unknown_sparse", &.{ 7, 42 }, &.{ 1.5, 0.5 }, 5);
    defer std.testing.allocator.free(unknown_sparse_query_body);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchQuery(base_uri, "docs", unknown_sparse_query_body));
}

test "public api e2e adds managed embeddings indexes to existing tables" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-semantic-existing-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-semantic-existing-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3113,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3113,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    var embed_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeEmbeddingProvider.executor());
    defer embed_listener.deinit();
    try embed_listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const embed_base_uri = try embed_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(embed_base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "semantic mutable docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"alpha body"},
        \\  "doc:b":{"title":"beta","body":"beta body"}
        \\}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    const semantic_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexRequest(
        std.testing.allocator,
        "semantic_idx",
        "body",
        3,
        .{
            .provider = .openai,
            .model = "text-embedding-3-small",
            .url = embed_base_uri,
        },
        null,
    );
    defer std.testing.allocator.free(semantic_index_body);
    var semantic_index_resp = try client.createTableIndex(base_uri, "docs", "semantic_idx", semantic_index_body);
    defer semantic_index_resp.deinit(std.testing.allocator);

    rounds = 0;
    while (rounds < 12) : (rounds += 1) try svc.runRound();

    var semantic_index = try client.fetchTableIndex(base_uri, "docs", "semantic_idx");
    defer semantic_index.deinit(std.testing.allocator);
    var parsed_semantic_index = try parseJsonBody(IndexStatusSummary, std.testing.allocator, semantic_index.body);
    defer parsed_semantic_index.deinit();
    try std.testing.expectEqualStrings("semantic_idx", parsed_semantic_index.value.config.name);
    try std.testing.expectEqual(@as(?bool, false), parsed_semantic_index.value.status.backfill_active);
    try std.testing.expectEqual(@as(?u64, 2), parsed_semantic_index.value.status.doc_count);

    const semantic_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.testing.allocator, "alpha concept", &.{"semantic_idx"}, 5);
    defer std.testing.allocator.free(semantic_query_body);
    var semantic_query = try client.fetchQuery(base_uri, "docs", semantic_query_body);
    defer semantic_query.deinit(std.testing.allocator);
    var parsed_semantic_query = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, semantic_query.body, .{});
    defer parsed_semantic_query.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_semantic_query.value.responses.?[0].hits.?.hits.?[0]._id);

    var dropped = try client.deleteTableIndex(base_uri, "docs", "semantic_idx");
    defer dropped.deinit(std.testing.allocator);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchTableIndex(base_uri, "docs", "semantic_idx"));
    var indexes_after_drop = try client.fetchTableIndexes(base_uri, "docs");
    defer indexes_after_drop.deinit(std.testing.allocator);
    var parsed_indexes_after_drop = try parseJsonBodyIgnoreUnknown([]IndexStatusSummary, std.testing.allocator, indexes_after_drop.body);
    defer parsed_indexes_after_drop.deinit();
    for (parsed_indexes_after_drop.value) |index_status| {
        try std.testing.expect(!std.mem.eql(u8, index_status.config.name, "semantic_idx"));
    }
}

test "public api e2e recreates managed embeddings index after corrupt artifact" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-semantic-corrupt-recreate-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-semantic-corrupt-recreate-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3113,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3113,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    var embed_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeEmbeddingProvider.executor());
    defer embed_listener.deinit();
    try embed_listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const embed_base_uri = try embed_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(embed_base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "semantic mutable docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const semantic_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexRequest(
        std.testing.allocator,
        "semantic_idx",
        "body",
        3,
        .{
            .provider = .openai,
            .model = "text-embedding-3-small",
            .url = embed_base_uri,
        },
        null,
    );
    defer std.testing.allocator.free(semantic_index_body);
    var semantic_index_resp = try client.createTableIndex(base_uri, "docs", "semantic_idx", semantic_index_body);
    defer semantic_index_resp.deinit(std.testing.allocator);

    rounds = 0;
    while (rounds < 12) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"alpha concept overview"},
        \\  "doc:b":{"title":"beta","body":"beta architecture notes"}
        \\}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    rounds = 0;
    while (rounds < 16) : (rounds += 1) try svc.runRound();

    _ = try provisioned_write_source.source().corruptEmbeddingArtifact(std.testing.allocator, "docs", "doc:a", "semantic_idx");

    var dropped = try client.deleteTableIndex(base_uri, "docs", "semantic_idx");
    defer dropped.deinit(std.testing.allocator);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchTableIndex(base_uri, "docs", "semantic_idx"));

    var recreated = try client.createTableIndex(base_uri, "docs", "semantic_idx", semantic_index_body);
    defer recreated.deinit(std.testing.allocator);

    rounds = 0;
    while (rounds < 24) : (rounds += 1) try svc.runRound();

    var semantic_index = try client.fetchTableIndex(base_uri, "docs", "semantic_idx");
    defer semantic_index.deinit(std.testing.allocator);
    var parsed_semantic_index = try parseJsonBody(IndexStatusSummary, std.testing.allocator, semantic_index.body);
    defer parsed_semantic_index.deinit();
    try std.testing.expectEqualStrings("semantic_idx", parsed_semantic_index.value.config.name);
    try std.testing.expectEqual(@as(?bool, false), parsed_semantic_index.value.status.backfill_active);
    try std.testing.expectEqual(@as(?u64, 2), parsed_semantic_index.value.status.doc_count);
}

test "public api e2e restores managed embeddings from table backup" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-semantic-backup-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-semantic-backup-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-semantic-backup-out", .{tmp.sub_path});
    defer std.testing.allocator.free(backup_root);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3113,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3113,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    var embed_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeEmbeddingProvider.executor());
    defer embed_listener.deinit();
    try embed_listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const embed_base_uri = try embed_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(embed_base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "semantic docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const semantic_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexRequest(
        std.testing.allocator,
        "semantic_idx",
        "body",
        3,
        .{
            .provider = .openai,
            .model = "text-embedding-3-small",
            .url = embed_base_uri,
        },
        null,
    );
    defer std.testing.allocator.free(semantic_index_body);
    var semantic_index_resp = try client.createTableIndex(base_uri, "docs", "semantic_idx", semantic_index_body);
    defer semantic_index_resp.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"alpha body"},
        \\  "doc:b":{"title":"beta","body":"beta body"}
        \\}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    const semantic_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.testing.allocator, "alpha concept", &.{"semantic_idx"}, 5);
    defer std.testing.allocator.free(semantic_query_body);
    var semantic_query_before = try client.fetchQuery(base_uri, "docs", semantic_query_body);
    defer semantic_query_before.deinit(std.testing.allocator);
    var parsed_semantic_before = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, semantic_query_before.body, .{});
    defer parsed_semantic_before.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_semantic_before.value.responses.?[0].hits.?.hits.?[0]._id);

    var semantic_index_before = try client.fetchTableIndex(base_uri, "docs", "semantic_idx");
    defer semantic_index_before.deinit(std.testing.allocator);
    var parsed_semantic_index_before = try parseJsonBody(IndexStatusSummary, std.testing.allocator, semantic_index_before.body);
    defer parsed_semantic_index_before.deinit();
    try std.testing.expectEqual(@as(?bool, false), parsed_semantic_index_before.value.status.backfill_active);
    try std.testing.expectEqual(@as(?u64, 2), parsed_semantic_index_before.value.status.doc_count);

    const backup_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"semantic-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(backup_body);
    var backup_resp = try client.fetchBackupTable(base_uri, "docs", backup_body);
    defer backup_resp.deinit(std.testing.allocator);
    var parsed_backup = try parseJsonBody(metadata_openapi.ClusterBackupResponse, std.testing.allocator, backup_resp.body);
    defer parsed_backup.deinit();
    try std.testing.expectEqualStrings("successful", parsed_backup.value.status);

    _ = try client.dropTable(base_uri, "docs");

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchTable(base_uri, "docs"));

    const restore_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"semantic-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(restore_body);
    var restore_resp = try client.fetchRestoreTable(base_uri, "docs", restore_body);
    defer restore_resp.deinit(std.testing.allocator);
    var parsed_restore = try parseJsonBody(metadata_openapi.ClusterRestoreResponse, std.testing.allocator, restore_resp.body);
    defer parsed_restore.deinit();
    try std.testing.expectEqualStrings("triggered", parsed_restore.value.status);

    rounds = 0;
    while (rounds < 12) : (rounds += 1) try svc.runRound();

    var restored_table = try client.fetchTable(base_uri, "docs");
    defer restored_table.deinit(std.testing.allocator);
    var parsed_restored_table = try parseJsonBody(metadata_openapi.TableStatus, std.testing.allocator, restored_table.body);
    defer parsed_restored_table.deinit();
    try std.testing.expect(parsed_restored_table.value.indexes.map.get("semantic_idx") != null);

    var restored_indexes = try client.fetchTableIndexes(base_uri, "docs");
    defer restored_indexes.deinit(std.testing.allocator);
    var parsed_restored_indexes = try parseJsonBodyIgnoreUnknown([]IndexStatusSummary, std.testing.allocator, restored_indexes.body);
    defer parsed_restored_indexes.deinit();
    try std.testing.expectEqualStrings("semantic_idx", parsed_restored_indexes.value[0].config.name);

    var semantic_index_after = try client.fetchTableIndex(base_uri, "docs", "semantic_idx");
    defer semantic_index_after.deinit(std.testing.allocator);
    var parsed_semantic_index_after = try parseJsonBody(IndexStatusSummary, std.testing.allocator, semantic_index_after.body);
    defer parsed_semantic_index_after.deinit();
    try std.testing.expectEqual(@as(?bool, false), parsed_semantic_index_after.value.status.backfill_active);
    try std.testing.expectEqual(@as(?u64, 2), parsed_semantic_index_after.value.status.doc_count);

    var restored_lookup = try client.fetchLookup(base_uri, "docs", "doc:a", null);
    defer restored_lookup.deinit(std.testing.allocator);
    var parsed_restored_lookup = try parseJsonBody(LookupTitle, std.testing.allocator, restored_lookup.body);
    defer parsed_restored_lookup.deinit();
    try std.testing.expectEqualStrings("alpha", parsed_restored_lookup.value.title);

    var semantic_query_after = try client.fetchQuery(base_uri, "docs", semantic_query_body);
    defer semantic_query_after.deinit(std.testing.allocator);
    var parsed_semantic_after = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, semantic_query_after.body, .{});
    defer parsed_semantic_after.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_semantic_after.value.responses.?[0].hits.?.hits.?[0]._id);
}

test "public api e2e supports managed sparse embeddings generation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-managed-sparse-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-managed-sparse-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 2721,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 2721,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    var antfly_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeAntflyProvider.executor());
    defer antfly_listener.deinit();
    try antfly_listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const antfly_base_uri = try antfly_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(antfly_base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "managed sparse docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const sparse_index_body = try test_contract_helpers.encodeManagedSparseEmbeddingsIndexRequest(
        std.testing.allocator,
        "sparse_idx",
        "body",
        .{
            .provider = .antfly,
            .model = "antfly-sparse-v1",
            .api_url = antfly_base_uri,
        },
    );
    defer std.testing.allocator.free(sparse_index_body);
    var sparse_index_resp = try client.createTableIndex(base_uri, "docs", "sparse_idx", sparse_index_body);
    defer sparse_index_resp.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"alpha body"},
        \\  "doc:b":{"title":"beta","body":"beta body"}
        \\}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const sparse_query_body = try test_contract_helpers.encodeSparseEmbeddingsQueryRequest(std.testing.allocator, "sparse_idx", &.{ 7, 42 }, &.{ 1.5, 0.5 }, 5);
    defer std.testing.allocator.free(sparse_query_body);
    var sparse_query = try client.fetchQuery(base_uri, "docs", sparse_query_body);
    defer sparse_query.deinit(std.testing.allocator);
    var parsed_sparse = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, sparse_query.body, .{});
    defer parsed_sparse.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_sparse.value.responses.?[0].hits.?.hits.?[0]._id);
}

test "public api e2e supports hybrid query pruner and reranker" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-hybrid-rerank-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-hybrid-rerank-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 31225,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 31225,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    var antfly_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeAntflyProvider.executor());
    defer antfly_listener.deinit();
    try antfly_listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const antfly_base_uri = try antfly_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(antfly_base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "hybrid query docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const dense_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexRequest(
        std.testing.allocator,
        "semantic_idx",
        "body",
        3,
        .{
            .provider = .antfly,
            .model = "antfly-embed-v1",
            .api_url = antfly_base_uri,
        },
        null,
    );
    defer std.testing.allocator.free(dense_index_body);
    var dense_index_resp = try client.createTableIndex(base_uri, "docs", "semantic_idx", dense_index_body);
    defer dense_index_resp.deinit(std.testing.allocator);

    const sparse_index_body = try test_contract_helpers.encodeManagedSparseEmbeddingsIndexRequest(
        std.testing.allocator,
        "sparse_idx",
        "body",
        .{
            .provider = .antfly,
            .model = "antfly-sparse-v1",
            .api_url = antfly_base_uri,
        },
    );
    defer std.testing.allocator.free(sparse_index_body);
    var sparse_index_resp = try client.createTableIndex(base_uri, "docs", "sparse_idx", sparse_index_body);
    defer sparse_index_resp.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"alpha body"},
        \\  "doc:b":{"title":"beta","body":"beta body"},
        \\  "doc:c":{"title":"plain","body":"body body"}
        \\}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    const hybrid_query_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"full_text_search\":{{\"match\":{{\"field\":\"body\",\"text\":\"body\"}}}},\"semantic_search\":\"alpha concept\",\"embeddings\":{{\"sparse_idx\":{{\"indices\":[7,42],\"values\":[1.5,0.5]}}}},\"indexes\":[\"semantic_idx\",\"sparse_idx\"],\"merge_config\":{{\"strategy\":\"rsf\",\"window_size\":10}},\"pruner\":{{\"require_multi_index\":true}},\"reranker\":{{\"provider\":\"antfly\",\"model\":\"cross-encoder/ms-marco-MiniLM-L-6-v2\",\"url\":{f},\"field\":\"body\",\"top_n\":2}},\"limit\":3}}",
        .{std.json.fmt(antfly_base_uri, .{})},
    );
    defer std.testing.allocator.free(hybrid_query_body);

    var hybrid_query = try client.fetchQuery(base_uri, "docs", hybrid_query_body);
    defer hybrid_query.deinit(std.testing.allocator);
    var parsed_hybrid = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, hybrid_query.body, .{});
    defer parsed_hybrid.deinit();

    const hybrid_response = parsed_hybrid.value.responses.?[0];
    const hits = hybrid_response.hits.?.hits.?;
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expectEqualStrings("doc:b", hits[0]._id);
    try std.testing.expectEqualStrings("doc:a", hits[1]._id);
    const hybrid_profile = hybrid_response.profile.?.object;
    const hybrid_reranker = hybrid_profile.get("reranker").?.object;
    try std.testing.expectEqualStrings("cross-encoder/ms-marco-MiniLM-L-6-v2", hybrid_reranker.get("model").?.string);
    for (hits) |hit| try std.testing.expect(!std.mem.eql(u8, hit._id, "doc:c"));
}

test "public api e2e supports retrieval agent pipeline queries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-agent-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-agent-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 31226,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 31226,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "retrieval agent docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"hello retrieval"},"doc:b":{"title":"beta","body":"secondary document"}}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const retrieval_body =
        \\{"query":"find retrieval docs","stream":false,"queries":[{"table":"docs","full_text_search":{"query":"body:retrieval"},"limit":5}]}
    ;
    var retrieval = try client.fetchRetrievalAgent(base_uri, retrieval_body);
    defer retrieval.deinit(std.testing.allocator);
    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, retrieval.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(AgentStatus.completed, parsed.value.status);
    try std.testing.expectEqual(RetrievalStrategy.bm25, parsed.value.strategy_used.?);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.hits.len);
    try std.testing.expectEqualStrings("doc:a", parsed.value.hits[0]._id);
}

test "public api e2e supports retrieval agent generation step" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-generation-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-generation-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 31227,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 31227,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    var antfly_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeAntflyProvider.executor());
    defer antfly_listener.deinit();
    try antfly_listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const antfly_base_uri = try antfly_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(antfly_base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "retrieval generation docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"hello retrieval"},"doc:b":{"title":"beta","body":"secondary document"}}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const retrieval_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"query\":\"find retrieval docs\",\"stream\":false,\"generator\":{{\"provider\":\"antfly\",\"model\":\"local-generator\",\"api_url\":{f}}},\"steps\":{{\"generation\":{{\"enabled\":true}}}},\"queries\":[{{\"table\":\"docs\",\"full_text_search\":{{\"query\":\"body:retrieval\"}},\"limit\":5}}]}}",
        .{std.json.fmt(antfly_base_uri, .{})},
    );
    defer std.testing.allocator.free(retrieval_body);
    var retrieval = try client.fetchRetrievalAgent(base_uri, retrieval_body);
    defer retrieval.deinit(std.testing.allocator);
    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, retrieval.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(AgentStatus.completed, parsed.value.status);
    try std.testing.expectEqualStrings("Generated answer citing doc:a", parsed.value.generation.?);
    try std.testing.expectEqualStrings("local-generator", parsed.value.model.?);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.steps.?.len);
}

test "public api e2e supports retrieval agent semantic and hybrid strategies" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-strategy-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-strategy-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 31230,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 31230,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "retrieval strategy docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const dense_index_body = "{\"name\":\"dense_idx\",\"type\":\"embeddings\",\"external\":true,\"dimension\":3}";
    var dense_index = try client.createTableIndex(base_uri, "docs", "dense_idx", dense_index_body);
    defer dense_index.deinit(std.testing.allocator);
    const sparse_index_body = "{\"name\":\"sparse_idx\",\"type\":\"embeddings\",\"external\":true,\"sparse\":true}";
    var sparse_index = try client.createTableIndex(base_uri, "docs", "sparse_idx", sparse_index_body);
    defer sparse_index.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"hello retrieval agent","status":"active","_embeddings":{"dense_idx":[1.0,0.0,0.0],"sparse_idx":{"7":1.5,"42":0.5}}},
        \\  "doc:b":{"title":"beta","body":"secondary document","status":"draft","_embeddings":{"dense_idx":[0.0,1.0,0.0],"sparse_idx":{"99":2.0}}},
        \\  "doc:c":{"title":"gamma","body":"retrieval systems combine lexical and dense search","status":"active","_embeddings":{"dense_idx":[0.9,0.1,0.0],"sparse_idx":{"7":1.2,"42":0.4,"50":1.0}}}
        \\}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const semantic_body =
        \\{"query":"find semantically related docs","stream":false,"queries":[{"table":"docs","embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":5}]}
    ;
    var semantic_retrieval = try client.fetchRetrievalAgent(base_uri, semantic_body);
    defer semantic_retrieval.deinit(std.testing.allocator);
    var parsed_semantic = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, semantic_retrieval.body, .{});
    defer parsed_semantic.deinit();
    try std.testing.expectEqual(RetrievalStrategy.semantic, parsed_semantic.value.strategy_used.?);
    try std.testing.expectEqualStrings("doc:a", parsed_semantic.value.hits[0]._id);

    const hybrid_body =
        \\{"query":"find hybrid retrieval docs","stream":false,"queries":[{"table":"docs","full_text_search":{"query":"body:retrieval"},"embeddings":{"dense_idx":[1.0,0.0,0.0],"sparse_idx":{"indices":[7,42],"values":[1.5,0.5]}},"indexes":["dense_idx","sparse_idx"],"limit":5}]}
    ;
    var hybrid_retrieval = try client.fetchRetrievalAgent(base_uri, hybrid_body);
    defer hybrid_retrieval.deinit(std.testing.allocator);
    var parsed_hybrid = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, hybrid_retrieval.body, .{});
    defer parsed_hybrid.deinit();
    try std.testing.expectEqual(RetrievalStrategy.hybrid, parsed_hybrid.value.strategy_used.?);
    try std.testing.expectEqualStrings("doc:a", parsed_hybrid.value.hits[0]._id);

    const metadata_body =
        \\{"query":"find active docs","stream":false,"queries":[{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    var metadata_retrieval = try client.fetchRetrievalAgent(base_uri, metadata_body);
    defer metadata_retrieval.deinit(std.testing.allocator);
    var parsed_metadata = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, metadata_retrieval.body, .{});
    defer parsed_metadata.deinit();
    try std.testing.expectEqual(RetrievalStrategy.metadata, parsed_metadata.value.strategy_used.?);
    try std.testing.expectEqual(@as(usize, 2), parsed_metadata.value.hits.len);
}

test "public api e2e supports retrieval agent tree search pipeline" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-tree-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-tree-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 31231,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 31231,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "retrieval tree docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const graph_index_body =
        \\{"name":"doc_hierarchy","type":"graph","edge_types":[{"name":"contains","topology":"tree"}]}
    ;
    var graph_index = try client.createTableIndex(base_uri, "docs", "doc_hierarchy", graph_index_body);
    defer graph_index.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc:root":{"title":"root","body":"architecture overview","_edges":{"doc_hierarchy":{"contains":[{"target":"doc:child","weight":1.0}]}}},
        \\  "doc:child":{"title":"child","body":"details about the architecture"},
        \\  "doc:other":{"title":"other","body":"unrelated notes"}
        \\}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const retrieval_body =
        \\{"query":"how does the architecture work","stream":false,"queries":[{"table":"docs","full_text_search":{"query":"body:overview"},"limit":5},{"table":"docs","tree_search":{"index":"doc_hierarchy","start_nodes":"$find_start","max_depth":2},"limit":5}]}
    ;
    var retrieval = try client.fetchRetrievalAgent(base_uri, retrieval_body);
    defer retrieval.deinit(std.testing.allocator);
    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, retrieval.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(RetrievalStrategy.hybrid, parsed.value.strategy_used.?);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.hits.len);
    try std.testing.expectEqualStrings("doc:root", parsed.value.hits[0]._id);
    try std.testing.expectEqualStrings("doc:child", parsed.value.hits[1]._id);
}

test "public api e2e supports retrieval agent tree search from roots" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-tree-roots-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-tree-roots-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 31232,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 31232,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "retrieval root tree docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const graph_index_body =
        \\{"name":"doc_hierarchy","type":"graph","edge_types":[{"name":"contains","topology":"tree"}]}
    ;
    var graph_index = try client.createTableIndex(base_uri, "docs", "doc_hierarchy", graph_index_body);
    defer graph_index.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc:root":{"title":"root","body":"architecture overview","_edges":{"doc_hierarchy":{"contains":[{"target":"doc:child","weight":1.0}]}}},
        \\  "doc:child":{"title":"child","body":"details about the architecture"}
        \\}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const retrieval_body =
        \\{"query":"how does the architecture work","stream":false,"queries":[{"table":"docs","tree_search":{"index":"doc_hierarchy","start_nodes":"$roots","max_depth":2},"limit":5}]}
    ;
    var retrieval = try client.fetchRetrievalAgent(base_uri, retrieval_body);
    defer retrieval.deinit(std.testing.allocator);
    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, retrieval.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(RetrievalStrategy.tree, parsed.value.strategy_used.?);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.hits.len);
    try std.testing.expectEqualStrings("doc:child", parsed.value.hits[0]._id);
}

test "public api e2e supports retrieval agent classification confidence and followup" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-classify-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-classify-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 31229,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 31229,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    var antfly_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeAntflyProvider.executor());
    defer antfly_listener.deinit();
    try antfly_listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const antfly_base_uri = try antfly_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(antfly_base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "retrieval classify docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"hello retrieval"},"doc:b":{"title":"beta","body":"secondary document"}}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const retrieval_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"query\":\"How does retrieval work?\",\"stream\":false,\"generator\":{{\"provider\":\"antfly\",\"model\":\"local-generator\",\"api_url\":{f}}},\"steps\":{{\"classification\":{{\"enabled\":true,\"with_reasoning\":true}},\"generation\":{{\"enabled\":true}},\"confidence\":{{\"enabled\":true}},\"followup\":{{\"enabled\":true,\"count\":3}}}},\"queries\":[{{\"table\":\"docs\",\"full_text_search\":{{\"query\":\"body:retrieval\"}},\"limit\":5}}]}}",
        .{std.json.fmt(antfly_base_uri, .{})},
    );
    defer std.testing.allocator.free(retrieval_body);
    var retrieval = try client.fetchRetrievalAgent(base_uri, retrieval_body);
    defer retrieval.deinit(std.testing.allocator);
    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, retrieval.body, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.classification != null);
    try std.testing.expectEqual(generating_api_openapi.RouteType.question, parsed.value.classification.?.route_type);
    try std.testing.expectEqual(generating_api_openapi.QueryStrategy.step_back, parsed.value.classification.?.strategy);
    try std.testing.expect(parsed.value.classification.?.step_back_query != null);
    try std.testing.expect(parsed.value.classification.?.multi_phrases != null);
    try std.testing.expect(parsed.value.classification.?.reasoning != null);
    try std.testing.expect(parsed.value.generation_confidence != null);
    try std.testing.expect(parsed.value.context_relevance != null);
    try std.testing.expect(parsed.value.followup_questions != null);
    try std.testing.expectEqual(@as(usize, 3), parsed.value.followup_questions.?.len);
}

test "public api e2e supports retrieval agent fixed-body sse streaming" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-stream-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-stream-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 31228,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 31228,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    var antfly_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeAntflyProvider.executor());
    defer antfly_listener.deinit();
    try antfly_listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const antfly_base_uri = try antfly_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(antfly_base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "retrieval stream docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"hello retrieval"},"doc:b":{"title":"beta","body":"secondary document"}}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const retrieval_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"query\":\"find retrieval docs\",\"stream\":true,\"generator\":{{\"provider\":\"antfly\",\"model\":\"local-generator\",\"api_url\":{f}}},\"steps\":{{\"generation\":{{\"enabled\":true}},\"followup\":{{\"enabled\":true,\"count\":2}}}},\"queries\":[{{\"table\":\"docs\",\"full_text_search\":{{\"query\":\"body:retrieval\"}},\"limit\":5}}]}}",
        .{std.json.fmt(antfly_base_uri, .{})},
    );
    defer std.testing.allocator.free(retrieval_body);
    var retrieval = try client.fetchRetrievalAgent(base_uri, retrieval_body);
    defer retrieval.deinit(std.testing.allocator);
    try std.testing.expect(retrieval.content_type != null);
    try std.testing.expectEqualStrings("text/event-stream", retrieval.content_type.?);
    const events = try parseSseEventsAlloc(std.testing.allocator, retrieval.body);
    defer std.testing.allocator.free(events);

    var saw_step_started = false;
    var saw_generation = false;
    var saw_followup = false;
    var saw_hit = false;
    var saw_done = false;
    for (events) |event| {
        if (std.mem.eql(u8, event.event, "step_started")) {
            saw_step_started = true;
        } else if (std.mem.eql(u8, event.event, "generation")) {
            saw_generation = true;
        } else if (std.mem.eql(u8, event.event, "followup")) {
            saw_followup = true;
        } else if (std.mem.eql(u8, event.event, "hit")) {
            saw_hit = true;
            var parsed_hit = try parseJsonBody(metadata_openapi.QueryHit, std.testing.allocator, event.data);
            defer parsed_hit.deinit();
            try std.testing.expectEqualStrings("doc:a", parsed_hit.value._id);
        } else if (std.mem.eql(u8, event.event, "done")) {
            saw_done = true;
            var parsed_done = try parseJsonBody(metadata_openapi.RetrievalAgentResult, std.testing.allocator, event.data);
            defer parsed_done.deinit();
            try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, parsed_done.value.status);
            try std.testing.expectEqual(@as(usize, 1), parsed_done.value.hits.len);
            try std.testing.expectEqualStrings("doc:a", parsed_done.value.hits[0]._id);
        }
    }
    try std.testing.expect(saw_step_started);
    try std.testing.expect(saw_generation);
    try std.testing.expect(saw_followup);
    try std.testing.expect(saw_hit);
    try std.testing.expect(saw_done);
}

test "public api e2e retrieval streaming emits clarification events" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-stream-clarify-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-stream-clarify-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 41441,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 41441,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "retrieval stream clarify docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"raft","body":"raft consensus in antfly","status":"active"},"doc:b":{"title":"other","body":"unrelated notes","status":"draft"}}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const retrieval_body =
        \\{"query":"How does Raft consensus work in Antfly?","stream":true,"max_internal_iterations":3,"max_user_clarifications":1,"require_decision_after":0,"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    var retrieval = try client.fetchRetrievalAgent(base_uri, retrieval_body);
    defer retrieval.deinit(std.testing.allocator);
    try std.testing.expect(retrieval.content_type != null);
    try std.testing.expectEqualStrings("text/event-stream", retrieval.content_type.?);
    const events = try parseSseEventsAlloc(std.testing.allocator, retrieval.body);
    defer std.testing.allocator.free(events);

    var saw_reasoning = false;
    var saw_clarification_progress = false;
    var saw_done = false;
    for (events) |event| {
        if (std.mem.eql(u8, event.event, "reasoning")) {
            saw_reasoning = true;
        } else if (std.mem.eql(u8, event.event, "step_progress")) {
            var parsed_progress = try parseJsonBody(RetrievalClarificationProgress, std.testing.allocator, event.data);
            defer parsed_progress.deinit();
            if (std.mem.eql(u8, parsed_progress.value.phase, "clarification")) {
                saw_clarification_progress = true;
                try std.testing.expectEqualStrings("clarification", parsed_progress.value.name);
                try std.testing.expect(parsed_progress.value.questions != null);
                try std.testing.expectEqual(@as(usize, 1), parsed_progress.value.questions.?.len);
                try std.testing.expectEqualStrings("select_query", parsed_progress.value.questions.?[0].id);
            }
        } else if (std.mem.eql(u8, event.event, "done")) {
            saw_done = true;
            var parsed_done = try parseJsonBody(metadata_openapi.RetrievalAgentResult, std.testing.allocator, event.data);
            defer parsed_done.deinit();
            try std.testing.expectEqual(metadata_openapi.AgentStatus.clarification_required, parsed_done.value.status);
            try std.testing.expect(parsed_done.value.questions != null);
            try std.testing.expectEqualStrings("select_query", parsed_done.value.questions.?[0].id);
        }
    }
    try std.testing.expect(saw_reasoning);
    try std.testing.expect(saw_clarification_progress);
    try std.testing.expect(saw_done);
}

test "public api e2e supports bounded agentic retrieval mode" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-agentic-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-agentic-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 31233,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 31233,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "retrieval agentic docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"raft","body":"raft consensus in antfly"},"doc:b":{"title":"other","body":"unrelated notes"}}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const retrieval_body =
        \\{"query":"How does Raft consensus work in Antfly?","stream":false,"max_internal_iterations":3,"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5}]}
    ;
    var retrieval = try client.fetchRetrievalAgent(base_uri, retrieval_body);
    defer retrieval.deinit(std.testing.allocator);
    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, retrieval.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(AgentStatus.completed, parsed.value.status);
    try std.testing.expect(parsed.value.tool_calls_made != null);
    try std.testing.expect(parsed.value.tool_calls_made.? > 0);
    try std.testing.expectEqual(@as(i64, 1), parsed.value.iteration.?);
    try std.testing.expectEqual(@as(i64, 2), parsed.value.remaining_internal_iterations.?);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.hits.len);
}

test "public api e2e agentic retrieval selects the best declared query" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-agentic-select-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-agentic-select-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 31234,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 31234,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "retrieval agentic select docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"raft","body":"raft consensus in antfly","status":"active"},"doc:b":{"title":"other","body":"unrelated notes","status":"draft"}}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const retrieval_body =
        \\{"query":"How does Raft consensus work in Antfly?","stream":false,"max_internal_iterations":3,"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    var retrieval = try client.fetchRetrievalAgent(base_uri, retrieval_body);
    defer retrieval.deinit(std.testing.allocator);
    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, retrieval.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed.value.tool_calls_made.?);
    try std.testing.expectEqual(RetrievalStrategy.bm25, parsed.value.strategy_used.?);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.hits.len);
    try std.testing.expectEqualStrings("doc:a", parsed.value.hits[0]._id);
}

test "public api e2e agentic retrieval evaluates misses and falls back to the next query" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-agentic-fallback-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-agentic-fallback-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 31248,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 31248,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "retrieval agentic fallback docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"raft","body":"raft consensus in antfly","status":"active"},"doc:b":{"title":"other","body":"unrelated notes","status":"draft"}}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const retrieval_body =
        \\{"query":"How does Raft consensus work in Antfly?","stream":false,"max_internal_iterations":3,"queries":[{"table":"docs","full_text_search":{"query":"body:missing"},"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    var retrieval = try client.fetchRetrievalAgent(base_uri, retrieval_body);
    defer retrieval.deinit(std.testing.allocator);
    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, retrieval.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(AgentStatus.completed, parsed.value.status);
    try std.testing.expectEqual(@as(i64, 2), parsed.value.tool_calls_made.?);
    try std.testing.expectEqual(RetrievalStrategy.hybrid, parsed.value.strategy_used.?);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.hits.len);
    try std.testing.expectEqualStrings("doc:a", parsed.value.hits[0]._id);

    var saw_evaluate = false;
    var saw_evaluation_select = false;
    for (parsed.value.steps.?) |step| {
        if (std.mem.eql(u8, step.name, "evaluate")) saw_evaluate = true;
        if (std.mem.eql(u8, step.name, "select_strategy") and step.details != null and step.details.? == .object) {
            if (step.details.?.object.get("selection_source")) |selection_source| {
                if (selection_source == .string and std.mem.eql(u8, selection_source.string, "evaluation")) {
                    saw_evaluation_select = true;
                }
            }
        }
    }
    try std.testing.expect(saw_evaluate);
    try std.testing.expect(saw_evaluation_select);
}

test "public api e2e agentic retrieval can require clarification and continue from a decision" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-agentic-decision-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-retrieval-agentic-decision-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 31235,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 31235,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "retrieval agentic decision docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"raft","body":"raft consensus in antfly","status":"active"},"doc:b":{"title":"other","body":"unrelated notes","status":"draft"}}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const clarify_body =
        \\{"query":"How does Raft consensus work in Antfly?","stream":false,"session_id":"retrieval-decision-session","max_internal_iterations":3,"max_user_clarifications":1,"require_decision_after":0,"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    var clarify = try client.fetchRetrievalAgent(base_uri, clarify_body);
    defer clarify.deinit(std.testing.allocator);
    var parsed_clarify = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, clarify.body, .{});
    defer parsed_clarify.deinit();
    try std.testing.expectEqual(AgentStatus.clarification_required, parsed_clarify.value.status);
    try std.testing.expect(parsed_clarify.value.questions != null);
    try std.testing.expectEqualStrings("select_query", parsed_clarify.value.questions.?[0].id);

    const continue_body =
        \\{"query":"How does Raft consensus work in Antfly?","stream":false,"session_id":"retrieval-decision-session","max_internal_iterations":3,"max_user_clarifications":1,"require_decision_after":0,"decisions":[{"question_id":"select_query","answer":0}],"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    var continued = try client.fetchRetrievalAgent(base_uri, continue_body);
    defer continued.deinit(std.testing.allocator);
    var parsed_continued = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, continued.body, .{});
    defer parsed_continued.deinit();
    try std.testing.expectEqual(AgentStatus.completed, parsed_continued.value.status);
    try std.testing.expectEqual(@as(i64, 1), parsed_continued.value.clarification_count.?);
    try std.testing.expectEqual(@as(i64, 0), parsed_continued.value.remaining_user_clarifications.?);
    try std.testing.expectEqual(RetrievalStrategy.bm25, parsed_continued.value.strategy_used.?);
    try std.testing.expectEqual(@as(usize, 1), parsed_continued.value.hits.len);
    try std.testing.expectEqualStrings("doc:a", parsed_continued.value.hits[0]._id);
}

test "public api e2e restores managed sparse embeddings from table backup" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-managed-sparse-backup-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-managed-sparse-backup-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-managed-sparse-backup-out", .{tmp.sub_path});
    defer std.testing.allocator.free(backup_root);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 2723,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 2723,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    var antfly_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeAntflyProvider.executor());
    defer antfly_listener.deinit();
    try antfly_listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const antfly_base_uri = try antfly_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(antfly_base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "managed sparse docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const sparse_index_body = try test_contract_helpers.encodeManagedSparseEmbeddingsIndexRequest(
        std.testing.allocator,
        "sparse_idx",
        "body",
        .{
            .provider = .antfly,
            .model = "antfly-sparse-v1",
            .api_url = antfly_base_uri,
        },
    );
    defer std.testing.allocator.free(sparse_index_body);
    var sparse_index_resp = try client.createTableIndex(base_uri, "docs", "sparse_idx", sparse_index_body);
    defer sparse_index_resp.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"alpha body"},
        \\  "doc:b":{"title":"beta","body":"beta body"}
        \\}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const sparse_query_body = try test_contract_helpers.encodeSparseEmbeddingsQueryRequest(std.testing.allocator, "sparse_idx", &.{ 7, 42 }, &.{ 1.5, 0.5 }, 5);
    defer std.testing.allocator.free(sparse_query_body);
    var sparse_query_before = try client.fetchQuery(base_uri, "docs", sparse_query_body);
    defer sparse_query_before.deinit(std.testing.allocator);
    var parsed_sparse_before = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, sparse_query_before.body, .{});
    defer parsed_sparse_before.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_sparse_before.value.responses.?[0].hits.?.hits.?[0]._id);

    var sparse_index_before = try client.fetchTableIndex(base_uri, "docs", "sparse_idx");
    defer sparse_index_before.deinit(std.testing.allocator);
    var parsed_sparse_index_before = try parseJsonBody(IndexStatusSummary, std.testing.allocator, sparse_index_before.body);
    defer parsed_sparse_index_before.deinit();
    try std.testing.expectEqual(@as(?bool, false), parsed_sparse_index_before.value.status.backfill_active);
    try std.testing.expectEqual(@as(?u64, 2), parsed_sparse_index_before.value.status.doc_count);

    const backup_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"sparse-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(backup_body);
    var backup_resp = try client.fetchBackupTable(base_uri, "docs", backup_body);
    defer backup_resp.deinit(std.testing.allocator);
    var parsed_backup = try parseJsonBody(metadata_openapi.ClusterBackupResponse, std.testing.allocator, backup_resp.body);
    defer parsed_backup.deinit();
    try std.testing.expectEqualStrings("successful", parsed_backup.value.status);

    _ = try client.dropTable(base_uri, "docs");

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchTable(base_uri, "docs"));

    const restore_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"sparse-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(restore_body);
    var restore_resp = try client.fetchRestoreTable(base_uri, "docs", restore_body);
    defer restore_resp.deinit(std.testing.allocator);
    var parsed_restore = try parseJsonBody(metadata_openapi.ClusterRestoreResponse, std.testing.allocator, restore_resp.body);
    defer parsed_restore.deinit();
    try std.testing.expectEqualStrings("triggered", parsed_restore.value.status);

    rounds = 0;
    while (rounds < 12) : (rounds += 1) try svc.runRound();

    var restored_table = try client.fetchTable(base_uri, "docs");
    defer restored_table.deinit(std.testing.allocator);
    var parsed_restored_table = try parseJsonBody(metadata_openapi.TableStatus, std.testing.allocator, restored_table.body);
    defer parsed_restored_table.deinit();
    try std.testing.expect(parsed_restored_table.value.indexes.map.get("sparse_idx") != null);

    var sparse_index_after = try client.fetchTableIndex(base_uri, "docs", "sparse_idx");
    defer sparse_index_after.deinit(std.testing.allocator);
    var parsed_sparse_index_after = try parseJsonBody(IndexStatusSummary, std.testing.allocator, sparse_index_after.body);
    defer parsed_sparse_index_after.deinit();
    try std.testing.expectEqual(@as(?bool, false), parsed_sparse_index_after.value.status.backfill_active);
    try std.testing.expectEqual(@as(?u64, 2), parsed_sparse_index_after.value.status.doc_count);

    var sparse_query_after = try client.fetchQuery(base_uri, "docs", sparse_query_body);
    defer sparse_query_after.deinit(std.testing.allocator);
    var parsed_sparse_after = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, sparse_query_after.body, .{});
    defer parsed_sparse_after.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_sparse_after.value.responses.?[0].hits.?.hits.?[0]._id);
}

test "public api e2e supports embedding_template remote media helper" {
    const FakeRemoteMedia = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.GET, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/kitten.png"));
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "image/png"),
                .body = try alloc.dupe(u8, "png-bytes"),
            };
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-embedding-template-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-embedding-template-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 2722,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 2722,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    var antfly_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeAntflyProvider.executor());
    defer antfly_listener.deinit();
    try antfly_listener.start();

    var media_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeRemoteMedia.executor());
    defer media_listener.deinit();
    try media_listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const antfly_base_uri = try antfly_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(antfly_base_uri);
    const media_base_uri = try media_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(media_base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "template docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const semantic_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexRequest(
        std.testing.allocator,
        "semantic_idx",
        "body",
        3,
        .{
            .provider = .antfly,
            .model = "antfly-clip-v1",
            .api_url = antfly_base_uri,
            .multimodal = true,
        },
        null,
    );
    defer std.testing.allocator.free(semantic_index_body);
    var semantic_index_resp = try client.createTableIndex(base_uri, "docs", "semantic_idx", semantic_index_body);
    defer semantic_index_resp.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"alpha body","photo":"MEDIA_URL_A"},
        \\  "doc:b":{"title":"beta","body":"beta body","photo":"MEDIA_URL_B"}
        \\}}
    );
    defer std.testing.allocator.free(batch_body);
    const media_url_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/kitten.png", .{media_base_uri});
    defer std.testing.allocator.free(media_url_a);
    const media_url_b = try std.fmt.allocPrint(std.testing.allocator, "{s}/doc.txt", .{media_base_uri});
    defer std.testing.allocator.free(media_url_b);
    const batch_body_replaced = try std.mem.replaceOwned(u8, std.testing.allocator, batch_body, "MEDIA_URL_A", media_url_a);
    defer std.testing.allocator.free(batch_body_replaced);
    const final_batch_body = try std.mem.replaceOwned(u8, std.testing.allocator, batch_body_replaced, "MEDIA_URL_B", media_url_b);
    defer std.testing.allocator.free(final_batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", final_batch_body);
    defer batch.deinit(std.testing.allocator);

    const query_body = try test_contract_helpers.encodeSemanticQueryWithTemplateRequest(
        std.testing.allocator,
        media_url_a,
        "{{remoteMedia url=this}}",
        &.{"semantic_idx"},
        5,
    );
    defer std.testing.allocator.free(query_body);
    var query = try client.fetchQuery(base_uri, "docs", query_body);
    defer query.deinit(std.testing.allocator);
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, query.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed.value.responses.?[0].hits.?.hits.?[0]._id);

    const template_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.testing.allocator, "alpha concept", &.{"semantic_template_idx"}, 5);
    defer std.testing.allocator.free(template_query_body);
    var template_query = try client.fetchQuery(base_uri, "docs", template_query_body);
    defer template_query.deinit(std.testing.allocator);
    var parsed_template = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, template_query.body, .{});
    defer parsed_template.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_template.value.responses.?[0].hits.?.hits.?[0]._id);
}

test "public api e2e supports template chunked remote text enrichment and query helper failures" {
    const FakeRemoteAssets = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.GET, req.method);
            if (std.mem.endsWith(u8, req.uri, "/alpha.txt")) {
                return .{
                    .status = 200,
                    .content_type = try alloc.dupe(u8, "text/plain"),
                    .body = try alloc.dupe(u8, "alpha body alpha body alpha tail"),
                };
            }
            if (std.mem.endsWith(u8, req.uri, "/beta.txt")) {
                return .{
                    .status = 200,
                    .content_type = try alloc.dupe(u8, "text/plain"),
                    .body = try alloc.dupe(u8, "beta body beta tail"),
                };
            }
            if (std.mem.endsWith(u8, req.uri, "/kitten.png")) {
                return .{
                    .status = 200,
                    .content_type = try alloc.dupe(u8, "image/png"),
                    .body = try alloc.dupe(u8, "png-bytes"),
                };
            }
            return error.TestUnexpectedResult;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-template-chunked-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-template-chunked-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3322,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3322,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    var embed_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeEmbeddingProvider.executor());
    defer embed_listener.deinit();
    try embed_listener.start();

    var remote_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeRemoteAssets.executor());
    defer remote_listener.deinit();
    try remote_listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const embed_base_uri = try embed_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(embed_base_uri);
    const remote_base_uri = try remote_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(remote_base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "template chunked docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const template_chunked_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexTemplateWithChunkerRequest(
        std.testing.allocator,
        "semantic_template_chunked_idx",
        "{{title}} {{remoteText url=transcript}}",
        3,
        .{
            .provider = .openai,
            .model = "text-embedding-3-small",
            .url = embed_base_uri,
        },
        .{
            .provider = .antfly,
            .model = "fixed-bert-tokenizer",
        },
    );
    defer std.testing.allocator.free(template_chunked_index_body);
    var template_chunked_index_resp = try client.createTableIndex(base_uri, "docs", "semantic_template_chunked_idx", template_chunked_index_body);
    defer template_chunked_index_resp.deinit(std.testing.allocator);

    const sparse_index_body = "{\"name\":\"sparse_idx\",\"type\":\"embeddings\",\"sparse\":true,\"external\":true}";
    var sparse_index_resp = try client.createTableIndex(base_uri, "docs", "sparse_idx", sparse_index_body);
    defer sparse_index_resp.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const transcript_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/alpha.txt", .{remote_base_uri});
    defer std.testing.allocator.free(transcript_a);
    const transcript_b = try std.fmt.allocPrint(std.testing.allocator, "{s}/beta.txt", .{remote_base_uri});
    defer std.testing.allocator.free(transcript_b);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc-a":{"title":"alpha","transcript":"TRANSCRIPT_A","_embeddings":{"sparse_idx":{"7":1.5,"42":0.5}}},
        \\  "doc-b":{"title":"beta","transcript":"TRANSCRIPT_B","_embeddings":{"sparse_idx":{"99":2.0}}}
        \\},"sync_level":"full_index"}
    );
    defer std.testing.allocator.free(batch_body);
    const batch_a = try std.mem.replaceOwned(u8, std.testing.allocator, batch_body, "TRANSCRIPT_A", transcript_a);
    defer std.testing.allocator.free(batch_a);
    const final_batch_body = try std.mem.replaceOwned(u8, std.testing.allocator, batch_a, "TRANSCRIPT_B", transcript_b);
    defer std.testing.allocator.free(final_batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", final_batch_body);
    defer batch.deinit(std.testing.allocator);

    const full_text_query_body = try test_contract_helpers.encodeMatchQueryRequest(
        std.testing.allocator,
        "body",
        "routing",
        &.{},
        5,
    );
    defer std.testing.allocator.free(full_text_query_body);
    var full_text_query = try client.fetchQuery(base_uri, "docs", full_text_query_body);
    defer full_text_query.deinit(std.testing.allocator);
    var parsed_full_text = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, full_text_query.body, .{});
    defer parsed_full_text.deinit();
    try public_test_helpers.expectSingleOpenapiTopHit(parsed_full_text.value, "doc-a");

    const query_text_url = try std.fmt.allocPrint(std.testing.allocator, "{s}/alpha.txt", .{remote_base_uri});
    defer std.testing.allocator.free(query_text_url);
    const template_query_body = try test_contract_helpers.encodeSemanticQueryWithTemplateRequest(
        std.testing.allocator,
        query_text_url,
        "{{remoteText url=this}}",
        &.{"semantic_template_chunked_idx"},
        5,
    );
    defer std.testing.allocator.free(template_query_body);
    var query = try client.fetchQuery(base_uri, "docs", template_query_body);
    defer query.deinit(std.testing.allocator);
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, query.body, .{});
    defer parsed.deinit();
    try public_test_helpers.expectSingleOpenapiTopHit(parsed.value, "doc-a");

    const sparse_query_body = try test_contract_helpers.encodeSparseEmbeddingsQueryRequest(
        std.testing.allocator,
        "sparse_idx",
        &.{ 7, 42 },
        &.{ 1.5, 0.5 },
        5,
    );
    defer std.testing.allocator.free(sparse_query_body);
    var sparse_query = try client.fetchQuery(base_uri, "docs", sparse_query_body);
    defer sparse_query.deinit(std.testing.allocator);
    var parsed_sparse = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, sparse_query.body, .{});
    defer parsed_sparse.deinit();
    try public_test_helpers.expectSingleOpenapiTopHit(parsed_sparse.value, "doc-a");

    const hybrid_query_body = try test_contract_helpers.encodeSemanticSparseHybridQueryRequest(
        std.testing.allocator,
        query_text_url,
        "semantic_template_chunked_idx",
        "sparse_idx",
        &.{ 7, 42 },
        &.{ 1.5, 0.5 },
        5,
    );
    defer std.testing.allocator.free(hybrid_query_body);
    var hybrid_query = try client.fetchQuery(base_uri, "docs", hybrid_query_body);
    defer hybrid_query.deinit(std.testing.allocator);
    var parsed_hybrid = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, hybrid_query.body, .{});
    defer parsed_hybrid.deinit();
    try public_test_helpers.expectSingleOpenapiTopHit(parsed_hybrid.value, "doc-a");

    const unknown_dense_hybrid_query_body = try test_contract_helpers.encodeSemanticSparseHybridQueryRequest(
        std.testing.allocator,
        query_text_url,
        "unknown_semantic",
        "sparse_idx",
        &.{ 7, 42 },
        &.{ 1.5, 0.5 },
        5,
    );
    defer std.testing.allocator.free(unknown_dense_hybrid_query_body);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchQuery(base_uri, "docs", unknown_dense_hybrid_query_body));

    const unknown_sparse_hybrid_query_body = try test_contract_helpers.encodeSemanticSparseHybridQueryRequest(
        std.testing.allocator,
        query_text_url,
        "semantic_template_chunked_idx",
        "unknown_sparse",
        &.{ 7, 42 },
        &.{ 1.5, 0.5 },
        5,
    );
    defer std.testing.allocator.free(unknown_sparse_hybrid_query_body);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchQuery(base_uri, "docs", unknown_sparse_hybrid_query_body));

    const projected_ranges = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, projected_ranges);
    try std.testing.expect(projected_ranges.len > 0);
    const group_id = projected_ranges[0].group_id;
    const provisioned_db_path = try metadata_mod.groupDbPathFromReplicaRoot(std.testing.allocator, replica_root, group_id);
    defer std.testing.allocator.free(provisioned_db_path);
    var db = try db_mod.DB.open(std.testing.allocator, provisioned_db_path, .{});
    defer db.close();

    const chunk_zero = try internal_keys.chunkArtifactKeyAlloc(std.testing.allocator, "doc-a", "semantic_template_chunked_idx_chunks", 0);
    defer std.testing.allocator.free(chunk_zero);
    const chunk_one = try internal_keys.chunkArtifactKeyAlloc(std.testing.allocator, "doc-a", "semantic_template_chunked_idx_chunks", 1);
    defer std.testing.allocator.free(chunk_one);
    const raw_zero = try db.get(std.testing.allocator, chunk_zero);
    defer if (raw_zero) |raw| std.testing.allocator.free(raw);
    try std.testing.expect(raw_zero != null);
    const raw_one = try db.get(std.testing.allocator, chunk_one);
    defer if (raw_one) |raw| std.testing.allocator.free(raw);
    try std.testing.expect(raw_one != null);

    const bad_query_url = try std.fmt.allocPrint(std.testing.allocator, "{s}/kitten.png", .{remote_base_uri});
    defer std.testing.allocator.free(bad_query_url);
    const bad_query_body = try test_contract_helpers.encodeSemanticQueryWithTemplateRequest(
        std.testing.allocator,
        bad_query_url,
        "{{remoteText url=this}}",
        &.{"semantic_template_chunked_idx"},
        5,
    );
    defer std.testing.allocator.free(bad_query_body);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchQuery(base_uri, "docs", bad_query_body));

    try std.testing.expectError(
        error.UnexpectedHttpStatus,
        client.fetchQuery(
            base_uri,
            "docs",
            "{\"embedding_template\":\"{{remoteText url=this}}\",\"indexes\":[\"semantic_template_chunked_idx\"],\"limit\":5}",
        ),
    );
}

test "public api e2e supports fixed and antfly chunked semantic search" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-chunked-semantic-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-chunked-semantic-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3122,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3122,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    var openai_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeEmbeddingProvider.executor());
    defer openai_listener.deinit();
    try openai_listener.start();

    var antfly_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeAntflyProvider.executor());
    defer antfly_listener.deinit();
    try antfly_listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const openai_base_uri = try openai_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(openai_base_uri);
    const antfly_base_uri = try antfly_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(antfly_base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "chunked docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const fixed_chunked_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexRequest(
        std.testing.allocator,
        "semantic_fixed_idx",
        "body",
        3,
        .{
            .provider = .openai,
            .model = "text-embedding-3-small",
            .url = openai_base_uri,
        },
        .{
            .provider = .antfly,
            .model = "fixed-bert-tokenizer",
        },
    );
    defer std.testing.allocator.free(fixed_chunked_index_body);
    var fixed_index_resp = try client.createTableIndex(base_uri, "docs", "semantic_fixed_idx", fixed_chunked_index_body);
    defer fixed_index_resp.deinit(std.testing.allocator);

    const antfly_chunk_api = try std.fmt.allocPrint(std.testing.allocator, "{s}/api", .{antfly_base_uri});
    defer std.testing.allocator.free(antfly_chunk_api);
    const antfly_chunked_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexRequest(
        std.testing.allocator,
        "semantic_antfly_idx",
        "body",
        3,
        .{
            .provider = .antfly,
            .model = "antfly-embed-v1",
            .api_url = antfly_base_uri,
        },
        .{
            .provider = .antfly,
            .api_url = antfly_chunk_api,
            .model = "antfly-chunker-v1",
        },
    );
    defer std.testing.allocator.free(antfly_chunked_index_body);
    var antfly_index_resp = try client.createTableIndex(base_uri, "docs", "semantic_antfly_idx", antfly_chunked_index_body);
    defer antfly_index_resp.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"alpha body alpha body alpha body alpha body alpha tail"}
        \\}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    const fixed_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.testing.allocator, "alpha concept", &.{"semantic_fixed_idx"}, 5);
    defer std.testing.allocator.free(fixed_query_body);
    var fixed_query = try client.fetchQuery(base_uri, "docs", fixed_query_body);
    defer fixed_query.deinit(std.testing.allocator);
    var parsed_fixed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, fixed_query.body, .{});
    defer parsed_fixed.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_fixed.value.responses.?[0].hits.?.hits.?[0]._id);

    const antfly_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.testing.allocator, "alpha concept", &.{"semantic_antfly_idx"}, 5);
    defer std.testing.allocator.free(antfly_query_body);
    var antfly_query = try client.fetchQuery(base_uri, "docs", antfly_query_body);
    defer antfly_query.deinit(std.testing.allocator);
    var parsed_antfly = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, antfly_query.body, .{});
    defer parsed_antfly.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_antfly.value.responses.?[0].hits.?.hits.?[0]._id);

    const projected_ranges = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, projected_ranges);
    try std.testing.expect(projected_ranges.len > 0);
    const group_id = projected_ranges[0].group_id;
    const provisioned_db_path = try metadata_mod.groupDbPathFromReplicaRoot(std.testing.allocator, replica_root, group_id);
    defer std.testing.allocator.free(provisioned_db_path);

    var db = try db_mod.DB.open(std.testing.allocator, provisioned_db_path, .{});
    defer db.close();

    const fixed_chunk_zero = try internal_keys.chunkArtifactKeyAlloc(std.testing.allocator, "doc:a", "semantic_fixed_idx_chunks", 0);
    defer std.testing.allocator.free(fixed_chunk_zero);
    const fixed_chunk_one = try internal_keys.chunkArtifactKeyAlloc(std.testing.allocator, "doc:a", "semantic_fixed_idx_chunks", 1);
    defer std.testing.allocator.free(fixed_chunk_one);
    const antfly_chunk_zero = try internal_keys.chunkArtifactKeyAlloc(std.testing.allocator, "doc:a", "semantic_antfly_idx_chunks", 0);
    defer std.testing.allocator.free(antfly_chunk_zero);
    const antfly_chunk_one = try internal_keys.chunkArtifactKeyAlloc(std.testing.allocator, "doc:a", "semantic_antfly_idx_chunks", 1);
    defer std.testing.allocator.free(antfly_chunk_one);

    const fixed_raw_zero = try db.get(std.testing.allocator, fixed_chunk_zero);
    defer if (fixed_raw_zero) |raw| std.testing.allocator.free(raw);
    try std.testing.expect(fixed_raw_zero != null);
    const fixed_raw_one = try db.get(std.testing.allocator, fixed_chunk_one);
    defer if (fixed_raw_one) |raw| std.testing.allocator.free(raw);
    try std.testing.expect(fixed_raw_one != null);

    const antfly_raw_zero = try db.get(std.testing.allocator, antfly_chunk_zero);
    defer if (antfly_raw_zero) |raw| std.testing.allocator.free(raw);
    try std.testing.expect(antfly_raw_zero != null);
    const antfly_raw_one = try db.get(std.testing.allocator, antfly_chunk_one);
    defer if (antfly_raw_one) |raw| std.testing.allocator.free(raw);
    try std.testing.expect(antfly_raw_one != null);
}

test "public api e2e restores chunked managed embeddings from table backup" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-chunked-backup-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-chunked-backup-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-chunked-backup-out", .{tmp.sub_path});
    defer std.testing.allocator.free(backup_root);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3123,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3123,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    var openai_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeEmbeddingProvider.executor());
    defer openai_listener.deinit();
    try openai_listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const openai_base_uri = try openai_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(openai_base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "chunked docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    const fixed_chunked_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexRequest(
        std.testing.allocator,
        "semantic_fixed_idx",
        "body",
        3,
        .{
            .provider = .openai,
            .model = "text-embedding-3-small",
            .url = openai_base_uri,
        },
        .{
            .provider = .antfly,
            .model = "fixed-bert-tokenizer",
        },
    );
    defer std.testing.allocator.free(fixed_chunked_index_body);
    var fixed_index_resp = try client.createTableIndex(base_uri, "docs", "semantic_fixed_idx", fixed_chunked_index_body);
    defer fixed_index_resp.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"alpha body alpha body alpha body alpha body alpha tail"}
        \\}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    const fixed_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.testing.allocator, "alpha concept", &.{"semantic_fixed_idx"}, 5);
    defer std.testing.allocator.free(fixed_query_body);
    var fixed_query_before = try client.fetchQuery(base_uri, "docs", fixed_query_body);
    defer fixed_query_before.deinit(std.testing.allocator);
    var parsed_fixed_before = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, fixed_query_before.body, .{});
    defer parsed_fixed_before.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_fixed_before.value.responses.?[0].hits.?.hits.?[0]._id);

    const projected_ranges_before = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, projected_ranges_before);
    try std.testing.expect(projected_ranges_before.len > 0);
    const group_id_before = projected_ranges_before[0].group_id;
    const db_path_before = try metadata_mod.groupDbPathFromReplicaRoot(std.testing.allocator, replica_root, group_id_before);
    defer std.testing.allocator.free(db_path_before);

    var db_before = try db_mod.DB.open(std.testing.allocator, db_path_before, .{});
    defer db_before.close();

    const fixed_chunk_zero = try internal_keys.chunkArtifactKeyAlloc(std.testing.allocator, "doc:a", "semantic_fixed_idx_chunks", 0);
    defer std.testing.allocator.free(fixed_chunk_zero);
    const fixed_chunk_one = try internal_keys.chunkArtifactKeyAlloc(std.testing.allocator, "doc:a", "semantic_fixed_idx_chunks", 1);
    defer std.testing.allocator.free(fixed_chunk_one);

    const fixed_raw_zero_before = try db_before.get(std.testing.allocator, fixed_chunk_zero);
    defer if (fixed_raw_zero_before) |raw| std.testing.allocator.free(raw);
    try std.testing.expect(fixed_raw_zero_before != null);
    const fixed_raw_one_before = try db_before.get(std.testing.allocator, fixed_chunk_one);
    defer if (fixed_raw_one_before) |raw| std.testing.allocator.free(raw);
    try std.testing.expect(fixed_raw_one_before != null);

    const backup_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"chunked-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(backup_body);
    var backup_resp = try client.fetchBackupTable(base_uri, "docs", backup_body);
    defer backup_resp.deinit(std.testing.allocator);
    var parsed_backup = try parseJsonBody(metadata_openapi.ClusterBackupResponse, std.testing.allocator, backup_resp.body);
    defer parsed_backup.deinit();
    try std.testing.expectEqualStrings("successful", parsed_backup.value.status);

    _ = try client.dropTable(base_uri, "docs");

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchTable(base_uri, "docs"));

    const restore_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"chunked-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(restore_body);
    var restore_resp = try client.fetchRestoreTable(base_uri, "docs", restore_body);
    defer restore_resp.deinit(std.testing.allocator);
    var parsed_restore = try parseJsonBody(metadata_openapi.ClusterRestoreResponse, std.testing.allocator, restore_resp.body);
    defer parsed_restore.deinit();
    try std.testing.expectEqualStrings("triggered", parsed_restore.value.status);

    rounds = 0;
    while (rounds < 12) : (rounds += 1) try svc.runRound();

    var restored_table = try client.fetchTable(base_uri, "docs");
    defer restored_table.deinit(std.testing.allocator);
    var parsed_restored_table = try parseJsonBody(metadata_openapi.TableStatus, std.testing.allocator, restored_table.body);
    defer parsed_restored_table.deinit();
    try std.testing.expect(parsed_restored_table.value.indexes.map.get("semantic_fixed_idx") != null);

    var fixed_index_after = try client.fetchTableIndex(base_uri, "docs", "semantic_fixed_idx");
    defer fixed_index_after.deinit(std.testing.allocator);
    var parsed_fixed_index_after = try parseJsonBody(IndexStatusSummary, std.testing.allocator, fixed_index_after.body);
    defer parsed_fixed_index_after.deinit();
    try std.testing.expectEqual(@as(?bool, false), parsed_fixed_index_after.value.status.backfill_active);
    try std.testing.expectEqual(@as(?u64, 1), parsed_fixed_index_after.value.status.doc_count);

    const projected_ranges_after = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, projected_ranges_after);
    try std.testing.expect(projected_ranges_after.len > 0);
    const group_id_after = projected_ranges_after[0].group_id;
    const db_path_after = try metadata_mod.groupDbPathFromReplicaRoot(std.testing.allocator, replica_root, group_id_after);
    defer std.testing.allocator.free(db_path_after);

    var db_after = try db_mod.DB.open(std.testing.allocator, db_path_after, .{});
    defer db_after.close();

    const fixed_raw_zero_after = try db_after.get(std.testing.allocator, fixed_chunk_zero);
    defer if (fixed_raw_zero_after) |raw| std.testing.allocator.free(raw);
    try std.testing.expect(fixed_raw_zero_after != null);
    const fixed_raw_one_after = try db_after.get(std.testing.allocator, fixed_chunk_one);
    defer if (fixed_raw_one_after) |raw| std.testing.allocator.free(raw);
    try std.testing.expect(fixed_raw_one_after != null);

    var fixed_query_after = try client.fetchQuery(base_uri, "docs", fixed_query_body);
    defer fixed_query_after.deinit(std.testing.allocator);
    var parsed_fixed_after = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, fixed_query_after.body, .{});
    defer parsed_fixed_after.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_fixed_after.value.responses.?[0].hits.?.hits.?[0]._id);
}

test "public api e2e supports graph queries" {
    const expectSingleGraphResult = struct {
        fn get(parsed: metadata_openapi.QueryResponses, name: []const u8) !indexes_openapi.GraphQueryResult {
            const responses = parsed.responses orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(@as(usize, 1), responses.len);
            const graph_results = responses[0].graph_results orelse return error.TestUnexpectedResult;
            return graph_results.map.get(name) orelse return error.TestUnexpectedResult;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3212,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3212,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "graph docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var graph_index_resp = try client.createTableIndex(base_uri, "docs", "graph_idx", "{\"name\":\"graph_idx\",\"type\":\"graph\"}");
    defer graph_index_resp.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc-a":{"title":"alpha","_edges":{"graph_idx":{"cites":[{"target":"doc-b","weight":1.5}],"related":[{"target":"doc-c","weight":0.5}]}}},
        \\  "doc-b":{"title":"beta","_edges":{"graph_idx":{"cites":[{"target":"doc-c","weight":2.0}]}}},
        \\  "doc-c":{"title":"gamma"}
        \\}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    const graph_query_body = try test_contract_helpers.encodeGraphNeighborsQueryRequest(
        std.testing.allocator,
        "neighbors",
        "graph_idx",
        &.{"doc-a"},
        &.{ "cites", "related" },
        10,
    );
    defer std.testing.allocator.free(graph_query_body);
    var graph_query = try client.fetchQuery(base_uri, "docs", graph_query_body);
    defer graph_query.deinit(std.testing.allocator);

    var parsed_graph = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, graph_query.body, .{});
    defer parsed_graph.deinit();
    try std.testing.expectEqual(@as(i64, 0), parsed_graph.value.responses.?[0].hits.?.total);
    const neighbors = try expectSingleGraphResult.get(parsed_graph.value, "neighbors");
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.neighbors, neighbors.type);
    try std.testing.expectEqual(@as(i64, 2), neighbors.total);
    try std.testing.expectEqual(@as(usize, 2), neighbors.nodes.?.len);
    try std.testing.expectEqualStrings("doc-b", neighbors.nodes.?[0].key);
    try std.testing.expectEqualStrings("doc-c", neighbors.nodes.?[1].key);

    const traverse_query_body = try test_contract_helpers.encodeGraphTraverseQueryRequestWithPaths(
        std.testing.allocator,
        "traverse",
        "graph_idx",
        &.{"doc-a"},
        &.{"cites"},
        2,
        10,
    );
    defer std.testing.allocator.free(traverse_query_body);
    var traverse_query = try client.fetchQuery(base_uri, "docs", traverse_query_body);
    defer traverse_query.deinit(std.testing.allocator);
    var parsed_traverse = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, traverse_query.body, .{});
    defer parsed_traverse.deinit();
    const traverse = try expectSingleGraphResult.get(parsed_traverse.value, "traverse");
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.traverse, traverse.type);
    try std.testing.expectEqual(@as(i64, 2), traverse.total);
    try std.testing.expectEqual(@as(usize, 2), traverse.nodes.?.len);
    try std.testing.expectEqualStrings("doc-b", traverse.nodes.?[0].key);
    try std.testing.expectEqualStrings("doc-c", traverse.nodes.?[1].key);
    try std.testing.expectEqual(@as(i64, 2), traverse.nodes.?[1].depth.?);
    try std.testing.expectEqual(@as(usize, 3), traverse.nodes.?[1].path.?.len);
    try std.testing.expectEqualStrings("doc-a", traverse.nodes.?[1].path.?[0]);
    try std.testing.expectEqualStrings("doc-b", traverse.nodes.?[1].path.?[1]);
    try std.testing.expectEqualStrings("doc-c", traverse.nodes.?[1].path.?[2]);

    const shortest_path_query_body = try test_contract_helpers.encodeGraphShortestPathQueryRequest(
        std.testing.allocator,
        "shortest",
        "graph_idx",
        &.{"doc-a"},
        &.{"doc-c"},
        &.{"cites"},
        4,
        10,
    );
    defer std.testing.allocator.free(shortest_path_query_body);
    var shortest_path_query = try client.fetchQuery(base_uri, "docs", shortest_path_query_body);
    defer shortest_path_query.deinit(std.testing.allocator);
    var parsed_shortest = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, shortest_path_query.body, .{});
    defer parsed_shortest.deinit();
    const shortest = try expectSingleGraphResult.get(parsed_shortest.value, "shortest");
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.shortest_path, shortest.type);
    try std.testing.expectEqual(@as(i64, 1), shortest.total);
    try std.testing.expectEqual(@as(usize, 1), shortest.nodes.?.len);
    try std.testing.expectEqualStrings("doc-c", shortest.nodes.?[0].key);
    try std.testing.expectEqual(@as(i64, 2), shortest.nodes.?[0].depth.?);
    try std.testing.expectEqual(@as(usize, 3), shortest.nodes.?[0].path.?.len);
    try std.testing.expectEqualStrings("doc-a", shortest.nodes.?[0].path.?[0]);
    try std.testing.expectEqualStrings("doc-b", shortest.nodes.?[0].path.?[1]);
    try std.testing.expectEqualStrings("doc-c", shortest.nodes.?[0].path.?[2]);

    try std.testing.expectError(
        error.UnexpectedHttpStatus,
        client.fetchQuery(
            base_uri,
            "docs",
            "{\"graph_searches\":{\"neighbors\":{\"type\":\"neighbors\",\"index_name\":\"graph_idx\",\"params\":{\"edge_types\":[\"cites\"]}}}}",
        ),
    );
    try std.testing.expectError(
        error.UnexpectedHttpStatus,
        client.fetchQuery(
            base_uri,
            "docs",
            "{\"graph_searches\":{\"traverse\":{\"type\":\"traverse\",\"index_name\":\"graph_idx\",\"params\":{\"edge_types\":[\"cites\"],\"max_depth\":2}}}}",
        ),
    );
    try std.testing.expectError(
        error.UnexpectedHttpStatus,
        client.fetchQuery(
            base_uri,
            "docs",
            "{\"graph_searches\":{\"shortest\":{\"type\":\"shortest_path\",\"index_name\":\"graph_idx\",\"start_nodes\":{\"keys\":[\"doc-a\"]},\"params\":{\"edge_types\":[\"cites\"],\"max_depth\":4}}}}",
        ),
    );
}

test "public api e2e graph queries respect full_index sync level" {
    const expectSingleGraphResult = struct {
        fn get(parsed: metadata_openapi.QueryResponses, name: []const u8) !indexes_openapi.GraphQueryResult {
            const responses = parsed.responses orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(@as(usize, 1), responses.len);
            const graph_results = responses[0].graph_results orelse return error.TestUnexpectedResult;
            return graph_results.map.get(name) orelse return error.TestUnexpectedResult;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-full-index-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-full-index-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3213,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3213,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "graph full index docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var graph_index_resp = try client.createTableIndex(base_uri, "docs", "graph_idx", "{\"name\":\"graph_idx\",\"type\":\"graph\"}");
    defer graph_index_resp.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc-a":{"title":"alpha","_edges":{"graph_idx":{"cites":[{"target":"doc-b","weight":1.0},{"target":"doc-c","weight":2.0}]}}},
        \\  "doc-b":{"title":"beta"},
        \\  "doc-c":{"title":"gamma"}
        \\},"sync_level":"full_index"}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    const graph_query_body = try test_contract_helpers.encodeGraphNeighborsQueryRequest(
        std.testing.allocator,
        "neighbors",
        "graph_idx",
        &.{"doc-a"},
        &.{"cites"},
        10,
    );
    defer std.testing.allocator.free(graph_query_body);

    var graph_query = try client.fetchQuery(base_uri, "docs", graph_query_body);
    defer graph_query.deinit(std.testing.allocator);
    var parsed_graph = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, graph_query.body, .{});
    defer parsed_graph.deinit();
    const neighbors = try expectSingleGraphResult.get(parsed_graph.value, "neighbors");
    try std.testing.expectEqual(@as(i64, 2), neighbors.total);
    try std.testing.expectEqualStrings("doc-b", neighbors.nodes.?[0].key);
    try std.testing.expectEqualStrings("doc-c", neighbors.nodes.?[1].key);

    const delete_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"deletes":["doc-b"],"sync_level":"full_index"}
    );
    defer std.testing.allocator.free(delete_body);
    var delete_batch = try client.fetchBatch(base_uri, "docs", delete_body);
    defer delete_batch.deinit(std.testing.allocator);

    var graph_query_after = try client.fetchQuery(base_uri, "docs", graph_query_body);
    defer graph_query_after.deinit(std.testing.allocator);
    var parsed_after = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, graph_query_after.body, .{});
    defer parsed_after.deinit();
    const neighbors_after = try expectSingleGraphResult.get(parsed_after.value, "neighbors");
    try std.testing.expectEqual(@as(i64, 1), neighbors_after.total);
    try std.testing.expectEqualStrings("doc-c", neighbors_after.nodes.?[0].key);
}

test "public api e2e restores graph indexes from table backup" {
    const expectSingleGraphResult = struct {
        fn get(parsed: metadata_openapi.QueryResponses, name: []const u8) !indexes_openapi.GraphQueryResult {
            const responses = parsed.responses orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(@as(usize, 1), responses.len);
            const graph_results = responses[0].graph_results orelse return error.TestUnexpectedResult;
            return graph_results.map.get(name) orelse return error.TestUnexpectedResult;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-backup-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-backup-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-backup-out", .{tmp.sub_path});
    defer std.testing.allocator.free(backup_root);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3213,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3213,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "graph docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var graph_index_resp = try client.createTableIndex(base_uri, "docs", "graph_idx", "{\"name\":\"graph_idx\",\"type\":\"graph\"}");
    defer graph_index_resp.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\  "doc-a":{"title":"alpha","_edges":{"graph_idx":{"cites":[{"target":"doc-b","weight":1.5}],"related":[{"target":"doc-c","weight":0.5}]}}},
        \\  "doc-b":{"title":"beta","_edges":{"graph_idx":{"cites":[{"target":"doc-c","weight":2.0}]}}},
        \\  "doc-c":{"title":"gamma"}
        \\}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    const graph_query_body = try test_contract_helpers.encodeGraphNeighborsQueryRequest(
        std.testing.allocator,
        "neighbors",
        "graph_idx",
        &.{"doc-a"},
        &.{ "cites", "related" },
        10,
    );
    defer std.testing.allocator.free(graph_query_body);
    var graph_query_before = try client.fetchQuery(base_uri, "docs", graph_query_body);
    defer graph_query_before.deinit(std.testing.allocator);
    var parsed_graph_before = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, graph_query_before.body, .{});
    defer parsed_graph_before.deinit();
    const neighbors_before = try expectSingleGraphResult.get(parsed_graph_before.value, "neighbors");
    try std.testing.expectEqual(@as(i64, 2), neighbors_before.total);
    try std.testing.expectEqualStrings("doc-b", neighbors_before.nodes.?[0].key);
    try std.testing.expectEqualStrings("doc-c", neighbors_before.nodes.?[1].key);

    var graph_index_before = try client.fetchTableIndex(base_uri, "docs", "graph_idx");
    defer graph_index_before.deinit(std.testing.allocator);
    var parsed_graph_index_before = try parseJsonBody(IndexStatusSummary, std.testing.allocator, graph_index_before.body);
    defer parsed_graph_index_before.deinit();
    try std.testing.expectEqual(@as(?bool, false), parsed_graph_index_before.value.status.backfill_active);
    try std.testing.expectEqual(@as(?u64, 3), parsed_graph_index_before.value.status.node_count);
    try std.testing.expectEqual(@as(?u64, 3), parsed_graph_index_before.value.status.edge_count);

    const backup_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"graph-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(backup_body);
    var backup_resp = try client.fetchBackupTable(base_uri, "docs", backup_body);
    defer backup_resp.deinit(std.testing.allocator);
    var parsed_backup = try parseJsonBody(metadata_openapi.ClusterBackupResponse, std.testing.allocator, backup_resp.body);
    defer parsed_backup.deinit();
    try std.testing.expectEqualStrings("successful", parsed_backup.value.status);

    _ = try client.dropTable(base_uri, "docs");

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchTable(base_uri, "docs"));

    const restore_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"graph-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(restore_body);
    var restore_resp = try client.fetchRestoreTable(base_uri, "docs", restore_body);
    defer restore_resp.deinit(std.testing.allocator);
    var parsed_restore = try parseJsonBody(metadata_openapi.ClusterRestoreResponse, std.testing.allocator, restore_resp.body);
    defer parsed_restore.deinit();
    try std.testing.expectEqualStrings("triggered", parsed_restore.value.status);

    rounds = 0;
    while (rounds < 12) : (rounds += 1) try svc.runRound();

    var restored_table = try client.fetchTable(base_uri, "docs");
    defer restored_table.deinit(std.testing.allocator);
    var parsed_restored_table = try parseJsonBody(metadata_openapi.TableStatus, std.testing.allocator, restored_table.body);
    defer parsed_restored_table.deinit();
    try std.testing.expect(parsed_restored_table.value.indexes.map.get("graph_idx") != null);

    var graph_index_after = try client.fetchTableIndex(base_uri, "docs", "graph_idx");
    defer graph_index_after.deinit(std.testing.allocator);
    var parsed_graph_index_after = try parseJsonBody(IndexStatusSummary, std.testing.allocator, graph_index_after.body);
    defer parsed_graph_index_after.deinit();
    try std.testing.expectEqual(@as(?bool, false), parsed_graph_index_after.value.status.backfill_active);
    try std.testing.expectEqual(@as(?u64, 3), parsed_graph_index_after.value.status.node_count);
    try std.testing.expectEqual(@as(?u64, 3), parsed_graph_index_after.value.status.edge_count);

    var graph_query_after = try client.fetchQuery(base_uri, "docs", graph_query_body);
    defer graph_query_after.deinit(std.testing.allocator);
    var parsed_graph_after = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, graph_query_after.body, .{});
    defer parsed_graph_after.deinit();
    const neighbors_after = try expectSingleGraphResult.get(parsed_graph_after.value, "neighbors");
    try std.testing.expectEqual(@as(i64, 2), neighbors_after.total);
    try std.testing.expectEqualStrings("doc-b", neighbors_after.nodes.?[0].key);
    try std.testing.expectEqualStrings("doc-c", neighbors_after.nodes.?[1].key);
}

test "public api smoke e2e queries across split ranges" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-split-e2e-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-split-e2e-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3112,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3112,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var metadata_admin_server: metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_listener: std_http_listener.StdHttpListener = undefined;
    const metadata_api = try startMetadataAdminListener(
        std.testing.allocator,
        &svc,
        &metadata_admin_server,
        &metadata_admin_listener,
    );
    defer std.testing.allocator.free(metadata_api);
    defer metadata_admin_listener.deinit();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "split docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);
    var created_table = try std.json.parseFromSlice(metadata_openapi.Table, std.testing.allocator, created.body, .{});
    defer created_table.deinit();
    try std.testing.expectEqualStrings("docs", created_table.value.name);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const projected_tables = try svc.listProjectedTables(std.testing.allocator);
    defer svc.freeProjectedTables(std.testing.allocator, projected_tables);
    const projected_ranges = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, projected_ranges);
    try std.testing.expectEqual(@as(usize, 1), projected_tables.len);
    try std.testing.expectEqual(@as(usize, 1), projected_ranges.len);

    const left_group_id = projected_ranges[0].group_id;
    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":39001,\"source_group_id\":{d},\"destination_group_id\":3902,\"split_key\":\"doc:m\"}}", .{
        left_group_id,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(metadata_api, "docs", split_body);

    const projected_splits = try svc.listProjectedSplitTransitions(std.testing.allocator);
    defer svc.freeProjectedSplitTransitions(std.testing.allocator, projected_splits);
    try std.testing.expectEqual(@as(usize, 1), projected_splits.len);
    try std.testing.expectEqual(@as(u64, 39001), projected_splits[0].transition_id);
    try std.testing.expectEqualStrings("doc:m", projected_splits[0].split_key.?);

    var finalized = false;
    rounds = 0;
    while (rounds < 16) : (rounds += 1) {
        try svc.runRound();
        if (try svc.observeSplitTransition(39001)) |observation| {
            if (observation.status.phase == .finalized) {
                finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(finalized);

    rounds = 0;
    while (rounds < 16) : (rounds += 1) {
        try metadata_client.triggerReallocate(metadata_api);
        const updated_ranges = try svc.listProjectedRanges(std.testing.allocator);
        defer svc.freeProjectedRanges(std.testing.allocator, updated_ranges);
        const updated_splits = try svc.listProjectedSplitTransitions(std.testing.allocator);
        defer svc.freeProjectedSplitTransitions(std.testing.allocator, updated_splits);
        if (updated_ranges.len == 2 and updated_splits.len == 0) break;
    }

    const updated_ranges = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, updated_ranges);
    try std.testing.expectEqual(@as(usize, 2), updated_ranges.len);

    const updated_splits = try svc.listProjectedSplitTransitions(std.testing.allocator);
    defer svc.freeProjectedSplitTransitions(std.testing.allocator, updated_splits);
    try std.testing.expectEqual(@as(usize, 0), updated_splits.len);

    const right_db_path = try metadata_mod.groupDbPathFromReplicaRoot(std.testing.allocator, replica_root, 3902);
    defer std.testing.allocator.free(right_db_path);
    var right_db = try db_mod.DB.open(std.testing.allocator, right_db_path, .{});
    defer right_db.close();
    try std.testing.expect(right_db.core.index_manager.textIndex("full_text_index_v0") != null);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"hello left"},"doc:z":{"title":"zeta","body":"hello right"}}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);
    var parsed_batch = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.testing.allocator, batch.body, .{});
    defer parsed_batch.deinit();
    try std.testing.expectEqual(@as(i64, 2), parsed_batch.value.inserted.?);

    const query_body = try test_contract_helpers.encodeMatchQueryRequest(std.testing.allocator, "body", "hello", &.{ "title", "body" }, 10);
    defer std.testing.allocator.free(query_body);
    var query = try client.fetchQuery(base_uri, "docs", query_body);
    defer query.deinit(std.testing.allocator);
    var query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, query.body, .{});
    defer query_responses.deinit();

    try std.testing.expectEqual(@as(usize, 1), query_responses.value.responses.?.len);
    const query_result = query_responses.value.responses.?[0];
    try std.testing.expectEqualStrings("docs", query_result.table.?);
    try std.testing.expectEqual(@as(i64, 2), query_result.hits.?.total.?);
    try std.testing.expectEqualStrings("doc:a", query_result.hits.?.hits.?[0]._id);
    try std.testing.expectEqualStrings("doc:z", query_result.hits.?.hits.?[1]._id);

    var lookup = try client.fetchLookup(base_uri, "docs", "doc:z", null);
    defer lookup.deinit(std.testing.allocator);
    var parsed_lookup = try parseJsonBody(LookupTitle, std.testing.allocator, lookup.body);
    defer parsed_lookup.deinit();
    try std.testing.expectEqualStrings("zeta", parsed_lookup.value.title);

    const delete_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"deletes\":[\"doc:z\"]}");
    defer std.testing.allocator.free(delete_body);
    var deleted = try client.fetchBatch(base_uri, "docs", delete_body);
    defer deleted.deinit(std.testing.allocator);
    var parsed_deleted = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.testing.allocator, deleted.body, .{});
    defer parsed_deleted.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed_deleted.value.deleted.?);

    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(base_uri, "docs", "doc:z", null));

    const deleted_query_body = try test_contract_helpers.encodeMatchQueryRequest(std.testing.allocator, "body", "right", &.{ "title", "body" }, 10);
    defer std.testing.allocator.free(deleted_query_body);
    var deleted_query = try client.fetchQuery(base_uri, "docs", deleted_query_body);
    defer deleted_query.deinit(std.testing.allocator);
    var deleted_query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, deleted_query.body, .{});
    defer deleted_query_responses.deinit();
    const deleted_query_result = deleted_query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 0), deleted_query_result.hits.?.total.?);
}

test "public api split e2e uses distributed global text stats for bm25 and significant_terms" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-split-global-stats-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-split-global-stats-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3113,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3113,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var metadata_admin_server: metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_listener: std_http_listener.StdHttpListener = undefined;
    const metadata_api = try startMetadataAdminListener(
        std.testing.allocator,
        &svc,
        &metadata_admin_server,
        &metadata_admin_listener,
    );
    defer std.testing.allocator.free(metadata_api);
    defer metadata_admin_listener.deinit();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "split stats docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const projected_ranges = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, projected_ranges);
    try std.testing.expectEqual(@as(usize, 1), projected_ranges.len);

    const left_group_id = projected_ranges[0].group_id;
    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":39011,\"source_group_id\":{d},\"destination_group_id\":3912,\"split_key\":\"doc:m\"}}", .{
        left_group_id,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(metadata_api, "docs", split_body);

    var finalized = false;
    rounds = 0;
    while (rounds < 16) : (rounds += 1) {
        try svc.runRound();
        if (try svc.observeSplitTransition(39011)) |observation| {
            if (observation.status.phase == .finalized) {
                finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(finalized);

    rounds = 0;
    while (rounds < 16) : (rounds += 1) {
        try metadata_client.triggerReallocate(metadata_api);
        const updated_ranges = try svc.listProjectedRanges(std.testing.allocator);
        defer svc.freeProjectedRanges(std.testing.allocator, updated_ranges);
        const updated_splits = try svc.listProjectedSplitTransitions(std.testing.allocator);
        defer svc.freeProjectedSplitTransitions(std.testing.allocator, updated_splits);
        if (updated_ranges.len == 2 and updated_splits.len == 0) break;
    }

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{
        \\"doc:a":{"title":"left-heavy","body":"alpha alpha alpha alpha alpha rareleft"},
        \\"doc:b":{"title":"left-1","body":"alpha"},
        \\"doc:c":{"title":"left-2","body":"alpha"},
        \\"doc:d":{"title":"left-3","body":"alpha"},
        \\"doc:e":{"title":"left-4","body":"alpha"},
        \\"doc:z":{"title":"right","body":"alpha rareright"}}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);
    var parsed_batch = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.testing.allocator, batch.body, .{});
    defer parsed_batch.deinit();
    try std.testing.expectEqual(@as(i64, 6), parsed_batch.value.inserted.?);

    const bm25_query_body = try test_contract_helpers.encodeMatchQueryRequest(std.testing.allocator, "body", "alpha", &.{ "title", "body" }, 6);
    defer std.testing.allocator.free(bm25_query_body);
    var bm25_query = try client.fetchQuery(base_uri, "docs", bm25_query_body);
    defer bm25_query.deinit(std.testing.allocator);
    var bm25_query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, bm25_query.body, .{});
    defer bm25_query_responses.deinit();

    try std.testing.expectEqual(@as(usize, 1), bm25_query_responses.value.responses.?.len);
    const bm25_result = bm25_query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 6), bm25_result.hits.?.total.?);
    try std.testing.expectEqualStrings("doc:a", bm25_result.hits.?.hits.?[0]._id);

    const significant_terms_body = try std.testing.allocator.dupe(u8,
        \\{"full_text_search":{"query":"body:rareleft OR body:rareright"},"fields":["title","body"],"limit":6,"aggregations":{"sig_body":{"type":"significant_terms","field":"body","size":4,"background_filter":{"match":{"body":"alpha"}}}}}
    );
    defer std.testing.allocator.free(significant_terms_body);
    var significant_terms_query = try client.fetchQuery(base_uri, "docs", significant_terms_body);
    defer significant_terms_query.deinit(std.testing.allocator);
    const SignificantTermsBucket = struct {
        key: []const u8,
        bg_count: i64,
    };
    const SignificantTermsMetadata = struct {
        bg_doc_count: i64,
    };
    const SignificantTermsAggregation = struct {
        metadata: SignificantTermsMetadata,
        buckets: []const SignificantTermsBucket,
    };
    const SignificantTermsQueryResult = struct {
        aggregations: std.json.ArrayHashMap(SignificantTermsAggregation),
    };
    const SignificantTermsQueryResponses = struct {
        responses: []const SignificantTermsQueryResult,
    };
    var significant_terms_responses = try std.json.parseFromSlice(SignificantTermsQueryResponses, std.testing.allocator, significant_terms_query.body, .{});
    defer significant_terms_responses.deinit();

    try std.testing.expectEqual(@as(usize, 1), significant_terms_responses.value.responses.len);
    const sig_body = significant_terms_responses.value.responses[0].aggregations.map.get("sig_body").?;
    try std.testing.expectEqual(@as(i64, 6), sig_body.metadata.bg_doc_count);

    const buckets = sig_body.buckets;
    var saw_rareleft = false;
    var saw_rareright = false;
    for (buckets) |bucket| {
        const key = bucket.key;
        const bg_count = bucket.bg_count;
        if (std.mem.eql(u8, key, "rareleft")) {
            saw_rareleft = true;
            try std.testing.expectEqual(@as(i64, 1), bg_count);
        } else if (std.mem.eql(u8, key, "rareright")) {
            saw_rareright = true;
            try std.testing.expectEqual(@as(i64, 1), bg_count);
        }
    }
    try std.testing.expect(saw_rareleft);
    try std.testing.expect(saw_rareright);
}

test "public api e2e serves cluster backup list and restore routes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/api-cluster-backup-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/api-cluster-backup-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/api-cluster-backup-out", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(backup_root);

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 2114,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 2114,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_docs_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "docs table");
    defer std.testing.allocator.free(create_docs_body);
    var created_docs = try client.createTable(base_uri, "docs", create_docs_body);
    defer created_docs.deinit(std.testing.allocator);

    const create_logs_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "logs table");
    defer std.testing.allocator.free(create_logs_body);
    var created_logs = try client.createTable(base_uri, "logs", create_logs_body);
    defer created_logs.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const docs_batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\"}}}");
    defer std.testing.allocator.free(docs_batch_body);
    var docs_batch = try client.fetchBatch(base_uri, "docs", docs_batch_body);
    defer docs_batch.deinit(std.testing.allocator);

    const logs_batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"inserts\":{\"log:a\":{\"title\":\"entry\"}}}");
    defer std.testing.allocator.free(logs_batch_body);
    var logs_batch = try client.fetchBatch(base_uri, "logs", logs_batch_body);
    defer logs_batch.deinit(std.testing.allocator);

    const cluster_backup_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"cluster-snap\",\"location\":\"file://{s}\",\"table_names\":[\"docs\",\"logs\"]}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(cluster_backup_body);
    var backup_resp = try client.fetchClusterBackup(base_uri, cluster_backup_body);
    defer backup_resp.deinit(std.testing.allocator);
    var parsed_backup = try std.json.parseFromSlice(metadata_openapi.ClusterBackupResponse, std.testing.allocator, backup_resp.body, .{});
    defer parsed_backup.deinit();
    try std.testing.expectEqualStrings("cluster-snap", parsed_backup.value.backup_id);
    try std.testing.expectEqualStrings("successful", parsed_backup.value.status);
    try std.testing.expectEqual(@as(usize, 2), parsed_backup.value.tables.len);
    for (parsed_backup.value.tables) |table_status| {
        try std.testing.expectEqualStrings("successful", table_status.status);
        try std.testing.expect(table_status.@"error" == null);
    }

    const backups_location = try std.fmt.allocPrint(std.testing.allocator, "file://{s}", .{backup_root});
    defer std.testing.allocator.free(backups_location);
    var backups_resp = try client.fetchListBackups(base_uri, backups_location);
    defer backups_resp.deinit(std.testing.allocator);
    var parsed_backups = try std.json.parseFromSlice(metadata_openapi.BackupListResponse, std.testing.allocator, backups_resp.body, .{});
    defer parsed_backups.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_backups.value.backups.len);
    try std.testing.expectEqualStrings("cluster-snap", parsed_backups.value.backups[0].backup_id);
    try std.testing.expectEqual(@as(usize, 2), parsed_backups.value.backups[0].tables.len);

    const fail_restore_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"cluster-snap\",\"location\":\"file://{s}\",\"restore_mode\":\"fail_if_exists\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(fail_restore_body);
    const fail_restore_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/restore");
    defer std.testing.allocator.free(fail_restore_uri);
    var fail_restore_resp = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = fail_restore_uri,
        .content_type = "application/json",
        .body = fail_restore_body,
    });
    defer fail_restore_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), fail_restore_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, fail_restore_resp.body, "table already exists") != null);

    const docs_mutation_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"inserts\":{\"doc:a\":{\"title\":\"mutated\"}}}");
    defer std.testing.allocator.free(docs_mutation_body);
    var docs_mutation = try client.fetchBatch(base_uri, "docs", docs_mutation_body);
    defer docs_mutation.deinit(std.testing.allocator);

    const logs_mutation_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"inserts\":{\"log:a\":{\"title\":\"changed\"}}}");
    defer std.testing.allocator.free(logs_mutation_body);
    var logs_mutation = try client.fetchBatch(base_uri, "logs", logs_mutation_body);
    defer logs_mutation.deinit(std.testing.allocator);

    const skip_restore_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"cluster-snap\",\"location\":\"file://{s}\",\"restore_mode\":\"skip_if_exists\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(skip_restore_body);
    var skip_restore_resp = try client.fetchClusterRestore(base_uri, skip_restore_body);
    defer skip_restore_resp.deinit(std.testing.allocator);
    var parsed_skip_restore = try std.json.parseFromSlice(metadata_openapi.ClusterRestoreResponse, std.testing.allocator, skip_restore_resp.body, .{});
    defer parsed_skip_restore.deinit();
    try std.testing.expectEqualStrings("triggered", parsed_skip_restore.value.status);
    try std.testing.expectEqual(@as(usize, 2), parsed_skip_restore.value.tables.len);
    for (parsed_skip_restore.value.tables) |table_status| {
        try std.testing.expectEqualStrings("skipped", table_status.status);
        try std.testing.expect(table_status.@"error" == null);
    }

    var docs_lookup_after_skip = try client.fetchLookup(base_uri, "docs", "doc:a", null);
    defer docs_lookup_after_skip.deinit(std.testing.allocator);
    var parsed_docs_lookup_after_skip = try parseJsonBody(LookupTitle, std.testing.allocator, docs_lookup_after_skip.body);
    defer parsed_docs_lookup_after_skip.deinit();
    try std.testing.expectEqualStrings("mutated", parsed_docs_lookup_after_skip.value.title);

    var logs_lookup_after_skip = try client.fetchLookup(base_uri, "logs", "log:a", null);
    defer logs_lookup_after_skip.deinit(std.testing.allocator);
    var parsed_logs_lookup_after_skip = try parseJsonBody(LookupTitle, std.testing.allocator, logs_lookup_after_skip.body);
    defer parsed_logs_lookup_after_skip.deinit();
    try std.testing.expectEqualStrings("changed", parsed_logs_lookup_after_skip.value.title);

    _ = try client.dropTable(base_uri, "docs");
    _ = try client.dropTable(base_uri, "logs");

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchTable(base_uri, "docs"));
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchTable(base_uri, "logs"));

    const cluster_restore_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"cluster-snap\",\"location\":\"file://{s}\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(cluster_restore_body);
    var restore_resp = try client.fetchClusterRestore(base_uri, cluster_restore_body);
    defer restore_resp.deinit(std.testing.allocator);
    var parsed_restore = try std.json.parseFromSlice(metadata_openapi.ClusterRestoreResponse, std.testing.allocator, restore_resp.body, .{});
    defer parsed_restore.deinit();
    try std.testing.expectEqualStrings("triggered", parsed_restore.value.status);
    try std.testing.expectEqual(@as(usize, 2), parsed_restore.value.tables.len);
    for (parsed_restore.value.tables) |table_status| {
        try std.testing.expectEqualStrings("triggered", table_status.status);
        try std.testing.expect(table_status.@"error" == null);
    }

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    var docs_lookup = try client.fetchLookup(base_uri, "docs", "doc:a", null);
    defer docs_lookup.deinit(std.testing.allocator);
    var parsed_docs_lookup = try parseJsonBody(LookupTitle, std.testing.allocator, docs_lookup.body);
    defer parsed_docs_lookup.deinit();
    try std.testing.expectEqualStrings("alpha", parsed_docs_lookup.value.title);

    var logs_lookup = try client.fetchLookup(base_uri, "logs", "log:a", null);
    defer logs_lookup.deinit(std.testing.allocator);
    var parsed_logs_lookup = try parseJsonBody(LookupTitle, std.testing.allocator, logs_lookup.body);
    defer parsed_logs_lookup.deinit();
    try std.testing.expectEqualStrings("entry", parsed_logs_lookup.value.title);

    const docs_overwrite_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"inserts\":{\"doc:a\":{\"title\":\"overwrite-me\"}}}");
    defer std.testing.allocator.free(docs_overwrite_body);
    var docs_overwrite = try client.fetchBatch(base_uri, "docs", docs_overwrite_body);
    defer docs_overwrite.deinit(std.testing.allocator);

    const logs_overwrite_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"inserts\":{\"log:a\":{\"title\":\"overwrite-log\"}}}");
    defer std.testing.allocator.free(logs_overwrite_body);
    var logs_overwrite = try client.fetchBatch(base_uri, "logs", logs_overwrite_body);
    defer logs_overwrite.deinit(std.testing.allocator);

    const overwrite_restore_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"cluster-snap\",\"location\":\"file://{s}\",\"restore_mode\":\"overwrite\"}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(overwrite_restore_body);
    var overwrite_restore_resp = try client.fetchClusterRestore(base_uri, overwrite_restore_body);
    defer overwrite_restore_resp.deinit(std.testing.allocator);
    var parsed_overwrite_restore = try std.json.parseFromSlice(metadata_openapi.ClusterRestoreResponse, std.testing.allocator, overwrite_restore_resp.body, .{});
    defer parsed_overwrite_restore.deinit();
    try std.testing.expectEqualStrings("triggered", parsed_overwrite_restore.value.status);
    try std.testing.expectEqual(@as(usize, 2), parsed_overwrite_restore.value.tables.len);
    for (parsed_overwrite_restore.value.tables) |table_status| {
        try std.testing.expectEqualStrings("triggered", table_status.status);
        try std.testing.expect(table_status.@"error" == null);
    }

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    var docs_lookup_after_overwrite = try client.fetchLookup(base_uri, "docs", "doc:a", null);
    defer docs_lookup_after_overwrite.deinit(std.testing.allocator);
    var parsed_docs_lookup_after_overwrite = try parseJsonBody(LookupTitle, std.testing.allocator, docs_lookup_after_overwrite.body);
    defer parsed_docs_lookup_after_overwrite.deinit();
    try std.testing.expectEqualStrings("alpha", parsed_docs_lookup_after_overwrite.value.title);

    var logs_lookup_after_overwrite = try client.fetchLookup(base_uri, "logs", "log:a", null);
    defer logs_lookup_after_overwrite.deinit(std.testing.allocator);
    var parsed_logs_lookup_after_overwrite = try parseJsonBody(LookupTitle, std.testing.allocator, logs_lookup_after_overwrite.body);
    defer parsed_logs_lookup_after_overwrite.deinit();
    try std.testing.expectEqualStrings("entry", parsed_logs_lookup_after_overwrite.value.title);
}

test "public api e2e reports partial cluster backup and restore statuses" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/api-cluster-partial-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/api-cluster-partial-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/api-cluster-partial-out", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(backup_root);

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 2115,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 2115,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_docs_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "docs table");
    defer std.testing.allocator.free(create_docs_body);
    var created_docs = try client.createTable(base_uri, "docs", create_docs_body);
    defer created_docs.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const docs_batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\"}}}");
    defer std.testing.allocator.free(docs_batch_body);
    var docs_batch = try client.fetchBatch(base_uri, "docs", docs_batch_body);
    defer docs_batch.deinit(std.testing.allocator);

    const partial_backup_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"cluster-partial\",\"location\":\"file://{s}\",\"table_names\":[\"docs\",\"missing\"]}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(partial_backup_body);
    var partial_backup_resp = try client.fetchClusterBackup(base_uri, partial_backup_body);
    defer partial_backup_resp.deinit(std.testing.allocator);
    var parsed_partial_backup = try std.json.parseFromSlice(metadata_openapi.ClusterBackupResponse, std.testing.allocator, partial_backup_resp.body, .{});
    defer parsed_partial_backup.deinit();
    try std.testing.expectEqualStrings("partial", parsed_partial_backup.value.status);
    try std.testing.expectEqual(@as(usize, 2), parsed_partial_backup.value.tables.len);

    var saw_docs_backup = false;
    var saw_missing_backup = false;
    for (parsed_partial_backup.value.tables) |table_status| {
        if (std.mem.eql(u8, table_status.name, "docs")) {
            saw_docs_backup = true;
            try std.testing.expectEqualStrings("completed", table_status.status);
            try std.testing.expect(table_status.@"error" == null);
        } else if (std.mem.eql(u8, table_status.name, "missing")) {
            saw_missing_backup = true;
            try std.testing.expectEqualStrings("failed", table_status.status);
            try std.testing.expectEqualStrings("not found", table_status.@"error".?);
        }
    }
    try std.testing.expect(saw_docs_backup);
    try std.testing.expect(saw_missing_backup);

    const backups_location = try std.fmt.allocPrint(std.testing.allocator, "file://{s}", .{backup_root});
    defer std.testing.allocator.free(backups_location);
    var backups_resp = try client.fetchListBackups(base_uri, backups_location);
    defer backups_resp.deinit(std.testing.allocator);
    var parsed_backups = try std.json.parseFromSlice(metadata_openapi.BackupListResponse, std.testing.allocator, backups_resp.body, .{});
    defer parsed_backups.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_backups.value.backups.len);
    try std.testing.expectEqualStrings("cluster-partial", parsed_backups.value.backups[0].backup_id);
    try std.testing.expectEqual(@as(usize, 1), parsed_backups.value.backups[0].tables.len);
    try std.testing.expectEqualStrings("docs", parsed_backups.value.backups[0].tables[0]);

    _ = try client.dropTable(base_uri, "docs");
    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchTable(base_uri, "docs"));

    const partial_restore_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"cluster-partial\",\"location\":\"file://{s}\",\"table_names\":[\"docs\",\"missing\"]}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(partial_restore_body);
    var partial_restore_resp = try client.fetchClusterRestore(base_uri, partial_restore_body);
    defer partial_restore_resp.deinit(std.testing.allocator);
    var parsed_partial_restore = try std.json.parseFromSlice(metadata_openapi.ClusterRestoreResponse, std.testing.allocator, partial_restore_resp.body, .{});
    defer parsed_partial_restore.deinit();
    try std.testing.expectEqualStrings("partial", parsed_partial_restore.value.status);
    try std.testing.expectEqual(@as(usize, 2), parsed_partial_restore.value.tables.len);

    var saw_docs_restore = false;
    var saw_missing_restore = false;
    for (parsed_partial_restore.value.tables) |table_status| {
        if (std.mem.eql(u8, table_status.name, "docs")) {
            saw_docs_restore = true;
            try std.testing.expectEqualStrings("triggered", table_status.status);
            try std.testing.expect(table_status.@"error" == null);
        } else if (std.mem.eql(u8, table_status.name, "missing")) {
            saw_missing_restore = true;
            try std.testing.expectEqualStrings("failed", table_status.status);
            try std.testing.expectEqualStrings("backup does not include table", table_status.@"error".?);
        }
    }
    try std.testing.expect(saw_docs_restore);
    try std.testing.expect(saw_missing_restore);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    var docs_lookup = try client.fetchLookup(base_uri, "docs", "doc:a", null);
    defer docs_lookup.deinit(std.testing.allocator);
    var parsed_docs_lookup = try parseJsonBody(LookupTitle, std.testing.allocator, docs_lookup.body);
    defer parsed_docs_lookup.deinit();
    try std.testing.expectEqualStrings("alpha", parsed_docs_lookup.value.title);
}

test "public api e2e reports unsupported multi-range tables in cluster backup" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/api-cluster-multirange-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/api-cluster-multirange-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/api-cluster-multirange-out", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(backup_root);

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 2116,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 2116,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var metadata_admin_server: metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_listener: std_http_listener.StdHttpListener = undefined;
    const metadata_api = try startMetadataAdminListener(
        std.testing.allocator,
        &svc,
        &metadata_admin_server,
        &metadata_admin_listener,
    );
    defer std.testing.allocator.free(metadata_api);
    defer metadata_admin_listener.deinit();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.testing.allocator, executor.executor());

    const create_docs_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "split docs");
    defer std.testing.allocator.free(create_docs_body);
    var created_docs = try client.createTable(base_uri, "docs", create_docs_body);
    defer created_docs.deinit(std.testing.allocator);

    const create_logs_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "logs table");
    defer std.testing.allocator.free(create_logs_body);
    var created_logs = try client.createTable(base_uri, "logs", create_logs_body);
    defer created_logs.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const projected_tables = try svc.listProjectedTables(std.testing.allocator);
    defer svc.freeProjectedTables(std.testing.allocator, projected_tables);
    var docs_table_id: ?u64 = null;
    for (projected_tables) |table| {
        if (std.mem.eql(u8, table.name, "docs")) {
            docs_table_id = table.table_id;
            break;
        }
    }
    try std.testing.expect(docs_table_id != null);

    const projected_ranges = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, projected_ranges);
    var docs_group_id: ?u64 = null;
    for (projected_ranges) |range| {
        if (range.table_id == docs_table_id.?) {
            docs_group_id = range.group_id;
            break;
        }
    }
    try std.testing.expect(docs_group_id != null);

    const split_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"transition_id\":49001,\"source_group_id\":{d},\"destination_group_id\":4902,\"split_key\":\"doc:m\"}}",
        .{docs_group_id.?},
    );
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(metadata_api, "docs", split_body);

    var split_finalized = false;
    rounds = 0;
    while (rounds < 16) : (rounds += 1) {
        try svc.runRound();
        if (try svc.observeSplitTransition(49001)) |observation| {
            if (observation.status.phase == .finalized) {
                split_finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(split_finalized);

    rounds = 0;
    while (rounds < 16) : (rounds += 1) {
        try metadata_client.triggerReallocate(metadata_api);
        const updated_ranges = try svc.listProjectedRanges(std.testing.allocator);
        defer svc.freeProjectedRanges(std.testing.allocator, updated_ranges);
        const updated_splits = try svc.listProjectedSplitTransitions(std.testing.allocator);
        defer svc.freeProjectedSplitTransitions(std.testing.allocator, updated_splits);
        if (updated_ranges.len >= 3 and updated_splits.len == 0) break;
    }

    const updated_ranges = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, updated_ranges);
    var docs_range_count: usize = 0;
    for (updated_ranges) |range| {
        if (range.table_id == docs_table_id.?) docs_range_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), docs_range_count);

    const logs_batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"inserts\":{\"log:a\":{\"title\":\"entry\"}}}");
    defer std.testing.allocator.free(logs_batch_body);
    var logs_batch = try client.fetchBatch(base_uri, "logs", logs_batch_body);
    defer logs_batch.deinit(std.testing.allocator);

    const partial_backup_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"cluster-multirange\",\"location\":\"file://{s}\",\"table_names\":[\"docs\",\"logs\"]}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(partial_backup_body);
    var partial_backup_resp = try client.fetchClusterBackup(base_uri, partial_backup_body);
    defer partial_backup_resp.deinit(std.testing.allocator);
    var parsed_partial_backup = try std.json.parseFromSlice(metadata_openapi.ClusterBackupResponse, std.testing.allocator, partial_backup_resp.body, .{});
    defer parsed_partial_backup.deinit();
    try std.testing.expectEqualStrings("partial", parsed_partial_backup.value.status);
    try std.testing.expectEqual(@as(usize, 2), parsed_partial_backup.value.tables.len);

    var saw_docs_backup = false;
    var saw_logs_backup = false;
    for (parsed_partial_backup.value.tables) |table_status| {
        if (std.mem.eql(u8, table_status.name, "docs")) {
            saw_docs_backup = true;
            try std.testing.expectEqualStrings("failed", table_status.status);
            try std.testing.expectEqualStrings("backup does not support multi-range tables", table_status.@"error".?);
        } else if (std.mem.eql(u8, table_status.name, "logs")) {
            saw_logs_backup = true;
            try std.testing.expectEqualStrings("completed", table_status.status);
            try std.testing.expect(table_status.@"error" == null);
        }
    }
    try std.testing.expect(saw_docs_backup);
    try std.testing.expect(saw_logs_backup);

    const backups_location = try std.fmt.allocPrint(std.testing.allocator, "file://{s}", .{backup_root});
    defer std.testing.allocator.free(backups_location);
    var backups_resp = try client.fetchListBackups(base_uri, backups_location);
    defer backups_resp.deinit(std.testing.allocator);
    var parsed_backups = try std.json.parseFromSlice(metadata_openapi.BackupListResponse, std.testing.allocator, backups_resp.body, .{});
    defer parsed_backups.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_backups.value.backups.len);
    try std.testing.expectEqualStrings("cluster-multirange", parsed_backups.value.backups[0].backup_id);
    try std.testing.expectEqual(@as(usize, 1), parsed_backups.value.backups[0].tables.len);
    try std.testing.expectEqualStrings("logs", parsed_backups.value.backups[0].tables[0]);

    _ = try client.dropTable(base_uri, "docs");
    _ = try client.dropTable(base_uri, "logs");
    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const partial_restore_body = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"backup_id\":\"cluster-multirange\",\"location\":\"file://{s}\",\"table_names\":[\"docs\",\"logs\"]}}",
        .{backup_root},
    );
    defer std.testing.allocator.free(partial_restore_body);
    var partial_restore_resp = try client.fetchClusterRestore(base_uri, partial_restore_body);
    defer partial_restore_resp.deinit(std.testing.allocator);
    var parsed_partial_restore = try std.json.parseFromSlice(metadata_openapi.ClusterRestoreResponse, std.testing.allocator, partial_restore_resp.body, .{});
    defer parsed_partial_restore.deinit();
    try std.testing.expectEqualStrings("partial", parsed_partial_restore.value.status);
    try std.testing.expectEqual(@as(usize, 2), parsed_partial_restore.value.tables.len);

    var saw_docs_restore = false;
    var saw_logs_restore = false;
    for (parsed_partial_restore.value.tables) |table_status| {
        if (std.mem.eql(u8, table_status.name, "docs")) {
            saw_docs_restore = true;
            try std.testing.expectEqualStrings("failed", table_status.status);
            try std.testing.expectEqualStrings("backup does not include table", table_status.@"error".?);
        } else if (std.mem.eql(u8, table_status.name, "logs")) {
            saw_logs_restore = true;
            try std.testing.expectEqualStrings("triggered", table_status.status);
            try std.testing.expect(table_status.@"error" == null);
        }
    }
    try std.testing.expect(saw_docs_restore);
    try std.testing.expect(saw_logs_restore);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    var logs_lookup = try client.fetchLookup(base_uri, "logs", "log:a", null);
    defer logs_lookup.deinit(std.testing.allocator);
    var parsed_logs_lookup = try parseJsonBody(LookupTitle, std.testing.allocator, logs_lookup.body);
    defer parsed_logs_lookup.deinit();
    try std.testing.expectEqualStrings("entry", parsed_logs_lookup.value.title);
}

test "public api smoke e2e commits transaction across split ranges" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-split-txn-e2e-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-split-txn-e2e-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3113,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3113,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var metadata_admin_server: metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_listener: std_http_listener.StdHttpListener = undefined;
    const metadata_api = try startMetadataAdminListener(
        std.testing.allocator,
        &svc,
        &metadata_admin_server,
        &metadata_admin_listener,
    );
    defer std.testing.allocator.free(metadata_api);
    defer metadata_admin_listener.deinit();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "txn docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const projected_ranges = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, projected_ranges);
    try std.testing.expectEqual(@as(usize, 1), projected_ranges.len);

    const left_group_id = projected_ranges[0].group_id;
    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":39101,\"source_group_id\":{d},\"destination_group_id\":3912,\"split_key\":\"doc:m\"}}", .{
        left_group_id,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(metadata_api, "docs", split_body);

    var finalized = false;
    rounds = 0;
    while (rounds < 16) : (rounds += 1) {
        try svc.runRound();
        if (try svc.observeSplitTransition(39101)) |observation| {
            if (observation.status.phase == .finalized) {
                finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(finalized);

    rounds = 0;
    while (rounds < 16) : (rounds += 1) {
        try metadata_client.triggerReallocate(metadata_api);
        const updated_ranges = try svc.listProjectedRanges(std.testing.allocator);
        defer svc.freeProjectedRanges(std.testing.allocator, updated_ranges);
        if (updated_ranges.len == 2) break;
    }

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"left"},"doc:z":{"title":"zeta","body":"right"}}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);

    var left_lookup = try client.fetchLookup(base_uri, "docs", "doc:a", null);
    defer left_lookup.deinit(std.testing.allocator);
    var right_lookup = try client.fetchLookup(base_uri, "docs", "doc:z", null);
    defer right_lookup.deinit(std.testing.allocator);

    const commit_batch = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"alpha txn","body":"left committed"},"doc:z":{"title":"zeta txn","body":"right committed"}}}
    );
    defer std.testing.allocator.free(commit_batch);
    const commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.testing.allocator,
        &.{
            .{ .table_name = "docs", .key = "doc:a", .version = left_lookup.version.? },
            .{ .table_name = "docs", .key = "doc:z", .version = right_lookup.version.? },
        },
        &.{.{ .table_name = "docs", .batch_json = commit_batch }},
        "write",
    );
    defer std.testing.allocator.free(commit_body);

    var committed = try client.fetchTransactionCommit(base_uri, commit_body);
    defer committed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), committed.status);
    var parsed_commit = try std.json.parseFromSlice(transactions_api.CommitResponse, std.testing.allocator, committed.body, .{});
    defer parsed_commit.deinit();
    try std.testing.expectEqualStrings("committed", parsed_commit.value.status);

    var updated_left = try client.fetchLookup(base_uri, "docs", "doc:a", null);
    defer updated_left.deinit(std.testing.allocator);
    var parsed_updated_left = try parseJsonBody(LookupTitle, std.testing.allocator, updated_left.body);
    defer parsed_updated_left.deinit();
    try std.testing.expectEqualStrings("alpha txn", parsed_updated_left.value.title);
    var updated_right = try client.fetchLookup(base_uri, "docs", "doc:z", null);
    defer updated_right.deinit(std.testing.allocator);
    var parsed_updated_right = try parseJsonBody(LookupTitle, std.testing.allocator, updated_right.body);
    defer parsed_updated_right.deinit();
    try std.testing.expectEqualStrings("zeta txn", parsed_updated_right.value.title);

    const stale_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.testing.allocator,
        &.{
            .{ .table_name = "docs", .key = "doc:a", .version = left_lookup.version.? },
            .{ .table_name = "docs", .key = "doc:z", .version = right_lookup.version.? },
        },
        &.{.{ .table_name = "docs", .batch_json = commit_batch }},
        null,
    );
    defer std.testing.allocator.free(stale_body);

    var aborted = try client.fetchTransactionCommit(base_uri, stale_body);
    defer aborted.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 409), aborted.status);
    var parsed_abort = try std.json.parseFromSlice(transactions_api.CommitResponse, std.testing.allocator, aborted.body, .{});
    defer parsed_abort.deinit();
    try std.testing.expectEqualStrings("aborted", parsed_abort.value.status);
    try std.testing.expect(parsed_abort.value.conflict != null);
}

test "public api smoke e2e commits transactions across two tables atomically" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-cross-table-txn-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-cross-table-txn-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 3114,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 3114,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());

    const create_users_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "users");
    defer std.testing.allocator.free(create_users_body);
    var created_users = try client.createTable(base_uri, "users", create_users_body);
    defer created_users.deinit(std.testing.allocator);

    const create_orders_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "orders");
    defer std.testing.allocator.free(create_orders_body);
    var created_orders = try client.createTable(base_uri, "orders", create_orders_body);
    defer created_orders.deinit(std.testing.allocator);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const seed_orders_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"order:old":{"user":"user:0","item":"legacy","qty":1}}}
    );
    defer std.testing.allocator.free(seed_orders_body);
    var seeded_orders = try client.fetchBatch(base_uri, "orders", seed_orders_body);
    defer seeded_orders.deinit(std.testing.allocator);

    const commit_insert_a = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"user:1":{"name":"Alice","email":"alice@example.com"},"user:2":{"name":"Bob","email":"bob@example.com"}}}
    );
    defer std.testing.allocator.free(commit_insert_a);
    const commit_insert_b = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"order:1":{"user":"user:1","item":"widget","qty":5},"order:2":{"user":"user:2","item":"gadget","qty":3}}}
    );
    defer std.testing.allocator.free(commit_insert_b);
    const cross_table_commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.testing.allocator,
        &.{},
        &.{
            .{ .table_name = "users", .batch_json = commit_insert_a },
            .{ .table_name = "orders", .batch_json = commit_insert_b },
        },
        "write",
    );
    defer std.testing.allocator.free(cross_table_commit_body);

    var committed = try client.fetchTransactionCommit(base_uri, cross_table_commit_body);
    defer committed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), committed.status);
    var parsed_commit = try std.json.parseFromSlice(transactions_api.CommitResponse, std.testing.allocator, committed.body, .{});
    defer parsed_commit.deinit();
    try std.testing.expectEqualStrings("committed", parsed_commit.value.status);

    var user_one = try client.fetchLookup(base_uri, "users", "user:1", null);
    defer user_one.deinit(std.testing.allocator);
    var parsed_user_one = try parseJsonBody(UserName, std.testing.allocator, user_one.body);
    defer parsed_user_one.deinit();
    try std.testing.expectEqualStrings("Alice", parsed_user_one.value.name);
    var user_two = try client.fetchLookup(base_uri, "users", "user:2", null);
    defer user_two.deinit(std.testing.allocator);
    var parsed_user_two = try parseJsonBody(UserName, std.testing.allocator, user_two.body);
    defer parsed_user_two.deinit();
    try std.testing.expectEqualStrings("Bob", parsed_user_two.value.name);
    var order_one = try client.fetchLookup(base_uri, "orders", "order:1", null);
    defer order_one.deinit(std.testing.allocator);
    var parsed_order_one = try parseJsonBody(OrderItem, std.testing.allocator, order_one.body);
    defer parsed_order_one.deinit();
    try std.testing.expectEqualStrings("widget", parsed_order_one.value.item);
    var order_two = try client.fetchLookup(base_uri, "orders", "order:2", null);
    defer order_two.deinit(std.testing.allocator);
    var parsed_order_two = try parseJsonBody(OrderItem, std.testing.allocator, order_two.body);
    defer parsed_order_two.deinit();
    try std.testing.expectEqualStrings("gadget", parsed_order_two.value.item);

    const mixed_insert_a = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"user:3":{"name":"Charlie"}}}
    );
    defer std.testing.allocator.free(mixed_insert_a);
    const mixed_delete_b = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"deletes":["order:old"]}
    );
    defer std.testing.allocator.free(mixed_delete_b);
    const mixed_commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.testing.allocator,
        &.{},
        &.{
            .{ .table_name = "users", .batch_json = mixed_insert_a },
            .{ .table_name = "orders", .batch_json = mixed_delete_b },
        },
        "write",
    );
    defer std.testing.allocator.free(mixed_commit_body);

    var mixed_commit = try client.fetchTransactionCommit(base_uri, mixed_commit_body);
    defer mixed_commit.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), mixed_commit.status);

    var user_three = try client.fetchLookup(base_uri, "users", "user:3", null);
    defer user_three.deinit(std.testing.allocator);
    var parsed_user_three = try parseJsonBody(UserName, std.testing.allocator, user_three.body);
    defer parsed_user_three.deinit();
    try std.testing.expectEqualStrings("Charlie", parsed_user_three.value.name);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(base_uri, "orders", "order:old", null));

    const invalid_commit_a = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"user:phantom":{"name":"Phantom"}}}
    );
    defer std.testing.allocator.free(invalid_commit_a);
    const invalid_commit_missing = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"missing:1":{"data":"should fail"}}}
    );
    defer std.testing.allocator.free(invalid_commit_missing);
    const invalid_commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.testing.allocator,
        &.{},
        &.{
            .{ .table_name = "users", .batch_json = invalid_commit_a },
            .{ .table_name = "missing", .batch_json = invalid_commit_missing },
        },
        "write",
    );
    defer std.testing.allocator.free(invalid_commit_body);

    const commit_uri = try raft_routes.Routes.join(std.testing.allocator, base_uri, "/transactions/commit");
    defer std.testing.allocator.free(commit_uri);
    var invalid_resp = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = commit_uri,
        .content_type = "application/json",
        .body = invalid_commit_body,
    });
    defer invalid_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), invalid_resp.status);

    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(base_uri, "users", "user:phantom", null));
}

test "public api smoke e2e queries after merge finalization" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-merge-e2e-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-merge-e2e-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 4112,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 4112,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var metadata_admin_server: metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_listener: std_http_listener.StdHttpListener = undefined;
    const metadata_api = try startMetadataAdminListener(
        std.testing.allocator,
        &svc,
        &metadata_admin_server,
        &metadata_admin_listener,
    );
    defer std.testing.allocator.free(metadata_api);
    defer metadata_admin_listener.deinit();

    var provisioned_read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
        svc.raft.readableLeaseRequester(),
    );
    var provisioned_write_source = table_writes.ProvisionedTableWriteSource.init(
        replica_root,
        table_catalog.CatalogSource.fromMetadataService(&svc),
    );
    var server = http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        http_server.StatusSource.fromMetadataService(&svc),
        provisioned_read_source.source(),
        provisioned_write_source.source(),
    );
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var client = http_client.ApiHttpClient.init(std.testing.allocator, executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.testing.allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "merge docs");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.testing.allocator);
    var created_table = try std.json.parseFromSlice(metadata_openapi.Table, std.testing.allocator, created.body, .{});
    defer created_table.deinit();
    try std.testing.expectEqualStrings("docs", created_table.value.name);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const projected_tables = try svc.listProjectedTables(std.testing.allocator);
    defer svc.freeProjectedTables(std.testing.allocator, projected_tables);
    const projected_ranges = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, projected_ranges);
    try std.testing.expectEqual(@as(usize, 1), projected_tables.len);
    try std.testing.expectEqual(@as(usize, 1), projected_ranges.len);

    const receiver_group_id = projected_ranges[0].group_id;
    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":49000,\"source_group_id\":{d},\"destination_group_id\":4902,\"split_key\":\"doc:m\"}}", .{
        receiver_group_id,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(metadata_api, "docs", split_body);

    var split_finalized = false;
    rounds = 0;
    while (rounds < 16) : (rounds += 1) {
        try svc.runRound();
        if (try svc.observeSplitTransition(49000)) |observation| {
            if (observation.status.phase == .finalized) {
                split_finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(split_finalized);

    var donor_group_id: u64 = 0;
    rounds = 0;
    while (rounds < 16) : (rounds += 1) {
        try metadata_client.triggerReallocate(metadata_api);
        donor_group_id = (try table_catalog.resolveGroupForKey(std.testing.allocator, table_catalog.CatalogSource.fromMetadataService(&svc), "docs", "doc:z")) orelse 0;
        if (donor_group_id != 0 and donor_group_id != receiver_group_id) break;
    }
    try std.testing.expect(donor_group_id != 0);
    try std.testing.expect(donor_group_id != receiver_group_id);

    const merge_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":49001,\"donor_group_id\":{d},\"receiver_group_id\":{d}}}", .{
        donor_group_id,
        receiver_group_id,
    });
    defer std.testing.allocator.free(merge_body);
    try metadata_client.requestTableMerge(metadata_api, "docs", merge_body);

    const projected_merges = try svc.listProjectedMergeTransitions(std.testing.allocator);
    defer svc.freeProjectedMergeTransitions(std.testing.allocator, projected_merges);
    try std.testing.expectEqual(@as(usize, 1), projected_merges.len);
    try std.testing.expectEqual(@as(u64, 49001), projected_merges[0].transition_id);

    var finalized = false;
    rounds = 0;
    while (rounds < 16) : (rounds += 1) {
        try svc.runRound();
        if (try svc.observeMergeTransition(49001)) |observation| {
            if (observation.receiver.phase == .finalized) {
                finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(finalized);

    rounds = 0;
    while (rounds < 16) : (rounds += 1) {
        try metadata_client.triggerReallocate(metadata_api);
        const updated_ranges = try svc.listProjectedRanges(std.testing.allocator);
        defer svc.freeProjectedRanges(std.testing.allocator, updated_ranges);
        const updated_merges = try svc.listProjectedMergeTransitions(std.testing.allocator);
        defer svc.freeProjectedMergeTransitions(std.testing.allocator, updated_merges);
        if (updated_ranges.len == 1 and updated_merges.len == 0) break;
    }

    const updated_ranges = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, updated_ranges);
    try std.testing.expectEqual(@as(usize, 1), updated_ranges.len);
    try std.testing.expectEqual(receiver_group_id, updated_ranges[0].group_id);
    try std.testing.expectEqualStrings("doc:a", updated_ranges[0].start_key);
    try std.testing.expectEqualStrings("doc:z", updated_ranges[0].end_key.?);

    const updated_merges = try svc.listProjectedMergeTransitions(std.testing.allocator);
    defer svc.freeProjectedMergeTransitions(std.testing.allocator, updated_merges);
    try std.testing.expectEqual(@as(usize, 0), updated_merges.len);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:z":{"title":"zeta","body":"hello merged world"}}}
    );
    defer std.testing.allocator.free(batch_body);
    var batch = try client.fetchBatch(base_uri, "docs", batch_body);
    defer batch.deinit(std.testing.allocator);
    var parsed_batch = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.testing.allocator, batch.body, .{});
    defer parsed_batch.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed_batch.value.inserted.?);

    var lookup = try client.fetchLookup(base_uri, "docs", "doc:z", null);
    defer lookup.deinit(std.testing.allocator);
    var parsed_lookup = try parseJsonBody(LookupTitle, std.testing.allocator, lookup.body);
    defer parsed_lookup.deinit();
    try std.testing.expectEqualStrings("zeta", parsed_lookup.value.title);

    const query_body = try test_contract_helpers.encodeMatchQueryRequest(std.testing.allocator, "body", "merged", &.{ "title", "body" }, 10);
    defer std.testing.allocator.free(query_body);
    var query = try client.fetchQuery(base_uri, "docs", query_body);
    defer query.deinit(std.testing.allocator);
    var query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, query.body, .{});
    defer query_responses.deinit();
    try std.testing.expectEqual(@as(usize, 1), query_responses.value.responses.?.len);
    const query_result = query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 1), query_result.hits.?.total.?);
    try std.testing.expectEqualStrings("doc:z", query_result.hits.?.hits.?[0]._id);

    const delete_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"deletes\":[\"doc:z\"]}");
    defer std.testing.allocator.free(delete_body);
    var deleted = try client.fetchBatch(base_uri, "docs", delete_body);
    defer deleted.deinit(std.testing.allocator);
    var parsed_deleted = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.testing.allocator, deleted.body, .{});
    defer parsed_deleted.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed_deleted.value.deleted.?);

    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(base_uri, "docs", "doc:z", null));

    const deleted_query_body = try test_contract_helpers.encodeMatchQueryRequest(std.testing.allocator, "body", "merged", &.{ "title", "body" }, 10);
    defer std.testing.allocator.free(deleted_query_body);
    var deleted_query = try client.fetchQuery(base_uri, "docs", deleted_query_body);
    defer deleted_query.deinit(std.testing.allocator);
    var deleted_query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, deleted_query.body, .{});
    defer deleted_query_responses.deinit();
    const deleted_query_result = deleted_query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 0), deleted_query_result.hits.?.total.?);
}
